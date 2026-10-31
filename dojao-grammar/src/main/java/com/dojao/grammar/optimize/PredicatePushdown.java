package com.dojao.grammar.optimize;

import com.dojao.grammar.analyzer.expression.Expression;
import com.dojao.grammar.analyzer.plan.LogicalPlan;

import java.util.*;

/**
 * 谓词下推：把 WHERE 条件尽可能下推到最底层的表扫描。
 *
 * <p>对于带 JOIN 的 SELECT：
 * <ol>
 *   <li>在顶层 AND 处拆分 WHERE</li>
 *   <li>判断每个谓词引用哪些 relation</li>
 *   <li>单 relation 引用 → 下推到该 relation 的 filter</li>
 *   <li>跨 relation 引用 → 保留在主 WHERE</li>
 * </ol>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/5
 */
public final class PredicatePushdown implements OptimizerRule {

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        return switch (plan) {
            case LogicalPlan.Select s -> pushdownSelect(s);
            case LogicalPlan.With w -> pushdownWith(w);
            case LogicalPlan.Insert i -> pushdownInsert(i);
            case LogicalPlan.SetOperation so -> pushdownSetOp(so);
            case LogicalPlan.CreateTableAsSelect ctas -> pushdownCtas(ctas);
            case LogicalPlan.CreateView cv -> pushdownCreateView(cv);
            case LogicalPlan.Explain e -> pushdownExplain(e);
            default -> plan;
        };
    }

    // ==================== Select ====================

    private LogicalPlan.Select pushdownSelect(LogicalPlan.Select select) {
        // ★先递归进 FROM 派生表 / CTE 查询体：优化器入口只拿得到最外层 Select，
        //   而 `FROM (SELECT ... FROM a JOIN b ON ... WHERE a.dt=.. AND b.dt=..) t` 的
        //   JOIN+WHERE 在 subquery 槽里。不递归 ⇒ 该层永不优化 ⇒ WHERE 残留在 JOIN 之上，
        //   Planner 只能「先 join 后过滤」（线上 30M×3.5M 全量排序归并）。必须在下面的
        //   「没有 JOIN 或没有 WHERE」早退<b>之前</b>做 —— 外层查询（如 GROUP BY 派生表）本身
        //   往往既无 JOIN 也无 WHERE，早退会把子查询一起挡在外面。
        List<LogicalPlan.ResolvedRelation> relations = pushdownRelations(select.fromRelations());
        if (relations != select.fromRelations()) {
            select = new LogicalPlan.Select(select.selectItems(), relations, select.joins(),
                    select.where(), select.groupBy(), select.having(), select.scalarSubqueries(),
                    select.orderBy(), select.limit(), select.offset(), select.distinct());
        }
        List<LogicalPlan.JoinInfo> joins = select.joins();
        Optional<Expression> where = select.where();

        // 单表 + 标量子查询（典型 EXISTS / NOT EXISTS / IN (子查询)）：没有显式 JOIN 时原本直接早退，
        // 但 Planner 会在去相关阶段把子查询变成 LEFT JOIN，并把整条（重写后的）WHERE 统一挂到 join 之上 ——
        // 于是 `a.dt='...'` 这类只引用外层表的普通谓词永远到不了扫描端 ⇒ 连接器拿不到分区谓词、
        // 分区裁剪失效（压测：单分区 EXISTS 仍扫描全表 9126 万行 / 1.9GB，占查询耗时 94%）。
        // 这里先把「不含子查询 + 只引用该关系」的合取项搬进 rel.filter；子查询谓词留在 WHERE
        // 供 Planner 去相关改写（挪走会覆盖不到，语义静默丢失）。
        if (joins.isEmpty()) {
            if (where.isPresent()
                    && relations.size() == 1
                    && !select.scalarSubqueries().isEmpty()
                    && relations.get(0).subquery().isEmpty()) {
                List<Expression> keep = new ArrayList<>();
                List<Expression> push = new ArrayList<>();
                for (Expression pred : splitPredicates(where.get(), relations)) {
                    if (mentionsSubquery(pred)) {
                        keep.add(pred);            // EXISTS / (SELECT ...) 必须留给去相关
                    } else if (referencedAliases(pred, relations).size() == 1) {
                        push.add(pred);
                    } else {
                        keep.add(pred);
                    }
                }
                if (!push.isEmpty()) {
                    LogicalPlan.ResolvedRelation rel = relations.get(0);
                    LogicalPlan.ResolvedRelation pushed = new LogicalPlan.ResolvedRelation(
                            rel.catalog(), rel.schema(), rel.table(), rel.alias(), rel.columns(),
                            Optional.of(combineWithExisting(rel.filter(), push)),
                            rel.subquery());
                    Optional<Expression> remaining = keep.isEmpty()
                            ? Optional.empty() : Optional.of(combinePredicates(keep));
                    return new LogicalPlan.Select(select.selectItems(), List.of(pushed),
                            select.joins(), remaining, select.groupBy(), select.having(),
                            select.scalarSubqueries(), select.orderBy(), select.limit(),
                            select.offset(), select.distinct());
                }
            }
            // 没有任何 JOIN：单表查询，WHERE 由扫描端自己处理
            return select;
        }

        // 把每条谓词分配到对应的 relation
        Map<String, List<Expression>> perRelationPredicates = new LinkedHashMap<>();
        List<Expression> remainingPredicates = new ArrayList<>();

        // ON 条件里的单侧条件（如 `ON a.user_id = b.user_id AND b.event_type = 'add_cart'`）
        // 同样要下推到扫描端：它们不是连接键，Planner 的键解析只看等值两侧、遇到单侧条件
        // 直接报「无法解析 JOIN 键」⇒ 整条 SQL 规划失败（压测缺陷 2）。提前过滤还能让 Hive
        // 分区裁剪生效。安全性按 join 类型判断（见 safeOnAliases）。
        List<LogicalPlan.JoinInfo> newJoins =
            pushdownJoinConditions(joins, relations, perRelationPredicates);

        if (where.isPresent()) {
            // 外连接的「空补位侧」不能下推：LEFT JOIN 的右表、RIGHT JOIN 的左链、FULL JOIN 两侧。
            // WHERE 里引用空补位侧的谓词语义上属于 join **之后**（典型：`WHERE o.user_id IS NULL`
            // 反连接）。若推到该表的扫描过滤，未匹配行不再是 NULL、谓词又被从 WHERE 摘掉 ⇒
            // 过滤与 join 语义双双失效（实测 count(*) 退化为左表全量）。
            Set<String> nullSupplying = nullSupplyingAliases(joins);

            for (Expression pred : splitPredicates(where.get(), relations)) {
                // EXISTS 子查询谓词不能下推：Planner 需要它在 WHERE 原文里做去相关改写
                // （替换为派生计数列判定）；推进关系 filter 后改写覆盖不到，语义会静默丢失。
                if (containsExistsSubquery(pred)) {
                    remainingPredicates.add(pred);
                    continue;
                }
                Set<String> referencedAliases = referencedAliases(pred, relations);
                if (referencedAliases.size() == 1) {
                    String alias = referencedAliases.iterator().next();
                    if (nullSupplying.contains(alias)) {
                        // 空补位侧：必须留在 join 之后（见上方 nullSupplying 说明）
                        remainingPredicates.add(pred);
                    } else {
                        // 可以下推到单个 relation
                        perRelationPredicates
                            .computeIfAbsent(alias, k -> new ArrayList<>())
                            .add(pred);
                    }
                } else {
                    // 跨 relation：保留在主 WHERE
                    remainingPredicates.add(pred);
                }
            }
        }

        // 没有任何可下推的谓词、ON 也没动过：原样返回（优化器按结构相等收敛）
        if (newJoins == joins
                && perRelationPredicates.isEmpty()
                && remainingPredicates.isEmpty()) {
            return select;
        }

        // 构建新的 ResolvedRelation 列表（带下推的 filter）
        List<LogicalPlan.ResolvedRelation> newRelations = new ArrayList<>();
        for (LogicalPlan.ResolvedRelation rel : relations) {
            String alias = rel.alias().orElse(rel.table()).toLowerCase();
            List<Expression> relPreds =
                perRelationPredicates.getOrDefault(alias, List.of());
            Optional<Expression> filter;
            if (relPreds.isEmpty()) {
                filter = rel.filter();
            } else {
                filter = Optional.of(
                    combineWithExisting(rel.filter(), relPreds));
            }
            newRelations.add(new LogicalPlan.ResolvedRelation(
                rel.catalog(), rel.schema(), rel.table(),
                rel.alias(), rel.columns(), filter, rel.subquery()));
        }

        // 新的 WHERE（仅保留跨 relation 谓词）
        Optional<Expression> newWhere;
        if (remainingPredicates.isEmpty()) {
            newWhere = Optional.empty();
        } else {
            newWhere = Optional.of(combinePredicates(remainingPredicates));
        }

        return new LogicalPlan.Select(
            select.selectItems(), newRelations, newJoins,
            newWhere, select.groupBy(), select.having(), select.scalarSubqueries(),
            select.orderBy(), select.limit(), select.offset(), select.distinct());
    }

    /**
     * 把 ON 条件里<b>只引用单侧</b>的顶层合取项搬进该侧的 relation filter。
     *
     * <p>语义安全性按 join 类型区分（与 WHERE 下推的 {@code nullSupplying} 不同口径）：</p>
     * <ul>
     *   <li><b>INNER</b>：ON 等价于 WHERE，两侧单侧谓词都可提前求值；</li>
     *   <li><b>LEFT</b>：只有<b>右表</b>（非保留侧）可以提前过滤 —— 右行不满足谓词时本来也不会
     *       参与匹配，删掉它们不改变输出；左链是保留侧，`ON a.x=1` 的意思是「x≠1 的左行仍然
     *       输出（右侧补 NULL）」，提前过滤会把整行删掉 ⇒ <b>禁止</b>；</li>
     *   <li><b>RIGHT</b>：镜像 —— 只有左链可提前过滤，右表禁止；</li>
     *   <li><b>FULL</b>：两侧都保留，一个都不能动。</li>
     * </ul>
     *
     * <p>约束：本 join 至少要留一个合取项在 ON 里（否则等值键全被搬走，Planner 会认为
     * 「未找到等值 JOIN 键」，且非等值外连接本来就不支持）；含 EXISTS 子查询的条件不搬
     * （Planner 要去相关改写）。</p>
     *
     * @return 新 join 列表；没有任何搬动时返回<b>同一实例</b>（调用方按身份判断）
     */
    private List<LogicalPlan.JoinInfo> pushdownJoinConditions(
            List<LogicalPlan.JoinInfo> joins,
            List<LogicalPlan.ResolvedRelation> relations,
            Map<String, List<Expression>> perRelationPredicates) {
        List<LogicalPlan.JoinInfo> out = null;
        Set<String> leftChain = new LinkedHashSet<>();
        leftChain.add(normalizeAlias(joins.get(0).leftAlias()));
        for (int idx = 0; idx < joins.size(); idx++) {
            LogicalPlan.JoinInfo join = joins.get(idx);
            String rightAlias = normalizeAlias(join.rightAlias());
            Expression condition = join.condition();

            if (condition != null && join.joinType() != LogicalPlan.JoinType.CROSS) {
                List<String> parts = condition instanceof Expression.OpaqueExpr oe
                        ? splitOnTopLevelAnd(oe.originalText())
                        : List.of(condition.text());
                Set<String> safeAliases = safeOnAliases(join.joinType(), leftChain, rightAlias);
                List<Expression> keep = new ArrayList<>(parts.size());
                List<Map.Entry<String, Expression>> move = new ArrayList<>();
                for (String part : parts) {
                    if (part.isBlank()) continue;
                    Expression p = new Expression.OpaqueExpr(part);
                    if (!safeAliases.isEmpty() && !containsExistsSubquery(p)) {
                        Set<String> aliases = referencedAliases(p, relations);
                        if (aliases.size() == 1) {
                            String alias = aliases.iterator().next();
                            // 只搬进<b>基表</b>扫描过滤：派生表的 rel.filter 在未展平时不会被执行
                            // （buildScan 的派生分支只认外层 WHERE），搬过去等于丢谓词。
                            if (safeAliases.contains(alias) && isBaseRelation(alias, relations)) {
                                move.add(Map.entry(alias, p));
                                continue;
                            }
                        }
                    }
                    keep.add(p);
                }
                // 键全被搬走 / 没有可搬的：保持原样（等值键必须留在 ON 里给 Planner）
                if (!move.isEmpty() && !keep.isEmpty()) {
                    for (Map.Entry<String, Expression> e : move) {
                        perRelationPredicates
                            .computeIfAbsent(e.getKey(), k -> new ArrayList<>())
                            .add(e.getValue());
                    }
                    if (out == null) {
                        out = new ArrayList<>(joins);
                    }
                    out.set(idx, new LogicalPlan.JoinInfo(join.joinType(),
                            join.leftAlias(), join.rightAlias(),
                            combinePredicates(keep)));
                }
            }
            leftChain.add(rightAlias);
        }
        return out == null ? joins : out;
    }

    /** 别名对应的是否基表（非派生表 / CTE 重放）。 */
    private static boolean isBaseRelation(String alias,
                                          List<LogicalPlan.ResolvedRelation> relations) {
        for (LogicalPlan.ResolvedRelation rel : relations) {
            if (normalizeAlias(rel.alias().orElse(rel.table())).equals(alias)) {
                return rel.subquery().isEmpty();
            }
        }
        return false;
    }

    /**
     * ON 条件下推的「安全侧」：即该 join 的<b>非保留侧</b>别名集合（左链 / 右侧已归一为小写）。
     */
    private static Set<String> safeOnAliases(LogicalPlan.JoinType type,
                                             Set<String> leftChain,
                                             String rightAlias) {
        return switch (type) {
            case INNER, CROSS -> {
                Set<String> all = new LinkedHashSet<>(leftChain);
                all.add(rightAlias);
                yield all;
            }
            case LEFT -> Set.of(rightAlias);
            case RIGHT -> Set.copyOf(leftChain);
            case FULL -> Set.of();
        };
    }

    /**
     * 谓词引用了哪些 relation 的别名（小写）。
     *
     * <p>优先用分析器解析出的绑定（{@code resolvedColumns}）；裸列名按「哪些 relation 有这一列」
     * 展开 —— 命中多个时返回多个别名，调用方据此保守地不下推（歧义列绝不能猜一侧）。</p>
     */
    private Set<String> referencedAliases(Expression pred,
                                          List<LogicalPlan.ResolvedRelation> relations) {
        Set<ColumnRefExtractor.ColRef> refs = ColumnRefExtractor.extract(pred, relations);
        Set<String> referencedAliases = new LinkedHashSet<>();
        for (ColumnRefExtractor.ColRef ref : refs) {
            if (ref.qualifier() != null) {
                referencedAliases.add(ref.qualifier().toLowerCase(Locale.ROOT));
            } else {
                // 裸列名：查找它属于哪些 relation
                for (LogicalPlan.ResolvedRelation rel : relations) {
                    if (rel.columns().stream()
                            .anyMatch(c -> c.equalsIgnoreCase(ref.columnName()))) {
                        referencedAliases.add(
                            rel.alias().orElse(rel.table()).toLowerCase(Locale.ROOT));
                    }
                }
            }
        }
        return referencedAliases;
    }

    /**
     * 对每个 FROM 关系，若其 subquery 槽里有查询体则递归下推（派生表 / 内联 CTE 均落在这里）。
     * <p>查询体无变化时返回原列表（优化器按结构相等收敛，不做多余重建）。</p>
     */
    private List<LogicalPlan.ResolvedRelation> pushdownRelations(
            List<LogicalPlan.ResolvedRelation> relations) {
        List<LogicalPlan.ResolvedRelation> out = null;
        for (int i = 0; i < relations.size(); i++) {
            LogicalPlan.ResolvedRelation rel = relations.get(i);
            if (rel.subquery().isEmpty()) {
                continue;
            }
            LogicalPlan inner = rel.subquery().get();
            LogicalPlan optimized = apply(inner);
            if (optimized.equals(inner)) {
                continue;
            }
            if (out == null) {
                out = new ArrayList<>(relations);
            }
            out.set(i, new LogicalPlan.ResolvedRelation(
                    rel.catalog(), rel.schema(), rel.table(), rel.alias(),
                    rel.columns(), rel.filter(), Optional.of(optimized)));
        }
        return out == null ? relations : out;
    }

    /**
     * 左深 join 链里「空补位侧」的关系别名集合（小写）。
     *
     * <p>LEFT JOIN 的右表、RIGHT JOIN 的左链、FULL JOIN 的两侧：未匹配时该侧列被补 NULL。
     * WHERE 中只引用这些关系的谓词不能在 join 之前求值 —— 下推会改变谓词看到的行
     * （NULL 补位行消失），且从 WHERE 摘除后 join 之后的过滤彻底丢失。</p>
     */
    private static Set<String> nullSupplyingAliases(List<LogicalPlan.JoinInfo> joins) {
        if (joins.isEmpty()) return Set.of();
        Set<String> leftChain = new LinkedHashSet<>();
        leftChain.add(normalizeAlias(joins.get(0).leftAlias()));
        Set<String> nullSupplying = new LinkedHashSet<>();
        for (LogicalPlan.JoinInfo ji : joins) {
            String right = normalizeAlias(ji.rightAlias());
            switch (ji.joinType()) {
                case LEFT -> nullSupplying.add(right);
                case RIGHT -> nullSupplying.addAll(leftChain);
                case FULL -> {
                    nullSupplying.addAll(leftChain);
                    nullSupplying.add(right);
                }
                default -> { }
            }
            leftChain.add(right);
        }
        return nullSupplying;
    }

    private static String normalizeAlias(String alias) {
        return alias == null ? "" : alias.toLowerCase(Locale.ROOT);
    }

    /**
     * 谓词原文是否含子查询：{@code EXISTS (} 关键字，或任一 {@code ( SELECT} 形式
     * （标量子查询 / {@code IN (SELECT ...)} / {@code ANY|ALL|SOME (SELECT ...)}）。
     *
     * <p>引号内的文本跳过，避免把字符串字面量里的 {@code "(select"} 误判成子查询。</p>
     */
    static boolean mentionsSubquery(Expression pred) {
        if (containsExistsSubquery(pred)) return true;
        String text = pred.text();
        char quote = 0;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (quote != 0) {
                if (c == quote) quote = 0;
                continue;
            }
            if (c == '\'' || c == '"' || c == '`') { quote = c; continue; }
            if (c != '(') continue;
            int k = i + 1;
            while (k < text.length() && Character.isWhitespace(text.charAt(k))) k++;
            if (k + 6 <= text.length()
                    && text.regionMatches(true, k, "SELECT", 0, 6)
                    && (k + 6 == text.length() || !isIdentChar(text.charAt(k + 6)))) {
                return true;
            }
        }
        return false;
    }

    /** 谓词原文是否含 {@code [NOT] EXISTS (} 子查询（词边界 + 允许空白）。 */
    static boolean containsExistsSubquery(Expression pred) {
        String text = pred.text();
        int i = 0;
        while (i < text.length()) {
            int p = text.toUpperCase(Locale.ROOT).indexOf("EXISTS", i);
            if (p < 0) return false;
            boolean leftOk = p == 0 || !isIdentChar(text.charAt(p - 1));
            int e = p + 6;
            boolean rightOk = e >= text.length() || !isIdentChar(text.charAt(e));
            if (leftOk && rightOk) {
                int k = e;
                while (k < text.length() && Character.isWhitespace(text.charAt(k))) k++;
                if (k < text.length() && text.charAt(k) == '(') return true;
            }
            i = e;
        }
        return false;
    }

    // ==================== 谓词拆分 ====================

    /** 在顶层 AND 处拆分 WHERE 表达式（处理括号嵌套）。 */
    private List<Expression> splitPredicates(Expression where,
                                              List<LogicalPlan.ResolvedRelation> relations) {
        if (where instanceof Expression.OpaqueExpr oe) {
            List<String> parts = splitOnTopLevelAnd(oe.originalText());
            if (parts.size() <= 1) return List.of(where);
            return parts.stream()
                .<Expression>map(Expression.OpaqueExpr::new)
                .toList();
        }
        return List.of(where);
    }

    /**
     * 在顶层 AND 处拆分（括号深度 0、引号外），正确处理：
     * <ul>
     *   <li>括号嵌套（{@code (a AND b) AND c} 只在最外层 AND 切分）；</li>
     *   <li>{@code BETWEEN lo AND hi} —— 区间里的 "and" 是<b>分隔符而非合取</b>，
     *       不能把 {@code a.user_id between 3 and 8} 错切成 {@code [a.user_id between 3, 8]}
     *       （后者 8 会被当作残留 WHERE → 规划报"复杂 WHERE"）。</li>
     * </ul>
     */
    List<String> splitOnTopLevelAnd(String text) {
        List<String> result = new ArrayList<>();
        int depth = 0;
        int lastSplit = 0;
        char quote = 0;
        // 最近扫到 BETWEEN（深度 0）→ 下一个顶层 AND 是区间分隔符，切分时必须跳过
        boolean inBetween = false;

        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (quote != 0) {
                if (c == quote) quote = 0;
                continue;
            }
            if (c == '\'' || c == '"' || c == '`') { quote = c; continue; }
            if (c == '(') { depth++; continue; }
            if (c == ')') { depth--; continue; }
            if (depth != 0) continue;
            // 只从词首（前一个字符非标识符）识别关键字
            if (i > 0 && isIdentChar(text.charAt(i - 1))) continue;

            if (text.regionMatches(true, i, "BETWEEN", 0, 7)) {
                int e = i + 7;
                if (e >= text.length() || !isIdentChar(text.charAt(e))) {
                    inBetween = true;
                    i = e - 1;
                }
                continue;
            }
            if (i + 3 <= text.length()
                    && text.regionMatches(true, i, "AND", 0, 3)) {
                int e = i + 3;
                if (e >= text.length() || !isIdentChar(text.charAt(e))) {
                    if (inBetween) {
                        // BETWEEN lo AND hi 的区间分隔符：跳过，不切分（边界扫描结束）
                        inBetween = false;
                        i = e - 1;
                        continue;
                    }
                    result.add(text.substring(lastSplit, i).trim());
                    lastSplit = e;
                    i = e - 1;
                }
            }
        }

        String remainder = text.substring(lastSplit).trim();
        if (!remainder.isEmpty()) {
            result.add(remainder);
        }
        return result;
    }

    private static boolean isIdentChar(char c) {
        return Character.isLetterOrDigit(c) || c == '_';
    }

    // ==================== 谓词合并 ====================

    Expression combinePredicates(List<Expression> preds) {
        if (preds.size() == 1) return preds.get(0);
        String combined = preds.stream()
            .map(Expression::text)
            .reduce((a, b) -> a + " AND " + b)
            .orElse("");
        return new Expression.OpaqueExpr(combined);
    }

    private Expression combineWithExisting(Optional<Expression> existing,
                                            List<Expression> newPreds) {
        List<Expression> all = new ArrayList<>();
        existing.ifPresent(all::add);
        all.addAll(newPreds);
        return combinePredicates(all);
    }

    // ==================== 递归 handlers ====================

    private LogicalPlan.With pushdownWith(LogicalPlan.With w) {
        List<LogicalPlan.CteDef> ctes = w.ctes().stream()
            .map(cte -> new LogicalPlan.CteDef(
                cte.name(), cte.columns(), apply(cte.query())))
            .toList();
        return new LogicalPlan.With(ctes, apply(w.query()));
    }

    private LogicalPlan.Insert pushdownInsert(LogicalPlan.Insert i) {
        return new LogicalPlan.Insert(i.catalog(), i.schema(), i.table(),
            i.columnAliases(), i.overwrite(), apply(i.query()));
    }

    private LogicalPlan.SetOperation pushdownSetOp(LogicalPlan.SetOperation so) {
        return new LogicalPlan.SetOperation(so.opType(), so.distinct(),
            apply(so.left()), apply(so.right()),
            so.orderBy(), so.limit(), so.offset());
    }

    private LogicalPlan.CreateTableAsSelect pushdownCtas(
            LogicalPlan.CreateTableAsSelect ctas) {
        return new LogicalPlan.CreateTableAsSelect(ctas.qualifiedName(),
            ctas.columnAliases(), ctas.properties(), ctas.comment(),
            ctas.ifNotExists(), apply(ctas.query()));
    }

    private LogicalPlan.CreateView pushdownCreateView(LogicalPlan.CreateView cv) {
        return new LogicalPlan.CreateView(cv.qualifiedName(), cv.orReplace(),
            apply(cv.query()));
    }

    private LogicalPlan.Explain pushdownExplain(LogicalPlan.Explain e) {
        return new LogicalPlan.Explain(apply(e.statement()), e.analyze(), e.verbose());
    }
}
