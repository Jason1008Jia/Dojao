package com.dojao.grammar.optimize;

import com.dojao.grammar.analyzer.expression.Expression;
import com.dojao.grammar.analyzer.plan.LogicalPlan;

import java.util.*;

/**
 * 列裁剪：只保留查询实际需要的列，丢弃未引用的列。
 *
 * <p>算法：
 * <ol>
 *   <li>收集 SELECT 中所有列引用（selectItems / WHERE / GROUP BY / HAVING /
 *       JOIN condition / relation filter / window functions）</li>
 *   <li>按 relation alias 分组</li>
 *   <li>对每个 ResolvedRelation，裁剪 columns 为仅需要的列</li>
 *   <li>对于 CTE，把裁剪需求向上传播到 CTE 的 selectItems</li>
 * </ol>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/5
 */
public final class ColumnPruning implements OptimizerRule {

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        return switch (plan) {
            case LogicalPlan.Select s -> pruneSelect(s);
            case LogicalPlan.With w -> pruneWith(w);
            case LogicalPlan.Insert i -> pruneInsert(i);
            case LogicalPlan.SetOperation so -> pruneSetOp(so);
            case LogicalPlan.CreateTableAsSelect ctas -> pruneCtas(ctas);
            case LogicalPlan.CreateView cv -> pruneCreateView(cv);
            case LogicalPlan.Explain e -> pruneExplain(e);
            default -> plan;
        };
    }

    // ==================== Select ====================

    private LogicalPlan.Select pruneSelect(LogicalPlan.Select select) {
        List<LogicalPlan.ResolvedRelation> relations = select.fromRelations();

        // 1) 收集所有列引用
        Set<ColumnRefExtractor.ColRef> allRefs = new LinkedHashSet<>();

        for (LogicalPlan.SelectItem item : select.selectItems()) {
            allRefs.addAll(ColumnRefExtractor.extract(item.expression(), relations));
        }
        select.where().ifPresent(w ->
            allRefs.addAll(ColumnRefExtractor.extract(w, relations)));
        allRefs.addAll(ColumnRefExtractor.extractAll(select.groupBy(), relations));
        select.having().ifPresent(h ->
            allRefs.addAll(ColumnRefExtractor.extract(h, relations)));
        for (LogicalPlan.JoinInfo join : select.joins()) {
            allRefs.addAll(ColumnRefExtractor.extract(join.condition(), relations));
        }
        for (LogicalPlan.ResolvedRelation rel : relations) {
            rel.filter().ifPresent(f ->
                allRefs.addAll(ColumnRefExtractor.extract(f, relations)));
        }
        // ★EXISTS / 标量子查询的相关外层列（如 EXISTS(... WHERE o.user_id = u.user_id) 的 u.user_id）
        // 只出现在子查询原文里，WHERE 的 OpaqueExpr.resolvedColumns 因作用域隔离拿不到它们。
        // 不在这里登记 ⇒ 关联列被裁掉 ⇒ 规划期「无法解析 JOIN 键（左: [province]）」。
        for (Expression.ScalarSubquery sub : select.scalarSubqueries()) {
            allRefs.addAll(ColumnRefExtractor.extract(sub, relations));
        }
        // ★UNNEST 的展开表达式引用的左表列（如 UNNEST(json_extract(t.payload, '$.items')) 的 t.payload）
        // 只出现在关系 DTO 里，不登记就会被裁掉 → 运行期求值找不到列。
        for (LogicalPlan.ResolvedRelation rel : relations) {
            if (rel.subquery().orElse(null) instanceof LogicalPlan.Unnest un) {
                allRefs.addAll(ColumnRefExtractor.extract(un.expression(), relations));
            }
        }

        // 2) 检查 SELECT *
        boolean hasStar = select.selectItems().stream()
            .anyMatch(item -> {
                if (item.expression() instanceof Expression.OpaqueExpr oe) {
                    return "*".equals(oe.originalText().trim());
                }
                return false;
            });

        // 3) 按 relation alias 分组
        Map<String, Set<String>> neededByAlias = new LinkedHashMap<>();
        for (ColumnRefExtractor.ColRef ref : allRefs) {
            if (ref.qualifier() != null) {
                neededByAlias
                    .computeIfAbsent(ref.qualifier().toLowerCase(),
                        k -> new LinkedHashSet<>())
                    .add(ref.columnName().toLowerCase());
            }
        }

        // 4) 修剪每个 relation 的 columns（并对 FROM 子查询递归传播需求）
        List<LogicalPlan.ResolvedRelation> prunedRelations = new ArrayList<>();
        for (LogicalPlan.ResolvedRelation rel : relations) {
            String alias = rel.alias().orElse(rel.table()).toLowerCase();
            List<String> declared = rel.columns(); // 派生表对外声明列（与内层 selectItems 对齐）
            List<String> prunedColumns;
            if (hasStar) {
                prunedColumns = rel.columns(); // 保留全部
            } else {
                Set<String> needed = neededByAlias.getOrDefault(alias, Set.of());
                if (needed.isEmpty()) {
                    // 无引用 → 至少保留 filter 中引用的列
                    prunedColumns = columnsFromFilter(rel);
                    if (prunedColumns.isEmpty() && !rel.columns().isEmpty()) {
                        // 至少保留第一列（保证有效扫描）
                        prunedColumns = List.of(rel.columns().get(0));
                    }
                } else {
                    // 保留需要的列，保持原顺序
                    prunedColumns = new ArrayList<>();
                    for (String col : rel.columns()) {
                        if (needed.contains(col.toLowerCase())) {
                            prunedColumns.add(col);
                        }
                    }
                }
            }
            // 需求穿透进 FROM 子查询：把外层真正引用的列集传下去，使内层扫描读更少列。
            // 只在无 `*`（无从按名裁）且外层确有需求时进行；歧义一律原样保留。
            Optional<LogicalPlan> prunedSub = rel.subquery();
            if (!hasStar && rel.subquery().isPresent()) {
                Set<String> needed = neededByAlias.getOrDefault(alias, Set.of());
                if (!needed.isEmpty()) {
                    prunedSub = Optional.of(
                        propagateIntoSubquery(rel.subquery().get(), needed, declared));
                }
            }
            prunedRelations.add(new LogicalPlan.ResolvedRelation(
                rel.catalog(), rel.schema(), rel.table(),
                rel.alias(), prunedColumns, rel.filter(), prunedSub));
        }

        return new LogicalPlan.Select(
            select.selectItems(), prunedRelations, select.joins(),
            select.where(), select.groupBy(), select.having(), select.scalarSubqueries(),
            select.orderBy(), select.limit(), select.offset(), select.distinct());
    }

    /** 从 relation.filter 中提取该 relation 自身需要的列。 */
    private List<String> columnsFromFilter(LogicalPlan.ResolvedRelation rel) {
        if (rel.filter().isEmpty()) return List.of();
        Set<ColumnRefExtractor.ColRef> filterRefs =
            ColumnRefExtractor.extract(rel.filter().get(), List.of(rel));
        List<String> cols = new ArrayList<>();
        for (ColumnRefExtractor.ColRef fr : filterRefs) {
            if (!cols.contains(fr.columnName())) {
                cols.add(fr.columnName());
            }
        }
        return cols;
    }

    // ==================== CTE 列裁剪 ====================

    private LogicalPlan.With pruneWith(LogicalPlan.With w) {
        // 先递归裁剪 CTE 内部查询
        List<LogicalPlan.CteDef> prunedCtes = new ArrayList<>();
        for (LogicalPlan.CteDef cte : w.ctes()) {
            prunedCtes.add(new LogicalPlan.CteDef(
                cte.name(), cte.columns(), apply(cte.query())));
        }

        // 裁剪主查询
        LogicalPlan prunedQuery = apply(w.query());

        // 收集「每个 CTE 被引用的列集」（lowercased CTE 名 → 需要的列集合并）。
        // 来源 = 主查询 + 各 CTE 已裁剪的 body（后者覆盖 CTE 引用另一 CTE 的链式需求）。
        Map<String, Set<String>> neededByCte =
            collectCteColumnRefs(prunedQuery, w.ctes(), prunedCtes);

        // 根据引用裁剪 CTE 的 selectItems（主查询整体是 `*` 时无从按名裁 → 跳过）
        if (!neededByCte.isEmpty() && !hasStarSelect(prunedQuery)) {
            List<LogicalPlan.CteDef> finalCtes = new ArrayList<>();
            for (int i = 0; i < prunedCtes.size(); i++) {
                LogicalPlan.CteDef cte = prunedCtes.get(i);
                LogicalPlan.CteDef originalCte = w.ctes().get(i);
                Set<String> needed =
                    neededByCte.get(originalCte.name().toLowerCase());
                if (needed != null && !needed.isEmpty()) {
                    finalCtes.add(pruneCteToColumns(cte, originalCte, needed));
                } else {
                    finalCtes.add(cte);
                }
            }
            return new LogicalPlan.With(finalCtes, prunedQuery);
        }

        return new LogicalPlan.With(prunedCtes, prunedQuery);
    }

    /** 收集「每个 CTE 被引用的列集」：主查询 + 各 CTE 已裁剪的 body 之并集。 */
    private Map<String, Set<String>> collectCteColumnRefs(LogicalPlan query,
                                                          List<LogicalPlan.CteDef> ctes,
                                                          List<LogicalPlan.CteDef> prunedCtes) {
        Map<String, Set<String>> refs = new LinkedHashMap<>();
        Set<String> cteNames = new LinkedHashSet<>();
        for (LogicalPlan.CteDef cte : ctes) {
            cteNames.add(cte.name().toLowerCase());
        }
        collectCteRefsRecursive(query, cteNames, refs);
        // CTE body 内引用其它 CTE 时，其 rel.columns() 也已被 apply(body) 收窄 → 一并采集。
        for (LogicalPlan.CteDef cte : prunedCtes) {
            collectCteRefsRecursive(cte.query(), cteNames, refs);
        }
        return refs;
    }

    private void collectCteRefsRecursive(LogicalPlan plan,
                                          Set<String> cteNames,
                                          Map<String, Set<String>> refs) {
        if (plan instanceof LogicalPlan.Select s) {
            for (LogicalPlan.ResolvedRelation rel : s.fromRelations()) {
                String tableName = rel.table().toLowerCase();
                if (cteNames.contains(tableName)) {
                    // rel.columns() 此时已被外层裁剪为真正引用的子集
                    refs.computeIfAbsent(tableName, k -> new LinkedHashSet<>())
                        .addAll(rel.columns().stream().map(String::toLowerCase).toList());
                }
                // 递归进 FROM 子查询（CTE 也可能被子查询引用）
                rel.subquery().ifPresent(sub ->
                    collectCteRefsRecursive(sub, cteNames, refs));
            }
        }
        // 递归子计划
        switch (plan) {
            case LogicalPlan.With w -> collectCteRefsRecursive(w.query(), cteNames, refs);
            case LogicalPlan.Insert i -> collectCteRefsRecursive(i.query(), cteNames, refs);
            case LogicalPlan.SetOperation so -> {
                collectCteRefsRecursive(so.left(), cteNames, refs);
                collectCteRefsRecursive(so.right(), cteNames, refs);
            }
            case LogicalPlan.CreateTableAsSelect ctas ->
                collectCteRefsRecursive(ctas.query(), cteNames, refs);
            case LogicalPlan.CreateView cv ->
                collectCteRefsRecursive(cv.query(), cteNames, refs);
            default -> {}
        }
    }

    /**
     * 裁剪 CTE 的 selectItems 以仅保留外层引用的列，并同步收窄 {@code CteDef.columns()}
     * （否则内联时 {@code Planner.inlineCtes} 抄 {@code def.columns()} 会把收窄结果丢光）。
     */
    private LogicalPlan.CteDef pruneCteToColumns(LogicalPlan.CteDef cte,
                                                   LogicalPlan.CteDef originalCte,
                                                   Set<String> neededColumns) {
        if (neededColumns == null || neededColumns.isEmpty()) return cte;
        if (!(cte.query() instanceof LogicalPlan.Select s)) return cte;
        List<String> cteColNames = originalCte.columns();
        if (cteColNames.isEmpty()) return cte;

        Narrowed n = narrowItems(s, cteColNames, neededColumns);
        if (n.select() == s) return cte; // 未收窄 → 原样
        return new LogicalPlan.CteDef(cte.name(), n.declared(),
            apply(n.select()));
    }

    // ==================== 需求穿透（子查询 / 派生表） ====================

    /** 把外层需求列集 {@code needed} 穿透进 FROM 子查询，令内层扫描只读被引用的列。 */
    private LogicalPlan propagateIntoSubquery(LogicalPlan sub,
                                              Set<String> needed,
                                              List<String> declared) {
        return switch (sub) {
            case LogicalPlan.Select s -> apply(narrowItems(s, declared, needed).select());
            case LogicalPlan.With w -> {
                if (w.query() instanceof LogicalPlan.Select s) {
                    LogicalPlan narrow = narrowItems(s, declared, needed).select();
                    yield pruneWith(new LogicalPlan.With(w.ctes(), narrow));
                }
                yield apply(sub);
            }
            // SetOperation / TableFunction / 其它 → 仅递归 apply，不按名裁
            default -> apply(sub);
        };
    }

    /**
     * 用「对外声明列名表」{@code declared}（与 selectItems 逐项对齐）和需求列集 {@code needed}
     * 收窄 selectItems，同时返回收窄后的声明列表。
     *
     * <p><b>零误删口径</b>：任何歧义一律原样返回 ——
     * {@code needed} 空 / {@code declared} 空 / {@code declared} 与 items 数量不符（星号展开）
     * / CTE body 带 GROUP BY 或 DISTINCT（按名裁会改变基数语义）/ 无匹配项 / 匹配到全部。
     * 返回的 {@code select()} 与传入对象同一引用表示「未收窄」。
     */
    private Narrowed narrowItems(LogicalPlan.Select s,
                                 List<String> declared,
                                 Set<String> needed) {
        if (needed == null || needed.isEmpty()) return new Narrowed(s, declared);
        if (declared == null || declared.isEmpty()
                || declared.size() != s.selectItems().size()) {
            return new Narrowed(s, declared);
        }
        // GROUP BY / DISTINCT 时裁掉 selectItem 会改变基数 → 放弃
        if (!s.groupBy().isEmpty() || s.distinct()) return new Narrowed(s, declared);

        Set<String> wanted = new LinkedHashSet<>();
        for (String n : needed) wanted.add(n.toLowerCase());

        List<LogicalPlan.SelectItem> keptItems = new ArrayList<>();
        List<String> keptDeclared = new ArrayList<>();
        for (int i = 0; i < s.selectItems().size(); i++) {
            if (wanted.contains(declared.get(i).toLowerCase())) {
                keptItems.add(s.selectItems().get(i));
                keptDeclared.add(declared.get(i));
            }
        }
        if (keptItems.isEmpty() || keptItems.size() == s.selectItems().size()) {
            return new Narrowed(s, declared);
        }
        LogicalPlan.Select narrowed = new LogicalPlan.Select(
            keptItems, s.fromRelations(), s.joins(),
            s.where(), s.groupBy(), s.having(), s.scalarSubqueries(),
            s.orderBy(), s.limit(), s.offset(), s.distinct());
        return new Narrowed(narrowed, keptDeclared);
    }

    /** 收窄结果：{@code select} 与 {@code declared} 始终逐项对齐。 */
    private record Narrowed(LogicalPlan.Select select, List<String> declared) {}

    private boolean hasStarSelect(LogicalPlan plan) {
        if (plan instanceof LogicalPlan.Select s) {
            return s.selectItems().stream().anyMatch(item ->
                item.expression() instanceof Expression.OpaqueExpr oe
                && "*".equals(oe.originalText().trim()));
        }
        return false;
    }

    // ==================== 递归 handlers ====================

    private LogicalPlan.Insert pruneInsert(LogicalPlan.Insert i) {
        return new LogicalPlan.Insert(i.catalog(), i.schema(), i.table(),
            i.columnAliases(), i.overwrite(), apply(i.query()));
    }

    private LogicalPlan.SetOperation pruneSetOp(LogicalPlan.SetOperation so) {
        return new LogicalPlan.SetOperation(so.opType(), so.distinct(),
            apply(so.left()), apply(so.right()),
            so.orderBy(), so.limit(), so.offset());
    }

    private LogicalPlan.CreateTableAsSelect pruneCtas(
            LogicalPlan.CreateTableAsSelect ctas) {
        return new LogicalPlan.CreateTableAsSelect(ctas.qualifiedName(),
            ctas.columnAliases(), ctas.properties(), ctas.comment(),
            ctas.ifNotExists(), apply(ctas.query()));
    }

    private LogicalPlan.CreateView pruneCreateView(LogicalPlan.CreateView cv) {
        return new LogicalPlan.CreateView(cv.qualifiedName(), cv.orReplace(),
            apply(cv.query()));
    }

    private LogicalPlan.Explain pruneExplain(LogicalPlan.Explain e) {
        return new LogicalPlan.Explain(apply(e.statement()), e.analyze(), e.verbose());
    }
}
