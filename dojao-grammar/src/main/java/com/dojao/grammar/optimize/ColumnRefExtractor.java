package com.dojao.grammar.optimize;

import com.dojao.grammar.analyzer.expression.Expression;
import com.dojao.grammar.analyzer.plan.LogicalPlan;

import java.util.*;

/**
 * 从 Expression 中提取列引用（用于谓词下推和列裁剪）。
 *
 * <p><b>绑定优先</b>：{@link Expression.OpaqueExpr}/{@link Expression.WindowFunction} 若带有
 * {@link Expression#resolvedColumns()}（语义分析期由 {@code SemanticAnalyzer} 经
 * {@code Scope.resolveColumn} 持久化的权威绑定：大小写不敏感、alias/表名统一、列归属校验、歧义检测），
 * 直接采用；否则回退「<b>按关系校验</b>」的兜底解析（见 {@link Resolver#resolveText}）——
 * 以真实 {@code relation.columns()} 为白名单，不再用 SQL 关键字黑名单，忽略字符串/引号内文本，
 * 校验 {@code a.b} 的 {@code b} 确属 {@code a}，裸列歧义对每个匹配关系各发一条（保守）。
 *
 * <p>结果始终**只加不减**（歧义/解析不出时宁可不发），零误删。
 *
 * @author jiaxin
 * @version 2.0
 * @date 2026/10/08
 */
public final class ColumnRefExtractor {

    private ColumnRefExtractor() {}

    /** (qualifier, columnName) 对，qualifier 为关系规范别名，无修饰时为 null。 */
    public record ColRef(String qualifier, String columnName) {}

    /** 从单个表达式提取列引用。 */
    public static Set<ColRef> extract(Expression expr,
                                       List<LogicalPlan.ResolvedRelation> relations) {
        Set<ColRef> refs = new LinkedHashSet<>();
        collectFromExpression(expr, new Resolver(relations), refs);
        return refs;
    }

    /** 从表达式列表提取列引用。 */
    public static Set<ColRef> extractAll(List<Expression> exprs,
                                          List<LogicalPlan.ResolvedRelation> relations) {
        Set<ColRef> refs = new LinkedHashSet<>();
        Resolver resolver = new Resolver(relations);
        for (Expression expr : exprs) {
            collectFromExpression(expr, resolver, refs);
        }
        return refs;
    }

    private static void collectFromExpression(Expression expr,
                                               Resolver resolver,
                                               Set<ColRef> refs) {
        if (expr == null) return;
        switch (expr) {
            case Expression.ColumnRef c -> addBindings(c.resolvedColumns(), refs);
            case Expression.OpaqueExpr o -> {
                List<Expression.ColumnRef> bound = o.resolvedColumns();
                if (bound.isEmpty()) {
                    resolver.resolveText(o.originalText(), refs);
                } else {
                    addBindings(bound, refs);
                }
            }
            case Expression.WindowFunction wf -> {
                List<Expression.ColumnRef> bound = wf.resolvedColumns();
                if (bound.isEmpty()) {
                    for (String arg : wf.args()) resolver.resolveText(arg, refs);
                    for (String pb : wf.partitionBy()) resolver.resolveText(pb, refs);
                    for (String ob : wf.orderBy()) resolver.resolveText(ob, refs);
                } else {
                    addBindings(bound, refs);
                }
            }
            // 相关列（外层引用）无持久化绑定；多维分组成员可能带绑定 → 统一走 collectFromExpression。
            case Expression.ScalarSubquery s -> resolver.resolveText(s.originalText(), refs);
            case Expression.GroupingSets gs -> {
                for (List<Expression> set : gs.sets()) {
                    for (Expression member : set) {
                        collectFromExpression(member, resolver, refs);
                    }
                }
            }
        }
    }

    private static void addBindings(List<Expression.ColumnRef> bindings, Set<ColRef> refs) {
        for (Expression.ColumnRef rc : bindings) {
            refs.add(new ColRef(rc.qualifier(), rc.columnName()));
        }
    }

    /**
     * 兜底解析器：以真实 {@code relation(schema/alias/table/columns)} 为白名单，把一段 SQL 文本的
     * 列引用解析出来。忽略引号（{@code '...'} / {@code "..."} / 反引号）内文本；无关键字黑名单
     * （关键字/函数名/类型名天然不匹配任何真实列名）。
     */
    private static final class Resolver {

        /** 小写 alias/表名 → 关系。 */
        private final Map<String, LogicalPlan.ResolvedRelation> byName = new LinkedHashMap<>();
        /** 小写列名 → 含该列的关系（可多个 ⇒ 歧义）。 */
        private final Map<String, List<LogicalPlan.ResolvedRelation>> byColumn = new LinkedHashMap<>();

        Resolver(List<LogicalPlan.ResolvedRelation> relations) {
            for (LogicalPlan.ResolvedRelation rel : relations) {
                byName.putIfAbsent(rel.table().toLowerCase(Locale.ROOT), rel);
                String alias = rel.alias().orElse(rel.table());
                byName.putIfAbsent(alias.toLowerCase(Locale.ROOT), rel);
                for (String col : rel.columns()) {
                    byColumn.computeIfAbsent(col.toLowerCase(Locale.ROOT), k -> new ArrayList<>())
                        .add(rel);
                }
            }
        }

        void resolveText(String text, Set<ColRef> refs) {
            if (text == null || text.isEmpty()) return;
            int n = text.length();
            int i = 0;
            while (i < n) {
                char c = text.charAt(i);
                if (c == '\'' || c == '"' || c == '`') {     // 跳过引号区域（字符串/标识符引号）
                    i = skipQuoted(text, i, c);
                    continue;
                }
                if (!isIdentStart(c)) { i++; continue; }
                // 读一段点号链 a.b.c ...
                List<String> chain = new ArrayList<>();
                int[] after = readChain(text, i, chain);
                i = after[0];
                emitChain(chain, refs);
            }
        }

        /** 多段引用 {@code a.b(.c)}：取末两段做关系.列解析；{@code a} 非本层关系（schema/catalog 限定）时才各段按裸列保守尝试。 */
        private void emitChain(List<String> chain, Set<ColRef> refs) {
            if (chain.size() == 1) {
                emitBare(chain.get(0), refs);
                return;
            }
            String qualifier = chain.get(chain.size() - 2);
            String col = chain.get(chain.size() - 1);
            LogicalPlan.ResolvedRelation rel = byName.get(qualifier.toLowerCase(Locale.ROOT));
            if (rel != null) {
                // qualifier 是本层关系：只有列确属该关系才发（消除 a.b 幻影）。
                if (containsColumn(rel, col)) {
                    refs.add(new ColRef(rel.alias().orElse(rel.table()), col));
                }
                return;
            }
            // qualifier 非本层关系（可能是 schema/catalog 限定）→ 各段按裸列保守尝试（匹配不到真实列即自然不发）。
            for (String part : chain) emitBare(part, refs);
        }

        private void emitBare(String word, Set<ColRef> refs) {
            List<LogicalPlan.ResolvedRelation> matched = byColumn.get(word.toLowerCase(Locale.ROOT));
            if (matched == null || matched.isEmpty()) return;  // 关键字/函数名/类型名 → 不发
            for (LogicalPlan.ResolvedRelation rel : matched) {
                refs.add(new ColRef(rel.alias().orElse(rel.table()), word));
            }
        }

        private static boolean containsColumn(LogicalPlan.ResolvedRelation rel, String col) {
            String lower = col.toLowerCase(Locale.ROOT);
            for (String c : rel.columns()) {
                if (c.toLowerCase(Locale.ROOT).equals(lower)) return true;
            }
            return false;
        }

        /** 跳过 {@code [start]} 处的引号区域，返回闭合引号之后的下标。 */
        private static int skipQuoted(String text, int start, char quote) {
            int n = text.length();
            int i = start + 1;
            while (i < n) {
                char c = text.charAt(i);
                if (c == quote) {
                    if (i + 1 < n && text.charAt(i + 1) == quote) { i += 2; continue; }  // '' 转义
                    return i + 1;
                }
                i++;
            }
            return n;
        }

        /** 从 {@code start} 读点号链 {@code ident(.ident)*}；返回 {@code [下一扫描下标]} 并填充 {@code chain}。 */
        private static int[] readChain(String text, int start, List<String> chain) {
            int n = text.length();
            int i = start;
            while (true) {
                int s = i;
                i++;
                while (i < n && isIdentPart(text.charAt(i))) i++;
                chain.add(text.substring(s, i));
                if (i < n && text.charAt(i) == '.' && i + 1 < n && isIdentStart(text.charAt(i + 1))) {
                    i++;  // 跳过 '.'
                    continue;
                }
                break;
            }
            return new int[]{i};
        }

        private static boolean isIdentStart(char c) {
            return Character.isLetter(c) || c == '_';
        }

        private static boolean isIdentPart(char c) {
            return Character.isLetterOrDigit(c) || c == '_';
        }
    }
}
