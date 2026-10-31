package com.dojao.grammar.analyzer.expression;

import com.dojao.grammar.analyzer.plan.LogicalPlan;

import java.util.List;
import java.util.Optional;

/**
 * SQL 表达式表示（最小化 sealed 接口）。
 * ColumnRef 表示已解析的列引用，OpaqueExpr 表示不做语义分析的其他表达式。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/6/27 22:27
 */
public sealed interface Expression {

    /** 列名提取：ColumnRef 返回 columnName，OpaqueExpr 返回原始文本。 */
    String text();

    /**
     * 已语义解析的列引用（权威绑定）。默认空——由 {@code SemanticAnalyzer} 在语义分析期持久化
     * （经 {@code Scope.resolveColumn} 精确解析：大小写不敏感、alias/表名统一、列归属校验、歧义检测）。
     * 列裁剪/谓词下推优先消费此绑定，仅在为空时回退「按关系校验」的兜底解析。
     */
    default List<ColumnRef> resolvedColumns() { return List.of(); }

    /**
     * 已解析的列引用：{@code qualifier.columnName} 或 {@code columnName}。
     *
     * @param qualifier  表名或别名（无修饰时为 null）
     * @param columnName 列名
     */
    record ColumnRef(String qualifier, String columnName) implements Expression {
        @Override public String text() { return columnName; }
        @Override public List<ColumnRef> resolvedColumns() { return List.of(this); }
    }

    /**
     * 不透明表达式：字面量、函数调用、算术运算、比较等，
     * 不做进一步语义分析，仅保留原始 SQL 文本。
     *
     * @param originalText    原始 SQL 文本
     * @param resolvedColumns 语义分析期持久化的权威列绑定（无则空）
     */
    record OpaqueExpr(String originalText, List<ColumnRef> resolvedColumns) implements Expression {
        /** 兼容构造器：无持久化绑定时（程序化构造 / 未走 analyzer 的场景）。 */
        public OpaqueExpr(String originalText) { this(originalText, List.of()); }
        @Override public String text() { return originalText; }
    }

    /**
     * 窗口函数表达式（RANK / ROW_NUMBER / DENSE_RANK / LEAD / LAG / ... OVER）。
     *
     * @param functionName    函数名
     * @param args            函数参数（原始 SQL 文本）
     * @param partitionBy     PARTITION BY 表达式（原始 SQL 文本）
     * @param orderBy         ORDER BY 排序项（原始 SQL 文本）
     * @param windowFrame     窗口帧（如 ROWS BETWEEN ... AND ...），无则为空
     * @param resolvedColumns 语义分析期持久化的权威列绑定（含 args/partition/order 中的列引用）
     */
    record WindowFunction(
        String functionName,
        List<String> args,
        List<String> partitionBy,
        List<String> orderBy,
        Optional<String> windowFrame,
        List<ColumnRef> resolvedColumns
    ) implements Expression {
        /** 兼容构造器：无持久化绑定时。 */
        public WindowFunction(String functionName, List<String> args,
                              List<String> partitionBy, List<String> orderBy,
                              Optional<String> windowFrame) {
            this(functionName, args, partitionBy, orderBy, windowFrame, List.of());
        }
        @Override public String text() { return functionName + "(" + String.join(", ", args) + ") OVER (...)"; }
        /** 附加持久化绑定（语义分析期由 SemanticAnalyzer 调用）。 */
        public WindowFunction withResolvedColumns(List<ColumnRef> resolved) {
            return new WindowFunction(functionName, args, partitionBy, orderBy, windowFrame, resolved);
        }
    }

    /**
     * 标量子查询表达式：{@code (SELECT agg(x) FROM t WHERE ...)}，含引用外层列的相关形式。
     * 内层 {@code inner} 是已语义分析的子查询 {@link LogicalPlan}（SELECT 列表 / WHERE / FROM）。
     * Dojao 谓词是 opaque 文本，相关列不在此处解析，交给 Planner 去相关时做相关性检测。
     *
     * <p>同一载体也承载 {@code [NOT] EXISTS (query)}：分析期把内层统一改写成
     * {@code SELECT count(*) ...}（存在性 ⇔ count > 0），{@code exists=true} /
     * {@code negated=true} 让 Planner 在 WHERE/SELECT 原文里替换为对计数派生列的判定，
     * 而不是像普通标量子查询那样直接用派生列值。{@code originalText} 对外层文本替换
     * 是整段谓词原文（如 {@code NOT EXISTS (...)}）。</p>
     */
    record ScalarSubquery(LogicalPlan inner, String originalText,
                          boolean exists, boolean negated) implements Expression {
        /** 普通标量子查询（非 EXISTS）。 */
        public ScalarSubquery(LogicalPlan inner, String originalText) {
            this(inner, originalText, false, false);
        }
        @Override public String text() { return originalText; }
    }

    /**
     * 多维分组（CUBE / ROLLUP / GROUPING SETS）：已展开的全部 grouping sets。
     *
     * <p>只作为 {@link LogicalPlan.Select#groupBy()} 的<b>唯一元素</b>出现：{@code sets}
     * 的每个内层 list = 一个 grouping set 的列表达式（可为空 = 全局总计 {@code ()}）。
     * Planner 据此把多维聚合降级为若干 {@code GROUP BY 子集} 的 {@code SELECT} 腿再
     * {@code UNION ALL} 拼接。</p>
     *
     * @param sets         展开后的 grouping set 列表（每个内层为空 list 表示全局总计）
     * @param originalText 原始 GROUP BY 文本（如 {@code CUBE(a, b)}）
     */
    record GroupingSets(List<List<Expression>> sets, String originalText) implements Expression {
        @Override public String text() { return originalText; }
    }
}
