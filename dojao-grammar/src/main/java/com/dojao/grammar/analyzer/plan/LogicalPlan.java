package com.dojao.grammar.analyzer.plan;

import com.dojao.grammar.analyzer.expression.Expression;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 逻辑计划：语义分析后的结果表示。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/6/27 22:27
 */
public sealed interface LogicalPlan {

    // ========== Query ==========

    record Select(
        List<SelectItem> selectItems,
        List<ResolvedRelation> fromRelations,
        List<JoinInfo> joins,
        Optional<Expression> where,
        List<Expression> groupBy,
        Optional<Expression> having,
        List<Expression.ScalarSubquery> scalarSubqueries,
        List<SortItem> orderBy,
        Optional<Long> limit,
        Optional<Long> offset,
        /** {@code SELECT DISTINCT}：投影之后按输出列去重（见 Planner 的空聚合去重节点）。 */
        boolean distinct
    ) implements LogicalPlan {
        /** 无标量子查询（既有调用方）。 */
        public Select(List<SelectItem> selectItems, List<ResolvedRelation> fromRelations,
                      List<JoinInfo> joins, Optional<Expression> where,
                      List<Expression> groupBy, Optional<Expression> having) {
            this(selectItems, fromRelations, joins, where, groupBy, having, List.of());
        }

        /** 无 ORDER BY / LIMIT / OFFSET（标量子查询调用方）。 */
        public Select(List<SelectItem> selectItems, List<ResolvedRelation> fromRelations,
                      List<JoinInfo> joins, Optional<Expression> where,
                      List<Expression> groupBy, Optional<Expression> having,
                      List<Expression.ScalarSubquery> scalarSubqueries) {
            this(selectItems, fromRelations, joins, where, groupBy, having,
                    scalarSubqueries, List.of(), Optional.empty(), Optional.empty(), false);
        }
    }

    /**
     * 顶层 {@code ORDER BY} 排序项。表达式限定为输出列 / 别名 / 位置序号（Planner 解析成裸列名），
     * 仅承载文本供规划期解析；{@code ascending} 缺省 ASC，{@code nullsFirst} 缺省空（跟随默认方向）。
     */
    record SortItem(
        Expression expression,
        boolean ascending,
        Optional<Boolean> nullsFirst
    ) {}

    /**
     * SELECT 项。{@code windowCalls} 承载「窗口函数参与表达式」时的窗口调用清单
     * （如 {@code gmv / sum(gmv) OVER ()}）：
     * <ul>
     *   <li>整项就是一个窗口函数 → {@code expression} 为 {@link Expression.WindowFunction}，此列表为空；</li>
     *   <li>窗口函数嵌在表达式里 → {@code expression} 保留<b>完整表达式原文</b>（{@code OpaqueExpr}，
     *       不能再被裁成单独的窗口调用，否则 {@code + - * /} 会被静默丢弃），此列表按出现顺序
     *       给出每个 OVER 调用（原文片段 + 结构化信息），供 Planner 物化合成列并回写引用。</li>
     * </ul>
     */
    record SelectItem(
        Expression expression,
        Optional<String> alias,
        List<WindowCall> windowCalls
    ) {
        /** 兼容构造器：常规 SELECT 项（无嵌套窗口调用）。 */
        public SelectItem(Expression expression, Optional<String> alias) {
            this(expression, alias, List.of());
        }
    }

    /** 嵌套在 SELECT 表达式里的一个窗口调用：{@code text} = 原始 SQL 片段（含 OVER 子句）。 */
    record WindowCall(String text, Expression.WindowFunction window) {}

    record ResolvedRelation(
        String catalog,
        String schema,
        String table,
        Optional<String> alias,
        List<String> columns,
        Optional<Expression> filter,
        Optional<LogicalPlan> subquery
    ) {
        /** Convenience constructor for backward compatibility (no filter, no subquery). */
        public ResolvedRelation(String catalog, String schema, String table,
                                 Optional<String> alias, List<String> columns) {
            this(catalog, schema, table, alias, columns, Optional.empty(), Optional.empty());
        }

        /** Convenience constructor for backward compatibility (no subquery). */
        public ResolvedRelation(String catalog, String schema, String table,
                                 Optional<String> alias, List<String> columns,
                                 Optional<Expression> filter) {
            this(catalog, schema, table, alias, columns, filter, Optional.empty());
        }
    }

    /**
     * 行展开关系：{@code FROM t, UNNEST(json_array_expr) [WITH ORDINALITY] [AS u(item[, ord])]}。
     *
     * <p>承载在 {@link ResolvedRelation#subquery()} 槽位，Planner 识别后把它构建成
     * {@code UnnestOperator}（上游 = 左侧关系，逐行或逐批展开 JSON 数组）。</p>
     *
     * @param expression       计算结果为 JSON 数组文本的表达式（可引用左侧关系的列）
     * @param alias            关系别名（缺省 {@code unnest}）
     * @param elementColumn    展开出的元素列名（缺省 {@code unnest}）
     * @param ordinalityColumn {@code WITH ORDINALITY} 时的序号列名（缺省 {@code ordinality}）
     */
    record Unnest(
        Expression expression,
        Optional<String> alias,
        String elementColumn,
        Optional<String> ordinalityColumn
    ) implements LogicalPlan {}

    record JoinInfo(
        JoinType joinType,
        String leftAlias,
        String rightAlias,
        Expression condition
    ) {}

    enum JoinType { INNER, LEFT, RIGHT, FULL, CROSS }

    record Insert(
        String catalog,
        String schema,
        String table,
        Optional<List<String>> columnAliases,
        boolean overwrite,
        LogicalPlan query
    ) implements LogicalPlan {
        /** 便捷构造：普通 INSERT INTO（非 overwrite）。 */
        Insert(String catalog, String schema, String table,
               Optional<List<String>> columnAliases, LogicalPlan query) {
            this(catalog, schema, table, columnAliases, false, query);
        }
    }

    // ========== USE ==========

    record Use(
        String catalog,
        Optional<String> schema
    ) implements LogicalPlan {}

    // ========== Catalog DDL ==========

    record CreateCatalog(
        String name,
        String connectorType,
        Map<String, String> properties,
        boolean ifNotExists
    ) implements LogicalPlan {}

    record DropCatalog(
        String name,
        boolean ifExists
    ) implements LogicalPlan {}

    // ========== Schema DDL ==========

    record CreateSchema(
        List<String> qualifiedName,
        Map<String, String> properties,
        boolean ifNotExists
    ) implements LogicalPlan {}

    record DropSchema(
        List<String> qualifiedName,
        boolean ifExists,
        boolean cascade
    ) implements LogicalPlan {}

    // ========== Table DDL ==========

    record ColumnDef(
        String name,
        String dataType,
        Optional<String> comment,
        Optional<String> defaultValue,
        boolean nullable
    ) {}

    record CreateTable(
        List<String> qualifiedName,
        List<ColumnDef> columns,
        Map<String, String> properties,
        Optional<String> comment,
        boolean ifNotExists
    ) implements LogicalPlan {}

    record CreateTableAsSelect(
        List<String> qualifiedName,
        Optional<List<String>> columnAliases,
        Map<String, String> properties,
        Optional<String> comment,
        boolean ifNotExists,
        LogicalPlan query
    ) implements LogicalPlan {}

    record DropTable(
        List<String> qualifiedName,
        boolean ifExists
    ) implements LogicalPlan {}

    /** {@code TRUNCATE TABLE name} —— 清空表数据，保留表结构（执行期交给连接器）。 */
    record TruncateTable(
        List<String> qualifiedName
    ) implements LogicalPlan {}

    record AlterTable(
        List<String> qualifiedName,
        AlterAction action
    ) implements LogicalPlan {}

    sealed interface AlterAction {
        record RenameTo(String newName) implements AlterAction {}
        record AddColumn(ColumnDef column) implements AlterAction {}
        record DropColumn(String columnName) implements AlterAction {}
        record RenameColumn(String oldName, String newName) implements AlterAction {}
    }

    // ========== View DDL ==========

    record CreateView(
        List<String> qualifiedName,
        boolean orReplace,
        LogicalPlan query
    ) implements LogicalPlan {}

    record DropView(
        List<String> qualifiedName,
        boolean ifExists
    ) implements LogicalPlan {}

    // ========== Function DDL ==========

    record CreateFunction(
        List<String> qualifiedName,
        boolean temporary,
        Map<String, String> properties,
        Optional<String> comment,
        boolean ifNotExists
    ) implements LogicalPlan {}

    record DropFunction(
        List<String> qualifiedName,
        boolean temporary,
        boolean ifExists
    ) implements LogicalPlan {}

    // ========== Table Function（FROM TABLE(...)） ==========

    /**
     * 表函数引用 —— {@code FROM TABLE(fn(常量参数)) [AS t(a,b)]}，表生成器语义。
     *
     * <p>承载在 {@link ResolvedRelation#subquery()} 槽位：Planner 在 buildScan 中识别并构建
     * TableFunctionOperator（叶子算子，open 时调 {@code fn.evaluate(args)} 一次，物化 N 行成单页）。</p>
     *
     * @param functionName  TABLE(fn(...)) 的限定名各段（单段即函数名）
     * @param args          参数原文（字面量表达式文本），规划期经 literalValue 解析为常量
     * @param alias         表别名（AS t）；缺省用函数名末段
     * @param columnAliases AS t(a,b) 的列别名；为空则输出列执行期推断 c0..cN-1
     */
    record TableFunction(
        List<String> functionName,
        List<String> args,
        Optional<String> alias,
        List<String> columnAliases
    ) implements LogicalPlan {}

    // ========== Show / Describe / Explain ==========

    record ShowCatalogs() implements LogicalPlan {}
    record ShowSchemas(Optional<String> catalog) implements LogicalPlan {}
    record ShowTables(Optional<List<String>> qualifiedName, Optional<String> likePattern) implements LogicalPlan {}
    record ShowColumns(List<String> qualifiedName) implements LogicalPlan {}
    record ShowCreateTable(List<String> qualifiedName) implements LogicalPlan {}
    record Describe(List<String> qualifiedName) implements LogicalPlan {}
    record Explain(LogicalPlan statement, boolean analyze, boolean verbose) implements LogicalPlan {
        public Explain(LogicalPlan statement) {
            this(statement, false, false);
        }
    }

    // ========== Set Operations ==========

    record SetOperation(
        SetOpType opType,
        boolean distinct,
        LogicalPlan left,
        LogicalPlan right,
        List<SortItem> orderBy,
        Optional<Long> limit,
        Optional<Long> offset
    ) implements LogicalPlan {
        /** 无 ORDER BY / LIMIT / OFFSET（嵌套集合运算调用方）。 */
        public SetOperation(SetOpType opType, boolean distinct, LogicalPlan left, LogicalPlan right) {
            this(opType, distinct, left, right, List.of(), Optional.empty(), Optional.empty());
        }
    }

    enum SetOpType { UNION, INTERSECT, EXCEPT }

    // ========== CTE / WITH ==========

    record CteDef(
        String name,
        List<String> columns,
        LogicalPlan query
    ) {}

    record With(
        List<CteDef> ctes,
        LogicalPlan query
    ) implements LogicalPlan {}

    // ========== Values ==========

    record Values(List<List<Expression>> rows) implements LogicalPlan {}

    // ========== Fallback ==========

    record Unsupported(String statementType) implements LogicalPlan {}
}
