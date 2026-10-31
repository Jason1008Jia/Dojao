package com.dojao.core.operator.expr;

import java.util.List;

/**
 * 通用标量表达式 AST（第一性实现：GROUP BY 表达式 / SELECT 表达式派生列求值）。
 *
 * <p>解析对象是语义层保留的 {@code OpaqueExpr} 原始 SQL 文本（语法层不建表达式树），
 * 由 {@link ExprParser} 递归下降解析成此树，再由 {@link ExprEvaluator} 逐行求值成
 * Arrow 向量。覆盖范围（第一版）：算术 {@code + - * / %}、一元负号、比较
 * {@code = != < <= > >=}、{@code IS [NOT] NULL}、{@code [NOT] IN (...)}、
 * {@code [NOT] BETWEEN a AND b}、{@code [NOT] LIKE 'pattern'}、布尔 {@code AND OR NOT}、函数调用（内置 / 已注册
 * 标量函数）、{@code CASE WHEN ... END}、{@code IF(cond, a, b)} 与类型转换
 * {@code CAST(expr AS type)} / {@code TRY_CAST(expr AS type)}。</p>
 *
 * @author jiaxin
 * @date 2026/9/6
 */
public sealed interface ExprNode {

    /** 列引用（已去限定符：{@code u.city} → {@code city}）。 */
    record Column(String name) implements ExprNode {}

    /** 字面量：null / Long / Double / String / Boolean。 */
    record Literal(Object value) implements ExprNode {}

    /** 函数调用（内置 ExprBuiltins 或已注册标量函数 ScalarFunctionRegistry）。 */
    record FuncCall(String name, List<ExprNode> args) implements ExprNode {}

    /** 二元算术。 */
    record Arith(Op op, ExprNode left, ExprNode right) implements ExprNode {
        public enum Op { ADD, SUB, MUL, DIV, MOD }

        public String symbol() {
            return switch (op) {
                case ADD -> "+";
                case SUB -> "-";
                case MUL -> "*";
                case DIV -> "/";
                case MOD -> "%";
            };
        }
    }

    /** 一元负号。 */
    record Negate(ExprNode operand) implements ExprNode {}

    /** 二元比较（求值结果为 Boolean）。 */
    record Compare(Op op, ExprNode left, ExprNode right) implements ExprNode {
        public enum Op { EQ, NE, GT, GE, LT, LE }

        public String symbol() {
            return switch (op) {
                case EQ -> "=";
                case NE -> "<>";
                case GT -> ">";
                case GE -> ">=";
                case LT -> "<";
                case LE -> "<=";
            };
        }
    }

    /** {@code x IS [NOT] NULL}。 */
    record IsNull(ExprNode operand, boolean negated) implements ExprNode {}

    /** {@code x [NOT] IN (v1, v2, ...)}。 */
    record InList(ExprNode operand, List<ExprNode> values, boolean negated) implements ExprNode {}

    /**
     * {@code x [NOT] LIKE 'pattern'}（{@code %}/{@code _} 通配，与 SQL LIKE 同口径）。
     *
     * <p>SQL 三值逻辑：{@code operand} 或 {@code pattern} 为 NULL 时结果未知 → 求值返回 null
     * （{@code NOT LIKE} 亦然），与 {@code SqlTextUtil.LikeSpec} 的「只留 TRUE」口径一致。</p>
     */
    record Like(ExprNode operand, ExprNode pattern, boolean negated) implements ExprNode {}

    /** 布尔与 / 或。 */
    record BoolOp(Op op, ExprNode left, ExprNode right) implements ExprNode {
        public enum Op { AND, OR }

        public String symbol() {
            return op == Op.AND ? "AND" : "OR";
        }
    }

    /** 逻辑非（CASE 条件 / 布尔表达式）。 */
    record Not(ExprNode operand) implements ExprNode {}

    /**
     * 类型转换 {@code CAST(operand AS type)} / {@code TRY_CAST(...)}。
     *
     * <p>{@code type} 为 {@link ExprEvaluator#normalizeCastType} 归一化后的规范类型名
     * （如 {@code double} / {@code bigint} / {@code varchar} / {@code decimal} / {@code boolean}
     * / {@code date} / {@code timestamp}）；解析期即校验，未知类型直接解析失败（计划期报错）。
     * {@code tryCast}（TRY_CAST）语义：转换失败返回 NULL 而非抛错。</p>
     *
     * <p>{@code precision} / {@code scale}：{@code DECIMAL(p,s)} 的类型参数，未书写时为 {@code -1}。
     * 带 scale 的 CAST 必须把值舍入到该小数位（{@code CAST(0.1 AS DECIMAL(10,2))} → {@code 0.10}），
     * 否则后续 BigDecimal 运算与聚合会丢失 DECIMAL 的定标语义。</p>
     */
    record Cast(ExprNode operand, String type, int precision, int scale, boolean tryCast) implements ExprNode {

        /** 无类型参数的 CAST（非 DECIMAL 或不带 {@code (p,s)}）。 */
        public Cast(ExprNode operand, String type, boolean tryCast) {
            this(operand, type, -1, -1, tryCast);
        }
    }

    /** CASE 分支：WHEN 条件 → THEN 结果。 */
    record WhenThen(ExprNode cond, ExprNode then) {}

    /** {@code CASE WHEN cond THEN val [WHEN ...] [ELSE val] END}（searched CASE）。 */
    record Case(List<WhenThen> branches, ExprNode elseExpr) implements ExprNode {
    }
}
