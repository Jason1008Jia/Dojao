package com.dojao.core.operator.expr;

/**
 * {@link ExprNode} → SQL 文本的打印器（{@link ExprParser} 的逆变换）。
 *
 * <p>存在理由：派生表表达式内联（{@code Planner} 的阶段 2）需要把「外层表达式里的派生列引用」
 * 替换成内层表达式本身，而下游 {@code ExpressionProjectOperator} / 扫描谓词只接受<b>文本</b>
 * （按 {@link ExprParser} 重新解析求值）—— 因此替换必须产出语义等价的 SQL 文本。</p>
 *
 * <p><b>正确性约束</b>：优先级低于外层上下文的子树一律加括号（{@code a * (b + c)} 不能打成
 * {@code a * b + c}）；字符串字面量按 SQL 规则转义单引号（{@code ''}）；{@code BETWEEN} 在解析期
 * 已脱糖成两个比较，这里无需还原。</p>
 *
 * @author jiaxin
 * @date 2026/11/21
 */
public final class ExprPrinter {

    /** 运算符优先级（数字越大绑定越紧）：OR &lt; AND &lt; NOT &lt; 比较 &lt; 加减 &lt; 乘除 &lt; 一元 &lt; primary。 */
    private static final int P_OR = 1;
    private static final int P_AND = 2;
    private static final int P_NOT = 3;
    private static final int P_CMP = 4;
    private static final int P_ADD = 5;
    private static final int P_MUL = 6;
    private static final int P_UNARY = 7;
    private static final int P_PRIMARY = 8;

    private ExprPrinter() { }

    public static String print(ExprNode node) {
        StringBuilder sb = new StringBuilder();
        write(node, sb);
        return sb.toString();
    }

    /** 打印为「至少要满足 {@code minPrec} 才可裸写」的形式，不够则整体加括号。 */
    private static void writeChild(ExprNode node, StringBuilder sb, int minPrec) {
        int prec = precedence(node);
        if (prec < minPrec) {
            sb.append('(');
            write(node, sb);
            sb.append(')');
        } else {
            write(node, sb);
        }
    }

    private static int precedence(ExprNode n) {
        if (n instanceof ExprNode.BoolOp b) return b.op() == ExprNode.BoolOp.Op.AND ? P_AND : P_OR;
        if (n instanceof ExprNode.Not) return P_NOT;
        if (n instanceof ExprNode.Compare || n instanceof ExprNode.IsNull
                || n instanceof ExprNode.InList || n instanceof ExprNode.Like) return P_CMP;
        if (n instanceof ExprNode.Arith a) {
            return a.op() == ExprNode.Arith.Op.ADD || a.op() == ExprNode.Arith.Op.SUB
                    ? P_ADD : P_MUL;
        }
        if (n instanceof ExprNode.Negate) return P_UNARY;
        return P_PRIMARY;
    }

    private static void write(ExprNode n, StringBuilder sb) {
        if (n instanceof ExprNode.Column c) {
            sb.append(c.name());
        } else if (n instanceof ExprNode.Literal l) {
            writeLiteral(l.value(), sb);
        } else if (n instanceof ExprNode.FuncCall f) {
            sb.append(f.name()).append('(');
            for (int i = 0; i < f.args().size(); i++) {
                if (i > 0) sb.append(", ");
                write(f.args().get(i), sb);
            }
            sb.append(')');
        } else if (n instanceof ExprNode.Arith a) {
            // 减法 / 除法右侧按更高一级处理：a - (b - c) 不能打成 a - b - c
            writeChild(a.left(), sb, precedence(a));
            sb.append(' ').append(a.symbol()).append(' ');
            writeChild(a.right(), sb, precedence(a) + 1);
        } else if (n instanceof ExprNode.Negate neg) {
            sb.append('-');
            writeChild(neg.operand(), sb, P_UNARY);
        } else if (n instanceof ExprNode.Compare c) {
            writeChild(c.left(), sb, P_CMP + 1);
            sb.append(' ').append(c.symbol()).append(' ');
            writeChild(c.right(), sb, P_CMP + 1);
        } else if (n instanceof ExprNode.IsNull isNull) {
            writeChild(isNull.operand(), sb, P_CMP + 1);
            sb.append(isNull.negated() ? " IS NOT NULL" : " IS NULL");
        } else if (n instanceof ExprNode.InList in) {
            writeChild(in.operand(), sb, P_CMP + 1);
            sb.append(in.negated() ? " NOT IN (" : " IN (");
            for (int i = 0; i < in.values().size(); i++) {
                if (i > 0) sb.append(", ");
                write(in.values().get(i), sb);
            }
            sb.append(')');
        } else if (n instanceof ExprNode.Like like) {
            writeChild(like.operand(), sb, P_CMP + 1);
            sb.append(like.negated() ? " NOT LIKE " : " LIKE ");
            writeChild(like.pattern(), sb, P_CMP + 1);
        } else if (n instanceof ExprNode.BoolOp b) {
            writeChild(b.left(), sb, precedence(b));
            sb.append(' ').append(b.symbol()).append(' ');
            writeChild(b.right(), sb, precedence(b) + 1);
        } else if (n instanceof ExprNode.Not not) {
            sb.append("NOT ");
            writeChild(not.operand(), sb, P_NOT);
        } else if (n instanceof ExprNode.Cast cast) {
            sb.append(cast.tryCast() ? "TRY_CAST(" : "CAST(");
            write(cast.operand(), sb);
            sb.append(" AS ").append(cast.type());
            if (cast.precision() > 0 || cast.scale() >= 0) {
                sb.append('(').append(cast.precision());
                if (cast.scale() >= 0) sb.append(", ").append(cast.scale());
                sb.append(')');
            }
            sb.append(')');
        } else if (n instanceof ExprNode.Case c) {
            sb.append("CASE");
            for (ExprNode.WhenThen wt : c.branches()) {
                sb.append(" WHEN ");
                write(wt.cond(), sb);
                sb.append(" THEN ");
                write(wt.then(), sb);
            }
            if (c.elseExpr() != null) {
                sb.append(" ELSE ");
                write(c.elseExpr(), sb);
            }
            sb.append(" END");
        } else {
            throw new IllegalStateException("未知表达式节点: " + n.getClass().getName());
        }
    }

    private static void writeLiteral(Object value, StringBuilder sb) {
        if (value == null) {
            sb.append("NULL");
        } else if (value instanceof Boolean b) {
            sb.append(b ? "TRUE" : "FALSE");
        } else if (value instanceof Number) {
            sb.append(value);
        } else {
            sb.append('\'').append(String.valueOf(value).replace("'", "''")).append('\'');
        }
    }
}
