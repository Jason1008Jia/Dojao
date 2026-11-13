package com.dojao.core.operator.expr;

import com.dojao.core.operator.CaseProjection;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.math.BigDecimal;
import java.util.Map;

/**
 * 表达式结果 Arrow 类型的<b>静态推断</b>（{@code null} = 推断不出）。
 *
 * <p>与 {@link ExprEvaluator#evaluateTo} 的运行期类型提升保持一致：整数 → Int64、浮点 → Float8、
 * DECIMAL → Decimal(p,s)、布尔 → Bool、字符串/日期时间/二进制 → Utf8。</p>
 *
 * <p>用途有二：</p>
 * <ul>
 *   <li>投影型派生源的<b>声明列</b>（Planner）；</li>
 *   <li>CASE / IF / 表达式派生列（聚合参数改写）的声明列 —— 分布式路径下声明类型必须与
 *       Worker 实际产出的页 schema 一致，否则下游（尤其是 Join）按声明宽度解码会静默错值。</li>
 * </ul>
 *
 * <p>★DECIMAL 必须保持 Decimal 而不是退化成 Float8：{@code CAST(x AS DECIMAL(20,2))}、
 * {@code total - discount}（两侧 DECIMAL）的求和精度全靠这里的类型不降级。</p>
 *
 * @author jiaxin
 * @date 2026/10/22
 */
public final class ExprTypeInference {

    private static final ArrowType INT64 = new ArrowType.Int(64, true);
    private static final ArrowType FLOAT8 =
            new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);

    private ExprTypeInference() {
    }

    /** 表达式结果类型；{@code null} = 不可静态推断（调用方回退运行期口径）。 */
    public static ArrowType infer(ExprNode node, Map<String, Field> fields) {
        if (node == null) return null;
        if (node instanceof ExprNode.Literal lit) {
            return literalType(lit.value());
        }
        if (node instanceof ExprNode.Column c) {
            return columnType(c.name(), fields);
        }
        if (node instanceof ExprNode.Negate g) {
            ArrowType o = numericOperandType(g.operand(), fields);
            if (o == null) return null;
            return o instanceof ArrowType.Decimal dec ? dec : (isFloating(o) ? FLOAT8 : INT64);
        }
        if (node instanceof ExprNode.Arith a) {
            return arithType(a, fields);
        }
        if (node instanceof ExprNode.Compare || node instanceof ExprNode.IsNull
                || node instanceof ExprNode.InList || node instanceof ExprNode.Like
                || node instanceof ExprNode.BoolOp || node instanceof ExprNode.Not) {
            return ArrowType.Bool.INSTANCE;
        }
        if (node instanceof ExprNode.Case cs) {
            ArrowType acc = null;
            for (ExprNode.WhenThen wt : cs.branches()) {
                // THEN NULL 不约束结果类型（跳过）；推断不出的分支才放弃
                if (isNullLiteral(wt.then())) continue;
                ArrowType t = infer(wt.then(), fields);
                if (t == null) return null;
                acc = merge(acc, t);
            }
            if (!isNullLiteral(cs.elseExpr())) {
                ArrowType e = infer(cs.elseExpr(), fields);
                if (e == null) return null;
                acc = merge(acc, e);
            }
            return acc;
        }
        if (node instanceof ExprNode.Cast cs) {
            return castType(cs);
        }
        if (node instanceof ExprNode.FuncCall fc) {
            return funcCallType(fc, fields);
        }
        return null;   // 其余函数调用：返回类型取决于实现 → 不可推断
    }

    /**
     * 少数「结果类型完全由参数类型决定」的内置函数 —— 只为 JOIN ON 表达式键的声明列服务。
     *
     * <p>JOIN ON 的等值键（如 {@code coalesce(if(dept_id='',null,dept_id), if(bu_id='',null,bu_id))}）
     * 要在 Worker 侧物化成派生列，其 Arrow 类型必须与运行期产出完全一致，否则下游 join
     * 按声明宽度解码会错值。只覆盖语义明确的几个；其余函数一律返回 {@code null}
     * （调用方回退运行期口径 / Master 单机 join）。</p>
     */
    private static ArrowType funcCallType(ExprNode.FuncCall fc, Map<String, Field> fields) {
        String name = fc.name() == null ? "" : fc.name().toLowerCase();
        var args = fc.args();
        return switch (name) {
            case "coalesce", "nvl", "ifnull", "if_null", "greatest", "least" -> mergeAll(args, fields);
            case "if" -> args.size() == 3 ? mergeArgs(args.get(1), args.get(2), fields) : null;
            case "nullif" -> args.size() == 2 ? infer(args.get(0), fields) : null;
            // 结果恒为字符串的内置标量函数（ScalarEvaluator 按 Java String 结果建 Utf8 向量）：
            // CTAS scan-sink 需要据静态类型先建目标表，故在这里补全（其余函数仍返回 null → 回退）。
            case "get_json_object", "json_extract", "parse_url",
                 "concat", "concat_ws", "substr", "substring", "replace", "regexp_replace",
                 "regexp_extract", "repeat", "reverse", "lpad", "rpad", "space", "chr",
                 "soundex", "initcap", "printf", "translate", "elt", "hex",
                 "md5", "sha1", "sha2", "uuid", "date_format", "from_unixtime" ->
                    ArrowType.Utf8.INSTANCE;
            default -> null;
        };
    }

    /** 逐参数合并（NULL 字面量跳过）；任一非空参数推断不出 → null。 */
    private static ArrowType mergeAll(java.util.List<ExprNode> args, Map<String, Field> fields) {
        ArrowType acc = null;
        boolean any = false;
        for (ExprNode a : args) {
            if (isNullLiteral(a)) continue;
            ArrowType t = infer(a, fields);
            if (t == null) return null;
            acc = merge(acc, t);
            any = true;
        }
        return any ? acc : null;
    }

    private static ArrowType mergeArgs(ExprNode a, ExprNode b, Map<String, Field> fields) {
        ArrowType acc = null;
        boolean any = false;
        if (!isNullLiteral(a)) {
            ArrowType t = infer(a, fields);
            if (t == null) return null;
            acc = merge(acc, t);
            any = true;
        }
        if (!isNullLiteral(b)) {
            ArrowType t = infer(b, fields);
            if (t == null) return null;
            acc = merge(acc, t);
            any = true;
        }
        return any ? acc : null;
    }

    /** 是否是 NULL 字面量（{@code null} 表达式视同 NULL）。 */
    private static boolean isNullLiteral(ExprNode node) {
        return node == null || (node instanceof ExprNode.Literal lit && lit.value() == null);
    }

    /** 字面量类型；NULL 字面量不约束类型（返回 null）。 */
    private static ArrowType literalType(Object v) {
        if (v == null) return null;
        if (v instanceof BigDecimal bd) return decimalOf(bd.precision(), bd.scale());
        if (v instanceof Double || v instanceof Float) return FLOAT8;
        if (v instanceof Long || v instanceof Integer || v instanceof Short || v instanceof Byte) {
            return INT64;
        }
        if (v instanceof Boolean) return ArrowType.Bool.INSTANCE;
        return ArrowType.Utf8.INSTANCE;
    }

    /**
     * 列在运行期的取值类型：整数 → Int64、浮点 → Float8、DECIMAL <b>保持 Decimal(p,s)</b>、
     * 布尔 → Bool、其余（字符串/日期/二进制）→ Utf8（{@code readColumn} 返回 Timestamp 等对象，
     * 落入字符串分支）。
     */
    private static ArrowType columnType(String name, Map<String, Field> fields) {
        if (name == null || fields == null) return null;
        Field f = fields.get(name);
        if (f == null) return null;
        ArrowType t = f.getType();
        if (t instanceof ArrowType.Int) return INT64;
        if (t instanceof ArrowType.FloatingPoint) return FLOAT8;
        if (t instanceof ArrowType.Decimal d) {
            return decimalOf(Math.max(d.getPrecision(), d.getScale() + 1), d.getScale());
        }
        if (t instanceof ArrowType.Bool) return ArrowType.Bool.INSTANCE;
        return ArrowType.Utf8.INSTANCE;
    }

    /** 算术结果类型 —— 任一侧 DECIMAL 即 DECIMAL（保留小数位），否则整数/浮点按运行期提升。 */
    private static ArrowType arithType(ExprNode.Arith a, Map<String, Field> fields) {
        ArrowType l = numericOperandType(a.left(), fields);
        ArrowType r = numericOperandType(a.right(), fields);
        if (l == null || r == null) return null;
        boolean ld = l instanceof ArrowType.Decimal dl;
        boolean rd = r instanceof ArrowType.Decimal dr;
        if (!ld && !rd) {
            // 纯整数/浮点：除法恒双精度（镜像 ExprEvaluator），其余浮点优先
            if (a.op() == ExprNode.Arith.Op.DIV) return FLOAT8;
            return (isFloating(l) || isFloating(r)) ? FLOAT8 : INT64;
        }
        int ls = ld ? ((ArrowType.Decimal) l).getScale() : 0;
        int rs = rd ? ((ArrowType.Decimal) r).getScale() : 0;
        int lint = ld ? ((ArrowType.Decimal) l).getPrecision() - ls : intDigits(l);
        int rint = rd ? ((ArrowType.Decimal) r).getPrecision() - rs : intDigits(r);
        int scale;
        int intPart;
        switch (a.op()) {
            case ADD, SUB, MOD -> {
                scale = Math.max(ls, rs);
                intPart = Math.max(lint, rint);
            }
            case MUL -> {
                scale = ls + rs;
                intPart = lint + rint;
            }
            default -> {
                // DIV：Decimal 除法保留最大小数位（ExprEvaluator 的 arithDecimal 口径）
                scale = Math.max(ls, rs);
                intPart = lint + rint;
            }
        }
        return decimalOf(intPart + scale, scale);
    }

    private static ArrowType castType(ExprNode.Cast cs) {
        return switch (cs.type()) {
            case "int", "bigint", "smallint", "tinyint" -> INT64;
            case "double" -> FLOAT8;
            case "decimal" -> cs.scale() >= 0
                    // CAST(... AS DECIMAL(p,s))：结果类型静态可知（与 toDecimal 定标一致）；
                    // 不带 (p,s) 时 scale 由数据决定 → 不可静态推断。
                    ? decimalOf(cs.precision() > 0 ? cs.precision() : 38, cs.scale())
                    : null;
            case "boolean" -> ArrowType.Bool.INSTANCE;
            default -> ArrowType.Utf8.INSTANCE;
        };
    }

    /** 数值上下文：只接受数值列 / 数值字面量 / 算术 / 负数 / CAST（字符串参与算术 → null）。 */
    private static ArrowType numericOperandType(ExprNode node, Map<String, Field> fields) {
        ArrowType t;
        if (node instanceof ExprNode.Column c) t = columnType(c.name(), fields);
        else if (node instanceof ExprNode.Literal || node instanceof ExprNode.Arith
                || node instanceof ExprNode.Negate || node instanceof ExprNode.Cast) {
            t = infer(node, fields);
        } else {
            return null;
        }
        return t != null && isNumeric(t) ? t : null;
    }

    /** 数值类型：整数 / 浮点 / DECIMAL。 */
    private static boolean isNumeric(ArrowType t) {
        return t instanceof ArrowType.Int || t instanceof ArrowType.Decimal || isFloating(t);
    }

    /**
     * 合并两个分支类型：相同 → 该类型；数值混合 → 数值提升（DECIMAL 优先，避免精度降级）；
     * 任一字符串 → Utf8；其余 → null（保守放弃）。
     */
    public static ArrowType merge(ArrowType acc, ArrowType t) {
        if (t == null) return null;
        if (acc == null) return t;
        if (acc.equals(t)) return acc;
        boolean an = isNumeric(acc);
        boolean tn = isNumeric(t);
        if (an && tn) {
            ArrowType.Decimal da = acc instanceof ArrowType.Decimal x ? x : null;
            ArrowType.Decimal db = t instanceof ArrowType.Decimal y ? y : null;
            if (da != null && db != null) {
                int scale = Math.max(da.getScale(), db.getScale());
                int intPart = Math.max(da.getPrecision() - da.getScale(),
                        db.getPrecision() - db.getScale());
                return decimalOf(intPart + scale, scale);
            }
            if (da != null) {
                int scale = Math.max(da.getScale(), 0);
                int intPart = Math.max(da.getPrecision() - da.getScale(), intDigits(t));
                return decimalOf(intPart + scale, scale);
            }
            if (db != null) {
                int scale = Math.max(db.getScale(), 0);
                int intPart = Math.max(db.getPrecision() - db.getScale(), intDigits(acc));
                return decimalOf(intPart + scale, scale);
            }
            return FLOAT8;
        }
        if (ArrowType.Utf8.INSTANCE.equals(acc) || ArrowType.Utf8.INSTANCE.equals(t)) {
            return ArrowType.Utf8.INSTANCE;
        }
        return null;
    }

    /**
     * CASE / IF / 纯表达式派生列的静态结果类型：逐分支合并结果值表达式；任一支不可推断 → null。
     *
     * @param projection 派生列定义（{@code branches} 为空时只看 {@code elseValue}）
     * @param fields     上游列名 → Field
     */
    public static ArrowType inferProjection(CaseProjection projection, Map<String, Field> fields) {
        if (projection == null) return null;
        ArrowType acc = null;
        boolean any = false;
        for (CaseProjection.CaseBranch br : projection.branches()) {
            String text = br.value();
            if (isNullOrBlank(text)) continue;   // NULL 分支不约束结果类型
            ArrowType t = inferValueText(text, fields);
            if (t == null) return null;          // 有值但推断不出 → 放弃
            acc = merge(acc, t);
            any = true;
        }
        String elseText = projection.elseValue();
        if (!isNullOrBlank(elseText)) {
            ArrowType e = inferValueText(elseText, fields);
            if (e == null) return null;
            acc = merge(acc, e);
            any = true;
        }
        return any ? acc : null;
    }

    private static boolean isNullOrBlank(String text) {
        return text == null || text.isBlank() || text.trim().equalsIgnoreCase("NULL");
    }

    /** 派生列结果值文本（字面量 / 列名 / 表达式）→ 类型；NULL 文本不约束。 */
    private static ArrowType inferValueText(String text, Map<String, Field> fields) {
        if (isNullOrBlank(text)) return null;
        String t = text.trim();
        ExprNode node;
        try {
            node = ExprParser.parse(t);
        } catch (RuntimeException ex) {
            return null;
        }
        return infer(node, fields);
    }

    /** DECIMAL(p,s)：精度封顶 38，至少给 scale 留 1 位整数。 */
    public static ArrowType.Decimal decimalOf(int precision, int scale) {
        int s = Math.max(0, Math.min(scale, 38));
        int p = Math.max(s + 1, Math.min(38, Math.max(precision, s + 1)));
        return new ArrowType.Decimal(p, s, 128);
    }

    private static boolean isFloating(ArrowType t) {
        return t instanceof ArrowType.FloatingPoint;
    }

    /** 整数类型能容纳的十进制位数（含符号位留量）。 */
    private static int intDigits(ArrowType t) {
        if (t instanceof ArrowType.Int i) {
            return switch (i.getBitWidth()) {
                case 8 -> 3;
                case 16 -> 5;
                case 32 -> 10;
                default -> 19;
            };
        }
        // 浮点（Double）整数位按 15 位十进制有效数字估算
        return 15;
    }
}
