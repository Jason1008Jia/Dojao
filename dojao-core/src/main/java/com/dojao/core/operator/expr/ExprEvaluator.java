package com.dojao.core.operator.expr;

import com.dojao.core.function.scalar.ScalarExpr;
import com.dojao.core.function.scalar.ScalarFunctionRegistry;
import com.dojao.core.planner.SqlTextUtil;
import com.dojao.spi.function.ScalarFunction;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * 通用标量表达式求值器 —— 对 {@link ExprNode} 在一个 batch 上逐行求值，产出结果列。
 *
 * <p>表达式结果类型跨行统一（关键：分组键须类型稳定，否则同值跨行落入不同哈希桶）：
 * 任一行出现字符串 → Utf8；否则任一行为浮点 → Float8；否则任一行为 BigDecimal → Decimal；
 * 否则任一行整数 → Int64；否则仅布尔 → Bool。数值/布尔与字符串混排时按字符串
 * 输出（数值 toString，对齐 MySQL CASE 字符串分支的隐性强转）。全 NULL（本批无任何非空值）时
 * 无法由数据判定类型，改按<b>表达式静态类型</b>推断：字符串语义的表达式（字符串字面量 / 字符串列 /
 * 字符串返回函数 / CAST 成文本 / 含字符串分支的 CASE）→ Utf8，其余沿用历史 Int64 兜底。这保证
 * {@code SELECT concat('a', NULL)} 这类恒 NULL 的字符串表达式输出 Utf8 而非 Int64。</p>
 *
 * <p>函数解析：先在 {@link ExprBuiltins}（内置）找，找不到回退 {@link ScalarFunctionRegistry}
 * （用户注册标量 / 后端加载），与现有 ScalarProject 同一注册表。</p>
 *
 * @author jiaxin
 * @date 2026/9/6
 */
public final class ExprEvaluator {

    private final ExprNode node;

    public ExprEvaluator(ExprNode node) {
        this.node = node;
    }

    /** 表达式引用的全部列名（已去限定符，去重）—— 供规划期 requireColumn / 列裁剪。 */
    public Set<String> columnArgs() {
        Set<String> cols = new LinkedHashSet<>();
        collectColumns(node, cols);
        return cols;
    }

    private static void collectColumns(ExprNode n, Set<String> out) {
        if (n instanceof ExprNode.Column c) out.add(c.name());
        else if (n instanceof ExprNode.Arith a) { collectColumns(a.left(), out); collectColumns(a.right(), out); }
        else if (n instanceof ExprNode.Negate g) collectColumns(g.operand(), out);
        else if (n instanceof ExprNode.Compare c) { collectColumns(c.left(), out); collectColumns(c.right(), out); }
        else if (n instanceof ExprNode.IsNull i) collectColumns(i.operand(), out);
        else if (n instanceof ExprNode.InList il) { collectColumns(il.operand(), out); for (ExprNode v : il.values()) collectColumns(v, out); }
        else if (n instanceof ExprNode.Like lk) { collectColumns(lk.operand(), out); collectColumns(lk.pattern(), out); }
        else if (n instanceof ExprNode.BoolOp b) { collectColumns(b.left(), out); collectColumns(b.right(), out); }
        else if (n instanceof ExprNode.Not nt) collectColumns(nt.operand(), out);
        else if (n instanceof ExprNode.Case c) { for (ExprNode.WhenThen w : c.branches()) { collectColumns(w.cond(), out); collectColumns(w.then(), out); } if (c.elseExpr() != null) collectColumns(c.elseExpr(), out); }
        else if (n instanceof ExprNode.FuncCall f) { for (ExprNode a : f.args()) collectColumns(a, out); }
        else if (n instanceof ExprNode.Cast c) collectColumns(c.operand(), out);
    }

    /**
     * 对一个 batch 求值，返回输出列（行数与 batch 一致），列名为 outputName。
     * 函数名已在规划期校验存在（builtin / 已注册），此处仍 fail-fast。
     */
    public FieldVector evaluate(VectorSchemaRoot root, BufferAllocator allocator, String outputName) {
        return evaluateTo(root, allocator, outputName, indexOf(root));
    }

    /** 列名 → 下标（同名取首个）——供 {@link #evalRow} 逐行求值复用，避免每行重建映射。 */
    public static Map<String, Integer> indexOf(VectorSchemaRoot root) {
        Map<String, Integer> indexOf = new HashMap<>();
        List<Field> schemaFields = root.getSchema().getFields();
        for (int i = 0; i < schemaFields.size(); i++) {
            indexOf.putIfAbsent(schemaFields.get(i).getName(), i);
        }
        return indexOf;
    }

    /**
     * 单行求值（供 CASE 派生列等复用：结果值是表达式时逐行算，而非整列产出）。
     * 调用方先用 {@link #indexOf} 建表并按行循环。
     */
    public static Object evalRow(ExprNode node, VectorSchemaRoot root, int row, Map<String, Integer> indexOf) {
        return eval(node, root, row, indexOf);
    }

    /**
     * LIKE 模式 → 正则的编译缓存：逐行求值时同一模式跨行/跨批复用（上限 256 条，超出整体清空）。
     * 模式串来自字面量/列值，可能每行不同，故用有界缓存而非无界 Map。
     */
    private static final Map<String, java.util.regex.Pattern> LIKE_PATTERN_CACHE =
            new java.util.concurrent.ConcurrentHashMap<>();

    private static java.util.regex.Pattern likePattern(String sqlPattern) {
        java.util.regex.Pattern p = LIKE_PATTERN_CACHE.get(sqlPattern);
        if (p != null) return p;
        if (LIKE_PATTERN_CACHE.size() >= 256) LIKE_PATTERN_CACHE.clear();
        p = SqlTextUtil.compileLikePattern(sqlPattern);
        LIKE_PATTERN_CACHE.put(sqlPattern, p);
        return p;
    }

    private FieldVector evaluateTo(VectorSchemaRoot root, BufferAllocator allocator,
                                   String outputName, Map<String, Integer> indexOf) {
        int rowCount = root.getRowCount();
        // 第一遍：逐行求值收集结果 + 类型类别标记（决定输出向量类型）
        Object[] results = new Object[rowCount];
        boolean anyFloat = false, anyDecimal = false, anyInt = false, anyBool = false, anyStr = false;
        for (int r = 0; r < rowCount; r++) {
            Object v = eval(node, root, r, indexOf);
            results[r] = v;
            if (v == null) continue;
            if (v instanceof BigDecimal) anyDecimal = true;
            else if (v instanceof Double || v instanceof Float) anyFloat = true;
            else if (v instanceof Long || v instanceof Integer || v instanceof Short || v instanceof Byte) anyInt = true;
            else if (v instanceof Boolean) anyBool = true;
            else anyStr = true;
        }
        FieldVector out;
        int decimalScale = 0;
        if (anyStr) out = new VarCharVector(outputName, allocator);
        else if (anyFloat) out = new Float8Vector(outputName, allocator);
        else if (anyDecimal) {
            // ★DECIMAL 结果必须落 Decimal64/128 向量（而不是 Float8）：否则「0.30」在输出端
            //   又变回 0.30000000000000004 / 科学计数法。scale 取各值最大小数位，所有值按此定标。
            for (Object v : results) {
                if (v instanceof BigDecimal bd) decimalScale = Math.max(decimalScale, bd.scale());
            }
            decimalScale = Math.max(0, Math.min(decimalScale, 38));
            int precision = 1;
            for (Object v : results) {
                if (v instanceof BigDecimal bd) {
                    precision = Math.max(precision, bd.setScale(decimalScale, RoundingMode.HALF_UP).precision());
                } else if (v instanceof Long || v instanceof Integer || v instanceof Short || v instanceof Byte) {
                    precision = Math.max(precision, BigDecimal.valueOf(((Number) v).longValue())
                            .setScale(decimalScale, RoundingMode.HALF_UP).precision());
                }
            }
            out = new DecimalVector(outputName, allocator, Math.min(precision, 38), decimalScale);
        }
        else if (anyInt) out = new BigIntVector(outputName, allocator);
        else if (anyBool) out = new BitVector(outputName, allocator);
        else {
            // 整批全 NULL：类型只能由<b>静态信息</b>推断。数值 CAST（`CAST(NULL AS DOUBLE)`）
            // 若落回 Int64 兜底，UNION ALL 与 DOUBLE 兄弟分支比较时会被判「类型不一致」
            // （压测缺陷 4），且 CTAS 会把该列建成 BIGINT。
            NullKind kind = staticNullKind(node, root, indexOf);
            out = switch (kind) {
                case STRING -> new VarCharVector(outputName, allocator);
                case DOUBLE -> new Float8Vector(outputName, allocator);
                case DECIMAL -> decimalVector(outputName, allocator, node);
                case BIGINT -> new BigIntVector(outputName, allocator);
                case BIT -> new BitVector(outputName, allocator);
                // 静态类型确实未知 → 保持旧的 Int64 兜底（`SELECT 1 + NULL` 等）
                case NONE -> new BigIntVector(outputName, allocator);
            };
        }

        out.setInitialCapacity(rowCount);
        out.allocateNew();
        for (int r = 0; r < rowCount; r++) {
            Object v = results[r];
            if (v == null) {
                out.setNull(r);
            } else if (out instanceof VarCharVector vc) {
                vc.setSafe(r, string(v).getBytes(StandardCharsets.UTF_8));
            } else if (out instanceof Float8Vector f) {
                f.setSafe(r, asDouble(v));
            } else if (out instanceof DecimalVector dv) {
                dv.setSafe(r, toBigDecimal(v).setScale(decimalScale, RoundingMode.HALF_UP));
            } else if (out instanceof BigIntVector b) {
                b.setSafe(r, asLong(v));
            } else if (out instanceof BitVector bt) {
                bt.setSafe(r, truthy(v) ? 1 : 0);
            }
        }
        out.setValueCount(rowCount);
        return out;
    }

    // ==================== 全 NULL 结果的静态类型推断 ====================

    /**
     * 静态返回文本的内置标量函数（仅用于「本批全 NULL、数据判不出类型」的兜底）。
     * 数值/布尔返回的函数（length/round/year/instr/...）与多态函数不在此列。
     * 新增字符串函数时须同步维护（口径 = ExprBuiltins / BuiltinScalars 各分类的实际返回类型）。
     */
    private static final Set<String> STRING_RETURNING_FUNCTIONS = Set.of(
            // ExprBuiltins 字符串
            "concat", "concat_ws", "upper", "ucase", "lower", "lcase",
            // BuiltinScalars 字符串
            "trim", "ltrim", "rtrim", "substr", "substring", "replace",
            "regexp_replace", "regexp_extract", "repeat", "reverse", "lpad", "rpad",
            "space", "chr", "soundex", "parse_url", "get_json_object", "json_extract", "elt",
            "initcap", "printf", "translate", "hex", "unhex", "bin",
            // 加密（文本摘要）
            "md5", "sha1", "sha2", "uuid",
            // 日期时间（返回 LocalDate/LocalDateTime，运行时统一按文本落 Utf8）
            "date_format", "from_unixtime", "to_date", "current_date", "current_timestamp",
            "date_add", "date_sub", "add_months", "last_day", "next_day",
            "from_utc_timestamp", "to_utc_timestamp");

    /** 结果类型取自参数的多态函数：任一参数静态为字符串 → 结果按字符串输出。 */
    private static final Set<String> POLYMORPHIC_FUNCTIONS = Set.of(
            "coalesce", "nvl", "nullif", "greatest", "least");

    /** 全 NULL 结果的静态类型类别（决定输出向量类型）。 */
    private enum NullKind { NONE, STRING, DOUBLE, DECIMAL, BIGINT, BIT }

    /**
     * 整批全 NULL 时按<b>静态信息</b>推断输出类型类别。
     *
     * <p>数据驱动（逐值看 {@code instanceof}）在全 NULL 时完全失效，若不看静态信息，
     * `CAST(NULL AS DOUBLE)` 会落 Int64 —— UNION ALL 类型比对失败、CTAS 列型错误。
     * 这里只覆盖<b>确定</b>的形态（CAST 目标类型、列自身的 Arrow 类型、字符串返回函数），
     * 其余返回 {@link NullKind#NONE} 保持旧的 Int64 兜底。</p>
     */
    private static NullKind staticNullKind(ExprNode n, VectorSchemaRoot root,
                                           Map<String, Integer> indexOf) {
        if (n instanceof ExprNode.Cast cs) {
            return switch (cs.type()) {
                case "double" -> NullKind.DOUBLE;
                case "decimal", "numeric" -> NullKind.DECIMAL;
                case "int", "integer", "bigint", "smallint", "tinyint" -> NullKind.BIGINT;
                case "boolean", "bool" -> NullKind.BIT;
                default -> NullKind.STRING;   // varchar/date/timestamp/... 运行时一律按文本落 Utf8
            };
        }
        if (n instanceof ExprNode.Column c) {
            Integer idx = indexOf.get(c.name());
            if (idx != null) {
                ArrowType t = root.getVector(idx).getField().getType();
                if (t instanceof ArrowType.Int) return NullKind.BIGINT;
                if (t instanceof ArrowType.FloatingPoint) return NullKind.DOUBLE;
                if (t instanceof ArrowType.Decimal) return NullKind.DECIMAL;
                if (t instanceof ArrowType.Bool) return NullKind.BIT;
                return NullKind.STRING;
            }
        }
        if (n instanceof ExprNode.Case cs) {
            for (ExprNode.WhenThen wt : cs.branches()) {
                NullKind k = staticNullKind(wt.then(), root, indexOf);
                if (k != NullKind.NONE) return k;
            }
            if (cs.elseExpr() != null) return staticNullKind(cs.elseExpr(), root, indexOf);
            return NullKind.NONE;
        }
        return stringTyped(n, root, indexOf) ? NullKind.STRING : NullKind.NONE;
    }

    /** 全 NULL DECIMAL 的输出向量：类型参数优先取 CAST 的 (p,s)，缺省 38/10 并夹紧。 */
    private static FieldVector decimalVector(String name, BufferAllocator allocator, ExprNode node) {
        int precision = 38;
        int scale = 10;
        if (node instanceof ExprNode.Cast cs) {
            if (cs.precision() > 0) precision = Math.min(cs.precision(), 38);
            if (cs.scale() >= 0) scale = Math.min(cs.scale(), precision);
        }
        precision = Math.max(precision, Math.max(1, scale));
        return new DecimalVector(name, allocator, precision, scale);
    }

    /** 表达式在本批全 NULL 时是否应落 Utf8（只判断「确定是字符串」，其余一律 false）。 */
    private static boolean stringTyped(ExprNode n, VectorSchemaRoot root, Map<String, Integer> indexOf) {
        if (n instanceof ExprNode.Literal l) return l.value() instanceof String;
        if (n instanceof ExprNode.Column c) {
            Integer idx = indexOf.get(c.name());
            if (idx == null) return false;
            // 与 readColumn 的取值类型一致：整数/浮点/BigDecimal/布尔之外（字符串、日期时间、二进制等）
            // 的值都会落入 anyStr，运行时按 Utf8 输出。
            ArrowType t = root.getVector(idx).getField().getType();
            return !(t instanceof ArrowType.Int) && !(t instanceof ArrowType.FloatingPoint)
                    && !(t instanceof ArrowType.Bool) && !(t instanceof ArrowType.Decimal);
        }
        if (n instanceof ExprNode.Cast cs) {
            return switch (cs.type()) {
                case "int", "bigint", "smallint", "tinyint", "double", "decimal", "boolean" -> false;
                default -> true;   // varchar / varbinary / date / time / timestamp / json / uuid / ipaddress
            };
        }
        if (n instanceof ExprNode.Case cs) {
            // 混排时 evaluateTo 按字符串提升，故任一分支确定为字符串即可。
            for (ExprNode.WhenThen wt : cs.branches()) {
                if (stringTyped(wt.then(), root, indexOf)) return true;
            }
            return cs.elseExpr() != null && stringTyped(cs.elseExpr(), root, indexOf);
        }
        if (n instanceof ExprNode.FuncCall f) {
            String name = f.name().toLowerCase(Locale.ROOT);
            if (STRING_RETURNING_FUNCTIONS.contains(name)) return true;
            if (POLYMORPHIC_FUNCTIONS.contains(name)) {
                for (ExprNode a : f.args()) if (stringTyped(a, root, indexOf)) return true;
            }
            return false;
        }
        return false;
    }

    // ==================== 逐行求值 ====================

    private static Object eval(ExprNode n, VectorSchemaRoot root, int row, Map<String, Integer> indexOf) {
        if (n instanceof ExprNode.Column c) {
            return readColumn(root, row, indexOf, c.name());
        }
        if (n instanceof ExprNode.Literal l) {
            return l.value();
        }
        if (n instanceof ExprNode.Negate g) {
            Object v = eval(g.operand(), root, row, indexOf);
            return v == null ? null : negate(v);
        }
        if (n instanceof ExprNode.Arith a) {
            Object l = eval(a.left(), root, row, indexOf);
            Object r = eval(a.right(), root, row, indexOf);
            return l == null || r == null ? null : arith(a.op(), l, r);
        }
        if (n instanceof ExprNode.Compare cmp) {
            Object l = eval(cmp.left(), root, row, indexOf);
            Object r = eval(cmp.right(), root, row, indexOf);
            if (l == null || r == null) return null;
            return compare(cmp.op(), l, r);
        }
        if (n instanceof ExprNode.IsNull i) {
            Object v = eval(i.operand(), root, row, indexOf);
            return i.negated() ? v != null : v == null;
        }
        if (n instanceof ExprNode.InList il) {
            Object v = eval(il.operand(), root, row, indexOf);
            if (v == null) return null;
            boolean found = false;
            for (ExprNode item : il.values()) {
                Object o = eval(item, root, row, indexOf);
                if (o == null) continue;
                if (equalsValue(v, o)) { found = true; break; }
            }
            return il.negated() ? !found : found;
        }
        if (n instanceof ExprNode.Like lk) {
            Object v = eval(lk.operand(), root, row, indexOf);
            Object p = eval(lk.pattern(), root, row, indexOf);
            // SQL 三值逻辑：任一侧 NULL → 结果未知（NOT LIKE 亦然）
            if (v == null || p == null) return null;
            boolean matched = likePattern(string(p)).matcher(string(v)).matches();
            return lk.negated() != matched;
        }
        if (n instanceof ExprNode.BoolOp b) {
            Object l = eval(b.left(), root, row, indexOf);
            Object r = eval(b.right(), root, row, indexOf);
            return boolOp(b.op(), l, r);
        }
        if (n instanceof ExprNode.Not nt) {
            Object v = eval(nt.operand(), root, row, indexOf);
            if (v == null) return null;
            return !truthy(v);
        }
        if (n instanceof ExprNode.Case c) {
            for (ExprNode.WhenThen w : c.branches()) {
                Object cond = eval(w.cond(), root, row, indexOf);
                if (truthy(cond)) return eval(w.then(), root, row, indexOf);
            }
            return c.elseExpr() == null ? null : eval(c.elseExpr(), root, row, indexOf);
        }
        if (n instanceof ExprNode.FuncCall f) {
            return evalFuncCall(f, root, row, indexOf);
        }
        if (n instanceof ExprNode.Cast c) {
            Object v = eval(c.operand(), root, row, indexOf);
            return v == null ? null : castTo(c.type(), v, c.precision(), c.scale(), c.tryCast());
        }
        throw new IllegalStateException("未知表达式节点: " + n);
    }

    private static Object evalFuncCall(ExprNode.FuncCall f, VectorSchemaRoot root, int row,
                                       Map<String, Integer> indexOf) {
        String name = f.name().toLowerCase();
        Object[] args = new Object[f.args().size()];
        for (int i = 0; i < args.length; i++) {
            args[i] = eval(f.args().get(i), root, row, indexOf);
        }
        if (ExprBuiltins.isBuiltin(name)) {
            return ExprBuiltins.apply(name, args);
        }
        ScalarFunction fn = ScalarFunctionRegistry.resolve(name);
        return fn.evaluate(args);
    }

    private static Object readColumn(VectorSchemaRoot root, int row, Map<String, Integer> indexOf, String name) {
        Integer idx = indexOf.get(name);
        if (idx == null) {
            throw new IllegalStateException("表达式列不存在: " + name + "（可用: " + indexOf.keySet() + "）");
        }
        FieldVector v = root.getVector(idx);
        if (v.isNull(row)) return null;
        Object val = v.getObject(row);
        // varchar 列 getObject 返回 byte[] / Text：归一化为 String，保证字符串分组稳定可比
        if (val instanceof byte[] bytes) return new String(bytes, StandardCharsets.UTF_8);
        if (val instanceof org.apache.arrow.vector.util.Text text) return text.toString();
        if (val instanceof java.sql.Timestamp || val instanceof java.sql.Date) return val;
        return val;
    }

    // ==================== 值运算 ====================

    private static Object negate(Object v) {
        if (v instanceof BigDecimal bd) return bd.negate();
        if (v instanceof Double || v instanceof Float) return -asDouble(v);
        if (v instanceof Number) return -asLong(v);
        throw new IllegalArgumentException("负号仅支持数值: " + v);
    }

    private static Object arith(ExprNode.Arith.Op op, Object l, Object r) {
        // ★DECIMAL（BigDecimal）走精确十进制运算：绝不能转 double ——
        //   0.1 + 0.2 会变成 0.30000000000000004，sum(DECIMAL) 也会带浮点尾差。
        if (l instanceof BigDecimal || r instanceof BigDecimal) {
            return arithDecimal(op, l, r);
        }
        boolean isDouble = op == ExprNode.Arith.Op.DIV
                || l instanceof Double || l instanceof Float
                || r instanceof Double || r instanceof Float;
        if (isDouble) {
            double a = asDouble(l), b = asDouble(r);
            return switch (op) {
                case ADD -> a + b;
                case SUB -> a - b;
                case MUL -> a * b;
                case DIV -> b == 0 ? null : a / b;
                case MOD -> b == 0 ? null : a % b;
            };
        }
        long a = asLong(l), b = asLong(r);
        return switch (op) {
            case ADD -> a + b;
            case SUB -> a - b;
            case MUL -> a * b;
            case DIV -> b == 0 ? null : (double) a / b;
            case MOD -> b == 0 ? null : a % b;
        };
    }

    /**
     * 精确十进制四则运算。结果 scale 取操作数 scale 的较大者（乘法则收敛回该 scale），
     * 保证 {@code DECIMAL(10,2) + DECIMAL(10,2)} / {@code *} 仍精确到分（{@code 0.30} / {@code 3.30}）。
     * 除法至少保留 6 位小数（对齐常见 SQL DECIMAL 除法口径）。
     */
    private static Object arithDecimal(ExprNode.Arith.Op op, Object l, Object r) {
        BigDecimal a = toBigDecimal(l);
        BigDecimal b = toBigDecimal(r);
        int scale = Math.max(a.scale(), b.scale());
        scale = Math.max(scale, 0);
        return switch (op) {
            case ADD -> a.add(b);
            case SUB -> a.subtract(b);
            case MUL -> a.multiply(b).setScale(scale, RoundingMode.HALF_UP);
            case DIV -> b.signum() == 0 ? null
                    : a.divide(b, Math.max(scale, 6), RoundingMode.HALF_UP);
            case MOD -> b.signum() == 0 ? null : a.remainder(b);
        };
    }

    /** 数值对象 → BigDecimal（double 经 {@code valueOf} 保留其十进制字面值）。非数值抛错。 */
    private static BigDecimal toBigDecimal(Object v) {
        if (v instanceof BigDecimal bd) return bd;
        if (v instanceof Double || v instanceof Float) return BigDecimal.valueOf(((Number) v).doubleValue());
        if (v instanceof Number n) return BigDecimal.valueOf(n.longValue());
        try {
            return new BigDecimal(string(v).trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("非数值无法转 DECIMAL: " + v);
        }
    }

    /**
     * 比较两值。走与 {@link ScalarExpr#compare} 同一套阶梯（整数 → 小数 → 字符串），
     * 保证同一表达式在 WHERE / CASE / 聚合 / GROUP BY 各子句语义一致：
     * 数值字符串（如 varchar 列 {@code '10'} 与字面量 {@code 9}）按数值比较，
     * 大整数不经过 double 丢精度。
     */
    static boolean compare(ExprNode.Compare.Op op, Object l, Object r) {
        return ScalarExpr.compare(string(l), opName(op), string(r));
    }

    private static String opName(ExprNode.Compare.Op op) {
        return switch (op) {
            case EQ -> "=";
            case NE -> "!=";
            case GT -> ">";
            case GE -> ">=";
            case LT -> "<";
            case LE -> "<=";
        };
    }

    private static boolean equalsValue(Object l, Object r) {
        return compare(ExprNode.Compare.Op.EQ, l, r);
    }

    /** 布尔三值：AND / OR（null 语义：truthy(null)=false，遇 null 视未知）。 */
    private static Object boolOp(ExprNode.BoolOp.Op op, Object l, Object r) {
        boolean tl = truthy(l), tr = truthy(r);
        if (op == ExprNode.BoolOp.Op.AND) {
            if (!tl || !tr) return false;
            if (l == null || r == null) return null;
            return true;
        }
        if (tl || tr) return true;
        if (l == null || r == null) return null;
        return false;
    }

    // ==================== CAST ====================

    /**
     * CAST 目标类型的规范化 + 白名单校验：返回统一类型名，未知类型返回 {@code null}。
     * 入口有两个 —— 解析期（{@link ExprParser#parseCastType}，null ⇒ 解析失败 ⇒ 计划期报错）与
     * 求值期（{@link #castTo}）；同一份映射保证「能解析」⇔「能求值」。
     */
    static String normalizeCastType(String raw) {
        if (raw == null) return null;
        String t = raw.trim().toLowerCase(Locale.ROOT);
        return switch (t) {
            case "int", "integer" -> "int";
            case "bigint", "long" -> "bigint";
            case "smallint" -> "smallint";
            case "tinyint" -> "tinyint";
            case "double", "float", "real" -> "double";
            case "decimal", "numeric" -> "decimal";
            case "boolean", "bool" -> "boolean";
            case "varchar", "char", "string", "text" -> "varchar";
            case "varbinary", "binary" -> "varbinary";
            case "date" -> "date";
            case "time" -> "time";
            case "timestamp", "datetime" -> "timestamp";
            case "json" -> "json";
            case "uuid" -> "uuid";
            case "ipaddress" -> "ipaddress";
            default -> null;
        };
    }

    /**
     * 类型转换求值。目标类型 = 规范名（{@link #normalizeCastType} 产物）。
     *
     * <p>转换失败（脏数据无法解析、DECIMAL 精度溢出）一律返回 {@code null} —— 与 Hive/Spark 的
     * CAST 语义一致：一行脏数据不应让整批报表失败。{@code TRY_CAST} 与 {@code CAST} 因此在
     * 值层面同义，保留 {@code tryCast} 参数只为表达意图与后续差异化（如需要严格模式）。</p>
     */
    private static Object castTo(String type, Object v, int precision, int scale, boolean tryCast) {
        try {
            return switch (type) {
                case "int", "bigint", "smallint", "tinyint" -> asLong(v);
                case "double" -> asDouble(v);
                case "decimal" -> toDecimal(v, precision, scale);
                case "boolean" -> toBoolean(v);
                // varchar / varbinary / date / time / timestamp / json / uuid / ipaddress → 统一按文本
                // （与 ExprEvaluator 产出 Utf8 向量一致；日期时间列 readColumn 返回 Timestamp/Date，此处 toString）。
                default -> string(v);
            };
        } catch (RuntimeException e) {
            return null;
        }
    }

    /**
     * 转 BigDecimal：数值对象直接 valueOf，字符串按文本解析（保精度）。
     * 若目标声明了 {@code DECIMAL(p,s)}（scale ≥ 0），按 s 四舍五入定标 ——
     * {@code CAST(0.1 AS DECIMAL(10,2))} 得 {@code 0.10}，后续算术/聚合才能保持「精确到分」。
     * 给定的精度 p 用于越界检查 —— 解析失败 / 溢出时本方法抛异常，由
     * {@link #castTo} 统一捕获并返回 {@code NULL}（与 Hive/Spark 的 CAST 语义一致）。
     */
    private static BigDecimal toDecimal(Object v, int precision, int scale) {
        BigDecimal bd;
        if (v instanceof BigDecimal b) bd = b;
        else if (v instanceof Double || v instanceof Float) bd = BigDecimal.valueOf(((Number) v).doubleValue());
        else if (v instanceof Number n) bd = BigDecimal.valueOf(n.longValue());
        else {
            try {
                bd = new BigDecimal(string(v).trim());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("CAST(… AS DECIMAL) 无法解析为数值: " + v);
            }
        }
        if (scale >= 0) bd = bd.setScale(scale, RoundingMode.HALF_UP);
        if (precision > 0) {
            int digits = bd.precision() - bd.scale();   // 整数位数（scale 已定标）
            if (digits > precision - Math.max(scale, 0)) {
                throw new ArithmeticException("DECIMAL(" + precision + "," + scale + ") 溢出: " + v);
            }
        }
        return bd;
    }

    /** 转布尔：Boolean 直取；Number 非零为真；字符串按 true/false/yes/no/1/0 或数值解析。 */
    private static Boolean toBoolean(Object v) {
        if (v instanceof Boolean b) return b;
        if (v instanceof Number n) return n.doubleValue() != 0;
        String s = string(v).trim();
        if (s.isEmpty()) return Boolean.FALSE;
        String low = s.toLowerCase(Locale.ROOT);
        if (low.equals("true") || low.equals("t") || low.equals("yes") || low.equals("y")) return Boolean.TRUE;
        if (low.equals("false") || low.equals("f") || low.equals("no") || low.equals("n")) return Boolean.FALSE;
        try {
            return Double.parseDouble(s) != 0;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("CAST(… AS BOOLEAN) 无法解析为布尔: " + v);
        }
    }

    // ==================== 类型工具 ====================

    /** SQL 真值：null → false；Boolean → 值；Number → 非零；其余 → true（与 IfFunction.truthy 一致）。 */
    public static boolean truthy(Object v) {
        if (v == null) return false;
        if (v instanceof Boolean b) return b;
        if (v instanceof Number n) return n.doubleValue() != 0;
        return true;
    }

    public static boolean isNumeric(Object v) {
        return v instanceof Number;
    }

    public static double asDouble(Object v) {
        if (v instanceof Number n) return n.doubleValue();
        if (v instanceof Boolean b) return b ? 1 : 0;
        try {
            return Double.parseDouble(string(v).trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("非数值无法转 double: " + v);
        }
    }

    public static long asLong(Object v) {
        if (v instanceof Number n) return n.longValue();
        if (v instanceof Boolean b) return b ? 1 : 0;
        try {
            return Long.parseLong(string(v).trim());
        } catch (NumberFormatException e) {
            return (long) Double.parseDouble(string(v).trim());
        }
    }

    public static String string(Object v) {
        if (v == null) return null;
        if (v instanceof byte[] bytes) return new String(bytes, StandardCharsets.UTF_8);
        return v.toString();
    }

    /** 建向量 + 类型推断（供标量函数/临时值）。 */
    static FieldVector createVector(Class<?> resultClass, BufferAllocator allocator, String name) {
        ArrowType type;
        if (resultClass == Double.class || resultClass == Float.class) {
            type = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        } else if (resultClass == Long.class || resultClass == Integer.class
                || resultClass == Short.class || resultClass == Byte.class) {
            type = new ArrowType.Int(64, true);
        } else if (resultClass == Boolean.class) {
            type = new ArrowType.Bool();
        } else {
            type = new ArrowType.Utf8();
        }
        return new Field(name, FieldType.nullable(type), null).createVector(allocator);
    }
}
