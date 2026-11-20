package com.dojao.core.operator.expr;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * {@link ExprNode.FuncCall} 内置标量函数表 —— 为 GROUP BY 表达式（及后续 SELECT 派生列）提供
 * 常用日期 / 数学 / 字符串函数。与引擎表达式求值器是同一套值模型（String/Long/Double/Boolean/
 * BigDecimal/时态对象），NULL 传播，类型由调用方 ExprEvaluator 推断。
 *
 * <p><b>不</b>注册进 {@link com.dojao.core.function.scalar.ScalarFunctionRegistry}：内置函数仅供
 * 表达式引擎解析，避免 {@code ScalarExpr.tryParse("year(x)")} 误命中（那会让 SELECT 投影把
 * {@code year(x)} 误判成 ScalarProject 裸函数而非 group-key 派生列引用，产生冲突）。</p>
 *
 * <p>时态列兼容性：Arrow 时间戳向量 {@code getObject(r)} 的返回类型依赖字段时区——无时区
 * {@code TimeStampMilliVector} → {@link LocalDateTime}，带时区 → {@link ZonedDateTime}；日期 →
 * {@link LocalDate}；亦可能透传 {@link java.sql.Timestamp}/{@link java.sql.Date}。故
 * {@code year/month/day/date} 走 {@link #toZoned(Object)} 统一归一（带时区/瞬时型转 UTC；
 * 朴素 LocalDate/LocalDateTime 视为 UTC 本地墙钟），保证确定性分组。</p>
 *
 * @author jiaxin
 * @date 2026/9/6
 */
public final class ExprBuiltins {

    private static final Set<String> NAMES = Set.of(
            "year", "month", "day", "date",          // 日期提取
            "floor", "ceil", "ceiling", "round", "abs", // 数学
            "concat", "upper", "ucase", "lower", "lcase", "length", // 字符串
            "split",                                 // 分隔符切分 → 数组
            "from_str_to_array",                     // JSON 数组文本 → 数组
            "size");                                 // 数组长度（字符串长度用 length）

    /**
     * split 的分隔符正则编译缓存：分隔符是「每行都传」的参数，逐行 {@code Pattern.compile} 在百万行
     * 上是纯浪费。与 LIKE 缓存同口径（上限 256 条，超出整体清空）。
     */
    private static final java.util.Map<String, java.util.regex.Pattern> SPLIT_PATTERN_CACHE =
            new java.util.concurrent.ConcurrentHashMap<>();

    /** from_str_to_array 的 JSON 解析器（ObjectMapper 线程安全，与 StringFunctions 同款）。 */
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private ExprBuiltins() {
    }

    /** 全部内置函数名（小写、只读），供函数目录 / UI 补全枚举。 */
    public static Set<String> names() {
        return NAMES;
    }

    /** 是否内置函数名（大小写不敏感；调用方已小写化，此处再兜底一次）。 */
    public static boolean isBuiltin(String name) {
        return NAMES.contains(name.toLowerCase(Locale.ROOT));
    }

    /**
     * 应用内置函数。NULL 传播：参数含 NULL（concat 任一 NULL）→ null。
     * 未知函数 / 参数个数错误抛 IllegalArgumentException（fail-fast）。
     */
    public static Object apply(String name, Object[] args) {
        switch (name.toLowerCase(Locale.ROOT)) {
            case "year":  require(name, args, 1); return nullIfNull(name, args[0], v -> (long) toZoned(v).getYear());
            case "month": require(name, args, 1); return nullIfNull(name, args[0], v -> (long) toZoned(v).getMonthValue());
            case "day":   require(name, args, 1); return nullIfNull(name, args[0], v -> (long) toZoned(v).getDayOfMonth());
            case "date":  require(name, args, 1); return nullIfNull(name, args[0], v -> toZoned(v).toLocalDate().toEpochDay());
            case "floor": {
                require(name, args, 1);
                if (args[0] == null) return null;
                return (long) Math.floor(ExprEvaluator.asDouble(args[0]));
            }
            case "ceil":
            case "ceiling": {
                require(name, args, 1);
                if (args[0] == null) return null;
                return (long) Math.ceil(ExprEvaluator.asDouble(args[0]));
            }
            case "round": {
                require(name, args, 1, 2);
                if (args[0] == null) return null;
                if (args.length == 1) {
                    return Math.round(ExprEvaluator.asDouble(args[0]));   // round(x) → Long
                }
                // round(x, n) → 保留 n 位小数的 Double（按十进制舍入，与 SQL 语义一致）
                if (args[1] == null) return null;
                int n = (int) ExprEvaluator.asLong(args[1]);
                double x = ExprEvaluator.asDouble(args[0]);
                return new BigDecimal(x).setScale(n, RoundingMode.HALF_UP).doubleValue();
            }
            case "abs": {
                require(name, args, 1);
                Object v = args[0];
                if (v == null) return null;
                if (v instanceof BigDecimal || v instanceof Double || v instanceof Float) {
                    return Math.abs(ExprEvaluator.asDouble(v));
                }
                return Math.abs(ExprEvaluator.asLong(v));
            }
            case "concat": {
                if (args.length == 0) throw new IllegalArgumentException("concat 至少 1 个参数");
                StringBuilder sb = new StringBuilder();
                for (Object a : args) {
                    if (a == null) return null;   // SQL concat 遇 NULL → NULL
                    sb.append(ExprEvaluator.string(a));
                }
                return sb.toString();
            }
            case "upper":
            case "ucase": {
                require(name, args, 1);
                return args[0] == null ? null : ExprEvaluator.string(args[0]).toUpperCase(Locale.ROOT);
            }
            case "lower":
            case "lcase": {
                require(name, args, 1);
                return args[0] == null ? null : ExprEvaluator.string(args[0]).toLowerCase(Locale.ROOT);
            }
            case "length": {
                require(name, args, 1);
                if (args[0] == null) return null;
                // 口径约定：字符串长度用 length、数组长度用 size（见 function-conventions.md）。
                // 数组若落进 string() 会变成 "[a, b]" 的字符数 —— 静默错值，必须响亮拒绝。
                if (args[0] instanceof java.util.Collection<?>) {
                    throw new IllegalArgumentException("length 只用于字符串；数组长度请用 size(...)");
                }
                return (long) ExprEvaluator.string(args[0]).length();
            }
            case "size": {
                // size(array) —— list/array 的长度。字符串请用 length（Spark 同口径）。
                require(name, args, 1);
                if (args[0] == null) return null;
                if (args[0] instanceof java.util.Collection<?> c) return (long) c.size();
                throw new IllegalArgumentException(
                        "size 只用于数组（list/array）；字符串长度请用 length(...)，实得: "
                                + args[0].getClass().getSimpleName());
            }
            case "split": {
                // split(str, regex) —— 分隔符按正则解释（Hive/Spark 口径）；limit=-1 保留末尾空串，
                // 否则 'a,b,' 会被 Java 默认行为裁成 [a, b] 而与上游数据列数对不上。
                require(name, args, 2);
                if (args[0] == null || args[1] == null) return null;
                String[] parts = splitPattern(ExprEvaluator.string(args[1]))
                        .split(ExprEvaluator.string(args[0]), -1);
                return java.util.List.of(parts);
            }
            case "from_str_to_array": {
                // from_str_to_array(str) —— 只接受合法 JSON 数组文本（如 '["a","b"]'）；
                // 非法 JSON / 非数组 → NULL。元素一律落 Utf8 文本，JSON null → NULL 元素。
                require(name, args, 1);
                if (args[0] == null) return null;
                return jsonTextArray(ExprEvaluator.string(args[0]));
            }
            default:
                throw new IllegalArgumentException("未内置函数: " + name);
        }
    }

    // ==================== 工具 ====================

    /** split 分隔符 → 编译好的正则（带缓存；非法正则报可读错误而不是每行抛一次）。 */
    private static java.util.regex.Pattern splitPattern(String regex) {
        java.util.regex.Pattern p = SPLIT_PATTERN_CACHE.get(regex);
        if (p != null) return p;
        try {
            p = java.util.regex.Pattern.compile(regex);
        } catch (java.util.regex.PatternSyntaxException e) {
            throw new IllegalArgumentException("split 的分隔符不是合法正则: '" + regex + "'", e);
        }
        if (SPLIT_PATTERN_CACHE.size() >= 256) SPLIT_PATTERN_CACHE.clear();
        SPLIT_PATTERN_CACHE.put(regex, p);
        return p;
    }

    /**
     * JSON 数组文本 → 文本列表；非法 JSON 或非数组 → null。元素口径与 get_json_object 一致：
     * 值节点取 {@code asText()}，对象/数组子节点保留 JSON 文本，JSON null → null 元素。
     * 用 ArrayList 而非 List.of —— 子元素允许 NULL。
     */
    private static List<String> jsonTextArray(String text) {
        JsonNode node;
        try {
            node = JSON_MAPPER.readTree(text);
        } catch (Exception e) {
            return null;
        }
        if (node == null || !node.isArray()) return null;
        List<String> out = new ArrayList<>(node.size());
        for (JsonNode el : node) {
            out.add(el == null || el.isNull() ? null
                    : el.isValueNode() ? el.asText() : el.toString());
        }
        return out;
    }

    private static void require(String name, Object[] args, int... allowed) {
        for (int n : allowed) {
            if (args.length == n) return;
        }
        throw new IllegalArgumentException("函数 " + name + " 需 " + java.util.Arrays.toString(allowed)
                + " 个参数，实得 " + args.length);
    }

    /** 单参 null 传播包装：v==null → null，否则应用函数。 */
    private static Object nullIfNull(String name, Object v, java.util.function.Function<Object, Object> f) {
        return v == null ? null : f.apply(v);
    }

    /**
     * 时态值 → {@link ZonedDateTime}（提取 year/month/day 的统一入口）。带时区/瞬时型归一到 UTC；
     * 朴素 LocalDate/LocalDateTime 无时区概念，按 UTC 本地墙钟处理（epoch 语义确定性）；String
     * 依次尝试 ISO 标准与 SQL 常见 {@code yyyy-MM-dd[ HH:mm:ss]} 格式。
     */
    private static ZonedDateTime toZoned(Object v) {
        if (v instanceof ZonedDateTime z) return z;
        if (v instanceof OffsetDateTime odt) return odt.toZonedDateTime();
        if (v instanceof Instant i) return i.atZone(ZoneOffset.UTC);
        if (v instanceof LocalDateTime ldt) return ldt.atZone(ZoneOffset.UTC);
        if (v instanceof LocalDate ld) return ld.atStartOfDay(ZoneOffset.UTC);
        if (v instanceof java.sql.Timestamp ts) return ts.toInstant().atZone(ZoneOffset.UTC);
        if (v instanceof java.sql.Date d) return d.toLocalDate().atStartOfDay(ZoneOffset.UTC);
        if (v instanceof java.util.Date d) return d.toInstant().atZone(ZoneOffset.UTC);
        if (v instanceof Number n) return Instant.ofEpochMilli(n.longValue()).atZone(ZoneOffset.UTC);
        if (v instanceof String || v instanceof byte[]) {
            String s = ExprEvaluator.string(v).trim();
            if (s.isEmpty()) throw new IllegalArgumentException("空字符串非有效时态值");
            try {
                return ZonedDateTime.parse(s).toOffsetDateTime().toZonedDateTime();
            } catch (Exception ignored) { }
            try {
                return OffsetDateTime.parse(s).toZonedDateTime();
            } catch (Exception ignored) { }
            try {
                return Instant.parse(s).atZone(ZoneOffset.UTC);
            } catch (Exception ignored) { }
            try {
                return LocalDateTime.parse(s).atZone(ZoneOffset.UTC);
            } catch (Exception ignored) { }
            try {
                return LocalDate.parse(s).atStartOfDay(ZoneOffset.UTC);
            } catch (Exception ignored) { }
            // SQL 常见 'yyyy-MM-dd HH:mm:ss'（T 由 LocalDateTime.parse 已覆盖）
            try {
                DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                return LocalDateTime.parse(s, f).atZone(ZoneOffset.UTC);
            } catch (Exception ignored) { }
            throw new IllegalArgumentException("无法识别的时态值: '" + s + "'");
        }
        throw new IllegalArgumentException("非时态类型无法提取日期: " + v.getClass().getSimpleName());
    }
}
