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
            "json_array_explode",                    // JSON 数组文本 + 字段名 → 字段值数组
            "json_array_explode_with_separator",     // JSON 数组 + 分隔符 + 多 key → 拼接字符串数组
            "size");                                 // 数组长度（字符串长度用 length）

    /**
     * split 的分隔符正则编译缓存：分隔符是「每行都传」的参数，逐行 {@code Pattern.compile} 在百万行
     * 上是纯浪费。与 LIKE 缓存同口径（上限 256 条，超出整体清空）。
     */
    private static final java.util.Map<String, java.util.regex.Pattern> SPLIT_PATTERN_CACHE =
            new java.util.concurrent.ConcurrentHashMap<>();

    /** from_str_to_array / json_array_explode 的 JSON 解析器（ObjectMapper 线程安全，与 StringFunctions 同款）。 */
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
            case "json_array_explode": {
                // json_array_explode(arr, key) —— JSON 数组文本 + 首层对象字段名 → 字段值数组，
                // 例如 '[{"jobName":"储备干部"},{"jobName":"装机工"}]' + 'jobName' → ['储备干部','装机工']。
                // 非法 JSON / 非数组 → NULL；非对象元素或缺少该字段 → NULL 元素（保留下标对齐，
                // 便于配合 UNNEST WITH ORDINALITY 逐条对应原数组位置）。
                require(name, args, 2);
                if (args[0] == null || args[1] == null) return null;
                return jsonFieldArray(ExprEvaluator.string(args[0]), ExprEvaluator.string(args[1]));
            }
            case "json_array_explode_with_separator": {
                // json_array_explode_with_separator(arr, sep, ...keyvalue) —— 每个数组元素按给定
                // key/path 取值，再用分隔符拼成一个字符串，逐个元素成数组：
                // '[{"jobName":"储备干部","province":44},...]' + ',' + '$.jobName' + '$.province'
                //   → ['储备干部,44', ...]。
                // 非法 JSON / 非数组 → NULL；非对象元素、或任一 key 缺失/NULL → NULL 元素（保留下标对齐）。
                if (args.length < 3) {
                    throw new IllegalArgumentException(
                            "函数 " + name + " 至少需要 3 个参数（数组, 分隔符, 至少一个 key），实得 "
                                    + args.length);
                }
                for (Object a : args) {
                    if (a == null) return null;
                }
                String[] paths = new String[args.length - 2];
                for (int i = 2; i < args.length; i++) {
                    paths[i - 2] = ExprEvaluator.string(args[i]);
                }
                return jsonFieldJoinArray(ExprEvaluator.string(args[0]),
                        ExprEvaluator.string(args[1]), paths);
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
     * JSON 文本 → 节点（非法 / null → null）。供表达式求值器「同一行同一文本只解析一次」的
     * 行级缓存复用；数组函数另有节点版入口，缓存命中时可直接跳过解析。
     */
    public static JsonNode parseJsonText(String text) {
        if (text == null) return null;
        try {
            return JSON_MAPPER.readTree(text);
        } catch (Exception e) {
            return null;
        }
    }

    /** 文本版入口（内置函数 apply 路径）。 */
    private static List<String> jsonTextArray(String text) {
        return jsonTextArray(parseJsonText(text));
    }

    /**
     * 已解析节点 → 文本列表；null / 非数组 → null。元素口径与 get_json_object 一致：
     * 值节点取 {@code asText()}，对象/数组子节点保留 JSON 文本，JSON null → null 元素。
     * 用 ArrayList 而非 List.of —— 子元素允许 NULL。
     */
    public static List<String> jsonTextArray(JsonNode node) {
        if (node == null || !node.isArray()) return null;
        List<String> out = new ArrayList<>(node.size());
        for (JsonNode el : node) {
            out.add(el == null || el.isNull() ? null
                    : el.isValueNode() ? el.asText() : el.toString());
        }
        return out;
    }

    /**
     * JSON 数组文本 + 分隔符 + 多个 key/path → 每元素拼接成一个字符串（列表结果）；非法 JSON 或
     * 非数组 → null。key 写法支持 {@code jobName}、{@code $.jobName} 与逐层 {@code $.a.b}。
     * 元素不是对象、或任一 key 缺失/为 JSON null → null 元素（保留下标对齐）；值口径与
     * {@link #jsonFieldArray} 一致（标量取 {@code asText()}，对象/数组保留 JSON 文本）。
     */
    private static List<String> jsonFieldJoinArray(String text, String separator, String[] paths) {
        return jsonFieldJoinArray(parseJsonText(text), separator, paths);
    }

    /** 已解析节点版（供行级 JSON 缓存复用，跳过重复 readTree）。 */
    public static List<String> jsonFieldJoinArray(JsonNode node, String separator, String[] paths) {
        if (node == null || !node.isArray()) return null;
        List<String> out = new ArrayList<>(node.size());
        for (JsonNode el : node) {
            if (el == null || !el.isObject()) {
                out.add(null);
                continue;
            }
            StringBuilder sb = new StringBuilder();
            boolean complete = true;
            for (int i = 0; i < paths.length; i++) {
                JsonNode v = pathValue(el, paths[i]);
                if (v == null || v.isNull()) {
                    complete = false;
                    break;
                }
                if (i > 0) sb.append(separator);
                sb.append(v.isValueNode() ? v.asText() : v.toString());
            }
            out.add(complete ? sb.toString() : null);
        }
        return out;
    }

    /**
     * 取 JSON path：{@code jobName}（裸字段名）/ {@code $.jobName} / {@code $.a.b} /
     * {@code $['a']} / {@code $[0]}；任一层缺失或 JSON null → null。
     *
     * <p><b>裸名与 {@code $.} 前缀等价</b>：旧 {@code json_array_explode} 只认字面键
     * （{@code el.get(key)}），用户写 {@code '$.jobName'} 会整列静默 NULL；这里先归一
     * 成 {@code $.} 路径再走 {@link StringFunctions#resolveNode}，与
     * {@code json_array_explode_with_separator} 的口径统一。</p>
     */
    private static JsonNode pathValue(JsonNode obj, String path) {
        String p = path == null ? "" : path.trim();
        if (p.isEmpty()) return obj;
        if (!p.startsWith("$")) p = "$." + p;
        // 归一：'$' / '$.' / '.a' / '$.a' 都按「从根开始」处理；括号段（$['a'] / $[0]）交给 resolveNode
        int i = 1;
        while (i < p.length() && p.charAt(i) == '.') i++;
        if (i >= p.length()) return obj;
        String rest = p.substring(i);
        return com.dojao.core.function.scalar.string.StringFunctions.resolveNode(
                obj, rest.startsWith("[") ? "$" + rest : "$." + rest);
    }

    /**
     * JSON 数组文本 + 首层对象字段名 → 字段值列表；非法 JSON 或非数组 → null。
     * 元素口径与 {@link #jsonTextArray} 一致（标量取 {@code asText()}，对象/数组保留 JSON 文本，
     * JSON null → null）；元素不是对象或没有该字段 → null（保留下标对齐）。
     */
    private static List<String> jsonFieldArray(String text, String key) {
        return jsonFieldArray(parseJsonText(text), key);
    }

    /** 已解析节点版（供行级 JSON 缓存复用，跳过重复 readTree）。 */
    public static List<String> jsonFieldArray(JsonNode node, String key) {
        if (node == null || !node.isArray()) return null;
        List<String> out = new ArrayList<>(node.size());
        for (JsonNode el : node) {
            JsonNode v = el != null && el.isObject() ? pathValue(el, key) : null;
            out.add(v == null || v.isNull() ? null
                    : v.isValueNode() ? v.asText() : v.toString());
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
