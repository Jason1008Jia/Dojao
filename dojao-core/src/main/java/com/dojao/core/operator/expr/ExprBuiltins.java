package com.dojao.core.operator.expr;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
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
            "concat", "upper", "ucase", "lower", "lcase", "length"); // 字符串

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
                return args[0] == null ? null : (long) ExprEvaluator.string(args[0]).length();
            }
            default:
                throw new IllegalArgumentException("未内置函数: " + name);
        }
    }

    // ==================== 工具 ====================

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
