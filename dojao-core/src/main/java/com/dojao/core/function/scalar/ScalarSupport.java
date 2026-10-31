package com.dojao.core.function.scalar;

import com.dojao.core.operator.expr.ExprEvaluator;
import com.dojao.spi.function.ScalarFunction;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.function.Function;

/**
 * 标量函数公共助手 —— 类型强转 / NULL 传播 / 参数个数自守卫 / 时态归一 / 函数式适配。
 *
 * <p>与 {@link com.dojao.core.operator.expr.ExprBuiltins} 的私有工具同源，但供本包（内置标量函数）
 * 复用：值模型一致（String/Long/Double/Boolean/BigDecimal/时态对象），类型强转委托
 * {@link ExprEvaluator#asDouble}/{@link ExprEvaluator#asLong}/{@link ExprEvaluator#string}。</p>
 *
 * @author jiaxin
 * @date 2026/10/11
 */
public final class ScalarSupport {

    private ScalarSupport() {
    }

    /**
     * 把一个 lambda 适配成 {@link ScalarFunction}（避免为每个函数单独建类）。
     * {@code impl} 收整批参数（含 null = SQL NULL），返回标量结果。
     */
    public static ScalarFunction fn(String name, String display, Function<Object[], Object> impl) {
        return new ScalarFunction() {
            @Override public String functionName() { return name; }
            @Override public String display() { return display; }
            @Override public Object evaluate(Object[] inputs) { return impl.apply(inputs); }
        };
    }

    /** 参数个数自守卫（引擎无 arity 校验），不匹配抛 {@link IllegalArgumentException}（fail-fast）。 */
    public static void requireArity(String name, Object[] args, int... allowed) {
        for (int n : allowed) {
            if (args.length == n) return;
        }
        throw new IllegalArgumentException("函数 " + name + " 需 " + Arrays.toString(allowed)
                + " 个参数，实得 " + args.length);
    }

    /** 可变参函数：至少 {@code min} 个参数。 */
    public static void requireAtLeast(String name, Object[] args, int min) {
        if (args.length < min) {
            throw new IllegalArgumentException("函数 " + name + " 至少需 " + min
                    + " 个参数，实得 " + args.length);
        }
    }

    // ==================== 类型强转 ====================

    public static double asDouble(Object v) {
        return ExprEvaluator.asDouble(v);
    }

    public static long asLong(Object v) {
        return ExprEvaluator.asLong(v);
    }

    public static String string(Object v) {
        return ExprEvaluator.string(v);
    }

    public static boolean truthy(Object v) {
        return ExprEvaluator.truthy(v);
    }

    /**
     * 参数转 int（位置 / 长度 / 索引类参数）。兼容两种字面量形态：
     * {@code ScalarExpr.tryParse} 把数字解析成 {@link Double}，通用 {@code ExprParser} 产 {@link Long}。
     */
    public static int asInt(Object v) {
        if (v instanceof Number n) return n.intValue();
        String s = string(v);
        if (s == null) throw new IllegalArgumentException("空值无法转 int");
        return (int) Math.round(Double.parseDouble(s.trim()));
    }

    // ==================== 时态归一 ====================

    /**
     * 时态值 → {@link ZonedDateTime}。带时区/瞬时型归一到 UTC；朴素 LocalDate/LocalDateTime 无时区
     * 概念，按 UTC 本地墙钟处理；String 依次尝试 ISO 与 SQL 常见格式；Number 视为 epoch 毫秒。
     * （与 {@code ExprBuiltins.toZoned} 同款归一，保证确定性。）
     */
    public static ZonedDateTime toZoned(Object v) {
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
            String s = string(v).trim();
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
            try {
                return LocalDateTime.parse(s, java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                        .atZone(ZoneOffset.UTC);
            } catch (Exception ignored) { }
            throw new IllegalArgumentException("无法识别的时态值: '" + s + "'");
        }
        throw new IllegalArgumentException("非时态类型无法提取日期: " + v.getClass().getSimpleName());
    }

    /** 时态值 → {@link ZonedDateTime}，解析失败返回 null（Hive 日期函数对非法输入的宽松语义）。 */
    public static ZonedDateTime toZonedOrNull(Object v) {
        if (v == null) return null;
        try {
            return toZoned(v);
        } catch (RuntimeException e) {
            return null;
        }
    }

    public static LocalDate toLocalDate(Object v) {
        return toZoned(v).toLocalDate();
    }

    public static LocalDateTime toLocalDateTime(Object v) {
        return toZoned(v).toLocalDateTime();
    }
}
