package com.dojao.core.function.scalar.datetime;

import com.dojao.core.function.scalar.ScalarFunctionRegistry;
import com.dojao.core.function.scalar.ScalarSupport;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.time.temporal.WeekFields;
import java.util.Locale;
import java.util.function.Function;

/**
 * Hive 标准日期时间标量函数（{@code year/month/day/date} 见 {@code ExprBuiltins}，此处不重复）。
 *
 * <p>日期/时间戳型函数返回 {@link LocalDate}/{@link LocalDateTime}（引擎按 UTF-8 文本序列化 ISO 形式）；
 * 本就返回字符串的（{@code from_unixtime}/{@code date_format}）直接产出格式化文本。时间语义统一按 UTC
 * （朴素 LocalDate/LocalDateTime/字符串 视为 UTC 墙钟，见 {@code ScalarSupport.toZoned}）。</p>
 *
 * @author jiaxin
 * @date 2026/10/11
 */
public final class DateTimeFunctions {

    /** Hive from_unixtime / unix_timestamp 默认格式。 */
    private static final String DEFAULT_TS_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private DateTimeFunctions() {
    }

    public static void registerAll() {
        reg("current_date", "当前日期", args -> {
            ScalarSupport.requireArity("current_date", args, 0);
            return LocalDate.now();
        });
        reg("current_timestamp", "当前时间戳", args -> {
            ScalarSupport.requireArity("current_timestamp", args, 0);
            return LocalDateTime.now();
        });
        reg("unix_timestamp", "unix 秒：unix_timestamp() 当前；unix_timestamp(s[,fmt]) 解析", args -> {
            ScalarSupport.requireArity("unix_timestamp", args, 0, 1, 2);
            if (args.length == 0) return Instant.now().getEpochSecond();
            if (args[0] == null) return null;
            if (args.length == 1) {
                ZonedDateTime z = ScalarSupport.toZonedOrNull(args[0]);
                return z == null ? null : z.toEpochSecond();
            }
            if (args[1] == null) return null;
            return parseToEpoch(ScalarSupport.string(args[0]), ScalarSupport.string(args[1]));
        });
        reg("from_unixtime", "unix 秒 → 时间字符串（默认 'yyyy-MM-dd HH:mm:ss'）", args -> {
            ScalarSupport.requireArity("from_unixtime", args, 1, 2);
            if (args[0] == null) return null;
            String fmt = args.length == 2 ? ScalarSupport.string(args[1]) : DEFAULT_TS_FORMAT;
            if (fmt == null) return null;
            LocalDateTime ldt = LocalDateTime.ofEpochSecond(ScalarSupport.asLong(args[0]), 0, ZoneOffset.UTC);
            return ldt.format(JavaDateFormat.toFormatter(fmt));
        });
        reg("to_date", "取日期部分", args -> {
            ScalarSupport.requireArity("to_date", args, 1);
            ZonedDateTime z = ScalarSupport.toZonedOrNull(args[0]);
            return z == null ? null : z.toLocalDate();
        });
        reg("hour", "小时（0-23）", simple("hour", 1, z -> (long) z.getHour()));
        reg("minute", "分钟（0-59）", simple("minute", 1, z -> (long) z.getMinute()));
        reg("second", "秒（0-59）", simple("second", 1, z -> (long) z.getSecond()));
        reg("dayofmonth", "当月第几天（1-31）", simple("dayofmonth", 1, z -> (long) z.getDayOfMonth()));
        reg("weekofyear", "ISO 周序号", simple("weekofyear", 1,
                z -> (long) z.get(WeekFields.ISO.weekOfWeekBasedYear())));
        reg("dayofweek", "星期几（Hive：1=周日 … 7=周六）", simple("dayofweek", 1,
                z -> (long) (z.getDayOfWeek().getValue() % 7) + 1));
        reg("datediff", "日期差（d1-d2，天）", args -> {
            ScalarSupport.requireArity("datediff", args, 2);
            ZonedDateTime a = ScalarSupport.toZonedOrNull(args[0]);
            ZonedDateTime b = ScalarSupport.toZonedOrNull(args[1]);
            if (a == null || b == null) return null;
            return ChronoUnit.DAYS.between(b.toLocalDate(), a.toLocalDate());
        });
        reg("date_add", "日期加天数 date_add(d,n)", args -> shiftDays("date_add", args, 1));
        reg("date_sub", "日期减天数 date_sub(d,n)", args -> shiftDays("date_sub", args, -1));
        reg("add_months", "加月（月末自动对齐）add_months(d,n)", args -> {
            ScalarSupport.requireArity("add_months", args, 2);
            ZonedDateTime z = ScalarSupport.toZonedOrNull(args[0]);
            if (z == null || args[1] == null) return null;
            return z.toLocalDate().plusMonths(ScalarSupport.asLong(args[1]));
        });
        reg("months_between", "月差 months_between(d1,d2)（double）", args -> {
            ScalarSupport.requireArity("months_between", args, 2);
            ZonedDateTime a = ScalarSupport.toZonedOrNull(args[0]);
            ZonedDateTime b = ScalarSupport.toZonedOrNull(args[1]);
            if (a == null || b == null) return null;
            LocalDate d1 = a.toLocalDate(), d2 = b.toLocalDate();
            int months = (d1.getYear() - d2.getYear()) * 12 + (d1.getMonthValue() - d2.getMonthValue());
            return months + (d1.getDayOfMonth() - d2.getDayOfMonth()) / 31.0;
        });
        reg("last_day", "当月最后一天", args -> {
            ScalarSupport.requireArity("last_day", args, 1);
            ZonedDateTime z = ScalarSupport.toZonedOrNull(args[0]);
            return z == null ? null : z.toLocalDate().with(TemporalAdjusters.lastDayOfMonth());
        });
        reg("next_day", "下一个星期几 next_day(d,'MO')", args -> {
            ScalarSupport.requireArity("next_day", args, 2);
            ZonedDateTime z = ScalarSupport.toZonedOrNull(args[0]);
            if (z == null || args[1] == null) return null;
            DayOfWeek target = parseDow(ScalarSupport.string(args[1]));
            if (target == null) return null;
            LocalDate d = z.toLocalDate();
            int delta = target.getValue() - d.getDayOfWeek().getValue();
            if (delta <= 0) delta += 7;
            return d.plusDays(delta);
        });
        reg("trunc", "截断到 YEAR/QUARTER/MONTH/WEEK/DAY", args -> {
            ScalarSupport.requireArity("trunc", args, 2);
            ZonedDateTime z = ScalarSupport.toZonedOrNull(args[0]);
            if (z == null || args[1] == null) return null;
            LocalDate d = z.toLocalDate();
            return switch (ScalarSupport.string(args[1]).trim().toUpperCase(Locale.ROOT)) {
                case "YEAR", "YYYY", "YY" -> d.withDayOfYear(1);
                case "QUARTER" -> d.withMonth(((d.getMonthValue() - 1) / 3) * 3 + 1).withDayOfMonth(1);
                case "MONTH", "MM", "MON" -> d.withDayOfMonth(1);
                case "WEEK" -> d.with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY));
                case "DAY", "DD" -> d;
                default -> null;
            };
        });
        reg("date_format", "格式化日期为字符串 date_format(d,'%Y-%m-%d')（亦兼容 'yyyy-MM-dd'）", args -> {
            ScalarSupport.requireArity("date_format", args, 2);
            ZonedDateTime z = ScalarSupport.toZonedOrNull(args[0]);
            if (z == null || args[1] == null) return null;
            return z.toLocalDateTime().format(JavaDateFormat.toFormatter(ScalarSupport.string(args[1])));
        });
        reg("from_utc_timestamp", "UTC 时间戳 → 指定时区墙钟", args -> {
            ScalarSupport.requireArity("from_utc_timestamp", args, 2);
            ZonedDateTime z = ScalarSupport.toZonedOrNull(args[0]);
            if (z == null || args[1] == null) return null;
            ZoneId zone = zoneOf(ScalarSupport.string(args[1]));
            return zone == null ? null : z.withZoneSameInstant(zone).toLocalDateTime();
        });
        reg("to_utc_timestamp", "指定时区墙钟 → UTC 时间戳", args -> {
            ScalarSupport.requireArity("to_utc_timestamp", args, 2);
            ZonedDateTime z = ScalarSupport.toZonedOrNull(args[0]);
            if (z == null || args[1] == null) return null;
            ZoneId zone = zoneOf(ScalarSupport.string(args[1]));
            if (zone == null) return null;
            return z.toLocalDateTime().atZone(zone).withZoneSameInstant(ZoneOffset.UTC).toLocalDateTime();
        });
    }

    private static void reg(String name, String display, Function<Object[], Object> impl) {
        ScalarFunctionRegistry.register(ScalarSupport.fn(name, display, impl));
    }

    /** 单参日期函数：取 toZoned 后套 lambda 提取整型结果（null 传播）。 */
    private static Function<Object[], Object> simple(String name, int arity, Function<ZonedDateTime, Object> extract) {
        return args -> {
            ScalarSupport.requireArity(name, args, arity);
            ZonedDateTime z = ScalarSupport.toZonedOrNull(args[0]);
            return z == null ? null : extract.apply(z);
        };
    }

    private static Object shiftDays(String name, Object[] args, int sign) {
        ScalarSupport.requireArity(name, args, 2);
        ZonedDateTime z = ScalarSupport.toZonedOrNull(args[0]);
        if (z == null || args[1] == null) return null;
        return z.toLocalDate().plusDays(sign * ScalarSupport.asLong(args[1]));
    }

    private static Long parseToEpoch(String s, String fmt) {
        DateTimeFormatter f = JavaDateFormat.toFormatter(fmt);
        try {
            return LocalDateTime.parse(s.trim(), f).toEpochSecond(ZoneOffset.UTC);
        } catch (Exception ignored) { }
        try {
            return LocalDate.parse(s.trim(), f).atStartOfDay().toEpochSecond(ZoneOffset.UTC);
        } catch (Exception ignored) { }
        return null;
    }

    private static DayOfWeek parseDow(String s) {
        String t = s.trim().toUpperCase(Locale.ROOT);
        if (t.startsWith("MO")) return DayOfWeek.MONDAY;
        if (t.startsWith("TU")) return DayOfWeek.TUESDAY;
        if (t.startsWith("WE")) return DayOfWeek.WEDNESDAY;
        if (t.startsWith("TH")) return DayOfWeek.THURSDAY;
        if (t.startsWith("FR")) return DayOfWeek.FRIDAY;
        if (t.startsWith("SA")) return DayOfWeek.SATURDAY;
        if (t.startsWith("SU")) return DayOfWeek.SUNDAY;
        return null;
    }

    private static ZoneId zoneOf(String tz) {
        try {
            return ZoneId.of(tz.trim());
        } catch (Exception e) {
            try {
                return ZoneId.of(tz.trim(), ZoneId.SHORT_IDS);
            } catch (Exception e2) {
                return null;
            }
        }
    }
}
