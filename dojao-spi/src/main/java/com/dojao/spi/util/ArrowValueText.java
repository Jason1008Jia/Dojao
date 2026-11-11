package com.dojao.spi.util;

import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

/**
 * Arrow 值 → 展示文本（结果集回传、CLI、JDBC 驱动预览共用）。
 *
 * <p>必须按向量类型格式化，不能统一走 {@link FieldVector#getObject(int)}：
 * Arrow 15 的 {@code DateDayVector.getObject} 返回 Integer（epoch days）、
 * {@code TimeSecVector.getObject} 返回 Integer、{@code TimeMicro/NanoVector} 返回 Long。
 * 直接 {@code String.valueOf} 会把 2025-01-01 显示成 20089，也会让 JDBC 客户端的
 * {@code getDate()}/{@code getTime()} 无法解析（它们按字符串反序列化）。
 *
 * <p>本类只负责「输出文本」；引擎内部仍保留 DATE/TIME/TIMESTAMP 类型，
 * year/date_format 等日期函数不受影响。
 *
 * @author jiaxin
 * @version 1.0
 */
public final class ArrowValueText {

    private ArrowValueText() {
    }

    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    /** 时间戳：日期 + 时间，小数秒按实际精度输出（无小数秒时不补 .000）。 */
    private static final DateTimeFormatter TIMESTAMP = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .toFormatter();

    /** 时间：时分秒 + 可选小数秒。 */
    private static final DateTimeFormatter TIME = new DateTimeFormatterBuilder()
            .appendPattern("HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .toFormatter();

    /** 单元格文本；调用方负责先判空（NULL 显示由上层决定，通常是字面量 "NULL"）。 */
    public static String text(FieldVector vec, int row) {
        if (NestedTypeSupport.isNested(vec)) {
            return NestedTypeSupport.formatValue(vec, row);
        }
        // DATE
        if (vec instanceof DateDayVector d) {
            return DATE.format(LocalDate.ofEpochDay(d.get(row)));
        }
        if (vec instanceof DateMilliVector d) {
            return DATE.format(Instant.ofEpochMilli(d.get(row)).atZone(ZoneOffset.UTC).toLocalDate());
        }
        // TIMESTAMP（含 TZ 变体：raw 值为 epoch 计数，按字段时区还原墙钟）
        if (vec instanceof TimeStampVector ts) {
            return TIMESTAMP.format(timestampOf(ts, row));
        }
        // TIME
        LocalTime time = timeOf(vec, row);
        if (time != null) {
            return TIME.format(time);
        }
        return scalarText(vec.getObject(row));
    }

    /** TIMESTAMP 向量的原始值 → LocalDateTime（非 TZ 变体按 UTC 墙钟，TZ 变体按字段时区）。 */
    public static LocalDateTime timestampOf(TimeStampVector ts, int row) {
        ArrowType.Timestamp type = (ArrowType.Timestamp) ts.getField().getType();
        return toLocalDateTime(ts.get(row), type);
    }

    /** TIME 向量 → LocalTime；非 TIME 类型返回 null。 */
    public static LocalTime timeOf(FieldVector vec, int row) {
        if (vec instanceof TimeSecVector t) {
            return LocalTime.ofSecondOfDay(t.get(row));
        }
        if (vec instanceof TimeMilliVector t) {
            return LocalTime.ofNanoOfDay(Math.multiplyExact((long) t.get(row), 1_000_000L));
        }
        if (vec instanceof TimeMicroVector t) {
            return LocalTime.ofNanoOfDay(Math.multiplyExact(t.get(row), 1_000L));
        }
        if (vec instanceof TimeNanoVector t) {
            return LocalTime.ofNanoOfDay(t.get(row));
        }
        return null;
    }

    /** 非日期时间类的统一文本：浮点/Decimal 展开为十进制，避免科学计数法。 */
    public static String scalarText(Object value) {
        if (value instanceof Double d) {
            return Double.isFinite(d) ? new BigDecimal(d.toString()).toPlainString() : d.toString();
        }
        if (value instanceof Float f) {
            return Float.isFinite(f) ? new BigDecimal(f.toString()).toPlainString() : f.toString();
        }
        if (value instanceof BigDecimal bd) {
            return bd.toPlainString();
        }
        return String.valueOf(value);
    }

    private static LocalDateTime toLocalDateTime(long value, ArrowType.Timestamp type) {
        ZoneId zone = zoneOf(type.getTimezone());
        return switch (type.getUnit()) {
            case SECOND -> Instant.ofEpochSecond(value).atZone(zone).toLocalDateTime();
            case MILLISECOND -> Instant.ofEpochMilli(value).atZone(zone).toLocalDateTime();
            case MICROSECOND -> Instant.ofEpochSecond(
                    Math.floorDiv(value, 1_000_000L),
                    Math.floorMod(value, 1_000_000L) * 1_000L).atZone(zone).toLocalDateTime();
            case NANOSECOND -> Instant.ofEpochSecond(
                    Math.floorDiv(value, 1_000_000_000L),
                    Math.floorMod(value, 1_000_000_000L)).atZone(zone).toLocalDateTime();
        };
    }

    private static ZoneId zoneOf(String timezone) {
        if (timezone == null || timezone.isBlank()) {
            return ZoneOffset.UTC;
        }
        try {
            return ZoneId.of(timezone);
        } catch (Exception ignored) {
            return ZoneOffset.UTC;
        }
    }
}
