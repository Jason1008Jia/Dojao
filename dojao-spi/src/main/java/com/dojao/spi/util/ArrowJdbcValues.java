package com.dojao.spi.util;

import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.util.Text;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;

/**
 * Arrow 行值 → JDBC {@code PreparedStatement.setObject} 可用的 Java 对象。
 *
 * <p>JDBC 系连接器写库时统一走这里，不要直接用 {@link FieldVector#getObject(int)}：
 * Arrow 15 的 {@code DateDayVector.getObject} 返回 Integer（epoch days），
 * {@code TimeSecVector.getObject} 返回 Integer，直接绑定会把 2025-01-01 写成 20089，
 * 目标表为 DATE/TIME 时轻则语义错误，重则插入失败。
 *
 * <p>只转换「值」，不改 Arrow 类型；读取/计算链路仍保持 DATE/TIME/TIMESTAMP 语义。
 *
 * @author jiaxin
 * @version 1.0
 */
public final class ArrowJdbcValues {

    private ArrowJdbcValues() {
    }

    /** 返回 setObject 可直接使用的值；NULL 行返回 null。 */
    public static Object toJdbc(FieldVector vec, int row) {
        if (vec.isNull(row)) {
            return null;
        }
        if (NestedTypeSupport.isNested(vec)) {
            return NestedTypeSupport.toJavaValue(vec, row);
        }
        if (vec instanceof DateDayVector d) {
            return Date.valueOf(LocalDate.ofEpochDay(d.get(row)));
        }
        if (vec instanceof DateMilliVector d) {
            return Date.valueOf(Instant.ofEpochMilli(d.get(row)).atZone(ZoneOffset.UTC).toLocalDate());
        }
        if (vec instanceof TimeStampVector ts) {
            return Timestamp.valueOf(ArrowValueText.timestampOf(ts, row));
        }
        LocalTime time = ArrowValueText.timeOf(vec, row);
        if (time != null) {
            return Time.valueOf(time);
        }
        Object value = vec.getObject(row);
        return value instanceof Text text ? text.toString() : value;
    }
}
