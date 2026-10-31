package com.dojao.spi.util;

import com.dojao.spi.bean.DojaoType;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * DojaoType → Arrow 物理类型 兜底映射（读方向缺精确元数据时的宽安全默认）。
 *
 * <p>用途：CTAS 在 Master 侧推导目标表列类型时，源列若走 {@link DojaoType}（连接器
 * getColumnTypes 的 native→Dojao 映射，无精度元数据）需要落成 Arrow 类型。原则是
 * <b>只宽不窄</b>：任何取值都能落进目标列而不截断 —— DECIMAL 无 p/s 信息一律给宽的
 * Decimal(38,10)（窄了会静默截断小数）；精确保真的类型应由连接器覆写
 * {@code DojaoConnector.sourceFields}（MySQL 走 JDBC 元数据，int 位宽 / decimal(p,s) 原样保留）。</p>
 *
 * @author jiaxin
 * @date 2026/9/5
 */
public final class DojaoTypeMapper {

    private DojaoTypeMapper() {
    }

    /** DojaoType → Arrow 类型。未知/复合（缺元数据）→ 文本占位。 */
    public static ArrowType toArrowType(DojaoType type) {
        if (type == null) {
            return ArrowType.Utf8.INSTANCE;
        }
        return switch (type) {
            case BOOLEAN -> ArrowType.Bool.INSTANCE;
            case TINYINT -> new ArrowType.Int(8, true);
            case SMALLINT -> new ArrowType.Int(16, true);
            case INT -> new ArrowType.Int(32, true);
            case BIGINT -> new ArrowType.Int(64, true);
            case FLOAT -> new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case DOUBLE -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            // 未知 p/s → 宽 Decimal(38,10)：任何源精度的值都能存下、不截断（窄默认如 (10,0) 会丢小数）
            case DECIMAL -> new ArrowType.Decimal(38, 10, 128);
            case STRING, JSON -> ArrowType.Utf8.INSTANCE;
            // 复合类型缺元素/维度元数据，无法在此构造合法 Arrow 结构 → 文本占位（保值不崩溃），
            // 需要精确结构的连接器覆写 sourceFields
            case ARRAY, VECTOR, LIST, MAP, STRUCT -> ArrowType.Utf8.INSTANCE;
            case DATE -> new ArrowType.Date(DateUnit.DAY);
            case DATETIME, TIMESTAMP, TIMESTAMP_TZ ->
                    new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
        };
    }

    /** 按列名 + DojaoType 构建可空 Arrow Field。 */
    public static Field toField(String name, DojaoType type) {
        return new Field(name, FieldType.nullable(toArrowType(type)), null);
    }

    /** 按名从既有字段清单取列字段；缺列返回 null。 */
    public static Field findByName(java.util.List<Field> fields, String name) {
        for (Field f : fields) {
            if (f.getName().equalsIgnoreCase(name)) {
                return f;
            }
        }
        return null;
    }

    /** 便捷：把列名 → DojaoType 映射按指定列序展开为 Field 清单。 */
    public static List<Field> fieldsFor(String[] names, Map<String, DojaoType> types) {
        List<Field> out = new java.util.ArrayList<>();
        for (String n : names) {
            out.add(toField(n, types.get(n)));
        }
        return out;
    }
}
