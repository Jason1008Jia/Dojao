package com.dojao.spi.util;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 嵌套类型（LIST / FIXED_SIZE_LIST / MAP / STRUCT）的公共工具。
 *
 * <p>三件事：</p>
 * <ol>
 *   <li><b>识别</b>：{@link #isNested(ArrowType)} / {@link #isNested(Field)} —— 算子拿到列先问一句，
 *       不支持嵌套的路径据此给出可读报错，而不是在更深的地方抛 ClassCastException；</li>
 *   <li><b>渲染</b>：{@link #formatValue(FieldVector, int)} —— 把一行嵌套值转成
 *       {@code [1,2]} / {@code {"a":1}} / {@code {"字段":值}} 的文本（与 Hive 文本口径一致），
 *       供结果 API、日志、错误信息使用；</li>
 *   <li><b>逐行深拷贝</b>：{@link #copyRow(FieldVector, int, FieldVector, int)} —— 统一走 Arrow
 *       自带 transfer pair 的 {@code copyValueSafe}，它能递归处理子向量（offset/data/validity），
 *       避免各算子自己写一套嵌套拷贝逻辑。</li>
 * </ol>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/25
 */
public final class NestedTypeSupport {

    private static final DateTimeFormatter TS_TEXT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private NestedTypeSupport() {
    }

    /** Arrow 类型是否嵌套（LIST / FIXED_SIZE_LIST / MAP / STRUCT）。 */
    public static boolean isNested(ArrowType type) {
        return type instanceof ArrowType.List
                || type instanceof ArrowType.FixedSizeList
                || type instanceof ArrowType.Map
                || type instanceof ArrowType.Struct;
    }

    /** 字段是否嵌套；子字段嵌套时父字段的 type 必然也是复合类型，故只看类型即可。 */
    public static boolean isNested(Field field) {
        return field != null && isNested(field.getType());
    }

    /** 向量是否嵌套。 */
    public static boolean isNested(FieldVector vector) {
        return vector != null && isNested(vector.getField());
    }

    /**
     * 能力闸门：该列若为嵌套类型，抛带上下文的可读异常。
     *
     * <p>嵌套列当前只支持「整列读取 / 投影透传 / 落盘写入」，不能参与分组、排序、连接、
     * 窗口、表达式与聚合 —— 那些路径没有定义嵌套语义（NULL 传播、字典序、相等性），
     * 与其静默算出可疑结果，不如在入口显式拒绝。</p>
     */
    public static void requireSupported(Field field, String context) {
        if (field == null || !isNested(field)) return;
        throw new UnsupportedOperationException(context + " 暂不支持嵌套类型列 '"
                + field.getName() + "'（" + typeText(field) + "）；嵌套列目前只能整列读取、投影透传或写入");
    }

    /** 批量版 {@link #requireSupported(Field, String)}；null 元素跳过。 */
    public static void requireSupported(List<Field> fields, String context) {
        if (fields == null) return;
        for (Field f : fields) requireSupported(f, context);
    }

    /**
     * 字段的可读类型文本（{@code list<bigint>} / {@code map<string,int>} /
     * {@code struct<a:int>}），用于报错与 EXPLAIN。
     */
    public static String typeText(Field field) {
        if (field == null) return "unknown";
        ArrowType type = field.getType();
        List<Field> children = field.getChildren();
        if (type instanceof ArrowType.List) {
            return "list<" + typeText(child(children, 0)) + ">";
        }
        if (type instanceof ArrowType.FixedSizeList f) {
            return "fixed_size_list<" + typeText(child(children, 0)) + ">[" + f.getListSize() + "]";
        }
        if (type instanceof ArrowType.Map) {
            return "map<" + typeText(child(children, 0)) + "," + typeText(child(children, 1)) + ">";
        }
        if (type instanceof ArrowType.Struct) {
            StringBuilder sb = new StringBuilder("struct<");
            for (int i = 0; i < children.size(); i++) {
                if (i > 0) sb.append(',');
                sb.append(children.get(i).getName()).append(':').append(typeText(children.get(i)));
            }
            return sb.append('>').toString();
        }
        return scalarTypeText(type);
    }

    /**
     * 把第 {@code row} 行的嵌套值渲染成文本；NULL 返回 {@code "null"}。
     *
     * <p>容器内字符串加双引号并转义（{@code ["a,b"]}），避免与分隔符混淆；容器内 NULL
     * 记为 {@code null}（与 Hive 文本口径一致）。</p>
     */
    public static String formatValue(FieldVector vector, int row) {
        if (vector == null || row < 0) return "null";
        if (row >= vector.getValueCount() || vector.isNull(row)) return "null";
        return formatNonNull(vector, row);
    }

    /**
     * 嵌套值 → Java 容器：LIST/FIXED_SIZE_LIST → {@code Object[]}，MAP/STRUCT →
     * {@link LinkedHashMap}，标量取 {@code getObject}。
     *
     * <p>JDBC 写回（PG 数组、ClickHouse Array/Map）需要「驱动认识的 Java 形态」，
     * 而 Arrow 的嵌套向量没有 {@code getObject} 可用的通用容器，故统一在这里递归转换。</p>
     */
    public static Object toJavaValue(FieldVector vector, int row) {
        if (vector == null || row < 0 || row >= vector.getValueCount() || vector.isNull(row)) {
            return null;
        }
        // MapVector 是 ListVector 的子类，必须先判 Map
        if (vector instanceof MapVector map) {
            List<FieldVector> entries = ((StructVector) map.getDataVector()).getChildrenFromFields();
            FieldVector keys = entries.get(0);
            FieldVector values = entries.get(1);
            Map<Object, Object> out = new LinkedHashMap<>();
            int start = map.getElementStartIndex(row);
            int end = map.getElementEndIndex(row);
            for (int i = start; i < end; i++) {
                out.put(toJavaValue(keys, i), toJavaValue(values, i));
            }
            return out;
        }
        if (vector instanceof ListVector list) {
            return sliceToArray(list, list.getElementStartIndex(row), list.getElementEndIndex(row));
        }
        if (vector instanceof FixedSizeListVector list) {
            return sliceToArray(list, list.getElementStartIndex(row), list.getElementEndIndex(row));
        }
        if (vector instanceof StructVector struct) {
            List<FieldVector> kids = struct.getChildrenFromFields();
            List<Field> fields = struct.getField().getChildren();
            Map<String, Object> out = new LinkedHashMap<>();
            for (int i = 0; i < kids.size(); i++) {
                String name = i < fields.size() ? fields.get(i).getName() : "c" + i;
                out.put(name, toJavaValue(kids.get(i), row));
            }
            return out;
        }
        Object value = vector.getObject(row);
        // Arrow 的字符串 getObject 是 Text，JDBC 驱动不认识 → 统一转 String
        return value instanceof org.apache.arrow.vector.util.Text t ? t.toString() : value;
    }

    private static Object[] sliceToArray(FieldVector list, int start, int end) {
        FieldVector child = childVector(list);
        Object[] out = new Object[end - start];
        for (int i = start; i < end; i++) {
            out[i - start] = toJavaValue(child, i);
        }
        return out;
    }

    /**
     * 逐行深拷贝（含 NULL）。
     *
     * <p>Arrow 的 {@code copyValueSafe} 对嵌套向量是递归的：List 复制 offset/长度并拷贝子区间，
     * Struct 复制 validity 与全部子字段。调用方只需保证两侧 valueCount 已按行数设置。</p>
     */
    public static void copyRow(FieldVector src, int srcRow, FieldVector dst, int dstRow) {
        if (src == null || dst == null) return;
        if (src.isNull(srcRow)) {
            dst.setNull(dstRow);
            return;
        }
        dst.copyFromSafe(srcRow, dstRow, src);
    }

    // ==================== 渲染实现 ====================

    private static String formatNonNull(FieldVector vector, int row) {
        if (vector instanceof MapVector map) {
            // MapVector 的数据向量是 {key, value} 的非空 struct；key/value 逐行一一对应
            List<FieldVector> entries = ((StructVector) map.getDataVector()).getChildrenFromFields();
            FieldVector keys = entries.get(0);
            FieldVector values = entries.get(1);
            int start = map.getElementStartIndex(row);
            int end = map.getElementEndIndex(row);
            StringBuilder sb = new StringBuilder("{");
            for (int i = start; i < end; i++) {
                if (i > start) sb.append(',');
                sb.append(formatValue(keys, i)).append(':').append(formatValue(values, i));
            }
            return sb.append('}').toString();
        }
        if (vector instanceof ListVector list) {
            return renderRepeated(list, list.getElementStartIndex(row), list.getElementEndIndex(row));
        }
        if (vector instanceof FixedSizeListVector list) {
            return renderRepeated(list, list.getElementStartIndex(row), list.getElementEndIndex(row));
        }
        if (vector instanceof StructVector struct) {
            List<FieldVector> kids = struct.getChildrenFromFields();
            List<Field> fields = vector.getField().getChildren();
            StringBuilder sb = new StringBuilder("{");
            for (int i = 0; i < kids.size(); i++) {
                if (i > 0) sb.append(',');
                String name = i < fields.size() ? fields.get(i).getName() : "c" + i;
                sb.append(quote(name)).append(':').append(formatValue(kids.get(i), row));
            }
            return sb.append('}').toString();
        }
        return formatScalar(vector, row);
    }

    private static String renderRepeated(FieldVector list, int start, int end) {
        FieldVector child = childVector(list);
        StringBuilder sb = new StringBuilder("[");
        for (int i = start; i < end; i++) {
            if (i > start) sb.append(',');
            sb.append(formatValue(child, i));
        }
        return sb.append(']').toString();
    }

    private static FieldVector childVector(FieldVector list) {
        if (list instanceof ListVector l) return l.getDataVector();
        if (list instanceof FixedSizeListVector f) return f.getDataVector();
        throw new IllegalArgumentException("不是列表向量: " + list.getClass().getName());
    }

    private static String formatScalar(FieldVector v, int row) {
        if (v instanceof BitVector x) return x.get(row) != 0 ? "true" : "false";
        if (v instanceof TinyIntVector x) return Long.toString(x.get(row));
        if (v instanceof SmallIntVector x) return Long.toString(x.get(row));
        if (v instanceof IntVector x) return Long.toString(x.get(row));
        if (v instanceof BigIntVector x) return Long.toString(x.get(row));
        if (v instanceof Float4Vector x) return Float.toString(x.get(row));
        if (v instanceof Float8Vector x) return Double.toString(x.get(row));
        if (v instanceof DecimalVector x) {
            Object o = x.getObject(row);
            return o instanceof BigDecimal bd ? bd.toPlainString() : String.valueOf(o);
        }
        if (v instanceof VarCharVector x) {
            return quote(new String(x.get(row), StandardCharsets.UTF_8));
        }
        if (v instanceof VarBinaryVector x) {
            return new String(x.get(row), StandardCharsets.UTF_8);
        }
        if (v instanceof DateDayVector x) return LocalDate.ofEpochDay(x.get(row)).toString();
        if (v instanceof TimeStampVector x) {
            Object o = x.getObject(row);
            return o instanceof LocalDateTime ldt ? TS_TEXT.format(ldt) : String.valueOf(o);
        }
        Object o = v.getObject(row);
        return o == null ? "null" : o.toString();
    }

    private static Field child(List<Field> children, int index) {
        return children != null && index < children.size() ? children.get(index) : null;
    }

    private static String scalarTypeText(ArrowType type) {
        if (type instanceof ArrowType.Bool) return "boolean";
        if (type instanceof ArrowType.Int i) {
            return switch (i.getBitWidth()) {
                case 8 -> "tinyint";
                case 16 -> "smallint";
                case 32 -> "int";
                default -> "bigint";
            };
        }
        if (type instanceof ArrowType.FloatingPoint fp) {
            return fp.getPrecision() == FloatingPointPrecision.SINGLE ? "float" : "double";
        }
        if (type instanceof ArrowType.Decimal d) {
            return "decimal(" + d.getPrecision() + "," + d.getScale() + ")";
        }
        if (type instanceof ArrowType.Date) return "date";
        if (type instanceof ArrowType.Timestamp) return "timestamp";
        if (type instanceof ArrowType.Binary) return "binary";
        if (type instanceof ArrowType.Utf8 || type instanceof ArrowType.LargeUtf8) return "string";
        return type.toString();
    }

    /** 复合值里的字符串加引号并转义，避免与分隔符 / 结构括号混淆。 */
    private static String quote(String s) {
        StringBuilder sb = new StringBuilder(s.length() + 2).append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' || c == '"') sb.append('\\');
            sb.append(c);
        }
        return sb.append('"').toString();
    }
}
