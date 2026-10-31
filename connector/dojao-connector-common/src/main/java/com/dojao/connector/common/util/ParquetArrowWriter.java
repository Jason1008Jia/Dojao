package com.dojao.connector.common.util;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

/**
 * Arrow VectorSchemaRoot → Parquet 文件的写入器（{@link ParquetFileArrowConverter} 的反向）。
 *
 * <p>逐行构造 Parquet {@code SimpleGroup} 交给 {@code ExampleParquetWriter}，NULL 直接不写该字段
 * （Parquet 的 optional 语义）。用于 Hive 连接器把 SELECT 结果落到 Parquet 表
 * （CTAS / INSERT INTO / INSERT OVERWRITE）。</p>
 *
 * <p><b>类型映射与 Hive 对齐</b>：{@code tinyint/smallint} → 带 {@code INT_8/INT_16} 注解的 INT32，
 * {@code decimal(p,s)} 按精度选 INT32 / INT64 / FIXED_LEN_BYTE_ARRAY（与 Hive Parquet 写法一致），
 * {@code date} → 带 DATE 注解的 INT32，{@code timestamp} → 带 TIMESTAMP(MILLIS) 注解的 INT64，
 * 字符串 → 带 STRING 注解的 BINARY。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/25
 */
public class ParquetArrowWriter implements ArrowFileWriter {

    private final ParquetWriter<Group> writer;
    private final SimpleGroupFactory factory;
    /** Parquet 列 → 源（Page）向量下标。 */
    private final int[] sourceIndexes;
    private boolean closed;

    /**
     * @param dataFields    需要落 Parquet 文件的列（顺序 = 文件列序 = Hive 数据列序）
     * @param sourceIndexes 与 {@code dataFields} 一一对应，指向写入页里的列下标
     */
    public ParquetArrowWriter(Configuration conf, Path path, FileSystem fs,
                              List<Field> dataFields, int[] sourceIndexes) throws IOException {
        this.sourceIndexes = sourceIndexes.clone();
        MessageType schema = parquetSchemaOf(dataFields);
        this.factory = new SimpleGroupFactory(schema);
        this.writer = ExampleParquetWriter
                .builder(HadoopOutputFile.fromPath(path, conf))
                .withConf(conf)
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build();
    }

    @Override
    public void write(VectorSchemaRoot root) throws IOException {
        int rows = root.getRowCount();
        for (int r = 0; r < rows; r++) {
            Group group = factory.newGroup();
            for (int col = 0; col < sourceIndexes.length; col++) {
                FieldVector src = root.getVector(sourceIndexes[col]);
                if (r >= src.getValueCount() || src.isNull(r)) continue;
                fillField(group, col, src, r);
            }
            writer.write(group);
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) return;
        closed = true;
        writer.close();
    }

    // ==================== 类型映射 ====================

    /** Arrow Field 清单 → Parquet MessageType（根名沿用 Hive 惯用的 {@code hive_schema}）。 */
    public static MessageType parquetSchemaOf(List<Field> dataFields) {
        Types.MessageTypeBuilder builder = Types.buildMessage();
        for (Field f : dataFields) {
            builder.addField(parquetFieldOf(f));
        }
        return builder.named("hive_schema");
    }

    /**
     * Arrow 字段 → Parquet 字段（递归，与 {@link ParquetFileArrowConverter#buildField} 对称）。
     *
     * <p>嵌套类型按 Parquet 标准结构写出 —— {@code LIST} 用 3-level
     * （{@code optional group x (LIST) { repeated group list { optional <elem> element } }}）、
     * {@code MAP} 用 {@code key_value} 组（{@code key} 为 required）、{@code STRUCT} 写成
     * optional group。这样 Hive / Spark / Trino 都能按原生嵌套列读回。</p>
     */
    public static Type parquetFieldOf(Field field) {
        return parquetFieldOf(field.getName(), field);
    }

    private static Type parquetFieldOf(String name, Field field) {
        ArrowType type = field.getType();
        List<Field> children = field.getChildren();
        if (type instanceof ArrowType.List || type instanceof ArrowType.FixedSizeList) {
            Field elem = children.isEmpty() ? null : children.get(0);
            Type element = elem == null
                    ? Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                            .as(LogicalTypeAnnotation.stringType()).named("element")
                    : parquetFieldOf("element", elem);
            // 3-level LIST：外层的 repeated group 只是包装，元素为 optional（可为 NULL）
            return Types.optionalGroup()
                    .as(LogicalTypeAnnotation.listType())
                    .addField(Types.repeatedGroup().addField(element).named("list"))
                    .named(name);
        }
        if (type instanceof ArrowType.Map) {
            Field entries = children.isEmpty() ? null : children.get(0);
            List<Field> kv = entries == null ? List.of() : entries.getChildren();
            Type key = kv.isEmpty()
                    ? Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                            .as(LogicalTypeAnnotation.stringType()).named("key")
                    : requiredParquetFieldOf("key", kv.get(0));
            Type value = kv.size() < 2
                    ? Types.optional(PrimitiveType.PrimitiveTypeName.INT32).named("value")
                    : parquetFieldOf("value", kv.get(1));
            return Types.optionalGroup()
                    .as(LogicalTypeAnnotation.mapType())
                    .addField(Types.repeatedGroup().addField(key).addField(value).named("key_value"))
                    .named(name);
        }
        if (type instanceof ArrowType.Struct) {
            Types.GroupBuilder<GroupType> group = Types.optionalGroup();
            for (Field child : children) group.addField(parquetFieldOf(child));
            return group.named(name);
        }
        return scalarParquetField(name, type, false);
    }

    /** MAP 的键是 required —— 单独构造一份（不能复用 optional 的标量构造）。 */
    private static Type requiredParquetFieldOf(String name, Field field) {
        ArrowType type = field.getType();
        if (type instanceof ArrowType.Struct) {
            Types.GroupBuilder<GroupType> group = Types.requiredGroup();
            for (Field child : field.getChildren()) group.addField(parquetFieldOf(child));
            return group.named(name);
        }
        if (type instanceof ArrowType.List || type instanceof ArrowType.Map) {
            // Parquet 规范不允许 MAP 的键是复合类型 → 字符串兜底（宁可类型变宽，也不静默丢列）
            return Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                    .as(LogicalTypeAnnotation.stringType()).named(name);
        }
        return scalarParquetField(name, type, true);
    }

    /** Arrow 物理类型 → Parquet 标量字段（{@code required} 只用于 MAP 键）。 */
    private static Type scalarParquetField(String name, ArrowType type, boolean required) {
        if (type instanceof ArrowType.Bool) {
            return primitive(required, PrimitiveType.PrimitiveTypeName.BOOLEAN).named(name);
        }
        if (type instanceof ArrowType.Int i) {
            return switch (i.getBitWidth()) {
                case 8 -> primitive(required, PrimitiveType.PrimitiveTypeName.INT32)
                        .as(LogicalTypeAnnotation.intType(8, true)).named(name);
                case 16 -> primitive(required, PrimitiveType.PrimitiveTypeName.INT32)
                        .as(LogicalTypeAnnotation.intType(16, true)).named(name);
                case 32 -> primitive(required, PrimitiveType.PrimitiveTypeName.INT32).named(name);
                default -> primitive(required, PrimitiveType.PrimitiveTypeName.INT64).named(name);
            };
        }
        if (type instanceof ArrowType.FloatingPoint fp) {
            return fp.getPrecision() == FloatingPointPrecision.SINGLE
                    ? primitive(required, PrimitiveType.PrimitiveTypeName.FLOAT).named(name)
                    : primitive(required, PrimitiveType.PrimitiveTypeName.DOUBLE).named(name);
        }
        if (type instanceof ArrowType.Decimal d) {
            int precision = Math.min(Math.max(d.getPrecision(), 1), 38);
            int scale = Math.min(Math.max(d.getScale(), 0), precision);
            LogicalTypeAnnotation decimal = LogicalTypeAnnotation.decimalType(scale, precision);
            if (precision <= 9) {
                return primitive(required, PrimitiveType.PrimitiveTypeName.INT32).as(decimal).named(name);
            }
            if (precision <= 18) {
                return primitive(required, PrimitiveType.PrimitiveTypeName.INT64).as(decimal).named(name);
            }
            return primitive(required, PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                    .as(decimal).length(decimalByteLength(precision)).named(name);
        }
        if (type instanceof ArrowType.Date) {
            return primitive(required, PrimitiveType.PrimitiveTypeName.INT32)
                    .as(LogicalTypeAnnotation.dateType()).named(name);
        }
        if (type instanceof ArrowType.Timestamp) {
            return primitive(required, PrimitiveType.PrimitiveTypeName.INT64)
                    .as(LogicalTypeAnnotation.timestampType(false,
                            LogicalTypeAnnotation.TimeUnit.MILLIS)).named(name);
        }
        if (type instanceof ArrowType.Binary) {
            return primitive(required, PrimitiveType.PrimitiveTypeName.BINARY).named(name);
        }
        // Utf8 / 其余未知 → 带 STRING 注解的 BINARY
        return primitive(required, PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType()).named(name);
    }

    private static Types.PrimitiveBuilder<PrimitiveType> primitive(
            boolean required, PrimitiveType.PrimitiveTypeName name) {
        return required ? Types.required(name) : Types.optional(name);
    }

    /** Arrow 物理类型 → optional Parquet 字段（标量专用；嵌套类型走 {@link #parquetFieldOf(Field)}）。 */
    public static Type parquetTypeOf(String name, ArrowType type) {
        return scalarParquetField(name, type, false);
    }

    /** 存放 {@code precision} 位十进制整数所需的最少字节数（含符号位）。 */
    static int decimalByteLength(int precision) {
        int bits = BigInteger.TEN.pow(precision).subtract(BigInteger.ONE).bitLength() + 1;
        return (bits + 7) / 8;
    }

    // ==================== 列填充（Arrow → Parquet Group） ====================

    /** 按源向量类型分派：嵌套结构递归成 Parquet group，标量直写。 */
    private static void fillField(Group group, int col, FieldVector src, int row) {
        // MapVector 是 ListVector 的子类，必须先判 Map 再判 List
        if (src instanceof MapVector v) {
            fillMap(group, col, v, row);
        } else if (src instanceof ListVector v) {
            fillList(group, col, v, row);
        } else if (src instanceof StructVector v) {
            fillStruct(group, col, v, row);
        } else {
            fillValue(group, col, src, row);
        }
    }

    /**
     * Arrow List → Parquet 3-level LIST：外层 group 只包一个 repeated {@code list}，
     * 每个元素再包一层 {@code element}。NULL 元素写成「空的 element 组」——
     * 读侧按 {@code repetition == 0} 还原成 NULL，与空数组（LIST 组存在但 0 个元素）区分开。
     */
    private static void fillList(Group parent, int col, ListVector src, int row) {
        int begin = src.getElementStartIndex(row);
        int end = src.getElementEndIndex(row);
        FieldVector data = src.getDataVector();
        Group listGroup = parent.addGroup(col);
        for (int i = begin; i < end; i++) {
            Group element = listGroup.addGroup(0);
            if (data.isNull(i)) continue;
            fillField(element, 0, data, i);
        }
    }

    /** Arrow Map → Parquet MAP：每个键值对是一个 repeated {@code key_value} 组（key 为 required）。 */
    private static void fillMap(Group parent, int col, MapVector src, int row) {
        int begin = src.getElementStartIndex(row);
        int end = src.getElementEndIndex(row);
        StructVector entries = (StructVector) src.getDataVector();
        List<FieldVector> kv = entries.getChildrenFromFields();
        FieldVector keys = kv.get(0);
        FieldVector values = kv.get(1);
        Group mapGroup = parent.addGroup(col);
        for (int i = begin; i < end; i++) {
            Group entry = mapGroup.addGroup(0);
            // key 是 required：即便源标了 NULL 也必须写个值，否则 Parquet 写不出来
            fillField(entry, 0, keys, i);
            if (!values.isNull(i)) fillField(entry, 1, values, i);
        }
    }

    /** Arrow Struct → Parquet optional group；子字段为 NULL 时不写（Parquet optional 语义）。 */
    private static void fillStruct(Group parent, int col, StructVector src, int row) {
        Group structGroup = parent.addGroup(col);
        List<FieldVector> children = src.getChildrenFromFields();
        for (int k = 0; k < children.size(); k++) {
            FieldVector child = children.get(k);
            if (child.isNull(row)) continue;
            fillField(structGroup, k, child, row);
        }
    }

    private static void fillValue(Group group, int col, FieldVector src, int row) {
        if (src instanceof BitVector v) {
            group.add(col, v.get(row) != 0);
        } else if (src instanceof TinyIntVector v) {
            group.add(col, (int) v.get(row));
        } else if (src instanceof SmallIntVector v) {
            group.add(col, (int) v.get(row));
        } else if (src instanceof IntVector v) {
            group.add(col, v.get(row));
        } else if (src instanceof BigIntVector v) {
            group.add(col, v.get(row));
        } else if (src instanceof Float4Vector v) {
            group.add(col, v.get(row));
        } else if (src instanceof Float8Vector v) {
            group.add(col, v.get(row));
        } else if (src instanceof DateDayVector v) {
            group.add(col, v.get(row));
        } else if (src instanceof TimeStampMilliVector v) {
            group.add(col, v.get(row));
        } else if (src instanceof DecimalVector v) {
            addDecimal(group, col, v, row);
        } else if (src instanceof VarCharVector v) {
            group.add(col, Binary.fromConstantByteArray(v.get(row)));
        } else if (src instanceof VarBinaryVector v) {
            group.add(col, Binary.fromConstantByteArray(v.get(row)));
        } else {
            throw new IllegalStateException("Parquet 写入器不支持的列向量类型: " + src.getClass().getName());
        }
    }

    /** 按目标 Parquet 物理类型写十进制值（未缩放整数的字节序为大端补码）。 */
    private static void addDecimal(Group group, int col, DecimalVector src, int row) {
        BigDecimal value = (BigDecimal) src.getObject(row);
        ArrowType.Decimal decimal = (ArrowType.Decimal) src.getField().getType();
        PrimitiveType primitive = group.getType().getType(col).asPrimitiveType();
        BigInteger unscaled = value.setScale(decimal.getScale(), RoundingMode.HALF_UP).unscaledValue();
        switch (primitive.getPrimitiveTypeName()) {
            case INT32 -> group.add(col, unscaled.intValueExact());
            case INT64 -> group.add(col, unscaled.longValueExact());
            default -> group.add(col, Binary.fromConstantByteArray(
                    padSign(unscaled.toByteArray(), primitive.getTypeLength())));
        }
    }

    /** 大端补码按目标长度做符号扩展（正值补 0x00、负值补 0xFF）。 */
    private static byte[] padSign(byte[] bytes, int length) {
        if (bytes.length == length) return bytes;
        if (bytes.length > length) {
            throw new IllegalArgumentException("十进制值超出 Parquet FIXED_LEN_BYTE_ARRAY 长度: "
                    + bytes.length + " > " + length);
        }
        byte fill = (bytes.length > 0 && bytes[0] < 0) ? (byte) 0xFF : 0;
        byte[] out = new byte[length];
        int pad = length - bytes.length;
        for (int i = 0; i < pad; i++) out[i] = fill;
        System.arraycopy(bytes, 0, out, pad, bytes.length);
        return out;
    }

    /** 按 Parquet 列序把 Page 列名映射成下标；缺列抛错（宁可报错也不要错列）。 */
    public static int[] sourceIndexes(VectorSchemaRoot root, List<String> dataColumnNames) {
        List<String> names = new ArrayList<>();
        for (FieldVector v : root.getFieldVectors()) names.add(v.getField().getName());
        int[] out = new int[dataColumnNames.size()];
        for (int i = 0; i < dataColumnNames.size(); i++) {
            int idx = indexOfIgnoreCase(names, dataColumnNames.get(i));
            if (idx < 0) {
                throw new IllegalArgumentException("写入页缺少 Hive 数据列: " + dataColumnNames.get(i)
                        + "（页列: " + names + "）");
            }
            out[i] = idx;
        }
        return out;
    }

    private static int indexOfIgnoreCase(List<String> names, String wanted) {
        for (int i = 0; i < names.size(); i++) {
            if (names.get(i).equalsIgnoreCase(wanted)) return i;
        }
        return -1;
    }
}
