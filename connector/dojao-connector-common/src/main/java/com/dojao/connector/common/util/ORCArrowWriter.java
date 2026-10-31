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
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Arrow VectorSchemaRoot → ORC 文件的写入器（{@link ORCFileArrowConverter} 的反向）。
 *
 * <p>逐列把 Arrow 向量搬进 ORC 的 {@link VectorizedRowBatch}，不产生行级 Java 对象。
 * NULL 走 ORC 的 {@code isNull} 标记。用于 Hive 连接器把 SELECT 结果落到 Hive 表
 * （CTAS / INSERT INTO / INSERT OVERWRITE）。</p>
 *
 * <p><b>只写数据列</b>：分区列不在 ORC 文件里（值在目录名 {@code k=v} 段），因此本写入器
 * 按调用方给的「源列下标映射」只填充需要落文件的那几列 —— 分区列的下标不传即可。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/29
 */
public class ORCArrowWriter implements ArrowFileWriter {

    private final Writer writer;
    /** ORC 列 → 源（Page）向量下标：ORC 第 i 列取自 {@code root.getVector(sourceIndexes[i])}。 */
    private final int[] sourceIndexes;
    private final TypeDescription orcSchema;
    private boolean closed;

    /**
     * @param dataFields    需要落 ORC 文件的列（顺序 = ORC 文件列序 = Hive 数据列序）
     * @param sourceIndexes 与 {@code dataFields} 一一对应，指向写入页里的列下标
     */
    public ORCArrowWriter(Configuration conf, Path path, FileSystem fs,
                          List<Field> dataFields, int[] sourceIndexes) throws IOException {
        this.sourceIndexes = sourceIndexes.clone();
        this.orcSchema = orcSchemaOf(dataFields);
        OrcFile.WriterOptions opts = OrcFile.writerOptions(conf)
                .fileSystem(fs)
                .setSchema(orcSchema)
                .overwrite(true);
        this.writer = OrcFile.createWriter(path, opts);
    }

    /** 把一页数据写进 ORC（只写映射到的那几列）。 */
    public void write(VectorSchemaRoot root) throws IOException {
        int rows = root.getRowCount();
        if (rows <= 0) return;
        VectorizedRowBatch batch = orcSchema.createRowBatch(rows);
        batch.size = rows;
        for (int col = 0; col < sourceIndexes.length; col++) {
            fillColumn(batch.cols[col], root.getVector(sourceIndexes[col]), rows);
        }
        writer.addRowBatch(batch);
    }

    public TypeDescription orcSchema() { return orcSchema; }

    @Override
    public void close() throws IOException {
        if (closed) return;
        closed = true;
        writer.close();
    }

    // ==================== 类型映射 ====================

    /** Arrow Field 清单 → ORC struct schema。 */
    public static TypeDescription orcSchemaOf(List<Field> dataFields) {
        TypeDescription schema = TypeDescription.createStruct();
        for (Field f : dataFields) {
            schema.addField(f.getName(), orcTypeOf(f));
        }
        return schema;
    }

    /**
     * Arrow 字段 → ORC 类型（与 {@link ORCFileArrowConverter#buildField} 反向对称）。
     *
     * <p>{@code list} / {@code map} / {@code struct} 递归构造真实 ORC 结构，供 CTAS / INSERT 写回
     * 原生嵌套列；子字段名对读回无影响（读侧按位置取 key/value、按名字取 struct 子列）。</p>
     */
    public static TypeDescription orcTypeOf(Field field) {
        ArrowType type = field.getType();
        List<Field> children = field.getChildren();
        if (type instanceof ArrowType.List || type instanceof ArrowType.FixedSizeList) {
            Field elem = children.isEmpty() ? null : children.get(0);
            return elem == null ? TypeDescription.createString()
                    : TypeDescription.createList(orcTypeOf(elem));
        }
        if (type instanceof ArrowType.Map) {
            // Arrow Map 的孩子只有一个 entries(struct<key,value>)
            Field entries = children.isEmpty() ? null : children.get(0);
            List<Field> kv = entries == null ? List.of() : entries.getChildren();
            if (kv.size() < 2) return TypeDescription.createString();
            return TypeDescription.createMap(orcTypeOf(kv.get(0)), orcTypeOf(kv.get(1)));
        }
        if (type instanceof ArrowType.Struct) {
            TypeDescription st = TypeDescription.createStruct();
            for (Field child : children) st.addField(child.getName(), orcTypeOf(child));
            return st;
        }
        return orcTypeOf(type);
    }

    /** Arrow 物理类型 → ORC 标量类型（嵌套类型走 {@link #orcTypeOf(Field)}）。 */
    public static TypeDescription orcTypeOf(ArrowType type) {
        if (type instanceof ArrowType.Bool) return TypeDescription.createBoolean();
        if (type instanceof ArrowType.Int i) {
            return switch (i.getBitWidth()) {
                case 8 -> TypeDescription.createByte();
                case 16 -> TypeDescription.createShort();
                case 32 -> TypeDescription.createInt();
                default -> TypeDescription.createLong();
            };
        }
        if (type instanceof ArrowType.FloatingPoint fp) {
            return fp.getPrecision() == FloatingPointPrecision.SINGLE
                    ? TypeDescription.createFloat() : TypeDescription.createDouble();
        }
        if (type instanceof ArrowType.Decimal d) {
            return TypeDescription.createDecimal()
                    .withPrecision(d.getPrecision()).withScale(d.getScale());
        }
        if (type instanceof ArrowType.Date) return TypeDescription.createDate();
        if (type instanceof ArrowType.Timestamp) return TypeDescription.createTimestamp();
        if (type instanceof ArrowType.Binary) return TypeDescription.createBinary();
        // Utf8 / LargeUtf8 / 其余未知 → 字符串
        return TypeDescription.createString();
    }

    // ==================== 列填充（Arrow → ORC ColumnVector） ====================

    private static void fillColumn(ColumnVector dst, FieldVector src, int rows) {
        fillRange(dst, src, 0, 0, rows);
    }

    /**
     * 把 Arrow 向量的 {@code [srcFrom, srcFrom + len)} 填进 ORC 列的
     * {@code [dstFrom, dstFrom + len)}。顶层两侧都从 0 开始，嵌套时由父列按元素区间递归。
     *
     * <p>ORC 的 {@code ListColumnVector} / {@code MapColumnVector} 与 Arrow 一样是
     * 「父列 offsets/lengths + 子列连续存放」，所以元素下标在两侧可以保持相同的相对顺序。</p>
     */
    private static void fillRange(ColumnVector dst, FieldVector src, int srcFrom, int dstFrom, int len) {
        if (len <= 0) return;
        // 目标列统一按「可能含空」写入（noNulls=false 只是少了点压缩，正确性不受影响）
        dst.noNulls = false;
        int srcCount = src.getValueCount();

        if (dst instanceof ListColumnVector lcv) {
            fillList(lcv, (ListVector) src, srcFrom, dstFrom, len);
        } else if (dst instanceof MapColumnVector mcv) {
            fillMap(mcv, (MapVector) src, srcFrom, dstFrom, len);
        } else if (dst instanceof StructColumnVector scv) {
            fillStruct(scv, (StructVector) src, srcFrom, dstFrom, len);
        } else if (dst instanceof LongColumnVector lcv) {
            for (int i = 0; i < len; i++) {
                int r = dstFrom + i;
                int s = srcFrom + i;
                if (s >= srcCount || src.isNull(s)) { lcv.isNull[r] = true; continue; }
                lcv.isNull[r] = false;
                lcv.vector[r] = longValue(src, s);
            }
        } else if (dst instanceof DoubleColumnVector dcv) {
            for (int i = 0; i < len; i++) {
                int r = dstFrom + i;
                int s = srcFrom + i;
                if (s >= srcCount || src.isNull(s)) { dcv.isNull[r] = true; continue; }
                dcv.isNull[r] = false;
                dcv.vector[r] = doubleValue(src, s);
            }
        } else if (dst instanceof BytesColumnVector bcv) {
            for (int i = 0; i < len; i++) {
                int r = dstFrom + i;
                int s = srcFrom + i;
                if (s >= srcCount || src.isNull(s)) { bcv.isNull[r] = true; continue; }
                bcv.isNull[r] = false;
                byte[] bytes = bytesValue(src, s);
                bcv.setVal(r, bytes, 0, bytes.length);
            }
        } else if (dst instanceof TimestampColumnVector tcv) {
            for (int i = 0; i < len; i++) {
                int r = dstFrom + i;
                int s = srcFrom + i;
                if (s >= srcCount || src.isNull(s)) { tcv.isNull[r] = true; continue; }
                tcv.isNull[r] = false;
                tcv.time[r] = ((TimeStampMilliVector) src).get(s);
                tcv.nanos[r] = 0;
            }
        } else if (dst instanceof DecimalColumnVector decv) {
            for (int i = 0; i < len; i++) {
                int r = dstFrom + i;
                int s = srcFrom + i;
                if (s >= srcCount || src.isNull(s)) { decv.isNull[r] = true; continue; }
                decv.isNull[r] = false;
                BigDecimal v = (BigDecimal) ((DecimalVector) src).getObject(s);
                decv.vector[r].set(HiveDecimal.create(v));
            }
        } else {
            throw new IllegalStateException("ORC 写入器不支持的列向量类型: " + src.getClass().getName());
        }
    }

    /** Arrow List → ORC {@code list}（offsets/lengths + 连续子列）。 */
    private static void fillList(ListColumnVector dst, ListVector src, int srcFrom, int dstFrom, int len) {
        FieldVector data = src.getDataVector();
        int srcCount = src.getValueCount();
        int total = 0;
        for (int i = 0; i < len; i++) {
            int s = srcFrom + i;
            if (s >= srcCount || src.isNull(s)) continue;
            total += src.getElementEndIndex(s) - src.getElementStartIndex(s);
        }
        if (total > 0) dst.child.ensureSize(total, false);

        int dstChild = 0;
        for (int i = 0; i < len; i++) {
            int s = srcFrom + i;
            int r = dstFrom + i;
            boolean isNull = s >= srcCount || src.isNull(s);
            int n = 0;
            if (!isNull) {
                int begin = src.getElementStartIndex(s);
                n = src.getElementEndIndex(s) - begin;
                if (n > 0) fillRange(dst.child, data, begin, dstChild, n);
            }
            dst.offsets[r] = dstChild;
            dst.lengths[r] = n;
            dst.isNull[r] = isNull;
            dstChild += n;
        }
    }

    /** Arrow Map → ORC {@code map}（keys / values 两条连续子列 + 父列 offsets/lengths）。 */
    private static void fillMap(MapColumnVector dst, MapVector src, int srcFrom, int dstFrom, int len) {
        StructVector entries = (StructVector) src.getDataVector();
        List<FieldVector> kv = entries.getChildrenFromFields();
        FieldVector srcKeys = kv.get(0);
        FieldVector srcValues = kv.get(1);
        int srcCount = src.getValueCount();
        int total = 0;
        for (int i = 0; i < len; i++) {
            int s = srcFrom + i;
            if (s >= srcCount || src.isNull(s)) continue;
            total += src.getElementEndIndex(s) - src.getElementStartIndex(s);
        }
        if (total > 0) {
            dst.keys.ensureSize(total, false);
            dst.values.ensureSize(total, false);
        }

        int dstChild = 0;
        for (int i = 0; i < len; i++) {
            int s = srcFrom + i;
            int r = dstFrom + i;
            boolean isNull = s >= srcCount || src.isNull(s);
            int n = 0;
            if (!isNull) {
                int begin = src.getElementStartIndex(s);
                n = src.getElementEndIndex(s) - begin;
                if (n > 0) {
                    fillRange(dst.keys, srcKeys, begin, dstChild, n);
                    fillRange(dst.values, srcValues, begin, dstChild, n);
                }
            }
            dst.offsets[r] = dstChild;
            dst.lengths[r] = n;
            dst.isNull[r] = isNull;
            dstChild += n;
        }
    }

    /** Arrow Struct → ORC {@code struct}（逐子字段按同一行区间填充；父行 NULL 时子列一并置 NULL）。 */
    private static void fillStruct(StructColumnVector dst, StructVector src,
                                   int srcFrom, int dstFrom, int len) {
        List<FieldVector> children = src.getChildrenFromFields();
        int n = Math.min(children.size(), dst.fields.length);
        for (int k = 0; k < n; k++) {
            fillRange(dst.fields[k], children.get(k), srcFrom, dstFrom, len);
        }
        for (int i = 0; i < len; i++) {
            boolean isNull = src.isNull(srcFrom + i);
            dst.isNull[dstFrom + i] = isNull;
            if (!isNull) continue;
            // 父 struct 为 NULL 时子列的值没有意义：显式置 NULL，避免写出上一行的残留值
            for (int k = 0; k < n; k++) dst.fields[k].isNull[dstFrom + i] = true;
        }
    }

    private static long longValue(FieldVector src, int r) {
        if (src instanceof BitVector v)     return v.get(r) != 0 ? 1 : 0;
        if (src instanceof TinyIntVector v) return v.get(r);
        if (src instanceof SmallIntVector v) return v.get(r);
        if (src instanceof IntVector v)     return v.get(r);
        if (src instanceof BigIntVector v)  return v.get(r);
        if (src instanceof DateDayVector v) return v.get(r);
        throw new IllegalStateException("ORC 写入器无法取整型值: " + src.getClass().getName());
    }

    private static double doubleValue(FieldVector src, int r) {
        if (src instanceof Float4Vector v) return v.get(r);
        if (src instanceof Float8Vector v) return v.get(r);
        throw new IllegalStateException("ORC 写入器无法取浮点值: " + src.getClass().getName());
    }

    private static byte[] bytesValue(FieldVector src, int r) {
        if (src instanceof VarCharVector v)   return v.get(r);
        if (src instanceof VarBinaryVector v) return v.get(r);
        throw new IllegalStateException("ORC 写入器无法取字节值: " + src.getClass().getName());
    }

    /** 按 ORC 列序把 Page 列名映射成下标；缺列抛错（宁可报错也不要错列）。 */
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
