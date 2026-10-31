package com.dojao.connector.common.util;

import com.dojao.common.data.Page;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.EnumLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.JsonLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Parquet File → Arrow 批量转换器（与 {@link ORCFileArrowConverter} 对称的 API）。
 *
 * <p>逐行读 Parquet {@code SimpleGroup} 后直填 Arrow 向量；Parquet 的 optional 字段以
 * {@code getFieldRepetitionCount(i) == 0} 表达 NULL。批量大小由调用方给定，每批产出一个
 * {@link Page}，读完返回 {@link Page#eof()}。</p>
 *
 * <p><b>按 row group 读，不是整文件流读</b>：走低层 {@link ParquetFileReader#readRowGroup(int)}
 * 拿 {@link PageReadStore}，再交给 {@link ColumnIOFactory} + {@link GroupRecordConverter} 逐行产出
 * {@code Group}。这样才可能只读<b>指定的 row group 子集</b>（{@code startOffset / length} 的字节区间），
 * 大文件因此可以在文件内切分并行读；整文件读不过是「选中全部 row group」的特例。</p>
 *
 * <p><b>嵌套类型（GROUP / LIST / MAP / repeated）</b>有两种读出模式，由 {@code nestedNative} 决定：</p>
 * <ul>
 *   <li>{@code true}（默认）→ 读成 Arrow List / Map / Struct，可原样 CTAS / INSERT 写回；</li>
 *   <li>{@code false} → 渲染成文本（{@code array} → {@code [...]}、{@code map} → {@code {k:v}}、
 *       {@code struct} → {@code {"字段":值}}），与 ORC 读侧同一口径。</li>
 * </ul>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/19
 */
public class ParquetFileArrowConverter implements AutoCloseable {

    private final BufferAllocator allocator;
    private final int batchSize;
    private final Schema arrowSchema;
    private final ParquetFileReader fileReader;
    private final MessageColumnIO columnIO;
    private final MessageType parquetSchema;
    /** 嵌套列是否读成结构化 Arrow（List/Map/Struct）；{@code false} = 一律读成文本。 */
    private final boolean nestedNative;
    /** 本次要读的 row group 下标（升序），由字节区间选出；整文件时是全部。 */
    private final List<Integer> rowGroups;
    private int nextRowGroup;
    /** 当前 row group 尚未读出的行数。 */
    private long remainingRows;
    private RecordReader<Group> rowReader;
    private boolean exhausted;

    public ParquetFileArrowConverter(BufferAllocator allocator, Path path, int batchSize)
            throws IOException {
        this(allocator, path, new Configuration(), batchSize, 0L, -1L);
    }

    public ParquetFileArrowConverter(BufferAllocator allocator, Path path, FileSystem fs,
                                     int batchSize) throws IOException {
        this(allocator, path, fs.getConf(), batchSize, 0L, -1L);
    }

    public ParquetFileArrowConverter(BufferAllocator allocator, Path path, FileSystem fs,
                                     int batchSize, long start, long length) throws IOException {
        this(allocator, path, fs.getConf(), batchSize, start, length);
    }

    /** 带列投影的 FileSystem 构造（Hive 连接器用的就是这个）。 */
    public ParquetFileArrowConverter(BufferAllocator allocator, Path path, FileSystem fs,
                                     int batchSize, long start, long length,
                                     List<String> projection) throws IOException {
        this(allocator, path, fs, batchSize, start, length, projection, true);
    }

    /**
     * 带列投影 + <b>嵌套类型模式</b>的构造（Hive 连接器用的就是这个）。
     *
     * @param nestedNative {@code true} = 嵌套列读成 Arrow List/Map/Struct；
     *                     {@code false} = 渲染成文本
     */
    public ParquetFileArrowConverter(BufferAllocator allocator, Path path, FileSystem fs,
                                     int batchSize, long start, long length,
                                     List<String> projection, boolean nestedNative) throws IOException {
        this(allocator, path, fs.getConf(), batchSize, start, length, projection, nestedNative);
    }

    /**
     * 带<b>字节区间</b>的构造 —— 只读起始偏移落在 {@code [start, start + length)} 内的 row group。
     *
     * <p>区间由调用方（Hive 连接器）按 row group 边界给出：{@code start} = 第一段的起始偏移、
     * {@code start + length} = 「下一段起始偏移」（末段取文件长度）。因此取值是精确的 ——
     * <b>不依赖 {@code getCompressedSize()} 这类估算</b>，不会多读也不会漏读。</p>
     *
     * <p>区间选不中任何 row group 属于调用方 bug（载荷与 footer 对不上），直接报错而非静默返回 0 行。</p>
     */
    public ParquetFileArrowConverter(BufferAllocator allocator, Path path,
                                     Configuration conf, int batchSize,
                                     long start, long length) throws IOException {
        this(allocator, path, conf, batchSize, start, length, List.of());
    }

    /**
     * 带<b>列投影</b>的构造 —— {@code projection} 非空时只读这些列（列名大小写不敏感），
     * 走 {@link ParquetFileReader#setRequestedSchema(MessageType)} 让 Parquet 读取层跳过其余列，
     * <b>不是</b>读完再裁。空 = 全列。
     *
     * <p>投影 schema 一变，{@code ColumnIO} / {@code GroupRecordConverter} / Arrow schema 必须
     * <b>全部</b>换成它 —— 三者与 {@code fillRow} 里按下标取的
     * {@code group.getFieldRepetitionCount(col)} 是一套位置口径，混用即错列。</p>
     *
     * <p>投影里文件一个都不存在的列 → 退回全 schema（宁可多读也不构造 0 列 schema），
     * 由上层按名补 NULL。</p>
     */
    public ParquetFileArrowConverter(BufferAllocator allocator, Path path,
                                     Configuration conf, int batchSize,
                                     long start, long length, List<String> projection) throws IOException {
        this(allocator, path, conf, batchSize, start, length, projection, true);
    }

    /** 最底层构造：列投影 + 嵌套类型模式。 */
    public ParquetFileArrowConverter(BufferAllocator allocator, Path path,
                                     Configuration conf, int batchSize,
                                     long start, long length, List<String> projection,
                                     boolean nestedNative) throws IOException {
        this.allocator = allocator;
        this.batchSize = batchSize;
        this.nestedNative = nestedNative;
        this.fileReader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf));
        this.parquetSchema = project(fileReader.getFooter().getFileMetaData().getSchema(), projection);
        this.fileReader.setRequestedSchema(parquetSchema);
        this.arrowSchema = buildArrowSchema(parquetSchema, nestedNative);
        this.columnIO = new ColumnIOFactory().getColumnIO(parquetSchema);
        this.rowGroups = selectRowGroups(fileReader.getRowGroups(), start, length);
    }

    /** 文件 schema → 投影 schema（按文件序保留命中的列）；无命中则退回全 schema。 */
    private static MessageType project(MessageType fileSchema, List<String> projection) {
        if (projection == null || projection.isEmpty()) return fileSchema;
        List<Type> kept = new ArrayList<>();
        for (Type t : fileSchema.getFields()) {
            if (containsIgnoreCase(projection, t.getName())) kept.add(t);
        }
        if (kept.isEmpty()) return fileSchema;
        return new MessageType(fileSchema.getName(), kept);
    }

    private static boolean containsIgnoreCase(List<String> names, String wanted) {
        for (String n : names) {
            if (n.equalsIgnoreCase(wanted)) return true;
        }
        return false;
    }

    /**
     * 选出本单元要读的 row group 下标：{@code length <= 0} 取全部；否则取
     * {@code startingPos ∈ [start, start + length)}。空结果直接报错（宁可炸也不要静默 0 行）。
     */
    private static List<Integer> selectRowGroups(List<BlockMetaData> blocks, long start, long length) {
        List<Integer> out = new ArrayList<>();
        if (length <= 0L) {
            for (int i = 0; i < blocks.size(); i++) out.add(i);
            return out;
        }
        long end = start + length;
        for (int i = 0; i < blocks.size(); i++) {
            long pos = blocks.get(i).getStartingPos();
            if (pos >= start && pos < end) out.add(i);
        }
        if (out.isEmpty()) {
            throw new IllegalStateException("Parquet 区间选不中任何 row group（start=" + start
                    + ", length=" + length + "）：分片载荷与该文件 footer 不一致");
        }
        return out;
    }

    // ==================== 批次转换 ====================

    /** 读取下一批 Parquet 数据，无更多行时返回 EOF Page。 */
    public Page convertNextBatch() throws IOException {
        if (exhausted) return Page.eof();

        List<FieldVector> vectors = createVectors(arrowSchema);
        int rowCount = 0;
        while (rowCount < batchSize) {
            if (remainingRows == 0L && !advanceRowGroup()) break;
            if (remainingRows == 0L) continue;      // 空 row group：继续找下一个
            long take = Math.min(batchSize - rowCount, remainingRows);
            for (long i = 0; i < take; i++) {
                fillRow(rowReader.read(), vectors, rowCount++);
            }
            remainingRows -= take;
        }
        if (rowCount == 0) {
            exhausted = true;
            closeVectors(vectors);
            return Page.eof();
        }

        for (FieldVector v : vectors) {
            v.setValueCount(rowCount);
        }
        VectorSchemaRoot root = new VectorSchemaRoot(arrowSchema.getFields(), vectors, rowCount);
        return Page.fromRoot(root);
    }

    /** 打开下一个选中的 row group（跨 row group 拼接成连续页流）；已无更多返回 false。 */
    private boolean advanceRowGroup() throws IOException {
        if (nextRowGroup >= rowGroups.size()) return false;
        PageReadStore pages = fileReader.readRowGroup(rowGroups.get(nextRowGroup++));
        remainingRows = pages.getRowCount();
        rowReader = columnIO.getRecordReader(pages, new GroupRecordConverter(parquetSchema));
        return true;
    }

    // ==================== Schema 构建 ====================

    public static Schema buildArrowSchema(MessageType parquetSchema) {
        return buildArrowSchema(parquetSchema, true);
    }

    public static Schema buildArrowSchema(MessageType parquetSchema, boolean nestedNative) {
        List<Field> fields = new ArrayList<>();
        for (Type type : parquetSchema.getFields()) {
            fields.add(buildField(type.getName(), type, nestedNative));
        }
        return new Schema(fields);
    }

    public static Field buildField(String name, Type parquetType) {
        return buildField(name, parquetType, true);
    }

    /**
     * Parquet 类型 → Arrow 字段。
     *
     * <p>{@code nestedNative=false} 时 GROUP / LIST / MAP / repeated 一律以文本输出；
     * {@code true} 时按 Parquet 结构递归构造 Arrow List / Map / Struct。</p>
     */
    public static Field buildField(String name, Type parquetType, boolean nestedNative) {
        if (!nestedNative) {
            // GROUP / LIST / MAP 以及 repeated 字段（数组）一律以文本输出
            if (!parquetType.isPrimitive()
                    || parquetType.isRepetition(Type.Repetition.REPEATED)) {
                return new Field(name, FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
            }
            return new Field(name, FieldType.nullable(mapParquetToArrow(parquetType)), null);
        }
        if (isList(parquetType)) {
            Type element = listElementType(parquetType);
            List<Field> kids = element == null ? List.of()
                    : List.of(buildField(NestedFieldNames.LIST_ELEMENT, element, true));
            return new Field(name, FieldType.nullable(new ArrowType.List()), kids);
        }
        if (isMap(parquetType)) {
            GroupType kv = parquetType.asGroupType().getType(0).asGroupType();
            Field key = buildField(NestedFieldNames.MAP_KEY, kv.getType(0), true);
            key = new Field(NestedFieldNames.MAP_KEY,
                    FieldType.notNullable(key.getType()), key.getChildren());
            Field value = buildField(NestedFieldNames.MAP_VALUE, kv.getType(1), true);
            Field entries = new Field(NestedFieldNames.MAP_ENTRIES,
                    FieldType.notNullable(new ArrowType.Struct()), List.of(key, value));
            return new Field(name, FieldType.nullable(new ArrowType.Map(false)), List.of(entries));
        }
        if (parquetType.isRepetition(Type.Repetition.REPEATED)) {
            // 老式 2-level 数组：repeated <primitive|group>，元素就是该字段本身
            Field element = parquetType.isPrimitive()
                    ? new Field(NestedFieldNames.LIST_ELEMENT,
                            FieldType.nullable(mapParquetToArrow(parquetType)), null)
                    : new Field(NestedFieldNames.LIST_ELEMENT,
                            FieldType.nullable(new ArrowType.Struct()), structChildren(parquetType));
            return new Field(name, FieldType.nullable(new ArrowType.List()), List.of(element));
        }
        if (!parquetType.isPrimitive()) {
            // 普通 GROUP = struct
            return new Field(name, FieldType.nullable(new ArrowType.Struct()),
                    structChildren(parquetType));
        }
        return new Field(name, FieldType.nullable(mapParquetToArrow(parquetType)), null);
    }

    /** Parquet GROUP 的子字段（按文件顺序递归）。 */
    private static List<Field> structChildren(Type groupType) {
        GroupType group = groupType.asGroupType();
        List<Field> kids = new ArrayList<>(group.getFieldCount());
        for (int i = 0; i < group.getFieldCount(); i++) {
            Type child = group.getType(i);
            kids.add(buildField(child.getName(), child, true));
        }
        return kids;
    }

    /**
     * 列表的元素类型：3-level（{@code repeated group list { element }}）取内层 element；
     * 2-level（{@code repeated <type> array}）取 repeated 节点本身。
     */
    private static Type listElementType(Type listType) {
        GroupType group = listType.asGroupType();
        if (group.getFieldCount() == 0) return null;
        Type repeated = group.getType(0);
        if (repeated.isPrimitive()) return repeated;
        GroupType repeatedGroup = repeated.asGroupType();
        // 标准 3-level：repeated group list { optional <element> element; }
        if (repeatedGroup.getFieldCount() == 1) return repeatedGroup.getType(0);
        return repeated;
    }

    // ==================== Parquet → Arrow 类型映射 ====================

    public static ArrowType mapParquetToArrow(Type parquetType) {
        PrimitiveType pt = parquetType.asPrimitiveType();
        // 用 LogicalTypeAnnotation（新 API）。老的 converted_type 会被 parquet-mr 在解析 schema 时
        // 翻译成注解，故新老文件（含 converted_type 的 Hive 老文件）都能命中。
        LogicalTypeAnnotation lta = pt.getLogicalTypeAnnotation();
        return switch (pt.getPrimitiveTypeName()) {
            case BOOLEAN -> ArrowType.Bool.INSTANCE;
            case INT32 -> {
                if (lta instanceof DateLogicalTypeAnnotation) yield new ArrowType.Date(DateUnit.DAY);
                if (lta instanceof TimeLogicalTypeAnnotation)
                    yield new ArrowType.Time(TimeUnit.MILLISECOND, 32);
                if (lta instanceof DecimalLogicalTypeAnnotation) yield decimalType(lta);
                // Hive 的 tinyint/smallint 在 Parquet 里就是带 INT_8 / INT_16 注解的 INT32，
                // 按位宽还原成 Int(8)/Int(16)，才能与 Hive 元数据的类型对齐（无注解 = 普通 int）
                if (lta instanceof IntLogicalTypeAnnotation i && i.isSigned())
                    yield new ArrowType.Int(i.getBitWidth(), true);
                yield new ArrowType.Int(32, true);
            }
            case INT64 -> {
                if (lta instanceof TimestampLogicalTypeAnnotation)
                    yield new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
                if (lta instanceof TimeLogicalTypeAnnotation t)
                    yield t.getUnit() == LogicalTypeAnnotation.TimeUnit.NANOS
                            ? new ArrowType.Time(TimeUnit.NANOSECOND, 64)
                            : new ArrowType.Time(TimeUnit.MICROSECOND, 64);
                if (lta instanceof DecimalLogicalTypeAnnotation) yield decimalType(lta);
                if (lta instanceof IntLogicalTypeAnnotation i && i.isSigned())
                    yield new ArrowType.Int(i.getBitWidth(), true);
                yield new ArrowType.Int(64, true);
            }
            // INT96 = 老式 Hive/Impala 时间戳（Julian day + nanos），按毫秒时间戳读
            case INT96 -> new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
            case FLOAT -> new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case DOUBLE -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case BINARY -> {
                if (lta instanceof StringLogicalTypeAnnotation
                        || lta instanceof EnumLogicalTypeAnnotation
                        || lta instanceof JsonLogicalTypeAnnotation)
                    yield ArrowType.Utf8.INSTANCE;
                yield ArrowType.Binary.INSTANCE;
            }
            case FIXED_LEN_BYTE_ARRAY -> {
                if (lta instanceof DecimalLogicalTypeAnnotation) yield decimalType(lta);
                yield ArrowType.Binary.INSTANCE;
            }
            default -> ArrowType.Utf8.INSTANCE;
        };
    }

    /** Arrow Decimal128 上限 38 位，超出的精度只能截到 38（否则构造器直接抛）。 */
    private static ArrowType decimalType(LogicalTypeAnnotation lta) {
        int precision = 38;
        int scale = 0;
        if (lta instanceof DecimalLogicalTypeAnnotation d) {
            if (d.getPrecision() > 0) precision = Math.min(d.getPrecision(), 38);
            if (d.getScale() > 0) scale = Math.min(d.getScale(), precision);
        }
        return new ArrowType.Decimal(precision, scale, 128);
    }

    // ==================== Vector 创建 ====================

    private List<FieldVector> createVectors(Schema schema) {
        List<FieldVector> vectors = new ArrayList<>();
        for (Field field : schema.getFields()) {
            vectors.add(field.createVector(allocator));
        }
        return vectors;
    }

    private static void closeVectors(List<FieldVector> vectors) {
        for (FieldVector v : vectors) {
            v.close();
        }
    }

    // ==================== 核心填充 ====================

    /** 把一行 Parquet {@code SimpleGroup} 填进各 Arrow 向量的第 {@code row} 行。 */
    private static void fillRow(Group group, List<FieldVector> vectors, int row) {
        for (int col = 0; col < vectors.size(); col++) {
            FieldVector vec = vectors.get(col);
            Type type = group.getType().getType(col);
            // optional 字段缺失（0 次重复）即 NULL；required 字段恒 >= 1。
            // repeated 字段为 0 次重复 = 空数组，仍需渲染成 []（不是 NULL）。
            boolean repeated = type.isRepetition(Type.Repetition.REPEATED);
            if (group.getFieldRepetitionCount(col) == 0 && !repeated) {
                setNullValue(vec, row);
                continue;
            }
            if (vec instanceof MapVector mv) {
                fillMap(mv, group.getGroup(col, 0), type, row);
            } else if (vec instanceof ListVector lv) {
                if (isList(type)) fillList(lv, group.getGroup(col, 0), type, row);
                else fillRepeatedList(lv, group, col, type, row);   // 老式 2-level repeated
            } else if (vec instanceof StructVector sv) {
                fillStruct(sv, group.getGroup(col, 0), type, row);
            } else {
                fillValue(vec, group, col, 0, row);
            }
        }
    }

    /** Parquet {@code LIST} 组 → Arrow {@link ListVector}。 */
    private static void fillList(ListVector v, Group listGroup, Type listType, int row) {
        Type repeated = listType.asGroupType().getType(0);
        Type elementType = listElementType(listType);
        int n = listGroup.getFieldRepetitionCount(0);
        FieldVector data = v.getDataVector();
        int start = v.startNewValue(row);
        for (int i = 0; i < n; i++) {
            if (repeated.isPrimitive()) {
                fillValue(data, listGroup, 0, i, start + i);
            } else if (repeated.asGroupType().getFieldCount() == 1) {
                // 标准 3-level：repeated group list { <element> } —— element 为 optional，可能为 NULL
                Group wrapper = listGroup.getGroup(0, i);
                if (wrapper.getFieldRepetitionCount(0) == 0) setNullValue(data, start + i);
                else fillFromGroup(data, wrapper, 0, elementType, 0, start + i);
            } else {
                fillStruct((StructVector) data, listGroup.getGroup(0, i), repeated, start + i);
            }
        }
        v.endValue(row, n);
    }

    /** 老式 2-level 数组（{@code repeated <primitive|group>}）→ Arrow {@link ListVector}。 */
    private static void fillRepeatedList(ListVector v, Group group, int col, Type type, int row) {
        int n = group.getFieldRepetitionCount(col);
        FieldVector data = v.getDataVector();
        int start = v.startNewValue(row);
        for (int i = 0; i < n; i++) {
            if (type.isPrimitive()) {
                fillValue(data, group, col, i, start + i);
            } else {
                fillStruct((StructVector) data, group.getGroup(col, i), type, start + i);
            }
        }
        v.endValue(row, n);
    }

    /** Parquet {@code MAP} 组 → Arrow {@link MapVector}。 */
    private static void fillMap(MapVector v, Group mapGroup, Type mapType, int row) {
        Type kvType = mapType.asGroupType().getType(0);
        GroupType kv = kvType.asGroupType();
        Type keyType = kv.getType(0);
        Type valueType = kv.getType(1);
        int n = mapGroup.getFieldRepetitionCount(0);
        StructVector entries = (StructVector) v.getDataVector();
        List<FieldVector> children = entries.getChildrenFromFields();
        FieldVector keys = children.get(0);
        FieldVector values = children.get(1);
        int start = v.startNewValue(row);
        for (int i = 0; i < n; i++) {
            Group e = mapGroup.getGroup(0, i);
            if (e.getFieldRepetitionCount(0) == 0) setNullValue(keys, start + i);
            else fillFromGroup(keys, e, 0, keyType, 0, start + i);
            if (e.getFieldRepetitionCount(1) == 0) setNullValue(values, start + i);
            else fillFromGroup(values, e, 1, valueType, 0, start + i);
            // ★ entries 是非空 struct：Arrow 的 Map 拷贝（ComplexCopier）与 IPC 都会看它的 validity，
            //   只写 offset 不置位 ⇒ 引擎拷贝/溢写后整张 map 退化成 {null:null}。必须逐条置位。
            entries.setIndexDefined(start + i);
        }
        v.endValue(row, n);
    }

    /** Parquet GROUP（struct）→ Arrow {@link StructVector}：按文件顺序逐子字段填。 */
    private static void fillStruct(StructVector v, Group sg, Type structType, int row) {
        GroupType gt = structType.asGroupType();
        List<FieldVector> children = v.getChildrenFromFields();
        for (int i = 0; i < gt.getFieldCount() && i < children.size(); i++) {
            Type childType = gt.getType(i);
            FieldVector child = children.get(i);
            boolean repeated = childType.isRepetition(Type.Repetition.REPEATED);
            if (sg.getFieldRepetitionCount(i) == 0 && !repeated) {
                setNullValue(child, row);
                continue;
            }
            fillFromGroup(child, sg, i, childType, 0, row);
        }
        v.setIndexDefined(row);
    }

    /** 按目标向量类型分派：嵌套继续递归，标量走 {@link #fillValue}。 */
    private static void fillFromGroup(FieldVector vec, Group group, int col, Type type, int rep, int row) {
        if (vec instanceof MapVector mv) {
            fillMap(mv, group.getGroup(col, rep), type, row);
        } else if (vec instanceof ListVector lv) {
            if (isList(type)) fillList(lv, group.getGroup(col, rep), type, row);
            else fillRepeatedList(lv, group, col, type, row);
        } else if (vec instanceof StructVector sv) {
            fillStruct(sv, group.getGroup(col, rep), type, row);
        } else {
            fillValue(vec, group, col, rep, row);
        }
    }

    private static void fillValue(FieldVector vec, Group g, int col, int rep, int row) {
        if (vec instanceof BitVector v) {
            v.setSafe(row, g.getBoolean(col, rep) ? 1 : 0);
        } else if (vec instanceof TinyIntVector v) {
            v.setSafe(row, (byte) g.getInteger(col, rep));
        } else if (vec instanceof SmallIntVector v) {
            v.setSafe(row, (short) g.getInteger(col, rep));
        } else if (vec instanceof IntVector v) {
            v.setSafe(row, g.getInteger(col, rep));
        } else if (vec instanceof BigIntVector v) {
            v.setSafe(row, g.getLong(col, rep));
        } else if (vec instanceof Float4Vector v) {
            v.setSafe(row, g.getFloat(col, rep));
        } else if (vec instanceof Float8Vector v) {
            v.setSafe(row, g.getDouble(col, rep));
        } else if (vec instanceof DateDayVector v) {
            v.setSafe(row, g.getInteger(col, rep));
        } else if (vec instanceof TimeMilliVector v) {
            v.setSafe(row, g.getInteger(col, rep));
        } else if (vec instanceof TimeMicroVector v) {
            v.setSafe(row, g.getLong(col, rep));
        } else if (vec instanceof TimeNanoVector v) {
            v.setSafe(row, g.getLong(col, rep));
        } else if (vec instanceof TimeStampMilliVector v) {
            v.setSafe(row, readTimestampMillis(g, col, rep));
        } else if (vec instanceof VarCharVector v) {
            Type type = g.getType().getType(col);
            if (type.isPrimitive() && !type.isRepetition(Type.Repetition.REPEATED)) {
                v.setSafe(row, g.getBinary(col, rep).getBytes());
            } else {
                v.setSafe(row, renderValue(g, col, rep).getBytes(StandardCharsets.UTF_8));
            }
        } else if (vec instanceof VarBinaryVector v) {
            v.setSafe(row, g.getBinary(col, rep).getBytes());
        } else if (vec instanceof DecimalVector v) {
            v.setSafe(row, readDecimal(g, col, rep, (ArrowType.Decimal) vec.getField().getType()));
        } else {
            // 宁可炸也不要静默填 0：走不到的分支意味着类型映射有缺口
            throw new UnsupportedOperationException("Parquet → Arrow 未支持的向量类型: "
                    + vec.getClass().getSimpleName() + "，列: " + vec.getField().getName());
        }
    }

    /**
     * 写 NULL —— Arrow Map 的 key 是 non-nullable，不能 {@code setNull}，写类型零值占位。
     */
    private static void setNullValue(FieldVector v, int idx) {
        if (v.getField().isNullable()) {
            v.setNull(idx);
            return;
        }
        if (v instanceof BitVector b) b.setSafe(idx, 0);
        else if (v instanceof TinyIntVector t) t.setSafe(idx, (byte) 0);
        else if (v instanceof SmallIntVector t) t.setSafe(idx, (short) 0);
        else if (v instanceof IntVector t) t.setSafe(idx, 0);
        else if (v instanceof BigIntVector t) t.setSafe(idx, 0L);
        else if (v instanceof Float4Vector t) t.setSafe(idx, 0f);
        else if (v instanceof Float8Vector t) t.setSafe(idx, 0d);
        else if (v instanceof DateDayVector t) t.setSafe(idx, 0);
        else if (v instanceof TimeStampMilliVector t) t.setSafe(idx, 0L);
        else if (v instanceof VarCharVector t) t.setSafe(idx, new byte[0]);
        else if (v instanceof VarBinaryVector t) t.setSafe(idx, new byte[0]);
        else if (v instanceof DecimalVector t) t.setSafe(idx, BigDecimal.ZERO.setScale(t.getScale()));
        else if (v instanceof ListVector t) {
            t.startNewValue(idx);
            t.endValue(idx, 0);
        } else {
            throw new UnsupportedOperationException(
                    "非空嵌套字段无法写入 NULL 占位: " + v.getField().getName());
        }
    }

    /** INT64 存的时间戳按注解单位折算成毫秒；INT96 走 Julian day + nanos。 */
    private static long readTimestampMillis(Group g, int col, int rep) {
        Type type = g.getType().getType(col);
        PrimitiveType.PrimitiveTypeName name = type.asPrimitiveType().getPrimitiveTypeName();
        if (name == PrimitiveType.PrimitiveTypeName.INT96) {
            return int96ToMillis(g.getBinary(col, rep).getBytes());
        }
        long raw = g.getLong(col, rep);
        if (type.getLogicalTypeAnnotation() instanceof TimestampLogicalTypeAnnotation ts) {
            return switch (ts.getUnit()) {
                case MICROS -> raw / 1_000L;
                case NANOS -> raw / 1_000_000L;
                case MILLIS -> raw;
            };
        }
        return raw;
    }

    /** INT96 = 8 字节当日纳秒 + 4 字节 Julian day（均小端）。 */
    static long int96ToMillis(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        long nanosOfDay = buf.getLong();
        int julianDay = buf.getInt();
        // 2440588 = 1970-01-01 的 Julian day
        long daysSinceEpoch = (long) julianDay - 2440588L;
        return daysSinceEpoch * 86_400_000L + nanosOfDay / 1_000_000L;
    }

    private static BigDecimal readDecimal(Group g, int col, int rep, ArrowType.Decimal decimalType) {
        Type type = g.getType().getType(col);
        PrimitiveType.PrimitiveTypeName name = type.asPrimitiveType().getPrimitiveTypeName();
        int scale = decimalType.getScale();
        return switch (name) {
            case INT32 -> BigDecimal.valueOf(g.getInteger(col, rep), scale);
            case INT64 -> BigDecimal.valueOf(g.getLong(col, rep), scale);
            // BINARY / FIXED_LEN_BYTE_ARRAY：大端二进制补码无标度数
            default -> new BigDecimal(new BigInteger(g.getBinary(col, rep).getBytes()), scale);
        };
    }

    // ==================== 复合类型 → 文本 ====================

    /**
     * 把嵌套值的第 {@code index} 次重复渲染成文本。字符串在复合值内加引号，
     * 与 ORC 读侧保持一致。
     */
    private static String renderValue(Group g, int field, int index) {
        Type type = g.getType().getType(field);
        if (type.isPrimitive()) {
            if (type.isRepetition(Type.Repetition.REPEATED)) {
                return renderRepeatedPrimitive(g, field, type);
            }
            return renderPrimitive(g, field, type, index);
        }
        if (isList(type)) return renderList(g, field, type);
        if (isMap(type)) return renderMap(g, field, type);
        if (type.isRepetition(Type.Repetition.REPEATED)) return renderRepeatedGroup(g, field, type);
        return renderStruct(g, field, type);
    }

    private static boolean isList(Type type) {
        if (type.getLogicalTypeAnnotation()
                instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation) return true;
        if (type.isPrimitive()) return false;
        // 老文件 / Hive 2-level 数组：group 名 list/array，唯一子字段是 repeated
        GroupType group = type.asGroupType();
        if (group.getFieldCount() != 1) return false;
        String name = type.getName().toLowerCase(java.util.Locale.ROOT);
        return (name.equals("list") || name.equals("array"))
                && group.getType(0).isRepetition(Type.Repetition.REPEATED);
    }

    private static boolean isMap(Type type) {
        if (type.getLogicalTypeAnnotation()
                instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation) return true;
        if (type.getLogicalTypeAnnotation()
                instanceof LogicalTypeAnnotation.MapKeyValueTypeAnnotation) return true;
        if (type.isPrimitive()) return false;
        GroupType group = type.asGroupType();
        if (group.getFieldCount() != 1) return false;
        String name = type.getName().toLowerCase(java.util.Locale.ROOT);
        Type kv = group.getType(0);
        return (name.equals("map") || name.equals("map_key_value"))
                && kv.isRepetition(Type.Repetition.REPEATED)
                && !kv.isPrimitive() && kv.asGroupType().getFieldCount() == 2;
    }

    private static String renderList(Group g, int field, Type listType) {
        Group listGroup = g.getGroup(field, 0);
        Type repeated = listType.asGroupType().getType(0);
        int n = listGroup.getFieldRepetitionCount(0);
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < n; i++) {
            if (i > 0) sb.append(',');
            if (repeated.isPrimitive()) {
                sb.append(renderPrimitive(listGroup, 0, repeated, i));
            } else if (repeated.asGroupType().getFieldCount() == 1) {
                // 标准 3-level：repeated group list { element } —— 取 element
                sb.append(renderValue(listGroup.getGroup(0, i), 0, 0));
            } else {
                sb.append(renderStructGroup(listGroup.getGroup(0, i), repeated));
            }
        }
        return sb.append(']').toString();
    }

    private static String renderMap(Group g, int field, Type mapType) {
        Group mapGroup = g.getGroup(field, 0);
        int n = mapGroup.getFieldRepetitionCount(0);
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < n; i++) {
            if (i > 0) sb.append(',');
            Group kv = mapGroup.getGroup(0, i);
            sb.append(kv.getFieldRepetitionCount(0) == 0 ? "null" : renderValue(kv, 0, 0));
            sb.append(':');
            sb.append(kv.getFieldRepetitionCount(1) == 0 ? "null" : renderValue(kv, 1, 0));
        }
        return sb.append('}').toString();
    }

    private static String renderStruct(Group g, int field, Type type) {
        return renderStructGroup(g.getGroup(field, 0), type);
    }

    private static String renderStructGroup(Group struct, Type type) {
        GroupType group = type.asGroupType();
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < group.getFieldCount(); i++) {
            if (i > 0) sb.append(',');
            sb.append(quote(group.getType(i).getName())).append(':');
            sb.append(struct.getFieldRepetitionCount(i) == 0
                    ? "null" : renderValue(struct, i, 0));
        }
        return sb.append('}').toString();
    }

    private static String renderRepeatedPrimitive(Group g, int field, Type type) {
        int n = g.getFieldRepetitionCount(field);
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < n; i++) {
            if (i > 0) sb.append(',');
            sb.append(renderPrimitive(g, field, type, i));
        }
        return sb.append(']').toString();
    }

    private static String renderRepeatedGroup(Group g, int field, Type type) {
        int n = g.getFieldRepetitionCount(field);
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < n; i++) {
            if (i > 0) sb.append(',');
            sb.append(renderStructGroup(g.getGroup(field, i), type));
        }
        return sb.append(']').toString();
    }

    private static String renderPrimitive(Group g, int field, Type type, int index) {
        String s = g.getValueToString(field, index);
        return isStringType(type) ? quote(s) : s;
    }

    private static boolean isStringType(Type type) {
        if (!type.isPrimitive()) return false;
        LogicalTypeAnnotation lta = type.getLogicalTypeAnnotation();
        return lta instanceof StringLogicalTypeAnnotation
                || lta instanceof EnumLogicalTypeAnnotation
                || lta instanceof JsonLogicalTypeAnnotation;
    }

    private static String quote(String s) {
        return '"' + s.replace("\\", "\\\\").replace("\"", "\\\"") + '"';
    }

    // ==================== 访问器 ====================

    public Schema getArrowSchema() { return arrowSchema; }
    public int getBatchSize() { return batchSize; }

    @Override
    public void close() throws IOException {
        fileReader.close();
    }
}
