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
import org.apache.arrow.vector.util.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * ORC File → Arrow VectorSchemaRoot 批量转换器。
 * 逐列直填 Arrow 向量，不产生行级 Java 对象。ORC ColumnVector 自带 NULL 标记。
 *
 * <p>支持只读文件的<b>一个 stripe 区间</b>（{@link #ORCFileArrowConverter(BufferAllocator, Path,
 * FileSystem, int, long, long)}），供大文件在文件内切分并行读。</p>
 *
 * <p><b>嵌套类型</b>有两种读出模式，由 {@code nestedNative} 决定：</p>
 * <ul>
 *   <li>{@code true}（默认）→ {@code array} 读成 Arrow List、{@code map} 读成 Arrow Map、
 *       {@code struct} 读成 Arrow Struct，可原样 CTAS / INSERT 写回；</li>
 *   <li>{@code false} → 全部渲染成文本（{@code [1,2]} / {@code {"k":v}} / {@code {"a":1}}），
 *       兼容只按字符串处理嵌套列的老引擎行为。</li>
 * </ul>
 * <p>{@code uniontype} 没有 Arrow 对应结构，两种模式下都按文本读出（{@code {tag:值}}）。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/11
 */
public class ORCFileArrowConverter implements AutoCloseable {

    private final BufferAllocator allocator;
    private final Reader reader;
    private final RecordReader rows;
    private final int batchSize;
    private final Schema arrowSchema;
    /** 本次读取用的 ORC schema —— 有投影时是投影后的子 schema，否则是全 schema。 */
    private final TypeDescription readSchema;
    /** 嵌套列是否读成结构化 Arrow（List/Map/Struct）；{@code false} = 一律读成文本。 */
    private final boolean nestedNative;
    private boolean exhausted;

    public ORCFileArrowConverter(BufferAllocator allocator, String filePath, int batchSize)
            throws IOException {
        this(allocator, new Path(filePath), new Configuration(), batchSize);
    }

    public ORCFileArrowConverter(BufferAllocator allocator, Path path,
                                  Configuration conf, int batchSize) throws IOException {
        this.allocator = allocator;
        this.batchSize = batchSize;
        this.nestedNative = true;
        this.reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
        this.readSchema = reader.getSchema();
        this.rows = reader.rows(reader.options().schema(readSchema));
        this.arrowSchema = buildArrowSchema(readSchema, nestedNative);
    }

    /**
     * 显式绑定 {@link FileSystem} 的构造 —— HDFS 上读文件的路径。
     *
     * <p>{@code OrcFile.readerOptions(conf)} 默认靠路径 scheme / {@code fs.defaultFS} 反查文件系统；
     * 当 conf 里没设 {@code fs.defaultFS}、而文件路径又是相对的，就会落到本地文件系统。
     * Hive 连接器直读 {@code hdfs://...} 时用本构造显式给出 FileSystem，避免这种静默走错。</p>
     */
    public ORCFileArrowConverter(BufferAllocator allocator, Path path,
                                  FileSystem fs, int batchSize) throws IOException {
        this(allocator, path, fs, batchSize, 0L, -1L);
    }

    /**
     * 带<b>字节区间</b>的构造 —— 只读落在 {@code [start, start + length)} 内的 stripe。
     *
     * <p>一次大文件内部的切割（一个 10GB 文件切成若干 stripe 组）靠的就是它：ORC 的 stripe
     * 偏移在 footer 里，区间按 stripe 边界给，读取器自会跳过区间之外的 stripe，不扫多余数据。</p>
     *
     * <p>{@code length <= 0} = 整文件，等价于不带区间的构造。</p>
     */
    public ORCFileArrowConverter(BufferAllocator allocator, Path path, FileSystem fs, int batchSize,
                                  long start, long length) throws IOException {
        this(allocator, path, fs, batchSize, start, length, List.of());
    }

    /**
     * 带<b>列投影</b>的构造 —— {@code projection} 非空时只读这些列（列名大小写不敏感），
     * ORC 读取层直接跳过其余列，<b>不是</b>读完再裁。空 = 全列。
     *
     * <p>投影 schema 由文件 schema 的子节点 {@code clone()} 拼成：{@code TypeDescription.addField}
     * 会把子节点从原父节点摘下，不 clone 会破坏全 schema。产出的 Arrow schema / RowBatch / 列序
     * 全部以投影 schema 为准，因此 {@code orcBatch.cols[col] ↔ vectors.get(col)} 的位置对应依旧成立。</p>
     *
     * <p>投影里文件不存在的列<b>直接忽略</b>（多半是 Hive 表后加的列，老文件没有）—— 由上层按名补 NULL。</p>
     */
    public ORCFileArrowConverter(BufferAllocator allocator, Path path, FileSystem fs, int batchSize,
                                  long start, long length, List<String> projection) throws IOException {
        this(allocator, path, fs, batchSize, start, length, projection, true);
    }

    /**
     * 带列投影 + <b>嵌套类型模式</b>的构造（Hive 连接器用的就是这个）。
     *
     * @param nestedNative {@code true} = 嵌套列读成 Arrow List/Map/Struct；
     *                     {@code false} = 渲染成文本
     */
    public ORCFileArrowConverter(BufferAllocator allocator, Path path, FileSystem fs, int batchSize,
                                  long start, long length, List<String> projection,
                                  boolean nestedNative) throws IOException {
        this.allocator = allocator;
        this.batchSize = batchSize;
        this.nestedNative = nestedNative;
        long t0 = System.nanoTime();
        this.reader = OrcFile.createReader(path,
                OrcFile.readerOptions(fs.getConf()).filesystem(fs));
        logSlowStep("createReader（读 footer）", t0, path);
        t0 = System.nanoTime();
        this.readSchema = project(reader.getSchema(), projection);
        Reader.Options options = reader.options().schema(readSchema);
        if (length > 0L) options.range(start, length);
        this.rows = reader.rows(options);
        logSlowStep("reader.rows（建读取器）", t0, path);
        t0 = System.nanoTime();
        this.arrowSchema = buildArrowSchema(readSchema, nestedNative);
        logSlowStep("buildArrowSchema", t0, path);
    }

    /** 文件 schema → 投影 schema（按 {@code projection} 顺序；空投影或无命中则返回原 schema）。 */
    private static TypeDescription project(TypeDescription fileSchema, List<String> projection) {
        if (projection == null || projection.isEmpty()) return fileSchema;
        List<String> names = fileSchema.getFieldNames();
        List<TypeDescription> children = fileSchema.getChildren();
        TypeDescription out = TypeDescription.createStruct();
        for (String wanted : projection) {
            int idx = indexOfIgnoreCase(names, wanted);
            if (idx < 0) continue;
            out.addField(names.get(idx), children.get(idx).clone());
        }
        // 一个都没命中（投影列在文件里全不存在）→ 退回全 schema，避免空 schema 读取
        return out.getChildren().isEmpty() ? fileSchema : out;
    }

    private static int indexOfIgnoreCase(List<String> names, String wanted) {
        for (int i = 0; i < names.size(); i++) {
            if (names.get(i).equalsIgnoreCase(wanted)) return i;
        }
        return -1;
    }

    /** 构造期的分段计时（同 {@code HiveConnector.logSlow}）：正常一秒不到，慢的时候指出卡在哪一步。 */
    private static final long SLOW_STEP_MS = 300L;

    private static void logSlowStep(String what, long t0, Path path) {
        long ms = (System.nanoTime() - t0) / 1_000_000L;
        if (ms < SLOW_STEP_MS) return;
        System.err.printf("[ORC] 慢步骤 %d ms：%s（%s）%n", ms, what, path);
    }

    // ==================== 批次转换 ====================

    /** 读取下一批 ORC 数据，无更多 stripe 时返回 EOF Page。 */
    public Page convertNextBatch() throws IOException {
        if (exhausted) return Page.eof();

        VectorizedRowBatch orcBatch = readSchema.createRowBatch(batchSize);
        if (!rows.nextBatch(orcBatch)) {
            exhausted = true;
            return Page.eof();
        }

        List<FieldVector> vectors = createVectors(arrowSchema);
        fillBatch(orcBatch, vectors);

        int rowCount = orcBatch.size;
        for (FieldVector v : vectors) {
            v.setValueCount(rowCount);
        }

        VectorSchemaRoot root = new VectorSchemaRoot(arrowSchema.getFields(), vectors, rowCount);
        return Page.fromRoot(root);
    }

    // ==================== Schema 构建 ====================

    static Schema buildArrowSchema(TypeDescription orcSchema) {
        return buildArrowSchema(orcSchema, true);
    }

    static Schema buildArrowSchema(TypeDescription orcSchema, boolean nestedNative) {
        List<String> fieldNames = orcSchema.getFieldNames();
        List<TypeDescription> children = orcSchema.getChildren();
        List<Field> fields = new ArrayList<>();
        for (int i = 0; i < children.size(); i++) {
            fields.add(buildField(fieldNames.get(i), children.get(i), nestedNative));
        }
        return new Schema(fields);
    }

    /**
     * ORC 类型 → Arrow 字段。
     *
     * <p>{@code nestedNative=false} 时复合类型（LIST / MAP / STRUCT / UNION）统一以文本形式读出，
     * 不再构造 Arrow 子字段；{@code true} 时按 ORC 结构递归构造（{@code uniontype} 仍按文本）。</p>
     */
    static Field buildField(String name, TypeDescription orcType, boolean nestedNative) {
        if (!nestedNative) {
            return new Field(name, FieldType.nullable(mapOrcToArrow(orcType)), null);
        }
        switch (orcType.getCategory()) {
            case LIST: {
                List<TypeDescription> children = orcType.getChildren();
                TypeDescription child = children.isEmpty() ? null : children.get(0);
                List<Field> kids = child == null ? List.of()
                        : List.of(buildField(NestedFieldNames.LIST_ELEMENT, child, true));
                return new Field(name, FieldType.nullable(new ArrowType.List()), kids);
            }
            case MAP: {
                List<TypeDescription> children = orcType.getChildren();
                if (children.size() < 2) {
                    return new Field(name, FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
                }
                Field key = buildField(NestedFieldNames.MAP_KEY, children.get(0), true);
                key = new Field(NestedFieldNames.MAP_KEY,
                        FieldType.notNullable(key.getType()), key.getChildren());
                Field value = buildField(NestedFieldNames.MAP_VALUE, children.get(1), true);
                Field entries = new Field(NestedFieldNames.MAP_ENTRIES,
                        FieldType.notNullable(new ArrowType.Struct()), List.of(key, value));
                return new Field(name, FieldType.nullable(new ArrowType.Map(false)), List.of(entries));
            }
            case STRUCT: {
                List<String> names = orcType.getFieldNames();
                List<TypeDescription> children = orcType.getChildren();
                List<Field> kids = new ArrayList<>(children.size());
                for (int i = 0; i < children.size(); i++) {
                    kids.add(buildField(names.get(i), children.get(i), true));
                }
                return new Field(name, FieldType.nullable(new ArrowType.Struct()), kids);
            }
            case UNION:
                // 结构化 union 没有 Arrow 对应结构 → 文本（{tag:值}）
                return new Field(name, FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
            default:
                return new Field(name, FieldType.nullable(mapOrcToArrow(orcType)), null);
        }
    }

    // ==================== Vector 创建 ====================

    private List<FieldVector> createVectors(Schema schema) {
        List<FieldVector> vectors = new ArrayList<>();
        for (Field field : schema.getFields()) {
            vectors.add(field.createVector(allocator));
        }
        return vectors;
    }

    // ==================== 核心填充 ====================

    private void fillBatch(VectorizedRowBatch orcBatch, List<FieldVector> arrowVectors) {
        int size = orcBatch.size;
        List<TypeDescription> types = readSchema.getChildren();
        for (int col = 0; col < arrowVectors.size(); col++) {
            TypeDescription type = types != null && col < types.size() ? types.get(col) : null;
            fillColumn(arrowVectors.get(col), orcBatch.cols[col], size, type);
        }
    }

    /**
     * 把一个 ORC {@link ColumnVector} 的 {@code [0, size)} 行填进等价的 Arrow {@link FieldVector}。
     *
     * <p>{@code public} 是给 Hive ACID 读取器用的：ACID ORC 的用户列嵌在顶层 {@code row}
     * 结构里（{@code row<struct<_col0,...>>}），解包时需要把 {@link StructColumnVector#fields}
     * 的每个子列填进对应数据列 —— 与普通 ORC 读路径复用同一套逐类型填充，不另写一份。</p>
     */
    public static void fillColumn(FieldVector arrowVec, ColumnVector orcCol, int size) {
        fillColumn(arrowVec, orcCol, size, null);
    }

    /**
     * 带 ORC {@link TypeDescription} 的填充 —— 复合列（array / map / struct / uniontype）
     * 需要它才能渲染出字段名与嵌套结构；{@code orcType == null} 时退化为无名渲染。
     */
    public static void fillColumn(FieldVector arrowVec, ColumnVector orcCol, int size,
                                  TypeDescription orcType) {
        fillSlice(arrowVec, orcCol, 0, 0, size, orcType);
    }

    /**
     * 把 ORC 列的 {@code [srcFrom, srcFrom + len)} 填进 Arrow 向量的
     * {@code [dstFrom, dstFrom + len)} —— 顶层就是 {@code fillColumn}（两侧都从 0 开始），
     * 嵌套时由父列按元素区间递归调用。
     */
    @SuppressWarnings("unchecked")
    private static void fillSlice(FieldVector arrowVec, ColumnVector orcCol,
                                  int srcFrom, int dstFrom, int len, TypeDescription orcType) {
        if (len <= 0) return;
        // ORC 的 RLE 快路径会把整批同值的列标成 isRepeating，只填下标 0：
        //   ① 字符串列 vector[i] (i>0) 为 null → new String(null) NPE；
        //   ② 数值列 vector[i] (i>0) 保留 0 → 静默错值。
        // 先 flatten 把下标 0 的值展开到整批，两种类型都归位。
        if (orcCol.isRepeating) {
            orcCol.flatten(false, null, srcFrom + len);
        }
        boolean noNulls = orcCol.noNulls;

        if (arrowVec instanceof MapVector v && orcCol instanceof MapColumnVector mcv) {
            fillMap(v, mcv, srcFrom, dstFrom, len, orcType);
        } else if (arrowVec instanceof ListVector v && orcCol instanceof ListColumnVector lcv) {
            fillList(v, lcv, srcFrom, dstFrom, len, orcType);
        } else if (arrowVec instanceof StructVector v && orcCol instanceof StructColumnVector scv) {
            // 结构列：逐子字段递归填充（ACID 的 row<struct<...>> 解包走这里）。
            // 子列与父行一一对应，直接按同一区间切片。
            List<FieldVector> children = v.getChildrenFromFields();
            List<TypeDescription> childTypes = orcType == null ? null : orcType.getChildren();
            for (int k = 0; k < children.size() && k < scv.fields.length; k++) {
                TypeDescription childType = childTypes != null && k < childTypes.size()
                        ? childTypes.get(k) : null;
                fillSlice(children.get(k), scv.fields[k], srcFrom, dstFrom, len, childType);
            }
            // StructVector 的 NULL 由 validity 位表示（默认 0 = NULL），非空行必须显式置位
            for (int i = 0; i < len; i++) {
                if (!noNulls && scv.isNull[srcFrom + i]) setNullValue(v, dstFrom + i);
                else v.setIndexDefined(dstFrom + i);
            }
        } else if (arrowVec instanceof BitVector v) {
            LongColumnVector lcv = (LongColumnVector) orcCol;
            for (int i = 0; i < len; i++) {
                int s = srcFrom + i;
                if (!noNulls && lcv.isNull[s]) setNullValue(v, dstFrom + i);
                else v.setSafe(dstFrom + i, lcv.vector[s] != 0 ? 1 : 0);
            }
        } else if (arrowVec instanceof TinyIntVector v) {
            LongColumnVector lcv = (LongColumnVector) orcCol;
            for (int i = 0; i < len; i++) {
                int s = srcFrom + i;
                if (!noNulls && lcv.isNull[s]) setNullValue(v, dstFrom + i);
                else v.setSafe(dstFrom + i, (byte) lcv.vector[s]);
            }
        } else if (arrowVec instanceof SmallIntVector v) {
            LongColumnVector lcv = (LongColumnVector) orcCol;
            for (int i = 0; i < len; i++) {
                int s = srcFrom + i;
                if (!noNulls && lcv.isNull[s]) setNullValue(v, dstFrom + i);
                else v.setSafe(dstFrom + i, (short) lcv.vector[s]);
            }
        } else if (arrowVec instanceof IntVector v) {
            LongColumnVector lcv = (LongColumnVector) orcCol;
            for (int i = 0; i < len; i++) {
                int s = srcFrom + i;
                if (!noNulls && lcv.isNull[s]) setNullValue(v, dstFrom + i);
                else v.setSafe(dstFrom + i, (int) lcv.vector[s]);
            }
        } else if (arrowVec instanceof BigIntVector v) {
            LongColumnVector lcv = (LongColumnVector) orcCol;
            for (int i = 0; i < len; i++) {
                int s = srcFrom + i;
                if (!noNulls && lcv.isNull[s]) setNullValue(v, dstFrom + i);
                else v.setSafe(dstFrom + i, lcv.vector[s]);
            }
        } else if (arrowVec instanceof Float4Vector v) {
            DoubleColumnVector dcv = (DoubleColumnVector) orcCol;
            for (int i = 0; i < len; i++) {
                int s = srcFrom + i;
                if (!noNulls && dcv.isNull[s]) setNullValue(v, dstFrom + i);
                else v.setSafe(dstFrom + i, (float) dcv.vector[s]);
            }
        } else if (arrowVec instanceof Float8Vector v) {
            DoubleColumnVector dcv = (DoubleColumnVector) orcCol;
            for (int i = 0; i < len; i++) {
                int s = srcFrom + i;
                if (!noNulls && dcv.isNull[s]) setNullValue(v, dstFrom + i);
                else v.setSafe(dstFrom + i, dcv.vector[s]);
            }
        } else if (arrowVec instanceof VarCharVector v) {
            if (orcCol instanceof BytesColumnVector bcv) {
                for (int i = 0; i < len; i++) {
                    int s = srcFrom + i;
                    // bcv.vector[i]==null 出现在「ORC 结构列某行为 NULL」时：该行子列无缓冲，
                    // 但子列的 isNull 不一定被置位 —— 直接 new String(null) 会 NPE，按 NULL 处理。
                    if ((!noNulls && bcv.isNull[s]) || bcv.vector[s] == null) setNullValue(v, dstFrom + i);
                    else v.setSafe(dstFrom + i, new Text(
                            new String(bcv.vector[s], bcv.start[s], bcv.length[s], StandardCharsets.UTF_8)));
                }
            } else {
                // 文本模式（nested.mode=text）或 uniontype：复合值渲染成 JSON 风格文本。
                for (int i = 0; i < len; i++) {
                    int s = srcFrom + i;
                    if (!noNulls && orcCol.isNull[s]) setNullValue(v, dstFrom + i);
                    else v.setSafe(dstFrom + i, new Text(renderValue(orcCol, orcType, s)));
                }
            }
        } else if (arrowVec instanceof VarBinaryVector v) {
            BytesColumnVector bcv = (BytesColumnVector) orcCol;
            for (int i = 0; i < len; i++) {
                int s = srcFrom + i;
                if ((!noNulls && bcv.isNull[s]) || bcv.vector[s] == null) {
                    setNullValue(v, dstFrom + i);
                } else {
                    byte[] bytes = new byte[bcv.length[s]];
                    System.arraycopy(bcv.vector[s], bcv.start[s], bytes, 0, bcv.length[s]);
                    v.setSafe(dstFrom + i, bytes);
                }
            }
        } else if (arrowVec instanceof TimeStampMilliVector v) {
            TimestampColumnVector tcv = (TimestampColumnVector) orcCol;
            for (int i = 0; i < len; i++) {
                int s = srcFrom + i;
                if (!noNulls && tcv.isNull[s]) setNullValue(v, dstFrom + i);
                else v.setSafe(dstFrom + i, tcv.time[s] + tcv.nanos[s] / 1_000_000);
            }
        } else if (arrowVec instanceof DateDayVector v) {
            LongColumnVector lcv = (LongColumnVector) orcCol;
            for (int i = 0; i < len; i++) {
                int s = srcFrom + i;
                if (!noNulls && lcv.isNull[s]) setNullValue(v, dstFrom + i);
                else v.setSafe(dstFrom + i, (int) lcv.vector[s]);
            }
        } else if (arrowVec instanceof DecimalVector v) {
            DecimalColumnVector dcv = (DecimalColumnVector) orcCol;
            for (int i = 0; i < len; i++) {
                int s = srcFrom + i;
                if (!noNulls && dcv.isNull[s]) setNullValue(v, dstFrom + i);
                else {
                    HiveDecimalWritable w = dcv.vector[s];
                    BigDecimal bd = w.getHiveDecimal().bigDecimalValue();
                    v.setSafe(dstFrom + i, bd);
                }
            }
        }
    }

    /**
     * ORC {@code list} → Arrow {@link ListVector}。
     *
     * <p>ORC 的元素子列是<b>全局</b>下标（各行共用一条子向量、offsets/lengths 切段），
     * Arrow 的元素则写在 {@code startNewValue()} 返回的起点上 —— 两者一一对应。</p>
     */
    private static void fillList(ListVector v, ListColumnVector lcv,
                                 int srcFrom, int dstFrom, int len, TypeDescription orcType) {
        FieldVector data = v.getDataVector();
        TypeDescription childType = orcType == null || orcType.getChildren().isEmpty()
                ? null : orcType.getChildren().get(0);
        // repeating 子列要按「本批最大元素下标」一次性展开（后续切片不再展开）
        int srcEnd = 0;
        for (int i = 0; i < len; i++) {
            srcEnd = Math.max(srcEnd, (int) (lcv.offsets[srcFrom + i] + lcv.lengths[srcFrom + i]));
        }
        if (lcv.child.isRepeating) lcv.child.flatten(false, null, srcEnd);

        for (int i = 0; i < len; i++) {
            int s = srcFrom + i;
            int row = dstFrom + i;
            int start = (int) lcv.offsets[s];
            int n = (int) lcv.lengths[s];
            int dstStart = v.startNewValue(row);
            fillSlice(data, lcv.child, start, dstStart, n, childType);
            v.endValue(row, n);
            if (!lcv.noNulls && lcv.isNull[s]) v.setNull(row);
        }
    }

    /** ORC {@code map} → Arrow {@link MapVector}（entries 结构 + key/value 两子列）。 */
    private static void fillMap(MapVector v, MapColumnVector mcv,
                                int srcFrom, int dstFrom, int len, TypeDescription orcType) {
        List<TypeDescription> children = orcType == null ? List.of() : orcType.getChildren();
        TypeDescription keyType = children.size() > 0 ? children.get(0) : null;
        TypeDescription valueType = children.size() > 1 ? children.get(1) : null;
        StructVector entries = (StructVector) v.getDataVector();
        List<FieldVector> kv = entries.getChildrenFromFields();
        FieldVector keys = kv.get(0);
        FieldVector values = kv.get(1);

        int srcEnd = 0;
        for (int i = 0; i < len; i++) {
            srcEnd = Math.max(srcEnd, (int) (mcv.offsets[srcFrom + i] + mcv.lengths[srcFrom + i]));
        }
        if (mcv.keys.isRepeating) mcv.keys.flatten(false, null, srcEnd);
        if (mcv.values.isRepeating) mcv.values.flatten(false, null, srcEnd);

        for (int i = 0; i < len; i++) {
            int s = srcFrom + i;
            int row = dstFrom + i;
            int start = (int) mcv.offsets[s];
            int n = (int) mcv.lengths[s];
            int dstStart = v.startNewValue(row);
            fillSlice(keys, mcv.keys, start, dstStart, n, keyType);
            fillSlice(values, mcv.values, start, dstStart, n, valueType);
            // ★ entries 是非空 struct：Arrow 的 Map 拷贝（ComplexCopier）与 IPC 都会看它的 validity，
            //   只写 offset 不置位 ⇒ 引擎拷贝/溢写后整张 map 退化成 {null:null}。必须逐条置位。
            for (int e = 0; e < n; e++) entries.setIndexDefined(dstStart + e);
            v.endValue(row, n);
            if (!mcv.noNulls && mcv.isNull[s]) v.setNull(row);
        }
    }

    /**
     * 写 NULL —— 非空字段（如 Arrow Map 的 key，规范要求 non-nullable）不能 {@code setNull}，
     * 写类型零值占位。
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

    // ==================== 复合类型 → 文本 ====================

    private static final DateTimeFormatter TS_TEXT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    /** 把 ORC 复合列的第 {@code row} 行渲染成文本（array / map / struct / uniontype 递归）。 */
    private static String renderValue(ColumnVector col, TypeDescription type, int row) {
        if (col instanceof StructColumnVector scv) return renderStruct(scv, type, row);
        if (col instanceof ListColumnVector lcv) return renderList(lcv, type, row);
        if (col instanceof MapColumnVector mcv) return renderMap(mcv, type, row);
        if (col instanceof UnionColumnVector ucv) return renderUnion(ucv, type, row);
        return renderScalar(col, type, row);
    }

    private static String renderStruct(StructColumnVector scv, TypeDescription type, int row) {
        List<String> names = type == null ? List.of() : type.getFieldNames();
        List<TypeDescription> children = type == null ? List.of() : type.getChildren();
        StringBuilder sb = new StringBuilder("{");
        for (int k = 0; k < scv.fields.length; k++) {
            if (k > 0) sb.append(',');
            String name = k < names.size() ? names.get(k) : "_c" + k;
            sb.append(quote(name)).append(':');
            ColumnVector child = scv.fields[k];
            sb.append(isNullValue(child, row) ? "null"
                    : renderValue(child, k < children.size() ? children.get(k) : null, row));
        }
        return sb.append('}').toString();
    }

    private static String renderList(ListColumnVector lcv, TypeDescription type, int row) {
        TypeDescription childType = type == null || type.getChildren().isEmpty()
                ? null : type.getChildren().get(0);
        int offset = (int) lcv.offsets[row];
        int length = (int) lcv.lengths[row];
        StringBuilder sb = new StringBuilder("[");
        for (int j = 0; j < length; j++) {
            if (j > 0) sb.append(',');
            int childRow = offset + j;
            sb.append(isNullValue(lcv.child, childRow) ? "null"
                    : renderValue(lcv.child, childType, childRow));
        }
        return sb.append(']').toString();
    }

    private static String renderMap(MapColumnVector mcv, TypeDescription type, int row) {
        List<TypeDescription> children = type == null ? List.of() : type.getChildren();
        TypeDescription keyType = children.size() > 0 ? children.get(0) : null;
        TypeDescription valueType = children.size() > 1 ? children.get(1) : null;
        int offset = (int) mcv.offsets[row];
        int length = (int) mcv.lengths[row];
        StringBuilder sb = new StringBuilder("{");
        for (int j = 0; j < length; j++) {
            if (j > 0) sb.append(',');
            int childRow = offset + j;
            sb.append(isNullValue(mcv.keys, childRow) ? "null"
                    : renderValue(mcv.keys, keyType, childRow));
            sb.append(':');
            sb.append(isNullValue(mcv.values, childRow) ? "null"
                    : renderValue(mcv.values, valueType, childRow));
        }
        return sb.append('}').toString();
    }

    private static String renderUnion(UnionColumnVector ucv, TypeDescription type, int row) {
        int tag = ucv.tags[row];
        List<TypeDescription> children = type == null ? List.of() : type.getChildren();
        TypeDescription childType = tag >= 0 && tag < children.size() ? children.get(tag) : null;
        ColumnVector child = ucv.fields[tag];
        String value = isNullValue(child, row) ? "null" : renderValue(child, childType, row);
        return "{" + tag + ':' + value + '}';
    }

    private static String renderScalar(ColumnVector col, TypeDescription type, int row) {
        TypeDescription.Category cat = type == null ? null : type.getCategory();
        if (col instanceof LongColumnVector lcv) {
            long v = lcv.vector[row];
            if (cat == TypeDescription.Category.BOOLEAN) return v != 0 ? "true" : "false";
            if (cat == TypeDescription.Category.DATE) return LocalDate.ofEpochDay(v).toString();
            return Long.toString(v);
        }
        if (col instanceof DoubleColumnVector dcv) return Double.toString(dcv.vector[row]);
        if (col instanceof DecimalColumnVector dcv) {
            return dcv.vector[row].getHiveDecimal().bigDecimalValue().toPlainString();
        }
        if (col instanceof TimestampColumnVector tcv) {
            Instant instant = Instant.ofEpochMilli(tcv.time[row] + tcv.nanos[row] / 1_000_000L);
            return TS_TEXT.format(instant.atZone(ZoneOffset.UTC));
        }
        if (col instanceof BytesColumnVector bcv) {
            String s = bcv.vector[row] == null ? "" : new String(
                    bcv.vector[row], bcv.start[row], bcv.length[row], StandardCharsets.UTF_8);
            return cat == TypeDescription.Category.BINARY ? s : quote(s);
        }
        return "";
    }

    private static boolean isNullValue(ColumnVector col, int row) {
        return col != null && !col.noNulls && col.isNull[row];
    }

    /** 复合值里的字符串加引号并转义，避免与分隔符 / 结构括号混淆。 */
    private static String quote(String s) {
        return '"' + s.replace("\\", "\\\\").replace("\"", "\\\"") + '"';
    }

    // ==================== ORC → Arrow 类型映射 ====================

    public static ArrowType mapOrcToArrow(TypeDescription orcType) {
        return switch (orcType.getCategory()) {
            case BOOLEAN           -> ArrowType.Bool.INSTANCE;
            case BYTE              -> new ArrowType.Int(8, true);
            case SHORT             -> new ArrowType.Int(16, true);
            case INT               -> new ArrowType.Int(32, true);
            case LONG              -> new ArrowType.Int(64, true);
            case FLOAT             -> new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case DOUBLE            -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case DECIMAL           -> new ArrowType.Decimal(orcType.getPrecision(), orcType.getScale(), 128);
            case STRING, CHAR, VARCHAR -> ArrowType.Utf8.INSTANCE;
            case BINARY            -> ArrowType.Binary.INSTANCE;
            case TIMESTAMP, TIMESTAMP_INSTANT -> new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
            case DATE              -> new ArrowType.Date(DateUnit.DAY);
            // 复合类型以文本形式进入引擎（不构造 Arrow List/Map/Struct）：
            // 算子流水线只按标量处理，读出来能直接 SELECT / 导出 / 参与字符串函数。
            case LIST, MAP, STRUCT, UNION -> ArrowType.Utf8.INSTANCE;
            default                -> ArrowType.Utf8.INSTANCE;
        };
    }

    // ==================== 访问器 ====================

    public Schema getArrowSchema() { return arrowSchema; }
    public int getBatchSize() { return batchSize; }

    @Override
    public void close() throws IOException {
        rows.close();
    }
}
