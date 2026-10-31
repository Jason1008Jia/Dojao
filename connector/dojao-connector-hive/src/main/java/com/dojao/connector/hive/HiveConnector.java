package com.dojao.connector.hive;

import com.dojao.common.config.CommonConfig;
import com.dojao.common.data.Page;
import com.dojao.common.memory.NodeMemory;
import com.dojao.common.util.PartitionPruner;
import com.dojao.spi.bean.Catalog;
import com.dojao.spi.bean.Column;
import com.dojao.spi.bean.ColumnStats;
import com.dojao.spi.bean.DojaoType;
import com.dojao.spi.bean.ReadRequest;
import com.dojao.spi.bean.ScanSlice;
import com.dojao.spi.bean.Schema;
import com.dojao.spi.bean.Split;
import com.dojao.spi.bean.TableStats;
import com.dojao.spi.bean.WriteRequest;
import com.dojao.spi.service.DojaoConnector;
import com.dojao.spi.service.OverwriteEnable;
import com.dojao.spi.service.ScanReader;
import com.dojao.spi.util.CatalogProperties;
import com.dojao.connector.common.util.ORCFileArrowConverter;
import com.dojao.connector.common.util.ParquetFileArrowConverter;
import com.dojao.connector.common.util.TextFileArrowConverter;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.StripeInformation;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Hive 连接器 —— <b>直连 HMS（Thrift）探查 + 直读 HDFS 文件</b>，不使用 Hive-JDBC / HiveServer2。
 *
 * <p>设计要点：</p>
 * <ol>
 *   <li><b>元数据</b>走 Hive MetaStore Thrift 客户端（{@link HiveMetastore}）：拿到表的 HDFS
 *       location、分区清单、列定义、行数/体积统计参数；<b>HMS 就是这张表的目录</b> —— 表在哪、
 *       有哪些分区、分区在哪个目录，一次 Thrift 调用全有，不必让 Namenode 去遍历；</li>
 *   <li><b>数据</b>直连 HDFS 读文件：MPP 框架天然可切分并行，不必再经 SQL 引擎转一手。
 *       并行分片由 {@link #planSplits} 把读取单元清单（见 {@link HiveSplitUnit}）编成本连接器
 *       私有载荷（见 {@link HiveSplitPayload}）装进 {@link Split}，Worker 侧 {@link #openSplit}
 *       解出后逐单元顺序读。读取单元的<b>定位</b>分两档：分区表先经 HMS 按谓词裁剪出分区目录，
 *       只列举这些目录（未命中的目录一个都不碰，HMS 未登记分区才回退表根）；然后再对目录内的
 *       文件做<b>两头收敛</b> —— 小文件按阈值合并避免报出上万个分区、超大文件按 stripe / row group
 *       边界切开避免并行度封顶 1，目标单元大小同为 {@code catalog.hive.compact.target.bytes}（默认取
 *       {@code dfs.blocksize}，见 {@link #planUnits}）。<b>切分必须在这一步做完</b>：切片边界在
 *       派发前定死，一个切片只交给一条 reader 串行读，留到 Worker 侧再切换不来跨 Worker 并行；</li>
 *   <li><b>格式无感 + 列投影下推</b>：ORC / Parquet 各走专门的 File→Arrow 转换器，其余 SerDe
 *       一律按分隔文本读，三者对上层是同一条 {@code convertNextBatch() -> Page} 路径。
 *       {@code ReadRequest.columns()} 对 ORC/Parquet <b>推给读取器</b>（只读这几列的字节，
 *       不是读完再裁），文本无列式索引可跳读、整行读入后按名取列；</li>
 *   <li><b>分区列物化</b>：分区列不在数据文件里，读到的文件页要按路径 {@code key=value} 补出常量列，
 *       拼回 Hive 表结构（有列投影时只补被投影到的分区列）；</li>
 *   <li><b>ACID 表</b>（{@code transactional=true}）默认<b>明确报错</b>（防误读错数据）；catalog 属性
 *       {@code catalog.hive.acid.read=true} 开启后走行级合并读法（见 {@link HiveAcidReader}）：
 *       按写事务快照过滤 {@code base_/delta_/delete_delta_} 目录、解 ACID 的 {@code row} 结构、
 *       按 {@code RecordIdentifier} 做删除/更新合并。</li>
 * </ol>
 *
 * <p>读之外还支持<b>写</b>：CTAS（建 HDFS 目录 + HMS 建表）、INSERT INTO（追加）、
 * INSERT OVERWRITE（staging 临时目录 + 全成或全不成）。新建表的物理格式由 catalog 属性
 * {@code catalog.hive.default.format} 决定（{@code orc} / {@code parquet} / {@code text}，
 * 默认 ORC）；写已有表时按目标表自身的格式落文件（ORC / Parquet / LazySimpleSerDe 分隔文本）。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/19
 */
public class HiveConnector implements DojaoConnector, OverwriteEnable {

    /** staging 标识里「真实表名」与「staging 临时目录」的分隔符（表名不含 {@code |}，不会歧义）。 */
    private static final String STAGING_SEP = "|";

    /** 一条 HDFS 目录递归找文件的深度上限（分区列 + 桶目录一般 ≤ 2 层）。 */
    private static final int MAX_WALK_DEPTH = 4;

    /** 小文件合并的目标字节数（catalog 属性 {@code catalog.hive.compact.target.bytes}）。 */
    private static final String COMPACT_TARGET_KEY = "hive.compact.target.bytes";

    /** 小文件合并阈值的默认来源：HDFS 块大小（阿里云默认 128MB）。 */
    private static final String DFS_BLOCK_SIZE_KEY = "dfs.blocksize";

    /** 取不到 {@code dfs.blocksize} 时的兜底阈值（128MB，与 HDFS 默认一致）。 */
    private static final long DEFAULT_COMPACT_TARGET_BYTES = 128L * 1024 * 1024;

    /**
     * 自适应单元目标大小（见 {@link #unitTarget}）的<b>下限</b>：再小也不低于它 ——
     * 单元过小只会把「建连接 + 读 footer」的固定开销摊到更多单元上，并行度却由切片数封顶，
     * 换不来收益。
     */
    private static final long MIN_UNIT_TARGET_BYTES = 8L * 1024 * 1024;

    /** 单个文件大到什么程度才值得在文件内切割（catalog 属性）。 */
    private static final String SPLIT_FILE_MIN_KEY = "hive.split.file.min.bytes";

    /**
     * 写入物理格式的键（catalog 规范名 {@code catalog.hive.default.format}，归一化后即此名）。
     * 合法取值 {@code orc} / {@code parquet} / {@code text}（{@code textfile} 同义），默认 {@code orc}；
     * 只影响 CTAS 新建表，写已有表时按目标表自身格式。
     */
    private static final String WRITE_FORMAT_KEY = "hive.default.format";

    /** 大文件切割的默认触发阈值：1GB。小于它的文件「一个文件一片」足够，切了反而多开销。 */
    private static final long DEFAULT_SPLIT_FILE_MIN_BYTES = 1024L * 1024 * 1024;

    /** 桶裁剪开关（catalog 属性 {@code catalog.hive.bucket.pruning}，归一化后键名即此名）。默认开启。 */
    private static final String BUCKET_PRUNING_KEY = "hive.bucket.pruning";

    /** ACID（事务表）读取开关（catalog 属性 {@code catalog.hive.acid.read}）。默认关闭。 */
    private static final String ACID_READ_KEY = "hive.acid.read";

    /**
     * 复合类型（array / map / struct）的呈现方式（catalog 属性 {@code catalog.hive.nested.mode}）：
     * {@code native}（默认）读成真实嵌套向量、可原样写回；{@code text} 读成 JSON 风格字符串列。
     * {@code uniontype} 两种模式下都是文本。
     */
    private static final String NESTED_MODE_KEY = "hive.nested.mode";

    private final String catalogName;
    private final HiveVersion version;
    private final HiveMetastore metastore;
    private final Configuration conf;
    private final BufferAllocator allocator;
    private final int batchSize;
    private final int partitionLimit;

    /**
     * 读取单元的目标大小上限（字节），见 {@link #planUnits}。连接器构造期解析一次：
     * catalog 属性 {@code hive.compact.target.bytes} → Hadoop {@code dfs.blocksize}（默认 128MB）。
     * <b>必须从 {@code props} 读</b> —— {@link HadoopEnvironment#newConfiguration} 只搬运少数固定键，
     * 属性不会被整份灌进 conf。
     */
    private final long compactTargetBytes;

    /**
     * 文件内切割的触发阈值（字节）：只有单个文件<b>大于</b>它才尝试按 stripe / row group 切开
     * （见 {@link #splitBigFile}）。catalog 属性 {@code catalog.hive.split.file.min.bytes}，
     * 默认 1GB。
     */
    private final long splitFileMinBytes;

    /**
     * CTAS 新建表的物理格式：catalog 属性 {@code catalog.hive.default.format}
     * （归一化后键名 {@code hive.default.format}），默认 ORC。写已有表（INSERT INTO / OVERWRITE）
     * 不看这个值，按目标表自身的格式落文件。
     */
    private final HiveStorageFormat writeFormat;

    /**
     * 桶裁剪开关（catalog 属性 {@code catalog.hive.bucket.pruning}，默认 <b>true</b>）：
     * 分桶表 + 分桶列全部被等值 / {@code IN} 命中时，只读命中桶对应的文件。
     * 拿不准（类型不支持 / 文件名不是桶名 / 缺约束）一律回退全扫，绝不漏读。
     */
    private final boolean bucketPruning;

    /**
     * ACID（事务表）读取开关（catalog 属性 {@code catalog.hive.acid.read}，默认 <b>false</b>）。
     * 关闭时遇到 {@code transactional=true} 的表<b>明确报错</b>（默认行为，防误读错数据）；
     * 开启后走 {@link HiveAcidReader}：按写事务快照过滤 base/delta、解 {@code row} 结构、
     * 按 {@code RecordIdentifier} 做行级删除/更新合并。
     */
    private final boolean acidRead;

    /**
     * 复合类型按原生嵌套结构读取（{@code catalog.hive.nested.mode=native}，默认）还是按文本读取
     * （{@code text}）。决定列声明的 Arrow 类型、文件转换器的填充方式，以及写入时的目标结构。
     */
    private final boolean nestedNative;

    /**
     * 该表实际使用的复合类型呈现方式（见 {@link HiveTableMeta#effectiveNestedNative(boolean)}）：
     * <b>TEXT 格式表恒按文本</b>，ORC / Parquet 才跟随 {@code catalog.hive.nested.mode}。
     */
    private boolean nestedNativeFor(HiveTableMeta meta) {
        return meta.effectiveNestedNative(nestedNative);
    }

    /**
     * {@link #read} 的整表读游标，按线程隔离 —— 同一连接器实例会被多个 Worker 线程并发使用。
     * 带载荷的切片不走这里：{@link #openSplit} 每片给一个独立 reader，游标随 reader 走。
     */
    private final ThreadLocal<ScanReader> cursorLocal = new ThreadLocal<>();

    /**
     * 写会话，<b>按线程隔离</b> —— 连接器实例按进程共享（{@code MetaStore.getConnector}），
     * 而 Worker 任务池真并发跑片段、Master 侧 TableSink 也有 N 条写线程 ⇒ 一个线程一个会话，
     * 否则两个片段会往同一份 ORC 写文件里交错写。
     */
    private final ConcurrentMap<Thread, HiveWriteSession> writeSessions = new ConcurrentHashMap<>();

    /** 慢操作告警阈值：低于它的步骤一声不吭，超过就报 —— 定位「读一个小查询为什么几十秒」用。 */
    private static final long SLOW_STEP_MS = 300L;

    public HiveConnector(HiveVersion version, String catalogName, Map<String, String> properties) {
        this.version = version;
        this.catalogName = catalogName;

        // 键统一按 catalog 规范写（catalog.hive.xxx）；normalize 降级成裸键，旧写法原样保留。
        // hive2 / hive3 共用家族前缀 catalog.hive.，故 family = version.family()。
        Map<String, String> props = CatalogProperties.normalize(version.type(), version.family(),
                properties == null ? Map.of() : properties);
        String uris = firstNonBlank(props.get("hive.metastore.uris"), props.get("metastore.uris"));
        if (uris == null) {
            throw new IllegalArgumentException("Hive catalog '" + catalogName
                    + "' 必须声明 Hive MetaStore 地址，例如 "
                    + "WITH ('catalog.hive.metastore.uris'='thrift://host:9083', "
                    + "'catalog.hive.fs.defaultFS'='hdfs://host:9000')");
        }

        HadoopEnvironment.prepare(props);
        this.conf = HadoopEnvironment.newConfiguration(props);
        this.metastore = new HiveMetastore(catalogName, uris, conf,
                Boolean.parseBoolean(props.getOrDefault("hive.acid.read", "false")), version);
        this.allocator = NodeMemory.shared().newChildAllocator("connector/hive", 0);
        this.batchSize = intProp(props, "hive.batch.size", CommonConfig.DEFAULT_BATCH_SIZE);
        this.partitionLimit = intProp(props, "hive.partition.limit", 10000);
        this.compactTargetBytes = resolveCompactTarget(props);
        long splitMin = parseSizeBytes(props.get(SPLIT_FILE_MIN_KEY));
        this.splitFileMinBytes = splitMin > 0L ? splitMin : DEFAULT_SPLIT_FILE_MIN_BYTES;
        this.writeFormat = resolveWriteFormat(props);
        this.bucketPruning = Boolean.parseBoolean(props.getOrDefault(BUCKET_PRUNING_KEY, "true"));
        this.acidRead = Boolean.parseBoolean(props.getOrDefault(ACID_READ_KEY, "false"));
        this.nestedNative = !"text".equalsIgnoreCase(
                props.getOrDefault(NESTED_MODE_KEY, "native").trim());
    }

    public String getCatalogName() { return catalogName; }
    public HiveVersion getVersion() { return version; }
    public HiveMetastore getMetastore() { return metastore; }

    // ==================== Catalog 级别 ====================

    @Override
    public boolean catalogExists(String catalog) {
        return catalogName.equalsIgnoreCase(catalog);
    }

    @Override
    public Optional<Catalog> getCatalog(String catalog) {
        if (!catalogExists(catalog)) return Optional.empty();
        Map<String, String> props = new LinkedHashMap<>();
        props.put("hive.metastore.uris", metastore.uris());
        props.put("hive.metastore.version", version.shortName());
        return Optional.of(new Catalog(catalogName, version.type(), props, null, null));
    }

    @Override
    public List<String> listCatalogs() {
        return List.of(catalogName);
    }

    // ==================== Schema 级别 ====================

    @Override
    public boolean schemaExists(String catalog, String schema) {
        if (!catalogExists(catalog)) return false;
        return metastore.databaseExists(schema);
    }

    @Override
    public Optional<Schema> getSchema(String catalog, String schema) {
        if (!schemaExists(catalog, schema)) return Optional.empty();
        return Optional.of(new Schema(catalogName, schema, Map.of()));
    }

    @Override
    public List<String> listSchemas(String catalog) {
        if (!catalogExists(catalog)) return List.of();
        return metastore.databases();
    }

    // ==================== Table 级别 ====================

    @Override
    public boolean tableExists(String catalog, String schema, String table) {
        if (!catalogExists(catalog)) return false;
        return metastore.tableExists(schema, table);
    }

    @Override
    public Optional<com.dojao.spi.bean.Table> getTable(String catalog, String schema, String table) {
        Optional<HiveTableMeta> meta = metastore.table(schema, table);
        if (meta.isEmpty()) return Optional.empty();
        HiveTableMeta m = meta.get();
        String comment = m.parameters().get("comment");
        return Optional.of(new com.dojao.spi.bean.Table(catalogName, schema, table,
                m.allColumns(), new LinkedHashMap<>(m.parameters()),
                comment == null || comment.isEmpty() ? null : comment));
    }

    @Override
    public List<Column> getTableColumns(String catalog, String schema, String table) {
        Optional<HiveTableMeta> meta = metastore.table(schema, table);
        return meta.map(HiveTableMeta::allColumns).orElse(List.of());
    }

    @Override
    public List<String> listTables(String catalog, String schema) {
        if (!catalogExists(catalog)) return List.of();
        return metastore.tables(schema);
    }

    /**
     * <b>分区列</b>（保序，与 Hive 分区定义一致）。空 = 非分区表。
     *
     * <p>供分区裁剪使用：内部用 {@link HiveTableMeta#partitionColumns()}（<b>带声明类型</b>，
     * 范围谓词要按类型比）交给 {@link PartitionPruner} 解析 WHERE 谓词，命中则只扫匹配分区。
     * 本 SPI 方法只回名字，够调用方判断「有没有分区列」；裁剪走的是连接器内部那条带类型的路径。
     * 分区列的值不在数据文件里，而在各分区目录的 {@code dt=…} 路径段里。</p>
     */
    @Override
    public List<String> getPartitionColumns(String catalog, String schema, String table) {
        Optional<HiveTableMeta> meta = metastore.table(schema, table);
        return meta.map(HiveTableMeta::partitionKeys).orElse(List.of());
    }

    // ==================== 分区元数据扫描（不读数据文件） ====================

    /**
     * 结构性判定：只有「分区表 + ORC/Parquet + 非事务表」才可能给出精确行数。
     * 文本文件没有 footer（行数只能扫数据才知道），事务表读到的行数 ≠ 文件行数（行级合并）。
     */
    @Override
    public boolean supportsPartitionMetadataScan(String catalog, String schema, String table) {
        Optional<HiveTableMeta> metaOpt = metastore.table(schema, table);
        if (metaOpt.isEmpty()) return false;
        HiveTableMeta meta = metaOpt.get();
        return meta.partitioned() && !meta.transactional()
                && meta.format() != HiveStorageFormat.TEXT;
    }

    /**
     * 一行为一个<b>非空分区</b>：分区列（目录 {@code key=value}，按声明类型物化）+
     * {@link DojaoConnector#PARTITION_ROW_COUNT_COLUMN}（该分区精确行数）。
     *
     * <p>行数<b>只认文件 footer</b>（ORC {@code getNumberOfRows} / Parquet 各 row group 之和），
     * 不用 HMS 的 {@code numRows} 统计 —— 后者是快照估算（未 ANALYZE 时根本不存在，即便存在也可能
     * 过期），{@code count(*)} 会因此算错。任一环节拿不到精确值（目录列举失败 / footer 读失败）→
     * 整页返回 {@code null}，调用方回退真实扫描。<b>绝不返回部分精确的结果。</b></p>
     *
     * <p>分区裁剪与 {@link #planSplits} 同一口径（{@link #prunePartitions}）：谓词命中的分区
     * 之外连目录都不列举。裁剪为空 → 返回 0 行页（不是 null）—— 这正对应「没有分区命中」，
     * 全局聚合据此得到 {@code count(*)=0}。</p>
     */
    @Override
    public Page scanPartitionMetadata(ReadRequest request) {
        Optional<HiveTableMeta> metaOpt = metastore.table(request.schema(), request.table());
        if (metaOpt.isEmpty()) return null;
        HiveTableMeta meta = metaOpt.get();
        if (!meta.partitioned() || meta.transactional() || meta.format() == HiveStorageFormat.TEXT) {
            return null;
        }

        String filter = request.filterExpression().orElse(null);
        List<Partition> parts = metastore.partitions(meta.database(), meta.table(), partitionLimit);
        if (parts.isEmpty()) return null;   // HMS 没登记分区：分区值不可知，回退

        List<Partition> kept = prunePartitions(meta, parts, filter);
        List<String> keys = meta.partitionKeys();
        List<Map<String, String>> values = new ArrayList<>(kept.size());
        List<Long> rows = new ArrayList<>(kept.size());
        for (Partition p : kept) {
            String loc = p.getSd() == null ? null : p.getSd().getLocation();
            if (loc == null || loc.isBlank()) return null;
            List<FileEntry> files = new ArrayList<>();
            try {
                walk(new Path(loc), MAX_WALK_DEPTH, files);
            } catch (IOException e) {
                return null;               // 目录列举失败 → 行数不可知
            }
            long n = files.isEmpty() ? 0L : footerRows(meta, files);
            if (n < 0) return null;        // footer 读失败 → 绝不猜
            if (n == 0) continue;          // 空分区不贡献任何行
            values.add(parsePartitionValues(loc, keys));
            rows.add(n);
        }
        return buildPartitionMetadataPage(meta.partitionFields(nestedNativeFor(meta)), values, rows);
    }

    /** 把「分区值 + 精确行数」物化成 Arrow 页（分区列按声明类型解析，坏值置 NULL）。 */
    private Page buildPartitionMetadataPage(List<Field> partFields,
                                            List<Map<String, String>> values, List<Long> rows) {
        int n = values.size();
        List<Field> fields = new ArrayList<>(partFields.size() + 1);
        fields.addAll(partFields);
        Field rowCountField = new Field(DojaoConnector.PARTITION_ROW_COUNT_COLUMN,
                FieldType.nullable(new ArrowType.Int(64, true)), null);
        fields.add(rowCountField);

        List<FieldVector> vectors = new ArrayList<>(fields.size());
        try {
            for (Field f : partFields) {
                FieldVector v = f.createVector(allocator);
                vectors.add(v);
                v.setInitialCapacity(Math.max(n, 1));
                v.allocateNew();
                for (int r = 0; r < n; r++) {
                    String raw = values.get(r).get(f.getName());
                    if (raw == null || "__HIVE_DEFAULT_PARTITION__".equals(raw)) {
                        v.setNull(r);
                        continue;
                    }
                    try {
                        TextFileArrowConverter.fillValue(v, raw, r);
                    } catch (RuntimeException e) {
                        v.setNull(r);      // 与数据路径一致：坏值整格 NULL，不打断整批
                    }
                }
                v.setValueCount(n);
            }
            BigIntVector rc = (BigIntVector) rowCountField.createVector(allocator);
            vectors.add(rc);
            rc.setInitialCapacity(Math.max(n, 1));
            rc.allocateNew();
            for (int r = 0; r < n; r++) rc.setSafe(r, rows.get(r));
            rc.setValueCount(n);
            return Page.fromRoot(new VectorSchemaRoot(fields, vectors, n));
        } catch (RuntimeException e) {
            for (FieldVector v : vectors) {
                try { v.close(); } catch (RuntimeException ignored) {}
            }
            throw e;
        }
    }

    // ==================== Column 级别 ====================

    @Override
    public boolean columnExists(String catalog, String schema, String table, String column) {
        return getColumn(catalog, schema, table, column).isPresent();
    }

    @Override
    public Optional<Column> getColumn(String catalog, String schema, String table, String column) {
        if (column == null) return Optional.empty();
        for (Column c : getTableColumns(catalog, schema, table)) {
            if (c.name().equalsIgnoreCase(column)) return Optional.of(c);
        }
        return Optional.empty();
    }

    // ==================== 类型映射（CTAS 读方向精确实现） ====================

    @Override
    public Map<String, DojaoType> getColumnTypes(String schema, String table) {
        Optional<HiveTableMeta> meta = metastore.table(schema, table);
        if (meta.isEmpty()) return Map.of();
        Map<String, DojaoType> out = new LinkedHashMap<>();
        boolean nativeNested = nestedNativeFor(meta.get());
        for (Column c : meta.get().allColumns()) {
            // 声明类型必须与实际读出的列一致：文本模式 / TEXT 表里嵌套列读成 Utf8，
            // 若仍声明 LIST/MAP/STRUCT，INSERT 类型校验会与实际页对不上。
            DojaoType type = HiveTypeMapper.toDojaoType(c.dataType());
            out.put(c.name(), nativeNested || !HiveTypeMapper.isStructurallyNested(c.dataType())
                    ? type : DojaoType.STRING);
        }
        return out;
    }

    @Override
    public List<Field> sourceFields(String catalog, String schema, String table, List<String> columns) {
        Optional<HiveTableMeta> meta = metastore.table(schema, table);
        if (meta.isEmpty()) return List.of();
        List<Field> all = meta.get().allFields(nestedNativeFor(meta.get()));
        if (columns == null || columns.isEmpty()) return all;
        List<Field> out = new ArrayList<>(columns.size());
        for (String name : columns) {
            for (Field f : all) {
                if (f.getName().equals(name)) {
                    out.add(f);
                    break;
                }
            }
        }
        return out;
    }

    // ==================== 统计探查 ====================

    /**
     * 行数：优先 Hive 落库统计（表参数 {@code numRows} → 各分区 {@code numRows} 之和），
     * 缺失则读 ORC/Parquet 文件 footer 求和（footer 取数不扫数据，1 亿行也是秒回）。
     * 文本文件无 footer → {@code -1}（未知，框架退默认并行度）。体积恒为各文件字节之和。
     *
     * <p><b>天然分区数 = 读取单元数</b>（{@link TableStats#splitCount()}）：本连接器的分区策略是
     * 「一个文件一片」，常规表上单元数就是文件数，这里顺手报给引擎做并行度第一次决策 ——
     * 文件清单上面已经列出来了，零额外 I/O。少了这个数，10 个 87MB 文件会因
     * 「总量 0.82G ÷ 1G 槽位 = 1」被当成 1 片串扫。</p>
     *
     * <p>两个方向的收敛都算进这个数（见 {@link #planUnits}）：小文件海量时先合并
     * （否则会报出一万个分区，把并行度决策带偏 —— 合并只收敛分区数、不减少 I/O）；
     * 单个超大文件则切开（否则一个 10GB 文件只报 1，并行度封顶 1）。</p>
     */
    @Override
    public TableStats getTableStats(String catalog, String schema, String table) {
        HiveTableMeta meta = requireTable(schema, table);
        if (meta.partitioned()) {
            // ★ 分区表走纯 HMS：分区目录清单 + 各分区统计参数，一个 HDFS 请求都不发。
            //   文件清单 HMS 里没有，从前为了拿体积/行数要递归 HDFS 列遍【所有】分区目录、
            //   再逐个读 ORC/Parquet footer —— 分区一多就是「读一个小查询先卡几十秒」的主因，
            //   而这里连谓词都没有（SPI 不带 filter）→ 列举范围是整个表，代价最不该付。
            //   分区统计缺失（表没做过 ANALYZE）如实报 -1，由引擎退回默认估计，不猜。
            //   注：切片规划（planSplits）那边已经列了【裁剪后的】目录，本方法刻意不跟着列 ——
            //   那会把「元数据探查」重新变成全表列举。
            List<PartitionUnit> units = listPartitionUnits(meta, null);
            return new TableStats(catalogName, schema, table,
                    sumOrUnknown(units, true), sumOrUnknown(units, false),
                    getColumnStats(catalog, schema, table), units.size());
        }

        // 非分区表：表根目录一层列举（代价可控），HMS 参数优先、footer 兜底
        List<FileEntry> files = listDataFiles(meta);
        long bytes;
        if (!files.isEmpty()) {
            bytes = 0L;
            for (FileEntry f : files) bytes += f.length();
        } else {
            bytes = paramLong(meta, "totalSize");
        }
        long rows = paramLong(meta, "numRows");
        if (rows < 0) rows = footerRows(meta, files);
        long splits = files.isEmpty() ? -1L : planUnits(meta, files).size();
        return new TableStats(catalogName, schema, table, rows, bytes,
                getColumnStats(catalog, schema, table), splits);
    }

    /**
     * 谓词感知的行数估算（监控 Scan 进度条的分母）：分区表 → 对<b>裁剪后</b>的分区求
     * {@code numRows} 之和。HMS 缺分区统计则返回 {@code -1}（交回默认整表估计）。
     */
    @Override
    public long estimateRows(String catalog, String schema, String table, String filter) {
        HiveTableMeta meta = requireTable(schema, table);
        if (!meta.partitioned()) return -1L;
        return sumOrUnknown(listPartitionUnits(meta, filter), true);
    }

    /**
     * 谓词感知的<b>字节</b>估算，与 {@link #estimateRows} 严格对称：分区表 → 对<b>裁剪后</b>的分区
     * 求 {@code totalSize} 之和；普通表 → {@code -1}。同样一个 HDFS 请求都不发。
     *
     * <p><b>它比「整表字节 × 行数比例」更可靠</b>：那条路要求 {@code numRows} 在册（未 ANALYZE 的
     * 分区表没有），而 {@code totalSize} 往往有。实测 {@code where dt='2026-09-11'}（30 分区命中 1）
     * 走比例口径时缩不下去，右表被估成 2.4GB 整表 → 选到排序归并；本方法直接给 ~1/30 的真值。</p>
     */
    @Override
    public long estimateBytes(String catalog, String schema, String table, String filter) {
        HiveTableMeta meta = requireTable(schema, table);
        if (!meta.partitioned()) return -1L;
        return sumOrUnknown(listPartitionUnits(meta, filter), false);
    }

    /**
     * join 解压膨胀系数（表级）—— 本连接器报的 {@code byteSize} 是<b>压缩后</b>的物理字节
     * （HDFS 文件长度 / HMS {@code totalSize}），而 join build 侧驻留内存是<b>解压后</b>的 Arrow 向量。
     * 由此交给 {@link HiveCompressionRatio} 按「存储格式 + 压缩算法」查中位压缩比，并与 HMS 里
     * {@code rawDataSize/totalSize} 的实测比取 {@code max}，作为解压膨胀系数，避免大压缩比表被
     * 误判成中表而选到不可溢写的 HashJoin。
     *
     * <p>不读任何数据文件，只查一次 HMS 表元数据。</p>
     */
    @Override
    public double joinSizeExpandRatio(String catalog, String schema, String table) {
        HiveTableMeta meta = requireTable(schema, table);
        return HiveCompressionRatio.expandRatio(meta.format(), meta.parameters());
    }

    /** 分区单元统计求和：任一分区缺该项 → {@code -1}（不部分求和、不猜）。 */
    private static long sumOrUnknown(List<PartitionUnit> units, boolean rows) {
        if (units.isEmpty()) return 0L;
        long sum = 0L;
        for (PartitionUnit u : units) {
            long v = rows ? u.rows() : u.bytes();
            if (v < 0) return -1L;
            sum += v;
        }
        return sum;
    }

    @Override
    public List<ColumnStats> getColumnStats(String catalog, String schema, String table) {
        HiveTableMeta meta = requireTable(schema, table);
        List<String> names = new ArrayList<>();
        for (Column c : meta.allColumns()) names.add(c.name());
        return metastore.columnStats(schema, table, names);
    }

    // ==================== 数据读取 ====================

    /**
     * <b>false —— Hive 侧目前不做谓词过滤。</b>
     *
     * <p>本连接器按文件读取（ORC/Parquet/Text），读取路径只看 {@link ReadRequest} 的分片信息与列投影，
     * <b>不求值 {@code filterExpression}</b>。因此必须如实声明 false，让引擎在扫描之上保留本地
     * {@code Filter} —— 否则规划器下推后消掉本地 Filter，而这里又不执行过滤，查询会静默多返回行。</p>
     *
     * <p>（可选优化：ORC/Parquet 具备按谓词跳过 row group 的能力（SearchArgument），届时实现过滤
     * 后可改回 true，把过滤真正推到文件读取层。）</p>
     */
    @Override
    public boolean supportsPredicatePushdown() {
        return false;
    }

    /**
     * 读一批数据（<b>整表无分片</b>的直调路径）。同一线程连续调用会从上次位置续读；
     * 单元清单 / 投影 / limit 变了就换游标。
     *
     * <p>产出页的列 = Hive 表结构（常规列 + 分区列），列序与 HMS 定义一致；若请求带列投影则按其裁剪。</p>
     *
     * <p>这条路径<b>只在单机回退时走</b>，单个 reader 顺序读，切成多单元也没人并行消费，
     * 因此这里一律用整文件单元（不做文件内切割）。走分片派发的正常路径<b>不经过这里</b>：
     * 引擎拿 {@link #planSplits} 的切片调 {@link #openSplit}，每个切片一个独立 reader。</p>
     */
    @Override
    public Page read(ReadRequest request) {
        HiveTableMeta meta = requireTable(request.schema(), request.table());
        checkReadable(meta);

        List<HiveSplitUnit> units = defaultUnits(meta, request.filterExpression().orElse(null));
        List<String> projection = normalizeProjection(request.columns().orElse(List.of()));
        validateProjection(meta, projection);

        ScanReader reader = cursorLocal.get();
        boolean reusable = reader instanceof HiveScanReader r
                && r.matches(units, projection, request.limit());
        if (!reusable) {
            if (reader != null) reader.close();
            reader = createReader(meta, units, projection, request.limit());
            cursorLocal.set(reader);
        }
        return reader.next();
    }

    @Override
    public void closeRead() {
        ScanReader reader = cursorLocal.get();
        if (reader != null) reader.close();
        cursorLocal.remove();
    }

    /** 按表是否 ACID 造游标：ACID 走行级合并读取器，其余走普通扫描读取器。 */
    private ScanReader createReader(HiveTableMeta meta, List<HiveSplitUnit> units,
                                    List<String> projection, long limit) {
        return isAcid(meta)
                ? new HiveAcidReader(meta, units, projection, limit)
                : new HiveScanReader(meta, units, projection, limit);
    }

    /** 事务表 + 已开启 ACID 读取 → 走 ACID 路径。 */
    private boolean isAcid(HiveTableMeta meta) {
        return meta.transactional() && acidRead;
    }

    // ==================== 并行扫描分片规划 ====================

    /** 直接调用（测试 / 老调用点）：无截断下推。 */
    @Override
    public List<ScanSlice> planSplits(ReadRequest request, int parallelism) {
        return planSplits(request, parallelism, 0L);
    }

    /**
     * 规划分片 —— <b>先「跟着分区信息走」定位目录，再对目录内的文件做合并 / 大文件切割</b>，
     * 最后归并成「并行度上限」片。
     *
     * <p>定位目录这一步分两档：<b>分区表</b>由 HMS 给出<b>已按谓词裁剪</b>的分区目录，Master
     * 只列举这些目录，未命中的分区目录一个都不碰（「把 HMS 当成表的目录」——分区再多也不必让
     * Namenode 递归遍历整表）；HMS 连分区都没登记时才回退表根目录递归列举（最笨但保底）。
     * 普通表没有分区信息可用，直接列举表根。两条路的产物都是同一份文件清单。</p>
     *
     * <p>拿到文件清单后统一进 {@link #planUnits}：小文件按阈值<b>合并</b>（防「一万个小文件 →
     * 一万个分区」），单个超大文件在文件内按 stripe / row group <b>切开</b>（防「一个 10GB 文件 →
     * 并行度封顶 1」）。<b>切分必须发生在 Master 的这一步</b> —— 切片边界在派发前就定死了，
     * 一个切片只会交给一个 Worker 上的一条 reader 串行读；放到 Worker 侧再切，切出来的区段仍由
     * 同一条 reader 顺序读，跨 Worker 并行读大文件就成了空话。</p>
     *
     * <p>再 round-robin 归并成 {@code min(parallelism, 单元数)} 片（既保证并行，又不把 task 数放大到
     * 单元数量级）。每片把自己的单元清单编成私有载荷（{@link HiveSplitPayload}）放进 {@link Split}，
     * Worker 侧 {@link #openSplit} 据此只读属于自己的那部分数据。</p>
     *
     * <p><b>截断下推</b>（{@code fetchLimit > 0}，顶层 LIMIT 的前缀读）：只要结果前缀，因此只列举
     * 「够前缀的那几个文件」并返回<b>一个</b>切片（见 {@link #prefixFiles}）—— 连裁剪后的目录都
     * 不用列全，也不去开那些根本读不到的并行线程。该分支不参与合并、也不做文件内切割：它的分组
     * 口径是「按行数凑够前缀」，且 reader 拿到前缀文件后读够 limit 即自停。</p>
     *
     * <p>返回空列表 = 「不可切分，按整表单片走」（并行度 ≤ 1、带 limit/offset、无数据文件、
     * 前缀文件数不可知），由引擎回退成 {@code ScanSlice.whole}。谓词原样保留在 {@code request} 里
     * —— 切片不借用 {@code filterExpression} 传递私有信息。</p>
     */
    @Override
    public List<ScanSlice> planSplits(ReadRequest request, int parallelism, long fetchLimit) {
        // 截断下推的前提是「读到的行 = 输出的行」，本连接器<b>不执行 filterExpression</b>
        // （supportsPredicatePushdown()=false，谓词由引擎在扫描之上求值）→ 前缀截断拿到的是
        // 「前 N 行里命中的那几行」，而不是「命中的前 N 行」，后面文件里的命中行永远读不到
        // （实测 select * … where content like … limit 500 只回 1 行，全表命中 225 行）→
        // 按契约无视该提示，全量列举；早停交给顶层 LimitOperator（满 N 行即关上游）。
        if (fetchLimit > 0L && request.filterExpression().isPresent()) {
            fetchLimit = 0L;
        }

        // 便宜的先判：不切分就直接返回，连 HMS 都不必探。
        // 带 limit/offset 的请求是数据源分页语义（MySQL 的 LIMIT-OFFSET 兜底分片靠它），本连接器不切分。
        if (fetchLimit <= 0L && (parallelism <= 1 || request.limit() > 0 || request.offset() > 0)) {
            return List.of();
        }

        Optional<HiveTableMeta> metaOpt = metastore.table(request.schema(), request.table());
        if (metaOpt.isEmpty()) return List.of();
        HiveTableMeta meta = metaOpt.get();
        checkReadable(meta);

        // ACID 表走独立分片：一个 (分区, 桶) 一片，片内把该桶全部 base/delta/delete_delta
        // 交给同一条 reader 做行级合并（跨片合并会看不到别片的删除）。
        if (isAcid(meta)) {
            return planAcidSplits(request, meta);
        }

        // 截断下推：与并行度无关，只要「够前缀」的文件，一个切片
        // （`prefixFiles` 内部同样先按分区裁剪定位目录 —— 「跟着分区信息走」不变。
        //   早停另由顶层 LimitOperator 兜底，满 N 行即关闭上游。）
        if (fetchLimit > 0L) {
            List<FileEntry> prefix = prefixFiles(meta, fetchLimit);
            if (prefix == null || prefix.isEmpty()) return List.of();
            List<HiveSplitUnit> units = new ArrayList<>(prefix.size());
            for (FileEntry e : prefix) units.add(HiveSplitUnit.wholeFile(e.path()));
            System.err.printf("[Hive] 截断下推：只需前 %d 行 → 读 %d 个文件%n", fetchLimit, units.size());
            // 切片请求带上 limit：reader 读够即自停（上方 LimitOperator 仍照常兜底 offset/limit）
            return List.of(new ScanSlice(request.withLimit(0L, fetchLimit),
                    new Split(HiveSplitPayload.encode(units))));
        }

        String filter = request.filterExpression().orElse(null);
        // ① 定位目录：分区表走 HMS 裁剪后的分区目录（未命中的目录一个都不列举），
        //    没有分区信息（普通表 / HMS 未登记分区）才回退表根递归列举。
        // ② 目录内文件统一进 planUnits 做合并 / 大文件切割 —— 必须在这一步切开，
        //    否则切片边界已成定局，Worker 侧再切也换不来跨 Worker 并行。
        List<FileEntry> entries = listDataFiles(meta, filter);
        if (entries.isEmpty()) return List.of();
        long unitTarget = unitTarget(entries, parallelism);
        List<List<HiveSplitUnit>> units = planUnits(meta, entries, unitTarget);
        if (units.size() != entries.size()) {
            System.err.printf("[Hive] 单元规划：%d 个文件 → %d 个读取单元（目标单元大小 %d MB）%n",
                    entries.size(), units.size(), unitTarget / 1024 / 1024);
        }

        int groups = Math.min(parallelism, units.size());
        List<List<HiveSplitUnit>> buckets = new ArrayList<>(groups);
        for (int i = 0; i < groups; i++) buckets.add(new ArrayList<>());
        for (int i = 0; i < units.size(); i++) {
            buckets.get(i % groups).addAll(units.get(i));
        }

        List<ScanSlice> parts = new ArrayList<>(groups);
        for (List<HiveSplitUnit> bucket : buckets) {
            parts.add(new ScanSlice(request, new Split(HiveSplitPayload.encode(bucket))));
        }
        // 无条件把「收到的谓词」一并打出：裁剪是静默优化，切片数正常 ≠ 谓词到过连接器
        // （谓词没到 → 该剪的没剪，切片数看起来一样"正常"）。这一行是排查裁剪失效的第一现场。
        System.err.printf("[Hive] 分片：%d 个文件 → %d 个读取单元 → %d 片（并行度上限 %d；谓词: %s）%n",
                entries.size(), units.size(), groups, parallelism, filter == null ? "(无)" : filter);
        return parts;
    }

    /**
     * 把文件整理成 <b>读取单元</b>，两个方向共用同一个目标大小 {@code target}
     * （默认 {@link #compactTargetBytes}，即 {@code dfs.blocksize} = 128MB）；
     * <ol>
     *   <li><b>切开大文件</b>：单个文件 &gt; {@link #splitFileMinBytes}（默认 1GB）时，按格式自带的
     *       索引边界切成若干 ≤ 目标大小的区间（见 {@link #splitBigFile}）；</li>
     *   <li><b>合并小文件</b>：其余文件按顺序装填 —— <b>阈值 = 单元的目标大小上限</b>，
     *       装不下（再加一个就超上限）就封口；单个文件自己就 ≥ 上限 → 独占一个单元。</li>
     * </ol>
     *
     * <p><b>为什么合并是「装到不超」而不是「攒到超过」</b>：后者会把正常大小的文件也并起来。
     * 例：阈值取 {@code dfs.blocksize}=128MB（默认），而 Hive 写出的文件常见就是「差一点满块」的
     * 87.8MB —— 若按「攒到 ≥ 128MB 才封口」，两个 87.8MB 会被并成一个 175MB 单元，
     * 10 个文件从 10 片退化成 5 片，正好把「按文件数定并行度」的好处吃掉。
     * 按「装不下就封口」则 87.8 + 87.8 &gt; 128 → 各自成单元，10 片不变。
     * （与 Spark {@code FilePartition.getFilePartitions} 同一套口径。）</p>
     *
     * <p><b>为什么要有「合并小文件」这一步</b>：本连接器的天然分区数 = 单元数，常规表（十几个大文件）
     * 上正是想要的；但遇到「一万个几 MB 小文件」就会报出一万个分区，把引擎的并行度决策带偏
     * （切片数、线程数、每片元数据开销一起放大）。合并后报出去的是<b>单元数</b>，
     * 常规表上单元数 == 文件数，与「一个文件一片」完全一致。</p>
     *
     * <p><b>为什么要有「切开大文件」这一步</b>：反方向的极端 —— 一个几十 GB 的大文件只报 1 个单元，
     * 引擎把并行度定成 1，全表串行扫。按 stripe / row group 切开后，单文件也能多路并行。
     * 切的是<b>字节区间</b>，不是重新组织数据，零写放大。</p>
     *
     * <p><b>合并的代价（必须知道）</b>：合并只减少「切片 / 分区数」，<b>不减少 I/O</b> —— 每个文件该建的
     * 连接、该读的 footer 一次不少，只是换到同一个线程里顺序做。小文件海量要真正快，
     * 得在 Hive 侧把文件重写大（ORC 表 {@code ALTER TABLE ... CONCATENATE}，或
     * {@code INSERT OVERWRITE} 重写）。（切开大文件没有这个代价：区间是真实并行。）</p>
     *
     * <p>返回顺序继承入参（{@link #listDataFiles} 已按 path 排序）→ 分组可复现。</p>
     *
     * <p>{@code target} 由调用方给定：分片规划用自适应值（{@link #unitTarget}，把「小数据量被
     * 过度合并成 1 个单元」那一端修正掉）；{@link #getTableStats} 报「天然单元数」时用固定值。</p>
     */
    private List<List<HiveSplitUnit>> planUnits(HiveTableMeta meta, List<FileEntry> files) {
        return planUnits(meta, files, compactTargetBytes);
    }

    private List<List<HiveSplitUnit>> planUnits(HiveTableMeta meta, List<FileEntry> files, long target) {
        List<List<HiveSplitUnit>> units = new ArrayList<>();
        List<HiveSplitUnit> pending = new ArrayList<>();
        long pendingBytes = 0L;
        for (FileEntry f : files) {
            if (f.length() > splitFileMinBytes) {
                List<HiveSplitUnit> split = splitBigFile(meta, f, target);
                if (split.size() > 1) {     // 真切开了：每段各自成单元（段本身就接近目标大小）
                    if (!pending.isEmpty()) {   // 先封口，保持单元顺序与文件顺序一致
                        units.add(pending);
                        pending = new ArrayList<>();
                        pendingBytes = 0L;
                    }
                    for (HiveSplitUnit u : split) units.add(List.of(u));
                    continue;
                }
            }
            // 装不下就先封口（含「手上已有文件」的情况）；空单元时无条件收下，
            // 保证单个超大文件也能独占一个单元而不是被丢弃。
            if (!pending.isEmpty() && pendingBytes + f.length() > target) {
                units.add(pending);
                pending = new ArrayList<>();
                pendingBytes = 0L;
            }
            pending.add(HiveSplitUnit.wholeFile(f.path()));
            pendingBytes += f.length();
            if (pendingBytes >= target) {   // 自身就够上限 → 立刻独立成单元
                units.add(pending);
                pending = new ArrayList<>();
                pendingBytes = 0L;
            }
        }
        if (!pending.isEmpty()) units.add(pending);
        return units;
    }

    /**
     * 本次分片规划的<b>读取单元目标大小</b>：默认 {@link #compactTargetBytes}（128MB），但当
     * 「数据总量 ÷ 并行度」明显更小时取后者（下限 {@link #MIN_UNIT_TARGET_BYTES}）。
     *
     * <p><b>为什么必须自适应</b>：目标大小同时是「合并小文件」的上限。固定 128MB 时，一个 82MB 的
     * 小分区（2 个 ~41MB 文件）会被<b>整段合成 1 个单元</b> ⇒ {@code groups = min(并行度, 单元数) = 1}
     * ⇒ 并行度 8 全废、读退化成单线程串行（实测 82MB / 约 10.5s，这就是 54s 查询里的大头）。
     * 按「总量 ÷ 并行度」取值后每个 41MB 文件各自成单元 ⇒ 2 片、2 个 Worker 并行读。</p>
     *
     * <p>大数据量下与固定值完全一致（总量 ≥ {@code compactTargetBytes × 并行度} ⟹
     * {@code min} 取回 {@code compactTargetBytes}），故只修正「小数据量被过度合并」这一端，
     * 常规表的切片口径不变。</p>
     */
    private long unitTarget(List<FileEntry> files, int parallelism) {
        long total = 0L;
        for (FileEntry f : files) total += Math.max(0L, f.length());
        return unitTarget(total, parallelism, compactTargetBytes);
    }

    /**
     * {@link #unitTarget(List, int)} 的纯函数形式（便于离线探针直击取值表）。
     * 取三段：并行度 &le; 1 或总量未知 → 固定目标；否则 {@code clamp(总量 ÷ 并行度, 下限, 固定目标)}。
     */
    static long unitTarget(long totalBytes, int parallelism, long compactTarget) {
        if (parallelism <= 1 || totalBytes <= 0L) return compactTarget;
        return Math.max(MIN_UNIT_TARGET_BYTES, Math.min(compactTarget, totalBytes / parallelism));
    }

    /**
     * 把一个（大）文件切成若干读取单元。
     *
     * <p>不切的情形一律返回<b>单个整文件单元</b> —— 这是「只慢不错」的兜底：</p>
     * <ul>
     *   <li>格式是文本（无 footer 索引；压缩文本更不可切）；</li>
     *   <li>索引读不出来（footer 读失败 → 打一行日志退回）；</li>
     *   <li>切完仍然只有一段（文件里就一个 stripe / row group）。</li>
     * </ul>
     */
    private List<HiveSplitUnit> splitBigFile(HiveTableMeta meta, FileEntry f, long target) {
        if (meta.format() == HiveStorageFormat.TEXT) return List.of(HiveSplitUnit.wholeFile(f.path()));
        List<long[]> ranges;
        try {
            ranges = meta.format() == HiveStorageFormat.ORC
                    ? orcSplitRanges(f.path(), target)
                    : parquetSplitRanges(f.path(), f.length(), target);
        } catch (Exception e) {
            System.err.println("[Hive] 大文件切割失败，退回整文件读取: " + f.path() + "（" + e + "）");
            return List.of(HiveSplitUnit.wholeFile(f.path()));
        }
        if (ranges.size() <= 1) return List.of(HiveSplitUnit.wholeFile(f.path()));
        List<HiveSplitUnit> out = new ArrayList<>(ranges.size());
        for (long[] r : ranges) out.add(new HiveSplitUnit(f.path(), r[0], r[1] - r[0]));
        return out;
    }

    /**
     * ORC：按 <b>stripe 边界</b>分组，每组的字节跨度 ≤ 目标单元大小。stripe 的偏移就在 footer 里，
     * 读 footer 即可，不扫数据（与 {@link #orcRowCount} 同一条路径）。
     */
    private List<long[]> orcSplitRanges(String file, long target) throws IOException {
        Path path = new Path(file);
        try (Reader reader = OrcFile.createReader(path,
                OrcFile.readerOptions(conf).filesystem(path.getFileSystem(conf)))) {
            List<StripeInformation> stripes = reader.getStripes();
            if (stripes.size() <= 1) return List.of();
            long[] segStart = new long[stripes.size()];
            long[] segEnd = new long[stripes.size()];
            for (int i = 0; i < stripes.size(); i++) {
                segStart[i] = stripes.get(i).getOffset();
                segEnd[i] = segStart[i] + stripes.get(i).getLength();
            }
            return groupByBytes(segStart, segEnd, target);
        }
    }

    /**
     * Parquet：按 <b>row group 边界</b>分组。
     *
     * <p>段尾不取 {@code getCompressedSize()}（那是估算，字典页/索引页未必都算在内），而是取
     * 「<b>下一个 row group 的起始偏移</b>」，末段取文件长度 —— 这样 {@code [start, start+length)}
     * 与读取侧的选取条件（{@code startingPos ∈ [start, start+length)}）严格对齐，多一段不多、
     * 少一段不少。</p>
     */
    private List<long[]> parquetSplitRanges(String file, long fileLength, long target) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(
                HadoopInputFile.fromPath(new Path(file), conf))) {
            List<BlockMetaData> blocks = reader.getRowGroups();
            if (blocks.size() <= 1) return List.of();
            long[] segStart = new long[blocks.size()];
            long[] segEnd = new long[blocks.size()];
            for (int i = 0; i < blocks.size(); i++) {
                segStart[i] = blocks.get(i).getStartingPos();
                long end = i + 1 < blocks.size()
                        ? blocks.get(i + 1).getStartingPos() : fileLength;
                segEnd[i] = Math.max(end, segStart[i] + 1L);   // 防御：段必须非空
            }
            return groupByBytes(segStart, segEnd, target);
        }
    }

    /**
     * 把顺序分段（{@code segStart} 严格递增、{@code segEnd[i] >= segStart[i]}）按
     * 「本组字节跨度 ≤ {@code target}」贪心分组，返回每组的 {@code [start, end)}。
     *
     * <p>单段自己就超 {@code target} → 自成一组（不丢数据）；段与段首尾相接（{@code segEnd[i]}
     * 即 {@code segStart[i+1]}）时分组结果就是「若干连续段」。</p>
     */
    private static List<long[]> groupByBytes(long[] segStart, long[] segEnd, long target) {
        List<long[]> out = new ArrayList<>();
        int i = 0;
        while (i < segStart.length) {
            long start = segStart[i];
            int j = i;
            while (j + 1 < segStart.length && segEnd[j + 1] - start <= target) j++;
            out.add(new long[]{start, segEnd[j]});
            i = j + 1;
        }
        return out;
    }

    /** 小文件合并阈值：catalog 属性 {@code catalog.hive.compact.target.bytes} → {@code dfs.blocksize} → 128MB。 */
    private long resolveCompactTarget(Map<String, String> props) {
        long configured = parseSizeBytes(props.get(COMPACT_TARGET_KEY));
        if (configured > 0L) return configured;
        long blockSize = conf.getLong(DFS_BLOCK_SIZE_KEY, DEFAULT_COMPACT_TARGET_BYTES);
        return blockSize > 0L ? blockSize : DEFAULT_COMPACT_TARGET_BYTES;
    }

    /**
     * CTAS 新表的物理格式：catalog 属性 {@code catalog.hive.default.format}
     * （可选 {@code orc} / {@code parquet} / {@code text}，{@code textfile} 等价 text），默认 ORC。
     *
     * <p>无法识别的取值构造期直接报错 —— 宁可 catalog 建不出来，也不要写出意料之外的数据。</p>
     */
    private static HiveStorageFormat resolveWriteFormat(Map<String, String> props) {
        String raw = firstNonBlank(props.get(WRITE_FORMAT_KEY), props.get("default.format"));
        if (raw == null) return HiveStorageFormat.ORC;
        return HiveStorageFormat.fromCatalogValue(raw);
    }

    /** 解析 {@code 64M} / {@code 1G} / {@code 8388608}；空或非法返回 {@code -1}（语义同 ResourceConfig）。 */
    private static long parseSizeBytes(String raw) {
        if (raw == null || raw.isBlank()) return -1L;
        String v = raw.trim();
        long mult = 1L;
        if (v.endsWith("B") || v.endsWith("b")) v = v.substring(0, v.length() - 1).trim();
        if (!v.isEmpty()) {
            char unit = Character.toUpperCase(v.charAt(v.length() - 1));
            if (unit == 'K') {
                mult = 1024L;
                v = v.substring(0, v.length() - 1);
            } else if (unit == 'M') {
                mult = 1024L * 1024;
                v = v.substring(0, v.length() - 1);
            } else if (unit == 'G') {
                mult = 1024L * 1024 * 1024;
                v = v.substring(0, v.length() - 1);
            }
        }
        try {
            return (long) (Double.parseDouble(v.trim()) * mult);
        } catch (NumberFormatException e) {
            return -1L;
        }
    }

    /**
     * 截断下推的「前缀文件」：从表的第一份数据开始按序收文件，直到累计行数 ≥ {@code fetchLimit}
     * 为止 —— 只读够用的部分，<b>且保证够用</b>。
     *
     * <p>为什么要按行数收而不是「只取第一个文件」：{@code SELECT ... LIMIT n} 的结果要靠扫描把
     * 前 n 行喂给上方 {@code LimitOperator}，若只给一个文件而它没有 n 行，查询会少返回行 ——
     * 那是静默错数据。行数取 ORC/Parquet 的 footer（不扫数据，秒回），以文件为单位只增不减地累加。</p>
     *
     * <p>行数不可知（文本文件无 footer、footer 读失败）→ 返回 {@code null}：宁可退回整表列举
     * （多读，但不读少）。</p>
     */
    private List<FileEntry> prefixFiles(HiveTableMeta meta, long fetchLimit) {
        if (meta.format() == HiveStorageFormat.TEXT) return null;
        List<FileEntry> acc = new ArrayList<>();
        long rows = 0L;
        for (String loc : dataLocations(meta, null)) {
            List<FileEntry> files = new ArrayList<>();
            try {
                walk(new Path(loc), MAX_WALK_DEPTH, files);
            } catch (IOException e) {
                throw new UncheckedIOException("列举 Hive 表数据目录失败: " + loc, e);
            }
            files.sort(Comparator.comparing(FileEntry::path));   // 与全量列举同一顺序 → 前缀稳定
            for (FileEntry f : files) {
                long t0 = System.nanoTime();
                long n = meta.format() == HiveStorageFormat.ORC
                        ? orcRowCount(f.path()) : parquetRowCount(f.path());
                logSlow("读 footer 行数（截断下推）", System.nanoTime() - t0, f.path());
                if (n < 0) return null;     // 行数不可知 → 不敢截断
                acc.add(f);
                rows += n;
                if (rows >= fetchLimit) return acc;
            }
        }
        // 全表都数不满 fetchLimit：整表读完也就这么多行，交给 reader 读全部（结果正确）
        return acc.isEmpty() ? null : acc;
    }

    /**
     * 为一个切片开独立读游标。reader 自己持有游标（文件进度 / 投影 / limit），
     * 因此同一连接器实例可以被多个线程并发开 reader 而互不干扰 —— 这正是
     * {@link ScanReader} 相对「连接器自身持有游标 + {@code read()}」的意义所在。
     */
    @Override
    public ScanReader openSplit(ReadRequest request, Split split) {
        HiveTableMeta meta = requireTable(request.schema(), request.table());
        checkReadable(meta);
        List<HiveSplitUnit> units = split != null && split.hasPayload()
                ? HiveSplitPayload.decode(split.payload())
                : defaultUnits(meta, request.filterExpression().orElse(null));
        List<String> projection = normalizeProjection(request.columns().orElse(List.of()));
        validateProjection(meta, projection);
        return createReader(meta, units, projection, request.limit());
    }

    // ==================== 写（CTAS / INSERT INTO / INSERT OVERWRITE） ====================
    //
    // 写已有表时物理格式跟随目标表（meta.format()）：ORC / Parquet / LazySimpleSerDe 分隔文本；
    // 新建表（CTAS）用 `catalog.hive.default.format`。数据先写进表目录下的
    // `_dojao_tmp_<uuid>/`（分区表再按 `<k>=<v>/` 分子目录），全部写完才 mv 到最终位置 ——
    // 半成品不对外可见。
    //
    // 目标表名有两种：
    //   · 真实表名（CTAS / INSERT INTO）→ 会话收尾即 mv 入位；
    //   · staging 标识 `真实表名|staging目录`（INSERT OVERWRITE，由 beginOverwrite 产出）
    //     → 会话只 flush，搬运留给 commitOverwrite（要等全部 Worker 写完才动原表）。

    @Override
    public void write(WriteRequest request) {
        if (!catalogExists(request.catalog())) {
            throw new IllegalArgumentException("Catalog '" + request.catalog() + "' does not exist");
        }
        VectorSchemaRoot root = request.data() == null ? null : request.data().getRoot();
        if (root == null || root.getRowCount() == 0) return;

        String target = request.table();
        int sep = target.indexOf(STAGING_SEP);
        String realTable = sep < 0 ? target : target.substring(0, sep);
        String stagingDir = sep < 0 ? null : target.substring(sep + 1);

        HiveTableMeta meta = requireTable(request.schema(), realTable);
        checkWritableTarget(meta);
        // 目标列名（INSERT INTO t(a,b)）：页列按位置对应这些名字，否则用页自己的列名
        List<String> pageColumns = request.targetColumns();
        if (pageColumns != null && pageColumns.size() != root.getSchema().getFields().size()) {
            pageColumns = null;   // 数量不符 → 退回按页列名（宁可报「缺列」也不错位写）
        }
        try {
            sessionFor(meta, stagingDir).write(root, pageColumns);
        } catch (IOException e) {
            throw new RuntimeException("写 Hive 表失败: " + meta.database() + "." + realTable, e);
        }
    }

    /**
     * 关闭<b>当前线程</b>的写会话（Worker 侧 {@code FragmentExecutor.runAndSink} 结束时调）。
     * 不碰别的线程的会话 —— 并发的兄弟片段还在写，误收尾会把它的数据丢在半路。
     */
    @Override
    public void closeWrite() {
        HiveWriteSession s = writeSessions.remove(Thread.currentThread());
        if (s != null) s.close();
    }

    /**
     * 写语句整体收尾（Master 侧 {@code TableSinkOperator.doClose} 调，此时写线程都已停）：
     * 把本连接器里所有线程的会话都收掉。Worker 路径不走这里（各片段各收各的，见 {@link #closeWrite}）。
     */
    @Override
    public void finishWrite() {
        closeWrite();
        for (Thread t : new ArrayList<>(writeSessions.keySet())) {
            HiveWriteSession s = writeSessions.remove(t);
            if (s != null) s.close();
        }
    }

    /**
     * 写入前的目标表护栏（fail-loud，宁可报错也不写坏表）：
     * <ul>
     *   <li><b>ACID 事务表</b>：拒绝 —— 写出非 ACID 文件会破坏事务表；</li>
     *   <li><b>文本格式（TEXTFILE）</b>：只接受 LazySimpleSerDe，且<b>拒绝嵌套类型列</b> ——
     *       分隔文本没有结构信息，array/map/struct 写进去 Hive 读回来只是字符串；</li>
     *   <li><b>分桶表</b>：校验每个分桶列类型可哈希（{@code date}/{@code timestamp}/{@code decimal}/
     *       复杂类型暂不支持写入分桶），否则报错而不是把行散进错误的桶。</li>
     * </ul>
     */
    static void checkWritableTarget(HiveTableMeta meta) {
        if (meta.transactional()) {
            throw new UnsupportedOperationException("暂不支持写入 Hive ACID 事务表: "
                    + meta.database() + "." + meta.table());
        }
        if (meta.format() == HiveStorageFormat.TEXT) {
            String serde = meta.serdeLib() == null ? "" : meta.serdeLib();
            // 文本写侧产出的是 LazySimpleSerDe 口径（分隔符 / 转义 / \N）；其它 SerDe（RegexSerDe /
            // OpenCSVSerde / JsonSerDe …）读法不同，写进去只会是「Hive 读不回来的数据」→ 明确拒绝。
            if (!serde.isEmpty() && !serde.endsWith("LazySimpleSerDe")) {
                throw new UnsupportedOperationException("Hive 文本表写入仅支持 LazySimpleSerDe"
                        + "（TEXTFILE 默认），目标表 " + meta.database() + "." + meta.table()
                        + " 使用 " + serde + "；如需该格式请改用 Parquet / ORC 目标表");
            }
            // 分隔文本没有结构信息：array/map/struct 只能写成字符串，LazySimpleSerDe 读回来仍是字符串。
            // 与其静默写「读不回来的数据」，不如在写入前明确拒绝。
            for (Column c : meta.dataColumns()) {
                if (HiveTypeMapper.isNested(c.dataType())) {
                    throw new UnsupportedOperationException("Hive 文本表（TEXTFILE）不支持写入嵌套类型列 '"
                            + c.name() + "'（" + c.dataType() + "）：分隔文本没有结构信息，写进去 Hive "
                            + "读回来只是一段字符串。请改用 ORC / Parquet 目标表"
                            + "（CTAS 可设 catalog.hive.default.format=orc|parquet）");
                }
            }
        }
        if (meta.bucketed()) {
            List<String> cols = meta.bucketCols();
            List<String> types = meta.bucketColumnTypes();
            for (int i = 0; i < types.size(); i++) {
                if (!HiveBucketHash.isHashable(types.get(i))) {
                    throw new UnsupportedOperationException("分桶列 " + cols.get(i) + "(" + types.get(i)
                            + ") 类型暂不支持写入分桶表: " + meta.database() + "." + meta.table());
                }
            }
        }
    }

    /** 取当前线程的写会话；没有就新建（并顺手清掉所属线程已死的旧会话）。 */
    private HiveWriteSession sessionFor(HiveTableMeta meta, String stagingDir) {
        Thread self = Thread.currentThread();
        HiveWriteSession existing = writeSessions.get(self);
        if (existing != null) return existing;

        writeSessions.keySet().removeIf(t -> t != self && !t.isAlive());   // 失败/取消残留的会话
        FileSystem fs = fsOf(meta.location());
        String tempDir;
        boolean moveOnClose;
        if (stagingDir != null) {
            tempDir = stagingDir;
            moveOnClose = false;
        } else {
            tempDir = new Path(new Path(meta.location()), HiveWriteSession.newTempDirName()).toString();
            moveOnClose = true;
        }
        HiveWriteSession created = new HiveWriteSession(conf, fs, meta, metastore, tempDir, moveOnClose,
                nestedNativeFor(meta));
        HiveWriteSession raced = writeSessions.putIfAbsent(self, created);
        return raced != null ? raced : created;
    }

    // ==================== 建表 / 删表（CTAS） ====================

    /**
     * CTAS 建表：HMS 登记表（格式由 {@code catalog.hive.default.format} 决定：ORC / Parquet / 文本）
     * + 建 HDFS 目录（Hive MANAGED TABLE 的惯例目录 {@code <db.location>/<table>}）。
     *
     * <p>表已存在：{@code ifNotExists} → 返回 false（调用方跳过写入，不误插旧表）；否则让 HMS
     * 抛标准「already exists」错误（与 MySQL/PG 连接器同一语义）。</p>
     */
    @Override
    public boolean createTable(String catalog, String schema, String table,
                               List<Field> fields, boolean ifNotExists) {
        if (tableExists(catalog, schema, table)) {
            if (ifNotExists) return false;
        }
        // CTAS 落在文本表上时提前拒绝：文本格式无法表达 array/map/struct，
        // 否则会先建出一张空表、等写入阶段才失败。
        if (writeFormat == HiveStorageFormat.TEXT) {
            for (Field f : fields) {
                if (isNestedField(f)) {
                    throw new UnsupportedOperationException("CTAS 目标格式为文本（catalog.hive.default.format=text），"
                            + "不支持嵌套类型列 '" + f.getName() + "'（array/map/struct）；"
                            + "请把 catalog.hive.default.format 设为 orc 或 parquet");
                }
            }
        }
        String location = new Path(new Path(databaseLocation(schema)), table).toString();
        Table t = new Table();
        t.setDbName(schema);
        t.setTableName(table);
        t.setOwner(System.getProperty("HADOOP_USER_NAME", System.getProperty("user.name", "dojao")));
        t.setCreateTime((int) (System.currentTimeMillis() / 1000));
        t.setLastAccessTime(0);
        t.setRetention(0);
        t.setTableType("MANAGED_TABLE");
        t.setSd(tableSd(location, fields));
        t.setPartitionKeys(new ArrayList<>());
        t.setParameters(new LinkedHashMap<>(Map.of("EXTERNAL", "FALSE")));
        metastore.createTable(t);

        try {
            fsOf(location).mkdirs(new Path(location));
        } catch (IOException e) {
            // 目录建不出来 → 回滚元数据，别留一张没有目录的空表
            try { metastore.dropTable(schema, table); } catch (Throwable ignored) { }
            throw new RuntimeException("创建 Hive 表目录失败: " + location, e);
        }
        System.err.printf("[Hive] CTAS 建表 %s.%s（%s，location=%s）%n", schema, table, writeFormat, location);
        return true;
    }

    @Override
    public void dropTable(String catalog, String schema, String table) {
        String location = metastore.table(schema, table).map(HiveTableMeta::location).orElse(null);
        metastore.dropTable(schema, table);
        if (location != null && !location.isBlank()) {
            try {
                fsOf(location).delete(new Path(location), true);
            } catch (IOException e) {
                System.err.println("[Hive] 删表目录失败（元数据已删）: " + location + " —— " + e);
            }
        }
    }

    @Override
    public void truncateTable(String catalog, String schema, String table) {
        String location = metastore.table(schema, table).map(HiveTableMeta::location).orElse(null);
        if (location == null || location.isBlank()) {
            throw new IllegalStateException(
                    "Hive 表不存在或缺少 location，无法 TRUNCATE: " + schema + "." + table);
        }
        try {
            FileSystem fs = fsOf(location);
            Path dir = new Path(location);
            // 只清空表目录内容、保留目录本身（表结构 / 分区登记不动）；表目录不存在视为已空。
            if (fs.exists(dir)) {
                FileStatus[] children = fs.listStatus(dir);
                if (children != null) {
                    for (FileStatus child : children) {
                        fs.delete(child.getPath(), true);
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to truncate Hive table: " + schema + "." + table, e);
        }
    }

    /** Arrow 字段是否是嵌套类型（array / map / struct，含 fixed-size list）。 */
    private static boolean isNestedField(Field field) {
        ArrowType type = field.getType();
        return type instanceof ArrowType.List || type instanceof ArrowType.FixedSizeList
                || type instanceof ArrowType.Map || type instanceof ArrowType.Struct;
    }

    private StorageDescriptor tableSd(String location, List<Field> fields) {
        List<FieldSchema> cols = new ArrayList<>(fields.size());
        for (Field f : fields) {
            cols.add(new FieldSchema(f.getName(), HiveTypeMapper.arrowToHiveType(f), null));
        }
        SerDeInfo serde = new SerDeInfo();
        serde.setSerializationLib(writeFormat.serdeLib());
        serde.setParameters(new HashMap<>(writeFormat.serdeParameters()));
        StorageDescriptor sd = new StorageDescriptor();
        sd.setCols(cols);
        sd.setLocation(location);
        sd.setInputFormat(writeFormat.inputFormat());
        sd.setOutputFormat(writeFormat.outputFormat());
        sd.setSerdeInfo(serde);
        sd.setNumBuckets(-1);
        sd.setBucketCols(new ArrayList<>());
        sd.setSortCols(new ArrayList<>());
        sd.setParameters(new HashMap<>());
        return sd;
    }

    /** 库的物理目录（HMS 登记优先；没登记 → 按 Hive 惯例 {@code <fs.defaultFS>/user/hive/warehouse/<db>.db}）。 */
    private String databaseLocation(String schema) {
        Optional<Database> db = metastore.rawDatabase(schema);
        if (db.isPresent()) {
            String loc = db.get().getLocationUri();
            if (loc != null && !loc.isBlank()) return loc;
        }
        String base = conf.get("fs.defaultFS", "file:///");
        if (!base.endsWith("/")) base = base + "/";
        return base + "user/hive/warehouse/" + schema + ".db";
    }

    // ==================== INSERT OVERWRITE（OverwriteEnable） ====================

    /**
     * 能力探针：真实地在表目录下建一个 throwaway 子目录再删掉 —— 反映角色授权的<b>有效</b>写权限
     * （比查权限表可靠）；只碰临时对象，目标表数据一动不动。
     */
    @Override
    public boolean supportsOverwrite(String catalog, String schema, String table) {
        HiveTableMeta meta = metastore.table(schema, table).orElse(null);
        if (meta == null || meta.location() == null || meta.location().isBlank()) return false;
        try {
            FileSystem fs = fsOf(meta.location());
            Path probe = new Path(new Path(meta.location()), "_dojao_probe_" + shortUuid());
            fs.mkdirs(probe);
            fs.delete(probe, true);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 建覆盖用 staging：表目录下一个临时目录。返回 {@code 真实表名|staging目录} ——
     * Worker 侧只有这个字符串，从中既能还原真实表名（取分区列定义）又能定位 staging 目录。
     */
    @Override
    public String beginOverwrite(String catalog, String schema, String table) {
        HiveTableMeta meta = requireTable(schema, table);
        if (meta.location() == null || meta.location().isBlank()) {
            throw new IllegalStateException("INSERT OVERWRITE 目标表无 location: "
                    + schema + "." + table);
        }
        checkWritableTarget(meta);
        String staging = new Path(new Path(meta.location()), HiveWriteSession.newTempDirName()).toString();
        try {
            fsOf(staging).mkdirs(new Path(staging));
        } catch (IOException e) {
            throw new IllegalStateException("创建 INSERT OVERWRITE staging 目录失败: " + staging, e);
        }
        System.err.printf("[Hive] INSERT OVERWRITE staging: %s（表 %s.%s）%n", staging, schema, table);
        return table + STAGING_SEP + staging;
    }

    /**
     * 收齐全部 N 分区写入后把 staging 切进目标表。
     *
     * <p><b>非分区表</b>：删掉表目录下的旧数据文件（跳过 {@code _}/{@code .} 开头的临时对象），
     * 再把 staging 里的文件搬上来 —— 表目录本身与 staging 目录都在其中，不会自删。
     * <b>分区表</b>：只覆盖这次真正写到的分区（逐个分区删旧数据 + 搬新数据 + 更新 HMS 分区），
     * 没落到新数据的分区原样保留（与 Hive 动态分区 OVERWRITE 一致）。</p>
     */
    @Override
    public void commitOverwrite(String catalog, String schema, String table, String staging) {
        int sep = staging.indexOf(STAGING_SEP);
        String realTable = sep < 0 ? table : staging.substring(0, sep);
        String stagingDir = sep < 0 ? staging : staging.substring(sep + 1);
        HiveTableMeta meta = requireTable(schema, realTable);
        Path stagingPath = new Path(stagingDir);
        Path location = new Path(meta.location());
        try {
            FileSystem fs = fsOf(meta.location());
            fs.mkdirs(location);
            if (!meta.partitioned()) {
                deleteDataFiles(fs, location);
                moveFilesInDir(fs, stagingPath, location);
            } else {
                FileStatus[] sub = fs.listStatus(stagingPath);
                for (FileStatus s : sub) {
                    if (!s.isDirectory()) continue;
                    Path dest = new Path(location, s.getPath().getName());
                    fs.delete(dest, true);
                    fs.mkdirs(dest);
                    moveFilesInDir(fs, s.getPath(), dest);
                    upsertPartition(meta, dest);
                }
            }
            fs.delete(stagingPath, true);
        } catch (IOException e) {
            throw new RuntimeException("INSERT OVERWRITE 提交失败: " + schema + "." + realTable, e);
        }
        System.err.printf("[Hive] INSERT OVERWRITE 提交完成: %s.%s%n", schema, realTable);
    }

    /** 覆盖失败/取消：删 staging 目录，原表从未被触碰。幂等。 */
    @Override
    public void abortOverwrite(String catalog, String schema, String table, String staging) {
        int sep = staging.indexOf(STAGING_SEP);
        String stagingDir = sep < 0 ? staging : staging.substring(sep + 1);
        try {
            fsOf(stagingDir).delete(new Path(stagingDir), true);
        } catch (IOException e) {
            System.err.println("[Hive] 清理 INSERT OVERWRITE staging 失败: " + stagingDir + " —— " + e);
        }
    }

    /** 删掉目录下的一层数据文件（保留子目录与 {@code _}/{@code .} 开头的临时对象）。 */
    private static void deleteDataFiles(FileSystem fs, Path dir) throws IOException {
        for (FileStatus s : fs.listStatus(dir)) {
            if (s.isFile() && isDataFileName(s.getPath().getName())) {
                fs.delete(s.getPath(), false);
            }
        }
    }

    private static void moveFilesInDir(FileSystem fs, Path dir, Path dest) throws IOException {
        for (FileStatus f : fs.listStatus(dir)) {
            if (!f.isFile()) continue;
            Path dst = new Path(dest, f.getPath().getName());
            if (!fs.rename(f.getPath(), dst)) {
                throw new IOException("移动 Hive 文件失败: " + f.getPath() + " → " + dst);
            }
        }
    }

    /** 由分区目录名（{@code k=v/...}）解析分区值并登记 / 更新 HMS 分区。 */
    private void upsertPartition(HiveTableMeta meta, Path partitionDir) {
        List<String> values = new ArrayList<>(meta.partitionColumns().size());
        for (String segment : partitionDir.toString().split("/")) {
            int eq = segment.indexOf('=');
            if (eq <= 0) continue;
            values.add(HiveWriteSession.unescapePathName(segment.substring(eq + 1)));
        }
        if (values.size() != meta.partitionColumns().size()) return;

        Partition p = new Partition();
        p.setDbName(meta.database());
        p.setTableName(meta.table());
        p.setValues(values);
        p.setSd(partitionSd(meta, partitionDir.toString()));
        p.setParameters(new HashMap<>());
        metastore.upsertPartition(p);
    }

    private static StorageDescriptor partitionSd(HiveTableMeta meta, String location) {
        List<FieldSchema> cols = new ArrayList<>();
        for (Column c : meta.dataColumns()) {
            cols.add(new FieldSchema(c.name(), c.dataType(), c.comment()));
        }
        HiveStorageFormat format = meta.format();
        SerDeInfo serde = new SerDeInfo();
        serde.setSerializationLib(meta.serdeLib() == null ? format.serdeLib() : meta.serdeLib());
        serde.setParameters(serdeParams(meta, format));
        StorageDescriptor sd = new StorageDescriptor();
        sd.setCols(cols);
        sd.setLocation(location);
        sd.setInputFormat(meta.inputFormat() == null ? format.inputFormat() : meta.inputFormat());
        sd.setOutputFormat(format.outputFormat());
        sd.setSerdeInfo(serde);
        if (meta.bucketed()) {
            sd.setNumBuckets(meta.numBuckets());
            sd.setBucketCols(new ArrayList<>(meta.bucketCols()));
            sd.setSortCols(sortOrders(meta.sortCols()));
        } else {
            sd.setNumBuckets(-1);
            sd.setBucketCols(new ArrayList<>());
            sd.setSortCols(new ArrayList<>());
        }
        sd.setParameters(new HashMap<>());
        return sd;
    }

    /**
     * 分区 SD 的 SerDe 参数：ORC / Parquet 只需 {@code serialization.format=1}；文本表沿用
     * <b>目标表真实的</b>分隔符 / 转义 / NULL 串（不能拿默认值覆盖，否则读回自己写的数据会错位）。
     */
    private static Map<String, String> serdeParams(HiveTableMeta meta, HiveStorageFormat format) {
        Map<String, String> params = new LinkedHashMap<>(format.serdeParameters());
        if (format == HiveStorageFormat.TEXT) {
            char delim = meta.textDelimiter();
            params.put("field.delim", String.valueOf(delim));
            params.put("serialization.format", String.valueOf(delim));
            params.put("line.delim", "\n");
            params.put("escape.delim", String.valueOf(meta.textEscape()));
            if (meta.textNullString() != null) {
                params.put("serialization.null.format", meta.textNullString());
            }
        }
        return params;
    }

    /** {@code SORTED BY} 列名 → HMS {@link Order}（升序，Hive 默认）。 */
    private static List<Order> sortOrders(List<String> cols) {
        List<Order> out = new ArrayList<>(cols.size());
        for (String c : cols) out.add(new Order(c, 1));
        return out;
    }

    private static String shortUuid() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    }

    private FileSystem fsOf(String location) {
        try {
            Path p = new Path(location);
            return p.getFileSystem(conf);
        } catch (IOException e) {
            throw new UncheckedIOException("取 Hive 数据目录文件系统失败: " + location, e);
        }
    }

    // ==================== 列对齐（文件列序 / 列名 → Hive 表定义） ====================

    /**
     * 把文件页的列按<b>位置</b>对齐到 Hive 表定义（Hive 自己就是这么读的）：
     * 文件列少 → 补 NULL（列是后加的，老文件没有）；文件列多 → 丢弃。列名与类型一致时零拷贝直通。
     */
    private Page alignToTableSchema(HiveTableMeta meta, Page filePage) {
        List<Field> hiveFields = meta.dataFields(nestedNativeFor(meta));
        List<FieldVector> srcVecs = filePage.getRoot().getFieldVectors();
        if (sameInOrder(srcVecs, hiveFields)) return filePage;

        int rows = filePage.getRowCount();
        List<FieldVector> out = new ArrayList<>(hiveFields.size());
        for (int i = 0; i < hiveFields.size(); i++) {
            Field field = hiveFields.get(i);
            if (i >= srcVecs.size()) {
                out.add(nullVector(field, rows));
                continue;
            }
            FieldVector src = srcVecs.get(i);
            if (!src.getField().getType().equals(field.getType())) {
                throw new IllegalStateException("Hive 列类型与数据文件不一致: 表列 " + field.getName()
                        + " = " + field.getType() + "，文件列 = " + src.getField().getType()
                        + "（文件: " + meta.table() + "）");
            }
            FieldVector dst = field.createVector(src.getAllocator());
            src.makeTransferPair(dst).transfer();      // 零拷贝：只换列名/列序，不搬数据
            if (rows > 0 && dst instanceof BaseVariableWidthVector bv) bv.setLastSet(rows - 1);
            dst.setValueCount(rows);
            out.add(dst);
        }
        filePage.close();     // 源向量已被 transfer 掏空
        return Page.fromRoot(new VectorSchemaRoot(hiveFields, out, rows));
    }

    private static boolean sameInOrder(List<FieldVector> srcVecs, List<Field> hiveFields) {
        if (srcVecs.size() != hiveFields.size()) return false;
        for (int i = 0; i < hiveFields.size(); i++) {
            FieldVector v = srcVecs.get(i);
            Field f = hiveFields.get(i);
            if (!v.getField().getName().equals(f.getName())) return false;
            if (!v.getField().getType().equals(f.getType())) return false;
        }
        return true;
    }

    private FieldVector nullVector(Field field, int rows) {
        FieldVector v = field.createVector(allocator);
        v.setInitialCapacity(rows);
        v.allocateNew();
        for (int r = 0; r < rows; r++) v.setNull(r);
        v.setValueCount(rows);
        return v;
    }

    // ==================== 分区列物化 ====================

    /**
     * 分区列的值来自文件所在目录的 {@code key=value} 路径段，按列类型解析后整列填成常量。
     * 返回的新页接管数据页的向量（数据页不再单独 close）。
     */
    private Page attachPartitionColumns(HiveTableMeta meta, String filePath, Page page) {
        if (!meta.partitioned()) return page;

        List<Field> partFields = meta.partitionFields(nestedNativeFor(meta));
        Map<String, String> values = parsePartitionValues(filePath, meta.partitionKeys());
        List<Field> fields = new ArrayList<>(page.getRoot().getSchema().getFields());
        List<FieldVector> vectors = new ArrayList<>(page.getRoot().getFieldVectors());
        int rows = page.getRowCount();

        for (Field field : partFields) {
            fields.add(field);
            vectors.add(partitionConstantVector(field, values.get(field.getName()), rows));
        }
        return Page.fromRoot(new VectorSchemaRoot(fields, vectors, rows));
    }

    /**
     * 把一个分区列物化成整列常量向量：值来自文件路径里的 {@code key=value} 段，按列类型解析。
     * 值缺失 / {@code __HIVE_DEFAULT_PARTITION__} / 解析失败 → 整列 NULL（一行坏值不打断整批）。
     */
    private FieldVector partitionConstantVector(Field field, String raw, int rows) {
        FieldVector v = field.createVector(allocator);
        v.setInitialCapacity(rows);
        v.allocateNew();
        boolean isNull = raw == null || "__HIVE_DEFAULT_PARTITION__".equals(raw);
        for (int r = 0; r < rows; r++) {
            if (isNull) {
                v.setNull(r);
                continue;
            }
            try {
                TextFileArrowConverter.fillValue(v, raw, r);
            } catch (RuntimeException e) {
                v.setNull(r);
            }
        }
        v.setValueCount(rows);
        return v;
    }

    // ==================== 页归一化（无投影全列 / 有投影按名挑列） ====================

    /**
     * 文件原始页 → 表结构页。两条路径：
     * <ul>
     *   <li><b>无投影</b>（{@code projection} 空）→ 旧的「位置对齐 + 分区列整列补齐」，
     *       {@link #alignToTableSchema} 一行未改，行为与从前完全一致；</li>
     *   <li><b>有投影</b> → {@link #projectAndAttach}，按<b>名字</b>从文件页取列（因为投影已把
     *       文件读取器裁到只剩这几列，位置口径不再成立），分区列从路径补，输出严格按投影顺序。</li>
     * </ul>
     */
    private Page normalizePage(HiveTableMeta meta, String filePath, Page raw, List<String> projection) {
        if (projection.isEmpty()) {
            return attachPartitionColumns(meta, filePath, alignToTableSchema(meta, raw));
        }
        return projectAndAttach(meta, filePath, raw, projection);
    }

    /**
     * 带列投影时的归一化：<b>按名取列</b>（投影把文件读取器裁到只剩数据列，位置对齐不再适用）
     * + 分区列从文件路径物化 + 输出严格按 {@code projection} 顺序（与旧 {@code Page.select} 一致）。
     *
     * <p>与 {@link #alignToTableSchema} 一样做<b>类型一致性校验</b>：不一致即抛
     * {@code Hive 列类型与数据文件不一致} —— 这是 ORC 物理类型与 HMS 声明类型不符的既有安全网，
     * 换条路径也绝不能丢。</p>
     *
     * <p>文件页里缺的列（Hive 表后加、老文件没有）→ 整列补 NULL，和位置对齐路径同一语义。</p>
     */
    private Page projectAndAttach(HiveTableMeta meta, String filePath, Page filePage,
                                  List<String> projection) {
        int rows = filePage.getRowCount();
        Map<String, FieldVector> byName = new LinkedHashMap<>();
        for (FieldVector v : filePage.getRoot().getFieldVectors()) {
            byName.putIfAbsent(v.getField().getName(), v);
        }
        Map<String, Field> dataByName = fieldsByName(meta.dataFields(nestedNativeFor(meta)));
        Map<String, Field> partByName = fieldsByName(meta.partitionFields(nestedNativeFor(meta)));
        Map<String, String> partValues = meta.partitioned()
                ? parsePartitionValues(filePath, meta.partitionKeys()) : Map.of();

        List<Field> outFields = new ArrayList<>(projection.size());
        List<FieldVector> outVecs = new ArrayList<>(projection.size());
        for (String name : projection) {
            Field pField = partByName.get(name);
            if (pField != null) {
                outFields.add(pField);
                outVecs.add(partitionConstantVector(pField, partValues.get(name), rows));
                continue;
            }
            Field dField = dataByName.get(name);
            if (dField == null) {
                throw new IllegalArgumentException("Hive 表 " + meta.database() + "." + meta.table()
                        + " 中不存在列: " + name);
            }
            FieldVector src = byName.get(name);
            if (src == null) {
                outFields.add(dField);
                outVecs.add(nullVector(dField, rows));
                continue;
            }
            if (!src.getField().getType().equals(dField.getType())) {
                throw new IllegalStateException("Hive 列类型与数据文件不一致: 表列 " + dField.getName()
                        + " = " + dField.getType() + "，文件列 = " + src.getField().getType()
                        + "（文件: " + meta.table() + "）");
            }
            FieldVector dst = dField.createVector(allocator);
            src.makeTransferPair(dst).transfer();      // 零拷贝：只换列名/列序，不搬数据
            if (rows > 0 && dst instanceof BaseVariableWidthVector bv) bv.setLastSet(rows - 1);
            dst.setValueCount(rows);
            outFields.add(dField);
            outVecs.add(dst);
        }
        filePage.close();     // 源向量已被 transfer 掏空
        return Page.fromRoot(new VectorSchemaRoot(outFields, outVecs, rows));
    }

    private static Map<String, Field> fieldsByName(List<Field> fields) {
        Map<String, Field> out = new LinkedHashMap<>();
        for (Field f : fields) out.putIfAbsent(f.getName(), f);
        return out;
    }

    /** 投影里属于「数据列」的那些（文件读取器只认数据列；分区列从路径补）。 */
    private static List<String> dataProjection(HiveTableMeta meta, List<String> projection) {
        if (projection.isEmpty()) return List.of();
        Set<String> data = new HashSet<>();
        for (Column c : meta.dataColumns()) data.add(c.name());
        List<String> out = new ArrayList<>();
        for (String n : projection) {
            if (data.contains(n)) out.add(n);
        }
        return out;
    }

    /** 从文件路径提取分区列值：{@code .../dt=2026-09-19/city=beijing/part-00000}。 */
    static Map<String, String> parsePartitionValues(String filePath, List<String> keys) {
        Map<String, String> out = new HashMap<>();
        if (filePath == null || keys.isEmpty()) return out;
        Set<String> wanted = new HashSet<>(keys);
        for (String segment : filePath.split("/")) {
            int eq = segment.indexOf('=');
            if (eq <= 0) continue;
            String key = segment.substring(0, eq);
            if (!wanted.contains(key) || out.containsKey(key)) continue;
            out.put(key, unescapePathName(segment.substring(eq + 1)));
        }
        return out;
    }

    /** Hive 落盘时会把分区值里的特殊字符 %XX 转义，读回时按需还原。 */
    private static String unescapePathName(String value) {
        if (value.indexOf('%') < 0) return value;
        try {
            return URLDecoder.decode(value, StandardCharsets.UTF_8);
        } catch (Exception e) {
            return value;
        }
    }

    // ==================== 目录定位（HMS 分区 → 数据文件） ====================

    /**
     * 默认读取单元（无切片载荷时用：单机回退 {@link #read} 与 {@link #openSplit} 的兜底）：
     * 一律整文件单元（这条路径由一个 reader 顺序读，切成多单元也没人并行消费）。
     * 文件清单的定位口径与 {@link #planSplits} 相同 —— 分区表先经 HMS 裁剪。
     */
    private List<HiveSplitUnit> defaultUnits(HiveTableMeta meta, String filterExpression) {
        if (isAcid(meta)) {
            List<HiveSplitUnit> units = new ArrayList<>();
            for (HiveAcidLayout.BucketGroup g : acidGroups(meta, filterExpression)) {
                for (HiveAcidLayout.AcidFile f : g.files()) units.add(HiveSplitUnit.wholeFile(f.path()));
            }
            return units;
        }
        return listDataFiles(meta, filterExpression).stream()
                .map(f -> HiveSplitUnit.wholeFile(f.path())).toList();
    }

    /** 归一化列投影：空 / 含 {@code *} → 不投影（全列），与 {@code Page.select} 的口径一致。 */
    private static List<String> normalizeProjection(List<String> projection) {
        if (projection == null || projection.isEmpty()) return List.of();
        for (String p : projection) {
            if ("*".equals(p)) return List.of();
        }
        return List.copyOf(projection);
    }

    /** 一个分区定位单元：分区目录 + 该分区的 HMS 统计（缺失为 {@code -1}）。 */
    private record PartitionUnit(String location, long rows, long bytes) {
    }

    /**
     * 分区表的<b>目录定位</b> = HMS 登记的分区目录（经 {@link PartitionPruner} 按谓词裁剪）。
     *
     * <p>这是「把 HMS 当成表的目录」的落点：文件清单 HMS 里没有，但<b>目录</b>有 —— 拿到目录即可
     * 直接定位数据，不必让 Namenode 去递归遍历（分区多时正是最慢的一步）。列出来的目录紧接着由
     * {@link #listDataFiles} 就地列举文件，再进 {@link #planUnits} 做合并 / 大文件切割。</p>
     *
     * <p>各分区的 {@code numRows}/{@code totalSize} 只在 Hive 侧做过 ANALYZE（或
     * {@code hive.stats.autogather} 生效）时存在，缺失如实报 {@code -1}，由调用方按「未知」处理。</p>
     *
     * <p>与前身 {@code dataLocations} 同一条安全线：<b>只有「HMS 一个分区都没登记」才回退表根目录</b>
     * （「最笨但保底」的兜底列举）；「有分区、但被裁剪剪空」绝不能回退 —— 那会把整表重扫一遍，
     * 裁剪白做。</p>
     */
    private List<PartitionUnit> listPartitionUnits(HiveTableMeta meta, String filterExpression) {
        if (!meta.partitioned()) {
            return meta.location() == null ? List.of() : List.of(new PartitionUnit(meta.location(), -1L, -1L));
        }
        long t0 = System.nanoTime();
        List<Partition> parts = metastore.partitions(meta.database(), meta.table(), partitionLimit);
        logSlow("HMS 列举分区", System.nanoTime() - t0,
                meta.database() + "." + meta.table() + " 共 " + parts.size() + " 个");

        List<Partition> kept = prunePartitions(meta, parts, filterExpression);

        List<PartitionUnit> out = new ArrayList<>(kept.size());
        for (Partition p : kept) {
            String loc = p.getSd() == null ? null : p.getSd().getLocation();
            if (loc == null || loc.isBlank()) continue;
            Map<String, String> params = p.getParameters();
            out.add(new PartitionUnit(loc, paramLong(params, "numRows"), paramLong(params, "totalSize")));
        }
        if (out.isEmpty() && parts.isEmpty() && meta.location() != null) {
            System.err.println("[Hive] HMS 未登记分区，回退扫描表根目录: " + meta.location()
                    + "（可在 Hive 侧执行 MSCK REPAIR TABLE 登记分区）");
            out.add(new PartitionUnit(meta.location(), -1L, -1L));
        }
        return out;
    }

    // ==================== 文件列举 ====================

    /** 一个数据文件条目（包可见：供同包离线探针构造，验证桶裁剪）。 */
    record FileEntry(String path, long length) {
    }

    /**
     * 本表全部数据文件（分区表 → 各分区目录；未登记分区 → 回退扫表根目录并递归）。
     *
     * <p>列举失败<b>不做兜底</b>：早先这里是「打一行日志、返回空清单」，结果是 HDFS 不通时
     * 查询安静地返回 0 行，用户以为是没数据。现在直接抛 —— 连不上数据源就该报错。
     * （目录不存在是另一回事，{@link #walk} 内部已按空目录处理。）</p>
     */
    private List<FileEntry> listDataFiles(HiveTableMeta meta) {
        return listDataFiles(meta, null);
    }

    /**
     * 本表数据文件（分区表 → 各分区目录；未登记分区 → 回退扫表根目录并递归）。
     *
     * <p>{@code filterExpression} 用于<b>分区裁剪</b>：谓词命中分区列时只列举匹配分区对应的目录，
     * 其余分区连目录都不列。传 {@code null} 表示不裁剪（统计探查 / 无谓词场景）。</p>
     *
     * <p>列举失败<b>不做兜底</b>：早先这里是「打一行日志、返回空清单」，结果是 HDFS 不通时
     * 查询安静地返回 0 行，用户以为是没数据。现在直接抛 —— 连不上数据源就该报错。
     * （目录不存在是另一回事，{@link #walk} 内部已按空目录处理。）</p>
     */
    private List<FileEntry> listDataFiles(HiveTableMeta meta, String filterExpression) {
        List<FileEntry> out = new ArrayList<>();
        for (String loc : dataLocations(meta, filterExpression)) {
            long t0 = System.nanoTime();
            try {
                walk(new Path(loc), MAX_WALK_DEPTH, out);
            } catch (IOException e) {
                throw new UncheckedIOException("列举 Hive 表数据目录失败: " + loc, e);
            }
            logSlow("HDFS 列举目录", System.nanoTime() - t0, loc);
        }
        out.sort(Comparator.comparing(FileEntry::path));   // 稳定顺序 → 分片可复现
        return applyBucketPruning(bucketPruning, meta, filterExpression, out);
    }

    // ==================== 桶裁剪（分桶表 CLUSTERED BY） ====================

    /**
     * <b>桶裁剪</b> —— 分桶表 + 分桶列<b>全部</b>被等值 / {@code IN} 命中时，只保留命中桶对应的文件。
     *
     * <p>与分区裁剪同源（都走 {@link PartitionPruner} 解析谓词），落点则在<b>文件清单</b>上：
     * 分区裁剪剪的是目录，桶裁剪剪的是目录内的桶文件（{@code 000000_0} 之类）。谓词里若不是
     * <b>每个</b>分桶列都有等值 / {@code IN}（如只有一个分桶列被约束、或只有 {@code >} 范围、
     * 或 {@code OR}）→ 桶号无法唯一确定 → <b>不裁剪</b>（回退全扫）。</p>
     *
     * <p><b>安全铁律：拿不准一律回退全扫。</b>类型不支持（{@link HiveBucketHash} 对 {@code date} /
     * {@code decimal} 等返回 {@code null}）、值解析不出、文件名解析不出桶号 —— 任何一处不确定就
     * 原样返回全量文件。多读几个桶只是慢，读错桶就是<b>静默丢数据</b>。</p>
     */
    static List<FileEntry> applyBucketPruning(boolean enabled, HiveTableMeta meta, String filter,
                                              List<FileEntry> files) {
        if (!enabled || files.isEmpty() || !meta.bucketed()) return files;
        if (filter == null || filter.isBlank()) return files;

        List<String> bucketCols = meta.bucketCols();
        List<String> bucketTypes = meta.bucketColumnTypes();
        List<PartitionPruner.PartitionColumn> cols = new ArrayList<>(bucketCols.size());
        for (int i = 0; i < bucketCols.size(); i++) {
            cols.add(new PartitionPruner.PartitionColumn(bucketCols.get(i), bucketTypes.get(i)));
        }
        Map<String, PartitionPruner.ColumnConstraint> constraints =
                PartitionPruner.extract(filter, cols);

        // 每个分桶列都必须有等值 / IN 约束（allowed 非空），否则桶号不唯一 → 不裁。
        List<Set<String>> allowedPerCol = new ArrayList<>(bucketCols.size());
        for (String col : bucketCols) {
            PartitionPruner.ColumnConstraint c = findConstraint(constraints, col);
            Set<String> allowed = c == null ? null : c.allowedValues();
            if (allowed == null || allowed.isEmpty()) {
                System.err.printf("[Hive] 桶裁剪：分桶列 %s 缺等值/IN 约束（谓词: %s）→ 不裁剪，%d 个文件全扫%n",
                        col, filter, files.size());
                return files;
            }
            allowedPerCol.add(allowed);
        }

        // 分桶列取值笛卡尔积 → 命中桶号集合；任一组合哈希不出（类型不支持/值非法）→ 不裁。
        Set<Integer> hitBuckets = new HashSet<>();
        if (!collectHitBuckets(allowedPerCol, bucketTypes, meta.numBuckets(), hitBuckets)) {
            System.err.printf("[Hive] 桶裁剪：分桶列 %s 类型/取值不支持哈希（谓词: %s）→ 不裁剪，%d 个文件全扫%n",
                    bucketCols, filter, files.size());
            return files;
        }

        // 过滤：任一文件名解析不出桶号 → 回退全扫（保底，绝不漏读）。
        List<FileEntry> kept = new ArrayList<>(files.size());
        for (FileEntry f : files) {
            Integer b = bucketIdOf(f.path());
            if (b == null) {
                System.err.printf("[Hive] 桶裁剪：文件名解析不出桶号（%s）→ 不裁剪，%d 个文件全扫%n",
                        f.path(), files.size());
                return files;
            }
            if (hitBuckets.contains(b)) kept.add(f);
        }
        System.err.printf("[Hive] 桶裁剪：%d 个文件 → %d 个（分桶列 %s；命中桶 %s；谓词: %s）%n",
                files.size(), kept.size(), bucketCols, hitBuckets, filter);
        return kept;
    }

    /** 大小写不敏感取约束。 */
    private static PartitionPruner.ColumnConstraint findConstraint(
            Map<String, PartitionPruner.ColumnConstraint> constraints, String column) {
        for (Map.Entry<String, PartitionPruner.ColumnConstraint> e : constraints.entrySet()) {
            if (e.getKey().equalsIgnoreCase(column)) return e.getValue();
        }
        return null;
    }

    /** 分桶列逗号组合 → 命中桶号集合；任一组合哈希不出返回 {@code false}（调用方回退全扫）。 */
    private static boolean collectHitBuckets(List<Set<String>> allowedPerCol, List<String> types,
                                             int numBuckets, Set<Integer> out) {
        return cartesian(allowedPerCol, 0, types, new ArrayList<>(), numBuckets, out);
    }

    private static boolean cartesian(List<Set<String>> allowedPerCol, int idx, List<String> types,
                                     List<String> combo, int numBuckets, Set<Integer> out) {
        if (idx == allowedPerCol.size()) {
            Integer h = HiveBucketHash.bucketHash(types, combo);
            if (h == null) return false;               // 类型不支持 → 整体放弃
            out.add(HiveBucketHash.bucketNumber(h, numBuckets));
            return true;
        }
        for (String v : allowedPerCol.get(idx)) {
            combo.add(v);
            boolean ok = cartesian(allowedPerCol, idx + 1, types, combo, numBuckets, out);
            combo.remove(combo.size() - 1);
            if (!ok) return false;
        }
        return true;
    }

    /**
     * 桶文件名 → 桶号。现代布局 {@code <bucketId>_<copyId>}（如 {@code 000000_0}）取前缀数字；
     * 旧布局支持文件名 / 父目录名 {@code bucket_<N>}。解析不出返回 {@code null}（调用方不裁剪）。
     */
    static Integer bucketIdOf(String path) {
        int slash = path.lastIndexOf('/');
        String name = slash < 0 ? path : path.substring(slash + 1);
        Integer modern = parseBucketPrefix(name);      // 000000_0 → 0
        if (modern != null) return modern;
        Integer oldFile = parseBucketTag(name);        // bucket_00000
        if (oldFile != null) return oldFile;
        if (slash > 0) {                               // .../bucket_5/<file>
            int pslash = path.lastIndexOf('/', slash - 1);
            String parent = path.substring(pslash + 1, slash);
            Integer oldDir = parseBucketTag(parent);
            if (oldDir != null) return oldDir;
        }
        return null;
    }

    /** {@code <digits>_<rest>} → 前导桶号；否则 null。 */
    private static Integer parseBucketPrefix(String name) {
        int us = name.indexOf('_');
        if (us <= 0) return null;
        String head = name.substring(0, us);
        return allDigits(head) ? parseIntSafe(head) : null;
    }

    /** {@code bucket_<digits>} → 桶号；否则 null。 */
    private static Integer parseBucketTag(String name) {
        String prefix = "bucket_";
        if (!name.startsWith(prefix)) return null;
        String num = name.substring(prefix.length());
        return allDigits(num) ? parseIntSafe(num) : null;
    }

    private static boolean allDigits(String s) {
        if (s.isEmpty()) return false;
        for (int i = 0; i < s.length(); i++) {
            if (!Character.isDigit(s.charAt(i))) return false;
        }
        return true;
    }

    private static Integer parseIntSafe(String s) {
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    // ==================== ACID 布局与分片 ====================

    /**
     * ACID 表的分片规划：<b>一个 (分区目录, 桶号) 一片</b>。
     *
     * <p>为什么不能沿用普通表的「文件级并行」：行级合并要按 {@code RecordIdentifier
     * =(originalTransaction, bucketId, rowId)} 把同一行的多条记录（insert / update / delete）
     * 归并到一条，而一个桶的记录可能分散在 base 与多个 delta/delete_delta 文件里 —— 把这些文件
     * 拆给不同分片，各片都只看到自己那份，删除看不到、更新覆盖不到，结果必错。因此一片之内必须
     * 把该桶<b>全部</b>文件交给同一条 reader。</p>
     */
    private List<ScanSlice> planAcidSplits(ReadRequest request, HiveTableMeta meta) {
        List<HiveAcidLayout.BucketGroup> groups = acidGroups(meta, request.filterExpression().orElse(null));
        if (groups.isEmpty()) {
            System.err.printf("[Hive] ACID 分片：%s.%s 无可见数据文件%n", meta.database(), meta.table());
            return List.of();
        }
        List<ScanSlice> parts = new ArrayList<>(groups.size());
        int fileCount = 0;
        for (HiveAcidLayout.BucketGroup g : groups) {
            List<HiveSplitUnit> units = new ArrayList<>(g.files().size());
            for (HiveAcidLayout.AcidFile f : g.files()) units.add(HiveSplitUnit.wholeFile(f.path()));
            fileCount += units.size();
            parts.add(new ScanSlice(request, new Split(HiveSplitPayload.encode(units))));
        }
        System.err.printf("[Hive] ACID 分片：%d 个文件 → %d 个 (分区,桶) 分片%n", fileCount, parts.size());
        return parts;
    }

    /**
     * 枚举当前读快照下可见的 ACID 文件，并按 (分区目录, 桶号) 分组。
     *
     * <p>每个分区目录下的 {@code base_<id>} / {@code delta_<min>_<max>_<stmt>} /
     * {@code delete_delta_*} 子目录按当前读快照（{@link HiveAcidLayout.Visibility}：Hive3 用写 id
     * {@link ValidReaderWriteIdList}，Hive2 用事务 id）过滤 —— 未提交 / 已回滚的事务不可见；
     * 直接躺在分区目录下的非事务遗留文件（无 ACID 列）归为
     * {@link HiveAcidLayout.Kind#ORIGINAL}，它们不可能被删除/更新，原样并入最前。</p>
     */
    private List<HiveAcidLayout.BucketGroup> acidGroups(HiveTableMeta meta, String filterExpression) {
        HiveAcidLayout.Visibility visible = acidVisibility(meta);
        List<HiveAcidLayout.AcidFile> files = new ArrayList<>();
        for (String loc : dataLocations(meta, filterExpression)) {
            listAcidFiles(loc, visible, files);
        }
        List<HiveAcidLayout.BucketGroup> groups = HiveAcidLayout.groupByBucket(files);
        System.err.printf("[Hive] ACID 读快照：%s；%d 个文件 → %d 个桶组%n",
                visible.describe(), files.size(), groups.size());
        return groups;
    }

    /**
     * 依 catalog 版本取 ACID 读快照 —— Hive2 目录名里的 id 是<b>事务 id</b>（走
     * {@code get_open_txns_info} 的 {@link HiveAcidLayout.Visibility#txns}），Hive3 是<b>写 id</b>
     * （走 {@code getValidWriteIds}）。
     */
    private HiveAcidLayout.Visibility acidVisibility(HiveTableMeta meta) {
        if (metastore.hive2()) {
            return HiveAcidLayout.Visibility.txns(metastore.validTxns());
        }
        return HiveAcidLayout.Visibility.writeIds(metastore.validWriteIds(meta.database(), meta.table()));
    }

    /** 列举一个分区目录下的 ACID 文件（按读快照过滤），写进 {@code out}。 */
    private void listAcidFiles(String dir, HiveAcidLayout.Visibility visible,
                               List<HiveAcidLayout.AcidFile> out) {
        FileSystem fs;
        FileStatus[] statuses;
        try {
            Path p = new Path(dir);
            fs = p.getFileSystem(conf);
            statuses = fs.listStatus(p);
        } catch (FileNotFoundException e) {
            return;   // 目录还不存在 / 已被清理 → 当空
        } catch (IOException e) {
            throw new UncheckedIOException("列举 Hive ACID 目录失败: " + dir, e);
        }
        List<Path> subDirs = new ArrayList<>();
        for (FileStatus s : statuses) {
            String name = s.getPath().getName();
            if (s.isDirectory()) {
                if (name.startsWith(".") || name.startsWith(HiveAcidLayout.VISIBILITY_PREFIX)) continue;
                subDirs.add(s.getPath());
            } else if (s.isFile() && s.getLen() > 0 && isDataFileName(name)) {
                // 直接躺在分区目录下的文件 = 非事务时代的遗留数据（ORIGINAL）
                out.add(new HiveAcidLayout.AcidFile(dir, s.getPath().toString(), s.getLen(),
                        HiveAcidLayout.Kind.ORIGINAL, 0L));
            }
        }
        for (Path sub : subDirs) {
            String dn = sub.getName();
            HiveAcidLayout.Kind kind;
            long writeId;
            Long baseW = HiveAcidLayout.baseWriteId(dn);
            long[] range = HiveAcidLayout.deltaRange(dn);
            if (baseW != null) {
                if (!visible.baseVisible(baseW)) continue;
                kind = HiveAcidLayout.Kind.BASE;
                writeId = baseW;
            } else if (range != null) {
                if (!visible.deltaVisible(range[0], range[1])) continue;
                kind = HiveAcidLayout.Kind.DELTA;
                writeId = range[1];
            } else {
                continue;   // 未知子目录（不是 ACID 布局）→ 跳过
            }
            try {
                for (FileStatus f : fs.listStatus(sub)) {
                    if (!f.isFile() || f.getLen() <= 0) continue;
                    if (!isDataFileName(f.getPath().getName())) continue;
                    out.add(new HiveAcidLayout.AcidFile(dir, f.getPath().toString(), f.getLen(),
                            kind, writeId));
                }
            } catch (FileNotFoundException ignored) {
                // 子目录刚被清理 → 跳过
            } catch (IOException e) {
                throw new UncheckedIOException("列举 Hive ACID 增量目录失败: " + sub, e);
            }
        }
    }

    private List<String> dataLocations(HiveTableMeta meta, String filterExpression) {
        List<String> locs = new ArrayList<>();
        for (PartitionUnit u : listPartitionUnits(meta, filterExpression)) locs.add(u.location());
        return locs;
    }

    /**
     * <b>分区裁剪</b> —— 按 {@code filterExpression} 里的分区列约束过滤分区清单。
     *
     * <p>约束的识别全部交给 {@link PartitionPruner}：等值 / {@code IN} / {@code !=} / 范围
     * （{@code < <= > >=}、{@code BETWEEN}）都认；范围按分区列的<b>声明类型</b>比较（数值列按数值、
     * {@code date}/{@code timestamp} 按时间、字符串按字典序），类型未知或值解析不出则<b>不剪</b>。</p>
     *
     * <p>识别不出任何分区列约束（无谓词 / 非分区列 / {@code OR}·函数等）→ <b>原样返回</b>，绝不误剪。
     * 某分区列约束自相矛盾（如 {@code dt='a' AND dt='b'}）→ 没有任何分区能满足 → 剪成 0（真·0 行）。</p>
     *
     * <p><b>有谓词就一定会打一行日志</b>（命中 / 未命中分区列都打）—— 裁剪是个静默优化，
     * 出问题时最难判的就是「谓词到底有没有传到连接器」。因此这里把<b>收到的谓词原文</b>、
     * 解析结果与分区数变化一起打出来：<b>没看到这行 = 谓词压根没到</b>（多半是 Master 跑着旧
     * 连接器，`HiveConnector` 常驻 {@code MetaStore.connectors}，换 jar 必须重启进程）。</p>
     */
    private List<Partition> prunePartitions(HiveTableMeta meta, List<Partition> parts, String filterExpression) {
        // 无谓词（统计探查 / 无 WHERE）静默返回：每次规划都会走到，打日志只是噪声。
        if (filterExpression == null || filterExpression.isBlank()) return parts;
        List<String> keys = meta.partitionKeys();
        if (parts.isEmpty()) {
            System.err.printf("[Hive] 分区裁剪：HMS 未登记任何分区（谓词: %s）→ 无从裁剪%n", filterExpression);
            return parts;
        }
        Map<String, PartitionPruner.ColumnConstraint> constraints =
                PartitionPruner.extract(filterExpression, pruningColumns(meta));
        if (constraints.isEmpty()) {
            System.err.printf("[Hive] 分区裁剪：谓词未命中分区列 %s（谓词: %s）→ 不裁剪，%d 个分区全扫%n",
                    keys, filterExpression, parts.size());
            return parts;
        }

        List<Partition> kept = pruneKept(meta, parts, filterExpression);
        System.err.printf("[Hive] 分区裁剪：%d 个分区 → %d 个（谓词: %s；命中: %s）%n",
                parts.size(), kept.size(), filterExpression, constraints);
        return kept;
    }

    /**
     * {@link #prunePartitions} 的<b>无日志</b>内核：判定到「保留下来的分区清单」为止。
     * 无谓词 / HMS 无分区 / 谓词未命中分区列 → 原样返回（绝不误剪）。
     *
     * <p>判定逻辑全在 {@link PartitionPruner}（引擎侧通用能力，任何有分区的数据源共用，见其类注释）；
     * 这里只把 Hive 的<b>位置型</b>分区值（{@code Partition.getValues()} 按 partitionKeys 顺序）
     * 适配成「列名 → 值」。</p>
     */
    private List<Partition> pruneKept(HiveTableMeta meta, List<Partition> parts, String filterExpression) {
        Map<String, PartitionPruner.ColumnConstraint> constraints =
                PartitionPruner.extract(filterExpression, pruningColumns(meta));
        if (constraints.isEmpty()) return parts;
        return PartitionPruner.prune(parts,
                p -> PartitionPruner.byColumnOrder(meta.partitionKeys(), p.getValues()), constraints);
    }

    /** 分区列 → 裁剪器输入。<b>必须带声明类型</b>：范围谓词按类型决定比较口径（数值 vs 字符串）。 */
    private static List<PartitionPruner.PartitionColumn> pruningColumns(HiveTableMeta meta) {
        List<PartitionPruner.PartitionColumn> out = new ArrayList<>(meta.partitionColumns().size());
        for (Column c : meta.partitionColumns()) {
            out.add(new PartitionPruner.PartitionColumn(c.name(), c.dataType()));
        }
        return out;
    }

    /**
     * <b>Hive 侧「兑现」声明</b>：把 {@link #prunePartitions} 已按之裁剪分区的合取项报给引擎，让扫描
     * 节点免于在 {@code FilterOperator} 里把这些分区谓词再求值一遍（本连接器不兑现谓词下推，谓词本该
     * 全由引擎求值，其中分区列那部分其实是多余的）。
     *
     * <p><b>安全口径与 {@link #listPartitionUnits} 的回退前提严格对齐</b>：只有「HMS 登记了分区、且
     * 确实按这些分区裁剪过、剪完还剩分区」才声张兑现 —— HMS 一个分区都没登记时本连接器回退扫表根目录
     * （等于没裁）或剪成 0 个分区（结果本就 0 行），此时一律不报，否则会把谓词整串丢掉、静默多返回行。</p>
     */
    @Override
    public List<String> partitionEnforcedConjuncts(ReadRequest request, List<String> conjuncts) {
        if (conjuncts == null || conjuncts.isEmpty()) return List.of();
        Optional<String> filterOpt = request.filterExpression();
        if (filterOpt.isEmpty() || filterOpt.get().isBlank()) return List.of();
        Optional<HiveTableMeta> metaOpt = metastore.table(request.schema(), request.table());
        if (metaOpt.isEmpty() || !metaOpt.get().partitioned()) return List.of();
        HiveTableMeta meta = metaOpt.get();

        List<Partition> parts;
        try {
            parts = metastore.partitions(meta.database(), meta.table(), partitionLimit);
        } catch (RuntimeException e) {
            return List.of();                    // 列举不了分区 → 无从谈兑现
        }
        if (parts.isEmpty()) return List.of();   // ★与 listPartitionUnits 的回退前提一致：没分区 = 没裁过

        String filter = filterOpt.get();
        List<Partition> kept = pruneKept(meta, parts, filter);
        if (kept.isEmpty()) return List.of();    // 剪成 0 个 → 保守不声张（此时结果本就 0 行）

        List<String> enforced = PartitionPruner.enforcedConjuncts(filter, pruningColumns(meta),
                conjuncts, kept, p -> PartitionPruner.byColumnOrder(meta.partitionKeys(), p.getValues()));
        if (!enforced.isEmpty()) {
            System.err.printf("[Hive] 分区裁剪兑现：%d/%d 项谓词无需引擎再求值 %s（谓词: %s）%n",
                    enforced.size(), conjuncts.size(), enforced, filter);
        }
        return enforced;
    }

    /**
     * 递归收集目录下的数据文件；本层已有数据文件就不再往下走（分区目录 / 桶目录两种布局都覆盖）。
     * <b>跳过 {@code _}/{@code .} 开头的子目录</b> —— 写入自建的 {@code _dojao_tmp_*} 临时目录与
     * Hive 自己的 {@code .hive-staging_*} 都不该被当成数据读。
     */
    private void walk(Path dir, int depth, List<FileEntry> acc) throws IOException {
        FileSystem fs = dir.getFileSystem(conf);
        FileStatus[] statuses;
        try {
            statuses = fs.listStatus(dir);
        } catch (FileNotFoundException e) {
            return;   // 分区目录还不存在（或已被清理）→ 当空目录
        }
        List<Path> subDirs = new ArrayList<>();
        List<FileEntry> files = new ArrayList<>();
        for (FileStatus s : statuses) {
            if (s.isDirectory()) {
                if (!isDataFileName(s.getPath().getName())) continue;   // 跳过 _/. 前缀子目录
                subDirs.add(s.getPath());
            } else if (s.isFile() && s.getLen() > 0 && isDataFileName(s.getPath().getName())) {
                files.add(new FileEntry(s.getPath().toString(), s.getLen()));
            }
        }
        if (!files.isEmpty()) {
            acc.addAll(files);
            return;
        }
        if (depth <= 0) return;
        for (Path sub : subDirs) {
            walk(sub, depth - 1, acc);
        }
    }

    /** 过滤 {@code _SUCCESS} / {@code _metadata} / {@code .crc} / 一切隐藏文件。 */
    private static boolean isDataFileName(String name) {
        if (name == null || name.isEmpty()) return false;
        char c = name.charAt(0);
        return c != '_' && c != '.';
    }

    // ==================== 统计辅助 ====================

    private long paramLong(HiveTableMeta meta, String key) {
        return paramLong(meta.parameters(), key);
    }

    /** Hive 表 / 分区参数里的长整数；缺失 / 非法 / 负数一律 {@code -1}（未知）。 */
    private static long paramLong(Map<String, String> params, String key) {
        String v = params == null ? null : params.get(key);
        if (v == null || v.isBlank()) return -1L;
        try {
            long n = Long.parseLong(v.trim());
            return n < 0 ? -1L : n;
        } catch (NumberFormatException e) {
            return -1L;
        }
    }

    private long footerRows(HiveTableMeta meta, List<FileEntry> files) {
        if (files.isEmpty() || meta.format() == HiveStorageFormat.TEXT) return -1L;
        long total = 0L;
        for (FileEntry f : files) {
            long n = meta.format() == HiveStorageFormat.ORC ? orcRowCount(f.path()) : parquetRowCount(f.path());
            if (n < 0) return -1L;
            total += n;
        }
        return total;
    }

    private long orcRowCount(String file) {
        try (Reader reader = OrcFile.createReader(new Path(file),
                OrcFile.readerOptions(conf).filesystem(new Path(file).getFileSystem(conf)))) {
            return reader.getNumberOfRows();
        } catch (Exception e) {
            return -1L;
        }
    }

    private long parquetRowCount(String file) {
        try (ParquetFileReader reader = ParquetFileReader.open(
                HadoopInputFile.fromPath(new Path(file), conf))) {
            long total = 0L;
            for (BlockMetaData block : reader.getFooter().getBlocks()) {
                total += block.getRowCount();
            }
            return total;
        } catch (Exception e) {
            return -1L;
        }
    }

    // ==================== 转换器打开 / 读取 / 关闭 ====================

    /**
     * 为一个读取单元开转换器。ORC / Parquet 传字节区间（整文件单元时 {@code length <= 0}，
     * 转换器内部即整文件语义）；文本无索引概念，区间恒不传。
     */
    private Object openConverter(HiveTableMeta meta, HiveSplitUnit unit, List<String> projection)
            throws IOException {
        Path path = new Path(unit.path());
        long t0 = System.nanoTime();
        FileSystem fs = path.getFileSystem(conf);
        logSlow("取 FileSystem", System.nanoTime() - t0, unit.path());
        t0 = System.nanoTime();
        Object converter = switch (meta.format()) {
            // ORC / Parquet 把投影推给读取器：只读这几列的字节，不是读完再裁
            case ORC -> new ORCFileArrowConverter(allocator, path, fs, batchSize,
                    unit.start(), unit.length(), projection, nestedNativeFor(meta));
            case PARQUET -> new ParquetFileArrowConverter(allocator, path, fs, batchSize,
                    unit.start(), unit.length(), projection, nestedNativeFor(meta));
            // 文本文件不自描述 schema：显式给出 Hive 列定义 + SerDe 分隔符 / 转义 / NULL 串。
            // 无列式索引可跳读，整行读入后由上层按名取列（投影照常生效）。
            case TEXT -> new TextFileArrowConverter(allocator, path, fs,
                    new org.apache.arrow.vector.types.pojo.Schema(meta.dataFields(nestedNativeFor(meta))),
                    meta.textDelimiter(), meta.textEscape(), meta.textNullString(), batchSize);
        };
        logSlow("打开数据文件（读 footer / schema）", System.nanoTime() - t0, unit.describe());
        return converter;
    }

    /**
     * 只在某一步超过 {@link #SLOW_STEP_MS} 时出声 —— 正常路径零噪声，慢的时候一眼看到卡在哪。
     * 读小查询却要几十秒时，靠它把「HDFS footer」「FileSystem 初始化」「HMS 探表」分开。
     */
    private static void logSlow(String what, long nanos, String detail) {
        long ms = nanos / 1_000_000L;
        if (ms < SLOW_STEP_MS) return;
        System.err.printf("[Hive] 慢步骤 %d ms：%s%s%n", ms, what,
                detail == null || detail.isBlank() ? "" : "（" + detail + "）");
    }

    private static Page nextBatch(Object converter) throws IOException {
        if (converter instanceof ORCFileArrowConverter c) return c.convertNextBatch();
        if (converter instanceof ParquetFileArrowConverter c) return c.convertNextBatch();
        return ((TextFileArrowConverter) converter).convertNextBatch();
    }

    private static void closeConverter(Object converter) {
        if (converter == null) return;
        try {
            if (converter instanceof ORCFileArrowConverter c) c.close();
            else if (converter instanceof ParquetFileArrowConverter c) c.close();
            else ((TextFileArrowConverter) converter).close();
        } catch (Exception ignored) {
        }
    }

    // ==================== 读游标（切片 reader） ====================

    /**
     * 一个切片（或一次整表读）的独立游标：按顺序遍历 {@code units}，逐批产出规范化后的页。
     *
     * <p>游标状态全在 reader 实例上 —— 连接器本身无读状态，因此可被多线程共享，
     * 每个线程各开各的 reader（见 {@link #openSplit}）。</p>
     */
    private final class HiveScanReader implements ScanReader {
        private final HiveTableMeta meta;
        private final List<HiveSplitUnit> units;
        private final List<String> projection;
        /** 投影里属于数据列的那些，推给文件读取器（分区列从路径补，不读文件）。 */
        private final List<String> dataProjection;
        private final long limit;
        private int unitIndex;
        private long emitted;
        private boolean done;
        private Object converter;
        // ---- 诊断统计：纯日志，零行为影响（用于区分「读侧」与「发送/聚合侧」耗时归属）----
        private int statBatches;
        private long statRows;
        private long statReadNanos;
        private long statOpenNanos;
        private boolean statLogged;
        private boolean projectionLogged;

        HiveScanReader(HiveTableMeta meta, List<HiveSplitUnit> units,
                       List<String> projection, long limit) {
            this.meta = meta;
            this.units = List.copyOf(units);
            this.projection = projection;
            this.dataProjection = dataProjection(meta, projection);
            this.limit = limit;
        }

        /** 与既有游标是否同源（供 {@link #read} 的线程本地续读判断用）。 */
        boolean matches(List<HiveSplitUnit> u, List<String> p, long l) {
            return limit == l && units.equals(u) && projection.equals(p);
        }

        @Override
        public Page next() {
            while (true) {
                if (done) return Page.eof();
                if (limit > 0 && emitted >= limit) {
                    done = true;
                    return Page.eof();
                }

                if (converter == null) {
                    if (unitIndex >= units.size()) {
                        done = true;
                        logReadStats();
                        return Page.eof();
                    }
                    try {
                        long openT0 = System.nanoTime();
                        converter = openConverter(meta, units.get(unitIndex), dataProjection);
                        statOpenNanos += System.nanoTime() - openT0;
                        if (!projectionLogged) {
                            projectionLogged = true;
                            System.err.printf("[Hive] 投影下推：请求 %d 列 %s → 文件读取 %d 列 %s%n",
                                    projection.size(), projection, dataProjection.size(), dataProjection);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(
                                "打开 Hive 数据文件失败: " + units.get(unitIndex).describe(), e);
                    }
                }

                Page raw;
                long t0 = System.nanoTime();
                try {
                    raw = nextBatch(converter);
                } catch (Exception e) {
                    closeConverter(converter);
                    converter = null;
                    throw new RuntimeException(
                            "读取 Hive 数据文件失败: " + units.get(unitIndex).describe(), e);
                }
                logSlow("读一批数据", System.nanoTime() - t0, units.get(unitIndex).describe());
                statBatches++;
                statReadNanos += System.nanoTime() - t0;

                if (raw == null || raw.isEOF() || raw.getRowCount() == 0) {
                    if (raw != null && !raw.isEOF()) raw.close();
                    closeConverter(converter);
                    converter = null;
                    unitIndex++;
                    continue;
                }

                Page page = normalizePage(meta, units.get(unitIndex).path(), raw, projection);
                if (limit > 0 && emitted + page.getRowCount() > limit) {
                    page = truncate(page, (int) (limit - emitted));
                }
                emitted += page.getRowCount();
                statRows += page.getRowCount();
                return page;
            }
        }

        /**
         * 打印本游标的读取统计（至多一次）。用于把「查询总耗时」拆成
         * <b>读数据 / 建读取器</b> 与「其余（洗牌 + 聚合）」两段 —— 这是定位
         * 「扫描慢还是下游慢」的判据，纯诊断，无任何行为影响。
         */
        private void logReadStats() {
            if (statLogged) return;
            statLogged = true;
            System.err.printf(
                    "[Hive] 读取统计：%d 批 / %d 行；读数据 %d ms（均 %.2f ms/批）；建读取器 %d ms%n",
                    statBatches, statRows, statReadNanos / 1_000_000,
                    statBatches == 0 ? 0.0 : statReadNanos / 1_000_000.0 / statBatches,
                    statOpenNanos / 1_000_000);
        }

        @Override
        public void close() {
            closeConverter(converter);
            converter = null;
            done = true;
            logReadStats();
        }
    }

    // ==================== ACID 行级合并读取器 ====================

    /**
     * <b>ACID（事务表）行级合并读取器</b>。
     *
     * <p>输入是一份文件清单（切片载荷给的是路径）。Master 侧已按 (分区目录, 桶) 把文件切成独立的
     * 切片（见 {@link HiveAcidLayout#groupByBucket}），且已按写事务快照过滤掉不可见的 base / delta，
     * 因此每个切片内是<b>同 (分区, 桶)</b> 的全部可见文件（原始 → base → delta 排序）。</p>
     *
     * <p>逐个切片调 {@link HiveAcidMerger#mergeGroup} 做行级合并（同一 {@code RecordIdentifier}
     * 取 {@code currentTransaction} 最大者；删除只落标记），再把合并出的「活行」按批成页。</p>
     *
     * <p><b>内存</b>：一个切片的常驻内存 ≈ 该 (分区, 桶) 的<b>活行</b>数据。切片按桶粒度拆，
     * 桶越多单桶越小。合并完成后即释放中间态向量（见 {@link HiveAcidMerger.Result#close}）。</p>
     */
    private final class HiveAcidReader implements ScanReader {

        private final HiveTableMeta meta;
        private final List<String> projection;
        private final long limit;
        private final List<List<HiveSplitUnit>> groups;
        private int groupIndex = -1;

        // ---- 当前切片状态 ----
        private String groupRoot;
        private HiveAcidMerger.Result current;
        private int cursor;

        // ---- 输出 ----
        private long emitted;
        private boolean done;

        HiveAcidReader(HiveTableMeta meta, List<HiveSplitUnit> units,
                       List<String> projection, long limit) {
            this.meta = meta;
            this.projection = projection;
            this.limit = limit;
            this.groups = splitByRoot(units);
        }

        @Override
        public Page next() {
            while (true) {
                if (done) return Page.eof();
                if (limit > 0 && emitted >= limit) {
                    done = true;
                    return Page.eof();
                }
                if (current == null && !openNextGroup()) {
                    done = true;
                    return Page.eof();
                }
                Page page = drainGroup();
                if (page == null) {
                    closeGroup();
                    continue;
                }
                if (limit > 0 && emitted + page.getRowCount() > limit) {
                    page = truncate(page, (int) (limit - emitted));
                }
                emitted += page.getRowCount();
                return page;
            }
        }

        @Override
        public void close() {
            closeGroup();
            done = true;
        }

        // ---------- 分组 ----------

        /** 按分区目录（ACID 根）把文件清单切成若干组，组内保持清单原序。 */
        private List<List<HiveSplitUnit>> splitByRoot(List<HiveSplitUnit> units) {
            Map<String, List<HiveSplitUnit>> byRoot = new LinkedHashMap<>();
            for (HiveSplitUnit u : units) {
                byRoot.computeIfAbsent(HiveAcidLayout.acidRoot(u.path()), k -> new ArrayList<>()).add(u);
            }
            return new ArrayList<>(byRoot.values());
        }

        private boolean openNextGroup() {
            groupIndex++;
            while (groupIndex < groups.size()) {
                List<HiveSplitUnit> unitList = groups.get(groupIndex);
                groupIndex++;
                if (unitList.isEmpty()) continue;
                groupRoot = HiveAcidLayout.acidRoot(unitList.get(0).path());
                List<String> paths = new ArrayList<>(unitList.size());
                for (HiveSplitUnit u : unitList) paths.add(u.path());
                long start = System.nanoTime();
                HiveAcidMerger.Result res =
                        HiveAcidMerger.mergeGroup(groupRoot, paths, meta, allocator, conf, batchSize,
                                nestedNative);
                System.err.printf("[Hive] ACID 合并：%s 桶组（%d 个文件）→ %d 行（耗时 %d ms）%n",
                        baseName(groupRoot), unitList.size(), res.rowCount(),
                        (System.nanoTime() - start) / 1_000_000L);
                if (res.rowCount() == 0) {
                    res.close();
                    continue;   // 空组（全被删 / 无数据）→ 直接进下一组
                }
                current = res;
                cursor = 0;
                return true;
            }
            return false;
        }

        private void closeGroup() {
            if (current != null) {
                current.close();
                current = null;
            }
            cursor = 0;
        }

        // ---------- 输出 ----------

        private Page drainGroup() {
            if (current == null || cursor >= current.rowCount()) return null;
            int n = Math.min(batchSize, current.rowCount() - cursor);
            List<Field> fields = current.fields();
            List<FieldVector> src = current.columns();
            List<FieldVector> out = new ArrayList<>(fields.size());
            for (int k = 0; k < fields.size(); k++) {
                FieldVector dst = fields.get(k).createVector(allocator);
                dst.setInitialCapacity(n);
                dst.allocateNew();
                for (int j = 0; j < n; j++) {
                    dst.copyFromSafe(cursor + j, j, src.get(k));
                }
                dst.setValueCount(n);
                out.add(dst);
            }
            cursor += n;
            Page page = Page.fromRoot(new VectorSchemaRoot(fields, out, n));
            return normalizePage(meta, groupRoot, page, projection);
        }
    }

    private static String baseName(String path) {
        int slash = path.lastIndexOf('/');
        return slash < 0 ? path : path.substring(slash + 1);
    }

    // ==================== 校验 / 工具 ====================

    private HiveTableMeta requireTable(String schema, String table) {
        return metastore.table(schema, table).orElseThrow(() ->
                new IllegalArgumentException("Hive 表不存在: " + catalogName + "." + schema + "." + table));
    }

    /**
     * 读之前的前置校验：ACID 表 / 需正则切分的 SerDe 一律<b>明确拒绝</b>，
     * 而不是静默给出 NULL 或错列 —— 宁可报错也不要错数据。
     *
     * <p>ACID 表：catalog 属性 {@code catalog.hive.acid.read} 未开启时明确报错（并提示如何开启）；
     * 开启后放行，走 {@link HiveAcidReader} 的行级合并路径。嵌套列（array / map / struct /
     * uniontype）不拒绝，按文本读出。</p>
     */
    private void checkReadable(HiveTableMeta meta) {
        String qualified = meta.database() + "." + meta.table();
        if (meta.transactional() && !acidRead) {
            throw new UnsupportedOperationException("Hive ACID（事务表）读取未开启: " + qualified
                    + "（表参数 transactional=true）—— 请在 catalog 属性设置 "
                    + "'catalog.hive.acid.read'='true' 后重试");
        }
        String serde = meta.serdeLib() == null ? "" : meta.serdeLib();
        if (meta.format() == HiveStorageFormat.TEXT && serde.contains("RegexSerDe")) {
            throw new UnsupportedOperationException("Hive 连接器暂不支持 RegexSerDe（需按正则切分字段）: "
                    + qualified + " (" + serde + ")");
        }
    }

    /** 投影列必须真实存在于 Hive 表结构；否则报错（Page.select 对未知列是静默丢弃）。 */
    private static void validateProjection(HiveTableMeta meta, List<String> projection) {
        if (projection.isEmpty()) return;
        // 只校验列名存在性，与 nested.mode 无关（两种模式下字段名完全一致）
        List<Field> all = meta.allFields();
        for (String name : projection) {
            if ("*".equals(name)) continue;
            boolean found = false;
            for (Field f : all) {
                if (f.getName().equals(name)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw new IllegalArgumentException("Hive 表 " + meta.database() + "." + meta.table()
                        + " 中不存在列: " + name);
            }
        }
    }

    /** 只保留前 {@code rows} 行（顶层 LIMIT 下推时用；引擎侧 LimitOperator 仍会兜底截断）。 */
    private static Page truncate(Page page, int rows) {
        if (rows >= page.getRowCount()) return page;
        VectorSchemaRoot src = page.getRoot();
        List<Field> fields = src.getSchema().getFields();
        List<FieldVector> out = new ArrayList<>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            FieldVector s = src.getVector(i);
            FieldVector d = fields.get(i).createVector(s.getAllocator());
            d.setInitialCapacity(rows);
            d.allocateNew();
            TransferPair tp = s.makeTransferPair(d);
            for (int r = 0; r < rows; r++) tp.copyValueSafe(r, r);
            d.setValueCount(rows);
            out.add(d);
        }
        Page truncated = Page.fromRoot(new VectorSchemaRoot(fields, out, rows));
        page.close();
        return truncated;
    }

    private static int intProp(Map<String, String> props, String key, int fallback) {
        String v = props.get(key);
        if (v == null || v.isBlank()) return fallback;
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static String firstNonBlank(String... values) {
        for (String v : values) {
            if (v != null && !v.isBlank()) return v.trim();
        }
        return null;
    }
}
