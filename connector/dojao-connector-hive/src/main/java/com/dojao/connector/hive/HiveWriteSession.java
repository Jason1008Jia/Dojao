package com.dojao.connector.hive;

import com.dojao.connector.common.util.ArrowFileWriter;
import com.dojao.connector.common.util.ORCArrowWriter;
import com.dojao.connector.common.util.ParquetArrowWriter;
import com.dojao.connector.common.util.TextArrowWriter;
import com.dojao.spi.bean.Column;
import com.dojao.spi.util.ArrowValueText;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 一次 Hive 写入的会话 —— 把某线程正在写的页攒成数据文件，最后整体落到表目录。
 *
 * <p><b>文件格式跟随目标表</b>：ORC / Parquet / 分隔文本三种格式各有一个
 * {@link ArrowFileWriter} 实现，由 {@link HiveTableMeta#format()} 决定用哪个（分区表的
 * 每个分区沿用同一格式，扩展名取 {@link HiveStorageFormat#fileSuffix()}）。</p>
 *
 * <p><b>临时目录 + mv</b>（用户要求 / Hive 同款）：数据先写进
 * {@code <表目录>/_dojao_tmp_<uuid>/}（分区表再按 {@code <k>=<v>/} 分子目录），
 * 全部写完后再搬到最终位置 —— 半成品数据不对外可见。</p>
 *
 * <p><b>两种收尾模式</b>：</p>
 * <ul>
 *   <li>{@code moveOnClose} = true（CTAS / INSERT INTO）：{@code close()} 时把临时目录内容
 *       搬进表目录并登记分区；</li>
 *   <li>{@code moveOnClose} = false（INSERT OVERWRITE）：{@code close()} 只 flush 文件，
 *       搬运留给 {@code commitOverwrite}（要等全部 Worker 都写完才动，原表在此之前保持原样）。</li>
 * </ul>
 *
 * <p><b>ACID 事务表</b>（{@code transactional=true}）：文件不落分区目录，而是落
 * {@code <分区目录>/delta_<id>_<id>_0/}，并走「开事务 → 写 → mv 入位 → 提交事务」。
 * {@code id} 在 Hive3 是 writeId（{@code allocateTableWriteId}），Hive2 是事务 id；
 * full ACID 文件带五列包装（{@link HiveAcidArrowWriter}），insert_only 表则写普通数据文件
 * （无包装列，可见性只靠 delta 目录名）。收尾顺序固定为「先落地、后提交」—— 见
 * {@link #commitAcid()}。</p>
 *
 * <p><b>线程隔离</b>：连接器实例按进程共享，Worker 任务池真并发跑片段 ⇒ 每个写线程一个会话
 * （{@link HiveConnector} 用 {@code Thread} 做键）。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/29
 */
final class HiveWriteSession implements AutoCloseable {

    /** 同时打开的数据文件上限（超出按最早打开关掉，再写时另起新文件）—— 分区多时不炸文件描述符。 */
    private static final int MAX_OPEN_WRITERS = 128;

    private static final DateTimeFormatter TS_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final Configuration conf;
    private final FileSystem fs;
    private final HiveTableMeta meta;
    private final HiveMetastore metastore;
    /** 本会话写文件的临时目录。 */
    private final Path tempDir;
    /** true = close 时搬进表目录；false = 留给 commitOverwrite 搬。 */
    private final boolean moveOnClose;
    /** 复合类型按原生嵌套结构写（{@code catalog.hive.nested.mode=native}）还是按文本写。 */
    private final boolean nestedNative;

    /** 目标表是否 ACID 事务表（写侧走 delta 目录 + 事务提交）。 */
    private final boolean acid;
    /** insert_only 事务表：delta 目录里写普通数据文件（无 ACID 包装列）。 */
    private final boolean insertOnly;
    /** 事务 id（懒开启：首次真正写数据才 openTxn）；-1 = 尚未开启。 */
    private long txnId = -1;
    /** 目录 / 行标识里的 id（Hive3 = writeId，Hive2 = txnId）。 */
    private long acidId;
    /** delta 目录名 —— 本会话写出的全部文件都在这个目录下。 */
    private String deltaDirName;
    /** 事务保活线程（写时长超过 {@code hive.txn.timeout} 时防止被 HMS 回滚）。 */
    private ScheduledExecutorService heartbeat;
    /** 任一页写入失败 → 收尾时回滚事务，不提交半截数据。 */
    private boolean failed;
    /** (分区, 桶) → 下一条记录的 rowId（跨文件连续，保证 RecordIdentifier 唯一）。 */
    private final Map<String, Long> nextRowId = new HashMap<>();
    /** (分区, 桶) → 该桶已打开的数据文件数（文件句柄轮转时用于 {@code bucket_XXXXX_<n>} 命名）。 */
    private final Map<String, Integer> fileCopies = new HashMap<>();
    /** 已 mv 进表目录的 delta 目录（回滚时清理）。 */
    private final List<Path> placedDeltaDirs = new ArrayList<>();

    private final List<String> dataColumnNames;
    private final List<String> partitionColumnNames;
    /** 分桶列名（{@code CLUSTERED BY}，保序）；非分桶表为空。 */
    private final List<String> bucketColumnNames;
    /** 分桶列声明类型（顺序同 {@link #bucketColumnNames}）；非分桶表为空。 */
    private final List<String> bucketColumnTypes;
    /** 写入分组键（分区目录 + 桶号）→ 数据文件写句柄。 */
    private final Map<String, ArrowFileWriter> writers = new LinkedHashMap<>();
    /** 本会话写出的文件名前缀 —— 多写者（并发片段 / INSERT INTO 多次）搬进同一分区目录也不撞名。 */
    private final String fileToken = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    private long fileSeq;
    private boolean closed;

    HiveWriteSession(Configuration conf, FileSystem fs, HiveTableMeta meta, HiveMetastore metastore,
                     String tempDirLocation, boolean moveOnClose) {
        this(conf, fs, meta, metastore, tempDirLocation, moveOnClose, true);
    }

    HiveWriteSession(Configuration conf, FileSystem fs, HiveTableMeta meta, HiveMetastore metastore,
                     String tempDirLocation, boolean moveOnClose, boolean nestedNative) {
        this.conf = conf;
        this.fs = fs;
        this.meta = meta;
        this.metastore = metastore;
        this.tempDir = new Path(tempDirLocation);
        this.moveOnClose = moveOnClose;
        this.nestedNative = nestedNative;
        this.acid = meta.transactional();
        this.insertOnly = meta.insertOnly();
        this.dataColumnNames = new ArrayList<>();
        for (Column c : meta.dataColumns()) dataColumnNames.add(c.name());
        this.partitionColumnNames = meta.partitionKeys();
        this.bucketColumnNames = meta.bucketCols();
        this.bucketColumnTypes = meta.bucketColumnTypes();
    }

    static String newTempDirName() {
        return "_dojao_tmp_" + UUID.randomUUID().toString().replace("-", "");
    }

    String tempDirLocation() { return tempDir.toString(); }

    // ==================== 写入 ====================

    /**
     * @param pageColumns INSERT INTO 的显式目标列（页列按位置对应这些名字）；为 {@code null} 时
     *                    用页自己的列名找 Hive 列
     */
    void write(VectorSchemaRoot root, List<String> pageColumns) throws IOException {
        int rows = root.getRowCount();
        if (rows <= 0) return;
        try {
            Map<String, Integer> idx = indexByName(root, pageColumns);

            if (!meta.partitioned() && !meta.bucketed()) {
                writeGroup(root, allRows(rows), new WriteGroup("", -1), idx);
                return;
            }
            // 动态分区 × 分桶：按（分区列值, 桶号）联合分组；非分桶表桶号恒 -1（只有分区维）
            Map<WriteGroup, List<Integer>> groups = new LinkedHashMap<>();
            for (int r = 0; r < rows; r++) {
                String dir = "";
                if (meta.partitioned()) {
                    List<String> values = new ArrayList<>(partitionColumnNames.size());
                    for (String pc : partitionColumnNames) {
                        values.add(valueAsString(root, idx, pc, r, "分区列"));
                    }
                    dir = partitionDir(values);
                }
                int bucketId = meta.bucketed() ? bucketId(root, idx, r) : -1;
                groups.computeIfAbsent(new WriteGroup(dir, bucketId), k -> new ArrayList<>()).add(r);
            }
            for (Map.Entry<WriteGroup, List<Integer>> e : groups.entrySet()) {
                writeGroup(root, e.getValue(), e.getKey(), idx);
            }
        } catch (IOException | RuntimeException e) {
            // 字面量失败 → 收尾时回滚事务，宁可整事务失败也不要提交半截数据
            failed = true;
            throw e;
        }
    }

    /** 一行的桶号（Hive 同口径 {@code (hash & MAX) % n}）；分桶列类型不可哈希 → 抛异常（宁报错不写错桶）。 */
    private int bucketId(VectorSchemaRoot root, Map<String, Integer> idx, int row) {
        List<String> values = new ArrayList<>(bucketColumnNames.size());
        for (String bc : bucketColumnNames) {
            values.add(valueAsString(root, idx, bc, row, "分桶列"));
        }
        Integer hash = HiveBucketHash.bucketHash(bucketColumnTypes, values);
        if (hash == null) {
            throw new IllegalArgumentException("分桶列类型不支持写入分桶: " + bucketColumnNames
                    + " 类型 " + bucketColumnTypes);
        }
        return HiveBucketHash.bucketNumber(hash, meta.numBuckets());
    }

    /**
     * Hive 列名 → 页列下标。优先按 {@code pageColumns} 的<b>位置</b>映射（{@code INSERT INTO t(a,b)} 时
     * 页列名未必等于表列名），再用页自身的列名兜底（名字对上即可，不区分大小写）。
     */
    private static Map<String, Integer> indexByName(VectorSchemaRoot root, List<String> pageColumns) {
        List<FieldVector> vecs = root.getFieldVectors();
        Map<String, Integer> idx = new HashMap<>();
        for (int i = 0; i < vecs.size(); i++) {
            if (pageColumns != null && i < pageColumns.size()) {
                idx.putIfAbsent(pageColumns.get(i).toLowerCase(Locale.ROOT), i);
            }
            idx.putIfAbsent(vecs.get(i).getField().getName().toLowerCase(Locale.ROOT), i);
        }
        return idx;
    }

    private void writeGroup(VectorSchemaRoot root, List<Integer> rows, WriteGroup group,
                            Map<String, Integer> idx) throws IOException {
        ArrowFileWriter writer = writers.get(group.key());
        if (writer == null) {
            if (writers.size() >= MAX_OPEN_WRITERS) {
                var it = writers.entrySet().iterator();
                if (it.hasNext()) {
                    closeQuietly(it.next().getValue());
                    it.remove();
                }
            }
            writer = openWriter(group);
            writers.put(group.key(), writer);
        }
        VectorSchemaRoot slice = sliceRows(root, rows, idx);
        try {
            writer.write(slice);
        } finally {
            slice.close();
        }
        if (acid && !insertOnly) {
            // rowId 在同一 (分区, 桶) 内单调递增 —— 轮转出的第二个文件必须接着数，不能从 0 重来
            nextRowId.merge(group.key(), (long) rows.size(), Long::sum);
        }
    }

    /**
     * 新开一个数据文件（格式取 {@link HiveTableMeta#format()}）：
     * <ul>
     *   <li>普通表：分桶 → {@code <6位桶号>_<token>_<seq><后缀>}（读侧 {@code bucketIdOf} 按前导数字归桶）；
     *       非分桶 → {@code part-<token>-<seq><后缀>}；</li>
     *   <li>ACID 表：路径固定 {@code <分区目录>/delta_<id>_<id>_0000/}，文件名用 Hive 口径
     *       {@code bucket_%05d}（同桶轮转出的第 n 个文件带 {@code _<n>} 后缀）；full ACID 额外写
     *       {@code _orc_acid_version}，且写入器换成 {@link HiveAcidArrowWriter}。</li>
     * </ul>
     */
    private ArrowFileWriter openWriter(WriteGroup group) throws IOException {
        List<Field> dataFields = meta.dataFields(nestedNative);
        HiveStorageFormat format = meta.format();
        Path root = group.dir().isEmpty() ? tempDir : new Path(tempDir, group.dir());

        if (acid) {
            // 事务表：文件必须落在 delta_<id>_<id>_0000/ 下，收尾统一 mv + 提交事务
            ensureTxn();
            Path dir = new Path(root, deltaDirName);
            fs.mkdirs(dir);
            writeAcidVersionFile(dir);
            int copy = fileCopies.merge(group.key(), 1, Integer::sum) - 1;
            String name = group.bucketId() >= 0
                    ? HiveAcidLayout.bucketFileName(group.bucketId(), copy)
                    : "part-" + fileToken + "-" + (fileSeq++) + format.fileSuffix();
            Path file = new Path(dir, name);
            if (!insertOnly) {
                return new HiveAcidArrowWriter(conf, file, fs, dataFields, acidId,
                        group.bucketId(), nextRowId.getOrDefault(group.key(), 0L));
            }
            return formatWriter(format, file, dataFields);
        }

        Path dir = root;
        fs.mkdirs(dir);
        String suffix = format.fileSuffix();
        String name = group.bucketId() >= 0
                ? String.format("%06d", group.bucketId()) + "_" + fileToken + "_" + (fileSeq++) + suffix
                : "part-" + fileToken + "-" + (fileSeq++) + suffix;
        return formatWriter(format, new Path(dir, name), dataFields);
    }

    /** 按目标表物理格式建普通数据文件写入器（非 ACID 与 insert_only 共用）。 */
    private ArrowFileWriter formatWriter(HiveStorageFormat format, Path file, List<Field> dataFields)
            throws IOException {
        int[] sourceIndexes = identity(dataFields.size());
        return switch (format) {
            case ORC -> new ORCArrowWriter(conf, file, fs, dataFields, sourceIndexes);
            case PARQUET -> new ParquetArrowWriter(conf, file, fs, dataFields, sourceIndexes);
            case TEXT -> new TextArrowWriter(conf, file, fs, dataFields, sourceIndexes,
                    meta.textDelimiter(), meta.textEscape(), meta.textNullString());
        };
    }

    /** 取页里选中的行，只留数据列（分区列不落文件）。 */
    private VectorSchemaRoot sliceRows(VectorSchemaRoot root, List<Integer> rows,
                                       Map<String, Integer> idxByName) {
        List<FieldVector> srcVecs = root.getFieldVectors();
        int n = rows.size();
        List<Field> outFields = new ArrayList<>(dataColumnNames.size());
        List<FieldVector> outVecs = new ArrayList<>(dataColumnNames.size());
        for (String name : dataColumnNames) {
            Integer idx = idxByName.get(name.toLowerCase(Locale.ROOT));
            if (idx == null) {
                throw new IllegalArgumentException("写入页缺少 Hive 数据列: " + name
                        + "（页列: " + idxByName.keySet() + "）");
            }
            FieldVector src = srcVecs.get(idx);
            FieldVector dst = src.getField().createVector(src.getAllocator());
            dst.setInitialCapacity(Math.max(1, n));
            dst.allocateNew();
            var tp = src.makeTransferPair(dst);
            for (int r = 0; r < n; r++) {
                if (src.isNull(rows.get(r))) dst.setNull(r);
                else tp.copyValueSafe(rows.get(r), r);
            }
            dst.setValueCount(n);
            outFields.add(src.getField());
            outVecs.add(dst);
        }
        return new VectorSchemaRoot(outFields, outVecs, n);
    }

    // ==================== 收尾 ====================

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        for (ArrowFileWriter w : writers.values()) closeQuietly(w);
        writers.clear();
        try {
            if (!moveOnClose) {                 // OVERWRITE：留给 commitOverwrite
                if (acid) abortAcid();
                return;
            }
            if (acid) {
                if (failed) abortAcid();        // 任一页失败 → 整事务回滚
                else if (txnId >= 0) commitAcid();   // txnId<0：没写出数据，无需提交空事务
                return;
            }
            moveIntoPlace();
        } catch (IOException | RuntimeException e) {
            if (acid) abortAcid();
            throw new RuntimeException("Hive 写入收尾失败（临时目录 " + tempDir + "）", e);
        } finally {
            stopHeartbeat();
        }
    }

    /**
     * 丢弃一个<b>再也不会被收尾</b>的会话（所属线程已死 / 查询取消后连接器不再替它收尾）：
     * 回滚事务（ACID）并删掉临时目录，<b>绝不提交</b>半截数据。
     *
     * <p>与 {@link #close()} 的区别：这里不做「搬入表目录 / commitTxn」—— 没有「写完了」的信号时，
     * 宁可让本次写出的数据整体消失，也不能把被取消 / 异常打断的追加提交进去；同时停掉 ACID
     * 心跳线程，免得事务被无限续命（HMS 会按 {@code hive.txn.timeout} 回收）。</p>
     */
    void discard() {
        if (closed) return;
        closed = true;
        for (ArrowFileWriter w : writers.values()) closeQuietly(w);
        writers.clear();
        if (acid) {
            abortAcid();
        } else {
            try {
                fs.delete(tempDir, true);
            } catch (IOException ignored) {
                // 清理失败无碍：临时目录以 _dojao_tmp_ 前缀命名，不会被当成数据文件读
            }
        }
        stopHeartbeat();
    }

    // ==================== ACID 收尾 ====================

    /**
     * 首次真正写数据时开启事务 —— 空写（没有任何行）不产生空事务 / 空 writeId。
     *
     * <p>{@code acidId}：Hive3 为 {@code allocateTableWriteId} 分配到的写 id，Hive2 直接是事务 id
     * （见 {@link HiveMetastore#allocateWriteId(long, String, String)}）。分配失败时先回滚刚开的
     * 事务再抛出，不留悬挂事务。</p>
     */
    private void ensureTxn() {
        if (txnId >= 0) return;
        // 事务归属用户必须与 catalog 的 Hadoop 身份一致（HMS 按连接身份做 HDFS 权限检查），
        // 不能用 System 属性兜底 —— 同一 JVM 多 catalog 时那是进程级别的、已被别的 catalog 改过。
        String user = metastore.user() != null ? metastore.user()
                : System.getProperty("user.name", "hive");
        long opened = metastore.openTxn(user);
        try {
            acidId = metastore.allocateWriteId(opened, meta.database(), meta.table());
        } catch (RuntimeException e) {
            try { metastore.rollbackTxn(opened); } catch (RuntimeException ignored) { }
            throw e;
        }
        txnId = opened;
        deltaDirName = HiveAcidLayout.deltaDirName(acidId);
        startHeartbeat();
    }

    /**
     * 事务保活线程（60s 一次）。HMS 的 {@code hive.txn.timeout} 默认 300s —— 大 INSERT 写 HDFS
     * 可能远超这个时间，没有心跳就会被判超时回滚，commit 阶段直接失败。
     */
    private void startHeartbeat() {
        long id = txnId;
        heartbeat = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "hive-acid-heartbeat");
            t.setDaemon(true);
            return t;
        });
        heartbeat.scheduleWithFixedDelay(() -> metastore.heartbeatTxn(id), 60, 60, TimeUnit.SECONDS);
    }

    private void stopHeartbeat() {
        ScheduledExecutorService hb = heartbeat;
        heartbeat = null;
        if (hb != null) hb.shutdownNow();
    }

    /**
     * 在 delta 目录写 ACID 格式标记 —— 与 Hive {@code OrcRecordUpdater} 同款：文件
     * {@code _orc_acid_version}，内容是 4 字节大端整数 {@code 0}。Hive 的读侧靠它区分
     * 「老的两列 ACID 格式」与现在的六列包装格式，缺失会被当成老格式误读。
     */
    private void writeAcidVersionFile(Path dir) throws IOException {
        if (insertOnly) return;      // insert_only 的数据文件没有包装列，也不需要版本标记
        Path version = new Path(dir, "_orc_acid_version");
        if (fs.exists(version)) return;
        try (FSDataOutputStream os = fs.create(version, false)) {
            os.writeInt(0);
        }
    }

    /**
     * ACID 收尾：把临时目录里的 delta 目录 mv 到各分区目录 → 登记新分区 → 提交事务。
     *
     * <p><b>顺序不能反</b>：先落地文件、后提交事务。反过来的话「事务已提交、文件还没 mv」的窗口里
     * 读者会看到一个已提交但没有任何文件的写 id（静默丢行）；而文件已落地、提交失败只留下一个
     * 不可见（事务未提交 / 已回滚）的 delta 目录，不影响读，HMS 清道夫之后也会回收。</p>
     */
    private void commitAcid() throws IOException {
        Path target = new Path(meta.location());
        fs.mkdirs(target);
        if (!meta.partitioned()) {
            moveDeltaDirs(tempDir, target);
        } else {
            FileStatus[] sub = fs.listStatus(tempDir);
            if (sub != null) {
                for (FileStatus s : sub) {
                    if (!s.isDirectory()) continue;
                    Path dest = new Path(target, s.getPath().getName());
                    fs.mkdirs(dest);
                    moveDeltaDirs(s.getPath(), dest);
                    registerPartition(s.getPath().getName(), dest);
                }
            }
        }
        fs.delete(tempDir, true);
        metastore.commitTxn(txnId);
    }

    /** 把 {@code src} 下的 {@code delta_*} 子目录整体改名进 {@code dest}（同文件系统内 mv）。 */
    private void moveDeltaDirs(Path src, Path dest) throws IOException {
        FileStatus[] children = fs.listStatus(src);
        if (children == null) return;
        for (FileStatus c : children) {
            if (!c.isDirectory() || !c.getPath().getName().startsWith(HiveAcidLayout.DELTA_PREFIX)) {
                continue;
            }
            Path dst = new Path(dest, c.getPath().getName());
            if (!fs.rename(c.getPath(), dst)) {
                throw new IOException("移动 Hive ACID delta 目录失败: " + c.getPath() + " → " + dst
                        + "（临时目录与表目录应在同一文件系统）");
            }
            placedDeltaDirs.add(dst);
        }
    }

    /** 失败 / 取消收尾：回滚事务并清掉本次写出的 delta（已 mv 入位的也删，避免留垃圾）。 */
    private void abortAcid() {
        if (txnId >= 0) {
            try { metastore.rollbackTxn(txnId); } catch (RuntimeException ignored) { }
        }
        try { fs.delete(tempDir, true); } catch (IOException ignored) { }
        for (Path p : placedDeltaDirs) {
            try { fs.delete(p, true); } catch (IOException ignored) { }
        }
        placedDeltaDirs.clear();
    }

    /**
     * 临时目录内容 → 真实表目录（+ 分区登记）。非分区表直接铺平，分区表逐个分区搬。
     *
     * <p><b>追加语义</b>（CTAS 表本就是空的 / INSERT INTO 就是 append）：不删目标目录已有数据，
     * 只把本会话产出的、带唯一前缀的 ORC 文件搬进去。真正的「覆盖」走
     * {@code HiveConnector.commitOverwrite}（staging + 删旧 + 搬新）。</p>
     */
    void moveIntoPlace() throws IOException {
        Path target = new Path(meta.location());
        fs.mkdirs(target);
        if (!meta.partitioned()) {
            moveFilesInDir(tempDir, target);
            fs.delete(tempDir, true);
            return;
        }
        FileStatus[] sub = fs.listStatus(tempDir);
        for (FileStatus s : sub) {
            if (!s.isDirectory()) continue;
            String dirName = s.getPath().getName();
            Path dest = new Path(target, dirName);
            fs.mkdirs(dest);
            moveFilesInDir(s.getPath(), dest);
            registerPartition(dirName, dest);
        }
        fs.delete(tempDir, true);
    }

    /** 把 {@code dir} 下的文件搬到 {@code dest}（不搬子目录）。 */
    private void moveFilesInDir(Path dir, Path dest) throws IOException {
        for (FileStatus f : fs.listStatus(dir)) {
            if (!f.isFile()) continue;
            Path dst = new Path(dest, f.getPath().getName());
            if (!fs.rename(f.getPath(), dst)) {
                throw new IOException("移动 Hive 溢写文件失败: " + f.getPath() + " → " + dst
                        + "（临时目录与表目录应在同一文件系统）");
            }
        }
    }

    /** 由已搬好的分区目录名（{@code k=v}）登记 / 更新 HMS 分区。 */
    private void registerPartition(String dirName, Path location) {
        List<String> values = new ArrayList<>(partitionColumnNames.size());
        for (String segment : dirName.split("/")) {
            int eq = segment.indexOf('=');
            if (eq <= 0) continue;
            values.add(unescapePathName(segment.substring(eq + 1)));
        }
        if (values.size() != partitionColumnNames.size()) return;

        Partition p = new Partition();
        p.setDbName(meta.database());
        p.setTableName(meta.table());
        p.setValues(values);
        p.setSd(partitionSd(location.toString()));
        p.setParameters(new HashMap<>());
        metastore.upsertPartition(p);
    }

    private StorageDescriptor partitionSd(String location) {
        List<FieldSchema> cols = new ArrayList<>();
        for (Column c : meta.dataColumns()) {
            cols.add(new FieldSchema(c.name(), c.dataType(), c.comment()));
        }
        HiveStorageFormat format = meta.format();
        SerDeInfo serde = new SerDeInfo();
        serde.setSerializationLib(meta.serdeLib() == null ? format.serdeLib() : meta.serdeLib());
        serde.setParameters(serdeParams(format));
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

    /** 分区 SD 的 SerDe 参数（文本表沿用目标表真实的分隔 / 转义 / NULL 串）。 */
    private Map<String, String> serdeParams(HiveStorageFormat format) {
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

    // ==================== 行 / 值工具 ====================

    /** 写入分组：分区目录相对路径 + 桶号（非分桶表桶号恒 -1）。 */
    private record WriteGroup(String dir, int bucketId) {
        String key() { return dir + '#' + bucketId; }
    }

    private static List<Integer> allRows(int rows) {
        List<Integer> out = new ArrayList<>(rows);
        for (int i = 0; i < rows; i++) out.add(i);
        return out;
    }

    private String partitionDir(List<String> values) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) sb.append('/');
            String v = values.get(i);
            sb.append(partitionColumnNames.get(i)).append('=');
            sb.append(v == null ? "__HIVE_DEFAULT_PARTITION__" : escapePathName(v));
        }
        return sb.toString();
    }

    /** Hive 目录名转义：非 {@code [A-Za-z0-9_.-]} 一律 {@code %XX}（与读侧 {@code unescapePathName} 对称）。 */
    static String escapePathName(String value) {
        StringBuilder sb = new StringBuilder(value.length() + 8);
        for (byte b : value.getBytes(StandardCharsets.UTF_8)) {
            int c = b & 0xFF;
            if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
                    || c == '_' || c == '-' || c == '.') {
                sb.append((char) c);
            } else {
                sb.append('%');
                sb.append(Character.toUpperCase(Character.forDigit((c >> 4) & 0xF, 16)));
                sb.append(Character.toUpperCase(Character.forDigit(c & 0xF, 16)));
            }
        }
        return sb.toString();
    }

    /** 对称还原（只认 {@code %XX}，绝不把 {@code +} 当空格）。 */
    static String unescapePathName(String value) {
        if (value == null || value.indexOf('%') < 0) return value;
        byte[] out = new byte[value.length()];
        int len = 0;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '%' && i + 2 < value.length()) {
                int hi = Character.digit(value.charAt(i + 1), 16);
                int lo = Character.digit(value.charAt(i + 2), 16);
                if (hi >= 0 && lo >= 0) {
                    out[len++] = (byte) ((hi << 4) + lo);
                    i += 2;
                    continue;
                }
            }
            out[len++] = (byte) c;
        }
        return new String(out, 0, len, StandardCharsets.UTF_8);
    }

    /** 取某行某列的值并转成字符串（NULL → null）；分区列 / 分桶列共用。{@code role} 仅用于缺失列的报错文案。 */
    private static String valueAsString(VectorSchemaRoot root, Map<String, Integer> idx,
                                        String column, int row, String role) {
        Integer pos = idx.get(column.toLowerCase(Locale.ROOT));
        if (pos == null) {
            throw new IllegalArgumentException("写入页缺少 " + role + ": " + column
                    + "（页列: " + root.getSchema().getFields() + "）");
        }
        FieldVector v = root.getFieldVectors().get(pos);
        if (v.isNull(row)) return null;
        // DATE/TIME 的 getObject 是 epoch 数字（Arrow 15），分区/分桶列必须按向量类型转日期文本
        if (v instanceof DateDayVector dd) return LocalDate.ofEpochDay(dd.get(row)).toString();
        if (v instanceof DateMilliVector dm) {
            return Instant.ofEpochMilli(dm.get(row)).atZone(ZoneOffset.UTC).toLocalDate().toString();
        }
        if (v instanceof TimeStampVector ts) return TS_FORMAT.format(ArrowValueText.timestampOf(ts, row));
        Object o = v.getObject(row);
        if (o == null) return null;
        if (o instanceof byte[] bytes) return new String(bytes, StandardCharsets.UTF_8);
        if (o instanceof Text t) return t.toString();
        if (o instanceof LocalDate d) return d.toString();
        if (o instanceof LocalDateTime ts) return TS_FORMAT.format(ts);
        if (o instanceof BigDecimal bd) return bd.toPlainString();
        if (o instanceof Boolean b) return b ? "true" : "false";
        return String.valueOf(o);
    }

    private static int[] identity(int n) {
        int[] out = new int[n];
        for (int i = 0; i < n; i++) out[i] = i;
        return out;
    }

    private static void closeQuietly(ArrowFileWriter w) {
        try {
            w.close();
        } catch (IOException ignored) {
        }
    }
}
