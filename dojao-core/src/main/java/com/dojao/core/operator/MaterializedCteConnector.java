package com.dojao.core.operator;

import com.dojao.common.data.Page;
import com.dojao.common.memory.NodeMemory;
import com.dojao.core.config.ResourceConfig;
import com.dojao.spi.bean.Catalog;
import com.dojao.spi.bean.Column;
import com.dojao.spi.bean.ColumnStats;
import com.dojao.spi.bean.ReadRequest;
import com.dojao.spi.bean.Schema;
import com.dojao.spi.bean.ScanSlice;
import com.dojao.spi.bean.Split;
import com.dojao.spi.bean.Table;
import com.dojao.spi.bean.TableStats;
import com.dojao.spi.bean.WriteRequest;
import com.dojao.spi.service.DojaoConnector;
import com.dojao.spi.service.ScanReader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.pojo.Field;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * <b>物化 CTE 的读取连接器</b> —— 让经 Master 物化的多引用 CTE 也能被 join / 聚合 / 窗口的
 * <b>分布式</b>执行路径消费，而不再因为「查询体已物化、引用点不是基表」整条链回退 Master 单机。
 *
 * <p><b>两种形态，同一份字节协议：</b></p>
 * <ul>
 *   <li><b>Store 形态</b>（Master 侧，规划 / 派发 / 单机执行）：持有
 *       {@link MaterializedCteScanOperator.Store}，{@link #planSplits} 首次触发物化（查询体仍只执行
 *       一次），把每个物化 Arrow 文件重编码成 <b>Arrow IPC stream 字节</b>装进
 *       {@link Split#payload()}；此后切片随 {@code DispatchScanRequest.split_payload} 原样下发。</li>
 *   <li><b>Wire 形态</b>（Worker 侧，独立 JVM）：无状态单例 {@link #wire()}，{@link #openSplit} 解码
 *       载荷并按 {@link ReadRequest#columns()} 投影。Worker 不认识 {@code __dojao_cte__} catalog，
 *       三个扫描执行点（{@code FragmentExecutor} / {@code WorkerControlProducer} /
 *       {@code LocalWorkerGateway}）按 {@link #isCteCatalog(String)} 直接换成本连接器，
 *       不经过 catalog 注册表。</li>
 * </ul>
 *
 * <p><b>为什么用载荷而不是共享文件</b>：物化文件写在 Master 本地溢写目录，Worker 可能是另一台机器
 * （多主机部署时共享路径不成立）。载荷随切片走既有传输链路，单机 / 伪分布式 / 真分布式行为一致。</p>
 *
 * <p><b>谓词</b>：本连接器不兑现下推（{@link #supportsPredicatePushdown()} = {@code false}）——
 * 物化文件已经落盘，没有「数据源侧过滤」可言；{@code ConnectorScanOperator.openWithEngineFilter}
 * 会把整串谓词包成本节点的 {@code FilterOperator}，与 Hive 等文件型连接器同一口径。</p>
 *
 * <p><b>内存</b>：解码页从进程级共享子分配器 {@code connector/cte-wire} 分配，页关闭即归还；
 * 分配器本身与进程同生命周期（与会话型连接器持有的连接器分配器同构），不随单个 reader 关闭，
 * 避免「页仍在下游队列里、分配器已释放」的悬空指针。</p>
 *
 * @author jiaxin
 * @date 2026/11/5
 */
public final class MaterializedCteConnector implements DojaoConnector {

    /** 物化 CTE 的哨兵 catalog 前缀。Worker 据此识别，无需注册定义。 */
    public static final String CATALOG_PREFIX = "__dojao_cte__";

    /** 是否为物化 CTE 的哨兵 catalog。 */
    public static boolean isCteCatalog(String catalog) {
        return catalog != null && catalog.startsWith(CATALOG_PREFIX);
    }

    /** 稳定的哨兵 catalog 名：同一 Store 在进程内恒得同一名字（EXPLAIN / 日志可读）。 */
    public static String catalogFor(String name, long sequence) {
        String safe = name == null || name.isBlank() ? "cte" : name.replaceAll("[^A-Za-z0-9_]+", "_");
        return CATALOG_PREFIX + sequence + "_" + safe;
    }

    /** Worker 侧无状态解码单例（不持有 Store，不触发物化）。 */
    private static final MaterializedCteConnector WIRE = new MaterializedCteConnector(null);

    public static MaterializedCteConnector wire() {
        return WIRE;
    }

    /** 解码页的进程级分配器（页关闭即归还，分配器本身长存）。 */
    private static final class WireAllocatorHolder {
        private static final BufferAllocator INSTANCE =
                NodeMemory.shared().newChildAllocator("connector/cte-wire", 0);
    }

    /** Master 侧物化存储；{@code null} = Wire 形态。 */
    private final MaterializedCteScanOperator.Store store;

    public MaterializedCteConnector(MaterializedCteScanOperator.Store store) {
        this.store = store;
    }

    /** Master 侧的物化存储；{@code null} = Wire 形态（Worker）。 */
    public MaterializedCteScanOperator.Store store() {
        return store;
    }

    // ==================== 切片规划：物化一次 → 每文件一个载荷切片 ====================

    /**
     * {@inheritDoc}
     *
     * <p>Store 形态下首次调用触发 CTE 查询体执行（共享 {@link MaterializedCteScanOperator.Store}
     * 保证只执行一次），随后把每个单批 Arrow 文件编码成独立载荷切片 —— 每个切片一个 Worker 任务，
     * 天然并行。查询体零行 → 空列表（调用方退化为整表切片，Wire 形态读到 EOF）。</p>
     */
    @Override
    public List<ScanSlice> planSplits(ReadRequest request, int parallelism) {
        if (store == null) {
            return List.of();
        }
        try {
            // 载荷编码在 Store 内缓存：多个引用点共用同一份结果（不再每引用重编码一次）。
            List<byte[]> payloads = store.wirePayloads();
            List<ScanSlice> out = new ArrayList<>(payloads.size());
            for (byte[] payload : payloads) {
                out.add(new ScanSlice(request, Split.of(payload)));
            }
            return out;
        } catch (IOException e) {
            throw new IllegalStateException("物化 CTE 切片编码失败（" + store.name() + "）", e);
        } catch (Exception e) {
            throw new IllegalStateException("物化 CTE 执行失败（" + store.name() + "）", e);
        }
    }

    /** 读一个单批 Arrow 文件并重编码成 Arrow IPC <b>stream</b> 字节（自描述、无需 seek）。 */
    static byte[] encodeFile(Path file) throws IOException {
        BufferAllocator tmp = NodeMemory.shared()
                .newOperatorAllocator("cte-encode", ResourceConfig.queryDirectLimit());
        FileChannel ch = FileChannel.open(file, StandardOpenOption.READ);
        ArrowFileReader reader = null;
        try {
            reader = new ArrowFileReader(ch, tmp);
            List<ArrowBlock> blocks = reader.getRecordBlocks();
            if (blocks.isEmpty()) {
                return new byte[0];
            }
            requireSingleBatch(file, blocks.size());
            reader.loadRecordBatch(blocks.get(0));
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            WritableByteChannel out = Channels.newChannel(bos);
            try (ArrowStreamWriter writer = new ArrowStreamWriter(reader.getVectorSchemaRoot(), null, out)) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
            return bos.toByteArray();
        } finally {
            if (reader != null) {
                try { reader.close(); } catch (Throwable ignored) { }
            }
            try { ch.close(); } catch (Throwable ignored) { }
            try { tmp.close(); } catch (Throwable ignored) { }
        }
    }

    // ==================== 读取：载荷解码 ====================

    @Override
    public ScanReader openSplit(ReadRequest request, Split split) {
        List<Path> files = List.of();
        if (store != null) {
            try {
                files = store.materialize();
            } catch (Exception e) {
                throw new IllegalStateException("物化 CTE 执行失败（" + store.name() + "）", e);
            }
        }
        return new CteScanReader(files, request, split);
    }

    /**
     * 顺序重放：有载荷切片读该载荷；无载荷（{@code WHOLE}）时顺序重放全部分物化文件
     * （Store 形态的整表切片；查询体零行时即为空）。
     */
    private static final class CteScanReader implements ScanReader {
        private final List<Path> files;
        private final ReadRequest request;
        private final byte[] payload;
        private int nextFile;
        private boolean done;

        private CteScanReader(List<Path> files, ReadRequest request, Split split) {
            this.files = files == null ? List.of() : files;
            this.request = request;
            this.payload = split != null && split.hasPayload() ? split.payload() : null;
        }

        @Override
        public Page next() {
            if (done) return Page.eof();
            try {
                if (payload != null) {
                    done = true;
                    return project(decodePayload(payload, request), request);
                }
                while (nextFile < files.size()) {
                    Page page = project(readFile(files.get(nextFile++)), request);
                    if (page != null && !page.isEOF() && page.getRowCount() > 0) {
                        return page;
                    }
                    if (page != null) page.close();
                }
                done = true;
                return Page.eof();
            } catch (IOException e) {
                done = true;
                throw new IllegalStateException("物化 CTE 切片解码失败", e);
            }
        }

        @Override
        public void close() {
            done = true;
        }
    }

    /** 解码一个 Arrow stream 载荷为一个页（拷贝到进程级 wire 分配器）。 */
    private static Page decodePayload(byte[] payload, ReadRequest request) throws IOException {
        BufferAllocator tmp = NodeMemory.shared()
                .newOperatorAllocator("cte-decode", ResourceConfig.queryDirectLimit());
        VectorSchemaRoot copied = null;
        try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(payload), tmp)) {
            if (!reader.loadNextBatch()) {
                return Page.eof();
            }
            copied = copyRoot(reader.getVectorSchemaRoot());
        } finally {
            try { tmp.close(); } catch (Throwable ignored) { }
        }
        return Page.fromRoot(copied);
    }

    /**
     * 物化文件当前保证「一页一文件 = 单 record batch」。读取端只处理第 0 个 batch，
     * 多 batch 会静默丢行 —— 用显式断言把这个内部契约钉死。
     */
    static void requireSingleBatch(Path file, int blockCount) {
        if (blockCount != 1) {
            throw new IllegalStateException("物化 CTE 文件应恰好含 1 个 record batch，实际 "
                    + blockCount + " 个: " + file
                    + "（写入路径必须保持一页一文件；多 batch 读取尚未实现，继续执行会丢行）");
        }
    }

    /** 读一个单批 Arrow 文件为一个页（拷贝到进程级 wire 分配器；本地单机重放路径）。 */
    private static Page readFile(Path file) throws IOException {
        BufferAllocator tmp = NodeMemory.shared()
                .newOperatorAllocator("cte-read", ResourceConfig.queryDirectLimit());
        FileChannel ch = FileChannel.open(file, StandardOpenOption.READ);
        ArrowFileReader reader = null;
        try {
            reader = new ArrowFileReader(ch, tmp);
            List<ArrowBlock> blocks = reader.getRecordBlocks();
            if (blocks.isEmpty()) {
                return Page.eof();
            }
            requireSingleBatch(file, blocks.size());
            reader.loadRecordBatch(blocks.get(0));
            return Page.fromRoot(copyRoot(reader.getVectorSchemaRoot()));
        } finally {
            if (reader != null) {
                try { reader.close(); } catch (Throwable ignored) { }
            }
            try { ch.close(); } catch (Throwable ignored) { }
            try { tmp.close(); } catch (Throwable ignored) { }
        }
    }

    /** 把源 root 逐列拷贝到 wire 分配器（源 reader 随即可关）。 */
    private static VectorSchemaRoot copyRoot(VectorSchemaRoot loaded) {
        int rc = loaded.getRowCount();
        List<FieldVector> copied = new ArrayList<>(loaded.getFieldVectors().size());
        for (int col = 0; col < loaded.getFieldVectors().size(); col++) {
            FieldVector src = loaded.getVector(col);
            FieldVector dst = src.getField().createVector(WireAllocatorHolder.INSTANCE);
            dst.setInitialCapacity(rc);
            dst.allocateNew();
            RowAccumulator.copyColumnBatch(src, dst, 0, 0, rc);
            dst.setValueCount(rc);
            copied.add(dst);
        }
        return new VectorSchemaRoot(loaded.getSchema().getFields(), copied, rc);
    }

    /** 按请求列集投影；列缺失响亮报错（静默跳过会让下游按位置取到错列）。 */
    private static Page project(Page page, ReadRequest request) {
        if (page == null || page.isEOF() || page.getRowCount() == 0) {
            return page;
        }
        List<String> cols = request == null ? null : request.columns().orElse(null);
        if (cols == null || cols.isEmpty()) {
            return page;
        }
        List<String> schemaNames = new ArrayList<>();
        for (Field f : page.getRoot().getSchema().getFields()) {
            schemaNames.add(f.getName());
        }
        for (String c : cols) {
            if (!schemaNames.contains(c)) {
                page.close();
                throw new IllegalStateException("物化 CTE 输出缺少列 '" + c + "'（实际列：" + schemaNames + "）");
            }
        }
        Page projected = page.select(cols);
        page.close();   // select 已零拷贝移走 buffer，close 安全
        return projected;
    }

    // ==================== 类型 / 元数据 ====================

    /**
     * 扫描列类型：Store 已物化 → 直接用首页真实 schema（精确 int 位宽 / decimal 精度）；
     * 未物化（规划期）→ 返回空，调用方退回声明列兜底，<b>不在规划期触发 IO</b>。
     */
    @Override
    public List<Field> sourceFields(String catalog, String schema, String table, List<String> columns) {
        List<Field> fields = store == null ? null : store.fields();
        if (fields == null || fields.isEmpty()) {
            return List.of();
        }
        if (columns == null || columns.isEmpty()) {
            return fields;
        }
        List<String> names = com.dojao.spi.util.SqlIdentifiers.unquoteAll(columns);
        List<Field> out = new ArrayList<>(names.size());
        for (String n : names) {
            Field found = null;
            for (Field f : fields) {
                if (f.getName().equals(n)) { found = f; break; }
            }
            if (found == null) {
                for (Field f : fields) {
                    if (f.getName().equalsIgnoreCase(n)) { found = f; break; }
                }
            }
            if (found == null) {
                return List.of();   // 列不在物化输出 → 整体退回兜底口径
            }
            out.add(found);
        }
        return out;
    }

    @Override
    public boolean catalogExists(String catalog) {
        return isCteCatalog(catalog);
    }

    @Override
    public Optional<Catalog> getCatalog(String catalog) {
        return Optional.of(new Catalog(catalog, "cte", Map.of(), "", ""));
    }

    @Override
    public List<String> listCatalogs() {
        return List.of();
    }

    @Override
    public boolean schemaExists(String catalog, String schema) {
        return true;
    }

    @Override
    public Optional<Schema> getSchema(String catalog, String schema) {
        return Optional.of(new Schema(catalog, schema, Map.of()));
    }

    @Override
    public List<String> listSchemas(String catalog) {
        return List.of();
    }

    @Override
    public boolean tableExists(String catalog, String schema, String table) {
        return true;
    }

    @Override
    public Optional<Table> getTable(String catalog, String schema, String table) {
        return Optional.of(new Table(catalog, schema, table, columns(), Map.of(), "materialized cte"));
    }

    @Override
    public List<Column> getTableColumns(String catalog, String schema, String table) {
        return columns();
    }

    @Override
    public List<String> listTables(String catalog, String schema) {
        return List.of();
    }

    @Override
    public boolean columnExists(String catalog, String schema, String table, String column) {
        for (Column c : columns()) {
            if (c.name().equalsIgnoreCase(column)) return true;
        }
        return false;
    }

    @Override
    public Optional<Column> getColumn(String catalog, String schema, String table, String column) {
        for (Column c : columns()) {
            if (c.name().equalsIgnoreCase(column)) return Optional.of(c);
        }
        return Optional.empty();
    }

    /** 行数 / 体量未知 → 估算层退回「外键假设 / 最保守排序归并」，不赌内存。 */
    @Override
    public TableStats getTableStats(String catalog, String schema, String table) {
        return new TableStats(catalog, schema, table, -1, -1, List.of(), -1);
    }

    @Override
    public List<ColumnStats> getColumnStats(String catalog, String schema, String table) {
        return List.of();
    }

    @Override
    public void write(WriteRequest request) {
        throw new UnsupportedOperationException("物化 CTE 是只读连接器，不支持写入");
    }

    private List<Column> columns() {
        List<Field> fields = store == null ? null : store.fields();
        if (fields == null || fields.isEmpty()) {
            List<String> names = store == null ? List.of() : store.columns();
            List<Column> out = new ArrayList<>(names.size());
            for (String n : names) {
                out.add(new Column(n, "string", null, null, true));
            }
            return out;
        }
        List<Column> out = new ArrayList<>(fields.size());
        for (Field f : fields) {
            out.add(new Column(f.getName(), f.getType().toString(), null, null, true));
        }
        return out;
    }
}
