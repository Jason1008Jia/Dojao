package com.dojao.core.operator;

import com.dojao.common.data.Page;
import com.dojao.common.memory.NodeMemory;
import com.dojao.core.config.ResourceConfig;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowBlock;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * <b>物化 CTE 扫描算子</b> —— 多次引用的 CTE 只执行一次，结果落盘后被各引用点重放。
 *
 * <p>{@code Planner.inlineCtes} 把同一个 CTE 查询体对象塞进每个引用点，物理规划时逐个规划
 * ⇒ N 个引用 = N 份独立扫描/聚合（线上漏斗 SQL 把 9000 万行源表读了 4 次）。本算子替代
 * 这种重复执行：{@link Store} 在<b>第一次</b>被打开时执行一次 CTE 查询体，把每页写成单批
 * Arrow IPC 文件；此后每个引用点的算子实例按序重放这些文件。</p>
 *
 * <p><b>为什么是叶子 + Store</b>：物化的查询体是一棵完整的算子链（可能含分布式聚合），只能
 * 执行一次，不能作为多个父节点的公共子节点（那会共享可变算子状态）。因此每个引用点各自持有
 * 一个轻量扫描实例，共享同一个 {@link Store}（幂等物化 + 文件只读）。</p>
 *
 * <p><b>生命周期</b>：溢写目录名为 {@code cte-<名>-p<PID>-<序号>}（符合 {@link SpillDirs}
 * 的孤儿清理约定）。全部扫描实例关闭后删目录并摘除登记；进程被强杀时由 SpillDirs 启动清理兜底。</p>
 *
 * <p><b>EXPLAIN</b>：实现 {@link Explainable}，把被物化的查询体计划以附加行显示 —— 规划期
 * <b>不触发任何 IO</b>（与 Explainable 契约一致）。</p>
 *
 * @author jiaxin
 * @date 2026/10/20
 */
public final class MaterializedCteScanOperator extends AbstractMeasurableOperator implements Explainable {

    private static final AtomicInteger INSTANCE_SEQUENCE = new AtomicInteger();
    private static final AtomicInteger SPILL_ROOT_SEQUENCE = new AtomicInteger();

    private final Store store;
    private final BufferAllocator allocator;
    private List<Path> files = List.of();
    private int nextFile;

    public MaterializedCteScanOperator(Store store) {
        super("MaterializedCteScan[" + store.name() + "]", "MaterializedCteScan", 1);
        this.store = store;
        this.allocator = NodeMemory.shared()
                .newOperatorAllocator("cte-materialized", ResourceConfig.queryDirectLimit());
        store.registerScanner();
    }

    /** 结果列（= 物化查询体自身的输出列），规划期即可确定。 */
    public List<String> outputColumns() {
        return store.columns();
    }

    @Override
    protected void doOpen() throws Exception {
        files = store.materialize();
        nextFile = 0;
    }

    /**
     * 顺序重放物化文件。{@code synchronized} 是必要的：本地派生 join 的两侧各有一个
     * {@link HashPartitionOperator}，其 N 个分发线程会<b>并发</b>调用上游 {@code next()}
     * （正常上游是 ParallelTableScan，可安全并发拉页）。本算子只有一个共享的 {@code nextFile}
     * 游标，并发进入会让两个线程取到同一/越界下标 —— 表现为
     * {@code IndexOutOfBoundsException: Index: 1 Size: 1}。重放本身是顺序 IO，串行化不损失能力。
     */
    @Override
    protected synchronized Page doNext() throws Exception {
        while (nextFile < files.size()) {
            VectorSchemaRoot root = readFile(files.get(nextFile++));
            if (root == null || root.getRowCount() == 0) {
                if (root != null) root.close();
                continue;
            }
            return Page.fromRoot(root);
        }
        return Page.eof();
    }

    /** 读一个单批 Arrow IPC 文件，拷贝到本算子分配器（源 reader/批随即释放）。 */
    private VectorSchemaRoot readFile(Path file) throws IOException {
        FileChannel ch = FileChannel.open(file, StandardOpenOption.READ);
        ArrowFileReader reader = null;
        VectorSchemaRoot loaded = null;
        List<FieldVector> copied = new ArrayList<>();
        boolean ok = false;
        try {
            reader = new ArrowFileReader(ch, allocator);
            List<ArrowBlock> blocks = reader.getRecordBlocks();
            if (blocks.isEmpty()) {
                ok = true;
                return null;
            }
            MaterializedCteConnector.requireSingleBatch(file, blocks.size());
            reader.loadRecordBatch(blocks.get(0));
            loaded = reader.getVectorSchemaRoot();
            int rc = loaded.getRowCount();
            for (int col = 0; col < loaded.getFieldVectors().size(); col++) {
                FieldVector src = loaded.getVector(col);
                FieldVector dst = src.getField().createVector(allocator);
                dst.setInitialCapacity(rc);
                dst.allocateNew();
                RowAccumulator.copyColumnBatch(src, dst, 0, 0, rc);
                dst.setValueCount(rc);
                copied.add(dst);
            }
            ok = true;
            return new VectorSchemaRoot(loaded.getSchema().getFields(), copied, rc);
        } finally {
            if (!ok) {
                for (FieldVector v : copied) {
                    try { v.close(); } catch (Throwable ignored) { }
                }
            }
            if (reader != null) { try { reader.close(); } catch (Throwable ignored) { } }
            if (loaded != null) { try { loaded.close(); } catch (Throwable ignored) { } }
            try { ch.close(); } catch (Throwable ignored) { }
        }
    }

    @Override
    protected void doClose() {
        try { allocator.close(); } catch (Throwable ignored) { }
        store.scannerClosed();
    }

    @Override
    public List<String> explainLines() {
        List<String> out = new ArrayList<>();
        out.add("materialize once → spill, replay per reference");
        for (String line : store.bodyExplain()) {
            out.add("body | " + line);
        }
        return out;
    }

    /**
     * 规划好的 CTE 查询体：可执行算子链 + 输出列 + EXPLAIN 明细行。由 Planner 提供，
     * {@link Store} 保证只构建一次。
     */
    public record PlannedBody(Operator operator, List<String> columns, List<String> explain) { }

    /**
     * 共享的物化存储：幂等执行 CTE 查询体一次，把每页写成单批 Arrow IPC 文件。
     *
     * <p>线程安全：物化在 {@code synchronized} 内完成（首个消费者触发），其余消费者等待后
     * 直接复用文件列表。失败会记录并原样上抛（<b>不静默回退</b>到重复扫描 —— 计划期已经
     * 按「只执行一次」构建，静默换路径会让 EXPLAIN 与实际执行不一致）。</p>
     */
    public static final class Store {

        private final String name;
        private final Supplier<PlannedBody> bodySupplier;
        private final Object lock = new Object();

        /** 查询体物理计划（首次取列 / 执行时构建，之后复用）。 */
        private PlannedBody body;
        /** null = 尚未物化；空列表 = 已物化但查询体零行。 */
        private List<Path> files;
        /** 物化输出的真实 schema（首个非空页）；未物化 / 零行时为 null。 */
        private List<org.apache.arrow.vector.types.pojo.Field> fields;
        private Throwable failure;
        private Path dir;
        private int scanners;
        private int closedScanners;
        private boolean cleaned;
        /** wire 载荷（每个物化文件一份），首次取用编码一次后缓存，避免每个引用点重复编码。 */
        private List<byte[]> wirePayloads;
        /** 载荷编码次数（探针 / 遥测：多引用应只编码一次）。 */
        private int wireEncodeCount;
        /** 载荷总字节数（超限判定与遥测）。 */
        private long wirePayloadBytes;
        /** EXPLAIN 里查询体明细只展开一次（其余引用点指回首个节点），避免「看起来又扫了 N 次」。 */
        private boolean bodyShown;
        /** 同层派生体预取组（Planner 规划期设置；{@code null} = 无动作）。 */
        private volatile PrefetchGroup prefetchGroup;

        public Store(String name, Supplier<PlannedBody> bodySupplier) {
            this.name = name;
            this.bodySupplier = bodySupplier;
        }

        public String name() {
            return name;
        }

        private PlannedBody body() {
            synchronized (lock) {
                if (body == null) {
                    body = bodySupplier.get();
                }
                return body;
            }
        }

        public List<String> columns() {
            return body().columns();
        }

        /**
         * 只<b>规划</b>查询体（不执行、不产生 IO）—— 供同一层多个派生体并行物化前，把会写
         * Planner 状态的规划工作先在调用线程上串行做完。幂等：已规划直接返回缓存。
         */
        public void prepare() {
            body();
        }

        /** 物化输出真实 schema（int 位宽 / decimal 精度）；未物化或零行时为 {@code null}。 */
        public synchronized List<org.apache.arrow.vector.types.pojo.Field> fields() {
            return fields;
        }

        List<String> bodyExplain() {
            if (bodyShown) {
                return List.of("body executed once for all references"
                        + "（明细见首个 MaterializedCte[" + name + "]）");
            }
            bodyShown = true;
            return body().explain();
        }

        void registerScanner() {
            synchronized (lock) {
                scanners++;
            }
        }

        void scannerClosed() {
            boolean release;
            synchronized (lock) {
                closedScanners++;
                release = !cleaned && closedScanners >= scanners;
            }
            if (release) {
                release();
            }
        }

        /**
         * 显式回收物化目录与缓存载荷（幂等）。由查询收尾统一调用 —— 分布式路径不创建
         * {@link MaterializedCteScanOperator}，不能依赖扫描算子的 {@link #scannerClosed()}，
         * 否则物化文件只能等 JVM 退出时靠 {@code SpillDirs} 的 shutdown hook 清理，
         * 长驻 Master 会随每次多引用 CTE 查询累积。
         */
        public void release() {
            Path toDelete;
            synchronized (lock) {
                if (cleaned) {
                    return;
                }
                cleaned = true;
                toDelete = dir;
                dir = null;
                wirePayloads = null;
                wirePayloadBytes = 0;
            }
            if (toDelete != null) {
                SpillDirs.deleteRecursively(toDelete);
                SpillDirs.unregister(toDelete);
            }
        }

        /** 物化目录（未物化 / 已回收 = {@code null}）—— 探针与排障用。 */
        public Path spillDirectory() {
            synchronized (lock) {
                return dir;
            }
        }

        /** 载荷编码次数（探针：多个引用点共用同一次编码）。 */
        public int wireEncodeCount() {
            synchronized (lock) {
                return wireEncodeCount;
            }
        }

        /** 载荷总字节数（未编码 = 0）。 */
        public long wirePayloadBytes() {
            synchronized (lock) {
                return wirePayloadBytes;
            }
        }

        /**
         * 登记「同一层的多个派生体」为同一预取组：组内任一成员首次取载荷时，把其余成员并行拉起，
         * 自己照常走惰性路径 —— 一层 N 个派生体不再串行跑 N 个完整子查询。每组只拉起一次。
         *
         * <p><b>只在首用（open 期）执行</b>，不在规划期：{@code Planner.plan()} 返回前查询还没登记进
         * {@code QueryManager}，此时派发的 Worker 片段会被僵尸片段回收判成「Master 不认」而取消。
         * 查询体的规划必须由 Planner 在单线程里先做完（见 {@code Planner.prepareMaterializedBodies}）。</p>
         */
        public static void linkPrefetchGroup(List<Store> group) {
            if (group == null || group.size() < 2) {
                return;
            }
            PrefetchGroup pg = new PrefetchGroup(group);
            for (Store store : group) {
                store.prefetchGroup = pg;
            }
        }

        /** 预取组状态：组内共享的「只拉起一次」开关 + 成员列表。 */
        private static final class PrefetchGroup {
            final List<Store> members;
            final AtomicBoolean started = new AtomicBoolean();

            PrefetchGroup(List<Store> members) {
                this.members = List.copyOf(members);
            }
        }

        /**
         * 全部物化文件的 wire 载荷（每个文件一份，首次调用编码并缓存）。
         *
         * <p>多个引用点共享同一份编码结果 —— 旧实现在每个引用点都重读并重编码全部文件，
         * N 个引用即 N 倍 CPU / 内存峰值。总量超过 {@code query.cte.wire.maxBytes}
         * 直接失败（不静默回退），避免一条查询吃穿 Master 堆。</p>
         */
        public List<byte[]> wirePayloads() throws Exception {
            List<Future<?>> siblings = startSiblingPrefetch();
            try {
                return wirePayloadsLocked();
            } finally {
                joinPrefetch(siblings);
            }
        }

        /**
         * 把预取组里<b>除自己以外</b>的物化体并行拉起（各自独立 queryId / 片段池；不碰 Planner 状态）。
         * 自己由调用线程随后物化 —— 与其余体并行。已在预取中的组员走各自的惰性路径。
         */
        private List<Future<?>> startSiblingPrefetch() {
            PrefetchGroup group = prefetchGroup;
            if (group == null || group.members.size() < 2 || !group.started.compareAndSet(false, true)) {
                return List.of();
            }
            ExecutorService pool = Executors.newFixedThreadPool(Math.min(group.members.size(), 8), r -> {
                Thread t = new Thread(r, "dojao-derive-materialize");
                t.setDaemon(true);
                return t;
            });
            List<Future<?>> tasks = new ArrayList<>(group.members.size());
            for (Store sibling : group.members) {
                if (sibling == this) {
                    continue;
                }
                tasks.add(pool.submit(() -> {
                    try {
                        sibling.wirePayloads();
                    } catch (Throwable ignored) {
                        // 失败由各 Store 记录并缓存；首个用到它的 planSlices / 扫描会把完整错误抛出来。
                    }
                }));
            }
            return tasks;
        }

        /** 等预取组跑完再返回 —— 避免查询收尾回收目录时组员还在写盘。 */
        private static void joinPrefetch(List<Future<?>> tasks) {
            for (Future<?> task : tasks) {
                try {
                    task.get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (ExecutionException ignored) {
                    // 同上：预取失败不在这里上抛，保持惰性路径的错误可见性。
                }
            }
        }

        private List<byte[]> wirePayloadsLocked() throws Exception {
            synchronized (lock) {
                if (wirePayloads != null) {
                    return wirePayloads;
                }
                long t0 = System.nanoTime();
                List<Path> fs = materialize();
                long max = ResourceConfig.cteWireMaxBytes();
                long total = 0L;
                List<byte[]> out = new ArrayList<>(fs.size());
                for (Path file : fs) {
                    byte[] payload = MaterializedCteConnector.encodeFile(file);
                    if (payload.length == 0) {
                        continue;
                    }
                    total += payload.length;
                    if (max > 0 && total > max) {
                        throw new IllegalStateException("物化 CTE 载荷超过上限（" + name + "）："
                                + "已编码 " + total + " 字节 > " + ResourceConfig.QUERY_CTE_WIRE_MAX_BYTES
                                + "=" + max + "；请调高该配置，或减少物化 CTE 的引用/结果规模。");
                    }
                    out.add(payload);
                }
                wireEncodeCount++;
                wirePayloadBytes = total;
                wirePayloads = List.copyOf(out);
                System.err.printf("[MaterializedCte] %s 载荷编码完成 payloads=%d bytes=%d 耗时=%dms%n",
                        name, out.size(), total, (System.nanoTime() - t0) / 1_000_000L);
                return wirePayloads;
            }
        }

        /** 首次调用执行查询体并落盘；后续调用直接返回文件列表。 */
        List<Path> materialize() throws Exception {
            synchronized (lock) {
                if (failure != null) {
                    throw new IllegalStateException("物化 CTE 执行失败（" + name + "）", failure);
                }
                if (files != null) return files;

                long t0 = System.nanoTime();
                long rows = 0L;
                List<Path> written = new ArrayList<>();
                Operator op = body().operator();
                try {
                    op.open();
                    Page p;
                    while (!(p = op.next()).isEOF()) {
                        try {
                            if (p.getRowCount() > 0) {
                                if (dir == null) {
                                    dir = spillDir();
                                    Files.createDirectories(dir);
                                    SpillDirs.register(dir);
                                }
                                Path file = dir.resolve("cte_" + written.size() + ".arrow");
                                AbstractSpillableOperator.writeArrowFile(file, p.getRoot());
                                written.add(file);
                                rows += p.getRowCount();
                                if (fields == null) {
                                    fields = List.copyOf(p.getRoot().getSchema().getFields());
                                }
                            }
                        } finally {
                            p.close();
                        }
                    }
                } catch (Exception e) {
                    failure = e;
                    if (dir != null) {
                        SpillDirs.deleteRecursively(dir);
                        SpillDirs.unregister(dir);
                        dir = null;
                        cleaned = true;
                    }
                    throw e;
                } finally {
                    try { op.close(); } catch (Exception ignored) { }
                }
                files = List.copyOf(written);
                System.err.printf("[MaterializedCte] %s 物化完成 rows=%d files=%d 耗时=%dms%n",
                        name, rows, written.size(), (System.nanoTime() - t0) / 1_000_000L);
                return files;
            }
        }

        private Path spillDir() {
            List<Path> roots = ResourceConfig.spillDirs();
            Path root = roots.get(Math.floorMod(SPILL_ROOT_SEQUENCE.getAndIncrement(), roots.size()));
            String safe = name.replaceAll("[^A-Za-z0-9_]+", "_");
            return root.resolve("cte-" + safe + "-p" + ProcessHandle.current().pid()
                    + "-" + INSTANCE_SEQUENCE.incrementAndGet());
        }
    }
}
