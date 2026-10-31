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

    @Override
    protected Page doNext() throws Exception {
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
        private Throwable failure;
        private Path dir;
        private int scanners;
        private int closedScanners;
        private boolean cleaned;
        /** EXPLAIN 里查询体明细只展开一次（其余引用点指回首个节点），避免「看起来又扫了 N 次」。 */
        private boolean bodyShown;

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
            boolean remove;
            synchronized (lock) {
                closedScanners++;
                remove = !cleaned && closedScanners >= scanners;
                if (remove) cleaned = true;
            }
            if (remove) {
                synchronized (lock) {
                    if (dir != null) {
                        SpillDirs.deleteRecursively(dir);
                        SpillDirs.unregister(dir);
                    }
                }
            }
        }

        /** 首次调用执行查询体并落盘；后续调用直接返回文件列表。 */
        List<Path> materialize() throws Exception {
            synchronized (lock) {
                if (failure != null) {
                    throw new IllegalStateException("物化 CTE 执行失败（" + name + "）", failure);
                }
                if (files != null) return files;

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
