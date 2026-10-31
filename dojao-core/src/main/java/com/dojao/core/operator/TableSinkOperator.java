package com.dojao.core.operator;

import com.dojao.common.data.Page;
import com.dojao.spi.bean.WriteRequest;
import com.dojao.spi.service.DojaoConnector;
import com.dojao.spi.service.OverwriteEnable;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 表写算子 —— 把上游算子链产出的数据页并行写入目标表（连接器 {@link DojaoConnector#write}）。
 *
 * <p><b>并行写</b>：{@code writeParallelism} 个后台写线程（= 上游并行度，硬约束"上游多少 写入时
 * 就多少"，只增不减），各自经连接器持久会话批量写。有界 {@code writeQueue}（容量
 * {@link #WRITE_QUEUE_CAPACITY}）提供背压，写线程把已写页放入无界 {@code doneQueue} 回传调用方
 * 计数与关闭。Page 透传语义：写入后原样返回同一 Page，由调用方负责计数行数并 close
 * （连接器 write 只读不关页，所有权仍归调用方）。</p>
 *
 * <p>支持两种形态：
 * <ul>
 *   <li>INSERT INTO ... SELECT：{@code createTable=false}，目标表已存在，直接逐页写入</li>
 *   <li>CREATE TABLE AS SELECT：{@code createTable=true}，首个数据页到达时取其 Arrow schema
 *       调用 {@link DojaoConnector#createTable} 建表，再并行写入。标准 SQL 语义：表已存在且
 *       用户未写 {@code IF NOT EXISTS} → 建表报错；写 {@code IF NOT EXISTS} → 跳过建表与写入
 *       （不把 SELECT 结果误插入旧表）</li>
 * </ul>
 *
 * <p>建表在消费者线程单点执行（保证建表先于任何写），写并行只作用于 {@code connector.write}。</p>
 *
 * @author jiaxin
 * @date 2026/8/10
 */
public final class TableSinkOperator extends AbstractMeasurableOperator {

    private static final int WRITE_QUEUE_CAPACITY = 32;   // 有界背压队列容量
    private static final Page POISON = Page.eof();        // 写线程收尾哨兵（引用判等）

    private Operator upstream;
    private final DojaoConnector connector;
    private final String catalog;
    private final String schema;
    private final String table;
    private final List<String> targetColumns;  // 目标表列名（INSERT 用）；null → 用 Page 列名（CTAS）
    private final int writeParallelism;        // 写线程数 = 上游并行度
    private boolean createTable;   // CTAS：首页建表
    private boolean ifNotExists;   // 用户 SQL 是否写 IF NOT EXISTS（表已存在则跳过建表与写入）
    private boolean tableCreated;

    // ===== INSERT OVERWRITE 生命周期（可选；staging 与提交契约见 OverwriteEnable） =====
    private OverwriteEnable overwrite;   // null = 普通 append（INSERT INTO / CTAS）
    private volatile String writeTable;  // writerLoop 实际写入的表名（overwrite 时 = staging 标识）
    private String stagingId;            // beginOverwrite 产出；doClose 未提交则据此 abort
    private boolean overwriteCommitted;

    // ===== 并行写通道 =====
    private final BlockingQueue<Page> writeQueue = new LinkedBlockingQueue<>(WRITE_QUEUE_CAPACITY);
    private final BlockingQueue<Page> doneQueue  = new LinkedBlockingQueue<>();
    private final AtomicInteger writersFinished = new AtomicInteger();
    private final List<Page> drainBuffer = new ArrayList<>();  // EOF 收尾缓冲：先返回所有已写页再给 EOF
    private volatile Throwable writerFailure;
    private ExecutorService writerPool;
    private Page eofPage;        // 上游 EOF 页（收尾后返回）
    private boolean eofMode;     // 已进入 EOF 收尾：只从 drainBuffer 出页

    public TableSinkOperator(Operator upstream, DojaoConnector connector,
                             String catalog, String schema, String table) {
        this(upstream, connector, catalog, schema, table, false, false, null, 1);
    }

    public TableSinkOperator(Operator upstream, DojaoConnector connector,
                             String catalog, String schema, String table, boolean createTable) {
        this(upstream, connector, catalog, schema, table, createTable, false, null, 1);
    }

    /**
     * @param ifNotExists       用户 SQL 是否写 IF NOT EXISTS（仅 CTAS 有意义；表已存在 → 跳过建表+写入）
     * @param targetColumns     目标表列名（按位置对应 SELECT 输出列）。INSERT 时由 Planner 解析
     *                          （列别名子句优先，否则目标表全列）；CTAS 传 null（建表即按 Page 列建）
     * @param writeParallelism  写线程数 = 上游并行度（上下游一致，只增不减）
     */
    public TableSinkOperator(Operator upstream, DojaoConnector connector,
                             String catalog, String schema, String table,
                             boolean createTable, boolean ifNotExists, List<String> targetColumns,
                             int writeParallelism) {
        super("TableSink[" + catalog + "." + schema + "." + table + "]", "TableSink",
                Math.max(1, writeParallelism));
        this.upstream = upstream;
        this.connector = connector;
        this.catalog = catalog;
        this.schema = schema;
        this.table = table;
        this.createTable = createTable;
        this.ifNotExists = ifNotExists;
        this.targetColumns = targetColumns;
        this.writeParallelism = Math.max(1, writeParallelism);
        this.writeTable = table;
    }

    /** 更新上游算子引用（列裁剪优化使用，须在 {@link #doOpen()} 前调用）。 */
    public void setUpstream(Operator upstream) { this.upstream = upstream; }

    /**
     * 挂上 INSERT OVERWRITE 生命周期（须在 {@link #doOpen()} 前调用，只对已存在表有意义，
     * 与 {@code createTable}（CTAS）互斥）。首个数据页到达时 {@code beginOverwrite} 建 staging，
     * 全部页写完后 {@code commitOverwrite} 一次性切换；中途失败/取消则 {@code abortOverwrite}。
     */
    public void setOverwrite(OverwriteEnable overwrite) { this.overwrite = overwrite; }

    /** 目标表名（catalog.schema.table）。 */
    public String catalog() { return catalog; }
    public String schema() { return schema; }
    public String table() { return table; }

    @Override
    protected void doOpen() throws Exception {
        upstream.open();
        AtomicInteger seq = new AtomicInteger();
        writerPool = Executors.newFixedThreadPool(writeParallelism, r -> {
            // 线程名唯一（连接器按 线程名+SQL 建立独立写会话，重名会共享同一 PreparedStatement）
            Thread t = new Thread(r, "table-sink-writer-" + catalog + "." + schema + "." + table
                    + "-" + seq.incrementAndGet());
            t.setDaemon(true);
            return t;
        });
        for (int i = 0; i < writeParallelism; i++) {
            writerPool.execute(this::writerLoop);
        }
    }

    @Override
    protected Page doNext() throws Exception {
        checkWriterFailure();
        // EOF 收尾模式：先把已写页全部返回，再返回 EOF
        if (eofMode) {
            if (!drainBuffer.isEmpty()) return drainBuffer.remove(0);
            return eofPage;
        }

        Page page = upstream.next();
        recordInput(page);
        if (page.isEOF()) {
            eofPage = page;
            eofMode = true;
            finishWriting();   // 发 POISON、等所有写线程收尾、收集剩余已写页
            checkWriterFailure();
            commitOverwriteIfNeeded();
            return drainBuffer.isEmpty() ? eofPage : drainBuffer.remove(0);
        }
        if (page.getRoot() == null) return page;   // 分区结束标记：透传，不写

        // CTAS：首个数据页到达时用其 schema 建表（消费者线程单点，保证建表先于任何写）
        if (createTable && !tableCreated) {
            List<Field> fields = page.getRoot().getSchema().getFields();
            // false = 表已存在且 IF NOT EXISTS 跳过 → 本语句不建表不写入（rows_inserted = 0）
            boolean created;
            try {
                created = connector.createTable(catalog, schema, table, fields, ifNotExists);
            } catch (RuntimeException e) {
                page.close();   // 建表抛错：该页未入队，close 防 Arrow 泄漏
                throw e;
            }
            tableCreated = true;
            createTable = false;
            if (!created) {
                // 排空上游数据页（不写），返回 EOF 结束；避免把 SELECT 结果误写入已存在的旧表
                page.close();
                while (!(page = upstream.next()).isEOF()) {
                    recordInput(page);
                    page.close();
                }
                eofPage = page;
                eofMode = true;
                finishWriting();   // 写线程此刻空闲，POISON 立即被消费退出
                checkWriterFailure();
                return eofPage;
            }
        }

        ensureOverwriteBegun();
        // 入队写（有界队列背压），等待一个已写页回传（doneQueue 无界，写线程不阻塞）
        writeQueue.put(page);
        Page written = doneQueue.take();
        checkWriterFailure();
        return written;
    }

    @Override
    protected void doClose() throws Exception {
        if (writerPool != null) {
            writerPool.shutdownNow();
            try {
                writerPool.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            // 清理孤儿页（未写入的入队页 + 已写未回传页）
            Page p;
            while ((p = writeQueue.poll()) != null) {
                if (p.getRoot() != null) p.close();
            }
            while ((p = doneQueue.poll()) != null) {
                if (p.getRoot() != null) p.close();
            }
        }
        for (Page p : drainBuffer) {
            if (p.getRoot() != null) p.close();
        }
        drainBuffer.clear();
        if (overwrite != null && !overwriteCommitted && stagingId != null) {
            try {
                overwrite.abortOverwrite(catalog, schema, table, stagingId);
            } catch (Throwable t) {
                // 收尾阶段不因清理失败掩盖原始异常；staging 是 _ 前缀临时目录，读路径会跳过
                System.err.println("[TableSink] 清理 overwrite staging 失败: " + catalog + "."
                        + schema + "." + table + " —— " + t);
            }
        }
        connector.finishWrite();
        upstream.close();
    }

    // ==================== INSERT OVERWRITE 生命周期 ====================

    /** 首个数据页前建 staging（消费者线程单点，先于任何 write 入队）。 */
    private void ensureOverwriteBegun() {
        if (overwrite == null || stagingId != null) return;
        stagingId = overwrite.beginOverwrite(catalog, schema, table);
        writeTable = stagingId;
    }

    /**
     * 全部页写完（写线程已停）后一次性提交覆盖。<b>空结果也要提交</b> ——
     * {@code INSERT OVERWRITE ... SELECT} 命中 0 行在 Hive 里就是把表清空，不是「什么都不做」。
     */
    private void commitOverwriteIfNeeded() {
        if (overwrite == null || overwriteCommitted) return;
        ensureOverwriteBegun();
        try {
            // 先把所有写会话 flush 落盘（staging 里的 ORC 文件必须完整），再搬运
            connector.finishWrite();
            overwrite.commitOverwrite(catalog, schema, table, stagingId);
            overwriteCommitted = true;
        } catch (RuntimeException e) {
            throw new RuntimeException("TableSink 覆盖提交失败: " + catalog + "." + schema + "." + table, e);
        }
    }

    // ==================== 并行写内部 ====================

    /** 写线程主体：取页 → 写 → 回传。POISON 消费后计数并退出。 */
    private void writerLoop() {
        try {
            while (true) {
                Page page = writeQueue.take();   // 有界队列 → 背压
                if (page == POISON) {
                    writersFinished.incrementAndGet();
                    return;
                }
                if (writerFailure == null) {
                    try {
                        connector.write(new WriteRequest(catalog, schema, writeTable, page, targetColumns));
                    } catch (Throwable t) {
                        writerFailure = t;   // 记录失败，进入透传模式继续消费，避免生产端卡死
                    }
                }
                doneQueue.put(page);           // 无界 → 不会阻塞写线程
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            writersFinished.incrementAndGet(); // 关停路径：不再等它
        }
    }

    /** EOF 收尾：发 K 个 POISON → 等所有写线程结束 → 把剩余已写页收集进 drainBuffer。 */
    private void finishWriting() throws Exception {
        for (int i = 0; i < writeParallelism; i++) {
            writeQueue.put(POISON);
        }
        while (writersFinished.get() < writeParallelism) {
            Page done = doneQueue.poll(100, TimeUnit.MILLISECONDS);
            if (done != null) drainBuffer.add(done);
            checkWriterFailure();
        }
        Page done;
        while ((done = doneQueue.poll()) != null) {
            drainBuffer.add(done);
        }
    }

    private void checkWriterFailure() {
        if (writerFailure != null) {
            throw new RuntimeException("TableSink 写线程失败: " + catalog + "." + schema + "." + table,
                    writerFailure);
        }
    }
}
