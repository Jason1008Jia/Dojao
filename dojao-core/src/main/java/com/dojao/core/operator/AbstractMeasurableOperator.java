package com.dojao.core.operator;

import com.dojao.common.data.Page;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CancellationException;

/**
 *
 * 可度量算子抽象基类 —— 封装监控样板代码，子类只需关注业务逻辑。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/12
 */
public abstract class AbstractMeasurableOperator implements MeasurableOperator {

    private static final Logger log = LoggerFactory.getLogger(AbstractMeasurableOperator.class);

    private final OperatorStats stats;
    private volatile boolean started;
    private volatile boolean finished;

    protected AbstractMeasurableOperator(String operatorId, String operatorType, int parallelism) {
        this.stats = new OperatorStats(operatorId, operatorType, parallelism);
    }

    // ==================== 子类实现 ====================

    protected abstract void doOpen() throws Exception;
    protected abstract Page doNext() throws Exception;
    protected abstract void doClose() throws Exception;

    // ==================== Operator 生命周期 ====================

    @Override
    public void open() {
        onStart();
        try {
            doOpen();
        } catch (Exception e) {
            preserveInterrupt(e);
            throw new RuntimeException("Operator " + stats.getOperatorId() + " open failed: "
                    + rootCauseSummary(e), e);
        }
    }

    @Override
    public Page next() {
        // ★取消检查点：执行线程被中断 = 查询被取消（QueryManager.cancel / WorkerManager.cancelTask）。
        //   过去只有阻塞点（队列 poll / sleep）能感知中断，纯 CPU 的 doNext（归并、排序、聚合）
        //   会在取消后继续烧 CPU：任务槽已摘除（WebUI 显示 0 个运行中），线程却永不退出。
        //   在这里统一抬头，任何算子链的每次拉取都能立刻收到取消信号。
        if (Thread.currentThread().isInterrupted()) {
            throw new CancellationException("operator cancelled: " + stats.getOperatorId());
        }
        long startNanos = System.nanoTime();
        try {
            Page page = doNext();
            long elapsed = System.nanoTime() - startNanos;
            recordCPUTime(elapsed);
            recordOutput(page);
            return page;
        } catch (Exception e) {
            preserveInterrupt(e);
            throw new RuntimeException("Operator " + stats.getOperatorId() + " next failed: "
                    + rootCauseSummary(e), e);
        }
    }

    /**
     * 根因摘要 —— 异常链<b>最深一层</b>的类型 + 首行 message。
     *
     * <h3>为什么外壳要带根因</h3>
     * <p>算子包装层（本类与 {@link AbstractSpillableOperator}）在链顶只暴露
     * {@code Operator Limit next failed} 这类没有任何信息量的外壳；真因（Arrow 分配多大失败 / 磁盘满 /
     * 溢写读回异常 / 类型不匹配）埋在链底。线上工具侧往往只回传顶层 message 与截断栈，使用者据此
     * 无法判断该调内存、清磁盘还是改 SQL，只能反复猜。这里把根因摘要直接写进外壳 message：
     * 完整 cause 链与堆栈照旧保留（零信息损失），但第一行就能看出失败类别。</p>
     *
     * <p>只取「类型 + 首行」：多层 {@code getMessage()} 全量拼接会变成一屏文字，
     * 反而把关键信息淹掉。空 message 只给类型名；超长截断到 300 字符。</p>
     */
    static String rootCauseSummary(Throwable e) {
        if (e == null) {
            return "unknown";
        }
        // 链上若已有「自带现场」的包装（溢写算子的 Phase 2 失败上下文），整条 message 都要保留：
        // 那里面的溢写字节 / 目录剩余空间 / 根因摘要比再抽一层根因更有用。
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof AbstractSpillableOperator.Phase2Exception p2) {
                return p2.getMessage();
            }
            if (t.getCause() == t) {
                break;
            }
        }
        Throwable root = e;
        // 防御自引用 / 异常环（第三方异常链偶尔会 getCause() 指回自己）
        for (int depth = 0; root.getCause() != null && root.getCause() != root && depth < 32; depth++) {
            root = root.getCause();
        }
        String cls = root.getClass().getSimpleName();
        String msg = root.getMessage();
        if (msg == null || msg.isBlank()) {
            return cls;
        }
        String first = msg.strip();
        int cut = first.indexOf('\n');
        if (cut < 0) {
            cut = first.indexOf('\r');
        }
        if (cut >= 0) {
            first = first.substring(0, cut).strip();
        }
        if (first.length() > 300) {
            first = first.substring(0, 300) + "…";
        }
        return cls + ": " + first;
    }

    /**
     * 保留线程的中断标记 —— 这里的包装（"Operator X next failed"）会把取消信号一并埋进
     * 异常链，而底层线程池（FutureTask.get / 阻塞队列）在抛 InterruptedException 时会
     * <b>清掉</b>线程的中断标记。上层（FragmentExecutor）靠 <code>isInterrupted()</code>
     * 区分「取消」与「真失败」，标记丢了就会把取消误报成 FragmentFailed。
     * 所以包装前先把标记补回：异常里带 InterruptedException，或原本就已被中断，都补。
     */
    private static void preserveInterrupt(Throwable e) {
        if (Thread.currentThread().isInterrupted()) return;
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    @Override
    public void close() {
        try {
            doClose();
        } catch (Exception e) {
            log.warn("Operator {} close error: {}", stats.getOperatorId(), e.getMessage());
        }
        onFinish();
    }

    // ==================== MeasurableOperator 钩子 ====================

    @Override
    public void onStart() {
        if (started) return;
        started = true;
        stats.markStart();
        log.debug("Operator {} started", stats.getOperatorId());
    }

    @Override
    public void onFinish() {
        if (finished) return;
        finished = true;
        stats.markEnd();
        log.debug("Operator {} finished, elapsed={}ms, rowsIn={}, rowsOut={}",
                stats.getOperatorId(), stats.getElapsedMs(),
                stats.getRowsProcessed(), stats.getRowsOutput());
    }

    @Override
    public void markFailed(Throwable t) {
        String msg = t == null ? "unknown" : t.getClass().getSimpleName() + ": " + t.getMessage();
        stats.markFailed(msg);
    }

    @Override
    public OperatorStats getStats() {
        return stats;
    }

    // ==================== 内存 ====================

    @Override
    public long getCurrentMemoryBytes() {
        return stats.getCurrentMemoryBytes();
    }

    @Override
    public void addSpilledBytes(long bytes) {
        stats.recordSpilledBytes(bytes);
    }

    // ==================== 吞吐量记录 ====================

    @Override
    public void recordInput(Page page) {
        if (page.isEOF()) return;
        stats.recordRowsProcessed(page.getRowCount());
        stats.recordInputBytes(page.getSizeInBytes());
        stats.recordCurrentMemory(page.getSizeInBytes());
    }

    /**
     * 释放输入页的内存跟踪 —— 当输入页被 close 时调用，递减当前内存估算。
     */
    public void releaseInput(Page page) {
        if (page.isEOF()) return;
        stats.recordCurrentMemory(-page.getSizeInBytes());
    }

    @Override
    public void recordOutput(Page page) {
        if (page.isEOF()) return;
        stats.recordRowsOutput(page.getRowCount());
        stats.recordOutputBytes(page.getSizeInBytes());
    }

    @Override
    public void recordCPUTime(long nanos) {
        stats.recordCpuTime(nanos);
    }
}
