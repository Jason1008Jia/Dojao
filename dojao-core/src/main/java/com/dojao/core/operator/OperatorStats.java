package com.dojao.core.operator;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 算子运行时统计，实时更新，供 WebUI 可视化和监控使用。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/11
 */
public class OperatorStats {

    private final String operatorId;
    private final String operatorType;
    private final int parallelism;

    // 运行状态（QueryManager / WebUI 实时读取）
    private volatile OperatorState state = OperatorState.NOT_STARTED;
    private volatile String failureMessage;

    // 时间
    private long startTimeNanos;
    private long endTimeNanos;

    // 行数
    private final AtomicLong rowsProcessed = new AtomicLong(0);
    private final AtomicLong rowsOutput = new AtomicLong(0);

    // 内存
    private final AtomicLong currentMemoryBytes = new AtomicLong(0);
    private volatile long peakMemoryBytes;
    private final AtomicLong spilledBytes = new AtomicLong(0);

    // CPU
    private final AtomicLong cpuTimeNanos = new AtomicLong(0);

    // IO
    private final AtomicLong inputBytes = new AtomicLong(0);
    private final AtomicLong outputBytes = new AtomicLong(0);

    public OperatorStats(String operatorId, String operatorType, int parallelism) {
        this.operatorId = operatorId;
        this.operatorType = operatorType;
        this.parallelism = parallelism;
    }

    // ==================== 状态 ====================

    void markStart() {
        this.startTimeNanos = System.nanoTime();
        this.state = OperatorState.RUNNING;
    }

    void markEnd() {
        this.endTimeNanos = System.nanoTime();
        this.state = OperatorState.FINISHED;
    }

    /** 标记算子失败（由 QueryManager 在查询失败时统一调用，把未完成算子置 FAILED）。 */
    void markFailed(String message) {
        this.failureMessage = message;
        this.state = OperatorState.FAILED;
    }

    public OperatorState getState() { return state; }

    public String getFailureMessage() { return failureMessage; }

    // ==================== 时间 ====================

    public long getElapsedMs() {
        if (startTimeNanos == 0) return 0;
        long end = endTimeNanos > 0 ? endTimeNanos : System.nanoTime();
        return (end - startTimeNanos) / 1_000_000;
    }

    // ==================== 行数 ====================

    void recordRowsProcessed(long n) { rowsProcessed.addAndGet(n); }

    void recordRowsOutput(long n) { rowsOutput.addAndGet(n); }

    // ==================== 内存 ====================

    void recordCurrentMemory(long bytes) {
        long cur = currentMemoryBytes.addAndGet(bytes);
        // track peak
        long peak;
        while ((peak = peakMemoryBytes) < cur) {
            // CAS to update peak (best-effort, don't block)
            // volatile write is simpler:
            peakMemoryBytes = cur;
            break;
        }
    }

    /**
     * 记账累计溢写字节。<b>只更新 {@code spilledBytes}，不得再扣 {@code currentMemoryBytes}</b>。
     *
     * <p>后者只累计「在途输入页」字节（{@code recordInput} / {@code releaseInput} 成对归零），
     * 聚合哈希表 / 排序缓冲等算子状态<b>从未</b>计入其中。旧实现每次溢写都
     * {@code currentMemoryBytes.addAndGet(-bytes)} ⇒ 累计溢写量被从一个从未包含它的计数器里
     * 扣掉，算子一溢写就显示 {@code Memory = -Σspill}（线上曾出现
     * {@code Memory=-1759560748 B}，即约 1.64GB 溢写量取负）。溢写量是累计吞吐指标，
     * 与瞬时驻留内存无关，两个口径必须分开。</p>
     */
    void recordSpilledBytes(long bytes) {
        spilledBytes.addAndGet(bytes);
    }

    // ==================== CPU ====================

    void recordCpuTime(long nanos) { cpuTimeNanos.addAndGet(nanos); }

    // ==================== IO ====================

    void recordInputBytes(long bytes) { inputBytes.addAndGet(bytes); }

    void recordOutputBytes(long bytes) { outputBytes.addAndGet(bytes); }

    // ==================== Getter ====================

    public String getOperatorId() { return operatorId; }
    public String getOperatorType() { return operatorType; }
    public int getParallelism() { return parallelism; }
    public long getStartTimeNanos() { return startTimeNanos; }
    public long getEndTimeNanos() { return endTimeNanos; }
    public long getRowsProcessed() { return rowsProcessed.get(); }
    public long getRowsOutput() { return rowsOutput.get(); }
    public long getCurrentMemoryBytes() { return currentMemoryBytes.get(); }
    public long getPeakMemoryBytes() { return peakMemoryBytes; }
    public long getSpilledBytes() { return spilledBytes.get(); }
    public long getCpuTimeNanos() { return cpuTimeNanos.get(); }
    public long getInputBytes() { return inputBytes.get(); }
    public long getOutputBytes() { return outputBytes.get(); }
}
