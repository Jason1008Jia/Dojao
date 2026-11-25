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
    /**
     * 累计溢写字节中<b>归并重写</b>（compaction）的部分。{@link #spilledBytes} 是写盘吞吐总量
     * （首次溢写 + 每次归并重写），本字段单独记后者 ⇒ 首次溢写量 = spilledBytes − compactionBytes，
     * 与磁盘上实际残留的数据量同量级（监控用它解释「为什么 WebUI 的溢写比 du 大几倍」）。
     */
    private final AtomicLong compactionBytes = new AtomicLong(0);
    /** 已写出的溢写文件数（含归并重写产生的批次文件）——平均文件大小 = spilledBytes / spillFiles。 */
    private final AtomicLong spillFiles = new AtomicLong(0);
    /** 溢写触发次数（一个分区缓冲槽满 → 提交一次溢写 = 一次）。与 spillFiles 分开：归并会写文件但不触发。 */
    private final AtomicLong spillTriggers = new AtomicLong(0);
    /**
     * 溢写目录的峰值磁盘占用（字节，实际遍历目录求和，含换盘后的多个目录）。
     * 与累计写入量的区别：归并改写 / 消费后删除的文件不重复计入 ⇒ 反映「任一时刻最多占了多少盘」。
     */
    private volatile long peakSpillDiskBytes;

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
     *
     * <p>注意：这是<b>写盘吞吐</b>口径（首次溢写 + 每次归并重写都算）。想拿「数据集规模」看
     * {@link #getLogicalSpilledBytes()}，想拿「实际占了多少盘」看 {@link #getPeakSpillDiskBytes()}。</p>
     */
    void recordSpilledBytes(long bytes) {
        spilledBytes.addAndGet(bytes);
    }

    /** 记账一次归并重写（这部分同时已计入 {@link #spilledBytes}，此处只做拆分标记）。 */
    void recordCompactionBytes(long bytes) {
        compactionBytes.addAndGet(bytes);
    }

    /** 记一次溢写文件写出（含归并重写）。 */
    void recordSpillFile() {
        spillFiles.incrementAndGet();
    }

    /** 记一次溢写触发（分区缓冲槽达到阈值 → 提交溢写）。 */
    void recordSpillTrigger() {
        spillTriggers.incrementAndGet();
    }

    /** 记录一次溢写目录磁盘占用采样，仅当高于历史峰值时更新（best-effort，采样线程并发调用）。 */
    void recordPeakSpillDiskBytes(long bytes) {
        if (bytes > peakSpillDiskBytes) {
            peakSpillDiskBytes = bytes;
        }
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
    /** 首次溢写字节（不含归并重写）= 溢写总量 − 重写量；≈ 数据集本身的溢写规模。 */
    public long getLogicalSpilledBytes() { return Math.max(0L, spilledBytes.get() - compactionBytes.get()); }
    /** 累计归并重写字节（已包含在 {@link #getSpilledBytes()} 中）。 */
    public long getCompactionBytes() { return compactionBytes.get(); }
    public long getSpillFiles() { return spillFiles.get(); }
    public long getSpillTriggers() { return spillTriggers.get(); }
    /** 溢写目录峰值磁盘占用（实际目录遍历结果；0 = 尚未采样 / 无溢写）。 */
    public long getPeakSpillDiskBytes() { return peakSpillDiskBytes; }
    public long getCpuTimeNanos() { return cpuTimeNanos.get(); }
    public long getInputBytes() { return inputBytes.get(); }
    public long getOutputBytes() { return outputBytes.get(); }
}
