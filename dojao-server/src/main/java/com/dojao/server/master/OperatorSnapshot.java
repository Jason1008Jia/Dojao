package com.dojao.server.master;

import com.dojao.core.operator.OperatorState;

import java.util.List;

/**
 * 单算子实时指标快照 —— 由 {@link QueryManager#snapshot()} 实时采集生成，不可变。
 *
 * <p>{@link #tasks()}：分布式算子（{@link com.dojao.core.operator.AbstractDistributedOperator} /
 * {@link com.dojao.core.operator.DistributedScanOperator}）已派发的 Worker 任务列表（任务级监控）；
 * 单机算子树恒空。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/11
 */
public record OperatorSnapshot(
        String operatorId,
        String operatorType,
        int parallelism,
        OperatorState state,
        /** 输入行数（rowsProcessed）。 */
        long rowsIn,
        /** 输出行数（rowsOutput）。 */
        long rowsOut,
        long inputBytes,
        long outputBytes,
        long cpuMs,
        long currentMemBytes,
        long peakMemBytes,
        long spilledBytes,
        long elapsedMs,
        /** 瞬时吞吐（行/秒）：两次快照间 rowsOut 差分 / 时间差。 */
        long instantRowsPerSec,
        /** 累计平均吞吐（行/秒）：rowsOut / 算子耗时。 */
        long avgRowsPerSec,
        /** 瞬时流量（字节/秒）：两次快照间 outputBytes 差分 / 时间差。 */
        long instantBytesPerSec,
        /** 累计平均流量（字节/秒）：outputBytes / 算子耗时。 */
        long avgBytesPerSec,
        String failureMessage,
        /** 分布式算子已派发的 Worker 任务（无 = 单机算子）。 */
        List<Task> tasks) {

    /** 单个 Worker 任务的实时快照（任务级监控，见 core DistributedTask）。 */
    public record Task(String taskId, String workerId, String kind, boolean producer,
                       String state, long rowsOut,
                       /** 累计读入 / 发出字节（Worker 遥测；非终端片段才有，其余 0）。 */
                       long bytesIn, long bytesOut,
                       long dispatchNanos, long finishNanos,
                       String failure, String source,
                       /** Producer 扫描基表的探查行数总量估计（{@code schema.table} 命中才有，否则 -1）。 */
                       long expectedRows) {
    }
}
