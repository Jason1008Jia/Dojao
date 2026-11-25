package com.dojao.ui.monitor;

import java.util.List;

/**
 * 单算子实时指标 —— 监控页「算子 DAG」中一个节点的全部可视化字段。
 *
 * <p>{@code idx} 为算子在执行 DAG 中的节点下标（0 = 根，即最下游算子）；
 * {@code children} 为该节点在 DAG 中的子节点下标列表（上游 = 它的输入源，
 * 即物理计划树里 operator 的 child 算子），前端据此绘制有向无环图。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/6
 */
public record MonitorOperator(
        int idx,
        String operatorId,
        String type,
        String state,
        int parallelism,
        long rowsIn,
        long rowsOut,
        /** 累计输入字节（Input 流量）。 */
        long inputBytes,
        /** 累计输出字节（Output 流量）。 */
        long outputBytes,
        long currentMemBytes,
        long peakMemBytes,
        /** 累计溢写字节（写盘吞吐：首次溢写 + 每次归并重写）。 */
        long spilledBytes,
        /** spilledBytes 中属于归并重写的部分：首次溢写量 = spilledBytes − compactionBytes。 */
        long compactionBytes,
        /** 累计溢写文件数（含归并重写）；平均文件大小 = spilledBytes / spillFiles。 */
        long spillFiles,
        /** 累计溢写触发次数（分区缓冲槽满 → 提交一次溢写）。 */
        long spillTriggers,
        /** 溢写目录峰值实际磁盘占用（与累计写入量口径不同，可与 du 对照）。 */
        long peakSpillDiskBytes,
        long cpuMs,
        long elapsedMs,
        long instantRowsPerSec,
        long avgRowsPerSec,
        /** 瞬时流量（字节/秒）：两次快照间 outputBytes 差分 / 时间差。 */
        long instantBytesPerSec,
        /** 累计平均流量（字节/秒）：outputBytes / 算子耗时。 */
        long avgBytesPerSec,
        String failureMessage,
        List<Integer> children,
        List<MonitorTask> tasks) {
}
