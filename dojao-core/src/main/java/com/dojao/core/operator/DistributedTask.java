package com.dojao.core.operator;

/**
 * 分布式 Worker 任务的不可变可观测快照（监控 DTO）—— 两类 Master 派发的任务统一形态：
 *
 * <ul>
 *   <li><b>片段任务</b>（{@link AbstractDistributedOperator} 派发，见 {@code fragmentTasks()}）：
 *       Master 把一个分布式算子切出的可独立执行任务 —— Producer 扫切片推送 /
 *       Consumer 接桶本地算子回传 / 中间层接桶本地算子转发；</li>
 *   <li><b>扫描任务</b>（{@link DistributedScanOperator} 派发，见 {@code scanTasks()}）：
 *       一个分区扫描切片在某个 Worker 上执行回传。</li>
 * </ul>
 *
 * <p>生命周期：{@code RUNNING}（派发即入）→ {@code FINISHED} / {@code FAILED}；
 * 扫描任务另有 {@code LOST}（看门狗超时判死）。状态与 {@code rowsOut} 由 Master 事件流驱动，
 * 快照为只读、监控线程并发读取安全（记录内字段由 volatile / AtomicLong 支撑）。</p>
 *
 * <p>{@code source} = Producer 片段扫描的基表（{@code schema.table}），非扫描片段为 {@code null}；
 * 监控据此把同算子内不同数据来源的扫描任务分组（DAG 的每个 Scan 叶子节点）。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/6
 */
public record DistributedTask(String taskId, String workerId, String kind,
                              boolean producer, String state, long rowsOut,
                              long dispatchNanos, long finishNanos, String failure,
                              String source) {

    public static final String RUNNING = "RUNNING";
    public static final String FINISHED = "FINISHED";
    public static final String FAILED = "FAILED";
    public static final String LOST = "LOST";

    /** 片段内部分区子任务 id 的分隔符（{@code <fragmentId>#p<分区号>}）。 */
    public static final String PARTITION_SEP = "#p";

    /**
     * 片段内部第 {@code partition} 个分区子任务的 id。
     *
     * <p>有些算子（{@link HashJoinOperator}）把 {@code numPartitions} 个分区线程收在<b>一个</b>片段
     * 任务里执行（跨机时一个 Worker 一个片段）—— 监控只看到 1 个 task，与 Scan/Agg 的「每分区一个
     * task」不一致。Worker 遥测按本约定把每个分区登记为独立子任务（行数 = 该分区真实输出、可独立
     * 收尾），Master 侧据此把片段展开成 n 个分区任务展示，不新增派发、不改数据面。</p>
     */
    public static String partitionTaskId(String taskId, int partition) {
        return taskId + PARTITION_SEP + partition;
    }
}
