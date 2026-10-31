package com.dojao.ui.monitor;

/**
 * 单个分布式 Worker 任务（片段任务 / 分区扫描任务）的监控摘要。
 *
 * <p>对应 server 侧 {@code OperatorSnapshot.Task}，再降为 UI 需要的字段：
 * 去掉了 dispatch/finish 纳秒时间，补了人类可读耗时。{@code state} 取值
 * RUNNING / FINISHED / FAILED / LOST；{@code failure} 失败原因（成功为空）；
 * {@code source} = 该任务扫描的基表（{@code schema.table}，非扫描片段为 null），
 * 前端据此把 DAG 的 Scan 叶子节点按数据来源区分命名；{@code expectedRows} =
 * 该源表连接器探查的行数总量估计（命中才 &gt;0，否则 -1），Scan 进度条分母（实时扫行/总量）。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/6
 */
public record MonitorTask(String taskId, String workerId, String kind, boolean producer,
                          String state, long rows, long elapsedMs, String failure, String source,
                          long expectedRows) {
}
