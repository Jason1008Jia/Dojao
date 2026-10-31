package com.dojao.server.master;

/**
 * Job 生命周期状态。
 *
 * <p>SUBMITTED 提交未入队 → QUEUED 已进入 JobManager 调度队列等待执行 → RUNNING 执行中 →
 * FINISHED / FAILED / CANCELED 终态（不可逆）。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/11
 */
public enum JobState {
    SUBMITTED,
    QUEUED,
    RUNNING,
    FINISHED,
    FAILED,
    CANCELED
}
