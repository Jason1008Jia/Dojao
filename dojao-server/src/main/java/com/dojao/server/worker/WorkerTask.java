package com.dojao.server.worker;

/**
 * Master 分配给 Worker 的任务描述。
 *
 * @param taskId      任务唯一 ID（Master 侧生成）
 * @param jobId       所属 Job（queryId）
 * @param sql         待执行 SQL
 * @param parallelism 请求并行度（&lt;=0 由 Worker 自行决定）
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/11
 */
public record WorkerTask(String taskId, String jobId, String sql, int parallelism) {
}
