package com.dojao.ui.monitor;

import java.util.List;

/**
 * 集群总览 —— 监控页「集群概览」一屏数据：Master 自身快照 + Worker 列表 + 最近/活动查询。
 *
 * <p>{@code master} 为 Master 进程自身资源快照（{@code workerId = "master"}），
 * 前端把它与 workers 一起渲染成节点卡片。取不到时为 null。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/6
 */
public record MonitorCluster(
        MonitorWorker master,
        List<MonitorWorker> workers,
        List<MonitorJob> activeQueries,
        List<MonitorJob> recentQueries) {
}
