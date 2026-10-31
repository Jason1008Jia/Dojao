package com.dojao.ui.monitor;

import java.util.List;

/**
 * 集群中单个 Worker 节点的健康/负载摘要 —— 监控页「节点卡片 / 集群总览」数据。
 *
 * <p>{@code role} = {@code "master"} / {@code "worker"}，前端据此给节点卡打标；
 * {@code heapUsedBytes}/{@code heapMaxBytes}/{@code threadCount} 为 JVM 进程压力（堆 / 线程），
 * 与「物理内存」是两个口径（JVM 只占物理内存一部分）。
 *
 * <p>{@code directUsedBytes}/{@code directBudgetBytes}/{@code allocators} 为受管直接内存
 * （进程级共享根，算子 / 连接器 / transport 全量纳入）：用量 / 上限 + 按类别聚合明细。
 *
 * @author jiaxin
 * @version 1.1
 * @date 2026/9/6
 */
public record MonitorWorker(
        String workerId,
        String host,
        int port,
        String state,
        boolean healthy,
        int cpuCores,
        int cpuUsagePct,
        long totalMemoryBytes,
        long usedMemoryBytes,
        int runningQueries,
        String version,
        String role,
        long heapUsedBytes,
        long heapMaxBytes,
        int threadCount,
        long totalDiskBytes,
        long freeDiskBytes,
        int maxConcurrency,
        long directUsedBytes,
        long directBudgetBytes,
        List<MonitorAllocator> allocators,
        long processRssBytes) {
}
