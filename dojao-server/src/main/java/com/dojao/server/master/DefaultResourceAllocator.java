package com.dojao.server.master;

/**
 * 默认资源分配策略（占位）：
 * 请求并行度与集群可用 slot 取小，至少授予 1 个 slot。
 * 后续真正分配算法（公平 / 权重 / 队列优先级 / 内存配额）在此演进。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/11
 */
public final class DefaultResourceAllocator implements ResourceAllocator {

    @Override
    public ResourceAllocation allocate(JobRequest request, ClusterResources cluster) {
        int wanted = request.parallelism() > 0 ? request.parallelism() : cluster.totalSlots();
        int granted = Math.max(1, Math.min(wanted, cluster.availableSlots()));
        cluster.reserve(granted);
        return new ResourceAllocation(granted, 0L);
    }

    @Override
    public void release(Job job, ResourceAllocation allocation, ClusterResources cluster) {
        cluster.release(allocation.parallelism());
    }
}
