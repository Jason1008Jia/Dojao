package com.dojao.server.master;

/**
 * 集群资源分配策略（接口）—— 后续可插拔：默认 / 公平 / 权重 / 队列优先级等。
 * v1 只搭架子，{@link DefaultResourceAllocator} 为占位实现。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/11
 */
public interface ResourceAllocator {

    /** Job 开始运行前分配资源（按请求与集群现状授予并行度/内存）。 */
    ResourceAllocation allocate(JobRequest request, ClusterResources cluster);

    /** Job 结束（无论成败 / 取消）后归还资源。 */
    void release(Job job, ResourceAllocation allocation, ClusterResources cluster);
}
