package com.dojao.server.master;

/**
 * 资源分配结果 —— 一次 {@link ResourceAllocator#allocate} 授予 Job 的并行度 / 内存。
 *
 * <p>v1 只按并行度分配；memoryBytes 恒为 0，待资源分配策略讨论后启用。
 *
 * @param parallelism  授予的并行度（>=1）
 * @param memoryBytes  授予的内存配额（字节；预留）
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/11
 */
public record ResourceAllocation(int parallelism, long memoryBytes) {
}
