package com.dojao.ui.monitor;

/**
 * 单个命名分配器类别的直接内存用量快照 —— 监控页「直接内存明细」逐类别用量 / 峰值 / 上限。
 *
 * <p>{@code category} 例如 {@code op/hashjoin}、{@code connector/mysql}、{@code exchange/local}；
 * {@code limitBytes} = 该类别所有活跃子分配器上限的最大值（0 = 未显式限制，受根预算约束）。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/07
 */
public record MonitorAllocator(
        String category,
        long allocatedBytes,
        long peakBytes,
        long limitBytes) {
}
