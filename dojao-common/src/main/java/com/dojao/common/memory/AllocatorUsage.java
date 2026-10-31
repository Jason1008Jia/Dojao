package com.dojao.common.memory;

/**
 * 单个「命名分配器类别」的堆外用量快照 —— {@link NodeMemory#snapshot()} 的返回元素，
 * 供监控 UI / 心跳细看每类消费方（算子 / 连接器 / 传输）的当前用量、峰值与上限。
 *
 * @param category       类别名（如 {@code op/hashjoin}、{@code connector/mysql}、{@code exchange/shuffle}）
 * @param allocatedBytes 当前真实占用（字节）；该类别下全部子分配器之和（已 close 的计 0）
 * @param peakBytes      历次采样观测到的峰值占用（字节）
 * @param limitBytes     该类别下单个子分配器的上限（字节）；多实例取其中最大值
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/7
 */
public record AllocatorUsage(String category, long allocatedBytes, long peakBytes, long limitBytes) {
}
