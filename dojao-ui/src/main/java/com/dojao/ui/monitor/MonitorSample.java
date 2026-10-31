package com.dojao.ui.monitor;

/**
 * 资源时间序列上的一个采样点 —— 监控页折线图的最小数据单元（纯数值，前端直接画）。
 *
 * <p>{@code cpuPct} = 进程 CPU 使用率（0-100，不可用为 -1）；{@code heapMaxBytes} 为 -1 表示无上限
 * （前端画堆占比时按 0 处理）。{@code usedMemoryBytes}/{@code totalMemoryBytes} 是<b>物理内存</b>口径；
 * {@code directUsedBytes} 是受管直接内存（进程级共享根）口径。
 *
 * <p>{@code diskReadBytesPerSec}/{@code diskWriteBytesPerSec} 是<b>进程级磁盘 IO 速率</b>
 * （字节/秒）—— 由 {@code ResourceSummary} 的累计字节按相邻采样点差分而来；首个采样点 / 非 Linux 为 0。
 *
 * @param t                 采样时刻（wall clock 毫秒）
 * @param cpuPct            进程 CPU 使用率（0-100，-1 = 不可用）
 * @param heapUsedBytes     JVM 堆已用（字节）
 * @param heapMaxBytes      JVM 堆上限（字节，-1 = 无上限）
 * @param threadCount       JVM 活跃线程数
 * @param usedMemoryBytes   已用物理内存（字节）
 * @param totalMemoryBytes  物理内存总量（字节）
 * @param directUsedBytes   受管直接内存已用（字节）
 * @param diskReadBytesPerSec  进程级磁盘读速率（字节/秒；0 = 无数据）
 * @param diskWriteBytesPerSec 进程级磁盘写速率（字节/秒；0 = 无数据）
 * @author jiaxin
 * @version 1.1
 * @date 2026/9/29
 */
public record MonitorSample(
        long t,
        int cpuPct,
        long heapUsedBytes,
        long heapMaxBytes,
        int threadCount,
        long usedMemoryBytes,
        long totalMemoryBytes,
        long directUsedBytes,
        long diskReadBytesPerSec,
        long diskWriteBytesPerSec) {
}
