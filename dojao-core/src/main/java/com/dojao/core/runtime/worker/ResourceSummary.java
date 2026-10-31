package com.dojao.core.runtime.worker;

import com.dojao.common.memory.AllocatorUsage;
import com.dojao.common.memory.NodeMemory;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.List;

/**
 * 节点资源摘要 —— Worker 定时采集并随心跳汇报给 Master，作为 Master 分配任务的依据。
 *
 * <p>采集口径：CPU 核数与进程 CPU 使用率（com.sun.management.OperatingSystemMXBean，
 * 非 HotSpot 环境降级为 -1 / 0）；内存总量 / 已用；磁盘总量 / 空闲（所有根目录累加）；
 * JVM 堆使用 / 上限与线程数（MemoryMXBean / ThreadMXBean，监控页用）；受管直接内存
 * （进程级共享根 {@link NodeMemory} 的用量 / 上限 + 按类别聚合明细）。
 *
 * @param cpuCores         CPU 核数
 * @param cpuUsagePct      进程 CPU 使用率（0-100，不可用为 -1）
 * @param totalMemoryBytes 物理内存总量（字节）
 * @param usedMemoryBytes  主机已用物理内存（字节；Linux 用 total - MemAvailable，排除可回收页缓存）
 * @param totalDiskBytes   磁盘总量（字节，所有根目录累加）
 * @param freeDiskBytes    磁盘空闲（字节）
 * @param maxConcurrency   该 Worker 可并发的任务数上限（= 任务池容量），Master 聚合为集群可用并行度
 * @param timestampNanos   采集时刻（nanoTime）
 * @param heapUsedBytes    JVM 堆已用（字节）
 * @param heapMaxBytes     JVM 堆上限（字节，-1 = 无上限）
 * @param threadCount      当前 JVM 活跃线程数
 * @param directUsedBytes  受管直接内存已用（字节，进程级共享根当前用量）
 * @param directBudgetBytes 受管直接内存上限（字节，进程级共享根预算）
 * @param allocators       按类别聚合的直接内存明细（op/… / connector/… / exchange/… 等）
 * @param diskReadBytes    进程级磁盘读累计字节（Linux {@code /proc/self/io} 的 read_bytes；非 Linux 为 0）
 * @param diskWriteBytes   进程级磁盘写累计字节（Linux {@code /proc/self/io} 的 write_bytes；非 Linux 为 0）
 * @param processRssBytes  本进程常驻内存 RSS（Linux {@code /proc/self/status} 的 VmRSS；取不到为 0）
 * @author jiaxin
 * @version 1.2
 * @date 2026/8/11
 */
public record ResourceSummary(int cpuCores, int cpuUsagePct, long totalMemoryBytes, long usedMemoryBytes,
                              long totalDiskBytes, long freeDiskBytes, int maxConcurrency, long timestampNanos,
                              long heapUsedBytes, long heapMaxBytes, int threadCount,
                              long directUsedBytes, long directBudgetBytes, List<AllocatorUsage> allocators,
                              long diskReadBytes, long diskWriteBytes, long processRssBytes) {

    /**
     * 旧 11 参构造（兼容既有探针 / 测试的位置参数调用）：直接内存 / 磁盘 IO / RSS 字段缺省 0 / 空表。
     */
    public ResourceSummary(int cpuCores, int cpuUsagePct, long totalMemoryBytes, long usedMemoryBytes,
                           long totalDiskBytes, long freeDiskBytes, int maxConcurrency, long timestampNanos,
                           long heapUsedBytes, long heapMaxBytes, int threadCount) {
        this(cpuCores, cpuUsagePct, totalMemoryBytes, usedMemoryBytes, totalDiskBytes, freeDiskBytes,
                maxConcurrency, timestampNanos, heapUsedBytes, heapMaxBytes, threadCount,
                0L, 0L, List.of(), 0L, 0L, 0L);
    }

    /** 采集本节点当前资源摘要。 */
    public static ResourceSummary collect() {
        int cores = Runtime.getRuntime().availableProcessors();
        int cpuPct = -1;
        long totalMem = 0L;
        long freeMem = 0L;

        Object osBean = ManagementFactory.getOperatingSystemMXBean();
        if (osBean instanceof com.sun.management.OperatingSystemMXBean sun) {
            double load = sun.getProcessCpuLoad();
            cpuPct = load < 0 ? -1 : (int) Math.round(load * 100);
            totalMem = sun.getTotalMemorySize();
            freeMem = sun.getFreeMemorySize();
        }

        long totalDisk = 0L;
        long freeDisk = 0L;
        for (File root : File.listRoots()) {
            if (root == null) continue;
            totalDisk += root.getTotalSpace();
            freeDisk += root.getFreeSpace();
        }

        // JVM 堆（不等同于物理内存：JVM 只占物理内存一部分；监控页单列，看的是「本进程压力」）
        MemoryUsage heap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        long heapUsed = Math.max(0L, heap.getUsed());
        long heapMax = heap.getMax();

        int threads = ManagementFactory.getThreadMXBean().getThreadCount();

        // Linux 下 MXBean 的 free = MemFree（不含可回收的 page cache / slab）。直接用它算「已用」
        // 会把读 Hive 文件 / 写溢写文件留下的页缓存当成内存泄漏（线上误报：7.1/7.3GB，而本进程
        // 堆只有 81MB、受管直接内存 4.8MB）。改用 /proc/meminfo 的 MemAvailable（内核估算的
        // 「应用还能拿到多少」）作为空闲口径；取不到（非 Linux / 读失败）时回退 MXBean。
        Long available = readMemAvailableBytes();
        long freeForUsage = available != null ? Math.min(totalMem > 0 ? totalMem : available, available) : freeMem;
        long usedMem = totalMem > 0 ? Math.max(0L, totalMem - freeForUsage) : 0L;
        int maxConcurrency = com.dojao.core.config.ResourceConfig.workerParallelism();

        // 受管直接内存：进程级共享根（算子 / 连接器 / transport 全量纳入）
        NodeMemory nm = NodeMemory.shared();
        long directUsed = nm.allocatedBytes();
        long directBudget = nm.budgetBytes();
        List<AllocatorUsage> allocators = nm.snapshot();

        // 进程级磁盘 IO（累计字节；非 Linux 无 /proc/self/io → 0）
        long[] diskIo = readProcessDiskIo();

        return new ResourceSummary(cores, cpuPct, totalMem, usedMem, totalDisk, freeDisk, maxConcurrency,
                System.nanoTime(), heapUsed, heapMax, threads, directUsed, directBudget, allocators,
                diskIo[0], diskIo[1], readProcessRssBytes());
    }

    /**
     * 读 Linux {@code /proc/meminfo} 的 {@code MemAvailable}（字节）。这是内核对「新应用还能分配
     * 多少内存」的估算，已扣除不可回收部分、计入可回收的 page cache —— 与 {@code free} 命令的
     * {@code available} 列同口径。非 Linux / 读失败 → {@code null}（调用方回退 MXBean）。
     */
    private static Long readMemAvailableBytes() {
        try {
            for (String line : java.nio.file.Files.readAllLines(
                    java.nio.file.Path.of("/proc/meminfo"), java.nio.charset.StandardCharsets.US_ASCII)) {
                if (!line.startsWith("MemAvailable:")) continue;
                // 形如 "MemAvailable:   12345678 kB"
                String rest = line.substring("MemAvailable:".length()).strip();
                String kb = rest.split("\\s+")[0];
                return Long.parseLong(kb) * 1024L;
            }
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 读 Linux {@code /proc/self/status} 的 {@code VmRSS}（本进程常驻物理内存，字节）。
     * 监控页用它区分「进程自己占多少」与「主机已用（含页缓存）」：查询结束资源未回落时，
     * RSS 不降才是真泄漏，RSS 降而主机已用不降多为页缓存。非 Linux / 读失败 → 0。
     */
    private static long readProcessRssBytes() {
        try {
            for (String line : java.nio.file.Files.readAllLines(
                    java.nio.file.Path.of("/proc/self/status"), java.nio.charset.StandardCharsets.US_ASCII)) {
                if (!line.startsWith("VmRSS:")) continue;
                String rest = line.substring("VmRSS:".length()).strip();
                String kb = rest.split("\\s+")[0];
                return Long.parseLong(kb) * 1024L;
            }
            return 0L;
        } catch (Exception e) {
            return 0L;
        }
    }

    /**
     * 读 Linux {@code /proc/self/io} 的 {@code read_bytes} / {@code write_bytes}（进程级累计块设备 IO）。
     * 非 Linux（无该文件 / 读异常）→ {@code {0, 0}}（监控页显示平 0，不报错）。
     */
    private static long[] readProcessDiskIo() {
        try {
            List<String> lines = java.nio.file.Files.readAllLines(
                    java.nio.file.Path.of("/proc/self/io"), java.nio.charset.StandardCharsets.US_ASCII);
            long read = 0L;
            long write = 0L;
            for (String line : lines) {
                if (line.startsWith("read_bytes:")) read = ioValue(line);
                else if (line.startsWith("write_bytes:")) write = ioValue(line);
            }
            return new long[]{read, write};
        } catch (Exception e) {
            return new long[]{0L, 0L};
        }
    }

    /** 解析 {@code "read_bytes: 12345"} 冒号后的数值。 */
    private static long ioValue(String line) {
        return Long.parseLong(line.substring(line.indexOf(':') + 1).trim());
    }

    /** 人类可读摘要（监控/日志用）。 */
    public String describe() {
        StringBuilder sb = new StringBuilder();
        sb.append("cpu=").append(cpuCores).append("core");
        if (cpuUsagePct >= 0) sb.append(' ').append(cpuUsagePct).append('%');
        sb.append(" mem=").append(human(totalMemoryBytes)).append('/').append(human(usedMemoryBytes)).append("used")
                .append(" disk=").append(human(totalDiskBytes)).append('/').append(human(freeDiskBytes)).append("free")
                .append(" slots=").append(maxConcurrency)
                .append(" heap=").append(human(heapUsedBytes)).append('/').append(human(heapMaxBytes))
                .append(" direct=").append(human(directUsedBytes)).append('/').append(human(directBudgetBytes))
                .append(" threads=").append(threadCount)
                .append(" io=r").append(human(diskReadBytes)).append("/w").append(human(diskWriteBytes))
                .append(" rss=").append(human(processRssBytes));
        return sb.toString();
    }

    private static String human(long bytes) {
        if (bytes >= 1L << 30) return String.format("%.1fGB", bytes / (double) (1L << 30));
        if (bytes >= 1L << 20) return String.format("%.1fMB", bytes / (double) (1L << 20));
        return bytes + "B";
    }
}
