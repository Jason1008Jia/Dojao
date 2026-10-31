package com.dojao.core.operator;

/**
 * EXPLAIN 里 join 算法与溢写口径的统一措辞。
 *
 * <p>对外只暴露三个算法名 —— {@code broadcast} / {@code shuffle} / {@code sort_merge}：
 * 内部 {@link FragmentSpec.JoinAlgorithm#HASH} 统一叫 {@code shuffle}（两侧按 join 键重分区后
 * 各 Worker 本地哈希 join），{@code BROADCAST} / {@code SORT_MERGE} 同名透出。溢写只分两类：
 * <ul>
 *   <li>{@code sort_merge}：两侧排序阶段各自可溢写，任意体量都能跑完；</li>
 *   <li>{@code broadcast} / {@code shuffle}：build 侧必须整份装进<b>每分区内存预算</b>，
 *       超了 fail-fast 报错，<b>不静默降级</b> —— 所以 {@code spill=no} 必须写出来，
 *       否则用户会误以为「大表 join 失败」是数据问题。</li>
 * </ul>
 *
 * <p>所有 join 节点（单机算子 / 分布式逐层）共用本类，保证 EXPLAIN 与日志口径一致。</p>
 */
public final class JoinExplain {

    private JoinExplain() {
    }

    /** 对外算法名：{@code broadcast} / {@code shuffle} / {@code sort_merge}（null 视为 shuffle）。 */
    public static String label(FragmentSpec.JoinAlgorithm algorithm) {
        if (algorithm == null) {
            return "shuffle";
        }
        return switch (algorithm) {
            case BROADCAST -> "broadcast";
            case SORT_MERGE -> "sort_merge";
            case HASH -> "shuffle";
        };
    }

    /**
     * 分布式 join 一层的算法 + 溢写说明。
     *
     * @param algorithm   该层选型（null = HASH）
     * @param memoryLimit Worker 侧 join 算子的内存预算（BROADCAST 用 Master 侧的单 worker 预算 U：
     *                    每台恰 own 一个桶，右表可用量就是 U）
     * @param partitions  Worker 侧分区数
     */
    public static String distributed(FragmentSpec.JoinAlgorithm algorithm, long memoryLimit, int partitions) {
        FragmentSpec.JoinAlgorithm algo = algorithm == null ? FragmentSpec.JoinAlgorithm.HASH : algorithm;
        return switch (algo) {
            case BROADCAST -> "broadcast（右表整份复制到每台 worker，左表切片就地不 shuffle）"
                    + " spill=no（build 侧超每分区预算 " + format(memoryLimit) + " 即失败）";
            case SORT_MERGE -> "sort_merge（两侧按 join 键重分区后各自排序归并）"
                    + " spill=yes（两侧排序各自按每分区预算 " + budget(memoryLimit, partitions)
                    + " × query.spill.ratio 溢写）";
            case HASH -> "shuffle（两侧按 join 键重分区，各 worker 本地哈希 join）"
                    + " spill=no（build 侧超每分区预算 " + budget(memoryLimit, partitions) + " 即失败）";
        };
    }

    /** 单机 / Worker 本地哈希 join 的算法 + 溢写说明。 */
    public static String localHash(long memoryLimit, int partitions) {
        return "shuffle（hash join, probe=left, build=right）"
                + " spill=no（build 侧超每分区预算 " + budget(memoryLimit, partitions) + " 即失败）";
    }

    /** 单机 / Worker 本地排序归并 join 的算法 + 溢写说明。 */
    public static String localSortMerge(long memoryLimit, int partitions) {
        return "sort_merge（probe=left, build=right）"
                + " spill=yes（两侧排序各自按每分区预算 " + budget(memoryLimit, partitions)
                + " × query.spill.ratio 溢写）";
    }

    /** 每分区内存预算 = memoryLimit / 分区数（与 {@code HashJoinOperator} 的 build 闸门同口径）。 */
    public static long perPartition(long memoryLimit, int partitions) {
        return memoryLimit / Math.max(1, partitions);
    }

    private static String budget(long memoryLimit, int partitions) {
        return format(perPartition(memoryLimit, partitions));
    }

    /** 人类可读字节数（单位到 GB，避免大表打出「12345.67 MB」）。 */
    public static String format(long bytes) {
        if (bytes < 0) {
            return "未知";
        }
        if (bytes < 1024) {
            return bytes + " B";
        }
        if (bytes < 1024 * 1024) {
            return String.format("%.1f KB", bytes / 1024.0);
        }
        if (bytes < 1024L * 1024 * 1024) {
            return String.format("%.2f MB", bytes / (1024.0 * 1024.0));
        }
        return String.format("%.2f GB", bytes / (1024.0 * 1024.0 * 1024.0));
    }
}
