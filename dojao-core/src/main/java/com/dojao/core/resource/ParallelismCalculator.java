package com.dojao.core.resource;

import com.dojao.core.config.ResourceConfig;
import com.dojao.spi.bean.TableStats;

import java.util.ArrayList;
import java.util.List;

/**
 * 初始并行度计算器 —— <b>分区规划下推到连接器做第一次规划</b>，这里只做最后的汇总决策。
 *
 * <p>每个扫描表先由<b>它自己的连接器</b>算出天然分区数（{@link TableStats#splitCount()}：
 * Hive 按文件切、一个文件一片；MySQL/PG 没有天然分片数、报 -1），然后：</p>
 *
 * <pre>
 *   单表：perScan = Min(连接器报的天然分区数, 集群容量)
 *                  天然分区数未知(-1)时退回 Min(ceil(byteSize / bytesPerSlot), 集群容量)，
 *                  体积也未知再退回 ceil(rowCount / rowsPerSlot)
 *   query  = Min(MaxPara, Max(ScanA, ScanB, ...), 集群容量)
 * </pre>
 *
 * <p>即「取最宽的那一路扫描」定并行度、再按每 query 硬上限封顶：ScanA 想出 10 片、ScanB 想出 20 片，
 * 而 {@code MaxPara=15} → 最终 15（各连接器再在上限内按自己的策略切）。</p>
 *
 * <p><b>为什么取 Max 而不是 Min/Sum。</b>并行度是「查询级」的一个数字（扫描线程数、hash 分区数、
 * 分布式聚合并行度共用），取 Max 保证每一路扫描都能拿到它想要的并行度；取 Min 会让大表被小表拖住，
 * 取 Sum 则会让 join 两侧各开一份线程、总线程数翻倍。</p>
 *
 * <ul>
 *   <li>全部元数据无法探查：回退 {@code query.default.parallelism}；</li>
 *   <li>{@code maxParallelism} = 每 query 硬上限（{@code ParallelismBudget.perQueryMax()}：
 *       配置了 {@code system.query.max.parallelism} 用用户值，否则 = 集群容量 / worker 数）。</li>
 * </ul>
 *
 * <p>注意：本类只做<b>计算</b>（纯函数），不参与准入控制 —— 计算器按集群<b>总容量</b>与
 * <b>每 query 硬上限</b>封顶，准入控制（{@link ParallelismBudget}）另按当前<b>可用</b>并行度
 * 做 0.8 阈值降级 / 阻塞，两者解耦，保证 0.8 降级逻辑有意义
 * （若计算器按当前可用封顶，降级分支将永不触发）。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/29
 */
public final class ParallelismCalculator {

    private ParallelismCalculator() {
    }

    /** 表统计解析器：由调用方按连接器探查（MySQL=information_schema 等），失败返回 null/未知。 */
    @FunctionalInterface
    public interface StatsResolver {
        /** 返回 {@code null} 或字段 {@code <= 0} 均视为未知。 */
        TableStats resolve(TableRef table);
    }

    /**
     * 计算 query 初始并行度。
     *
     * @param tables          待扫描表（空 → 默认）
     * @param resolver        表统计探查器（框架只调接口，不关心连接器实现）
     * @param clusterCapacity 集群总容量（健康 Worker 的 maxConcurrency 之和；无集群时用默认）
     * @param maxParallelism  每 query 并行度硬上限（ParallelismBudget.perQueryMax()：用户配置或 集群容量 / worker 数）
     * @return query 初始并行度（≥ 1，≤ maxParallelism）
     */
    public static int decide(List<TableRef> tables, StatsResolver resolver, int clusterCapacity, int maxParallelism) {
        int hardCap = Math.max(1, maxParallelism);
        int fallback = Math.min(ResourceConfig.queryDefaultParallelism(), hardCap);
        if (tables == null || tables.isEmpty()) {
            return fallback;
        }
        List<Integer> perTable = new ArrayList<>();
        for (TableRef table : tables) {
            TableStats stats = null;
            try {
                stats = resolver != null ? resolver.resolve(table) : null;
            } catch (Exception ignored) {
                stats = null;   // 探查失败 → 该表未知，跳过；其余已知表取最大
            }
            int p = perTableParallelism(stats, clusterCapacity);
            if (p > 0) {
                perTable.add(p);
            }
        }
        if (perTable.isEmpty()) {
            return fallback;   // 全部未知 → 默认
        }
        int max = 1;
        for (int p : perTable) {
            max = Math.max(max, p);
        }
        max = Math.min(max, hardCap);
        return Math.max(1, max);
    }

    /**
     * 单表并行度（连接器自己的第一次规划优先）：
     * <ol>
     *   <li>连接器报了天然分区数（{@link TableStats#splitCount()} &gt; 0，如 Hive 的文件数）
     *       → <b>直接采信</b>，这是在按「数据实际长什么样」定并行度；</li>
     *   <li>没报（JDBC 系，-1）→ {@code ceil(byteSize / bytesPerSlot)}（体积未知再退行数）——
     *       按「数据有多大」估，只是个粗糙替代品；</li>
     *   <li>全未知 → 0（由调用方跳过该表）。</li>
     * </ol>
     * 结果再按集群容量封顶。
     */
    private static int perTableParallelism(TableStats stats, int clusterCapacity) {
        if (stats == null) {
            return 0;
        }
        long computed;
        if (stats.splitCount() > 0) {
            computed = stats.splitCount();
        } else if (stats.byteSize() > 0) {
            computed = ceilDiv(stats.byteSize(), ResourceConfig.parallelismBytesPerSlot());
        } else if (stats.rowCount() > 0) {
            computed = ceilDiv(stats.rowCount(), ResourceConfig.parallelismRowsPerSlot());
        } else {
            return 0;
        }
        return (int) Math.max(1, Math.min(computed, Math.max(1, clusterCapacity)));
    }

    private static long ceilDiv(long a, long b) {
        return b <= 0 ? 1 : (a + b - 1) / b;
    }
}
