package com.dojao.core.resource;

import com.dojao.core.config.ResourceConfig;
import com.dojao.core.operator.JoinType;
import com.dojao.spi.bean.TableStats;
import com.dojao.spi.service.DojaoConnector;

/**
 * <b>Join 算法选择器</b> —— 按「两侧表的估算体量」与「单台 Worker 的 join 内存预算」在
 * {@code HASH} / {@code BROADCAST} / {@code SORT_MERGE} 之间选一个，并给出 build 侧。
 *
 * <p>纯函数、无状态、无 IO（统计由调用方通过 {@link #estimateBytes} 先探好后传入），
 * 与 {@link ParallelismCalculator} 同款风格 —— 便于离线单测把「什么体量落到什么算法」钉死。</p>
 *
 * <h3>判定依据只有两个数</h3>
 * <pre>
 *   s = 单侧估算体量（字节，已含解压膨胀系数）
 *   U = 单台 Worker 的 join 内存预算（{@link #unit}）
 * </pre>
 *
 * <h3>为什么是这两个数：三种算法的内存约束不同</h3>
 * <table border="1">
 *   <caption>可行性条件</caption>
 *   <tr><th>算法</th><th>build 侧内存约束</th><th>网络代价</th></tr>
 *   <tr><td>HASH</td>
 *       <td>build 侧总量 {@code B} 摊到 n 个分区，每分区预算 {@code M/n} ⇒
 *           <b>{@code B ≤ M}</b>（n 约掉，<u>与并行度无关</u>）</td>
 *       <td>{@code B + P}（两侧都 shuffle）</td></tr>
 *   <tr><td>SORT_MERGE</td>
 *       <td>无（两侧排序阶段三槽溢写，单侧体量不限）</td>
 *       <td>{@code B + P}</td></tr>
 *   <tr><td>BROADCAST</td>
 *       <td>整份副本落在<b>每一台</b> Worker 上 ⇒ <b>{@code B ≤ 单台可用内存}</b>
 *           （不除以 n、不除以 worker 数，比 HASH 严格）</td>
 *       <td>{@code B × w}（小表复制），<b>省掉 probe 侧 shuffle</b></td></tr>
 * </table>
 *
 * <p>关键推论：<b>集群规模只进入广播的判据</b>。HASH 与 SORT_MERGE 的分界只取决于
 * {@code query.slot.memory}（引擎给单个片段的内存预算），而广播要「装进一台」，所以
 * {@code U} 必须取「最弱一台 Worker」而非平均值（木桶效应），且随 worker 数放大复制代价。</p>
 *
 * <h3>三档与决策表</h3>
 * <pre>
 *   s ≤ U × query.join.smallRatio (0.25)   → 小
 *   s ≤ U × query.join.largeRatio (0.8)    → 中
 *   否则                                    → 大
 *
 *   体量未知(UNKNOWN)                       → SORT_MERGE（最保守：可溢写，不赌内存）
 *   右表(小) + 左表(大)                → BROADCAST
 *   右表不是大表                        → HASH
 *   右表是大表                          → SORT_MERGE
 * </pre>
 *
 * <p><b>build 侧固定为右表。</b>{@code HashJoinOperator} 的 build 侧必须装进内存，挑小的一侧总是更稳，
 * 但翻转 build 侧会连带改变输出列顺序（算子输出「左全列 + 右非键列」）与 LEFT/RIGHT 语义，
 * 需要配套的列重排投影，<b>本期未做</b>。实践含义：把维表写在 join 的右边（
 * {@code FROM 事实表 f JOIN 维表 d}）才能吃到广播；小表在左时退回 HASH / SORT_MERGE（结果正确，只是慢）。</p>
 *
 * <h3>已知边界（不是 bug，是设计取舍）</h3>
 * <ul>
 *   <li><b>倾斜看不见</b>：{@code B ≤ M} 是<b>总量</b>判据，热点 key 能把单个分区撑爆。
 *       列统计（{@code ColumnStats.distinctCount}）理论上能估最大分组占比，但 MySQL / PostgreSQL 都
 *       不报，覆盖率太低 —— 本期不做，留给运行时兜底（超预算 fail-fast，不静默降级）。</li>
 *   <li><b>无基数估计 / 无直方图 / 无代价模型</b>：不知道 join 结果多大，也不比较真实的
 *       网络与 IO 代价。这是「可行性启发式」，不是 CBO —— 见 {@code blog16}。</li>
 *   <li><b>阈值是留余量拍的</b>：{@code 0.8} 而不是 {@code 1.0}，因为连接器统计是估算值
 *       （MySQL {@code TABLE_ROWS} 是 InnoDB 采样值），按 1.0 卡等于把估算误差变成运行时失败。</li>
 * </ul>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/23
 */
public final class JoinDecider {

    /** 体量未知的哨兵（连接器探不到）。 */
    public static final long UNKNOWN = -1L;

    /** 参与选型的 join 算法（与 {@code FragmentSpec.JoinAlgorithm} 一一对应，此处独立一份避免资源层依赖算子细节）。 */
    public enum Algorithm {
        /** 分区哈希 join：build 侧装得进每分区预算时最快，无外存 IO。 */
        HASH,
        /** 广播 join：小表整份复制到每台 Worker，大表切片就地不跨机。 */
        BROADCAST,
        /** 排序归并 join：两侧可溢写，单侧体量再大也不会因内存失败。 */
        SORT_MERGE
    }

    /** 单侧体量相对 {@code U} 的档位。 */
    public enum Tier {
        /** {@code ≤ U × smallRatio}：够小，可整份复制到每台 Worker。 */
        SMALL,
        /** {@code ≤ U × largeRatio}：中等，hash build 侧装得下。 */
        MEDIUM,
        /** 超出：只能走可溢写的排序归并。 */
        LARGE
    }

    /** build 侧（哈希表由哪一侧构建）。 */
    public enum BuildSide {
        LEFT, RIGHT
    }

    /**
     * 选型结论。
     *
     * @param algorithm 选中的算法
     * @param buildSide build 侧（本期恒 {@link BuildSide#RIGHT}，见类 javadoc）
     * @param leftTier  左表档位（已按 {@code U} 归一）
     * @param rightTier 右表档位
     * @param leftBytes 左表估算体量（{@link #UNKNOWN} = 未知）
     * @param rightBytes 右表估算体量
     * @param unit      本次判定用的单台 Worker 预算 {@code U}
     * @param reason    人类可读的判定理由（日志 / EXPLAIN 用）
     */
    public record Decision(Algorithm algorithm, BuildSide buildSide, Tier leftTier, Tier rightTier,
                           long leftBytes, long rightBytes, long unit, String reason) {

        /** 判定摘要：{@code BROADCAST[L=SMALL(120MB) R=LARGE(3.2GB) U=2.0GB] 右表够小且左表够大，复制小表…} */
        public String describe() {
            return algorithm + "[L=" + leftTier + "(" + human(leftBytes) + ")"
                    + " R=" + rightTier + "(" + human(rightBytes) + ")"
                    + " U=" + human(unit) + "] " + reason;
        }

        private static String human(long bytes) {
            if (bytes < 0) return "未知";
            if (bytes >= 1024L * 1024 * 1024) {
                return String.format("%.1fGB", bytes / 1024.0 / 1024 / 1024);
            }
            if (bytes >= 1024L * 1024) {
                return String.format("%.1fMB", bytes / 1024.0 / 1024);
            }
            if (bytes >= 1024L) {
                return String.format("%.1fKB", bytes / 1024.0);
            }
            return bytes + "B";
        }
    }

    /**
     * 统计探查接缝 —— 决策器只用数据源的这三点能力，收窄成一个小接口便于离线单测
     * （完整 {@link DojaoConnector} 有几十个抽象方法，为测一个选型去实现它不现实）。
     * 生产路径用 {@link #of(DojaoConnector)} 适配。
     */
    public interface StatsProbe {

        /** 表的数据量与体积（-1 = 未知）。 */
        TableStats stats(String catalog, String schema, String table);

        /**
         * 带过滤条件的行数估算（-1 = 无法估算）。
         * 默认返回 -1 = 不估算，此时体量按整表算（语义正确，只是把「谓词过滤后的小表」当大表看）。
         */
        default long filteredRows(String catalog, String schema, String table, String filter) {
            return -1L;
        }

        /**
         * 带过滤条件的<b>字节</b>估算（-1 = 无法估算），有值时优先于行数比例。
         *
         * <p>选型判据是体量，而「整表字节 × filteredRows/rowCount」这条比例路要求连接器
         * <b>同时</b>报得出过滤前后的行数 —— 行数缺失时（Hive 分区表未 ANALYZE 无 {@code numRows}，
         * 但 {@code totalSize} 常在）比例算不出来，整表体量被原样当成过滤后体量。能直接给出
         * 「过滤后字节」的连接器覆写本方法，即可绕过该前提。</p>
         */
        default long filteredBytes(String catalog, String schema, String table, String filter) {
            return -1L;
        }

        /**
         * 解压膨胀系数（≥ 1.0）：{@code stats()} 报的是物理存储量（文件型数据源 = 压缩后字节），
         * 而 join build 侧驻留内存是解压后的列式向量。默认 1.0 = 不膨胀。
         */
        default double expandRatio() {
            return 1.0;
        }

        /**
         * 解压膨胀系数（<b>表级</b>）：同一连接器下不同表的压缩特性可能不同，默认委托
         * {@link #expandRatio()}（向后兼容）。
         */
        default double expandRatio(String catalog, String schema, String table) {
            return expandRatio();
        }

        /** 把连接器适配成探查接缝（连接器本就继承 {@code DataSourceMetaService}，这三个方法都已在册）。 */
        static StatsProbe of(DojaoConnector connector) {
            return new StatsProbe() {
                @Override
                public TableStats stats(String catalog, String schema, String table) {
                    return connector.getTableStats(catalog, schema, table);
                }

                @Override
                public long filteredRows(String catalog, String schema, String table, String filter) {
                    return connector.estimateRows(catalog, schema, table, filter);
                }

                @Override
                public long filteredBytes(String catalog, String schema, String table, String filter) {
                    return connector.estimateBytes(catalog, schema, table, filter);
                }

                @Override
                public double expandRatio() {
                    // 无表上下文时退回连接器的无参声明；配置值是全局兜底。
                    return Math.max(connector.joinSizeExpandRatio(), ResourceConfig.joinCompressedExpandRatio());
                }

                @Override
                public double expandRatio(String catalog, String schema, String table) {
                    // 连接器按表声明的系数优先（它最了解该表的存储格式与压缩算法）；配置值是全局兜底 —— 对
                    // 「统计是压缩后字节」的数据源统一抬高，故取较大者而非覆盖。
                    return Math.max(connector.joinSizeExpandRatio(catalog, schema, table),
                            ResourceConfig.joinCompressedExpandRatio());
                }
            };
        }
    }

    private JoinDecider() {
    }

    // ==================== 体量解析 ====================

    /**
     * 把一个 join 侧解析成估算体量。有谓词时两条路，逐级降级：
     * <ol>
     *   <li><b>连接器直报过滤后字节</b>（{@link StatsProbe#filteredBytes}）→ 首选。
     *       它不依赖行数，故分区表「有 {@code totalSize} 但没 {@code numRows}」时照样缩得下去；</li>
     *   <li><b>整表字节 × 行数比例</b>：元数据探查拿 {@code byteSize} / {@code rowCount}，再用
     *       {@link StatsProbe#filteredRows} 按 {@code filteredRows / rowCount} 缩小体积。
     *       前提是<b>两个行数都在册</b>，缺一则比例不成立、体量按整表算；</li>
     * </ol>
     * 最后统一乘解压膨胀系数 —— 文件型数据源报的是<b>压缩后</b>字节，而 build 侧驻留内存是<b>解压后</b>的列式向量。
     *
     * <p>体积探不到（{@code ≤ 0}，如视图 / 未分析的空表）时返回 {@link #UNKNOWN}，
     * <b>不用行数 × 经验行长去猜</b> —— 猜错 10 倍比不猜更糟（决策器对未知一律走最保守的排序归并）。</p>
     *
     * <p>本方法吞掉探查异常：选型失败只该让查询慢，不该让查询失败。</p>
     */
    public static long estimateBytes(StatsProbe probe, String catalog, String schema, String table, String filter) {
        if (probe == null) {
            return UNKNOWN;
        }
        try {
            double expand = probe.expandRatio(catalog, schema, table);
            if (expand < 1.0) {
                expand = 1.0;
            }

            // ① 连接器能直接给出「过滤后字节」→ 首选：不受「必须同时报得出过滤前后行数」前提约束
            boolean hasFilter = filter != null && !filter.isBlank();
            if (hasFilter) {
                long filteredBytes = probe.filteredBytes(catalog, schema, table, filter);
                if (filteredBytes > 0) {
                    return (long) Math.ceil(filteredBytes * expand);
                }
            }

            // ② 否则退回：整表字节 × (过滤后行数 / 整表行数) × 膨胀系数
            TableStats stats = probe.stats(catalog, schema, table);
            if (stats == null) {
                return UNKNOWN;
            }
            long byteSize = stats.byteSize();
            if (byteSize <= 0) {
                return UNKNOWN;
            }

            long rows = stats.rowCount();
            long filtered = rows;
            if (hasFilter) {
                long est = probe.filteredRows(catalog, schema, table, filter);
                if (est > 0) {
                    filtered = est;
                }
            }

            double factor = expand;
            if (rows > 0 && filtered > 0 && filtered != rows) {
                factor *= (double) filtered / rows;
            }
            return (long) Math.ceil(byteSize * factor);
        } catch (Exception e) {
            return UNKNOWN;
        }
    }

    // ==================== 容量基准 ====================

    /**
     * 单台 Worker 的 join 内存预算 {@code U}。
     *
     * <p>{@code min(配置的 slot 内存, 最弱一台 Worker 的物理可用内存 × workerMemRatio)}：</p>
     * <ul>
     *   <li>配置值 {@code query.slot.memory} 是<b>主口径</b> —— 它是引擎真正用来卡算子内存的数；</li>
     *   <li>物理可用内存只用来<b>封顶</b>（防止「机器只有 4G 却配了 2G × 8 槽位」这种脱节配置），
     *       且只取一部分：{@code ResourceSummary} 报的是<b>物理内存不是 JVM 堆</b>，占满会误导；</li>
     *   <li>取<b>最弱一台</b>而非平均 —— 广播要求「每一台」都装得下副本，平均值会让弱机器成为定时炸弹。</li>
     * </ul>
     *
     * @param memoryLimit              引擎给单个片段的 join 内存预算（{@code query.slot.memory}）
     * @param weakestWorkerFreeBytes   最弱一台 Worker 的物理可用内存；{@code ≤ 0} = 无心跳数据 → 只用配置值
     */
    public static long unit(long memoryLimit, long weakestWorkerFreeBytes) {
        long fromConfig = Math.max(1L, memoryLimit);
        if (weakestWorkerFreeBytes <= 0) {
            return fromConfig;
        }
        long fromPhysical = Math.max(1L,
                (long) (weakestWorkerFreeBytes * ResourceConfig.joinWorkerMemRatio()));
        return Math.min(fromConfig, fromPhysical);
    }

    /** 单侧体量相对 {@code U} 的档位。{@code bytes < 0}（未知）一律当 {@link Tier#LARGE}（保守，但其实用不到）。 */
    public static Tier tier(long bytes, long unit) {
        if (bytes < 0 || unit <= 0) {
            return Tier.LARGE;
        }
        if (bytes <= unit * ResourceConfig.joinSmallRatio()) {
            return Tier.SMALL;
        }
        if (bytes <= unit * ResourceConfig.joinLargeRatio()) {
            return Tier.MEDIUM;
        }
        return Tier.LARGE;
    }

    // ==================== 决策 ====================

    /**
     * 选型主入口（纯函数）。
     *
     * <p>判定顺序：<b>体量未知 → SORT_MERGE</b>；否则 <b>广播 → 哈希 → 排序归并</b>。每一步的取舍见类 javadoc 的决策表。</p>
     *
     * <p><b>体量探不到时走 SORT_MERGE（最保守）</b>：元数据探查对某些数据源本来就经常失败（视图、
     * 权限受限、无统计的表），此时<b>无法证明 build 侧装得下</b>。HASH 一旦装不下就 fail-fast
     * （不溢写、不降级）→ 查询直接失败；SORT_MERGE 两侧排序阶段都可溢写，任何体量都能跑完。
     * 所以「估不出」不等于「大概率小」，赌 HASH 的代价是<b>查询失败</b>，而 SORT_MERGE 的代价只是<b>慢</b>。
     * 用户可用 {@code query.join.algorithm=hash} 强制覆盖（明确知道装得下时）。</p>
     *
     * @param leftBytes  左表（probe 侧）估算体量；{@link #UNKNOWN} = 未知
     * @param rightBytes 右表（build 侧）估算体量；{@link #UNKNOWN} = 未知
     * @param unit       单台 Worker 的 join 预算 {@code U}（{@link #unit}）
     * @param workers    健康 Worker 数（仅用于日志与理由描述；广播的真正判据是 {@code U}）
     * @param joinType   join 类型 —— <b>改变结论</b>：只有 INNER / LEFT 允许广播（其余复用 build 侧
     *                   复制会重复输出「无匹配的 build 行」，见 {@link #broadcastSafe}）
     */
    public static Decision decide(long leftBytes, long rightBytes, long unit, int workers, JoinType joinType) {
        if (rightBytes == UNKNOWN || leftBytes == UNKNOWN) {
            return new Decision(Algorithm.SORT_MERGE, BuildSide.RIGHT, Tier.LARGE, Tier.LARGE, leftBytes, rightBytes,
                    unit, "体量未知（连接器探不到统计）→ 走最保守的可溢写排序归并（宁可慢，不赌内存）");
        }

        Tier leftTier = tier(leftBytes, unit);
        Tier rightTier = tier(rightBytes, unit);
        String who = joinType + ", 左=" + leftBytes + "B/右=" + rightBytes + "B, U=" + unit + "B, workers=" + workers;

        // ① 广播：右表（build 侧）够小、左表（probe 侧）够大 —— 复制小表比打散大表划算。
        //    ★仅 INNER / LEFT：广播把 build 侧整份复制到每台，而「没有匹配上的 build 行」会被每台各吐
        //    一遍 → RIGHT / FULL 会把这些行重复输出 w 份（LEFT 侧每行只落一台，不会重复）。
        if (rightTier == Tier.SMALL && leftTier == Tier.LARGE
                && broadcastSafe(joinType) && ResourceConfig.joinBroadcastEnabled()) {
            return new Decision(Algorithm.BROADCAST, BuildSide.RIGHT, leftTier, rightTier, leftBytes, rightBytes,
                    unit, "右表够小(≤U×smallRatio)且左表够大(>U×largeRatio)：整份复制小表到每台 Worker，"
                    + "省掉大表的 shuffle（" + who + "）");
        }

        // ② 哈希：build 侧（右表）装得进每分区内存预算。
        if (rightTier != Tier.LARGE) {
            return new Decision(Algorithm.HASH, BuildSide.RIGHT, leftTier, rightTier, leftBytes, rightBytes,
                    unit, "右表(build 侧)为中/小表(≤U×largeRatio)，哈希表装得下（" + who + "）");
        }

        // ③ 排序归并：build 侧超出内存预算 → 走可溢写路径。
        return new Decision(Algorithm.SORT_MERGE, BuildSide.RIGHT, leftTier, rightTier, leftBytes, rightBytes,
                unit, "右表(build 侧)超出 U×largeRatio，哈希表装不下 → 排序归并（两侧排序可溢写）（" + who + "）");
    }

    /**
     * 该 join 类型能否安全走广播（build 侧整份复制到每台 Worker）。
     *
     * <p><b>判定依据 = 有没有「不属于任何 probe 行」的 build 行需要输出</b>：广播复制的是 build 侧
     * （右表），每台 Worker 都持有<b>完整</b>右表；probe 侧（左表）每行只落一台。于是：</p>
     * <ul>
     *   <li><b>INNER</b>：只输出两两匹配的行，无重复（左行只在一台）。✓</li>
     *   <li><b>LEFT</b>：还输出「左行无匹配」的空值扩展 —— 左行只在一台，也不会重复。✓</li>
     *   <li><b>RIGHT / FULL</b>：要输出「右行无匹配」的空值扩展，但右表在<b>每台</b>都有 →
     *       同一行被 w 台各输出一遍（<b>静默多行</b>）。✗ 退回哈希 / 排序归并。</li>
     * </ul>
     *
     * <p>CROSS 无等值键，本来就不进这条判定。</p>
     */
    public static boolean broadcastSafe(JoinType joinType) {
        return joinType == JoinType.INNER || joinType == JoinType.LEFT;
    }

    /**
     * 配置强制覆盖（{@code query.join.algorithm} = {@code hash} / {@code sort_merge} / {@code broadcast}）。
     *
     * <p>{@code auto}（默认）返回 {@code null}，由调用方走 {@link #decide}。
     * 覆盖值仍会走一遍体量判定以保留档位信息（日志里能看到「本来该选什么」），
     * 但算法以配置为准 —— 这是给用户做 A/B 对照验证的逃生门。</p>
     */
    public static Decision forced(String configured, long leftBytes, long rightBytes, long unit,
                                  int workers, JoinType joinType) {
        if (configured == null || configured.isBlank() || "auto".equalsIgnoreCase(configured)) {
            return null;
        }
        Decision auto = decide(leftBytes, rightBytes, unit, workers, joinType);
        Algorithm forced = switch (configured.trim().toLowerCase()) {
            case "hash" -> Algorithm.HASH;
            case "sort_merge" -> Algorithm.SORT_MERGE;
            case "broadcast" -> Algorithm.BROADCAST;
            default -> null;
        };
        // 强制广播也要过安全门：RIGHT / FULL 下复制 build 侧会重复输出「无匹配的 build 行」——
        // 宁可忽略这条配置（退回自动判定）也不能静默算错。
        if (forced == Algorithm.BROADCAST && !broadcastSafe(joinType)) {
            return auto;
        }
        if (forced == null || forced == auto.algorithm()) {
            return auto;
        }
        return new Decision(forced, auto.buildSide(), auto.leftTier(), auto.rightTier(),
                leftBytes, rightBytes, unit,
                "配置强制 " + forced + "（自动判定本应为 " + auto.algorithm() + "：" + auto.reason() + "）");
    }
}
