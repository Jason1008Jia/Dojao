package com.dojao.core.resource;

import com.dojao.spi.bean.ColumnStats;
import com.dojao.spi.bean.TableStats;

import java.util.List;

/**
 * <b>中间结果基数估算器</b> —— 多 join 链里「上一层 join 的输出有多大」的纯函数估算，
 * 供 {@link JoinDecider} 在多表链的<b>末层</b>判定「左表够不够大、值不值得广播」。
 *
 * <p>与 {@link JoinDecider} 同款风格：无状态、无 IO（统计由调用方探好后传入），便于离线单测钉死公式。</p>
 *
 * <h3>公式与「外键假设」</h3>
 * <pre>
 *   |L ⋈ R| = |L| × |R| / max(1, max(ndv_L, ndv_R))
 * </pre>
 *
 * <p>连接键的<b>不同值个数</b>（ndv）决定膨胀倍数：一对多（外键）时 ndv 大的一侧是「一方」，
 * 输出 ≈ 大的一侧；多对多时 ndv 小，输出按 {@code |L|×|R|/ndv} 膨胀。</p>
 *
 * <p><b>统计取不到时按外键假设</b>（{@code ndv = min(|L|, |R|)}）—— 即假设输出 ≈ 大的一侧
 * （{@code |L|×|R|/min = max}）。选它是两个理由：</p>
 * <ul>
 *   <li><b>它是真值的下界</b>：连接键的 ndv 恒 {@code ≤ min(|L|, |R|)}（每侧至多这么多个不同值），
 *       故真值 {@code |L|×|R|/ndv ≥ max(|L|, |R|)} —— 外键假设给出的估算<b>永远不高估</b>
 *       中间结果的行数，绝不因高估而触发「右表实际巨大却当成小表复制到每台」的 OOM。</li>
 *   <li><b>它对齐「事实表 ⋈ 维表」这个主场景</b>：维表做 build 侧（ndv = 维表行数 = 小的一侧）时
 *       输出 = 事实表行数；取 {@code max} 会退化成 {@code min}（输出 ≈ 维表），把 5GB 的中间结果
 *       估成 50MB → 末层广播永远不触发（P1-a 落空）。</li>
 * </ul>
 * <p>低估的代价只是<b>少一次优化</b>（退回 HASH / SORT_MERGE，结果依旧正确）；
 * 而末层是否广播还另有安全门（{@code decide} 的 INNER/LEFT + 右表体量），左表估大不会算错。</p>
 *
 * <h3>为什么需要它</h3>
 * <p>多表链的 {@code Planner.chooseChainAlgorithm} 只看右表（build 侧）体量 —— 这是<b>可行性</b>
 * 判据（哈希的内存约束只在 build 侧），不是<b>代价</b>判据。有了中间结果基数，末层就能用
 * {@code (左估算, 右估算)} 二元组跑完整的 {@link JoinDecider#decide}，把「膨胀后的中间结果」
 * 判成大左表，从而<b>就地广播小维表、省掉中间结果的那一次 shuffle</b>（P1-a，见 blog16）。</p>
 *
 * <h3>已知边界（不是 bug，是设计取舍）</h3>
 * <ul>
 *   <li><b>ndv 覆盖率低</b>：{@code ColumnStats.distinctCount} 由连接器报，MySQL 取
 *       {@code information_schema.STATISTICS.CARDINALITY}、PostgreSQL 取 {@code pg_stats.n_distinct}，
 *       视图 / 无统计的表拿不到 → 退回外键假设（低估）。</li>
 *   <li><b>平均行宽用左首表的口径</b>：join 输出比单表宽，用 T0 的行宽会<b>低估</b>中间结果字节 ——
 *       与上一条同向（保守），故不做逐列宽度推演（那需要连接器的列类型分布，覆盖率更低）。</li>
 * </ul>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/24
 */
public final class JoinCardinality {

    /** 未知哨兵（与 {@link JoinDecider#UNKNOWN} 同值，便于互相代换）。 */
    public static final long UNKNOWN = -1L;

    private JoinCardinality() {
    }

    // ==================== 单侧估算 ====================

    /**
     * 单侧行数估算（与 {@link JoinDecider#estimateBytes} 同口径）：元数据行数按「下推谓词过滤后行数」
     * 比例缩小。<b>不用字节 ÷ 经验行宽去猜</b>——猜错比不猜更糟，决策器对未知一律走最保守的排序归并。
     *
     * <p>吞掉探查异常：估算失败只该让选型保守，不该让查询失败。</p>
     */
    public static long sideRows(JoinDecider.StatsProbe probe, String catalog, String schema,
                                String table, String filter) {
        if (probe == null) {
            return UNKNOWN;
        }
        try {
            TableStats stats = probe.stats(catalog, schema, table);
            if (stats == null || stats.rowCount() <= 0) {
                return UNKNOWN;
            }
            long rows = stats.rowCount();
            if (filter != null && !filter.isBlank()) {
                long est = probe.filteredRows(catalog, schema, table, filter);
                if (est > 0 && est < rows) {
                    rows = est;
                }
            }
            return rows;
        } catch (Exception e) {
            return UNKNOWN;
        }
    }

    /**
     * 单侧平均行宽（字节） = {@code byteSize / rowCount}；任一 {@code ≤ 0} → {@link #UNKNOWN}。
     * 用于把「估算行数」折回「估算字节」（决策器只吃字节）。
     */
    public static long avgRowBytes(TableStats stats) {
        if (stats == null || stats.rowCount() <= 0 || stats.byteSize() <= 0) {
            return UNKNOWN;
        }
        return Math.max(1L, stats.byteSize() / stats.rowCount());
    }

    // ==================== join 输出基数 ====================

    /**
     * join 输出行数估算：{@code |L| × |R| / max(1, max(ndv_L, ndv_R))}。
     *
     * <p>任一侧行数未知（{@code ≤ 0}）→ {@link #UNKNOWN}（调用方据此保守回退）。
     * 两侧 ndv 都未知（{@code ≤ 0}）→ 按<b>外键假设</b>取 {@code ndv = min(|L|, |R|)}（输出 ≈ 大的一侧）。</p>
     *
     * <p><b>只知一侧 ndv</b>（另一侧传 {@link #UNKNOWN}）时按教科书式 {@code max} 取已知的那个 ——
     * 这正是「维表 build 侧报 pk 的 distinctCount、事实表 join 键无统计」的常见形态：
     * {@code |L|×|R|/ndv_R} = {@code |L|}（输出 ≈ 事实表行数）。</p>
     *
     * <p>用双精度算中间积再落回 long：基数<b>本来就是估算</b>，而 {@code 3e9 × 3e9} 这种量级用整型
     * 会静默溢出成负数（比不精确更糟）。</p>
     */
    public static long estimateJoinRows(long leftRows, long rightRows, long ndvLeft, long ndvRight) {
        if (leftRows <= 0 || rightRows <= 0) {
            return UNKNOWN;
        }
        long ndv = Math.max(ndvLeft, ndvRight);
        if (ndv <= 0) {
            // 外键假设：ndv = 小的一侧 ⇒ 输出 = 大的一侧。真值 = |L|×|R|/ndv_true，
            // 而 ndv_true ≤ min(|L|,|R|) ⇒ 真值 ≥ max(|L|,|R|) ⇒ 估算恒为下界（不高估）。
            ndv = Math.min(leftRows, rightRows);
        }
        double approx = (double) leftRows * (double) rightRows / Math.max(1L, ndv);
        if (approx >= Long.MAX_VALUE) {
            return Long.MAX_VALUE;
        }
        return Math.max(0L, (long) approx);
    }

    /**
     * 从表统计里取 join 键的 ndv（不同值个数）——多列键取各列 {@code distinctCount} 的
     * <b>最大值</b>（最保守：ndv 越大 → 膨胀越小 → 越不倾向广播），且只采信 {@code > 0} 的列统计。
     * 全部取不到 → {@link #UNKNOWN}（调用方退回外键假设）。
     */
    public static long ndvForKeys(TableStats stats, List<String> keyColumns) {
        if (stats == null || stats.columnStats() == null || keyColumns == null || keyColumns.isEmpty()) {
            return UNKNOWN;
        }
        long max = UNKNOWN;
        for (String key : keyColumns) {
            for (ColumnStats cs : stats.columnStats()) {
                if (cs != null && key.equalsIgnoreCase(cs.columnName()) && cs.distinctCount() > 0) {
                    max = Math.max(max, cs.distinctCount());
                }
            }
        }
        return max;
    }
}
