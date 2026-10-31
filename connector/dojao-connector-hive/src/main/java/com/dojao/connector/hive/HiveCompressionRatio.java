package com.dojao.connector.hive;

import java.util.Locale;
import java.util.Map;

/**
 * Hive 表的「物理压缩比」估算 —— 把 {@link HiveTableMeta} 报出的<b>压缩后</b>字节换算回
 * 「join build 侧解压后驻留内存」的量级。
 *
 * <p>为什么需要它：{@code HiveConnector.getTableStats().byteSize()} 是 HDFS 文件长度 / HMS
 * {@code totalSize}，即<b>压缩后</b>的物理字节；而 ORC / Parquet 通常压缩 3~6 倍。若不换算，
 * 引擎会拿「压缩后小体积」去判 join 体量，把大表误判成中表 → 选到不可溢写的
 * {@code HashJoinOperator} → build 侧超预算直接 fail-fast（见
 * {@code com.dojao.core.resource.JoinDecider}）。</p>
 *
 * <p>本类只用<b>元数据</b>（表参数里的 {@code orc.compress} / {@code parquet.compression}，
 * 或 Hive 侧 ANALYZE 落下的 {@code rawDataSize} / {@code totalSize}），<b>不读任何数据文件</b>，
 * 因此是纯函数、可离线单测。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/01
 */
public final class HiveCompressionRatio {

    /**
     * ORC 压缩算法 → 压缩比中位数（保守侧）。键用元数据里的原始写法（已转大写）。
     *
     * <p>取值口径：取「正常数据」的中位数、并偏向保守（偏大），因为高估最多多走一次可溢写的
     * 排序归并，低估才会撞内存。{@code orc.compress} 缺省 = ZLIB。</p>
     */
    private static final Map<String, Double> ORC = Map.ofEntries(
            Map.entry("NONE", 1.0),
            Map.entry("ZLIB", 6.0),      // ORC 默认
            Map.entry("DEFLATE", 6.0),   // ZLIB 的别名
            Map.entry("SNAPPY", 4.0),
            Map.entry("ZSTD", 5.0),
            Map.entry("LZO", 3.0));

    /**
     * Parquet 压缩算法 → 压缩比中位数（保守侧）。{@code parquet.compression} 缺省 = SNAPPY。
     */
    private static final Map<String, Double> PARQUET = Map.ofEntries(
            Map.entry("UNCOMPRESSED", 1.0),
            Map.entry("NONE", 1.0),
            Map.entry("SNAPPY", 4.0),    // Parquet 默认
            Map.entry("GZIP", 6.0),
            Map.entry("ZLIB", 6.0),
            Map.entry("DEFLATE", 6.0),
            Map.entry("ZSTD", 5.0),
            Map.entry("BROTLI", 6.0),
            Map.entry("LZO", 3.0),
            Map.entry("LZ4", 3.0),
            Map.entry("LZ4_RAW", 3.0));

    /** 未识别 codec 时按格式默认码率取值：ORC 默认 ZLIB、Parquet 默认 SNAPPY（比无脑 1.0 保守）。 */
    private static final double ORC_DEFAULT = 6.0;
    private static final double PARQUET_DEFAULT = 4.0;

    private HiveCompressionRatio() {
    }

    /**
     * 解压膨胀系数 = {@code max(系数法, rawDataSize法, 1.0)}。
     *
     * <ul>
     *   <li><b>系数法</b>：由「存储格式 + 压缩算法」查上表中位数；</li>
     *   <li><b>rawDataSize 法</b>：HMS 里 {@code rawDataSize / totalSize} 的<b>该表实测</b>压缩比
     *       （仅当两值都在册且 &gt; 0 时成立，否则为 0 不参与）。</li>
     * </ul>
     *
     * @param format 表存储格式（{@code null} / {@link HiveStorageFormat#TEXT} → 1.0）
     * @param params HMS 表参数（TBLPROPERTIES），可为 {@code null}
     * @return ≥ 1.0 的乘数
     */
    public static double expandRatio(HiveStorageFormat format, Map<String, String> params) {
        double codec = codecRatio(format, params);
        double raw = rawDataSizeRatio(params);
        return Math.max(Math.max(codec, raw), 1.0);
    }

    private static double codecRatio(HiveStorageFormat format, Map<String, String> params) {
        if (format == null) return 1.0;
        switch (format) {
            case ORC:
                return lookup(ORC, param(params, "orc.compress"), ORC_DEFAULT);
            case PARQUET:
                String codec = param(params, "parquet.compression");
                if (codec == null) codec = param(params, "parquet.compress");
                return lookup(PARQUET, codec, PARQUET_DEFAULT);
            default:
                return 1.0;   // TEXT：通常不压缩，不做膨胀
        }
    }

    private static double rawDataSizeRatio(Map<String, String> params) {
        long raw = parseLong(raw(params, "rawDataSize"));
        long total = parseLong(raw(params, "totalSize"));
        if (raw > 0 && total > 0) {
            return (double) raw / total;
        }
        return 0.0;
    }

    private static double lookup(Map<String, Double> table, String codec, double fallback) {
        if (codec == null) return fallback;
        Double v = table.get(codec);
        return v == null ? fallback : v;
    }

    /** 取压缩算法参数值并归一化（trim + 大写）；缺失 / 空白 → {@code null}。 */
    private static String param(Map<String, String> params, String key) {
        String v = raw(params, key);
        if (v == null) return null;
        v = v.trim().toUpperCase(Locale.ROOT);
        return v.isEmpty() ? null : v;
    }

    private static String raw(Map<String, String> params, String key) {
        return params == null ? null : params.get(key);
    }

    private static long parseLong(String v) {
        if (v == null) return 0L;
        try {
            return Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            return 0L;
        }
    }
}
