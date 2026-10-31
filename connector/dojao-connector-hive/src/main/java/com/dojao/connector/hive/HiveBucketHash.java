package com.dojao.connector.hive;

import org.apache.hadoop.io.Text;

import java.util.List;
import java.util.Locale;

/**
 * Hive <b>桶哈希</b> —— 复刻 Hive 的 {@code ObjectInspectorUtils.getBucketHashCode} /
 * {@code getBucketNumber}，用于<b>桶裁剪</b>：当 WHERE 只命中分桶列的少数取值时，
 * 只读命中桶对应的文件，其余桶文件连打开都不打开。
 *
 * <p><b>算法</b>（与 Hive 一致）：</p>
 * <ol>
 *   <li>逐分桶列累加：{@code hash = 31 * hash + hashCode(field)}（初值 0）；</li>
 *   <li>桶号：{@code (hash & Integer.MAX_VALUE) % numBuckets}。</li>
 * </ol>
 *
 * <p><b>每列 {@code hashCode} 与 Hive {@code ObjectInspectorUtils.hashCode(o, oi)} 对齐</b>
 * （<b>不是</b> murmur —— murmur 是 Hive 另一个独立的 {@code hashCodeMurmur}，分桶不用它）：</p>
 * <ul>
 *   <li>{@code int} → 值本身；{@code bigint} → {@code (int)(l ^ (l >>> 32))}（即 Java {@code Long.hashCode}）；
 *       {@code tinyint}/{@code smallint} → 值；{@code boolean} → 1/0；
 *       {@code float} → {@code Float.floatToIntBits}；{@code double} → {@code doubleToLongBits} 折半；</li>
 *   <li>{@code string} → <b>Hadoop {@code Text.hashCode()}</b>（初值 1、按 UTF-8 <b>有符号</b>字节 {@code 31*h+b} 累加，
 *       与 Java {@code String.hashCode} 不同）—— 直接调 {@link Text#hashCode()} 复刻，不自写。</li>
 * </ul>
 *
 * <p><b>安全铁律：裁剪只许「删」文件，绝不许「留」错文件。</b>类型不支持（{@code date}/{@code timestamp}/
 * {@code decimal}/复杂类型）、值解析不出、列数对不上 —— 一律返回 {@code null} = <b>该列不支持桶裁剪</b>，
 * 调用方必须据此<b>回退全扫</b>（多读几个桶只是慢，读错桶就是静默丢数据）。</p>
 *
 * <p>纯函数、无 I/O，可离线探针验证。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/7
 */
public final class HiveBucketHash {

    private HiveBucketHash() {
    }

    /**
     * 单个分桶列值的哈希；返回 {@code null} = 该类型不支持（调用方不得据其裁剪）。
     *
     * @param hiveType Hive 声明类型（如 {@code string} / {@code int} / {@code bigint}；可带参数如 {@code varchar(64)}）
     * @param value    取值原文（来自 WHERE 字面量或分区值）；{@code null} = NULL（Hive 视为 hashCode 0）
     */
    public static Integer hashCodeOf(String hiveType, String value) {
        if (value == null) return 0;                 // Hive: null 字段的 hashCode 记为 0
        String t = normalizeType(hiveType);
        try {
            return switch (t) {
                case "tinyint" -> (int) Byte.parseByte(value.trim());
                case "smallint" -> (int) Short.parseShort(value.trim());
                case "int", "integer" -> Integer.parseInt(value.trim());
                case "bigint" -> {
                    long l = Long.parseLong(value.trim());
                    yield (int) (l ^ (l >>> 32));
                }
                case "boolean" -> Boolean.parseBoolean(value.trim()) ? 1 : 0;
                case "float" -> Float.floatToIntBits(Float.parseFloat(value.trim()));
                case "double" -> {
                    long l = Double.doubleToLongBits(Double.parseDouble(value.trim()));
                    yield (int) (l ^ (l >>> 32));
                }
                case "string" -> new Text(value).hashCode();
                default -> null;                     // date/timestamp/decimal/复杂类型 → 不支持，回退全扫
            };
        } catch (NumberFormatException e) {
            return null;                             // 值解析不出 → 不支持（保守）
        }
    }

    /**
     * 多分桶列组合的桶哈希（Hive: {@code hash = 31*hash + hashCode(field)}）。
     * 任一列不支持或列数对不上 → {@code null}（调用方回退全扫）。
     */
    public static Integer bucketHash(List<String> types, List<String> values) {
        if (types == null || values == null || types.size() != values.size()) return null;
        int hash = 0;
        for (int i = 0; i < types.size(); i++) {
            Integer h = hashCodeOf(types.get(i), values.get(i));
            if (h == null) return null;
            hash = 31 * hash + h;
        }
        return hash;
    }

    /** 桶号（Hive: {@code (hash & Integer.MAX_VALUE) % numBuckets}）。 */
    public static int bucketNumber(int hash, int numBuckets) {
        return (hash & Integer.MAX_VALUE) % numBuckets;
    }

    /**
     * 该类型是否可参与分桶哈希（写侧护栏用；读侧裁剪可仅靠 {@link #hashCodeOf} 判 {@code null}）。
     * 用 {@code "1"} 作探针值：能算出非 {@code null} 哈希 = 类型受支持。
     */
    public static boolean isHashable(String hiveType) {
        return hashCodeOf(hiveType, "1") != null;
    }

    /** 类型名归一：去空白 / 小写 / 去掉 {@code decimal(10,2)}、{@code varchar(64)} 的参数。 */
    private static String normalizeType(String hiveType) {
        if (hiveType == null) return "";
        String t = hiveType.trim().toLowerCase(Locale.ROOT);
        int paren = t.indexOf('(');
        if (paren > 0) t = t.substring(0, paren).trim();
        return t;
    }
}
