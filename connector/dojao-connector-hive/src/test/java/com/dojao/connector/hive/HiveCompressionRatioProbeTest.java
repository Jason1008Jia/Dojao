package com.dojao.connector.hive;

import java.util.HashMap;
import java.util.Map;

/**
 * <b>Hive 表解压膨胀系数</b>的离线探针（纯本地，无 HDFS / 无 HMS，毫秒级）。
 *
 * <p>背景：{@code HiveConnector} 报出的 {@code byteSize} 是<b>压缩后</b>字节，而 join build 侧
 * 驻留内存是<b>解压后</b>的 Arrow 向量。{@link HiveCompressionRatio} 按「存储格式 + 压缩算法」查出
 * 中位压缩比，并与 HMS 里 {@code rawDataSize/totalSize} 的实测比取 {@code max}。「低估才会撞内存」，
 * 所以本探针钉死：① 系数取值表；② {@code rawDataSize} 法没有被系数法吞掉；③ 缺失 / 非法参数
 * 都有安全兜底（不 NPE、不返回 &lt; 1.0）。</p>
 *
 * @author jiaxin
 * @date 2026/10/01
 */
public class HiveCompressionRatioProbeTest {

    private static int pass = 0;
    private static int fail = 0;

    public static void main(String[] args) {
        System.err.println("========== Hive 压缩比系数 探针 ==========");
        testOrcCodecs();
        testParquetCodecs();
        testRawDataSizeMethod();
        testFallbacks();
        System.err.printf("========== 结果: %d PASS / %d FAIL ==========%n", pass, fail);
        if (fail > 0) {
            System.exit(1);
        }
    }

    // ==================== 1. ORC 系数表 ====================

    private static void testOrcCodecs() {
        System.err.println("---------- 1. ORC 压缩算法系数 ----------");
        check("ORC + SNAPPY = 4.0", ratio(HiveStorageFormat.ORC, prop("orc.compress", "SNAPPY")) == 4.0);
        check("ORC + ZLIB = 6.0", ratio(HiveStorageFormat.ORC, prop("orc.compress", "ZLIB")) == 6.0);
        check("ORC + ZSTD = 5.0", ratio(HiveStorageFormat.ORC, prop("orc.compress", "ZSTD")) == 5.0);
        check("ORC + LZO = 3.0", ratio(HiveStorageFormat.ORC, prop("orc.compress", "LZO")) == 3.0);
        check("ORC + NONE = 1.0（未压缩）", ratio(HiveStorageFormat.ORC, prop("orc.compress", "NONE")) == 1.0);
        check("ORC 无 compress 键 = 6.0（默认 ZLIB）", ratio(HiveStorageFormat.ORC, Map.of()) == 6.0);
        check("ORC 小写 'snappy' 归一大写 → 4.0", ratio(HiveStorageFormat.ORC, prop("orc.compress", "snappy")) == 4.0);
        check("ORC 未识别 codec('LZ77') = 6.0（格式默认，不用 1.0）",
                ratio(HiveStorageFormat.ORC, prop("orc.compress", "LZ77")) == 6.0);
    }

    // ==================== 2. Parquet 系数表 ====================

    private static void testParquetCodecs() {
        System.err.println("---------- 2. Parquet 压缩算法系数 ----------");
        Map<String, Double> expect = Map.of(
                "SNAPPY", 4.0, "GZIP", 6.0, "ZSTD", 5.0,
                "BROTLI", 6.0, "LZO", 3.0, "LZ4", 3.0, "UNCOMPRESSED", 1.0);
        for (Map.Entry<String, Double> e : expect.entrySet()) {
            double got = ratio(HiveStorageFormat.PARQUET, prop("parquet.compression", e.getKey()));
            check("PARQUET + " + e.getKey() + " = " + e.getValue(), got == e.getValue());
        }
        check("PARQUET 无 compression 键 = 4.0（默认 SNAPPY）",
                ratio(HiveStorageFormat.PARQUET, Map.of()) == 4.0);
        check("PARQUET 别名键 parquet.compress 也能识别（GZIP → 6.0）",
                ratio(HiveStorageFormat.PARQUET, prop("parquet.compress", "GZIP")) == 6.0);
    }

    // ==================== 3. rawDataSize 法（与系数法取 max） ====================

    private static void testRawDataSizeMethod() {
        System.err.println("---------- 3. rawDataSize 法（max 取大） ----------");
        // ORC+SNAPPY 系数 4.0；实测 rawDataSize/totalSize = 800/100 = 8.0 → 取 8.0
        Map<String, String> p = new HashMap<>();
        p.put("orc.compress", "SNAPPY");
        p.put("rawDataSize", "800");
        p.put("totalSize", "100");
        check("ORC+SNAPPY, raw/total=8 → max(4, 8) = 8.0", ratio(HiveStorageFormat.ORC, p) == 8.0);

        // ORC+ZLIB 系数 6.0；实测比 200/100 = 2.0 → 系数法更大 → 6.0
        Map<String, String> q = new HashMap<>();
        q.put("orc.compress", "ZLIB");
        q.put("rawDataSize", "200");
        q.put("totalSize", "100");
        check("ORC+ZLIB, raw/total=2 → max(6, 2) = 6.0（不被实测比拉低）", ratio(HiveStorageFormat.ORC, q) == 6.0);

        // 仅 rawDataSize 无 totalSize → 实测比不成立，退回系数法
        check("只有 rawDataSize、无 totalSize → 退回系数法（ORC+SNAPPY → 4.0）",
                ratio(HiveStorageFormat.ORC, prop2("orc.compress", "SNAPPY", "rawDataSize", "800")) == 4.0);
    }

    // ==================== 4. 兜底 / 边界 ====================

    private static void testFallbacks() {
        System.err.println("---------- 4. 兜底与边界 ----------");
        check("TEXT = 1.0（文本不做膨胀）", ratio(HiveStorageFormat.TEXT, prop("orc.compress", "SNAPPY")) == 1.0);
        check("format=null = 1.0", ratio(null, Map.of()) == 1.0);
        check("params=null, ORC → 6.0（不 NPE）", ratio(HiveStorageFormat.ORC, null) == 6.0);
        check("rawDataSize 非法('abc') → 退回系数法（ORC+SNAPPY → 4.0）",
                ratio(HiveStorageFormat.ORC, prop2("orc.compress", "SNAPPY", "rawDataSize", "abc")) == 4.0);
        check("rawDataSize=0 → 退回系数法（ORC+SNAPPY → 4.0）",
                ratio(HiveStorageFormat.ORC,
                        prop2("orc.compress", "SNAPPY", "rawDataSize", "0")) == 4.0);
        check("系数恒 ≥ 1.0（ORC+空值不塌到 0）",
                ratio(HiveStorageFormat.ORC, prop("orc.compress", "  ")) >= 1.0);
    }

    // ==================== 辅助 ====================

    /** 直接调被测纯函数。 */
    private static double ratio(HiveStorageFormat format, Map<String, String> params) {
        return HiveCompressionRatio.expandRatio(format, params);
    }

    private static Map<String, String> prop(String k, String v) {
        Map<String, String> m = new HashMap<>();
        m.put(k, v);
        return m;
    }

    private static Map<String, String> prop2(String k1, String v1, String k2, String v2) {
        Map<String, String> m = prop(k1, v1);
        m.put(k2, v2);
        return m;
    }

    private static void check(String name, boolean ok) {
        if (ok) {
            pass++;
            System.err.println("  [PASS] " + name);
        } else {
            fail++;
            System.err.println("  [FAIL] " + name);
        }
    }
}
