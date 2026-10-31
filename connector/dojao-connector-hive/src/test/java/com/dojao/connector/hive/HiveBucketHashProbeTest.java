package com.dojao.connector.hive;

import com.dojao.spi.bean.Column;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * <b>Hive 桶裁剪</b>的离线探针（纯本地，无 HDFS / 无 HMS，毫秒级）。
 *
 * <p>钉死三件事：</p>
 * <ol>
 *   <li><b>桶哈希算术</b>（{@link HiveBucketHash}）与 Hive 一致：逐列 {@code 31*hash+hashCode(field)}、
 *       {@code (hash & MAX_VALUE) % n}；每列 hashCode 按类型（int 恒等、bigint 折半、
 *       <b>string 走 Hadoop {@code Text.hashCode}</b>）。string 的期望值<b>硬编码</b>（由真实
 *       {@code Text.hashCode} 测得，非本实现回环）；</li>
 *   <li><b>桶文件名解析</b>（{@link HiveConnector#bucketIdOf}）：现代 {@code 000005_0} / 旧
 *       {@code bucket_5} / 父目录 {@code bucket_5/}；非桶名 → {@code null}；</li>
 *   <li><b>桶裁剪落点</b>（{@link HiveConnector#applyBucketPruning}）：等值命中只留命中桶、
 *       {@code IN} 留多桶、缺约束 / 范围 / 类型不支持 / 非桶文件名 / 开关关闭 <b>一律回退全扫</b>
 *       —— 「拿不准不裁」的安全铁律。</li>
 * </ol>
 *
 * @author jiaxin
 * @date 2026/10/7
 */
public class HiveBucketHashProbeTest {

    private static int pass = 0;
    private static int fail = 0;

    public static void main(String[] args) {
        System.err.println("========== Hive 桶裁剪 探针 ==========");
        testPerTypeHash();
        testBucketHash();
        testBucketNumber();
        testBucketIdOf();
        testApplyPruning();
        System.err.printf("========== 结果: %d PASS / %d FAIL ==========%n", pass, fail);
        if (fail > 0) {
            System.exit(1);
        }
    }

    // ==================== 1. 每类型 hashCode ====================

    private static void testPerTypeHash() {
        System.err.println("---------- 1. 每类型 hashCode ----------");
        // int 恒等（Hive: hash_int(i) == i）
        hashIs("int 5", "int", "5", 5);
        hashIs("int -7", "int", "-7", -7);
        hashIs("integer 42", "integer", "42", 42);
        hashIs("tinyint 3", "tinyint", "3", 3);
        hashIs("smallint 300", "smallint", "300", 300);
        // bigint 折半（小值 l < 2^32 → (int)(l ^ 0) = l）
        hashIs("bigint 5", "bigint", "5", 5);
        hashIs("bigint 大值 4294967297 = 2^32+1", "bigint", "4294967297", (int) (4294967297L ^ (4294967297L >>> 32)));
        // boolean
        hashIs("boolean true", "boolean", "true", 1);
        hashIs("boolean false", "boolean", "false", 0);
        // float / double
        hashIs("float 1.0", "float", "1.0", Float.floatToIntBits(1.0f));
        hashIs("double 1.0", "double", "1.0", (int) (Double.doubleToLongBits(1.0) ^ (Double.doubleToLongBits(1.0) >>> 32)));
        // string → Hadoop Text.hashCode（硬编码期望值：真实 Text.hashCode 测得）
        hashIs("string 'a'（Text.hashCode=128，≠String.hashCode=97）", "string", "a", 128);
        hashIs("string 'alice'", "string", "alice", 121532191);
        hashIs("string 'bob'", "string", "bob", 127508);
        hashIs("string 'user_123'", "string", "user_123", -2073662785);
        hashIs("string ''（Text.hashCode=1，≠String.hashCode=0）", "string", "", 1);
        hashIs("string '123'", "string", "123", 78481);
        // 不支持的类型 → null（回退全扫）
        unsupported("date 不支持", "date", "2020-01-01");
        unsupported("timestamp 不支持", "timestamp", "2020-01-01 00:00:00");
        unsupported("decimal(10,2) 不支持", "decimal(10,2)", "1.23");
        unsupported("array<int> 不支持", "array<int>", "[1,2]");
        // 值解析不出 → null
        unsupported("int 值非法 'abc'", "int", "abc");
        unsupported("bigint 值非法 'x'", "bigint", "x");
        // null 值 → 0
        hashIs("null 值 → 0", "int", null, 0);
    }

    // ==================== 2. 多列组合 hash（31* 累加） ====================

    private static void testBucketHash() {
        System.err.println("---------- 2. 多列组合 bucketHash ----------");
        check("单列 int [3] → 3", HiveBucketHash.bucketHash(List.of("int"), List.of("3")) == 3);
        check("两列 int,string [3,'bob'] → 31*3+127508 = 127601",
                HiveBucketHash.bucketHash(List.of("int", "string"), List.of("3", "bob")) == 127601);
        check("两列 int,string [0,'a'] → 31*0+128 = 128",
                HiveBucketHash.bucketHash(List.of("int", "string"), List.of("0", "a")) == 128);
        check("任一列不支持 → null",
                HiveBucketHash.bucketHash(List.of("int", "date"), List.of("3", "2020-01-01")) == null);
        check("列数对不上 → null",
                HiveBucketHash.bucketHash(List.of("int", "string"), List.of("3")) == null);
    }

    // ==================== 3. 桶号 ====================

    private static void testBucketNumber() {
        System.err.println("---------- 3. 桶号 (hash & MAX_VALUE) % n ----------");
        check("(5, 8) → 5", HiveBucketHash.bucketNumber(5, 8) == 5);
        check("(10, 8) → 2", HiveBucketHash.bucketNumber(10, 8) == 2);
        check("哈希 -1 掩码后 (2147483647 % 8) → 7", HiveBucketHash.bucketNumber(-1, 8) == 7);
        check("哈希 MIN_VALUE 掩码后 0 % 4 → 0", HiveBucketHash.bucketNumber(Integer.MIN_VALUE, 4) == 0);
        check("('alice' hash=121532191, 8) → 7", HiveBucketHash.bucketNumber(121532191, 8) == 7);
    }

    // ==================== 4. 桶文件名解析 ====================

    private static void testBucketIdOf() {
        System.err.println("---------- 4. 桶文件名解析 ----------");
        idIs("现代 /t/000000_0", "/t/000000_0", 0);
        idIs("现代 /t/000007_3", "/t/000007_3", 7);
        idIs("现代 带 scheme", "hdfs://nn:9000/db/t/000005_0", 5);
        idIs("旧式文件名 bucket_00012", "/t/bucket_00012", 12);
        idIs("旧式父目录 /t/bucket_5/part-0", "/t/bucket_5/part-0", 5);
        idIs("非桶名 part-00000 → null", "/t/part-00000", null);
        idIs("非桶名 data.orc → null", "/t/data.orc", null);
        idIs("分区目录下的桶文件 /t/dt=2020/000003_0", "/t/dt=2020/000003_0", 3);
    }

    // ==================== 5. 桶裁剪落点 ====================

    private static void testApplyPruning() {
        System.err.println("---------- 5. applyBucketPruning ----------");

        HiveTableMeta idBucketed = bucketed(List.of("id"), 8,
                List.of(new Column("id", "int", null, null, true),
                        new Column("name", "string", null, null, true)));
        List<HiveConnector.FileEntry> eight = bucketFiles("id", 8);

        // 等值 → 只留命中桶
        List<HiveConnector.FileEntry> eq = HiveConnector.applyBucketPruning(true, idBucketed, "id = 5", eight);
        check("'id = 5' → 1 个文件，只 000005_0", eq.size() == 1 && eq.get(0).path().endsWith("000005_0"));

        // IN → 留多桶
        List<HiveConnector.FileEntry> in = HiveConnector.applyBucketPruning(true, idBucketed, "id IN (1, 3)", eight);
        check("'id IN (1,3)' → 2 个文件", in.size() == 2
                && in.stream().anyMatch(f -> f.path().endsWith("000001_0"))
                && in.stream().anyMatch(f -> f.path().endsWith("000003_0")));

        // 等值 + 额外过滤（非分桶列）→ 仍裁
        List<HiveConnector.FileEntry> eqExtra = HiveConnector.applyBucketPruning(true, idBucketed,
                "id = 5 AND name = 'x'", eight);
        check("'id = 5 AND name = 'x'' → 仍裁到 000005_0", eqExtra.size() == 1);

        // 无分桶列约束 → 全扫
        check("'name = ''x'''（无分桶列约束）→ 全扫 8 个",
                HiveConnector.applyBucketPruning(true, idBucketed, "name = 'x'", eight).size() == 8);
        // 范围（非等值）→ 全扫
        check("'id > 2'（范围非等值）→ 全扫 8 个",
                HiveConnector.applyBucketPruning(true, idBucketed, "id > 2", eight).size() == 8);
        // OR → 全扫
        check("'id = 5 OR id = 6' → 全扫 8 个",
                HiveConnector.applyBucketPruning(true, idBucketed, "id = 5 OR id = 6", eight).size() == 8);
        // 无谓词 → 全扫
        check("filter=null → 全扫 8 个",
                HiveConnector.applyBucketPruning(true, idBucketed, null, eight).size() == 8);
        // 开关关闭 → 全扫
        check("enabled=false → 全扫 8 个",
                HiveConnector.applyBucketPruning(false, idBucketed, "id = 5", eight).size() == 8);

        // 字符串分桶列：'name = alice' → Text.hashCode→121532191 → %8 = 7
        HiveTableMeta nameBucketed = bucketed(List.of("name"), 8,
                List.of(new Column("id", "int", null, null, true),
                        new Column("name", "string", null, null, true)));
        List<HiveConnector.FileEntry> s = HiveConnector.applyBucketPruning(true, nameBucketed,
                "name = 'alice'", eight);
        check("string 分桶 'name = ''alice''' → 000007_0（hash%8=7）",
                s.size() == 1 && s.get(0).path().endsWith("000007_0"));

        // 多分桶列：'id = 3 AND name = ''bob''' → 31*3+127508=127601 → %8 = 1
        HiveTableMeta two = bucketed(List.of("id", "name"), 8,
                List.of(new Column("id", "int", null, null, true),
                        new Column("name", "string", null, null, true)));
        List<HiveConnector.FileEntry> twoRes = HiveConnector.applyBucketPruning(true, two,
                "id = 3 AND name = 'bob'", eight);
        check("双分桶列 → 000001_0", twoRes.size() == 1 && twoRes.get(0).path().endsWith("000001_0"));

        // 不支持的桶列类型（date）→ 全扫
        HiveTableMeta dateBucketed = bucketed(List.of("dt"), 8,
                List.of(new Column("dt", "date", null, null, true)));
        check("date 分桶列（不支持哈希）→ 全扫 8 个",
                HiveConnector.applyBucketPruning(true, dateBucketed, "dt = '2020-01-01'", eight).size() == 8);

        // 非桶文件名（解析不出桶号）→ 全扫（保底）
        List<HiveConnector.FileEntry> nonBucket = new ArrayList<>();
        for (int i = 0; i < 8; i++) nonBucket.add(new HiveConnector.FileEntry("/t/part-0000" + i, 10));
        check("非桶文件名 → 全扫 8 个",
                HiveConnector.applyBucketPruning(true, idBucketed, "id = 5", nonBucket).size() == 8);

        // 非分桶表 → 全扫
        HiveTableMeta plain = bucketed(List.of(), 0,
                List.of(new Column("id", "int", null, null, true)));
        check("非分桶表 → 全扫 8 个",
                HiveConnector.applyBucketPruning(true, plain, "id = 5", eight).size() == 8);

        // 空文件清单 → 不炸
        check("空文件清单 → 空",
                HiveConnector.applyBucketPruning(true, idBucketed, "id = 5", List.of()).isEmpty());
    }

    // ==================== 辅助 ====================

    private static HiveTableMeta bucketed(List<String> bucketCols, int numBuckets, List<Column> dataCols) {
        return new HiveTableMeta("cat", "db", "t", "/tmp/t", HiveStorageFormat.ORC,
                "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat", "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
                false, Map.of(), dataCols, List.of(),
                bucketCols, numBuckets, List.of(), '\001', '\\', "\\N");
    }

    /** 造 N 个现代桶文件 {@code /t/000000_0 … }。 */
    private static List<HiveConnector.FileEntry> bucketFiles(String table, int n) {
        List<HiveConnector.FileEntry> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            out.add(new HiveConnector.FileEntry(String.format("/t/%06d_0", i), 10));
        }
        return out;
    }

    private static void hashIs(String name, String type, String value, int expected) {
        Integer got = HiveBucketHash.hashCodeOf(type, value);
        check("hashCodeOf(" + name + ") = " + expected, got != null && got == expected);
    }

    private static void unsupported(String name, String type, String value) {
        check(name + " → null（回退全扫）", HiveBucketHash.hashCodeOf(type, value) == null);
    }

    private static void idIs(String name, String path, Integer expected) {
        Integer got = HiveConnector.bucketIdOf(path);
        check(name + " → " + expected, expected == null ? got == null : expected.equals(got));
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
