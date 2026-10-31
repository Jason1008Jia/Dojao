package com.dojao.connector.hive;

import com.dojao.spi.bean.Column;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.metastore.api.TxnInfo;
import org.apache.hadoop.hive.metastore.api.TxnState;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

/**
 * <b>Hive ACID（事务表）读取</b>的离线探针（本地 ORC 文件，无 HDFS / 无 HMS，秒级）。
 *
 * <p>覆盖两块：</p>
 * <ol>
 *   <li><b>纯布局逻辑</b> {@link HiveAcidLayout}：目录名解析（{@code base_}/{@code delta_}/
 *       {@code delete_delta_}）、写快照可见性（{@code ValidReaderWriteIdList} 的 base/range 判定）、
 *       按 (分区目录, 桶号) 分组与排序、{@code acidRoot} 反推分区目录；</li>
 *   <li><b>行级合并</b> {@link HiveAcidMerger}：本地写 ACID 形状的 ORC（顶层
 *       {@code operation/originalTransaction/bucket/rowId/currentTransaction/row<...>}），
 *       断言 更新覆盖（同 {@code RecordIdentifier} 取更大 {@code currentTransaction}）、
 *       删除合并（base 2 行 + delta 删 1 行 + delete_delta 删 1 行 → 1 行）、
 *       {@code row} 结构按位置解包、整结构 NULL 安全、非 ACID 遗留文件回退。</li>
 * </ol>
 *
 * <p>需要 {@code --add-opens=java.base/java.nio=ALL-UNNAMED}（Arrow 走堆外内存）。</p>
 *
 * @author jiaxin
 * @date 2026/10/7
 */
public class HiveAcidProbeTest {

    /** ACID ORC 顶层结构：用户列嵌在 {@code row}（按位置，名为 {@code _col0/_col1}）。 */
    private static final String ACID_SCHEMA =
            "struct<operation:int,originalTransaction:bigint,bucket:int,rowId:bigint,"
                    + "currentTransaction:bigint,row:struct<_col0:int,_col1:string>>";

    public static void main(String[] args) throws Exception {
        java.nio.file.Path dir = Files.createTempDirectory("dojao-acid-probe");
        Configuration conf = new Configuration();
        Counter counter = new Counter();
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {

            System.out.println("=== 布局解析（纯函数） ===");
            check(counter, "目录名解析：base / delta / delete_delta", () -> {
                boolean ok = HiveAcidLayout.baseWriteId("base_0000010") == 10L;
                ok &= HiveAcidLayout.baseWriteId("delta_1_2_0") == null;
                ok &= HiveAcidLayout.baseWriteId("base_notnum") == null;
                ok &= Arrays.equals(HiveAcidLayout.deltaRange("delta_0000001_0000005_0"),
                        new long[]{1L, 5L});
                ok &= Arrays.equals(HiveAcidLayout.deltaRange("delete_delta_0000007_0000007_0"),
                        new long[]{7L, 7L});
                ok &= HiveAcidLayout.deltaRange("base_3") == null;
                ok &= HiveAcidLayout.isDeleteDelta("delete_delta_1_2_0");
                ok &= !HiveAcidLayout.isDeleteDelta("delta_1_2_0");
                return ok;
            });

            check(counter, "acidRoot：从数据文件反推分区目录", () -> {
                boolean ok = "hdfs://nn/p/dt=2020".equals(
                        HiveAcidLayout.acidRoot("hdfs://nn/p/dt=2020/base_0000010/000000_0"));
                ok &= "hdfs://nn/p/dt=2020".equals(
                        HiveAcidLayout.acidRoot("hdfs://nn/p/dt=2020/000000_0"));
                ok &= "hdfs://nn/p/dt=2020".equals(
                        HiveAcidLayout.acidRoot("hdfs://nn/p/dt=2020/delete_delta_1_2_0/000000_0"));
                // 非分区的表根（没有 base/delta 子目录）→ 上一级即根
                ok &= "hdfs://nn/p".equals(HiveAcidLayout.acidRoot("hdfs://nn/p/000000_0"));
                return ok;
            });

            check(counter, "写快照可见性：base / delta 过滤", () -> {
                // exceptions={3}, hw=10, minOpen=MAX ⇒ isValidBase(w) = w<=10 && w!=3
                ValidReaderWriteIdList v = new ValidReaderWriteIdList(
                        "db.t", new long[]{3L}, new BitSet(), 10L, Long.MAX_VALUE);
                boolean ok = HiveAcidLayout.baseVisible(v, 5) && !HiveAcidLayout.baseVisible(v, 11)
                        && !HiveAcidLayout.baseVisible(v, 20);
                ok &= HiveAcidLayout.deltaVisible(v, 1, 2);      // 区间全可见
                ok &= !HiveAcidLayout.deltaVisible(v, 3, 3);     // 区间内唯一写事务已失效
                ok &= HiveAcidLayout.deltaVisible(v, 2, 4);      // 部分可见
                ok &= !HiveAcidLayout.deltaVisible(v, 11, 12);   // 高于水位
                // minOpenWriteId 生效（exceptions 非空）：w < minOpen 才算闭合 base
                ValidReaderWriteIdList v2 = new ValidReaderWriteIdList(
                        "db.t", new long[]{3L}, new BitSet(), 10L, 7L);
                ok &= HiveAcidLayout.baseVisible(v2, 5) && !HiveAcidLayout.baseVisible(v2, 8);
                ok &= HiveAcidLayout.baseVisible(null, 999);     // 拿不到快照 → 一律可见
                return ok;
            });

            System.out.println("=== Hive2 事务快照（txn-id，与 Hive3 写快照并存） ===");

            check(counter, "txn 快照：open/aborted 不可见，committed/低于水位可见", () -> {
                // 打开事务：5=OPEN, 7=ABORTED, 9=COMMITTED；水位 10
                ValidReadTxnList v = HiveMetastore.buildTxnSnapshot(10L, List.of(
                        new TxnInfo(5, TxnState.OPEN, "u", "h"),
                        new TxnInfo(7, TxnState.ABORTED, "u", "h"),
                        new TxnInfo(9, TxnState.COMMITTED, "u", "h")));
                HiveAcidLayout.Visibility vis = HiveAcidLayout.Visibility.txns(v);
                boolean ok = vis.baseVisible(3) && !vis.baseVisible(5) && !vis.baseVisible(7)
                        && vis.baseVisible(9) && !vis.baseVisible(11) && !vis.baseVisible(20);
                ok &= vis.deltaVisible(1, 4);      // 区间在最小无效事务之下 → 全可见
                ok &= !vis.deltaVisible(5, 5);     // 唯一事务已打开 → 不可见
                ok &= vis.deltaVisible(6, 6);      // 6 不在无效集 → 可见
                ok &= !vis.deltaVisible(11, 12);   // 高于水位
                return ok;
            });

            check(counter, "txn 快照：无打开事务 / null 输入 / 空快照全可见", () -> {
                ValidReadTxnList empty = HiveMetastore.buildTxnSnapshot(10L, List.of());
                boolean ok = empty.isTxnValid(1) && empty.isTxnValid(10) && !empty.isTxnValid(11);
                ValidReadTxnList nullInfos = HiveMetastore.buildTxnSnapshot(10L, null);
                ok &= nullInfos.isTxnValid(10) && !nullInfos.isTxnValid(11);
                // 拿不到快照 → Visibility.ALWAYS（Hive2 / Hive3 一致）
                ok &= HiveAcidLayout.Visibility.txns(null) == HiveAcidLayout.Visibility.ALWAYS;
                ok &= HiveAcidLayout.Visibility.writeIds(null) == HiveAcidLayout.Visibility.ALWAYS;
                return ok;
            });

            check(counter, "groupByBucket：按 (分区, 桶号) 分组 + 排序", () -> {
                String root = "hdfs://nn/p/dt=2020";
                List<HiveAcidLayout.AcidFile> files = new ArrayList<>();
                files.add(new HiveAcidLayout.AcidFile(root, root + "/delta_1_2_0/000000_0", 10,
                        HiveAcidLayout.Kind.DELTA, 2));
                files.add(new HiveAcidLayout.AcidFile(root, root + "/base_0000010/000000_0", 10,
                        HiveAcidLayout.Kind.BASE, 10));
                files.add(new HiveAcidLayout.AcidFile(root, root + "/base_0000010/000001_0", 10,
                        HiveAcidLayout.Kind.BASE, 10));
                files.add(new HiveAcidLayout.AcidFile(root, root + "/000000_0", 10,
                        HiveAcidLayout.Kind.ORIGINAL, 0));
                List<HiveAcidLayout.BucketGroup> groups = HiveAcidLayout.groupByBucket(files);
                if (groups.size() != 2) return false;
                HiveAcidLayout.BucketGroup b0 = groups.stream()
                        .filter(g -> g.bucketId() == 0).findFirst().orElse(null);
                HiveAcidLayout.BucketGroup b1 = groups.stream()
                        .filter(g -> g.bucketId() == 1).findFirst().orElse(null);
                if (b0 == null || b1 == null) return false;
                // 桶 0：原始 + base + delta 共 3 个；顺序 = ORIGINAL → BASE → DELTA
                boolean ok = b0.files().size() == 3
                        && b0.files().get(0).kind() == HiveAcidLayout.Kind.ORIGINAL
                        && b0.files().get(1).kind() == HiveAcidLayout.Kind.BASE
                        && b0.files().get(2).kind() == HiveAcidLayout.Kind.DELTA;
                ok &= b1.files().size() == 1
                        && b1.files().get(0).path().endsWith("000001_0");
                return ok;
            });

            System.out.println("=== 行级合并（本地 ACID ORC） ===");
            HiveTableMeta meta = acidMeta(dir.toString());

            check(counter, "更新合并：同 RecordIdentifier 取更大 currentTransaction", () -> {
                String base = dir.resolve("u_base_0000010_000000_0").toString();
                String delta = dir.resolve("u_delta_11_11_0_000000_0").toString();
                writeAcid(conf, base, List.of(
                        new Rec(0, 1, 0, 10, 5, 1, "x", false)));
                writeAcid(conf, delta, List.of(
                        new Rec(1, 1, 0, 10, 9, 2, "y", false)));
                try (HiveAcidMerger.Result r = HiveAcidMerger.mergeGroup(
                        dir.toString(), List.of(base, delta), meta, allocator, conf, 1024)) {
                    if (r.rowCount() != 1) return false;
                    return intOf(r, 0, 0) == 2 && "y".equals(strOf(r, 0, 1));
                }
            });

            check(counter, "删除合并：base 2 行 + delta 删 1 + delete_delta 删 1 → 1 行", () -> {
                String base = dir.resolve("d_base_0000010_000000_0").toString();
                String del = dir.resolve("d_delta_12_12_0_000001_0").toString();
                String ddel = dir.resolve("d_delete_delta_13_13_0_000000_0").toString();
                writeAcid(conf, base, List.of(
                        new Rec(0, 2, 0, 20, 6, 100, "a", false),
                        new Rec(0, 2, 0, 21, 6, 101, "b", false)));
                // delta 里的一条 delete（op=2）删掉 rowId=20
                writeAcid(conf, del, List.of(
                        new Rec(2, 2, 0, 20, 8, null, null, false)));
                // delete_delta 里删掉 rowId=21
                writeAcid(conf, ddel, List.of(
                        new Rec(2, 2, 0, 21, 9, null, null, false)));
                try (HiveAcidMerger.Result r = HiveAcidMerger.mergeGroup(
                        dir.toString(), List.of(base, del, ddel), meta, allocator, conf, 1024)) {
                    return r.rowCount() == 0;
                }
            });

            check(counter, "删除合并：只删其一 → 剩 1 行且值正确", () -> {
                String base = dir.resolve("d2_base_0000010_000000_0").toString();
                String ddel = dir.resolve("d2_delete_delta_13_13_0_000000_0").toString();
                writeAcid(conf, base, List.of(
                        new Rec(0, 2, 0, 20, 6, 100, "a", false),
                        new Rec(0, 2, 0, 21, 6, 101, "b", false)));
                writeAcid(conf, ddel, List.of(
                        new Rec(2, 2, 0, 20, 9, null, null, false)));
                try (HiveAcidMerger.Result r = HiveAcidMerger.mergeGroup(
                        dir.toString(), List.of(base, ddel), meta, allocator, conf, 1024)) {
                    return r.rowCount() == 1
                            && intOf(r, 0, 0) == 101 && "b".equals(strOf(r, 0, 1));
                }
            });

            check(counter, "删除后又插入（同 rowId 更大 cTxn 的 insert 复活）", () -> {
                String base = dir.resolve("r_base_0000010_000000_0").toString();
                String del = dir.resolve("r_delete_delta_11_11_0_000000_0").toString();
                String re = dir.resolve("r_delta_12_12_0_000000_0").toString();
                writeAcid(conf, base, List.of(
                        new Rec(0, 5, 0, 40, 4, 7, "old", false)));
                writeAcid(conf, del, List.of(
                        new Rec(2, 5, 0, 40, 8, null, null, false)));
                // 更晚的同一 rowId insert（cTxn 更大）→ 覆盖删除标记
                writeAcid(conf, re, List.of(
                        new Rec(0, 5, 0, 40, 12, 9, "new", false)));
                try (HiveAcidMerger.Result r = HiveAcidMerger.mergeGroup(
                        dir.toString(), List.of(base, del, re), meta, allocator, conf, 1024)) {
                    return r.rowCount() == 1
                            && intOf(r, 0, 0) == 9 && "new".equals(strOf(r, 0, 1));
                }
            });

            check(counter, "row 结构整行为 NULL → 数据列全 NULL（不 NPE）", () -> {
                String base = dir.resolve("n_base_0000010_000000_0").toString();
                writeAcid(conf, base, List.of(
                        new Rec(0, 3, 0, 30, 2, null, null, true)));
                try (HiveAcidMerger.Result r = HiveAcidMerger.mergeGroup(
                        dir.toString(), List.of(base), meta, allocator, conf, 1024)) {
                    if (r.rowCount() != 1) return false;
                    IntVector a = (IntVector) r.columns().get(0);
                    VarCharVector b = (VarCharVector) r.columns().get(1);
                    return a.isNull(0) && b.isNull(0);
                }
            });

            check(counter, "非 ACID 遗留文件回退：按列名逐行读入", () -> {
                String plain = dir.resolve("plain_000000_0").toString();
                writePlain(conf, plain, new Integer[]{1, 2, 3},
                        new String[]{"p", "q", "r"});
                try (HiveAcidMerger.Result r = HiveAcidMerger.mergeGroup(
                        dir.toString(), List.of(plain), meta, allocator, conf, 1024)) {
                    if (r.rowCount() != 3) return false;
                    return intOf(r, 0, 0) == 1 && "p".equals(strOf(r, 0, 1))
                            && intOf(r, 1, 0) == 2 && "q".equals(strOf(r, 1, 1))
                            && intOf(r, 2, 0) == 3 && "r".equals(strOf(r, 2, 1));
                }
            });
        }
        System.out.println("=== 结果 ===");
        System.out.printf("通过 %d，失败 %d%n", counter.pass, counter.fail);
        System.out.println("临时目录: " + dir);
        if (counter.fail > 0) System.exit(1);
    }

    // ==================== 本地 ORC 写入 ====================

    /** 一条 ACID 记录（{@code a}/{@code b} 为用户数据列；{@code rowNull} = 整 {@code row} 结构为 NULL）。 */
    private record Rec(long op, long origTxn, long bucket, long rowId, long cTxn,
                       Integer a, String b, boolean rowNull) {
    }

    private static void writeAcid(Configuration conf, String path, List<Rec> recs) throws Exception {
        TypeDescription schema = TypeDescription.fromString(ACID_SCHEMA);
        Path p = new Path(path);
        try (Writer w = OrcFile.createWriter(p,
                OrcFile.writerOptions(conf).setSchema(schema).overwrite(true))) {
            VectorizedRowBatch batch = schema.createRowBatch();
            LongColumnVector op = (LongColumnVector) batch.cols[0];
            LongColumnVector orig = (LongColumnVector) batch.cols[1];
            LongColumnVector buk = (LongColumnVector) batch.cols[2];
            LongColumnVector rid = (LongColumnVector) batch.cols[3];
            LongColumnVector cur = (LongColumnVector) batch.cols[4];
            StructColumnVector row = (StructColumnVector) batch.cols[5];
            LongColumnVector c0 = (LongColumnVector) row.fields[0];
            BytesColumnVector c1 = (BytesColumnVector) row.fields[1];
            op.noNulls = true; orig.noNulls = true; buk.noNulls = true;
            rid.noNulls = true; cur.noNulls = true;
            row.noNulls = false;   // 允许某些行整结构 NULL
            c0.noNulls = false; c1.noNulls = false;
            for (Rec r : recs) {
                int i = batch.size;
                op.vector[i] = r.op();
                orig.vector[i] = r.origTxn();
                buk.vector[i] = r.bucket();
                rid.vector[i] = r.rowId();
                cur.vector[i] = r.cTxn();
                row.isNull[i] = r.rowNull();
                if (r.rowNull()) {
                    c0.isNull[i] = true;
                    c1.isNull[i] = true;
                } else {
                    c0.isNull[i] = r.a() == null;
                    if (r.a() != null) c0.vector[i] = r.a();
                    c1.isNull[i] = r.b() == null;
                    if (r.b() != null) c1.setVal(i, r.b().getBytes(StandardCharsets.UTF_8));
                }
                batch.size = ++i;
            }
            w.addRowBatch(batch);
        }
    }

    private static void writePlain(Configuration conf, String path, Integer[] as, String[] bs)
            throws Exception {
        TypeDescription schema = TypeDescription.fromString("struct<a:int,b:string>");
        Path p = new Path(path);
        try (Writer w = OrcFile.createWriter(p,
                OrcFile.writerOptions(conf).setSchema(schema).overwrite(true))) {
            VectorizedRowBatch batch = schema.createRowBatch();
            LongColumnVector a = (LongColumnVector) batch.cols[0];
            BytesColumnVector b = (BytesColumnVector) batch.cols[1];
            a.noNulls = false; b.noNulls = false;
            for (int i = 0; i < as.length; i++) {
                a.isNull[i] = as[i] == null;
                if (as[i] != null) a.vector[i] = as[i];
                b.isNull[i] = bs[i] == null;
                if (bs[i] != null) b.setVal(i, bs[i].getBytes(StandardCharsets.UTF_8));
                batch.size = i + 1;
            }
            w.addRowBatch(batch);
        }
    }

    // ==================== 结果读取 ====================

    private static int intOf(HiveAcidMerger.Result r, int row, int col) {
        return ((IntVector) r.columns().get(col)).get(row);
    }

    private static String strOf(HiveAcidMerger.Result r, int row, int col) {
        return new String(((VarCharVector) r.columns().get(col)).get(row), StandardCharsets.UTF_8);
    }

    private static HiveTableMeta acidMeta(String location) {
        return new HiveTableMeta("hive", "dojao_test", "acid_t", location, HiveStorageFormat.ORC,
                "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
                "org.apache.hadoop.hive.ql.io.orc.OrcSerde", true, Map.of(),
                List.of(col("a", "int"), col("b", "string")), List.of(),
                List.of(), 0, List.of(), '\001', '\\', "\\N");
    }

    private static Column col(String name, String type) {
        return new Column(name, type, null, null, true);
    }

    // ==================== 断言工具 ====================

    private static final class Counter {
        int pass;
        int fail;
    }

    @FunctionalInterface
    private interface Probe {
        boolean ok() throws Exception;
    }

    private static void check(Counter counter, String name, Probe probe) {
        boolean ok;
        try {
            ok = probe.ok();
        } catch (Throwable e) {
            System.out.printf("[FAIL] %s —— 抛异常: %s%n", name, e);
            e.printStackTrace(System.out);
            counter.fail++;
            return;
        }
        System.out.printf("[%s] %s%n", ok ? "PASS" : "FAIL", name);
        if (ok) counter.pass++;
        else counter.fail++;
    }
}
