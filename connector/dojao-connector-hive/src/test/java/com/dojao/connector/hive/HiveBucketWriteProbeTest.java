package com.dojao.connector.hive;

import com.dojao.common.data.Page;
import com.dojao.connector.common.util.ORCFileArrowConverter;
import com.dojao.spi.bean.Column;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * <b>Hive 分桶写入</b>的离线探针（本地文件，无 HDFS / 无 HMS，秒级）。
 *
 * <p>验证 INSERT INTO / INSERT OVERWRITE 写分桶（+动态分区）目标表时：</p>
 * <ol>
 *   <li><b>分桶非分区</b>：行按 {@link HiveBucketHash} 散进 {@code <桶号>_<token>_<seq>.orc}，
 *       每个文件里的所有行都归同名桶，总行数不漏；</li>
 *   <li><b>分桶 × 动态分区</b>：目录先按 {@code <k=v>/} 拆分区，分区内再按桶号出文件；</li>
 *   <li><b>fail-loud</b>：分桶列类型不可哈希（{@code date}）时拒绝 —— 写前护栏
 *       {@link HiveConnector#checkWritableTarget} 抛 {@link UnsupportedOperationException}，
 *       会话内 {@link HiveWriteSession} 抛 {@link IllegalArgumentException}；</li>
 *   <li><b>命名兼容</b>：新文件名能被读侧 {@link HiveConnector#bucketIdOf} 正确归桶。</li>
 * </ol>
 *
 * <p>需要 {@code --add-opens=java.base/java.nio=ALL-UNNAMED}（Arrow 走堆外内存）。</p>
 *
 * @author jiaxin
 * @date 2026/10/7
 */
public class HiveBucketWriteProbeTest {

    public static void main(String[] args) throws Exception {
        java.nio.file.Path dir = Files.createTempDirectory("dojao-bucket-write-probe");
        Counter counter = new Counter();
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {

            System.out.println("=== 分桶非分区写 ===");
            check(counter, "行按 id 散进 <桶号>_... 文件；每文件行同桶、总行数不漏、四桶齐全", () -> {
                // moveOnClose=false：只 flush，不 move（Windows 上 Hadoop listStatus 走 NativeIO 会炸）
                java.nio.file.Path tmp = Files.createTempDirectory(dir, "_dojao_tmp_");
                HiveTableMeta meta = bucketedMeta(tmp.toString(), false);
                Integer[] ids = {0, 1, 2, 3, 4, 5, 6, 7};
                HiveWriteSession session = new HiveWriteSession(
                        new Configuration(), fsOf(tmp.toString()), meta, null, tmp.toString(), false);
                VectorSchemaRoot page = page(allocator, ids, null);
                session.write(page, null);
                session.close();
                page.close();

                int total = 0;
                boolean sawAllBuckets = true;
                boolean[] bucketSeen = new boolean[4];
                for (java.nio.file.Path f : listFiles(tmp)) {
                    Integer fileBucket = HiveConnector.bucketIdOf(f.getFileName().toString());
                    if (fileBucket == null) return false;
                    bucketSeen[fileBucket] = true;
                    for (Integer id : readIds(allocator, f.toAbsolutePath().toString())) {
                        total++;
                        int expected = bucketOf(id, "int", 4);
                        if (fileBucket != expected) return false;   // 行跑错桶 = 静默丢数据
                    }
                }
                for (boolean b : bucketSeen) sawAllBuckets &= b;
                return total == ids.length && sawAllBuckets;
            });

            System.out.println("=== 分桶 × 动态分区 ===");
            check(counter, "先按 dt 拆分区目录、再按 id 出桶文件；分区与桶都对", () -> {
                java.nio.file.Path tmp = Files.createTempDirectory(dir, "_dojao_tmp_");
                HiveTableMeta meta = bucketedMeta(tmp.toString(), true);
                String[] dts = {"2026-09-01", "2026-09-01", "2026-09-01", "2026-09-01",
                        "2026-09-02", "2026-09-02", "2026-09-02", "2026-09-02"};
                Integer[] ids = {0, 1, 2, 3, 0, 1, 2, 3};
                HiveWriteSession session = new HiveWriteSession(
                        new Configuration(), fsOf(tmp.toString()), meta, null, tmp.toString(), false);
                VectorSchemaRoot page = page(allocator, ids, dts);
                session.write(page, null);
                session.close();
                page.close();

                List<String> partDirs = new ArrayList<>();
                int total = 0;
                for (java.nio.file.Path sub : listDirs(tmp)) {
                    partDirs.add(sub.getFileName().toString());
                    for (java.nio.file.Path f : listFiles(sub)) {
                        Integer fileBucket = HiveConnector.bucketIdOf(f.getFileName().toString());
                        if (fileBucket == null) return false;
                        for (Integer id : readIds(allocator, f.toAbsolutePath().toString())) {
                            total++;
                            if (fileBucket != bucketOf(id, "int", 4)) return false;
                        }
                    }
                }
                partDirs.sort(String::compareTo);
                return partDirs.equals(List.of("dt=2026-09-01", "dt=2026-09-02"))
                        && total == ids.length;
            });

            System.out.println("=== fail-loud：不可哈希的分桶列 ===");
            check(counter, "checkWritableTarget：分桶列 date → UnsupportedOperationException", () -> {
                try {
                    HiveConnector.checkWritableTarget(dateBucketMeta(dir.toString()));
                    return false;
                } catch (UnsupportedOperationException e) {
                    return true;
                }
            });
            check(counter, "HiveWriteSession：分桶列 date → IllegalArgumentException", () -> {
                java.nio.file.Path tmp = Files.createTempDirectory(dir, "_dojao_tmp_");
                HiveTableMeta meta = dateBucketMeta(tmp.toString());
                HiveWriteSession session = new HiveWriteSession(
                        new Configuration(), fsOf(tmp.toString()), meta, null, tmp.toString(), false);
                VectorSchemaRoot page = datePage(allocator);
                try {
                    session.write(page, null);
                    return false;
                } catch (IllegalArgumentException e) {
                    return true;
                } finally {
                    page.close();
                    session.close();
                }
            });
            check(counter, "checkWritableTarget：ACID 事务表 → 拒绝", () -> {
                HiveTableMeta acid = new HiveTableMeta("hive", "db", "acid_t", dir.toString(),
                        HiveStorageFormat.ORC, ORC_IN, ORC_SERDE, true, Map.of(),
                        List.of(col("id", "int")), List.of(), List.of(), 0, List.of(),
                        '\001', '\\', "\\N");
                try {
                    HiveConnector.checkWritableTarget(acid);
                    return false;
                } catch (UnsupportedOperationException e) {
                    return true;
                }
            });

            System.out.println("=== 命名兼容（读侧归桶） ===");
            check(counter, "bucketIdOf(\"000003_ab12cd34_0\") == 3", () ->
                    Integer.valueOf(3).equals(HiveConnector.bucketIdOf("/x/y/000003_ab12cd34_0")));
        }
        System.out.println("=== 结果 ===");
        System.out.printf("通过 %d，失败 %d%n", counter.pass, counter.fail);
        System.out.println("临时目录: " + dir);
        if (counter.fail > 0) System.exit(1);
    }

    private static final String ORC_IN = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
    private static final String ORC_SERDE = "org.apache.hadoop.hive.ql.io.orc.OrcSerde";

    /** 期望桶号：与 Hive 同口径（单 int 列时 hash = id，桶 = id % n）。 */
    private static int bucketOf(Integer id, String type, int numBuckets) {
        String v = id == null ? null : String.valueOf(id);
        Integer hash = HiveBucketHash.bucketHash(List.of(type), java.util.Collections.singletonList(v));
        return HiveBucketHash.bucketNumber(hash, numBuckets);
    }

    // ==================== 样例数据 / 元数据 ====================

    /** 数据列 id(int)/name(string)；可选分区列 dt(string)。 */
    private static VectorSchemaRoot page(BufferAllocator allocator, Integer[] ids, String[] dts) {
        int rows = ids.length;
        List<Field> fields = new ArrayList<>();
        List<FieldVector> vecs = new ArrayList<>();
        fields.add(new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null));
        fields.add(new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null));
        IntVector id = new IntVector("id", allocator);
        VarCharVector name = new VarCharVector("name", allocator);
        id.setInitialCapacity(Math.max(1, rows));
        id.allocateNew();
        name.setInitialCapacity(Math.max(1, rows));
        name.allocateNew();
        for (int r = 0; r < rows; r++) {
            if (ids[r] == null) id.setNull(r);
            else id.setSafe(r, ids[r]);
            name.setSafe(r, ("n" + r).getBytes(StandardCharsets.UTF_8));
        }
        id.setValueCount(rows);
        name.setValueCount(rows);
        vecs.add(id);
        vecs.add(name);
        if (dts != null) {
            VarCharVector dt = new VarCharVector("dt", allocator);
            dt.setInitialCapacity(rows);
            dt.allocateNew();
            for (int r = 0; r < rows; r++) {
                if (dts[r] == null) dt.setNull(r);
                else dt.setSafe(r, dts[r].getBytes(StandardCharsets.UTF_8));
            }
            dt.setValueCount(rows);
            fields.add(new Field("dt", FieldType.nullable(ArrowType.Utf8.INSTANCE), null));
            vecs.add(dt);
        }
        return new VectorSchemaRoot(fields, vecs, rows);
    }

    /** id(int)/d(date) 两列 —— 分桶列 d 为不可哈希类型。 */
    private static VectorSchemaRoot datePage(BufferAllocator allocator) {
        List<Field> fields = List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("d", FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), null));
        IntVector id = new IntVector("id", allocator);
        DateDayVector d = new DateDayVector("d", allocator);
        id.setInitialCapacity(2);
        id.allocateNew();
        d.setInitialCapacity(2);
        d.allocateNew();
        id.setSafe(0, 1);
        id.setSafe(1, 2);
        d.setSafe(0, 19_000);
        d.setSafe(1, 19_001);
        id.setValueCount(2);
        d.setValueCount(2);
        return new VectorSchemaRoot(fields, List.of(id, d), 2);
    }

    private static HiveTableMeta bucketedMeta(String location, boolean partitioned) {
        List<Column> parts = partitioned ? List.of(col("dt", "string")) : List.of();
        return new HiveTableMeta("hive", "dojao_test", "bkt", location, HiveStorageFormat.ORC,
                ORC_IN, ORC_SERDE, false, Map.of(),
                List.of(col("id", "int"), col("name", "string")), parts,
                List.of("id"), 4, List.of(), '\001', '\\', "\\N");
    }

    private static HiveTableMeta dateBucketMeta(String location) {
        return new HiveTableMeta("hive", "dojao_test", "bkt_date", location, HiveStorageFormat.ORC,
                ORC_IN, ORC_SERDE, false, Map.of(),
                List.of(col("id", "int"), col("d", "date")), List.of(),
                List.of("d"), 4, List.of(), '\001', '\\', "\\N");
    }

    private static Column col(String name, String type) {
        return new Column(name, type, null, null, true);
    }

    // ==================== 读取 / 文件工具 ====================

    private static List<Page> readAll(BufferAllocator allocator, String path) throws Exception {
        Path p = new Path(path);
        FileSystem fs = p.getFileSystem(new Configuration());
        List<Page> pages = new ArrayList<>();
        try (ORCFileArrowConverter converter = new ORCFileArrowConverter(
                allocator, p, fs, 1024, 0L, -1L, null)) {
            while (true) {
                Page page = converter.convertNextBatch();
                if (page == null || page.isEOF()) break;
                if (page.getRowCount() == 0) {
                    page.close();
                    continue;
                }
                pages.add(page);
            }
        }
        return pages;
    }

    private static List<Integer> readIds(BufferAllocator allocator, String path) throws Exception {
        List<Integer> out = new ArrayList<>();
        for (Page p : readAll(allocator, path)) {
            IntVector id = (IntVector) p.getRoot().getVector("id");
            for (int r = 0; r < p.getRowCount(); r++) {
                out.add(id.isNull(r) ? null : id.get(r));
            }
            p.close();
        }
        return out;
    }

    private static FileSystem fsOf(String location) throws Exception {
        return new Path(location).getFileSystem(new Configuration());
    }

    /** Windows 上 Hadoop `fs.listStatus` 会走 NativeIO（缺 winutils 会 UnsatisfiedLinkError），探针改用 java.nio。 */
    private static List<java.nio.file.Path> listDirs(java.nio.file.Path dir) throws Exception {
        try (var s = java.nio.file.Files.list(dir)) {
            return s.filter(java.nio.file.Files::isDirectory).sorted().toList();
        }
    }

    private static List<java.nio.file.Path> listFiles(java.nio.file.Path dir) throws Exception {
        try (var s = java.nio.file.Files.list(dir)) {
            return s.filter(java.nio.file.Files::isRegularFile)
                    .filter(p -> !p.getFileName().toString().startsWith("."))
                    .sorted().toList();
        }
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
