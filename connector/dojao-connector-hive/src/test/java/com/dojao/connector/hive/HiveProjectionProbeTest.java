package com.dojao.connector.hive;

import com.dojao.common.data.Page;
import com.dojao.connector.common.util.ORCFileArrowConverter;
import com.dojao.connector.common.util.ParquetFileArrowConverter;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * <b>列投影下推 + 分片载荷</b>的离线探针（本地文件，无 HDFS / 无 HMS，秒级）。
 *
 * <p>验证：ORC / Parquet 读取器在给投影时，<b>产出的页只含被投影的列</b>（证明投影进到了 schema
 * 构造，而不是像从前那样整行读再裁），行数与值都无损；投影命中不到的列时退回全 schema；
 * 以及 {@link HiveSplitPayload} 对「整文件 / 文件内区间」两种单元的编解码往返。</p>
 *
 * @author jiaxin
 * @date 2026/9/27
 */
public class HiveProjectionProbeTest {

    private static final int ROWS = 5;

    public static void main(String[] args) throws Exception {
        java.nio.file.Path dir = Files.createTempDirectory("dojao-projection-probe");
        Counter counter = new Counter();
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            String orcPath = writeOrc(dir);
            String parquetPath = writeParquet(dir);

            System.out.println("=== ORC 列投影 ===");
            check(counter, "ORC 投影 [b] → 只剩 1 列", () -> {
                List<Field> fields = projectedOrcFields(allocator, orcPath, List.of("b"));
                return fields.size() == 1 && fields.get(0).getName().equals("b");
            });
            check(counter, "ORC 投影 [b] 的值与行数无损", () -> {
                List<Page> pages = readAllOrc(allocator, orcPath, List.of("b"));
                long rows = pages.stream().mapToLong(Page::getRowCount).sum();
                String first = firstString(pages, "b");
                String last = lastString(pages, "b");
                pages.forEach(Page::close);
                return rows == ROWS && "row0".equals(first) && "row4".equals(last);
            });
            check(counter, "ORC 投影 [c, a] → 2 列且按投影顺序", () -> {
                List<Field> fields = projectedOrcFields(allocator, orcPath, List.of("c", "a"));
                return fields.size() == 2 && fields.get(0).getName().equals("c")
                        && fields.get(1).getName().equals("a");
            });
            check(counter, "ORC 投影列名大小写不敏感", () -> {
                List<Field> fields = projectedOrcFields(allocator, orcPath, List.of("B"));
                return fields.size() == 1 && fields.get(0).getName().equals("b");
            });
            check(counter, "ORC 投影全不命中 → 退回全 3 列", () -> {
                List<Field> fields = projectedOrcFields(allocator, orcPath, List.of("zzz"));
                return fields.size() == 3;
            });

            System.out.println("=== Parquet 列投影 ===");
            check(counter, "Parquet 投影 [b] → 只剩 1 列", () -> {
                List<Field> fields = projectedParquetFields(allocator, parquetPath, List.of("b"));
                return fields.size() == 1 && fields.get(0).getName().equals("b");
            });
            check(counter, "Parquet 投影 [b] 的值与行数无损", () -> {
                List<Page> pages = readAllParquet(allocator, parquetPath, List.of("b"));
                long rows = pages.stream().mapToLong(Page::getRowCount).sum();
                String first = firstString(pages, "b");
                String last = lastString(pages, "b");
                pages.forEach(Page::close);
                return rows == ROWS && "row0".equals(first) && "row4".equals(last);
            });
            check(counter, "Parquet 投影 [a, c] → 2 列（文件序）", () -> {
                List<Field> fields = projectedParquetFields(allocator, parquetPath, List.of("a", "c"));
                return fields.size() == 2 && fields.get(0).getName().equals("a")
                        && fields.get(1).getName().equals("c");
            });
            check(counter, "Parquet 投影全不命中 → 退回全 3 列", () -> {
                List<Field> fields = projectedParquetFields(allocator, parquetPath, List.of("zzz"));
                return fields.size() == 3;
            });

            System.out.println("=== 分片载荷 v3（整文件 / 区间） ===");
            check(counter, "HiveSplitPayload v3 往返（整文件 + 区间）", () -> {
                List<HiveSplitUnit> in = List.of(
                        HiveSplitUnit.wholeFile("/p/dt=2026-09-11/a.orc"),
                        new HiveSplitUnit("/p/dt=2026-09-12/b.orc", 100L, 200L));
                List<HiveSplitUnit> out = HiveSplitPayload.decode(HiveSplitPayload.encode(in));
                return out.equals(in) && !out.get(0).isRange() && out.get(1).isRange()
                        && out.get(1).start() == 100L && out.get(1).length() == 200L;
            });
        }
        System.out.println("=== 结果 ===");
        System.out.printf("通过 %d，失败 %d%n", counter.pass, counter.fail);
        System.out.println("临时目录: " + dir);
        if (counter.fail > 0) System.exit(1);
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
        } catch (Exception e) {
            System.out.printf("[FAIL] %s —— 抛异常: %s%n", name, e);
            e.printStackTrace(System.out);
            counter.fail++;
            return;
        }
        System.out.printf("[%s] %s%n", ok ? "PASS" : "FAIL", name);
        if (ok) counter.pass++;
        else counter.fail++;
    }

    // ==================== ORC ====================

    private static String writeOrc(java.nio.file.Path dir) throws Exception {
        TypeDescription schema = TypeDescription.fromString("struct<a:int,b:string,c:double>");
        Configuration conf = new Configuration();
        Path file = new Path(dir.resolve("probe.orc").toAbsolutePath().toString());
        try (org.apache.orc.Writer writer = OrcFile.createWriter(file,
                OrcFile.writerOptions(conf).setSchema(schema))) {
            VectorizedRowBatch batch = schema.createRowBatch(ROWS);
            LongColumnVector a = (LongColumnVector) batch.cols[0];
            BytesColumnVector b = (BytesColumnVector) batch.cols[1];
            DoubleColumnVector c = (DoubleColumnVector) batch.cols[2];
            for (int i = 0; i < ROWS; i++) {
                a.vector[i] = 100 + i;
                byte[] bytes = ("row" + i).getBytes(StandardCharsets.UTF_8);
                b.setVal(i, bytes, 0, bytes.length);
                c.vector[i] = i;
            }
            batch.size = ROWS;
            writer.addRowBatch(batch);
        }
        return file.toString();
    }

    private static List<Page> readAllOrc(BufferAllocator allocator, String path, List<String> projection)
            throws Exception {
        Path p = new Path(path);
        FileSystem fs = p.getFileSystem(new Configuration());
        List<Page> pages = new ArrayList<>();
        try (ORCFileArrowConverter converter = new ORCFileArrowConverter(
                allocator, p, fs, 1024, 0L, -1L, projection)) {
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

    private static List<Field> projectedOrcFields(BufferAllocator allocator, String path,
                                                  List<String> projection) throws Exception {
        List<Page> pages = readAllOrc(allocator, path, projection);
        if (pages.isEmpty()) {
            throw new IllegalStateException("ORC 读不到任何行");
        }
        List<Field> fields = pages.get(0).getRoot().getSchema().getFields();
        pages.forEach(Page::close);
        return fields;
    }

    // ==================== Parquet ====================

    private static String writeParquet(java.nio.file.Path dir) throws Exception {
        MessageType schema = MessageTypeParser.parseMessageType(
                "message m { required int32 a; required binary b (UTF8); required double c; }");
        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(schema, conf);
        Path file = new Path(dir.resolve("probe.parquet").toAbsolutePath().toString());
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(file)
                .withConf(conf)
                .withType(schema)
                .build()) {
            for (int i = 0; i < ROWS; i++) {
                SimpleGroup g = new SimpleGroup(schema);
                g.add("a", 100 + i);
                g.add("b", "row" + i);
                g.add("c", (double) i);
                writer.write(g);
            }
        }
        return file.toString();
    }

    private static List<Page> readAllParquet(BufferAllocator allocator, String path,
                                             List<String> projection) throws Exception {
        Path p = new Path(path);
        FileSystem fs = p.getFileSystem(new Configuration());
        List<Page> pages = new ArrayList<>();
        try (ParquetFileArrowConverter converter = new ParquetFileArrowConverter(
                allocator, p, fs, 1024, 0L, -1L, projection)) {
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

    private static List<Field> projectedParquetFields(BufferAllocator allocator, String path,
                                                      List<String> projection) throws Exception {
        List<Page> pages = readAllParquet(allocator, path, projection);
        if (pages.isEmpty()) {
            throw new IllegalStateException("Parquet 读不到任何行");
        }
        List<Field> fields = pages.get(0).getRoot().getSchema().getFields();
        pages.forEach(Page::close);
        return fields;
    }

    // ==================== 取值 ====================

    private static String firstString(List<Page> pages, String column) {
        for (Page page : pages) {
            if (page.getRowCount() == 0) continue;
            return stringAt(page, column, 0);
        }
        return null;
    }

    private static String lastString(List<Page> pages, String column) {
        for (int i = pages.size() - 1; i >= 0; i--) {
            Page page = pages.get(i);
            if (page.getRowCount() == 0) continue;
            return stringAt(page, column, page.getRowCount() - 1);
        }
        return null;
    }

    private static String stringAt(Page page, String column, int row) {
        FieldVector v = page.getRoot().getVector(column);
        if (v == null) throw new IllegalStateException("页里没有列: " + column);
        return new String(((VarCharVector) v).get(row), StandardCharsets.UTF_8);
    }
}
