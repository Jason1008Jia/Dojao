package com.dojao.connector.hive;

import com.dojao.common.data.Page;
import com.dojao.connector.common.util.ORCArrowWriter;
import com.dojao.connector.common.util.ORCFileArrowConverter;
import com.dojao.connector.common.util.ParquetArrowWriter;
import com.dojao.connector.common.util.ParquetFileArrowConverter;
import com.dojao.connector.common.util.TextArrowWriter;
import com.dojao.connector.common.util.TextFileArrowConverter;
import com.dojao.spi.bean.Column;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * <b>Hive 写路径</b>的离线探针（本地文件，无 HDFS / 无 HMS，秒级）。
 *
 * <p>覆盖四块：</p>
 * <ol>
 *   <li>{@link ORCArrowWriter} / {@link ParquetArrowWriter} / {@link TextArrowWriter}：
 *       Arrow → 文件 → Arrow 往返，值 / NULL / 空串 / decimal / timestamp / date 无损；</li>
 *   <li>{@link HiveWriteSession}：按目标任务格式选写入器与扩展名；动态分区按 {@code k=v} 拆目录、
 *       只写数据列、NULL 分区值落 {@code __HIVE_DEFAULT_PARTITION__}；非分区表 {@code close()}
 *       后文件搬进表目录（append 语义）；</li>
 *   <li>文本写入的边界：分隔符 / 转义符往返、含换行符的值明确报错；</li>
 *   <li>{@link HiveWriteSession#escapePathName} / {@code unescapePathName} 与
 *       {@link HiveTypeMapper#arrowToHiveType} 的往返。</li>
 * </ol>
 *
 * <p>需要 {@code --add-opens=java.base/java.nio=ALL-UNNAMED}（Arrow 走堆外内存）。</p>
 *
 * @author jiaxin
 * @date 2026/9/29
 */
public class HiveOrcWriteProbeTest {

    public static void main(String[] args) throws Exception {
        java.nio.file.Path dir = Files.createTempDirectory("dojao-orc-write-probe");
        Counter counter = new Counter();
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            System.out.println("=== ORCArrowWriter 往返 ===");
            String orcPath = dir.resolve("roundtrip.orc").toAbsolutePath().toString();
            check(counter, "Arrow → ORC → Arrow：行数 / 值 / NULL 无损", () -> {
                writeSampleRoot(allocator, orcPath, 7);
                List<Page> pages = readAll(allocator, orcPath, null);
                long rows = pages.stream().mapToLong(Page::getRowCount).sum();
                boolean ok = rows == 7;
                Page p = pages.get(0);
                ok &= ((IntVector) p.getRoot().getVector("id")).get(0) == 100;
                ok &= "row0".equals(str(p.getRoot().getVector("name"), 0));
                ok &= new BigDecimal("12.34").compareTo(
                        (BigDecimal) ((DecimalVector) p.getRoot().getVector("amount")).getObject(0)) == 0;
                ok &= ((IntVector) p.getRoot().getVector("id")).isNull(3);          // NULL 列保留
                ok &= "row4".equals(str(p.getRoot().getVector("name"), 4));
                ok &= ((BitVector) p.getRoot().getVector("flag")).get(1) == 1;
                pages.forEach(Page::close);
                return ok;
            });
            check(counter, "ORC 物理 schema 与 Arrow 类型一致", () -> {
                TypeDescriptionProbe t = new TypeDescriptionProbe(orcPath);
                return t.schema.startsWith("struct<")
                        && t.schema.contains("id:int") && t.schema.contains("name:string")
                        && t.schema.contains("amount:decimal(12,2)") && t.schema.contains("flag:boolean")
                        && t.schema.contains("ts:timestamp") && t.schema.contains("d:date")
                        && t.schema.contains("ratio:double");
            });

            System.out.println("=== 字符串 NULL / 空串往返 ===");
            check(counter, "VarChar 的 NULL 与空串写读往返（不 NPE、值正确）", () -> {
                String strPath = dir.resolve("strnull.orc").toAbsolutePath().toString();
                writeStrRoot(allocator, strPath);
                List<Page> pages = readAll(allocator, strPath, null);
                if (pages.size() != 1) {
                    pages.forEach(Page::close);
                    return false;
                }
                Page pg = pages.get(0);
                VarCharVector v = (VarCharVector) pg.getRoot().getVector("s");
                boolean ok = "a".equals(new String(v.get(0), StandardCharsets.UTF_8));
                ok &= !v.isNull(1) && v.get(1) != null && v.get(1).length == 0;  // 空串必须仍是空串
                ok &= v.isNull(2);                                               // NULL 必须仍是 NULL
                ok &= "b".equals(new String(v.get(3), StandardCharsets.UTF_8));
                pages.forEach(Page::close);
                return ok;
            });

            System.out.println("=== 空串 + 无 NULL（noNulls 快路径） ===");
            check(counter, "整批无 NULL 但含空串 → 不得 NPE（ORC 空串的 null 指针编码）", () -> {
                String strPath = dir.resolve("emptyonly.orc").toAbsolutePath().toString();
                writeStrRoot(allocator, strPath, new String[]{"a", "", "b"});
                List<Page> pages = readAll(allocator, strPath, null);
                if (pages.size() != 1) {
                    pages.forEach(Page::close);
                    return false;
                }
                Page pg = pages.get(0);
                VarCharVector v = (VarCharVector) pg.getRoot().getVector("s");
                boolean ok = "a".equals(new String(v.get(0), StandardCharsets.UTF_8));
                ok &= !v.isNull(1) && v.get(1) != null && v.get(1).length == 0;
                ok &= "b".equals(new String(v.get(2), StandardCharsets.UTF_8));
                pages.forEach(Page::close);
                return ok;
            });

            System.out.println("=== 常量字符串列（ORC RLE / repeating 快路径） ===");
            check(counter, "整列同一值 + 多行 → 读回全量（ORC repeating 不得 NPE）", () -> {
                String strPath = dir.resolve("constant.orc").toAbsolutePath().toString();
                String[] vals = new String[5000];
                java.util.Arrays.fill(vals, "2026-09-01");
                writeStrRoot(allocator, strPath, vals);
                List<Page> pages = readAll(allocator, strPath, null);
                long rows = pages.stream().mapToLong(Page::getRowCount).sum();
                boolean ok = rows == 5000;
                VarCharVector v = (VarCharVector) pages.get(0).getRoot().getVector("s");
                for (int i = 0; i < Math.min(64, pages.get(0).getRowCount()); i++) {
                    ok &= !v.isNull(i) && v.get(i) != null
                            && "2026-09-01".equals(new String(v.get(i), StandardCharsets.UTF_8));
                }
                pages.forEach(Page::close);
                return ok;
            });

            System.out.println("=== ParquetArrowWriter 往返 ===");
            String parquetPath = dir.resolve("roundtrip.parquet").toAbsolutePath().toString();
            check(counter, "Arrow → Parquet → Arrow：行数 / 值 / NULL / decimal / timestamp / date 无损", () -> {
                VectorSchemaRoot root = sampleRoot(allocator, 7, new String[0]);
                try {
                    Path file = new Path(parquetPath);
                    FileSystem fs = file.getFileSystem(new Configuration());
                    try (ParquetArrowWriter writer = new ParquetArrowWriter(new Configuration(), file, fs,
                            root.getSchema().getFields(), identity(SAMPLE_COLS))) {
                        writer.write(root);
                    }
                } finally {
                    root.close();
                }
                List<Page> pages = readAllParquet(allocator, parquetPath);
                long rows = pages.stream().mapToLong(Page::getRowCount).sum();
                boolean ok = rows == 7 && !pages.isEmpty();
                Page p = pages.get(0);
                ok &= ((IntVector) p.getRoot().getVector("id")).get(0) == 100;
                ok &= "row0".equals(str(p.getRoot().getVector("name"), 0));
                ok &= new BigDecimal("12.34").compareTo(
                        (BigDecimal) ((DecimalVector) p.getRoot().getVector("amount")).getObject(0)) == 0;
                ok &= ((IntVector) p.getRoot().getVector("id")).isNull(3);          // NULL 列保留
                ok &= "row4".equals(str(p.getRoot().getVector("name"), 4));
                ok &= ((BitVector) p.getRoot().getVector("flag")).get(1) == 1;
                ok &= ((TimeStampMilliVector) p.getRoot().getVector("ts")).get(0) == 1_700_000_000_000L;
                ok &= ((DateDayVector) p.getRoot().getVector("d")).get(0) == 19_000;
                pages.forEach(Page::close);
                return ok;
            });
            check(counter, "Parquet 物理 schema 采用 Hive 口径（timestamp=INT64/MILLIS、decimal=FIXED_LEN_BYTE_ARRAY）", () -> {
                Path file = new Path(parquetPath);
                try (org.apache.parquet.hadoop.ParquetFileReader reader =
                             org.apache.parquet.hadoop.ParquetFileReader.open(
                                     org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(
                                             file, new Configuration()))) {
                    String schema = reader.getFooter().getFileMetaData().getSchema().toString();
                    return schema.contains("TIMESTAMP(MILLIS") && schema.contains("DECIMAL(12,2)")
                            && schema.contains("DATE") && schema.contains("STRING");
                }
            });

            System.out.println("=== TextArrowWriter 往返 ===");
            String textPath = dir.resolve("roundtrip.txt").toAbsolutePath().toString();
            check(counter, "Arrow → 分隔文本 → Arrow：值 / 空串 / NULL / 分隔符与转义符无损", () -> {
                List<Field> fields = List.of(
                        new Field("s", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
                        new Field("n", FieldType.nullable(new ArrowType.Int(32, true)), null));
                String[] sv = {"a", "", null, "x\001y", "back\\slash", "中文"};
                Integer[] nv = {1, 2, null, 4, 5, 6};
                try (VarCharVector s = new VarCharVector("s", allocator);
                     IntVector n = new IntVector("n", allocator)) {
                    s.setInitialCapacity(6);
                    s.allocateNew();
                    n.setInitialCapacity(6);
                    n.allocateNew();
                    for (int r = 0; r < 6; r++) {
                        if (sv[r] == null) s.setNull(r);
                        else s.setSafe(r, sv[r].getBytes(StandardCharsets.UTF_8));
                        if (nv[r] == null) n.setNull(r);
                        else n.setSafe(r, nv[r]);
                    }
                    s.setValueCount(6);
                    n.setValueCount(6);
                    try (VectorSchemaRoot root = new VectorSchemaRoot(fields, List.of(s, n), 6)) {
                        Path file = new Path(textPath);
                        FileSystem fs = file.getFileSystem(new Configuration());
                        try (TextArrowWriter writer = new TextArrowWriter(new Configuration(), file, fs,
                                fields, new int[]{0, 1})) {
                            writer.write(root);
                        }
                    }
                }
                List<Page> pages = readAllText(allocator, textPath, new Schema(fields));
                long rows = pages.stream().mapToLong(Page::getRowCount).sum();
                boolean ok = rows == 6 && !pages.isEmpty();
                Page p = pages.get(0);
                VarCharVector rs = (VarCharVector) p.getRoot().getVector("s");
                IntVector rn = (IntVector) p.getRoot().getVector("n");
                ok &= "a".equals(str(rs, 0));
                ok &= !rs.isNull(1) && rs.get(1) != null && rs.get(1).length == 0;  // 空串仍是空串
                ok &= rs.isNull(2);                                                  // NULL 仍是 NULL
                ok &= "x\001y".equals(str(rs, 3));                                   // 分隔符在值内被转义
                ok &= "back\\slash".equals(str(rs, 4));                              // 转义符自身
                ok &= "中文".equals(str(rs, 5));
                ok &= rn.get(0) == 1 && rn.isNull(2) && rn.get(5) == 6;
                pages.forEach(Page::close);
                return ok;
            });
            check(counter, "文本写入含换行符的值 → 明确报错（不静默把一条记录写成两条）", () -> {
                Field s = new Field("s", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
                try (VarCharVector v = new VarCharVector("s", allocator)) {
                    v.setInitialCapacity(1);
                    v.allocateNew();
                    v.setSafe(0, "a\nb".getBytes(StandardCharsets.UTF_8));
                    v.setValueCount(1);
                    try (VectorSchemaRoot root = new VectorSchemaRoot(List.of(s), List.of(v), 1)) {
                        Path file = new Path(dir.resolve("newline.txt").toAbsolutePath().toString());
                        FileSystem fs = file.getFileSystem(new Configuration());
                        try (TextArrowWriter writer = new TextArrowWriter(new Configuration(), file, fs,
                                List.of(s), new int[]{0})) {
                            writer.write(root);
                        } catch (IllegalArgumentException expected) {
                            return expected.getMessage().contains("换行");
                        }
                    }
                }
                return false;
            });

            System.out.println("=== HiveWriteSession 动态分区拆分 ===");
            String tableLoc = dir.resolve("dw.db").resolve("events").toAbsolutePath().toString();
            HiveTableMeta partMeta = partitionMeta(tableLoc, HiveStorageFormat.ORC);
            check(counter, "两页按 dt 拆成 3 个分区目录（含默认分区），文件只含数据列", () -> {
                java.nio.file.Path tmp = Files.createTempDirectory(dir, "_dojao_tmp_");
                HiveWriteSession session = new HiveWriteSession(
                        new Configuration(), fsOf(tmp.toString()), partMeta, null,
                        tmp.toString(), false);
                VectorSchemaRoot page1 = samplePage(allocator, new String[]{
                        "2026-09-01", "2026-09-01", "2026-09-02", null});
                VectorSchemaRoot page2 = samplePage(allocator, new String[]{"2026-09-02", "2026-09-02"});
                session.write(page1, null);
                session.write(page2, null);
                session.close();
                page1.close();
                page2.close();

                List<String> names = new ArrayList<>();
                int totalRows = 0;
                for (java.nio.file.Path sub : listDirs(tmp)) {
                    names.add(sub.getFileName().toString());
                    for (java.nio.file.Path f : listFiles(sub)) {
                        List<Page> pages = readAll(allocator, f.toAbsolutePath().toString(), null);
                        for (Page p : pages) {
                            totalRows += p.getRowCount();
                            // 文件里只有数据列，不该出现分区列 dt
                            if (p.getRoot().getVector("dt") != null) {
                                pages.forEach(Page::close);
                                return false;
                            }
                            p.close();
                        }
                    }
                }
                names.sort(String::compareTo);
                boolean ok = names.equals(List.of("dt=2026-09-01", "dt=2026-09-02",
                        "dt=__HIVE_DEFAULT_PARTITION__"));
                ok &= totalRows == 6;
                return ok;
            });

            System.out.println("=== 非分区 close() 搬入表目录（append） ===");
            check(counter, "close() 后临时目录消失、文件落在表目录且可读回", () -> {
                HiveTableMeta meta = flatMeta(tableLoc);
                Files.createDirectories(java.nio.file.Path.of(tableLoc));
                HiveWriteSession session = new HiveWriteSession(
                        new Configuration(), nioFs(), meta, null,
                        java.nio.file.Path.of(tableLoc).resolve("_dojao_tmp_probe").toString(), true);
                VectorSchemaRoot page = samplePage(allocator, new String[]{"2026-09-01", "2026-09-02"});
                session.write(page, null);
                session.close();
                page.close();

                boolean tmpGone = !java.nio.file.Files.exists(
                        java.nio.file.Path.of(tableLoc).resolve("_dojao_tmp_probe"));
                long rows = 0;
                for (java.nio.file.Path f : listFiles(java.nio.file.Path.of(tableLoc))) {
                    List<Page> pages = readAll(allocator, f.toAbsolutePath().toString(), null);
                    for (Page p : pages) { rows += p.getRowCount(); p.close(); }
                }
                return tmpGone && rows == 2;
            });

            System.out.println("=== HiveWriteSession 按目标表格式落文件 ===");
            check(counter, "PARQUET 表 → .parquet 文件且写读往返 2 行", () -> {
                String loc = dir.resolve("fmt.db").resolve("p_events").toAbsolutePath().toString();
                Files.createDirectories(java.nio.file.Path.of(loc));
                HiveTableMeta meta = flatMeta(loc, HiveStorageFormat.PARQUET);
                HiveWriteSession session = new HiveWriteSession(new Configuration(), nioFs(), meta, null,
                        java.nio.file.Path.of(loc).resolve("_dojao_tmp_p").toString(), true);
                VectorSchemaRoot page = samplePage(allocator, new String[]{"2026-09-01", "2026-09-02"});
                session.write(page, null);
                session.close();
                page.close();

                long rows = 0;
                boolean suffixOk = true;
                for (java.nio.file.Path f : listFiles(java.nio.file.Path.of(loc))) {
                    suffixOk &= f.getFileName().toString().endsWith(".parquet");
                    List<Page> pages = readAllParquet(allocator, f.toAbsolutePath().toString());
                    for (Page p : pages) { rows += p.getRowCount(); p.close(); }
                }
                return suffixOk && rows == 2;
            });
            check(counter, "TEXT 表 → .txt 文件且写读往返 2 行", () -> {
                String loc = dir.resolve("fmt.db").resolve("t_events").toAbsolutePath().toString();
                Files.createDirectories(java.nio.file.Path.of(loc));
                HiveTableMeta meta = flatMeta(loc, HiveStorageFormat.TEXT);
                HiveWriteSession session = new HiveWriteSession(new Configuration(), nioFs(), meta, null,
                        java.nio.file.Path.of(loc).resolve("_dojao_tmp_t").toString(), true);
                VectorSchemaRoot page = samplePage(allocator, new String[]{"2026-09-01", "2026-09-02"});
                session.write(page, null);
                session.close();
                page.close();

                long rows = 0;
                boolean suffixOk = true;
                for (java.nio.file.Path f : listFiles(java.nio.file.Path.of(loc))) {
                    suffixOk &= f.getFileName().toString().endsWith(".txt");
                    List<Page> pages = readAllText(allocator, f.toAbsolutePath().toString(),
                            new Schema(meta.dataFields()));
                    for (Page p : pages) { rows += p.getRowCount(); p.close(); }
                }
                return suffixOk && rows == 2;
            });

            System.out.println("=== 路径转义 / 类型映射 ===");
            check(counter, "escapePathName / unescapePathName 往返（含中文与 %）", () -> {
                for (String v : List.of("2026-09-01", "a b", "中文", "x%y", "a/b", "")) {
                    if (!v.equals(HiveWriteSession.unescapePathName(HiveWriteSession.escapePathName(v)))) {
                        return false;
                    }
                }
                return "__HIVE_DEFAULT_PARTITION__".equals(HiveWriteSession.unescapePathName(
                        HiveWriteSession.escapePathName("__HIVE_DEFAULT_PARTITION__")));
            });
            check(counter, "arrowToHiveType 覆盖标量类型", () ->
                    "int".equals(HiveTypeMapper.arrowToHiveType(new ArrowType.Int(32, true)))
                            && "bigint".equals(HiveTypeMapper.arrowToHiveType(new ArrowType.Int(64, true)))
                            && "decimal(12,2)".equals(HiveTypeMapper.arrowToHiveType(
                                    new ArrowType.Decimal(12, 2, 128)))
                            && "string".equals(HiveTypeMapper.arrowToHiveType(ArrowType.Utf8.INSTANCE))
                            && "double".equals(HiveTypeMapper.arrowToHiveType(
                                    new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)))
                            && "timestamp".equals(HiveTypeMapper.arrowToHiveType(
                                    new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)))
                            && "date".equals(HiveTypeMapper.arrowToHiveType(new ArrowType.Date(DateUnit.DAY)))
                            && "boolean".equals(HiveTypeMapper.arrowToHiveType(ArrowType.Bool.INSTANCE)));
            check(counter, "复合类型（array / map / struct）原生结构化，uniontype 仍为文本", () -> {
                if (HiveTypeMapper.toDojaoType("array<string>") != com.dojao.spi.bean.DojaoType.LIST) return false;
                if (HiveTypeMapper.toDojaoType("map<string,int>") != com.dojao.spi.bean.DojaoType.MAP) return false;
                if (HiveTypeMapper.toDojaoType("struct<a:int,b:string>")
                        != com.dojao.spi.bean.DojaoType.STRUCT) return false;
                if (HiveTypeMapper.toDojaoType("uniontype<int,string>")
                        != com.dojao.spi.bean.DojaoType.STRING) return false;

                if (!(HiveTypeMapper.toArrowType("array<string>") instanceof ArrowType.List)) return false;
                if (!(HiveTypeMapper.toArrowType("map<string,int>") instanceof ArrowType.Map)) return false;
                if (!(HiveTypeMapper.toArrowType("struct<a:int,b:string>") instanceof ArrowType.Struct)) return false;
                if (!(HiveTypeMapper.toArrowType("uniontype<int,string>") instanceof ArrowType.Utf8)) return false;

                // 精确 Field 结构：List 元素名 item、Map entries/key/value 与非空约束
                var listField = HiveTypeMapper.toField("c", "array<string>");
                if (!listField.getChildren().get(0).getName().equals("item")) return false;
                var mapField = HiveTypeMapper.toField("c", "map<string,int>");
                var entries = mapField.getChildren().get(0);
                if (!entries.getName().equals("entries") || entries.getFieldType().isNullable()) return false;
                if (!entries.getChildren().get(0).getName().equals("key")
                        || entries.getChildren().get(0).getFieldType().isNullable()) return false;
                if (!entries.getChildren().get(1).getName().equals("value")) return false;

                // 文本模式：复合类型降级为 Utf8，uniontype 恒文本
                if (!(HiveTypeMapper.toField("c", "array<string>", false).getType()
                        instanceof ArrowType.Utf8)) return false;
                if (!(HiveTypeMapper.toField("c", "map<string,int>", false).getType()
                        instanceof ArrowType.Utf8)) return false;
                if (!(HiveTypeMapper.toField("c", "struct<a:int>", false).getType()
                        instanceof ArrowType.Utf8)) return false;

                // 反向：Arrow Field → Hive 类型（CTAS 建表用），嵌套可往返
                if (!"array<string>".equals(HiveTypeMapper.arrowToHiveType(listField))) return false;
                if (!"map<string,int>".equals(HiveTypeMapper.arrowToHiveType(mapField))) return false;
                if (!"struct<a:int,b:string>".equals(HiveTypeMapper.arrowToHiveType(
                        HiveTypeMapper.toField("c", "struct<a:int,b:string>")))) return false;

                // 标量不能被误判成复合
                return !HiveTypeMapper.isNested("string") && !HiveTypeMapper.isNested("decimal(10,2)");
            });
        }
        System.out.println("=== 结果 ===");
        System.out.printf("通过 %d，失败 %d%n", counter.pass, counter.fail);
        System.out.println("临时目录: " + dir);
        if (counter.fail > 0) System.exit(1);
    }

    // ==================== 样例数据 ====================

    private static final int SAMPLE_COLS = 7;

    private static void writeSampleRoot(BufferAllocator allocator, String orcPath, int rows)
            throws Exception {
        VectorSchemaRoot root = sampleRoot(allocator, rows, new String[0]);
        Path file = new Path(orcPath);
        FileSystem fs = file.getFileSystem(new Configuration());
        try (ORCArrowWriter writer = new ORCArrowWriter(new Configuration(), file, fs,
                root.getSchema().getFields(), identity(SAMPLE_COLS))) {
            writer.write(root);
        } finally {
            root.close();
        }
    }

    /** 单列 {@code s:string}，值为 [ "a", ""(空串), NULL, "b" ] —— 覆盖 ORC 空串与空值的边界。 */
    private static void writeStrRoot(BufferAllocator allocator, String orcPath) throws Exception {
        writeStrRoot(allocator, orcPath, new String[]{"a", "", null, "b"});
    }

    /** 单列 {@code s:string}，值由调用方给定（{@code null} 元素写 NULL，{@code ""} 写空串）。 */
    private static void writeStrRoot(BufferAllocator allocator, String orcPath, String[] values)
            throws Exception {
        Field f = new Field("s", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        VarCharVector v = new VarCharVector("s", allocator);
        v.setInitialCapacity(Math.max(1, values.length));
        v.allocateNew();
        for (int r = 0; r < values.length; r++) {
            if (values[r] == null) v.setNull(r);
            else v.setSafe(r, values[r].getBytes(StandardCharsets.UTF_8));
        }
        v.setValueCount(values.length);
        VectorSchemaRoot root = new VectorSchemaRoot(List.of(f), List.of(v), values.length);
        Path file = new Path(orcPath);
        FileSystem fs = file.getFileSystem(new Configuration());
        try (ORCArrowWriter writer = new ORCArrowWriter(new Configuration(), file, fs,
                root.getSchema().getFields(), new int[]{0})) {
            writer.write(root);
        } finally {
            root.close();
        }
    }

    /** 一页全类型样例（只填前 {@code partitionVals.length} 行的 dt；用于分区会话探针）。 */
    private static VectorSchemaRoot samplePage(BufferAllocator allocator, String[] partitionVals) {
        return sampleRoot(allocator, partitionVals.length, partitionVals);
    }

    /**
     * 全类型 Arrow 根：id(int) / name(utf8) / amount(decimal12,2) / ratio(double) / flag(bool)
     * / ts(timestamp) / d(date) 为数据列；若给了分区值再追加一列 {@code dt}。
     */
    private static VectorSchemaRoot sampleRoot(BufferAllocator allocator, int rows,
                                               String[] partitionVals) {
        boolean withDt = partitionVals.length > 0;
        List<Field> fields = new ArrayList<>();
        List<FieldVector> vecs = new ArrayList<>();
        fields.add(new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null));
        fields.add(new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null));
        fields.add(new Field("amount", FieldType.nullable(new ArrowType.Decimal(12, 2, 128)), null));
        fields.add(new Field("ratio", FieldType.nullable(
                new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null));
        fields.add(new Field("flag", FieldType.nullable(ArrowType.Bool.INSTANCE), null));
        fields.add(new Field("ts", FieldType.nullable(
                new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)), null));
        fields.add(new Field("d", FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), null));

        IntVector id = new IntVector("id", allocator);
        VarCharVector name = new VarCharVector("name", allocator);
        DecimalVector amount = new DecimalVector("amount", allocator, 12, 2);
        Float8Vector ratio = new Float8Vector("ratio", allocator);
        BitVector flag = new BitVector("flag", allocator);
        TimeStampMilliVector ts = new TimeStampMilliVector("ts", allocator);
        DateDayVector d = new DateDayVector("d", allocator);
        for (FieldVector v : List.of(id, name, amount, ratio, flag, ts, d)) {
            v.setInitialCapacity(Math.max(1, rows));
            v.allocateNew();
        }
        for (int r = 0; r < rows; r++) {
            if (r == 3) id.setNull(r);                       // 一个 NULL 数据列
            else id.setSafe(r, 100 + r);
            byte[] bytes = ("row" + r).getBytes(StandardCharsets.UTF_8);
            name.setSafe(r, bytes);
            amount.setSafe(r, new BigDecimal("12.34").add(BigDecimal.valueOf(r)));
            ratio.setSafe(r, r * 1.5);
            flag.setSafe(r, r % 2);
            ts.setSafe(r, 1_700_000_000_000L + r * 1000L);
            d.setSafe(r, 19_000 + r);
        }
        vecs.addAll(List.of(id, name, amount, ratio, flag, ts, d));

        if (withDt) {
            VarCharVector dt = new VarCharVector("dt", allocator);
            dt.setInitialCapacity(rows);
            dt.allocateNew();
            for (int r = 0; r < rows; r++) {
                if (partitionVals[r] == null) dt.setNull(r);
                else dt.setSafe(r, partitionVals[r].getBytes(StandardCharsets.UTF_8));
            }
            fields.add(new Field("dt", FieldType.nullable(ArrowType.Utf8.INSTANCE), null));
            vecs.add(dt);
        }
        for (FieldVector v : vecs) v.setValueCount(rows);
        return new VectorSchemaRoot(fields, vecs, rows);
    }

    private static HiveTableMeta partitionMeta(String location, HiveStorageFormat format) {
        return new HiveTableMeta("hive", "dojao_test", "events", location, format,
                format.inputFormat(), format.serdeLib(), false, Map.of(),
                List.of(col("id", "int"), col("name", "string"), col("amount", "decimal(12,2)"),
                        col("ratio", "double"), col("flag", "boolean"), col("ts", "timestamp"),
                        col("d", "date")),
                List.of(col("dt", "string")), List.of(), 0, List.of(), '\001', '\\', "\\N");
    }

    private static HiveTableMeta flatMeta(String location) {
        return flatMeta(location, HiveStorageFormat.ORC);
    }

    private static HiveTableMeta flatMeta(String location, HiveStorageFormat format) {
        HiveTableMeta p = partitionMeta(location, format);
        return new HiveTableMeta(p.catalogName(), p.database(), p.table(), location, p.format(),
                p.inputFormat(), p.serdeLib(), false, Map.of(), p.dataColumns(), List.of(),
                List.of(), 0, List.of(), '\001', '\\', "\\N");
    }

    private static Column col(String name, String type) {
        return new Column(name, type, null, null, true);
    }

    // ==================== 读取 ====================

    private static List<Page> readAll(BufferAllocator allocator, String path, List<String> projection)
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

    private static List<Page> readAllParquet(BufferAllocator allocator, String path) throws Exception {
        Path p = new Path(path);
        FileSystem fs = p.getFileSystem(new Configuration());
        List<Page> pages = new ArrayList<>();
        try (ParquetFileArrowConverter converter = new ParquetFileArrowConverter(
                allocator, p, fs, 1024, 0L, -1L)) {
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

    private static List<Page> readAllText(BufferAllocator allocator, String path, Schema schema)
            throws Exception {
        Path p = new Path(path);
        FileSystem fs = p.getFileSystem(new Configuration());
        List<Page> pages = new ArrayList<>();
        try (TextFileArrowConverter converter = new TextFileArrowConverter(
                allocator, p, fs, schema, 1024)) {
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

    private static String str(FieldVector v, int row) {
        return new String(((VarCharVector) v).get(row), StandardCharsets.UTF_8);
    }

    private static FileSystem fsOf(String location) throws Exception {
        return new Path(location).getFileSystem(new Configuration());
    }

    /**
     * 探针专用本地文件系统：Windows 上 Hadoop 的 {@code RawLocalFileSystem.listStatus}
     * 走 {@code FileUtil.canRead} → {@code NativeIO$Windows.access0}，缺 winutils 直接
     * {@link UnsatisfiedLinkError}。这里只把目录列举换成 {@code java.io.File#listFiles()}，
     * 其余（create / mkdirs / rename / delete）仍是 Hadoop 自己的实现 ——
     * 恰好覆盖 {@link HiveWriteSession#close()} 的搬文件路径。
     */
    static final class NioLocalFileSystem extends RawLocalFileSystem {
        @Override
        public FileStatus[] listStatus(Path f) throws java.io.IOException {
            java.io.File local = pathToFile(f);
            if (!local.exists()) {
                throw new java.io.FileNotFoundException("File " + f + " does not exist");
            }
            if (local.isFile()) return new FileStatus[]{getFileStatus(f)};
            java.io.File[] children = local.listFiles();
            if (children == null) return new FileStatus[0];
            List<FileStatus> statuses = new ArrayList<>(children.length);
            for (java.io.File child : children) {
                statuses.add(getFileStatus(new Path(f, child.getName())));
            }
            return statuses.toArray(new FileStatus[0]);
        }
    }

    private static FileSystem nioFs() throws Exception {
        NioLocalFileSystem fs = new NioLocalFileSystem();
        fs.initialize(java.net.URI.create("file:///"), new Configuration());
        return fs;
    }

    /** Windows 上 Hadoop `fs.listStatus` 会走 NativeIO（缺 winutils 会 UnsatisfiedLinkError），探针改用 java.nio。 */
    private static List<java.nio.file.Path> listDirs(java.nio.file.Path dir) throws Exception {
        try (var s = java.nio.file.Files.list(dir)) {
            return s.filter(java.nio.file.Files::isDirectory).sorted().toList();
        }
    }

    private static List<java.nio.file.Path> listFiles(java.nio.file.Path dir) throws Exception {
        try (var s = java.nio.file.Files.list(dir)) {
            // java.nio 能看到 Hadoop LocalFileSystem 的隐藏校验文件（.part-*.orc.crc），需排除
            return s.filter(java.nio.file.Files::isRegularFile)
                    .filter(p -> !p.getFileName().toString().startsWith("."))
                    .sorted().toList();
        }
    }

    private static int[] identity(int n) {
        int[] out = new int[n];
        for (int i = 0; i < n; i++) out[i] = i;
        return out;
    }

    private static final class TypeDescriptionProbe {
        final String schema;

        TypeDescriptionProbe(String path) throws Exception {
            Path p = new Path(path);
            try (org.apache.orc.Reader reader = org.apache.orc.OrcFile.createReader(
                    p, org.apache.orc.OrcFile.readerOptions(new Configuration()))) {
                schema = reader.getSchema().toString();
            }
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
            // 捕获 Throwable（非仅 Exception）：Windows 缺 winutils 时 Hadoop 原生 listStatus 抛
            // UnsatisfiedLinkError（Error）—— 别让一个环境受限的用例中断整个探针套件。
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
