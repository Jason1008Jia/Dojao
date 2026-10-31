package com.dojao.connector.excel;

import com.dojao.common.data.Page;
import com.dojao.spi.bean.DojaoType;
import com.dojao.spi.bean.ReadRequest;
import com.dojao.spi.bean.ScanSlice;
import com.dojao.spi.bean.Split;
import com.dojao.spi.bean.TableStats;
import com.dojao.spi.service.ConnectorFactory;
import com.dojao.spi.service.ScanReader;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VarCharVector;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Excel 连接器离线探针 —— 不依赖 Apache POI，用 {@link ZipOutputStream} 现造最小 OOXML 工作簿。
 *
 * <p>覆盖：ServiceLoader 发现 / 工厂参数校验 / <b>单分区契约</b> / 元数据（schema=工作簿、
 * table=工作表）/ 类型推断（整数、小数、布尔、共享字符串、ISO 日期、日期序列值 + 样式）/
 * 多页流式读取 / 列投影 / offset+limit / 表头开关 / 单文件根路径 / 空工作表 / 错误语义。</p>
 *
 * <p>运行（模块目录下）：{@code java --add-opens=java.base/java.nio=ALL-UNNAMED -cp ... }。</p>
 */
public final class ExcelConnectorProbeTest {

    private static int pass;
    private static int fail;

    public static void main(String[] args) throws Exception {
        Path tmp = Files.createTempDirectory("dojao-excel-probe");
        try {
            buildFixtures(tmp);
            runAll(tmp);
        } finally {
            deleteRecursively(tmp);
        }
        System.out.println();
        System.out.println("==== Excel 连接器探针: " + pass + " PASS / " + fail + " FAIL ====");
        if (fail > 0) System.exit(1);
    }

    // ==================== 场景 ====================

    private static void runAll(Path tmp) {
        scenario("S1  ServiceLoader 能发现 excel 工厂", ExcelConnectorProbeTest::s1Discovery);
        scenario("S2  工厂参数校验 / 创建", () -> s2Factory(tmp));
        scenario("S3  ★单分区契约（planSplits 恒 1 片 + splitCount=1）",
                () -> s3SinglePartition(tmp));
        scenario("S4  元数据（schema=工作簿 / table=工作表 / 列类型）", () -> s4Metadata(tmp));
        scenario("S5  类型与取值（整数/小数/布尔/共享串/ISO 日期/日期序列）",
                () -> s5ReadTypes(tmp));
        scenario("S6  超过一页（>8192 行）的流式读取", () -> s6MultiPage(tmp));
        scenario("S7  列投影（大小写不敏感 + 顺序按请求）", () -> s7Projection(tmp));
        scenario("S8  offset / limit 分页语义", () -> s8OffsetLimit(tmp));
        scenario("S9  header=false：列名 col1…colN、首行即数据", () -> s9NoHeader(tmp));
        scenario("S10 根路径指向单个工作簿文件", () -> s10SingleFile(tmp));
        scenario("S11 空工作表 / 只有表头", () -> s11EmptySheets(tmp));
        scenario("S12 日期格式识别（formatCode 纯函数）", ExcelConnectorProbeTest::s12DateFormats);
        scenario("S13 列坐标解析（纯函数）", ExcelConnectorProbeTest::s13ColumnIndex);
        scenario("S14 日期序列值换算（纯函数）", ExcelConnectorProbeTest::s14Serial);
        scenario("S15 probeConnectivity 会真的打开工作簿", () -> s15Probe(tmp));
        scenario("S16 错误语义（未知列 / 只读）", () -> s16Errors(tmp));
    }

    private static void s1Discovery() {
        boolean found = ServiceLoader.load(ConnectorFactory.class).stream()
                .map(ServiceLoader.Provider::get)
                .anyMatch(f -> ExcelConnector.TYPE.equals(f.type()));
        check(found, "ServiceLoader 未发现 type()=excel 的工厂");
        ExcelConnectorFactory factory = new ExcelConnectorFactory();
        assertEquals("Excel", factory.displayName(), "displayName");
        check(factory.describeProperties().containsKey("catalog.excel.path"), "缺少 catalog.excel.path 说明");
        check(factory.describeProperties().containsKey("catalog.excel.header"), "缺少 catalog.excel.header 说明");
        check(factory.describeProperties().containsKey("catalog.excel.sampleRows"), "缺少 catalog.excel.sampleRows 说明");
    }

    private static void s2Factory(Path tmp) {
        ExcelConnectorFactory factory = new ExcelConnectorFactory();
        assertThrows(IllegalArgumentException.class,
                () -> factory.validate("c", Map.of()), "缺 path 必须拒绝");
        assertThrows(IllegalArgumentException.class,
                () -> factory.validate("c", Map.of("catalog.excel.path", "relative/dir")),
                "相对路径必须拒绝（Master/Worker 工作目录不同）");
        assertThrows(IllegalArgumentException.class,
                () -> factory.validate("c", Map.of(
                        "catalog.excel.path", tmp.toString(),
                        "catalog.excel.header", "maybe")), "非法 header 必须拒绝");
        assertThrows(IllegalArgumentException.class,
                () -> factory.validate("c", Map.of(
                        "catalog.excel.path", tmp.toString(),
                        "catalog.excel.sampleRows", "-3")), "负 sampleRows 必须拒绝");

        factory.validate("c", Map.of("catalog.excel.path", tmp.toString()));
        Object created = factory.create("excel_sales", Map.of(
                "catalog.excel.path", tmp.toString(),
                "catalog.excel.header", "false",
                "catalog.excel.sampleRows", "5"));
        check(created instanceof ExcelConnector, "create 未返回 ExcelConnector");
        ExcelConnector conn = (ExcelConnector) created;
        assertEquals("excel_sales", conn.listCatalogs().get(0), "catalog 名");
        assertEquals(tmp, conn.root(), "根路径");
    }

    /** 用户明确的契约：创建 Excel connector 时全部默认使用一个分区读取。 */
    private static void s3SinglePartition(Path tmp) {
        ExcelConnector conn = new ExcelConnector("excel_sales", tmp, true);
        ReadRequest request = ReadRequest.of("excel_sales", "sales", "Orders");
        for (int parallelism : new int[]{1, 2, 4, 8, 64}) {
            List<ScanSlice> slices = conn.planSplits(request, parallelism);
            assertEquals(1, slices.size(), "parallelism=" + parallelism + " 时切片数必须恒为 1");
            check(!slices.get(0).hasSplit(), "单分区切片不应带私有载荷");
            assertEquals("sales", slices.get(0).request().schema(), "切片请求应原样携带 schema");
        }
        assertEquals(1, conn.planSplits(request, 8, 10).size(), "带 fetchLimit 时仍必须是 1 片");

        TableStats stats = conn.getTableStats("excel_sales", "sales", "Orders");
        assertEquals(1L, stats.splitCount(), "TableStats.splitCount 必须是 1（规划期就知道并行度上限）");
        assertEquals(-1L, stats.rowCount(), "rowCount 未知应为 -1（不能报 0 被当空表）");
        check(stats.byteSize() > 0, "byteSize 应取工作簿文件大小");
        check(!conn.supportsPredicatePushdown(), "Excel 不能声明谓词下推（否则 Filter 被消掉会多返回行）");
    }

    private static void s4Metadata(Path tmp) {
        ExcelConnector conn = new ExcelConnector("excel_sales", tmp, true);
        List<String> schemas = conn.listSchemas("excel_sales");
        check(schemas.contains("sales"), "listSchemas 应包含 sales，实际: " + schemas);
        check(schemas.contains("empty"), "listSchemas 应包含 empty，实际: " + schemas);
        check(conn.schemaExists("excel_sales", "sales"), "schemaExists(sales)");
        check(!conn.schemaExists("excel_sales", "nope"), "schemaExists(nope) 必须为 false");
        check(conn.getSchema("excel_sales", "sales").isPresent(), "getSchema(sales)");

        List<String> tables = conn.listTables("excel_sales", "sales");
        assertEquals(List.of("Orders", "Second"), tables, "listTables 按工作簿内顺序");
        check(conn.tableExists("excel_sales", "sales", "orders"), "表名应大小写不敏感");
        check(!conn.tableExists("excel_sales", "sales", "Nope"), "不存在的表");

        List<com.dojao.spi.bean.Column> columns = conn.getTableColumns("excel_sales", "sales", "Orders");
        List<String> names = new ArrayList<>();
        List<String> types = new ArrayList<>();
        for (com.dojao.spi.bean.Column c : columns) {
            names.add(c.name());
            types.add(c.dataType());
        }
        assertEquals(List.of("id", "amount", "name", "active", "created", "ship_date", "note"),
                names, "列名（表头）");
        assertEquals(List.of("BIGINT", "DOUBLE", "STRING", "BOOLEAN", "DATETIME", "DATETIME", "STRING"),
                types, "采样推断的列类型");

        Map<String, DojaoType> typeMap = conn.getColumnTypes("sales", "Orders");
        assertEquals(DojaoType.BIGINT, typeMap.get("id"), "id 类型");
        assertEquals(DojaoType.DOUBLE, typeMap.get("amount"), "amount 类型（整数+小数）");
        assertEquals(DojaoType.DATETIME, typeMap.get("ship_date"), "日期序列值应识别为 DATETIME");
        assertEquals(7, conn.sourceFields("excel_sales", "sales", "Orders", null).size(),
                "sourceFields 应覆盖全部列");
        assertEquals(2, conn.sourceFields("excel_sales", "sales", "Orders",
                List.of("name", "id")).size(), "sourceFields 按投影");
    }

    private static void s5ReadTypes(Path tmp) {
        ExcelConnector conn = new ExcelConnector("excel_sales", tmp, true);
        List<Page> pages = readAll(conn, ReadRequest.of("excel_sales", "sales", "Orders"));
        assertEquals(1, pages.size(), "3 行数据应只有 1 页");
        Page page = pages.get(0);
        assertEquals(3, page.getRowCount(), "空行应被跳过");

        BigIntVector id = page.getVector("id");
        Float8Vector amount = page.getVector("amount");
        VarCharVector name = page.getVector("name");
        BitVector active = page.getVector("active");
        TimeStampMilliVector created = page.getVector("created");
        TimeStampMilliVector ship = page.getVector("ship_date");
        VarCharVector note = page.getVector("note");

        assertEquals(1L, id.get(0), "id[0]");
        assertEquals(2L, id.get(1), "id[1]");
        assertEquals(3L, id.get(2), "id[2]");

        assertEquals(10.5d, amount.get(0), "amount[0]");
        assertEquals(3.0d, amount.get(1), "amount[1]（整数按 DOUBLE 列读）");
        check(amount.isNull(2), "amount[2] 缺失应为 NULL");

        assertEquals("Alice", text(name, 0), "共享字符串（多 run 拼接）");
        assertEquals("Bob", text(name, 1), "name[1]");
        check(name.isNull(2), "空共享字符串应为 NULL");

        assertEquals(1, active.get(0), "active[0]");
        assertEquals(0, active.get(1), "active[1]");
        assertEquals(1, active.get(2), "active[2]");

        long expectedCreated0 = LocalDateTime.parse("2026-10-21T10:30:00")
                .toInstant(ZoneOffset.UTC).toEpochMilli();
        assertEquals(expectedCreated0, created.get(0), "ISO 日期 t=d");
        assertEquals(LocalDateTime.parse("2026-10-22T00:00:00").toInstant(ZoneOffset.UTC).toEpochMilli(),
                created.get(1), "只有日期的 ISO 值");

        long expectedShip0 = LocalDateTime.parse("2026-10-21T12:00:00")
                .toInstant(ZoneOffset.UTC).toEpochMilli();
        assertEquals(expectedShip0, ship.get(0), "日期序列值 + numFmtId=14 应换算成时间");
        assertEquals(LocalDateTime.parse("2026-10-22T00:00:00").toInstant(ZoneOffset.UTC).toEpochMilli(),
                ship.get(1), "第二个日期序列值");

        assertEquals("hi", text(note, 1), "inline 富文本应拼接 <t> 并跳过 <rPh>");
        check(note.isNull(0), "note[0] 缺失应为 NULL");
        check(note.isNull(2), "错误值 t=e 应按 NULL 读（不把 #DIV/0! 当文本）");

        for (Page p : pages) p.close();
    }

    private static void s6MultiPage(Path tmp) {
        ExcelConnector conn = new ExcelConnector("excel_big", tmp, true);
        List<Page> pages = readAll(conn, ReadRequest.of("excel_big", "big", "S"));
        assertEquals(2, pages.size(), "9000 行 / 每页 8192 行 = 2 页");
        long total = 0;
        long first = Long.MIN_VALUE;
        long last = Long.MIN_VALUE;
        for (Page page : pages) {
            BigIntVector k = page.getVector("k");
            if (first == Long.MIN_VALUE) first = k.get(0);
            last = k.get(page.getRowCount() - 1);
            total += page.getRowCount();
        }
        assertEquals(9000L, total, "总行数");
        assertEquals(1L, first, "首页首行");
        assertEquals(9000L, last, "末页末行");
        for (Page p : pages) p.close();
    }

    private static void s7Projection(Path tmp) {
        ExcelConnector conn = new ExcelConnector("excel_sales", tmp, true);
        ReadRequest request = ReadRequest.of("excel_sales", "sales", "Orders",
                List.of("NAME", "id"));
        List<Page> pages = readAll(conn, request);
        assertEquals(1, pages.size(), "投影读取页数");
        Page page = pages.get(0);
        assertEquals(List.of("name", "id"),
                page.getSchema().getFields().stream()
                        .map(org.apache.arrow.vector.types.pojo.Field::getName).toList(),
                "输出列顺序按请求、名字用表头原名");
        assertEquals("Alice", text(page.getVector("name"), 0), "投影值");
        assertEquals(1L, ((BigIntVector) page.getVector("id")).get(0), "投影 id");
        for (Page p : pages) p.close();

        int before = pass;
        assertThrows(IllegalArgumentException.class,
                () -> readAll(conn, ReadRequest.of("excel_sales", "sales", "Orders",
                        List.of("nope"))), "未知列必须 fail-fast");
        check(pass == before + 1, "未知列错误分支");
    }

    private static void s8OffsetLimit(Path tmp) {
        ExcelConnector conn = new ExcelConnector("excel_sales", tmp, true);
        List<Page> pages = readAll(conn, ReadRequest.of("excel_sales", "sales", "Orders", 1, 1));
        assertEquals(1, pages.size(), "offset=1 limit=1 → 1 页");
        assertEquals(1, pages.get(0).getRowCount(), "limit=1 → 1 行");
        assertEquals(2L, ((BigIntVector) pages.get(0).getVector("id")).get(0),
                "offset 跳过的应是数据行（空行不计数）");
        for (Page p : pages) p.close();

        List<Page> beyond = readAll(conn, ReadRequest.of("excel_sales", "sales", "Orders", 100, 10));
        check(beyond.isEmpty(), "offset 越过末尾应直接 EOF");

        List<Page> all = readAll(conn, ReadRequest.of("excel_sales", "sales", "Orders", 2, 0));
        assertEquals(1, all.get(0).getRowCount(), "offset=2 只剩 1 行");
        for (Page p : all) p.close();
    }

    private static void s9NoHeader(Path tmp) {
        Path file = tmp.resolve("raw.xlsx");
        ExcelConnector conn = new ExcelConnector("excel_raw", file, false);
        assertEquals(List.of("raw"), conn.listSchemas("excel_raw"), "单文件根 → schema 即文件名");
        assertEquals(List.of("Data"), conn.listTables("excel_raw", "raw"), "工作表清单");
        List<com.dojao.spi.bean.Column> cols = conn.getTableColumns("excel_raw", "raw", "Data");
        assertEquals(List.of("col1", "col2"),
                cols.stream().map(com.dojao.spi.bean.Column::name).toList(), "无表头列名");
        assertEquals(DojaoType.BIGINT, conn.getColumnTypes("raw", "Data").get("col1"), "col1 类型");

        List<Page> pages = readAll(conn, ReadRequest.of("excel_raw", "raw", "Data"));
        assertEquals(2, pages.get(0).getRowCount(), "header=false 时首行也是数据");
        assertEquals(1L, ((BigIntVector) pages.get(0).getVector("col1")).get(0), "首行值");
        assertEquals("x", text(pages.get(0).getVector("col2"), 0), "首行第二列");
        for (Page p : pages) p.close();
    }

    private static void s10SingleFile(Path tmp) {
        ExcelConnector conn = new ExcelConnector("excel_one", tmp.resolve("sales.xlsx"), true);
        assertEquals(List.of("sales"), conn.listSchemas("excel_one"), "文件根 → 一个 schema");
        List<Page> pages = readAll(conn, ReadRequest.of("excel_one", "sales", "Orders"));
        assertEquals(3, pages.get(0).getRowCount(), "文件根读取");
        for (Page p : pages) p.close();
        assertEquals(1, conn.planSplits(ReadRequest.of("excel_one", "sales", "Orders"), 16).size(),
                "文件根同样单分区");
    }

    private static void s11EmptySheets(Path tmp) {
        ExcelConnector conn = new ExcelConnector("excel_empty", tmp.resolve("empty.xlsx"), true);
        assertEquals(List.of("Empty", "HeaderOnly"), conn.listTables("excel_empty", "empty"),
                "空工作表也应出现在清单里");
        check(conn.getTableColumns("excel_empty", "empty", "Empty").isEmpty(), "空工作表没有列");
        check(readAll(conn, ReadRequest.of("excel_empty", "empty", "Empty")).isEmpty(), "空工作表直接 EOF");
        assertEquals(1, conn.getTableColumns("excel_empty", "empty", "HeaderOnly").size(),
                "只有表头也有列定义");
        check(readAll(conn, ReadRequest.of("excel_empty", "empty", "HeaderOnly")).isEmpty(),
                "只有表头 → 无数据行");
    }

    private static void s12DateFormats() {
        check(XlsxWorkbook.isDateFormatCode("yyyy-mm-dd"), "yyyy-mm-dd 应为日期格式");
        check(XlsxWorkbook.isDateFormatCode("yyyy\"年\"m\"月\"d\"日\""), "中文日期格式");
        check(XlsxWorkbook.isDateFormatCode("h:mm AM/PM"), "时间格式");
        check(XlsxWorkbook.isDateFormatCode("[h]:mm:ss"), "经过时间格式");
        check(XlsxWorkbook.isDateFormatCode("yyyy-mm-dd hh:mm:ss.000"), "带毫秒的时间格式");
        check(!XlsxWorkbook.isDateFormatCode("General"), "General 不是日期");
        check(!XlsxWorkbook.isDateFormatCode("Standard"), "Standard 不是日期（按整段字母判定）");
        check(!XlsxWorkbook.isDateFormatCode("0.00"), "0.00 不是日期");
        check(!XlsxWorkbook.isDateFormatCode("#,##0.00"), "千分位不是日期");
        check(!XlsxWorkbook.isDateFormatCode("0.00E+00"), "科学计数不是日期");
        check(!XlsxWorkbook.isDateFormatCode("#,##0;[Red]-#,##0"), "条件段不是日期");
        check(!XlsxWorkbook.isDateFormatCode("\"日期\"0.00"), "引号字面量里的日期字样不算");
        check(!XlsxWorkbook.isDateFormatCode(null), "null 不是日期");
    }

    private static void s13ColumnIndex() {
        assertEquals(0, XlsxSheetStream.columnIndex("A1"), "A1");
        assertEquals(25, XlsxSheetStream.columnIndex("Z9"), "Z9");
        assertEquals(26, XlsxSheetStream.columnIndex("AA3"), "AA3");
        assertEquals(1, XlsxSheetStream.columnIndex("$B$2"), "$B$2");
        assertEquals(27, XlsxSheetStream.columnIndex("ab"), "小写列");
        assertEquals(-1, XlsxSheetStream.columnIndex(null), "null");
        assertEquals(-1, XlsxSheetStream.columnIndex("12"), "纯数字");
    }

    private static void s14Serial() {
        assertEquals("2026-10-21T00:00", XlsxSheetStream.excelSerialToIso("46316", false), "1900 基准");
        assertEquals("2026-10-21T12:00", XlsxSheetStream.excelSerialToIso("46316.5", false), "带时刻");
        assertEquals("1900-01-01T00:00", XlsxSheetStream.excelSerialToIso("2", false), "最早有效日期");
        assertEquals("1899-12-30T00:00", XlsxSheetStream.excelSerialToIso("0", false), "序列 0");
        assertEquals("1904-01-01T00:00", XlsxSheetStream.excelSerialToIso("0", true), "1904 日期系统");
        assertEquals(null, XlsxSheetStream.excelSerialToIso("-1", false), "负数不解释成日期");
        assertEquals(null, XlsxSheetStream.excelSerialToIso("abc", false), "非数字");
        assertEquals(null, XlsxSheetStream.excelSerialToIso("NaN", false), "NaN");
    }

    private static void s15Probe(Path tmp) {
        ExcelConnector ok = new ExcelConnector("excel_sales", tmp, true);
        ok.probeConnectivity("excel_sales");
        pass++;

        Path missing = tmp.resolve("does-not-exist");
        ExcelConnector bad = new ExcelConnector("excel_missing", missing, true);
        assertThrows(RuntimeException.class, () -> bad.probeConnectivity("excel_missing"),
                "不存在的路径必须 probe 失败");

        Path emptyDir;
        try {
            emptyDir = Files.createDirectory(tmp.resolve("emptydir"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        ExcelConnector empty = new ExcelConnector("excel_empty_dir", emptyDir, true);
        assertThrows(RuntimeException.class, () -> empty.probeConnectivity("excel_empty_dir"),
                "目录下没有工作簿必须 probe 失败");
    }

    private static void s16Errors(Path tmp) {
        ExcelConnector conn = new ExcelConnector("excel_sales", tmp, true);
        assertThrows(UnsupportedOperationException.class,
                () -> conn.write(null), "Excel 只读");
        assertThrows(IllegalArgumentException.class,
                () -> readAll(conn, ReadRequest.of("excel_sales", "nope", "Orders")),
                "不存在的 schema");
        assertThrows(RuntimeException.class,
                () -> conn.openSplit(ReadRequest.of("excel_sales", "sales", "Missing"), Split.WHOLE),
                "不存在的工作表");
        int before = pass;
        try {
            conn.openSplit(ReadRequest.of("excel_sales", "sales", "Orders"), Split.of(new byte[]{1, 2}));
            fail("带载荷的 Split 必须被拒绝");
        } catch (UnsupportedOperationException expected) {
            pass++;
        }
        check(pass == before + 1, "载荷拒绝分支");
    }

    // ==================== 读取工具 ====================

    private static List<Page> readAll(ExcelConnector conn, ReadRequest request) {
        List<Page> pages = new ArrayList<>();
        try (ScanReader reader = conn.openSplit(request, Split.WHOLE)) {
            while (true) {
                Page page = reader.next();
                if (page.isEOF()) break;
                pages.add(page);
            }
        }
        return pages;
    }

    private static String text(FieldVector vector, int index) {
        VarCharVector v = (VarCharVector) vector;
        if (v.isNull(index)) return null;
        return new String(v.get(index), StandardCharsets.UTF_8);
    }

    // ==================== 夹具：手写最小 OOXML ====================

    private static void buildFixtures(Path dir) throws IOException {
        // sales.xlsx：两页 —— Orders（类型齐全）+ Second（用于 listTables 顺序）
        Sheet orders = new Sheet("Orders")
                .row(s("id"), s("amount"), s("name"), s("active"), s("created"),
                        s("ship_date"), s("note"))
                .row(num("1"), num("10.5"), shared(0), bool(true), iso("2026-10-21T10:30:00"),
                        styledNum("46316.5"), null)
                .row(num("2"), num("3"), shared(1), bool(false), iso("2026-10-22"),
                        styledNum("46317"), inlineRich("h", "i"))
                .blank()
                .row(num("3"), null, shared(2), bool(true), iso("2026-10-23T08:15:00"),
                        null, error("#DIV/0!"));
        Sheet second = new Sheet("Second")
                .row(s("k"))
                .row(num("42"));
        writeWorkbook(dir.resolve("sales.xlsx"), List.of(orders, second), true,
                List.of(rich("A", "lice"), "Bob"));

        // big.xlsx：9000 行验证跨页
        Sheet big = new Sheet("S").row(s("k"));
        for (int i = 1; i <= 9000; i++) big.row(num(Integer.toString(i)));
        writeWorkbook(dir.resolve("big.xlsx"), List.of(big), true, List.of());

        // raw.xlsx：无表头
        Sheet data = new Sheet("Data")
                .row(num("1"), inline("x"))
                .row(num("2"), inline("y"));
        writeWorkbook(dir.resolve("raw.xlsx"), List.of(data), true, List.of());

        // empty.xlsx：空工作表 + 只有表头
        Sheet empty = new Sheet("Empty");
        Sheet headerOnly = new Sheet("HeaderOnly").row(s("c"));
        writeWorkbook(dir.resolve("empty.xlsx"), List.of(empty, headerOnly), true, List.of());
    }

    private static final class Sheet {
        private final String name;
        private final StringBuilder xml = new StringBuilder();
        private int rowNo;

        Sheet(String name) {
            this.name = name;
        }

        Sheet row(String... cells) {
            rowNo++;
            xml.append("<row r=\"").append(rowNo).append("\">");
            for (int i = 0; i < cells.length; i++) {
                if (cells[i] == null) continue;    // 缺单元格 → 坐标递推由 <c> 的 r 提供
                xml.append(cells[i].replace("$REF$", column(i) + rowNo));
            }
            xml.append("</row>");
            return this;
        }

        Sheet blank() {
            rowNo++;
            xml.append("<row r=\"").append(rowNo).append("\"/>");
            return this;
        }

        String xmlData() {
            return "<worksheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\">"
                    + "<sheetData>" + xml + "</sheetData></worksheet>";
        }
    }

    private static String column(int index) {
        StringBuilder sb = new StringBuilder();
        int n = index + 1;
        while (n > 0) {
            int rem = (n - 1) % 26;
            sb.insert(0, (char) ('A' + rem));
            n = (n - 1) / 26;
        }
        return sb.toString();
    }

    /** 共享字符串单元格（值在 sharedStrings.xml）。 */
    private static String shared(int index) {
        return "<c r=\"$REF$\" t=\"s\"><v>" + index + "</v></c>";
    }

    /** 内联字符串单元格。 */
    private static String inline(String text) {
        return "<c r=\"$REF$\" t=\"inlineStr\"><is><t>" + esc(text) + "</t></is></c>";
    }

    /** 内联富文本：多个 run 拼接（并带一段拼音注音 rPh，必须被跳过）。 */
    private static String inlineRich(String first, String second) {
        return "<c r=\"$REF$\" t=\"inlineStr\"><is>"
                + "<r><t>" + esc(first) + "</t></r>"
                + "<rPh><t>かな</t></rPh>"
                + "<r><t>" + esc(second) + "</t></r></is></c>";
    }

    private static String num(String raw) {
        return "<c r=\"$REF$\"><v>" + raw + "</v></c>";
    }

    /** 带样式 1（numFmtId=14，日期）的数值单元格。 */
    private static String styledNum(String raw) {
        return "<c r=\"$REF$\" s=\"1\"><v>" + raw + "</v></c>";
    }

    private static String bool(boolean value) {
        return "<c r=\"$REF$\" t=\"b\"><v>" + (value ? "1" : "0") + "</v></c>";
    }

    private static String iso(String value) {
        return "<c r=\"$REF$\" t=\"d\"><v>" + esc(value) + "</v></c>";
    }

    private static String error(String value) {
        return "<c r=\"$REF$\" t=\"e\"><v>" + esc(value) + "</v></c>";
    }

    /** 表头用：共享字符串 / 直接内联二选一 —— 这里用共享字符串，顺带覆盖共享串路径。 */
    private static String s(String text) {
        return "<c r=\"$REF$\" t=\"inlineStr\"><is><t>" + esc(text) + "</t></is></c>";
    }

    private static String rich(String first, String second) {
        return first + second;
    }

    private static void writeWorkbook(Path file, List<Sheet> sheets, boolean withStyles,
                                      List<String> sharedStrings) throws IOException {
        try (OutputStream raw = Files.newOutputStream(file);
             ZipOutputStream zip = new ZipOutputStream(raw, StandardCharsets.UTF_8)) {
            put(zip, "[Content_Types].xml", contentTypes(sheets.size(),
                    !sharedStrings.isEmpty(), withStyles));

            StringBuilder wb = new StringBuilder();
            wb.append("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>")
                    .append("<workbook xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\"")
                    .append(" xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\">")
                    .append("<sheets>");
            StringBuilder rels = new StringBuilder();
            rels.append("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>")
                    .append("<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">");
            for (int i = 0; i < sheets.size(); i++) {
                int idx = i + 1;
                wb.append("<sheet name=\"").append(esc(sheets.get(i).name))
                        .append("\" sheetId=\"").append(idx)
                        .append("\" r:id=\"rId").append(idx).append("\"/>");
                rels.append("<Relationship Id=\"rId").append(idx)
                        .append("\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet\"")
                        .append(" Target=\"worksheets/sheet").append(idx).append(".xml\"/>");
            }
            wb.append("</sheets></workbook>");
            rels.append("</Relationships>");
            put(zip, "xl/workbook.xml", wb.toString());
            put(zip, "xl/_rels/workbook.xml.rels", rels.toString());

            put(zip, "xl/styles.xml",
                    "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
                            + "<styleSheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\">"
                            + "<cellXfs count=\"2\">"
                            + "<xf numFmtId=\"0\" xfId=\"0\"/>"
                            + "<xf numFmtId=\"14\" xfId=\"0\" applyNumberFormat=\"1\"/>"
                            + "</cellXfs></styleSheet>");

            if (!sharedStrings.isEmpty()) {
                StringBuilder ss = new StringBuilder();
                ss.append("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>")
                        .append("<sst xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\"")
                        .append(" count=\"").append(sharedStrings.size())
                        .append("\" uniqueCount=\"").append(sharedStrings.size()).append("\">");
                // 第 0 条是富文本（多 run），第 1 条普通文本，第 2 条空串
                ss.append("<si><r><t>").append(esc("A")).append("</t></r>")
                        .append("<r><t>").append(esc("lice")).append("</t></r></si>");
                for (int i = 1; i < sharedStrings.size(); i++) {
                    ss.append("<si><t>").append(esc(sharedStrings.get(i))).append("</t></si>");
                }
                ss.append("<si><t></t></si>");
                ss.append("</sst>");
                put(zip, "xl/sharedStrings.xml", ss.toString());
            }

            for (int i = 0; i < sheets.size(); i++) {
                put(zip, "xl/worksheets/sheet" + (i + 1) + ".xml", sheets.get(i).xmlData());
            }
        }
    }

    private static String contentTypes(int sheets, boolean sharedStrings, boolean styles) {
        StringBuilder sb = new StringBuilder();
        sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>")
                .append("<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\">")
                .append("<Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/>")
                .append("<Default Extension=\"xml\" ContentType=\"application/xml\"/>")
                .append("<Override PartName=\"/xl/workbook.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml\"/>");
        for (int i = 0; i < sheets; i++) {
            sb.append("<Override PartName=\"/xl/worksheets/sheet").append(i + 1)
                    .append(".xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/>");
        }
        if (sharedStrings) {
            sb.append("<Override PartName=\"/xl/sharedStrings.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml\"/>");
        }
        if (styles) {
            sb.append("<Override PartName=\"/xl/styles.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml\"/>");
        }
        sb.append("</Types>");
        return sb.toString();
    }

    private static void put(ZipOutputStream zip, String name, String content) throws IOException {
        zip.putNextEntry(new ZipEntry(name));
        zip.write(content.getBytes(StandardCharsets.UTF_8));
        zip.closeEntry();
    }

    private static String esc(String text) {
        return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                .replace("\"", "&quot;");
    }

    private static void deleteRecursively(Path dir) {
        if (dir == null || !Files.exists(dir)) return;
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                    // 临时目录清理失败不影响探针结论
                }
            });
        } catch (IOException ignored) {
            // 同上
        }
    }

    // ==================== 断言 ====================

    private interface Body {
        void run() throws Exception;
    }

    private static void scenario(String name, Body body) {
        int before = fail;
        try {
            body.run();
        } catch (Throwable t) {
            fail++;
            System.out.println("  [FAIL] " + name + " → " + t);
            t.printStackTrace(System.out);
            return;
        }
        System.out.println((fail == before ? "  [ OK ] " : "  [FAIL] ") + name);
    }

    private static void check(boolean condition, String message) {
        if (condition) {
            pass++;
            return;
        }
        fail++;
        System.out.println("  [FAIL] " + message);
    }

    private static void assertEquals(Object expected, Object actual, String what) {
        if (expected == null ? actual == null : expected.equals(actual)) {
            pass++;
            return;
        }
        fail++;
        System.out.println("  [FAIL] " + what + " 期望=" + expected + " 实际=" + actual);
    }

    private static void assertEquals(long expected, long actual, String what) {
        if (expected == actual) {
            pass++;
            return;
        }
        fail++;
        System.out.println("  [FAIL] " + what + " 期望=" + expected + " 实际=" + actual);
    }

    private static void assertEquals(double expected, double actual, String what) {
        if (Double.compare(expected, actual) == 0) {
            pass++;
            return;
        }
        fail++;
        System.out.println("  [FAIL] " + what + " 期望=" + expected + " 实际=" + actual);
    }

    private static void assertThrows(Class<? extends Throwable> type, Body body, String what) {
        try {
            body.run();
        } catch (Throwable t) {
            if (type.isInstance(t)) {
                pass++;
                return;
            }
            fail++;
            System.out.println("  [FAIL] " + what + " 抛出了 " + t.getClass().getSimpleName()
                    + " 而不是 " + type.getSimpleName() + ": " + t.getMessage());
            return;
        }
        fail++;
        System.out.println("  [FAIL] " + what + " 未抛异常（期望 " + type.getSimpleName() + "）");
    }

    private static void fail(String message) {
        fail++;
        System.out.println("  [FAIL] " + message);
    }
}
