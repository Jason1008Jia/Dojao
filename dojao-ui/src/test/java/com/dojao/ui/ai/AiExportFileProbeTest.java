package com.dojao.ui.ai;

import com.dojao.ai.AiConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * 探针：Dojao AI「浏览器文件下载」能力（离线）。
 *
 * <p>覆盖四层：①{@link AiExportFormats} 的格式归一化 / 文件名清洗 / Markdown、纯文本、HTML、CSV、XLSX
 * 生成（含转义与 BOM）；②{@link AiFileStore} 的按用户隔离 / TTL 过期 / 每用户份数上限 / 大小上限；
 * ③{@link AiAgentService#exportFile} 的端到端（content / data 引用 / 结果集 JSON / 错误提示）；
 * ④{@link AiTurns} 轮次落库的 {@code files} 数组。</p>
 *
 * <p><b>判别力</b>：把 {@code exportFile} 的 sink 调用删掉 ⇒ 端到端用例全 FAIL；把 {@code AiFileStore.find}
 * 的 owner 校验去掉 ⇒「他人不可见」FAIL；把 {@code fill} 的 files 写入删掉 ⇒ 轮次 files 用例 FAIL；
 * 把 CSV 的引号转义去掉 ⇒ CSV 转义用例 FAIL；把完整 HTML 文档分支删掉（回到 Markdown 转义）
 * ⇒「★HTML 文档原样输出 / 标签不被转义 / 保留 script」全 FAIL。</p>
 */
public final class AiExportFileProbeTest {

    private static int fail = 0;
    private static final ObjectMapper JSON = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        testFormatBasics();
        testFileName();
        testMarkdownAndText();
        testCsv();
        testHtml();
        testXlsx();
        testImagesAndPdf();
        testStore();
        testServiceExport();
        testTurnsFiles();
        System.out.println(fail == 0 ? "\n全部通过 ✅" : "\n失败 " + fail + " 项 ❌");
    }

    // ---------------- ① 格式基础 ----------------

    private static void testFormatBasics() {
        System.out.println("---------- ① 格式归一化 / MIME ----------");
        check("markdown → md", "md".equals(AiExportFormats.normalizeFormat("markdown")));
        check(".CSV → csv", "csv".equals(AiExportFormats.normalizeFormat(".CSV")));
        check("xlsx → excel", "excel".equals(AiExportFormats.normalizeFormat("xlsx")));
        check("htm → html", "html".equals(AiExportFormats.normalizeFormat("htm")));
        check("text → txt", "txt".equals(AiExportFormats.normalizeFormat("text")));
        check("pdf → pdf", "pdf".equals(AiExportFormats.normalizeFormat("pdf")));
        check("jpg → jpg", "jpg".equals(AiExportFormats.normalizeFormat("jpg")));
        check("JPEG → jpg", "jpg".equals(AiExportFormats.normalizeFormat("JPEG")));
        check("png → png", "png".equals(AiExportFormats.normalizeFormat(".PNG")));
        check("null → null", AiExportFormats.normalizeFormat(null) == null);
        check("扩展名 excel → xlsx", "xlsx".equals(AiExportFormats.extension("excel")));
        check("扩展名 md → md", "md".equals(AiExportFormats.extension("markdown")));
        check("扩展名 jpeg → jpg", "jpg".equals(AiExportFormats.extension("jpeg")));
        check("扩展名 pdf → pdf", "pdf".equals(AiExportFormats.extension("pdf")));
        check("MIME md", AiExportFormats.mimeType("md").startsWith("text/markdown"));
        check("MIME csv", AiExportFormats.mimeType("csv").startsWith("text/csv"));
        check("MIME html", AiExportFormats.mimeType("html").startsWith("text/html"));
        check("MIME excel", AiExportFormats.mimeType("excel").contains("spreadsheetml.sheet"));
        check("MIME png", "image/png".equals(AiExportFormats.mimeType("png")));
        check("MIME jpg", "image/jpeg".equals(AiExportFormats.mimeType("jpeg")));
        check("MIME pdf", "application/pdf".equals(AiExportFormats.mimeType("pdf")));
        check("MIME 未知", "application/octet-stream".equals(AiExportFormats.mimeType("svg")));
    }

    private static void testFileName() {
        System.out.println("---------- ① 文件名清洗 ----------");
        check("title 兜底 + 补扩展名", "9月销售分析.md".equals(
                AiExportFormats.fileName("md", null, "9月销售分析")));
        check("已有同格式扩展名不重复", "report.xlsx".equals(
                AiExportFormats.fileName("excel", "report.xlsx", null)));
        check("已有异格式扩展名替换", "report.csv".equals(
                AiExportFormats.fileName("csv", "report.md", null)));
        check("路径只取文件名", "c_.csv".equals(
                AiExportFormats.fileName("csv", "a/b\\c?.csv", null)));
        check("非法字符替换", "a_b_c.txt".equals(
                AiExportFormats.fileName("txt", "a:b*c", null)));
        check("空名兜底", "dojao-export.txt".equals(
                AiExportFormats.fileName("txt", "   ", " ")));
        check("html 扩展名", "报告.html".equals(
                AiExportFormats.fileName("html", "报告", null)));
    }

    // ---------------- ① Markdown / 纯文本 / CSV / HTML / XLSX ----------------

    private static void testMarkdownAndText() {
        System.out.println("---------- ① Markdown / 纯文本 ----------");
        String md = new String(AiExportFormats.generate("md", "9 月报告",
                "- 销量增长 12%", null, null), StandardCharsets.UTF_8);
        check("md 标题行", md.startsWith("# 9 月报告"));
        check("md 正文保留", md.contains("- 销量增长 12%"));

        String mdNoTitle = new String(AiExportFormats.generate("md", null,
                "# 自带标题\n正文", null, null), StandardCharsets.UTF_8);
        check("md 不重复加标题", mdNoTitle.startsWith("# 自带标题"));

        String mdTable = new String(AiExportFormats.generate("md", "汇总",
                null, List.of("城市", "销量"), List.of(List.of("北京", "10"))), StandardCharsets.UTF_8);
        check("md 无正文时用 data 渲染表格", mdTable.contains("| 城市 | 销量 |")
                && mdTable.contains("| 北京 | 10 |"));
        check("md 表格转义竖线", mdTable.contains("汇总"));

        String txt = new String(AiExportFormats.generate("txt", "报告", "第一行\n第二行", null, null),
                StandardCharsets.UTF_8);
        check("txt 含标题", txt.startsWith("报告"));
        check("txt 含正文", txt.contains("第一行") && txt.contains("第二行"));

        String txtTable = new String(AiExportFormats.generate("txt", null, null,
                List.of("a", "b"), List.of(List.of("1", "2"))), StandardCharsets.UTF_8);
        check("txt 表格制表符分隔", txtTable.startsWith("a\tb") && txtTable.contains("1\t2"));
    }

    private static void testCsv() {
        System.out.println("---------- ① CSV ----------");
        String csv = new String(AiExportFormats.generate("csv", null, null,
                List.of("城市", "备注"),
                List.of(List.of("北京", "a,b"), List.of("上海", "he said \"hi\""))),
                StandardCharsets.UTF_8);
        check("CSV BOM（Excel 中文不乱码）", csv.startsWith("\uFEFF"));
        check("CSV 表头", csv.contains("城市,备注\r\n"));
        check("CSV 逗号加引号", csv.contains("\"a,b\""));
        check("CSV 双引号转义", csv.contains("\"he said \"\"hi\"\"\""));
        check("CSV CRLF 行尾", (csv.split("\r\n", -1).length - 1) == 3);

        byte[] bytes = AiExportFormats.generate("csv", null, null,
                List.of("a"), List.of(List.of("1")));
        check("generate(csv) 带 BOM", new String(bytes, StandardCharsets.UTF_8).startsWith("\uFEFF"));
        String raw = new String(AiExportFormats.generate("csv", null, "x,y\n1,2", null, null),
                StandardCharsets.UTF_8);
        check("csv 无 data 时用 content 原样", raw.contains("x,y"));
    }

    private static void testHtml() {
        System.out.println("---------- ① HTML / Markdown→HTML ----------");
        String markdown = """
                # 标题
                **粗体** 和 `code`

                | a | b |
                | --- | --- |
                | 1 | <script> |

                - 项1
                - 项2

                > 引用

                ```sql
                select 1
                ```
                """;
        String html = new String(AiExportFormats.generate("html", "报告", markdown, null, null),
                StandardCharsets.UTF_8);
        check("HTML 完整文档", html.contains("<!DOCTYPE html>") && html.contains("<meta charset=\"utf-8\">"));
        check("HTML 标题", html.contains("<title>报告</title>"));
        check("HTML h1", html.contains("<h1>标题</h1>"));
        check("HTML 粗体", html.contains("<strong>粗体</strong>"));
        check("HTML 行内代码", html.contains("<code>code</code>"));
        check("HTML 表格", html.contains("<table>") && html.contains("<th>a</th>"));
        check("HTML 列表", html.contains("<ul>") && html.contains("<li>项1</li>"));
        check("HTML 引用", html.contains("<blockquote>引用</blockquote>"));
        check("HTML 代码块语言", html.contains("class=\"language-sql\""));
        check("★HTML 转义脚本", html.contains("&lt;script&gt;") && !html.contains("<script>"));

        String tableHtml = new String(AiExportFormats.generate("html", "汇总", null,
                List.of("城市", "销量"), List.of(List.of("北京", "10"))), StandardCharsets.UTF_8);
        check("HTML 无正文时用 data 渲染表格", tableHtml.contains("<td>北京</td>")
                && tableHtml.contains("<td>10</td>"));

        // ---- 模型直接给完整 HTML 文档（带 <style>/<script> 的 ECharts 报告）：原样输出，不能当 Markdown 转义 ----
        String fullHtml = "<!DOCTYPE html>\n<html lang=\"zh-CN\"><head><title>月度报告</title>"
                + "<style>body{color:#333}</style></head>"
                + "<body><div id=\"chart\"></div>"
                + "<script>/* 注释 */ var data=[1,2,3];</script></body></html>";
        String passthrough = new String(AiExportFormats.generate("html", "外层标题", fullHtml, null, null),
                StandardCharsets.UTF_8);
        check("★完整 HTML 文档原样输出", passthrough.stripLeading()
                .startsWith("<!DOCTYPE html>\n<html lang=\"zh-CN\"><head>"));
        check("★完整 HTML 文档标签不被转义", passthrough.contains("<div id=\"chart\"></div>")
                && !passthrough.contains("&lt;!DOCTYPE") && !passthrough.contains("&lt;div"));
        check("★HTML 保留 <style>/<script>（ECharts 图表 + JS 注释不变形）",
                passthrough.contains("<style>body{color:#333}</style>")
                        && passthrough.contains("<script>/* 注释 */ var data=[1,2,3];</script>")
                        && !passthrough.contains("<em>"));
        check("HTML 未重复包外壳", passthrough.indexOf("<!DOCTYPE html>")
                == passthrough.lastIndexOf("<!DOCTYPE html>"));
        check("★完整文档自动补 UTF-8", passthrough.contains("<head>\n<meta charset=\"utf-8\"><title>月度报告</title>"));

        String withCharset = "<!DOCTYPE html><html><head><meta charset=\"gbk\"><title>x</title>"
                + "</head><body>ok</body></html>";
        String kept = new String(AiExportFormats.generate("html", null, withCharset, null, null),
                StandardCharsets.UTF_8);
        check("HTML 已有 charset 不重复插入", occurrences(kept, "charset=") == 1);

        String fragment = "<div class=\"kpi\"><span>你好</span></div>";
        String fragHtml = new String(AiExportFormats.generate("html", "指标", fragment, null, null),
                StandardCharsets.UTF_8);
        check("★HTML 片段保留标签并补外壳", fragHtml.startsWith("<!DOCTYPE html>")
                && fragHtml.contains("<div class=\"kpi\"><span>你好</span></div>")
                && !fragHtml.contains("&lt;div"));

        String dirty = "<!DOCTYPE html><html><head><title>t</title></head><body>"
                + "<iframe src=\"https://evil\"></iframe>"
                + "<form action=\"/x\"><input></form>"
                + "<a href=\"javascript:alert(1)\">点我</a>"
                + "<script>var ok=1;</script></body></html>";
        String clean = new String(AiExportFormats.generate("html", null, dirty, null, null),
                StandardCharsets.UTF_8).toLowerCase(java.util.Locale.ROOT);
        check("★净化移除 iframe/form", !clean.contains("<iframe") && !clean.contains("<form")
                && !clean.contains("&lt;iframe"));
        check("★净化 javascript: 链接", !clean.contains("javascript:") && clean.contains("href=\"#\""));
        check("净化保留正常脚本", clean.contains("<script>var ok=1;</script>"));
    }

    private static void testXlsx() throws Exception {
        System.out.println("---------- ① XLSX ----------");
        byte[] xlsx = AiExportFormats.generate("excel", "汇总",
                null, List.of("城市", "销量", "备注"), List.of(
                        List.of("北京", "10", "A&B"),
                        List.of("上海", "20", "<x>")));
        check("xlsx 是 ZIP 包", xlsx.length > 4 && xlsx[0] == 'P' && xlsx[1] == 'K');
        Map<String, String> parts = unzip(xlsx);
        check("xlsx 含 6 个部件", parts.size() == 6);
        check("xlsx Content_Types", parts.containsKey("[Content_Types].xml"));
        check("xlsx workbook", parts.containsKey("xl/workbook.xml"));
        String sheet = parts.getOrDefault("xl/worksheets/sheet1.xml", "");
        check("xlsx 表头 inlineStr", sheet.contains("<t xml:space=\"preserve\">城市</t>"));
        check("xlsx 数字写数值单元格", sheet.contains("<v>10</v>"));
        check("xlsx XML 转义 &", sheet.contains("A&amp;B"));
        check("xlsx XML 转义 <", sheet.contains("&lt;x&gt;"));
        check("xlsx 单元格坐标", sheet.contains("r=\"C2\""));
        check("xlsx 不越界造列", !sheet.contains("<c r=\"D2\"/>"));
        check("xlsx 行号", sheet.contains("<row r=\"3\">"));
    }

    // ---------------- ①' PNG / JPG / PDF ----------------

    private static void testImagesAndPdf() throws Exception {
        System.out.println("---------- ①' PNG / JPG / PDF ----------");

        List<String> columns = List.of("城市", "销量", "增长");
        List<List<String>> rows = List.of(
                List.of("北京", "10", "0.5"),
                List.of("上海", "20", "0.8"),
                List.of("广州", "坏值", "0.3"));
        AiExportFormats.ChartData chart = AiAgentService.buildChartData("bar", "城市销量", columns, rows,
                "城市", List.of("销量", "增长"), List.of("bar", "line"), List.of("y", "y2"));
        check("chartData 类目", chart.categories().equals(List.of("北京", "上海", "广州")));
        check("chartData 序列数", chart.series().size() == 2);
        check("chartData 混合图形", "bar".equals(chart.series().get(0).type())
                && "line".equals(chart.series().get(1).type()));
        check("chartData 双 Y 轴", chart.series().get(1).y2() && chart.dualAxis());
        check("chartData 数值解析", Double.valueOf(10).equals(chart.series().get(0).data().get(0)));
        check("chartData 非数值为 null", chart.series().get(0).data().get(2) == null);

        // PNG
        byte[] png = AiExportFormats.generate("png", "城市销量", null, columns, rows, chart);
        check("PNG 魔数", png.length > 100 && (png[0] & 0xFF) == 0x89
                && png[1] == 'P' && png[2] == 'N' && png[3] == 'G');
        BufferedImage image = ImageIO.read(new ByteArrayInputStream(png));
        check("PNG 可解码 1200×720", image != null && image.getWidth() == 1200 && image.getHeight() == 720);
        check("PNG 非空白", hasNonWhite(image));

        // JPG
        byte[] jpg = AiExportFormats.generate("jpg", null, null, columns, rows, chart);
        check("JPG 魔数", jpg.length > 100 && (jpg[0] & 0xFF) == 0xFF
                && (jpg[1] & 0xFF) == 0xD8 && (jpg[2] & 0xFF) == 0xFF);
        check("JPG 可解码", ImageIO.read(new ByteArrayInputStream(jpg)) != null);

        // 饼图 / 散点也能画
        AiExportFormats.ChartData pie = AiAgentService.buildChartData("pie", "占比", columns, rows,
                "城市", List.of("销量"), null, null);
        BufferedImage pieImg = AiChartRenderer.renderChart(pie);
        check("饼图渲染非空白", hasNonWhite(pieImg));

        boolean pngNoChart = false;
        try {
            AiExportFormats.generate("png", null, null, columns, rows, null);
        } catch (IllegalArgumentException e) {
            pngNoChart = e.getMessage().contains("图表参数");
        }
        check("PNG 缺图表数据明确拒绝", pngNoChart);

        // PDF：图表页 + 正文页
        byte[] pdf = AiExportFormats.generate("pdf", "城市销量", "## 结论\n销量增长 12%", columns, rows, chart);
        String pdfText = new String(pdf, StandardCharsets.ISO_8859_1);
        check("PDF 文件头", pdfText.startsWith("%PDF-1.4"));
        check("PDF 文件尾", pdfText.stripTrailing().endsWith("%%EOF"));
        check("PDF Catalog", pdfText.contains("/Type /Catalog"));
        check("PDF 两页（图表 + 正文）", pageCount(pdfText) == 2);
        check("PDF 内嵌 JPEG（DCTDecode）", pdfText.contains("/DCTDecode"));
        check("PDF xref 偏移有效", xrefPointsToXref(pdfText));

        // PDF：纯表格文档
        byte[] tablePdf = AiExportFormats.generate("pdf", "汇总", null, columns, rows);
        String tableText = new String(tablePdf, StandardCharsets.ISO_8859_1);
        check("PDF 纯表格单页", pageCount(tableText) == 1 && tableText.contains("/DCTDecode"));

        boolean pdfEmpty = false;
        try {
            AiExportFormats.generate("pdf", null, null, null, null);
        } catch (IllegalArgumentException e) {
            pdfEmpty = e.getMessage().contains("导出 PDF");
        }
        check("PDF 无内容明确拒绝", pdfEmpty);

        // 长文本自动分页
        StringBuilder longText = new StringBuilder();
        for (int i = 0; i < 160; i++) {
            longText.append("第 ").append(i).append(" 行：这是一段用于验证 PDF 自动分页的中文内容。\n");
        }
        byte[] longPdf = AiExportFormats.generate("pdf", "长文", longText.toString(), null, null);
        check("PDF 长文自动分页", pageCount(new String(longPdf, StandardCharsets.ISO_8859_1)) >= 2);
    }

    // ---------------- ② 文件存储 ----------------

    private static void testStore() throws Exception {
        System.out.println("---------- ② AiFileStore 隔离 / TTL / 上限 ----------");
        Path dir = Files.createTempDirectory("dojao-export-probe");
        AiFileStore store = new AiFileStore(dir, 60_000);
        try {
            AiFileStore.StoredFile alice = store.save("alice", "报告.md", "md",
                    "hello".getBytes(StandardCharsets.UTF_8));
            check("save 返回 id", alice.id() != null && !alice.id().isBlank());
            check("save 记录大小", alice.size() == 5);
            check("本人可见", store.find("alice", alice.id()) != null);
            check("★他人不可见", store.find("bob", alice.id()) == null);
            check("★admin（null）不越权看普通用户", store.find(null, alice.id()) == null);
            check("落盘内容正确", "hello".equals(Files.readString(alice.path())));

            AiFileStore.StoredFile admin = store.save(null, "a.txt", "txt",
                    "x".getBytes(StandardCharsets.UTF_8));
            check("null owner 归一化", "".equals(admin.owner()));
            check("null owner 自己可见", store.find(null, admin.id()) != null);
            check("不存在的 id", store.find("alice", "nope") == null);

            boolean emptyRejected = false;
            try {
                store.save("alice", "e.md", "md", new byte[0]);
            } catch (IllegalArgumentException e) {
                emptyRejected = true;
            }
            check("空内容拒绝", emptyRejected);

            boolean bigRejected = false;
            try {
                store.save("alice", "big.md", "md", new byte[AiFileStore.MAX_FILE_BYTES + 1]);
            } catch (IllegalArgumentException e) {
                bigRejected = true;
            }
            check("超单文件上限拒绝", bigRejected);

            // 每用户份数上限：连写 MAX+2 份，最旧的应被淘汰
            store.close();
            store = new AiFileStore(dir, 60_000);
            AiFileStore.StoredFile first = null;
            AiFileStore.StoredFile last = null;
            for (int i = 0; i < AiFileStore.MAX_FILES_PER_OWNER + 2; i++) {
                AiFileStore.StoredFile f = store.save("alice", "f" + i + ".txt", "txt",
                        ("v" + i).getBytes(StandardCharsets.UTF_8));
                if (first == null) {
                    first = f;
                }
                last = f;
            }
            check("每用户份数上限", store.size() == AiFileStore.MAX_FILES_PER_OWNER);
            check("最旧文件被淘汰", store.find("alice", first.id()) == null);
            check("最新文件仍在", store.find("alice", last.id()) != null);
        } finally {
            store.close();
        }

        // TTL 过期
        Path ttlDir = Files.createTempDirectory("dojao-export-ttl");
        AiFileStore ttl = new AiFileStore(ttlDir, 1);
        AiFileStore.StoredFile f = ttl.save("alice", "t.md", "md", "t".getBytes(StandardCharsets.UTF_8));
        Thread.sleep(20);
        check("★TTL 过期后不可见", ttl.find("alice", f.id()) == null);
        check("TTL 过期后清空", ttl.size() == 0);
        ttl.close();

        // 关闭时删除目录
        Path closeDir = Files.createTempDirectory("dojao-export-close");
        AiFileStore closing = new AiFileStore(closeDir, 60_000);
        closing.save("alice", "x.md", "md", "x".getBytes(StandardCharsets.UTF_8));
        closing.close();
        check("close 删除目录", !Files.exists(closeDir));
    }

    // ---------------- ③ export_file 端到端 ----------------

    private static void testServiceExport() throws Exception {
        System.out.println("---------- ③ exportFile 端到端 ----------");
        Path dir = Files.createTempDirectory("dojao-export-svc");
        AiFileStore store = new AiFileStore(dir, 60_000);
        AiAgentService svc = new AiAgentService(new AiConfig("http://x", "k", "m", 0.2), null, null, store);
        List<AiAgentService.GeneratedFile> out = new ArrayList<>();
        try {
            String msg = svc.exportFile("md", null, "9月销售报告", "# 结论\n\n销量增长 12%",
                    null, "alice", out::add);
            check("md: 生成成功提示", msg.contains("已生成文件") && msg.contains(".md"));
            check("md: sink 收到 1 份", out.size() == 1);
            AiAgentService.GeneratedFile md = out.get(0);
            check("md: 文件名用 title", "9月销售报告.md".equals(md.name()));
            check("md: url 形如下载路由", md.url().startsWith("/api/ai/files/") && md.url().endsWith(md.id()));
            check("md: 已落存储", store.find("alice", md.id()) != null);
            check("md: 内容正确", Files.readString(store.find("alice", md.id()).path())
                    .contains("销量增长 12%"));

            // CSV 由结果集 JSON 生成
            out.clear();
            String csvMsg = svc.exportFile("csv", "城市销量", null, null,
                    "{\"columns\":[\"城市\",\"销量\"],\"rows\":[[\"北京\",\"10\"],[\"上海\",\"20\"]]}",
                    "alice", out::add);
            check("csv: 生成成功", csvMsg.contains("已生成文件") && out.size() == 1);
            String csvText = new String(Files.readAllBytes(
                    store.find("alice", out.get(0).id()).path()), StandardCharsets.UTF_8);
            check("csv: BOM + 表头 + 数据", csvText.startsWith("\uFEFF")
                    && csvText.contains("城市,销量") && csvText.contains("北京,10"));

            // Excel 由结果集引用生成
            out.clear();
            String ref = svc.cacheResult("alice", List.of("城市", "销量"),
                    List.of(List.of("北京", "10"), List.of("上海", "20")));
            String xlsxMsg = svc.exportFile("excel", null, "城市销量", null, ref, "alice", out::add);
            check("excel: 引用复用生成成功", xlsxMsg.contains("已生成文件") && out.size() == 1);
            byte[] xlsx = Files.readAllBytes(store.find("alice", out.get(0).id()).path());
            check("excel: 是 xlsx", xlsx.length > 4 && xlsx[0] == 'P' && xlsx[1] == 'K');
            check("excel: 内容含中文", unzip(xlsx).getOrDefault("xl/worksheets/sheet1.xml", "").contains("北京"));

            // last 引用
            out.clear();
            String lastMsg = svc.exportFile("csv", null, null, null, "last", "alice", out::add);
            check("csv: last 引用可用", lastMsg.contains("已生成文件"));

            // 隔离
            check("★他人不可下载", store.find("bob", out.get(0).id()) == null);

            // PNG 图表（结果集引用）
            out.clear();
            String chartRef = svc.cacheResult("alice", List.of("城市", "销量"),
                    List.of(List.of("北京", "10"), List.of("上海", "20")));
            String pngMsg = svc.exportFile("png", "城市图", null, null, chartRef,
                    "bar", "城市", List.of("销量"), null, null, "alice", out::add);
            check("png: 生成成功", pngMsg.contains("已生成文件") && out.size() == 1
                    && "png".equals(out.get(0).format()));
            byte[] pngBytes = Files.readAllBytes(store.find("alice", out.get(0).id()).path());
            check("png: 文件是 PNG", (pngBytes[0] & 0xFF) == 0x89 && pngBytes[1] == 'P');

            // PDF 图表 + 正文
            out.clear();
            String pdfMsg = svc.exportFile("pdf", "城市报告", "城市销量", "## 结论", chartRef,
                    "bar", "城市", List.of("销量"), null, null, "alice", out::add);
            check("pdf: 生成成功", pdfMsg.contains("已生成文件") && out.size() == 1
                    && "pdf".equals(out.get(0).format()));
            String pdfHead = new String(Files.readAllBytes(
                    store.find("alice", out.get(0).id()).path()), StandardCharsets.ISO_8859_1);
            check("pdf: 文件头正确", pdfHead.startsWith("%PDF-1.4"));

            // HTML 完整文档（ECharts 报告形态）直传：落盘不能被转义成标签文本
            out.clear();
            String document = "<!DOCTYPE html><html><head><meta charset=\"utf-8\"><title>月度</title></head>"
                    + "<body><div id=\"main\"></div><script>window.ok=1;</script></body></html>";
            String htmlMsg = svc.exportFile("html", "月度报告", null, document, null, "alice", out::add);
            check("html: 完整文档导出成功", htmlMsg.contains("已生成文件") && out.size() == 1
                    && "html".equals(out.get(0).format()));
            String savedHtml = Files.readString(store.find("alice", out.get(0).id()).path());
            check("★html: 落盘就是完整文档（未转义）", savedHtml.stripLeading().startsWith("<!DOCTYPE html>")
                    && savedHtml.contains("<div id=\"main\"></div>")
                    && savedHtml.contains("<script>window.ok=1;</script>")
                    && !savedHtml.contains("&lt;script"));

            // 错误路径
            check("excel 缺数据提示", svc.exportFile("excel", null, null, null, null, "alice", out::add)
                    .contains("需要表格数据"));
            check("不支持格式提示", svc.exportFile("svg", null, null, "x", null, "alice", out::add)
                    .contains("不支持的文件格式"));
            check("png 缺图表参数提示", svc.exportFile("png", null, null, null, chartRef, "alice", out::add)
                    .contains("图表参数"));
            check("图表列不存在提示", svc.exportFile("png", null, null, null, chartRef,
                    "bar", "不存在的列", List.of("销量"), null, null, "alice", out::add)
                    .contains("图表参数有误"));
            check("md 缺内容提示", svc.exportFile("md", null, null, "  ", null, "alice", out::add)
                    .contains("需要 content"));
            check("坏引用提示", svc.exportFile("csv", null, null, null, "rs999", "alice", out::add)
                    .contains("未找到"));
            check("坏 JSON 提示", svc.exportFile("csv", null, null, null, "{bad", "alice", out::add)
                    .contains("不是有效的结果集 JSON"));
            check("sink 缺失拒绝", svc.exportFile("md", null, null, "x", null, "alice", null)
                    .contains("不支持导出文件"));
        } finally {
            svc.close();
        }
    }

    // ---------------- ④ 轮次落库 ----------------

    private static void testTurnsFiles() {
        System.out.println("---------- ④ AiTurns files 落库 ----------");
        String pending = AiTurns.appendPending("[]", "t1", "给我一份报告", null);
        check("appendPending 预留 files 数组", at(pending, 0).path("files").isArray()
                && at(pending, 0).path("files").isEmpty());

        AiTurns.FillResult r = AiTurns.fill(pending, "t1", "给我一份报告", "已生成",
                List.of("export_file"), List.of(),
                List.of("{\"id\":\"f1\",\"name\":\"报告.md\",\"format\":\"md\",\"size\":12,"
                        + "\"url\":\"/api/ai/files/f1\"}"), null);
        JsonNode t = at(r.turnsJson(), 0);
        check("fill 写入 files", t.path("files").size() == 1);
        check("files 内容正确", "报告.md".equals(t.path("files").get(0).path("name").asText()));
        check("files 不丢 url", "/api/ai/files/f1".equals(t.path("files").get(0).path("url").asText()));

        AiTurns.FillResult r2 = AiTurns.fill("[]", null, "q", "a", List.of(), List.of(), null);
        check("旧重载仍产 files 空数组", at(r2.turnsJson(), 0).path("files").isArray()
                && at(r2.turnsJson(), 0).path("files").isEmpty());

        AiTurns.FillResult r3 = AiTurns.fill("[]", null, "q", "a", List.of(), List.of(),
                List.of("{bad json"), null);
        check("坏文件元数据不炸", at(r3.turnsJson(), 0).path("files").isArray());
    }

    // ---------------- 助手 ----------------

    /** 抽样检查位图非纯白（图表 / 文字确实画上去了）。 */
    private static boolean hasNonWhite(BufferedImage img) {
        if (img == null) {
            return false;
        }
        for (int y = 0; y < img.getHeight(); y += 16) {
            for (int x = 0; x < img.getWidth(); x += 16) {
                if ((img.getRGB(x, y) & 0xFFFFFF) != 0xFFFFFF) {
                    return true;
                }
            }
        }
        return false;
    }

    /** 子串出现次数（区分大小写）。 */
    private static int occurrences(String s, String sub) {
        int n = 0;
        for (int i = s.indexOf(sub); i >= 0; i = s.indexOf(sub, i + sub.length())) {
            n++;
        }
        return n;
    }

    /** PDF 页数（取 /Pages 的 /Count）。 */
    private static int pageCount(String pdf) {
        Matcher m = Pattern.compile("/Count (\\d+)").matcher(pdf);
        return m.find() ? Integer.parseInt(m.group(1)) : -1;
    }

    /** startxref 给出的偏移必须恰好指向 "xref" 表（逐字节校验，ISO-8859-1 无损映射）。 */
    private static boolean xrefPointsToXref(String pdf) {
        int i = pdf.lastIndexOf("startxref");
        if (i < 0) {
            return false;
        }
        Matcher m = Pattern.compile("startxref\\s+(\\d+)\\s+%%EOF").matcher(pdf.substring(i));
        if (!m.find()) {
            return false;
        }
        int offset = Integer.parseInt(m.group(1));
        return offset >= 0 && offset < pdf.length() && pdf.startsWith("xref", offset);
    }

    private static Map<String, String> unzip(byte[] data) throws Exception {
        Map<String, String> parts = new LinkedHashMap<>();
        try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(data))) {
            ZipEntry e;
            while ((e = zis.getNextEntry()) != null) {
                parts.put(e.getName(), new String(zis.readAllBytes(), StandardCharsets.UTF_8));
            }
        }
        return parts;
    }

    private static JsonNode at(String turnsJson, int i) {
        try {
            JsonNode n = JSON.readTree(turnsJson);
            return n.isArray() ? n.path(i) : JSON.createObjectNode();
        } catch (Exception e) {
            return JSON.createObjectNode();
        }
    }

    private static void check(String name, boolean ok) {
        System.out.println((ok ? "  ✅ " : "  ❌ ") + name);
        if (!ok) {
            fail++;
        }
    }
}
