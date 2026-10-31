package com.dojao.ui.ai;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * 探针：{@link FileParser}（AI 对话附件解析）+ {@link AiTurns} 附件落库 —— 离线、零外部依赖。
 *
 * <p>覆盖：纯文本 UTF-8/BOM/GBK 回退、md/csv 直通、HTML 去标签、docx（段落 / 制表 / 表格 /
 * 域代码跳过）、xlsx（sharedStrings / inlineStr / 布尔 / 列号跳空）、旧版 doc/xls 与 PDF 的
 * 可操作报错、二进制拒绝、超长截断、XXE / zip 条目上限、附件 JSON 往返与配额、轮次落库。</p>
 *
 * <p><b>判别力</b>：把 {@code decode} 的 GB18030 回退删掉 → GBK 用例 FAIL；把 docx 表格拼接删掉 →
 * 表格用例 FAIL；把 {@code collect} 的 MAX_FILES_PER_MESSAGE 校验删掉 → 超限用例 FAIL；
 * 把 {@code AiTurns.appendPending} 的 {@code turn.set("attachments", …)} 删掉 → 落库用例 FAIL。</p>
 *
 * <p>运行：{@code java -cp "dojao-ui/target/test-classes;dojao-ui/target/classes;$(cat dojao-ui/cp.txt)" \
 * com.dojao.ui.ai.AiFileParserProbeTest}</p>
 */
public final class AiFileParserProbeTest {

    private static int pass = 0;
    private static int fail = 0;
    private static final ObjectMapper JSON = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        txtCases();
        htmlCases();
        docxCases();
        xlsxCases();
        errorCases();
        truncateCases();
        jsonCases();
        turnsCases();

        System.out.println("\n==== AiFileParserProbeTest: " + pass + " PASS / " + fail + " FAIL ====");
        if (fail > 0) {
            System.exit(1);
        }
    }

    // ==================== 纯文本 ====================

    private static void txtCases() {
        FileParser.ParsedFile f = FileParser.parse("report.md",
                "# 标题\n\n- 第一项\n- 第二项\n".getBytes(StandardCharsets.UTF_8));
        check("md：格式=markdown", "markdown".equals(f.format()));
        check("md：正文直通", f.text().contains("# 标题") && f.text().contains("- 第二项"));
        check("md：未截断", !f.truncated());

        // UTF-8 BOM
        byte[] bom = concat(new byte[]{(byte) 0xEF, (byte) 0xBB, (byte) 0xBF},
                "bom 文本".getBytes(StandardCharsets.UTF_8));
        FileParser.ParsedFile f2 = FileParser.parse("a.txt", bom);
        check("BOM：去掉 EF BB BF", "bom 文本".equals(f2.text()) && "text".equals(f2.format()));

        // GB18030 回退
        byte[] gbk = "中文报表".getBytes(Charset.forName("GB18030"));
        FileParser.ParsedFile f3 = FileParser.parse("gbk.csv", gbk);
        check("GBK 回退：中文正确", "中文报表".equals(f3.text()) && "csv".equals(f3.format()));

        // Windows 换行 + 行尾空白整理
        FileParser.ParsedFile f4 = FileParser.parse("win.txt", "a  \r\n\r\n\r\nb\t \r\n".getBytes(StandardCharsets.UTF_8));
        check("换行归一并压缩空行", "a\n\nb".equals(f4.text()));

        // 文件名路径剥离
        FileParser.ParsedFile f5 = FileParser.parse("C:\\tmp\\dir\\sales.txt", "x".getBytes(StandardCharsets.UTF_8));
        check("文件名剥离路径", "sales.txt".equals(f5.name()));
    }

    // ==================== HTML ====================

    private static void htmlCases() {
        String html = "<html><head><style>h1{color:red}</style></head><body>"
                + "<h1>报表</h1><p>第一段</p>"
                + "<table><tr><td>A</td><td>B</td></tr></table>"
                + "<script>alert(1)</script></body></html>";
        FileParser.ParsedFile f = FileParser.parse("page.html", html.getBytes(StandardCharsets.UTF_8));
        check("HTML：保留正文", f.text().contains("报表") && f.text().contains("第一段"));
        check("HTML：表格单元格转制表符", f.text().contains("A\tB"));
        check("HTML：去掉 script/style", !f.text().contains("alert") && !f.text().contains("color:red"));
        check("HTML：无残留标签", !f.text().contains("<h1>"));
    }

    // ==================== docx ====================

    private static void docxCases() {
        String documentXml = """
                <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
                <w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
                  <w:body>
                    <w:p><w:r><w:t>季度报告</w:t></w:r></w:p>
                    <w:p><w:r><w:t>第二段</w:t></w:r><w:r><w:tab/><w:t>制表后</w:t></w:r></w:p>
                    <w:p><w:r><w:instrText>PAGE</w:instrText><w:t>正文</w:t></w:r></w:p>
                    <w:tbl>
                      <w:tr>
                        <w:tc><w:p><w:r><w:t>地区</w:t></w:r></w:p></w:tc>
                        <w:tc><w:p><w:r><w:t>销量</w:t></w:r></w:p></w:tc>
                      </w:tr>
                      <w:tr>
                        <w:tc><w:p><w:r><w:t>华东</w:t></w:r></w:p></w:tc>
                        <w:tc><w:p><w:r><w:t>120</w:t></w:r></w:p></w:tc>
                      </w:tr>
                    </w:tbl>
                  </w:body>
                </w:document>
                """;
        Map<String, byte[]> parts = new LinkedHashMap<>();
        parts.put("word/document.xml", documentXml.getBytes(StandardCharsets.UTF_8));
        FileParser.ParsedFile f = FileParser.parse("报告.docx", zip(parts));
        check("docx：格式=word", "word".equals(f.format()));
        check("docx：段落文本", f.text().contains("季度报告") && f.text().contains("第二段"));
        check("docx：制表符保留", f.text().contains("第二段\t制表后"));
        check("docx：表格按 | 拼接", f.text().contains("地区 | 销量") && f.text().contains("华东 | 120"));
        check("docx：跳过域代码 / 保留正文", f.text().contains("正文") && !f.text().contains("PAGE"));
    }

    // ==================== xlsx ====================

    private static void xlsxCases() {
        Map<String, byte[]> parts = new LinkedHashMap<>();
        parts.put("xl/workbook.xml", """
                <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
                <workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"
                          xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
                  <sheets><sheet name="销售" sheetId="1" r:id="rId1"/></sheets>
                </workbook>
                """.getBytes(StandardCharsets.UTF_8));
        parts.put("xl/_rels/workbook.xml.rels", """
                <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
                <Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
                  <Relationship Id="rId1"
                    Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet"
                    Target="worksheets/sheet1.xml"/>
                </Relationships>
                """.getBytes(StandardCharsets.UTF_8));
        parts.put("xl/sharedStrings.xml", """
                <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
                <sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
                  <si><t>产品</t></si>
                  <si><t>销量</t></si>
                  <si><r><t>苹</t></r><r><t>果</t></r></si>
                </sst>
                """.getBytes(StandardCharsets.UTF_8));
        parts.put("xl/worksheets/sheet1.xml", """
                <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
                <worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
                  <sheetData>
                    <row r="1"><c r="A1" t="s"><v>0</v></c><c r="B1" t="s"><v>1</v></c></row>
                    <row r="2"><c r="A2" t="s"><v>2</v></c><c r="C2"><v>42.5</v></c>
                      <c r="D2" t="b"><v>1</v></c></row>
                    <row r="3"><c r="A3" t="inlineStr"><is><t>香蕉</t></is></c></row>
                  </sheetData>
                </worksheet>
                """.getBytes(StandardCharsets.UTF_8));
        FileParser.ParsedFile f = FileParser.parse("sales.xlsx", zip(parts));
        check("xlsx：格式=excel", "excel".equals(f.format()));
        check("xlsx：工作表名", f.text().contains("### 工作表: 销售"));
        check("xlsx：共享字符串表头", f.text().contains("产品\t销量"));
        check("xlsx：富文本 run 拼接", f.text().contains("苹果"));
        check("xlsx：跳空列 + 数值 + 布尔", f.text().contains("苹果\t\t42.5\tTRUE"));
        check("xlsx：inlineStr", f.text().contains("香蕉"));
        check("xlsx：行按换行分隔", f.text().split("\n").length >= 4);
    }

    // ==================== 报错 ====================

    private static void errorCases() {
        expectError("旧版 doc 可操作报错", "a.doc", new byte[]{1, 2, 3}, "另存为 .docx");
        expectError("旧版 xls 可操作报错", "a.xls", new byte[]{1, 2, 3}, "另存为 .xlsx");
        expectError("PDF 明确拒绝", "a.pdf", new byte[]{1, 2, 3}, "PDF");
        expectError("未知扩展名", "a.exe", new byte[]{1, 2, 3}, "不支持的文件类型");
        expectError("二进制（含 NUL）拒绝", "a.txt", new byte[]{65, 0, 66}, "二进制");
        expectError("空文件", "a.txt", new byte[0], "为空");
        expectError("损坏的 docx", "a.docx", "not a zip".getBytes(StandardCharsets.UTF_8), "Office");

        // XXE：DOCTYPE / 外部实体一律拒绝（不读本地文件）
        Map<String, byte[]> xxe = new LinkedHashMap<>();
        xxe.put("word/document.xml", """
                <?xml version="1.0"?>
                <!DOCTYPE foo [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>
                <w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
                  <w:body><w:p><w:r><w:t>&xxe;</w:t></w:r></w:p></w:body>
                </w:document>
                """.getBytes(StandardCharsets.UTF_8));
        expectError("docx 拒绝 DOCTYPE（防 XXE）", "xxe.docx", zip(xxe), "解析 .docx 内容失败");

        // ZIP 条目上限
        Map<String, byte[]> bomb = new LinkedHashMap<>();
        for (int i = 0; i < 2001; i++) {
            bomb.put("f" + i + ".xml", new byte[0]);
        }
        expectError("ZIP 条目过多拒绝", "bomb.docx", zip(bomb), "条目过多");
    }

    // ==================== 截断 ====================

    private static void truncateCases() {
        StringBuilder big = new StringBuilder();
        for (int i = 0; i < FileParser.MAX_TEXT_CHARS + 500; i++) {
            big.append('a');
        }
        FileParser.ParsedFile f = FileParser.parse("big.txt", big.toString().getBytes(StandardCharsets.UTF_8));
        check("超长：标记已截断", f.truncated());
        check("超长：正文以截断提示收尾", f.text().contains("已截断") && f.text().startsWith("aaa"));
        check("超长：chars 与正文长度一致", f.chars() == f.text().length());

        // 格式化后的提示词块
        FileParser.ParsedFile small = FileParser.parse("note.md", "内容".getBytes(StandardCharsets.UTF_8));
        String block = FileParser.promptBlock(List.of(small));
        check("提示词块：有起止标记", block.startsWith("\n\n【用户上传的参考文件】")
                && block.endsWith("【参考文件结束】\n"));
        check("提示词块：含文件名与正文", block.contains("文件: note.md") && block.contains("内容"));
        check("提示词块：空列表为空串", FileParser.promptBlock(List.of()).isEmpty()
                && FileParser.promptBlock(null).isEmpty());
    }

    // ==================== 附件 JSON ====================

    private static void jsonCases() throws Exception {
        FileParser.ParsedFile a = FileParser.parse("a.txt", "甲".getBytes(StandardCharsets.UTF_8));
        FileParser.ParsedFile b = FileParser.parse("b.md", "乙".getBytes(StandardCharsets.UTF_8));
        String payload = FileParser.toJson(List.of(a, b)).toString();

        List<FileParser.ParsedFile> back;
        try {
            back = FileParser.fromJson(JSON.readTree(payload));
        } catch (Exception e) {
            back = List.of();
        }
        check("JSON：往返 2 个附件", back.size() == 2);
        check("JSON：名字 / 格式 / 正文保持",
                back.size() == 2 && "a.txt".equals(back.get(0).name())
                        && "markdown".equals(back.get(1).format())
                        && "甲".equals(back.get(0).text()) && "乙".equals(back.get(1).text()));

        String tooMany;
        List<String> six = new ArrayList<>();
        for (int i = 0; i < FileParser.MAX_FILES_PER_MESSAGE + 1; i++) {
            six.add("{\"name\":\"f" + i + ".txt\",\"text\":\"x\"}");
        }
        tooMany = "[" + String.join(",", six) + "]";
        boolean rejected = false;
        String msg = "";
        try {
            FileParser.fromJson(JSON.readTree(tooMany));
        } catch (IllegalArgumentException e) {
            rejected = true;
            msg = e.getMessage();
        }
        check("JSON：附件数量超限拒绝", rejected && msg.contains("最多"));

        // 单文件 / 总量超限：截断并标记（不抛）
        String huge = "[" + hugeAttachment("huge.txt", FileParser.MAX_TEXT_CHARS + 1000)
                + "," + hugeAttachment("second.txt", 150_000) + "]";
        List<FileParser.ParsedFile> capped;
        try {
            capped = FileParser.fromJson(JSON.readTree(huge));
        } catch (Exception e) {
            capped = List.of();
        }
        check("JSON：单文件截到 10 万字上限",
                capped.size() == 2 && capped.get(0).text().length() <= FileParser.MAX_TEXT_CHARS
                        && capped.get(0).truncated());
        check("JSON：总量截到 20 万字上限",
                capped.size() == 2
                        && capped.get(0).text().length() + capped.get(1).text().length()
                        <= FileParser.MAX_TOTAL_CHARS
                        && capped.get(1).truncated());

        check("JSON：非数组 / 坏数据宽容为空", FileParser.fromJsonLenient(JSON.readTree("{}")).isEmpty()
                && FileParser.fromJsonLenient(null).isEmpty());
        check("JSON：promptBlockFromJson 同格式", FileParser.promptBlockFromJson(JSON.readTree(payload))
                .contains("【用户上传的参考文件】"));
    }

    private static String hugeAttachment(String name, int chars) {
        StringBuilder sb = new StringBuilder("{\"name\":\"").append(name).append("\",\"format\":\"text\",\"chars\":")
                .append(chars).append(",\"truncated\":false,\"text\":\"");
        for (int i = 0; i < chars; i++) {
            sb.append('x');
        }
        return sb.append("\"}").toString();
    }

    // ==================== 轮次落库 ====================

    private static void turnsCases() throws Exception {
        FileParser.ParsedFile a = FileParser.parse("季度报告.md", "# 数据".getBytes(StandardCharsets.UTF_8));
        String att = FileParser.toJson(List.of(a)).toString();

        String withAttachment;
        String without;
        try {
            withAttachment = AiTurns.appendPending("[]", "t1", "", null, JSON.readTree(att));
            without = AiTurns.appendPending("[]", "t2", "你好", null);
        } catch (Exception e) {
            withAttachment = "ERROR:" + e.getMessage();
            without = "ERROR:" + e.getMessage();
        }
        check("轮次：附件写入 attachments 数组",
                withAttachment.contains("\"attachments\"") && withAttachment.contains("季度报告.md"));
        check("轮次：旧 4 参重载不写 attachments", !without.contains("\"attachments\""));
        check("轮次：pending / turnId 仍正确",
                withAttachment.contains("\"pending\":true") && withAttachment.contains("\"turnId\":\"t1\""));

        // 完成回填不覆盖 attachments（fill 只改 assistant/ops/...）
        String filled = AiTurns.fill(withAttachment, "t1", "", "已完成", List.of(), null, null, null, null)
                .turnsJson();
        check("轮次：fill 后附件仍在", filled.contains("季度报告.md"));
    }

    // ==================== 工具 ====================

    private static void expectError(String what, String fileName, byte[] data, String contains) {
        try {
            FileParser.parse(fileName, data);
            check(what, false);
        } catch (IllegalArgumentException e) {
            String msg = e.getMessage() == null ? "" : e.getMessage();
            check(what, msg.contains(contains));
        } catch (Exception e) {
            check(what + "（异常类型错误：" + e.getClass().getSimpleName() + "）", false);
        }
    }

    private static byte[] zip(Map<String, byte[]> entries) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ZipOutputStream zos = new ZipOutputStream(bos)) {
            for (Map.Entry<String, byte[]> e : entries.entrySet()) {
                zos.putNextEntry(new ZipEntry(e.getKey()));
                zos.write(e.getValue());
                zos.closeEntry();
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return bos.toByteArray();
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] out = new byte[a.length + b.length];
        System.arraycopy(a, 0, out, 0, a.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }

    private static void check(String what, boolean ok) {
        if (ok) {
            pass++;
            System.out.println("[PASS] " + what);
        } else {
            fail++;
            System.out.println("[FAIL] " + what);
        }
    }
}
