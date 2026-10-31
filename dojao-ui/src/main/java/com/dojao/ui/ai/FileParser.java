package com.dojao.ui.ai;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * <b>AI 对话附件解析器</b>：把用户上传的文件统一解析成纯文本，供提示词拼装使用。
 *
 * <p>支持的格式（<b>零外部依赖</b>，只用 JDK）：</p>
 * <ul>
 *   <li><b>纯文本</b>：txt / md / csv / tsv / json / xml / yaml / log / sql / html 等
 *       （编码：UTF-8（含 BOM）/ UTF-16（BOM）/ UTF-8 不合法时回退 GB18030）；</li>
 *   <li><b>Word</b>：{@code .docx}（OOXML，解 {@code word/document.xml}：段落、换行、制表、
 *       表格按 {@code |} 拼接）；</li>
 *   <li><b>Excel</b>：{@code .xlsx}（OOXML，读 {@code sharedStrings} + 各工作表，输出
 *       「工作表名 + 制表符分隔的行」）。</li>
 * </ul>
 *
 * <p>旧版二进制格式（{@code .doc} / {@code .xls}）与 PDF 不做解析，给出可操作提示
 * （另存为 docx / xlsx 或转文本），<b>绝不静默返回空内容</b>。</p>
 *
 * <p><b>安全与配额</b>：单文件 ≤ {@link #MAX_FILE_BYTES}；ZIP 解压总量 ≤ 40MB、条目 ≤ 2000
 * （防 zip bomb）；XML 解析禁用 DOCTYPE / 外部实体（防 XXE）；文本解析前拒绝含 NUL 的
 * 疑似二进制文件；最终文本 ≤ {@link #MAX_TEXT_CHARS}（超出截断并注明）。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/21
 */
public final class FileParser {

    /** 单个上传文件最大字节数（与 AI 导出文件上限一致）。 */
    public static final int MAX_FILE_BYTES = 10 * 1024 * 1024;

    /** 单个文件解析后保留的最大字符数（超出截断，避免提示词 / 会话记录被撑爆）。 */
    public static final int MAX_TEXT_CHARS = 100_000;

    /** 单条消息最多携带的附件数。 */
    public static final int MAX_FILES_PER_MESSAGE = 5;

    /** 单条消息全部附件解析文本的总字符上限。 */
    public static final int MAX_TOTAL_CHARS = 200_000;

    /** ZIP 解压总量上限（防 zip bomb）。 */
    private static final int MAX_UNZIPPED_BYTES = 40 * 1024 * 1024;

    /** ZIP 条目数上限。 */
    private static final int MAX_ZIP_ENTRIES = 2000;

    /** 单个工作表最多读取的单元格数（超出停止并注明）。 */
    private static final int MAX_SHEET_CELLS = 200_000;

    /** 支持解析的纯文本扩展名。 */
    private static final Set<String> TEXT_EXTENSIONS = Set.of(
            "txt", "text", "log", "sql", "csv", "tsv", "json", "xml", "yaml", "yml",
            "md", "markdown", "html", "htm", "java", "py", "js", "ts", "sh", "conf",
            "ini", "properties", "toml");

    /** 用户可读的支持格式提示（报错文案用）。 */
    private static final String SUPPORTED_HINT =
            "txt / md / csv / tsv / json / xml / yaml / html / log / sql / docx / xlsx";

    /** 附件 JSON（轮次 attachments 字段 / 请求体）的读写 mapper。 */
    private static final ObjectMapper JSON = new ObjectMapper();

    /** 一条附件解析结果。{@code text} 已按 {@link #MAX_TEXT_CHARS} 截断。 */
    public record ParsedFile(String name, String format, int chars, boolean truncated, String text) {
        public ParsedFile {
            name = name == null || name.isBlank() ? "附件" : name.trim();
            format = format == null || format.isBlank() ? "txt" : format;
            text = text == null ? "" : text;
        }
    }

    private FileParser() {
    }

    /**
     * 解析一个上传文件。
     *
     * @throws IllegalArgumentException 文件为空 / 过大 / 类型不支持 / 内容损坏（文案可直接展示给用户）
     */
    public static ParsedFile parse(String fileName, byte[] data) {
        if (data == null || data.length == 0) {
            throw new IllegalArgumentException("文件内容为空。");
        }
        if (data.length > MAX_FILE_BYTES) {
            throw new IllegalArgumentException("文件过大（" + data.length + " 字节），单文件上限 "
                    + (MAX_FILE_BYTES / 1024 / 1024) + "MB。");
        }
        String name = safeName(fileName);
        String ext = extension(name);
        String text;
        switch (ext) {
            case "docx" -> text = parseDocx(data);
            case "xlsx" -> text = parseXlsx(data);
            case "doc" -> throw new IllegalArgumentException(
                    "暂不支持旧版 Word（.doc），请用 Office/WPS 另存为 .docx 后重试。");
            case "xls" -> throw new IllegalArgumentException(
                    "暂不支持旧版 Excel（.xls），请用 Office/WPS 另存为 .xlsx 后重试。");
            case "pdf" -> throw new IllegalArgumentException(
                    "暂不支持 PDF，请转换为 txt / md / docx 后重试。");
            default -> {
                if (!TEXT_EXTENSIONS.contains(ext)) {
                    throw new IllegalArgumentException("不支持的文件类型"
                            + (ext.isEmpty() ? "" : "（." + ext + "）")
                            + "，支持：" + SUPPORTED_HINT + "。");
                }
                text = parseText(data, ext);
            }
        }
        boolean truncated = text.length() > MAX_TEXT_CHARS;
        if (truncated) {
            text = text.substring(0, MAX_TEXT_CHARS)
                    + "\n…（内容过长，已截断，仅保留前 " + MAX_TEXT_CHARS + " 字符）";
        }
        text = tidy(text);
        return new ParsedFile(name, formatLabel(ext), text.length(), truncated, text);
    }

    /**
     * 把附件拼成模型提示词块（统一格式，历史轮次与本轮都走这里）。
     * 空列表返回空串。
     */
    public static String promptBlock(List<ParsedFile> files) {
        if (files == null || files.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder("\n\n【用户上传的参考文件】\n");
        for (ParsedFile f : files) {
            sb.append("----- 文件: ").append(f.name())
                    .append("（").append(f.format()).append("，").append(f.chars()).append(" 字")
                    .append(f.truncated() ? "，已截断" : "").append("）-----\n");
            sb.append(f.text()).append('\n');
            sb.append("----- 文件结束: ").append(f.name()).append(" -----\n");
        }
        sb.append("【参考文件结束】\n");
        return sb.toString();
    }

    // ==================== 附件 JSON 形状 ====================

    /**
     * 附件列表 → JSON 数组（{@code [{name,format,chars,truncated,text}]}）。
     * 前端解析接口的响应、轮次落库、请求体校验三处共用同一形状。
     */
    public static ArrayNode toJson(List<ParsedFile> files) {
        ArrayNode arr = JSON.createArrayNode();
        if (files == null) {
            return arr;
        }
        for (ParsedFile f : files) {
            if (f == null) {
                continue;
            }
            ObjectNode o = JSON.createObjectNode();
            o.put("name", f.name());
            o.put("format", f.format());
            o.put("chars", f.chars());
            o.put("truncated", f.truncated());
            o.put("text", f.text());
            arr.add(o);
        }
        return arr;
    }

    /**
     * JSON 数组 → 附件列表（请求体校验用）：超过 {@link #MAX_FILES_PER_MESSAGE} 直接拒绝，
     * 单文件 / 总量超限按上限截断并标记「已截断」。
     *
     * @throws IllegalArgumentException 附件数量超限（文案可直接展示给用户）
     */
    public static List<ParsedFile> fromJson(JsonNode attachments) {
        return collect(attachments, true);
    }

    /** 同 {@link #fromJson}，但<b>不抛异常</b>（历史轮次回放用：坏数据跳过即可）。 */
    public static List<ParsedFile> fromJsonLenient(JsonNode attachments) {
        return collect(attachments, false);
    }

    /** 历史轮次里的 attachments 数组 → 提示词块（坏数据 / 空数组返回空串）。 */
    public static String promptBlockFromJson(JsonNode attachments) {
        return promptBlock(fromJsonLenient(attachments));
    }

    private static List<ParsedFile> collect(JsonNode attachments, boolean strict) {
        List<ParsedFile> out = new ArrayList<>();
        if (attachments == null || !attachments.isArray()) {
            return out;
        }
        if (strict && attachments.size() > MAX_FILES_PER_MESSAGE) {
            throw new IllegalArgumentException("单条消息最多 " + MAX_FILES_PER_MESSAGE + " 个附件。");
        }
        int total = 0;
        for (JsonNode n : attachments) {
            if (!n.isObject()) {
                continue;
            }
            if (out.size() >= MAX_FILES_PER_MESSAGE) {
                break;
            }
            String text = n.path("text").asText("");
            boolean truncated = n.path("truncated").asBoolean(false);
            if (text.length() > MAX_TEXT_CHARS) {
                text = text.substring(0, MAX_TEXT_CHARS);
                truncated = true;
            }
            int room = MAX_TOTAL_CHARS - total;
            if (room <= 0) {
                break;
            }
            if (text.length() > room) {
                text = text.substring(0, room);
                truncated = true;
            }
            total += text.length();
            String name = n.path("name").asText(null);
            String format = n.path("format").asText(null);
            out.add(new ParsedFile(name, format, text.length(), truncated, text));
        }
        return out;
    }

    // ==================== 纯文本 ====================

    private static String parseText(byte[] data, String ext) {
        String text = decode(data);
        if ("html".equals(ext) || "htm".equals(ext)) {
            return stripHtml(text);
        }
        return text;
    }

    /** 解码文本：UTF-8 BOM / UTF-16 BOM 优先，其余严格 UTF-8，失败回退 GB18030。 */
    private static String decode(byte[] data) {
        if (startsWith(data, 0xEF, 0xBB, 0xBF)) {
            return new String(data, 3, data.length - 3, StandardCharsets.UTF_8);
        }
        if (startsWith(data, 0xFF, 0xFE)) {
            return new String(data, 2, data.length - 2, StandardCharsets.UTF_16LE);
        }
        if (startsWith(data, 0xFE, 0xFF)) {
            return new String(data, 2, data.length - 2, StandardCharsets.UTF_16BE);
        }
        for (byte b : data) {
            if (b == 0) {
                throw new IllegalArgumentException(
                        "文件内容疑似二进制（含 NUL 字节），无法作为文本解析；如为 Excel/Word 请上传 .xlsx/.docx。");
            }
        }
        try {
            return decodeStrict(data, StandardCharsets.UTF_8);
        } catch (CharacterCodingException e) {
            try {
                return decodeStrict(data, Charset.forName("GB18030"));
            } catch (CharacterCodingException ignored) {
                return new String(data, StandardCharsets.UTF_8);
            }
        }
    }

    private static String decodeStrict(byte[] data, Charset charset) throws CharacterCodingException {
        CharBuffer buf = charset.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(data));
        return buf.toString();
    }

    private static boolean startsWith(byte[] data, int... prefix) {
        if (data.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if ((data[i] & 0xFF) != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    /** HTML → 纯文本：去掉 script/style，块级标签转换行，表格单元格转制表符。 */
    private static String stripHtml(String html) {
        // 正文最终只要前 10 万字：超大 HTML 先截到 4 倍上限再跑正则，避免在 10MB 文本上做多次全量替换。
        String s = html.length() > MAX_TEXT_CHARS * 4 ? html.substring(0, MAX_TEXT_CHARS * 4) : html;
        s = s.replaceAll("(?is)<(script|style)\\b[^>]*>.*?</\\1\\s*>", "");
        s = s.replaceAll("(?i)</t[dh]\\s*>", "\t");
        s = s.replaceAll("(?i)</?(br|p|div|li|tr|h[1-6]|section|article|header|footer|"
                + "table|ul|ol|blockquote|pre)\\b[^>]*>", "\n");
        s = s.replaceAll("(?s)<[^>]+>", "");
        return decodeEntities(s);
    }

    private static String decodeEntities(String s) {
        return s.replace("&nbsp;", " ")
                .replace("&lt;", "<")
                .replace("&gt;", ">")
                .replace("&quot;", "\"")
                .replace("&#39;", "'")
                .replace("&apos;", "'")
                .replace("&amp;", "&");
    }

    // ==================== Word（.docx）====================

    private static String parseDocx(byte[] data) {
        Map<String, byte[]> entries = readZip(data);
        byte[] xml = entries.get("word/document.xml");
        if (xml == null) {
            throw new IllegalArgumentException("不是有效的 .docx 文件（缺少 word/document.xml）。");
        }
        Document doc = parseXml(xml, ".docx");
        Element body = firstChildElement(doc.getDocumentElement(), "body");
        if (body == null) {
            body = doc.getDocumentElement();
        }
        StringBuilder sb = new StringBuilder();
        appendDocxBlocks(body, sb);
        return sb.toString();
    }

    /** 按文档顺序输出段落与表格（正文顶层块，表格内段落不再重复输出）。 */
    private static void appendDocxBlocks(Element parent, StringBuilder sb) {
        for (Element el : childElements(parent)) {
            switch (localName(el)) {
                case "p" -> sb.append(docxParagraph(el)).append('\n');
                case "tbl" -> {
                    appendDocxTable(el, sb);
                    sb.append('\n');
                }
                // 内容控件 / 自定义 XML：继续向下找 p / tbl
                case "sdt", "sdtContent", "customXml" -> appendDocxBlocks(el, sb);
                default -> {
                    // 其它容器（w:body 外层等）不递归，避免重复
                }
            }
        }
    }

    /** 段落文本：w:t 文字 + w:tab → \t + w:br/w:cr → 换行；跳过域代码（w:instrText）。 */
    private static String docxParagraph(Element paragraph) {
        StringBuilder sb = new StringBuilder();
        appendDocxInline(paragraph, sb);
        return sb.toString().stripTrailing();
    }

    private static void appendDocxInline(Element parent, StringBuilder sb) {
        for (Element el : childElements(parent)) {
            String ln = localName(el);
            switch (ln) {
                case "t" -> sb.append(el.getTextContent());
                case "tab" -> sb.append('\t');
                case "br", "cr" -> sb.append('\n');
                case "instrText", "delText" -> {
                    // 域代码 / 修订删除文本不输出
                }
                case "p" -> {
                    sb.append(docxParagraph(el)).append('\n');
                }
                case "tbl" -> {
                    appendDocxTable(el, sb);
                    sb.append('\n');
                }
                default -> appendDocxInline(el, sb);
            }
        }
    }

    private static void appendDocxTable(Element table, StringBuilder sb) {
        for (Element row : childElements(table)) {
            if (!"tr".equals(localName(row))) {
                continue;
            }
            List<String> cells = new ArrayList<>();
            for (Element cell : childElements(row)) {
                if (!"tc".equals(localName(cell))) {
                    continue;
                }
                StringBuilder cellText = new StringBuilder();
                for (Element block : childElements(cell)) {
                    String ln = localName(block);
                    if ("p".equals(ln)) {
                        if (cellText.length() > 0) {
                            cellText.append(' ');
                        }
                        cellText.append(docxParagraph(block));
                    } else if ("tbl".equals(ln)) {
                        appendDocxTable(block, cellText);
                    } else if ("sdt".equals(ln) || "sdtContent".equals(ln)) {
                        appendDocxBlocks(block, cellText);
                    }
                }
                cells.add(cellText.toString().replace('\n', ' ').strip());
            }
            sb.append(String.join(" | ", cells)).append('\n');
        }
    }

    // ==================== Excel（.xlsx）====================

    private static String parseXlsx(byte[] data) {
        Map<String, byte[]> entries = readZip(data);
        List<String> shared = entries.get("xl/sharedStrings.xml") == null
                ? List.of() : parseSharedStrings(entries.get("xl/sharedStrings.xml"));
        List<String[]> sheets = sheetRefs(entries);
        if (sheets.isEmpty()) {
            throw new IllegalArgumentException("不是有效的 .xlsx 文件（找不到工作表）。");
        }
        StringBuilder sb = new StringBuilder();
        for (String[] sheet : sheets) {
            byte[] xml = entries.get(sheet[1]);
            if (xml == null) {
                continue;
            }
            if (sb.length() > 0) {
                sb.append('\n');
            }
            sb.append("### 工作表: ").append(sheet[0]).append('\n');
            appendSheet(parseXml(xml, ".xlsx").getDocumentElement(), shared, sb);
        }
        return sb.toString();
    }

    /** 工作表清单：workbook.xml 的顺序与名字 + rels 映射到具体 xml 路径。 */
    private static List<String[]> sheetRefs(Map<String, byte[]> entries) {
        List<String[]> out = new ArrayList<>();
        byte[] workbook = entries.get("xl/workbook.xml");
        if (workbook != null) {
            Map<String, String> rels = workbookRels(entries.get("xl/_rels/workbook.xml.rels"));
            Document doc = parseXml(workbook, ".xlsx");
            NodeList nodes = doc.getElementsByTagNameNS("*", "sheet");
            for (int i = 0; i < nodes.getLength(); i++) {
                Element sheet = (Element) nodes.item(i);
                String name = sheet.getAttribute("name");
                String rid = attrByLocalName(sheet, "id");
                String target = rid == null ? null : rels.get(rid);
                String path = target == null ? null : normalizePartPath(target);
                if (path != null && entries.containsKey(path)) {
                    out.add(new String[]{name == null || name.isBlank() ? "Sheet" + (i + 1) : name, path});
                }
            }
        }
        if (!out.isEmpty()) {
            return out;
        }
        // 兜底：按文件名枚举（缺少 workbook.xml 的极少见形态）
        int idx = 0;
        for (String path : entries.keySet()) {
            if (path.matches("xl/worksheets/sheet\\d+\\.xml")) {
                idx++;
                out.add(new String[]{"Sheet" + idx, path});
            }
        }
        out.sort((a, b) -> sheetNumber(a[1]) - sheetNumber(b[1]));
        return out;
    }

    private static int sheetNumber(String path) {
        int i = path.lastIndexOf("sheet");
        int j = path.lastIndexOf('.');
        try {
            return Integer.parseInt(path.substring(i + 5, j));
        } catch (Exception e) {
            return Integer.MAX_VALUE;
        }
    }

    private static Map<String, String> workbookRels(byte[] xml) {
        Map<String, String> out = new LinkedHashMap<>();
        if (xml == null) {
            return out;
        }
        try {
            Document doc = parseXml(xml, ".xlsx");
            NodeList nodes = doc.getElementsByTagNameNS("*", "Relationship");
            for (int i = 0; i < nodes.getLength(); i++) {
                Element rel = (Element) nodes.item(i);
                out.put(rel.getAttribute("Id"), rel.getAttribute("Target"));
            }
        } catch (IllegalArgumentException ignored) {
            // rels 损坏 → 走文件名兜底
        }
        return out;
    }

    /** Target 可能是 "worksheets/sheet1.xml" 或 "/xl/worksheets/sheet1.xml"。 */
    private static String normalizePartPath(String target) {
        String t = target.replace('\\', '/');
        if (t.startsWith("/")) {
            return t.substring(1);
        }
        return t.startsWith("xl/") ? t : "xl/" + t;
    }

    private static List<String> parseSharedStrings(byte[] xml) {
        List<String> out = new ArrayList<>();
        Document doc = parseXml(xml, ".xlsx");
        NodeList sis = doc.getElementsByTagNameNS("*", "si");
        for (int i = 0; i < sis.getLength(); i++) {
            StringBuilder sb = new StringBuilder();
            appendSharedText((Element) sis.item(i), sb);
            out.add(sb.toString());
        }
        return out;
    }

    /** 富文本 run：拼接所有 w:t 等价物（t），跳过拼音注音（rPh）。 */
    private static void appendSharedText(Element parent, StringBuilder sb) {
        for (Element el : childElements(parent)) {
            String ln = localName(el);
            if ("t".equals(ln)) {
                sb.append(el.getTextContent());
            } else if (!"rPh".equals(ln)) {
                appendSharedText(el, sb);
            }
        }
    }

    private static void appendSheet(Element root, List<String> shared, StringBuilder sb) {
        Element sheetData = firstDescendant(root, "sheetData");
        if (sheetData == null) {
            return;
        }
        int cells = 0;
        for (Element row : childElements(sheetData)) {
            if (!"row".equals(localName(row))) {
                continue;
            }
            TreeMap<Integer, String> values = new TreeMap<>();
            int maxCol = -1;
            int nextCol = 0;
            for (Element cell : childElements(row)) {
                if (!"c".equals(localName(cell))) {
                    continue;
                }
                int col = columnIndex(cell.getAttribute("r"));
                if (col < 0) {
                    col = nextCol;
                }
                nextCol = col + 1;
                maxCol = Math.max(maxCol, col);
                String v = cellValue(cell, shared);
                if (v != null && !v.isEmpty()) {
                    values.put(col, v);
                }
                if (++cells > MAX_SHEET_CELLS) {
                    sb.append("…（单元格过多，后续内容已省略）\n");
                    return;
                }
            }
            if (maxCol < 0) {
                sb.append('\n');
                continue;
            }
            for (int i = 0; i <= maxCol; i++) {
                if (i > 0) {
                    sb.append('\t');
                }
                sb.append(values.getOrDefault(i, ""));
            }
            sb.append('\n');
        }
    }

    private static String cellValue(Element cell, List<String> shared) {
        String type = cell.getAttribute("t");
        if ("inlineStr".equals(type)) {
            Element is = firstChildElement(cell, "is");
            if (is != null) {
                StringBuilder sb = new StringBuilder();
                appendSharedText(is, sb);
                return sb.toString();
            }
        }
        String v = childText(cell, "v");
        if (v == null) {
            return "";
        }
        if ("s".equals(type)) {
            try {
                int idx = Integer.parseInt(v.trim());
                return idx >= 0 && idx < shared.size() ? shared.get(idx) : "";
            } catch (NumberFormatException e) {
                return "";
            }
        }
        if ("b".equals(type)) {
            return "1".equals(v.trim()) ? "TRUE" : "FALSE";
        }
        if ("e".equals(type)) {
            return "#ERROR";
        }
        return v;
    }

    /** "B12" → 1（0 起）；无列引用返回 -1。 */
    private static int columnIndex(String ref) {
        if (ref == null || ref.isEmpty()) {
            return -1;
        }
        int col = 0;
        int i = 0;
        while (i < ref.length()) {
            char c = Character.toUpperCase(ref.charAt(i));
            if (c < 'A' || c > 'Z') {
                break;
            }
            col = col * 26 + (c - 'A' + 1);
            i++;
        }
        return i == 0 ? -1 : col - 1;
    }

    // ==================== ZIP / XML 基础设施 ====================

    /** 读取 ZIP 全部文件条目（限制条目数 / 解压总量）。 */
    private static Map<String, byte[]> readZip(byte[] data) {
        Map<String, byte[]> out = new LinkedHashMap<>();
        long total = 0;
        int count = 0;
        try (ZipInputStream in = new ZipInputStream(new ByteArrayInputStream(data))) {
            ZipEntry e;
            while ((e = in.getNextEntry()) != null) {
                if (e.isDirectory()) {
                    continue;
                }
                if (++count > MAX_ZIP_ENTRIES) {
                    throw new IllegalArgumentException("压缩包条目过多，已拒绝解析。");
                }
                long remaining = MAX_UNZIPPED_BYTES - total;
                if (remaining <= 0) {
                    throw new IllegalArgumentException("压缩包解压后过大，已拒绝解析。");
                }
                byte[] bytes = readLimited(in, remaining);
                total += bytes.length;
                out.put(e.getName().replace('\\', '/'), bytes);
            }
        } catch (IOException ex) {
            throw new IllegalArgumentException("不是有效的 Office 文件（ZIP 解析失败）: " + ex.getMessage(), ex);
        }
        if (out.isEmpty()) {
            throw new IllegalArgumentException("不是有效的 Office 文件（压缩包为空）。");
        }
        return out;
    }

    private static byte[] readLimited(InputStream in, long limit) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[8192];
        long read = 0;
        int n;
        while ((n = in.read(buf)) != -1) {
            read += n;
            if (read > limit) {
                throw new IllegalArgumentException("压缩包解压后过大，已拒绝解析。");
            }
            out.write(buf, 0, n);
        }
        return out.toByteArray();
    }

    /** 安全 XML 解析：禁 DOCTYPE / 外部实体（防 XXE），不展开实体引用。 */
    private static Document parseXml(byte[] xml, String what) {
        try {
            DocumentBuilderFactory f = DocumentBuilderFactory.newInstance();
            f.setNamespaceAware(true);
            f.setExpandEntityReferences(false);
            setFeature(f, "http://apache.org/xml/features/disallow-doctype-decl", true);
            setFeature(f, "http://xml.org/sax/features/external-general-entities", false);
            setFeature(f, "http://xml.org/sax/features/external-parameter-entities", false);
            setFeature(f, "http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
            f.setXIncludeAware(false);
            return f.newDocumentBuilder().parse(new ByteArrayInputStream(xml));
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException("解析 " + what + " 内容失败: " + e.getMessage(), e);
        }
    }

    private static void setFeature(DocumentBuilderFactory f, String name, boolean value) {
        try {
            f.setFeature(name, value);
        } catch (Exception ignored) {
            // 解析器不支持该 feature 时跳过（默认值通常已安全）
        }
    }

    // ==================== DOM 小工具 ====================

    private static List<Element> childElements(Element parent) {
        List<Element> out = new ArrayList<>();
        NodeList nodes = parent.getChildNodes();
        for (int i = 0; i < nodes.getLength(); i++) {
            Node n = nodes.item(i);
            if (n instanceof Element e) {
                out.add(e);
            }
        }
        return out;
    }

    private static Element firstChildElement(Element parent, String localName) {
        for (Element e : childElements(parent)) {
            if (localName.equals(localName(e))) {
                return e;
            }
        }
        return null;
    }

    private static Element firstDescendant(Element parent, String localName) {
        for (Element e : childElements(parent)) {
            if (localName.equals(localName(e))) {
                return e;
            }
            Element found = firstDescendant(e, localName);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private static String childText(Element parent, String localName) {
        Element e = firstChildElement(parent, localName);
        return e == null ? null : e.getTextContent();
    }

    private static String localName(Element e) {
        String ln = e.getLocalName();
        if (ln != null) {
            return ln;
        }
        String name = e.getNodeName();
        int idx = name.indexOf(':');
        return idx >= 0 ? name.substring(idx + 1) : name;
    }

    /** 按 localName 取属性（用于 {@code r:id} 这类带命名空间前缀的属性）。 */
    private static String attrByLocalName(Element e, String localName) {
        NamedNodeMap attrs = e.getAttributes();
        for (int i = 0; i < attrs.getLength(); i++) {
            Node a = attrs.item(i);
            String ln = a.getLocalName();
            if (ln == null) {
                String name = a.getNodeName();
                int idx = name.indexOf(':');
                ln = idx >= 0 ? name.substring(idx + 1) : name;
            }
            if (localName.equals(ln)) {
                return a.getNodeValue();
            }
        }
        return null;
    }

    // ==================== 通用 ====================

    private static String safeName(String fileName) {
        String name = fileName == null ? "" : fileName.trim();
        int slash = Math.max(name.lastIndexOf('/'), name.lastIndexOf('\\'));
        if (slash >= 0) {
            name = name.substring(slash + 1);
        }
        name = name.replaceAll("[\\r\\n\\t]", " ");
        if (name.length() > 200) {
            name = name.substring(0, 200);
        }
        return name.isBlank() ? "附件" : name;
    }

    private static String extension(String name) {
        int dot = name.lastIndexOf('.');
        if (dot <= 0 || dot == name.length() - 1) {
            return "";
        }
        return name.substring(dot + 1).toLowerCase(Locale.ROOT);
    }

    private static String formatLabel(String ext) {
        return switch (ext) {
            case "md", "markdown" -> "markdown";
            case "docx" -> "word";
            case "xlsx" -> "excel";
            case "txt", "text", "log" -> "text";
            default -> ext;
        };
    }

    /** 收尾整理：统一换行、去行尾空白、压缩连续空行、剪掉首尾空行。 */
    private static String tidy(String text) {
        String s = text.replace("\r\n", "\n").replace('\r', '\n');
        s = s.replaceAll("[ \\t]+\\n", "\n");
        s = s.replaceAll("\\n{3,}", "\n\n");
        int start = 0;
        int end = s.length();
        while (start < end && Character.isWhitespace(s.charAt(start))) {
            start++;
        }
        while (end > start && Character.isWhitespace(s.charAt(end - 1))) {
            end--;
        }
        return s.substring(start, end);
    }
}
