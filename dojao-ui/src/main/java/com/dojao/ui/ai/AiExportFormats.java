package com.dojao.ui.ai;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import javax.imageio.ImageIO;

/**
 * AI 导出文件的「格式生成器」（纯函数，<b>离线可测</b>）：把文本内容或查询结果集渲染成
 * Markdown / 纯文本 / HTML / CSV / Excel(xlsx)，把图表数据渲染成 PNG / JPG 图片，
 * 或把图表与正文排成 PDF。
 *
 * <p>刻意<b>不引入任何第三方依赖</b>：xlsx 用 {@link ZipOutputStream} 直接写 OOXML 最小包，
 * Markdown→HTML 用本类的轻量转换（标题 / 列表 / 表格 / 代码块 / 引用 / 粗斜体 / 行内代码 / 链接），
 * 图表用 Java2D 绘制，PDF 由 {@link AiPdfWriter} 把页面位图（JPEG）装进最小 PDF 结构。
 * 这样离线 {@code mvn -o} 构建、分发包体积都不受影响。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/18
 */
public final class AiExportFormats {

    private AiExportFormats() {
    }

    /** 工具参数 / 文件名扩展名可用格式（规范名；excel 对应 .xlsx，jpg 对应 .jpg）。 */
    public static final List<String> FORMATS =
            List.of("md", "txt", "html", "csv", "excel", "png", "jpg", "pdf");

    /** 图表数据结构（与 {@code render_chart} 的 spec 同源，供图片 / PDF 渲染）。 */
    public record ChartData(String type, String title, List<String> categories,
                            List<Series> series, boolean dualAxis) {

        /** 一条数值序列；{@code data} 中非数值 / 缺失为 {@code null}。 */
        public record Series(String name, String type, List<Double> data, boolean y2) {
        }
    }

    /** Excel 单元格最大字符数（OOXML 规范上限）。 */
    private static final int XLSX_MAX_CELL_CHARS = 32_767;

    private static final Pattern HEADING = Pattern.compile("^(#{1,6})\\s+(.*)$");
    private static final Pattern UNORDERED = Pattern.compile("^\\s*[-*+]\\s+(.*)$");
    private static final Pattern ORDERED = Pattern.compile("^\\s*\\d+[.)]\\s+(.*)$");
    private static final Pattern TABLE_SEP = Pattern.compile(
            "^\\s*\\|?\\s*:?-{2,}:?\\s*(\\|\\s*:?-{2,}:?\\s*)*\\|?\\s*$");
    private static final Pattern NUMERIC_CELL = Pattern.compile("^-?\\d{1,15}(\\.\\d{1,10})?$");
    /** 危险块级标签（连同内容一起删掉）。 */
    private static final Pattern DANGEROUS_BLOCK = Pattern.compile(
            "(?is)<(iframe|object|embed|form|base)\\b[^>]*>.*?</\\1\\s*>");
    /** 危险标签（自闭合 / 无配对时兜底）。 */
    private static final Pattern DANGEROUS_TAG = Pattern.compile(
            "(?is)<(iframe|object|embed|form|base)\\b[^>]*/?>");
    /** {@code href/src="javascript:..."} → {@code #}。 */
    private static final Pattern JS_URL = Pattern.compile(
            "(?i)(href|src)\\s*=\\s*([\"'])\\s*javascript:[^\"']*\\2");

    /**
     * 归一化格式名（接受常见别名：markdown / text / htm / xlsx / xls），不支持返回 {@code null}。
     */
    public static String normalizeFormat(String format) {
        if (format == null) {
            return null;
        }
        String f = format.trim().toLowerCase(Locale.ROOT);
        if (f.startsWith(".")) {
            f = f.substring(1);
        }
        return switch (f) {
            case "md", "markdown" -> "md";
            case "txt", "text", "plain" -> "txt";
            case "html", "htm" -> "html";
            case "csv" -> "csv";
            case "excel", "xlsx", "xls" -> "excel";
            case "png" -> "png";
            case "jpg", "jpeg" -> "jpg";
            case "pdf" -> "pdf";
            default -> null;
        };
    }

    /** 规范格式名 → 文件扩展名（不含点）；不支持返回 {@code null}。 */
    public static String extension(String format) {
        String f = normalizeFormat(format);
        if (f == null) {
            return null;
        }
        return f.equals("excel") ? "xlsx" : f;
    }

    /** 规范格式名 → HTTP MIME 类型（下载响应头用）。 */
    public static String mimeType(String format) {
        String f = normalizeFormat(format);
        if (f == null) {
            return "application/octet-stream";
        }
        return switch (f) {
            case "md" -> "text/markdown; charset=utf-8";
            case "txt" -> "text/plain; charset=utf-8";
            case "html" -> "text/html; charset=utf-8";
            case "csv" -> "text/csv; charset=utf-8";
            case "excel" -> "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
            case "png" -> "image/png";
            case "jpg" -> "image/jpeg";
            case "pdf" -> "application/pdf";
            default -> "application/octet-stream";
        };
    }

    /**
     * 生成规范文件名：优先 {@code requested}，其次 {@code title}，否则 {@code dojao-export}；
     * 去掉路径分隔符 / 控制字符 / Windows 非法字符，去掉已有的同格式扩展名后补齐正确扩展名。
     */
    public static String fileName(String format, String requested, String title) {
        String ext = extension(format);
        if (ext == null) {
            throw new IllegalArgumentException("不支持的文件格式: " + format);
        }
        String base = (requested == null || requested.isBlank()) ? title : requested;
        if (base == null || base.isBlank()) {
            base = "dojao-export";
        }
        base = base.strip();
        // 只取最后一段文件名（模型可能给出路径）
        int slash = Math.max(base.lastIndexOf('/'), base.lastIndexOf('\\'));
        if (slash >= 0) {
            base = base.substring(slash + 1);
        }
        base = base.replaceAll("[\\x00-\\x1f\\x7f<>:\"/\\\\|?*]", "_");
        base = base.replaceAll("\\.+$", "").strip();
        String lower = base.toLowerCase(Locale.ROOT);
        for (String candidate : List.of(ext, "xlsx", "md", "markdown", "txt", "text", "html", "htm",
                "csv", "png", "jpg", "jpeg", "pdf")) {
            String suffix = "." + candidate;
            if (lower.endsWith(suffix)) {
                base = base.substring(0, base.length() - suffix.length()).strip();
                break;
            }
        }
        if (base.isBlank()) {
            base = "dojao-export";
        }
        if (base.length() > 80) {
            base = base.substring(0, 80).strip();
        }
        return base + "." + ext;
    }

    /**
     * 生成文件字节（无图表数据）。文本格式优先用 {@code content}，否则用 {@code columns/rows}
     * 渲染表格；Excel 必须提供表格数据。
     *
     * @throws IllegalArgumentException 格式不支持 / 无可用内容
     */
    public static byte[] generate(String format, String title, String content,
                                  List<String> columns, List<List<String>> rows) {
        return generate(format, title, content, columns, rows, null);
    }

    /**
     * 生成文件字节。文本格式优先用 {@code content}，否则用 {@code columns/rows} 渲染表格；
     * Excel 必须提供表格数据；PNG / JPG 必须提供 {@code chart}；PDF 可用 {@code chart}
     * （图表页）和 / 或 {@code content}、表格（文档页）任意组合。
     *
     * @throws IllegalArgumentException 格式不支持 / 无可用内容
     */
    public static byte[] generate(String format, String title, String content,
                                  List<String> columns, List<List<String>> rows, ChartData chart) {
        String f = normalizeFormat(format);
        if (f == null) {
            throw new IllegalArgumentException("不支持的文件格式: " + format);
        }
        boolean hasContent = content != null && !content.isBlank();
        boolean hasTable = columns != null && !columns.isEmpty();
        return switch (f) {
            case "md" -> {
                if (!hasContent && !hasTable) {
                    throw new IllegalArgumentException("没有可导出的内容（content 与 data 均为空）。");
                }
                yield utf8(hasContent ? markdownDocument(title, content) : markdownTable(title, columns, rows));
            }
            case "txt" -> {
                if (!hasContent && !hasTable) {
                    throw new IllegalArgumentException("没有可导出的内容（content 与 data 均为空）。");
                }
                String body = hasContent ? content : plainTable(columns, rows);
                yield utf8(plainDocument(title, body));
            }
            case "html" -> {
                if (!hasContent && !hasTable) {
                    throw new IllegalArgumentException("没有可导出的内容（content 与 data 均为空）。");
                }
                if (!hasContent) {
                    yield utf8(htmlDocument(title, htmlTable(columns, rows)));
                }
                // 模型给的是完整 HTML 文档（含 <script> 图表等）：原样输出，不能当 Markdown 转义
                if (looksLikeHtmlDocument(content)) {
                    yield utf8(sanitizeHtmlDocument(ensureCharset(content)));
                }
                // HTML 片段：保留标签，仅补一层文档外壳
                if (looksLikeHtmlFragment(content)) {
                    yield utf8(htmlDocument(title, sanitizeHtmlDocument(content)));
                }
                yield utf8(htmlDocument(title, markdownToHtml(content)));
            }
            case "csv" -> {
                if (!hasTable && !hasContent) {
                    throw new IllegalArgumentException("没有可导出的内容（content 与 data 均为空）。");
                }
                // UTF-8 BOM：让 Excel 直接双击打开中文不乱码
                yield utf8("\uFEFF" + (hasTable ? csv(columns, rows) : content.strip()));
            }
            case "excel" -> {
                if (!hasTable) {
                    throw new IllegalArgumentException("导出 Excel 需要表格数据（先用 execute_sql 取数，再用 data=\"rs<编号>\" 或 \"last\" 指定）。");
                }
                yield xlsx(title, columns, rows);
            }
            case "png", "jpg" -> {
                if (chart == null) {
                    throw new IllegalArgumentException("导出图片需要图表参数（type / category / values）与数据。");
                }
                yield image(AiChartRenderer.renderChart(chart), f);
            }
            case "pdf" -> {
                List<BufferedImage> pages = AiChartRenderer.documentPages(title, content, columns, rows, chart);
                if (pages.isEmpty()) {
                    throw new IllegalArgumentException(
                            "导出 PDF 需要 content / 表格数据，或图表参数（type / category / values）与数据。");
                }
                yield AiPdfWriter.pdf(pages);
            }
            default -> throw new IllegalArgumentException("不支持的文件格式: " + format);
        };
    }

    /** 位图编码为 PNG / JPG 字节。 */
    private static byte[] image(BufferedImage img, String format) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ImageIO.write(img, format, bos);
            return bos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("生成图片失败: " + e.getMessage(), e);
        }
    }

    // ==================== Markdown / 文本 ====================

    /** 有标题且正文没有以一级标题开头时补一行标题。 */
    private static String markdownDocument(String title, String content) {
        String body = content == null ? "" : content;
        if (title == null || title.isBlank() || body.stripLeading().startsWith("#")) {
            return body.strip();
        }
        return "# " + title.strip() + "\n\n" + body.strip();
    }

    /** 纯文本：标题一行 + 空行 + 正文。 */
    private static String plainDocument(String title, String body) {
        String b = body == null ? "" : body.strip();
        if (title == null || title.isBlank()) {
            return b + "\n";
        }
        return title.strip() + "\n" + "=".repeat(Math.min(title.strip().length(), 40)) + "\n\n" + b + "\n";
    }

    /** 结果集 → GFM 表格（含标题）。 */
    private static String markdownTable(String title, List<String> columns, List<List<String>> rows) {
        StringBuilder sb = new StringBuilder();
        if (title != null && !title.isBlank()) {
            sb.append("# ").append(title.strip()).append("\n\n");
        }
        sb.append("| ").append(columns.stream().map(AiExportFormats::mdCell)
                .reduce((a, b) -> a + " | " + b).orElse("")).append(" |\n");
        sb.append("|");
        for (int i = 0; i < columns.size(); i++) {
            sb.append(" --- |");
        }
        sb.append('\n');
        if (rows != null) {
            for (List<String> row : rows) {
                sb.append("|");
                for (int i = 0; i < columns.size(); i++) {
                    sb.append(' ').append(mdCell(cell(row, i))).append(" |");
                }
                sb.append('\n');
            }
        }
        return sb.toString().strip();
    }

    private static String mdCell(String s) {
        return s == null ? "" : s.replace("\\", "\\\\").replace("|", "\\|").replaceAll("\\R", " ");
    }

    /** 结果集 → 制表符分隔的纯文本。 */
    private static String plainTable(List<String> columns, List<List<String>> rows) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sb.append('\t');
            }
            sb.append(columns.get(i));
        }
        sb.append('\n');
        if (rows != null) {
            for (List<String> row : rows) {
                for (int i = 0; i < columns.size(); i++) {
                    if (i > 0) {
                        sb.append('\t');
                    }
                    String v = cell(row, i);
                    sb.append(v.replace('\t', ' ').replace('\n', ' '));
                }
                sb.append('\n');
            }
        }
        return sb.toString();
    }

    // ==================== CSV ====================

    /** 结果集 → RFC 4180 风格 CSV（逗号 / 引号 / 换行做转义）。 */
    public static String csv(List<String> columns, List<List<String>> rows) {
        StringBuilder sb = new StringBuilder();
        appendCsvRow(sb, columns);
        if (rows != null) {
            for (List<String> row : rows) {
                List<String> cells = new ArrayList<>(columns.size());
                for (int i = 0; i < columns.size(); i++) {
                    cells.add(cell(row, i));
                }
                appendCsvRow(sb, cells);
            }
        }
        return sb.toString();
    }

    private static void appendCsvRow(StringBuilder sb, List<String> cells) {
        for (int i = 0; i < cells.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            String v = cells.get(i);
            if (v == null) {
                v = "";
            }
            boolean needQuote = v.contains(",") || v.contains("\"") || v.contains("\n") || v.contains("\r");
            if (needQuote) {
                sb.append('"').append(v.replace("\"", "\"\"")).append('"');
            } else {
                sb.append(v);
            }
        }
        sb.append("\r\n");
    }

    // ==================== HTML ====================

    /** 完整 HTML 文档（内联样式，UTF-8，可直接浏览器打开）。 */
    private static String htmlDocument(String title, String bodyHtml) {
        String t = title == null || title.isBlank() ? "Dojao 导出" : title.strip();
        return """
                <!DOCTYPE html>
                <html lang="zh-CN">
                <head>
                <meta charset="utf-8">
                <meta name="viewport" content="width=device-width, initial-scale=1">
                <title>%s</title>
                <style>
                body{font-family:-apple-system,"Segoe UI","Microsoft YaHei",sans-serif;line-height:1.7;
                max-width:960px;margin:32px auto;padding:0 20px;color:#1f2937;background:#fff;}
                h1,h2,h3,h4{line-height:1.3;margin:1.4em 0 .6em;}
                h1{border-bottom:2px solid #e5e7eb;padding-bottom:.3em;}
                table{border-collapse:collapse;width:100%%;margin:1em 0;}
                th,td{border:1px solid #d1d5db;padding:6px 10px;text-align:left;}
                th{background:#f3f4f6;}
                tr:nth-child(even) td{background:#fafafa;}
                pre{background:#f6f8fa;padding:12px 14px;border-radius:6px;overflow:auto;}
                code{font-family:ui-monospace,Consolas,monospace;font-size:.92em;}
                blockquote{border-left:4px solid #d1d5db;margin:1em 0;padding:.2em 1em;color:#4b5563;}
                a{color:#2563eb;}
                </style>
                </head>
                <body>
                %s
                </body>
                </html>
                """.formatted(esc(t), bodyHtml == null ? "" : bodyHtml);
    }

    /** 是否「完整 HTML 文档」：以 {@code <!DOCTYPE} 或 {@code <html} 开头（忽略前导空白/大小写）。 */
    private static boolean looksLikeHtmlDocument(String content) {
        String t = content.stripLeading().toLowerCase(Locale.ROOT);
        return t.startsWith("<!doctype") || t.startsWith("<html");
    }

    /**
     * 是否「HTML 片段」：以标签开头且含成对/自闭合标签。
     * 用来区分「模型直接给的 HTML」与「Markdown」——前者不能再走 Markdown 转义。
     */
    private static boolean looksLikeHtmlFragment(String content) {
        String t = content.stripLeading();
        return t.startsWith("<") && (t.contains("</") || t.contains("/>"))
                && Pattern.compile("^<[A-Za-z][^>]*>").matcher(t).find();
    }

    /**
     * 完整 HTML 文档的轻量净化：去掉 {@code iframe / object / embed / form / base}
     * 与 {@code javascript:} 链接；<b>保留 {@code <script>} / {@code <style>}</b>
     * （模型用它内嵌图表数据，删掉就渲染不出图表了）。
     *
     * <p>导出文件由用户在浏览器本地打开，脚本与页面同源；这里只拦截「把外部页面嵌进来」
     * 一类明显多余的危险结构，不做完整的 HTML 允许列表。</p>
     */
    static String sanitizeHtmlDocument(String html) {
        if (html == null) {
            return "";
        }
        String s = DANGEROUS_BLOCK.matcher(html).replaceAll("");
        s = DANGEROUS_TAG.matcher(s).replaceAll("");
        s = JS_URL.matcher(s).replaceAll("$1=\"#\"");
        return s;
    }

    /** 完整 HTML 文档若没声明字符集，在 {@code <head>} 后补 UTF-8，避免中文乱码。 */
    private static String ensureCharset(String html) {
        String lower = html.toLowerCase(Locale.ROOT);
        if (lower.contains("<meta charset") || lower.contains("charset=")) {
            return html;
        }
        int head = lower.indexOf("<head");
        if (head < 0) {
            return html;
        }
        int close = lower.indexOf('>', head);
        if (close < 0) {
            return html;
        }
        return html.substring(0, close + 1) + "\n<meta charset=\"utf-8\">" + html.substring(close + 1);
    }

    /** 结果集 → HTML 表格。 */
    private static String htmlTable(List<String> columns, List<List<String>> rows) {
        StringBuilder sb = new StringBuilder("<table>\n<thead><tr>");
        for (String c : columns) {
            sb.append("<th>").append(esc(c)).append("</th>");
        }
        sb.append("</tr></thead>\n<tbody>\n");
        if (rows != null) {
            for (List<String> row : rows) {
                sb.append("<tr>");
                for (int i = 0; i < columns.size(); i++) {
                    sb.append("<td>").append(esc(cell(row, i))).append("</td>");
                }
                sb.append("</tr>\n");
            }
        }
        sb.append("</tbody>\n</table>\n");
        return sb.toString();
    }

    /**
     * 轻量 Markdown → HTML（标题 / 无序与有序列表 / GFM 表格 / 围栏代码块 / 引用 / 分隔线 /
     * 段落 / 粗体 / 斜体 / 行内代码 / http(s) 链接）。先转义再变换，<b>不引入脚本风险</b>。
     */
    public static String markdownToHtml(String md) {
        if (md == null || md.isBlank()) {
            return "";
        }
        String[] lines = md.replace("\r\n", "\n").replace('\r', '\n').split("\n", -1);
        StringBuilder out = new StringBuilder();
        int i = 0;
        while (i < lines.length) {
            String line = lines[i];
            String trimmed = line.strip();
            if (trimmed.isEmpty()) {
                i++;
                continue;
            }
            // 围栏代码块
            if (trimmed.startsWith("```")) {
                String lang = trimmed.substring(3).strip();
                i++;
                StringBuilder code = new StringBuilder();
                while (i < lines.length && !lines[i].strip().startsWith("```")) {
                    code.append(lines[i]).append('\n');
                    i++;
                }
                if (i < lines.length) {
                    i++;
                }
                out.append("<pre><code");
                if (!lang.isEmpty()) {
                    out.append(" class=\"language-").append(esc(lang)).append('"');
                }
                out.append('>').append(esc(code.toString())).append("</code></pre>\n");
                continue;
            }
            // 标题
            Matcher h = HEADING.matcher(trimmed);
            if (h.matches()) {
                int level = h.group(1).length();
                out.append("<h").append(level).append('>')
                        .append(inline(h.group(2))).append("</h").append(level).append(">\n");
                i++;
                continue;
            }
            // 分隔线
            if (trimmed.matches("^([-*_])(\\s*\\1){2,}$")) {
                out.append("<hr>\n");
                i++;
                continue;
            }
            // GFM 表格（当前行含 | 且下一行是分隔行）
            if (trimmed.contains("|") && i + 1 < lines.length && TABLE_SEP.matcher(lines[i + 1].strip()).matches()) {
                i = appendMarkdownTable(lines, i, out);
                continue;
            }
            // 无序列表
            if (UNORDERED.matcher(trimmed).matches()) {
                out.append("<ul>\n");
                while (i < lines.length) {
                    Matcher m = UNORDERED.matcher(lines[i].strip());
                    if (!m.matches()) {
                        break;
                    }
                    out.append("<li>").append(inline(m.group(1))).append("</li>\n");
                    i++;
                }
                out.append("</ul>\n");
                continue;
            }
            // 有序列表
            if (ORDERED.matcher(trimmed).matches()) {
                out.append("<ol>\n");
                while (i < lines.length) {
                    Matcher m = ORDERED.matcher(lines[i].strip());
                    if (!m.matches()) {
                        break;
                    }
                    out.append("<li>").append(inline(m.group(1))).append("</li>\n");
                    i++;
                }
                out.append("</ol>\n");
                continue;
            }
            // 引用
            if (trimmed.startsWith(">")) {
                List<String> quoteLines = new ArrayList<>();
                while (i < lines.length && lines[i].strip().startsWith(">")) {
                    quoteLines.add(inline(lines[i].strip().substring(1).strip()));
                    i++;
                }
                out.append("<blockquote>").append(String.join("<br>\n", quoteLines))
                        .append("</blockquote>\n");
                continue;
            }
            // 段落：连续非空、非块起始的行
            StringBuilder para = new StringBuilder();
            while (i < lines.length) {
                String cur = lines[i].strip();
                if (cur.isEmpty() || HEADING.matcher(cur).matches() || cur.startsWith("```")
                        || cur.startsWith(">") || UNORDERED.matcher(cur).matches()
                        || ORDERED.matcher(cur).matches() || cur.matches("^([-*_])(\\s*\\1){2,}$")
                        || (cur.contains("|") && i + 1 < lines.length
                        && TABLE_SEP.matcher(lines[i + 1].strip()).matches())) {
                    break;
                }
                if (!para.isEmpty()) {
                    para.append(' ');
                }
                para.append(inline(cur));
                i++;
            }
            if (!para.isEmpty()) {
                out.append("<p>").append(para).append("</p>\n");
            } else {
                // 防死循环：无法识别的行原样输出
                out.append("<p>").append(inline(trimmed)).append("</p>\n");
                i++;
            }
        }
        return out.toString().strip();
    }

    private static int appendMarkdownTable(String[] lines, int start, StringBuilder out) {
        List<String> header = splitTableRow(lines[start]);
        int i = start + 2;   // 跳过表头与分隔行
        out.append("<table>\n<thead><tr>");
        for (String h : header) {
            out.append("<th>").append(inline(h)).append("</th>");
        }
        out.append("</tr></thead>\n<tbody>\n");
        while (i < lines.length) {
            String cur = lines[i].strip();
            if (cur.isEmpty() || !cur.contains("|")) {
                break;
            }
            List<String> cells = splitTableRow(lines[i]);
            out.append("<tr>");
            for (int c = 0; c < header.size(); c++) {
                out.append("<td>").append(inline(c < cells.size() ? cells.get(c) : "")).append("</td>");
            }
            out.append("</tr>\n");
            i++;
        }
        out.append("</tbody>\n</table>\n");
        return i;
    }

    /** 按未转义的 | 切分表格行，去掉首尾空段。 */
    private static List<String> splitTableRow(String line) {
        String s = line.strip();
        if (s.startsWith("|")) {
            s = s.substring(1);
        }
        if (s.endsWith("|") && !s.endsWith("\\|")) {
            s = s.substring(0, s.length() - 1);
        }
        List<String> cells = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < s.length() && s.charAt(i + 1) == '|') {
                cur.append('|');
                i++;
            } else if (c == '|') {
                cells.add(cur.toString().strip());
                cur.setLength(0);
            } else {
                cur.append(c);
            }
        }
        cells.add(cur.toString().strip());
        return cells;
    }

    /** 行内元素：先转义，再套用代码 / 链接 / 粗体 / 斜体。 */
    private static String inline(String s) {
        String e = esc(s == null ? "" : s);
        e = e.replaceAll("`([^`]+)`", "<code>$1</code>");
        e = e.replaceAll("\\[([^\\]]+)\\]\\((https?://[^)\\s]+)\\)",
                "<a href=\"$2\" target=\"_blank\" rel=\"noopener noreferrer\">$1</a>");
        e = e.replaceAll("\\*\\*([^*]+)\\*\\*", "<strong>$1</strong>");
        e = e.replaceAll("(?<!\\*)\\*([^*]+)\\*(?!\\*)", "<em>$1</em>");
        return e;
    }

    // ==================== XLSX（OOXML 最小包，无第三方依赖）====================

    /** 结果集 → xlsx 字节流（单工作表 Sheet1，表头 + 数据行；纯数字写数值单元格）。 */
    public static byte[] xlsx(String title, List<String> columns, List<List<String>> rows) {
        StringBuilder sheet = new StringBuilder();
        sheet.append("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>");
        sheet.append("<worksheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\">");
        sheet.append("<sheetData>");
        int rowNo = 1;
        appendXlsxRow(sheet, rowNo++, columns);
        if (rows != null) {
            for (List<String> row : rows) {
                List<String> cells = new ArrayList<>(columns.size());
                for (int i = 0; i < columns.size(); i++) {
                    cells.add(cell(row, i));
                }
                appendXlsxRow(sheet, rowNo++, cells);
            }
        }
        sheet.append("</sheetData></worksheet>");

        String contentTypes = """
                <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
                <Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
                <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
                <Default Extension="xml" ContentType="application/xml"/>
                <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
                <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
                <Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
                </Types>
                """;
        String rels = """
                <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
                <Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
                <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
                </Relationships>
                """;
        String workbook = """
                <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
                <workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
                <sheets><sheet name="Sheet1" sheetId="1" r:id="rId1"/></sheets>
                </workbook>
                """;
        String workbookRels = """
                <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
                <Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
                <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
                <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
                </Relationships>
                """;
        String styles = """
                <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
                <styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
                <fonts count="1"><font><sz val="11"/><name val="Calibri"/></font></fonts>
                <fills count="2"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill></fills>
                <borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>
                <cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>
                <cellXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/></cellXfs>
                <cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>
                </styleSheet>
                """;
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            try (ZipOutputStream zos = new ZipOutputStream(bos)) {
                zos.setLevel(Deflater.DEFAULT_COMPRESSION);
                putZipEntry(zos, "[Content_Types].xml", contentTypes);
                putZipEntry(zos, "_rels/.rels", rels);
                putZipEntry(zos, "xl/workbook.xml", workbook);
                putZipEntry(zos, "xl/_rels/workbook.xml.rels", workbookRels);
                putZipEntry(zos, "xl/styles.xml", styles);
                putZipEntry(zos, "xl/worksheets/sheet1.xml", sheet.toString());
            }
            return bos.toByteArray();
        } catch (Exception e) {
            throw new IllegalStateException("生成 Excel 失败: " + e.getMessage(), e);
        }
    }

    private static void appendXlsxRow(StringBuilder sb, int rowNo, List<String> cells) {
        sb.append("<row r=\"").append(rowNo).append("\">");
        for (int i = 0; i < cells.size(); i++) {
            String ref = columnLetter(i) + rowNo;
            String v = cells.get(i);
            if (v == null || v.isEmpty()) {
                sb.append("<c r=\"").append(ref).append("\"/>");
                continue;
            }
            if (NUMERIC_CELL.matcher(v).matches()) {
                sb.append("<c r=\"").append(ref).append("\"><v>").append(v).append("</v></c>");
            } else {
                String text = v.length() > XLSX_MAX_CELL_CHARS ? v.substring(0, XLSX_MAX_CELL_CHARS) : v;
                sb.append("<c r=\"").append(ref).append("\" t=\"inlineStr\"><is><t xml:space=\"preserve\">")
                        .append(esc(text)).append("</t></is></c>");
            }
        }
        sb.append("</row>");
    }

    private static String columnLetter(int index) {
        StringBuilder sb = new StringBuilder();
        int n = index;
        while (true) {
            sb.insert(0, (char) ('A' + n % 26));
            n = n / 26 - 1;
            if (n < 0) {
                break;
            }
        }
        return sb.toString();
    }

    private static void putZipEntry(ZipOutputStream zos, String name, String content) throws Exception {
        zos.putNextEntry(new ZipEntry(name));
        zos.write(content.getBytes(StandardCharsets.UTF_8));
        zos.closeEntry();
    }

    // ==================== 公共助手 ====================

    private static String cell(List<String> row, int i) {
        if (row == null || i >= row.size()) {
            return "";
        }
        String v = row.get(i);
        return v == null ? "" : v;
    }

    private static String esc(String s) {
        if (s == null) {
            return "";
        }
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                .replace("\"", "&quot;").replace("'", "&#39;");
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
