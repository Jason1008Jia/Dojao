package com.dojao.connector.excel;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * 只读的 <b>.xlsx / .xlsm（OOXML）工作簿</b> —— 工作簿结构（工作表清单、共享字符串）解析。
 *
 * <p><b>为什么不用 Apache POI。</b>连接器是插件：多一个第三方依赖就多一份要随 jar 分发 / 与引擎版本
 * 对齐的负担。xlsx 本质是一个 ZIP + 若干 XML，用 JDK 自带的 {@code java.util.zip} 与 StAX
 * （{@code javax.xml.stream}）就能流式读完 —— 不把整张表 DOM 进内存，几百万行的表也只占一页的量。</p>
 *
 * <p><b>安全。</b>XML 解析一律关闭 DTD 与外部实体（{@link #newFactory()}）：工作簿是外部输入，
 * 不能因为一个 {@code <!DOCTYPE ... SYSTEM "file:///...">} 就把服务器上的文件读进查询结果。</p>
 *
 * <p>本类不是线程安全的；每次操作打开一份（reader 各自持有），连接器本身只缓存解析结果。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/21
 */
final class XlsxWorkbook implements AutoCloseable {

    /** 工作表：展示名（Excel 里的 sheet 名）+ ZIP 内的 XML 路径。 */
    record SheetRef(String name, String part) {
    }

    /** 工作簿文件扩展名（只接受 OOXML；.xls 是二进制 BIFF，明确拒绝）。 */
    private static final String[] EXTENSIONS = {".xlsx", ".xlsm"};

    private final ZipFile zip;
    private final List<String> sharedStrings;
    /** 单元格样式下标 → 是否为日期/时间格式（numeric 单元格按 Excel 日期序列值解读）。 */
    private final boolean[] dateStyles;
    /** 是否 1904 日期系统（Mac 版 Excel 导出，序列值基准是 1904-01-01）。 */
    private final boolean date1904;

    private XlsxWorkbook(ZipFile zip, List<String> sharedStrings,
                         boolean[] dateStyles, boolean date1904) {
        this.zip = zip;
        this.sharedStrings = sharedStrings;
        this.dateStyles = dateStyles;
        this.date1904 = date1904;
    }

    /** 打开一个工作簿（读共享字符串表 / 样式 / 日期系统）。调用方负责 {@link #close()}。 */
    static XlsxWorkbook open(Path file) throws IOException {
        ZipFile zip = new ZipFile(file.toFile());
        try {
            return new XlsxWorkbook(zip, readSharedStrings(zip), readDateStyles(zip),
                    readDate1904(zip));
        } catch (IOException | RuntimeException e) {
            zip.close();
            throw e;
        }
    }

    /** 工作表清单（按工作簿里的顺序）。 */
    List<SheetRef> sheets() throws IOException {
        return sheetRefs(zip);
    }

    /** 是否 1904 日期系统（读取侧把纯数字的日期列换算成时间时需要同一基准）。 */
    boolean date1904() {
        return date1904;
    }

    /** 打开某张工作表的行流。 */
    XlsxSheetStream openSheet(String part) throws IOException {
        return XlsxSheetStream.open(zip, part, sharedStrings, dateStyles, date1904);
    }

    @Override
    public void close() {
        try {
            zip.close();
        } catch (IOException ignored) {
            // 只读文件关闭失败无补救动作
        }
    }

    // ==================== 文件形态 ====================

    /** 是否是本连接器支持的工作簿扩展名（不区分大小写）。 */
    static boolean isWorkbookFile(String fileName) {
        if (fileName == null) return false;
        String lower = fileName.toLowerCase(Locale.ROOT);
        for (String ext : EXTENSIONS) {
            if (lower.endsWith(ext)) return true;
        }
        return false;
    }

    /** 去掉工作簿扩展名后的名字（作为 schema 名）。 */
    static String baseName(String fileName) {
        String lower = fileName.toLowerCase(Locale.ROOT);
        for (String ext : EXTENSIONS) {
            if (lower.endsWith(ext)) {
                return fileName.substring(0, fileName.length() - ext.length());
            }
        }
        return fileName;
    }

    // ==================== ZT 条目 ====================

    /**
     * 按名字找 ZIP 条目：先精确匹配，未命中再忽略大小写 / 前导斜杠匹配。
     *
     * <p>少数导出工具会写出 {@code /xl/workbook.xml} 或大写 {@code XL/WORKBOOK.XML}；
     * 严格匹配会把一个本质合法的工作簿判成坏文件。</p>
     */
    static ZipEntry findEntry(ZipFile zip, String name) {
        if (name == null) return null;
        ZipEntry direct = zip.getEntry(name);
        if (direct != null) return direct;
        String want = stripLeadingSlash(name);
        Enumeration<? extends ZipEntry> entries = zip.entries();
        while (entries.hasMoreElements()) {
            ZipEntry e = entries.nextElement();
            if (stripLeadingSlash(e.getName()).equalsIgnoreCase(want)) return e;
        }
        return null;
    }

    private static String stripLeadingSlash(String s) {
        String t = s.replace('\\', '/');
        while (t.startsWith("/")) t = t.substring(1);
        return t;
    }

    // ==================== 工作表清单 ====================

    private static List<SheetRef> sheetRefs(ZipFile zip) throws IOException {
        Map<String, String> rels = workbookRels(zip);
        List<SheetRef> out = new ArrayList<>();
        ZipEntry workbook = findEntry(zip, "xl/workbook.xml");
        if (workbook != null) {
            try (InputStream in = zip.getInputStream(workbook)) {
                XMLStreamReader r = newFactory().createXMLStreamReader(in);
                int index = 0;
                while (r.hasNext()) {
                    int ev = r.next();
                    if (ev != XMLStreamConstants.START_ELEMENT || !"sheet".equals(r.getLocalName())) {
                        continue;
                    }
                    index++;
                    String name = attr(r, "name");
                    String rid = attrByLocalName(r, "id");
                    String target = rid == null ? null : rels.get(rid);
                    String part = target == null ? null : normalizePartPath(target);
                    if (part == null || findEntry(zip, part) == null) {
                        part = "xl/worksheets/sheet" + index + ".xml";
                    }
                    if (findEntry(zip, part) != null) {
                        out.add(new SheetRef(
                                name == null || name.isBlank() ? "Sheet" + index : name, part));
                    }
                }
                r.close();
            } catch (XMLStreamException e) {
                throw new IOException("解析工作簿结构失败（xl/workbook.xml）: " + e.getMessage(), e);
            }
        }
        if (!out.isEmpty()) return out;

        // 兜底：缺少 workbook.xml / rels 的极少见形态 —— 直接按 sheetN.xml 文件名枚举
        List<String> parts = new ArrayList<>();
        Enumeration<? extends ZipEntry> entries = zip.entries();
        while (entries.hasMoreElements()) {
            String name = stripLeadingSlash(entries.nextElement().getName());
            if (name.matches("(?i)xl/worksheets/sheet\\d+\\.xml")) parts.add(name);
        }
        parts.sort((a, b) -> Integer.compare(sheetNumber(a), sheetNumber(b)));
        for (int i = 0; i < parts.size(); i++) {
            out.add(new SheetRef("Sheet" + (i + 1), parts.get(i)));
        }
        return out;
    }

    private static int sheetNumber(String path) {
        int i = path.lastIndexOf("sheet");
        int j = path.lastIndexOf('.');
        try {
            return Integer.parseInt(path.substring(i + 5, j));
        } catch (RuntimeException e) {
            return Integer.MAX_VALUE;
        }
    }

    private static Map<String, String> workbookRels(ZipFile zip) throws IOException {
        Map<String, String> out = new HashMap<>();
        ZipEntry rels = findEntry(zip, "xl/_rels/workbook.xml.rels");
        if (rels == null) return out;
        try (InputStream in = zip.getInputStream(rels)) {
            XMLStreamReader r = newFactory().createXMLStreamReader(in);
            while (r.hasNext()) {
                int ev = r.next();
                if (ev != XMLStreamConstants.START_ELEMENT
                        || !"Relationship".equals(r.getLocalName())) {
                    continue;
                }
                String id = attr(r, "Id");
                String target = attr(r, "Target");
                if (id != null && target != null) out.put(id, target);
            }
            r.close();
        } catch (XMLStreamException e) {
            // rels 损坏 → 交给工作表清单的文件名兜底
        }
        return out;
    }

    /**
     * 关系里的 Target 可能是 {@code worksheets/sheet1.xml}、{@code /xl/worksheets/sheet1.xml}
     * 或带 {@code ../} 的相对路径 —— 统一成 ZIP 内条目名。
     */
    static String normalizePartPath(String target) {
        String t = target.replace('\\', '/');
        if (!t.startsWith("/") && !t.toLowerCase(Locale.ROOT).startsWith("xl/")) {
            t = "xl/" + t;
        }
        Deque<String> stack = new ArrayDeque<>();
        for (String seg : t.split("/")) {
            if (seg.isEmpty() || ".".equals(seg)) continue;
            if ("..".equals(seg)) {
                if (!stack.isEmpty()) stack.removeLast();
            } else {
                stack.addLast(seg);
            }
        }
        return String.join("/", stack);
    }

    // ==================== 共享字符串 ====================

    private static List<String> readSharedStrings(ZipFile zip) throws IOException {
        List<String> out = new ArrayList<>();
        ZipEntry entry = findEntry(zip, "xl/sharedStrings.xml");
        if (entry == null) return out;
        try (InputStream in = zip.getInputStream(entry)) {
            XMLStreamReader r = newFactory().createXMLStreamReader(in);
            while (r.hasNext()) {
                int ev = r.next();
                if (ev == XMLStreamConstants.START_ELEMENT && "si".equals(r.getLocalName())) {
                    out.add(collectInlineText(r));
                }
            }
            r.close();
        } catch (XMLStreamException e) {
            throw new IOException("解析共享字符串失败（xl/sharedStrings.xml）: " + e.getMessage(), e);
        }
        return out;
    }

    /**
     * 收集一个 {@code <si>} / {@code <is>} 里的全部文本：拼接各 run 的 {@code <t>}，
     * 跳过拼音注音 {@code <rPh>}（Excel 用它存日文注音，混进数据会得到重复内容）。
     * 返回时解析器停在容器结束标签上。
     */
    static String collectInlineText(XMLStreamReader r) throws XMLStreamException {
        StringBuilder sb = new StringBuilder();
        int depth = 1;
        int tDepth = -1;
        int rphDepth = -1;
        while (r.hasNext()) {
            int ev = r.next();
            if (ev == XMLStreamConstants.START_ELEMENT) {
                depth++;
                String ln = r.getLocalName();
                if ("t".equals(ln)) {
                    tDepth = depth;
                } else if ("rPh".equals(ln)) {
                    rphDepth = depth;
                }
            } else if (ev == XMLStreamConstants.END_ELEMENT) {
                String ln = r.getLocalName();
                if ("t".equals(ln)) {
                    tDepth = -1;
                } else if ("rPh".equals(ln)) {
                    rphDepth = -1;
                }
                depth--;
                if (depth == 0) break;
            } else if (ev == XMLStreamConstants.CHARACTERS || ev == XMLStreamConstants.CDATA) {
                if (tDepth >= 0 && rphDepth < 0) sb.append(r.getText());
            }
        }
        return sb.toString();
    }

    // ==================== 样式 / 日期系统 ====================

    /**
     * 单元格样式下标 → 是否为日期/时间格式。
     *
     * <p>Excel 把日期<b>存成数字</b>（序列值），只有 {@code styles.xml} 里的 numberFormat 能说明
     * 它是日期。不读它，{@code 2026-10-21} 就会以 {@code 46305} 进入查询。这里只看「是不是日期/时间格式」，
     * 不需要完整实现样式表。</p>
     *
     * <p>内置 numFmtId 14–22 / 45–47 是国际通用日期时间；27–36 / 50–58 是 OOXML 为本地化日期
     * 预留的区间（中文版 Excel 的「yyyy"年"m"月"」就在这里）。自定义格式（id ≥ 164）解析
     * {@code formatCode}。</p>
     */
    private static boolean[] readDateStyles(ZipFile zip) throws IOException {
        ZipEntry entry = findEntry(zip, "xl/styles.xml");
        if (entry == null) return new boolean[0];
        Map<Integer, String> custom = new HashMap<>();
        List<Integer> xfNumFmtIds = new ArrayList<>();
        try (InputStream in = zip.getInputStream(entry)) {
            XMLStreamReader r = newFactory().createXMLStreamReader(in);
            boolean inCellXfs = false;
            while (r.hasNext()) {
                int ev = r.next();
                if (ev == XMLStreamConstants.START_ELEMENT) {
                    String ln = r.getLocalName();
                    if ("numFmt".equals(ln)) {
                        String id = attr(r, "numFmtId");
                        String code = attr(r, "formatCode");
                        if (id != null && code != null) {
                            try {
                                custom.put(Integer.parseInt(id.trim()), code);
                            } catch (NumberFormatException ignored) {
                                // 坏样式项不影响其余单元格
                            }
                        }
                    } else if ("cellXfs".equals(ln)) {
                        inCellXfs = true;
                    } else if (inCellXfs && "xf".equals(ln)) {
                        String id = attr(r, "numFmtId");
                        int parsed = -1;
                        if (id != null) {
                            try {
                                parsed = Integer.parseInt(id.trim());
                            } catch (NumberFormatException ignored) {
                                // 缺省按通用格式处理
                            }
                        }
                        xfNumFmtIds.add(parsed);
                    }
                } else if (ev == XMLStreamConstants.END_ELEMENT && "cellXfs".equals(r.getLocalName())) {
                    inCellXfs = false;
                }
            }
            r.close();
        } catch (XMLStreamException e) {
            // 样式损坏 → 不把整份工作簿判死：所有数字按数字读（日期列退化为序列值）
            return new boolean[0];
        }
        boolean[] out = new boolean[xfNumFmtIds.size()];
        for (int i = 0; i < out.length; i++) {
            int id = xfNumFmtIds.get(i);
            out[i] = isBuiltinDateNumFmt(id) || isDateFormatCode(custom.get(id));
        }
        return out;
    }

    private static boolean isBuiltinDateNumFmt(int id) {
        return (id >= 14 && id <= 22) || (id >= 27 && id <= 36)
                || (id >= 45 && id <= 47) || (id >= 50 && id <= 58);
    }

    /**
     * 自定义格式串是否描述日期/时间：跳过引号字面量、{@code [...]} 段与反斜杠转义后，
     * 只要存在一段**全部由 y/m/d/h/s 组成**的字母串即认为是日期时间格式。
     *
     * <p>按「整段字母」判定而不是「包含某个字母」，避免把 {@code Standard}、{@code General}、
     * 科学计数 {@code 0.00E+00} 这类纯数字格式误判成日期。</p>
     */
    static boolean isDateFormatCode(String code) {
        if (code == null || code.isEmpty()) return false;
        StringBuilder run = new StringBuilder();
        boolean quoted = false;
        for (int i = 0; i < code.length(); i++) {
            char ch = code.charAt(i);
            if (quoted) {
                if (ch == '"') quoted = false;
                continue;
            }
            if (ch == '"') {
                // 引号前的字母段已经结束，先判定再清空 —— 否则 yyyy"年"m"月"d"日" 这类
                // 「日期段紧跟字面量」的中文格式会被整段丢掉
                if (isDateLetterRun(run)) return true;
                quoted = true;
                run.setLength(0);
                continue;
            }
            if (ch == '\\') {           // 转义一个字符
                if (isDateLetterRun(run)) return true;
                i++;
                run.setLength(0);
                continue;
            }
            if (ch == '[') {            // [Red] / [$-409] / [h]（[h] 是经过时间，算日期时间）
                int end = code.indexOf(']', i);
                if (end < 0) break;
                String inner = code.substring(i + 1, end).toLowerCase(Locale.ROOT);
                if (!inner.isEmpty() && inner.chars().allMatch(c -> "ymdhs".indexOf(c) >= 0)) {
                    return true;
                }
                if (isDateLetterRun(run)) return true;
                i = end;
                run.setLength(0);
                continue;
            }
            if (Character.isLetter(ch)) {
                run.append(Character.toLowerCase(ch));
            } else {
                if (isDateLetterRun(run)) return true;
                run.setLength(0);
            }
        }
        return isDateLetterRun(run);
    }

    private static boolean isDateLetterRun(StringBuilder run) {
        if (run.length() == 0) return false;
        for (int i = 0; i < run.length(); i++) {
            if ("ymdhs".indexOf(run.charAt(i)) < 0) return false;
        }
        return true;
    }

    /** {@code workbookPr date1904="1"} = Mac 1904 日期系统。 */
    private static boolean readDate1904(ZipFile zip) throws IOException {
        ZipEntry workbook = findEntry(zip, "xl/workbook.xml");
        if (workbook == null) return false;
        try (InputStream in = zip.getInputStream(workbook)) {
            XMLStreamReader r = newFactory().createXMLStreamReader(in);
            while (r.hasNext()) {
                int ev = r.next();
                if (ev == XMLStreamConstants.START_ELEMENT && "workbookPr".equals(r.getLocalName())) {
                    String v = trim(attr(r, "date1904"));
                    boolean on = "1".equals(v) || "true".equalsIgnoreCase(v);
                    r.close();
                    return on;
                }
            }
            r.close();
        } catch (XMLStreamException e) {
            // 读不到 → 默认 1900 日期系统
        }
        return false;
    }

    private static String trim(String s) {
        return s == null ? "" : s.trim();
    }

    // ==================== 工具 ====================

    /** 关闭 DTD / 外部实体的 StAX 工厂（每个流一个新工厂；工厂本身不保证线程安全）。 */
    static XMLInputFactory newFactory() {
        XMLInputFactory f = XMLInputFactory.newFactory();
        trySet(f, XMLInputFactory.SUPPORT_DTD, false);
        trySet(f, XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
        trySet(f, XMLInputFactory.IS_COALESCING, true);
        return f;
    }

    private static void trySet(XMLInputFactory f, String name, Object value) {
        try {
            f.setProperty(name, value);
        } catch (IllegalArgumentException ignored) {
            // 某些实现不支持该属性 —— 不因此判定工作簿损坏
        }
    }

    /** 按限定名取属性（无命名空间）；不存在返回 null。 */
    static String attr(XMLStreamReader r, String name) {
        String v = r.getAttributeValue(null, name);
        if (v != null) return v;
        return attrByLocalName(r, name);
    }

    /** 按本地名取属性（关系 id 这类属性带命名空间，前缀因生成器而异）。 */
    static String attrByLocalName(XMLStreamReader r, String localName) {
        for (int i = 0; i < r.getAttributeCount(); i++) {
            if (localName.equals(r.getAttributeLocalName(i))) return r.getAttributeValue(i);
        }
        return null;
    }
}
