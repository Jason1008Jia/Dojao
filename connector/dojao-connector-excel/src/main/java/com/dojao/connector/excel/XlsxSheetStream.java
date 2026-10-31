package com.dojao.connector.excel;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * 一张工作表的<b>流式行读取器</b> —— 逐个 {@code <row>} 解析，内存里始终只有一行。
 *
 * <p>Excel 的单元格分两类值来源：</p>
 * <ul>
 *   <li>{@code t="s"}：值在 {@code sharedStrings.xml}，单元格里只存索引（最常见）；</li>
 *   <li>{@code t="inlineStr"}：文本直接写在单元格内（另存 / 部分工具导出）；</li>
 *   <li>其余：{@code <v>} 里是数字 / 布尔 / ISO 日期 / 错误码原文。</li>
 * </ul>
 *
 * <p>坐标（{@code r="C7"}）用来定位列；缺坐标的单元格按上一列 +1 递推（规范允许省略）。
 * 空单元格（无 {@code <v>}、错误值）按 {@code null} 处理 —— 对查询而言它就是 SQL NULL。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/21
 */
final class XlsxSheetStream implements AutoCloseable {

    /** 单元格原始形态（决定类型推断与读入 Arrow 时的转换方式）。 */
    enum CellKind {
        /** 数字（{@code <v>} 原文，Excel 里日期序列值也走这里）。 */
        NUMBER,
        /** 文本（共享字符串 / 内联字符串 / 公式字符串结果）。 */
        STRING,
        /** 布尔（{@code t="b"}，{@code <v>} = 1/0）。 */
        BOOLEAN,
        /** ISO 日期（{@code t="d"}）。 */
        DATE,
        /** 错误值（{@code t="e"}，如 {@code #DIV/0!}）：按 NULL 读。 */
        ERROR
    }

    record Cell(CellKind kind, String text) {
    }

    /** 一行：列下标（0 起）→ 单元格；{@code maxCol} 是本行最后一个非空单元格的下标。 */
    record Row(Map<Integer, Cell> cells, int maxCol) {

        Cell cell(int col) {
            return cells.get(col);
        }

        /** 整行没有可用值（裁剪后不会产出任何数据）。 */
        boolean isEmpty() {
            for (Cell c : cells.values()) {
                if (c != null && c.kind() != CellKind.ERROR
                        && c.text() != null && !c.text().isEmpty()) {
                    return false;
                }
            }
            return true;
        }
    }

    private final InputStream in;
    private final XMLStreamReader xml;
    private final List<String> sharedStrings;
    private final boolean[] dateStyles;
    private final boolean date1904;
    private Row current;

    private XlsxSheetStream(InputStream in, XMLStreamReader xml, List<String> sharedStrings,
                            boolean[] dateStyles, boolean date1904) {
        this.in = in;
        this.xml = xml;
        this.sharedStrings = sharedStrings;
        this.dateStyles = dateStyles;
        this.date1904 = date1904;
    }

    static XlsxSheetStream open(ZipFile zip, String part, List<String> sharedStrings,
                                boolean[] dateStyles, boolean date1904) throws IOException {
        ZipEntry entry = XlsxWorkbook.findEntry(zip, part);
        if (entry == null) {
            throw new IOException("工作簿里缺少工作表数据: " + part);
        }
        InputStream in = zip.getInputStream(entry);
        try {
            XMLStreamReader xml = XlsxWorkbook.newFactory().createXMLStreamReader(in);
            return new XlsxSheetStream(in, xml, sharedStrings, dateStyles, date1904);
        } catch (XMLStreamException | RuntimeException e) {
            in.close();
            throw new IOException("打开工作表失败: " + part + "（" + e.getMessage() + "）", e);
        }
    }

    /** 前进到下一行；没有更多行返回 false。 */
    boolean nextRow() {
        try {
            while (xml.hasNext()) {
                if (xml.next() == XMLStreamConstants.START_ELEMENT
                        && "row".equals(xml.getLocalName())) {
                    current = parseRow();
                    return true;
                }
            }
            current = null;
            return false;
        } catch (XMLStreamException e) {
            throw new IllegalStateException("解析 Excel 工作表失败: " + e.getMessage(), e);
        }
    }

    Row row() {
        return current;
    }

    // ==================== 行 / 单元格解析 ====================

    private Row parseRow() throws XMLStreamException {
        Map<Integer, Cell> cells = new HashMap<>();
        int depth = 1;
        int nextCol = 0;
        int maxCol = -1;
        while (xml.hasNext()) {
            int ev = xml.next();
            if (ev == XMLStreamConstants.START_ELEMENT) {
                if (depth == 1 && "c".equals(xml.getLocalName())) {
                    int ref = columnIndex(XlsxWorkbook.attr(xml, "r"));
                    int col = ref >= 0 ? ref : nextCol;
                    nextCol = col + 1;
                    Cell cell = parseCell(XlsxWorkbook.attr(xml, "t"),
                            styleIndex(XlsxWorkbook.attr(xml, "s")));
                    if (cell != null) {
                        cells.put(col, cell);
                        if (col > maxCol) maxCol = col;
                    }
                } else {
                    depth++;
                }
            } else if (ev == XMLStreamConstants.END_ELEMENT) {
                depth--;
                if (depth == 0) break;
            }
        }
        return new Row(cells, maxCol);
    }

    /** 单元格 {@code s="N"} 样式下标；缺省 / 非整数 → -1（不是日期格式）。 */
    private static int styleIndex(String attr) {
        if (attr == null || attr.isBlank()) return -1;
        try {
            return Integer.parseInt(attr.trim());
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private boolean isDateStyle(int styleIndex) {
        return styleIndex >= 0 && styleIndex < dateStyles.length && dateStyles[styleIndex];
    }

    /** 解析一个 {@code <c>}（进入时位于其 START_ELEMENT，返回时消费掉 END_ELEMENT）。 */
    private Cell parseCell(String type, int styleIndex) throws XMLStreamException {
        StringBuilder sb = new StringBuilder();
        boolean inV = false;
        int depth = 1;
        int tDepth = -1;
        int rphDepth = -1;
        while (xml.hasNext()) {
            int ev = xml.next();
            if (ev == XMLStreamConstants.START_ELEMENT) {
                depth++;
                String ln = xml.getLocalName();
                if ("v".equals(ln)) {
                    inV = true;
                } else if ("t".equals(ln)) {
                    tDepth = depth;
                } else if ("rPh".equals(ln)) {
                    rphDepth = depth;
                }
            } else if (ev == XMLStreamConstants.END_ELEMENT) {
                String ln = xml.getLocalName();
                if ("v".equals(ln)) {
                    inV = false;
                } else if ("t".equals(ln)) {
                    tDepth = -1;
                } else if ("rPh".equals(ln)) {
                    rphDepth = -1;
                }
                depth--;
                if (depth == 0) break;
            } else if (ev == XMLStreamConstants.CHARACTERS || ev == XMLStreamConstants.CDATA) {
                if (inV || (tDepth >= 0 && rphDepth < 0)) sb.append(xml.getText());
            }
        }
        String text = sb.toString();
        return switch (type == null ? "" : type) {
            case "s" -> sharedCell(text);
            case "inlineStr", "str" -> text.isEmpty() ? null : new Cell(CellKind.STRING, text);
            case "b" -> text.isBlank() ? null : new Cell(CellKind.BOOLEAN, text.trim());
            case "d" -> text.isBlank() ? null : new Cell(CellKind.DATE, text.trim());
            case "e" -> new Cell(CellKind.ERROR, text);
            // "n" 与缺省都是数字；空 <v> 视为空单元格
            default -> {
                if (text.isEmpty()) yield null;
                String raw = text.trim();
                String iso = isDateStyle(styleIndex) ? excelSerialToIso(raw, date1904) : null;
                yield iso != null ? new Cell(CellKind.DATE, iso) : new Cell(CellKind.NUMBER, raw);
            }
        };
    }

    /**
     * Excel 日期序列值 → ISO 本地日期时间。
     *
     * <p>序列值 = 距基准日的天数（小数部分是一天内的时刻）。基准日按 1900 / 1904 日期系统分别是
     * {@code 1899-12-30} / {@code 1904-01-01}。仅当值是<b>非负数字</b>时转换；否则返回 {@code null}
     * （按普通数字读，不把异常值硬解释成 1899 年）。</p>
     */
    static String excelSerialToIso(String raw, boolean date1904) {
        double serial;
        try {
            serial = Double.parseDouble(raw);
        } catch (NumberFormatException e) {
            return null;
        }
        if (Double.isNaN(serial) || Double.isInfinite(serial) || serial < 0) return null;
        long days = (long) Math.floor(serial);
        long nanos = Math.round((serial - days) * 86_400_000_000_000d);
        java.time.LocalDate base = date1904
                ? java.time.LocalDate.of(1904, 1, 1)
                : java.time.LocalDate.of(1899, 12, 30);
        java.time.LocalDateTime dt = base.plusDays(days).atStartOfDay().plusNanos(nanos);
        return dt.toString();
    }

    private Cell sharedCell(String text) {
        int idx;
        try {
            idx = Integer.parseInt(text.trim());
        } catch (NumberFormatException e) {
            return new Cell(CellKind.ERROR, text);
        }
        if (idx < 0 || idx >= sharedStrings.size()) {
            return new Cell(CellKind.ERROR, text);
        }
        return new Cell(CellKind.STRING, sharedStrings.get(idx));
    }

    /**
     * 单元格坐标 → 列下标（0 起）：{@code "A1" → 0}、{@code "AA3" → 26}。
     * 允许 {@code $A$1} 这种绝对引用写法；无法识别返回 -1（调用方按递推处理）。
     */
    static int columnIndex(String ref) {
        if (ref == null || ref.isEmpty()) return -1;
        int col = 0;
        int i = 0;
        boolean any = false;
        while (i < ref.length() && ref.charAt(i) == '$') i++;
        for (; i < ref.length(); i++) {
            char ch = ref.charAt(i);
            if (ch >= 'A' && ch <= 'Z') {
                col = col * 26 + (ch - 'A' + 1);
                any = true;
            } else if (ch >= 'a' && ch <= 'z') {
                col = col * 26 + (ch - 'a' + 1);
                any = true;
            } else {
                break;
            }
        }
        return any ? col - 1 : -1;
    }

    @Override
    public void close() {
        try {
            xml.close();
        } catch (XMLStreamException ignored) {
            // 关闭流失败无补救动作
        }
        try {
            in.close();
        } catch (IOException ignored) {
            // 同上
        }
    }
}
