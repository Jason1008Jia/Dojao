package com.dojao.cli;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * 查询结果格式化：{@code table}（列宽对齐）/ {@code csv} / {@code json}。
 *
 * <p>行到即写、内存有界：csv / json 完全流式；table 需要列宽，只缓冲前
 * {@value #TABLE_BUFFER_ROWS} 行用于对齐，之后的行按已算出的列宽直接输出。</p>
 *
 * <p>引擎把 SQL NULL 渲染成字面量 {@code "NULL"}（见 QueryServiceImpl），json 输出时还原为
 * {@code null}，csv / table 原样保留。</p>
 *
 * @author jiaxin
 * @date 2026/9/17
 */
final class ResultFormatter {

    /** 输出格式。 */
    enum Format {
        TABLE, CSV, JSON;

        static Format parse(String s) {
            switch (s.toLowerCase(Locale.ROOT)) {
                case "table", "t":
                    return TABLE;
                case "csv":
                    return CSV;
                case "json":
                    return JSON;
                default:
                    throw new IllegalArgumentException("未知输出格式: " + s + "（可选 table / csv / json）");
            }
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    /** table 模式用于计算列宽的最大缓冲行数，超出后不再调整列宽。 */
    private static final int TABLE_BUFFER_ROWS = 2000;

    /** JSON 里可直接当数字输出的形态（拒绝前导零，避免把 "007" 变成 7）。 */
    private static final Pattern NUMBER = Pattern.compile("-?(0|[1-9]\\d*)(\\.\\d+)?([eE][+-]?\\d+)?");

    private final Format format;
    private final PrintStream out;
    private List<String> columns = List.of();
    private final List<List<String>> buffer = new ArrayList<>();
    private int[] widths;
    private boolean tableFlushed;
    private boolean jsonFirst = true;
    /** {@link #begin} 是否已调用 —— 未开始就 {@link #end}（如服务端报错）不得补写收尾符。 */
    private boolean started;

    ResultFormatter(Format format, PrintStream out) {
        this.format = format;
        this.out = out;
    }

    /** 列头到达（每块结果都带列头，调用方只取首块）。 */
    void begin(List<String> cols) {
        this.columns = cols;
        this.started = true;
        if (format == Format.CSV && !cols.isEmpty()) {
            out.println(joinCsv(cols));
        } else if (format == Format.JSON) {
            out.println("[");
        }
    }

    /** 一行数据到达。 */
    void row(List<String> values) {
        switch (format) {
            case CSV:
                out.println(joinCsv(values));
                break;
            case JSON:
                out.print(jsonFirst ? "  " : ",\n  ");
                jsonFirst = false;
                out.print(jsonObject(values));
                break;
            case TABLE:
                if (!tableFlushed && buffer.size() < TABLE_BUFFER_ROWS) {
                    buffer.add(values);
                } else {
                    if (!tableFlushed) {
                        flushTable();
                    }
                    out.println(tableLine(values));
                }
                break;
            default:
                throw new IllegalStateException("未处理的格式: " + format);
        }
    }

    /** 结果收尾：table 冲刷缓冲；json 闭合数组。未 {@link #begin} 过（例如查询报错）则什么都不写。 */
    void end() {
        if (!started) {
            out.flush();
            return;
        }
        if (format == Format.TABLE && !tableFlushed && !columns.isEmpty()) {
            flushTable();
        }
        if (format == Format.JSON) {
            out.println(jsonFirst ? "]" : "\n]");
        }
        out.flush();
    }

    // ==================== table ====================

    private void flushTable() {
        tableFlushed = true;
        widths = new int[columns.size()];
        for (int i = 0; i < widths.length; i++) {
            widths[i] = columns.get(i).length();
        }
        for (List<String> r : buffer) {
            for (int i = 0; i < widths.length && i < r.size(); i++) {
                widths[i] = Math.max(widths[i], displayWidth(r.get(i)));
            }
        }
        out.println(tableLine(columns));
        out.println(separator());
        for (List<String> r : buffer) {
            out.println(tableLine(r));
        }
        buffer.clear();
    }

    private String separator() {
        StringBuilder sb = new StringBuilder();
        for (int w : widths) {
            if (sb.length() > 0) {
                sb.append("-+-");
            }
            sb.append("-".repeat(w));
        }
        return sb.toString();
    }

    private String tableLine(List<String> values) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < widths.length; i++) {
            if (i > 0) {
                sb.append(" | ");
            }
            String v = i < values.size() ? values.get(i) : "";
            sb.append(v);
            if (i < widths.length - 1) {
                sb.append(" ".repeat(Math.max(0, widths[i] - displayWidth(v))));
            }
        }
        while (sb.length() > 0 && sb.charAt(sb.length() - 1) == ' ') {
            sb.setLength(sb.length() - 1);
        }
        return sb.toString();
    }

    /** 终端显示宽度：CJK / 全角字符占 2 列，其余占 1 列。 */
    private static int displayWidth(String s) {
        int w = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            w += isWide(c) ? 2 : 1;
        }
        return w;
    }

    private static boolean isWide(char c) {
        return (c >= 0x1100 && c <= 0x115F)        // 韩文字母
                || (c >= 0x2E80 && c <= 0xA4CF)    // CJK 部首 ~ 注音
                || (c >= 0xAC00 && c <= 0xD7A3)    // 韩文音节
                || (c >= 0xF900 && c <= 0xFAFF)    // CJK 兼容表意
                || (c >= 0xFE30 && c <= 0xFE6F)    // CJK 兼容形式
                || (c >= 0xFF00 && c <= 0xFF60)    // 全角
                || (c >= 0xFFE0 && c <= 0xFFE6);
    }

    // ==================== csv / json ====================

    private static String joinCsv(List<String> values) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(csvCell(values.get(i)));
        }
        return sb.toString();
    }

    private static String csvCell(String v) {
        if (v.indexOf(',') < 0 && v.indexOf('"') < 0 && v.indexOf('\n') < 0 && v.indexOf('\r') < 0) {
            return v;
        }
        return '"' + v.replace("\"", "\"\"") + '"';
    }

    private String jsonObject(List<String> values) {
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < columns.size() && i < values.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append('"').append(escapeJson(columns.get(i))).append("\": ")
                    .append(jsonValue(values.get(i)));
        }
        return sb.append('}').toString();
    }

    private static String jsonValue(String v) {
        if ("NULL".equals(v)) {
            return "null";
        }
        if (NUMBER.matcher(v).matches()) {
            return v;
        }
        return '"' + escapeJson(v) + '"';
    }

    private static String escapeJson(String s) {
        StringBuilder sb = new StringBuilder(s.length() + 8);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"':
                    sb.append("\\\"");
                    break;
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                default:
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        return sb.toString();
    }
}
