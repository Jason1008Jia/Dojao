package com.dojao.core.function.scalar.datetime;

import java.time.format.DateTimeFormatter;
import java.util.Locale;

/**
 * 日期格式串 → {@link DateTimeFormatter} 适配。
 *
 * <p>同时接受两种写法：</p>
 * <ul>
 *   <li><b>Java/SimpleDateFormat 风格</b>（{@code yyyy-MM-dd HH:mm:ss}）—— 原样交给
 *       {@code DateTimeFormatter.ofPattern}；</li>
 *   <li><b>MySQL 风格</b>（{@code %Y-%m-%d %H:%i:%s}）—— 检测到 {@code %X} 指令后逐段翻译成
 *       DateTimeFormatter 模式，非指令文本按字面量引用（避免 {@code at} 里的 {@code a}/{@code t}
 *       被当成模式字母）。</li>
 * </ul>
 *
 * <p>★历史问题：{@code %Y-%m-%d} 曾被直接送进 {@code ofPattern} —— {@code %} 是字面量、
 * {@code Y/m/d} 却恰好是模式字母（周历年 / 分钟 / 日），于是 2024-05-05 被格式化成
 * {@code %2024-%0-%5}（分钟 0、日 5）。现将 {@code %} 指令显式翻译，杜绝这类静默错位。</p>
 *
 * @author jiaxin
 * @date 2026/10/11
 */
public final class JavaDateFormat {

    private JavaDateFormat() {
    }

    /** 格式串 → 格式化器（英文区域，保证月份/星期名稳定）。 */
    public static DateTimeFormatter toFormatter(String pattern) {
        return DateTimeFormatter.ofPattern(adapt(pattern), Locale.ENGLISH);
    }

    /** {@code %}-指令串 → DateTimeFormatter 模式；不含任何已知指令时原样返回（Java 风格）。 */
    static String adapt(String pattern) {
        if (pattern == null || !containsDirective(pattern)) return pattern;

        StringBuilder out = new StringBuilder(pattern.length() + 8);
        StringBuilder literal = new StringBuilder();
        int i = 0;
        while (i < pattern.length()) {
            char c = pattern.charAt(i);
            if (c == '%' && i + 1 < pattern.length()) {
                char d = pattern.charAt(i + 1);
                String repl = d == '%' ? "%" : translate(d);
                if (repl != null) {
                    flushLiteral(out, literal);
                    out.append(repl);
                    i += 2;
                    continue;
                }
            }
            literal.append(c);
            i++;
        }
        flushLiteral(out, literal);
        return out.toString();
    }

    /**
     * 存在 {@code %} + 字母即视为 MySQL 风格（单独的 {@code %} 沿用字面量语义）。
     * 含未知指令（如 {@code %Q}）也算 —— 否则 {@code %Q} 会被 DateTimeFormatter 当成
     * 季度字母 Q，静默输出 {@code %2}。
     */
    private static boolean containsDirective(String pattern) {
        for (int i = 0; i + 1 < pattern.length(); i++) {
            if (pattern.charAt(i) != '%') continue;
            char d = pattern.charAt(i + 1);
            if (d == '%' || (d >= 'A' && d <= 'Z') || (d >= 'a' && d <= 'z')) return true;
        }
        return false;
    }

    /**
     * 字面量段追加：DateTimeFormatter 把 A-Z/a-z（及 {@code []{}#'}）当保留字符，其余原样；
     * 含保留字符的字面量须整体用单引号引用（内部单引号写成两个）。
     */
    private static void flushLiteral(StringBuilder out, StringBuilder literal) {
        if (literal.isEmpty()) return;
        if (needsQuoting(literal)) {
            out.append('\'');
            for (int i = 0; i < literal.length(); i++) {
                char c = literal.charAt(i);
                if (c == '\'') out.append("''");
                else out.append(c);
            }
            out.append('\'');
        } else {
            out.append(literal);
        }
        literal.setLength(0);
    }

    private static boolean needsQuoting(CharSequence text) {
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
                    || c == '[' || c == ']' || c == '{' || c == '}' || c == '#' || c == '\'') {
                return true;
            }
        }
        return false;
    }

    /** MySQL 指令 → DateTimeFormatter 模式；未知指令返回 null（保留为字面量）。 */
    private static String translate(char d) {
        return switch (d) {
            case 'Y' -> "yyyy";
            case 'y' -> "yy";
            case 'm' -> "MM";
            case 'c' -> "M";
            case 'd' -> "dd";
            case 'e' -> "d";
            case 'H' -> "HH";
            case 'k' -> "H";
            case 'h', 'I' -> "hh";
            case 'l' -> "h";
            case 'i' -> "mm";
            case 's', 'S' -> "ss";
            case 'f' -> "SSSSSS";       // 微秒（6 位）
            case 'p' -> "a";
            case 'M' -> "MMMM";
            case 'b' -> "MMM";
            case 'a' -> "EEE";
            case 'W' -> "EEEE";
            case 'j' -> "DDD";
            case 'D' -> "MM/dd/yy";
            case 'F' -> "yyyy-MM-dd";
            case 'T' -> "HH:mm:ss";
            case 'r' -> "hh:mm:ss a";
            default -> null;
        };
    }
}
