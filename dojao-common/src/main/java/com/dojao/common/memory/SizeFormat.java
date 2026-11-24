package com.dojao.common.memory;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 字节数 / 内存错误的统一人类可读格式。
 *
 * <p>规则（用户口径）：&lt; 1GB 用 <b>MB</b>，≥ 1GB 用 <b>GB</b>，保留 2 位小数；
 * 这样 {@code 1115774684} 会显示成 {@code 1.04 GB}，而不是一串看不清位数的裸数字。</p>
 *
 * <p>{@link #humanize(String)} 用于把 Arrow 的原始 OOM 文案
 * （{@code Unable to allocate buffer of size 2097152 ... Current allocation: 1115774684}）
 * 中的字节数就地替换成人类可读单位，完整堆栈与信息不丢失。</p>
 */
public final class SizeFormat {

    private static final long KB = 1024L;
    private static final long MB = 1024L * 1024L;
    private static final long GB = 1024L * 1024L * 1024L;

    private static final Pattern BUFFER_SIZE = Pattern.compile("buffer of size (\\d+)");
    private static final Pattern CURRENT_ALLOCATION = Pattern.compile("Current allocation: (\\d+)");
    private static final Pattern MEMORY_LEAKED = Pattern.compile("Memory leaked: \\((\\d+)\\)");
    private static final Pattern MEMORY_LIMIT = Pattern.compile("memory limit: (\\d+)");

    private SizeFormat() {
    }

    /** 字节数 → {@code 512.00 KB / 35.70 MB / 1.04 GB}（< 1MB 保留 KB，< 1KB 原样 B）。 */
    public static String bytes(long bytes) {
        if (bytes < KB) return bytes + " B";
        if (bytes < MB) return String.format(Locale.ROOT, "%.2f KB", bytes / (double) KB);
        if (bytes < GB) return String.format(Locale.ROOT, "%.2f MB", bytes / (double) MB);
        return String.format(Locale.ROOT, "%.2f GB", bytes / (double) GB);
    }

    /** 把错误文本里的「buffer of size / Current allocation / Memory leaked」字节数格式化。 */
    public static String humanize(String text) {
        if (text == null || text.isEmpty()) return text;
        String out = replace(BUFFER_SIZE, text);
        out = replace(CURRENT_ALLOCATION, out);
        out = replace(MEMORY_LEAKED, out);
        out = replace(MEMORY_LIMIT, out);
        return out;
    }

    private static String replace(Pattern pattern, String text) {
        Matcher m = pattern.matcher(text);
        if (!m.find()) return text;
        StringBuilder sb = new StringBuilder(text.length() + 16);
        int last = 0;
        do {
            sb.append(text, last, m.start());
            long value;
            try {
                value = Long.parseLong(m.group(1));
            } catch (NumberFormatException e) {
                sb.append(m.group());
                last = m.end();
                continue;
            }
            // 保留数值前后的原文（如 "Memory leaked: (" 的 "(" 与结尾 ")"）
            sb.append(text, m.start(), m.start(1)).append(bytes(value));
            sb.append(text, m.end(1), m.end());
            last = m.end();
        } while (m.find());
        sb.append(text, last, text.length());
        return sb.toString();
    }
}
