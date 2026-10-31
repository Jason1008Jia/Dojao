package com.dojao.common.sql;

import java.util.ArrayList;
import java.util.List;

/**
 * 把一段文本按<b>顶层分号</b>切成语句，供批量提交（CLI 脚本 / Web 控制台）与 REPL 续行判断使用。
 *
 * <p>识别并跳过单引号 / 双引号 / 反引号字符串（含 {@code ''} 转义）、{@code --} 行注释与
 * {@code /* *}{@code /} 块注释，注释内或字符串内的分号不会误切；连续分号 / 纯注释产生的
 * 空语句会被丢弃。</p>
 *
 * @author jiaxin
 * @date 2026/9/17
 */
public final class SqlSplitter {

    /** 切分结果：{@code statements} = 已完整（带结尾分号）的语句；{@code rest} = 末尾未结束的残留。 */
    public static final class Parts {
        public final List<String> statements = new ArrayList<>();
        public String rest = "";
        /** {@code rest} 中是否含真正的 SQL 代码（排除空白与纯注释）—— 用于判断"半句要继续输入"还是"直接丢弃"。 */
        public boolean restHasCode;

        /** 是否已构成至少一条完整语句。 */
        public boolean hasStatement() {
            return !statements.isEmpty();
        }
    }

    private SqlSplitter() {}

    public static Parts split(String text) {
        Parts parts = new Parts();
        StringBuilder cur = new StringBuilder();
        boolean hasCode = false;      // 当前语句里是否出现过真正的 SQL 代码（排除纯注释/空语句）
        int i = 0;
        int n = text == null ? 0 : text.length();
        while (i < n) {
            char c = text.charAt(i);
            // ---- 字符串字面量 ----
            if (c == '\'' || c == '"' || c == '`') {
                hasCode = true;
                char quote = c;
                cur.append(c);
                i++;
                while (i < n) {
                    char d = text.charAt(i);
                    cur.append(d);
                    i++;
                    if (d == quote) {
                        if (i < n && text.charAt(i) == quote) {   // '' 转义
                            cur.append(quote);
                            i++;
                        } else {
                            break;
                        }
                    }
                }
                continue;
            }
            // ---- 行注释：-- 直到换行 ----
            if (c == '-' && i + 1 < n && text.charAt(i + 1) == '-') {
                while (i < n && text.charAt(i) != '\n') {
                    cur.append(text.charAt(i));
                    i++;
                }
                continue;
            }
            // ---- 块注释：/* ... */ ----
            if (c == '/' && i + 1 < n && text.charAt(i + 1) == '*') {
                cur.append("/*");
                i += 2;
                while (i < n && !(text.charAt(i) == '*' && i + 1 < n && text.charAt(i + 1) == '/')) {
                    cur.append(text.charAt(i));
                    i++;
                }
                if (i < n) {
                    cur.append("*/");
                    i += 2;
                }
                continue;
            }
            // ---- 语句分隔 ----
            if (c == ';') {
                cur.append(';');
                i++;
                if (hasCode) {
                    parts.statements.add(cur.toString().trim());
                }
                cur.setLength(0);
                hasCode = false;
                continue;
            }
            if (!Character.isWhitespace(c)) {
                hasCode = true;
            }
            cur.append(c);
            i++;
        }
        return finish(parts, cur, hasCode);
    }

    private static Parts finish(Parts parts, StringBuilder cur, boolean hasCode) {
        parts.rest = cur.toString();
        parts.restHasCode = hasCode;
        return parts;
    }

    /**
     * 切出<b>可直接执行</b>的语句列表：
     * <ul>
     *   <li>顶层分号切分，字符串 / 注释内的分号不切；</li>
     *   <li>末尾没有分号的最后一句照样算一条（{@code select 1} 这种用法不必写分号）；</li>
     *   <li>空语句 / 纯注释丢弃；</li>
     *   <li>剥掉每句结尾的分号（引擎的校验入口按单语句解析，不需要终结符）。</li>
     * </ul>
     *
     * @return 按书写顺序排列的语句；没有可执行语句时为空列表
     */
    public static List<String> executableStatements(String text) {
        Parts parts = split(text);
        List<String> out = new ArrayList<>(parts.statements.size() + 1);
        for (String s : parts.statements) {
            String stmt = stripTerminator(s);
            if (!stmt.isEmpty()) {
                out.add(stmt);
            }
        }
        if (parts.restHasCode) {
            String stmt = stripTerminator(parts.rest);
            if (!stmt.isEmpty()) {
                out.add(stmt);
            }
        }
        return out;
    }

    /** 剥掉语句末尾的分号（仅一个，已 trim）。 */
    public static String stripTerminator(String sql) {
        String s = sql == null ? "" : sql.trim();
        if (s.endsWith(";")) {
            s = s.substring(0, s.length() - 1).trim();
        }
        return s;
    }

    /** 文本是否已包含至少一条完整语句（REPL 续行判断）。 */
    public static boolean isComplete(String text) {
        return split(text).hasStatement();
    }
}
