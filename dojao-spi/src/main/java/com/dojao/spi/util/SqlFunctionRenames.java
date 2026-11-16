package com.dojao.spi.util;

import java.util.Map;
import java.util.Optional;

/**
 * 按别名表重写 SQL 表达式里的<b>函数名</b>（{@code to_date(...)} → {@code toDate(...)}），
 * 供连接器实现 {@link com.dojao.spi.service.DojaoConnector#renderFunctionPredicate} 使用。
 *
 * <p><b>规则</b>：只改「标识符紧跟 {@code (}」的函数名，裸列名、字符串/引号标识符原样保留；
 * 表达式里出现<b>任何</b>不在别名表里的函数名 → 整体返回空（不允许只翻译一半 —— 半翻译的谓词
 * 发给数据源只会报「未知函数」或更糟的静默错行）。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/11/16
 */
public final class SqlFunctionRenames {

    private SqlFunctionRenames() {
    }

    /**
     * 重写表达式中的函数名。
     *
     * @param expr    谓词 / 表达式原文
     * @param aliases 引擎函数名（小写）→ 数据源函数名；值可以与键相同（同名等价函数）
     * @return 重写后的文本；任一函数名不在表中 / 没有出现任何函数调用 / 入参为空 → {@link Optional#empty()}
     */
    public static Optional<String> rewrite(String expr, Map<String, String> aliases) {
        if (expr == null || expr.isBlank() || aliases == null || aliases.isEmpty()) {
            return Optional.empty();
        }
        StringBuilder out = new StringBuilder(expr.length() + 8);
        int i = 0;
        int renamed = 0;
        char quote = 0;
        while (i < expr.length()) {
            char c = expr.charAt(i);
            if (quote != 0) {
                out.append(c);
                if (c == quote) {
                    quote = 0;
                }
                i++;
                continue;
            }
            if (c == '\'' || c == '"' || c == '`') {
                quote = c;
                out.append(c);
                i++;
                continue;
            }
            if (Character.isLetter(c) || c == '_') {
                int j = i;
                while (j < expr.length()
                        && (Character.isLetterOrDigit(expr.charAt(j)) || expr.charAt(j) == '_')) {
                    j++;
                }
                int k = j;
                while (k < expr.length() && Character.isWhitespace(expr.charAt(k))) {
                    k++;
                }
                String name = expr.substring(i, j);
                if (k < expr.length() && expr.charAt(k) == '(') {
                    String target = aliases.get(name.toLowerCase(java.util.Locale.ROOT));
                    if (target == null) {
                        return Optional.empty();   // 出现不认识的函数名 → 整体不推
                    }
                    out.append(target);
                    renamed++;
                } else {
                    out.append(name);              // 裸列名 / 字面量片段，原样保留
                }
                i = j;
                continue;
            }
            out.append(c);
            i++;
        }
        return renamed == 0 ? Optional.empty() : Optional.of(out.toString());
    }
}
