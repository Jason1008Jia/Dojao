package com.dojao.spi.util;

import java.util.ArrayList;
import java.util.List;

/**
 * SQL 标识符工具：识别并剥除方言转义引号。
 *
 * <p><b>为什么需要</b>：连接器的 {@link com.dojao.spi.service.DojaoConnector#pruneColumns}
 * 需要把列名转成方言转义形态（MySQL / ClickHouse 的 {@code `col`}、PostgreSQL 的
 * {@code "col"}）后再放进 {@link com.dojao.spi.bean.ReadRequest#columns()} —— 它承载的是
 * 「可直接拼进 SELECT 的 SQL 片段」。但引擎侧还有第二个消费者：按列名查连接器元数据
 * （{@code sourceFields} / {@code getColumnTypes}）推导声明列类型。带引号的名字在元数据
 * 映射里查不到，类型会静默退化成兜底口径（如 decimal 聚合输出被当成 DOUBLE），运行期再按
 * 错误宽度拷贝即产生静默错值。凡从 {@code ReadRequest.columns()} 取列名做元数据推导的地方，
 * 都应先用本类剥掉这层转义。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/22
 */
public final class SqlIdentifiers {

    private SqlIdentifiers() {
    }

    /**
     * 剥除一层成对方言引号：{@code `x`} / {@code "x"} / {@code [x]} → {@code x}，
     * 并把成对转义（双反引号 / 双引号 / 双方括号）还原为单个字符。
     * 无引号的标识符原样返回（不 trim，避免误伤合法列名）。
     */
    public static String unquote(String name) {
        if (name == null) return null;
        String t = name.trim();
        if (t.length() >= 2) {
            char first = t.charAt(0);
            char last = t.charAt(t.length() - 1);
            if (first == '`' && last == '`') {
                return t.substring(1, t.length() - 1).replace("``", "`");
            }
            if (first == '"' && last == '"') {
                return t.substring(1, t.length() - 1).replace("\"\"", "\"");
            }
            if (first == '[' && last == ']') {
                return t.substring(1, t.length() - 1).replace("]]", "]");
            }
        }
        return name;
    }

    /** 逐列 {@link #unquote}；null 清单原样返回。 */
    public static List<String> unquoteAll(List<String> names) {
        if (names == null) return null;
        List<String> out = new ArrayList<>(names.size());
        for (String n : names) out.add(unquote(n));
        return out;
    }
}
