package com.dojao.spi.bean;

import com.dojao.common.data.Page;

import java.util.List;

/**
 * 数据写入请求 —— Worker 调用连接器写入数据时传入的参数。
 *
 * <p>{@code targetColumns}：目标表列名（按位置对应 {@code data} 的列）。null 表示不指定
 * （CTAS 建表即按 Page schema 建列，或 SELECT 输出列名即目标列名）。连接器用它组装
 * {@code INSERT INTO t (targetColumns) VALUES (...)} 并按位置绑定值，从而支持
 * {@code INSERT INTO t(a,b) SELECT x,y} 这类列名不一致的写入。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/11
 */
public record WriteRequest(
    String catalog,
    String schema,
    String table,
    Page data,
    List<String> targetColumns
) {
    /** 兼容旧签名：不指定目标列（CTAS 或 SELECT 输出名即目标列名）。 */
    public WriteRequest(String catalog, String schema, String table, Page data) {
        this(catalog, schema, table, data, null);
    }
}
