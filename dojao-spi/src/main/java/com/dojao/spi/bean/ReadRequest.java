package com.dojao.spi.bean;

import java.util.List;
import java.util.Optional;

/**
 * 数据读取请求 —— Worker 调用连接器读取数据时传入的参数。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/11
 */
public record ReadRequest(
    String catalog,
    String schema,
    String table,
    Optional<List<String>> columns,     // 列投影，空表示读取所有列
    Optional<String> filterExpression,  // 过滤条件（连接器方言的 SQL 表达式，如 "id > 10 AND status = 'A'"）
    long offset,                        // 偏移量（分页）
    long limit                          // 最大返回行数，0 表示不限制
) {

    public static ReadRequest of(String catalog, String schema, String table) {
        return new ReadRequest(catalog, schema, table,
            Optional.empty(), Optional.empty(), 0, 0);
    }

    public static ReadRequest of(String catalog, String schema, String table, List<String> columns) {
        return new ReadRequest(catalog, schema, table,
            Optional.of(columns), Optional.empty(), 0, 0);
    }

    public static ReadRequest of(String catalog, String schema, String table,long offset,long limit) {
        return new ReadRequest(catalog, schema, table,
                Optional.empty(), Optional.empty(), offset, limit);
    }

    /** 返回列集替换为 {@code columns} 的新请求（null/空 → 读取所有列）。 */
    public ReadRequest withColumns(List<String> columns) {
        return new ReadRequest(catalog, schema, table,
            columns == null || columns.isEmpty() ? Optional.empty() : Optional.of(columns),
            filterExpression, offset, limit);
    }

    /** 返回过滤条件替换为 {@code filter} 的新请求（null/空串 → 无过滤）。 */
    public ReadRequest withFilter(String filter) {
        return new ReadRequest(catalog, schema, table,
            columns,
            filter == null || filter.isBlank() ? Optional.empty() : Optional.of(filter),
            offset, limit);
    }

    /**
     * 返回 offset/limit 替换为 {@code offset}/{@code limit} 的新请求（分页下推：把顶层
     * {@code LIMIT} 交给数据源执行，使其读满即停，不必等游标被关）。
     */
    public ReadRequest withLimit(long offset, long limit) {
        return new ReadRequest(catalog, schema, table, columns, filterExpression, offset, limit);
    }
}
