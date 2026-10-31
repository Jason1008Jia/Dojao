package com.dojao.core.resource;

/**
 * 待扫描表引用 —— 初始并行度决策时需探查元数据的表（catalog.schema.table 三段限定）。
 *
 * @param catalog 数据源 catalog（连接器注册名，如 {@code mysql}）
 * @param schema  数据源 schema（可为空）
 * @param table   表名
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/29
 */
public record TableRef(String catalog, String schema, String table) {

    /** 人类可读限定名：catalog.schema.table（空段省略）。 */
    public String qualifiedName() {
        StringBuilder sb = new StringBuilder();
        if (catalog != null && !catalog.isBlank()) sb.append(catalog).append('.');
        if (schema != null && !schema.isBlank()) sb.append(schema).append('.');
        sb.append(table);
        return sb.toString();
    }
}
