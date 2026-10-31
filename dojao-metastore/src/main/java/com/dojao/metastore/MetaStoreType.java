package com.dojao.metastore;

/**
 * 元数据存储后端类型枚举。
 *
 * <ul>
 *   <li>{@link #MYSQL} — 自动加载 {@code MysqlMetaStoreStorage}</li>
 *   <li>{@link #SQLITE} — 本地单文件 SQLite（{@code SqliteMetaStoreStorage}），持久化</li>
 *   <li>{@link #LOCAL} — 内存模式，不持久化（测试用）</li>
 *   <li>{@link #POSTGRESQL} — 自动加载 PostgreSQL 存储</li>
 * </ul>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/09
 */
public enum MetaStoreType {
    MYSQL,
    SQLITE,
    LOCAL,
    POSTGRESQL
}
