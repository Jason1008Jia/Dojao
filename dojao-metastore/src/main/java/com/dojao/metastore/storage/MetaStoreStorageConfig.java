package com.dojao.metastore.storage;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 元数据存储配置。
 *
 * @param storageType 存储类型
 * @param jdbcUrl     JDBC 连接地址（MYSQL / POSTGRESQL / SQLSERVER / SQLITE 使用）
 * @param username    数据库用户名
 * @param password    数据库密码
 * @param properties  额外属性
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/6/27
 */
public record MetaStoreStorageConfig(
    StorageType storageType,
    String jdbcUrl,
    String username,
    String password,
    Map<String, String> properties
) {

    /** SQLite 文件路径系统属性：指定 db 文件位置（默认 {@code ./data/dojao-metastore.db}）。 */
    public static final String SQLITE_PATH_KEY = "system.metastore.sqlite.path";

    /** SQLite db 文件默认相对路径（相对进程工作目录，与 conf/ 同级）。 */
    public static final String DEFAULT_SQLITE_PATH = "data/dojao-metastore.db";

    /** 默认 MySQL 本地配置。 */
    public static MetaStoreStorageConfig defaultMysql() {
        return new MetaStoreStorageConfig(
            StorageType.MYSQL,
            "jdbc:mysql://localhost:3306/dojao",
            "root",
            "123456",
            new LinkedHashMap<>()
        );
    }

    /** 默认本地内存配置（不持久化）。 */
    public static MetaStoreStorageConfig defaultLocal() {
        return new MetaStoreStorageConfig(
            StorageType.LOCAL,
            null,
            null,
            null,
            new LinkedHashMap<>()
        );
    }

    /**
     * 本地 SQLite 文件存储配置：{@code jdbcUrl} 为 {@code jdbc:sqlite:<path>}，
     * 供 {@code SqliteMetaStoreStorage} 直接使用（与 MySQL/PG 复用同一字段语义）。
     */
    public static MetaStoreStorageConfig defaultSqlite() {
        String path = System.getProperty(SQLITE_PATH_KEY, DEFAULT_SQLITE_PATH);
        return new MetaStoreStorageConfig(
            StorageType.SQLITE,
            "jdbc:sqlite:" + path,
            null,
            null,
            new LinkedHashMap<>()
        );
    }

    public MetaStoreStorageConfig {
        if (properties == null) {
            properties = new LinkedHashMap<>();
        }
    }
}
