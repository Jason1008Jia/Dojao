package com.dojao.metastore.storage;

/**
 * 元数据外接存储类型。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/6/27
 */
public enum StorageType {

    /** MySQL 存储（默认）。 */
    MYSQL,

    /** PostgreSQL 存储。 */
    POSTGRESQL,

    /** SQL Server 存储。 */
    SQLSERVER,

    /** 本地文件型嵌入式存储（SQLite 单文件，进程重启后仍保留）。 */
    SQLITE,

    /** 本地内存存储（不持久化，进程退出即丢）。 */
    LOCAL
}
