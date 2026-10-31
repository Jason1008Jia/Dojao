package com.dojao.metastore.storage;

import com.dojao.metastore.storage.local.LocalMetaStoreStorage;
import com.dojao.metastore.storage.mysql.MysqlMetaStoreStorage;
import com.dojao.metastore.storage.postgres.PostgresMetaStoreStorage;
import com.dojao.metastore.storage.sqlite.SqliteMetaStoreStorage;
import com.dojao.metastore.storage.sqlserver.SqlServerMetaStoreStorage;

/**
 * 元数据存储工厂：根据配置创建对应的存储后端实现。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/6/27
 */
public final class MetaStoreStorageFactory {

    private MetaStoreStorageFactory() {}

    /**
     * 根据配置创建存储后端。
     *
     * @param config 存储配置
     * @return 对应的 MetaStoreStorage 实现
     */
    public static MetaStoreStorage create(MetaStoreStorageConfig config) {
        return switch (config.storageType()) {
            case MYSQL -> new MysqlMetaStoreStorage(
                config.jdbcUrl(), config.username(), config.password());
            case POSTGRESQL -> new PostgresMetaStoreStorage(
                config.jdbcUrl(), config.username(), config.password());
            case SQLSERVER -> new SqlServerMetaStoreStorage(
                config.jdbcUrl(), config.username(), config.password());
            case SQLITE -> new SqliteMetaStoreStorage(config.jdbcUrl());
            case LOCAL -> new LocalMetaStoreStorage();
        };
    }
}
