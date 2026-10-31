package com.dojao.metastore.storage.postgres;

import com.dojao.metastore.storage.MetaStoreStorage;

import java.util.*;

/**
 * PostgreSQL 外接存储实现（占位，待实现）。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/6/27
 */
public class PostgresMetaStoreStorage implements MetaStoreStorage {

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private volatile boolean ready = false;

    public PostgresMetaStoreStorage(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    @Override
    public void initialize() {
        // TODO: 实现 PostgreSQL 元数据表的创建
        throw new UnsupportedOperationException("PostgreSQL metastore storage not yet implemented");
    }

    @Override
    public void close() {
        this.ready = false;
    }

    @Override
    public boolean isReady() { return ready; }

    @Override
    public void registerCatalog(String catalogName, String connectorType,
                                 Map<String, String> properties) {
        throw new UnsupportedOperationException("PostgreSQL metastore not yet implemented");
    }

    @Override
    public void unregisterCatalog(String catalogName) {
        throw new UnsupportedOperationException("PostgreSQL metastore not yet implemented");
    }

    @Override
    public List<String> listCatalogs() { return List.of(); }

    @Override
    public Optional<String> getConnectorType(String catalogName) { return Optional.empty(); }

    @Override
    public Map<String, String> getCatalogProperties(String catalogName) { return Map.of(); }

    @Override
    public boolean catalogExists(String catalogName) { return false; }
}
