package com.dojao.metastore.storage.local;

import com.dojao.metastore.storage.MetaStoreStorage;
import com.dojao.spi.bean.FunctionDefinition;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 本地嵌入式存储实现（基于内存 Map，不依赖外部数据库）。
 *
 * <p>适用于开发/测试环境，数据不持久化。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/6/27
 */
public class LocalMetaStoreStorage implements MetaStoreStorage {

    private final Map<String, String> connectorTypes = new ConcurrentHashMap<>();
    private final Map<String, Map<String, String>> properties = new ConcurrentHashMap<>();
    /** catalog 名 → 归属创建者（空=全局）。 */
    private final Map<String, String> catalogOwners = new ConcurrentHashMap<>();

    /** 函数名 → 函数定义（LOCAL 函数持久化：进程内 Map，重启丢失）。 */
    private final Map<String, FunctionDefinition> functions = new ConcurrentHashMap<>();

    private volatile boolean ready = false;

    @Override
    public void initialize() {
        this.ready = true;
    }

    @Override
    public void close() {
        this.ready = false;
        connectorTypes.clear();
        properties.clear();
        catalogOwners.clear();
        functions.clear();
    }

    @Override
    public boolean isReady() {
        return ready;
    }

    @Override
    public void registerCatalog(String catalogName, String connectorType,
                                 Map<String, String> props) {
        registerCatalog(catalogName, connectorType, props, null);
    }

    @Override
    public void registerCatalog(String catalogName, String connectorType,
                                 Map<String, String> props, String owner) {
        connectorTypes.put(catalogName, connectorType);
        properties.put(catalogName, new LinkedHashMap<>(props));
        if (owner == null || owner.isBlank()) {
            catalogOwners.remove(catalogName);
        } else {
            catalogOwners.put(catalogName, owner);
        }
    }

    @Override
    public Optional<String> getCatalogOwner(String catalogName) {
        return Optional.ofNullable(catalogOwners.get(catalogName)).filter(s -> !s.isBlank());
    }

    @Override
    public void unregisterCatalog(String catalogName) {
        connectorTypes.remove(catalogName);
        properties.remove(catalogName);
        catalogOwners.remove(catalogName);
    }

    @Override
    public List<String> listCatalogs() {
        List<String> result = new ArrayList<>(connectorTypes.keySet());
        Collections.sort(result);
        return result;
    }

    @Override
    public Optional<String> getConnectorType(String catalogName) {
        return Optional.ofNullable(connectorTypes.get(catalogName));
    }

    @Override
    public Map<String, String> getCatalogProperties(String catalogName) {
        Map<String, String> props = properties.get(catalogName);
        return props != null ? new LinkedHashMap<>(props) : Map.of();
    }

    @Override
    public boolean catalogExists(String catalogName) {
        return connectorTypes.containsKey(catalogName);
    }

    // ==================== Function 操作 ====================

    @Override
    public void registerFunction(FunctionDefinition def) {
        functions.put(def.name(), def);
    }

    @Override
    public void dropFunction(String name) {
        functions.remove(name);
    }

    @Override
    public Optional<FunctionDefinition> getFunction(String name) {
        return Optional.ofNullable(functions.get(name));
    }

    @Override
    public List<FunctionDefinition> listFunctions() {
        List<FunctionDefinition> result = new ArrayList<>(functions.values());
        result.sort(Comparator.comparing(FunctionDefinition::name));
        return result;
    }

    @Override
    public boolean functionExists(String name) {
        return functions.containsKey(name);
    }
}
