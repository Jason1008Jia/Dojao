package com.dojao.metastore.storage;

import com.dojao.spi.bean.FunctionDefinition;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 元数据外接存储接口。
 *
 * <p>负责持久化 catalog 注册信息（catalog 名称、连接器类型、属性等）。
 * 各实现类对应不同的存储后端。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/6/27
 */
public interface MetaStoreStorage {

    // ==================== 生命周期 ====================

    /**
     * 初始化存储（建表等）。
     */
    void initialize();

    /**
     * 关闭存储，释放资源。
     */
    void close();

    /**
     * 检查存储是否已初始化。
     */
    boolean isReady();

    // ==================== Catalog 操作 ====================

    /**
     * 注册一个 catalog。
     *
     * @param catalogName    catalog 名称
     * @param connectorType  连接器类型（如 mysql、postgres）
     * @param properties     catalog 属性（如 jdbcUrl 等）
     */
    void registerCatalog(String catalogName, String connectorType, Map<String, String> properties);

    /**
     * 注册一个 catalog（带归属创建者）。
     *
     * <p>默认委托无 owner 重载 —— 仅支持 owner 的存储后端（SQLite/MySQL/Local）覆盖。</p>
     *
     * @param owner 归属创建者（Web 控制台登录用户名）；{@code null} = 全局。
     */
    default void registerCatalog(String catalogName, String connectorType,
                                 Map<String, String> properties, String owner) {
        registerCatalog(catalogName, connectorType, properties);
    }

    /**
     * 获取 catalog 的归属创建者；未记录返回 {@link Optional#empty()}。
     */
    default Optional<String> getCatalogOwner(String catalogName) {
        return Optional.empty();
    }
    void unregisterCatalog(String catalogName);

    /**
     * 列出所有已注册的 catalog 名称。
     */
    List<String> listCatalogs();

    /**
     * 获取 catalog 的连接器类型。
     */
    Optional<String> getConnectorType(String catalogName);

    /**
     * 获取 catalog 的属性。
     */
    Map<String, String> getCatalogProperties(String catalogName);

    /**
     * 检查 catalog 是否已注册。
     */
    boolean catalogExists(String catalogName);

    /**
     * 获取 catalog 的创建时间。
     */
    default Optional<String> getCatalogCreatedAt(String catalogName) {
        return Optional.empty();
    }

    /**
     * 获取 catalog 的更新时间。
     */
    default Optional<String> getCatalogUpdatedAt(String catalogName) {
        return Optional.empty();
    }

    // ==================== Function 操作 ====================

    /**
     * 持久化一个函数定义（CREATE FUNCTION 永久）。
     *
     * <p>默认抛 {@link UnsupportedOperationException}：各存储后端按需覆盖。
     * {@link com.dojao.metastore.storage.sqlite.SqliteMetaStoreStorage} 与
     * {@link com.dojao.metastore.storage.local.LocalMetaStoreStorage} 已实现（前者落 db 文件、
     * 后者仅内存）；MySQL/PG/SQLServer 需建 {@code dojao_functions} 表后覆盖（后续 Phase）。</p>
     */
    default void registerFunction(FunctionDefinition def) {
        throw new UnsupportedOperationException(
                "存储后端尚未实现函数持久化（仅 LOCAL 已支持；MySQL/PG/SQLServer 需建 dojao_functions 表）");
    }

    /**
     * 删除一个函数定义（DROP FUNCTION）。
     */
    default void dropFunction(String name) {
        throw new UnsupportedOperationException(
                "存储后端尚未实现函数持久化（仅 LOCAL 已支持）");
    }

    /**
     * 按名取函数定义；未注册返回 {@link Optional#empty()}。
     */
    default Optional<FunctionDefinition> getFunction(String name) {
        return Optional.empty();
    }

    /**
     * 列出全部已持久化的函数定义（启动恢复用）。
     */
    default List<FunctionDefinition> listFunctions() {
        return List.of();
    }

    /**
     * 函数是否已存在。
     */
    default boolean functionExists(String name) {
        return getFunction(name).isPresent();
    }
}
