package com.dojao.spi.service;

import com.dojao.spi.bean.Catalog;
import com.dojao.spi.bean.FunctionDefinition;

import java.util.List;
import java.util.Optional;

/**
 * <b>Dojao 自身元数据服务</b> —— 面向「Dojao 自己的注册表」的接口，不涉及任何目标数据源。
 *
 * <p>与 {@link DataSourceMetaService} 的分工：本接口问的是 <b>Dojao 自己</b>注册过哪些 catalog、
 * 落库过哪些函数（跨重启仍在）；{@link DataSourceMetaService} 问的是<b>连接器背后那个数据源</b>
 * 里有什么、有多大。两者互不相干 —— 前者是 Dojao 的簿记，后者是对外部系统的探查。</p>
 *
 * <p>由 {@code MetaStore}（dojao-metastore）实现，底下接 {@code MetaStoreStorage}
 * （SQLite / MySQL / PostgreSQL / SQLServer / LOCAL 内存）。上层（AI Agent 等）只依赖本接口，
 * 不关心底层是哪个存储后端。</p>
 *
 * <p><b>为什么 catalog 只有读、没有写</b>：catalog 的写路径必须与「创建并装载对应连接器实例」
 * 原子绑定，只能走 {@code MetaStore.registerConnector(...)}，单独暴露一个写 catalog 行的方法会
 * 造成「注册表里有、内存里没连接器」的不一致状态。函数定义没有这类副作用，故读写都放开。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/17
 */
public interface DojaoMetaService {

    // ==================== Catalog 注册表（只读） ====================

    /**
     * 列出所有<b>已注册</b>的 catalog 名称。
     */
    List<String> listRegisteredCatalogs();

    /**
     * 按名取已注册 catalog 的详细信息（含连接器类型、属性、创建/更新时间）。
     *
     * @return 未注册返回 {@link Optional#empty()}
     */
    Optional<Catalog> getRegisteredCatalog(String catalog);

    /**
     * 该 catalog 是否已在 Dojao 注册。
     */
    boolean isCatalogRegistered(String catalog);

    // ==================== Function 注册表 ====================

    /**
     * 列出所有<b>已持久化</b>的函数定义（{@code CREATE FUNCTION} 落库的那些，跨重启仍在）。
     *
     * <p>只读外部调用方（如 AI 层的 Agent）通过本方法探查框架里现存的函数，
     * 无须关心底层是 SQLite / MySQL 还是别的实现。</p>
     */
    List<FunctionDefinition> listFunctions();

    /**
     * 按名获取已持久化的函数定义；未注册返回 {@link Optional#empty()}。
     */
    Optional<FunctionDefinition> getFunction(String name);

    /**
     * 函数是否已持久化。
     */
    boolean functionExists(String name);

    /**
     * 持久化一个函数定义（{@code CREATE FUNCTION}）。
     *
     * <p>后端未实现函数持久化时抛 {@link UnsupportedOperationException}。</p>
     */
    void registerFunction(FunctionDefinition definition);

    /**
     * 删除一个函数定义（{@code DROP FUNCTION}）。
     */
    void dropFunction(String name);
}
