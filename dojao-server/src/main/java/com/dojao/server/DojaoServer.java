package com.dojao.server;

import com.dojao.common.MasterModule;
import com.dojao.core.config.MasterConfig;
import com.dojao.server.function.PythonBackend;
import com.dojao.server.function.SidecarPool;
import com.dojao.server.util.PythonBackendBootstrap;
import com.dojao.server.master.MasterServer;
import com.dojao.metastore.CatalogDef;
import com.dojao.metastore.MetaStore;
import com.dojao.metastore.MetaStoreType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Dojao 服务启动器 —— 管理 Master 的启动和关闭。
 *
 * <pre>{@code
 *   DojaoServer server = new DojaoServer(MetaStoreType.MYSQL);
 *   server.registerConnector(new CatalogDef("mysql", "mysql", Map.of(
 *       "catalog.mysql.jdbcUrl", "jdbc:mysql://localhost:3306",
 *       "catalog.mysql.username", "root",
 *       "catalog.mysql.password", "123456"
 *   )));
 *   server.addModule(WebUiModule.class);
 *   server.start();
 *   Thread.currentThread().join();
 * }</pre>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/09
 */
public class DojaoServer {

    private static final Logger log = LoggerFactory.getLogger(DojaoServer.class);

    private final MetaStore metaStore;
    private final MasterConfig masterConfig;
    private final List<MasterModule> modules = new ArrayList<>();
    private MasterServer master;
    private PythonBackend pythonBackend;

    // ==================== 构造 ====================

    /** 默认构造器：SQLite 元数据存储（与生产部署一致）+ 默认 Master 配置。 */
    public DojaoServer() {
        this(new MetaStore(MetaStoreType.SQLITE), MasterConfig.defaults());
    }

    public DojaoServer(MetaStoreType metaStoreType) {
        this(new MetaStore(metaStoreType), MasterConfig.defaults());
    }

    public DojaoServer(MetaStore metaStore) {
        this(metaStore, MasterConfig.defaults());
    }

    public DojaoServer(MetaStore metaStore, MasterConfig masterConfig) {
        this.metaStore = metaStore;
        this.masterConfig = masterConfig;
    }

    // ==================== 配置 ====================

    /** 注册一个数据源 catalog。 */
    public void registerConnector(CatalogDef catalogDef) {
        metaStore.registerConnector(catalogDef);
        log.info("Catalog 注册完成: name={}, type={}", catalogDef.name(), catalogDef.type());
    }

    /** 注册可插拔模块实例，在 {@link #start()} 之前调用。 */
    public void addModule(MasterModule module) {
        modules.add(module);
        log.info("模块已注册: {}", module.getClass().getSimpleName());
    }

    /** 注册可插拔模块（通过 Class 反射实例化，需提供无参构造器），在 {@link #start()} 之前调用。 */
    public void addModule(Class<? extends MasterModule> moduleClass) {
        try {
            MasterModule module = moduleClass.getDeclaredConstructor().newInstance();
            modules.add(module);
            log.info("模块已注册: {}", moduleClass.getSimpleName());
        } catch (Exception e) {
            throw new RuntimeException("无法实例化模块: " + moduleClass.getName(), e);
        }
    }

    // ==================== 启停 ====================

    /** 启动 Master（对外提供 QueryService）。 */
    public void start() throws Exception {
        // 1. 创建 Master
        log.info(">>> 创建 Master...");
        master = new MasterServer(masterConfig, metaStore);

        // 2. 注册模块到 Master
        for (MasterModule module : modules) {
            master.addModule(module);
        }

        // 3. 注册 Python 函数后端（懒加载：首次用 python 函数才 spawn sidecar 池，
        //    启动不因无 python 环境而失败；available() 供心跳上报）
        pythonBackend = new PythonBackend(PythonBackendBootstrap.pythonBin(),
                PythonBackendBootstrap.udfRuntime(), SidecarPool.Config.defaults());
        pythonBackend.install();

        // 4. 启动 Master
        master.start();
        log.info(">>> Master 已启动: Flight={}", masterConfig.port());
        log.info(">>> Dojao 启动完成");
    }

    // ==================== 关闭 ====================

    /** 关闭所有服务。 */
    public void shutdown() {
        log.info(">>> 正在关闭 Dojao...");
        if (master != null) {
            master.shutdown();
        }
        if (pythonBackend != null) {
            pythonBackend.close();
        }
        log.info(">>> Dojao 已关闭");
    }

    // ==================== 访问器 ====================

    public MasterServer getMaster() {
        return master;
    }

    public MetaStore getMetaStore() {
        return metaStore;
    }

    public MasterConfig getMasterConfig() {
        return masterConfig;
    }

}
