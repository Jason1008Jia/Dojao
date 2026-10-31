package com.dojao.metastore;

import com.dojao.metastore.storage.MetaStoreStorage;
import com.dojao.metastore.storage.MetaStoreStorageConfig;
import com.dojao.metastore.storage.MetaStoreStorageFactory;
import com.dojao.metastore.storage.StorageType;
import com.dojao.spi.bean.Catalog;
import com.dojao.spi.bean.Column;
import com.dojao.spi.bean.FunctionDefinition;
import com.dojao.spi.bean.Schema;
import com.dojao.spi.bean.Table;
import com.dojao.spi.service.ConnectorFactory;
import com.dojao.spi.service.ConnectorRegistry;
import com.dojao.spi.service.DojaoConnector;
import com.dojao.spi.service.DojaoMetaService;
import com.dojao.spi.util.CatalogProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Dojao 元数据中心：管理所有连接器的元数据，提供统一的 catalog/schema/table/column 查询。
 *
 * <p>本类实现 SPI 层的 {@link DojaoMetaService}（<b>Dojao 自身</b>的注册表：catalog 注册信息 +
 * 已持久化函数）。此外还提供一层「扇出到各连接器」的聚合视图
 * （{@link #getConnector}/{@link #listAllSchemas}/{@link #getColumns} 等），
 * 那是给语法/语义层用的跨 catalog 便捷入口，不属于任何 SPI 契约。</p>
 *
 * <p>职责：
 * <ul>
 *   <li>注册 / 注销连接器（每个连接器对应一个 catalog 的 DojaoConnector）</li>
 *   <li>通过可配置的外接存储持久化 catalog 注册信息</li>
 *   <li>提供跨 catalog 的统一元数据查询</li>
 * </ul>
 *
 * <p>存储后端支持：
 * <ul>
 *   <li>MYSQL（{@code new MetaStore()} 无参时的默认）</li>
 *   <li>SQLITE（本地单文件，持久化；集群 Master / Worker 实际使用）</li>
 *   <li>POSTGRESQL</li>
 *   <li>SQLSERVER</li>
 *   <li>LOCAL（内存，不持久化；测试用）</li>
 * </ul>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/6/27
 */
public class MetaStore implements DojaoMetaService {

    private static final Logger log = LoggerFactory.getLogger(MetaStore.class);

    // ==================== 存储后端 ====================

    /** 外接存储后端（持久化 catalog 注册信息）。 */
    private final MetaStoreStorage storage;

    // ==================== 连接器注册表（内存） ====================

    /** catalog 名称 → 对应的连接器 DojaoConnector 实例。 */
    private final Map<String, DojaoConnector> connectors = new ConcurrentHashMap<>();

    /** 懒装载（{@link #getConnector} 未命中的回落路径）的串行化锁 —— miss 才走，不在热路径上。 */
    private final Object lazyLoadLock = new Object();

    /** 由派发载荷装入的 catalog 定义签名（{@link #installCatalog} 的幂等依据）。 */
    private final Map<String, String> installedSignatures = new ConcurrentHashMap<>();

    /**
     * 连接器工厂注册表：classpath（ServiceLoader）+ 插件目录 {@code lib/connectors/*.jar}。
     *
     * <p>静态 —— 「本进程有哪些连接器可用」是进程级事实，与具体 catalog 无关，
     * 且语法/语义层（{@code CREATE CATALOG ... USING <type>} 的准入校验）也要问它。</p>
     */
    private static final ConnectorRegistry CONNECTOR_FACTORIES = ConnectorRegistry.load();

    static {
        for (Map.Entry<String, String> e : CONNECTOR_FACTORIES.failures().entrySet()) {
            // 连接器加载失败必须吵 —— 否则现场表现是「未知的连接器类型」，没人想得到是 jar 的问题
            log.error("连接器加载失败 [{}]: {}", e.getKey(), e.getValue());
        }
    }

    // ==================== 构造函数 ====================

    /**
     * 使用 MetaStoreType 创建（自动加载对应存储后端）。
     *
     * <ul>
     *   <li>{@link MetaStoreType#MYSQL} → MysqlMetaStoreStorage</li>
     *   <li>{@link MetaStoreType#SQLITE} → SqliteMetaStoreStorage（本地 db 文件，持久化）</li>
     *   <li>{@link MetaStoreType#LOCAL} → 内存存储（不持久化）</li>
     *   <li>{@link MetaStoreType#POSTGRESQL} → PostgresMetaStoreStorage</li>
     * </ul>
     */
    public MetaStore(MetaStoreType type) {
        this(toStorageConfig(type));
    }

    /** 使用指定配置创建 MetaStore。 */
    public MetaStore(MetaStoreStorageConfig config) {
        this.storage = MetaStoreStorageFactory.create(config);
        this.storage.initialize();
        reloadCatalogsFromStorage();
    }

    /**
     * 从外接存储恢复所有 catalog 的连接器实例（Server 重启时调用）。
     *
     * <p>逐个 catalog 隔离：某个 catalog 装不起来（连接器 jar 不在 / 数据源连不上 / 属性写错）
     * 不该让整个进程起不来 —— 但<b>必须 ERROR 级报出来</b>，并把失败原因说清楚。
     * 早先这里缺了「连接器类型没有对应工厂就跳过」的可见性，症状是查询时报
     * 「未知 catalog」，而真正的原因（jar 没放对地方）谁也不知道。</p>
     */
    private void reloadCatalogsFromStorage() {
        for (String catalogName : storage.listCatalogs()) {
            try {
                DojaoConnector connector = createFromStorage(catalogName);
                if (connector != null) {
                    connectors.put(catalogName, connector);
                }
            } catch (RuntimeException e) {
                log.error("Catalog '{}' 装载失败，本次跳过: {}", catalogName, e.getMessage());
            }
        }
    }

    /**
     * 按外接存储里的注册信息实例化一个连接器。
     *
     * <p>存储里没有该 catalog → 返回 {@code null}。<b>有注册信息但装不起来 → 抛异常</b>，
     * 带上「缺哪个类型 / 去哪放 jar」的具体指引，由调用方决定是中断启动（{@link #registerConnector(CatalogDef)}）
     * 还是记日志继续（{@link #reloadCatalogsFromStorage()}）。</p>
     */
    private DojaoConnector createFromStorage(String catalogName) {
        String type = storage.getConnectorType(catalogName).orElse(null);
        if (type == null) return null;

        ConnectorFactory factory = CONNECTOR_FACTORIES.factory(type).orElseThrow(() ->
                new IllegalStateException("连接器类型 '" + type + "' 没有对应的 ConnectorFactory"
                        + "（catalog: " + catalogName + "）。当前可用的类型: ["
                        + String.join(", ", CONNECTOR_FACTORIES.types())
                        + "]；若该类型确实应该存在，请确认它的 jar 在 classpath 或插件目录 "
                        + ConnectorRegistry.pluginDirDescription() + " 下，且 jar 里有 META-INF/services/"
                        + ConnectorFactory.class.getName() + " 声明"));

        Map<String, String> properties = storage.getCatalogProperties(catalogName);
        try {
            DojaoConnector connector = factory.create(catalogName,
                    CatalogProperties.normalize(type, properties));
            log.info("Catalog 已从存储装载: name={}, type={}", catalogName, type);
            return connector;
        } catch (RuntimeException e) {
            throw new IllegalStateException("catalog '" + catalogName + "'（类型 " + type
                    + "）的连接器构造失败: " + e.getMessage(), e);
        }
    }

    /** 使用默认 MySQL 配置创建 MetaStore。 */
    public MetaStore() {
        this(MetaStoreType.MYSQL);
    }

    private static MetaStoreStorageConfig toStorageConfig(MetaStoreType type) {
        return switch (type) {
            case MYSQL -> MetaStoreStorageConfig.defaultMysql();
            case SQLITE -> MetaStoreStorageConfig.defaultSqlite();
            case LOCAL -> MetaStoreStorageConfig.defaultLocal();
            case POSTGRESQL -> new MetaStoreStorageConfig(
                    StorageType.POSTGRESQL,
                    "jdbc:postgresql://localhost:5432/dojao",
                    "postgres", "", new LinkedHashMap<>());
        };
    }

    // ==================== Connector Factory 注册 ====================

    /** 手动注册 ConnectorFactory（用于 SPI 无法覆盖的场景）。 */
    public static void registerConnectorFactory(ConnectorFactory factory) {
        CONNECTOR_FACTORIES.register(factory);
    }

    // ==================== 连接器注册 ====================

    /**
     * 通过 CatalogDef 注册连接器（简化 API，properties 不含 type）。
     *
     * <pre>{@code
     *   metaStore.registerConnector(new CatalogDef("mysql", "mysql", Map.of(
     *       "catalog.mysql.jdbcUrl",  "jdbc:mysql://localhost:3306",
     *       "catalog.mysql.username", "root",
     *       "catalog.mysql.password", "123456"
     *   )));
     * }</pre>
     *
     * <p>属性键遵循 catalog 命名规范 {@code catalog.<连接器类型>.<属性名>}
     * （见 {@link com.dojao.spi.util.CatalogProperties}）；旧的裸键（{@code jdbcUrl} 等）亦兼容。</p>
     *
     * <p><b>落库前会真的连一次数据源</b>（{@link DojaoConnector#probeConnectivity(String)}）：
     * 连不上直接抛异常，catalog 既不落库也不进内存。这就是「构造连接器」与「连得上」的分界 ——
     * 工厂的 {@code create()} 不发网络 I/O，光靠它注册，错误配置要等到首次使用才暴露。</p>
     */
    public void registerConnector(CatalogDef catalogDef) {
        ConnectorFactory factory = CONNECTOR_FACTORIES.factory(catalogDef.type()).orElseThrow(() ->
                new IllegalArgumentException("没有注册类型为 '" + catalogDef.type()
                        + "' 的 ConnectorFactory。当前可用的类型: ["
                        + String.join(", ", CONNECTOR_FACTORIES.types())
                        + "]；若该类型确实应该存在，请确认它的 jar 在 classpath 或插件目录 "
                        + ConnectorRegistry.pluginDirDescription() + " 下"));

        // 统一归一化（catalog.<类型>.<属性名> → 裸键）：第三方连接器即使没自己调 CatalogProperties，
        // 也能直接用规范写法。家族前缀（hive2/hive3 共用 catalog.hive.）由连接器侧补一次 normalize 兜住。
        DojaoConnector connector = factory.create(catalogDef.name(),
                CatalogProperties.normalize(catalogDef.type(), catalogDef.properties()));

        // 落库前<b>真的连一次数据源</b>：工厂的 create 只把 url/账号存进字段（不发网络 I/O），
        // 所以坏配置本来也能「注册成功」，直到首次查询/展开目录树才炸 —— 那时早已入库，
        // 用户看到的是「catalog 建好了却加载失败」，真因与创建时机脱节。探针失败 → 异常上抛，
        // 下面的持久化与内存注册一步都不执行：连不上的 catalog 什么都不留。
        connector.probeConnectivity(catalogDef.name());

        // 持久化：type 单独透传，properties 不含 type；owner 随定义落库（is_private 隔离用）
        storage.registerCatalog(catalogDef.name(), catalogDef.type(),
                new LinkedHashMap<>(catalogDef.properties()), catalogDef.owner());
        connectors.put(catalogDef.name(), connector);
    }

    /**
     * 通过 Map 注册连接器（兼容 JSON 反序列化）。
     *
     * <pre>{@code
     *   metaStore.registerConnector(Map.of(
     *       "name", "mysql", "type", "mysql",
     *       "properties", Map.of("catalog.mysql.jdbcUrl", "jdbc:mysql://localhost:3306", ...)
     *   ));
     * }</pre>
     */
    @SuppressWarnings("unchecked")
    public void registerConnector(Map<String, Object> catalogMap) {
        registerConnector(CatalogDef.fromMap(catalogMap));
    }

    /**
     * 注册一个连接器（同时持久化到外接存储）。
     *
     * @param catalogName   catalog 名称
     * @param metaService   连接器提供的元数据服务
     * @param properties    catalog 属性（如 connectorType, jdbcUrl 等）
     */
    public void registerConnector(String catalogName,
                                   DojaoConnector metaService,
                                   Map<String, String> properties) {
        Map<String, String> props = new LinkedHashMap<>(properties);
        String connectorType = props.remove("type");
        if (connectorType == null) {
            // 未显式给 type → 问连接器自己（如 MysqlConnector → "mysql"），避免注册表里留下 "unknown"
            connectorType = metaService.getCatalog(catalogName)
                    .map(Catalog::type)
                    .orElse("unknown");
        }

        // 持久化：type 单独透传，props 不再含 type
        storage.registerCatalog(catalogName, connectorType, props);

        // 注册到内存
        connectors.put(catalogName, metaService);
    }

    /** 注册一个连接器（无额外属性）。 */
    public void registerConnector(String catalogName, DojaoConnector metaService) {
        registerConnector(catalogName, metaService, Map.of());
    }

    /**
     * 注销一个连接器（同时从外接存储删除）。
     */
    public void unregisterConnector(String catalogName) {
        storage.unregisterCatalog(catalogName);
        connectors.remove(catalogName);
        // 已卸载 → 装过的签名也必须一起作废：否则「DROP 后按完全相同的定义重建」会命中幂等
        // 而跳过装配，connectors 里却没有它 → 解析成「未知 catalog」（静默失效）。
        installedSignatures.remove(catalogName);
    }

    /**
     * 是否已注册该 catalog。
     */
    public boolean hasCatalog(String catalogName) {
        return connectors.containsKey(catalogName);
    }

    /**
     * 获取 catalog 对应的 DojaoConnector。
     *
     * <p>内存表未命中时<b>回落到外接存储现场装载</b>（成功即缓存，失败返回空）：</p>
     *
     * <p>集群里 Master 与 Worker 各有<b>独立</b>的 MetaStore 实例，且都只在<b>进程启动时</b>
     * 从存储恢复一次（{@link #reloadCatalogsFromStorage()}）。若某个进程启动之后才执行
     * {@code CREATE CATALOG}，注册信息虽写进了共享的元数据库，其他进程的内存表却永远看不到它 ——
     * 分布式查询一派发过去就是「未知 catalog」。这条回落路径让新注册的 catalog 对<b>所有进程</b>
     * 即刻生效，不必重启 Worker。</p>
     *
     * <p>现场装载失败（连接器 jar 缺失 / 数据源连不上 / 属性写错）时返回空但<b>打 ERROR</b>：
     * 本方法刻意不抛 —— 调用方拿它当「这个 catalog 可用吗」的存在性判断（部分调用点还要求宽容，
     * 如 CTAS 目标校验不该抢下游报错的职责）。但把失败原因写进日志，免得现场只看到一句
     * 「unknown catalog」而不知道真正的原因。</p>
     */
    public Optional<DojaoConnector> getConnector(String catalogName) {
        DojaoConnector cached = connectors.get(catalogName);
        if (cached != null) return Optional.of(cached);
        // 存储里压根没注册过 → 直接空（别为了一个拼错的名字去连一次数据源）
        if (storage.getConnectorType(catalogName).isEmpty()) return Optional.empty();

        synchronized (lazyLoadLock) {
            cached = connectors.get(catalogName);      // 双检：可能已被其他线程装载
            if (cached != null) return Optional.of(cached);
            DojaoConnector created;
            try {
                created = createFromStorage(catalogName);
            } catch (RuntimeException e) {
                log.error("Catalog '{}' 现场装载失败: {}", catalogName, e.getMessage());
                return Optional.empty();
            }
            if (created == null) return Optional.empty();
            connectors.put(catalogName, created);
            return Optional.of(created);
        }
    }

    /** 已注册的 ConnectorFactory 类型（排序后，供错误提示 / 入参校验）。 */
    public static List<String> registeredTypes() {
        return CONNECTOR_FACTORIES.types();
    }

    /** 是否存在该类型的 ConnectorFactory（{@code CREATE CATALOG ... USING <type>} 的准入校验）。 */
    public static boolean supportsType(String type) {
        return CONNECTOR_FACTORIES.supports(type);
    }

    /** 按类型取 ConnectorFactory；未注册（含 type 为 null）返回空。 */
    public static Optional<ConnectorFactory> factoryFor(String type) {
        return CONNECTOR_FACTORIES.factory(type);
    }

    /** 连接器工厂注册表（只读用途：诊断 / 展示插件加载情况）。 */
    public static ConnectorRegistry connectorRegistry() {
        return CONNECTOR_FACTORIES;
    }

    // ==================== 派发随附的 catalog 定义（真分布式 Worker 用） ====================

    /**
     * 把一个 catalog <b>定义</b>装进内存表（<b>不落库</b>）—— 真分布式 Worker 的入口：
     * Master 派发任务时把该任务用到的 catalog 定义（type + 连接属性）附在载荷里，
     * Worker 收到即装入，于是<b>不必与 Master 共享元数据库、也不必预置任何 catalog</b>。
     *
     * <p>与 {@link #registerConnector(CatalogDef)} 的区别只在"是否持久化"：本方法纯粹是
     * 一次进程内的覆盖安装，Worker 不该因为执行一个任务就往元数据库里写行（那个库是 Master
     * 的地盘，且两进程未必看得到同一个文件）。</p>
     *
     * <p><b>幂等</b>：按「type + 属性」签名比对，定义没变（绝大多数派发）时直接返回，
     * 不会每个任务都重建一次连接器。定义变了（Master 端 DROP 后重建）则覆盖安装 ——
     * 以 Master 的载荷为准。</p>
     *
     * @throws IllegalArgumentException 类型没有对应的 ConnectorFactory（提示可用类型与 jar 位置）
     * @throws RuntimeException         连接器构造失败（属性写错 / 数据源连不上），原样上抛
     */
    public void installCatalog(CatalogDef def) {
        String signature = signatureOf(def);
        if (signature.equals(installedSignatures.get(def.name()))) {
            return;   // 同一份定义已装过 → 不动（派发热路径）
        }
        ConnectorFactory factory = factoryFor(def.type()).orElseThrow(() ->
                new IllegalArgumentException("未知的连接器类型: '" + def.type() + "'（catalog "
                        + def.name() + "）。当前可用的类型: " + String.join(", ", registeredTypes())
                        + "；若类型没问题，请确认对应连接器 jar 在 classpath 或插件目录 "
                        + ConnectorRegistry.pluginDirDescription() + " 下"
                        + "（服务发现走 META-INF/services/com.dojao.spi.service.ConnectorFactory）"));
        connectors.put(def.name(), factory.create(def.name(),
                CatalogProperties.normalize(def.type(), def.properties())));
        installedSignatures.put(def.name(), signature);
        log.info("Catalog 已由派发载荷装配: name={}, type={}", def.name(), def.type());
    }

    /** 定义签名：type + 按 key 排序的属性（与 Map 顺序无关，用于 installCatalog 的幂等比对）。 */
    private static String signatureOf(CatalogDef def) {
        StringBuilder sb = new StringBuilder(def.type());
        def.properties().entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(e -> sb.append('\u0000').append(e.getKey()).append('=').append(e.getValue()));
        return sb.toString();
    }

    // ==================== 元数据查询（聚合所有 catalog）====================

    /**
     * 列出所有已注册的 catalog（从外接存储读取）。
     */
    @Override
    public List<String> listRegisteredCatalogs() {
        return storage.listCatalogs();
    }

    /**
     * 列出对指定视角可见的 catalog：全局可见 + 归属该视角的私有 catalog。
     *
     * @param viewer 登录用户名；{@code null} = 看全部（admin / 无登录全局视角）。
     */
    public List<String> listRegisteredCatalogs(String viewer) {
        List<String> all = storage.listCatalogs();
        if (viewer == null) {
            return all;
        }
        List<String> result = new ArrayList<>();
        for (String name : all) {
            if (isCatalogVisibleTo(name, viewer)) {
                result.add(name);
            }
        }
        return result;
    }

    /** catalog 是否对 viewer 可见（全局 / 非私有 / viewer 是创建者）。{@code viewer==null} = 全可见。 */
    public boolean isCatalogVisibleTo(String catalogName, String viewer) {
        if (viewer == null) {
            return storage.catalogExists(catalogName);
        }
        if (!storage.catalogExists(catalogName)) {
            return false;
        }
        return visibleTo(viewer, storage.getCatalogOwner(catalogName).orElse(null),
                storage.getCatalogProperties(catalogName));
    }

    /** 可见性统一判据：非私有恒可见；私有仅创建者（或 {@code viewer==null} 的全见视角）可见。 */
    public static boolean visibleTo(String viewer, String owner, Map<String, String> properties) {
        if (viewer == null) {
            return true;   // 全见视角（admin / 无登录）
        }
        String priv = properties == null ? null : properties.get("is_private");
        boolean isPrivate = priv != null && "true".equalsIgnoreCase(priv.trim());
        if (!isPrivate) {
            return true;   // 未指定 is_private=true → 全局
        }
        if (owner == null || owner.isBlank()) {
            return true;   // 无归属（CLI/JDBC 创建）→ 全局
        }
        return owner.equals(viewer);
    }

    /**
     * 列出指定 catalog 下的所有 schema。
     */
    public List<String> listAllSchemas(String catalog) {
        DojaoConnector svc = connectors.get(catalog);
        if (svc == null) return List.of();
        return svc.listSchemas(catalog);
    }

    /**
     * 列出指定 schema 下的所有 table。
     */
    public List<String> listAllTables(String catalog, String schema) {
        DojaoConnector svc = connectors.get(catalog);
        if (svc == null) return List.of();
        return svc.listTables(catalog, schema);
    }

    /**
     * 获取指定表的列信息。
     */
    public List<Column> getColumns(String catalog, String schema, String table) {
        DojaoConnector svc = connectors.get(catalog);
        if (svc == null) return List.of();
        return svc.getTableColumns(catalog, schema, table);
    }

    // ==================== Catalog 信息 ====================

    /**
     * 获取 catalog 的详细信息（含属性，从外接存储读取）。
     */
    @Override
    public Optional<Catalog> getRegisteredCatalog(String catalogName) {
        Optional<String> connectorType = storage.getConnectorType(catalogName);
        if (connectorType.isEmpty()) return Optional.empty();

        Map<String, String> props = storage.getCatalogProperties(catalogName);
        String createdAt = storage.getCatalogCreatedAt(catalogName).orElse(null);
        String updatedAt = storage.getCatalogUpdatedAt(catalogName).orElse(null);
        return Optional.of(new Catalog(catalogName, connectorType.get(), props, createdAt, updatedAt));
    }

    // ==================== Function 信息 ====================

    /**
     * 列出所有<b>已持久化</b>的函数定义（CREATE FUNCTION 落库的那些，跨重启仍在）。
     *
     * <p>只读外部调用方（如 AI 层的 Agent）通过本方法探查框架里现存的函数，
     * 无须关心底层是 SQLite / MySQL 还是别的实现。</p>
     */
    @Override
    public List<FunctionDefinition> listFunctions() {
        return storage.listFunctions();
    }

    /**
     * 按名获取已持久化的函数定义；未注册返回 {@link Optional#empty()}。
     */
    @Override
    public Optional<FunctionDefinition> getFunction(String name) {
        return storage.getFunction(name);
    }

    /**
     * 函数是否已持久化。
     */
    @Override
    public boolean functionExists(String name) {
        return storage.functionExists(name);
    }

    /**
     * 持久化一个函数定义（CREATE FUNCTION）。
     */
    @Override
    public void registerFunction(FunctionDefinition definition) {
        storage.registerFunction(definition);
    }

    /**
     * 删除一个函数定义（DROP FUNCTION）。
     */
    @Override
    public void dropFunction(String name) {
        storage.dropFunction(name);
    }

    // ==================== Schema 信息 ====================

    /**
     * 获取 schema 详细信息。
     */
    public Optional<Schema> getSchemaInfo(String catalog, String schema) {
        DojaoConnector svc = connectors.get(catalog);
        if (svc == null) return Optional.empty();
        return svc.getSchema(catalog, schema);
    }

    // ==================== Table 信息 ====================

    /**
     * 获取 table 详细信息。
     */
    public Optional<Table> getTableInfo(String catalog, String schema, String table) {
        DojaoConnector svc = connectors.get(catalog);
        if (svc == null) return Optional.empty();
        return svc.getTable(catalog, schema, table);
    }

    // ==================== 存在性校验 ====================

    /**
     * 检查 catalog 是否已在 Dojao 注册（先查内存，再查存储）。
     */
    @Override
    public boolean isCatalogRegistered(String catalog) {
        return connectors.containsKey(catalog) || storage.catalogExists(catalog);
    }

    /**
     * 检查 schema 是否存在。
     */
    public boolean schemaExists(String catalog, String schema) {
        DojaoConnector svc = connectors.get(catalog);
        if (svc == null) return false;
        return svc.schemaExists(catalog, schema);
    }

    /**
     * 检查 table 是否存在。
     */
    public boolean tableExists(String catalog, String schema, String table) {
        DojaoConnector svc = connectors.get(catalog);
        if (svc == null) return false;
        return svc.tableExists(catalog, schema, table);
    }

    /**
     * 检查 column 是否存在。
     */
    public boolean columnExists(String catalog, String schema, String table, String column) {
        DojaoConnector svc = connectors.get(catalog);
        if (svc == null) return false;
        return svc.columnExists(catalog, schema, table, column);
    }

    // ==================== 存储管理 ====================

    /**
     * 获取底层存储后端（<b>仅供 Server 装配使用</b>，如注入 FunctionManager 做函数落库）。
     *
     * <p>业务代码与 Agent 请走 SPI 接口 {@link DojaoMetaService}（{@link #listFunctions()} /
     * {@link #getFunction(String)} / {@link #listRegisteredCatalogs()} /
     * {@link #getRegisteredCatalog(String)}），不要依赖具体后端类型
     * —— 后端由配置决定，可随时从 SQLite 换成 MySQL 等。</p>
     */
    public MetaStoreStorage getStorage() {
        return storage;
    }

    /**
     * 关闭 MetaStore，释放存储资源。
     */
    public void close() {
        storage.close();
        connectors.clear();
    }

    // ==================== 统计信息 ====================

    /**
     * 获取已注册的 catalog 数量。
     */
    public int catalogCount() {
        return connectors.size();
    }

    /**
     * 获取所有已注册 catalog 的 schema 总数。
     */
    public int schemaCount() {
        int count = 0;
        for (String catalog : connectors.keySet()) {
            count += listAllSchemas(catalog).size();
        }
        return count;
    }
}
