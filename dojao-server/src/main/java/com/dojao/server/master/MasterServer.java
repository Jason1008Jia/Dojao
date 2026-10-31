package com.dojao.server.master;

import com.dojao.common.MasterModule;
import com.dojao.common.memory.NodeMemory;
import com.dojao.core.config.MasterConfig;
import com.dojao.core.config.ResourceConfig;
import com.dojao.core.planner.Planner;
import com.dojao.core.planner.PlanningContext;
import com.dojao.core.runtime.worker.WorkerGateway;
import com.dojao.metastore.MetaStore;
import com.dojao.metastore.MetaStoreType;
import com.dojao.server.function.FunctionManager;
import com.dojao.spi.service.DojaoConnector;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Master 服务器（Arrow Flight）。生命周期: {@code start()} → {@code awaitTermination()} → {@code shutdown()}。
 *
 * <p>Master 对外提供 SQL 查询服务：语法/语义校验 → 物理规划 → 执行，经
 * <b>Arrow Flight doAction("execute_query")</b> 对客户端暴露（见 {@link MasterQueryFlightProducer}）。</p>
 *
 * <h3>使用方式</h3>
 * <pre>{@code
 *   MasterConfig config = MasterConfig.defaults();
 *   MasterServer server = new MasterServer(config);
 *   server.start();
 *   server.blockUntilShutdown();
 * }</pre>
 *
 *
 * 整个Dojao的Master服务
 *
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/07 15:30
 */
public class MasterServer {

    private static final Logger logger = LoggerFactory.getLogger(MasterServer.class);

    private final MasterConfig config;
    private final MetaStore metaStore;
    private final List<MasterModule> modules;
    // 进程级共享根（NodeMemory）的子分配器 —— Master 侧 Flight/网关的堆外占用进统一账本、受节点硬上限约束。
    private final BufferAllocator allocator = NodeMemory.shared().newChildAllocator("server/master", 0);
    private FlightServer flightServer;
    /** Master → Worker 派发通道；null → 查询只在本进程内执行（本地并行扫描）。 */
    private WorkerGateway workerGateway;
    /** 分布式执行开关：true → 单 join 算子链整体派发到 Worker 执行（默认关）。 */
    private boolean distributedExecution;
    /** 结果路由表（Worker 回传 → 任务队列）；null → start() 时新建（与 NetworkWorkerGateway 共享须注入）。 */
    private ResultRouter resultRouter;

    public MasterServer() {
        this(MasterConfig.defaults());
    }

    public MasterServer(MasterConfig config) {
        // 默认与真实部署（DojaoMaster）一致：SQLite 本地单文件，重启后 catalog / 函数仍在。
        // 刻意不用 new MetaStore() —— 那个无参默认是 MYSQL，会去连本地 MySQL 的 dojao 库，
        // 只有开发期才可能有那套环境，生产上等于「元数据凭空消失」。
        this(config, new MetaStore(MetaStoreType.SQLITE));
    }

    /** 允许注入自定义 MetaStore（测试用 LOCAL 等）。 */
    public MasterServer(MasterConfig config, MetaStore metaStore) {
        this.config = config;
        this.metaStore = metaStore;
        this.modules = new ArrayList<>();
    }

    /**
     * 注册可插拔模块实例。需在 {@link #start()} 之前调用。
     *
     * @param module 模块实例
     */
    public void addModule(MasterModule module) {
        modules.add(module);
        logger.info("模块已注册: {}", module.name());
    }

    /**
     * 注册可插拔模块（通过 Class 反射实例化，需提供无参构造器）。需在 {@link #start()} 之前调用。
     *
     * @param moduleClass 模块类
     */
    public void addModule(Class<? extends MasterModule> moduleClass) {
        try {
            MasterModule module = moduleClass.getDeclaredConstructor().newInstance();
            modules.add(module);
            logger.info("模块已注册: {}", module.name());
        } catch (Exception e) {
            throw new RuntimeException("无法实例化模块: " + moduleClass.getName(), e);
        }
    }

    /**
     * 注入 Master → Worker 派发通道（分布式扫描用）。需在 {@link #start()} 之前调用：
     * start() 构建 Planner 时会把它写进 {@link PlanningContext}，Planner 据此把 TableScan
     * 派发到 Worker 执行。不注入 → 查询只在本进程内执行，行为不变。
     */
    public void setWorkerGateway(WorkerGateway gateway) {
        this.workerGateway = gateway;
    }

    /**
     * 开启分布式执行：单 join 的算子链（扫描 / 分区 / join 本身）整体派发到 Worker 执行，
     * Master 只做派发 + 收集结果。需在 {@link #start()} 之前调用。默认关 → 现有单机行为不变。
     */
    public void setDistributedExecution(boolean on) {
        this.distributedExecution = on;
    }

    /**
     * 注入结果路由表（真分布式：{@link NetworkWorkerGateway} 与 Master Flight 生产者共享同一
     * 实例 —— 派发端注册 taskId→结果汇，接收端查表路由）。需在 {@link #start()} 之前调用；
     * 不注入 → start() 自建（单机 / 伪分布式场景无 Worker 回传，自建即可）。
     */
    public void setResultRouter(ResultRouter router) {
        this.resultRouter = router;
    }

    /** 获取已注册的模块列表（只读）。 */
    public List<MasterModule> getModules() {
        return Collections.unmodifiableList(modules);
    }

    /**
     * 构建 Arrow Flight 服务器并在配置的 host:port 上启动监听
     * 这是主Master 服务
     *
     * @throws IOException 端口绑定失败时抛出
     */
    public void start() throws Exception {
        // SQL 语法 + 语义验证模块
        SqlValidator sqlValidator = new SqlValidator(metaStore);

        // 物理规划：连接器提供者按 catalog 取连接器；每个分区要一份独立副本供并行扫描
        // （不支持复制的连接器由 SPI 默认 copy() 返回自身，退化为共享但不会读错）。
        PlanningContext.ConnectorProvider provider = catalog -> {
            DojaoConnector conn = metaStore.getConnector(catalog)
                    .orElseThrow(() -> new IllegalStateException("未知 catalog: " + catalog));
            return conn.copy();
        };
        PlanningContext.Builder ctxBuilder = PlanningContext.builder(provider);
        // 查询并行分区数：扫描 / hash 分区 / 分布式聚合共用（MasterConfig.parallelism，默认 4）
        ctxBuilder.parallelism(config.parallelism());
        // 算子内存预算统一在 ResourceConfig.query.slot.memory 管理（配置 > 内置默认 2G）
        ctxBuilder.memoryLimit(ResourceConfig.slotMemoryBytes());
        if (workerGateway != null) {
            ctxBuilder.workerGateway(workerGateway);
            logger.info("Master 已启用分布式扫描（WorkerGateway 已注入）");
        }
        if (distributedExecution) {
            ctxBuilder.distributedExecution(true);
            logger.info("Master 已启用分布式执行（单 join 算子链进 Worker）");
        }
        Planner planner = new Planner(ctxBuilder.build());
        // 函数持久化注入 + 启动恢复：从存储恢复永久函数定义（懒加载，首次查询才实例化）
        FunctionManager.init(metaStore.getStorage());
        FunctionManager.loadPermanentFunctions();
        QueryServiceImpl querySvc = new QueryServiceImpl(sqlValidator, planner, provider);

        // Flight 请求大小上限沿用原 gRPC 的 DojaoMaxMessageBytes（默认 512MB）
        // 合并控制面：查询入口 + Worker 注册/心跳/注销 + 结果回传路由（真分布式需要）
        ResultRouter router = resultRouter != null ? resultRouter : new ResultRouter();
        flightServer = FlightServer.builder()
                .allocator(allocator)
                .location(Location.forGrpcInsecure(config.host(), config.port()))
                .maxInboundMessageSize(Integer.getInteger("DojaoMaxMessageBytes", 512 << 20))
                .producer(new MasterControlFlightProducer(querySvc, com.dojao.core.runtime.master.WorkerRegistry.get(),
                        router, allocator))
                .build();
        flightServer.start();

        logger.info("===========================================");
        logger.info("  Dojao Master Server 已启动");
        logger.info("  host     : {}:{}", config.host(), config.port());
        logger.info("===========================================");

        // 查询监控接线：把 Master 实时执行信息（QueryManagerRegistry / WorkerRegistry / 算子快照）
        // 以 QueryMonitor 契约注入 WebUI 模块（dojao-ui 不感知 server，只面对契约与中性 DTO）。
        // 同时把引擎的 MetaStore 一并注入 —— UI 的目录树 / EXPLAIN 必须读引擎自己的注册表
        // （UI 侧另起一套 MetaStore 会写出分叉的 catalog，见 DojaoUiApp 类注释）。需在 start() 之前 bind。
        UiQueryMonitor monitor = new UiQueryMonitor(querySvc);
        for (MasterModule module : modules) {
            if (module instanceof com.dojao.ui.WebUiModule web) {
                web.bind(monitor, metaStore);
                logger.info("WebUI 模块已绑定查询监控与元数据库");
            }
        }

        // 启动所有注册的模块
        for (MasterModule module : modules) {
            logger.info("正在启动模块: {} ...", module.name());
            module.start();
            logger.info("模块 {} 已启动", module.name());
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("收到关闭信号，正在停止 MasterServer...");
            shutdown();
        }));
    }

    /** 阻塞直到 Flight 服务器终止。 */
    public void awaitTermination() throws InterruptedException {
        if (flightServer != null) {
            flightServer.awaitTermination();
        }
    }

    /** 启动并阻塞调用线程直到关闭。 */
    public void blockUntilShutdown() throws Exception {
        start();
        awaitTermination();
    }

    /** 优雅关闭 Flight 服务器。 */
    public void shutdown() {
        logger.info("正在停止 MasterServer...");

        // 逆序关闭模块
        List<MasterModule> reversed = new ArrayList<>(modules);
        Collections.reverse(reversed);
        for (MasterModule module : reversed) {
            try {
                logger.info("正在关闭模块: {} ...", module.name());
                module.shutdown();
                logger.info("模块 {} 已关闭", module.name());
            } catch (Exception e) {
                logger.error("关闭模块 {} 失败: {}", module.name(), e.getMessage());
            }
        }

        try {
            flightServer.shutdown();
            if (!flightServer.awaitTermination(30, TimeUnit.SECONDS)) {
                logger.warn("Flight 服务器在 30 秒内未能优雅终止");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        allocator.close();
        logger.info("MasterServer 已停止");
    }

    // ==================== 主入口 ====================

    public static void main(String[] args) throws Exception {
        MasterConfig config = MasterConfig.defaults();
        MasterServer server = new MasterServer(config);
        server.blockUntilShutdown();
    }
}
