package com.dojao.server.worker;

import com.dojao.common.memory.NodeMemory;
import com.dojao.core.config.ResourceConfig;
import com.dojao.core.operator.SpillDirs;
import com.dojao.core.planner.PlanningContext;
import com.dojao.core.runtime.worker.WorkerNode;
import com.dojao.core.runtime.worker.WorkerState;
import com.dojao.metastore.MetaStore;
import com.dojao.metastore.MetaStoreType;
import com.dojao.server.exchange.NetworkExchangeManager;
import com.dojao.server.external.ExternalServiceManager;
import com.dojao.server.function.FunctionManager;
import com.dojao.server.util.ExternalProcessRegistry;
import com.dojao.server.util.ParentWatchdog;
import com.dojao.server.util.PythonBackendBootstrap;
import com.dojao.spi.service.ConnectorFactory;
import com.dojao.spi.service.ConnectorRegistry;
import com.dojao.spi.service.DojaoConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Dojao <b>Worker 独立进程</b>（真分布式）：一个 JVM 一个 Worker，经 Arrow Flight 与 Master /
 * 兄弟 Worker 通信。
 *
 * <p>启动拓扑（用户约定）：Master 1 个 + Worker N 个，各自独立 JVM；本机演示
 * worker1:4007 / worker2:4008 / worker3:4009，master:6530（master 1G 堆、worker 各 6G 堆）。</p>
 *
 * <p>装配（与伪分布式 {@code PseudoDistributedClusterTest} 对称，只把内存 gateway 换成网络版）：
 * <ol>
 *   <li>{@link NetworkMasterGateway}：Worker → Master 注册 / 心跳 / 注销（同一 FlightClient）；
 *       结果回传用 {@code openResultClient} 建专用独立客户端（分配器与流根同 root，零拷贝安全）；</li>
 *   <li>{@link WorkerControlProducer}：Worker 侧合并控制面 —— exchange doExchange 委托纯交换
 *       producer，dispatch/cancel 走 doAction（先建 producer，exchangeManager/workerManager 后置注入）；</li>
 *   <li>{@link NetworkExchangeManager}：Worker↔Worker 交换（启动本 Worker 的 Flight 接收端）+ 增量学习集群拓扑；</li>
 *   <li>{@link WorkerManager}：注册 + 心跳 + 本地任务池执行 Master 派发的片段。</li>
 * </ol>
 *
 * <p>集群拓扑引导：注册响应携带 Master 已知的全量节点表 → 注册成功后
 * {@code learnNodes(clusterNodes)} 一次性补齐兄弟 Worker 地址；后续 Master 每次派发还会附带最新
 * 节点表，增量学习兜底（解决"先注册的 Worker 不知道后注册的兄弟"）。</p>
 *
 * <p><b>启动参数</b>（均可用 -D 覆盖，默认取 {@code conf/dojao.conf}）：
 * <pre>
 *   java -cp ... com.dojao.server.worker.DojaoWorker 4007   # 第一个位置参数 = worker 端口（优先于配置）
 *   -Dsystem.worker.host=127.0.0.1  -Dsystem.worker.port=4007  # 本 Worker Flight 监听
 *   -Dsystem.master.host=127.0.0.1  -Dsystem.master.port=6530  # Master 地址
 *   -Dsystem.connector.url=jdbc:mysql://localhost:3306/dga     # 扫描连接器（与 Master 同源）
 *   -Dsystem.connector.user=root  -Dsystem.connector.password=123456
 *   -Dsystem.connector.catalog=mysql
 *   -Dsystem.metastore.sqlite.path=data/dojao-metastore.db     # 元数据 db 文件（与 Master 须同一文件）
 * </pre></p>
 *
 * <p>推荐经 {@code bin/dojao} 一键启动（读 {@code conf/dojao.conf} 的 {@code system.role}）：
 * {@code worker} 角色直接 exec 本类（无常驻启动器），{@code both} 由 {@code ClusterMain} 拉起。
 * 无任何显式端口时取默认 {@code 4009}。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/29
 */
public final class DojaoWorker {

    private static final Logger log = LoggerFactory.getLogger(DojaoWorker.class);

    private static final String VERSION = "1.0";

    /** 注册重试：Master 尚未就绪时按退避重试，最多约 30s。 */
    private static final int MAX_REGISTER_ATTEMPTS = 10;

    public static void main(String[] args) throws Exception {
        // 配置初始化必须第一行：读 conf/dojao.conf → 按角色展平 → 回填系统属性，
        // 让 dojao-metastore 等看不到 ResourceConfig 的模块也吃上配置值。
        ResourceConfig.init();

        // 启动清残留：把上次 kill -9 / 崩溃遗留的孤儿溢写目录删掉（关闭钩子没机会跑的场合）。
        // 只认溢写实例子目录命名且归属 PID 已死，别的文件/目录一律不碰。
        SpillDirs.cleanOrphanDirs();

        // 父进程看门狗：由 ClusterMain / PseudoDistributedClusterTest 经 -Dsystem.parent.pid 传入父进程 PID，
        // 父进程（红方块 / 任务管理器强杀）一死本进程立即退出，避免孤儿进程占住 400x 端口。
        // 独立启动 DojaoWorker（无该属性）时 no-op。
        ParentWatchdog.install();

        // 端口优先级：位置参数 > -Dsystem.worker.port / 配置文件 > 默认 4009（全机唯一 Worker 的取值）。
        int workerPort = firstIntArg(args).orElseGet(ResourceConfig::workerPort);
        String workerHost = ResourceConfig.workerHost();
        String masterHost = ResourceConfig.masterHost();
        int masterPort = ResourceConfig.masterPort();

        // ===== ① 连接器提供者（Worker 侧扫描用独立副本，游标互不干扰；与 Master 同源配置） =====
        // SQLITE：与 Master 共用同一 db 文件（默认 ./data/dojao-metastore.db，-Dsystem.metastore.sqlite.path 覆盖）
        MetaStore metaStore = new MetaStore(MetaStoreType.SQLITE);
        String catalog = ResourceConfig.connectorCatalog();
        String type = ResourceConfig.connectorType();
        String url = ResourceConfig.connectorUrl();
        String user = ResourceConfig.connectorUser();
        String pass = ResourceConfig.connectorPassword();
        // 与 DojaoMaster 同源：连接属性一并落库，避免持久化成 '{}' 导致各进程重建连接器时缺 jdbcUrl。
        Map<String, String> connProps = Map.of(
                "jdbcUrl", url,
                "username", user,
                "password", pass);
        // ★连接器已插件化：按类型向 ConnectorFactory 要实例，不再 import 具体连接器类。
        ConnectorFactory factory = MetaStore.factoryFor(type).orElseThrow(() ->
                new IllegalStateException("未安装连接器 '" + type + "'：把连接器插件 jar 放进 "
                        + ConnectorRegistry.pluginDirDescription()
                        + "，或 -Dsystem.connector.plugins.dir=<目录>；"
                        + "当前可用类型: " + String.join(", ", MetaStore.registeredTypes())));
        metaStore.registerConnector(catalog, factory.create(catalog, connProps), connProps);
        // 每个任务要一份独立副本：同一 Worker 上多个任务并发跑，共享实例会让游标互相踩
        // （不支持复制的连接器由 SPI 默认 copy() 返回自身）。
        PlanningContext.ConnectorProvider provider = c -> metaStore.getConnector(c)
                .orElseThrow(() -> new IllegalStateException("未知 catalog: " + c))
                .copy();

        // ===== ② Worker → Master 网关（注册/心跳/注销 + 结果回传共用 FlightClient） =====
        NetworkMasterGateway masterGw = new NetworkMasterGateway(masterHost, masterPort);

        // 外部进程注册表：本 Worker 启动的全部外部服务（python sidecar 等）登记处。
        // shutdown 时 killAll 优先清掉再退出；任务结束时 killByQuery 回收该 query 绑定的进程。
        ExternalProcessRegistry registry = new ExternalProcessRegistry();

        // ===== ③ Worker 控制面 producer（先建：NetworkExchangeManager 需要它启动 Flight 服务器） =====
        // exchangeManager / workerManager 为后置注入（装配循环：producer → exchange → workerManager）。
        // metaStore 是「Master 随载荷送来的 catalog 定义」的落点：Worker 收到即装入内存表，
        // 于是运行时新建的 catalog 无需 Worker 重启、也不依赖与 Master 共享元数据库文件。
        WorkerControlProducer producer = new WorkerControlProducer(masterGw, provider, registry,
                metaStore, NodeMemory.shared().newChildAllocator("gateway/control", 0));

        // ===== ④ 交换层（Worker↔Worker）：启动本 Worker 的 Flight 接收端 + 网络发送通道 =====
        // localWorkerIds 只含自身（真分布式每个 Worker 独占 JVM），allNodes 先空，注册后 learnNodes 补齐。
        WorkerNode self = WorkerNode.create(workerHost, workerPort, VERSION);
        NetworkExchangeManager exchangeManager = NetworkExchangeManager.create(
                self.workerId(), self, Set.of(self.workerId()), List.of(),
                NodeMemory.shared().newChildAllocator("exchange/manager", 0), producer);

        // ===== ⑤ WorkerManager：注册 + 心跳 + 执行 Master 派发的任务 =====
        // maxConcurrentTasks=0 → 取配置 system.worker.parallelism；resourceProvider=null → 采集真实机器资源。
        WorkerManager workerManager = new WorkerManager(self, masterGw, 2000, 0, null, exchangeManager);
        producer.setExchangeManager(exchangeManager);
        producer.setWorkerManager(workerManager);

        // ===== ⑤a 查询遥测（非终端片段行数 ~1s 上抛 Master，UI Scan/Join 框实时行数来源）=====
        // 终端 Consumer / 独立 Scan 的行数仍由数据面回传（FragmentData / ScanPage）天然到 Master，
        // 遥测只覆盖 Producer / 中间层片段 → 无双计。注册成功后 start() 开始定时上抛。
        WorkerQueryTelemetry telemetry = new WorkerQueryTelemetry(masterGw, self);
        telemetry.setWorkerManager(workerManager);   // 僵尸片段回收：Master 回信 stale jobId → 就地取消残留片段
        producer.setTelemetry(telemetry);

        // 函数持久化注入 + 启动恢复：与 Master 一致，Worker 侧执行聚合/窗口片段时能重建自定义函数
        FunctionManager.init(metaStore.getStorage());
        FunctionManager.loadPermanentFunctions();

        // ===== ⑤b 外部服务后端（type=python：sidecar 进程池 + BackendRegistry 注册）=====
        // Worker 侧真正执行 collect-evaluate（COLLECT_EVALUATE 调 sidecar），进程池须就绪。
        // 无 python 环境 → 打印 SKIP 提示（Java 函数不受影响；用到 python 函数的查询会明确报错）。
        // sidecar 登记进上面的注册表（uuid/port），shutdown 时由注册表 killAll 统一清理。
        // 返回类型 = ExternalServiceManager：worker 不关心具体语言，shutdown 只调 close()。
        final ExternalServiceManager externalService = PythonBackendBootstrap.install("Worker", registry);

        // ===== ⑥ 启动：注册（Master 未就绪时退避重试）→ 心跳；成功后补齐兄弟拓扑 =====
        System.err.printf(">>> Worker 启动中: %s:%d，连接 Master %s:%d ...%n", workerHost, workerPort, masterHost, masterPort);
        startWithRetry(workerManager);
        exchangeManager.learnNodes(masterGw.clusterNodes());   // 注册响应里的集群节点表 → 兄弟 Worker 拓扑
        telemetry.start();   // 注册成功后才上抛（Master 控制面就绪；空表 tick 不发）
        System.err.printf(">>> Worker 就绪: id=%s host=%s:%d（已学习 %d 个集群节点）%n",
                shortId(self.workerId()), workerHost, workerPort, masterGw.clusterNodes().size());

        // ===== ⑦ 收尾：进程被终止（Ctrl+C / kill）时优雅关闭 =====
        // 顺序：先 kill 全部外部进程（用户明确要求：worker 关闭前必须先检查外部服务，有则杀掉再关机），
        // 再关 python 进程池（sidecar 已死，close 幂等 no-op）、停任务、关 master 网关。
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("\n>>> Worker 正在关闭...");
            try {
                registry.close();   // killAll 外部进程（幂等：无外部进程 no-op）
            } catch (Throwable ignored) {
            }
            if (externalService != null) {
                try {
                    externalService.close();   // 关外部服务进程池（已死进程 no-op）
                } catch (Throwable ignored) {
                }
            }
            try {
                workerManager.stop();
            } catch (Throwable ignored) {
            }
            try {
                telemetry.close();   // 停遥测定时上抛（byQuery 清空；masterGw 关闭前）
            } catch (Throwable ignored) {
            }
            try {
                masterGw.close();
            } catch (Throwable ignored) {
            }
        }));
        Thread.currentThread().join();
    }

    /**
     * 注册重试：Master 尚未就绪时按 2s*attempt 退避重试（WorkerManager.start 失败态不调度心跳，重试干净）。
     *
     * <p>必须捕获 {@link Throwable}：Master 的 Flight 端口尚未监听时，{@code gateway.register} 会直接抛
     * “连接被拒”（而非返回 false）。若让该异常穿透本方法，Worker 会在首次注册失败时立即退出、退避重试形同
     * 虚设（DevClusterMain 拉起 Master 后不等端口就绪即拉起 Worker，正是这个竞态）。捕获后按失败重试，
     * Master 起来后即可注册成功。</p>
     */
    private static void startWithRetry(WorkerManager wm) throws InterruptedException {
        Throwable last = null;
        for (int attempt = 1; attempt <= MAX_REGISTER_ATTEMPTS; attempt++) {
            try {
                wm.start();
                if (wm.state() != WorkerState.FAILED) {
                    return;
                }
            } catch (Throwable t) {
                last = t;   // Master 端口未就绪 / 网络异常 → 视为本次注册失败，退避后重试
            }
            log.warn("注册失败（第 {} 次），{}s 后重试...", attempt, attempt);
            Thread.sleep(2000L * attempt);
        }
        throw new IllegalStateException("注册被 Master 拒绝超过 " + MAX_REGISTER_ATTEMPTS + " 次，Worker 启动失败", last);
    }

    private static java.util.Optional<Integer> firstIntArg(String[] args) {
        if (args.length > 0) {
            try {
                return java.util.Optional.of(Integer.parseInt(args[0]));
            } catch (NumberFormatException ignored) {
            }
        }
        return java.util.Optional.empty();
    }

    private static String shortId(String id) {
        return id.substring(0, Math.min(8, id.length()));
    }
}
