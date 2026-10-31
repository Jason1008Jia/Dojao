package com.dojao.server.master;

import com.dojao.core.config.MasterConfig;
import com.dojao.core.config.ResourceConfig;
import com.dojao.core.operator.SpillDirs;
import com.dojao.core.runtime.master.WorkerRegistry;
import com.dojao.metastore.MetaStore;
import com.dojao.metastore.MetaStoreType;
import com.dojao.server.external.ExternalServiceManager;
import com.dojao.server.util.ExternalProcessRegistry;
import com.dojao.server.util.ParentWatchdog;
import com.dojao.server.util.PythonBackendBootstrap;
import com.dojao.spi.service.ConnectorFactory;
import com.dojao.spi.service.ConnectorRegistry;

import java.util.Map;

/**
 * Dojao <b>Master 独立进程</b>（真分布式）：一个 JVM 一个 Master，对外提供 SQL 查询服务，
 * 经 {@link NetworkWorkerGateway} 把扫描 / join 片段派发到远程 Worker，结果经 doExchange 回传。
 *
 * <p>装配（与伪分布式 {@code PseudoDistributedClusterTest} 对称，只把内存 gateway 换成网络版）：
 * <ol>
 *   <li>{@link ResultRouter} + {@link WorkerRegistry} 两个单例实例，同时注入
 *       {@link NetworkWorkerGateway}（派发端注册 taskId→结果汇）与 {@link MasterControlFlightProducer}
 *       （接收端查表路由 Worker 回传），经 {@code MasterServer.setResultRouter} 在 start 前共享；</li>
 *   <li>{@link NetworkWorkerGateway} 作为 {@code WorkerGateway} 注入 Planner（start 前），开启
 *       {@code setDistributedExecution(true)} 把片段整条派发到 Worker 执行（Master 只派发 + 收集）；</li>
 *   <li>不注入 → 单机行为不变（本地并行扫描）。</li>
 * </ol>
 *
 * <p><b>启动参数</b>（均可用 -D 覆盖）：
 * <pre>
 *   java -cp ... com.dojao.server.master.DojaoMaster
 *   -Dsystem.master.host=0.0.0.0   -Dsystem.master.port=6530   # Flight 监听（默认）
 *   -Dsystem.connector.url=jdbc:mysql://localhost:3306/dga      # 扫描连接器（元数据 + 兜底本地扫描）
 *   -Dsystem.connector.user=root  -Dsystem.connector.password=123456
 *   -Dsystem.connector.catalog=mysql
 *   -Dsystem.metastore.sqlite.path=data/dojao-metastore.db      # 元数据 db 文件（与 Worker 须同一文件）
 *   -Dsystem.ui.port=2380                                        # Web 控制台端口
 * </pre></p>
 *
 * <p>推荐经 {@code bin/dojao} 一键启动（读 {@code conf/dojao.conf} 的 {@code system.role}）：
 * {@code master} 角色直接 exec 本类（无常驻启动器），{@code both} 由 {@code ClusterMain} 拉起。
 * 手工入口同样从 {@code conf/dojao.conf} + {@code -D} 读取配置。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/29
 */
public final class DojaoMaster {

    public static void main(String[] args) throws Exception {
        // 配置初始化必须第一行：读 conf/dojao.conf → 按角色展平 → 回填系统属性，
        // 让 dojao-metastore / dojao-ui 等看不到 ResourceConfig 的模块也吃上配置值。
        ResourceConfig.init();

        // 启动清残留：把上次 kill -9 / 崩溃遗留的孤儿溢写目录删掉（关闭钩子没机会跑的场合）。
        // 只认溢写实例子目录命名且归属 PID 已死，别的文件/目录一律不碰。
        SpillDirs.cleanOrphanDirs();

        // 父进程看门狗：由 ClusterMain / PseudoDistributedClusterTest 经 -Dsystem.parent.pid 传入父进程 PID，
        // 父进程（红方块 / 任务管理器强杀）一死本进程立即退出，避免孤儿进程占住 6530 端口。
        // 独立启动 DojaoMaster（无该属性）时 no-op。
        ParentWatchdog.install();

        String host = ResourceConfig.masterHost();
        int port = ResourceConfig.masterPort();

        // ===== ① 元数据 + 连接器（Master 侧解析表结构 / 兜底本地扫描；与 Worker 同源配置） =====
        // SQLITE：catalog 注册信息 + 持久化函数落在本地 db 文件（默认 ./data/dojao-metastore.db，
        // -Dsystem.metastore.sqlite.path 覆盖），重启后仍在；Worker 打开同一文件（WAL，两进程启动期各写一次）。
        MetaStore metaStore = new MetaStore(MetaStoreType.SQLITE);
        String catalog = ResourceConfig.connectorCatalog();
        String type = ResourceConfig.connectorType();
        String url = ResourceConfig.connectorUrl();
        String user = ResourceConfig.connectorUser();
        String pass = ResourceConfig.connectorPassword();
        // 连接属性必须随注册一起落库：只注册连接器实例（不传 properties）会持久化成 '{}'，
        // 于是任何进程下次 new MetaStore(SQLITE) 时按工厂重建连接器都缺 jdbcUrl →
        // 刷一行 "Failed to reload catalog: <catalog>"。属性落库后，其他进程（以及运行时新建的
        // 其它 catalog）都能只凭共享元数据库把连接器现场装起来。
        Map<String, String> connProps = Map.of(
                "jdbcUrl", url,
                "username", user,
                "password", pass);
        // ★连接器已插件化：此处不再 import 任何具体连接器类，改为按类型向 ConnectorFactory 要实例。
        // 类型缺省 = catalog 名（见 ResourceConfig.connectorType()），故旧 -Dsystem.connector.catalog=mysql 不变。
        ConnectorFactory factory = MetaStore.factoryFor(type).orElseThrow(() ->
                new IllegalStateException("未安装连接器 '" + type + "'：把连接器插件 jar 放进 "
                        + ConnectorRegistry.pluginDirDescription()
                        + "，或 -Dsystem.connector.plugins.dir=<目录>；"
                        + "当前可用类型: " + String.join(", ", MetaStore.registeredTypes())));
        metaStore.registerConnector(catalog, factory.create(catalog, connProps), connProps);

        // ===== ② 结果路由表 + 集群注册表：派发端与接收端共享同一实例 =====
        // NetworkWorkerGateway（派发时注册 taskId→结果汇）与 MasterControlFlightProducer
        // （doExchange 查表路由 Worker 回传）必须看到同一张路由表，经 setResultRouter 注入。
        ResultRouter router = new ResultRouter();
        WorkerRegistry registry = WorkerRegistry.get();
        NetworkWorkerGateway workerGw = new NetworkWorkerGateway(router, registry, metaStore);

        // ===== ②b Python 后端（type=python：sidecar 进程池 + BackendRegistry 注册）=====
        // 无 python 环境 → 打印 SKIP 提示，集群照常服务（Java 函数不受影响）。
        // Master 侧只用于 CREATE FUNCTION 校验 + 查询期 ensureLoaded（规划），
        // 真正执行在 Worker（DojaoWorker 同样装后端，见 DojaoWorker）。
        // 外部进程注册表：每个 sidecar 登记（uuid/port），shutdown 时 killAll 优先清掉再退出。
        ExternalProcessRegistry extRegistry = new ExternalProcessRegistry();
        // 返回类型 = ExternalServiceManager：Master 不关心具体语言，shutdown 只调 close()。
        final ExternalServiceManager externalService = PythonBackendBootstrap.install("Master", extRegistry);

        // ===== ③ Master 服务器：注入网络派发网关 + 开启分布式执行 =====
        MasterConfig config = new MasterConfig(host, port);
        MasterServer master = new MasterServer(config, metaStore);
        master.setWorkerGateway(workerGw);
        master.setResultRouter(router);
        master.setDistributedExecution(true);
        // Web 控制台（监控 + SQL 页）：MasterServer.start() 会为它 bind UiQueryMonitor。
        // 默认 2380：避开工 4007-4009 与 master Flight 6530（多进程一键集群 DevClusterMain 用）。
        master.addModule(new com.dojao.ui.WebUiModule(ResourceConfig.uiPort()));
        master.start();
        System.err.printf(">>> Master 就绪: %s:%d（分布式执行已开启，等待 Worker 注册并提交 SQL）%n", host, port);

        // ===== ④ 收尾：进程被终止（Ctrl+C / kill）时优雅关闭 =====
        // 顺序：先 kill 全部外部进程，再关 python 进程池（sidecar 已死，close 幂等 no-op）、停服务。
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("\n>>> Master 正在关闭...");
            try {
                extRegistry.close();   // killAll 外部进程（幂等：无外部进程 no-op）
            } catch (Throwable ignored) {
            }
            if (externalService != null) {
                try {
                    externalService.close();   // 关外部服务进程池（已死进程 no-op）
                } catch (Throwable ignored) {
                }
            }
            try {
                master.shutdown();
            } catch (Throwable ignored) {
            }
            try {
                workerGw.close();
            } catch (Throwable ignored) {
            }
        }));
        Thread.currentThread().join();
    }
}
