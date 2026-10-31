package com.dojao.server;

import com.dojao.core.config.ResourceConfig;
import com.dojao.core.operator.SpillDirs;
import com.dojao.spi.service.ConnectorRegistry;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Dojao <b>一键集群</b>（统一启动入口）—— 用 {@link ProcessBuilder} 拉起 <b>独立 JVM 进程</b>：
 * Master 1 个（自带 Web 控制台）+ Worker N 个。
 *
 * <p>等于把三步手动操作合并成一条命令：
 * <ol>
 *   <li><b>Master 进程</b>：{@code com.dojao.server.master.DojaoMaster} —— 分布式执行 + 查询监控；
 *       它内部已挂 {@code WebUiModule}（默认 <code>system.ui.port=2380</code>，MasterServer.start 会
 *       bind UiQueryMonitor），浏览器访问 <b><a href="http://localhost:2380">...</a></b> 即可看到监控看板 + SQL 控制台；</li>
 *   <li><b>Worker 进程 ×N</b>：{@code com.dojao.server.worker.DojaoWorker} —— 注册 + 心跳 +
 *       Worker↔Worker Flight 交换；</li>
 *   <li>跨 JVM 全部走 Arrow Flight 网络通信（真分布式，与正式部署一致）。</li>
 * </ol>
 *
 * <p>用法（IntelliJ 直接 Run 本类的 main；也可 java -cp 全 classpath 运行）：
 * <pre>
 *   DevClusterMain              # 默认：Master + 3 Worker（4007/4008/4009），UI 2380
 *   DevClusterMain 5            # 第一个位置参数 = Worker 数量
 *
 *   # 常用 -D 覆盖（默认值见下；也可直接写进 conf/dojao.conf）：
 *   -Dsystem.ui.port=2380        # Master 的 Web 控制台端口
 *   -Dsystem.devcluster.workers=3        # Worker 数量（等价于位置参数）
 *   -Dsystem.devcluster.worker.base.port=4007   # Worker 起始端口（顺延 N 个）
 *   -Dsystem.master.memory=2g -Dsystem.worker.memory=2g   # 节点内存总量（堆/堆外按 heapRatio 派生，推荐只写这个）
 *   -Dsystem.master.heap=1g -Dsystem.master.direct=1g -Dsystem.worker.heap=2g   # 需精调时逐个覆盖（优先级更高）
 *   -Dsystem.connector.url=jdbc:mysql://localhost:3306/dga   # 扫描连接器（Master/Worker 同源）
 *   -Dsystem.connector.user=root  -Dsystem.connector.password=123456  -Dsystem.connector.catalog=mysql
 *   -Dsystem.metastore.sqlite.path=/abs/path/dojao-metastore.db  # 元数据 db 文件（默认 ./data/dojao-metastore.db，
 *                                                        # Master 与 Worker 必须指向同一文件；指定时透传给子进程）
 * </pre></p>
 *
 * <p>与 {@link ClusterMain} 的分工：本类是<b>开发态</b>（固定 Master + N Worker 三端口），
 * {@code ClusterMain} 是<b>部署态</b>（严格按 {@code conf/dojao.conf} 的 {@code system.role} 拉起进程）。</p>
 *
 * <p>进程生命周期：所有子进程带 {@code -Dsystem.parent.pid} 父进程看门狗 —— 本进程（红方块 /
 * 任务管理器）被强杀时子进程自杀退出，绝不残留占端口。Ctrl+C 正常退出则由关闭钩子先 destroy
 * 再 destroyForcibly 收掉全部子进程。Master 进程若意外退出，本入口也会终止其余 Worker 并退出。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/6
 */
public final class DevClusterMain {

    /** 开发态 Worker 起始端口：4007/4008/4009（与 Worker 默认 4009 各管各的）。 */
    private static final int DEF_WORKER_BASE_PORT = 4007;

    public static void main(String[] args) throws Exception {
        // 配置初始化必须第一行：读 conf/dojao.conf → 回填系统属性，下面的 System.getProperty 才吃得上配置。
        ResourceConfig.init();
        System.err.println("========== Dojao 一键集群拉起（Master + Web UI + N Worker 独立 JVM） ==========");

        // ===== ① 定位 java 可执行文件 + 当前 classpath（IntelliJ 下含本模块与全部依赖模块输出） =====
        String javaBin = System.getProperty("java.home") + File.separator + "bin" + File.separator
                + (isWindows() ? "java.exe" : "java");
        String classpath = ManagementFactory.getRuntimeMXBean().getClassPath();
        System.err.println("java      : " + javaBin);
        System.err.println("classpath : " + classpath.length() + " 字符");
        if (!new File(javaBin).canExecute()) {
            throw new IllegalStateException("找不到 java 可执行文件: " + javaBin);
        }

        // ===== ①b 预检：连接器 =====
        // 父进程 classpath 原样透传给子进程（见 ③ 的 -cp），故此处判定 == 子进程将会看到的结果。
        // 一个连接器都没有 ⇒ Master 注册默认 catalog 时必抛「未安装连接器 '<type>'」而退出，
        // 看门狗随即收掉整个集群（症状：控制台刷一屏栈 + exit code 1）。在这里提前拦下。
        if (!connectorPreflightOk()) {
            System.exit(1);
        }

        // ===== ② 解析配置 =====
        int workerCount = workerCount(args);
        int uiPort = ResourceConfig.uiPort();
        int masterPort = ResourceConfig.masterPort();
        int workerBase = Integer.getInteger("system.devcluster.worker.base.port", DEF_WORKER_BASE_PORT);
        for (int i = 0; i < workerCount; i++) {
            int p = workerBase + i;
            if (p == masterPort || p == uiPort) {
                System.err.println("警告: worker 端口 " + p + " 与 master(" + masterPort + ")/UI(" + uiPort
                        + ") 冲突，请用 -Dsystem.devcluster.worker.base.port 调开");
            }
        }

        // ===== ③ 构造 Master + Worker 命令 =====
        // 公共 JVM 参数：Arrow 15 + JDK21 首次分配缓冲需 --add-opens（子进程不继承父 JVM 参数，须显式传）
        List<String> commonOpts = new ArrayList<>(List.of("--add-opens=java.base/java.nio=ALL-UNNAMED"));
        // 父进程看门狗：子进程经 -Dsystem.parent.pid 知道父 PID，父进程一死子进程自杀退出，不残留占端口
        commonOpts.add("-Dsystem.parent.pid=" + ProcessHandle.current().pid());
        // 配置文件：显式透传绝对路径，子进程即便工作目录不同也读同一份 conf
        String confFile = ResourceConfig.confFile();
        if (confFile != null) {
            commonOpts.add("-Ddojao.conf=" + confFile);
        }
        // 安装根目录：子进程据此把相对路径类配置（如 query.spill.dir 默认值 <HOME>/spill）解析到同一处
        commonOpts.add("-Ddojao.home=" + ResourceConfig.dojaoHome());
        // stdout/stderr 强制 UTF-8：避免中文 System.err 按 GBK 写控制台再被按 UTF-8 解码乱码
        commonOpts.add("-Dfile.encoding=UTF-8");
        commonOpts.add("-Dstdout.encoding=UTF-8");
        commonOpts.add("-Dstderr.encoding=UTF-8");
        List<String> connectorProps = new ArrayList<>(List.of(
                "-Dsystem.connector.catalog=" + prop("system.connector.catalog", "mysql"),
                "-Dsystem.connector.url=" + prop("system.connector.url", "jdbc:mysql://localhost:3306/dga"),
                "-Dsystem.connector.user=" + prop("system.connector.user", "root"),
                "-Dsystem.connector.password=" + prop("system.connector.password", "123456")));
        // 元数据 SQLite db：显式指定路径时透传（子进程不继承父 JVM 属性）。
        // 不指定则各子进程按自身工作目录解析 ./data/dojao-metastore.db —— 本入口未设置 working directory，
        // 继承父进程 CWD，故 Master/Worker 天然指向同一文件。
        String sqlitePath = System.getProperty("system.metastore.sqlite.path");
        if (sqlitePath != null && !sqlitePath.isBlank()) {
            connectorProps.add("-Dsystem.metastore.sqlite.path=" + sqlitePath);
        }

        // Master：主控 + Web UI（角色显式盖成 master，角色前缀键 master.* 由它生效）
        List<String> masterCmd = new ArrayList<>();
        masterCmd.add(javaBin);
        masterCmd.addAll(commonOpts);
        masterCmd.add("-Dsystem.role=master");
        // 堆/堆外一律走 ResourceConfig 派生：显式键 > 节点总量×heapRatio 派生 > 内置默认。
        masterCmd.add("-Xmx" + ResourceConfig.heapMemoryBytes(ResourceConfig.Role.MASTER));
        masterCmd.add("-XX:MaxDirectMemorySize=" + ResourceConfig.directMemoryBytes(ResourceConfig.Role.MASTER));
        masterCmd.add("-Dsystem.master.host=" + prop("system.master.host", "0.0.0.0"));
        masterCmd.add("-Dsystem.master.port=" + masterPort);
        masterCmd.add("-Dsystem.ui.port=" + uiPort);
        masterCmd.addAll(connectorProps);
        masterCmd.add("-cp");
        masterCmd.add(classpath);
        masterCmd.add("com.dojao.server.master.DojaoMaster");

        // Worker ×N（角色显式盖成 worker，角色前缀键 worker.* 由它们生效）
        // ★ Worker 也须封顶直接内存：Arrow 向量全在堆外，此前只给 -Xmx ⇒ 堆外不受限，
        //   小内存机器上会被内核 OOM-killer 杀掉整个 worker（日志无 Java 异常）。堆/堆外均走派生。
        long workerHeapBytes = ResourceConfig.heapMemoryBytes(ResourceConfig.Role.WORKER);
        long workerDirectBytes = ResourceConfig.directMemoryBytes(ResourceConfig.Role.WORKER);
        String workerHost = prop("system.worker.host", "127.0.0.1");
        // Worker 连的 Master 地址：同机开发固定回环（Master 绑的是 0.0.0.0，连它无意义）。
        // 单独用 system.devcluster.master.host 覆盖，避免被 conf 里面向生产的 system.master.host 带偏。
        String workerMasterHost = prop("system.devcluster.master.host", "127.0.0.1");
        List<List<String>> workerCmds = new ArrayList<>();
        for (int i = 0; i < workerCount; i++) {
            List<String> cmd = new ArrayList<>();
            cmd.add(javaBin);
            cmd.addAll(commonOpts);
            cmd.add("-Dsystem.role=worker");
            cmd.add("-Xmx" + workerHeapBytes);
            cmd.add("-XX:MaxDirectMemorySize=" + workerDirectBytes);
            cmd.add("-Dsystem.worker.host=" + workerHost);
            cmd.add("-Dsystem.worker.port=" + (workerBase + i));
            cmd.add("-Dsystem.master.host=" + workerMasterHost);
            cmd.add("-Dsystem.master.port=" + masterPort);
            cmd.addAll(connectorProps);
            cmd.add("-cp");
            cmd.add(classpath);
            cmd.add("com.dojao.server.worker.DojaoWorker");
            cmd.add(String.valueOf(workerBase + i));   // DojaoWorker 第一个位置参数 = worker 端口
            workerCmds.add(cmd);
        }

        // ===== ④ 拉起进程（inheritIO：子进程 stdout/stderr 直接打到本控制台） =====
        List<Process> processes = new ArrayList<>();
        Process master = start(masterCmd);
        processes.add(master);
        System.err.println(">>> 已拉起 Master   pid=" + master.pid()
                + "  Flight=" + masterPort + "  Web UI=http://localhost:" + uiPort
                + "  堆=" + fmt(ResourceConfig.heapMemoryBytes(ResourceConfig.Role.MASTER))
                + "  直接内存=" + fmt(ResourceConfig.directMemoryBytes(ResourceConfig.Role.MASTER)));
        for (int i = 0; i < workerCmds.size(); i++) {
            Process p = start(workerCmds.get(i));
            processes.add(p);
            System.err.println(">>> 已拉起 Worker-" + i + "  pid=" + p.pid()
                    + "  Flight=" + (workerBase + i) + "  堆=" + fmt(workerHeapBytes)
                    + "  直接内存=" + fmt(workerDirectBytes));
        }

        // ===== ⑤ 关闭钩子（Ctrl+C）：先 destroy 再 destroyForcibly 收掉全部子进程 =====
        // 红方块 / 任务管理器强杀时不执行钩子 → 由子进程父进程看门狗（ParentWatchdog）自杀兜底。
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            shuttingDown = true;
            System.err.println("\n>>> 集群正在关闭，终止 " + processes.size() + " 个子进程...");
            for (Process p : processes) {
                if (p.isAlive()) {
                    p.destroy();
                }
            }
            try {
                Thread.sleep(3000);
            } catch (InterruptedException ignored) {
            }
            for (Process p : processes) {
                if (p.isAlive()) {
                    p.destroyForcibly();
                }
            }
            for (Process p : processes) {
                try {
                    p.waitFor();
                } catch (InterruptedException ignored) {
                }
            }
            // 兜底清盘：被 destroyForcibly（SIGKILL）的子进程没机会跑自己的关闭钩子 → 按「归属 PID 已死」扫残留
            SpillDirs.cleanOrphanDirs();
        }));

        // ===== ⑥ Master 崩溃看门狗：Master 进程意外退出 → 收掉其余 Worker 并退出 =====
        Thread watcher = new Thread(() -> {
            try {
                master.waitFor();
            } catch (InterruptedException ignored) {
                return;
            }
            if (shuttingDown) {
                return;   // 关闭钩子已在收尾
            }
            System.err.println("\n>>> Master 进程退出（code=" + master.exitValue() + "），正在终止整个集群...");
            for (Process p : processes) {
                if (p.isAlive()) {
                    p.destroyForcibly();
                }
            }
            System.exit(1);
        }, "devcluster-master-watch");
        watcher.setDaemon(true);
        watcher.start();

        // ===== ⑦ 主线程常驻 =====
        System.err.println();
        System.err.println(">>> 集群常驻中。浏览器打开 http://localhost:" + uiPort
                + " 查看监控看板 / SQL 控制台 / 执行 DAG。");
        System.err.println(">>> 提交 SQL 请用控制台 SQL 页或 Client.main()（Ctrl+C 结束整个集群）...");
        Thread.currentThread().join();
    }

    private static volatile boolean shuttingDown = false;

    /**
     * 启动前预检：本进程（= 子进程）的 classpath / 插件目录上到底有没有连接器。
     *
     * <p>{@link DevClusterMain} 把父 JVM 的 classpath 原样透传给子进程，所以父进程这里
     * {@link ConnectorRegistry} 扫出来的结果，就是 Master / Worker 子进程会得到的结果。
     * 一个工厂都找不到时，Master 会因注册默认 catalog 抛 {@code 未安装连接器 '<type>'} 而退出、
     * 看门狗再收掉整个集群（症状是刷一屏栈 + exit code 1）。此处提前拦截，
     * 把「连接器没进 classpath」用一句话讲清楚，而不是让子进程抛栈。</p>
     *
     * @return true = 至少发现一个连接器（继续拉起集群）；false = 一个都没有（已打印修复指引）
     */
    private static boolean connectorPreflightOk() {
        ConnectorRegistry registry = ConnectorRegistry.load();
        try {
            List<String> types = registry.types();
            if (!types.isEmpty()) {
                String dir = registry.pluginDir() == null ? "(插件目录未配置)" : registry.pluginDir().toString();
                System.err.println("连接器 : 已发现 " + types + "  插件目录=" + dir);
                // 有失败记录（同名冲突 / apiVersion 不符 / jar 损坏）也吵出来，但不阻断启动。
                logConnectorFailures(registry.failures(), "");
                return true;
            }

            System.err.println();
            System.err.println(">>> 启动中止：当前 classpath 与插件目录上没有任何连接器（ConnectorFactory）。");
            System.err.println("    子进程的 classpath 与本进程完全一致（DevClusterMain 原样透传），所以这里发现不了");
            System.err.println("    = Master / Worker 子进程也发现不了 —— Master 注册默认 catalog 时必然抛");
            System.err.println("    「未安装连接器」，整个集群随即被看门狗收掉。这里提前拦下，省掉那一屏栈。");
            System.err.println("    怎么修（二选一）：");
            System.err.println("      1) IDE 直跑（开发态）：Maven 工具窗 → Reload All Maven Projects，让 dojao-server 的");
            System.err.println("         test 作用域依赖（dojao-connector-mysql / postgres / hive 及其传递依赖）进入运行 classpath；");
            System.err.println("      2) 与 IDE 无关：把连接器插件 jar 放进 " + ConnectorRegistry.pluginDirDescription() + "，");
            System.err.println("         生产分发包由 dojao-dist 装配自动生成该目录。");
            logConnectorFailures(registry.failures(), "    ");
            return false;
        } finally {
            registry.close();
        }
    }

    /** 打印连接器加载失败记录（空则不打印）。 */
    private static void logConnectorFailures(Map<String, String> failures, String indent) {
        for (Map.Entry<String, String> e : failures.entrySet()) {
            System.err.println(indent + "连接器加载失败 [" + e.getKey() + "]: " + e.getValue());
        }
    }

    /** Worker 数量：位置参数 &gt; {@code system.devcluster.workers} &gt; 默认 3。 */
    private static int workerCount(String[] args) {
        if (args.length > 0) {
            try {
                return Math.max(1, Math.min(9, Integer.parseInt(args[0])));
            } catch (NumberFormatException ignored) {
                // 非数字 → 回落 -D / 默认
            }
        }
        return Math.max(1, Math.min(9, Integer.getInteger("system.devcluster.workers", 3)));
    }

    private static String prop(String key, String def) {
        return System.getProperty(key, def);
    }

    /** 人类可读内存格式（仅用于启动日志）。 */
    private static String fmt(long bytes) {
        if (bytes <= 0) {
            return "0";
        }
        if (bytes >= 1024L * 1024 * 1024) {
            return String.format("%.2fGB", bytes / (1024.0 * 1024 * 1024));
        }
        return String.format("%.0fMB", bytes / (1024.0 * 1024));
    }

    private static Process start(List<String> cmd) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(false);
        pb.inheritIO();
        return pb.start();
    }

    private static boolean isWindows() {
        return System.getProperty("os.name", "").toLowerCase().contains("win");
    }
}
