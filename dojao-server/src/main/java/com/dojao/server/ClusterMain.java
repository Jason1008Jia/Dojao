package com.dojao.server;

import com.dojao.core.config.ResourceConfig;
import com.dojao.core.operator.SpillDirs;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;

/**
 * Dojao <b>一键启动入口</b>（部署态）—— 读 {@code conf/dojao.conf}，按 {@code system.role} 拉起
 * 独立 JVM 进程（Master 与 / 或 Worker），把"一堆配置"收敛成"一个文件 + 一条命令"。
 *
 * <p><b>角色</b>（{@code system.role}，{@code -Dsystem.role=...} 可覆盖）：
 * <ul>
 *   <li>{@code master} → 只拉起 Master（含 Web 控制台）；</li>
 *   <li>{@code worker} → 只拉起 Worker；</li>
 *   <li>{@code both}（默认）→ <b>同一台机器</b>拉起 Master + Worker <b>两个 JVM 进程</b>
 *       （各自盖 {@code -Dsystem.role=master} / {@code -Dsystem.role=worker}）。</li>
 * </ul>
 *
 * <p><b>角色前缀</b>：子进程启动时角色是具体的，故配置文件可用 {@code <role>.<key>} 描述异构集群 ——
 * 例如三台机器共用同一份 conf：A 机 {@code master.system.master.host=0.0.0.0}，
 * B/C 机 {@code worker.system.master.host=<A 的 IP>}，各取各的段。</p>
 *
 * <p><b>常用配置键</b>（全部写在 {@code conf/dojao.conf}）：
 * <pre>
 *   system.role=both                          # master | worker | both
 *   system.master.port=6530                   # Master Flight 交互端口
 *   system.worker.port=4009                   # Worker Flight 端口
 *   system.ui.port=2380                       # Web 控制台端口
 *   system.metastore.sqlite.path=data/dojao-metastore.db   # Master/Worker 须同一文件
 *   system.connector.url=jdbc:mysql://localhost:3306/dga   # 扫描连接器（Master/Worker 同源）
 *   system.master.heap=1g  system.master.direct=1g  system.worker.heap=2g   # 子进程 JVM 内存（4C8G 小机预设）
 * </pre></p>
 *
 * <p>进程生命周期：子进程带 {@code -Dsystem.parent.pid} 看门狗 —— 本进程（红方块 / 任务管理器强杀）
 * 一死子进程自杀，不残留占端口；Ctrl+C 由关闭钩子先 destroy 再 destroyForcibly 收掉全部子进程。
 * 起了 Master 时另起看门狗：Master 意外退出则终止整个集群。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/19
 */
public final class ClusterMain {

    public static void main(String[] args) throws Exception {
        // 配置初始化必须第一行：读 conf/dojao.conf → 按角色展平 → 回填系统属性
        ResourceConfig.init();

        ResourceConfig.Role role = ResourceConfig.role();
        // 启动脚本专用干跑模式：只输出「生效角色 + 该角色的 JVM 内存参数」后立即退出。
        // master / worker 由脚本拿到参数后直接 exec DojaoMaster / DojaoWorker（不再常驻本进程）；
        // both 仍需本进程拉起并看护两个子 JVM（opts 留空，由下面的子进程命令自行计算）。
        if (isPrintLaunch(args)) {
            System.out.println("DOJAO_LAUNCH|" + role.prefix() + "|"
                    + (role == ResourceConfig.Role.BOTH ? "" : String.join(" ", memoryOpts(role))));
            System.out.flush();
            return;
        }

        System.err.println("========== Dojao 启动（角色=" + role.prefix() + "） ==========");

        // ===== ① 定位 java 可执行文件 + 当前 classpath（IntelliJ 下含本模块与全部依赖模块输出） =====
        String javaBin = System.getProperty("java.home") + File.separator + "bin" + File.separator
                + (isWindows() ? "java.exe" : "java");
        String classpath = ManagementFactory.getRuntimeMXBean().getClassPath();
        if (!new File(javaBin).canExecute()) {
            throw new IllegalStateException("找不到 java 可执行文件: " + javaBin);
        }

        // ===== ② 公共 JVM 参数 =====
        // Arrow 15 + JDK21 首次分配缓冲需 --add-opens（子进程不继承父 JVM 参数，须显式传）
        List<String> commonOpts = new ArrayList<>(List.of("--add-opens=java.base/java.nio=ALL-UNNAMED"));
        // 父进程看门狗：父进程一死子进程自杀退出
        commonOpts.add("-Dsystem.parent.pid=" + ProcessHandle.current().pid());
        // 配置文件绝对路径：子进程据此读同一份 conf（工作目录不同也不怕）
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

        // ===== ③ 按角色构造子进程命令 =====
        List<Process> processes = new ArrayList<>();
        Process master = null;
        if (role == ResourceConfig.Role.MASTER || role == ResourceConfig.Role.BOTH) {
            master = start(masterCmd(javaBin, classpath, commonOpts));
            processes.add(master);
            System.err.printf(">>> 已拉起 Master  pid=%d  Flight=%d  Web UI=http://localhost:%d  堆=%s 堆外=%s%n",
                    master.pid(), ResourceConfig.masterPort(), ResourceConfig.uiPort(),
                    fmt(ResourceConfig.heapMemoryBytes(ResourceConfig.Role.MASTER)),
                    fmt(ResourceConfig.directMemoryBytes(ResourceConfig.Role.MASTER)));
        }
        if (role == ResourceConfig.Role.WORKER || role == ResourceConfig.Role.BOTH) {
            Process worker = start(workerCmd(javaBin, classpath, commonOpts));
            processes.add(worker);
            System.err.printf(">>> 已拉起 Worker  pid=%d  Flight=%d  堆=%s 堆外=%s%n",
                    worker.pid(), ResourceConfig.workerPort(),
                    fmt(ResourceConfig.heapMemoryBytes(ResourceConfig.Role.WORKER)),
                    fmt(ResourceConfig.directMemoryBytes(ResourceConfig.Role.WORKER)));
        }
        if (processes.isEmpty()) {
            System.err.println(">>> 角色 " + role.prefix() + " 无需拉起任何进程，退出。");
            return;
        }

        // ===== ④ 关闭钩子（Ctrl+C）：先 destroy 再 destroyForcibly 收掉全部子进程 =====
        // 强杀时不执行钩子 → 由子进程父进程看门狗（ParentWatchdog）自杀兜底。
        final List<Process> all = processes;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            shuttingDown = true;
            System.err.println("\n>>> 集群正在关闭，终止 " + all.size() + " 个子进程...");
            for (Process p : all) {
                if (p.isAlive()) {
                    p.destroy();
                }
            }
            try {
                Thread.sleep(3000);
            } catch (InterruptedException ignored) {
            }
            for (Process p : all) {
                if (p.isAlive()) {
                    p.destroyForcibly();
                }
            }
            for (Process p : all) {
                try {
                    p.waitFor();
                } catch (InterruptedException ignored) {
                }
            }
            // 兜底清盘：子进程自己收到 SIGTERM 会跑各自的关闭钩子清溢写目录，但被
            // destroyForcibly（SIGKILL）的那个没机会跑 → 这里按「归属 PID 已死」把残留扫干净。
            SpillDirs.cleanOrphanDirs();
        }));

        // ===== ⑤ Master 崩溃看门狗：Master 意外退出 → 收掉其余进程并退出 =====
        if (master != null) {
            final Process masterProc = master;
            Thread watcher = new Thread(() -> {
                try {
                    masterProc.waitFor();
                } catch (InterruptedException ignored) {
                    return;
                }
                if (shuttingDown) {
                    return;   // 关闭钩子已在收尾
                }
                System.err.println("\n>>> Master 进程退出（code=" + masterProc.exitValue()
                        + "），正在终止整个集群...");
                for (Process p : all) {
                    if (p.isAlive()) {
                        p.destroyForcibly();
                    }
                }
                System.exit(1);
            }, "cluster-master-watch");
            watcher.setDaemon(true);
            watcher.start();
        }

        // ===== ⑥ 主线程常驻 =====
        System.err.println();
        if (role == ResourceConfig.Role.MASTER || role == ResourceConfig.Role.BOTH) {
            System.err.println(">>> 集群常驻中。浏览器打开 http://localhost:" + ResourceConfig.uiPort()
                    + " 查看监控看板 / SQL 控制台 / 执行 DAG。");
        } else {
            System.err.println(">>> Worker 常驻中，等待 Master 派发任务。");
        }
        System.err.println(">>> Ctrl+C 结束（配置文件 " + (confFile != null ? confFile : "(未找到，仅内置默认)")
                + "，改动需重启生效）。");
        Thread.currentThread().join();
    }

    private static volatile boolean shuttingDown = false;

    /**
     * 该角色的 JVM 内存参数（{@code -Xmx} / {@code -XX:MaxDirectMemorySize}）。
     *
     * <p>子进程命令与 {@code --print-launch} 共用同一出处 —— 启动脚本直启 Master/Worker 时
     * 拿到的是同一组值，避免「脚本自己算一套、ClusterMain 再算一套」的口径漂移。</p>
     */
    private static List<String> memoryOpts(ResourceConfig.Role role) {
        return List.of(
                "-Xmx" + ResourceConfig.heapMemoryBytes(role),
                "-XX:MaxDirectMemorySize=" + ResourceConfig.directMemoryBytes(role));
    }

    /** 是否请求干跑模式（{@code --print-launch}）。 */
    private static boolean isPrintLaunch(String[] args) {
        for (String a : args) {
            if ("--print-launch".equals(a)) {
                return true;
            }
        }
        return false;
    }

    /** Master 子进程命令：{@code -Dsystem.role=master} 让角色前缀键 {@code master.*} 生效。 */
    private static List<String> masterCmd(String javaBin, String classpath, List<String> commonOpts) {
        List<String> cmd = new ArrayList<>();
        cmd.add(javaBin);
        cmd.addAll(commonOpts);
        cmd.add("-Dsystem.role=master");
        // 堆/堆外一律走 ResourceConfig 派生：显式键 > 节点总量×heapRatio 派生 > 内置默认。
        // 这样配置文件只需写「节点总量」，无需再手算并写死 -Xmx / MaxDirectMemorySize。
        cmd.addAll(memoryOpts(ResourceConfig.Role.MASTER));
        cmd.add("-cp");
        cmd.add(classpath);
        cmd.add("com.dojao.server.master.DojaoMaster");
        return cmd;
    }

    /** Worker 子进程命令：{@code -Dsystem.role=worker} 让角色前缀键 {@code worker.*} 生效。 */
    private static List<String> workerCmd(String javaBin, String classpath, List<String> commonOpts) {
        List<String> cmd = new ArrayList<>();
        cmd.add(javaBin);
        cmd.addAll(commonOpts);
        cmd.add("-Dsystem.role=worker");
        // ★ Worker 必须和 Master 一样封顶直接内存：Arrow 向量全在堆外，此前只给了 -Xmx，
        //   堆外不受限 ⇒ 小内存机器上被内核 OOM-killer 直接杀进程（表现为「worker 莫名挂了」，
        //   日志里没有 Java 异常）。封顶后超限抛 OOM，可被查询捕获、节点存活。
        // 堆/堆外一律走 ResourceConfig 派生（同 masterCmd）。
        cmd.addAll(memoryOpts(ResourceConfig.Role.WORKER));
        cmd.add("-cp");
        cmd.add(classpath);
        cmd.add("com.dojao.server.worker.DojaoWorker");
        return cmd;
    }

    /** 人类可读内存格式（仅用于启动日志，不影响 -Xmx / MaxDirectMemorySize 的字节值）。 */
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
