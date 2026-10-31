package com.dojao.server.util;

/**
 * 父进程看门狗：让独立 JVM 的子进程在父进程退出后自杀，防止 IntelliJ 红方块 / 任务管理器
 * 强杀父进程（关闭钩子不执行）后留下孤儿进程占着端口。
 *
 * <p>由启动方通过 {@code -Dsystem.parent.pid=<pid>} 传入父进程 PID；属性缺失时 no-op
 * （独立启动 {@code DojaoMaster} / {@code DojaoWorker} 不受影响）。父进程一死，本进程经
 * {@link Runtime#halt(int)} 立即退出 —— 端口由操作系统释放，集群无残留。</p>
 *
 * <p>为什么不用 {@link System#exit(int)}：exit 会先跑关闭钩子（master.shutdown / workerManager.stop），
 * 若钩子被网络调用卡住，子进程反而迟迟不退；看门狗的职责是"父进程没了就尽快让出资源"，
 * 用 halt 直达内核强制终止最干脆。</p>
 */
public final class ParentWatchdog {

    /** 轮询间隔：父进程死后最多这么长时间子进程感知到并退出。 */
    private static final long POLL_INTERVAL_MS = 1000;

    private ParentWatchdog() {
    }

    /**
     * 安装看门狗；无 {@code -Dsystem.parent.pid} 属性时直接返回（no-op）。
     * 应在 main() 最开头调用，保证任何启动阶段父进程死亡都能被感知。
     */
    public static void install() {
        String pidStr = System.getProperty("system.parent.pid");
        if (pidStr == null || pidStr.isBlank()) {
            return;
        }
        long parentPid;
        try {
            parentPid = Long.parseLong(pidStr);
        } catch (NumberFormatException e) {
            return;
        }
        Thread watcher = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(POLL_INTERVAL_MS);
                } catch (InterruptedException e) {
                    return;
                }
                // ProcessHandle.of 为空（OS 已回收 PID）同样视为父进程已死
                boolean parentAlive = ProcessHandle.of(parentPid)
                        .map(ProcessHandle::isAlive)
                        .orElse(false);
                if (!parentAlive) {
                    System.err.println(">>> 父进程（pid=" + parentPid + "）已退出，本进程随之关闭...");
                    Runtime.getRuntime().halt(0);
                    return;
                }
            }
        }, "parent-watchdog");
        watcher.setDaemon(true);
        watcher.start();
    }
}
