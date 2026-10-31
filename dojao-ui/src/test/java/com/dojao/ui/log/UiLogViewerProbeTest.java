package com.dojao.ui.log;

import com.dojao.ui.monitor.MonitorCluster;
import com.dojao.ui.monitor.MonitorWorker;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.List;

/**
 * 探针：{@link LogViewer} —— 本地 tail 边界 + 节点解析 + 远端命令转义（离线，不连集群）。
 *
 * <p><b>判别力</b>：把 {@code tailLocal} 的 {@code start > 0} 残行处理去掉，8MB 窗口用例 FAIL；
 * 把节点解析退化成「请求参数直接当主机名」，未知节点/歧义前缀用例 FAIL；
 * 把 {@link LogViewer#shQuote} 换成裸拼接，引号/换行拒绝用例 FAIL。</p>
 */
public final class UiLogViewerProbeTest {

    private static int fail = 0;

    public static void main(String[] args) throws Exception {
        localTail();
        localTailByteWindow();
        nodeResolve();
        quoting();
        pathOverride();
        System.out.println(fail == 0 ? "[SUMMARY] PASS=all FAIL=0" : "[SUMMARY] FAIL=" + fail);
        System.exit(fail == 0 ? 0 : 1);
    }

    // ---------------------------------------------------------------- 本地 tail

    private static void localTail() throws Exception {
        Path f = Files.createTempFile("dojao-log-", ".out");
        try {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 10; i++) {
                sb.append("line-").append(i).append('\n');
            }
            Files.writeString(f, sb.toString(), StandardCharsets.UTF_8);
            List<String> last3 = LogViewer.tailLocal(f, 3);
            check("tail 3 行取尾部", last3.equals(List.of("line-7", "line-8", "line-9")));
            check("tail 超过总行数返回全部", LogViewer.tailLocal(f, 100).size() == 10);
            check("tail 1 行", LogViewer.tailLocal(f, 1).equals(List.of("line-9")));

            // 无结尾换行：最后一行不能丢
            Files.writeString(f, "a\nb\nc", StandardCharsets.UTF_8);
            check("无结尾换行保留最后一行", LogViewer.tailLocal(f, 2).equals(List.of("b", "c")));

            // CRLF
            Files.writeString(f, "x\r\ny\r\nz\r\n", StandardCharsets.UTF_8);
            check("CRLF 去掉 \\r", LogViewer.tailLocal(f, 2).equals(List.of("y", "z")));

            // UTF-8 中文
            Files.writeString(f, "第一行\n第二行\n第三行\n", StandardCharsets.UTF_8);
            check("UTF-8 中文", LogViewer.tailLocal(f, 2).equals(List.of("第二行", "第三行")));

            // 空文件
            Files.writeString(f, "", StandardCharsets.UTF_8);
            check("空文件返回空列表", LogViewer.tailLocal(f, 10).isEmpty());

            // 文件不存在
            Path missing = f.resolveSibling("dojao-log-missing.out");
            boolean threw = false;
            try {
                LogViewer.tailLocal(missing, 10);
            } catch (NoSuchFileException e) {
                threw = true;
            }
            check("文件不存在抛 NoSuchFileException", threw);

            boolean bad = false;
            try {
                LogViewer.tailLocal(f, 0);
            } catch (IllegalArgumentException e) {
                bad = true;
            }
            check("maxLines=0 拒绝", bad);
        } finally {
            Files.deleteIfExists(f);
        }
    }

    /** 超过 8MB 窗口时从字节中间开始：残行必须丢弃，尾部行必须完整。 */
    private static void localTailByteWindow() throws Exception {
        Path f = Files.createTempFile("dojao-log-big-", ".out");
        try {
            String pad = "x".repeat(1000);
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 9000; i++) {          // ≈ 9MB
                sb.append("big-").append(i).append('-').append(pad).append('\n');
            }
            Files.writeString(f, sb.toString(), StandardCharsets.UTF_8);
            List<String> tail = LogViewer.tailLocal(f, 3);
            check("大文件只取最后 3 行", tail.size() == 3);
            check("大文件首行不残缺", tail.get(0).startsWith("big-8997-"));
            check("大文件末行完整", tail.get(2).startsWith("big-8999-") && tail.get(2).length() == 1009);
        } finally {
            Files.deleteIfExists(f);
        }
    }

    // ---------------------------------------------------------------- 节点解析

    private static void nodeResolve() {
        MonitorWorker master = worker("master", "aliyun003");
        MonitorCluster c = new MonitorCluster(master, List.of(
                worker("e818b66e-5c47-4eab-ae56-2516efadec7d", "aliyun002"),
                worker("67ddb713-d438-4dd3-87c9-f1d021cfe75e", "aliyun003"),
                worker("76a96d21-941b-431b-a7de-8e34f887ff0a", "aliyun001")),
                List.of(), List.of());

        LogViewer.Target def = LogViewer.resolve(c, null);
        check("默认 = master", def != null && "master".equals(def.node()) && def.local());

        LogViewer.Target m1 = LogViewer.resolve(c, "master");
        check("master → 本机", m1 != null && m1.local() && "aliyun003".equals(m1.host()));

        LogViewer.Target m2 = LogViewer.resolve(c, "local");
        check("local → 本机", m2 != null && m2.local());

        LogViewer.Target m3 = LogViewer.resolve(c, "aliyun003");
        check("Master 主机名 → 本机", m3 != null && m3.local());

        LogViewer.Target w1 = LogViewer.resolve(c, "e818b66e-5c47-4eab-ae56-2516efadec7d");
        check("完整 workerId → 远端", w1 != null && !w1.local()
                && "e818b66e-5c47-4eab-ae56-2516efadec7d".equals(w1.node())
                && "aliyun002".equals(w1.host()));

        LogViewer.Target w2 = LogViewer.resolve(c, "67dd");
        check("workerId 唯一前缀 → 同机 Worker 读本机文件",
                w2 != null && w2.local() && "aliyun003".equals(w2.host())
                        && w2.node().startsWith("67dd"));

        LogViewer.Target w3 = LogViewer.resolve(c, "aliyun001");
        check("Worker 主机名 → 远端", w3 != null && !w3.local() && "aliyun001".equals(w3.host()));

        check("未知节点 → null", LogViewer.resolve(c, "nope") == null);

        MonitorCluster ambiguous = new MonitorCluster(master, List.of(
                worker("abc1", "h1"), worker("abc2", "h2")), List.of(), List.of());
        check("workerId 前缀有歧义 → null", LogViewer.resolve(ambiguous, "abc") == null);

        LogViewer.Target noMaster = LogViewer.resolve(
                new MonitorCluster(null, List.of(worker("w1", "h1")), List.of(), List.of()), "master");
        check("快照缺 Master 仍能解析 master 为本机", noMaster != null && noMaster.local());
        check("cluster=null → null", LogViewer.resolve(null, "master") == null);
    }

    // ---------------------------------------------------------------- 远端命令转义

    private static void quoting() {
        check("普通路径加单引号", "'/opt/module/dojao/logs/dojao.out'"
                .equals(LogViewer.shQuote("/opt/module/dojao/logs/dojao.out")));
        check("带空格路径安全", "'/tmp/my logs/dojao.out'"
                .equals(LogViewer.shQuote("/tmp/my logs/dojao.out")));

        boolean quote = false;
        try {
            LogViewer.shQuote("/tmp/a'b");
        } catch (IllegalArgumentException e) {
            quote = true;
        }
        check("单引号路径拒绝", quote);

        boolean nl = false;
        try {
            LogViewer.shQuote("/tmp/a\nb");
        } catch (IllegalArgumentException e) {
            nl = true;
        }
        check("换行路径拒绝", nl);
    }

    // ---------------------------------------------------------------- 路径覆盖

    private static void pathOverride() {
        String old = System.getProperty("system.ui.log.path");
        try {
            System.setProperty("system.ui.log.path", "/var/log/dojao.out");
            Path p = LogViewer.logPath();
            check("system.ui.log.path 生效", p.toString().replace('\\', '/').endsWith("/var/log/dojao.out"));
        } finally {
            if (old == null) {
                System.clearProperty("system.ui.log.path");
            } else {
                System.setProperty("system.ui.log.path", old);
            }
        }
        String def = LogViewer.logPath().toString().replace('\\', '/');
        check("默认路径 = ${user.dir}/logs/dojao.out", def.endsWith("/logs/dojao.out"));

        String oldHome = System.getProperty("dojao.home");
        try {
            System.setProperty("dojao.home", "/opt/module/dojao");
            String home = LogViewer.logPath().toString().replace('\\', '/');
            check("dojao.home 决定默认路径",
                    home.endsWith("/opt/module/dojao/logs/dojao.out"));
        } finally {
            if (oldHome == null) {
                System.clearProperty("dojao.home");
            } else {
                System.setProperty("dojao.home", oldHome);
            }
        }
    }

    // ---------------------------------------------------------------- 工具

    private static MonitorWorker worker(String id, String host) {
        return new MonitorWorker(id, host, 4009, "IDLE", true, 4, 0,
                0, 0, 0, "1.0", "worker", 0, 0, 0, 0, 0, 6, 0, 0, List.of(), 0);
    }

    private static void check(String name, boolean ok) {
        if (ok) {
            System.out.println("[PASS] " + name);
        } else {
            fail++;
            System.out.println("[FAIL] " + name);
        }
    }
}
