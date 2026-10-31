package com.dojao.ui.log;

import com.dojao.ui.monitor.MonitorCluster;
import com.dojao.ui.monitor.MonitorWorker;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * 集群日志只读查看：读取 Master / Worker 的 {@code dojao.out}。</p>
 *
 * <p><b>日志在哪</b>：推荐各机用
 * {@code nohup ./bin/dojao >> logs/dojao.out 2>&1} 启动，每台机器一个文件；logback 本身只输出到
 * STDOUT（见 {@code dojao-server/src/main/resources/logback.xml}），没有滚动文件。UI 跑在 Master
 * 进程内 ⇒ 本机日志直接读文件；其余 Worker 机走 <b>Master → Worker 的免密 ssh</b>
 * （需集群各机安装路径一致，并配置到各 Worker 的免密 ssh）。</p>
 *
 * <p><b>路径</b>：默认取 {@code ${dojao.home}/logs/dojao.out} 的绝对路径（{@code bin/dojao}
 * 会注入 {@code -Ddojao.home}）。集群各机安装路径一致，所以同一个绝对路径在 Worker 上也成立；
 * 路径不同时用 {@code -Dsystem.ui.log.path=...} 覆盖。
 * ssh 可执行文件与超时：{@code -Dsystem.ui.log.ssh}（默认 {@code ssh}）、
 * {@code -Dsystem.ui.log.timeout.ms}（默认 15000）。</p>
 *
 * <p><b>安全边界</b>：客户端只能从当前集群快照（{@link MonitorCluster}）里挑节点，主机名/路径
 * 都不来自请求参数；远端命令是固定的 {@code tail -n N -- '<path>'}，不做 shell 拼接。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/20
 */
public final class LogViewer {

    /** 默认返回行数。 */
    public static final int DEFAULT_TAIL = 200;
    /** 单次最多返回行数（防止把几十 MB 日志塞进一个 JSON）。 */
    public static final int MAX_TAIL = 5000;
    /** 本地从文件尾部最多读这么多字节（≈ 8MB）。 */
    private static final int MAX_BYTES = 8 * 1024 * 1024;

    private static final long SSH_TIMEOUT_MS =
            Long.getLong("system.ui.log.timeout.ms", 15_000L);

    private LogViewer() {
    }

    /**
     * 日志文件路径：{@code -Dsystem.ui.log.path} 优先，否则 {@code ${dojao.home}/logs/dojao.out}
     * （{@code bin/dojao} 会注入 {@code -Ddojao.home}；未注入时退回 {@code ${user.dir}}）。
     */
    public static Path logPath() {
        String configured = System.getProperty("system.ui.log.path");
        if (configured != null && !configured.isBlank()) {
            return Paths.get(configured.trim()).toAbsolutePath().normalize();
        }
        String home = System.getProperty("dojao.home");
        Path base = home != null && !home.isBlank()
                ? Paths.get(home)
                : Paths.get(System.getProperty("user.dir"));
        return base.resolve("logs").resolve("dojao.out").toAbsolutePath().normalize();
    }

    /**
     * 解析请求里的 {@code node}：{@code master}/{@code local}/Master 主机名 → 本机；
     * Worker 的 {@code workerId}（或唯一前缀）/主机名 → 该 Worker（与 Master 同机则仍读本机文件）。
     *
     * @return 命中返回目标；未知节点或前缀有歧义返回 {@code null}
     */
    public static Target resolve(MonitorCluster cluster, String requested) {
        if (cluster == null) {
            return null;
        }
        String node = requested == null || requested.isBlank() ? "master" : requested.trim();
        MonitorWorker master = cluster.master();
        String masterHost = master == null ? null : master.host();
        if ("master".equalsIgnoreCase(node) || "local".equalsIgnoreCase(node)
                || (masterHost != null && masterHost.equalsIgnoreCase(node))) {
            return new Target("master", masterHost == null ? "localhost" : masterHost, true);
        }

        List<MonitorWorker> workers = cluster.workers() == null ? List.of() : cluster.workers();
        MonitorWorker hit = null;
        for (MonitorWorker w : workers) {
            if (w != null && node.equals(w.workerId())) {
                hit = w;
                break;
            }
        }
        if (hit == null) {
            // 允许 workerId 前缀（界面上动辄 36 位，肉眼复制不方便），但必须唯一。
            for (MonitorWorker w : workers) {
                if (w == null || w.workerId() == null || !w.workerId().startsWith(node)) {
                    continue;
                }
                if (hit != null && !hit.workerId().equals(w.workerId())) {
                    return null;
                }
                hit = w;
            }
        }
        if (hit == null) {
            // 按主机名：同机多 Worker 写的是同一个 dojao.out，取第一个即可。
            for (MonitorWorker w : workers) {
                if (w != null && w.host() != null && w.host().equalsIgnoreCase(node)) {
                    hit = w;
                    break;
                }
            }
        }
        if (hit == null || hit.host() == null || hit.host().isBlank()) {
            return null;
        }
        boolean local = masterHost != null && masterHost.equalsIgnoreCase(hit.host());
        return new Target(hit.workerId(), hit.host(), local);
    }

    /**
     * 读本机日志尾部：按字节倒读，最多 {@value #MAX_BYTES} 字节 / {@code maxLines} 行。
     *
     * @throws NoSuchFileException 文件不存在
     */
    public static List<String> tailLocal(Path file, int maxLines) throws IOException {
        Objects.requireNonNull(file, "file");
        if (maxLines < 1) {
            throw new IllegalArgumentException("maxLines 必须 >= 1: " + maxLines);
        }
        if (!Files.isRegularFile(file)) {
            throw new NoSuchFileException(file.toString());
        }
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r")) {
            long length = raf.length();
            long start = Math.max(0, length - MAX_BYTES);
            int size = (int) (length - start);
            byte[] buf = new byte[size];
            raf.seek(start);
            int read = 0;
            while (read < size) {
                int n = raf.read(buf, read, size - read);
                if (n < 0) {
                    break;
                }
                read += n;
            }
            String text = new String(buf, 0, read, StandardCharsets.UTF_8);
            if (start > 0) {
                // 字节窗口从行中间开始：丢掉残行。
                int nl = text.indexOf('\n');
                text = nl < 0 ? "" : text.substring(nl + 1);
            }
            List<String> all = splitLines(text);
            int from = Math.max(0, all.size() - maxLines);
            return List.copyOf(all.subList(from, all.size()));
        }
    }

    /**
     * ssh 到 Worker 机读同一个绝对路径的日志尾部。
     *
     * @param host 来自集群快照的主机名（不接受任意请求参数）
     */
    public static List<String> tailRemote(String host, Path path, int maxLines) throws IOException {
        Objects.requireNonNull(host, "host");
        Objects.requireNonNull(path, "path");
        if (maxLines < 1) {
            throw new IllegalArgumentException("maxLines 必须 >= 1: " + maxLines);
        }
        String ssh = System.getProperty("system.ui.log.ssh", "ssh");
        List<String> cmd = new ArrayList<>();
        cmd.add(ssh);
        cmd.add("-o");
        cmd.add("BatchMode=yes");
        cmd.add("-o");
        cmd.add("ConnectTimeout=10");
        cmd.add(host);
        cmd.add("tail -n " + maxLines + " -- " + shQuote(path.toString()));

        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(true);   // ssh 成功时 stderr 为空；失败时整段当错误信息
        Process p = pb.start();
        p.getOutputStream().close();

        byte[][] out = new byte[1][];
        Throwable[] readerError = new Throwable[1];
        Thread reader = new Thread(() -> {
            try {
                out[0] = readBounded(p.getInputStream(), MAX_BYTES);
            } catch (Throwable t) {
                readerError[0] = t;
                p.destroyForcibly();
            }
        }, "dojao-log-tail-" + host);
        reader.setDaemon(true);
        reader.start();

        try {
            if (!p.waitFor(SSH_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                p.destroyForcibly();
                throw new IOException("ssh " + host + " 读取日志超时（" + SSH_TIMEOUT_MS + "ms）");
            }
            reader.join(2_000);
        } catch (InterruptedException e) {
            p.destroyForcibly();
            Thread.currentThread().interrupt();
            throw new IOException("ssh " + host + " 被中断", e);
        }
        if (readerError[0] != null) {
            throw new IOException("读取 ssh " + host + " 输出失败: " + readerError[0].getMessage(),
                    readerError[0]);
        }
        String text = new String(out[0] == null ? new byte[0] : out[0], StandardCharsets.UTF_8);
        if (p.exitValue() != 0) {
            String detail = text.strip();
            if (detail.length() > 2000) {
                detail = detail.substring(0, 2000) + "…";
            }
            throw new IOException("ssh " + host + " 退出码 " + p.exitValue()
                    + (detail.isEmpty() ? "" : ": " + detail));
        }
        // 远端 tail 只保证「最后 N 行」，这里再兜一次（防止实现差异）。
        List<String> all = splitLines(text);
        int from = Math.max(0, all.size() - maxLines);
        return List.copyOf(all.subList(from, all.size()));
    }

    /** 单引号包裹远端路径；路径由服务端配置给出，含单引号/换行时直接拒绝。 */
    static String shQuote(String s) {
        if (s.indexOf('\'') >= 0 || s.indexOf('\n') >= 0 || s.indexOf('\r') >= 0) {
            throw new IllegalArgumentException("非法日志路径: " + s);
        }
        return "'" + s + "'";
    }

    private static List<String> splitLines(String text) {
        List<String> out = new ArrayList<>();
        int from = 0;
        for (int i = 0; i < text.length(); i++) {
            if (text.charAt(i) == '\n') {
                int end = (i > from && text.charAt(i - 1) == '\r') ? i - 1 : i;
                out.add(text.substring(from, end));
                from = i + 1;
            }
        }
        if (from < text.length()) {
            out.add(text.substring(from));
        }
        return out;
    }

    private static byte[] readBounded(InputStream in, int max) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(Math.min(max, 64 * 1024));
        byte[] buf = new byte[16 * 1024];
        int n;
        while ((n = in.read(buf)) >= 0) {
            if (bos.size() + n > max) {
                throw new IOException("输出超过 " + max + " 字节，请减小 tail");
            }
            bos.write(buf, 0, n);
        }
        return bos.toByteArray();
    }

    /**
     * 一个可读节点：{@code node} 为请求里解析出的标识（Worker 为完整 workerId），
     * {@code host} 为集群快照里的主机名，{@code local} = 与 Master 同机（读本机文件）。
     */
    public record Target(String node, String host, boolean local) {
        public Target {
            Objects.requireNonNull(node, "node");
            Objects.requireNonNull(host, "host");
        }

        @Override
        public String toString() {
            return node + "@" + host + (local ? " (local)" : "");
        }
    }
}
