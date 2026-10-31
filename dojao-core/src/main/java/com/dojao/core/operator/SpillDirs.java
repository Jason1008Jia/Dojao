package com.dojao.core.operator;

import com.dojao.core.config.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * 溢写目录管理 —— 根目录清单、<b>盘容量水位校验</b>、<b>孤儿目录清理</b>。
 *
 * <h3>① 容量水位（为什么每次溢写都要查）</h3>
 * 溢写是「内存换磁盘」：盘写满了比内存炸掉更难恢复（查询失败 + 现场被写坏）。所以每次真正要写溢写
 * 文件前都查一次该目录所在盘的<b>使用率</b>，达到 {@code query.spill.disk.maxUsage}（默认 0.8）就不写：
 * 换个合格目录（{@code query.spill.dir} 配多块盘时）；<b>所有盘都超水位 → 抛
 * {@link SpillSpaceException}</b>，错误信息把全部候选目录与使用率列出来，一眼看出该清哪块盘。
 *
 * <p>判定不了（路径不存在且父目录也不存在、文件系统不支持 statfs）→ 视为<b>可用</b>（fail-open）：
 * 宁可写得进去，也不因为查不到容量而误杀查询。</p>
 *
 * <h3>② 清理（三种死法都要收干净）</h3>
 * <ul>
 *   <li><b>正常结束 / 算子关闭</b> → {@code AbstractSpillableOperator.doClose} 删自己的实例子目录；</li>
 *   <li><b>Ctrl+C / systemd stop（SIGINT / SIGTERM）</b> → 本类的 JVM 关闭钩子（{@link #register}
 *       在首次登记时安装）删掉本 JVM 用过的全部实例子目录；</li>
 *   <li><b>kill -9 / 崩溃 / 强杀</b> → 钩子没机会跑，只能靠下次启动的
 *       {@link #cleanOrphanDirs()}（启动第一件事）：扫各根目录，删「归属进程已死」的实例子目录。</li>
 * </ul>
 *
 * <p><b>实例子目录命名约定</b>（{@code <算子名>-p<PID>-<序号>}）是孤儿判定的唯一依据：
 * 目录名里带 PID ⇒ 能问 OS「这个 PID 还活着吗」；不带该后缀的目录一律<b>不碰</b>
 * （用户可能把别的东西放进去了）。这也让同机多 JVM 共享一个根目录时互不误删。</p>
 *
 * @author jiaxin
 * @date 2026/9/27
 */
public final class SpillDirs {

    private static final Logger log = LoggerFactory.getLogger(SpillDirs.class);

    /** 实例子目录名后缀：{@code <算子名>-p<PID>-<序号>}。 */
    private static final Pattern INSTANCE_DIR = Pattern.compile(".*-p(\\d+)-(\\d+)$");

    /** 本 JVM 在用（或用过）的实例子目录 —— 关闭钩子据此清盘。 */
    private static final Set<Path> LIVE_DIRS = ConcurrentHashMap.newKeySet();

    /** 关闭钩子只装一次。 */
    private static final AtomicBoolean HOOK_INSTALLED = new AtomicBoolean();

    private SpillDirs() {
    }

    /** 全部溢写根目录（{@code query.spill.dir}，默认 {@code <DOJAO_HOME>/spill}）。 */
    public static List<Path> roots() {
        return ResourceConfig.spillDirs();
    }

    /** 单块盘使用率上限（{@code query.spill.disk.maxUsage}，默认 0.8）：达到即不再往该盘写。 */
    public static double maxUsageRatio() {
        return ResourceConfig.spillDiskMaxUsageRatio();
    }

    /**
     * 该目录所在盘是否还能承接溢写（使用率 &lt; 上限）。判定不了 → {@code true}（fail-open）。
     */
    public static boolean hasRoom(Path dir) {
        double used = usageRatio(dir);
        return used < 0 || used < maxUsageRatio();
    }

    /** 目录所在盘使用率的可读文案（如 {@code 82.3%}）；判定不了返回 {@code 未知}。 */
    public static String describeUsage(Path dir) {
        double used = usageRatio(dir);
        return used < 0 ? "未知" : percent(used);
    }

    /** 使用率上限的可读文案（如 {@code 80.0%}）。 */
    public static String describeUsageLimit() {
        return percent(maxUsageRatio());
    }

    /**
     * 目录所在盘的<b>可用空间</b>可读文案（如 {@code 剩余 23.4 GB / 共 100.0 GB，已用 76.6%}）。
     *
     * <p>与 {@link #describeUsage(Path)} 互补：使用率说明「为什么不再写」，可用空间说明「还差多少」——
     * 溢写失败的错误信息两者都要带，否则用户无法判断是该清盘还是该加盘。判定不了返回 {@code 未知}。</p>
     */
    public static String describeFreeSpace(Path dir) {
        try {
            FileStore fs = fileStoreOf(dir);
            if (fs == null) {
                return "未知";
            }
            long total = fs.getTotalSpace();
            long usable = fs.getUsableSpace();
            if (total <= 0 || usable < 0) {
                return "未知";
            }
            return "剩余 " + humanBytes(usable) + " / 共 " + humanBytes(total)
                    + "，已用 " + percent((double) (total - usable) / (double) total);
        } catch (Throwable t) {
            return "未知";
        }
    }

    /** 人可读容量（B/KB/MB/GB/TB）。 */
    private static String humanBytes(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        }
        double v = bytes;
        String[] units = {"KB", "MB", "GB", "TB"};
        for (String unit : units) {
            v /= 1024.0;
            if (v < 1024 || "TB".equals(unit)) {
                return String.format("%.1f %s", v, unit);
            }
        }
        return bytes + " B";
    }

    /** 目录所在盘的使用率 [0,1]；判定不了（含盘根本不存在）返回 {@code -1}。 */
    public static double usageRatio(Path dir) {
        try {
            FileStore fs = fileStoreOf(dir);
            if (fs == null) {
                return -1;
            }
            long total = fs.getTotalSpace();
            long usable = fs.getUsableSpace();
            if (total <= 0 || usable < 0) {
                return -1;
            }
            return (double) (total - usable) / (double) total;
        } catch (Throwable t) {
            return -1;
        }
    }

    /**
     * 另选一个可写的实例子目录：遍历 {@link #roots()}，跳过 {@code currentDir} 所在根目录，
     * 返回第一个盘使用率合格的 {@code <root>/<dirName>}。
     *
     * <p>全部不合格（含只有一块盘的情况）→ 抛 {@link SpillSpaceException}，错误信息列出
     * <b>所有</b>候选目录及其使用率。</p>
     *
     * <p>不创建目录（只 statfs 最近的已存在祖先）—— 免得为了「探测一下」就在每块盘上留下空目录。</p>
     */
    public static Path pickUsableDir(Path currentDir, String dirName) {
        List<Path> roots = roots();
        for (Path root : roots) {
            Path candidate = root.resolve(dirName);
            if (candidate.equals(currentDir)) {
                continue;
            }
            if (hasRoom(candidate)) {
                return candidate;
            }
        }
        throw new SpillSpaceException(noSpaceMessage(roots, dirName));
    }

    /** 磁盘不够时的错误文案：把全部候选目录与使用率列出来。 */
    private static String noSpaceMessage(List<Path> roots, String dirName) {
        StringBuilder sb = new StringBuilder("磁盘溢写空间不够：所有溢写目录所在盘使用率均已达到上限 ")
                .append(percent(maxUsageRatio()))
                .append("，无法溢写。待溢写目录：");
        for (Path root : roots) {
            sb.append(System.lineSeparator()).append("  - ").append(root.resolve(dirName))
                    .append("（所在盘 ").append(root).append(" 已用 ")
                    .append(describeUsage(root)).append("）");
        }
        sb.append(System.lineSeparator())
                .append("请清理磁盘，或调大 query.spill.disk.maxUsage，或改 query.spill.dir 指向有空间的盘。");
        return sb.toString();
    }

    /** 使用率上限/实际值都超过 100% 时也照实打印（便于判断是不是配错了盘）。 */
    private static String percent(double ratio) {
        return String.format("%.1f%%", ratio * 100);
    }

    // ==================== 清理 ====================

    /**
     * 登记本 JVM 在用的实例子目录，并（首次）安装 JVM 关闭钩子 —— Ctrl+C / SIGTERM 时清盘。
     * 算子自己关干净后应调 {@link #unregister}，免得钩子重复走一遍。
     */
    public static void register(Path dir) {
        if (dir == null) {
            return;
        }
        LIVE_DIRS.add(dir);
        if (HOOK_INSTALLED.compareAndSet(false, true)) {
            try {
                Runtime.getRuntime().addShutdownHook(
                        new Thread(SpillDirs::cleanLiveDirs, "dojao-spill-cleanup"));
            } catch (IllegalStateException ignored) {
                // JVM 已在关闭中，钩子装不上 → 交给下次启动的孤儿扫描兜底
            }
        }
    }

    /** 算子已自行清干净该目录 → 从钩子名单摘除。 */
    public static void unregister(Path dir) {
        if (dir != null) {
            LIVE_DIRS.remove(dir);
        }
    }

    /** 关闭钩子：删本 JVM 用过的全部实例子目录。 */
    private static void cleanLiveDirs() {
        int removed = 0;
        for (Path dir : List.copyOf(LIVE_DIRS)) {
            if (deleteRecursively(dir)) {
                removed++;
            }
        }
        // 关闭钩子里 logback 可能已被它自己的钩子关掉，用 System.err 保稳
        if (removed > 0) {
            System.err.printf(">>> [溢写] 关闭清盘：已删除 %d 个溢写目录%n", removed);
        }
    }

    /**
     * <b>启动清残留</b>：删掉各根目录下「归属进程已死」的实例子目录（kill -9 / 崩溃 / 强杀的遗留）。
     *
     * <p>只认实例子目录命名且归属 PID 确已不存在，其余文件/目录一律不动 ⇒ 同机多 JVM、或用户
     * 往溢写根目录里放了别的东西，都安全。</p>
     *
     * @return 删除的目录数
     */
    public static int cleanOrphanDirs() {
        int removed = 0;
        for (Path root : roots()) {
            if (!Files.isDirectory(root)) {
                continue;
            }
            List<Path> children;
            try (Stream<Path> s = Files.list(root)) {
                children = s.toList();
            } catch (IOException e) {
                log.warn("[溢写] 无法列举溢出目录 {}：{}", root, e.toString());
                continue;
            }
            for (Path child : children) {
                if (!Files.isDirectory(child)) {
                    continue;
                }
                long pid = ownerPid(child);
                if (pid <= 0) {
                    continue;   // 不是溢写实例子目录 → 不是我们的东西，不动
                }
                if (ProcessHandle.of(pid).isPresent()) {
                    continue;   // 归属进程还活着（同机别的 Worker）→ 不动
                }
                if (deleteRecursively(child)) {
                    removed++;
                }
            }
        }
        if (removed > 0) {
            log.info("[溢写] 启动清残留：已删除 {} 个孤儿溢写目录（上次 kill -9 / 崩溃遗留）", removed);
        }
        return removed;
    }

    /** 实例子目录名里的归属 PID；不是实例子目录 → {@code -1}。 */
    static long ownerPid(Path dir) {
        Path name = dir.getFileName();
        if (name == null) {
            return -1;
        }
        Matcher m = INSTANCE_DIR.matcher(name.toString());
        if (!m.matches()) {
            return -1;
        }
        try {
            return Long.parseLong(m.group(1));
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    /** 递归删除（幂等：不存在返回 false）。 */
    public static boolean deleteRecursively(Path dir) {
        if (dir == null || !Files.exists(dir)) {
            return false;
        }
        try (Stream<Path> s = Files.walk(dir)) {
            s.sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (IOException ignored) {
                        }
                    });
        } catch (IOException e) {
            log.warn("[溢写] 删除目录失败 {}：{}", dir, e.toString());
            return false;
        }
        return true;
    }

    // ==================== 内部 ====================

    /**
     * 取路径所在文件系统 —— 沿父目录上溯到第一个<b>已存在</b>的祖先再问
     * （{@code Files.getFileStore} 要求路径存在；溢写目录此时往往还没建）。
     *
     * <p>整条祖先链都不存在（连挂载点都不在，典型是配错了盘 / 盘没挂上）→ 返回 {@code null}
     * 表示<b>不知道</b>，由调用方按「未知」处理（fail-open）。<b>绝不退化成拿当前工作目录所在的盘
     * 顶替</b> —— 那会报出另一块盘的容量，既误导用户又可能误判（比如溢写目录在满盘的 NFS 上，
     * 却按本地 C: 的低占用率放行）。</p>
     */
    private static FileStore fileStoreOf(Path path) throws IOException {
        Path p = path.toAbsolutePath().normalize();
        while (p != null && !Files.exists(p)) {
            p = p.getParent();
        }
        return p == null ? null : Files.getFileStore(p);
    }

    /**
     * 所有溢写目录所在盘都超过水位上限 ⇒ 溢写无法进行。
     *
     * <p>是 {@link IllegalStateException}：查询会失败并把这条消息带给用户（不是把节点写爆）。</p>
     */
    public static final class SpillSpaceException extends IllegalStateException {
        SpillSpaceException(String message) {
            super(message);
        }
    }
}
