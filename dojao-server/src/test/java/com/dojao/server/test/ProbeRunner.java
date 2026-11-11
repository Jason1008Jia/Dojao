package com.dojao.server.test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 探针集中回归 runner —— 顺序以<b>子进程</b>方式跑全部 main() 风格探针并汇总 FAIL。
 *
 * <p>探针用 {@code System.exit(...)} 表达失败（如 {@code ChainedWindowStageSplitProbeTest}），
 * 所以不能在同一 JVM 内调用 main；这里为每个探针 fork 一个 java 进程，沿用当前 classpath 与
 * {@code --add-opens}，解析输出里的 PASS/FAIL 汇总后打印总表。</p>
 *
 * <p>用法：</p>
 * <ul>
 *   <li>{@code ProbeRunner} —— 跑默认回归集（窗口 / join / CTE / 三表聚合）</li>
 *   <li>{@code ProbeRunner --only com.dojao.server.test.XxxProbeTest}</li>
 *   <li>{@code ProbeRunner --list} —— 打印默认集</li>
 * </ul>
 */
public final class ProbeRunner {

    /** 探针 + 传给它的场景参数（null = 不传）。 */
    private record Probe(String className, String args) { }

    private static final List<Probe> DEFAULT = List.of(
            new Probe("com.dojao.server.test.WindowArgExprOverJoinProbeTest", null),
            new Probe("com.dojao.server.test.ColumnRetentionProbeTest", null),
            new Probe("com.dojao.server.master.ChainedWindowStageSplitProbeTest", null),
            new Probe("com.dojao.server.test.NestedCteProbeTest", null),
            new Probe("com.dojao.server.test.ThreeTableAggFoldProbeTest", "ABCDEFGHIJKL"),
            new Probe("com.dojao.server.test.MultiWindowGroupDistributedProbeTest", null),
            new Probe("com.dojao.server.test.MultiWindowGroupAggregateDistributedProbeTest", null),
            new Probe("com.dojao.server.test.ShuffleStreamGenerationProbeTest", null),
            new Probe("com.dojao.server.test.DistributedDifferentialProbeTest", null),
            // 取消 → 资源释放（停止后 Worker 仍烧 CPU / 内存不降的回归护栏）
            new Probe("com.dojao.server.test.CancellationReleaseProbeTest", null),
            // Worker 叶子池弹性（固定尺寸 FIFO 会把打破环的生产者片段饿死 → 死锁）
            new Probe("com.dojao.server.test.WorkerLeafPoolElasticProbeTest", null),
            // 扫描存活心跳 / 完工后终态事件迟到不得误判 LOST
            new Probe("com.dojao.server.test.ScanLivenessProbeTest", null),
            // 中间层片段（写 shuffle）必须走弹性叶子池，不得排在等它的主池消费者后面
            new Probe("com.dojao.server.test.MiddleStagePoolProbeTest", null));

    private static final Pattern P1 = Pattern.compile("(\\d+)\\s*PASS\\s*/\\s*(\\d+)\\s*FAIL");
    private static final Pattern P2 = Pattern.compile("PASS=(\\d+)\\s+FAIL=(\\d+)");
    private static final Pattern P3 = Pattern.compile(":\\s*(\\d+)\\s*FAIL");

    public static void main(String[] args) throws Exception {
        List<Probe> probes;
        if (args.length > 0 && "--list".equals(args[0])) {
            DEFAULT.forEach(p -> System.out.println(p.className() + (p.args() == null ? "" : " " + p.args())));
            return;
        }
        if (args.length >= 2 && "--only".equals(args[0])) {
            probes = List.of(new Probe(args[1], args.length > 2 ? args[2] : null));
        } else {
            probes = DEFAULT;
        }

        int totalFail = 0;
        int failedProbes = 0;
        List<String> rows = new ArrayList<>();
        for (Probe p : probes) {
            long t0 = System.nanoTime();
            int[] r = runOne(p);
            long secs = (System.nanoTime() - t0) / 1_000_000_000L;
            totalFail += r[1];
            // 只按 FAIL 判定：部分探针只打印「N FAIL」没有 PASS 数（如 ThreeTableAggFold），
            // 此时 pass=-1 但 exit=0 表示成功，不应计入失败探针。
            if (r[1] > 0) failedProbes++;
            String row = String.format("%-62s pass=%-4s fail=%-3s %3ds",
                    p.className(), r[0] < 0 ? "?" : r[0], r[1], secs);
            rows.add(row);
            System.out.println("[probe] " + row);
        }

        System.out.println("\n================ ProbeRunner 汇总 ================");
        rows.forEach(r -> System.out.println("  " + r));
        System.out.printf("探针 %d 个，失败探针 %d 个，合计 FAIL %d%n", probes.size(), failedProbes, totalFail);
        if (totalFail > 0 || failedProbes > 0) {
            System.exit(1);
        }
    }

    /** 返回 {passCount, failCount}；无法解析时 passCount=-1。 */
    private static int[] runOne(Probe p) throws Exception {
        String java = Path.of(System.getProperty("java.home"), "bin",
                isWindows() ? "java.exe" : "java").toString();
        List<String> cmd = new ArrayList<>();
        cmd.add(java);
        cmd.add("--add-opens=java.base/java.nio=ALL-UNNAMED");
        // 把审计 / 严格开关透传给子进程（探针各自 fork，不继承父 JVM 的系统属性）。
        for (String key : new String[]{"dojao.plan.audit", "dojao.columnPruning.audit",
                "planner.distributed.strict"}) {
            String v = System.getProperty(key);
            if (v != null) {
                cmd.add("-D" + key + "=" + v);
            }
        }
        cmd.add("-cp");
        cmd.add(System.getProperty("java.class.path"));
        cmd.add(p.className());
        if (p.args() != null && !p.args().isBlank()) {
            cmd.add(p.args());
        }
        Path log = Files.createTempFile("dojao-probe-", ".log");
        ProcessBuilder pb = new ProcessBuilder(cmd).redirectErrorStream(true)
                .redirectOutput(log.toFile());
        Process proc = pb.start();
        int exit = proc.waitFor();
        // 探针输出是平台编码（Windows 下 GBK），这里只解析 ASCII 汇总行 ⇒ 用 ISO-8859-1
        // 逐字节无损解码，避免 UTF-8 严格解码在非 ASCII 字节上抛 MalformedInputException。
        String text = new String(Files.readAllBytes(log), StandardCharsets.ISO_8859_1);
        Files.deleteIfExists(log);

        int[] parsed = parse(text);
        if (parsed[0] < 0 && parsed[1] == 0 && exit != 0) {
            // 汇总只给 FAIL 数（如 `... 0 FAIL`）但进程失败：按 1 个失败计，避免假绿。
            parsed = new int[]{-1, 1};
        }
        if (parsed[0] < 0 && exit != 0) {
            // 无法解析汇总但进程失败：把尾部输出打出来帮助定位。
            System.err.println("---- " + p.className() + " (exit=" + exit + ") 尾部输出 ----");
            String[] lines = text.split("\\R");
            for (int i = Math.max(0, lines.length - 25); i < lines.length; i++) {
                System.err.println(lines[i]);
            }
        }
        return parsed;
    }

    private static int[] parse(String text) {
        Matcher m = P1.matcher(text);
        int pass = -1, fail = -1;
        while (m.find()) {
            pass = Integer.parseInt(m.group(1));
            fail = Integer.parseInt(m.group(2));
        }
        if (pass >= 0) return new int[]{pass, fail};

        m = P2.matcher(text);
        while (m.find()) {
            pass = Integer.parseInt(m.group(1));
            fail = Integer.parseInt(m.group(2));
        }
        if (pass >= 0) return new int[]{pass, fail};

        m = P3.matcher(text);
        while (m.find()) {
            fail = Integer.parseInt(m.group(1));
        }
        if (fail >= 0) return new int[]{-1, fail};

        int bad = 0;
        for (String line : text.split("\\R")) {
            if (line.contains("[FAIL]") || line.contains("[!!]")) bad++;
        }
        return new int[]{-1, bad};
    }

    private static boolean isWindows() {
        return System.getProperty("os.name", "").toLowerCase().contains("win");
    }
}
