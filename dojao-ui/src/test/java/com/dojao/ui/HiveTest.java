package com.dojao.ui;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * HDFS 造数工具 —— 往 {@code dojao_test.ods_test} 的分区目录里灌 1 亿条数据。
 *
 * <p><b>只写 HDFS，不碰 Hive 元数据。</b>分区注册由使用者自己在 Hive 客户端执行
 * （程序跑完会把命令再打一遍）。
 *
 * <p>目标表（ORC + SNAPPY，按 version 分区）：
 * <pre>
 *   CREATE TABLE dojao_test.ods_test (
 *     id bigint, guid string, content string, creationtime string,
 *     referenceid bigint, referenceimage string
 *   ) PARTITIONED BY (version string) STORED AS ORC;
 * </pre>
 *
 * <p><b>做法：</b>本机多线程用 ORC Writer 直接写 HDFS（{@code hdfs://} 走 RPC 9000），
 * 每个线程写一个 {@code 00000k_0} 文件。默认 <b>10 个文件 × 1000 万行 = 1 亿行</b>。
 * SNAPPY 是表属性（{@code orc.compress=SNAPPY}），这里显式指定一次，保证与 Hive 写出来的文件一致。
 *
 * <p>数据每列都由行号 n 经 splitmix64 混出，<b>可复现</b>（同样的 n 永远是同一份内容），
 * 不需要随机数状态，多线程天然安全。连 guid 也是混出来的（形如 UUID 的 8-4-4-4-12），
 * 不用 {@code UUID.randomUUID()} —— 那个走 SecureRandom，是 synchronized 的，
 * 10 个线程各造 1000 万行时会变成一个串行瓶颈。
 *
 * <p><b>用法（参数也可写在 IDEA 运行配置的 Program arguments 里）：</b>
 * <pre>
 *   （无参）                        全量：1 亿行 / 10 个文件（每个 1000 万）
 *   --rows=10000000 --files=1       小样冒烟（一个文件）
 *   --clean                         先删旧数据目录，再重写（重跑必加，否则同一目录里数据翻倍）
 *   --dry-run                       只打印计划，不连任何服务
 * </pre>
 *
 * <p><b>公网链路加固：</b>本机到 aliyun 是公网长连接，一次要传几个 GB、跑几十分钟。
 * 写 DataNode 的时候 NameNode 那条 RPC 连接是空闲的，而默认要 60 秒才发一次 ping ——
 * 中途的 NAT / 防火墙经常在 30 秒就把这条空闲连接 RST 掉，于是
 * {@code renewLease} 连续失败 → NameNode 判租约过期 → 把正在写的文件当垃圾删掉 →
 * 客户端 {@code ClosedChannelException}（现象就是进度一直 0、最后整个文件失败）。
 * 所以这里把 ping 间隔压到 20 秒让长连接一直有事做，并把各类重试放宽；
 * 另外<b>每个文件失败了会自动清掉半截文件重试 {@value #MAX_ATTEMPTS} 次</b>（数据可复现，重写结果一样）。
 *
 * <p><b>写完会做「落地自查」</b>：列 HDFS 目录 + 读每个 ORC 文件尾部的 NumberOfRows 求和，
 * 不经 Hive 就能确认实际写了多少行（ORC 行数写在 file footer 里，不扫数据，1 亿行也秒回）。
 *
 * <p>运行前请确认安全组已放开 {@code aliyun001:9000}（HDFS RPC）与 {@code 9866}（DataNode 数据传输）。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/19
 */
public class HiveTest {

    // ==================== 目标表 / HDFS ====================
    private static final String TABLE = "dojao_test.ods_test";
    private static final String PARTITION_VALUE = "v1";     // PARTITIONED BY version string
    private static final String NAMENODE = "hdfs://aliyun001:9000";
    private static final String TABLE_DIR = "/user/hive/warehouse/dojao_test.db/ods_test";
    private static final String HDFS_USER = "root";         // HDFS 侧文件属主

    // ==================== 规模 ====================
    /** 一个文件 1000 万行 × 10 个文件 = 1 亿行。 */
    private static final long ROWS_PER_FILE = 10_000_000L;
    private static final int DEFAULT_FILES = 10;
    private static final long DEFAULT_ROWS = ROWS_PER_FILE * DEFAULT_FILES;
    private static final int BATCH_ROWS = 8 * 1024;

    /** 单个文件写失败（公网抖动）时的最大尝试次数。 */
    private static final int MAX_ATTEMPTS = 5;

    /** 数据列（不含分区列 version —— 分区列不落在文件里）。顺序必须与建表语句一致。 */
    private static final String SCHEMA_TEXT =
            "struct<id:bigint,guid:string,content:string,creationtime:string,referenceid:bigint,referenceimage:string>";

    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final LocalDateTime TS_BASE = LocalDateTime.of(2026, 9, 19, 0, 0, 0);
    private static final long SECONDS_IN_YEAR = 365L * 24 * 3600;

    public static void main(String[] args) throws Exception {
        long rows = DEFAULT_ROWS;
        int files = DEFAULT_FILES;
        boolean clean = false;
        boolean dryRun = false;
        for (String a : args) {
            if ("--dry-run".equals(a)) {
                dryRun = true;
            } else if ("--clean".equals(a)) {
                clean = true;
            } else if (a.startsWith("--rows=")) {
                rows = Long.parseLong(a.substring("--rows=".length()).trim());
            } else if (a.startsWith("--files=")) {
                files = Integer.parseInt(a.substring("--files=".length()).trim());
            } else {
                System.out.println("未知参数: " + a + "（可用：--rows= --files= --clean --dry-run）");
                return;
            }
        }
        if (rows <= 0 || files <= 0) {
            System.out.println("--rows / --files 必须为正数");
            return;
        }
        if (files > rows) {
            files = (int) rows;
        }

        String partDir = TABLE_DIR + "/version=" + PARTITION_VALUE;
        System.out.println("目标表   : " + TABLE + "   PARTITION (version='" + PARTITION_VALUE + "')");
        System.out.println("HDFS     : " + NAMENODE + partDir);
        // 实测每行落盘约 83 字节（SNAPPY 之后 —— guid / content 尾缀 / referenceimage 都是随机串，压不动）
        System.out.printf("规模     : %,d 行 / %d 个 ORC 文件（每个约 %,d 行，SNAPPY）≈ %,.1f GB%n",
                rows, files, rows / files, rows * 83L / 1073741824.0);
        // 实测纯生成 ~270 万行/秒（1000 万行算 3.7 秒），所以墙上时间几乎全花在上传带宽上
        System.out.println("说明     : 生成不是瓶颈（~270 万行/秒），耗时基本等于这批数据走公网传到集群的时间");
        if (dryRun) {
            for (int i = 0; i < files; i++) {
                System.out.printf("  文件 %06d_0 → 行 [%,d, %,d)%n",
                        i, rows * i / files, rows * (i + 1) / files);
            }
            return;
        }

        System.setProperty("HADOOP_USER_NAME", HDFS_USER);
        prepareHadoopHome();
        FileSystem fs = FileSystem.get(hdfsConf());

        // 注意：表是 managed 时，Hive 的 DROP PARTITION 会连数据目录一起删。
        // 所以重跑时务必「先在 Hive 里 DROP PARTITION，再跑 --clean」——
        // 顺序反了新写的数据会被 DROP 当场清掉。
        if (clean) {
            System.out.println("\n--clean：删除 HDFS 目录 " + partDir);
            fs.delete(new Path(partDir), true);
        }
        fs.mkdirs(new Path(partDir));

        // ==================== 并行写 ORC ====================
        System.out.println("\n=== 开始写 " + files + " 个 ORC 文件 ===");
        long begin = System.currentTimeMillis();
        // 每文件已写行数：写线程更新、汇报线程读（只用于打进度）
        AtomicLongArray written = new AtomicLongArray(files);
        Thread reporter = startProgressReporter(written, files, rows, begin);

        ExecutorService pool = Executors.newFixedThreadPool(files);
        List<Future<long[]>> futures = new ArrayList<>();
        for (int i = 0; i < files; i++) {
            long from = rows * i / files;
            long to = rows * (i + 1) / files;
            int idx = i;
            futures.add(pool.submit(() -> writeOneFile(fs, partDir, idx, from, to, written)));
        }
        pool.shutdown();

        long totalRows = 0;
        long totalBytes = 0;
        int failed = 0;
        for (int i = 0; i < futures.size(); i++) {
            try {
                long[] r = futures.get(i).get();
                totalRows += r[0];
                totalBytes += r[1];
            } catch (Exception e) {
                failed++;
                System.out.printf("  !! 文件 %06d_0 最终失败：%s%n", i, e.getCause() == null ? e : e.getCause());
            }
        }
        reporter.interrupt();

        if (failed > 0) {
            System.out.printf("%n!! 有 %d 个文件没能写完（已重试 %d 次）。%n", failed, MAX_ATTEMPTS);
            System.out.println("   多半还是公网链路被掐（NameNode RPC 被 RST → 租约过期 → 文件被删）。");
            System.out.println("   确认安全组 9000 / 9866 通畅后重跑；重跑请带上 --clean 清掉半截文件。");
            return;
        }
        System.out.printf("写入完成：%,d 行 / %.1f MB / 耗时 %.1f 秒%n",
                totalRows, totalBytes / 1024.0 / 1024.0, (System.currentTimeMillis() - begin) / 1000.0);

        // ==================== 落地自查（只读 ORC 文件尾，不依赖 Hive） ====================
        try {
            hdfsSelfCheck(fs, partDir, rows);
        } catch (Exception e) {
            System.out.println("\n(自查跳过：读 HDFS 目录/ORC 文件尾失败 " + e + ")");
        }

        printHiveCommands(partDir);
    }

    // ==================== Hadoop 客户端配置 ====================

    /** HDFS 客户端配置：跨公网的写要大半天，容错参数比默认值放宽一截。 */
    private static Configuration hdfsConf() {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", NAMENODE);
        // NameNode 找得到、DataNode 连不上（9866 连接超时）时打开这个开关。
        // 默认情况下 NameNode 下发的是各 DataNode 注册的地址 —— 集群在 VPC 内时那是内网 IP
        // （如 172.21.57.15），VPC 外的机器根本没有路由，连过去就是干等超时。
        // 打开后改成下发主机名，本机再用 hosts 解析成公网 IP。
        // 前提：① hosts 里有这些 DataNode 主机名的条目；② 安全组放开 9866（DataNode 数据传输端口）。
        conf.setBoolean("dfs.client.use.datanode.hostname", true);

        // ---- 闲置长连接保活（这次踩的坑，最关键的一条）----
        // 写 DataNode 期间 NameNode 的 RPC 连接是空闲的，默认 60s 才 ping 一次，
        // 中途的 NAT / 防火墙常在 30s 就把这条空闲连接回收 → renewLease 失败 → 租约过期 → 文件被删。
        conf.setInt("ipc.ping.interval", 20_000);

        // ---- 连接、读写超时与重试 ----
        conf.setInt("ipc.client.connect.timeout", 30_000);
        conf.setInt("ipc.client.connect.max.retries", 10);
        conf.setInt("ipc.client.connect.max.retries.on.timeouts", 10);
        conf.setInt("dfs.client.socket-timeout", 120_000);
        // DataNode 管道坏了就地重建，别让整个文件失败
        conf.setInt("dfs.client.block.write.retries", 10);
        conf.setInt("dfs.client.block.write.locateFollowingBlock.retries", 10);
        // 连接被重置这类可重试异常，最多重试半小时
        conf.setBoolean("dfs.client.retry.policy.enabled", true);
        conf.setInt("dfs.client.retry.policy.max.time.sec", 1800);
        return conf;
    }

    // ==================== 进度汇报 ====================

    /**
     * 每 10 秒打一行合计进度 —— 1 亿行要跑挺久，不能全程黑屏。
     *
     * <p>带上**行/秒**是因为：这里的时间几乎全花在上传带宽上（生成本身有 270 万行/秒），
     * 一旦链路抖了就会看起来「纹丝不动」。有速率就能一眼分清「慢」和「卡死」——
     * 速率掉到 0 且持续几轮，才是真出问题了（此时每条文件的重试逻辑会接手）。
     */
    private static Thread startProgressReporter(AtomicLongArray written, int files, long rows, long begin) {
        Thread t = new Thread(() -> {
            long lastSum = 0;
            long lastAt = System.currentTimeMillis();
            while (true) {
                try {
                    Thread.sleep(10_000);
                } catch (InterruptedException e) {
                    return;
                }
                long sum = 0;
                for (int i = 0; i < files; i++) {
                    sum += written.get(i);
                }
                long now = System.currentTimeMillis();
                long elapsedMs = now - lastAt;
                double rate = elapsedMs <= 0 ? 0 : (sum - lastSum) * 1000.0 / elapsedMs;
                lastSum = sum;
                lastAt = now;
                System.out.printf("  …进度 %,d / %,d 行（%.2f%%） | %,.0f 行/秒 | 已耗时 %.0f 秒%n",
                        sum, rows, sum * 100.0 / rows, rate, (now - begin) / 1000.0);
            }
        }, "orc-progress");
        t.setDaemon(true);
        t.start();
        return t;
    }

    // ==================== ORC 写入 ====================

    /**
     * 写第 idx 个文件，负责行区间 [from, to)，返回 {行数, 字节数}。
     *
     * <p>跨公网的写得跑很久，中途断一次很正常 —— 这里失败会删掉半截文件重试，最多 {@value #MAX_ATTEMPTS} 次。
     * 数据由行号确定性地混出来，重写一遍内容完全一样，所以重试是安全的。
     */
    private static long[] writeOneFile(FileSystem fs, String partDir,
                                       int idx, long from, long to,
                                       AtomicLongArray written) throws Exception {
        Path path = new Path(partDir + "/" + fileOf(idx));
        Exception last = null;
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try {
                return writeOneFileOnce(fs, partDir, idx, from, to, written);
            } catch (Exception e) {
                last = e;
                written.set(idx, 0);
                System.out.printf("  %s  第 %d/%d 次失败：%s%n", fileOf(idx), attempt, MAX_ATTEMPTS, e);
                // 半截文件可能还挂在 NameNode 的租约里，先删一次；删不掉也无妨，
                // writeOneFileOnce 里的 overwrite(true) 会再覆盖一遍。
                try {
                    fs.delete(path, false);
                } catch (Exception ignored) {
                    // 无所谓，交给 overwrite(true)
                }
                if (attempt < MAX_ATTEMPTS) {
                    System.out.printf("  %s  等 %d 秒后重试…%n", fileOf(idx), 10 * attempt);
                    Thread.sleep(10_000L * attempt);
                }
            }
        }
        throw new IOException("文件 " + fileOf(idx) + " 重试 " + MAX_ATTEMPTS + " 次仍失败", last);
    }

    private static long[] writeOneFileOnce(FileSystem fs, String partDir,
                                           int idx, long from, long to,
                                           AtomicLongArray written) throws Exception {
        Path path = new Path(partDir + "/" + fileOf(idx));
        long begin = System.currentTimeMillis();

        // TypeDescription 非线程安全，每个 writer 单独建一份
        TypeDescription schema = TypeDescription.fromString(SCHEMA_TEXT);
        try (Writer writer = OrcFile.createWriter(path, OrcFile.writerOptions(fs.getConf())
                .setSchema(schema)
                .compress(CompressionKind.SNAPPY)
                .version(OrcFile.Version.V_0_12)
                .stripeSize(64L * 1024 * 1024)
                .overwrite(true))) {

            VectorizedRowBatch batch = schema.createRowBatch(BATCH_ROWS);
            LongColumnVector id = (LongColumnVector) batch.cols[0];
            BytesColumnVector guid = (BytesColumnVector) batch.cols[1];
            BytesColumnVector content = (BytesColumnVector) batch.cols[2];
            BytesColumnVector creationtime = (BytesColumnVector) batch.cols[3];
            LongColumnVector referenceid = (LongColumnVector) batch.cols[4];
            BytesColumnVector referenceimage = (BytesColumnVector) batch.cols[5];

            for (long n = from; n < to; n++) {
                int r = batch.size;
                long h = mix(n);
                id.vector[r] = n;
                setText(guid, r, uuidLike(n));
                setText(content, r, "item_" + Math.floorMod(h, 100_000) + "_" + hex16(h));
                setText(creationtime, r, TS.format(TS_BASE.minusSeconds(Math.floorMod(h, SECONDS_IN_YEAR))));
                referenceid.vector[r] = Math.floorMod(h >>> 17, 1_000_000);
                setText(referenceimage, r, "https://cdn.example.com/img/" + hex16(mix(n ^ 0x51ED270B5F2C1A3DL)) + ".jpg");

                batch.size = r + 1;
                if (batch.size == batch.getMaxSize()) {
                    writer.addRowBatch(batch);
                    batch.reset();
                }
                // 每 16384 行更新一次进度（位与代替取模，热路径上不花钱）。
                // 别把这个值放大 —— 写入是被 HDFS 上传带宽背压的，addRowBatch 一阻塞行数就停，
                // 阈值设成 100 万的话在慢链路上会有好几分钟一直显示 0%，看着像卡死。
                if (((n + 1) & 0x3FFFL) == 0) {
                    written.set(idx, n - from + 1);
                }
            }
            if (batch.size > 0) {
                writer.addRowBatch(batch);
            }
            written.set(idx, to - from);
        }

        long len = fs.getFileStatus(path).getLen();
        System.out.printf("  %s  %,12d 行  %8.1f MB  %.1f s%n",
                fileOf(idx), to - from, len / 1024.0 / 1024.0, (System.currentTimeMillis() - begin) / 1000.0);
        return new long[]{to - from, len};
    }

    private static String fileOf(int idx) {
        return String.format("%06d_0", idx);
    }

    private static void setText(BytesColumnVector v, int row, String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        v.setVal(row, b, 0, b.length);
    }

    /** splitmix64：行号 → 稳定的伪随机 64 位，保证造数可复现。 */
    private static long mix(long x) {
        long z = x + 0x9E3779B97F4A7C15L;
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }

    /** 64 位 → 定长 16 位十六进制。热路径上要跑 2 亿次，不用 {@code String.format}。 */
    private static String hex16(long v) {
        String s = Long.toHexString(v);
        return s.length() == 16 ? s : "0000000000000000".substring(s.length()) + s;
    }

    /**
     * 行号 → 形如 UUID 的 8-4-4-4-12 十六进制串。
     *
     * <p>不用 {@code UUID.randomUUID()}：它走 SecureRandom，方法是 synchronized 的，
     * 10 个线程各造 1000 万行时会变成一个串行点；而且那样数据就不可复现了。
     */
    private static String uuidLike(long n) {
        StringBuilder sb = new StringBuilder(36);
        appendHex16(sb, mix(n));
        appendHex16(sb, mix(n ^ 0x3C6EF372FE94F82BL));
        sb.insert(8, '-').insert(13, '-').insert(18, '-').insert(23, '-');
        return sb.toString();
    }

    private static void appendHex16(StringBuilder sb, long v) {
        String s = Long.toHexString(v);
        for (int i = s.length(); i < 16; i++) {
            sb.append('0');
        }
        sb.append(s);
    }

    // ==================== 落地自查（Hive 无关） ====================

    /**
     * 直接列 HDFS 目录 + 读每个 ORC 文件尾部的 NumberOfRows 求和。
     *
     * <p>ORC 的行数写在 **file footer** 里（postscript / footer.NumberOfRows），读一次文件尾就能拿到，
     * <b>不需要扫数据</b>，1 亿行也能秒回。这样不用 Hive 就能确认「到底写进去多少行」。
     */
    private static void hdfsSelfCheck(FileSystem fs, String partDir, long expected) throws IOException {
        System.out.println("\n=== 落地自查（读 ORC 文件尾，不经 Hive）===");
        FileStatus[] files = fs.listStatus(new Path(partDir),
                p -> !p.getName().startsWith("_") && !p.getName().startsWith("."));
        if (files.length == 0) {
            System.out.println("  !! 目录里没有任何数据文件：" + partDir);
            return;
        }
        long rows = 0;
        long bytes = 0;
        for (FileStatus st : files) {
            long r;
            try (Reader reader = OrcFile.createReader(st.getPath(),
                    OrcFile.readerOptions(fs.getConf()).filesystem(fs))) {
                r = reader.getNumberOfRows();
            }
            rows += r;
            bytes += st.getLen();
            System.out.printf("  %-16s %,12d 行  %8.1f MB%n",
                    st.getPath().getName(), r, st.getLen() / 1024.0 / 1024.0);
        }
        System.out.printf("  合计：%d 个文件 / %,d 行 / %.1f MB（期望 %,d 行）%s%n",
                files.length, rows, bytes / 1024.0 / 1024.0, expected, rows == expected ? "  ✔" : "  ✘ 不一致");
    }

    // ==================== 交给使用者在 Hive 里执行 ====================

    /** 打印注册/回滚命令 —— 造数程序本身不碰 Hive 元数据。 */
    private static void printHiveCommands(String partDir) {
        System.out.println("\n=== 数据已就绪，下面几条在 Hive 客户端里执行 ===");
        System.out.println("  ① 注册分区（二选一）：");
        System.out.println("     ALTER TABLE " + TABLE + " ADD IF NOT EXISTS PARTITION (version='"
                + PARTITION_VALUE + "') LOCATION '" + NAMENODE + partDir + "';");
        System.out.println("     MSCK REPAIR TABLE " + TABLE + ";     -- 等价，让 Hive 自己扫目录");
        System.out.println("  ② 核对行数：");
        System.out.println("     SELECT count(*) FROM " + TABLE + " WHERE version='" + PARTITION_VALUE + "';");
        System.out.println("  ③ 下次重跑 --clean 之前，先清元数据（managed 表会连数据目录一起删）：");
        System.out.println("     ALTER TABLE " + TABLE + " DROP PARTITION (version='" + PARTITION_VALUE + "');");
    }

    // ==================== Windows 前置条件 ====================

    /**
     * Windows 上跑 Hadoop FileSystem 的地基 —— Hadoop 3.3 的 {@code Shell.<clinit>} 在 Windows 下
     * 强制要求 {@code hadoop.home.dir}/{@code HADOOP_HOME} 指向一个含 {@code bin/winutils.exe} 的目录，
     * 否则任何 {@code FileSystem.get(...)}（**哪怕只是写 hdfs://**）都会直接抛
     * {@code FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset}。
     *
     * <p>这里：已配置且真带 {@code bin/winutils.exe} 就直接用；否则兜一个临时目录 + 空占位
     * {@code winutils.exe}（不往用户自己的 HADOOP_HOME 里写东西）。Hadoop 对该文件只做
     * 「存不存在」检查、不校验内容，而 HDFS 写入路径根本不会去执行它。若你后续还要做本地
     * 文件的权限/属主操作，请换成真正的 winutils.exe（版本对齐 Hadoop 3.3.x）。
     */
    private static void prepareHadoopHome() throws IOException {
        if (!System.getProperty("os.name", "").startsWith("Windows")) {
            return;
        }
        String home = System.getProperty("hadoop.home.dir");
        if (home == null || home.isBlank()) {
            home = System.getenv("HADOOP_HOME");
        }
        // 已配置并且真的带 bin/winutils.exe → 直接用用户那份（本地文件权限/属主操作会真正执行它）
        if (home != null && !home.isBlank() && Files.exists(Paths.get(home, "bin", "winutils.exe"))) {
            System.out.println("使用已配置的 hadoop.home.dir = " + home);
            return;
        }
        // 没配、或配了但目录里没有 winutils.exe → 兜一个临时 home + 空占位文件，
        // 不去往用户自己的 HADOOP_HOME 里写东西。
        java.nio.file.Path dir = Files.createTempDirectory("dojao-hadoop-home");
        java.nio.file.Path bin = Files.createDirectories(dir.resolve("bin"));
        home = dir.toAbsolutePath().toString();
        System.setProperty("hadoop.home.dir", home);
        Files.createFile(bin.resolve("winutils.exe"));
        System.out.println("HADOOP_HOME 未配置、或其中没有 bin/winutils.exe，已兜底 hadoop.home.dir = " + home);
        System.out.println("!! 该目录下放的是空占位 winutils.exe，只用于绕过 Hadoop 的「存在性」检查；");
        System.out.println("!! 写 hdfs:// 不会执行它，所以够用；要做本地文件的权限/属主操作请换成真正的 winutils.exe。");
    }
}
