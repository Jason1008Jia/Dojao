package com.dojao.ui;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * 电商埋点日志造数 —— 往 Hive 的 {@code dojao_test.ods_user_behavior} 里灌 1 亿条，
 * 用于和 MySQL {@code dga} 库做<b>跨源关联</b>。
 *
 * <p><b>只写 HDFS，不碰 Hive 元数据。</b>建表 + 注册分区由使用者在 Hive 客户端执行
 * （跑完会把 DDL 和命令再打一遍）。
 *
 * <h2>场景</h2>
 * dga 库里本来就有这条线：{@code users}(50万) / {@code products}(10万) / {@code categories}(100)
 * 是业务实体，{@code user_actions}(800万) 是行为日志。这张 Hive 表就是 {@code user_actions}
 * 的<b>全量历史版</b>（放大 12.5 倍）—— 业务实体留在 MySQL，海量明细下沉数仓。
 *
 * <pre>
 *   CREATE EXTERNAL TABLE dojao_test.ods_user_behavior (
 *     event_id bigint, user_id bigint, product_id bigint, category_id int,
 *     session_id string, event_type string, event_time string, device_type string,
 *     city string, duration_seconds int, quantity int, amount double
 *   ) PARTITIONED BY (dt string) STORED AS ORC;
 * </pre>
 *
 * <p>用 <b>EXTERNAL</b>：{@code DROP PARTITION} 只删元数据不动文件。managed 表一个 DROP
 * 就把几小时传上去的 GB 级数据清空了（老 {@code HiveTest} 的注释里已经踩过）。
 *
 * <h2>与 dga 的真实关联（关键）</h2>
 * 启动时<b>只读</b>地连一次 MySQL，把真实维表拉进内存数组，而不是各编一套假值：
 * <ul>
 *   <li>{@code products(id, category_id, price, sold_count)} → {@code category_id} 与真实的
 *       {@code products.category_id} 一致；{@code amount} 用的就是真实单价 × 件数，
 *       所以 {@code SUM(amount)} 能与 {@code JOIN dga.products} 算出来的结果互相校验。</li>
 *   <li>{@code users(id, city)} → {@code city} 与该用户真实的 {@code users.city} 一致。</li>
 *   <li>商品的「爆款」按真实的 {@code sold_count} 排序取头部，热度分布不与业务数据打架。</li>
 * </ul>
 * 走纯 JDBC 只读，<b>不经 Dojao 的 MetaStore</b> —— 那里 {@code registerConnector} 是无条件落库的，
 * 一个造数工具不该去动元数据。
 *
 * <h2>数据分布</h2>
 * 不是均匀随机，而是照着真实埋点长：
 * <ol>
 *   <li><b>用户三段式重尾</b>：40% 的行为来自头部 0.2% 的用户，30% 来自次热的 1.8%，剩 30% 铺满长尾。
 *       这样 join 出来有真实倾斜（测引擎的必备素材），又不是纯幂律那种「第一名吃掉 38%」的病态分布。</li>
 *   <li><b>80% 的行为落在该用户的「偏好品类」里</b>（由 user_id 派生），20% 是全站爆款/随机浏览。
 *       没有这一条，按 category 聚合就是一团噪声。</li>
 *   <li><b>漏斗比例</b> view 72% / click 13% / add_cart 6% / favorite 4% / order 3% / pay 1.7% / refund 0.3%。</li>
 *   <li><b>度量只在对应事件非 0</b>：{@code quantity} 仅 add_cart 及以后，{@code amount} 仅
 *       order/pay/refund。防止「SUM(amount) 把浏览也算进去」这种一眼假的数据。</li>
 *   <li><b>refund 的 amount 是负数</b>，所以 {@code SUM(amount)} 天然是净额口径。</li>
 *   <li><b>时间</b>：30 天日均增长 ~1%（{@code GROUP BY dt} 看得出趋势）；一天内按作息加权，
 *       晚高峰 19–22 点占大头。文件内时间<b>近似有序</b>（分位映射 + 几十秒抖动），
 *       既真实又能让 SNAPPY 压得动。</li>
 *   <li><b>session_id 按 30 分钟会话窗口切</b>（用户 id + 时间桶），同一次会话内的事件共享，
 *       这样才能做会话内漏斗/路径分析。</li>
 * </ol>
 *
 * <h2>用法</h2>
 * <pre>
 *   （无参）                    1 亿行 / 30 天 / 每天 1 个 ORC 文件
 *   --rows=3000000 --days=30   小样冒烟
 *   --clean                    先删 HDFS 上的表目录再重写
 *   --dry-run                  只打印计划，不连任何服务
 * </pre>
 *
 * <p>写文件时被 HDFS 上传带宽背压（生成本身有百万行/秒量级），所以墙上时间基本等于
 * 这批数据走公网传到集群的时间；中途断线会删掉半截文件重试（数据由行号确定性推出，重写结果一致）。
 * 运行前确认安全组已放开 {@code aliyun001:9000}（HDFS RPC）与 {@code 9866}（DataNode 数据传输）。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/19
 */
public class HiveBehaviorGen {

    // ==================== 目标表 / HDFS ====================
    private static final String TABLE = "dojao_test.ods_user_behavior";
    private static final String NAMENODE = "hdfs://aliyun001:9000";
    private static final String TABLE_DIR = "/user/hive/warehouse/dojao_test.db/ods_user_behavior";
    private static final String HDFS_USER = "root";

    // ==================== dga 源库（只读，保证关联列与真实业务一致） ====================
    private static final String MYSQL_URL =
            "jdbc:mysql://127.0.0.1:3306/dga?useSSL=false&allowPublicKeyRetrieval=true";
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASSWORD = "123456";

    // ==================== 规模 ====================
    private static final long DEFAULT_ROWS = 100_000_000L;
    private static final int DEFAULT_DAYS = 30;
    /** 最后一个分区是哪一天（往前推 days-1 天就是起始分区）。 */
    private static final LocalDate LAST_DAY = LocalDate.of(2026, 9, 19);

    private static final int BATCH_ROWS = 8 * 1024;
    private static final int MAX_ATTEMPTS = 5;

    /** 数据列（不含分区列 dt —— 分区列不落在文件里）。顺序必须与建表语句一致。 */
    private static final String SCHEMA_TEXT =
            "struct<event_id:bigint,user_id:bigint,product_id:bigint,category_id:int,"
                    + "session_id:string,event_type:string,event_time:string,device_type:string,"
                    + "city:string,duration_seconds:int,quantity:int,amount:double>";

    // ==================== 行为/设备分布 ====================
    private static final String[] EVENT_TYPES =
            {"view", "click", "favorite", "add_cart", "order", "pay", "refund"};
    /** 千分比，合计 1000。 */
    private static final int[] EVENT_WEIGHTS = {720, 130, 40, 60, 30, 17, 3};
    /** 下标常量，用来判断「这个事件有没有件数/金额」。 */
    private static final int EV_VIEW = 0, EV_ADD_CART = 3, EV_ORDER = 4, EV_REFUND = 6;

    private static final String[] DEVICES = {"android", "ios", "web", "pc"};
    private static final int[] DEVICE_WEIGHTS = {400, 300, 200, 100};

    /** 件数分布：绝大多数买 1 件，偶尔多件。 */
    private static final int[] QUANTITY_LUT = {1, 1, 1, 1, 2, 2, 3, 4, 5};

    /** 一天 24 小时的相对权重（凌晨低谷、晚高峰）。 */
    private static final int[] HOUR_WEIGHTS = {
            2, 1, 1, 1, 1, 2, 4, 8, 14, 18, 20, 22,
            22, 20, 22, 26, 30, 34, 40, 46, 44, 36, 20, 8};

    private static final int[] EVENT_LUT = weightedLut(EVENT_WEIGHTS);
    private static final int[] DEVICE_LUT = weightedLut(DEVICE_WEIGHTS);
    /** 均匀分位（0..86399）→ 按作息加权后的一天内秒数。建一次，之后每行一次数组下标。 */
    private static final int[] SEC_OF_DAY = buildSecOfDay();
    /** 一天内每秒的 "HH:mm:ss"，年月日在外面拼（建一次就够，格式与日期无关）。 */
    private static final String[] HMS = buildHms();

    /** 会话超时：30 分钟内的同一用户事件算同一次会话。 */
    private static final long SESSION_WINDOW_SEC = 1800;

    // ==================== dga 维表（启动时灌进来） ====================
    /** products.id → category_id。id 在 [1, PROD_MAX] 上连续，直接按下标存，热路径零查找。 */
    private static int[] PROD_CATEGORY;
    /** products.id → price。 */
    private static double[] PROD_PRICE;
    /** products.id 按真实 sold_count 降序 —— 头部就是「真爆款」。 */
    private static int[] HOT_PROD;
    private static int PROD_MAX;
    /** 商品品类枚举；PRODS_BY_CAT[i] = 该品类下的商品 id 列表。 */
    private static int[] CATEGORIES;
    private static int[][] PRODS_BY_CAT;

    /** users.id → city 在 CITY_NAMES 里的下标。同样按下标存。 */
    private static short[] USER_CITY;
    private static String[] CITY_NAMES;
    private static int USER_MAX;

    // ==================== 用户热度的三段切分（比例） ====================
    private static int userHead, userMid, userTail;
    private static int hotHeadCount;

    public static void main(String[] args) throws Exception {
        long rows = DEFAULT_ROWS;
        int days = DEFAULT_DAYS;
        boolean clean = false;
        boolean dryRun = false;
        for (String a : args) {
            if ("--dry-run".equals(a)) {
                dryRun = true;
            } else if ("--clean".equals(a)) {
                clean = true;
            } else if (a.startsWith("--rows=")) {
                rows = Long.parseLong(a.substring("--rows=".length()).trim());
            } else if (a.startsWith("--days=")) {
                days = Integer.parseInt(a.substring("--days=".length()).trim());
            } else {
                System.out.println("未知参数: " + a + "（可用：--rows= --days= --clean --dry-run）");
                return;
            }
        }
        if (rows <= 0 || days <= 0) {
            System.out.println("--rows / --days 必须为正数");
            return;
        }

        long[] counts = planDays(rows, days);
        LocalDate firstDay = LAST_DAY.minusDays(days - 1L);

        System.out.println("目标表   : " + TABLE + "   PARTITIONED BY (dt string)");
        System.out.println("HDFS     : " + NAMENODE + TABLE_DIR);
        System.out.printf("规模     : %,d 行 / %d 个分区（每天的 1 个 ORC 文件）≈ %,.1f GB%n",
                rows, days, rows * 94L / 1073741824.0);
        System.out.printf("分区     : dt=%s ~ dt=%s，每天 %,d ~ %,d 行（日均 +1%%，看得出增长趋势）%n",
                firstDay, LAST_DAY, counts[0], counts[days - 1]);
        System.out.println("说明     : 生成不是瓶颈，耗时基本等于这批数据走公网传到集群的时间");
        if (dryRun) {
            for (int d = 0; d < days; d++) {
                System.out.printf("  dt=%s → %,d 行%n", firstDay.plusDays(d), counts[d]);
            }
            return;
        }

        // ==================== 拉 dga 维表（只读） ====================
        System.out.println("\n=== 读取 dga 维表（只读，保证关联列与真实业务一致）===");
        loadDgaReference();

        System.setProperty("HADOOP_USER_NAME", HDFS_USER);
        prepareHadoopHome();
        FileSystem fs = FileSystem.get(hdfsConf());

        if (clean) {
            System.out.println("\n--clean：删除 HDFS 目录 " + TABLE_DIR);
            System.out.println("  注意：表是 EXTERNAL 的，删目录不会动元数据；重写完请用");
            System.out.println("  MSCK REPAIR TABLE " + TABLE + " SYNC PARTITIONS; 把登记表也对齐。");
            fs.delete(new Path(TABLE_DIR), true);
        }
        fs.mkdirs(new Path(TABLE_DIR));

        // ==================== 并行写 ORC ====================
        System.out.printf("\n=== 开始写 %d 个分区的 ORC 文件 ===%n", days);
        long begin = System.currentTimeMillis();
        AtomicLongArray written = new AtomicLongArray(days);
        Thread reporter = startProgressReporter(written, days, rows, begin);

        ExecutorService pool = Executors.newFixedThreadPool(days);
        List<Future<long[]>> futures = new ArrayList<>();
        long from = 0;
        for (int d = 0; d < days; d++) {
            LocalDate day = firstDay.plusDays(d);
            long cnt = counts[d];
            long base = from;
            int idx = d;
            futures.add(pool.submit(() -> writeOneDay(fs, idx, day.toString(), base, cnt, day, written)));
            from += cnt;
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
                System.out.printf("  !! 分区 %s 最终失败：%s%n",
                        firstDay.plusDays(i), e.getCause() == null ? e : e.getCause());
            }
        }
        reporter.interrupt();

        if (failed > 0) {
            System.out.printf("%n!! 有 %d 个分区没能写完（已重试 %d 次）。%n", failed, MAX_ATTEMPTS);
            System.out.println("   多半还是公网链路被掐（NameNode RPC 被 RST → 租约过期 → 文件被删）。");
            System.out.println("   确认安全组 9000 / 9866 通畅后重跑；重跑请带上 --clean。");
            return;
        }
        System.out.printf("写入完成：%,d 行 / %.1f GB / 耗时 %.1f 秒%n",
                totalRows, totalBytes / 1073741824.0, (System.currentTimeMillis() - begin) / 1000.0);

        try {
            hdfsSelfCheck(fs, rows);
        } catch (Exception e) {
            System.out.println("\n(自查跳过：读 HDFS 目录/ORC 文件尾失败 " + e + ")");
        }

        printHiveSql(firstDay, LAST_DAY);
    }

    // ==================== dga 维表读取 ====================

    /**
     * 从 dga 只读拉三样东西进内存。
     *
     * <p>products / users 的 id 都在 [1, max] 上连续（各 10 万 / 50 万行），所以直接按下标存数组，
     * 而不是 HashMap —— 生成 1 亿行时每行都要查，数组下标和数据结构的差距是数量级的。
     */
    private static void loadDgaReference() throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        try (Connection c = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD)) {

            // ---------- products ----------
            PROD_MAX = queryInt(c, "select max(id) from products");
            if (PROD_MAX <= 0) {
                throw new IllegalStateException("dga.products 是空表");
            }
            PROD_CATEGORY = new int[PROD_MAX + 1];
            PROD_PRICE = new double[PROD_MAX + 1];
            int[] sold = new int[PROD_MAX + 1];
            try (Statement s = c.createStatement();
                 ResultSet rs = s.executeQuery("select id, category_id, price, sold_count from products")) {
                while (rs.next()) {
                    int id = rs.getInt(1);
                    PROD_CATEGORY[id] = rs.getInt(2);
                    PROD_PRICE[id] = rs.getDouble(3);
                    sold[id] = rs.getInt(4);
                }
            }

            // 真校验：每个商品都得有品类。别拿「各品类商品数之和 == 商品总数」当校验 ——
            // 被归进「0 号品类」的商品也算在里面，那个条件恒成立，是个假校验。
            int missingCat = 0;
            for (int id = 1; id <= PROD_MAX; id++) {
                if (PROD_CATEGORY[id] == 0) {
                    missingCat++;
                }
            }
            if (missingCat > 0) {
                throw new IllegalStateException("dga.products 有 " + missingCat + " 行的 category_id 为空，无法按品类生成");
            }

            // 按真实 sold_count 降序 → 头部就是真爆款，热度分布不与业务数据打架
            Integer[] order = new Integer[PROD_MAX];
            for (int i = 0; i < PROD_MAX; i++) {
                order[i] = i + 1;
            }
            Arrays.sort(order, (a, b) -> Integer.compare(sold[b], sold[a]));
            HOT_PROD = new int[PROD_MAX];
            for (int i = 0; i < PROD_MAX; i++) {
                HOT_PROD[i] = order[i];
            }

            // 品类 → 商品列表：用来按「用户偏好品类」挑商品
            Map<Integer, List<Integer>> byCat = new LinkedHashMap<>();
            for (int id = 1; id <= PROD_MAX; id++) {
                byCat.computeIfAbsent(PROD_CATEGORY[id], k -> new ArrayList<>()).add(id);
            }
            CATEGORIES = new int[byCat.size()];
            PRODS_BY_CAT = new int[byCat.size()][];
            int ci = 0;
            for (Map.Entry<Integer, List<Integer>> e : byCat.entrySet()) {
                CATEGORIES[ci] = e.getKey();
                PRODS_BY_CAT[ci] = e.getValue().stream().mapToInt(Integer::intValue).toArray();
                ci++;
            }

            // ---------- users ----------
            USER_MAX = queryInt(c, "select max(id) from users");
            if (USER_MAX <= 0) {
                throw new IllegalStateException("dga.users 是空表");
            }
            USER_CITY = new short[USER_MAX + 1];
            Map<String, Integer> cityIdx = new LinkedHashMap<>();
            try (Statement s = c.createStatement();
                 ResultSet rs = s.executeQuery("select id, city from users")) {
                while (rs.next()) {
                    String city = rs.getString(2);
                    if (city == null || city.isBlank()) {
                        city = "";
                    }
                    Integer idx = cityIdx.get(city);
                    if (idx == null) {
                        idx = cityIdx.size();
                        cityIdx.put(city, idx);
                    }
                    USER_CITY[rs.getInt(1)] = (short) (int) idx;
                }
            }
            CITY_NAMES = cityIdx.keySet().toArray(new String[0]);

            // 按品类 id 排序，让下面日志打出来的范围是真的（上面按商品 id 首次出现的顺序建的，不是有序的）
            Integer[] catOrder = new Integer[CATEGORIES.length];
            for (int i = 0; i < catOrder.length; i++) {
                catOrder[i] = i;
            }
            Arrays.sort(catOrder, (a, b) -> Integer.compare(CATEGORIES[a], CATEGORIES[b]));
            int[] sortedCats = new int[CATEGORIES.length];
            int[][] sortedProds = new int[CATEGORIES.length][];
            for (int i = 0; i < catOrder.length; i++) {
                sortedCats[i] = CATEGORIES[catOrder[i]];
                sortedProds[i] = PRODS_BY_CAT[catOrder[i]];
            }
            CATEGORIES = sortedCats;
            PRODS_BY_CAT = sortedProds;

            // 热度的三段切分：头部 0.2% / 次热 1.8% / 长尾 98%
            userHead = Math.max(1, USER_MAX / 500);
            userMid = Math.max(1, USER_MAX * 18 / 1000);
            userTail = Math.max(1, USER_MAX - userHead - userMid);
            hotHeadCount = Math.max(1, PROD_MAX / 500);

            System.out.printf("  products %,d 行 → %,d 个品类（%d..%d），热销榜头部取 %,d 个%n",
                    PROD_MAX, CATEGORIES.length, CATEGORIES[0], CATEGORIES[CATEGORIES.length - 1], hotHeadCount);
            System.out.printf("  users    %,d 行 → %d 个城市，热度切分 头部 %,d / 次热 %,d / 长尾 %,d%n",
                    USER_MAX, CITY_NAMES.length, userHead, userMid, userTail);
        }
    }

    private static int queryInt(Connection c, String sql) throws Exception {
        try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            if (!rs.next()) {
                throw new IllegalStateException("查询无结果: " + sql);
            }
            return rs.getInt(1);
        }
    }

    // ==================== 分区行数规划 ====================

    /** 按「日均 +1%」把总行数摊到各天，最后一天吃掉取整误差，保证总数精确。 */
    private static long[] planDays(long rows, int days) {
        double[] w = new double[days];
        double sum = 0;
        for (int d = 0; d < days; d++) {
            w[d] = 1.0 + 0.01 * d;
            sum += w[d];
        }
        long[] counts = new long[days];
        long acc = 0;
        for (int d = 0; d < days; d++) {
            counts[d] = Math.round(rows * w[d] / sum);
            acc += counts[d];
        }
        counts[days - 1] += rows - acc;
        return counts;
    }

    // ==================== Hadoop 客户端配置 ====================

    /** HDFS 客户端配置：跨公网的写要大半天，容错参数比默认值放宽一截。 */
    private static Configuration hdfsConf() {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", NAMENODE);
        // NameNode 找得到、DataNode 连不上时打开这个开关：集群在 VPC 内时 NameNode 下发的是内网 IP，
        // VPC 外根本没路由。打开后改成下发主机名，本机用 hosts 解析成公网 IP。
        conf.setBoolean("dfs.client.use.datanode.hostname", true);

        // ---- 闲置长连接保活（最关键的一条）----
        // 写 DataNode 期间 NameNode 的 RPC 连接是空闲的，默认 60s 才 ping 一次，
        // 中途的 NAT / 防火墙常在 30s 就把这条空闲连接 RST 掉 → renewLease 失败 → 租约过期 → 文件被删。
        conf.setInt("ipc.ping.interval", 20_000);

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
     * <p>带上<b>行/秒</b>是因为：时间几乎全花在上传带宽上，链路一抖看着就像「纹丝不动」。
     * 有速率就能一眼分清「慢」和「卡死」—— 速率掉到 0 且持续几轮才是真出问题了。
     */
    private static Thread startProgressReporter(AtomicLongArray written, int days, long rows, long begin) {
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
                for (int i = 0; i < days; i++) {
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

    private static Path fileOf(String dt) {
        return new Path(TABLE_DIR + "/dt=" + dt + "/000000_0");
    }

    /** 写某一天的分区文件，失败会删掉半截文件重试（数据由行号确定性推出，重写结果一致）。 */
    private static long[] writeOneDay(FileSystem fs, int idx, String dt, long from, long cnt,
                                      LocalDate day, AtomicLongArray written) throws Exception {
        Exception last = null;
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try {
                return writeOneDayOnce(fs, idx, dt, from, cnt, day, written);
            } catch (Exception e) {
                last = e;
                written.set(idx, 0);
                System.out.printf("  dt=%s  第 %d/%d 次失败：%s%n", dt, attempt, MAX_ATTEMPTS, e);
                try {
                    fs.delete(fileOf(dt), false);
                } catch (Exception ignored) {
                    // 无所谓，writeOneDayOnce 里的 overwrite(true) 会再覆盖一遍
                }
                if (attempt < MAX_ATTEMPTS) {
                    System.out.printf("  dt=%s  等 %d 秒后重试…%n", dt, 10 * attempt);
                    Thread.sleep(10_000L * attempt);
                }
            }
        }
        throw new IOException("分区 dt=" + dt + " 重试 " + MAX_ATTEMPTS + " 次仍失败", last);
    }

    private static long[] writeOneDayOnce(FileSystem fs, int idx, String dt, long from, long cnt,
                                          LocalDate day, AtomicLongArray written) throws Exception {
        long begin = System.currentTimeMillis();

        // TypeDescription 非线程安全，每个 writer 单独建一份
        TypeDescription schema = TypeDescription.fromString(SCHEMA_TEXT);
        try (Writer writer = OrcFile.createWriter(fileOf(dt), OrcFile.writerOptions(fs.getConf())
                .setSchema(schema)
                .compress(CompressionKind.SNAPPY)
                .version(OrcFile.Version.V_0_12)
                .stripeSize(64L * 1024 * 1024)
                .overwrite(true))) {

            VectorizedRowBatch batch = schema.createRowBatch(BATCH_ROWS);
            LongColumnVector cEventId = (LongColumnVector) batch.cols[0];
            LongColumnVector cUserId = (LongColumnVector) batch.cols[1];
            LongColumnVector cProductId = (LongColumnVector) batch.cols[2];
            LongColumnVector cCategoryId = (LongColumnVector) batch.cols[3];
            BytesColumnVector cSessionId = (BytesColumnVector) batch.cols[4];
            BytesColumnVector cEventType = (BytesColumnVector) batch.cols[5];
            BytesColumnVector cEventTime = (BytesColumnVector) batch.cols[6];
            BytesColumnVector cDevice = (BytesColumnVector) batch.cols[7];
            BytesColumnVector cCity = (BytesColumnVector) batch.cols[8];
            LongColumnVector cDuration = (LongColumnVector) batch.cols[9];
            LongColumnVector cQuantity = (LongColumnVector) batch.cols[10];
            DoubleColumnVector cAmount = (DoubleColumnVector) batch.cols[11];

            String[] tsOfDay = buildTimestamps(day);
            long epochBase = day.atStartOfDay().toEpochSecond(ZoneOffset.UTC);

            for (long k = 0; k < cnt; k++) {
                int r = batch.size;
                long n = from + k;

                long h1 = mix(n);
                long h2 = mix(n ^ 0x9E3779B97F4A7C15L);
                long h3 = mix(n ^ 0xC2B2AE3D27D4EB4FL);
                long rnd = h3 & 0x7FFFFFFFFFFFL;

                // 注意：hash 是有符号的 long，取模必须用 floorMod。用 `%` 的话负 hash 会得到负数，
                // 拿去做数组下标就是越界（这个坑真的踩到了：EVENT_LUT[-81] 在跑到一半时炸）。
                // ---- 用户：三段式重尾。40% 的行为集中在头部 0.2% 的用户身上 ----
                double u = (h1 >>> 11) * 0x1.0p-53;
                int userId;
                if (u < 0.40) {
                    userId = 1 + Math.floorMod(rnd, userHead);
                } else if (u < 0.70) {
                    userId = 1 + userHead + Math.floorMod(rnd, userMid);
                } else {
                    userId = 1 + userHead + userMid + Math.floorMod(rnd, userTail);
                }

                // ---- 商品：80% 落在该用户的「偏好品类」里，20% 是全站爆款/随机浏览 ----
                double u2 = (h2 >>> 11) * 0x1.0p-53;
                int productId;
                if (u2 < 0.80) {
                    int[] pool = PRODS_BY_CAT[Math.floorMod(userId, CATEGORIES.length)];
                    productId = pool[Math.floorMod(rnd, pool.length)];
                } else if (u2 < 0.92) {
                    productId = HOT_PROD[Math.floorMod(rnd, hotHeadCount)];
                } else {
                    productId = 1 + Math.floorMod(rnd, PROD_MAX);
                }
                int categoryId = PROD_CATEGORY[productId];

                // ---- 事件与设备 ----
                int ei = EVENT_LUT[Math.floorMod(h1, 1000)];
                int di = DEVICE_LUT[Math.floorMod(h2, 1000)];

                // ---- 时间：按作息加权落在当天的某一秒，再抖 ±20 秒（近似有序，压得动）----
                int sec = SEC_OF_DAY[(int) ((k * 86400L) / cnt)];
                sec += (int) ((h1 >>> 24) % 41) - 20;
                if (sec < 0) {
                    sec = 0;
                } else if (sec > 86399) {
                    sec = 86399;
                }

                // ---- 度量：只在对应事件非 0 ----
                long duration = 0;
                if (ei == EV_VIEW) {
                    int v = Math.floorMod(h1, 1000);
                    duration = 1 + v * v / 1000 * 6 / 10;   // 平方分布 → 多数很短，少数很长
                }
                long quantity = 0;
                double amount = 0;
                if (ei >= EV_ADD_CART) {
                    quantity = QUANTITY_LUT[Math.floorMod(h2, QUANTITY_LUT.length)];
                    if (ei >= EV_ORDER) {
                        // 用真实单价 × 件数 → SUM(amount) 能与 JOIN dga.products 的结果对上
                        amount = PROD_PRICE[productId] * quantity;
                        if (ei == EV_REFUND) {
                            amount = -amount;   // 退款为负，SUM 天然是净额口径
                        }
                    }
                }

                cEventId.vector[r] = n + 1;
                cUserId.vector[r] = userId;
                cProductId.vector[r] = productId;
                cCategoryId.vector[r] = categoryId;
                setText(cSessionId, r, Long.toHexString(userId) + '-'
                        + ((epochBase + sec) / SESSION_WINDOW_SEC));
                setText(cEventType, r, EVENT_TYPES[ei]);
                setText(cEventTime, r, tsOfDay[sec]);
                setText(cDevice, r, DEVICES[di]);
                setText(cCity, r, CITY_NAMES[USER_CITY[userId]]);
                cDuration.vector[r] = duration;
                cQuantity.vector[r] = quantity;
                cAmount.vector[r] = amount;

                batch.size = r + 1;
                if (batch.size == batch.getMaxSize()) {
                    writer.addRowBatch(batch);
                    batch.reset();
                }
                // 每 16384 行更新一次进度。别把这个值放大 —— 写入被 HDFS 上传带宽背压，
                // addRowBatch 一阻塞行数就停，阈值设太大在慢链路上会一直显示 0%，看着像卡死。
                if (((k + 1) & 0x3FFFL) == 0) {
                    written.set(idx, k + 1);
                }
            }
            if (batch.size > 0) {
                writer.addRowBatch(batch);
            }
            written.set(idx, cnt);
        }

        long len = fs.getFileStatus(fileOf(dt)).getLen();
        System.out.printf("  dt=%s  %,11d 行  %8.1f MB  %.1f s%n",
                dt, cnt, len / 1024.0 / 1024.0, (System.currentTimeMillis() - begin) / 1000.0);
        return new long[]{cnt, len};
    }

    private static void setText(BytesColumnVector v, int row, String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        v.setVal(row, b, 0, b.length);
    }

    /** 该天 86400 秒的完整 "yyyy-MM-dd HH:mm:ss" —— 建一次，热路径上就是一次数组下标。 */
    private static String[] buildTimestamps(LocalDate day) {
        String[] a = new String[86400];
        String prefix = day + " ";
        for (int s = 0; s < 86400; s++) {
            a[s] = prefix + HMS[s];
        }
        return a;
    }

    private static String[] buildHms() {
        String[] a = new String[86400];
        DateTimeFormatter f = DateTimeFormatter.ofPattern("HH:mm:ss");
        LocalDateTime midnight = LocalDateTime.of(2000, 1, 1, 0, 0, 0);
        for (int s = 0; s < 86400; s++) {
            a[s] = f.format(midnight.plusSeconds(s));
        }
        return a;
    }

    /** 权重表 → 1000 格的查表，热路径上就是一次数组下标。 */
    private static int[] weightedLut(int[] weights) {
        int total = 0;
        for (int w : weights) {
            total += w;
        }
        int[] lut = new int[1000];
        int pos = 0;
        for (int i = 0; i < weights.length && pos < lut.length; i++) {
            int n = (i == weights.length - 1)
                    ? (lut.length - pos)
                    : (int) Math.round((double) lut.length * weights[i] / total);
            n = Math.min(n, lut.length - pos);
            for (int j = 0; j < n; j++) {
                lut[pos++] = i;
            }
        }
        return lut;
    }

    /**
     * 均匀分位（0..86399）→ 按作息加权后的一天内秒数，严格非递减。
     *
     * <p>用它而不是均匀随机：文件里的 event_time 会近似有序，SNAPPY 才压得动
     * （19 字节的时间串乱序时基本压不动）。最后一小时吃掉取整余数，保证正好铺满 86400。
     */
    private static int[] buildSecOfDay() {
        int[] lut = new int[86400];
        int total = 0;
        for (int w : HOUR_WEIGHTS) {
            total += w;
        }
        int pos = 0;
        for (int h = 0; h < 24; h++) {
            int n = (h == 23) ? (86400 - pos) : (int) Math.round(86400.0 * HOUR_WEIGHTS[h] / total);
            n = Math.min(n, 86400 - pos);
            for (int i = 0; i < n; i++) {
                lut[pos++] = h * 3600 + (int) (3600L * i / n);
            }
        }
        return lut;
    }

    /** splitmix64：行号 → 稳定的伪随机 64 位，保证造数可复现（同样的行号永远是同一份内容）。 */
    private static long mix(long x) {
        long z = x + 0x9E3779B97F4A7C15L;
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }

    // ==================== 落地自查（Hive 无关） ====================

    /**
     * 列 HDFS 分区目录 + 读每个 ORC 文件尾部的 NumberOfRows 求和。
     *
     * <p>行数写在 <b>file footer</b> 里，读一次文件尾就能拿到，<b>不扫数据</b>，1 亿行也秒回。
     * 这样不用 Hive 就能确认到底写进去多少行。
     */
    private static void hdfsSelfCheck(FileSystem fs, long expected) throws IOException {
        System.out.println("\n=== 落地自查（读 ORC 文件尾，不经 Hive）===");
        FileStatus[] parts = fs.listStatus(new Path(TABLE_DIR), p -> p.getName().startsWith("dt="));
        if (parts.length == 0) {
            System.out.println("  !! 表目录下没有任何分区：" + TABLE_DIR);
            return;
        }
        Arrays.sort(parts, (a, b) -> a.getPath().getName().compareTo(b.getPath().getName()));
        long rows = 0;
        long bytes = 0;
        for (FileStatus part : parts) {
            FileStatus[] files = fs.listStatus(part.getPath(),
                    p -> !p.getName().startsWith("_") && !p.getName().startsWith("."));
            long partRows = 0;
            long partBytes = 0;
            for (FileStatus st : files) {
                try (Reader reader = OrcFile.createReader(st.getPath(),
                        OrcFile.readerOptions(fs.getConf()).filesystem(fs))) {
                    partRows += reader.getNumberOfRows();
                }
                partBytes += st.getLen();
            }
            rows += partRows;
            bytes += partBytes;
            System.out.printf("  %-16s %d 个文件  %,11d 行  %8.1f MB%n",
                    part.getPath().getName(), files.length, partRows, partBytes / 1024.0 / 1024.0);
        }
        System.out.printf("  合计：%d 个分区 / %,d 行 / %.1f GB（期望 %,d 行）%s%n",
                parts.length, rows, bytes / 1073741824.0, expected, rows == expected ? "  ✔" : "  ✘ 不一致");
    }

    // ==================== 交给使用者在 Hive 里执行 ====================

    /** 打印建表 DDL 与注册/回滚命令 —— 造数程序本身不碰 Hive 元数据。 */
    private static void printHiveSql(LocalDate firstDay, LocalDate lastDay) {
        System.out.println("\n=== 数据已就绪，下面在 Hive 客户端里执行 ===");
        System.out.println("① 建表（EXTERNAL —— DROP PARTITION 只删元数据、不动文件）：");
        System.out.println("     CREATE DATABASE IF NOT EXISTS dojao_test;");
        System.out.println("     CREATE EXTERNAL TABLE IF NOT EXISTS " + TABLE + " (");
        System.out.println("       event_id         bigint  COMMENT '事件唯一 id',");
        System.out.println("       user_id          bigint  COMMENT '关联 dga.users.id',");
        System.out.println("       product_id       bigint  COMMENT '关联 dga.products.id',");
        System.out.println("       category_id      int     COMMENT '关联 dga.categories.id，与 products 一致',");
        System.out.println("       session_id       string  COMMENT '会话 id，30 分钟窗口',");
        System.out.println("       event_type       string  COMMENT 'view/click/favorite/add_cart/order/pay/refund',");
        System.out.println("       event_time       string  COMMENT 'yyyy-MM-dd HH:mm:ss',");
        System.out.println("       device_type      string  COMMENT 'android/ios/web/pc',");
        System.out.println("       city             string  COMMENT '与 dga.users.city 一致',");
        System.out.println("       duration_seconds int     COMMENT '停留秒数，仅 view 非 0',");
        System.out.println("       quantity         int     COMMENT '件数，仅 add_cart 及以后非 0',");
        System.out.println("       amount           double  COMMENT '金额，仅 order/pay/refund 非 0；refund 为负'");
        System.out.println("     )");
        System.out.println("     PARTITIONED BY (dt string COMMENT 'yyyy-MM-dd')");
        System.out.println("     STORED AS ORC");
        System.out.println("     LOCATION '" + NAMENODE + TABLE_DIR + "'");
        System.out.println("     TBLPROPERTIES ('orc.compress'='SNAPPY');");
        System.out.println("② 一次性注册全部 " + java.time.temporal.ChronoUnit.DAYS.between(firstDay, lastDay)
                + " 个分区（别去手敲 ALTER）：");
        System.out.println("     MSCK REPAIR TABLE " + TABLE + ";");
        System.out.println("③ 核对（每天行数应呈 ~1% 的递增）：");
        System.out.println("     SELECT dt, count(*) FROM " + TABLE + " GROUP BY dt ORDER BY dt;");
        System.out.println("④ 跨源关联自查（金额口径：明细里的 amount 应与 JOIN products 的结果一致）：");
        System.out.println("     SELECT count(*), round(sum(amount), 2) FROM " + TABLE
                + " WHERE event_type IN ('order','pay');");
        System.out.println("⑤ 重跑 --clean 之后，用这个把已消失的分区从元数据里同步掉（Hive 3+）：");
        System.out.println("     MSCK REPAIR TABLE " + TABLE + " SYNC PARTITIONS;");
    }

    // ==================== Windows 前置条件 ====================

    /**
     * Windows 上跑 Hadoop FileSystem 的地基 —— Hadoop 3.3 的 {@code Shell.<clinit>} 在 Windows 下
     * 强制要求 {@code hadoop.home.dir}/{@code HADOOP_HOME} 指向含 {@code bin/winutils.exe} 的目录，
     * 否则任何 {@code FileSystem.get(...)}（哪怕只是写 hdfs://）都会直接抛
     * {@code FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset}。
     *
     * <p>已配置且真带 {@code bin/winutils.exe} 就直接用；否则兜一个临时目录 + 空占位
     * {@code winutils.exe}（不往用户自己的 HADOOP_HOME 里写东西）。Hadoop 对该文件只做
     * 「存不存在」检查、不校验内容，而 HDFS 写入路径根本不会去执行它。
     */
    private static void prepareHadoopHome() throws IOException {
        if (!System.getProperty("os.name", "").startsWith("Windows")) {
            return;
        }
        String home = System.getProperty("hadoop.home.dir");
        if (home == null || home.isBlank()) {
            home = System.getenv("HADOOP_HOME");
        }
        if (home != null && !home.isBlank() && Files.exists(Paths.get(home, "bin", "winutils.exe"))) {
            System.out.println("使用已配置的 hadoop.home.dir = " + home);
            return;
        }
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
