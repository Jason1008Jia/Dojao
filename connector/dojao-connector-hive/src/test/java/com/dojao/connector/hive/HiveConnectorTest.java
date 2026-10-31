package com.dojao.connector.hive;

import com.dojao.common.data.Page;
import com.dojao.spi.bean.Column;
import com.dojao.spi.bean.ReadRequest;
import com.dojao.spi.bean.ScanSlice;
import com.dojao.spi.bean.TableStats;
import com.dojao.spi.service.ScanReader;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.hive.metastore.api.Partition;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Hive 连接器冒烟（main() 风格，IntelliJ 直接 Run）。
 *
 * <p>不碰 HiveServer2，只验证两件事：<b>HMS 探查</b>（库 / 表 / 列 / 统计）与
 * <b>直读 HDFS 文件</b>（ORC / Parquet / 文本都走同一入口，用户无感）。</p>
 *
 * <p><b>命令行参数</b>（有默认值，连别的集群时改这里）：</p>
 * <pre>
 *   --uris=thrift://aliyun002:9083     HMS Thrift 地址
 *   --fs=hdfs://aliyun001:9000         默认 FS
 *   --user=hive                        HADOOP_USER_NAME（HDFS 属主）
 *   --db=dojao_test                    要探查的库
 *   --table=ods_user_behavior          要探查的表
 *   --rows=1000                        读多少行做校验
 *   --type=hive3                       hive2 / hive3
 *   --compact=1M                       读取单元目标大小（默认取 dfs.blocksize；小文件合并与
 *                                      大文件切割共用，设小了好观察两个方向的效果）
 *   --split=1G                         超过它才做文件内切割（默认 1GB；设小了好观察切分）
 * </pre>
 *
 * <p><b>运行前必配的 VM options</b>：{@code --add-opens=java.base/java.nio=ALL-UNNAMED}
 * （Arrow 15 + JDK 21 必需，漏了会在首次分配缓冲时抛 InaccessibleObjectException）。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/19
 */
public final class HiveConnectorTest {

    private static final String CATALOG = "hive_smoke";

    public static void main(String[] args) {
        Map<String, String> opt = parseArgs(args);
        String uris = opt.getOrDefault("uris", "thrift://aliyun002:9083");
        String fs = opt.getOrDefault("fs", "hdfs://aliyun001:9000");
        String user = opt.getOrDefault("user", "hive");
        String db = opt.getOrDefault("db", "dojao_test");
        String table = opt.getOrDefault("table", "ods_user_behavior");
        int rows = Integer.parseInt(opt.getOrDefault("rows", "1000"));
        String type = opt.getOrDefault("type", "hive3");

        Map<String, String> props = new LinkedHashMap<>();
        props.put("hive.metastore.uris", uris);
        props.put("fs.defaultFS", fs);
        props.put("hadoop.user.name", user);
        // 单元目标大小：设了才覆盖 dfs.blocksize（设很小 → 每文件各自成单元 / 大文件切很碎；设很大 → 全并成一个单元）
        String compact = opt.get("compact");
        if (compact != null) props.put("hive.compact.target.bytes", compact);
        // 文件内切割触发阈值（默认 1GB）
        String split = opt.get("split");
        if (split != null) props.put("hive.split.file.min.bytes", split);

        HiveVersion version = "hive2".equalsIgnoreCase(type) ? HiveVersion.HIVE2 : HiveVersion.HIVE3;
        System.out.println("== 连接 HMS " + uris + "（" + version + "）==");
        System.out.println("   catalog=" + CATALOG + " fs=" + fs + " user=" + user);

        HiveConnector conn = new HiveConnector(version, CATALOG, props);
        System.out.println("   单元目标大小 = " + (compact == null ? "dfs.blocksize（未覆盖）" : compact));
        System.out.println("   文件内切割阈值 = " + (split == null ? "1GB（默认）" : split));
        try {
            step("1. catalog", () ->
                    System.out.println("  type    = " + conn.getCatalog(CATALOG).map(c -> c.type()).orElse("?")
                            + "\n  schemas = " + conn.listSchemas(CATALOG)));

            step("2. 表清单 " + db, () -> {
                List<String> tables = conn.listTables(CATALOG, db);
                System.out.println("  共 " + tables.size() + " 张表：" + sample(tables, 20));
            });

            step("3. 列（getTableColumns）", () -> {
                List<Column> cols = conn.getTableColumns(CATALOG, db, table);
                for (Column c : cols) {
                    System.out.println("    " + c.name() + " : " + c.dataType()
                            + (c.nullable() ? "" : " NOT NULL"));
                }
                System.out.println("  共 " + cols.size() + " 列");
            });

            step("4. Arrow 字段（sourceFields，CTAS 读侧类型）", () -> {
                for (Field f : conn.sourceFields(CATALOG, db, table, null)) {
                    System.out.println("    " + f.getName() + " -> " + f.getType());
                }
            });

            step("5. 统计（getTableStats：行数 / 字节 / 天然分区数）", () -> {
                TableStats stats = conn.getTableStats(CATALOG, db, table);
                System.out.println("  rowCount   = " + stats.rowCount());
                System.out.println("  byteSize   = " + stats.byteSize()
                        + (stats.byteSize() >= 0
                        ? String.format(" (%.2f MB)", stats.byteSize() / 1024.0 / 1024.0) : " (未知)"));
                System.out.println("  splitCount = " + stats.splitCount()
                        + "（读取单元数：小文件已合并、大文件已切开；"
                        + "引擎按 Min(MaxPara, Max(ScanA, ScanB, ...)) 定并行度）");
                System.out.println("  colStats   = " + stats.columnStats().size() + " 条");
            });

            step("6. 分片（planSplits -> 4 分片；小文件先合并、大文件先切段）", () -> {
                List<ScanSlice> parts = conn.planSplits(ReadRequest.of(CATALOG, db, table), 4);
                System.out.println("  分片数 = " + parts.size());
                for (int i = 0; i < parts.size(); i++) {
                    ScanSlice part = parts.get(i);
                    List<HiveSplitUnit> units = part.hasSplit()
                            ? HiveSplitPayload.decode(part.split().payload()) : List.of();
                    long ranged = units.stream().filter(HiveSplitUnit::isRange).count();
                    System.out.println("    #" + i + " 单元数=" + units.size()
                            + "（其中文件内区段 " + ranged + "）"
                            + " 首个=" + (units.isEmpty() ? "-" : units.get(0).describe()));
                }

                List<ScanSlice> prefix = conn.planSplits(ReadRequest.of(CATALOG, db, table), 4, 500L);
                int prefixFiles = prefix.isEmpty() || !prefix.get(0).hasSplit() ? -1
                        : HiveSplitPayload.decode(prefix.get(0).split().payload()).size();
                System.out.println("  截断下推（前 500 行）= " + prefix.size()
                        + " 片 / " + prefixFiles + " 个文件（应为 1 片、只列够前缀的文件）");

                step("7. 读前 " + rows + " 行（按分片 reader 遍历，格式无感）", () -> {
                    long total = 0;
                    int pages = 0;
                    List<String> sample = new ArrayList<>();
                    for (ScanSlice part : parts) {
                        ReadRequest req = part.request().withLimit(0, rows);
                        ScanReader reader = conn.openSplit(req, part.split());
                        try {
                            while (total < rows) {
                                Page page = reader.next();
                                if (page.isEOF()) {
                                    page.close();
                                    break;
                                }
                                if (!page.isEmpty()) {
                                    pages++;
                                    total += page.getRowCount();
                                    collect(page, sample, 3);
                                }
                                page.close();
                            }
                        } finally {
                            reader.close();
                        }
                        if (total >= rows) break;
                    }
                    System.out.println("  实际读 " + total + " 行 / " + pages + " 页");
                    System.out.println("  样例：");
                    sample.forEach(s -> System.out.println("    " + s));
                });
            });

            step("6b. 分区裁剪（带分区列谓词的 planSplits）", () -> {
                List<String> pks = conn.getPartitionColumns(CATALOG, db, table);
                System.out.println("  分区列 = " + (pks.isEmpty() ? "（无）" : pks));
                if (pks.isEmpty()) {
                    System.out.println("  非分区表 → 连接器不裁剪（符合契约）");
                    return;
                }
                String key = pks.get(0);
                List<Partition> exist = conn.getMetastore().partitions(db, table, 5);
                if (exist.isEmpty()) {
                    System.out.println("  HMS 未登记分区 → 回退扫表根目录，无分区可剪"
                            + "（Hive 侧执行 MSCK REPAIR TABLE 后重跑本步）");
                } else {
                    String real = exist.get(0).getValues().get(0);
                    List<ScanSlice> hit = conn.planSplits(
                            ReadRequest.of(CATALOG, db, table).withFilter(key + " = '" + real + "'"), 8);
                    System.out.println("  谓词 " + key + "='" + real + "'（HMS 里真实存在）→ " + hit.size()
                            + " 片（应远少于整表片数：只列该分区目录）");
                }
                List<ScanSlice> miss = conn.planSplits(
                        ReadRequest.of(CATALOG, db, table).withFilter(key + " = '__no_such_partition__'"), 8);
                System.out.println("  谓词 " + key + "='__no_such_partition__'（不存在）→ " + miss.size()
                        + " 片（应为 0 片：剪空 → 引擎回退单切片但读 0 行，且不该看到整表文件数）");
            });

            step("8. 投影裁剪（只读前 3 列）", () -> {
                List<Column> cols = conn.getTableColumns(CATALOG, db, table);
                List<String> pick = new ArrayList<>();
                for (int i = 0; i < Math.min(3, cols.size()); i++) pick.add(cols.get(i).name());
                conn.closeRead();
                Page page = conn.read(ReadRequest.of(CATALOG, db, table, pick).withLimit(0, 5));
                try {
                    if (page.isEOF()) {
                        System.out.println("  （空表）");
                    } else {
                        System.out.println("  列 = " + page.getRoot().getSchema().getFields()
                                .stream().map(Field::getName).toList());
                        System.out.println("  行 = " + page.getRowCount());
                    }
                } finally {
                    page.close();
                }
            });

            step("9. 列统计（getColumnStats）", () ->
                    conn.getColumnStats(CATALOG, db, table).forEach(cs ->
                            System.out.println("    " + cs.columnName()
                                    + " null=" + cs.nullCount() + " distinct=" + cs.distinctCount())));
        } finally {
            conn.closeRead();
            try {
                conn.getMetastore().close();
            } catch (Exception e) {
                System.out.println("[close] " + e.getMessage());
            }
        }
        System.out.println("\n== 冒烟结束 ==");
    }

    // ==================== 工具 ====================

    /** 每一步单独 try-catch：局部失败不阻断后续（探查阶段这样最好调）。 */
    private static void step(String title, ThrowingRunnable body) {
        System.out.println("\n-- " + title);
        try {
            body.run();
        } catch (Throwable t) {
            System.out.println("  X " + t.getClass().getSimpleName() + ": " + t.getMessage());
            t.printStackTrace(System.out);
        }
    }

    private static void collect(Page page, List<String> out, int max) {
        List<FieldVector> vectors = page.getRoot().getFieldVectors();
        int n = Math.min(max, page.getRowCount());
        for (int r = 0; r < n; r++) {
            StringBuilder sb = new StringBuilder();
            for (FieldVector v : vectors) {
                if (sb.length() > 0) sb.append(" | ");
                sb.append(v.getField().getName()).append('=')
                        .append(v.isNull(r) ? null : v.getObject(r));
            }
            out.add(sb.toString());
        }
    }

    private static List<String> sample(List<String> list, int n) {
        return list.size() <= n ? list : new ArrayList<>(list.subList(0, n));
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> map = new LinkedHashMap<>();
        for (String a : args) {
            if (!a.startsWith("--")) continue;
            int eq = a.indexOf('=');
            if (eq < 0) {
                map.put(a.substring(2), "true");
            } else {
                map.put(a.substring(2, eq), a.substring(eq + 1));
            }
        }
        return map;
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
