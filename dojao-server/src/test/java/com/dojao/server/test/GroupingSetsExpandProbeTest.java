package com.dojao.server.test;

import com.dojao.common.data.Page;
import com.dojao.core.operator.Operator;
import com.dojao.core.operator.ShuffleReader;
import com.dojao.core.planner.PhysicalPlan;
import com.dojao.core.planner.PlanningContext;
import com.dojao.core.planner.Planner;
import com.dojao.core.runtime.master.WorkerRegistry;
import com.dojao.core.runtime.worker.ResourceSummary;
import com.dojao.core.runtime.worker.WorkerGateway;
import com.dojao.core.runtime.worker.WorkerNode;
import com.dojao.core.runtime.worker.WorkerState;
import com.dojao.grammar.analyzer.plan.LogicalPlan;
import com.dojao.metastore.MetaStore;
import com.dojao.metastore.MetaStoreType;
import com.dojao.server.master.SqlValidator;
import com.dojao.server.worker.ExchangeManager;
import com.dojao.server.worker.MasterGateway;
import com.dojao.server.worker.WorkerManager;
import com.dojao.spi.bean.Catalog;
import com.dojao.spi.bean.ColumnStats;
import com.dojao.spi.bean.ReadRequest;
import com.dojao.spi.bean.Schema;
import com.dojao.spi.bean.ScanSlice;
import com.dojao.spi.bean.TableStats;
import com.dojao.spi.bean.WriteRequest;
import com.dojao.spi.service.DojaoConnector;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * <b>一趟扫描（Expand）多维度聚合的正确性探针</b> —— 同一 SQL 分别用「展开」与「多腿 UNION」
 * 两条规划路径在 3 Worker 的 in-JVM 集群上真跑一遍，逐行比对结果集。
 *
 * <p>覆盖：ROLLUP / CUBE / GROUPING SETS、JOIN 链、派生表、真实 NULL 维度值（必须与「本组合不
 * 参与的占位 NULL」区分开）、{@code count(DISTINCT)}、{@code avg}、HAVING、聚合参数引用维度列
 * （应自动退回多腿，不得被掩码改变结果）。</p>
 *
 * <p>判别力：把 {@code query.groupingSets.expand} 打开 / 关闭各跑一次；两侧结果必须逐行相同。
 * 展开路径的 EXPLAIN 必须只含一个分布式聚合叶子且出现 {@code Expand[}。</p>
 *
 * @author jiaxin
 * @date 2026/10/29
 */
public class GroupingSetsExpandProbeTest {

    private static final String CATALOG = "mem";
    private static final String SCHEMA = "mem";
    private static final String F = "f_order";
    private static final String U = "d_user";

    private static int pass = 0;
    private static int fail = 0;

    private static Map<String, WorkerManager> workerMap;
    private static WorkerGateway gateway;
    private static final List<String> WORKER_IDS = List.of("W0", "W1", "W2");

    public static void main(String[] args) {
        System.err.println("========== 多维度聚合 一趟扫描 正确性探针 ==========");
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        java.nio.file.Path spill = null;
        try {
            spill = java.nio.file.Files.createTempDirectory("dojao-gs-expand-probe");
            System.setProperty("query.spill.dir", spill.toString());
            startCluster(allocator);

            // 注意：region / city 里有真实 NULL 值 —— 展开路径必须靠 __grouping_id 把它与
            // 「本组合不参与的占位 NULL」区分开，否则 (NULL, 北京) 与 (总计, 北京) 会被并成一组。
            compare("S1 ROLLUP(region, city) 单表",
                    "SELECT region, city, count(*) AS cnt, sum(amount) AS amt,"
                            + " count(DISTINCT user_id) AS uv, avg(qty) AS aq"
                            + " FROM " + CATALOG + "." + SCHEMA + "." + F
                            + " GROUP BY ROLLUP(region, city)");
            compare("S2 CUBE(region, city) 单表",
                    "SELECT region, city, count(*) AS cnt, sum(amount) AS amt"
                            + " FROM " + CATALOG + "." + SCHEMA + "." + F
                            + " GROUP BY CUBE(region, city)");
            compare("S3 GROUPING SETS((region, city),(city),())",
                    "SELECT region, city, count(*) AS cnt"
                            + " FROM " + CATALOG + "." + SCHEMA + "." + F
                            + " GROUP BY GROUPING SETS((region, city), (city), ())");
            compare("S4 JOIN 链 ROLLUP(region, ulevel)",
                    "SELECT f.region, u.ulevel, count(*) AS cnt, sum(f.amount) AS amt"
                            + " FROM " + CATALOG + "." + SCHEMA + "." + F + " f"
                            + " JOIN " + CATALOG + "." + SCHEMA + "." + U + " u ON f.user_id = u.user_id"
                            + " GROUP BY ROLLUP(f.region, u.ulevel)");
            compare("S5 派生表 ROLLUP(region, city)",
                    "SELECT region, city, count(*) AS cnt"
                            + " FROM (SELECT region, city, sum(amount) AS total FROM "
                            + CATALOG + "." + SCHEMA + "." + F + " GROUP BY region, city) t"
                            + " GROUP BY ROLLUP(region, city)");
            compare("S6 HAVING count(*) > 1",
                    "SELECT region, city, count(*) AS cnt"
                            + " FROM " + CATALOG + "." + SCHEMA + "." + F
                            + " GROUP BY ROLLUP(region, city) HAVING count(*) > 1");
            compare("S7 三表 ROLLUP 带 ORDER BY",
                    "SELECT f.region, u.ulevel, count(*) AS cnt, sum(f.amount) AS amt"
                            + " FROM " + CATALOG + "." + SCHEMA + "." + F + " f"
                            + " JOIN " + CATALOG + "." + SCHEMA + "." + U + " u ON f.user_id = u.user_id"
                            + " GROUP BY ROLLUP(f.region, u.ulevel)"
                            + " ORDER BY amt DESC");

            // S8：聚合参数引用维度列 —— 掩码会改变结果（max(region) 在粗粒度组合上变成 max(NULL)），
            // 必须自动退回多腿路径，且结果与关闭开关完全一致。
            String dimAgg = "SELECT region, city, max(region) AS mr, count(*) AS cnt"
                    + " FROM " + CATALOG + "." + SCHEMA + "." + F
                    + " GROUP BY ROLLUP(region, city)";
            Result on = run("S8 max(维度列) 展开开启", dimAgg, true);
            Result off = run("S8 max(维度列) 展开关闭", dimAgg, false);
            check("S8a 聚合参数引用维度列 ⇒ 退回多腿 UNION（计划含 UnionAll[ROLLUP），实际 "
                            + (on.explain.contains("UnionAll[ROLLUP") ? "含" : "不含"),
                    on.explain.contains("UnionAll[ROLLUP"));
            check("S8b 两条路径结果逐行相同", on.rows.equals(off.rows));
            check("S8c 粗粒度腿上 max(region) 仍是真实维度值（占位 NULL 未污染聚合参数），示例 "
                            + on.rows.stream().filter(r -> r.startsWith("NULL|NULL|")).findFirst().orElse("-"),
                    on.rows.stream().anyMatch(r -> r.startsWith("NULL|NULL|")
                            && !r.startsWith("NULL|NULL|NULL|")));

            // S9：单机（Master 本地）路径 —— 覆盖 buildAggregation 的本地展开分支
            compareLocal("S9 单机 ROLLUP(region, city)",
                    "SELECT region, city, count(*) AS cnt, sum(amount) AS amt"
                            + " FROM " + CATALOG + "." + SCHEMA + "." + F
                            + " GROUP BY ROLLUP(region, city)");

            // S10–S12：分组集合成员里有「未投影」的维度（SELECT 只取 region / city，
            // CUBE/ROLLUP 还带 user_id）。展开范围必须覆盖全部成员 —— 否则掩码漏维度、
            // 分组键少维度 ⇒ 各腿并组、行数与聚合值双错（比多腿少行）。
            compare("S10 CUBE(region, city, user_id)，user_id 未投影",
                    "SELECT region, count(*) AS cnt, sum(amount) AS amt"
                            + " FROM " + CATALOG + "." + SCHEMA + "." + F
                            + " GROUP BY CUBE(region, city, user_id)");
            compare("S11 ROLLUP(region, city, user_id)，user_id 未投影",
                    "SELECT region, city, count(*) AS cnt"
                            + " FROM " + CATALOG + "." + SCHEMA + "." + F
                            + " GROUP BY ROLLUP(region, city, user_id)");
            compare("S12 GROUPING SETS((region, city), (user_id), ())，city/user_id 未投影",
                    "SELECT region, count(*) AS cnt"
                            + " FROM " + CATALOG + "." + SCHEMA + "." + F
                            + " GROUP BY GROUPING SETS((region, city), (user_id), ())");

            // S13：聚合参数引用「未投影但属于分组集合」的维度 ⇒ 掩码会改变结果，
            // 必须退回多腿（判据也必须覆盖全部成员，不能只看投影维度）。
            String hiddenDimAgg = "SELECT region, max(city) AS mc, count(*) AS cnt"
                    + " FROM " + CATALOG + "." + SCHEMA + "." + F
                    + " GROUP BY CUBE(region, city, user_id)";
            Result hiddenOn = run("S13 max(city) 展开开启", hiddenDimAgg, true);
            Result hiddenOff = run("S13 max(city) 展开关闭", hiddenDimAgg, false);
            check("S13a 聚合参数引用维度（含未投影成员）⇒ 退回多腿，实际 "
                            + (hiddenOn.explain.contains("UnionAll[") ? "含 UnionAll[" : "仍走展开"),
                    hiddenOn.explain.contains("UnionAll["));
            check("S13b 两条路径结果逐行相同", hiddenOn.rows.equals(hiddenOff.rows));
        } catch (Throwable t) {
            fail++;
            System.err.println("[FAIL] 探针异常: " + t);
            t.printStackTrace();
        } finally {
            stopCluster();
            try { allocator.close(); } catch (Throwable ignored) { }
            if (spill != null) deleteRecursively(spill);
        }
        System.err.printf("========== 结果: %d PASS / %d FAIL ==========%n", pass, fail);
        if (fail > 0) System.exit(1);
    }

    // ==================== 场景驱动 ====================

    private static void compare(String label, String sql) {
        Result on = run(label + " [expand]", sql, true);
        Result off = run(label + " [union]", sql, false);
        check(label + "：两条路径结果逐行相同（" + on.rows.size() + " 行）", on.rows.equals(off.rows));
        check(label + "：展开路径计划树里没有按腿复制的分布式聚合（≤1 个叶子），实际 "
                        + countLeaves(on.explain), countLeaves(on.explain) <= 1);
        check(label + "：展开路径计划树命中一趟扫描形态", on.expanded);
        check(label + "：多腿路径计划出现 UnionAll[ 且不含展开", off.explain.contains("UnionAll[")
                && !off.expanded);
    }

    private static void compareLocal(String label, String sql) {
        Result on = local(sql, true);
        Result off = local(sql, false);
        check(label + "：单机两条路径结果逐行相同（" + on.rows.size() + " 行）", on.rows.equals(off.rows));
        check(label + "：单机展开路径计划树里有 ExpandOperator", on.expanded);
    }

    /** 计划树是否走一趟扫描形态：本地展开算子，或分布式算子带 expand 描述。 */
    private static boolean usesExpand(com.dojao.core.planner.PhysicalNode node) {
        if (node.operator() instanceof com.dojao.core.operator.ExpandOperator) return true;
        if (node.operator() instanceof com.dojao.core.operator.DistributedAggregationOperator d
                && d.expand() != null) return true;
        if (node.operator() instanceof com.dojao.core.operator.DistributedJoinAggregationOperator j
                && j.expand() != null) return true;
        for (com.dojao.core.planner.PhysicalNode child : node.children()) {
            if (usesExpand(child)) return true;
        }
        return false;
    }

    private static long countLeaves(String explain) {
        return explain.lines().filter(l -> l.contains("DistributedAggregation[")
                || l.contains("DistributedJoinShuffledAggregation[")).count();
    }

    // ==================== 执行 ====================

    private static Result run(String label, String sql, boolean expand) {
        System.clearProperty("query.groupingSets.expand");
        System.setProperty("query.groupingSets.expand", String.valueOf(expand));
        try {
            return execute(label, sql, true);
        } finally {
            System.clearProperty("query.groupingSets.expand");
        }
    }

    private static Result local(String sql, boolean expand) {
        System.setProperty("query.groupingSets.expand", String.valueOf(expand));
        try {
            return execute("local", sql, false);
        } finally {
            System.clearProperty("query.groupingSets.expand");
        }
    }

    private static Result execute(String label, String sql, boolean distributed) {
        Map<String, List<Object[]>> data = data();
        MetaStore metaStore = new MetaStore(MetaStoreType.LOCAL);
        BufferAllocator alloc = MASTER_ALLOC;
        metaStore.registerConnector(CATALOG, new MemConnector(alloc, data));
        try {
            LogicalPlan logical = new SqlValidator(metaStore).validate(sql);
            PlanningContext.Builder builder = PlanningContext.builder(
                            catalog -> new MemConnector(alloc, data))
                    .distributedExecution(distributed)
                    .memoryLimit(256L * 1024 * 1024)
                    .parallelism(distributed ? 4 : 2);
            if (distributed) builder.workerGateway(gateway);
            PhysicalPlan plan = new Planner(builder.build()).plan(logical);
            String explain = plan.explainText();
            System.err.println("---------- " + label + " ----------\n" + indent(explain));
            List<String> rows = new ArrayList<>();
            Operator op = plan.operator();
            try {
                op.open();
                Page p;
                while (!(p = op.next()).isEOF()) {
                    collectRows(p.getRoot(), rows);
                    p.close();
                }
            } finally {
                try { op.close(); } catch (Throwable ignored) { }
            }
            java.util.Collections.sort(rows);
            return new Result(rows, explain, usesExpand(plan.root()));
        } catch (Throwable t) {
            fail++;
            System.err.println("[FAIL] " + label + " 执行失败: " + t);
            t.printStackTrace();
            return new Result(new ArrayList<>(), "", false);
        }
    }

    private record Result(List<String> rows, String explain, boolean expanded) { }

    private static void collectRows(VectorSchemaRoot root, List<String> out) {
        if (root == null) return;
        List<FieldVector> vectors = root.getFieldVectors();
        for (int r = 0; r < root.getRowCount(); r++) {
            StringBuilder sb = new StringBuilder();
            for (int c = 0; c < vectors.size(); c++) {
                if (c > 0) sb.append('|');
                sb.append(cell(vectors.get(c).getObject(r)));
            }
            out.add(sb.toString());
        }
    }

    /** 数值按 6 位小数归一（两条路径的分区内行序可能不同，double 累加顺序随之不同）。 */
    private static String cell(Object v) {
        if (v == null) return "NULL";
        if (v instanceof Number n && !(v instanceof Long) && !(v instanceof Integer)) {
            return java.math.BigDecimal.valueOf(n.doubleValue())
                    .setScale(6, java.math.RoundingMode.HALF_UP).toPlainString();
        }
        return String.valueOf(v);
    }

    // ==================== 集群 / 数据 ====================

    private static BufferAllocator MASTER_ALLOC;

    private static void startCluster(BufferAllocator allocator) {
        MASTER_ALLOC = allocator;
        workerMap = new HashMap<>();
        ConcurrentHashMap<String, ConcurrentHashMap<String, ShuffleReader>> bus = new ConcurrentHashMap<>();
        MasterGateway noopMaster = new MasterGateway() {
            @Override public boolean register(WorkerNode info) { return true; }
            @Override public boolean heartbeat(WorkerNode info, WorkerState state, ResourceSummary summary) { return true; }
            @Override public boolean unregister(WorkerNode info) { return true; }
        };
        for (String id : WORKER_IDS) {
            WorkerNode node = new WorkerNode(id, "localhost", 0, "probe", System.nanoTime());
            workerMap.put(id, new WorkerManager(node, noopMaster, 60_000, 4,
                    () -> new ResourceSummary(4, 10, 8L * 1024 * 1024 * 1024,
                            3L * 1024 * 1024 * 1024, 0, 0, 4, System.nanoTime(), 0L, 0L, 0),
                    new ExchangeManager(id, bus)));
        }
        gateway = new LocalWorkerGateway(catalog -> new MemConnector(allocator, data()), workerMap);
        WorkerRegistry registry = WorkerRegistry.get();
        for (String id : WORKER_IDS) {
            registry.register(new WorkerNode(id, "localhost", 0, "probe", System.nanoTime()));
            registry.updateHeartbeat(id, WorkerState.IDLE,
                    new ResourceSummary(4, 10, 8L * 1024 * 1024 * 1024,
                            3L * 1024 * 1024 * 1024, 0, 0, 4, System.nanoTime(), 0L, 0L, 0));
        }
    }

    private static void stopCluster() {
        WorkerRegistry registry = WorkerRegistry.get();
        for (String id : WORKER_IDS) {
            try { registry.unregister(id); } catch (Throwable ignored) { }
        }
        if (workerMap != null) {
            for (WorkerManager wm : workerMap.values()) {
                try { wm.stop(); } catch (Throwable ignored) { }
            }
        }
    }

    private static Map<String, List<Object[]>> data() {
        Map<String, List<Object[]>> data = new LinkedHashMap<>();
        List<Object[]> f = new ArrayList<>();
        // id, region, city, user_id, amount, qty
        f.add(new Object[]{1L, "华东", "上海", 1L, 10.0, 1L});
        f.add(new Object[]{2L, "华东", "上海", 2L, 20.0, 2L});
        f.add(new Object[]{3L, "华东", "杭州", 1L, 30.0, 3L});
        f.add(new Object[]{4L, "华北", "北京", 3L, 40.0, 4L});
        f.add(new Object[]{5L, "华北", null, 2L, 50.0, 5L});       // 真实 NULL 维度值
        f.add(new Object[]{6L, null, null, 3L, 60.0, 6L});         // 真实 NULL 维度值
        f.add(new Object[]{7L, "华东", "杭州", null, 70.0, 7L});   // NULL 去重值
        f.add(new Object[]{8L, "华东", "上海", 2L, 5.0, 8L});
        data.put(F, f);

        List<Object[]> u = new ArrayList<>();
        u.add(new Object[]{1L, "gold"});
        u.add(new Object[]{2L, "silver"});
        u.add(new Object[]{3L, "gold"});
        data.put(U, u);
        return data;
    }

    private static final class MemConnector implements DojaoConnector {

        private final BufferAllocator allocator;
        private final Map<String, List<Object[]>> data;
        private final Map<String, Integer> cursors = new ConcurrentHashMap<>();
        private static final int BATCH = 4;

        MemConnector(BufferAllocator allocator, Map<String, List<Object[]>> data) {
            this.allocator = allocator;
            this.data = data;
        }

        @Override public boolean supportsPredicatePushdown() { return false; }

        @Override
        public List<Field> sourceFields(String catalog, String schema, String table, List<String> columns) {
            List<Field> full = schemaFor(table).getFields();
            if (columns == null || columns.isEmpty()) return full;
            List<Field> out = new ArrayList<>();
            for (String name : columns) {
                for (Field f : full) {
                    if (f.getName().equalsIgnoreCase(name)) { out.add(f); break; }
                }
            }
            return out;
        }

        @Override
        public List<ScanSlice> planSplits(ReadRequest request, int parallelism) {
            return List.of(ScanSlice.whole(request));
        }

        @Override
        public Page read(ReadRequest request) {
            List<Object[]> rows = data.getOrDefault(request.table(), List.of());
            String key = request.table() + "|" + request.columns().orElse(List.of("*"));
            int from = cursors.getOrDefault(key, 0);
            if (from >= rows.size()) return Page.eof();
            int n = Math.min(BATCH, rows.size() - from);
            List<Field> full = schemaFor(request.table()).getFields();
            List<Field> fields = sourceFields(CATALOG, request.schema(), request.table(),
                    request.columns().orElse(null));
            VectorSchemaRoot root = VectorSchemaRoot.create(
                    new org.apache.arrow.vector.types.pojo.Schema(fields), allocator);
            root.allocateNew();
            for (int i = 0; i < n; i++) {
                Object[] row = rows.get(from + i);
                for (int c = 0; c < fields.size(); c++) {
                    int idx = indexOf(full, fields.get(c).getName());
                    setValue(root.getVector(c), i, idx < 0 ? null : row[idx]);
                }
            }
            root.setRowCount(n);
            cursors.put(key, from + n);
            return Page.fromRoot(root);
        }

        private static int indexOf(List<Field> fields, String name) {
            for (int i = 0; i < fields.size(); i++) {
                if (fields.get(i).getName().equalsIgnoreCase(name)) return i;
            }
            return -1;
        }

        private static void setValue(FieldVector v, int row, Object val) {
            if (val == null) {
                v.setNull(row);
            } else if (v instanceof VarCharVector s) {
                s.setSafe(row, String.valueOf(val).getBytes(StandardCharsets.UTF_8));
            } else if (v instanceof Float8Vector d) {
                d.setSafe(row, ((Number) val).doubleValue());
            } else if (v instanceof BigIntVector l) {
                l.setSafe(row, ((Number) val).longValue());
            } else {
                v.setNull(row);
            }
        }

        private static org.apache.arrow.vector.types.pojo.Schema schemaFor(String table) {
            FieldType longT = FieldType.nullable(new ArrowType.Int(64, true));
            FieldType dblT = FieldType.nullable(
                    new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
            FieldType strT = FieldType.nullable(ArrowType.Utf8.INSTANCE);
            return switch (table) {
                case F -> new org.apache.arrow.vector.types.pojo.Schema(List.of(
                        new Field("id", longT, null),
                        new Field("region", strT, null),
                        new Field("city", strT, null),
                        new Field("user_id", longT, null),
                        new Field("amount", dblT, null),
                        new Field("qty", longT, null)));
                case U -> new org.apache.arrow.vector.types.pojo.Schema(List.of(
                        new Field("user_id", longT, null),
                        new Field("ulevel", strT, null)));
                default -> new org.apache.arrow.vector.types.pojo.Schema(List.of());
            };
        }

        @Override public long getExactRowCount(String schema, String table) {
            List<Object[]> rows = data.get(table);
            return rows == null ? 0 : rows.size();
        }

        @Override public TableStats getTableStats(String catalog, String schema, String table) {
            long rows = getExactRowCount(schema, table);
            return new TableStats(catalog, schema, table, rows, Math.max(rows, 1) * 32L, List.of());
        }

        @Override public List<ColumnStats> getColumnStats(String c, String s, String t) {
            return List.of();
        }

        @Override public void write(WriteRequest request) { }
        @Override public boolean catalogExists(String catalog) { return CATALOG.equals(catalog); }
        @Override public Optional<Catalog> getCatalog(String catalog) { return Optional.empty(); }
        @Override public List<String> listCatalogs() { return List.of(CATALOG); }
        @Override public boolean schemaExists(String catalog, String schema) { return true; }
        @Override public Optional<Schema> getSchema(String catalog, String schema) { return Optional.empty(); }
        @Override public List<String> listSchemas(String catalog) { return List.of(SCHEMA); }
        @Override public boolean tableExists(String catalog, String schema, String table) {
            return data.containsKey(table);
        }
        @Override public Optional<com.dojao.spi.bean.Table> getTable(String c, String s, String t) {
            return Optional.empty();
        }
        @Override public List<String> listTables(String catalog, String schema) {
            return new ArrayList<>(data.keySet());
        }
        @Override public boolean columnExists(String c, String s, String t, String column) {
            return indexOf(schemaFor(t).getFields(), column) >= 0;
        }
        @Override public Optional<com.dojao.spi.bean.Column> getColumn(String c, String s,
                                                                      String t, String column) {
            return Optional.empty();
        }
        @Override public List<com.dojao.spi.bean.Column> getTableColumns(String c, String s, String t) {
            List<com.dojao.spi.bean.Column> out = new ArrayList<>();
            for (Field f : schemaFor(t).getFields()) {
                out.add(new com.dojao.spi.bean.Column(f.getName(), "string", null, null));
            }
            return out;
        }
    }

    // ==================== 断言 / 工具 ====================

    private static void check(String what, boolean ok) {
        if (ok) { pass++; System.err.println("[PASS] " + what); }
        else { fail++; System.err.println("[FAIL] " + what); }
    }

    private static String indent(String text) {
        return "    " + text.replace("\n", "\n    ");
    }

    private static void deleteRecursively(java.nio.file.Path dir) {
        try (var walk = java.nio.file.Files.walk(dir)) {
            walk.sorted(java.util.Comparator.reverseOrder()).forEach(f -> {
                try { java.nio.file.Files.deleteIfExists(f); } catch (Exception ignored) { }
            });
        } catch (Exception ignored) { }
    }
}
