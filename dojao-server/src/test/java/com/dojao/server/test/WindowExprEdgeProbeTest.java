package com.dojao.server.test;

import com.dojao.common.data.Page;
import com.dojao.core.operator.Operator;
import com.dojao.core.operator.ShuffleReader;
import com.dojao.core.planner.PhysicalPlan;
import com.dojao.core.planner.PlanningContext;
import com.dojao.core.planner.Planner;
import com.dojao.core.runtime.master.WorkerRegistry;
import com.dojao.core.runtime.worker.ResourceSummary;
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
import com.dojao.spi.bean.Column;
import com.dojao.spi.bean.ColumnStats;
import com.dojao.spi.bean.ReadRequest;
import com.dojao.spi.bean.Schema;
import com.dojao.spi.bean.Table;
import com.dojao.spi.bean.TableStats;
import com.dojao.spi.bean.WriteRequest;
import com.dojao.spi.service.DojaoConnector;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 窗口表达式「相邻边界」探针：BUG-1（窗口参与二元算术）修好之后，把同一族的高危组合一次性跑一遍，
 * 找出仍然<b>静默算错</b>的形态（报错不算缺陷，静默错值才算）。
 *
 * <p>E1 分组占比 / E2 count(*) 窗口 / E3 row_number 窗口 / E4 CASE 包裹 / E5 表达式参数 /
 * E6 lag 表达式参数 / E7 JOIN + 嵌套窗口 / E8 DISTINCT / E9 UNION ALL / E10 ORDER BY 里的窗口 /
 * E11 WHERE 里的窗口（应报错）/ E12 两个不同窗口组 + 算术。</p>
 */
public final class WindowExprEdgeProbeTest {

    private static final String CATALOG = "mem";
    private static final String SCHEMA = "mem";
    private static final String[] WORKERS = {"W0", "W1", "W2"};
    private static final long[] GMV = {10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120};
    private static final double TOTAL = 780.0;

    private static int checks = 0;
    private static int failures = 0;

    public static void main(String[] args) {
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        Map<String, List<Object[]>> data = buildData();
        PlanningContext.ConnectorProvider provider = catalog -> new InMemoryConnector(catalog, data, allocator);

        Map<String, WorkerManager> workerMap = new HashMap<>();
        MasterGateway noopMaster = new MasterGateway() {
            @Override public boolean register(WorkerNode info) { return true; }
            @Override public boolean heartbeat(WorkerNode info, WorkerState state, ResourceSummary summary) { return true; }
            @Override public boolean unregister(WorkerNode info) { return true; }
        };
        // 共享交换 bus（伪分布式：单 JVM 多 Worker 必须共用同一路由表，否则跨 Worker 的
        // shuffle 页找不到接收端 —— join 场景会报「Worker 未注册交换接收端」）。
        ConcurrentHashMap<String, ConcurrentHashMap<String, ShuffleReader>> bus = new ConcurrentHashMap<>();
        for (String workerId : WORKERS) {
            WorkerNode node = new WorkerNode(workerId, "localhost", 0, "probe", System.nanoTime());
            workerMap.put(workerId, new WorkerManager(node, noopMaster, 60_000, 16,
                    () -> new ResourceSummary(4, 10, 8L * 1024 * 1024 * 1024,
                            2L * 1024 * 1024 * 1024, 0, 0, 4, System.nanoTime(), 0L, 0L, 0),
                    new ExchangeManager(workerId, bus)));
        }
        LocalWorkerGateway gateway = new LocalWorkerGateway(provider, workerMap);
        WorkerRegistry registry = WorkerRegistry.get();
        for (String workerId : WORKERS) {
            registry.register(new WorkerNode(workerId, "localhost", 0, "probe", System.nanoTime()));
            registry.updateHeartbeat(workerId, WorkerState.IDLE,
                    new ResourceSummary(4, 10, 8L * 1024 * 1024 * 1024,
                            2L * 1024 * 1024 * 1024, 0, 0, 4, System.nanoTime(), 0L, 0L, 0));
        }
        try {
            MetaStore metaStore = new MetaStore(MetaStoreType.LOCAL);
            metaStore.registerConnector(CATALOG, provider.get(CATALOG));
            PlanningContext context = PlanningContext.builder(provider)
                    .distributedExecution(true)
                    .workerGateway(gateway)
                    .memoryLimit(256L * 1024 * 1024)
                    .parallelism(3)
                    .build();

            String t = CATALOG + "." + SCHEMA;

            // E1 分组占比：A 组和 420 / B 组和 360
            caseRun("E1 PARTITION BY 分组占比", metaStore, context,
                    "SELECT id, gmv / sum(gmv) OVER (PARTITION BY dept) AS pct"
                            + " FROM " + t + ".sales ORDER BY id",
                    r -> check("A 行 pct = gmv/420、B 行 = gmv/360", allRows(r, row -> {
                        double id = num(row.get(0)), pct = num(row.get(1));
                        double denom = (id % 2 == 0) ? 420.0 : 360.0;
                        return near(pct, id * 10.0 / denom);
                    })));

            // E2 count(*) 窗口参与算术 → gmv/12
            caseRun("E2 gmv / count(*) OVER ()", metaStore, context,
                    "SELECT id, gmv / count(*) OVER () AS v FROM " + t + ".sales ORDER BY id",
                    r -> check("gmv / 12", allRows(r, row -> near(num(row.get(1)),
                            num(row.get(0)) * 10.0 / 12.0))));

            // E3 row_number 参与算术 → gmv / rn(=id)
            caseRun("E3 gmv / row_number() OVER (ORDER BY id)", metaStore, context,
                    "SELECT id, gmv / row_number() OVER (ORDER BY id) AS v"
                            + " FROM " + t + ".sales ORDER BY id",
                    r -> check("gmv / id = 10", allRows(r, row -> near(num(row.get(1)), 10.0))));

            // E4 CASE 包裹嵌套窗口
            caseRun("E4 CASE WHEN 包裹窗口算术", metaStore, context,
                    "SELECT id, CASE WHEN gmv > 60 THEN gmv / sum(gmv) OVER () ELSE 0 END AS v"
                            + " FROM " + t + ".sales ORDER BY id",
                    r -> check("gmv>60 行 = gmv/780，其余 0", allRows(r, row -> {
                        double gmv = num(row.get(0)) * 10.0;
                        double expected = gmv > 60 ? gmv / TOTAL : 0.0;
                        return near(num(row.get(1)), expected);
                    })));

            // E5 窗口参数是表达式
            caseRun("E5 sum(gmv * 1) OVER () 参与算术", metaStore, context,
                    "SELECT id, gmv / sum(gmv * 1) OVER () AS v FROM " + t + ".sales ORDER BY id",
                    r -> check("等效 gmv/780", allRows(r, row -> near(num(row.get(1)),
                            num(row.get(0)) * 10.0 / TOTAL))));

            // E6 lag 表达式参数
            caseRun("E6 gmv - lag(gmv * 2, 1, 0) OVER (ORDER BY id)", metaStore, context,
                    "SELECT id, gmv - lag(gmv * 2, 1, 0) OVER (ORDER BY id) AS v"
                            + " FROM " + t + ".sales ORDER BY id",
                    r -> check("首行 10，其余 gmv - 2*前一行 gmv", allRows(r, row -> {
                        double id = num(row.get(0));
                        double expected = id == 1 ? 10.0 : id * 10.0 - (id - 1) * 20.0;
                        return near(num(row.get(1)), expected);
                    })));

            // E7a 对照：同样的 JOIN 不带窗口（验证是 JOIN 本身还是「JOIN + 嵌套窗口」出问题）
            caseRun("E7a 对照：JOIN 不带窗口", metaStore, context,
                    "SELECT s.id, s.gmv FROM " + t + ".sales s JOIN " + t + ".dim d ON s.dept = d.dept"
                            + " ORDER BY s.id",
                    r -> check("12 行", r.rows().size() == 12));

            // E7 JOIN + 嵌套窗口
            caseRun("E7 JOIN 后 gmv / sum(gmv) OVER ()", metaStore, context,
                    "SELECT s.id, s.gmv / sum(s.gmv) OVER () AS pct"
                            + " FROM " + t + ".sales s JOIN " + t + ".dim d ON s.dept = d.dept"
                            + " ORDER BY s.id",
                    r -> check("JOIN 后仍是 gmv/780 且 12 行",
                            r.rows().size() == 12 && allRows(r, row -> near(num(row.get(1)),
                                    num(row.get(0)) * 10.0 / TOTAL))));

            // E8 DISTINCT + 嵌套窗口
            caseRun("E8 DISTINCT 占比", metaStore, context,
                    "SELECT DISTINCT gmv / sum(gmv) OVER () AS pct FROM " + t + ".sales",
                    r -> check("12 个不同占比", r.rows().size() == 12 && allRows(r, row ->
                            near(num(row.get(0)), Math.round(num(row.get(0)) * TOTAL / 10.0) * 10.0 / TOTAL))));

            // E9 UNION ALL
            caseRun("E9 UNION ALL 两个分支各带嵌套窗口", metaStore, context,
                    "SELECT id, gmv / sum(gmv) OVER () AS pct FROM " + t + ".sales"
                            + " UNION ALL SELECT id, gmv / sum(gmv) OVER (PARTITION BY dept) AS pct"
                            + " FROM " + t + ".sales",
                    r -> check("24 行、两段各自正确",
                            r.rows().size() == 24 && allRows(r, row -> {
                                double id = num(row.get(0));
                                double gmv = id * 10.0;
                                boolean firstLeg = row.get(1) != null
                                        && near(num(row.get(1)), gmv / TOTAL);
                                double denom = (id % 2 == 0) ? 420.0 : 360.0;
                                boolean secondLeg = near(num(row.get(1)), gmv / denom);
                                return firstLeg || secondLeg;
                            })));

            // E10 ORDER BY 里的窗口表达式（不在 SELECT 列表）：不支持，但必须显式报错、不许静默
            caseErr("E10 ORDER BY 里的窗口应显式报错", metaStore, context,
                    "SELECT id FROM " + t + ".sales ORDER BY gmv / sum(gmv) OVER () DESC");

            // E11 WHERE 里的窗口 → 应显式报错（不许静默放行全表）
            caseErr("E11 WHERE 里的窗口应报错", metaStore, context,
                    "SELECT id FROM " + t + ".sales WHERE row_number() OVER (ORDER BY id) <= 3");

            // E11b HAVING 里的窗口
            caseErr("E11b HAVING 里的窗口应报错", metaStore, context,
                    "SELECT dept, sum(gmv) AS s FROM " + t + ".sales GROUP BY dept"
                            + " HAVING sum(gmv) > 0 AND row_number() OVER (ORDER BY dept) = 1");

            // E11c GROUP BY 里的窗口
            caseErr("E11c GROUP BY 里的窗口应报错", metaStore, context,
                    "SELECT dept, count(*) AS c FROM " + t + ".sales"
                            + " GROUP BY row_number() OVER (ORDER BY id)");

            // E11d JOIN ON 里的窗口
            caseErr("E11d JOIN ON 里的窗口应报错", metaStore, context,
                    "SELECT s.id FROM " + t + ".sales s JOIN " + t + ".dim d"
                            + " ON s.dept = d.dept AND row_number() OVER (ORDER BY s.id) <= 3");

            // E12 两个不同窗口组 + 算术（累计占比）
            caseRun("E12 累计 sum OVER (PARTITION BY dept ORDER BY id) / 总计", metaStore, context,
                    "SELECT id, sum(gmv) OVER (PARTITION BY dept ORDER BY id)"
                            + " / sum(gmv) OVER () AS cum"
                            + " FROM " + t + ".sales ORDER BY id",
                    r -> check("A 组累计 20+40+…、B 组 10+30+…", allRows(r, row -> {
                        double id = num(row.get(0));
                        double running = 0;
                        for (long k = (id % 2 == 0) ? 2 : 1; k <= id; k += 2) running += k * 10.0;
                        return near(num(row.get(1)), running / TOTAL);
                    })));
        } finally {
            for (String workerId : WORKERS) registry.unregister(workerId);
            for (WorkerManager wm : workerMap.values()) {
                try { wm.stop(); } catch (Throwable ignored) { }
            }
            allocator.close();
        }
        System.out.println("\n==== WindowExprEdgeProbeTest: " + failures + " FAIL / " + checks + " checks ====");
        if (failures > 0) System.exit(1);
    }

    // ==================== 执行 ====================

    private record Result(List<String> columns, List<List<Object>> rows) {}

    private interface Checker { void check(Result r); }

    private interface RowPredicate { boolean test(List<Object> row); }

    private static boolean allRows(Result r, RowPredicate p) {
        if (r.rows().isEmpty()) return false;
        for (List<Object> row : r.rows()) if (!p.test(row)) {
            System.out.println("       未通过的行: " + row);
            return false;
        }
        return true;
    }

    private static void caseRun(String name, MetaStore metaStore, PlanningContext context,
                                String sql, Checker checker) {
        System.out.println("\n---------- " + name + " ----------");
        System.out.println(sql);
        Result r;
        try {
            r = execute(sql, metaStore, context);
        } catch (Throwable e) {
            System.out.println("[ERROR] 执行失败: " + rootMessage(e));
            failures++;
            return;
        }
        System.out.println("  输出列 = " + r.columns() + "，行数 = " + r.rows().size());
        for (int i = 0; i < Math.min(r.rows().size(), 5); i++) System.out.println("  " + r.rows().get(i));
        try {
            checker.check(r);
        } catch (Throwable e) {
            System.out.println("[ERROR] 断言异常: " + rootMessage(e));
            failures++;
        }
    }

    private static void caseErr(String name, MetaStore metaStore, PlanningContext context, String sql) {
        System.out.println("\n---------- " + name + " ----------");
        System.out.println(sql);
        try {
            Result r = execute(sql, metaStore, context);
            System.out.println("  [!!] 未报错，返回 " + r.rows().size() + " 行（静默放行）");
            failures++;
        } catch (Throwable e) {
            System.out.println("  [ok] 显式报错: " + rootMessage(e));
            checks++;
        }
    }

    private static Result execute(String sql, MetaStore metaStore, PlanningContext context) {
        LogicalPlan logical = new SqlValidator(metaStore).validate(sql);
        PhysicalPlan plan = new Planner(context).plan(logical);
        List<String> columns = plan.outputColumns();
        List<List<Object>> rows = new ArrayList<>();
        Operator op = plan.operator();
        try {
            op.open();
            Page p;
            while (!(p = op.next()).isEOF()) {
                VectorSchemaRoot root = p.getRoot();
                if (root != null) {
                    for (int i = 0; i < root.getRowCount(); i++) {
                        List<Object> row = new ArrayList<>(root.getFieldVectors().size());
                        for (FieldVector v : root.getFieldVectors()) {
                            row.add(v.isNull(i) ? null : v.getObject(i));
                        }
                        rows.add(row);
                    }
                }
                p.close();
            }
        } finally {
            try { op.close(); } catch (Throwable ignored) { }
        }
        return new Result(columns, rows);
    }

    private static String rootMessage(Throwable t) {
        Throwable c = t;
        while (c.getCause() != null && c.getCause() != c) c = c.getCause();
        return c.getClass().getSimpleName() + ": " + c.getMessage();
    }

    private static double num(Object v) {
        if (v == null) return Double.NaN;
        if (v instanceof Number n) return n.doubleValue();
        try {
            return Double.parseDouble(v.toString());
        } catch (RuntimeException e) {
            return Double.NaN;
        }
    }

    private static boolean near(Object v, double expected) {
        double actual = num(v);
        if (Double.isNaN(actual) || Double.isNaN(expected)) return false;
        return Math.abs(actual - expected) <= Math.max(1e-9, Math.abs(expected) * 1e-9);
    }

    private static void check(String what, boolean ok) {
        checks++;
        if (ok) System.out.println("  [ok] " + what);
        else {
            failures++;
            System.out.println("  [!!] " + what);
        }
    }

    // ==================== 数据 ====================

    private static Map<String, List<Object[]>> buildData() {
        Map<String, List<Object[]>> data = new HashMap<>();
        List<Object[]> sales = new ArrayList<>();
        for (int i = 0; i < GMV.length; i++) {
            long id = i + 1L;
            String dept = (id % 2 == 0) ? "A" : "B";
            sales.add(new Object[]{id, dept, GMV[i]});
        }
        data.put("sales", sales);
        List<Object[]> dim = new ArrayList<>();
        dim.add(new Object[]{"A", "east"});
        dim.add(new Object[]{"B", "west"});
        data.put("dim", dim);
        return data;
    }

    // ==================== 内存连接器 ====================

    private static final class InMemoryConnector implements DojaoConnector {

        private final String catalog;
        private final Map<String, List<Object[]>> data;
        private final BufferAllocator allocator;
        private static final int READ_BATCH = 5;

        private String cursorKey;
        private int cursorIdx;

        InMemoryConnector(String catalog, Map<String, List<Object[]>> data, BufferAllocator allocator) {
            this.catalog = catalog;
            this.data = data;
            this.allocator = allocator;
        }

        private static List<Field> schemaFor(String table) {
            if ("sales".equals(table)) {
                return List.of(
                        new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null),
                        new Field("dept", FieldType.nullable(new ArrowType.Utf8()), null),
                        new Field("gmv", FieldType.nullable(new ArrowType.Int(64, true)), null));
            }
            if ("dim".equals(table)) {
                return List.of(
                        new Field("dept", FieldType.nullable(new ArrowType.Utf8()), null),
                        new Field("region", FieldType.nullable(new ArrowType.Utf8()), null));
            }
            throw new IllegalArgumentException("未知表: " + table);
        }

        @Override
        public boolean supportsPredicatePushdown() {
            return true;
        }

        @Override
        public List<Field> sourceFields(String catalog, String schema, String table, List<String> columns) {
            List<Field> full = schemaFor(table);
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
        public List<com.dojao.spi.bean.ScanSlice> planSplits(ReadRequest request, int parallelism) {
            return List.of(com.dojao.spi.bean.ScanSlice.whole(request));
        }

        @Override
        public Page read(ReadRequest request) {
            List<Object[]> rows = data.get(request.table());
            if (rows == null || rows.isEmpty()) return Page.eof();
            List<String> cols = request.columns().orElse(null);
            List<Field> full = schemaFor(request.table());
            List<Field> outFields;
            int[] pick;
            if (cols == null || cols.isEmpty()) {
                outFields = full;
                pick = new int[full.size()];
                for (int i = 0; i < pick.length; i++) pick[i] = i;
            } else {
                outFields = new ArrayList<>(cols.size());
                pick = new int[cols.size()];
                for (int i = 0; i < cols.size(); i++) {
                    pick[i] = -1;
                    for (int f = 0; f < full.size(); f++) {
                        if (full.get(f).getName().equalsIgnoreCase(cols.get(i))) {
                            pick[i] = f;
                            outFields.add(full.get(f));
                            break;
                        }
                    }
                }
            }
            String key = request.table();
            int start = key.equals(cursorKey) ? cursorIdx : 0;
            int end = Math.min(rows.size(), start + READ_BATCH);
            if (start >= rows.size()) { cursorKey = null; cursorIdx = 0; return Page.eof(); }
            VectorSchemaRoot root = VectorSchemaRoot.create(
                    new org.apache.arrow.vector.types.pojo.Schema(outFields), allocator);
            root.allocateNew();
            int n = end - start;
            for (int c = 0; c < outFields.size(); c++) {
                FieldVector v = root.getVector(c);
                Field f = outFields.get(c);
                for (int r = 0; r < n; r++) {
                    Object value = pick[c] < 0 ? null : rows.get(start + r)[pick[c]];
                    if (v instanceof BigIntVector b) {
                        if (value == null) b.setNull(r); else b.setSafe(r, ((Number) value).longValue());
                    } else if (v instanceof VarCharVector s) {
                        if (value == null) s.setNull(r);
                        else s.setSafe(r, value.toString().getBytes(StandardCharsets.UTF_8));
                    }
                }
            }
            root.setRowCount(n);
            cursorKey = key;
            cursorIdx = end;
            return Page.fromRoot(root);
        }

        @Override
        public Optional<String> getPartitionKey(String schema, String table) {
            return Optional.empty();
        }

        @Override
        public long getExactRowCount(String schema, String table) {
            List<Object[]> rows = data.get(table);
            return rows == null ? 0 : rows.size();
        }

        @Override
        public TableStats getTableStats(String catalog, String schema, String table) {
            long rows = getExactRowCount(schema, table);
            return new TableStats(catalog, schema, table, rows, rows * 32L, List.of());
        }

        @Override
        public List<ColumnStats> getColumnStats(String catalog, String schema, String table) {
            return List.of();
        }

        @Override
        public void write(WriteRequest request) {
        }

        @Override
        public boolean catalogExists(String catalog) { return this.catalog.equals(catalog); }

        @Override
        public Optional<Catalog> getCatalog(String catalog) { return Optional.empty(); }

        @Override
        public List<String> listCatalogs() { return List.of(catalog); }

        @Override
        public boolean schemaExists(String catalog, String schema) { return catalogExists(catalog); }

        @Override
        public Optional<Schema> getSchema(String catalog, String schema) { return Optional.empty(); }

        @Override
        public List<String> listSchemas(String catalog) {
            return catalogExists(catalog) ? List.of(SCHEMA) : List.of();
        }

        @Override
        public boolean tableExists(String catalog, String schema, String table) {
            return data.containsKey(table);
        }

        @Override
        public Optional<Table> getTable(String catalog, String schema, String table) {
            return Optional.empty();
        }

        @Override
        public List<Column> getTableColumns(String catalog, String schema, String table) {
            List<Column> out = new ArrayList<>();
            for (Field f : schemaFor(table)) {
                String type = f.getType() instanceof ArrowType.Utf8 ? "varchar" : "bigint";
                out.add(new Column(f.getName(), type, null, null));
            }
            return out;
        }

        @Override
        public List<String> listTables(String catalog, String schema) {
            return catalogExists(catalog) ? new ArrayList<>(data.keySet()) : List.of();
        }

        @Override
        public boolean columnExists(String catalog, String schema, String table, String column) {
            return schemaFor(table).stream().anyMatch(f -> f.getName().equalsIgnoreCase(column));
        }

        @Override
        public Optional<Column> getColumn(String catalog, String schema, String table, String column) {
            return Optional.empty();
        }
    }
}
