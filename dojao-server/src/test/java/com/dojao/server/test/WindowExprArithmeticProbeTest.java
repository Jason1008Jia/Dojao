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
 * <b>P0 端到端复现探针</b>：BUG-1「窗口函数参与二元算术，结果退化为窗口函数值」。
 *
 * <pre>
 *   SELECT gmv / sum(gmv) OVER () FROM sales          -- 曾静默返回 sum(gmv) OVER () 的值
 *   SELECT gmv - lag(gmv) OVER (ORDER BY id) FROM ... -- 环比同样失效
 * </pre>
 *
 * <p><b>根因</b>：语义分析期 {@code SemanticAnalyzer} 把「表达式里捕获到的窗口函数」直接当成
 * 整个 SELECT 项，外层的 {@code + - * /} 全部被静默丢弃（不报错）。现在改为：完整表达式原文
 * 保留 + 窗口调用清单附在 {@code SelectItem.windowCalls()}，Planner 把每个窗口物化成合成列
 * {@code __win_expr_<项>_<窗口>} 后由标量投影回写求值。</p>
 *
 * <p>场景（数据 = 12 行，{@code gmv = id*10}，合计 780）：</p>
 * <ul>
 *   <li>S1：报告里的 8 个表达式（{@code + - * /}、左右顺序、括号）+ 纯列运算与单独窗口函数；</li>
 *   <li>S2：{@code gmv - lag(...) OVER (ORDER BY id)}（环比）与「两个窗口同表达式」；</li>
 *   <li>S3：GROUP BY 下窗口在聚合之后求值（{@code s / sum(s) OVER ()}，s = sum(gmv) 别名）；</li>
 *   <li>S4：派生表里算占比、外层按占比过滤。</li>
 * </ul>
 *
 * <pre>
 *   java --add-opens=java.base/java.nio=ALL-UNNAMED -cp "…" \
 *        -Dquery.result.watchdogMs=30000 com.dojao.server.test.WindowExprArithmeticProbeTest
 * </pre>
 */
public final class WindowExprArithmeticProbeTest {

    private static final String CATALOG = "mem";
    private static final String SCHEMA = "mem";
    private static final String[] WORKERS = {"W0", "W1", "W2"};

    /** 12 行，gmv = id*10 ⇒ 合计 780.0。 */
    private static final long[] GMV = {10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120};
    private static final double TOTAL = 780.0;

    /** S1：报告里同一 SQL 的 8 个表达式（唯一区别 = 每个根窗口都起了别名，避免重复列名）。 */
    private static final String S1_SQL =
            "SELECT id, gmv,"
                    + " sum(gmv) OVER () AS tot,"
                    + " gmv / sum(gmv) OVER () AS pct,"
                    + " 100.0 * gmv / sum(gmv) OVER () AS pct100,"
                    + " gmv - sum(gmv) OVER () AS diffv,"
                    + " sum(gmv) OVER () - gmv AS revdiff,"
                    + " gmv + sum(gmv) OVER () AS addv,"
                    + " gmv * sum(gmv) OVER () AS mult,"
                    + " (sum(gmv) OVER ()) / gmv AS divrev,"
                    + " gmv / 2 AS half"
                    + " FROM " + CATALOG + "." + SCHEMA + ".sales"
                    + " ORDER BY id";

    /** S2：环比（lag 带默认值，首行为 gmv-0）与「同一表达式里两个窗口」。 */
    private static final String S2_SQL =
            "SELECT id, gmv,"
                    + " gmv - lag(gmv, 1, 0) OVER (ORDER BY id) AS step,"
                    + " sum(gmv) OVER () - lag(gmv, 1, 0) OVER (ORDER BY id) AS two"
                    + " FROM " + CATALOG + "." + SCHEMA + ".sales"
                    + " ORDER BY id";

    /**
     * S3：聚合结果参与窗口算术（GROUP BY 结果先物化成派生列，再按占比求值）。
     *
     * <p>同层直接写 {@code sum(gmv) AS s, s / sum(s) OVER ()} 是「SELECT 别名在 SELECT 表达式里
     * 复用」—— 分析期就不允许（窗口只允许在 OVER 子句内引用同层别名），与本缺陷无关。</p>
     */
    private static final String S3_SQL =
            "SELECT dept, s, s / sum(s) OVER () AS share FROM ("
                    + " SELECT dept, sum(gmv) AS s"
                    + " FROM " + CATALOG + "." + SCHEMA + ".sales GROUP BY dept"
                    + ") t ORDER BY dept";

    /** S4：派生表里算占比，外层按占比过滤（pct = id/78 > 0.05 ⇒ id ≥ 4）。 */
    private static final String S4_SQL =
            "SELECT * FROM ("
                    + " SELECT id, gmv / sum(gmv) OVER () AS pct"
                    + " FROM " + CATALOG + "." + SCHEMA + ".sales"
                    + ") t WHERE pct > 0.05 ORDER BY id";

    /** S5：窗口嵌在普通函数参数里（round(gmv / sum(gmv) OVER (), 4)）。 */
    private static final String S5_SQL =
            "SELECT id, round(gmv / sum(gmv) OVER (), 4) AS pct4"
                    + " FROM " + CATALOG + "." + SCHEMA + ".sales"
                    + " ORDER BY id";

    /** S6：星号展开 + 窗口算术（* 与合成列共存，敏感列不得被裁掉）。 */
    private static final String S6_SQL =
            "SELECT *, gmv / sum(gmv) OVER () AS pct"
                    + " FROM " + CATALOG + "." + SCHEMA + ".sales"
                    + " ORDER BY id";

    private static int failures = 0;
    private static int checks = 0;

    public static void main(String[] args) {
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        Map<String, List<Object[]>> data = buildData();
        PlanningContext.ConnectorProvider provider = catalog -> new InMemoryConnector(catalog, data, allocator);

        ConcurrentHashMap<String, ConcurrentHashMap<String, ShuffleReader>> bus = new ConcurrentHashMap<>();
        Map<String, WorkerManager> workerMap = new HashMap<>();
        MasterGateway noopMaster = new MasterGateway() {
            @Override public boolean register(WorkerNode info) { return true; }
            @Override public boolean heartbeat(WorkerNode info, WorkerState state, ResourceSummary summary) { return true; }
            @Override public boolean unregister(WorkerNode info) { return true; }
        };
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

            runCase("S1：8 个表达式（+ - * /、左右顺序、括号）", S1_SQL, metaStore, context, WindowExprArithmeticProbeTest::checkS1);
            runCase("S2：环比 gmv - lag(...) 与双窗口表达式", S2_SQL, metaStore, context, WindowExprArithmeticProbeTest::checkS2);
            runCase("S3：聚合结果 s / sum(s) OVER ()（派生表）", S3_SQL, metaStore, context, WindowExprArithmeticProbeTest::checkS3);
            runCase("S4：派生表内算占比 + 外层按占比过滤", S4_SQL, metaStore, context, WindowExprArithmeticProbeTest::checkS4);
            runCase("S5：窗口嵌在普通函数参数里 round(...)", S5_SQL, metaStore, context, WindowExprArithmeticProbeTest::checkS5);
            runCase("S6：星号展开 + 窗口算术", S6_SQL, metaStore, context, WindowExprArithmeticProbeTest::checkS6);
        } finally {
            for (String workerId : WORKERS) registry.unregister(workerId);
            for (WorkerManager wm : workerMap.values()) {
                try { wm.stop(); } catch (Throwable ignored) { }
            }
            allocator.close();
        }
        System.out.println("\n==== WindowExprArithmeticProbeTest: " + failures
                + " FAIL / " + checks + " checks ====");
        if (failures > 0) System.exit(1);
    }

    // ==================== 场景执行 ====================

    private record Result(List<String> columns, List<List<Object>> rows, String explain) {}

    private interface Checker { void check(Result r); }

    private static void runCase(String name, String sql, MetaStore metaStore,
                                PlanningContext context, Checker checker) {
        System.out.println("\n---------- " + name + " ----------");
        System.out.println(sql);
        Result r = execute(sql, metaStore, context);
        if (r == null) {
            failures++;
            return;
        }
        System.out.println("物理计划:\n" + r.explain());
        System.out.println("输出列 = " + r.columns() + "，行数 = " + r.rows().size());
        for (List<Object> row : r.rows()) System.out.println("  " + row);
        try {
            checker.check(r);
        } catch (Throwable t) {
            System.out.println("[ERROR] 断言异常: " + t);
            t.printStackTrace(System.out);
            failures++;
        }
    }

    private static Result execute(String sql, MetaStore metaStore, PlanningContext context) {
        LogicalPlan logical;
        try {
            logical = new SqlValidator(metaStore).validate(sql);
        } catch (Throwable e) {
            System.out.println("[ERROR] 语义分析失败: " + e);
            e.printStackTrace(System.out);
            return null;
        }
        PhysicalPlan plan;
        try {
            plan = new Planner(context).plan(logical);
        } catch (Throwable e) {
            System.out.println("[ERROR] 物理规划失败: " + e);
            e.printStackTrace(System.out);
            return null;
        }
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
        } catch (Throwable t) {
            System.out.println("[ERROR] 执行失败（输出列 " + plan.outputColumns() + "）: " + t);
            t.printStackTrace(System.out);
            return null;
        } finally {
            try { op.close(); } catch (Throwable ignored) { }
        }
        return new Result(plan.outputColumns(), rows, plan.explainText());
    }

    // ==================== 断言 ====================

    private static void checkS1(Result r) {
        check("行数 = 12（窗口不改变行数）", r.rows().size() == 12);
        // 窗口里的 sum(gmv) 是窗口不是聚合：不应插 HashAggregation
        check("计划不含 HashAggregation（窗口内 sum 不被当成聚合）",
                !r.explain().contains("HashAggregation"));
        int idIdx = idx(r, "id"), gmvIdx = idx(r, "gmv"), totIdx = idx(r, "tot");
        int pctIdx = idx(r, "pct"), p100Idx = idx(r, "pct100"), diffIdx = idx(r, "diffv");
        int revIdx = idx(r, "revdiff"), addIdx = idx(r, "addv"), multIdx = idx(r, "mult");
        int divrevIdx = idx(r, "divrev"), halfIdx = idx(r, "half");
        boolean allPct = true, allP100 = true, allDiff = true, allRev = true;
        boolean allAdd = true, allMult = true, allDivRev = true, allHalf = true, allTot = true;
        for (List<Object> row : r.rows()) {
            double gmv = num(row.get(gmvIdx));
            allTot &= near(row.get(totIdx), TOTAL);
            allPct &= near(row.get(pctIdx), gmv / TOTAL);
            allP100 &= near(row.get(p100Idx), 100.0 * gmv / TOTAL);
            allDiff &= near(row.get(diffIdx), gmv - TOTAL);
            allRev &= near(row.get(revIdx), TOTAL - gmv);
            allAdd &= near(row.get(addIdx), gmv + TOTAL);
            allMult &= near(row.get(multIdx), gmv * TOTAL);
            allDivRev &= near(row.get(divrevIdx), TOTAL / gmv);
            allHalf &= near(row.get(halfIdx), gmv / 2.0);
        }
        check("sum(gmv) OVER () = 780（基线）", allTot);
        check("gmv / sum(gmv) OVER () = gmv/780（占比）", allPct);
        check("100.0 * gmv / sum(...) OVER () = 100*gmv/780", allP100);
        check("gmv - sum(...) OVER () = gmv-780", allDiff);
        check("sum(...) OVER () - gmv = 780-gmv（左窗口）", allRev);
        check("gmv + sum(...) OVER () = gmv+780", allAdd);
        check("gmv * sum(...) OVER () = gmv*780", allMult);
        check("(sum(...) OVER ()) / gmv = 780/gmv（括号）", allDivRev);
        check("gmv / 2 = gmv/2（纯列运算基线）", allHalf);
        if (r.rows().size() >= 12) {
            List<Object> first = r.rows().get(0);
            check("首行 id=1 / gmv=10（排除了乱序干扰）",
                    "1".equals(String.valueOf(first.get(idIdx)))
                            && near(first.get(gmvIdx), GMV[0]));
        }
    }

    private static void checkS2(Result r) {
        check("行数 = 12", r.rows().size() == 12);
        int gmvIdx = idx(r, "gmv"), stepIdx = idx(r, "step"), twoIdx = idx(r, "two");
        boolean allStep = true, allTwo = true;
        for (int i = 0; i < r.rows().size(); i++) {
            List<Object> row = r.rows().get(i);
            double prev = i == 0 ? 0.0 : GMV[i - 1];   // lag(gmv,1,0)：首行默认 0
            allStep &= near(row.get(stepIdx), GMV[i] - prev);
            allTwo &= near(row.get(twoIdx), TOTAL - prev);
        }
        check("gmv - lag(gmv,1,0) OVER (ORDER BY id) = 前一行差（首行 = gmv）", allStep);
        check("sum(gmv) OVER () - lag(...) = 780 - 前一行 gmv（两个窗口）", allTwo);
        // 退化检测：若整个表达式被换成窗口值，step 会**恒等于** lag 值（逐行 prev）。
        // 注意单看某一行可能巧合相等（第 2 行 10-10=10），必须按「全部行都相等」判定。
        boolean degraded = !r.rows().isEmpty();
        for (int i = 0; i < r.rows().size(); i++) {
            double prev = i == 0 ? 0.0 : GMV[i - 1];
            if (!near(r.rows().get(i).get(stepIdx), prev)) degraded = false;
        }
        check("未退化为 lag 值本身", !degraded);
        if (!r.rows().isEmpty()) {
            check("首行 step = 10（不是 NULL / 0）", near(r.rows().get(0).get(stepIdx), 10.0));
            check("首行 two = 780", near(r.rows().get(0).get(twoIdx), 780.0));
        }
        // 输出列必须至少包含 gmv 这样被外层引用的源列
        check("输出列含 gmv（外层算术操作数未被裁剪）", gmvIdx >= 0);
    }

    private static void checkS3(Result r) {
        check("行数 = 2（两个部门）", r.rows().size() == 2);
        int deptIdx = idx(r, "dept"), sIdx = idx(r, "s"), shareIdx = idx(r, "share");
        check("输出列含 dept / s / share", deptIdx >= 0 && sIdx >= 0 && shareIdx >= 0);
        Map<String, Double> sumOf = new HashMap<>();
        Map<String, Double> shareOf = new HashMap<>();
        for (List<Object> row : r.rows()) {
            sumOf.put(String.valueOf(row.get(deptIdx)), num(row.get(sIdx)));
            shareOf.put(String.valueOf(row.get(deptIdx)), num(row.get(shareIdx)));
        }
        // id 奇数 → B，偶数 → A：A = 20+40+…+120 = 420，B = 10+30+…+110 = 360
        check("A 组 sum(gmv) = 420", near(sumOf.get("A"), 420.0));
        check("B 组 sum(gmv) = 360", near(sumOf.get("B"), 360.0));
        check("A 组 s / sum(s) OVER () = 420/780", near(shareOf.get("A"), 420.0 / TOTAL));
        check("B 组 s / sum(s) OVER () = 360/780", near(shareOf.get("B"), 360.0 / TOTAL));
        check("share 两列之和 = 1（占比而非原始值）",
                near(sumOf.get("A") == null ? null : shareOf.get("A") + shareOf.get("B"), 1.0));
    }

    private static void checkS4(Result r) {
        check("行数 = 9（id 4..12 的 pct > 0.05）", r.rows().size() == 9);
        int idIdx = idx(r, "id"), pctIdx = idx(r, "pct");
        check("输出列含 id / pct", idIdx >= 0 && pctIdx >= 0);
        boolean allPct = true;
        boolean idsOk = true;
        for (List<Object> row : r.rows()) {
            long id = (long) num(row.get(idIdx));
            allPct &= near(row.get(pctIdx), id * 10.0 / TOTAL);
            idsOk &= id >= 4;
        }
        check("派生列 pct = gmv/780", allPct);
        check("外层过滤真的生效（id ≥ 4）", idsOk);
    }

    private static void checkS5(Result r) {
        check("行数 = 12", r.rows().size() == 12);
        int idIdx = idx(r, "id"), pctIdx = idx(r, "pct4");
        check("输出列含 id / pct4", idIdx >= 0 && pctIdx >= 0);
        boolean allOk = true;
        for (List<Object> row : r.rows()) {
            double id = num(row.get(idIdx));
            double expected = Math.round(id * 10.0 / TOTAL * 10000.0) / 10000.0;
            allOk &= near(row.get(pctIdx), expected);
        }
        check("round(gmv / sum(gmv) OVER (), 4) = round(gmv/780, 4)", allOk);
    }

    private static void checkS6(Result r) {
        check("行数 = 12", r.rows().size() == 12);
        check("输出列 = id / dept / gmv / pct（* 展开 + 合成列）",
                r.columns().size() == 4 && idx(r, "id") >= 0 && idx(r, "dept") >= 0
                        && idx(r, "gmv") >= 0 && idx(r, "pct") >= 0);
        int idIdx = idx(r, "id"), pctIdx = idx(r, "pct");
        boolean allOk = idIdx >= 0 && pctIdx >= 0;
        for (List<Object> row : r.rows()) {
            allOk &= near(row.get(pctIdx), num(row.get(idIdx)) * 10.0 / TOTAL);
        }
        check("* 场景下 pct 仍按 gmv/780 求值（源列未被裁掉）", allOk);
    }

    // ==================== 工具 ====================

    private static int idx(Result r, String name) {
        for (int i = 0; i < r.columns().size(); i++) {
            if (r.columns().get(i).equalsIgnoreCase(name)) return i;
        }
        return -1;
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
            if (!table.equals("sales")) throw new IllegalArgumentException("未知表: " + table);
            return List.of(
                    new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null),
                    new Field("dept", FieldType.nullable(new ArrowType.Utf8()), null),
                    new Field("gmv", FieldType.nullable(new ArrowType.Int(64, true)), null));
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
            String key = request.table() + "|" + (cols == null ? "*" : String.join(",", cols));
            if (!key.equals(cursorKey)) {
                cursorKey = key;
                cursorIdx = 0;
            }
            if (cursorIdx >= rows.size()) return Page.eof();
            List<Object[]> batch = new ArrayList<>();
            while (cursorIdx < rows.size() && batch.size() < READ_BATCH) {
                batch.add(rows.get(cursorIdx++));
            }
            VectorSchemaRoot root = VectorSchemaRoot.create(
                    new org.apache.arrow.vector.types.pojo.Schema(outFields), allocator);
            root.allocateNew();
            for (int r = 0; r < batch.size(); r++) {
                Object[] row = batch.get(r);
                for (int c = 0; c < pick.length; c++) {
                    int src = pick[c];
                    setValue(root.getVector(c), r, src < 0 || src >= row.length ? null : row[src]);
                }
            }
            root.setRowCount(batch.size());
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

        private static void setValue(FieldVector vec, int r, Object val) {
            if (val == null) {
                vec.setNull(r);
            } else if (vec instanceof BigIntVector bv) bv.setSafe(r, ((Number) val).longValue());
            else if (vec instanceof VarCharVector vv) vv.setSafe(r, val.toString().getBytes(StandardCharsets.UTF_8));
            else throw new IllegalStateException("内存连接器不支持的列类型: " + vec.getClass().getSimpleName());
        }
    }
}
