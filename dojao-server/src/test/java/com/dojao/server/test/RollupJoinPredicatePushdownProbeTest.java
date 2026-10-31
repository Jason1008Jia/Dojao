package com.dojao.server.test;

import com.dojao.common.data.Page;
import com.dojao.core.planner.PhysicalPlan;
import com.dojao.core.planner.PlanningContext;
import com.dojao.core.planner.Planner;
import com.dojao.core.runtime.master.WorkerRegistry;
import com.dojao.core.runtime.worker.ResourceSummary;
import com.dojao.core.runtime.worker.WorkerNode;
import com.dojao.core.runtime.worker.WorkerState;
import com.dojao.grammar.analyzer.SemanticAnalyzer;
import com.dojao.grammar.analyzer.plan.LogicalPlan;
import com.dojao.grammar.optimize.Optimizer;
import com.dojao.metastore.MetaStore;
import com.dojao.metastore.MetaStoreType;
import com.dojao.server.master.SqlValidator;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * <b>ROLLUP + 多表 JOIN 的谓词下推探针</b>（离线，无集群 / 无数据库）。
 *
 * <p>复刻用户 2026-10-29 报障：</p>
 * <pre>
 *   SELECT c.category_name, u.member_level, count(DISTINCT b.user_id), sum(b.amount), avg(b.stay_seconds)
 *   FROM hive.b b JOIN mysql.u u ON b.user_id=u.user_id
 *               LEFT JOIN mysql.c c ON b.category_id=c.category_id
 *   WHERE b.event_type='page_view' AND b.dt='2026-09-20'
 *   GROUP BY ROLLUP(c.category_name, u.member_level)
 * </pre>
 * <p>原实现按分组集合展开成 3 条腿（EXPLAIN 里 3 个 {@code DistributedJoinShuffledAggregation}
 * 叶子、b 表扫 3 次）。现改为<b>一趟扫描</b>形态：单条腿 + {@code ExpandOperator}，EXPLAIN 只有
 * 1 个叶子，{@code __grouping_id} 进分组键。本探针断言「谓词仍随派发载荷下发 + 只扫一次 + 展开
 * 描述确实随算子下到 Worker」。</p>
 *
 * @author jiaxin
 * @date 2026/10/29
 */
public class RollupJoinPredicatePushdownProbeTest {

    private static final String CATALOG = "mem";
    private static final String SCHEMA = "mem";
    private static final String B = "b_fact";
    private static final String U = "u_dim";
    private static final String C = "c_dim";

    private static int pass = 0;
    private static int fail = 0;

    public static void main(String[] args) {
        System.err.println("========== ROLLUP + JOIN 谓词下推 探针 ==========");
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        java.nio.file.Path spill = null;
        try {
            spill = java.nio.file.Files.createTempDirectory("dojao-rollup-pushdown-probe");
            System.setProperty("query.spill.dir", spill.toString());
            Map<String, List<Object[]>> data = data();
            MetaStore metaStore = new MetaStore(MetaStoreType.LOCAL);
            metaStore.registerConnector(CATALOG, new MemConnector(CATALOG, data, allocator,
                    new CopyOnWriteArrayList<>()));
            registerWorkers();

            run(metaStore, data, allocator);
        } catch (Throwable e) {
            e.printStackTrace();
            fail++;
        } finally {
            allocator.close();
            if (spill != null) deleteRecursively(spill);
        }
        System.err.printf("========== 结果: %d PASS / %d FAIL ==========%n", pass, fail);
        if (fail > 0) System.exit(1);
    }

    private static void run(MetaStore metaStore, Map<String, List<Object[]>> data,
                            BufferAllocator allocator) {
        String sql = "SELECT c.category_name, u.member_level,"
                + " count(DISTINCT b.user_id) AS uv, sum(b.amount) AS gmv,"
                + " avg(b.stay_seconds) AS avg_stay"
                + " FROM " + CATALOG + "." + SCHEMA + "." + B + " b"
                + " JOIN " + CATALOG + "." + SCHEMA + "." + U + " u ON b.user_id = u.user_id"
                + " LEFT JOIN " + CATALOG + "." + SCHEMA + "." + C + " c ON b.category_id = c.category_id"
                + " WHERE b.event_type = 'page_view' AND b.dt = '2026-09-20'"
                + " GROUP BY ROLLUP(c.category_name, u.member_level)"
                + " ORDER BY gmv DESC";

        LogicalPlan logical = new SqlValidator(metaStore).validate(sql);

        PhysicalPlan localPlan = plan(logical, data, allocator, false);
        String local = localPlan.explainText();
        System.err.println("---------- S1 单机计划 ----------\n" + indent(local));
        check("S1a 单机计划里 b 表两条谓词都存在于扫描之上（Filter[dt]/Filter[event_type]），实际 "
                        + (local.contains("Filter[dt = '2026-09-20']") && local.contains("Filter[event_type = 'page_view']")),
                local.contains("Filter[dt = '2026-09-20']")
                        && local.contains("Filter[event_type = 'page_view']"));
        check("S1b 单机计划走一趟扫描形态（出现 Expand[...] 节点，实际 "
                        + (local.contains("Expand[") ? "有" : "无") + "）", local.contains("Expand["));
        check("S1c b 表只扫一次（展开形态），实际扫描次数="
                        + countLines(local, "ParallelTableScan[mem." + B + "]"),
                countLines(local, "ParallelTableScan[mem." + B + "]") == 1);

        PhysicalPlan distPlan = plan(logical, data, allocator, true);
        String dist = distPlan.explainText();
        System.err.println("---------- S2 分布式计划 ----------\n" + indent(dist));

        long detailLines = countLines(dist, "DistributedJoinShuffledAggregation[");
        check("S2a EXPLAIN 里只有 1 个分布式聚合叶子（不再按腿复制），实际=" + detailLines,
                detailLines == 1);
        check("S2b EXPLAIN 明细打印 expand: groupingSets=...（表只扫一次）",
                dist.contains("expand: groupingSets="));
        long filterLines = dist.lines().filter(l -> l.contains("filter=["))
                .filter(l -> l.contains("dt") && l.contains("2026-09-20")).count();
        check("S2c probe 侧 EXPLAIN 明细都打印 filter=[...dt...page_view...]，实际 " + filterLines
                + "（期望 1）", filterLines == 1);

        List<com.dojao.core.operator.DistributedJoinAggregationOperator> ops = new ArrayList<>();
        collect(distPlan.root(), ops);
        check("S2d 分布式聚合算子数=" + ops.size(), ops.size() == 1);
        check("S2e 算子的 probe 侧（b 表）firstFilter 含 dt + page_view，实际 "
                        + ops.stream().map(com.dojao.core.operator.DistributedJoinAggregationOperator::firstFilter)
                        .toList(),
                !ops.isEmpty() && ops.stream().allMatch(o -> o.firstFilter() != null
                        && o.firstFilter().contains("dt") && o.firstFilter().contains("2026-09-20")
                        && o.firstFilter().contains("page_view")));
        check("S2f 维表 u/c 不下推任何谓词（它们本来就没有单表 WHERE）",
                ops.stream().flatMap(o -> o.links().stream())
                        .noneMatch(l -> l.rightFilter() != null && !l.rightFilter().isBlank()));
        check("S2g 展开描述随算子下发：3 个分组集合 + 2 个维度，实际 "
                        + (ops.isEmpty() || ops.get(0).expand() == null ? "null"
                        : ops.get(0).expand().groupingSets().size() + " sets " + ops.get(0).expand().dimColumns()),
                !ops.isEmpty() && ops.get(0).expand() != null
                        && ops.get(0).expand().groupingSets().size() == 3
                        && ops.get(0).expand().dimColumns().size() == 2);
        check("S2h 分组键末尾带 __grouping_id，实际 " + (ops.isEmpty() ? "-" : ops.get(0).groupKeys()),
                !ops.isEmpty() && ops.get(0).groupKeys().contains("__grouping_id"));
    }

    private static long countLines(String text, String needle) {
        return text.lines().filter(l -> l.contains(needle)).count();
    }

    private static void collect(com.dojao.core.planner.PhysicalNode node,
                                List<com.dojao.core.operator.DistributedJoinAggregationOperator> out) {
        if (node.operator() instanceof com.dojao.core.operator.DistributedJoinAggregationOperator op) {
            out.add(op);
        }
        for (com.dojao.core.planner.PhysicalNode child : node.children()) collect(child, out);
    }

    private static PhysicalPlan plan(LogicalPlan logical, Map<String, List<Object[]>> data,
                                     BufferAllocator allocator, boolean distributed) {
        PlanningContext context = PlanningContext.builder(
                        catalog -> new MemConnector(CATALOG, data, allocator, new CopyOnWriteArrayList<>()))
                .distributedExecution(distributed)
                .memoryLimit(256L * 1024 * 1024)
                .parallelism(distributed ? 4 : 2)
                .build();
        return new Planner(context).plan(logical);
    }

    private static void registerWorkers() {
        WorkerRegistry registry = WorkerRegistry.get();
        for (int i = 0; i < 3; i++) {
            String id = "W" + i;
            registry.register(new WorkerNode(id, "localhost", 0, "probe", System.nanoTime()));
            registry.updateHeartbeat(id, WorkerState.IDLE,
                    new ResourceSummary(4, 10, 4L * 1024 * 1024 * 1024,
                            2L * 1024 * 1024 * 1024, 0L, 0L, 4, System.nanoTime(), 0L, 0L, 0));
        }
    }

    // ==================== 数据 / 连接器 ====================

    private static Map<String, List<Object[]>> data() {
        Map<String, List<Object[]>> data = new LinkedHashMap<>();
        List<Object[]> b = new ArrayList<>();
        // user_id, event_type, dt, category_id, amount, stay_seconds
        b.add(new Object[]{1L, "page_view", "2026-09-20", 10L, 10.0, 5.0});
        b.add(new Object[]{2L, "page_view", "2026-09-20", 20L, 20.0, 7.0});
        b.add(new Object[]{3L, "page_view", "2026-09-20", 10L, 30.0, 9.0});
        b.add(new Object[]{4L, "page_view", "2026-09-19", 10L, 40.0, 1.0});   // 其它分区
        b.add(new Object[]{5L, "click", "2026-09-20", 20L, 50.0, 2.0});       // 其它事件
        data.put(B, b);

        List<Object[]> u = new ArrayList<>();
        u.add(new Object[]{1L, "gold"});
        u.add(new Object[]{2L, "silver"});
        u.add(new Object[]{3L, "gold"});
        data.put(U, u);

        List<Object[]> c = new ArrayList<>();
        c.add(new Object[]{10L, "数码"});
        c.add(new Object[]{20L, "家电"});
        data.put(C, c);
        return data;
    }

    private static final class MemConnector implements DojaoConnector {

        private final String catalog;
        private final Map<String, List<Object[]>> data;
        private final BufferAllocator allocator;
        private final List<String> filterRequests;
        private static final int READ_BATCH = 100;

        MemConnector(String catalog, Map<String, List<Object[]>> data,
                     BufferAllocator allocator, List<String> filterRequests) {
            this.catalog = catalog;
            this.data = data;
            this.allocator = allocator;
            this.filterRequests = filterRequests;
        }

        @Override
        public boolean supportsPredicatePushdown() {
            return false;
        }

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
            recordFilter(request);
            return List.of(ScanSlice.whole(request));
        }

        private void recordFilter(ReadRequest request) {
            String expr = request.filterExpression().orElse(null);
            if (expr == null || expr.isBlank()) return;
            String entry = request.table() + ": " + expr;
            if (!filterRequests.contains(entry)) filterRequests.add(entry);
        }

        @Override
        public Page read(ReadRequest request) {
            recordFilter(request);
            List<Object[]> rows = data.getOrDefault(request.table(), List.of());
            List<Field> full = schemaFor(request.table()).getFields();
            List<Field> fields = sourceFields(catalog, request.schema(), request.table(),
                    request.columns().orElse(null));
            int n = rows.size();
            if (n == 0) return Page.eof();
            VectorSchemaRoot root = VectorSchemaRoot.create(
                    new org.apache.arrow.vector.types.pojo.Schema(fields), allocator);
            root.allocateNew();
            for (int r = 0; r < n; r++) {
                Object[] row = rows.get(r);
                for (int c = 0; c < fields.size(); c++) {
                    String name = fields.get(c).getName();
                    int idx = indexOf(full, name);
                    setValue(root.getVector(c), r, idx < 0 ? null : row[idx]);
                }
            }
            root.setRowCount(n);
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
            Field user = new Field("user_id", FieldType.nullable(new ArrowType.Int(64, true)), null);
            Field amount = new Field("amount", FieldType.nullable(
                    new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);
            Field stay = new Field("stay_seconds", FieldType.nullable(
                    new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);
            Field event = new Field("event_type", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
            Field dt = new Field("dt", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
            Field catId = new Field("category_id", FieldType.nullable(new ArrowType.Int(64, true)), null);
            return switch (table) {
                case B -> new org.apache.arrow.vector.types.pojo.Schema(
                        List.of(user, event, dt, catId, amount, stay));
                case U -> new org.apache.arrow.vector.types.pojo.Schema(List.of(user,
                        new Field("member_level", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)));
                case C -> new org.apache.arrow.vector.types.pojo.Schema(List.of(catId,
                        new Field("category_name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)));
                default -> new org.apache.arrow.vector.types.pojo.Schema(List.of());
            };
        }

        @Override
        public Optional<String> getPartitionKey(String schema, String table) {
            return B.equals(table) ? Optional.of("dt") : Optional.empty();
        }

        @Override
        public long getExactRowCount(String schema, String table) {
            List<Object[]> rows = data.get(table);
            return rows == null ? 0 : rows.size();
        }

        @Override
        public TableStats getTableStats(String catalog, String schema, String table) {
            long rows = getExactRowCount(schema, table);
            return new TableStats(catalog, schema, table, rows, Math.max(rows, 1) * 32L, List.of());
        }

        @Override
        public List<ColumnStats> getColumnStats(String catalog, String schema, String table) {
            return List.of();
        }

        @Override
        public void write(WriteRequest request) {
        }

        @Override
        public boolean catalogExists(String catalog) {
            return this.catalog.equals(catalog);
        }

        @Override
        public Optional<Catalog> getCatalog(String catalog) {
            return Optional.empty();
        }

        @Override
        public List<String> listCatalogs() {
            return List.of(catalog);
        }

        @Override
        public boolean schemaExists(String catalog, String schema) {
            return catalogExists(catalog);
        }

        @Override
        public Optional<Schema> getSchema(String catalog, String schema) {
            return Optional.empty();
        }

        @Override
        public List<String> listSchemas(String catalog) {
            return List.of(SCHEMA);
        }

        @Override
        public boolean tableExists(String catalog, String schema, String table) {
            return data.containsKey(table);
        }

        @Override
        public Optional<com.dojao.spi.bean.Table> getTable(String catalog, String schema, String table) {
            return Optional.empty();
        }

        @Override
        public List<String> listTables(String catalog, String schema) {
            return new ArrayList<>(data.keySet());
        }

        @Override
        public Optional<com.dojao.spi.bean.Column> getColumn(String catalog, String schema,
                                                             String table, String column) {
            return Optional.empty();
        }

        @Override
        public boolean columnExists(String catalog, String schema, String table, String column) {
            return indexOf(schemaFor(table).getFields(), column) >= 0;
        }

        @Override
        public List<com.dojao.spi.bean.Column> getTableColumns(String catalog, String schema,
                                                               String table) {
            List<com.dojao.spi.bean.Column> out = new ArrayList<>();
            for (Field f : schemaFor(table).getFields()) {
                out.add(new com.dojao.spi.bean.Column(f.getName(), "string", null, null));
            }
            return out;
        }
    }

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
