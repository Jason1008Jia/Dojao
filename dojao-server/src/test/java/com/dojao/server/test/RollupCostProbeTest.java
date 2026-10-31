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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * <b>ROLLUP 改写的成本探针</b> —— 同一份数据上跑「单条 GROUP BY」vs「GROUP BY ROLLUP」，
 * 对比墙钟时间与源表读取次数（3 worker 的 in-JVM 集群）。
 *
 * <p>ROLLUP(2 维) 被 {@code Planner.buildGroupingSetsUnion} 展开成 3 条腿，每条腿都是一份
 * 完整的分布式 join + 聚合 ⇒ 源表被扫 3 次、join 做 3 次。本探针把这个倍数<b>量出来</b>，
 * 用于回答「ROLLUP 慢是不是因为没下推」——下推是好的（谓词随每条腿下发），代价在腿的重复。</p>
 *
 * @author jiaxin
 * @date 2026/10/29
 */
public class RollupCostProbeTest {

    private static final String CATALOG = "mem";
    private static final String SCHEMA = "mem";
    private static final String B = "b_fact";
    private static final String U = "u_dim";
    private static final String C = "c_dim";

    private static final int FACT_ROWS = 1_000_000;
    private static final int DAYS = 100;
    private static final int USERS = 200_000;
    private static final int CATEGORIES = 8;

    private static final AtomicLong FACT_READS = new AtomicLong();
    private static final AtomicLong FACT_ROWS_SCANNED = new AtomicLong();

    public static void main(String[] args) {
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        java.nio.file.Path spill = null;
        try {
            spill = java.nio.file.Files.createTempDirectory("dojao-rollup-cost-probe");
            System.setProperty("query.spill.dir", spill.toString());

            String select = "SELECT c.category_name, u.member_level,"
                    + " count(DISTINCT b.user_id) AS uv, sum(b.amount) AS gmv,"
                    + " avg(b.stay_seconds) AS avg_stay"
                    + " FROM " + CATALOG + "." + SCHEMA + "." + B + " b"
                    + " JOIN " + CATALOG + "." + SCHEMA + "." + U + " u ON b.user_id = u.user_id"
                    + " LEFT JOIN " + CATALOG + "." + SCHEMA + "." + C + " c ON b.category_id = c.category_id"
                    + " WHERE b.event_type = 'page_view' AND b.dt = '2026-09-20'";

            String single = select + " GROUP BY c.category_name, u.member_level";
            String rollup = select + " GROUP BY ROLLUP(c.category_name, u.member_level)";

            measure("A 单条 GROUP BY（1 条腿）", single, allocator);
            measure("B GROUP BY ROLLUP（3 条腿）", rollup, allocator);
        } catch (Throwable t) {
            t.printStackTrace();
        } finally {
            allocator.close();
            if (spill != null) deleteRecursively(spill);
        }
    }

    private static void measure(String label, String sql, BufferAllocator allocator) {
        Map<String, WorkerManager> workerMap = new HashMap<>();
        List<String> workerIds = new ArrayList<>();
        ConcurrentHashMap<String, ConcurrentHashMap<String, ShuffleReader>> bus = new ConcurrentHashMap<>();
        MasterGateway noopMaster = new MasterGateway() {
            @Override public boolean register(WorkerNode info) { return true; }
            @Override public boolean heartbeat(WorkerNode info, WorkerState state, ResourceSummary summary) { return true; }
            @Override public boolean unregister(WorkerNode info) { return true; }
        };
        for (String id : new String[]{"W0", "W1", "W2"}) {
            WorkerNode node = new WorkerNode(id, "localhost", 0, "probe", System.nanoTime());
            workerMap.put(id, new WorkerManager(node, noopMaster, 60_000, 4,
                    () -> new ResourceSummary(4, 10, 8L * 1024 * 1024 * 1024,
                            3L * 1024 * 1024 * 1024, 0, 0, 4, System.nanoTime(), 0L, 0L, 0),
                    new ExchangeManager(id, bus)));
        }
        com.dojao.core.runtime.worker.WorkerGateway gateway =
                new LocalWorkerGateway(catalog -> new GenConnector(allocator), workerMap);
        WorkerRegistry registry = WorkerRegistry.get();
        for (String id : workerMap.keySet()) {
            registry.register(new WorkerNode(id, "localhost", 0, "probe", System.nanoTime()));
            registry.updateHeartbeat(id, WorkerState.IDLE,
                    new ResourceSummary(4, 10, 8L * 1024 * 1024 * 1024,
                            3L * 1024 * 1024 * 1024, 0, 0, 4, System.nanoTime(), 0L, 0L, 0));
            workerIds.add(id);
        }
        FACT_READS.set(0);
        FACT_ROWS_SCANNED.set(0);
        try {
            MetaStore metaStore = new MetaStore(MetaStoreType.LOCAL);
            metaStore.registerConnector(CATALOG, new GenConnector(allocator));
            LogicalPlan logical = new SqlValidator(metaStore).validate(sql);
            PlanningContext context = PlanningContext.builder(catalog -> new GenConnector(allocator))
                    .distributedExecution(true)
                    .workerGateway(gateway)
                    .memoryLimit(256L * 1024 * 1024)
                    .parallelism(4)
                    .build();
            PhysicalPlan plan = new Planner(context).plan(logical);

            long t0 = System.nanoTime();
            long rows = 0;
            Operator op = plan.operator();
            try {
                op.open();
                Page p;
                while (!(p = op.next()).isEOF()) {
                    rows += p.getRoot() == null ? 0 : p.getRoot().getRowCount();
                    p.close();
                }
            } finally {
                try { op.close(); } catch (Throwable ignored) { }
            }
            long ms = (System.nanoTime() - t0) / 1_000_000;
            System.err.printf("[%s] 结果行=%d 耗时=%dms 源表 read() 次数=%d 扫描行=%d%n",
                    label, rows, ms, FACT_READS.get(), FACT_ROWS_SCANNED.get());
        } catch (Throwable t) {
            System.err.println("[" + label + "] 失败: " + t);
            t.printStackTrace();
        } finally {
            for (String id : workerIds) registry.unregister(id);
            for (WorkerManager wm : workerMap.values()) {
                try { wm.stop(); } catch (Throwable ignored) { }
            }
        }
    }

    // ==================== 生成式内存连接器 ====================

    private static final class GenConnector implements DojaoConnector {

        private final BufferAllocator allocator;
        private final Map<String, Integer> cursors = new ConcurrentHashMap<>();
        private static final int BATCH = 8192;

        GenConnector(BufferAllocator allocator) {
            this.allocator = allocator;
        }

        @Override public boolean supportsPredicatePushdown() { return false; }

        @Override
        public List<Field> sourceFields(String catalog, String schema, String table, List<String> columns) {
            List<Field> full = schemaFor(table).getFields();
            if (columns == null || columns.isEmpty()) return full;
            List<Field> out = new ArrayList<>();
            for (String name : columns) {
                for (Field f : full) if (f.getName().equalsIgnoreCase(name)) { out.add(f); break; }
            }
            return out;
        }

        @Override
        public List<ScanSlice> planSplits(ReadRequest request, int parallelism) {
            return List.of(ScanSlice.whole(request));
        }

        @Override
        public Page read(ReadRequest request) {
            String table = request.table();
            int total = rowCount(table);
            String key = table + "|" + request.columns().orElse(List.of("*"));
            int from = cursors.getOrDefault(key, 0);
            if (from >= total) return Page.eof();
            int n = Math.min(BATCH, total - from);
            List<Field> fields = sourceFields(CATALOG, SCHEMA, table, request.columns().orElse(null));
            VectorSchemaRoot root = VectorSchemaRoot.create(
                    new org.apache.arrow.vector.types.pojo.Schema(fields), allocator);
            root.allocateNew();
            for (int i = 0; i < n; i++) {
                int row = from + i;
                for (int c = 0; c < fields.size(); c++) {
                    String name = fields.get(c).getName();
                    setValue(root.getVector(c), i, name, row, table);
                }
            }
            root.setRowCount(n);
            cursors.put(key, from + n);
            if (B.equals(table)) {
                FACT_READS.incrementAndGet();
                FACT_ROWS_SCANNED.addAndGet(n);
            }
            return Page.fromRoot(root);
        }

        private static int rowCount(String table) {
            return switch (table) {
                case B -> FACT_ROWS;
                case U -> USERS;
                case C -> CATEGORIES;
                default -> 0;
            };
        }

        private static void setValue(FieldVector v, int i, String name, int row, String table) {
            switch (name) {
                case "user_id" -> ((BigIntVector) v).setSafe(i,
                        1 + ((long) row * 7919) % USERS);
                case "category_id" -> ((BigIntVector) v).setSafe(i, 1 + (row * 31) % CATEGORIES);
                case "member_level" -> ((VarCharVector) v).setSafe(i,
                        ("L" + (row % 5)).getBytes(StandardCharsets.UTF_8));
                case "category_name" -> ((VarCharVector) v).setSafe(i,
                        ("cat" + row).getBytes(StandardCharsets.UTF_8));
                case "event_type" -> ((VarCharVector) v).setSafe(i,
                        (row % 7 == 0 ? "click" : "page_view").getBytes(StandardCharsets.UTF_8));
                case "dt" -> ((VarCharVector) v).setSafe(i,
                        ("2026-09-" + String.format("%02d", 1 + row % DAYS)).getBytes(StandardCharsets.UTF_8));
                case "amount" -> ((Float8Vector) v).setSafe(i, (row % 1000) + 0.5);
                case "stay_seconds" -> ((Float8Vector) v).setSafe(i, (row % 500) + 1);
                default -> v.setNull(i);
            }
        }

        private static org.apache.arrow.vector.types.pojo.Schema schemaFor(String table) {
            FieldType longT = FieldType.nullable(new ArrowType.Int(64, true));
            FieldType dblT = FieldType.nullable(
                    new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
            FieldType strT = FieldType.nullable(ArrowType.Utf8.INSTANCE);
            return switch (table) {
                case B -> new org.apache.arrow.vector.types.pojo.Schema(List.of(
                        new Field("user_id", longT, null),
                        new Field("event_type", strT, null),
                        new Field("dt", strT, null),
                        new Field("category_id", longT, null),
                        new Field("amount", dblT, null),
                        new Field("stay_seconds", dblT, null)));
                case U -> new org.apache.arrow.vector.types.pojo.Schema(List.of(
                        new Field("user_id", longT, null),
                        new Field("member_level", strT, null)));
                case C -> new org.apache.arrow.vector.types.pojo.Schema(List.of(
                        new Field("category_id", longT, null),
                        new Field("category_name", strT, null)));
                default -> new org.apache.arrow.vector.types.pojo.Schema(List.of());
            };
        }

        @Override public Optional<String> getPartitionKey(String schema, String table) {
            return B.equals(table) ? Optional.of("dt") : Optional.empty();
        }
        @Override public long getExactRowCount(String schema, String table) { return rowCount(table); }
        @Override public TableStats getTableStats(String catalog, String schema, String table) {
            long rows = rowCount(table);
            return new TableStats(catalog, schema, table, rows, Math.max(rows, 1) * 32L, List.of());
        }
        @Override public List<ColumnStats> getColumnStats(String c, String s, String t) { return List.of(); }
        @Override public void write(WriteRequest request) { }
        @Override public boolean catalogExists(String catalog) { return CATALOG.equals(catalog); }
        @Override public Optional<Catalog> getCatalog(String catalog) { return Optional.empty(); }
        @Override public List<String> listCatalogs() { return List.of(CATALOG); }
        @Override public boolean schemaExists(String catalog, String schema) { return true; }
        @Override public Optional<Schema> getSchema(String catalog, String schema) { return Optional.empty(); }
        @Override public List<String> listSchemas(String catalog) { return List.of(SCHEMA); }
        @Override public boolean tableExists(String catalog, String schema, String table) {
            return rowCount(table) > 0;
        }
        @Override public Optional<com.dojao.spi.bean.Table> getTable(String c, String s, String t) {
            return Optional.empty();
        }
        @Override public List<String> listTables(String catalog, String schema) {
            return List.of(B, U, C);
        }
        @Override public boolean columnExists(String c, String s, String t, String col) {
            for (Field f : schemaFor(t).getFields()) if (f.getName().equalsIgnoreCase(col)) return true;
            return false;
        }
        @Override public Optional<com.dojao.spi.bean.Column> getColumn(String c, String s, String t, String col) {
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

    private static void deleteRecursively(java.nio.file.Path dir) {
        try (var walk = java.nio.file.Files.walk(dir)) {
            walk.sorted(java.util.Comparator.reverseOrder()).forEach(f -> {
                try { java.nio.file.Files.deleteIfExists(f); } catch (Exception ignored) { }
            });
        } catch (Exception ignored) { }
    }
}
