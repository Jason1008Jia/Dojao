package com.dojao.server.master;

import com.dojao.common.sql.SqlBatchRunner;
import com.dojao.common.sql.SqlSplitter;
import com.dojao.common.util.Errors;
import com.dojao.core.function.BuiltinFunctionCatalog;
import com.dojao.core.operator.DistributedTask;
import com.dojao.core.operator.OperatorState;
import com.dojao.core.planner.PlanningContext;
import com.dojao.core.planner.PhysicalNode;
import com.dojao.core.rpc.Column;
import com.dojao.core.rpc.QueryRequest;
import com.dojao.core.rpc.QueryResponse;
import com.dojao.core.rpc.Row;
import com.dojao.core.runtime.master.ResourceHistory;
import com.dojao.core.runtime.master.WorkerRegistration;
import com.dojao.core.runtime.master.WorkerRegistry;
import com.dojao.core.runtime.worker.ResourceSummary;
import com.dojao.core.runtime.worker.WorkerNode;
import com.dojao.core.runtime.worker.WorkerState;
import com.dojao.ui.monitor.MonitorCluster;
import com.dojao.ui.monitor.MonitorFunction;
import com.dojao.ui.monitor.MonitorHistory;
import com.dojao.ui.monitor.MonitorJob;
import com.dojao.ui.monitor.MonitorOperator;
import com.dojao.ui.monitor.MonitorSample;
import com.dojao.ui.monitor.MonitorQuery;
import com.dojao.ui.monitor.MonitorColumn;
import com.dojao.ui.monitor.MonitorResult;
import com.dojao.ui.monitor.MonitorSubmit;
import com.dojao.ui.monitor.MonitorTask;
import com.dojao.ui.monitor.MonitorWorker;
import com.dojao.ui.monitor.QueryMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link QueryMonitor} 的 Master 侧实现 —— 把 QueryManagerRegistry / WorkerRegistry /
 * 物理计划树 / 片段任务实时状态映射成 UI 中性 DTO。
 *
 * <p>三层职责：</p>
 * <ol>
 *   <li><b>监控读</b>：任务列表（Registry 活跃 + 最近完成）→ {@link MonitorJob}；
 *       单查询详情 → {@link QueryManager#snapshot()} 的算子 + 每算子分布式任务 → {@link MonitorQuery}
 *       （带 DAG 子节点下标，供前端画有向无环图）；集群 → {@link MonitorCluster}。</li>
 *   <li><b>SQL 异步执行</b>：经 {@link QueryServiceImpl#executeQuery(String, QueryRequest, java.util.function.Consumer)}
 *       以<b>外部预生成 queryId</b> 在共享执行线程池里真跑，结果预览<b>有界</b>缓存（仅留前 N 行），
 *       供 SQL 页分页 + 取消。取消 = {@link QueryManager#cancel()}，执行循环逐页检测停拉。</li>
 *   <li><b>取消</b>：{@link #cancel} 置 QueryManager CANCELED。</li>
 * </ol>
 *
 * <p>DAG 节点下标与 {@link QueryManager#measuredOperators()}（= snapshot operators 对齐列表）一致，
 * 每个算子的 {@code children} = 其物理计划树 child 中「可度量算子」的节点下标（同序）。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/6
 */
public final class UiQueryMonitor implements QueryMonitor {

    private static final Logger log = LoggerFactory.getLogger(UiQueryMonitor.class);

    /** 结果预览单查询最多缓存的行数（超出截断，SQL 页提示"仅预览前 N 行"）。前端只展示前 500 行。 */
    private static final int PREVIEW_CAP = Integer.getInteger("DojaoUiPreviewRows", 500);

    /** Master 自身资源采样周期（与 Worker 心跳 2s 对齐，折线图落在同一时间尺度上）。 */
    private static final long MASTER_SAMPLE_INTERVAL_MS = 2000L;

    /** Master 自身在资源序列 / 节点列表里的伪 workerId（不是真 Worker，前端按 role=master 区分）。 */
    private static final String MASTER_NODE_ID = "master";

    private final QueryServiceImpl queryService;
    /** SQL 页异步执行线程池：并发跑多条 UI 查询。 */
    private final ExecutorService executor;
    /** UI 提交过的查询（queryId → 结果预览缓存）；含已取消未注册到 Registry 的。 */
    private final Map<String, QueryRun> runs = new ConcurrentHashMap<>();
    /**
     * 最多保留的 UI 提交记录。每条已结束的 QueryRun 仍持有列头 + 最多 {@link #PREVIEW_CAP} 行预览，
     * 长时间使用控制台会随提交次数单调增长 —— 超出上限后按提交顺序淘汰最旧的<b>终态</b>记录
     * （运行中的记录跳过并重新入队，绝不丢弃活跃查询的预览）。
     */
    private static final int MAX_RETAINED_RUNS = Math.max(1, Integer.getInteger("DojaoUiRetainedRuns", 200));
    /** 提交顺序（与 {@link #runs} 同步维护，淘汰时从队头找最旧记录）。 */
    private final ConcurrentLinkedDeque<String> runOrder = new ConcurrentLinkedDeque<>();

    /** Master 自身最近一次资源快照（采样线程写、REST 读；未采到为 null）。 */
    private volatile ResourceSummary masterResource;

    public UiQueryMonitor(QueryServiceImpl queryService) {
        this(queryService, Integer.getInteger("DojaoUiAsyncQueries", 2));
    }

    public UiQueryMonitor(QueryServiceImpl queryService, int asyncThreads) {
        this.queryService = queryService;
        this.executor = Executors.newFixedThreadPool(Math.max(1, asyncThreads), r -> {
            Thread t = new Thread(r, "ui-query-async");
            t.setDaemon(true);
            return t;
        });
        startMasterResourceSampler();
    }

    /**
     * Master 自身资源采样：每 2s 采一次，既更新 {@link #masterResource}（当前快照卡片），
     * 也追加进 {@link WorkerRegistry#history()}（折线图，节点 id = {@code "master"}）。
     * 所有节点共用一条时间序列通道，前端一视同仁地画折线。
     */
    private void startMasterResourceSampler() {
        Thread t = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    ResourceSummary rs = ResourceSummary.collect();
                    masterResource = rs;
                    WorkerRegistry.get().history().append(MASTER_NODE_ID, rs);
                } catch (RuntimeException e) {
                    log.debug("Master 资源采样失败: {}", e.toString());
                }
                try {
                    Thread.sleep(MASTER_SAMPLE_INTERVAL_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }, "master-resource-sampler");
        t.setDaemon(true);
        t.start();
    }

    // ==================== 监控读 ====================

    @Override
    public List<MonitorJob> listQueries() {
        Map<String, MonitorJob> byId = new HashMap<>();
        // 先取 Registry（UI 提交 + 客户端提交都在这）：活跃 + 最近完成
        for (QueryManager qm : QueryManagerRegistry.get().activeQueries()) {
            byId.put(qm.queryId(), toJob(qm, qm.state()));
        }
        for (QueryManager qm : QueryManagerRegistry.get().recentlyFinished()) {
            byId.putIfAbsent(qm.queryId(), toJob(qm, qm.state()));
        }
        // 补充 UI 已提交但尚未注册/已提前取消的（EXPLAIN 不注册、极早取消）
        for (QueryRun run : runs.values()) {
            byId.putIfAbsent(run.queryId, new MonitorJob(
                    run.queryId, run.sql, run.state, run.wallMs(), run.storedRows()));
        }
        List<MonitorJob> out = new ArrayList<>(byId.values());
        // queryId 前缀 = yyyyMMddHHmmss，字符串倒序即最近优先
        out.sort((a, b) -> b.queryId().compareTo(a.queryId()));
        return out;
    }

    @Override
    public MonitorQuery snapshot(String queryId) {
        QueryManager qm = QueryManagerRegistry.get().get(queryId);
        if (qm != null) {
            return toQuery(qm);
        }
        // Registry 无（EXPLAIN / 提前取消 / 刚提交未注册）：退化为 UI run 摘要
        QueryRun run = runs.get(queryId);
        if (run != null) {
            return new MonitorQuery(run.queryId, run.sql, run.state, run.wallMs(),
                    run.storedRows(), run.error, List.of(), Map.of());
        }
        return new MonitorQuery(queryId, "", "UNKNOWN", 0L, 0L, "no such query",
                List.of(), Map.of());
    }

    @Override
    public MonitorCluster cluster() {
        long timeoutMs = PlanningContext.DEFAULT_WORKER_HEALTH_TIMEOUT_MS;
        List<WorkerRegistration> regs = WorkerRegistry.get().all();
        List<MonitorWorker> workers = new ArrayList<>(regs.size());
        Map<String, Integer> tasksPerWorker = runningTaskCounts();
        for (WorkerRegistration reg : regs) {
            WorkerNode info = reg.info();
            ResourceSummary rs = reg.lastResource();
            boolean healthy = reg.isFresh(timeoutMs)
                    && reg.state() != WorkerState.STOPPED && reg.state() != WorkerState.FAILED;
            workers.add(toWorker(info.workerId(), info.host(), info.port(), reg.state().name(), healthy,
                    rs, tasksPerWorker.getOrDefault(info.workerId(), 0), info.version(), "worker"));
        }
        List<MonitorJob> active = QueryManagerRegistry.get().activeQueries().stream()
                .map(qm -> toJob(qm, qm.state())).toList();
        List<MonitorJob> recent = QueryManagerRegistry.get().recentlyFinished().stream()
                .map(qm -> toJob(qm, qm.state())).toList();
        return new MonitorCluster(masterWorker(active), workers, active, recent);
    }

    /** Master 自身节点卡：采到才有；未采到时给一个「无数据」壳（前端仍能显示一个节点位）。 */
    private MonitorWorker masterWorker(List<MonitorJob> active) {
        String host = "master";
        try {
            host = java.net.InetAddress.getLocalHost().getHostName();
        } catch (Exception ignored) {
            // 取不到主机名就用字面 "master"，不影响监控
        }
        return toWorker(MASTER_NODE_ID, host, 0, "MASTER", true,
                masterResource, active.size(), "-", "master");
    }

    /** ResourceSummary + 节点标识 → 监控页节点卡（Master / Worker 共用一套映射，保证字段口径一致）。 */
    private static MonitorWorker toWorker(String workerId, String host, int port, String state, boolean healthy,
                                          ResourceSummary rs, int runningQueries, String version, String role) {
        return new MonitorWorker(
                workerId, host, port, state, healthy,
                rs != null ? rs.cpuCores() : 0,
                rs != null ? rs.cpuUsagePct() : -1,
                rs != null ? rs.totalMemoryBytes() : 0L,
                rs != null ? rs.usedMemoryBytes() : 0L,
                runningQueries, version, role,
                rs != null ? rs.heapUsedBytes() : 0L,
                rs != null ? rs.heapMaxBytes() : -1L,
                rs != null ? rs.threadCount() : 0,
                rs != null ? rs.totalDiskBytes() : 0L,
                rs != null ? rs.freeDiskBytes() : 0L,
                rs != null ? rs.maxConcurrency() : 0,
                rs != null ? rs.directUsedBytes() : 0L,
                rs != null ? rs.directBudgetBytes() : 0L,
                rs != null
                        ? rs.allocators().stream()
                                .map(a -> new com.dojao.ui.monitor.MonitorAllocator(
                                        a.category(), a.allocatedBytes(), a.peakBytes(), a.limitBytes()))
                                .toList()
                        : java.util.List.of(),
                rs != null ? rs.processRssBytes() : 0L);
    }

    @Override
    public MonitorHistory history(int maxPoints) {
        int cap = maxPoints <= 0 ? 180 : Math.min(maxPoints, 1000);
        Map<String, List<MonitorSample>> series = new LinkedHashMap<>();
        ResourceHistory h = WorkerRegistry.get().history();
        // Master 排最前（前端图例顺序），其余按 id 排序保证每次刷新次序稳定
        List<String> ids = new ArrayList<>(h.nodeIds());
        ids.sort(Comparator.naturalOrder());
        if (ids.remove(MASTER_NODE_ID)) {
            ids.add(0, MASTER_NODE_ID);
        }
        for (String id : ids) {
            List<MonitorSample> pts = new ArrayList<>();
            long prevTs = -1L, prevRead = -1L, prevWrite = -1L, prevRx = -1L, prevTx = -1L;
            for (ResourceHistory.Sample s : h.recent(id, cap)) {
                ResourceSummary r = s.resource();
                // 磁盘 IO / 网络收发都是累计字节 → 按相邻采样点差分算速率（首点无前驱 / 计数回退 → 0）
                long dt = prevTs > 0L ? s.tsMillis() - prevTs : 0L;
                long readRate = ioRate(r.diskReadBytes(), prevRead, dt);
                long writeRate = ioRate(r.diskWriteBytes(), prevWrite, dt);
                long rxRate = ioRate(r.netRxBytes(), prevRx, dt);
                long txRate = ioRate(r.netTxBytes(), prevTx, dt);
                pts.add(new MonitorSample(s.tsMillis(), r.cpuUsagePct(),
                        r.heapUsedBytes(), r.heapMaxBytes(), r.threadCount(),
                        r.usedMemoryBytes(), r.totalMemoryBytes(),
                        r.directUsedBytes(), readRate, writeRate, rxRate, txRate));
                prevTs = s.tsMillis();
                prevRead = r.diskReadBytes();
                prevWrite = r.diskWriteBytes();
                prevRx = r.netRxBytes();
                prevTx = r.netTxBytes();
            }
            if (!pts.isEmpty()) series.put(id, pts);
        }
        return new MonitorHistory(series);
    }

    /** 累计字节差分 → 速率（字节/秒）；无前驱 / 时间不前 / 计数回退（进程重启）→ 0。 */
    private static long ioRate(long cur, long prev, long dtMillis) {
        if (prev < 0L || dtMillis <= 0L || cur < prev) return 0L;
        return (cur - prev) * 1000L / dtMillis;
    }

    /** 汇总活跃查询的已派发任务 → workerId → 运行中任务数（集群每 Worker 负载列）。 */
    private static Map<String, Integer> runningTaskCounts() {
        Map<String, Integer> counts = new HashMap<>();
        for (QueryManager qm : QueryManagerRegistry.get().activeQueries()) {
            for (com.dojao.server.master.OperatorSnapshot.Task t : qm.snapshot().operators().stream()
                    .flatMap(o -> o.tasks().stream()).toList()) {
                if ("RUNNING".equals(t.state())) {
                    counts.merge(t.workerId(), 1, Integer::sum);
                }
            }
        }
        return counts;
    }

    /** 任务列表行。 */
    private static MonitorJob toJob(QueryManager qm, QueryState state) {
        return new MonitorJob(qm.queryId(), qm.sql(), state.name(),
                qm.wallMs(), qm.snapshot().totalRowsOut());
    }

    /** 单查询完整快照：算子（含 DAG 子节点下标）+ 每算子分布式任务进度。 */
    private static MonitorQuery toQuery(QueryManager qm) {
        QuerySnapshot snap = qm.snapshot();
        List<OperatorSnapshot> ops = snap.operators();
        List<PhysicalNode> measured = qm.measuredOperators();   // 与 ops 按下标对齐

        // PhysicalNode 恒等 → 在 measured 列表中的下标（DAG 子节点只连可度量算子）
        IdentityHashMap<PhysicalNode, Integer> index = new IdentityHashMap<>();
        for (int i = 0; i < measured.size(); i++) {
            index.put(measured.get(i), i);
        }

        // ===== 分布式算子「片段 → 算子粒度 DAG」=====
        // Master 物理树里一个叶子分布式算子（无子节点：单表 GROUP BY 的 DistributedAggregation、
        // join→GROUP BY 的 DistributedJoinAggregation …）常把整条 worker 流水线收进自己的片段
        // 任务里：每个数据来源一组 Producer（Scan→Shuffle，上游推送）、若干中间层
        // （Join→Shuffle，本地 join/聚合并重打散转发）+ 终端（Aggregate/Join/Window，唯一把结果
        // 页回传 Master、承载权威 rowsOut 的段）。把它按「数据来源 / 任务类别」拆成阶段节点：
        // 每个来源一个 Scan 叶子 → DAG 呈现「Scan users / Scan products ⇒ Join ⇒ Aggregate」。
        // join 等已带可度量子节点的 master 树不动（那些 scan 本就是独立节点），避免重复。
        int n = ops.size();
        List<Group>[] stagePlan = new List[n];
        for (int i = 0; i < n; i++) {
            if (i >= measured.size()) continue;
            if (hasMeasuredChild(measured.get(i), index)) continue;
            stagePlan[i] = planStages(ops.get(i));
        }
        // 展示下标分配：展开算子的输出段（终端）取原槽位（下游引用锚点），上游段用组内后续槽位；
        // 其余算子原样占一槽。
        int[] anchor = new int[n];
        int total = 0;
        for (int i = 0; i < n; i++) {
            anchor[i] = total;
            total += (stagePlan[i] != null ? stagePlan[i].size() : 1);
        }

        long now = System.nanoTime();
        List<MonitorOperator> operators = new ArrayList<>(total);
        for (int i = 0; i < n; i++) {
            OperatorSnapshot o = ops.get(i);
            List<Group> gs = stagePlan[i];
            if (gs == null) {
                operators.add(fullNode(o, anchor[i],
                        childrenIdx(i, measured, index, anchor), now));
                continue;
            }
            // 终端段 = 组内最后一个（producer 叶子在前、中间层居中、终端收尾）
            int termLocal = gs.size() - 1;
            int[] gidx = new int[gs.size()];
            gidx[termLocal] = anchor[i];
            int cursor = 1;
            for (int j = 0; j < gs.size(); j++) {
                if (j != termLocal) gidx[j] = anchor[i] + cursor++;
            }
            // 数据流边：叶子（各数据来源）→ 中间层依次串联（Join ⇒ Window…）→ 终端
            int leafCount = 0;
            for (Group g : gs) if (g.producer) leafCount++;
            List<List<Integer>> childLocal = new ArrayList<>(gs.size());
            for (int j = 0; j < gs.size(); j++) childLocal.add(new ArrayList<>());
            if (termLocal == leafCount) {
                // 无中间层（纯 join / 单聚合）：所有 Scan 叶子直接喂终端
                for (int l = 0; l < leafCount; l++) childLocal.get(termLocal).add(l);
            } else {
                // 线性流水线：叶子 → 首个中间层 → 逐级中间层 → 终端
                for (int l = 0; l < leafCount; l++) childLocal.get(leafCount).add(l);
                for (int j = leafCount + 1; j < termLocal; j++) childLocal.get(j).add(j - 1);
                childLocal.get(termLocal).add(termLocal - 1);
            }
            // 上游各段已归属的溢写：终端段 = 算子总溢写 - 上游各段，保证「各段相加 = 总量」且不双计
            long upstreamSpilled = 0L;
            for (int j = 0; j < gs.size(); j++) {
                Group g = gs.get(j);
                List<Integer> children = new ArrayList<>(childLocal.get(j).size());
                for (int cl : childLocal.get(j)) children.add(gidx[cl]);
                if (j == termLocal) {
                    // 终端段：承接原算子槽位与权威指标（结果页回传即此段输出）
                    operators.add(stageNode(o, o.operatorId(), g.kind, g.tasks,
                            gidx[j], children, now,
                            o.rowsIn(), o.rowsOut(), o.inputBytes(), o.outputBytes(),
                            o.elapsedMs(), o.cpuMs(),
                            o.currentMemBytes(), o.peakMemBytes(),
                            Math.max(0L, o.spilledBytes() - upstreamSpilled),
                            o.avgRowsPerSec(), o.instantBytesPerSec(), o.avgBytesPerSec(),
                            o.failureMessage()));
                } else {
                    long upRows = sumTaskRows(g.tasks);
                    long upBytesIn = sumTaskBytesIn(g.tasks);
                    long upBytesOut = sumTaskBytesOut(g.tasks);
                    long upSpilled = sumTaskSpilled(g.tasks);
                    upstreamSpilled += upSpilled;
                    long upElapsed = maxTaskElapsedMs(g.tasks, now);
                    String suffix = g.producer
                            ? "#scan:" + (g.source == null ? "" : g.source)
                            : "#mid:" + (g.kind == null ? "" : g.kind.replace("→", "_"));
                    operators.add(stageNode(o, o.operatorId() + suffix, g.kind, g.tasks,
                            gidx[j], children, now,
                            0L, upRows, upBytesIn, upBytesOut,
                            upElapsed, 0L, 0L, 0L, upSpilled,
                            avgRate(upRows, upElapsed), 0L, avgRate(upBytesOut, upElapsed),
                            firstTaskFailure(g.tasks)));
                }
            }
        }
        operators.sort(Comparator.comparingInt(MonitorOperator::idx));
        return new MonitorQuery(qm.queryId(), qm.sql(), snap.state().name(), snap.wallMs(),
                snap.totalRowsOut(), qm.failureMessage(), operators, snap.operatorFailures());
    }

    /** measured 算子是否有可度量 child —— 有则其上游 scan 已是独立节点，不再展开。 */
    private static boolean hasMeasuredChild(PhysicalNode node,
                                            IdentityHashMap<PhysicalNode, Integer> index) {
        for (PhysicalNode c : node.children()) {
            if (index.containsKey(c)) return true;
        }
        return false;
    }

    /**
     * 把叶子分布式算子的片段任务按「数据来源 / 执行阶段」排成阶段组
     * （[Scan 叶子 × 来源数] + [中间层按 Join ⇒ Aggregate ⇒ Window 排序] + [终端恰 1]）。
     *
     * <p>任务的三分类取自片段自身的 {@link OperatorSnapshot.Task#terminal()} 标志，不再靠任务类别
     * 文本推断 —— join 与窗口压在同一 Worker 片段时，片段已被拆成 {@code Join} 中间段 + 窗口/聚合
     * 段（见 core {@code fragmentTasks()}），各段都有独立行数/溢写/完成状态。</p>
     *
     * <p>不满足条件的算子返回 {@code null}（保持单节点，不误连边）：非分布式计算算子、
     * 无 producer 片段、终端种类不是恰好 1 个。</p>
     */
    private static List<Group> planStages(OperatorSnapshot o) {
        List<OperatorSnapshot.Task> tasks = o.tasks();
        if (tasks == null || tasks.isEmpty()) return null;
        if (!isFanOutOperator(o.operatorType())) return null;
        Map<String, List<OperatorSnapshot.Task>> leaves = new LinkedHashMap<>();
        Map<String, List<OperatorSnapshot.Task>> mids = new LinkedHashMap<>();
        Map<String, List<OperatorSnapshot.Task>> terms = new LinkedHashMap<>();
        for (OperatorSnapshot.Task t : tasks) {
            if (t.producer()) {
                String key = (t.source() == null || t.source().isEmpty()) ? "?" : t.source();
                leaves.computeIfAbsent(key, k -> new ArrayList<>()).add(t);
            } else if (!t.terminal()) {
                mids.computeIfAbsent(t.kind(), k -> new ArrayList<>()).add(t);
            } else {
                terms.computeIfAbsent(t.kind(), k -> new ArrayList<>()).add(t);
            }
        }
        if (leaves.isEmpty()) return null;      // 无来源片段：不是可拆的流水线
        if (terms.size() != 1) return null;     // 需恰好一个终端计算段承载权威输出
        List<Group> out = new ArrayList<>();
        for (Map.Entry<String, List<OperatorSnapshot.Task>> e : leaves.entrySet()) {
            out.add(new Group("Scan→Shuffle", e.getKey(), true, false, e.getValue()));
        }
        // 中间层按阶段先后排序（Join → Aggregate → Window），同类保持片段派发顺序
        List<Map.Entry<String, List<OperatorSnapshot.Task>>> midList =
                new ArrayList<>(mids.entrySet());
        midList.sort(Comparator.comparingInt(e -> stageRank(e.getKey())));
        for (Map.Entry<String, List<OperatorSnapshot.Task>> e : midList) {
            out.add(new Group(e.getKey(), null, false, false, e.getValue()));
        }
        for (Map.Entry<String, List<OperatorSnapshot.Task>> e : terms.entrySet()) {
            out.add(new Group(e.getKey(), null, false, true, e.getValue()));
        }
        return out;
    }

    /** 中间层阶段先后：Join 先于 Aggregate，Aggregate 先于 Window（同一片段的执行顺序）。 */
    private static int stageRank(String kind) {
        if (kind == null) return 2;
        if (kind.startsWith("Join")) return 0;
        if (kind.startsWith("Aggregate")) return 1;
        return 2;
    }

    /** 片段可拆分为多来源 Scan 叶子的分布式计算算子（write/scan/未知类型不拆）。 */
    private static boolean isFanOutOperator(String type) {
        if (type == null) return false;
        switch (type) {
            case "DistributedAggregation", "DistributedJoinAggregation", "DistributedChained",
                    "DistributedUnionDistinct", "DistributedAggregateWindow",
                    "DistributedWindow", "DistributedDerivedJoin", "DistributedJoin":
                return true;
            default:
                return false;
        }
    }

    /** 一个阶段组：producer 叶子（带 source 基表）/ 中间层 / 终端。 */
    private record Group(String kind, String source, boolean producer, boolean terminal,
                         List<OperatorSnapshot.Task> tasks) {
    }

    /** 不展开算子的完整节点（原逻辑）：children = 其可度量子节点的输出段下标。 */
    private static MonitorOperator fullNode(OperatorSnapshot o, int idx,
                                            List<Integer> children, long now) {
        return new MonitorOperator(
                idx, o.operatorId(), o.operatorType(), o.state().name(),
                o.parallelism(), o.rowsIn(), o.rowsOut(), o.inputBytes(), o.outputBytes(),
                o.currentMemBytes(), o.peakMemBytes(), o.spilledBytes(),
                o.cpuMs(), o.elapsedMs(), o.instantRowsPerSec(), o.avgRowsPerSec(),
                o.instantBytesPerSec(), o.avgBytesPerSec(),
                o.failureMessage(), children, toMonitorTasks(o.tasks(), now));
    }

    /** 阶段节点：operatorId 显式；type = 引擎任务类别（Scan→Shuffle / Aggregate / Join / Window…）。 */
    private static MonitorOperator stageNode(OperatorSnapshot o, String opId, String kind,
                                             List<OperatorSnapshot.Task> tasks,
                                             int idx, List<Integer> children, long now,
                                             long rowsIn, long rowsOut, long inputBytes, long outputBytes,
                                             long elapsedMs, long cpuMs,
                                             long currentMemBytes, long peakMemBytes, long spilledBytes,
                                             long avgRowsPerSec, long instantBytesPerSec, long avgBytesPerSec,
                                             String failureMessage) {
        String state = stageState(tasks, o.state());
        if ("FAILED".equals(state) && failureMessage == null) {
            failureMessage = firstTaskFailure(tasks);
        }
        return new MonitorOperator(
                idx, opId, kind, state,
                Math.max(1, tasks.size()), rowsIn, rowsOut, inputBytes, outputBytes,
                currentMemBytes, peakMemBytes, spilledBytes,
                cpuMs, elapsedMs, 0L, avgRowsPerSec, instantBytesPerSec, avgBytesPerSec,
                failureMessage, children, toMonitorTasks(tasks, now));
    }

    /** 阶段状态：任务任一 FAILED/LOST → FAILED；否则 RUNNING → 有运行；全 FINISHED → FINISHED。 */
    private static String stageState(List<OperatorSnapshot.Task> tasks, OperatorState opState) {
        if (opState == OperatorState.FAILED) return "FAILED";
        boolean running = false;
        int finished = 0;
        for (OperatorSnapshot.Task t : tasks) {
            String s = t.state();
            if (DistributedTask.FAILED.equals(s) || DistributedTask.LOST.equals(s)) {
                return "FAILED";
            }
            if (DistributedTask.RUNNING.equals(s)) running = true;
            if (DistributedTask.FINISHED.equals(s)) finished++;
        }
        if (running) return "RUNNING";
        if (finished == tasks.size() && finished > 0) return "FINISHED";
        return "NOT_STARTED";
    }

    /** measured 算子 i 的可度量子节点 → 各自输出段展示下标。 */
    private static List<Integer> childrenIdx(int i, List<PhysicalNode> measured,
                                             IdentityHashMap<PhysicalNode, Integer> index,
                                             int[] anchor) {
        if (i >= measured.size()) return List.of();
        List<Integer> children = new ArrayList<>();
        for (PhysicalNode child : measured.get(i).children()) {
            Integer ci = index.get(child);
            if (ci != null) children.add(anchor[ci]);
        }
        return children;
    }

    private static List<MonitorTask> toMonitorTasks(List<OperatorSnapshot.Task> tasks, long now) {
        List<MonitorTask> out = new ArrayList<>(tasks.size());
        for (OperatorSnapshot.Task t : tasks) {
            long dispatch = t.dispatchNanos();
            long end = t.finishNanos() > 0L ? t.finishNanos() : now;
            out.add(new MonitorTask(t.taskId(), t.workerId(), t.kind(), t.producer(),
                    t.state(), t.rowsOut(), Math.max(0L, (end - dispatch) / 1_000_000L),
                    t.failure(), t.source(), t.expectedRows()));
        }
        return out;
    }

    private static long sumTaskRows(List<OperatorSnapshot.Task> tasks) {
        long sum = 0L;
        for (OperatorSnapshot.Task t : tasks) sum += t.rowsOut();
        return sum;
    }

    /** 阶段框的溢写 = 该段各任务（片段 / join 分区子任务）Worker 侧溢写之和。 */
    private static long sumTaskSpilled(List<OperatorSnapshot.Task> tasks) {
        long sum = 0L;
        for (OperatorSnapshot.Task t : tasks) sum += t.spilledBytes();
        return sum;
    }

    /** 阶段框的读入字节 = 该段各任务（片段 / join 分区子任务）遥测之和。 */
    private static long sumTaskBytesIn(List<OperatorSnapshot.Task> tasks) {
        long sum = 0L;
        for (OperatorSnapshot.Task t : tasks) sum += t.bytesIn();
        return sum;
    }

    /** 阶段框的发出字节 = 该段各任务（片段 / join 分区子任务）遥测之和。 */
    private static long sumTaskBytesOut(List<OperatorSnapshot.Task> tasks) {
        long sum = 0L;
        for (OperatorSnapshot.Task t : tasks) sum += t.bytesOut();
        return sum;
    }

    private static long maxTaskElapsedMs(List<OperatorSnapshot.Task> tasks, long now) {
        long max = 0L;
        for (OperatorSnapshot.Task t : tasks) {
            long dispatch = t.dispatchNanos();
            long end = t.finishNanos() > 0L ? t.finishNanos() : now;
            max = Math.max(max, Math.max(0L, (end - dispatch) / 1_000_000L));
        }
        return max;
    }

    private static String firstTaskFailure(List<OperatorSnapshot.Task> tasks) {
        for (OperatorSnapshot.Task t : tasks) {
            if (t.failure() != null && !t.failure().isEmpty()) return t.failure();
        }
        return null;
    }

    private static long avgRate(long rows, long elapsedMs) {
        return elapsedMs > 0L ? rows * 1000L / elapsedMs : 0L;
    }

    // ==================== SQL 异步执行 ====================

    @Override
    public MonitorSubmit submit(String sql) {
        return submit(sql, null);
    }

    @Override
    public MonitorSubmit submit(String sql, String owner) {
        String queryId = QueryManager.newQueryId();
        // 引擎侧自动识别批量提交：按顶层分号切分（字符串 / 注释里的分号不切），
        // 多条时按书写顺序串行执行；最后一句不写分号也算一条。
        List<String> statements = SqlSplitter.executableStatements(sql);
        QueryRun run = new QueryRun(queryId, sql, owner);
        run.state = "SUBMITTED";
        runs.put(queryId, run);
        runOrder.addLast(queryId);
        evictRuns();
        if (statements.isEmpty()) {
            run.state = "FAILED";
            run.error = "没有可执行的 SQL 语句";
            return new MonitorSubmit(queryId, run.state, run.error);
        }
        run.statements = statements;
        try {
            if (statements.size() == 1) {
                QueryRequest request = QueryRequest.newBuilder().setSql(statements.get(0)).build();
                executor.submit(() -> execute(run, request));
            } else {
                executor.submit(() -> executeBatch(run));
            }
        } catch (Throwable t) {
            run.state = "FAILED";
            run.error = Errors.describe(t);
            return new MonitorSubmit(queryId, run.state, run.error);
        }
        String message = statements.size() > 1
                ? "已受理 " + statements.size() + " 条语句，按分号切分后串行执行"
                : "已受理，异步执行中";
        return new MonitorSubmit(queryId, "SUBMITTED", message);
    }

    /**
     * 异步执行体：真跑查询线（外部 queryId + 有界预览 sink），结束后把 run 状态对齐到
     * QueryManager（FINISHED / FAILED / CANCELED），预览行在有界缓存里截断。
     */
    private void execute(QueryRun run, QueryRequest request) {
        run.state = "RUNNING";
        run.startNanos = System.nanoTime();
        run.currentQueryId = run.queryId;
        try {
            // 结果预览 sink：只留列头 + 前 PREVIEW_CAP 行（超出截断但继续消费，查询照常跑完）
            queryService.executeQuery(run.queryId, request, response -> {
                synchronized (run) {
                    if (run.columns == null && response.getColumnsCount() > 0) {
                        List<String> cols = new ArrayList<>(response.getColumnsCount());
                        for (Column c : response.getColumnsList()) {
                            cols.add(c.getName());
                        }
                        run.columns = List.copyOf(cols);
                    }
                    for (Row row : response.getRowsList()) {
                        run.totalRows.incrementAndGet();
                        if (run.rows.size() < PREVIEW_CAP) {
                            List<String> vals = new ArrayList<>(row.getValuesCount());
                            for (String v : row.getValuesList()) {
                                vals.add(v == null ? "NULL" : v);
                            }
                            run.rows.add(List.copyOf(vals));
                        }
                    }
                }
            }, run.owner);
            // executeQuery 正常返回：状态以 Registry 里的 QueryManager 为准（cancel 不覆盖）
            QueryManager qm = QueryManagerRegistry.get().get(run.queryId);
            run.state = qm != null ? qm.state().name() : "FINISHED";
        } catch (Throwable t) {
            if (!"CANCELED".equals(run.state)) {
                run.state = "FAILED";
                // 顶层 message 多半是「Operator X next failed」这类无信息量的外壳：主区给整条
                // cause 链（真因在链尾），另存完整堆栈供前端折叠展开，交给人 / AI 定位。
                run.error = Errors.describe(t);
                run.errorDetail = Errors.stackTrace(t);
                log.warn("UI 查询执行失败: queryId={}: {}", run.queryId, run.error);
            }
        } finally {
            run.endNanos = System.nanoTime();
            evictRuns();
        }
    }

    /**
     * 保持 {@link #runs} 有界：从提交顺序队头开始淘汰<b>终态</b>记录。
     *
     * <p>运行中 / 已提交未开始的记录会重新入队（它们的预览仍可能被前端轮询），
     * 尝试次数以当前队列长度为界 —— 全是活跃查询时不会空转。</p>
     */
    private void evictRuns() {
        int attempts = runOrder.size();
        while (runs.size() > MAX_RETAINED_RUNS && attempts-- > 0) {
            String oldest = runOrder.pollFirst();
            if (oldest == null) return;
            QueryRun run = runs.get(oldest);
            if (run == null) continue;         // 已被淘汰（并发路径）
            if (!isTerminal(run.state)) {
                runOrder.addLast(oldest);      // 活跃查询不能丢：放回队尾
                continue;
            }
            runs.remove(oldest);
        }
    }

    private static boolean isTerminal(String state) {
        return "FINISHED".equals(state) || "FAILED".equals(state) || "CANCELED".equals(state);
    }

    /**
     * 批量执行体：语句已按顶层分号切好，按书写顺序<b>串行</b>执行 —— 前一条结束后才开始下一条。
     *
     * <p>每条语句用<b>独立的内部 queryId</b> 走正常执行线（因此会出现在任务列表 / 可单独观测），
     * 外层 batch queryId 只负责轮询与取消。任一条失败即 fail-fast 停止后续语句；取消在语句之间
     * 生效，并同时取消正在执行的那条内部查询。预览结果取<b>最后一条有结果集的语句</b>
     * （DDL / INSERT 没有结果集，不会覆盖前面的 SELECT 结果）。</p>
     */
    private void executeBatch(QueryRun run) {
        run.state = "RUNNING";
        run.startNanos = System.nanoTime();
        List<String> statements = run.statements;
        try {
            SqlBatchRunner.runAll(statements, (statement, index, total) -> {
                run.currentIndex = index;
                String innerQueryId = QueryManager.newQueryId();
                run.currentQueryId = innerQueryId;
                StmtPreview preview = new StmtPreview();
                log.info("批量查询执行第 {}/{} 条: batchId={}, queryId={}, sql={}",
                        index + 1, total, run.queryId, innerQueryId, statement);
                queryService.executeQuery(innerQueryId,
                        QueryRequest.newBuilder().setSql(statement).build(),
                        preview::accept, run.owner);
                // 语句执行期间被取消：不再把它当成功结果展示
                if (!"CANCELED".equals(run.state)) {
                    run.adopt(preview);
                }
            }, () -> "CANCELED".equals(run.state));
            if (!"CANCELED".equals(run.state)) {
                run.state = "FINISHED";
            }
        } catch (Throwable t) {
            if (!"CANCELED".equals(run.state)) {
                run.state = "FAILED";
                // 指出是第几条失败（前面已成功执行的不回滚 —— 与脚本执行语义一致）
                run.error = "第 " + (run.currentIndex + 1) + "/" + statements.size()
                        + " 条语句执行失败：" + Errors.describe(t);
                run.errorDetail = Errors.stackTrace(t);
                log.warn("UI 批量查询执行失败: batchId={}, index={}/{}: {}",
                        run.queryId, run.currentIndex + 1, statements.size(), run.error);
            }
        } finally {
            run.currentQueryId = null;
            run.endNanos = System.nanoTime();
            evictRuns();
        }
    }

    @Override
    public void cancel(String queryId) {
        QueryRun run = runs.get(queryId);
        String runningId = queryId;
        if (run != null) {
            run.state = "CANCELED";
            // 批量提交时真正在跑的是内部语句查询，取消它才能让执行线尽快停下
            if (run.currentQueryId != null) {
                runningId = run.currentQueryId;
            }
        }
        QueryManager qm = QueryManagerRegistry.get().get(runningId);
        if (qm != null) {
            qm.cancel();
        }
    }

    @Override
    public MonitorResult result(String queryId, int offset, int limit) {
        QueryRun run = runs.get(queryId);
        if (run == null) {
            return new MonitorResult(queryId, "FINISHED", List.of(), List.of(),
                    0L, 0L, false,
                    "no result preview retained (query was not submitted from this console)", null, 0L);
        }
        if ("FAILED".equals(run.state)) {
            return new MonitorResult(queryId, run.state, run.columns == null ? List.of() : run.columns,
                    List.of(), 0L, 0L, false, run.error, run.errorDetail, run.wallMs());
        }
        // 批量执行时每条语句结束后会整体替换预览（run.adopt），这里按 run 同步取快照，
        // 避免轮询撞上替换过程读到半新半旧的列头 / 行。
        List<String> columns;
        List<List<String>> page = new ArrayList<>();
        long totalRows;
        boolean truncated;
        synchronized (run) {
            columns = run.columns == null ? List.of() : run.columns;
            int from = Math.max(0, offset);
            int to = Math.min(run.rows.size(), offset + Math.max(1, limit));
            for (int i = from; i < to; i++) {
                page.add(run.rows.get(i));
            }
            totalRows = run.totalRows.get();
            truncated = totalRows > run.rows.size();
        }
        return new MonitorResult(queryId, run.state, columns, page, totalRows,
                sourceRowsOf(queryId), truncated, run.error, run.errorDetail, run.wallMs());
    }

    /**
     * 扫描源表行数估计之和（{@code schema.table} → 连接器探查的 rowCount）。单表查询即该表数据量，
     * 供 UI 展示「共约 N 行，预览前 M 行」。查询不在 Registry（如刚提交尚未开始）时返回 0。
     */
    private static long sourceRowsOf(String queryId) {
        QueryManager qm = QueryManagerRegistry.get().get(queryId);
        if (qm == null) return 0L;
        long sum = 0L;
        for (Long v : qm.sourceExpectedRows().values()) {
            if (v != null && v > 0L) sum += v;
        }
        return sum;
    }

    // ==================== 目录树（catalog → schema → table → column）====================

    @Override
    public List<String> listCatalogs(String viewer) {
        return queryService.metaStore().listRegisteredCatalogs(viewer);
    }

    @Override
    public List<String> listSchemas(String catalog) {
        return queryService.metaStore().listAllSchemas(catalog);
    }

    @Override
    public List<String> listTables(String catalog, String schema) {
        return queryService.metaStore().listAllTables(catalog, schema);
    }

    @Override
    public List<MonitorColumn> listColumns(String catalog, String schema, String table) {
        // SPI 的 Column（dojao-spi）→ ui 契约的 MonitorColumn（ui 不得依赖 server，这里做一次转换）
        List<com.dojao.spi.bean.Column> cols =
                queryService.metaStore().getColumns(catalog, schema, table);
        List<MonitorColumn> out = new ArrayList<>(cols.size());
        for (com.dojao.spi.bean.Column c : cols) {
            out.add(new MonitorColumn(c.name(), c.dataType(), c.comment(), c.nullable()));
        }
        return out;
    }

    // ==================== 函数目录（内置 + 自定义，SQL 补全用）====================

    @Override
    public List<MonitorFunction> listFunctions(String viewer) {
        List<MonitorFunction> out = new ArrayList<>();

        // 内置：dojao-core 汇总的目录（标量 / 聚合 / 窗口）。
        for (BuiltinFunctionCatalog.Entry e : BuiltinFunctionCatalog.all()) {
            out.add(new MonitorFunction(e.name(), "builtin", e.kind(), "", e.detail()));
        }

        // 自定义：内存注册表（永久 + 临时），persist 时已落 SQLite；按 is_private 过滤视角。
        for (com.dojao.spi.bean.FunctionDefinition def
                : com.dojao.server.function.FunctionManager.definitions(viewer).values()) {
            String name = def.name() == null ? "" : def.name().toLowerCase();
            String backend = def.type() == null ? "" : def.type();
            String detail = def.comment() == null ? "" : def.comment();
            out.add(new MonitorFunction(name, "custom", "", backend, detail));
        }

        out.sort(Comparator.comparing(MonitorFunction::name));
        return out;
    }

    // ==================== 内部：UI 查询结果预览缓存 ====================

    /** 单条 UI 提交查询的执行 + 有界结果预览缓存（线程安全）。 */
    private static final class QueryRun {
        final String queryId;
        final String sql;
        /** 提交该查询的视角用户名；{@code null} = 全局（CLI/JDBC/admin）。用于 catalog / 函数隔离。 */
        final String owner;
        /** 按顶层分号切好的语句（单条提交为单元素）；提交失败 / 空语句时为空列表。 */
        volatile List<String> statements = List.of();
        /** 批量执行中当前语句下标（0 起），失败信息与取消定位用。 */
        volatile int currentIndex;
        /** 正在执行的内部 queryId（批量时每条语句单独注册；取消时用它）。 */
        volatile String currentQueryId;
        volatile String state;
        volatile String error;
        /** 失败时的完整多行堆栈（{@code Errors.stackTrace}），供前端折叠展开；成功 / 运行中为 null。 */
        volatile String errorDetail;
        volatile List<String> columns;
        final List<List<String>> rows = new ArrayList<>();
        final AtomicLong totalRows = new AtomicLong();
        volatile long startNanos;
        volatile long endNanos;

        QueryRun(String queryId, String sql, String owner) {
            this.queryId = queryId;
            this.sql = sql;
            this.owner = owner;
        }

        /**
         * 采用某条语句的结果作为当前展示结果。没有结果集的语句（DDL / INSERT）不覆盖
         * 上一条的结果 —— 批量脚本里「先 SELECT 后 INSERT」时用户仍能看到 SELECT 的结果。
         */
        synchronized void adopt(StmtPreview preview) {
            if (preview.columns == null || preview.columns.isEmpty()) {
                return;
            }
            this.columns = preview.columns;
            this.rows.clear();
            this.rows.addAll(preview.rows);
            this.totalRows.set(preview.totalRows.get());
        }

        long wallMs() {
            long start = startNanos > 0L ? startNanos : 0L;
            if (start == 0L) return 0L;
            long end = endNanos > 0L ? endNanos : System.nanoTime();
            return (end - start) / 1_000_000L;
        }

        long storedRows() {
            return totalRows.get();
        }
    }

    /** 单条语句的结果预览累加器（批量执行时每条语句各建一个，成功后由 {@link QueryRun#adopt} 采用）。 */
    private static final class StmtPreview {
        volatile List<String> columns;
        final List<List<String>> rows = new ArrayList<>();
        final AtomicLong totalRows = new AtomicLong();

        void accept(QueryResponse response) {
            if (columns == null && response.getColumnsCount() > 0) {
                List<String> cols = new ArrayList<>(response.getColumnsCount());
                for (Column c : response.getColumnsList()) {
                    cols.add(c.getName());
                }
                columns = List.copyOf(cols);
            }
            for (Row row : response.getRowsList()) {
                totalRows.incrementAndGet();
                if (rows.size() < PREVIEW_CAP) {
                    List<String> vals = new ArrayList<>(row.getValuesCount());
                    for (String v : row.getValuesList()) {
                        vals.add(v == null ? "NULL" : v);
                    }
                    rows.add(List.copyOf(vals));
                }
            }
        }
    }
}
