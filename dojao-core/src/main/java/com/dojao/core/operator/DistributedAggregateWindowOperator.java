package com.dojao.core.operator;

import com.dojao.core.runtime.master.StageScheduler;
import com.dojao.core.runtime.master.StageScheduler.JoinChainStage;
import com.dojao.core.runtime.master.StageScheduler.JoinLink;
import com.dojao.core.runtime.master.WorkerRegistration;
import com.dojao.core.runtime.worker.WorkerGateway;
import com.dojao.spi.bean.ReadRequest;
import com.dojao.spi.service.DojaoConnector;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.List;

/**
 * 分布式「聚合 → 窗口」两级下沉算子（Master 侧）—— 承接上游分布式聚合算子的聚合规划参数，
 * 把聚合段（{@link FragmentSpec.MiddleAggregateConsumer}，非终端）+ 窗口段
 * （终端 {@link FragmentSpec.WindowConsumer}）<b>两级都留在 Worker</b>：聚合输出按窗口
 * PARTITION BY 重哈希推窗口交换 → 每 Worker 本地窗口排序 + 逐行计算 → 终端就地写走 /
 * 回传 Master。数据永不回 Master 的写形态（CTAS / INSERT INTO / INSERT OVERWRITE）随本算子
 * 自动全分布式（它是 {@link AbstractDistributedOperator}，{@code setSink} /
 * {@code setOverwriteLifecycle} / {@code outputFields} / {@code totalOutputRows} 全部继承自基类）。
 *
 * <pre>
 *   scan T0(WHERE) ─► Partition(join key) ─┐
 *   scan T1(WHERE) ─► Partition(join key) ─┴► join ─► Partition(分组键) ─► MiddleAggregateConsumer
 *                                                              (聚合, 按窗口 PARTITION BY 重哈希)
 *                                                                └─► WindowConsumer(窗口 → sink / Master)
 * </pre>
 *
 * <p><b>两类聚合源</b>（{@code links != null} 判别，一类承载、避免两个近乎相同的类）：
 * <ul>
 *   <li><b>join 源</b>（上游 = {@link DistributedJoinAggregationOperator}）：左深多 join 链 +
 *       分组键 ≠ join 键（Plan B 二次 shuffle）—— 聚合输入段与
 *       {@code StageScheduler.planJoinAggregation} 完全一致（中间层重哈希 + 基表 Producer），
 *       差异仅在终端：{@code MiddleJoinConsumer} 按分组键重哈希进每分区聚合交换 → 每分区
 *       {@code MiddleAggregateConsumer} 聚合后按窗口键再重哈希推窗口交换（不回传 Master）；</li>
 *   <li><b>scan 源</b>（上游 = {@link DistributedAggregationOperator}）：裸表 GROUP BY ——
 *       扫表 → 按分组键分区 → 每分区 {@code MiddleAggregateConsumer} 聚合后按窗口键重哈希推窗口交换。</li>
 * </ul></p>
 *
 * <p><b>窗口段</b>：每 Worker 一个终端 {@code WindowConsumer}（收窗口交换 → 本地
 * {@code WindowSortOperator} → {@code WindowFunctionOperator} → runAndSink / 回传）。
 * <b>全局窗口（空 PARTITION BY）</b>：{@code HashPartitionOperator} 空键 → 每行 hash=0 → pid 0 →
 * 所有聚合输出落 worker 0 → 该 worker 做全局排序 —— 天然退化单分区单 worker（全局排序本质不可并行，
 * 与既有 {@code planWindow} 语义一致的已知边界，非本算子新限制）。</p>
 *
 * <p><b>正确性</b>：全部 shuffle 共用同一 round-robin owner 映射（co-partitioning 不变量）—— 分组键
 * 与窗口键各自哈希落唯一桶 → 每分区本地聚合即该 key 完整聚合、每分区窗口本地排序即该 key 完整窗口，
 * Master 零合并，只收终端 Consumer 的回传 / 写 ack。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/5
 */
public class DistributedAggregateWindowOperator extends AbstractDistributedOperator {

    // ---- join 源（links != null）：基表 T0 + 左深 join 链 ----
    private final String firstCatalog, firstSchema, firstTable;
    private final List<String> firstLeftKeys;
    private final DojaoConnector firstProbe;
    private final String firstFilter;
    private final List<JoinLink> links;
    /** 首表（T0）实际扫描的列集（顺序即页内列序，已含 join 键）；null/空 = 读全列。 */
    private final List<String> firstColumns;

    // ---- scan 源（links == null）：裸表 GROUP BY ----
    private final ReadRequest baseRequest;
    private final List<String> predicates;
    private final DojaoConnector probe;

    // ---- 聚合段（两类源共用） ----
    private final List<String> groupKeys;
    private final List<AggregationSpec> aggregations;
    private final List<CaseProjection> caseProjections;
    private final long memoryLimit;

    // ---- 窗口段 ----
    private final List<String> windowPartitionBy, windowOrderBy;
    private final List<Boolean> windowAscending;
    private final List<WindowFunctionSpec> windowFunctions;

    /** join 源构造（上游 = DistributedJoinAggregationOperator）。 */
    public DistributedAggregateWindowOperator(String queryId,
                                              String firstCatalog, String firstSchema, String firstTable,
                                              List<String> firstLeftKeys, DojaoConnector firstProbe,
                                              String firstFilter,
                                              List<JoinLink> links,
                                              List<String> groupKeys, List<AggregationSpec> aggregations,
                                              List<CaseProjection> caseProjections, long memoryLimit,
                                              List<String> windowPartitionBy, List<String> windowOrderBy,
                                              List<Boolean> windowAscending, List<WindowFunctionSpec> windowFunctions,
                                              int numPartitions, WorkerGateway gateway,
                                              List<WorkerRegistration> workers,
                                              List<String> firstColumns) {
        this(queryId, groupKeys, aggregations, caseProjections, memoryLimit,
                windowPartitionBy, windowOrderBy, windowAscending, windowFunctions,
                numPartitions, gateway, workers,
                firstCatalog, firstSchema, firstTable, firstLeftKeys, firstProbe, firstFilter,
                links == null ? List.of() : List.copyOf(links), firstColumns,
                null, List.of(), null);
    }

    /** scan 源构造（上游 = DistributedAggregationOperator）。 */
    public DistributedAggregateWindowOperator(String queryId,
                                              ReadRequest baseRequest, List<String> predicates, DojaoConnector probe,
                                              List<String> groupKeys, List<AggregationSpec> aggregations,
                                              List<CaseProjection> caseProjections, long memoryLimit,
                                              List<String> windowPartitionBy, List<String> windowOrderBy,
                                              List<Boolean> windowAscending, List<WindowFunctionSpec> windowFunctions,
                                              int numPartitions, WorkerGateway gateway,
                                              List<WorkerRegistration> workers) {
        this(queryId, groupKeys, aggregations, caseProjections, memoryLimit,
                windowPartitionBy, windowOrderBy, windowAscending, windowFunctions,
                numPartitions, gateway, workers,
                null, null, null, List.of(), null, null, null, null,
                baseRequest, List.copyOf(predicates), probe);
    }

    private DistributedAggregateWindowOperator(String queryId,
                                               List<String> groupKeys, List<AggregationSpec> aggregations,
                                               List<CaseProjection> caseProjections, long memoryLimit,
                                               List<String> windowPartitionBy, List<String> windowOrderBy,
                                               List<Boolean> windowAscending, List<WindowFunctionSpec> windowFunctions,
                                               int numPartitions, WorkerGateway gateway,
                                               List<WorkerRegistration> workers,
                                               String firstCatalog, String firstSchema, String firstTable,
                                               List<String> firstLeftKeys, DojaoConnector firstProbe,
                                               String firstFilter, List<JoinLink> links,
                                               List<String> firstColumns,
                                               ReadRequest baseRequest, List<String> predicates, DojaoConnector probe) {
        super("DistributedAggregateWindow", queryId, numPartitions, gateway, workers);
        this.firstCatalog = firstCatalog;
        this.firstSchema = firstSchema;
        this.firstTable = firstTable;
        this.firstLeftKeys = List.copyOf(firstLeftKeys);
        this.firstProbe = firstProbe;
        this.firstFilter = firstFilter;
        this.firstColumns = firstColumns;
        this.links = links;   // null → scan 源
        this.baseRequest = baseRequest;
        this.predicates = predicates;
        this.probe = probe;
        this.groupKeys = List.copyOf(groupKeys);
        this.aggregations = List.copyOf(aggregations);
        this.caseProjections = caseProjections == null ? List.of() : List.copyOf(caseProjections);
        this.memoryLimit = memoryLimit;
        this.windowPartitionBy = List.copyOf(windowPartitionBy);
        this.windowOrderBy = List.copyOf(windowOrderBy);
        this.windowAscending = List.copyOf(windowAscending);
        this.windowFunctions = List.copyOf(windowFunctions);
    }

    @Override
    public List<ScanColumn> scanColumnSets() {
        if (links == null) {
            // scan 源：裸表扫描，列随 ReadRequest 走（空 = 连接器语义读全表 ⇒ optional）
            return List.of(ScanColumn.optional(baseRequest.catalog() + "." + baseRequest.schema()
                    + "." + baseRequest.table(), baseRequest.columns().orElse(null)));
        }
        List<ScanColumn> out = new ArrayList<>();
        out.add(ScanColumn.required(firstCatalog + "." + firstSchema + "." + firstTable, firstColumns));
        for (JoinLink link : links) {
            out.add(ScanColumn.required(
                    link.rightCatalog() + "." + link.rightSchema() + "." + link.rightTable(),
                    link.rightColumns()));
        }
        return out;
    }

    @Override
    protected int dispatchStage() throws Exception {
        // 片段已在 planJoinAggregateWindow / planAggregateWindow 内按反死锁顺序排好：
        // ① 每 Worker 一个 WindowConsumer（先注册窗口交换读端）→ ② 每分区 MiddleAggregateConsumer
        //   （注册聚合交换读端，聚合输出按窗口键重哈希推窗口交换）→ ③ join 源：最末层 MiddleJoinConsumer
        //   → 中间层逆序 → 基表 Producers（build 先 probe）；scan 源：直接基表 Producers。
        JoinChainStage stage;
        if (links == null) {
            // scan 源：复算扫描生效请求（列 + 谓词，与原聚合算子 dispatchStage 一致），交给连接器再分区规划
            ReadRequest effective = baseRequest;
            for (String p : predicates) {
                if (p == null || p.isBlank()) continue;
                effective = probe.pushdownPredicate(effective, p);
            }
            stage = StageScheduler.planAggregateWindow(
                    queryId, workers, numPartitions, effective,
                    groupKeys, aggregations, caseProjections, memoryLimit, probe,
                    windowPartitionBy, windowOrderBy, windowAscending, windowFunctions);
        } else {
            stage = StageScheduler.planJoinAggregateWindow(
                    queryId, workers, numPartitions,
                    firstCatalog, firstSchema, firstTable, firstLeftKeys, firstProbe, firstFilter,
                    links, memoryLimit, groupKeys, aggregations, caseProjections,
                    windowPartitionBy, windowOrderBy, windowAscending, windowFunctions, firstColumns);
        }
        for (FragmentSpec fragment : withTerminal(stage.fragments())) {   // 分布式写 SinkStep + top-N 截断尾

            dispatch(fragment);
        }
        System.err.printf("[DistributedAggregateWindow] query=%s workers=%d partitions=%d join=%s groupKeys=%s "
                        + "windowPartitionBy=%s consumers=%d fragments=%d%n",
                queryId, workers.size(), numPartitions, firstTable, groupKeys,
                windowPartitionBy, stage.finalConsumerCount(), stage.fragments().size());
        return stage.finalConsumerCount();
    }

    @Override
    protected void closeProbes() {
        if (links == null) {
            try { probe.closeRead(); } catch (Throwable ignored) { }
        } else {
            try { firstProbe.closeRead(); } catch (Throwable ignored) { }
            for (JoinLink link : links) {
                try { link.rightProbe().closeRead(); } catch (Throwable ignored) { }
            }
        }
    }

    @Override
    public List<Field> outputFields() {
        // 复刻终端 WindowConsumer 输出页 schema = 聚合输出页（分组键 + 聚合结果列）+ 窗口结果列：
        //   join 源 = 复算最末层 join 全输出（左/右全列逐层剔除「右 join key 且左已有同名」）再按分组键取列；
        //   scan 源 = 基表扫描的 key 列；
        //   窗口结果 = 逐 WindowFunctionSpec（ROW_NUMBER/RANK/COUNT → BIGINT、其余 → DOUBLE、
        //   LAG/LEAD 镜像输入列类型；输入列按名取聚合输出列 Field）。
        List<Field> aggOut = new ArrayList<>();
        List<Field> aggInputs;
        if (links == null) {
            aggOut.addAll(scanFields(probe, baseRequest.catalog(), baseRequest.schema(),
                    baseRequest.table(), groupKeys));
            // 聚合输入列可能在分组键之外 —— 从实际扫描列集里按名取类型（MIN/MAX 输出同型）
            aggInputs = scanFields(probe, baseRequest.catalog(), baseRequest.schema(),
                    baseRequest.table(), baseRequest.columns().orElse(null));
        } else {
            List<Field> wire = new ArrayList<>(
                    scanFields(firstProbe, firstCatalog, firstSchema, firstTable, firstColumns));
            for (JoinLink link : links) {
                List<Field> right = scanFields(link.rightProbe(), link.rightCatalog(),
                        link.rightSchema(), link.rightTable(), link.rightColumns());
                wire = joinOutputFields(link.joinType(), wire, right, link.leftKeys(), link.rightKeys());
            }
            aggOut.addAll(fieldsByNames(wire, groupKeys));
            aggInputs = wire;
        }
        // CASE/IF/表达式派生列（聚合参数改写）不在扫描列里 —— 补上静态类型，
        // 否则 sum(CAST(x AS DECIMAL)) 的输出声明会退化。
        aggInputs = withDerivedFields(aggInputs, caseProjections);
        for (AggregationSpec a : aggregations) {
            aggOut.add(aggregationOutputField(a, aggInputs));
        }
        List<Field> out = new ArrayList<>(aggOut);
        for (WindowFunctionSpec s : windowFunctions) {
            out.add(windowFunctionField(s, name -> fieldOf(aggOut, name)));
        }
        return out;
    }
}
