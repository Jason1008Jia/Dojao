package com.dojao.core.operator;

import com.dojao.core.runtime.master.StageScheduler;
import com.dojao.core.runtime.master.WorkerRegistration;
import com.dojao.core.runtime.worker.WorkerGateway;
import com.dojao.spi.bean.ReadRequest;
import com.dojao.spi.service.DojaoConnector;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.List;

/**
 * 分布式聚合算子（Master 侧）—— 一个 GROUP BY 聚合的整条链路（扫描 → 按 key 哈希分区 →
 * Worker 本地 HashAggregation）派发到集群执行，Master 只做派发 + 收集最终结果
 * （<b>cowork without master</b>：中间数据全程留在 Worker，只有聚合输出页回传 Master）。
 *
 * <p>执行形态：
 * <pre>
 *   doOpen → dispatchStage：
 *     StageScheduler.planAggregation()   切分：N 个 Producer + 每 Worker 一个 AggregateConsumer
 *     dispatch Consumer（先）            同步注册各 Worker 的 ShuffleReader
 *     dispatch Producer（后）            Worker 扫切片 → 按聚合 key 哈希分区 → 推给归属 Worker
 *     Consumer：own 桶 → PartitionedSourceOperator → 本地 HashAggregation → 输出页回传 Master
 *   doNext：从结果队列拉聚合输出页；全部 Consumer 完成 → EOF；任一失败 → fail-fast
 *   doClose：反向取消所有片段任务，排空队列，释放探针连接器
 * </pre>
 *
 * <p><b>正确性</b>：owner 映射与同查询其他 shuffle 完全一致（co-partitioning）。每个分组 key
 * 按哈希落唯一桶 → 落在唯一 Worker，Worker 内聚合结果即为该 key 的完整聚合，各 Worker 输出
 * 的并集 = 全局正确结果。</p>
 *
 * @author jiaxin
 * @version 3.0
 * @date 2026/8/20
 */
public class DistributedAggregationOperator extends AbstractDistributedOperator implements Explainable {

    /** 探针连接器：Master 侧本地做分区规划（不参与数据拉取）。 */
    private final ReadRequest baseRequest;
    private final List<String> predicates;
    private final List<String> keyColumns;
    private final List<AggregationSpec> aggregations;
    private final List<CaseProjection> caseProjections;   // CASE WHEN 派生列（Worker 侧先算再聚合）
    private final List<GroupKeyExpression> groupKeyExpressions;   // GROUP BY 表达式派生列（Producer 分区前物化）
    private final long memoryLimit;

    /**
     * 扫描并行度（切片数上限）—— 与 {@code numPartitions}（聚合桶数）<b>刻意分开</b>。
     * 全局聚合（无分组键）桶数被压到 1 以省掉空消费者的空转尾巴，但切片仍按真实并行度规划，
     * 否则读会退化成单线程串行。见 {@link StageScheduler#planAggregation}。
     */
    private final int scanParallelism;
    private final DojaoConnector probe;

    /**
     * 二级聚合（可空）：外层全局聚合下沉的 partial 段 —— 每个分区在本 Worker 上先算 partial
     * （COUNT/SUM/MIN/MAX），只回传 1 行，Master 侧 {@code PartialAggregationMergeOperator} 合并。
     *
     * <p>由 Planner 在 {@code doOpen} 前设置（{@link #setFollowUp}）。一旦设置，本算子回传的页
     * 列 = 二级聚合的输出列（不再是分组键 + 原聚合列）⇒ 不能再作为 CTAS/INSERT 的分布式根，
     * 也不能挂 top-N 尾（Planner 会在这类用法下改用 Master 本地写路径）。</p>
     */
    private volatile FragmentSpec.AggregateStep followUp;

    /**
     * 多维聚合展开（可空）：{@code GROUP BY ROLLUP/CUBE/GROUPING SETS} 只扫一次表 —— 每个 Producer
     * 在扫描之后、按 {@code keyColumns}（含 {@code __grouping_id}）哈希分区之前插一层
     * {@link ExpandOperator}，把每行复制成 N 份。null = 普通聚合（行为不变）。
     */
    private final FragmentSpec.ExpandStep expand;

    public DistributedAggregationOperator(String queryId, ReadRequest baseRequest, List<String> predicates,
                                          List<String> keyColumns, List<AggregationSpec> aggregations,
                                          List<CaseProjection> caseProjections,
                                          List<GroupKeyExpression> groupKeyExpressions,
                                          long memoryLimit, int numPartitions, int scanParallelism,
                                          WorkerGateway gateway, DojaoConnector probe,
                                          List<WorkerRegistration> workers) {
        this(queryId, baseRequest, predicates, keyColumns, aggregations, caseProjections,
                groupKeyExpressions, memoryLimit, numPartitions, scanParallelism, gateway, probe, workers, null);
    }

    /** 带多维聚合展开的完整形态（见 {@link #expand}）。 */
    public DistributedAggregationOperator(String queryId, ReadRequest baseRequest, List<String> predicates,
                                          List<String> keyColumns, List<AggregationSpec> aggregations,
                                          List<CaseProjection> caseProjections,
                                          List<GroupKeyExpression> groupKeyExpressions,
                                          long memoryLimit, int numPartitions, int scanParallelism,
                                          WorkerGateway gateway, DojaoConnector probe,
                                          List<WorkerRegistration> workers,
                                          FragmentSpec.ExpandStep expand) {
        super("DistributedAggregation", queryId, numPartitions, gateway, workers);
        this.scanParallelism = Math.max(1, scanParallelism);
        this.baseRequest = baseRequest;
        this.predicates = List.copyOf(predicates);
        this.keyColumns = List.copyOf(keyColumns);
        this.aggregations = List.copyOf(aggregations);
        this.caseProjections = List.copyOf(caseProjections);
        this.groupKeyExpressions = List.copyOf(groupKeyExpressions);
        this.memoryLimit = memoryLimit;
        this.probe = probe;
        this.expand = expand;
    }

    // ==================== 规划 spec 只读暴露 ====================
    // 上层（buildWindows 聚合→窗口重建 / 写路径）需要取回本算子已解析的聚合规划参数，
    // 以便原样重建「聚合 → 窗口」两级下沉链路（见 DistributedAggregateWindowOperator）。

    public ReadRequest baseRequest() { return baseRequest; }

    public List<String> predicates() { return predicates; }

    public List<String> keyColumns() { return keyColumns; }

    public List<AggregationSpec> aggregations() { return aggregations; }

    public List<CaseProjection> caseProjections() { return caseProjections; }

    public List<GroupKeyExpression> groupKeyExpressions() { return groupKeyExpressions; }

    public long memoryLimit() { return memoryLimit; }

    public DojaoConnector probe() { return probe; }

    /** 多维聚合展开描述（null = 普通聚合）。 */
    public FragmentSpec.ExpandStep expand() { return expand; }

    /** 桶数（内层聚合的分区数；二级聚合的槽数必须与之一致）。 */
    public int numPartitions() { return numPartitions; }

    /** 设置二级聚合（Planner 在 doOpen 前调用；null = 无二级聚合）。 */
    public void setFollowUp(FragmentSpec.AggregateStep f) { this.followUp = f; }

    /** 已设置的二级聚合（可空；Planner / EXPLAIN 只读）。 */
    public FragmentSpec.AggregateStep followUp() { return followUp; }

    /** EXPLAIN 明细：扫描表 / 分组键 / 聚合函数 / 下推谓词 / 聚合桶数。 */
    @Override
    public List<String> explainLines() {
        List<String> out = new ArrayList<>();
        out.add("scan " + baseRequest.catalog() + "." + baseRequest.schema() + "." + baseRequest.table()
                + (predicates.isEmpty() ? "" : " filter=[" + String.join(" AND ", predicates) + "]"));
        out.add("aggregate groupBy=" + (keyColumns.isEmpty() ? "<global>" : keyColumns)
                + " aggs=" + aggregations.stream().map(DistributedAggregationOperator::describeAgg).toList()
                + " partitions=" + numPartitions);
        if (expand != null) {
            out.add("expand groupingSets=" + expand.groupingSets() + "（表只扫一次）");
        }
        FragmentSpec.AggregateStep fu = followUp;
        if (fu != null) {
            // 二级聚合 = 外层全局聚合的 partial 段（每分区 1 行 partial，Master 再合并）
            out.add("follow-up aggregate on workers groupBy="
                    + (fu.keyColumns().isEmpty() ? "<global>" : fu.keyColumns())
                    + " aggs=" + fu.aggregations().stream()
                    .map(d -> d.aggregateName() + "(" + (d.inputColumn() == null ? "*" : d.inputColumn())
                            + ") AS " + d.outputColumn()).toList());
        }
        return out;
    }

    /** 聚合函数的人类可读形态（{@code sum(amount) AS total}）。 */
    private static String describeAgg(AggregationSpec spec) {
        if (spec.isCollect()) {
            return "collect(" + spec.collectInputColumns() + ") AS " + spec.outputColumn();
        }
        String fn = spec.aggregate() == null ? "aggregate" : spec.aggregate().functionName();
        return fn + "(" + (spec.inputColumn() == null ? "*" : spec.inputColumn()) + ") AS "
                + spec.outputColumn();
    }

    @Override
    public List<ScanColumn> scanColumnSets() {
        // 列随 ReadRequest 走（空 = 连接器语义读全表）：全局聚合（无分组键、COUNT(*)）本就无需扫列 ⇒ optional
        return List.of(ScanColumn.optional(
                baseRequest.catalog() + "." + baseRequest.schema() + "." + baseRequest.table(),
                baseRequest.columns().orElse(null)));
    }

    @Override
    protected int dispatchStage() throws Exception {
        // 复算扫描生效请求：列 + 谓词（与原 scan.pushdown 一致），交给连接器再分区规划
        ReadRequest effective = baseRequest;
        for (String p : predicates) {
            if (p == null || p.isBlank()) continue;
            effective = probe.pushdownPredicate(effective, p);
        }
        StageScheduler.DistributedStage stage = StageScheduler.planAggregation(
                queryId, workers, numPartitions, scanParallelism, effective, keyColumns, aggregations,
                caseProjections, groupKeyExpressions, memoryLimit, probe, followUp, expand);

        // 先 Consumer（同步注册接收端）后 Producer（发送端）—— 发送方 push 前所有接收端就绪
        List<FragmentSpec> consumers = withTerminal(stage.consumers());   // 分布式写 SinkStep + top-N 截断尾

        for (FragmentSpec consumer : consumers) {
            dispatch(consumer);
        }
        for (FragmentSpec producer : stage.producers()) {
            dispatch(producer);
        }
        return stage.finalConsumerCount();
    }

    @Override
    protected void closeProbes() {
        try { probe.closeRead(); } catch (Throwable ignored) { }
    }

    @Override
    public List<Field> outputFields() {
        // CTAS 建表 schema = 聚合输出页 schema（AggregationHashTable.buildOutput 的形态复刻）：
        // group key 列在前（Worker 扫描这些列，类型走连接器精确 sourceFields），聚合结果列在后
        // （AggregationSpec.outputArrowType：count→BIGINT、其余→DOUBLE / collect→元素类型）。
        // __grouping_id 是展开算子追加的合成列（不在扫描源里），类型固定 BIGINT
        List<String> scanKeys = new ArrayList<>(keyColumns.size());
        for (String k : keyColumns) {
            if (!ExpandOperator.GROUPING_ID_COLUMN.equalsIgnoreCase(k)) scanKeys.add(k);
        }
        List<Field> scannedKeys = scanFields(probe, baseRequest.catalog(), baseRequest.schema(),
                baseRequest.table(), scanKeys);
        List<Field> out = new ArrayList<>(keyColumns.size());
        int nextScanKey = 0;
        for (String k : keyColumns) {
            out.add(ExpandOperator.GROUPING_ID_COLUMN.equalsIgnoreCase(k)
                    ? ExpandOperator.GROUPING_ID_FIELD : scannedKeys.get(nextScanKey++));
        }
        // 聚合输入列类型：扫描请求列集里按名查（MIN/MAX 需要与输入同型的输出列）
        List<Field> inputFields = withDerivedFields(scanFields(probe, baseRequest.catalog(),
                baseRequest.schema(), baseRequest.table(), baseRequest.columns().orElse(null)),
                caseProjections);
        for (AggregationSpec a : aggregations) {
            out.add(aggregationOutputField(a, inputFields));
        }
        return out;
    }
}
