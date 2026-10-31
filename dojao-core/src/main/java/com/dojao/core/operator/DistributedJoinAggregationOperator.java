package com.dojao.core.operator;

import com.dojao.core.runtime.master.StageScheduler;
import com.dojao.core.runtime.master.StageScheduler.JoinChainStage;
import com.dojao.core.runtime.master.StageScheduler.JoinLink;
import com.dojao.core.runtime.master.WorkerRegistration;
import com.dojao.core.runtime.worker.WorkerGateway;
import com.dojao.spi.service.DojaoConnector;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.List;

/**
 * 分布式 join→聚合算子（Master 侧）—— 左深多 join 链输出之上、<b>分组键与 join 键不共分区</b>的
 * GROUP BY 聚合（Plan B 二次 shuffle 最终聚合）。Master 永远不做归约：join 输出留在 Worker，按
 * 分组键重哈希 → 每个分组落唯一 Worker 做最终聚合 → 只有聚合结果页回传 Master。
 *
 * <p>与 {@link DistributedJoinOperator} 的差异只在最末层与终点：
 * <pre>
 *   DistributedJoinOperator（同 key 链式 / 普通）:
 *     最末层 Consumer → 回传 Master
 *   DistributedJoinAggregationOperator（本算子）:
 *     最末层 MiddleJoinConsumer（join → Partition(分组键) 重哈希 → 推聚合交换，不回 Master）
 *     └─ 每分区 AggregateConsumer（读聚合交换 → 本地 HashAggregation → 回传 Master）
 * </pre>
 * 中间 join 层与基表 Producer 完全复用 {@link StageScheduler#planJoinAggregation} 的现逻辑
 * （= planJoinChain 镜像 + 换最末层与聚合终点）。分组键经同一 round-robin owner 映射哈希 →
 * co-partitioning 不变量 → 每个 group 落唯一 bucket/Worker → Worker 本地聚合即最终结果，
 * Master 零合并，仅收集每组输出页。
 *
 * <p>正确性前提（调用方保证）：最末层 join 已在分布式执行（上游是 {@code DistributedJoinOperator}），
 * 且本算子承接其整条 Producer/Consumer 链 —— 同一查询内两次交换（join 交换 + 聚合交换）共用同一
 * PartitionLayout，group key 哈希落桶与 join key 哈希互不干扰但都确定。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/4
 */
public class DistributedJoinAggregationOperator extends AbstractDistributedOperator
        implements Explainable {

    private final String firstCatalog, firstSchema, firstTable;
    private final List<String> firstLeftKeys;
    private final DojaoConnector firstProbe;
    private final String firstFilter;
    private final List<JoinLink> links;
    private final long memoryLimit;
    /** 首表（T0，probe 侧）实际扫描的列集（顺序即页内列序，已含 join 键）；null/空 = 读全列。 */
    private final List<String> firstColumns;
    private final List<String> groupKeys;
    private final List<AggregationSpec> aggregations;
    private final List<CaseProjection> caseProjections;
    /** 多维聚合展开（可空）：只挂最末层 join 的 MiddleJoinConsumer，join 链与基表扫描都只做一次。 */
    private final FragmentSpec.ExpandStep expand;

    public DistributedJoinAggregationOperator(String queryId,
                                              String firstCatalog, String firstSchema, String firstTable,
                                              List<String> firstLeftKeys, DojaoConnector firstProbe,
                                              String firstFilter,
                                              List<JoinLink> links, long memoryLimit,
                                              List<String> groupKeys, List<AggregationSpec> aggregations,
                                              List<CaseProjection> caseProjections,
                                              int numPartitions,
                                              WorkerGateway gateway, List<WorkerRegistration> workers) {
        this(queryId, firstCatalog, firstSchema, firstTable, firstLeftKeys, firstProbe, firstFilter,
                links, memoryLimit, groupKeys, aggregations, caseProjections, numPartitions, gateway,
                workers, null);
    }

    /** 列裁剪重载：{@code firstColumns} 空 = 首表读全列（各层右表列集随 {@link JoinLink#rightColumns()}）。 */
    public DistributedJoinAggregationOperator(String queryId,
                                              String firstCatalog, String firstSchema, String firstTable,
                                              List<String> firstLeftKeys, DojaoConnector firstProbe,
                                              String firstFilter,
                                              List<JoinLink> links, long memoryLimit,
                                              List<String> groupKeys, List<AggregationSpec> aggregations,
                                              List<CaseProjection> caseProjections,
                                              int numPartitions,
                                              WorkerGateway gateway, List<WorkerRegistration> workers,
                                              List<String> firstColumns) {
        this(queryId, firstCatalog, firstSchema, firstTable, firstLeftKeys, firstProbe, firstFilter,
                links, memoryLimit, groupKeys, aggregations, caseProjections, numPartitions, gateway,
                workers, firstColumns, null);
    }

    /** 带多维聚合展开的完整形态（见 {@link #expand}）。 */
    public DistributedJoinAggregationOperator(String queryId,
                                              String firstCatalog, String firstSchema, String firstTable,
                                              List<String> firstLeftKeys, DojaoConnector firstProbe,
                                              String firstFilter,
                                              List<JoinLink> links, long memoryLimit,
                                              List<String> groupKeys, List<AggregationSpec> aggregations,
                                              List<CaseProjection> caseProjections,
                                              int numPartitions,
                                              WorkerGateway gateway, List<WorkerRegistration> workers,
                                              List<String> firstColumns,
                                              FragmentSpec.ExpandStep expand) {
        super("DistributedJoinAggregation", queryId, numPartitions, gateway, workers);
        this.firstCatalog = firstCatalog;
        this.firstSchema = firstSchema;
        this.firstTable = firstTable;
        this.firstLeftKeys = firstLeftKeys;
        this.firstProbe = firstProbe;
        this.firstFilter = firstFilter;
        this.links = List.copyOf(links);
        this.memoryLimit = memoryLimit;
        this.firstColumns = firstColumns;
        this.groupKeys = List.copyOf(groupKeys);
        this.aggregations = List.copyOf(aggregations);
        this.caseProjections = caseProjections == null ? List.of() : List.copyOf(caseProjections);
        this.expand = expand;
    }

    // ==================== 规划 spec 只读暴露 ====================
    // 上层（buildWindows 聚合→窗口重建 / 写路径）需要取回本算子已解析的聚合规划参数，
    // 以便原样重建「聚合 → 窗口」两级下沉链路（见 DistributedAggregateWindowOperator）。

    public String firstCatalog() { return firstCatalog; }

    public String firstSchema() { return firstSchema; }

    public String firstTable() { return firstTable; }

    public List<String> firstLeftKeys() { return firstLeftKeys; }

    public DojaoConnector firstProbe() { return firstProbe; }

    public String firstFilter() { return firstFilter; }

    public List<JoinLink> links() { return links; }

    /** 首表（T0）实际扫描的列集；null/空 = 读全列。 */
    public List<String> firstColumns() { return firstColumns; }

    public long memoryLimit() { return memoryLimit; }

    public List<String> groupKeys() { return groupKeys; }

    public List<AggregationSpec> aggregations() { return aggregations; }

    public List<CaseProjection> caseProjections() { return caseProjections; }

    /** 多维聚合展开描述（null = 普通聚合）。 */
    public FragmentSpec.ExpandStep expand() { return expand; }

    @Override
    public List<ScanColumn> scanColumnSets() {
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
        JoinChainStage stage = StageScheduler.planJoinAggregation(
                queryId, workers, numPartitions,
                firstCatalog, firstSchema, firstTable, firstLeftKeys, firstProbe, firstFilter,
                links, memoryLimit, groupKeys, aggregations, caseProjections, firstColumns, expand);
        // 片段已在 planJoinAggregation 内按反死锁顺序排好：
        // ① 每分区 AggregateConsumer（先注册聚合交换读端）→ ② 最末层 MiddleJoinConsumer
        // （join→按分组键重哈希→推聚合交换）→ ③ 中间层逆序 → ④ 基表 Producers（build 先 probe）
        for (FragmentSpec fragment : withTerminal(stage.fragments())) {   // 分布式写 SinkStep + top-N 截断尾

            dispatch(fragment);
        }
        System.err.printf("[DistributedJoinAggregation] query=%s workers=%d partitions=%d join=%s groupKeys=%s "
                        + "consumers=%d fragments=%d joins=%d%n",
                queryId, workers.size(), numPartitions, firstTable, groupKeys,
                stage.finalConsumerCount(), stage.fragments().size(), links.size());
        return stage.finalConsumerCount();
    }

    @Override
    protected void closeProbes() {
        try { firstProbe.closeRead(); } catch (Throwable ignored) { }
        for (JoinLink link : links) {
            try { link.rightProbe().closeRead(); } catch (Throwable ignored) { }
        }
    }

    /**
     * EXPLAIN 明细：probe 侧来源 + 逐层 join（类型 / build 表 / 键 / 算法）+ 二次 shuffle 聚合规格。
     *
     * <p>本算子在 Master 侧是一个叶子节点，原先 EXPLAIN 完全看不到它把哪张表当 probe、每层 join
     * 用什么算法、以及<b>单表 WHERE 谓词有没有随派发载荷下发</b>（Hive 分区裁剪的唯一入口）——
     * 用户在 ROLLUP/多表场景下无法判断下推是否生效。这里与 {@link DistributedJoinOperator}
     * 同一口径逐层打印 {@code filter=[...]}。</p>
     */
    @Override
    public List<String> explainLines() {
        List<String> out = new ArrayList<>();
        out.add("probe: " + qualified(firstCatalog, firstSchema, firstTable)
                + " keys=" + firstLeftKeys + filterSuffix(firstFilter));
        for (int i = 0; i < links.size(); i++) {
            JoinLink link = links.get(i);
            String keys = link.joinType() == JoinType.CROSS
                    ? "CROSS(no keys)"
                    : link.leftKeys() + "=" + link.rightKeys();
            out.add("join-" + (i + 1) + ": " + link.joinType()
                    + " build=" + qualified(link.rightCatalog(), link.rightSchema(), link.rightTable())
                    + " keys=" + keys
                    + " algo=" + JoinExplain.distributed(link.algorithm(), memoryLimit, numPartitions)
                    + filterSuffix(link.rightFilter()));
        }
        out.add("aggregate: groupBy=" + groupKeys
                + " aggs=" + aggregations.stream().map(AggregationSpec::outputColumn).toList()
                + " partitions=" + numPartitions + "（join 输出按分组键二次 shuffle）");
        if (expand != null) {
            out.add("expand: groupingSets=" + expand.groupingSets()
                    + "（join 链与基表扫描都只做一次）");
        }
        return out;
    }

    private static String filterSuffix(String filter) {
        return filter == null || filter.isBlank() ? "" : " filter=[" + filter + "]";
    }

    private static String qualified(String catalog, String schema, String table) {
        if (table == null) return "<none>";
        return (catalog == null || catalog.isBlank() ? "" : catalog + ".")
                + (schema == null || schema.isBlank() ? "" : schema + ".") + table;
    }

    @Override
    public List<Field> outputFields() {
        // 复刻最终 AggregateConsumer 输出页 schema：先复算最末层 join 全输出（左/右扫描列逐层剔除
        // 「右 join key 且左已有同名」列），聚合在 join 输出上按分组键重哈希后本地做：
        // 输出 = 分组键列（join 输出列类型）+ 各聚合结果列（AggregationSpec.outputArrowType）。
        // 列集必须与实际扫描一致（firstColumns / link.rightColumns()，空 = 全列）。
        List<Field> wire = new ArrayList<>(
                scanFields(firstProbe, firstCatalog, firstSchema, firstTable, firstColumns));
        for (JoinLink link : links) {
            List<Field> right = scanFields(link.rightProbe(), link.rightCatalog(), link.rightSchema(),
                    link.rightTable(), link.rightColumns());
            wire = joinOutputFields(link.joinType(), wire, right, link.leftKeys(), link.rightKeys());
        }
        // __grouping_id 是展开算子追加的合成列（不在 join 输出里），类型固定 BIGINT —— 按原位置换回
        List<String> wireKeys = new ArrayList<>(groupKeys.size());
        for (String k : groupKeys) {
            if (!ExpandOperator.GROUPING_ID_COLUMN.equalsIgnoreCase(k)) wireKeys.add(k);
        }
        List<Field> resolved = fieldsByNames(wire, wireKeys);
        List<Field> out = new ArrayList<>(groupKeys.size());
        int nextWireKey = 0;
        for (String k : groupKeys) {
            out.add(ExpandOperator.GROUPING_ID_COLUMN.equalsIgnoreCase(k)
                    ? ExpandOperator.GROUPING_ID_FIELD : resolved.get(nextWireKey++));
        }
        // CASE/IF/表达式派生列（聚合参数改写）不在扫描列里 —— 补上其静态类型，否则
        // sum(CAST(x AS DECIMAL)) 这类聚合结果会声明成 DOUBLE 而实际是 DECIMAL。
        List<Field> aggInputs = withDerivedFields(wire, caseProjections);
        for (AggregationSpec a : aggregations) {
            out.add(aggregationOutputField(a, aggInputs));
        }
        return out;
    }
}
