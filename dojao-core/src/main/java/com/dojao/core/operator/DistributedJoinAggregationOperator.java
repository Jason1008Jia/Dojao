package com.dojao.core.operator;

import com.dojao.core.runtime.master.StageScheduler;
import com.dojao.core.runtime.master.StageScheduler.JoinChainStage;
import com.dojao.core.runtime.master.StageScheduler.JoinLink;
import com.dojao.core.runtime.master.WorkerRegistration;
import com.dojao.core.runtime.worker.WorkerGateway;
import com.dojao.spi.bean.DojaoType;
import com.dojao.spi.service.DojaoConnector;
import com.dojao.spi.util.DojaoTypeMapper;
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
    /** GROUP BY 表达式派生分组键（最末层 join 输出上先物化派生列，再按派生名重哈希）。 */
    private final List<GroupKeyExpression> groupKeyExpressions;
    /** 各关系的派生源（下标 0 = 首表；下标 k = 第 k 层 join 右表）；null = 全普通基表。 */
    private final List<StageScheduler.DerivedSource> sources;
    /** 外连接同名列消歧别名（Worker 在本地 join 后、重哈希前套用）；空 = 无别名。 */
    private final List<FragmentSpec.JoinAlias> outputAliases;
    /**
     * 二级聚合（外层全局聚合下沉）：AggregateConsumer 本地聚合输出后同 Worker 零交换再聚一次，
     * 每分区只回传 1 行 partial，Master 侧 {@code PartialAggregationMergeOperator} 合并。
     * Planner 在 doOpen 前设置；null = 无。
     */
    private volatile FragmentSpec.AggregateStep followUp;

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
        this(queryId, firstCatalog, firstSchema, firstTable, firstLeftKeys, firstProbe, firstFilter,
                links, memoryLimit, groupKeys, aggregations, caseProjections, numPartitions, gateway,
                workers, firstColumns, expand, List.of(), null);
    }

    /**
     * 完整形态：+ GROUP BY 表达式派生键（{@code groupKeyExpressions}，最末层 join 输出上物化）+
     * 派生源（{@code sources}，子查询 / CTE 流水线原样保留，链上 CROSS 的 build 侧单值广播同样生效）。
     */
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
                                              FragmentSpec.ExpandStep expand,
                                              List<GroupKeyExpression> groupKeyExpressions,
                                              List<StageScheduler.DerivedSource> sources) {
        this(queryId, firstCatalog, firstSchema, firstTable, firstLeftKeys, firstProbe, firstFilter,
                links, memoryLimit, groupKeys, aggregations, caseProjections, numPartitions, gateway,
                workers, firstColumns, expand, groupKeyExpressions, sources, List.of());
    }

    /** 完整形态 + 外连接同名列消歧别名（Worker 在 join 后、重哈希前套用）。 */
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
                                              FragmentSpec.ExpandStep expand,
                                              List<GroupKeyExpression> groupKeyExpressions,
                                              List<StageScheduler.DerivedSource> sources,
                                              List<FragmentSpec.JoinAlias> outputAliases) {
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
        this.groupKeyExpressions = groupKeyExpressions == null ? List.of() : List.copyOf(groupKeyExpressions);
        this.sources = sources;
        this.outputAliases = outputAliases == null ? List.of() : List.copyOf(outputAliases);
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

    /** GROUP BY 表达式派生分组键（空 = 纯裸列分组）。 */
    public List<GroupKeyExpression> groupKeyExpressions() { return groupKeyExpressions; }

    /** 各关系的派生源（null = 全普通基表）。 */
    public List<StageScheduler.DerivedSource> sources() { return sources; }

    /** 外连接同名列消歧别名（空 = 无别名）。 */
    public List<FragmentSpec.JoinAlias> outputAliases() { return outputAliases; }

    /** 桶数（聚合交换的分区数；二级聚合的槽数必须与之一致）。 */
    public int numPartitions() { return numPartitions; }

    /** 设置二级聚合（Planner 在 doOpen 前调用；null = 无二级聚合）。 */
    public void setFollowUp(FragmentSpec.AggregateStep f) { this.followUp = f; }

    /** 已设置的二级聚合（可空；Planner / EXPLAIN 只读）。 */
    public FragmentSpec.AggregateStep followUp() { return followUp; }

    @Override
    public List<ScanColumn> scanColumnSets() {
        List<ScanColumn> out = new ArrayList<>();
        out.add(scanColumn(0, firstCatalog, firstSchema, firstTable, firstColumns));
        for (int i = 0; i < links.size(); i++) {
            JoinLink link = links.get(i);
            out.add(scanColumn(i + 1, link.rightCatalog(), link.rightSchema(), link.rightTable(),
                    link.rightColumns()));
        }
        return out;
    }

    /** 下标 i（0 = 首表；k = 第 k 层 join 右表）：派生源 → pipeline，否则 required 基表扫描。 */
    private ScanColumn scanColumn(int i, String catalog, String schema, String table, List<String> columns) {
        String relation = catalog + "." + schema + "." + table;
        boolean derived = sources != null && i < sources.size() && sources.get(i) != null;
        return derived ? ScanColumn.pipeline(relation) : ScanColumn.required(relation, columns);
    }

    @Override
    protected int dispatchStage() throws Exception {
        JoinChainStage stage = StageScheduler.planJoinAggregation(
                queryId, workers, numPartitions,
                firstCatalog, firstSchema, firstTable, firstLeftKeys, firstProbe, firstFilter,
                links, memoryLimit, groupKeys, aggregations, caseProjections, firstColumns, expand,
                groupKeyExpressions, sources, outputAliases, followUp);
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
            // 派生源所在层的右表由聚合流水线喂入 → 无右表探针
            if (link.rightProbe() != null) {
                try { link.rightProbe().closeRead(); } catch (Throwable ignored) { }
            }
        }
        if (sources != null) {
            for (StageScheduler.DerivedSource ds : sources) {
                if (ds != null && ds.innerProbe() != null) {
                    try { ds.innerProbe().closeRead(); } catch (Throwable ignored) { }
                }
            }
        }
    }

    /**
     * 某关系（下标 {@code i}，0 = 首表）的输出列：派生源 → 派生体声明列（无连接器元数据可查）；
     * 普通基表 → 连接器元数据扫描列。
     */
    private List<Field> sourceFields(int i, DojaoConnector probe, String catalog, String schema,
                                     String table, List<String> columns) {
        StageScheduler.DerivedSource ds =
                (sources == null || i >= sources.size()) ? null : sources.get(i);
        return ds != null ? StageScheduler.derivedFields(ds)
                : scanFields(probe, catalog, schema, table, columns);
    }

    /**
     * 把 GROUP BY 表达式派生分组键（发送端物化列，不在 join 输出里）补成 STRING 静态列 ——
     * 与 {@code DistributedAggregationOperator} 对派生键的既有口径一致，不因聚合上游不同而抛缺列。
     */
    private List<Field> withDerivedKeyFields(List<Field> wire) {
        if (groupKeyExpressions.isEmpty()) return wire;
        List<Field> out = new ArrayList<>(wire);
        for (GroupKeyExpression g : groupKeyExpressions) {
            if (fieldOf(out, g.derivedColumn()) == null) {
                out.add(DojaoTypeMapper.toField(g.derivedColumn(), DojaoType.STRING));
            }
        }
        return out;
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
                    + filterSuffix(link.rightFilter())
                    // ON 单侧残差：保留侧不能下推扫描（孤儿行仍须输出），在 Worker 匹配阶段求值
                    + (link.onLeftFilter() != null ? " onLeft=[" + link.onLeftFilter() + "]" : "")
                    + (link.onRightFilter() != null ? " onRight=[" + link.onRightFilter() + "]" : ""));
        }
        out.add("aggregate: groupBy=" + groupKeys
                + " aggs=" + aggregations.stream().map(AggregationSpec::outputColumn).toList()
                + " partitions=" + numPartitions + "（join 输出按分组键二次 shuffle）");
        if (expand != null) {
            out.add("expand: groupingSets=" + expand.groupingSets()
                    + "（join 链与基表扫描都只做一次）");
        }
        FragmentSpec.AggregateStep fu = followUp;
        if (fu != null) {
            out.add("follow-up aggregate on workers groupBy="
                    + (fu.keyColumns().isEmpty() ? "<global>" : fu.keyColumns())
                    + " aggs=" + fu.aggregations().stream()
                    .map(d -> d.aggregateName() + "(" + (d.inputColumn() == null ? "*" : d.inputColumn())
                            + ") AS " + d.outputColumn()).toList());
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
        // 列集必须与实际扫描一致（firstColumns / link.rightColumns()，空 = 全列）；派生源按派生体声明列。
        List<Field> wire = new ArrayList<>(
                sourceFields(0, firstProbe, firstCatalog, firstSchema, firstTable, firstColumns));
        int idx = 1;
        for (JoinLink link : links) {
            List<Field> right = sourceFields(idx, link.rightProbe(), link.rightCatalog(), link.rightSchema(),
                    link.rightTable(), link.rightColumns());
            wire = joinOutputFields(link.joinType(), wire, right, link.leftKeys(), link.rightKeys());
            idx++;
        }
        // 表达式派生分组键（如 year(created_at)）不在 join 输出里 —— 发送端物化后才出现，类型按 STRING
        // 兜底（与 DistributedAggregationOperator 对派生键的既有口径一致）。
        List<Field> wireWithDerived = withDerivedKeyFields(wire);
        // __grouping_id 是展开算子追加的合成列（不在 join 输出里），类型固定 BIGINT —— 按原位置换回
        List<String> wireKeys = new ArrayList<>(groupKeys.size());
        for (String k : groupKeys) {
            if (!ExpandOperator.GROUPING_ID_COLUMN.equalsIgnoreCase(k)) wireKeys.add(k);
        }
        List<Field> resolved = fieldsByNames(wireWithDerived, wireKeys);
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
