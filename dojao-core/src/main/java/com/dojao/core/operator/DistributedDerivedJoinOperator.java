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
 * 分布式派生表 join 算子（Master 侧：派生表 = 单表 GROUP BY，可作 join 的<b>任一 SQL 侧</b>）——
 * 派生表一侧整条链路（扫派生基表 → 按分组键分区 → 每分区本地聚合 → 派生内层 HAVING 过滤 →
 * 按派生 join 键重哈希分区）与另一侧裸表（peer）join（扫表 → 按 peer join 键分区）<b>全部派发到
 * Worker</b>，Worker 本地做最终 join，Master 只做派发 + 收集最终 join 输出（唯一一次跨 Master 的数据）。
 *
 * <pre>
 *   doOpen（final，AbstractDistributedOperator）：
 *     dispatchStage()：
 *       StageScheduler.planDerivedJoin()   切分：最终 join Consumer + MiddleAggregateConsumer + 基表 Producer
 *       按反死锁顺序派发：最终 join Consumer → MiddleAggregateConsumer → Producer（build 侧先、probe 侧后）
 *       派生体输出经 MiddleAggregateConsumer 重哈希推给派生流所在交换，<b>不回传 Master</b>
 *   doNext（final）：从结果队列拉最末 join 输出页；全部 Consumer 完成 → EOF；任一失败 → fail-fast
 *   doClose（final）：反向取消所有片段任务，排空队列，closeProbes() 释放全部探针连接器
 * </pre>
 *
 * <p><b>列序归一</b>：派生 / peer 两关系按 SQL 顺序落 HashJoinOperator 通道（SQL 左 → probe 左通道，
 * SQL 右 → build 右通道），{@code derivedOnLeft} 只改派生流走哪个通道 —— join 键顺序、输出列顺序、
 * LEFT/RIGHT/FULL 空值扩展语义全部与单机一致，派生表在 LEFT 或 RIGHT 无需任何列重排。</p>
 *
 * <p><b>正确性</b>：全部 shuffle 共用同一 round-robin owner 映射（co-partitioning）。分组键 hash 到
 * 唯一桶 → 每分区本地聚合即该 key 的完整聚合 → 派生内层 HAVING 按 key（=聚合输出行）过滤再重分区
 * （每 key 全行同分区聚齐，语义与单机一致）→ 按派生 join 键重哈希（join 键 == 分组键时恒等、零跨机）
 * 与 peer join 键同桶 → Worker 本地 join 结果正确，各 Worker 输出并集 = 全局结果。</p>
 *
 * @author jiaxin
 * @version 2.0
 * @date 2026/9/2
 */
public class DistributedDerivedJoinOperator extends AbstractDistributedOperator
        implements Explainable, WindowChainCarrier {

    /** 派生表基表扫描请求（已列裁剪 + 派生内层 WHERE 下推，谓词由 ScanStep 承载）。 */
    private final ReadRequest innerRequest;
    /** 派生表聚合分组键（= 派生基表分区键）。 */
    private final List<String> innerKeys;
    /** 派生表聚合函数（AggregationSpec，输出列名 = 派生体输出列）。 */
    private final List<AggregationSpec> aggregations;
    /**
     * 两级聚合派生体的<b>外层</b>分组键 / 聚合规格（空 = 单级）。两级分组键须相同
     * （分区键 = 分组键 ⇒ 每分区两级都是终态）；DA 中间层顺序执行内层聚合 → 外层聚合。
     */
    private final List<String> outerKeys;
    private final List<AggregationSpec> outerAggregations;
    /** 派生内层聚合输入的 CASE/IF 派生列投影（SUM(CASE WHEN ...) 等；本地聚合前置逐行计算，可空）。 */
    private final List<CaseProjection> caseProjections;
    /** 派生表一侧的 join 键（ON 条件中引用派生体输出的列，须 ∈ 派生体输出列）。 */
    private final List<String> derivedJoinKeys;
    /** 派生内层 HAVING 谓词（已做聚合→输出列名替换，Worker 侧本地聚合后过滤，可 null）。 */
    private final String havingFilter;
    /** true → 派生表是 SQL 左侧关系（operator probe 侧）；false → SQL 右侧（build 侧）。 */
    private final boolean derivedOnLeft;

    /** peer（裸表）侧 表名 / join 键 / 探针 / 单表 WHERE 下推谓词。 */
    private final String peerCatalog, peerSchema, peerTable;
    private final List<String> peerKeys;
    /** peer 表扫描列集（已按查询引用裁剪，join 输出列序/宽度与 outCols 对齐，避免回传多余列）。 */
    private final List<String> peerColumns;
    private final DojaoConnector peerProbe;
    private final String peerFilter;
    private final JoinType joinType;
    private final long memoryLimit;
    /** 探针连接器：Master 侧本地做分区规划（不参与数据拉取）。 */
    private final DojaoConnector innerProbe;
    /** 投影型派生源的逐行投影派生列（无聚合）；空 = 聚合型派生源。 */
    private final List<StageScheduler.DerivedProjection> projections;
    /** 窗口型派生源的单窗口组（无聚合）；null = 非窗口型派生源。 */
    private final StageScheduler.DerivedWindow window;
    /** 最终 join 的 Worker 侧算法（规划期选型：HASH 不可溢写；build 侧预估过大选 SORT_MERGE）。 */
    private final FragmentSpec.JoinAlgorithm joinAlgorithm;
    /**
     * 最末层 join 输出上的<b>窗口分组链</b>（派生源 join 上的窗口同样下沉）：空 = 无窗口链。
     * 编排见 {@code StageScheduler.planDerivedJoin} 的窗口段。
     */
    private List<FragmentSpec.WindowStep> windowChain;
    /** 外连接同名列消歧别名（空 = 无）：最终 join 之后、窗口重分区之前套用。 */
    private final List<FragmentSpec.JoinAlias> outputAliases;

    public DistributedDerivedJoinOperator(String queryId,
                                          boolean derivedOnLeft,
                                          ReadRequest innerRequest, List<String> innerKeys,
                                          List<AggregationSpec> aggregations,
                                          List<CaseProjection> caseProjections,
                                          List<String> derivedJoinKeys, String havingFilter,
                                          String peerCatalog, String peerSchema, String peerTable,
                                          List<String> peerKeys, List<String> peerColumns, String peerFilter,
                                          JoinType joinType,
                                          long memoryLimit, int numPartitions,
                                          WorkerGateway gateway,
                                          DojaoConnector innerProbe, DojaoConnector peerProbe,
                                          List<WorkerRegistration> workers) {
        this(queryId, derivedOnLeft, innerRequest, innerKeys, aggregations, caseProjections,
                derivedJoinKeys, havingFilter, peerCatalog, peerSchema, peerTable, peerKeys, peerColumns,
                peerFilter, joinType, memoryLimit, numPartitions, gateway, innerProbe, peerProbe,
                workers, null, null);
    }

    /** 投影型派生源重载：{@code projections} 非空 ⇒ 派生体无聚合，只在 Producer 侧逐行物化派生列。 */
    public DistributedDerivedJoinOperator(String queryId,
                                          boolean derivedOnLeft,
                                          ReadRequest innerRequest, List<String> innerKeys,
                                          List<AggregationSpec> aggregations,
                                          List<CaseProjection> caseProjections,
                                          List<String> derivedJoinKeys, String havingFilter,
                                          String peerCatalog, String peerSchema, String peerTable,
                                          List<String> peerKeys, List<String> peerColumns, String peerFilter,
                                          JoinType joinType,
                                          long memoryLimit, int numPartitions,
                                          WorkerGateway gateway,
                                          DojaoConnector innerProbe, DojaoConnector peerProbe,
                                          List<WorkerRegistration> workers,
                                          List<StageScheduler.DerivedProjection> projections) {
        this(queryId, derivedOnLeft, innerRequest, innerKeys, aggregations, caseProjections,
                derivedJoinKeys, havingFilter, peerCatalog, peerSchema, peerTable, peerKeys, peerColumns,
                peerFilter, joinType, memoryLimit, numPartitions, gateway, innerProbe, peerProbe,
                workers, projections, null);
    }

    /** 窗口型派生源重载：{@code window} 非空 ⇒ 派生体无聚合，Worker 侧分区内本地窗口后流进 join。 */
    public DistributedDerivedJoinOperator(String queryId,
                                          boolean derivedOnLeft,
                                          ReadRequest innerRequest, List<String> innerKeys,
                                          List<AggregationSpec> aggregations,
                                          List<CaseProjection> caseProjections,
                                          List<String> derivedJoinKeys, String havingFilter,
                                          String peerCatalog, String peerSchema, String peerTable,
                                          List<String> peerKeys, List<String> peerColumns, String peerFilter,
                                          JoinType joinType,
                                          long memoryLimit, int numPartitions,
                                          WorkerGateway gateway,
                                          DojaoConnector innerProbe, DojaoConnector peerProbe,
                                          List<WorkerRegistration> workers,
                                          List<StageScheduler.DerivedProjection> projections,
                                          StageScheduler.DerivedWindow window) {
        this(queryId, derivedOnLeft, innerRequest, innerKeys, aggregations, caseProjections,
                derivedJoinKeys, havingFilter, peerCatalog, peerSchema, peerTable, peerKeys, peerColumns,
                peerFilter, joinType, memoryLimit, numPartitions, gateway, innerProbe, peerProbe,
                workers, projections, window, FragmentSpec.JoinAlgorithm.HASH);
    }

    /** 全参重载：显式指定最终 join 在 Worker 上的算法（HASH fail-fast；SORT_MERGE 可溢写）。 */
    public DistributedDerivedJoinOperator(String queryId,
                                          boolean derivedOnLeft,
                                          ReadRequest innerRequest, List<String> innerKeys,
                                          List<AggregationSpec> aggregations,
                                          List<CaseProjection> caseProjections,
                                          List<String> derivedJoinKeys, String havingFilter,
                                          String peerCatalog, String peerSchema, String peerTable,
                                          List<String> peerKeys, List<String> peerColumns, String peerFilter,
                                          JoinType joinType,
                                          long memoryLimit, int numPartitions,
                                          WorkerGateway gateway,
                                          DojaoConnector innerProbe, DojaoConnector peerProbe,
                                          List<WorkerRegistration> workers,
                                          List<StageScheduler.DerivedProjection> projections,
                                          StageScheduler.DerivedWindow window,
                                          FragmentSpec.JoinAlgorithm joinAlgorithm) {
        this(queryId, derivedOnLeft, innerRequest, innerKeys, aggregations, caseProjections,
                derivedJoinKeys, havingFilter, peerCatalog, peerSchema, peerTable, peerKeys, peerColumns,
                peerFilter, joinType, memoryLimit, numPartitions, gateway, innerProbe, peerProbe,
                workers, projections, window, joinAlgorithm, List.of());
    }

    /** 全参 + <b>窗口链</b>：join 输出在 Worker 内按首组窗口键重分区后依次执行各组窗口。 */
    public DistributedDerivedJoinOperator(String queryId,
                                          boolean derivedOnLeft,
                                          ReadRequest innerRequest, List<String> innerKeys,
                                          List<AggregationSpec> aggregations,
                                          List<CaseProjection> caseProjections,
                                          List<String> derivedJoinKeys, String havingFilter,
                                          String peerCatalog, String peerSchema, String peerTable,
                                          List<String> peerKeys, List<String> peerColumns, String peerFilter,
                                          JoinType joinType,
                                          long memoryLimit, int numPartitions,
                                          WorkerGateway gateway,
                                          DojaoConnector innerProbe, DojaoConnector peerProbe,
                                          List<WorkerRegistration> workers,
                                          List<StageScheduler.DerivedProjection> projections,
                                          StageScheduler.DerivedWindow window,
                                          FragmentSpec.JoinAlgorithm joinAlgorithm,
                                          List<FragmentSpec.WindowStep> windowChain) {
        this(queryId, derivedOnLeft, innerRequest, innerKeys, aggregations, caseProjections,
                derivedJoinKeys, havingFilter, peerCatalog, peerSchema, peerTable, peerKeys, peerColumns,
                peerFilter, joinType, memoryLimit, numPartitions, gateway, innerProbe, peerProbe,
                workers, projections, window, joinAlgorithm, windowChain, List.of());
    }

    /** 全参 + 两级聚合派生体（外层分组键 / 聚合规格；分区键 = 分组键 ⇒ 每分区两级终态）。 */
    public DistributedDerivedJoinOperator(String queryId,
                                          boolean derivedOnLeft,
                                          ReadRequest innerRequest, List<String> innerKeys,
                                          List<AggregationSpec> aggregations,
                                          List<CaseProjection> caseProjections,
                                          List<String> derivedJoinKeys, String havingFilter,
                                          String peerCatalog, String peerSchema, String peerTable,
                                          List<String> peerKeys, List<String> peerColumns, String peerFilter,
                                          JoinType joinType,
                                          long memoryLimit, int numPartitions,
                                          WorkerGateway gateway,
                                          DojaoConnector innerProbe, DojaoConnector peerProbe,
                                          List<WorkerRegistration> workers,
                                          List<StageScheduler.DerivedProjection> projections,
                                          StageScheduler.DerivedWindow window,
                                          List<String> outerKeys,
                                          List<AggregationSpec> outerAggregations,
                                          FragmentSpec.JoinAlgorithm joinAlgorithm) {
        this(queryId, derivedOnLeft, innerRequest, innerKeys, aggregations, caseProjections,
                derivedJoinKeys, havingFilter, peerCatalog, peerSchema, peerTable, peerKeys, peerColumns,
                peerFilter, joinType, memoryLimit, numPartitions, gateway, innerProbe, peerProbe,
                workers, projections, window, joinAlgorithm, List.of(), List.of(),
                outerKeys, outerAggregations);
    }

    /** 全参 + 窗口链 + 外连接同名列消歧别名（最终 join 之后、窗口重分区之前套用）。 */
    public DistributedDerivedJoinOperator(String queryId,
                                          boolean derivedOnLeft,
                                          ReadRequest innerRequest, List<String> innerKeys,
                                          List<AggregationSpec> aggregations,
                                          List<CaseProjection> caseProjections,
                                          List<String> derivedJoinKeys, String havingFilter,
                                          String peerCatalog, String peerSchema, String peerTable,
                                          List<String> peerKeys, List<String> peerColumns, String peerFilter,
                                          JoinType joinType,
                                          long memoryLimit, int numPartitions,
                                          WorkerGateway gateway,
                                          DojaoConnector innerProbe, DojaoConnector peerProbe,
                                          List<WorkerRegistration> workers,
                                          List<StageScheduler.DerivedProjection> projections,
                                          StageScheduler.DerivedWindow window,
                                          FragmentSpec.JoinAlgorithm joinAlgorithm,
                                          List<FragmentSpec.WindowStep> windowChain,
                                          List<FragmentSpec.JoinAlias> outputAliases) {
        this(queryId, derivedOnLeft, innerRequest, innerKeys, aggregations, caseProjections,
                derivedJoinKeys, havingFilter, peerCatalog, peerSchema, peerTable, peerKeys, peerColumns,
                peerFilter, joinType, memoryLimit, numPartitions, gateway, innerProbe, peerProbe,
                workers, projections, window, joinAlgorithm, windowChain, outputAliases,
                List.of(), List.of());
    }

    /** 全参 + 窗口链 + 别名 + 两级聚合派生体（唯一真正赋值的构造）。 */
    public DistributedDerivedJoinOperator(String queryId,
                                          boolean derivedOnLeft,
                                          ReadRequest innerRequest, List<String> innerKeys,
                                          List<AggregationSpec> aggregations,
                                          List<CaseProjection> caseProjections,
                                          List<String> derivedJoinKeys, String havingFilter,
                                          String peerCatalog, String peerSchema, String peerTable,
                                          List<String> peerKeys, List<String> peerColumns, String peerFilter,
                                          JoinType joinType,
                                          long memoryLimit, int numPartitions,
                                          WorkerGateway gateway,
                                          DojaoConnector innerProbe, DojaoConnector peerProbe,
                                          List<WorkerRegistration> workers,
                                          List<StageScheduler.DerivedProjection> projections,
                                          StageScheduler.DerivedWindow window,
                                          FragmentSpec.JoinAlgorithm joinAlgorithm,
                                          List<FragmentSpec.WindowStep> windowChain,
                                          List<FragmentSpec.JoinAlias> outputAliases,
                                          List<String> outerKeys,
                                          List<AggregationSpec> outerAggregations) {
        super("DistributedDerivedJoin", queryId, numPartitions, gateway, workers);
        this.derivedOnLeft = derivedOnLeft;
        this.innerRequest = innerRequest;
        this.innerKeys = List.copyOf(innerKeys);
        this.aggregations = List.copyOf(aggregations);
        this.outerKeys = outerKeys == null ? List.of() : List.copyOf(outerKeys);
        this.outerAggregations = outerAggregations == null ? List.of() : List.copyOf(outerAggregations);
        this.caseProjections = caseProjections == null ? List.of() : List.copyOf(caseProjections);
        this.derivedJoinKeys = List.copyOf(derivedJoinKeys);
        this.havingFilter = havingFilter;
        this.peerCatalog = peerCatalog;
        this.peerSchema = peerSchema;
        this.peerTable = peerTable;
        this.peerKeys = List.copyOf(peerKeys);
        this.peerColumns = peerColumns == null ? List.of() : List.copyOf(peerColumns);
        this.peerFilter = peerFilter;
        this.joinType = joinType;
        this.memoryLimit = memoryLimit;
        this.innerProbe = innerProbe;
        this.peerProbe = peerProbe;
        this.projections = projections == null || projections.isEmpty() ? null : List.copyOf(projections);
        this.window = window;
        this.joinAlgorithm = joinAlgorithm == null ? FragmentSpec.JoinAlgorithm.HASH : joinAlgorithm;
        this.windowChain = windowChain == null ? List.of() : List.copyOf(windowChain);
        this.outputAliases = outputAliases == null ? List.of() : List.copyOf(outputAliases);
    }

    /**
     * 克隆本算子并挂上<b>窗口分组链</b>（join → 窗口#0 → 重分区 → 窗口#1 …，全留 Worker）：
     * 派生流水线 / peer 扫描 / join 选型原样保留。返回的新算子替代原算子执行。
     */
    public DistributedDerivedJoinOperator withWindowChain(List<FragmentSpec.WindowStep> steps) {
        return withWindowChain(steps, outputAliases);
    }

    /** 同上 + 外连接同名列消歧别名（最终 join 之后、窗口重分区之前套用）。 */
    public DistributedDerivedJoinOperator withWindowChain(List<FragmentSpec.WindowStep> steps,
                                                          List<FragmentSpec.JoinAlias> aliases) {
        // 已有窗口链 ⇒ 本算子是不可再重建的边界（重建会把上一查询层的窗口整段顶掉）。
        if (!windowChain.isEmpty()) {
            throw new IllegalStateException(
                    "DistributedDerivedJoinOperator 已携带窗口链（" + windowChain.size()
                            + " 组），不能再叠加 windowChain —— 重建舞台会丢弃既有窗口");
        }
        return new DistributedDerivedJoinOperator(queryId, derivedOnLeft,
                innerRequest, innerKeys, aggregations, caseProjections, derivedJoinKeys, havingFilter,
                peerCatalog, peerSchema, peerTable, peerKeys, peerColumns, peerFilter, joinType,
                memoryLimit, numPartitions, gateway, innerProbe, peerProbe, workers,
                projections, window, joinAlgorithm, steps, aliases, outerKeys, outerAggregations);
    }

    /** 最末层 join 上的窗口分组链（空 = 无）。 */
    public List<FragmentSpec.WindowStep> windowChain() {
        return windowChain;
    }

    /** 派生表聚合规格（join 之前的内层聚合；审计用）。 */
    public List<AggregationSpec> aggregations() {
        return aggregations;
    }

    /** 两级聚合派生体的外层聚合规格（空 = 单级）。 */
    public List<AggregationSpec> outerAggregations() {
        return outerAggregations;
    }

    /** 两级聚合派生体的外层分组键（空 = 单级）。 */
    public List<String> outerKeys() {
        return outerKeys;
    }

    @Override
    public List<FragmentSpec.WindowStep> windowSteps() { return windowChain; }

    @Override
    public void replaceWindowSteps(List<FragmentSpec.WindowStep> steps) {
        if (steps == null || steps.isEmpty()) return;
        this.windowChain = List.copyOf(steps);
    }

    /** 最终 join 的 Worker 侧算法（只读访问器，供探针用）。 */
    public FragmentSpec.JoinAlgorithm joinAlgorithm() {
        return joinAlgorithm;
    }

    /** 哈希分区数（只读访问器，供窗口链下沉规划用）。 */
    public int numPartitions() {
        return numPartitions;
    }

    /**
     * 终端 join 输出的哈希分区键：两流（派生 / peer）都按 join 键分区，且 ON 条件相等 ⇒
     * 任一侧键都是完整分区键。外层分组聚合键覆盖它时可下沉为「每分区终态聚合」。
     */
    @Override
    public List<String> terminalPartitionKeys() {
        return derivedJoinKeys;
    }

    /**
     * EXPLAIN 明细：peer（裸表）扫描请求携带的谓词 —— 这是连接器做分区裁剪的唯一入口
     * （Hive 按 {@code dt='...'} 只读命中分区）；派生侧的内层扫描请求 / 分组键 / 选型同理。
     */
    @Override
    public List<String> explainLines() {
        List<String> out = new ArrayList<>();
        out.add("peer: " + peerCatalog + "." + peerSchema + "." + peerTable
                + " keys=" + peerKeys
                + (peerFilter != null && !peerFilter.isBlank() ? " filter=[" + peerFilter + "]" : ""));
        ReadRequest req = innerRequest;
        out.add("derived: " + req.catalog() + "." + req.schema() + "." + req.table()
                + (innerKeys.isEmpty() ? "" : " groupBy=" + innerKeys)
                + " keys=" + derivedJoinKeys
                + (req.filterExpression().filter(f -> !f.isBlank())
                        .map(f -> " filter=[" + f + "]").orElse(""))
                + " algo=" + JoinExplain.distributed(joinAlgorithm, memoryLimit, numPartitions));
        for (int g = 0; g < windowChain.size(); g++) {
            FragmentSpec.WindowStep w = windowChain.get(g);
            out.add("window-" + (g + 1) + ": partitionBy=" + w.partitionBy()
                    + " orderBy=" + w.orderBy()
                    + (w.filter() != null ? " preFilter=[" + w.filter().describe() + "]" : "")
                    + (g == 0 ? " (join 输出重分区)" : " (上一组窗口输出重分区)"));
        }
        return out;
    }

    @Override
    protected int dispatchStage() throws Exception {
        StageScheduler.JoinChainStage stage = StageScheduler.planDerivedJoin(
                queryId, workers, numPartitions,
                derivedOnLeft,
                innerRequest, innerKeys, aggregations, caseProjections, innerProbe,
                derivedJoinKeys, havingFilter,
                peerCatalog, peerSchema, peerTable, peerKeys, peerColumns, peerProbe, peerFilter,
                joinType, memoryLimit, projections, window, joinAlgorithm, windowChain, outputAliases,
                outerKeys, outerAggregations);
        // 片段已在 planDerivedJoin 内按反死锁顺序排好：
        // 最终 join Consumer → MiddleAggregateConsumer → Producer（build 侧先、probe 侧后）
        for (FragmentSpec fragment : withTerminal(stage.fragments())) {   // 分布式写 SinkStep + top-N 截断尾

            dispatch(fragment);
        }
        System.err.printf("[DistributedDerivedJoin] query=%s workers=%d partitions=%d consumers=%d fragments=%d%n",
                queryId, workers.size(), numPartitions, stage.finalConsumerCount(),
                stage.fragments().size());
        return stage.finalConsumerCount();
    }

    @Override
    protected void closeProbes() {
        try { innerProbe.closeRead(); } catch (Throwable ignored) { }
        try { peerProbe.closeRead(); } catch (Throwable ignored) { }
    }

    /**
     * 派生基表的扫描请求（已列裁剪 + 派生内层 WHERE）。
     *
     * <p>只读访问器：供规划期探针断言「谓词是否带上、谓词列是否进了投影」。运行时不经此路径。</p>
     */
    public ReadRequest innerRequest() {
        return innerRequest;
    }

    /** 派生表聚合分组键（= 派生基表分区键）。只读访问器，供探针用。 */
    public List<String> innerKeys() {
        return innerKeys;
    }

    /** 投影型派生源的逐行投影派生列（空 = 聚合型派生源）。只读访问器，供探针用。 */
    public List<StageScheduler.DerivedProjection> projections() {
        return projections == null ? List.of() : projections;
    }

    /** 窗口型派生源的单窗口组（null = 非窗口型派生源）。只读访问器，供探针用。 */
    public StageScheduler.DerivedWindow window() {
        return window;
    }

    @Override
    public List<ScanColumn> scanColumnSets() {
        // 派生体 / peer 两侧都是基表扫描。派生体列随 ReadRequest 走（空 = 连接器语义读全表 ⇒ optional）；
        // peer 是显式列集字段（Planner 漏传即违规 ⇒ required）。
        return List.of(
                ScanColumn.optional(innerRequest.catalog() + "." + innerRequest.schema() + "."
                        + innerRequest.table(), innerRequest.columns().orElse(null)),
                ScanColumn.required(peerCatalog + "." + peerSchema + "." + peerTable,
                        peerColumns.isEmpty() ? null : peerColumns));
    }

    @Override
    public List<Field> outputFields() {
        // 复刻最终 join Consumer 输出页 schema（派生 / peer 两流各自拼 wire，再按 derivedOnLeft 定向）：
        //   派生流  = MiddleAggregateConsumer 输出 = 分组键（派生基表列）+ 聚合结果列（AggregationSpec）；
        //   peer 流 = 裁剪后 peer 扫描（peerColumns 非空按查询引用裁剪，空 = 全表）；
        //   最终 join = ensureOutputSchema：左全列 + 右全列剔除「右 join key 且左已有同名」。
        List<Field> derived = new ArrayList<>();
        if (window != null) {
            // 窗口型：基表扫描列（+ 分区内本地窗口结果列）—— 与 StageScheduler 的声明列同源
            List<Field> wf = StageScheduler.windowSideFields(innerProbe, innerRequest, window);
            if (wf != null) {
                derived.addAll(wf);
            }
        } else if (projections != null) {
            // 投影型：基表扫描列（+ 逐行投影派生列）—— 与 StageScheduler 的声明列同源
            derived.addAll(scanFields(innerProbe, innerRequest.catalog(), innerRequest.schema(),
                    innerRequest.table(), innerRequest.columns().orElse(null)));
            for (StageScheduler.DerivedProjection p : projections) {
                derived.add(p.field());
            }
        } else {
            derived.addAll(scanFields(innerProbe,
                    innerRequest.catalog(), innerRequest.schema(), innerRequest.table(), innerKeys));
            // CASE/IF/表达式派生列（聚合参数改写）不在扫描列里 —— 补上静态类型，
            // 否则 sum(CAST(x AS DECIMAL)) 的输出声明会退化。
            List<Field> innerFields = withDerivedFields(scanFields(innerProbe, innerRequest.catalog(),
                    innerRequest.schema(), innerRequest.table(), innerRequest.columns().orElse(null)),
                    caseProjections);
            for (AggregationSpec a : aggregations) {
                derived.add(aggregationOutputField(a, innerFields));
            }
            // 两级聚合（max(sum(x))）：外层聚合的输入是上一级聚合的输出列 —— 按内层输出列反推类型。
            if (!outerAggregations.isEmpty()) {
                List<Field> innerOut = new ArrayList<>(derived);
                List<Field> outerOut = new ArrayList<>();
                for (String k : outerKeys) {
                    Field f = fieldOf(innerOut, k);
                    if (f == null) {
                        throw new IllegalStateException(
                                "两级聚合派生体：外层分组键 " + k + " 不在内层输出列 " + innerOut);
                    }
                    outerOut.add(f);
                }
                for (AggregationSpec a : outerAggregations) {
                    outerOut.add(aggregationOutputField(a, innerOut));
                }
                derived = outerOut;
            }
        }
        List<Field> peer = scanFields(peerProbe, peerCatalog, peerSchema, peerTable,
                peerColumns.isEmpty() ? null : peerColumns);
        List<Field> wire = derivedOnLeft
                ? joinOutputFields(joinType, derived, peer, derivedJoinKeys, peerKeys)
                : joinOutputFields(joinType, peer, derived, peerKeys, derivedJoinKeys);
        if (windowChain.isEmpty()) {
            return wire;
        }
        // 窗口链：逐组按声明输出列重定基并追加窗口函数列（K 形状投影吸收见基类 helper）。
        return windowChainOutputFields(wire, windowChain);
    }
}
