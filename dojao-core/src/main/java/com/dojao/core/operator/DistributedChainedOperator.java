package com.dojao.core.operator;

import com.dojao.core.runtime.master.StageScheduler;
import com.dojao.core.runtime.master.WorkerRegistration;
import com.dojao.core.runtime.worker.WorkerGateway;
import com.dojao.spi.service.DojaoConnector;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.List;

/**
 * 分布式链式算子（Master 侧）—— 一个 <b>join → 同 key 后续计算</b> 的整条链路
 * （两侧扫描 → 按 join key 哈希分区 → 归属 Worker 本地 join → 本地窗口 / 聚合 → 回传）派发到集群，
 * Master 只做派发 + 收集最终结果（<b>cowork without master</b>，Phase 2 零二次交换）。
 *
 * <p>与 {@link DistributedJoinOperator} 的区别：消费者不是"join 输出直接回传"，而是把 join 输出
 * <b>留在本 Worker</b>，紧接着执行同 key 的窗口 / 聚合（{@link FragmentSpec.ChainedJoinConsumer}）。
 * 前置条件（Planner 校验）：后续计算 key 与 join key 完全相同 → join 输出已在归属 Worker 上按 key
 * 分布 → 后续计算零二次交换。中间数据全程留在 Worker，只有最终结果回传 Master。</p>
 *
 * <p>生命周期与 {@link AbstractDistributedOperator} 一致（doOpen 切分派发 / doNext 收集 /
 * doClose 反向取消），只是 {@link #dispatchStage()} 用 {@link StageScheduler#planChainedJoin} 规划
 * 链式消费者。</p>
 *
 * @author jiaxin
 * @version 3.0
 * @date 2026/8/21
 */
public class DistributedChainedOperator extends AbstractDistributedOperator implements Explainable {

    private final String leftCatalog, leftSchema, leftTable;
    private final String rightCatalog, rightSchema, rightTable;
    private final List<String> leftKeys;
    private final List<String> rightKeys;
    private final JoinType joinType;
    private final long memoryLimit;
    /** 探针连接器：Master 侧本地做分区规划（不参与数据拉取）。 */
    private final DojaoConnector leftProbe;
    private final DojaoConnector rightProbe;
    /** 链式后续计算（窗口 / 聚合，sealed）；窗口提前过滤下推时由 Planner 重建。 */
    private FragmentSpec.FollowUp followUp;
    /** 单表 WHERE 下推谓词（已去限定符、已剔除数据源不认识的函数谓词；可 null）。 */
    private final String leftFilter;
    private final String rightFilter;
    /** 两侧基表实际扫描的列集（顺序即页内列序，已含 join 键）；null/空 = 读全列。 */
    private final List<String> leftColumns;
    private final List<String> rightColumns;

    public DistributedChainedOperator(String queryId,
                                      String leftCatalog, String leftSchema, String leftTable, List<String> leftKeys,
                                      String rightCatalog, String rightSchema, String rightTable, List<String> rightKeys,
                                      JoinType joinType, long memoryLimit, int numPartitions,
                                      WorkerGateway gateway, DojaoConnector leftProbe, DojaoConnector rightProbe,
                                      List<WorkerRegistration> workers, FragmentSpec.FollowUp followUp,
                                      String leftFilter, String rightFilter) {
        this(queryId, leftCatalog, leftSchema, leftTable, leftKeys,
                rightCatalog, rightSchema, rightTable, rightKeys, joinType, memoryLimit, numPartitions,
                gateway, leftProbe, rightProbe, workers, followUp, leftFilter, rightFilter, null, null);
    }

    /** 列裁剪重载：{@code leftColumns} / {@code rightColumns} 空 = 读全列。 */
    public DistributedChainedOperator(String queryId,
                                      String leftCatalog, String leftSchema, String leftTable, List<String> leftKeys,
                                      String rightCatalog, String rightSchema, String rightTable, List<String> rightKeys,
                                      JoinType joinType, long memoryLimit, int numPartitions,
                                      WorkerGateway gateway, DojaoConnector leftProbe, DojaoConnector rightProbe,
                                      List<WorkerRegistration> workers, FragmentSpec.FollowUp followUp,
                                      String leftFilter, String rightFilter,
                                      List<String> leftColumns, List<String> rightColumns) {
        super("DistributedChained", queryId, numPartitions, gateway, workers);
        this.leftCatalog = leftCatalog;
        this.leftSchema = leftSchema;
        this.leftTable = leftTable;
        this.rightCatalog = rightCatalog;
        this.rightSchema = rightSchema;
        this.rightTable = rightTable;
        this.leftKeys = List.copyOf(leftKeys);
        this.rightKeys = List.copyOf(rightKeys);
        this.joinType = joinType;
        this.memoryLimit = memoryLimit;
        this.leftProbe = leftProbe;
        this.rightProbe = rightProbe;
        this.followUp = followUp;
        this.leftFilter = leftFilter;
        this.rightFilter = rightFilter;
        this.leftColumns = leftColumns;
        this.rightColumns = rightColumns;
    }

    /** EXPLAIN 明细：两侧扫描 + join 算法 / 连接键 + 同 key 后续计算（聚合 / 窗口）。 */
    @Override
    public List<String> explainLines() {
        List<String> out = new ArrayList<>();
        out.add("probe: " + leftCatalog + "." + leftSchema + "." + leftTable + " keys=" + leftKeys
                + (leftFilter != null && !leftFilter.isBlank() ? " filter=[" + leftFilter + "]" : ""));
        out.add("build: " + rightCatalog + "." + rightSchema + "." + rightTable + " keys=" + rightKeys
                + " join=" + joinType + " (chained, no shuffle between join and follow-up)"
                + (rightFilter != null && !rightFilter.isBlank() ? " filter=[" + rightFilter + "]" : ""));
        if (followUp instanceof FragmentSpec.AggregateStep agg) {
            out.add("follow-up: aggregate groupBy=" + agg.keyColumns()
                    + " aggs=" + agg.aggregations().stream()
                            .map(a -> a.aggregateName() + " AS " + a.outputColumn()).toList());
        } else if (followUp instanceof FragmentSpec.WindowStep win) {
            out.add("follow-up: window partitionBy=" + win.partitionBy()
                    + " orderBy=" + win.orderBy()
                    + " functions=" + win.functions().stream()
                            .map(f -> f.type() + " AS " + f.outputColumn()).toList());
        }
        out.add("algorithm: " + JoinExplain.distributed(FragmentSpec.JoinAlgorithm.HASH, memoryLimit, numPartitions)
                + "（本地每分区哈希 join，join 与后续计算之间零二次交换）");
        return out;
    }

    @Override
    protected int dispatchStage() throws Exception {
        StageScheduler.DistributedStage stage = StageScheduler.planChainedJoin(
                queryId, workers, numPartitions,
                leftCatalog, leftSchema, leftTable, leftKeys,
                rightCatalog, rightSchema, rightTable, rightKeys,
                joinType, memoryLimit, leftProbe, rightProbe, followUp, leftFilter, rightFilter,
                leftColumns, rightColumns);

        // 先 Consumer（同步注册接收端）后 Producer（发送端）—— 发送方 push 前所有接收端就绪
        List<FragmentSpec> consumers = withTerminal(stage.consumers());   // 分布式写 SinkStep + top-N 截断尾

        for (FragmentSpec consumer : consumers) {
            dispatch(consumer);
        }
        for (FragmentSpec producer : stage.producers()) {
            dispatch(producer);
        }
        return consumers.size();
    }

    @Override
    protected void closeProbes() {
        try { leftProbe.closeRead(); } catch (Throwable ignored) { }
        try { rightProbe.closeRead(); } catch (Throwable ignored) { }
    }

    /**
     * 规划期下推窗口提前过滤（外层 {@code WHERE rn <= N}）：重建带上过滤配置的
     * {@link FragmentSpec.WindowStep}，随 {@code WindowStepMsg.filter} 下发 Worker，
     * 在 Phase 1 末尾就地裁剪（避免整组 join+窗口结果回传 Master 再过滤）。
     */
    public void setWindowFilter(WindowFilterConfig filter) {
        if (followUp instanceof FragmentSpec.WindowStep w) this.followUp = w.withFilter(filter);
    }

    /** 链式窗口函数规格（Planner 判断「该窗口函数是否支持下推」用）；非窗口后续返回空表。 */
    public List<WindowFunctionSpec> windowFunctions() {
        return followUp instanceof FragmentSpec.WindowStep w ? w.functions() : List.of();
    }

    /** 链式窗口 ORDER BY 列（空 = 全量窗口）；非窗口后续返回空表。 */
    public List<String> windowOrderBy() {
        return followUp instanceof FragmentSpec.WindowStep w ? w.orderBy() : List.of();
    }

    @Override
    public List<ScanColumn> scanColumnSets() {
        // 链式 join→后续：两侧都是基表扫描，列集必须随路径带下来（漏传 = 读全表，历史 bug）。
        return List.of(
                ScanColumn.required(leftCatalog + "." + leftSchema + "." + leftTable, leftColumns),
                ScanColumn.required(rightCatalog + "." + rightSchema + "." + rightTable, rightColumns));
    }

    @Override
    public List<Field> outputFields() {
        // 复刻 ChainedJoinConsumer 输出页 schema：单 join 输出（左/右扫描列，剔除「右 join key 且左已有
        // 同名」列）之上再执行同 key 后续计算 —— 窗口 = join 全列 + 函数列；聚合 = 分组键 + 聚合结果列。
        // 列集必须与实际扫描一致（空 = 全列），否则空桶退回声明值时列数会与其他分区不同。
        List<Field> wire = joinOutputFields(joinType,
                scanFields(leftProbe, leftCatalog, leftSchema, leftTable, leftColumns),
                scanFields(rightProbe, rightCatalog, rightSchema, rightTable, rightColumns),
                leftKeys, rightKeys);
        return switch (followUp) {
            case FragmentSpec.WindowStep w -> {
                List<Field> out = new ArrayList<>(wire);
                for (WindowFunctionSpec spec : w.functions()) {
                    out.add(windowFunctionField(spec, name -> fieldOf(wire, name)));
                }
                yield out;
            }
            case FragmentSpec.AggregateStep a -> {
                List<Field> out = new ArrayList<>(fieldsByNames(wire, a.keyColumns()));
                for (FragmentSpec.AggregationDto dto : a.aggregations()) {
                    out.add(dtoAggregationField(dto.outputColumn(), dto.aggregateName(),
                            wire, dto.inputColumn()));
                }
                yield out;
            }
        };
    }
}
