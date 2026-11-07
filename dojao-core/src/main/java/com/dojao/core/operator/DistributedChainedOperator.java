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
public class DistributedChainedOperator extends AbstractDistributedOperator
        implements Explainable, WindowChainCarrier {

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
    /**
     * 二级聚合（外层全局聚合下沉）：链式聚合输出后同 Worker 零交换再聚一次，每分区只回传
     * 1 行 partial，Master 侧 {@code PartialAggregationMergeOperator} 合并。null = 无。
     */
    private FragmentSpec.AggregateStep partialFollowUp;
    /**
     * 多窗口分组链（非 null 时取代 {@link #followUp}）：join 输出依次经过每组窗口，
     * 组间由 Worker 按下一组 PARTITION BY 重分区（不回传 Master）。单组链保持 null，
     * 走既有 {@link #followUp} 路径（行为不变）。
     */
    private List<FragmentSpec.WindowStep> windowChain;
    /** 单表 WHERE 下推谓词（已去限定符、已剔除数据源不认识的函数谓词；可 null）。 */
    private final String leftFilter;
    private final String rightFilter;
    /** 两侧基表实际扫描的列集（顺序即页内列序，已含 join 键）；null/空 = 读全列。 */
    private final List<String> leftColumns;
    private final List<String> rightColumns;
    /** 外连接同名列消歧别名（空 = 无）：join 输出先补 __jk_* 副本再进入窗口链。 */
    private final List<FragmentSpec.JoinAlias> outputAliases;

    public DistributedChainedOperator(String queryId,
                                      String leftCatalog, String leftSchema, String leftTable, List<String> leftKeys,
                                      String rightCatalog, String rightSchema, String rightTable, List<String> rightKeys,
                                      JoinType joinType, long memoryLimit, int numPartitions,
                                      WorkerGateway gateway, DojaoConnector leftProbe, DojaoConnector rightProbe,
                                      List<WorkerRegistration> workers, FragmentSpec.FollowUp followUp,
                                      String leftFilter, String rightFilter) {
        this(queryId, leftCatalog, leftSchema, leftTable, leftKeys,
                rightCatalog, rightSchema, rightTable, rightKeys, joinType, memoryLimit, numPartitions,
                gateway, leftProbe, rightProbe, workers, followUp, null, leftFilter, rightFilter, null, null);
    }

    /** 多窗口分组链重载：join 输出依次经过 {@code windowChain} 各组窗口（组间 Worker 内重分区）。 */
    public DistributedChainedOperator(String queryId,
                                      String leftCatalog, String leftSchema, String leftTable, List<String> leftKeys,
                                      String rightCatalog, String rightSchema, String rightTable, List<String> rightKeys,
                                      JoinType joinType, long memoryLimit, int numPartitions,
                                      WorkerGateway gateway, DojaoConnector leftProbe, DojaoConnector rightProbe,
                                      List<WorkerRegistration> workers, List<FragmentSpec.WindowStep> windowChain,
                                      String leftFilter, String rightFilter,
                                      List<String> leftColumns, List<String> rightColumns) {
        this(queryId, leftCatalog, leftSchema, leftTable, leftKeys,
                rightCatalog, rightSchema, rightTable, rightKeys, joinType, memoryLimit, numPartitions,
                gateway, leftProbe, rightProbe, workers, null, windowChain,
                leftFilter, rightFilter, leftColumns, rightColumns);
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
        this(queryId, leftCatalog, leftSchema, leftTable, leftKeys,
                rightCatalog, rightSchema, rightTable, rightKeys, joinType, memoryLimit, numPartitions,
                gateway, leftProbe, rightProbe, workers, followUp, null,
                leftFilter, rightFilter, leftColumns, rightColumns);
    }

    /** 列裁剪 + 多窗口链重载：{@code leftColumns} / {@code rightColumns} 空 = 读全列。 */
    public DistributedChainedOperator(String queryId,
                                      String leftCatalog, String leftSchema, String leftTable, List<String> leftKeys,
                                      String rightCatalog, String rightSchema, String rightTable, List<String> rightKeys,
                                      JoinType joinType, long memoryLimit, int numPartitions,
                                      WorkerGateway gateway, DojaoConnector leftProbe, DojaoConnector rightProbe,
                                      List<WorkerRegistration> workers, FragmentSpec.FollowUp followUp,
                                      List<FragmentSpec.WindowStep> windowChain,
                                      String leftFilter, String rightFilter,
                                      List<String> leftColumns, List<String> rightColumns) {
        this(queryId, leftCatalog, leftSchema, leftTable, leftKeys,
                rightCatalog, rightSchema, rightTable, rightKeys, joinType, memoryLimit, numPartitions,
                gateway, leftProbe, rightProbe, workers, followUp, windowChain,
                leftFilter, rightFilter, leftColumns, rightColumns, List.of());
    }

    /**
     * 同上 + <b>外连接同名列消歧</b>：{@code outputAliases} 由 Worker 在本地 join 之后、窗口重分区
     * 之前套用（窗口表达式引用 {@code __jk_*} 时需要；等价 Master 单机 {@code ColumnAliasOperator}）。
     */
    public DistributedChainedOperator(String queryId,
                                      String leftCatalog, String leftSchema, String leftTable, List<String> leftKeys,
                                      String rightCatalog, String rightSchema, String rightTable, List<String> rightKeys,
                                      JoinType joinType, long memoryLimit, int numPartitions,
                                      WorkerGateway gateway, DojaoConnector leftProbe, DojaoConnector rightProbe,
                                      List<WorkerRegistration> workers, FragmentSpec.FollowUp followUp,
                                      List<FragmentSpec.WindowStep> windowChain,
                                      String leftFilter, String rightFilter,
                                      List<String> leftColumns, List<String> rightColumns,
                                      List<FragmentSpec.JoinAlias> outputAliases) {
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
        this.windowChain = windowChain == null ? null : List.copyOf(windowChain);
        this.leftFilter = leftFilter;
        this.rightFilter = rightFilter;
        this.leftColumns = leftColumns;
        this.rightColumns = rightColumns;
        this.outputAliases = outputAliases == null ? List.of() : List.copyOf(outputAliases);
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
        if (windowChain != null) {
            for (int i = 0; i < windowChain.size(); i++) {
                FragmentSpec.WindowStep win = windowChain.get(i);
                boolean sameKey = i == 0 && (win.partitionBy().equals(leftKeys)
                        || win.partitionBy().equals(rightKeys));
                out.add("window#" + i + ": partitionBy=" + win.partitionBy()
                        + " orderBy=" + win.orderBy()
                        + " functions=" + win.functions().stream()
                                .map(f -> f.type() + " AS " + f.outputColumn()).toList()
                        + (win.filter() != null ? " preFilter=[" + win.filter().describe() + "]" : "")
                        + (sameKey ? " (same key as join, zero exchange)"
                                   : " (repartition from previous stage)"));
            }
            out.add("window-chain: " + windowChain.size() + " groups stay on workers"
                    + " (no return to master between groups)");
        } else if (followUp instanceof FragmentSpec.AggregateStep agg) {
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
        if (windowChain != null) {
            // 多窗口分组链：join → 窗口#0 → Partition → 窗口#1 → … → 末组回传/sink。
            // planJoinWindowChain 已按「终端窗口 → 中间窗口逆序 → join → 生产者」排好序，
            // 顺序派发即可保证发送方 push 前所有接收端就绪。
            StageScheduler.JoinChainStage stage = StageScheduler.planJoinWindowChain(
                    queryId, workers, numPartitions,
                    leftCatalog, leftSchema, leftTable, leftKeys,
                    rightCatalog, rightSchema, rightTable, rightKeys,
                    joinType, memoryLimit, leftProbe, rightProbe, windowChain,
                    leftFilter, rightFilter, leftColumns, rightColumns, outputAliases);

            List<FragmentSpec> fragments = withTerminal(stage.fragments());   // 仅终端组挂 sink / top-N
            for (FragmentSpec fragment : fragments) {
                dispatch(fragment);
            }
            return stage.finalConsumerCount();
        }

        StageScheduler.DistributedStage stage = StageScheduler.planChainedJoin(
                queryId, workers, numPartitions,
                leftCatalog, leftSchema, leftTable, leftKeys,
                rightCatalog, rightSchema, rightTable, rightKeys,
                joinType, memoryLimit, leftProbe, rightProbe, followUp, leftFilter, rightFilter,
                leftColumns, rightColumns, partialFollowUp);

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
        try { leftProbe.closeRead(); } catch (Throwable ignored) { }
        try { rightProbe.closeRead(); } catch (Throwable ignored) { }
    }

    /**
     * 窗口分组链（{@link WindowChainCarrier}）：多组链 → {@code windowChain}；
     * 单组 followUp → 长度 1 的步骤表。统一后 Planner 的下推不再区分单组 / 多组。
     */
    @Override
    public List<FragmentSpec.WindowStep> windowSteps() {
        if (windowChain != null) return windowChain;
        return followUp instanceof FragmentSpec.WindowStep w ? List.of(w) : List.of();
    }

    /** 替换窗口链：多组链整表替换；单组 followUp 只取末组步骤（下推只改末组）。 */
    @Override
    public void replaceWindowSteps(List<FragmentSpec.WindowStep> steps) {
        if (steps == null || steps.isEmpty()) return;
        if (windowChain != null) {
            this.windowChain = List.copyOf(steps);
            return;
        }
        if (followUp instanceof FragmentSpec.WindowStep) {
            this.followUp = steps.get(steps.size() - 1);
        }
    }

    /** 是否多窗口分组链（诊断 / 测试用）。 */
    public boolean hasWindowChain() { return windowChain != null; }

    /** 单段 followUp（聚合或窗口；null = 无；审计用）。 */
    public FragmentSpec.FollowUp followUp() { return followUp; }

    /** 桶数（链式交换分区数）。 */
    public int numPartitions() { return numPartitions; }

    /** 设置二级聚合（Planner 在 doOpen 前调用；null = 无）。 */
    public void setPartialFollowUp(FragmentSpec.AggregateStep f) { this.partialFollowUp = f; }

    /** 已设置的二级聚合（可空；审计 / EXPLAIN 只读）。 */
    public FragmentSpec.AggregateStep partialFollowUp() { return partialFollowUp; }

    /** 多窗口分组链（null = 单组 followUp 路径）。 */
    public List<FragmentSpec.WindowStep> windowChain() { return windowChain; }

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
        if (windowChain != null) {
            // 每组窗口在其输入页后追加本组函数列（输入 = 上一组输出）。LAG/LEAD 的镜像列
            // 必须按「当前累计列」解析（引用的是 join 输出列，不会引用窗口函数结果）。
            return windowChainOutputFields(wire, windowChain);
        }
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
