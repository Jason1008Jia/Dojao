package com.dojao.core.operator;

import com.dojao.common.data.Page;
import com.dojao.core.runtime.master.StageScheduler;
import com.dojao.core.runtime.master.WorkerRegistration;
import com.dojao.core.runtime.worker.WorkerGateway;
import com.dojao.spi.bean.ReadRequest;
import com.dojao.spi.service.DojaoConnector;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.List;

/**
 * 分布式窗口算子（Master 侧）—— 一个窗口函数分组的整条链路（扫描 → 按 PARTITION BY 哈希分区 →
 * Worker 本地排序 + 逐行计算）派发到集群执行，Master 只做派发 + 收集最终结果
 * （<b>cowork without master</b>：中间数据全程留在 Worker，只有窗口输出页回传 Master）。
 *
 * <p>执行形态：
 * <pre>
 *   doOpen → dispatchStage：
 *     StageScheduler.planWindow()       切分：N 个 Producer + 每 Worker 一个 WindowConsumer
 *     dispatch Consumer（先）           同步注册各 Worker 的 ShuffleReader
 *     dispatch Producer（后）           Worker 扫切片 → 按 PARTITION BY 哈希分区 → 推给归属 Worker
 *     Consumer：own 桶 → PartitionedSourceOperator → 本地 WindowSort + WindowFunction → 回传
 *   doNext：从结果队列拉窗口输出页；全部 Consumer 完成 → EOF；任一失败 → fail-fast
 *   doClose：反向取消所有片段任务，排空队列，释放探针连接器
 * </pre>
 *
 * <p><b>正确性</b>：owner 映射与同查询其他 shuffle 完全一致（co-partitioning）。同一逻辑分区
 * （相同 PARTITION BY 键值）按哈希落唯一桶 → 落在唯一 Worker，Worker 内排序 + 逐行计算即为该
 * 分区完整结果，各 Worker 输出的并集 = 全局正确结果。桶内可能含多个不同 key（哈希碰撞），
 * {@link WindowFunctionOperator} 按相邻行 PARTITION BY 值检测分区边界，天然正确。</p>
 *
 * @author jiaxin
 * @version 3.0
 * @date 2026/8/20
 */
public class DistributedWindowOperator extends AbstractDistributedOperator implements Explainable {

    /** 探针连接器：Master 侧本地做分区规划（不参与数据拉取）。 */
    private final ReadRequest baseRequest;
    private final List<String> predicates;
    /** 窗口算子上游所需的列（= pre-window 输出列 + 引擎 Filter 专用列）；Worker 扫描就取这些列。 */
    private final List<String> projection;
    /**
     * 本算子<b>对外声明的</b>窗口前输出列（= 上游输出列，不含「只为过滤取的」引擎过滤列）。
     * 默认 = {@link #projection}；Planner 在不兑现下推的连接器上把引擎过滤列并进扫描投影后，
     * 显式设置为不带这些列的上游输出列（见 {@link #setVisibleColumns}）。回传页按它裁剪，
     * 保证输出页 == 声明列（否则 {@code dt} 这类列会漏进结果 / CTAS 建表）。
     */
    private List<String> visibleColumns;
    private final List<String> partitionBy;
    private final List<String> orderBy;
    private final List<Boolean> ascending;
    private final List<WindowFunctionSpec> functions;
    private final long memoryLimit;
    private final DojaoConnector probe;
    /** 提前过滤下推（外层 WHERE 窗口函数列 op 值）；null = 无。规划期由 Planner 设置。 */
    private WindowFilterConfig filter;

    /** 规划期下推：把外层窗口过滤条件下推到 Worker 侧 WindowSortOperator（见 {@link WindowFilterConfig}）。 */
    public void setFilterConfig(WindowFilterConfig filter) { this.filter = filter; }

    /**
     * 设置本算子对外的窗口前输出列（须在 {@code doOpen()} 前调用）。
     *
     * <p>{@code projection}（Worker 扫描列）可能多带「只作过滤用」的引擎过滤列；本算子的输出页
     * 只该含 {@code visibleColumns}（= 上游真实输出列）。默认等于 {@code projection} → 无额外列时零开销。</p>
     */
    public void setVisibleColumns(List<String> columns) {
        this.visibleColumns = columns == null ? null : List.copyOf(columns);
    }

    /** 本算子对外输出页的全部列名（窗口前输出列 + 窗口函数结果列）。 */
    private List<String> emittedColumns() {
        List<String> base = visibleColumns != null ? visibleColumns : projection;
        List<String> out = new ArrayList<>(base.size() + functions.size());
        out.addAll(base);
        for (WindowFunctionSpec spec : functions) out.add(spec.outputColumn());
        return out;
    }

    @Override
    protected Page finalizeResultPage(Page page) {
        if (page == null || page.isEOF() || page.getRoot() == null) return page;
        List<String> want = emittedColumns();
        // 已同宽同名 → 直接走基类（归一分区号，见 AbstractDistributedOperator#finalizeResultPage）
        List<String> have = new ArrayList<>();
        for (Field f : page.getRoot().getSchema().getFields()) have.add(f.getName());
        if (have.equals(want)) return super.finalizeResultPage(page);
        Page trimmed = page.select(want);
        page.close();   // select 零拷贝转移 buffer 所有权，源页须关闭
        return super.finalizeResultPage(trimmed);
    }

    /** 窗口函数规格（Planner 判断「该窗口函数是否支持下推」用）。 */
    public List<WindowFunctionSpec> functions() { return functions; }

    /** ORDER BY 列（空 = 全量窗口 / 分区级聚合值 —— 下推语义分支用）。 */
    public List<String> orderBy() { return orderBy; }

    /** 窗口上游列（Worker 扫描列集）—— 聚合型下推解析输入列索引用。 */
    public List<String> projection() { return projection; }

    /** 已下推的窗口提前过滤配置（null = 未下推）—— 诊断 / 探针用。 */
    public WindowFilterConfig filter() { return filter; }

    /** EXPLAIN 明细：扫描表 / 窗口分组与排序键 / 窗口函数 / 下推谓词。 */
    @Override
    public List<String> explainLines() {
        List<String> out = new ArrayList<>();
        out.add("scan " + baseRequest.catalog() + "." + baseRequest.schema() + "." + baseRequest.table()
                + (predicates.isEmpty() ? "" : " filter=[" + String.join(" AND ", predicates) + "]"));
        out.add("window partitionBy=" + (partitionBy.isEmpty() ? "<all>" : partitionBy)
                + " orderBy=" + (orderBy.isEmpty() ? "<none>" : orderBy)
                + " functions=" + functions.stream()
                        .map(f -> f.type() + (f.functionName() != null ? "(" + f.functionName() + ")" : "")
                                + " AS " + f.outputColumn()).toList()
                + (filter != null ? " earlyFilter=" + filter : ""));
        return out;
    }

    @Override
    public List<ScanColumn> scanColumnSets() {
        // 窗口上游列（Planner 始终设为非空；空 = 列裁剪被丢弃 ⇒ 违规）
        return List.of(ScanColumn.required(
                baseRequest.catalog() + "." + baseRequest.schema() + "." + baseRequest.table(),
                projection));
    }

    public DistributedWindowOperator(String queryId, ReadRequest baseRequest, List<String> predicates,
                                     List<String> projection,
                                     List<String> partitionBy, List<String> orderBy, List<Boolean> ascending,
                                     List<WindowFunctionSpec> functions,
                                     long memoryLimit, int numPartitions,
                                     WorkerGateway gateway, DojaoConnector probe,
                                     List<WorkerRegistration> workers) {
        super("DistributedWindow", queryId, numPartitions, gateway, workers);
        this.baseRequest = baseRequest;
        this.predicates = List.copyOf(predicates);
        this.projection = projection == null ? List.of() : List.copyOf(projection);
        this.partitionBy = List.copyOf(partitionBy);
        this.orderBy = List.copyOf(orderBy);
        this.ascending = List.copyOf(ascending);
        this.functions = List.copyOf(functions);
        this.memoryLimit = memoryLimit;
        this.probe = probe;
    }

    @Override
    protected int dispatchStage() throws Exception {
        // 复算扫描生效请求：先列裁剪（pruneColumns），再谓词下推（与原 scan.doOpen 顺序一致）
        ReadRequest effective = baseRequest;
        if (!projection.isEmpty()) {
            effective = probe.pruneColumns(effective, projection);
        }
        for (String p : predicates) {
            if (p == null || p.isBlank()) continue;
            effective = probe.pushdownPredicate(effective, p);
        }
        StageScheduler.DistributedStage stage = StageScheduler.planWindow(
                queryId, workers, numPartitions, effective,
                partitionBy, orderBy, ascending, functions, memoryLimit, filter, probe);

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
        try { probe.closeRead(); } catch (Throwable ignored) { }
    }

    @Override
    public List<Field> outputFields() {
        // CTAS 建表 schema = 窗口输出页 schema（WindowFunctionOperator.buildOutputSchema 复刻）：
        // 窗口前输出列在前，窗口函数结果列在后。
        // ★用 visibleColumns（对外声明列）而非 projection —— 后者可能多带「只作过滤用」的引擎
        //   过滤列（如 Hive 非下推时的分区列 dt），带上它会让 CTAS 建出多余列。
        List<String> base = visibleColumns != null ? visibleColumns : projection;
        List<Field> out = new ArrayList<>();
        List<Field> input = scanFields(probe, baseRequest.catalog(), baseRequest.schema(),
                baseRequest.table(), base);
        out.addAll(input);
        // LAG/LEAD 镜像输入列类型（按列名匹配），其余函数类型已内嵌于 windowFunctionField
        for (WindowFunctionSpec spec : functions) {
            out.add(windowFunctionField(spec, name -> {
                for (Field f : input) {
                    if (f.getName().equalsIgnoreCase(name)) return f;
                }
                return null;
            }));
        }
        return out;
    }
}
