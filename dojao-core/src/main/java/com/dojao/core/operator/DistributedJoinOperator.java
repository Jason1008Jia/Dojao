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
 * 分布式 join 算子（Master 侧）—— 两表 join 与左深多 join 链<b>统一实现</b>：
 * 单 join 就是 {@code links.size()==1} 的多 join（一表 join 是多表 join 的特例），
 * 把 {@code T0 ⋈J1 T1} 或 {@code (T0 ⋈J1 T1) ⋈J2 T2 ⋈J3 ...} 整条链路切成
 * Producer / Consumer 片段派发到集群 Worker，Master 只做派发 + 收集最终结果。
 *
 * <pre>
 *   doOpen（final，AbstractDistributedOperator）：
 *     dispatchStage()：
 *       StageScheduler.planJoinChain()   切分：最终 Consumer + 中间层 + 基表 Producer
 *       （单 join = 1 个 link，中间层循环不执行，等价旧单 join 的 plan() 切片）
 *       按反死锁顺序派发：最终 Consumer → 中间层（join 层逆序）→ 基表 Producers（build 先 probe）
 *       join#k（k&lt;N-1）的 join 输出经 HashPartition 按下一层左键重哈希 → 推给 exchange#(k+1)-L，
 *       <b>不回传 Master</b>；只有最末层 join 的 Consumer 回传 Master
 *   doNext（final）：从结果队列拉最末层 join 输出页；全部 Consumer 完成 → EOF；任一失败 → fail-fast
 *   doClose（final）：反向取消所有片段任务，排空队列，closeProbes() 释放全部探针连接器
 * </pre>
 *
 * <p><b>不变量</b>：所有交换共用同一个 round-robin owner 映射（co-partitioning）。同 key 时
 * 重哈希天然落回本机（零跨机移动）；跨 key 时是真实 re-shuffle —— 统一模型覆盖两种情形。
 * 中间层片段经 exchange 的 complete/onComplete 协议让下一层 {@link ShuffleReader} 收齐
 * {@code partitionEnd}，数据流全程留在 Worker 侧，Master 只派发 + 收集最末层结果。
 * 结果队列有界 → 回传即背压 → 消费慢时整条链路逐级阻塞。</p>
 *
 * <p>开关 {@code PlanningContext.distributedExecution}（默认关）控制是否启用；关 → 走单机
 * HashJoin，现有测试零影响。</p>
 *
 * @author jiaxin
 * @version 3.0
 * @date 2026/8/31
 */
public class DistributedJoinOperator extends AbstractDistributedOperator
        implements Explainable, WindowChainCarrier {

    private final String firstCatalog, firstSchema, firstTable;
    private final List<String> firstLeftKeys;
    private final DojaoConnector firstProbe;
    private final String firstFilter;
    /** 首表（T0，probe 侧）实际扫描的列集（顺序即页内列序，已含 join 键）；null/空 = 读全列。 */
    private final List<String> firstColumns;
    private final List<JoinLink> links;
    private final long memoryLimit;
    /** 最末层 join 的同 key 后续计算（聚合 / 窗口）；null → 普通 Consumer（现行为）。 */
    private final FragmentSpec.FollowUp followUp;
    /**
     * 各关系的<b>派生源</b>（子查询 / CTE，展平不了的那种）：下标 0 = 首表 T0，下标 k = 第 k 层 join 的
     * 右表；元素 null = 该层普通基表扫描。null 列表 = 全普通（旧行为，逐字节不变）。
     */
    private final List<StageScheduler.DerivedSource> sources;
    /**
     * 最末层 join 输出上的<b>窗口分组链</b>（多 join 链 / 含派生源时同样下沉）：空 = 无窗口链。
     * 与 {@link #followUp} 互斥（followUp 只承载同 key 聚合 / 单窗口后续计算）。
     */
    private List<FragmentSpec.WindowStep> windowChain;
    /** 外连接同名列消歧别名（空 = 无）：最末层 join 片段在 join 之后、窗口重分区之前套用。 */
    private final List<FragmentSpec.JoinAlias> outputAliases;
    /**
     * 二级聚合（外层全局聚合下沉）：最末层 join→聚合输出后同 Worker 零交换再聚一次，每分区只回传
     * 1 行 partial，Master 侧 {@code PartialAggregationMergeOperator} 合并。null = 无。
     */
    private FragmentSpec.AggregateStep partialFollowUp;

    /**
     * 单 join 便捷构造：等价于 {@code links=[JoinLink(..., rightKeys, ...)]} 的 1 元素链。
     * 首表（probe 侧）= 左表，唯一 link = 右表（build 侧）。
     */
    public DistributedJoinOperator(String queryId,
                                   String leftCatalog, String leftSchema, String leftTable, List<String> leftKeys,
                                   String rightCatalog, String rightSchema, String rightTable, List<String> rightKeys,
                                   JoinType joinType, long memoryLimit, int numPartitions,
                                   WorkerGateway gateway, DojaoConnector leftProbe, DojaoConnector rightProbe,
                                   List<WorkerRegistration> workers,
                                   String leftFilter, String rightFilter) {
        this(queryId, leftCatalog, leftSchema, leftTable, leftKeys,
                rightCatalog, rightSchema, rightTable, rightKeys, joinType, memoryLimit, numPartitions,
                gateway, leftProbe, rightProbe, workers, leftFilter, rightFilter,
                FragmentSpec.JoinAlgorithm.HASH, null, null);
    }

    /**
     * 带算法选型的单 join 构造 —— {@code algorithm} 是 Master 规划期
     * （{@code Planner.chooseJoinAlgorithm}）的结论，随 {@link FragmentSpec.JoinStep} 下发 Worker。
     *
     * <p>{@code BROADCAST} 时 {@code StageScheduler.planJoinChain} 会改走广播切片
     * （右表复制到每台、左表就地不 shuffle，见 {@code StageScheduler.planBroadcastJoin}）。</p>
     */
    public DistributedJoinOperator(String queryId,
                                   String leftCatalog, String leftSchema, String leftTable, List<String> leftKeys,
                                   String rightCatalog, String rightSchema, String rightTable, List<String> rightKeys,
                                   JoinType joinType, long memoryLimit, int numPartitions,
                                   WorkerGateway gateway, DojaoConnector leftProbe, DojaoConnector rightProbe,
                                   List<WorkerRegistration> workers,
                                   String leftFilter, String rightFilter,
                                   FragmentSpec.JoinAlgorithm algorithm) {
        this(queryId, leftCatalog, leftSchema, leftTable, leftKeys, leftProbe, leftFilter,
                List.of(new JoinLink(leftKeys, rightKeys, joinType,
                        rightCatalog, rightSchema, rightTable, rightProbe, rightFilter, algorithm)),
                memoryLimit, numPartitions, gateway, workers, null, null);
    }

    /**
     * 带算法 + <b>列裁剪</b>的单 join 构造：{@code leftColumns} / {@code rightColumns} 是两侧基表
     * 实际扫描的列集（顺序即页内列序，须含 join 键；null/空 = 读全列）。{@code leftColumns} 由
     * Master 规划期从语法层 {@code ColumnPruning} 已裁剪的 {@code ResolvedRelation.columns()} 传来，
     * 随 {@code ScanStep} 交给连接器投影下推（Hive 宽表只读被引用列）。
     */
    public DistributedJoinOperator(String queryId,
                                   String leftCatalog, String leftSchema, String leftTable, List<String> leftKeys,
                                   String rightCatalog, String rightSchema, String rightTable, List<String> rightKeys,
                                   JoinType joinType, long memoryLimit, int numPartitions,
                                   WorkerGateway gateway, DojaoConnector leftProbe, DojaoConnector rightProbe,
                                   List<WorkerRegistration> workers,
                                   String leftFilter, String rightFilter,
                                   FragmentSpec.JoinAlgorithm algorithm,
                                   List<String> leftColumns, List<String> rightColumns) {
        this(queryId, leftCatalog, leftSchema, leftTable, leftKeys, leftProbe, leftFilter,
                List.of(new JoinLink(leftKeys, rightKeys, joinType,
                        rightCatalog, rightSchema, rightTable, rightProbe, rightFilter, algorithm,
                        rightColumns)),
                memoryLimit, numPartitions, gateway, workers, null, leftColumns);
    }

    /**
     * 带算法 + 列裁剪 + <b>ON 单侧残差谓词</b>的单 join 构造。
     *
     * <p>残差谓词只引用一侧（如 {@code FULL JOIN ... ON l.k = r.k AND l.dt = '...'}），
     * 不能下推到扫描（保留侧的未匹配行仍须输出），由 Worker 内的 {@code HashJoinOperator}
     * 在匹配阶段求值。带残差的 join 只能走 HASH（{@link JoinLink} 构造期已强制降级）。</p>
     */
    public DistributedJoinOperator(String queryId,
                                   String leftCatalog, String leftSchema, String leftTable, List<String> leftKeys,
                                   String rightCatalog, String rightSchema, String rightTable, List<String> rightKeys,
                                   JoinType joinType, long memoryLimit, int numPartitions,
                                   WorkerGateway gateway, DojaoConnector leftProbe, DojaoConnector rightProbe,
                                   List<WorkerRegistration> workers,
                                   String leftFilter, String rightFilter,
                                   FragmentSpec.JoinAlgorithm algorithm,
                                   List<String> leftColumns, List<String> rightColumns,
                                   String onLeftFilter, String onRightFilter) {
        this(queryId, leftCatalog, leftSchema, leftTable, leftKeys, leftProbe, leftFilter,
                List.of(new JoinLink(leftKeys, rightKeys, joinType,
                        rightCatalog, rightSchema, rightTable, rightProbe, rightFilter, algorithm,
                        rightColumns, null, onLeftFilter, onRightFilter)),
                memoryLimit, numPartitions, gateway, workers, null, leftColumns);
    }

    /**
     * 完整构造（多 join 链 / 带 followUp）：{@code followUp} 非 null 时最末层 Consumer 链式执行
     * 聚合 / 窗口（worker 本地 join → 本地后续计算 → 回传，零二次交换，master 不吸原始 join 行）。
     *
     * @param followUp 同 key 后续计算；调用方须保证分组键包含最末层 join 的分区键（last link 左键）
     */
    /** 无列裁剪的完整构造（首表读全列；各层右表列集随 {@link JoinLink#rightColumns()}）。 */
    public DistributedJoinOperator(String queryId,
                                   String firstCatalog, String firstSchema, String firstTable,
                                   List<String> firstLeftKeys, DojaoConnector firstProbe,
                                   String firstFilter,
                                   List<JoinLink> links, long memoryLimit, int numPartitions,
                                   WorkerGateway gateway, List<WorkerRegistration> workers,
                                   FragmentSpec.FollowUp followUp) {
        this(queryId, firstCatalog, firstSchema, firstTable, firstLeftKeys, firstProbe, firstFilter,
                links, memoryLimit, numPartitions, gateway, workers, followUp, null);
    }

    /** 完整构造（多 join 链 / 带 followUp）+ 首表列裁剪（{@code firstColumns} 空 = 读全列）。 */
    public DistributedJoinOperator(String queryId,
                                   String firstCatalog, String firstSchema, String firstTable,
                                   List<String> firstLeftKeys, DojaoConnector firstProbe,
                                   String firstFilter,
                                   List<JoinLink> links, long memoryLimit, int numPartitions,
                                   WorkerGateway gateway, List<WorkerRegistration> workers,
                                   FragmentSpec.FollowUp followUp, List<String> firstColumns) {
        this(queryId, firstCatalog, firstSchema, firstTable, firstLeftKeys, firstProbe, firstFilter,
                links, memoryLimit, numPartitions, gateway, workers, followUp, firstColumns, null);
    }

    /**
     * 完整构造 + <b>派生源</b>（多 join 链里含子查询 / CTE 的情况）：{@code sources} 与
     * 「T0, T1, ..., T(N-1)」一一对应（下标 0 = 首表；下标 k = 第 k 层 join 的右表），元素 null =
     * 普通基表扫描。派生源所在层由 Master 发「派生聚合流水线」喂入（见
     * {@code StageScheduler.emitDerivedSource}），该层的 {@code rightProbe} / 扫描列可为 null。
     */
    public DistributedJoinOperator(String queryId,
                                   String firstCatalog, String firstSchema, String firstTable,
                                   List<String> firstLeftKeys, DojaoConnector firstProbe,
                                   String firstFilter,
                                   List<JoinLink> links, long memoryLimit, int numPartitions,
                                   WorkerGateway gateway, List<WorkerRegistration> workers,
                                   FragmentSpec.FollowUp followUp, List<String> firstColumns,
                                   List<StageScheduler.DerivedSource> sources) {
        this(queryId, firstCatalog, firstSchema, firstTable, firstLeftKeys, firstProbe, firstFilter,
                links, memoryLimit, numPartitions, gateway, workers, followUp, firstColumns, sources,
                List.of());
    }

    /** 完整构造 + 派生源 + 窗口链（多 join / 含派生源的窗口下沉）。 */
    public DistributedJoinOperator(String queryId,
                                   String firstCatalog, String firstSchema, String firstTable,
                                   List<String> firstLeftKeys, DojaoConnector firstProbe,
                                   String firstFilter,
                                   List<JoinLink> links, long memoryLimit, int numPartitions,
                                   WorkerGateway gateway, List<WorkerRegistration> workers,
                                   FragmentSpec.FollowUp followUp, List<String> firstColumns,
                                   List<StageScheduler.DerivedSource> sources,
                                   List<FragmentSpec.WindowStep> windowChain) {
        this(queryId, firstCatalog, firstSchema, firstTable, firstLeftKeys, firstProbe, firstFilter,
                links, memoryLimit, numPartitions, gateway, workers, followUp, firstColumns, sources,
                windowChain, List.of());
    }

    /** 同上 + 外连接同名列消歧别名：最末层 join 片段在 join 之后、窗口重分区之前套用。 */
    public DistributedJoinOperator(String queryId,
                                   String firstCatalog, String firstSchema, String firstTable,
                                   List<String> firstLeftKeys, DojaoConnector firstProbe,
                                   String firstFilter,
                                   List<JoinLink> links, long memoryLimit, int numPartitions,
                                   WorkerGateway gateway, List<WorkerRegistration> workers,
                                   FragmentSpec.FollowUp followUp, List<String> firstColumns,
                                   List<StageScheduler.DerivedSource> sources,
                                   List<FragmentSpec.WindowStep> windowChain,
                                   List<FragmentSpec.JoinAlias> outputAliases) {
        super("DistributedJoin", queryId, numPartitions, gateway, workers);
        this.firstCatalog = firstCatalog;
        this.firstSchema = firstSchema;
        this.firstTable = firstTable;
        this.firstLeftKeys = firstLeftKeys;
        this.firstProbe = firstProbe;
        this.firstFilter = firstFilter;
        this.firstColumns = firstColumns;
        this.links = List.copyOf(links);
        this.memoryLimit = memoryLimit;
        this.followUp = followUp;
        this.sources = sources;
        this.windowChain = windowChain == null ? List.of() : List.copyOf(windowChain);
        this.outputAliases = outputAliases == null ? List.of() : List.copyOf(outputAliases);
    }

    @Override
    protected int dispatchStage() throws Exception {
        JoinChainStage stage = StageScheduler.planJoinChain(
                queryId, workers, numPartitions,
                firstCatalog, firstSchema, firstTable, firstLeftKeys, firstProbe, firstFilter,
                links, memoryLimit, followUp, firstColumns, sources, windowChain, outputAliases,
                partialFollowUp);
        // 片段已在 planJoinChain 内按反死锁顺序排好：最终 Consumer → 中间层逆序 → 基表 Producers
        for (FragmentSpec fragment : withTerminal(stage.fragments())) {   // 分布式写 SinkStep + top-N 截断尾

            dispatch(fragment);
        }
        System.err.printf("[DistributedJoin] query=%s workers=%d partitions=%d consumers=%d fragments=%d joins=%d%n",
                queryId, workers.size(), numPartitions, stage.finalConsumerCount(),
                stage.fragments().size(), links.size());
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

    // ==================== 只读访问 ====================

    /** 全部 join 环节（单 join = 1 元素，多 join = N 元素）。 */
    public List<JoinLink> links() { return links; }
    public String firstTable() { return firstTable; }
    public List<String> firstLeftKeys() { return firstLeftKeys; }
    /** 首表（T0，probe 侧）实际扫描的列集；null/空 = 读全列。 */
    public List<String> firstColumns() { return firstColumns; }
    public int numPartitions() { return numPartitions; }
    /** 各关系的派生源（下标 0 = 首表；下标 k = 第 k 层 join 右表）；null = 全普通基表。 */
    public List<StageScheduler.DerivedSource> sources() { return sources; }

    /**
     * 最末层 join 之后的同 key 后续阶段（聚合 / 单窗口）；null = 纯 join。
     *
     * <p><b>规划期不变量</b>：携带 followUp 的算子已是「后续计算边界」，Master 侧<b>不得</b>把它
     * 当作可重建的 join 基 —— {@link #withAggregate} / {@link #withWindowChain} 都只从
     * {@code links} 重建新算子，会把既有 followUp 静默丢弃（聚合套聚合 → 内层聚合消失、
     * 外层引用其别名恒 NULL）。见 {@code Planner#isBareJoin}。</p>
     */
    public FragmentSpec.FollowUp followUp() { return followUp; }

    /**
     * EXPLAIN 明细：probe 侧来源 + 逐层 join 的连接类型 / build 侧表 / 连接键 / 算法。
     *
     * <p>多表链在 Master 侧只是一个节点，看不出「每层用了什么」—— 把
     * {@link JoinLink} 里的选型结论逐层展开，是 EXPLAIN 可读性的关键。</p>
     */
    @Override
    public List<String> explainLines() {
        List<String> out = new ArrayList<>();
        boolean firstDerived = sources != null && !sources.isEmpty() && sources.get(0) != null;
        out.add("probe: " + (firstDerived ? "<derived>" : qualified(firstCatalog, firstSchema, firstTable))
                + " keys=" + firstLeftKeys
                + (firstFilter != null && !firstFilter.isBlank() ? " filter=[" + firstFilter + "]" : ""));
        for (int i = 0; i < links.size(); i++) {
            StageScheduler.JoinLink link = links.get(i);
            boolean derived = sources != null && sources.size() > i + 1 && sources.get(i + 1) != null;
            String build = derived ? "<derived>"
                    : qualified(link.rightCatalog(), link.rightSchema(), link.rightTable());
            String keys = link.joinType() == JoinType.CROSS
                    ? "CROSS(no keys)"
                    : link.leftKeys() + "=" + link.rightKeys();
            out.add("join-" + (i + 1) + ": " + link.joinType()
                    + " build=" + build
                    + " keys=" + keys
                    + " algo=" + JoinExplain.distributed(link.algorithm(), memoryLimit, numPartitions)
                    + (link.rightFilter() != null && !link.rightFilter().isBlank()
                            ? " filter=[" + link.rightFilter() + "]" : "")
                    // ON 单侧残差：未下推到扫描（保留侧还要输出孤儿行），在 Worker 的匹配阶段求值
                    + (link.onLeftFilter() != null ? " onLeft=[" + link.onLeftFilter() + "]" : "")
                    + (link.onRightFilter() != null ? " onRight=[" + link.onRightFilter() + "]" : ""));
        }
        for (int g = 0; g < windowChain.size(); g++) {
            FragmentSpec.WindowStep w = windowChain.get(g);
            out.add("window-" + (g + 1) + ": partitionBy=" + w.partitionBy()
                    + " orderBy=" + w.orderBy()
                    + (w.filter() != null ? " preFilter=[" + w.filter().describe() + "]" : "")
                    + (g == 0 ? " (join 输出重分区)" : " (上一组窗口输出重分区)"));
        }
        return out;
    }

    private static String qualified(String catalog, String schema, String table) {
        if (table == null) return "<none>";
        return (catalog == null || catalog.isBlank() ? "" : catalog + ".")
                + (schema == null || schema.isBlank() ? "" : schema + ".") + table;
    }

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

    /** 下标 i（0 = 首表；k = 第 k 层 join 右表）：派生源 → pipeline（列由流水线喂入），否则 required 基表扫描。 */
    private ScanColumn scanColumn(int i, String catalog, String schema, String table, List<String> columns) {
        String relation = catalog + "." + schema + "." + table;
        boolean derived = sources != null && i < sources.size() && sources.get(i) != null;
        return derived ? ScanColumn.pipeline(relation) : ScanColumn.required(relation, columns);
    }

    /**
     * 某关系（下标 {@code i}，0 = 首表）的输出列：派生源 → 派生体声明列（分组键 + 聚合结果，
     * 无连接器元数据可查）；普通基表 → 连接器元数据扫描列。
     */
    private List<Field> sourceFields(int i, DojaoConnector probe, String catalog, String schema,
                                     String table, List<String> columns) {
        StageScheduler.DerivedSource ds =
                (sources == null || i >= sources.size()) ? null : sources.get(i);
        return ds != null ? StageScheduler.derivedFields(ds)
                : scanFields(probe, catalog, schema, table, columns);
    }

    // 链式单 join 语义（join→聚合/窗口 用）：首表 = 左表（probe 侧），唯一 link = 右表（build 侧）。
    // 多 join 时这些访问器仅对 links.get(0) 有意义 —— 提取器已按 links().size() 限定场景。
    public String leftCatalog() { return firstCatalog; }
    public String leftSchema() { return firstSchema; }
    public String leftTable() { return firstTable; }
    public List<String> leftKeys() { return firstLeftKeys; }
    public DojaoConnector leftProbe() { return firstProbe; }
    public String leftFilter() { return firstFilter; }
    public JoinType joinType() { return links.get(0).joinType(); }
    public String rightCatalog() { return links.get(0).rightCatalog(); }
    public String rightSchema() { return links.get(0).rightSchema(); }
    public String rightTable() { return links.get(0).rightTable(); }
    public List<String> rightKeys() { return links.get(0).rightKeys(); }
    public DojaoConnector rightProbe() { return links.get(0).rightProbe(); }
    public String rightFilter() { return links.get(0).rightFilter(); }
    public long memoryLimit() { return memoryLimit; }
    public WorkerGateway gateway() { return gateway; }
    public List<WorkerRegistration> workers() { return workers; }

    /**
     * 克隆本算子并给最末层 join 挂上<b>同 key 聚合</b>（零二次交换）：join 输出留本 Worker
     * 直接按 key 本地聚合再回传（分区↔worker 绑定不变，owner 映射与全查询一致）。返回的新算子
     * 替代原算子执行，原算子实例丢弃（不会被 open）。调用方须保证分组键 {@code keys} 包含
     * 最末层 join 的分区键（last link 左键）——分区键 ⊆ 分组键 → 每个分组完全落在同一分区，
     * worker 本地聚合即为最终结果，master 零合并。
     */
    /**
     * 克隆本算子并挂上<b>窗口分组链</b>（join → 窗口#0 → 重分区 → 窗口#1 …，全留 Worker）：
     * links / sources / 各层过滤与列裁剪原样保留（派生流水线不丢）。返回的新算子替代原算子执行。
     */
    public DistributedJoinOperator withWindowChain(List<FragmentSpec.WindowStep> steps) {
        return withWindowChain(steps, outputAliases);
    }

    /**
     * 同上 + 外连接同名列消歧：{@code outputAliases} 由最末层 join 片段在 join 之后、窗口重分区
     * 之前套用（窗口键 / 值表达式经 {@code aliasOuterJoinKeyRefs} 改写为引用 {@code __jk_*}）。
     */
    public DistributedJoinOperator withWindowChain(List<FragmentSpec.WindowStep> steps,
                                                   List<FragmentSpec.JoinAlias> aliases) {
        requireBare("windowChain");
        return new DistributedJoinOperator(queryId, firstCatalog, firstSchema, firstTable,
                firstLeftKeys, firstProbe, firstFilter, links, memoryLimit, numPartitions,
                gateway, workers, null, firstColumns, sources, steps, aliases);
    }

    /**
     * 本算子必须仍是<b>纯 join</b>（无 followUp / windowChain）才能向上叠加新阶段。
     *
     * <p>从 {@code links} 重建的构造器不复制既有阶段 —— 一旦被误用在「已有后续计算」的算子上，
     * 会把内层聚合 / 窗口整段顶掉（历史缺陷：三表 join 下聚合套聚合静默返回 NULL）。这里改为
     * 规划期直接抛错，让任何未来的错误调用都显式失败，而不是产出错误数据。</p>
     */
    private void requireBare(String stage) {
        if (followUp != null || !windowChain.isEmpty()) {
            throw new IllegalStateException(
                    "DistributedJoinOperator 已携带后续阶段（followUp="
                            + (followUp == null ? "null" : followUp.getClass().getSimpleName())
                            + ", windowGroups=" + windowChain.size()
                            + "），不能再叠加 " + stage + " —— 重建舞台会丢弃既有阶段");
        }
    }

    /** 最末层 join 上的窗口分组链（空 = 无）。 */
    public List<FragmentSpec.WindowStep> windowChain() { return windowChain; }

    /** 外连接同名列消歧别名（空 = 无）。 */
    public List<FragmentSpec.JoinAlias> outputAliases() { return outputAliases; }

    /**
     * <b>全状态复制</b>：links / sources / followUp / windowChain / outputAliases 原样保留。
     *
     * <p>叠加阶段一律基于本方法再替换目标字段，禁止再用「只传 links + 部分字段」的位置参数
     * 构造器 —— 那正是历史上静默丢内层阶段的根因。</p>
     */
    public DistributedJoinOperator copy() {
        DistributedJoinOperator copy = new DistributedJoinOperator(queryId, firstCatalog, firstSchema,
                firstTable, firstLeftKeys, firstProbe, firstFilter, links, memoryLimit, numPartitions,
                gateway, workers, followUp, firstColumns, sources, windowChain, outputAliases);
        copy.partialFollowUp = this.partialFollowUp;
        return copy;
    }

    /** 设置二级聚合（外层全局聚合下沉；Planner 在 doOpen 前调用，null = 无）。 */
    public void setPartialFollowUp(FragmentSpec.AggregateStep f) { this.partialFollowUp = f; }

    /** 已设置的二级聚合（可空；审计 / EXPLAIN 只读）。 */
    public FragmentSpec.AggregateStep partialFollowUp() { return partialFollowUp; }

    @Override
    public List<FragmentSpec.WindowStep> windowSteps() { return windowChain; }

    @Override
    public void replaceWindowSteps(List<FragmentSpec.WindowStep> steps) {
        if (steps == null || steps.isEmpty()) return;
        this.windowChain = List.copyOf(steps);
    }

    public DistributedJoinOperator withAggregate(List<String> keys, List<AggregationSpec> aggs,
                                                 List<CaseProjection> caseProjections) {
        requireBare("aggregate");
        FragmentSpec.AggregateStep step = new FragmentSpec.AggregateStep(
                keys, aggs.stream().map(FragmentSpec.AggregationDto::of).toList(),
                caseProjections, memoryLimit, numPartitions);
        // 全状态构造：sources / windowChain / outputAliases 原样保留（禁止只传 links 的便捷
        // 构造器 —— 它会把派生流水线与别名静默清空，三表 join 静默 NULL 的历史根因）。
        return new DistributedJoinOperator(queryId, firstCatalog, firstSchema, firstTable,
                firstLeftKeys, firstProbe, firstFilter, links, memoryLimit, numPartitions,
                gateway, workers, step, firstColumns, sources, windowChain, outputAliases);
    }

    @Override
    public List<Field> outputFields() {
        // 复刻终端 Consumer / ChainedJoinConsumer 输出页 schema：
        //   a) 普通 Consumer：join 输出页 = 基表（左）扫描列 逐层 join 右表扫描列，剔除「右 join key 且左已有
        //      同名」列（HashJoinOperator.ensureOutputSchema 规则）—— 列集取与实际扫描严格相同的
        //      firstColumns / link.rightColumns()（空 = 全列），否则空桶退回声明值时列数会与其他分区不同。
        //   b) ChainedJoinConsumer（followUp 非 null）：在 a) 的 join 输出上再做同 key 聚合 / 窗口 ——
        //      聚合 = 分组键列 + 聚合结果列；窗口 = join 全列 + 窗口函数列。
        List<Field> wire = new ArrayList<>(
                sourceFields(0, firstProbe, firstCatalog, firstSchema, firstTable, firstColumns));
        int idx = 1;
        for (JoinLink link : links) {
            List<Field> right = sourceFields(idx, link.rightProbe(), link.rightCatalog(), link.rightSchema(),
                    link.rightTable(), link.rightColumns());
            wire = joinOutputFields(link.joinType(), wire, right, link.leftKeys(), link.rightKeys());
            idx++;
        }
        if (!windowChain.isEmpty()) {
            // 窗口链：逐组按声明输出列重定基并追加窗口函数列（K 形状投影吸收见基类 helper）。
            return windowChainOutputFields(wire, windowChain);
        }
        if (followUp == null) {
            return wire;   // 普通 Consumer：join 输出页即最终页
        }
        return switch (followUp) {
            case FragmentSpec.WindowStep w -> {
                // 窗口输出 = join 全列 + 每函数列（LAG/LEAD 镜像 join 列类型）
                List<Field> out = new ArrayList<>(wire);
                // wire 在上面链接循环被反复重赋值 → 非 effectively-final，lambda 须捕获 final 副本
                List<Field> joinWire = wire;
                for (WindowFunctionSpec spec : w.functions()) {
                    out.add(windowFunctionField(spec, name -> fieldOf(joinWire, name)));
                }
                yield out;
            }
            case FragmentSpec.AggregateStep a -> {
                // 同 key 聚合输出 = 分组键（join 列）+ 各聚合别名（count→BIGINT、其余→DOUBLE）
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
