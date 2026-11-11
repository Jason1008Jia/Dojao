package com.dojao.server.master;

import com.dojao.core.operator.AbstractDistributedOperator;
import com.dojao.core.operator.DistributedScanOperator;
import com.dojao.core.operator.DistributedTask;
import com.dojao.core.operator.MeasurableOperator;
import com.dojao.core.operator.Operator;
import com.dojao.core.operator.OperatorState;
import com.dojao.core.operator.OperatorStats;
import com.dojao.core.planner.PhysicalNode;
import com.dojao.core.planner.PhysicalPlan;
import com.dojao.common.util.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 查询守护管理器 —— 每个用户查询一个实例。
 *
 * <p>职责：实时收集整条算子链的指标（每个算子是否正在执行、输入/输出行数、
 * 吞吐、内存、溢写、CPU、耗时），供 WebUI / 客户端轮询；并在查询失败时统一
 * 标记失败算子、记录失败原因，为重试（后续迭代）预留钩子。
 *
 * <p>所有算子均实现 {@link MeasurableOperator}，因此只需遍历物理计划树，
 * 逐次 {@code snapshot()} 读 {@link OperatorStats} 即可拿到实时状态。
 * 瞬时吞吐 = 两次快照间 rowsOut 差分 / 时间差（快照差分口径）。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/11
 */
public final class QueryManager {

    private static final Logger log = LoggerFactory.getLogger(QueryManager.class);

    /** 可排序 queryId 时间戳格式：yyyyMMddHHmmss（14 位，提交即知任务时间）。 */
    private static final DateTimeFormatter QUERY_ID_TIMESTAMP =
            DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    /**
     * 生成可排序的 queryId：{@code yyyyMMddHHmmss_UUID}。
     * 时间戳前缀保证按提交时间可排序，UUID 保证并发唯一。
     */
    public static String newQueryId() {
        return LocalDateTime.now().format(QUERY_ID_TIMESTAMP) + "_" + UUID.randomUUID();
    }

    private final String queryId;
    private final String sql;
    private final PhysicalPlan physical;
    /** 计划树前序遍历的算子节点（第 0 个为根算子）。 */
    private final List<PhysicalNode> operatorNodes;
    /** 计划树中可度量算子节点（前序，第 0 个为根；{@link #snapshot()} 的 operators 与之按索引对齐）。 */
    private final List<PhysicalNode> measuredNodes;

    /**
     * 本查询物理计划内全部分布式算子（{@link AbstractDistributedOperator}）的 operator queryId 集合。
     * Worker 查询遥测的分组键 = 派发 jobId = 该算子的 queryId（格式 "query-&lt;hex&gt;"，Planner 生成），与
     * {@link #queryId()}（yyyyMMddHHmmss_UUID）不同构 —— Master 侧归并遥测时靠它反查归属本查询
     * （{@link #ownsOperatorQueryId}），否则 Worker 上抛的组因键不匹配被整组丢弃。
     */
    private final Set<String> operatorQueryIds = ConcurrentHashMap.newKeySet();

    /**
     * 执行期派发登记入口（由 {@link com.dojao.core.runtime.master.DispatchScope} 在片段派发时回调）：
     * 主计划树之外、执行期才规划的分布式子计划（物化 CTE / 物化派生体查询体）也把自己的 jobId
     * 归到本查询。并发安全 —— 派发在执行线程，归属判定在 Flight 遥测线程。
     */
    public void registerOperatorQueryId(String operatorQueryId) {
        if (operatorQueryId != null) {
            operatorQueryIds.add(operatorQueryId);
        }
    }

    /**
     * 主计划树<b>之外</b>、执行期惰性规划的分布式算子 jobId（物化 CTE / 物化派生体查询体）。
     *
     * <p>这些片段由查询体自己的算子派发，jobId 不在 {@link #operatorQueryIds} 里；若不参与归属判定，
     * Master 的僵尸片段回收会把正在跑的查询体片段误判成残留并取消（Worker 表现为
     * {@code Master 已不认该查询 → 回收残留片段} + 中间片段 {@code InterruptedException}）。</p>
     */
    private final Set<String> auxiliaryOperatorIds;

    private final long submitTimeNanos;
    private volatile long startTimeNanos;
    private volatile long endTimeNanos;
    private volatile QueryState state = QueryState.SUBMITTED;
    private volatile String failureMessage;

    /**
     * 执行本查询的线程（{@link #markRunning()} 登记，{@link #cancel()} 靠它中断）。
     *
     * <p>只置状态位是不够的：执行线程常常正阻塞在"等 Worker 回数据的队列 / 等分区归并的线程池 /
     * 等 shuffle 页"上，不主动抬头就永远读不到取消位 —— 长查询里一次 {@code next()} 可能几十秒
     * 不返回，取消会拖到那一刻才生效。{@code interrupt()} 是唯一能让阻塞中的等待<b>当场抛出</b>
     * 的机制（不必等超时），用它把取消信号真正送进数据面。</p>
     */
    private volatile Thread runningThread;

    /** 重试钩子：attempts 从 1 起，retryLimit 为 0 表示不重试（默认）。 */
    private final AtomicInteger attempts = new AtomicInteger(1);
    private final int retryLimit;

    /** 算子失败记录（operatorId → 失败信息）。 */
    private final ConcurrentHashMap<String, String> operatorFailures = new ConcurrentHashMap<>();

    // 瞬时吞吐差分簿记：operatorId -> {最近一次 rowsOut, 最近一次 outputBytes, 最近一次快照时刻}
    private final ConcurrentHashMap<String, Long> lastRowsOut = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> lastOutputBytes = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> lastSnapshotTime = new ConcurrentHashMap<>();

    /**
     * Worker 查询遥测覆盖表：taskId → 该<b>非终端</b>片段的累计发出行数（Worker ~300ms 上抛，best-effort）。
     * 只覆盖「数据不回传 Master」的 Producer / 中间层片段（Scan→Shuffle / Join→Shuffle /
     * Aggregate→Shuffle）——它们的行数只有 Worker 自己知道；终端 Consumer / 独立 Scan 叶子的
     * 权威行数由数据面（FragmentData / ScanPage）维护，遥测不涉足 → 无双计。行数单调累计、
     * 按 taskId 覆盖，丢一拍下拍自愈；查询结束移入 recent 后仍保留（冻结终值）。
     */
    private final ConcurrentHashMap<String, Long> liveFragmentRows = new ConcurrentHashMap<>();

    /**
     * Worker 遥测流量覆盖表（与 {@link #liveFragmentRows} 同源、同键、同语义）：taskId → 该非终端
     * 片段累计<b>读入 / 发出字节</b>。字节统计只存在于 Worker 侧算子对象上（{@code page.getSizeInBytes()}），
     * Master 无从得知，与行数一样只能靠遥测上抛 —— 否则 Scan / Join / Mid 阶段框的流量卡恒 0。
     * 键含片段<b>内部分区子任务</b> {@code <fragmentId>#p<k>}（join 分区线程各自计量）。
     */
    private final ConcurrentHashMap<String, Long> liveFragmentBytesIn = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> liveFragmentBytesOut = new ConcurrentHashMap<>();

    /**
     * Worker 遥测溢写覆盖表（与行数 / 字节同源、同键）：taskId → 该片段内溢写算子累计<b>溢写字节</b>。
     *
     * <p>溢写只发生在 Worker 进程内的算子对象上（{@code AbstractSpillableOperator.getSpilledBytes()}），
     * Master 侧同名算子的 {@link OperatorStats#getSpilledBytes()} 恒 0 —— 分布式查询的「溢写总量」
     * 因此一直为空。这里靠遥测通道补上；{@link #snapshot()} 按算子归并各片段之和。</p>
     */
    private final ConcurrentHashMap<String, Long> liveFragmentSpilled = new ConcurrentHashMap<>();

    /**
     * Worker 遥测已确认收尾的任务 id（含片段<b>内部分区子任务</b> {@code <fragmentId>#p<k>}）。
     * 片段级 id 与 {@link AbstractDistributedOperator#markTaskFinished} 双写（任务表置 FINISHED）；
     * 分区子任务 id 只在这里留痕 —— 它们不是 Master 派发的任务，{@link #collectTasks} 据此把片段
     * 展开成 n 个分区任务并逐一变绿（逐分区独立完成度）。
     */
    private final Set<String> finishedFragments = ConcurrentHashMap.newKeySet();

    /**
     * 探查行数总量表：{@code schema.table} → 该源表连接器探查的行数估计（{@code rowCount}）。
     * 物理计划按此量决定分区并行度；这里把它挂到每个 Producer 扫描任务上作为进度分母
     * （{@code expectedRows}），前端 Scan 进度条 = {@code 实时扫行 / 总量}。仅 {@code rowCount>0} 收录。
     */
    private final Map<String, Long> sourceExpectedRows;

    /**
     * 扫描源行数估计表（{@code schema.table} → 估算行数）。UI 用它展示「这个表有多少行」，
     * 与预览前 N 行区分开。单表查询时即该表的数据量（连接器估算，非精确 count）。
     */
    public Map<String, Long> sourceExpectedRows() {
        return sourceExpectedRows;
    }

    /** 便捷构造：自动生成可排序 queryId（yyyyMMddHHmmss_UUID）。 */
    public QueryManager(String sql, PhysicalPlan physical) {
        this(newQueryId(), sql, physical, 0, Map.of());
    }

    public QueryManager(String queryId, String sql, PhysicalPlan physical) {
        this(queryId, sql, physical, 0, Map.of());
    }

    public QueryManager(String queryId, String sql, PhysicalPlan physical, int retryLimit) {
        this(queryId, sql, physical, retryLimit, Map.of());
    }

    public QueryManager(String queryId, String sql, PhysicalPlan physical, int retryLimit,
                        Map<String, Long> sourceExpectedRows) {
        this.queryId = queryId;
        this.sql = sql;
        this.physical = physical;
        this.sourceExpectedRows = sourceExpectedRows == null ? Map.of() : Map.copyOf(sourceExpectedRows);
        this.retryLimit = Math.max(0, retryLimit);
        this.submitTimeNanos = System.nanoTime();
        this.auxiliaryOperatorIds = physical.auxiliaryOperatorIds();
        this.operatorNodes = new ArrayList<>();
        collect(this.operatorNodes, physical.root());
        this.measuredNodes = this.operatorNodes.stream()
                .filter(n -> n.operator() instanceof MeasurableOperator)
                .toList();
        for (PhysicalNode node : operatorNodes) {
            if (node.operator() instanceof AbstractDistributedOperator ado) {
                operatorQueryIds.add(ado.queryId());
            } else if (node.operator() instanceof DistributedScanOperator dso) {
                // 独立扫描叶子继承 AbstractMeasurableOperator（非 AbstractDistributedOperator），
                // queryId() 取不到 —— 不登记其 jobId，Worker 的扫描存活心跳会被路由整组丢弃，
                // 看门狗又会退回「按数据页判生死」（P1 的老毛病）。
                operatorQueryIds.add(dso.jobId());
            }
        }
    }

    /** 前序遍历物理计划树，收集所有算子节点。 */
    private static void collect(List<PhysicalNode> out, PhysicalNode node) {
        if (node == null) return;
        out.add(node);
        for (PhysicalNode child : node.children()) {
            collect(out, child);
        }
    }

    // ==================== 生命周期 ====================

    public void markRunning() {
        // 取消可能抢在执行开始之前到达（那会儿还没有执行线程可中断）→ 不能把 CANCELED 覆盖成 RUNNING
        if (state != QueryState.CANCELED) {
            this.state = QueryState.RUNNING;
        }
        this.startTimeNanos = System.nanoTime();
        // 登记执行线程：两处执行路径（respondRows / respondWriteResult）的第一句都是本方法，
        // 由真正跑算子的线程执行 —— 无论上层是 UI 线程池还是 Flight 线程，登记到的都是对的那个。
        this.runningThread = Thread.currentThread();
        if (state == QueryState.CANCELED) {
            // 补一次中断：此时才拿到执行线程，否则这次取消只能等第一次 next() 返回才被读到
            Thread.currentThread().interrupt();
        }
    }

    /** 正常完成；若已被取消（{@link #cancel()}）则保持 CANCELED，不被覆盖。 */
    public void markFinished() {
        if (state != QueryState.CANCELED) {
            this.state = QueryState.FINISHED;
            this.endTimeNanos = System.nanoTime();
            log.info("Query {} finished, wall={}ms", queryId, wallMs());
        }
    }

    /** 标记失败：查询置 FAILED，把尚未完成的算子统一置 FAILED 并记录失败原因；若已被取消则不覆盖 CANCELED。 */
    public void markFailed(Throwable t) {
        if (state == QueryState.CANCELED) {
            return;
        }
        this.state = QueryState.FAILED;
        this.endTimeNanos = System.nanoTime();
        this.failureMessage = describeFailure(t);
        for (PhysicalNode node : operatorNodes) {
            Operator op = node.operator();
            if (op instanceof MeasurableOperator mo
                    && mo.getStats().getState() != OperatorState.FINISHED) {
                mo.markFailed(t);
            }
        }
        log.error("Query {} failed: {}", queryId, failureMessage);
    }

    /**
     * 失败原因文本：自外向内串起整条 cause 链，一行读到底（见 {@link Errors#describe}）。
     *
     * <p>算子层统一把异常包成 {@code "Operator X next failed"}（见
     * {@code AbstractMeasurableOperator}），真因埋在 cause 链最底部 —— 例如 CTAS 目标表已存在时
     * 顶层只有 {@code Operator TableSink[...] next failed}，而 {@code Table 'tmp1' already exists}
     * 在第三层。只取 {@code t.toString()} 等于把真因丢掉，用户只能靠猜。</p>
     */
    private static String describeFailure(Throwable t) {
        return Errors.describe(t);
    }

    /**
     * 取消查询：置 CANCELED → <b>中断执行线程</b>；执行循环读到取消后停止拉取，
     * {@code finally} 里的 {@code close()} 反向取消 Worker 片段并释放后台线程。
     *
     * <p><b>顺序不能反</b>：先立状态位、再中断。被中断方醒来第一件事是判断"这是取消还是故障"，
     * 位子立着才走安静退出那条路；反过来则可能在位子写下去之前就被判成故障。</p>
     */
    public void cancel() {
        this.state = QueryState.CANCELED;          // ① 先立旗：这是「取消」不是「失败」
        this.endTimeNanos = System.nanoTime();
        Thread t = this.runningThread;
        if (t != null) {
            t.interrupt();                         // ② 再按对讲机：把阻塞中的等待立刻弹醒
        }
        log.warn("Query {} canceled", queryId);
    }

    /**
     * 重试钩子（仅预留，未实现自动重启）：记录某个算子（或其某个并行 task）
     * 失败。返回是否仍有重试额度，由上层决定是否重建并重新执行该查询。
     */
    public boolean onOperatorFailure(String operatorId, String operatorType, Throwable t) {
        String msg = operatorType + " -> "
                + (t == null ? "unknown" : t.getClass().getSimpleName() + ": " + t.getMessage());
        operatorFailures.put(operatorId, msg);
        boolean retryable = attempts.get() < retryLimit;
        log.warn("Query {} operator {} failed (attempt {}/{}, retryable={}): {}",
                queryId, operatorId, attempts.get(), retryLimit, retryable, msg);
        return retryable;
    }

    /** 重试钩子：进入下一次尝试（调用方决定是否真的重新执行）。 */
    public QueryManager newAttempt() {
        attempts.incrementAndGet();
        return this;
    }

    // ==================== Worker 查询遥测归并 ====================

    /**
     * Worker 遥测入口（MasterControlFlightProducer.queryTelemetry 每帧调用）：按 taskId 覆盖某个
     * <b>非终端</b>片段的累计发出行数与收发字节。均为单调累计、best-effort：丢一拍下拍自愈；任务
     * 结束 Worker 注销采样器后不再上抛，本表保留最后冻结值（查询移入 recent 后 UI 仍能看到终值）。
     *
     * <p>{@code done=true} = Worker 收尾补发的终值帧：除覆盖终值外，还把该片段任务从 RUNNING 置
     * FINISHED —— Scan / Join / Mid 阶段框在真实完成时立即变绿，而非等整条查询收尾被统一扫尾。</p>
     *
     * <p>{@code spilledBytes} 是<b>独立通道</b>：{@code -1} = 本帧无溢写读数（不覆盖）。终端 Consumer
     * （窗口排序 / 聚合 / 排序归并 join）是溢写主力但行数走数据面，其帧 {@code rows=-1} 而
     * {@code spilledBytes>=0} —— 故溢写必须在任何「按行数提前返回」之前落表。</p>
     */
    public void acceptFragmentTelemetry(String taskId, long rows, long bytesIn, long bytesOut,
                                        long spilledBytes, boolean done) {
        if (taskId == null) {
            return;
        }
        // 溢写与行数 / 字节 / 存活心跳互不影响，任何提前返回路径之前先落表。
        // spillAdvanced 只表示「这一拍的溢写量比上一拍有增长」—— 纯溢写帧（终端 Consumer，
        // rows=-1）无论有无进展都会按 300ms 周期到达，绝不能把「帧到了」当成「任务在推进」。
        boolean spillAdvanced = false;
        if (spilledBytes >= 0L) {
            Long prevSpilled = liveFragmentSpilled.put(taskId, spilledBytes);
            spillAdvanced = prevSpilled == null || prevSpilled.longValue() != spilledBytes;
        }
        // 扫描存活心跳（P1）：独立 Scan 叶子没有「数据面回 Master」的页，其片段 tick 只表达
        // 「任务还活着」—— 命中即刷新看门狗的接触时刻后直接返回，<b>不写</b> 行数 / 字节表：
        // 那些表的读数是数据面权威值（终端片段由 FragmentData / ScanPage 天然填），
        // 若让 rows=0 的心跳帧覆盖，会把监控上的真实行数清零。
        for (PhysicalNode node : operatorNodes) {
            if (node.operator() instanceof DistributedScanOperator dso && dso.markAlive(taskId)) {
                // done=true = Worker 侧扫描线程的收尾帧：此后不再有存活心跳，但 ScanFinished 终态
                // 事件可能还堵在满的 Master 事件队列里。告知算子「Worker 已完工」，看门狗改用长宽限期，
                // 避免把已完成的扫描判成 LOST 并 fail-fast（2026-11-11 偶发假死的第二个病根）。
                if (done) {
                    dso.markWorkerFinished(taskId);
                }
                return;
            }
        }
        // 存活心跳（P2，其余分布式算子）：聚合 / join / 窗口 / 写… 的 Producer / 中间层片段 tick
        // 同样是「还活着」的信号 —— 它们的终端 Consumer 要等<b>全部</b> Producer 灌完 shuffle 才可能
        // 吐第一页，大表 join / 聚合这段沉默天然超过看门狗阈值（60s），只看数据面必然误杀。
        //
        // ★「帧到了」≠「任务在推进」：终端 Consumer 只登记纯溢写采样器（rows=-1），其帧按 300ms
        // 周期持续到达，即使片段已彻底挂起（曾出现 RUNNING 100 分钟）。故纯溢写帧必须<b>有进展</b>
        // （溢写量增长）才刷新看门狗；真实的 Phase 1 收页 / Phase 2 归并推进由 Worker 心跳线程的
        // FragmentProgress（progressCounter 递增）走数据面续命，不依赖本通道。
        // 这里刷新看门狗后<b>不返回</b>：与 Scan 叶子不同，这些片段的行数 / 字节本就是监控上
        // Scan / Join 段读数的权威来源（数据不回传 Master），done 扫尾也照旧要跑。
        if (rows >= 0L || spillAdvanced) {
            for (PhysicalNode node : operatorNodes) {
                if (node.operator() instanceof AbstractDistributedOperator ado) {
                    ado.markAlive(taskId);
                }
            }
        }
        if (rows >= 0L) {
            liveFragmentRows.put(taskId, rows);
        }
        if (bytesIn >= 0L) {
            liveFragmentBytesIn.put(taskId, bytesIn);
        }
        if (bytesOut >= 0L) {
            liveFragmentBytesOut.put(taskId, bytesOut);
        }
        if (!done) {
            return;
        }
        finishedFragments.add(taskId);
        for (PhysicalNode node : operatorNodes) {
            if (node.operator() instanceof AbstractDistributedOperator ado && ado.markTaskFinished(taskId)) {
                return;
            }
        }
    }

    // ==================== 快照采集 ====================

    /**
     * 实时采集整条算子链指标，返回不可变快照。
     * 瞬时吞吐按快照差分口径：两次调用间 rowsOut 增量 / 时间差。
     */
    public QuerySnapshot snapshot() {
        long now = System.nanoTime();
        List<OperatorSnapshot> ops = new ArrayList<>(measuredNodes.size());
        for (PhysicalNode node : measuredNodes) {
            Operator op = node.operator();
            MeasurableOperator mo = (MeasurableOperator) op;   // measuredNodes 已过滤为可度量算子
            OperatorStats st = mo.getStats();
            String opId = st.getOperatorId();

            // 查询失败时，把未完成算子呈现为 FAILED
            OperatorState stState = st.getState();
            if (state == QueryState.FAILED && stState != OperatorState.FINISHED) {
                stState = OperatorState.FAILED;
            }

            // 瞬时吞吐（快照差分）：行、字节各一路
            long prevRows = lastRowsOut.getOrDefault(opId, 0L);
            long prevBytes = lastOutputBytes.getOrDefault(opId, 0L);
            long prevTime = lastSnapshotTime.getOrDefault(opId, now);
            long deltaRows = Math.max(0L, st.getRowsOutput() - prevRows);
            long deltaBytes = Math.max(0L, st.getOutputBytes() - prevBytes);
            long deltaNanos = now - prevTime;
            long instant = deltaNanos > 0 ? (long) (deltaRows * 1_000_000_000.0 / deltaNanos) : 0L;
            long instantBytes = deltaNanos > 0 ? (long) (deltaBytes * 1_000_000_000.0 / deltaNanos) : 0L;
            lastRowsOut.put(opId, st.getRowsOutput());
            lastOutputBytes.put(opId, st.getOutputBytes());
            lastSnapshotTime.put(opId, now);

            long elapsedMs = st.getElapsedMs();
            long avg = elapsedMs > 0 ? st.getRowsOutput() * 1000L / elapsedMs : 0L;
            long avgBytes = elapsedMs > 0 ? st.getOutputBytes() * 1000L / elapsedMs : 0L;

            ops.add(new OperatorSnapshot(
                    opId,
                    st.getOperatorType(),
                    st.getParallelism(),
                    stState,
                    st.getRowsProcessed(),
                    st.getRowsOutput(),
                    st.getInputBytes(),
                    st.getOutputBytes(),
                    st.getCpuTimeNanos() / 1_000_000L,
                    st.getCurrentMemoryBytes(),
                    st.getPeakMemoryBytes(),
                    // 溢写 = 本算子（单机 / Master 本地）自报 + 分布式 Worker 侧遥测之和
                    st.getSpilledBytes() + workerSpilled(op),
                    elapsedMs,
                    instant,
                    avg,
                    instantBytes,
                    avgBytes,
                    st.getFailureMessage(),
                    collectTasks(op)));
        }

        long totalRowsOut = ops.isEmpty() ? 0L : ops.get(0).rowsOut();
        return new QuerySnapshot(
                queryId, sql, state,
                submitTimeNanos, startTimeNanos, endTimeNanos,
                wallMs(), totalRowsOut,
                attempts.get(), retryLimit,
                ops, Map.copyOf(operatorFailures));
    }

    /**
     * 采集算子的分布式 Worker 任务（任务级监控）：片段任务（{@link AbstractDistributedOperator}）
     * 或分区扫描任务（{@link DistributedScanOperator}）；单机算子返回空。
     *
     * <p>任务 rowsOut 先经 Worker 遥测覆盖表合并：<b>非终端</b>片段（Producer / 中间层）的
     * {@link DistributedTask} 从不回传 FragmentData，数据面 rowsOut 恒 0，这里用 Worker ~1s 上抛的
     * 累计发出行数覆盖之（taskId 命中才覆盖）→ UI Scan / Join 阶段框行数由此变真实。终端 Consumer /
     * 独立 Scan 的任务不在覆盖表（Worker 只上报非终端）→ 保持数据面权威值，无双计。
     */
    private List<OperatorSnapshot.Task> collectTasks(Operator op) {
        List<DistributedTask> raw;
        if (op instanceof AbstractDistributedOperator ado) {
            raw = ado.fragmentTasks();
        } else if (op instanceof DistributedScanOperator dso) {
            raw = dso.scanTasks();
        } else {
            return List.of();
        }
        List<OperatorSnapshot.Task> out = new ArrayList<>(raw.size());
        for (DistributedTask t : raw) {
            // 片段内部分区并行（如中间层 hash join）：Worker 遥测按 #p<k> 报告每分区行数/收尾 →
            // 展开成 n 个分区任务（替换原片段任务，避免行数双计），Join 框因此显示 n 个并行 task。
            List<OperatorSnapshot.Task> partitions = expandPartitions(t);
            if (partitions != null) {
                out.addAll(partitions);
                continue;
            }
            long rows = liveFragmentRows.getOrDefault(t.taskId(), t.rowsOut());
            // 总量估计只对「扫切片」有意义：命中探查表的 Producer 任务带上 rowCount 估计，其余 -1
            String src = t.source();
            out.add(new OperatorSnapshot.Task(t.taskId(), t.workerId(), t.kind(), t.producer(),
                    t.terminal(), taskStateForDisplay(t.state()), rows,
                    liveFragmentBytesIn.getOrDefault(t.taskId(), 0L),
                    liveFragmentBytesOut.getOrDefault(t.taskId(), 0L),
                    liveFragmentSpilled.getOrDefault(t.taskId(), 0L),
                    t.dispatchNanos(), t.finishNanos(), t.failure(),
                    src, expectedFor(src)));
        }
        return out;
    }

    /**
     * 本算子的分布式 Worker 侧溢写字节合计（遥测表按片段任务 id 累加）。
     *
     * <p>溢写只发生在 Worker 进程内的算子对象上（窗口排序 / 聚合 / 排序归并 join），Master 同名算子
     * 的统计恒 0 —— 分布式查询的「溢写总量」由此通道补上；单机算子无片段任务 → 恒 0，保持自身统计。
     * 与行数 / 字节同键（片段 id，见 {@link FragmentExecutor} 的 {@code trackSpill}）。</p>
     */
    private long workerSpilled(Operator op) {
        List<DistributedTask> tasks;
        if (op instanceof AbstractDistributedOperator ado) {
            tasks = ado.fragmentTasks();
        } else if (op instanceof DistributedScanOperator dso) {
            tasks = dso.scanTasks();
        } else {
            return 0L;
        }
        long sum = 0L;
        for (DistributedTask t : tasks) {
            Long s = liveFragmentSpilled.get(t.taskId());
            if (s != null) {
                sum += s;
            }
        }
        return sum;
    }

    /**
     * 片段任务状态的展示归一：查询已正常结束（FINISHED）时，仍停在 RUNNING 的片段任务呈现为完成。
     *
     * <p>存在的理由：终端 Consumer 的完成事件（{@code FragmentFinished}）是 Master
     * <b>拉取时</b>才被消费的（{@code AbstractDistributedOperator.doNext}），而顶层
     * {@code LimitOperator} 满 limit 后立即停止拉取上游（如 {@code ... group by k limit 100}）
     * —— 排队的 FragmentFinished 再无人读取，{@code FragmentTaskState} 永远停在 RUNNING；
     * 收尾只对失败路径扫尾（{@code doClose}），成功路径不置位。结果是数据早已产出（rowsOut 稳定、
     * 结果正确），DAG 上的聚合框却一直显示「运行中 0%」，永远不变绿。
     * 与 {@link #snapshot()} 里「查询失败时未完成算子呈现 FAILED」对称：只改展示，不动真实状态。</p>
     */
    private String taskStateForDisplay(String taskState) {
        if (!DistributedTask.RUNNING.equals(taskState)) {
            return taskState;
        }
        if (state == QueryState.FINISHED) {
            return DistributedTask.FINISHED;
        }
        // 取消：片段没跑完，不能呈现成绿的；呈现 FAILED —— 与「查询失败时未完成算子呈现 FAILED」对称
        if (state == QueryState.CANCELED) {
            return DistributedTask.FAILED;
        }
        return taskState;
    }

    /** 扫描任务进度分母：任务 source（{@code schema.table}）命中探查表 → 其估计行数，否则 {@code -1}（无分母）。 */
    private long expectedFor(String source) {
        return source == null ? -1L : sourceExpectedRows.getOrDefault(source, -1L);
    }

    /**
     * 把一个片段任务展开成其内部 n 个分区子任务（Worker 遥测按 {@link DistributedTask#partitionTaskId}
     * 上报时才有）：返回 {@code null} 表示该片段没有分区级上报，保持单任务。
     *
     * <p>分区数由采样键的连续存在性推断（{@code #p0, #p1, ...} 直到首个缺失）—— 无需新增协议字段；
     * 每分区状态独立：遥测 {@code done} 命中该分区 → FINISHED，否则跟随片段状态（RUNNING）。</p>
     */
    private List<OperatorSnapshot.Task> expandPartitions(DistributedTask t) {
        String base = t.taskId();
        if (!liveFragmentRows.containsKey(DistributedTask.partitionTaskId(base, 0))) {
            return null;
        }
        List<OperatorSnapshot.Task> out = new ArrayList<>();
        for (int k = 0; ; k++) {
            String id = DistributedTask.partitionTaskId(base, k);
            Long rows = liveFragmentRows.get(id);
            if (rows == null) {
                break;
            }
            boolean done = finishedFragments.contains(id);
            long finishNanos = done && t.finishNanos() <= 0L ? System.nanoTime() : t.finishNanos();
            out.add(new OperatorSnapshot.Task(id, t.workerId(), t.kind(), t.producer(),
                    t.terminal(), done ? DistributedTask.FINISHED : taskStateForDisplay(t.state()), rows,
                    liveFragmentBytesIn.getOrDefault(id, 0L),
                    liveFragmentBytesOut.getOrDefault(id, 0L),
                    // 溢写是算子对象级（非分区级）：挂在 0 号分区上，阶段框按任务求和仍得真实值
                    k == 0 ? liveFragmentSpilled.getOrDefault(base, 0L) : 0L,
                    t.dispatchNanos(), finishNanos, t.failure(), t.source(),
                    expectedFor(t.source())));
        }
        return out.isEmpty() ? null : out;
    }

    // ==================== 报告 ====================

    /** 生成人类可读的报告（基于当前快照）。 */
    public String report() {
        return report(snapshot());
    }

    /** 基于指定快照生成人类可读的报告。 */
    public String report(QuerySnapshot s) {
        StringBuilder sb = new StringBuilder();
        sb.append("Query ").append(s.queryId()).append(" [").append(s.state()).append(']')
                .append(" wall=").append(humanDuration(s.wallMs()))
                .append(" rowsOut=").append(s.totalRowsOut())
                .append(" attempts=").append(s.attempts()).append('/').append(s.retryLimit())
                .append(" sql=").append(s.sql()).append('\n');
        int i = 1;
        for (OperatorSnapshot o : s.operators()) {
            sb.append("  [").append(i++).append("] ").append(o.operatorId())
                    .append(" state=").append(o.state())
                    .append(" rowsIn=").append(o.rowsIn())
                    .append(" rowsOut=").append(o.rowsOut())
                    .append(" avg=").append(humanRate(o.avgRowsPerSec())).append("/s")
                    .append(" inst=").append(humanRate(o.instantRowsPerSec())).append("/s")
                    .append(" cpu=").append(humanDuration(o.cpuMs()))
                    .append(" elap=").append(humanDuration(o.elapsedMs()))
                    .append(" mem=").append(o.currentMemBytes() >> 20).append("MB")
                    .append(" peak=").append(o.peakMemBytes() >> 20).append("MB")
                    .append(" spill=").append(o.spilledBytes() >> 20).append("MB")
                    .append('\n');
        }
        if (!s.operatorFailures().isEmpty()) {
            sb.append("  failures:\n");
            for (Map.Entry<String, String> e : s.operatorFailures().entrySet()) {
                sb.append("    ").append(e.getKey()).append(": ").append(e.getValue()).append('\n');
            }
        }
        return sb.toString();
    }

    private static String humanRate(long perSec) {
        if (perSec >= 1_000_000L) return String.format("%.1fM", perSec / 1_000_000.0);
        if (perSec >= 1_000L) return String.format("%.1fK", perSec / 1_000.0);
        return String.valueOf(perSec);
    }

    /**
     * 时长自适应格式化（输入单位毫秒）：<1s → "Xms"；≥1s 且 <60s → "X.XXs"；
     * ≥60s → "X.XXmin"（2 位小数）。如 61361ms → 61.36s / 1.02min。
     */
    public static String humanDuration(long ms) {
        if (ms < 1000L) return ms + "ms";
        if (ms < 60_000L) return String.format("%.2fs", ms / 1000.0);
        return String.format("%.2fmin", ms / 60_000.0);
    }

    // ==================== Getter ====================

    public long wallMs() {
        if (startTimeNanos == 0L) return 0L;
        long end = endTimeNanos > 0L ? endTimeNanos : System.nanoTime();
        return (end - startTimeNanos) / 1_000_000L;
    }

    public String queryId() { return queryId; }
    public String sql() { return sql; }
    public PhysicalPlan physical() { return physical; }
    public QueryState state() { return state; }
    public String failureMessage() { return failureMessage; }
    public int attempts() { return attempts.get(); }
    public int retryLimit() { return retryLimit; }
    public long submitTimeNanos() { return submitTimeNanos; }

    /** 可度量算子节点（snapshot 的 operators 列表与其一一对应、按索引对齐；DAG 拓扑据此构建）。 */
    public List<PhysicalNode> measuredOperators() { return measuredNodes; }

    /**
     * 各节点<b>实际输出行数</b>（节点对象 → rowsOut），EXPLAIN ANALYZE 回填用。
     *
     * <p>键用对象身份（{@link java.util.IdentityHashMap}）：{@link PhysicalNode} 未覆写
     * {@code equals}，同一棵计划树里的节点天然唯一。只在查询结束后调用（执行中的中间值无意义）。</p>
     */
    public Map<PhysicalNode, Long> actualRows() {
        List<OperatorSnapshot> ops = snapshot().operators();
        Map<PhysicalNode, Long> out = new java.util.IdentityHashMap<>();
        for (int i = 0; i < measuredNodes.size() && i < ops.size(); i++) {
            out.put(measuredNodes.get(i), ops.get(i).rowsOut());
        }
        return out;
    }

    /** 该查询是否拥有某 operator queryId（Worker 遥测分组键 = 派发 jobId）。 */
    public boolean ownsOperatorQueryId(String operatorQueryId) {
        return operatorQueryId != null
                && (operatorQueryIds.contains(operatorQueryId)
                    || auxiliaryOperatorIds.contains(operatorQueryId));
    }
}
