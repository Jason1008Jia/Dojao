package com.dojao.core.operator;

import com.dojao.common.data.Page;
import com.dojao.core.runtime.master.WorkerRegistration;
import com.dojao.core.runtime.worker.ScanEvent;
import com.dojao.core.runtime.worker.WorkerGateway;
import com.dojao.spi.bean.ReadRequest;
import com.dojao.spi.bean.ScanSlice;
import com.dojao.spi.service.DojaoConnector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 分布式扫描算子 —— 把分区扫描任务派发给集群 Worker，各 Worker 拉自己分区数据回传 Master。
 *
 * <p>只分布式化 Table Scan（最重的 I/O 部分）：Join / Aggregation / Window / Project 等
 * 其余算子保持在 Master。输出模式与 {@link ParallelTableScanOperator} 一致（有界队列 + 终止
 * 事件计数），下游算子（HashPartition / Join / Agg）<b>零感知变更</b>。</p>
 *
 * <h3>跨机器感知与管理（v1.1）</h3>
 * <p>回传队列元素从 {@link Page} 升级为 {@link ScanEvent} 信封，EOF 语义从「内容标记」变为
 * 「显式事件」，任务终止二选一：{@link ScanEvent.ScanFinished}（成功）/ {@link ScanEvent.ScanFailed}（失败）：</p>
 * <ol>
 *   <li><b>显式失败回传</b>：Worker 读取异常 → {@link ScanEvent.ScanFailed} → 本算子 {@link #doNext()}
 *       收到即 fail-fast 抛 {@link ScanExecutionException}，杜绝静默残缺数据；</li>
 *   <li><b>TaskTracker（控制面）</b>：每分区一条 {@link ScanTask}（taskId / workerId / state / 进度），
 *       由事件流驱动状态迁移 SUBMITTED → RUNNING → FINISHED / FAILED；</li>
 *   <li><b>看门狗（失联兜底）</b>：任务生死由<b>控制面存活心跳</b>表达（Worker 扫描线程周期性上报，
 *       经 {@code query_telemetry} 落到 {@link #markAlive}），不再用「是否吐数据页」推断 —— 慢读
 *       （如 ORC 整条 stripe、无 SARG 的 like）合法沉默不再被当成死。本算子按周期扫描 tracker，
 *       超过 {@code DEFAULT_TASK_TIMEOUT_MS} 既无数据页、又无存活心跳的 RUNNING 任务判 LOST → fail-fast，
 *       覆盖「Worker 失联 / 任务线程死亡」；且即使 {@link ScanEvent.ScanFailed} 事件丢失也会兜住；</li>
 *   <li><b>反向取消</b>：失败 / 关闭时 {@link WorkerGateway#cancelTask} 中断仍在跑的分区任务，
 *       避免 Worker 线程阻塞在满队列泄漏。</li>
 * </ol>
 *
 * @author jiaxin
 * @version 1.1
 * @date 2026/8/11
 */
public class DistributedScanOperator extends AbstractMeasurableOperator implements Explainable {

    /** 任务级失联超时（ms）：RUNNING 任务超过此窗口既无数据页、又无存活心跳 → 判 LOST。-DDojaoTaskTimeoutMs 覆盖。 */
    private static final long DEFAULT_TASK_TIMEOUT_MS = Integer.getInteger("DojaoTaskTimeoutMs", 30_000);
    /** 看门狗扫描周期（ms）。-DDojaoWatchdogPeriodMs 覆盖。 */
    private static final long WATCHDOG_PERIOD_MS = Integer.getInteger("DojaoWatchdogPeriodMs", 1_000);

    private final ReadRequest baseRequest;
    private final int parallelism;
    /** 探针连接器：Master 侧本地做列裁剪 / 谓词下推 / 分区规划（不参与数据拉取）。 */
    private final DojaoConnector probeConn;
    /** Master → Worker 派发通道。 */
    private final WorkerGateway gateway;
    private final String catalog;
    /** 可派发的健康 Worker 列表（round-robin 轮转）。 */
    private final List<WorkerRegistration> workers;
    /** 所属查询标识（任务上下文用）。 */
    private final String jobId;

    /**
     * 片内 top-N 截断尾（可空，Planner 命中下推 gate 时在 doOpen 前设置）：非 null → 每分区任务
     * Worker 扫完切片后按 ORDER BY 键本地排序 + 截断到 offset+limit 行再回传候选集（Master 全局
     * Sort+Limit 复用，正确性见 {@link OrderByTail}）。随每个 {@code dispatchScan} 载荷下发。
     */
    private volatile FragmentSpec.SortLimitStep tail;

    /**
     * 顶层 LIMIT 的截断下推（行数上限，0 = 不下推）—— 只对「扫描行与最终输出一一对应」的单表
     * 直通链由 Planner 设置。Worker 每片读满这么多行即停（各片之和是结果的超集，Master 端
     * {@link LimitOperator} 照常按 offset/limit 裁剪）。切分由连接器规划完成后再逐片写入，
     * 不改动 {@code planParallelScan} 的分区决策。
     */
    private long fetchLimit;

    /** 列裁剪：只读取这些列（空 = 全部列）。 */
    private final List<String> selectedColumns = new ArrayList<>();
    /** 谓词下推：要交给数据源执行的过滤条件。 */
    private final List<String> predicates = new ArrayList<>();

    /**
     * Worker 回传事件队列（有界即背压，容量 {@value #EVENT_QUEUE_CAPACITY}）。
     *
     * <p>容量按「页」计，而单页最多约 1 万行 × 行宽 —— 旧值 2000 相当于允许把数 GB 结果缓存在
     * Master 堆外，且 Flight 接收路径不经过 {@code NodeMemory.throttle()}，多窗口分组/单机回退
     * 这类「全部数据回 Master」的查询会直接把传输层额度吃穿（线上表现为 Flight 线程抛
     * {@code OutOfMemoryError: Cannot reserve ... direct buffer memory}）。64 页在保持流水线
     * 深度的同时，把 Master 端滞留量限制在可预期范围；队列满时接收线程阻塞 ⇒ Worker 的
     * {@code putNext} 随之阻塞，形成端到端背压。</p>
     */
    private static final int EVENT_QUEUE_CAPACITY = 64;
    private final BlockingQueue<ScanEvent> eventQueue = new LinkedBlockingQueue<>(EVENT_QUEUE_CAPACITY);
    /** 控制面：taskId → 分区任务跟踪记录。 */
    private final ConcurrentHashMap<String, ScanTask> taskTracker = new ConcurrentHashMap<>();
    /** 已确认完成（ScanFinished）的分区计数，到齐 totalPartitions 则 EOF。 */
    private final AtomicInteger finishedCount = new AtomicInteger(0);
    /** 首个失败即登记（fail-fast 源），doNext 检查到即抛。 */
    private volatile ScanExecutionException failure;
    private volatile boolean eofReturned;
    private volatile int totalPartitions;
    private volatile ScheduledExecutorService watchdog;

    /**
     * @param baseRequest 原始读请求（未列裁剪 / 未下推）
     * @param parallelism 期望并行分区数（交给连接器分区规划）
     * @param probeConn   探针连接器（仅规划用）
     * @param gateway     Master → Worker 派发通道
     * @param catalog     数据源 catalog
     * @param workers     健康 Worker 列表（空 → doOpen 抛异常，调用方保证非空）
     */
    public DistributedScanOperator(ReadRequest baseRequest, int parallelism,
                                   DojaoConnector probeConn, WorkerGateway gateway,
                                   String catalog, List<WorkerRegistration> workers) {
        super("DistributedScan", "DistributedScan", Math.max(1, parallelism));
        this.baseRequest = baseRequest;
        this.parallelism = Math.max(1, parallelism);
        this.probeConn = probeConn;
        this.gateway = gateway;
        this.catalog = catalog;
        this.workers = List.copyOf(workers);
        this.jobId = "query-" + Long.toHexString(System.nanoTime());
    }

    /** EXPLAIN 明细：真实扫描的表、切片并行度、列裁剪与谓词下推内容。 */
    @Override
    public List<String> explainLines() {
        List<String> out = new ArrayList<>();
        String filter = predicates.isEmpty() ? "" : " filter=[" + String.join(" AND ", predicates) + "]";
        out.add("scan " + baseRequest.catalog() + "." + baseRequest.schema() + "." + baseRequest.table()
                + " partitions=" + parallelism
                + (selectedColumns.isEmpty() ? " columns=<all>" : " columns=" + selectedColumns)
                + filter);
        return out;
    }

    // ==================== 列裁剪 & 谓词下推 ====================

    /** 只读取指定列（可多次调用，追加）。 */
    public DistributedScanOperator selectColumns(String... columns) {
        if (columns != null) Collections.addAll(this.selectedColumns, columns);
        return this;
    }

    /** 只读取指定列（替换已有列清单）。列裁剪优化使用。 */
    public DistributedScanOperator setColumns(List<String> columns) {
        this.selectedColumns.clear();
        if (columns != null) this.selectedColumns.addAll(columns);
        return this;
    }

    /**
     * 登记本次扫描<b>必须应用的谓词集合</b>（可多次调用，以 AND 合入）—— 语义是「这条扫描要执行的
     * WHERE 全集」，而<b>不是</b>「保证数据源会执行」。落地方式由连接器能力决定（写进派发载荷的
     * {@code filterExpression}）：兑现下推的由数据源在扫描时过滤；不兑现的由引擎在扫描节点补
     * {@code FilterOperator} 求值（见 {@link ConnectorScanOperator#withEngineFilter}）。两者都不做才是错的。
     */
    public DistributedScanOperator pushdown(String... predicates) {
        if (predicates != null) Collections.addAll(this.predicates, predicates);
        return this;
    }

    /**
     * 登记本次扫描<b>必须应用的谓词集合</b>（可多次调用，以 AND 合入）—— 语义是「这条扫描要执行的
     * WHERE 全集」，而<b>不是</b>「保证数据源会执行」。落地方式由连接器能力决定（写进派发载荷的
     * {@code filterExpression}）：兑现下推的由数据源在扫描时过滤；不兑现的由引擎在扫描节点补
     * {@code FilterOperator} 求值（见 {@link ConnectorScanOperator#withEngineFilter}）。两者都不做才是错的。
     */
    public DistributedScanOperator pushdown(List<String> predicates) {
        if (predicates != null) this.predicates.addAll(predicates);
        return this;
    }

    /**
     * 设置片内 top-N 截断尾（Planner 命中下推 gate 时在 doOpen 前调用；null = 不下推）。
     * Worker 每扫一个切片即本地排序 + 截断到 offset+limit 行再回传。
     */
    public void setTail(FragmentSpec.SortLimitStep t) {
        this.tail = t;
    }

    /**
     * 截断下推：顶层 LIMIT 的「够用行数」（= offset + limit，0 = 不下推）。见 {@link #fetchLimit}。
     * 必须在 {@link #open()} 前调用（切片请求在 doOpen 里派发）。
     */
    public void setFetchLimit(long rows) {
        this.fetchLimit = Math.max(0, rows);
    }

    // ==================== 只读访问（分布式算子的基表探测用） ====================

    /** 原始读请求（未列裁剪 / 未下推）。 */
    public ReadRequest baseRequest() { return baseRequest; }

    /** 已裁剪的列清单（空 = 全部列）。 */
    public List<String> selectedColumns() { return List.copyOf(selectedColumns); }

    /** 已下推的过滤条件（空 = 无）。 */
    public List<String> predicates() { return List.copyOf(predicates); }

    /**
     * 这些谓词是否由<b>引擎</b>执行（而非数据源）—— 连接器不兑现下推契约时为 true，此时 Worker
     * 扫描节点会补一层 {@code FilterOperator}（见 {@link ConnectorScanOperator#withEngineFilter}），
     * 于是谓词引用的列<b>必须</b>留在读取列清单里（列裁剪据此保留，见 Planner.pruneScanColumns）。
     */
    public boolean engineFilter() {
        return probeConn != null && !probeConn.supportsPredicatePushdown();
    }

    // ==================== 生命周期 ====================

    @Override
    protected void doOpen() throws Exception {
        String schema = baseRequest.schema();
        String table = baseRequest.table();
        if (workers.isEmpty()) {
            throw new IllegalStateException("DistributedScan 无可派发的 Worker");
        }

        // ---- 0. 列裁剪 + 谓词下推：探针连接器把列清单与过滤条件写进请求 ----
        ReadRequest effective = baseRequest;
        if (!selectedColumns.isEmpty()) {
            effective = probeConn.pruneColumns(effective, selectedColumns);
        }
        for (String p : predicates) {
            if (p == null || p.isBlank()) continue;
            effective = probeConn.pushdownPredicate(effective, p);
        }
        System.err.printf("[DistributedScan] probing %s.%s for partition strategy...%n", schema, table);

        // ---- 1. 切片策略完全交给连接器（同 ParallelTableScan；fetchLimit = 截断下推提示） ----
        List<ScanSlice> partitions = probeConn.planSplits(effective, parallelism, fetchLimit);
        if (partitions.isEmpty()) {
            System.err.println("[DistributedScan] no partition strategy, using single slice");
            partitions = List.of(ScanSlice.whole(effective));
        } else {
            System.err.printf("[DistributedScan] planned %d parallel slices%n", partitions.size());
        }
        totalPartitions = partitions.size();
        finishedCount.set(0);
        failure = null;
        eofReturned = false;
        taskTracker.clear();

        // ---- 2. 按 worker 数 round-robin 派发（分区数 > worker 数时轮转） ----
        System.err.printf("[DistributedScan] dispatching %d partitions across %d workers%n",
                totalPartitions, workers.size());
        for (int i = 0; i < totalPartitions; i++) {
            if (failure != null) break;   // 已有分区派发失败，停止继续派发，doNext 将抛错
            WorkerRegistration w = workers.get(i % workers.size());
            String taskId = jobId + "-p" + i;
            ScanTask task = new ScanTask(taskId, w.info().workerId(), jobId);
            taskTracker.put(taskId, task);
            try {
                gateway.dispatchScan(w.info().workerId(), jobId, taskId, catalog,
                        applyFetchLimit(partitions.get(i)), tail, eventQueue);
                task.state = TaskState.RUNNING;
                task.lastProgressNanos = System.nanoTime();
            } catch (Throwable ex) {
                // 派发即失败（如 Worker 已停止）：立即登记失败，避免 doNext 干等
                task.state = TaskState.FAILED;
                task.cause = String.valueOf(ex);
                task.finishNanos = System.nanoTime();
                System.err.printf("[DistributedScan] dispatch task %s failed: %s%n", taskId, ex);
                recordFailure(taskId, String.valueOf(ex));
            }
        }
        startWatchdog();
        System.err.printf("[DistributedScan] %d scan tasks dispatched, open() complete%n", taskTracker.size());
    }

    @Override
    protected Page doNext() throws Exception {
        while (true) {
            if (eofReturned) return Page.eof();
            ScanExecutionException f = failure;
            if (f != null) throw f;   // fail-fast：任一任务失败 / 超时判 LOST

            // 所有分区已确认完成：排空队列中残留的数据页
            if (finishedCount.get() >= totalPartitions) {
                ScanEvent ev = eventQueue.poll();
                if (ev == null || ev instanceof ScanEvent.ScanFinished) {
                    eofReturned = true;
                    return Page.eof();
                }
                if (ev instanceof ScanEvent.ScanPage sp) {
                    return acceptPage(sp);
                }
                if (ev instanceof ScanEvent.ScanFailed sf) {
                    recordFailure(sf.taskId(), sf.cause());
                    throw failure;
                }
                continue;
            }

            // 正常消费：Worker 回传的事件（数据 / 完成 / 失败）
            ScanEvent ev = eventQueue.poll(100, TimeUnit.MILLISECONDS);
            if (ev == null) continue;
            if (ev instanceof ScanEvent.ScanPage sp) {
                return acceptPage(sp);
            }
            if (ev instanceof ScanEvent.ScanFinished sf) {
                ScanTask t = taskTracker.get(sf.taskId());
                if (t != null) {
                    t.state = TaskState.FINISHED;
                    t.finishNanos = System.nanoTime();
                }
                if (finishedCount.incrementAndGet() >= totalPartitions) {
                    // 最后一个分区刚结束，检查队列是否有残余数据页
                    ScanEvent remaining = eventQueue.poll();
                    if (remaining instanceof ScanEvent.ScanPage sp) {
                        return acceptPage(sp);
                    }
                    eofReturned = true;
                    return Page.eof();
                }
                continue;
            }
            if (ev instanceof ScanEvent.ScanFailed sf) {
                ScanTask t = taskTracker.get(sf.taskId());
                if (t != null) {
                    t.state = TaskState.FAILED;
                    t.cause = sf.cause();
                    t.finishNanos = System.nanoTime();
                }
                System.err.printf("[DistributedScan] task %s failed on worker: %s%n", sf.taskId(), sf.cause());
                recordFailure(sf.taskId(), sf.cause());
                throw failure;   // fail-fast
            }
        }
    }

    /** 收一页扫描数据：更新分区进度（喂看门狗）+ 累计该任务行数 + 记录输入，页所有权交 Master。 */
    private Page acceptPage(ScanEvent.ScanPage sp) {
        ScanTask t = taskTracker.get(sp.taskId());
        if (t != null) {
            t.lastProgressNanos = System.nanoTime();
            t.rows.addAndGet(sp.page().getRowCount());
        }
        recordInput(sp.page());
        return sp.page();
    }

    /**
     * 把截断下推写进切片请求：{@code LIMIT fetchLimit OFFSET 0}。只在本片请求本身没有分页语义时
     * 生效（{@code offset==0 && limit==0}）—— 连接器自己生成的 LIMIT/OFFSET 切片不能被覆盖，
     * 否则各片读到的是同一段数据的副本。offset 一律给 0：本片返回 {@code [0, offset+limit)}
     * 超集，跳过 offset 的工作由 Master 端 {@link LimitOperator} 照常完成。
     */
    private ScanSlice applyFetchLimit(ScanSlice slice) {
        ReadRequest part = slice.request();
        if (fetchLimit <= 0 || part.offset() > 0 || part.limit() > 0) return slice;
        return slice.withRequest(part.withLimit(0L, fetchLimit));
    }

    @Override
    protected void doClose() throws Exception {
        if (watchdog != null) {
            watchdog.shutdownNow();
            watchdog = null;
        }
        // 取消仍在跑的分区任务（Worker 侧停止 read，避免线程阻塞在满队列泄漏）
        for (ScanTask t : taskTracker.values()) {
            if (t.state == TaskState.SUBMITTED || t.state == TaskState.RUNNING) {
                try {
                    gateway.cancelTask(t.workerId, t.taskId);
                } catch (Throwable ignored) {
                }
            }
        }
        // 排空事件队列并关闭残余数据页（对齐 AbstractDistributedOperator.doClose）—— 直接 clear()
        // 会丢弃排队中的 ScanPage 却不 close()，其 Arrow 缓冲随之泄漏（实测 32MB）。
        ScanEvent ev;
        while ((ev = eventQueue.poll()) != null) {
            if (ev instanceof ScanEvent.ScanPage sp) {
                try { sp.page().close(); } catch (Throwable ignored) { }
            }
        }
        taskTracker.clear();
    }

    // ==================== 控制面：看门狗 & 失败传播 ====================

    private void startWatchdog() {
        watchdog = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "distributed-scan-watchdog-" + jobId);
            t.setDaemon(true);
            return t;
        });
        watchdog.scheduleWithFixedDelay(this::watchdogTick,
                WATCHDOG_PERIOD_MS, WATCHDOG_PERIOD_MS, TimeUnit.MILLISECONDS);
    }

    /** 看门狗：周期扫描控制面，RUNNING 任务超时既无数据页、又无存活心跳 → 判 LOST → fail-fast（失联兜底）。 */
    private void watchdogTick() {
        if (failure != null) return;   // 已失败，不再处理
        long now = System.nanoTime();
        for (ScanTask t : taskTracker.values()) {
            TaskState s = t.state;
            if ((s == TaskState.SUBMITTED || s == TaskState.RUNNING)
                    && now - t.lastProgressNanos > DEFAULT_TASK_TIMEOUT_MS * 1_000_000L) {
                t.state = TaskState.LOST;
                t.cause = "no page or liveness heartbeat for " + DEFAULT_TASK_TIMEOUT_MS + "ms (task timeout)";
                t.finishNanos = now;
                System.err.printf("[DistributedScan] task %s on worker %s judged LOST: %s%n",
                        t.taskId, t.workerId, t.cause);
                recordFailure(t.taskId, t.cause);
            }
        }
    }

    /** 登记首个失败并取消仍在跑的分区任务（尽早止血），由 doNext 检查后抛错。 */
    private void recordFailure(String taskId, String cause) {
        if (failure == null) {
            failure = new ScanExecutionException("scan task " + taskId + " failed: " + cause);
        }
        for (ScanTask t : taskTracker.values()) {
            if (t.state == TaskState.SUBMITTED || t.state == TaskState.RUNNING) {
                try {
                    gateway.cancelTask(t.workerId, t.taskId);
                } catch (Throwable ignored) {
                }
            }
        }
    }

    /**
     * 存活心跳：Worker 扫描线程周期上报（经 {@code query_telemetry} → {@code QueryManager} 路由到本算子）。
     * 命中已知 taskId 即返回 true（该帧被本算子消费，不再当作片段遥测写入 rows/bytes 覆盖表）；仅当任务
     * 仍在跑（SUBMITTED / RUNNING）才刷新 {@link ScanTask#lastProgressNanos} —— FINISHED / FAILED / LOST
     * 只吞不刷新，防迟到帧把已结束任务当成活着。
     *
     * <p>这是「任务生死 = 控制面信号」的落点：看门狗不再靠「是否吐数据页」推断生死，慢读（ORC 整条
     * stripe）合法沉默不再被误杀；真正失联（Worker 死 / 线程亡）则心跳随之中断 → 判 LOST。</p>
     */
    public boolean markAlive(String taskId) {
        if (taskId == null) {
            return false;
        }
        ScanTask t = taskTracker.get(taskId);
        if (t == null) {
            return false;
        }
        if (t.state == TaskState.SUBMITTED || t.state == TaskState.RUNNING) {
            t.lastProgressNanos = System.nanoTime();
        }
        return true;
    }

    // ==================== 控制面：任务记录 ====================

    /** 分区任务状态机。 */
    enum TaskState {SUBMITTED, RUNNING, FINISHED, FAILED, LOST}

    /** 单个分区任务的控制面记录（可变：事件流驱动状态迁移）。 */
    private static final class ScanTask {
        final String taskId;
        final String workerId;
        final String jobId;
        final long dispatchNanos;
        /** 本分区已回传数据行数（监控：每分区处理进度）。 */
        final java.util.concurrent.atomic.AtomicLong rows = new java.util.concurrent.atomic.AtomicLong();
        volatile TaskState state = TaskState.SUBMITTED;
        /** 最后一次「接触」时刻：由 {@link #acceptPage}（数据页到达）与 {@link #markAlive}（存活心跳）共同刷新。 */
        volatile long lastProgressNanos;
        volatile long finishNanos;
        volatile String cause;

        ScanTask(String taskId, String workerId, String jobId) {
            this.taskId = taskId;
            this.workerId = workerId;
            this.jobId = jobId;
            this.dispatchNanos = System.nanoTime();
            this.lastProgressNanos = dispatchNanos;
        }
    }

    /** 扫描失败异常：携带失败 taskId 与原因，沿算子链传播到 QueryService 返回错误。 */
    public static class ScanExecutionException extends RuntimeException {
        public ScanExecutionException(String message) {
            super(message);
        }
    }

    // ==================== Getter ====================

    /**
     * 本次扫描派发使用的 jobId（= 查询执行 id）。用于把本算子的存活心跳接进
     * {@code QueryManager} 的遥测路由 —— 该算子继承 {@code AbstractMeasurableOperator}
     * 而非 {@code AbstractDistributedOperator}，{@code queryId()} 取不到，须由本方法暴露。
     */
    public String jobId() {
        return jobId;
    }

    /** 已规划的分区数（doOpen 后有效）。 */
    public int totalPartitions() {
        return totalPartitions;
    }

    /** 参与派发的 Worker 数。 */
    public int workerCount() {
        return workers.size();
    }

    /**
     * 全部已派发分区扫描任务的只读快照（按派发先后排序，状态可能含 LOST）。
     *
     * <p>扫描任务自建 {@link ScanTask} 控制面（本算子独立于 {@link AbstractDistributedOperator}），
     * 这里转成与片段任务同构的 {@link DistributedTask} DTO，供监控统一消费：kind 恒为 "Scan"、
     * producer 恒为 false（扫描分区把数据页回传 Master，语义同终端消费者）。</p>
     */
    public List<DistributedTask> scanTasks() {
        List<DistributedTask> out = new ArrayList<>(taskTracker.size());
        for (ScanTask t : taskTracker.values()) {
            String st = switch (t.state) {
                case FINISHED -> DistributedTask.FINISHED;
                case FAILED -> DistributedTask.FAILED;
                case LOST -> DistributedTask.LOST;
                case SUBMITTED, RUNNING -> DistributedTask.RUNNING;
            };
            out.add(new DistributedTask(t.taskId, t.workerId, "Scan", false, true,
                    st, t.rows.get(), t.dispatchNanos, t.finishNanos, t.cause, null));
        }
        out.sort((a, b) -> Long.compare(a.dispatchNanos(), b.dispatchNanos()));
        return out;
    }
}
