package com.dojao.server.worker;

import com.dojao.core.config.ResourceConfig;
import com.dojao.core.runtime.worker.ResourceSummary;
import com.dojao.core.runtime.worker.WorkerNode;
import com.dojao.core.runtime.worker.WorkerState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * WorkerManager —— 一个 Worker 节点一个实例，向 Master 注册并保持心跳，执行 Master 分配的任务。
 *
 * <p>职责：
 * <ul>
 *   <li><b>接受 Master 分配的任务</b>：{@link #submitTask} / {@link #submitLeafTask} 进入本地固定
 *       并行线程池执行；</li>
 *   <li><b>管理自己的状态</b>：INITIALIZING → REGISTERED → IDLE/BUSY（按运行任务数即时切换），
 *       注册 / 心跳失败 → FAILED，stop → STOPPED（终态）；</li>
 *   <li><b>心跳保活</b>：定时向 Master 心跳证明存活；</li>
 *   <li><b>资源摘要定时汇报</b>：心跳携带 {@link ResourceSummary}（CPU / 内存 / 磁盘），
 *       供 Master 按节点情况分配任务；</li>
 *   <li><b>池看门狗 / 僵尸回收</b>：{@link #watchdogTick()} 把「池满 + 有排队 + 长时间零推进」这类
 *       <b>整段阻塞型死锁</b>从「假死」变成明确失败；{@link #cancelTasksOfJob} 供 Master 驱动的
 *       残留（僵尸）片段回收；</li>
 *   <li><b>生命周期管理</b>：{@link #start()} 注册并启动心跳，{@link #stop()} 优雅停止并注销。</li>
 * </ul>
 *
 * @author jiaxin
 * @version 1.1
 * @date 2026/8/11
 */
public final class WorkerManager {

    private static final Logger log = LoggerFactory.getLogger(WorkerManager.class);

    /** 默认心跳间隔：-DDojaoHeartbeatIntervalMs 覆盖。 */
    private static final int DEFAULT_HEARTBEAT_MS = Integer.getInteger("DojaoHeartbeatIntervalMs", 5000);

    /** 池看门狗巡检间隔：-DDojaoPoolWatchdogMs 覆盖（默认 5s）。 */
    private static final long WATCHDOG_INTERVAL_MS =
            Math.max(1000L, Long.getLong("DojaoPoolWatchdogMs", 5_000L));

    /**
     * 池死锁判定阈值：主池线程被占满、且还有片段在排队、且<b>完全没有推进</b>持续超过该毫秒数
     * → 判定死锁（-DDojaoPoolDeadlockMs 覆盖，默认 5 分钟）。
     *
     * <p>阈值取得大是刻意的：这套算子链里「池满」本身是合法的（8 个片段跑在 8 个槽位上），
     * 「排队」也是合法的（后续片段等空闲槽位）。只有<b>三者同时成立且长时间零推进</b>才是死锁 ——
     * 而「零推进」的判据是「一页 shuffle 都没进来、也没有任何任务开始/结束」，正常大表聚合
     * （Phase 1 建表 / 排序期间的合法沉默）仍在持续收页，不会被误判。</p>
     */
    private static final long POOL_DEADLOCK_MS =
            Math.max(10_000L, Long.getLong("DojaoPoolDeadlockMs", 300_000L));

    private final WorkerNode info;
    private final MasterGateway gateway;
    private final long heartbeatIntervalMs;
    /** 资源采集器：null → 采集真实机器资源；测试/模拟注入固定资源。 */
    private final Supplier<ResourceSummary> resourceProvider;
    /** 本 Worker 的交换注册表（Worker↔Worker shuffle 的接收端登记 / 发送通道）。 */
    private final ExchangeManager exchangeManager;

    /** 池容量（两个池同尺寸）。 */
    private final int poolSize;

    /** 本地任务执行池：最多 maxConcurrentTasks 个任务并行，其余排队（消费者 / 中间层片段）。 */
    private final ThreadPoolExecutor taskExecutor;
    /**
     * <b>叶子任务专线</b>（Producer 片段 / 独立 Scan 任务）—— 与 {@link #taskExecutor} 同尺寸，
     * 但<b>完全独立</b>，保证「生产者永不排在阻塞着的消费者后面」。
     *
     * <p><b>为什么必须分池</b>：本引擎的每个分布式片段都<b>整段占用</b>一个线程 —— 消费者片段阻塞在
     * 等上游 shuffle 收齐（{@code ShuffleReader} / {@code PartitionedSourceOperator} 无超时地轮询
     * {@code partitionEnd}），生产者片段阻塞在 {@code HashPartitionOperator.awaitDistributedDone()}，
     * 中间层 join 阻塞在 Phase 1（两侧排序全部耗到 EOF）。这些等待都<b>不会自己让出线程</b>，而下游
     * 收齐又<b>必须</b>上游先把数据推完 ⇒ 放进同一个池就是死锁：消费者先占满槽位、生产者永远排不到
     * 线程，下游的 {@code partitionEnd} 也永远等不来。单池下只要「某台 Worker 的片段数 ≥ 池容量」
     * 就会触发，且往往是<b>零余量</b>——本查询最忙的一台恰为 3 个聚合消费者 + 1 个中间层 join +
     * 1 个 Hive 生产者 + 3 个 MySQL 生产者 = 8 = 池容量：再来一个残留片段（上一次查询未回收）就必炸。
     * 更糟的是被阻塞片段仍在 RUNNING、遥测心跳照发 ⇒ Master 看门狗永不触发 ⇒ 用户看到的是
     * <b>假死</b>而不是报错。分池后生产者的推进不再依赖空闲槽位，这条死锁链被切断。</p>
     *
     * <p><b>代价</b>：单 Worker 并发片段上限从 {@code parallelism} 变为 {@code 2 × parallelism}
     * （消费者一池 + 叶子一池）。两池各自<b>有界</b>，池满照旧排队，只是不同角色不再互相拖死。</p>
     */
    private final ThreadPoolExecutor leafExecutor;
    /** 心跳调度线程。 */
    private final ScheduledExecutorService scheduler;
    /**
     * 池看门狗调度线程（与心跳分开：心跳要走网络，网络卡住不能连带看门狗一起哑）。
     * 巡检 {@link #watchdogTick()}，见 {@link #POOL_DEADLOCK_MS}。
     */
    private final ScheduledExecutorService watchdog;
    /**
     * 异步失败回传线程池（缓存型，不拒绝）：看门狗判死排队片段时要往 Master 发
     * {@code FragmentFailed}（网络调用，可能阻塞在 gRPC 流控上）—— 绝不能占住看门狗线程，
     * 否则后续巡检全部停摆。
     */
    private final ExecutorService abortExecutor;

    /** 任务槽：taskId → 运行态（提交即建；完成 / 失败 / 取消即摘除）。 */
    private final Map<String, TaskSlot> taskSlots = new ConcurrentHashMap<>();
    /** 运行中任务的 Future：taskId → Future（支持 {@link #cancelTask} 反向取消）。 */
    private final ConcurrentHashMap<String, Future<?>> taskFutures = new ConcurrentHashMap<>();
    /**
     * 失败任务记录：taskId → 失败信息。只用于回传 Master 排障，不需要永久保留 ——
     * 失败任务一旦结束就再无人摘除，长跑 Worker 会随失败次数单调增长，故按 FIFO 保留最近
     * {@link #MAX_TASK_FAILURES} 条（默认 256，可 {@code -DDojaoWorkerTaskFailures} 覆盖）。
     */
    private static final int MAX_TASK_FAILURES =
            Math.max(1, Integer.getInteger("DojaoWorkerTaskFailures", 256));
    private final Map<String, String> taskFailures = Collections.synchronizedMap(
            new LinkedHashMap<>(16, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                    return size() > MAX_TASK_FAILURES;
                }
            });

    /**
     * 任务级进度计数：提交 / 开始 / 结束各 +1。与 {@link ExchangeManager#totalPagesReceived()}
     * 一起构成本节点「还在动」的证据（{@link #watchdogTick()} 读它）。
     */
    private final AtomicLong progressSeq = new AtomicLong();

    // ---- 看门狗停滞窗口状态（仅看门狗线程读写）----
    private long lastPagesSeen;
    private long lastSeqSeen;
    private long stalledSinceNanos;
    private boolean escalateArmed;

    private volatile WorkerState state = WorkerState.INITIALIZING;
    private volatile ResourceSummary lastResource;
    private volatile long lastHeartbeatNanos;

    public WorkerManager(WorkerNode info, MasterGateway gateway) {
        this(info, gateway, DEFAULT_HEARTBEAT_MS, Math.max(1, Runtime.getRuntime().availableProcessors()), null);
    }

    public WorkerManager(WorkerNode info, MasterGateway gateway, long heartbeatIntervalMs, int maxConcurrentTasks) {
        this(info, gateway, heartbeatIntervalMs, maxConcurrentTasks, null);
    }

    /**
     * 允许注入资源采集器（伪分布式模拟用：给节点设定固定 CPU / 内存）。
     * {@code resourceProvider} 为 null 时回退采集真实机器资源。
     */
    public WorkerManager(WorkerNode info, MasterGateway gateway, long heartbeatIntervalMs, int maxConcurrentTasks,
                         Supplier<ResourceSummary> resourceProvider) {
        this(info, gateway, heartbeatIntervalMs, maxConcurrentTasks, resourceProvider, null);
    }

    /**
     * 允许注入交换注册表（伪分布式模拟用：多个 Worker 共享同一 bus 实现 Worker↔Worker 直连）。
     * {@code exchangeManager} 为 null 时创建独立注册表（生产：每 Worker 进程一个）。
     */
    public WorkerManager(WorkerNode info, MasterGateway gateway, long heartbeatIntervalMs, int maxConcurrentTasks,
                         Supplier<ResourceSummary> resourceProvider, ExchangeManager exchangeManager) {
        this.info = info;
        this.gateway = gateway;
        this.heartbeatIntervalMs = Math.max(100L, heartbeatIntervalMs);
        this.resourceProvider = resourceProvider;
        this.exchangeManager = exchangeManager != null ? exchangeManager : new ExchangeManager(info.workerId());
        String shortId = info.workerId().substring(0, Math.min(8, info.workerId().length()));
        // 任务池容量：显式传正值优先；否则（0 / 负数）取配置文件 system.worker.parallelism
        this.poolSize = maxConcurrentTasks > 0 ? maxConcurrentTasks : ResourceConfig.workerParallelism();
        this.taskExecutor = newPool(poolSize, "dojao-worker-task-" + shortId);
        this.leafExecutor = newPool(poolSize, "dojao-worker-leaf-" + shortId);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "dojao-worker-heartbeat-" + shortId);
            t.setDaemon(true);
            return t;
        });
        this.watchdog = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "dojao-worker-pool-watchdog-" + shortId);
            t.setDaemon(true);
            return t;
        });
        this.abortExecutor = Executors.newCachedThreadPool(
                new NamedThreadFactory("dojao-worker-abort-" + shortId));
    }

    // ==================== 生命周期 ====================

    /** 启动 Worker：向 Master 注册，成功后启动定时心跳。 */
    public void start() {
        boolean ok = gateway.register(info);
        if (!ok) {
            this.state = WorkerState.FAILED;
            log.error("Worker {} 注册被 Master 拒绝，启动失败", info.workerId());
            return;
        }
        this.state = WorkerState.REGISTERED;
        log.info("Worker {} 注册成功: {}:{}", info.workerId(), info.host(), info.port());
        scheduler.scheduleWithFixedDelay(this::heartbeat,
                heartbeatIntervalMs, heartbeatIntervalMs, TimeUnit.MILLISECONDS);
        watchdog.scheduleWithFixedDelay(this::watchdogTick,
                WATCHDOG_INTERVAL_MS, WATCHDOG_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    /** 优雅停止：置 STOPPED、停心跳、停止接收新任务、向 Master 注销。 */
    public void stop() {
        if (state == WorkerState.STOPPED) {
            return;
        }
        this.state = WorkerState.STOPPED;
        scheduler.shutdownNow();
        watchdog.shutdownNow();
        abortExecutor.shutdownNow();
        taskExecutor.shutdown();   // 让已排队任务跑完（骨架阶段不强制打断）
        leafExecutor.shutdown();
        try {
            gateway.unregister(info);
            log.info("Worker {} 已注销并停止", info.workerId());
        } catch (Throwable t) {
            log.warn("Worker {} 注销失败（Master 将在心跳超时后剔除）", info.workerId(), t);
        }
        // 交换层共享分配器随 Worker 停止而关闭（此时所有交接页早已 close）
        try {
            exchangeManager.close();
        } catch (Throwable ignored) {
        }
    }

    // ==================== 任务 ====================

    /** 接收 Master 分配的任务并执行（本地并行池，超出并行度排队）。 */
    public void submitTask(WorkerTask task, WorkerTaskExecution execution) {
        submit(task, execution, taskExecutor, "task", null);
    }

    /**
     * 带「未及执行即判死」回传钩子的提交（消费者 / 独立 Scan：有 Master 侧结果汇，失败能回传）。
     *
     * @param aborter 任务被看门狗判死在<b>尚未抢到线程</b>阶段时调用（入参 = 判死原因文案）：把失败经
     *                结果链路回传 Master（{@code FragmentFailed} / {@code ScanFailed}），让 Master
     *                fail-fast 而不是干等
     */
    public void submitTask(WorkerTask task, WorkerTaskExecution execution, Consumer<String> aborter) {
        submit(task, execution, taskExecutor, "task", aborter);
    }

    /**
     * 接收 <b>叶子任务</b>（Producer 片段 / 独立 Scan）并丢进 {@link #leafExecutor} 执行。
     *
     * <p>与 {@link #submitTask} 的唯一区别是池：叶子任务必须能「无论如何都跑起来」—— 下游消费者的
     * 推进完全依赖它把数据推完，若它与消费者共用一池、排在阻塞中的消费者后面，整条查询就死锁
     * （详见 {@link #leafExecutor} 的说明）。任务表 / Future 表两池共享，故 {@link #cancelTask}
     * 对两池一视同仁。</p>
     */
    public void submitLeafTask(WorkerTask task, WorkerTaskExecution execution) {
        submit(task, execution, leafExecutor, "leaf", null);
    }

    /** 叶子任务 + 判死回传钩子（独立 Scan 有结果汇，见 {@link #submitTask(WorkerTask, WorkerTaskExecution, Runnable)}）。 */
    public void submitLeafTask(WorkerTask task, WorkerTaskExecution execution, Consumer<String> aborter) {
        submit(task, execution, leafExecutor, "leaf", aborter);
    }

    private void submit(WorkerTask task, WorkerTaskExecution execution, ThreadPoolExecutor pool,
                        String poolName, Consumer<String> aborter) {
        WorkerState s = state;
        if (s == WorkerState.STOPPED || s == WorkerState.FAILED) {
            throw new IllegalStateException("Worker 不可接收任务，当前状态: " + s);
        }
        TaskSlot slot = new TaskSlot(task, poolName, aborter);
        taskSlots.put(task.taskId(), slot);
        progressSeq.incrementAndGet();
        if (state == WorkerState.REGISTERED || state == WorkerState.IDLE) {
            state = WorkerState.BUSY;   // 即时切忙，不等下一个心跳
        }
        Future<?> future = pool.submit(() -> {
            slot.startNanos = System.nanoTime();   // 抢到线程（此后不再受看门狗「未及执行」判死）
            progressSeq.incrementAndGet();
            try {
                execution.execute(task, this);
                log.info("Worker {} 完成任务 taskId={}", info.workerId(), task.taskId());
            } catch (Throwable t) {
                taskFailures.put(task.taskId(), t.toString());
                log.error("Worker {} 任务失败 taskId={}", info.workerId(), task.taskId(), t);
            } finally {
                finishTask(task.taskId());
            }
        });
        taskFutures.put(task.taskId(), future);
    }

    /**
     * 取消一个任务：中断 Worker 侧执行线程。
     *
     * <p>被取消的任务直接退出、不再发终止事件（调用方已放弃该分区）。任务线程若阻塞在
     * 有界队列回传，中断会使其立即退出，避免线程泄漏。</p>
     *
     * <p><b>排队中的任务也算已取消</b>：{@code Future.cancel(true)} 对尚未启动的任务只是把它从池队列
     * 里摘掉（不打断任何线程），此时任务体永远不会执行 ⇒ 它的 {@code finally} 不会跑 ⇒ 若不在
     * {@link #cancelTask} 里主动摘槽，{@code taskSlots} 会留下永久条目（Worker 永远显示 BUSY、
     * 心跳一直报忙）。这正是「僵尸片段」在 Worker 侧留下的痕迹之一，故此处统一收尾。</p>
     */
    public void cancelTask(String taskId) {
        Future<?> future = taskFutures.remove(taskId);
        if (future != null) {
            future.cancel(true);
            log.info("Worker {} 取消任务 taskId={}", info.workerId(), taskId);
        }
        finishTask(taskId);
    }

    /**
     * 取消某查询（jobId）在本 Worker 上的全部片段 —— <b>僵尸（残留）片段回收</b>的落地动作。
     *
     * <p>由 Master 驱动的判定触发（遥测回信 {@code stale_query_ids}，见
     * {@code NetworkMasterGateway#telemetry}）：Master 已经不认这个 jobId（查询已结束 / 已超时剔除 /
     * Master 重启过），而本 Worker 还挂着它的片段 —— 这些片段正占着池槽位，会拖垮后续查询（典型表现：
     * 上一查询的残留消费者把主池占满 → 新查询的片段永久排队 → 假死）。</p>
     *
     * @return 实际取消的片段数（0 = 本机没有该 jobId 的片段）
     */
    public int cancelTasksOfJob(String jobId) {
        if (jobId == null) {
            return 0;
        }
        List<String> ids = new ArrayList<>();
        for (TaskSlot slot : taskSlots.values()) {
            if (jobId.equals(slot.task.jobId())) {
                ids.add(slot.task.taskId());
            }
        }
        for (String id : ids) {
            cancelTask(id);
        }
        if (!ids.isEmpty()) {
            log.warn("Worker {} 回收残留片段：jobId={} 共 {} 个任务已取消（Master 已不认该查询）",
                    info.workerId(), jobId, ids.size());
        }
        return ids.size();
    }

    /**
     * 该任务当前是否仍在本 Worker 的任务表里（含「已提交、但还在池队列里排队」）。
     *
     * <p>存活心跳据此判定「这个扫描任务是否还该继续上报」：任务一旦结束 / 失败 / 取消，
     * {@link #finishTask} 即摘槽 → 返回 false → 心跳采样器上报最后一帧 {@code done=true} 后注销。
     * 与只看「执行线程是否已启动」不同，排队中的任务也返回 true —— 扫描任务排在叶子池队列里
     * 一动不动时，Master 的 30s 看门狗仍然靠它续命，不会被误判 LOST。</p>
     */
    public boolean isTaskActive(String taskId) {
        return taskId != null && taskSlots.containsKey(taskId);
    }

    /** 本 Worker 当前有片段在跑的全部 jobId（遥测上抛「在跑什么」用）。 */
    public Set<String> runningJobIds() {
        Set<String> out = new LinkedHashSet<>();
        for (TaskSlot slot : taskSlots.values()) {
            out.add(slot.task.jobId());
        }
        return out;
    }

    /** 任务的统一收尾（正常结束 / 失败 / 取消三路共用；幂等）。 */
    private void finishTask(String taskId) {
        TaskSlot slot = taskSlots.remove(taskId);
        taskFutures.remove(taskId);
        progressSeq.incrementAndGet();
        if (slot != null) {
            slot.aborter = null;   // 已收尾：判死钩子作废（防看门狗拿旧槽位重复回传失败）
        }
        if (taskSlots.isEmpty() && state == WorkerState.BUSY) {
            state = WorkerState.IDLE;   // 即时回空闲
        }
    }

    // ==================== 池看门狗（整段阻塞型死锁 → 明确失败） ====================

    /**
     * 巡检「主池被占满 + 有片段在排队 + 长时间零推进」。
     *
     * <p><b>要解决什么</b>：本引擎的片段是<b>整段占用线程</b>的（消费者无超时地等 shuffle 的
     * {@code partitionEnd}、生产者阻塞在 {@code awaitDistributedDone}、中间层 join 阻塞在 Phase 1），
     * 而下游收齐必须上游先推完 ⇒ 一旦某台 Worker 的主池被「等上游」的片段占满、后来的片段只能排队，
     * 就是一个<b>线程等待环</b>：排队者等槽位、在跑者等排队者相关的上游。更糟的是被阻塞片段仍处
     * RUNNING、遥测心跳照发 ⇒ Master 结果看门狗永不触发 ⇒ 表现为<b>假死</b>（永不报错、片段永不回收、
     * 该 Worker 从此对所有后续查询都是污染态）。</p>
     *
     * <p><b>判据（三个同时成立且持续 {@link #POOL_DEADLOCK_MS}）</b>：
     * <ol>
     *   <li>主池 {@code activeCount == poolSize} 且池队列非空（有片段抢不到线程）；</li>
     *   <li>至少一个片段确实「尚未开始」（{@code startNanos == 0}）；</li>
     *   <li><b>零推进</b>：本窗口内 {@link ExchangeManager#totalPagesReceived()} 与任务级
     *       {@link #progressSeq} 都没变过（一页 shuffle 没进、也没有任务开始 / 结束）。</li>
     * </ol>
     * 第 3 条是抗误判的关键：正常的大表聚合 / 排序 / join build 阶段虽然长时间不吐结果，但仍在
     * <b>持续收 shuffle 页</b>（进度信号在动），绝不会被判定为死锁。</p>
     *
     * <p><b>处置（两段式，绝不静默）</b>：<br>
     * ① <b>判死排队片段</b>：对每个未启动的片段，摘除其池队列条目 + 经判死钩子把
     * {@code FragmentFailed} 回传 Master（有结果汇的消费者 / Scan 才有钩子；生产者不回传）。Master
     * 收到即 fail-fast → {@code doClose} 反向取消该查询的全部片段 → 在跑的阻塞片段被中断、池恢复。<br>
     * ② 若又过一个阈值仍「池满 + 有排队 + 零推进」（说明 Master 没接上，比如 Master 已死），
     * <b>整池自愈</b>：中断本 Worker 上全部任务，把池彻底腾空（日志 ERROR 级，附任务清单）。
     * 这是最后手段 —— 走到这一步该 Worker 已经确定卡死，腾空池是唯一能让它重新服务后续查询的办法。</p>
     */
    private void watchdogTick() {
        try {
            if (state == WorkerState.STOPPED || state == WorkerState.FAILED) {
                return;
            }
            long now = System.nanoTime();
            long pages = exchangeManager.totalPagesReceived();
            long seq = progressSeq.get();
            List<TaskSlot> queued = new ArrayList<>();
            for (TaskSlot slot : taskSlots.values()) {
                if (slot.startNanos == 0L) {
                    queued.add(slot);
                }
            }
            int activeMain = taskExecutor.getActiveCount();
            int waitMain = taskExecutor.getQueue().size();
            int activeLeaf = leafExecutor.getActiveCount();
            int waitLeaf = leafExecutor.getQueue().size();
            // 两个池任一「占满 + 有排队」都算（主池被阻塞消费者占满 = 经典死锁；叶子池被阻塞生产者
            // 占满 = 数据推不出去的对称形态）
            boolean saturated = (activeMain >= poolSize && waitMain > 0)
                    || (activeLeaf >= poolSize && waitLeaf > 0);
            boolean moved = pages != lastPagesSeen || seq != lastSeqSeen;
            if (!saturated || queued.isEmpty() || moved) {
                // 条件不成立 / 有推进：重置停滞窗口（含上次判死后的升级计时）
                lastPagesSeen = pages;
                lastSeqSeen = seq;
                stalledSinceNanos = 0L;
                escalateArmed = false;
                return;
            }
            if (stalledSinceNanos == 0L) {
                stalledSinceNanos = now;
                log.warn("Worker {} 池疑似死锁：主池 {}/{} 占满（排队 {}）、叶子池 {}/{} 占满（排队 {}）"
                                + "且停止推进（{} 个片段未启动）—— 开始计时（阈值 {}ms，可用 -DDojaoPoolDeadlockMs 调整）",
                        info.workerId(), activeMain, poolSize, waitMain, activeLeaf, poolSize, waitLeaf,
                        queued.size(), POOL_DEADLOCK_MS);
                return;
            }
            long stalledMs = (now - stalledSinceNanos) / 1_000_000L;
            if (stalledMs < POOL_DEADLOCK_MS) {
                return;
            }
            if (!escalateArmed) {
                escalateArmed = true;
                stalledSinceNanos = now;
                log.error("Worker {} 池死锁确认（停滞 {}ms：一页 shuffle 未进、无任务开始/结束）→ "
                                + "判死 {} 个排队片段并主动回传失败，交由 Master fail-fast 连带取消在跑片段：\n{}",
                        info.workerId(), stalledMs, queued.size(), describeSlots());
                for (TaskSlot slot : queued) {
                    killQueuedTask(slot, stalledMs);
                }
                return;
            }
            log.error("Worker {} 池死锁未恢复（判死排队片段后又停滞 {}ms）→ 整池自愈：中断全部本地任务，"
                    + "该 Worker 上的查询将失败，但池重新可用：\n{}", info.workerId(), stalledMs, describeSlots());
            cancelAllTasks();
        } catch (Throwable t) {
            log.warn("Worker {} 池看门狗巡检异常", info.workerId(), t);
        }
    }

    /**
     * 判死一个<b>尚未启动</b>的片段：先从池队列摘除（{@code cancel} 不会打断任何线程），再走判死钩子把
     * 失败回传 Master，最后摘槽。钩子可能阻塞在网络回传上，故丢进 {@link #abortExecutor} 异步执行。
     */
    private void killQueuedTask(TaskSlot slot, long stalledMs) {
        String taskId = slot.task.taskId();
        String cause = "Worker 池死锁：片段在队列中等待超过 " + stalledMs + "ms 未获执行线程"
                + "（主池 " + poolSize + " 个线程被等 shuffle 的片段占满、且无任何数据推进）"
                + " —— 该 Worker 池已停滞，任务被主动判死";
        Future<?> future = taskFutures.get(taskId);
        if (future != null) {
            future.cancel(true);   // 未启动 → 仅从池队列摘除
        }
        Consumer<String> aborter = slot.aborter;
        taskFailures.put(taskId, cause);
        finishTask(taskId);
        if (aborter != null) {
            abortExecutor.execute(() -> {
                try {
                    aborter.accept(cause);
                } catch (Throwable t) {
                    log.warn("Worker {} 判死片段失败回传异常 taskId={}", info.workerId(), taskId, t);
                }
            });
        }
        log.error("Worker {} 判死排队片段 taskId={} jobId={}（{} 池）：{}",
                info.workerId(), taskId, slot.task.jobId(), slot.poolName, cause);
    }

    /** 最后手段：中断本 Worker 上全部任务（池腾空）。任务自身的中断路径负责安静退出。 */
    private void cancelAllTasks() {
        for (String taskId : List.copyOf(taskSlots.keySet())) {
            Future<?> future = taskFutures.get(taskId);
            if (future != null) {
                future.cancel(true);
            }
            finishTask(taskId);
        }
    }

    /** 看门狗诊断输出：每个任务槽一行（池 / 是否已启动 / 排队或已运行时長 / jobId / 描述）。 */
    private String describeSlots() {
        long now = System.nanoTime();
        StringBuilder sb = new StringBuilder();
        for (TaskSlot slot : taskSlots.values()) {
            long started = slot.startNanos;
            sb.append("    - [").append(slot.poolName).append("] taskId=").append(slot.task.taskId())
                    .append(" jobId=").append(slot.task.jobId())
                    .append(started == 0L
                            ? " 状态=排队中(ms=" + (now - slot.submitNanos) / 1_000_000L + ")"
                            : " 状态=已运行(ms=" + (now - started) / 1_000_000L + ")")
                    .append(" desc=").append(slot.task.sql())
                    .append('\n');
        }
        return sb.toString();
    }

    // ==================== 心跳 ====================

    /** 采集资源摘要并心跳，失败标记 FAILED。 */
    private void heartbeat() {
        try {
            ResourceSummary summary = resourceProvider != null ? resourceProvider.get() : ResourceSummary.collect();
            lastResource = summary;
            WorkerState effective = taskSlots.isEmpty() ? WorkerState.IDLE : WorkerState.BUSY;
            boolean alive = gateway.heartbeat(info, effective, summary);
            lastHeartbeatNanos = System.nanoTime();
            if (!alive) {
                this.state = WorkerState.FAILED;
                log.error("Worker {} 心跳被 Master 拒绝，标记 FAILED", info.workerId());
                return;
            }
            if (state != WorkerState.FAILED && state != WorkerState.STOPPED) {
                this.state = effective;
            }
            log.debug("Worker {} 心跳: state={}, {}", info.workerId(), state, summary.describe());
        } catch (Throwable t) {
            this.state = WorkerState.FAILED;
            log.error("Worker {} 心跳异常", info.workerId(), t);
        }
    }

    // ==================== Getter ====================

    public WorkerNode info() { return info; }
    public WorkerState state() { return state; }
    public ResourceSummary lastResource() { return lastResource; }
    public ExchangeManager exchangeManager() { return exchangeManager; }
    public long lastHeartbeatNanos() { return lastHeartbeatNanos; }
    public List<WorkerTask> runningTasks() {
        List<WorkerTask> out = new ArrayList<>(taskSlots.size());
        for (TaskSlot slot : taskSlots.values()) {
            out.add(slot.task);
        }
        return out;
    }
    public int load() { return taskSlots.size(); }
    public Map<String, String> taskFailures() {
        synchronized (taskFailures) {
            return Map.copyOf(taskFailures);
        }
    }
    public long heartbeatIntervalMs() { return heartbeatIntervalMs; }

    // ==================== 内部 ====================

    /** 任务槽：提交即建（此刻尚未有线程），{@link #finishTask} 摘除。 */
    private static final class TaskSlot {
        final WorkerTask task;
        final String poolName;
        final long submitNanos = System.nanoTime();
        /** 抢到执行线程的时刻；0 = 仍在池队列里排队（看门狗「判死排队片段」的判据）。 */
        volatile long startNanos;
        /** 判死钩子（可空）：未及执行即被判死时把失败回传 Master。 */
        volatile Consumer<String> aborter;

        TaskSlot(WorkerTask task, String poolName, Consumer<String> aborter) {
            this.task = task;
            this.poolName = poolName;
            this.aborter = aborter;
        }
    }

    /**
     * 任务池工厂：<b>core == max</b> 固定尺寸 + 无界 FIFO 队列 —— 前 {@code size} 个任务各占一线程，
     * 其余排队等空闲槽位（不拒绝、不超时）。两个池（消费者主池 / 叶子专线）尺寸与语义完全一致。
     */
    private static ThreadPoolExecutor newPool(int size, String threadNamePrefix) {
        return new ThreadPoolExecutor(
                size, size, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                new NamedThreadFactory(threadNamePrefix));
    }

    private static final class NamedThreadFactory implements ThreadFactory {
        private final String prefix;
        private final AtomicInteger seq = new AtomicInteger();

        NamedThreadFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, prefix + "-" + seq.incrementAndGet());
            t.setDaemon(true);
            return t;
        }
    }
}
