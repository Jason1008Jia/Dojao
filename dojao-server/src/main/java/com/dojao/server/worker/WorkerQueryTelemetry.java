package com.dojao.server.worker;

import com.dojao.core.rpc.control.ControlSerde;
import com.dojao.core.rpc.control.QueryFragmentTelemetryMsg;
import com.dojao.core.rpc.control.QueryTelemetryGroup;
import com.dojao.core.rpc.control.QueryTelemetryMsg;
import com.dojao.core.runtime.worker.WorkerNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

/**
 * Worker 侧<b>查询遥测</b>（节点级，~1s 一帧上抛 Master）—— 打开 worker→master 上行链路的核心。
 *
 * <p><b>解决什么</b>：Producer（Scan→Shuffle）/ 中间层（Join→Shuffle / Aggregate→Shuffle）片段
 * 的数据不回传 Master，其行数/字节只有 Worker 自己知道 → Master 侧 Scan / Join 框行数与流量恒 0。
 * 本类把每个非终端片段<b>尾部 HashPartitionOperator 的 rowsProcessed / inputBytes / outputBytes</b>
 * （= 该段实际收发的数据量；中间层 join 则按分区记 join 算子的收发量）登记为实时采样器，每
 * {@link #INTERVAL_MS}（默认 300ms）把本节点上<b>所有运行查询</b>的片段采样值打包成一条
 * {@link QueryTelemetryMsg} 经 {@link MasterGateway#telemetry} 上抛，Master 按 queryId
 * 归并进对应 {@link com.dojao.server.master.QueryManager} 的覆盖表。</p>
 *
 * <p><b>权威分层（无双计）</b>：终端 Consumer / 独立 Scan 叶子的行数由数据面（FragmentData /
 * ScanPage）天然到 Master，属权威值 —— Worker 只登记非终端片段，Master 只对非终端片段应用覆盖，
 * 双向双保险。行数是<b>单调累计值</b>，Master 按 taskId 覆盖；丢一拍下拍自愈（best-effort，
 * 任务生死仍由既有 FragmentFinished / 看门狗兜底）。</p>
 *
 * <p><b>僵尸（残留）片段回收（本通道的第二职责）</b>：每 {@link #REAP_INTERVAL_MS} 一次，随帧带上本节点
 * <b>正在跑的全部 jobId</b>（{@code running_query_ids}），Master 回信其中它<b>已不认</b>的子集
 * （查询已结束 / 已超时剔除 / Master 重启过，见 {@code QueryManagerRegistry#isOperatorQueryActive}）
 * → 本类调 {@link WorkerManager#cancelTasksOfJob} 就地取消这些片段。残留片段正是把池槽位占满、
 * 把后续查询拖进死锁的元凶（典型：上一查询的消费者永久阻塞在等 shuffle，占着槽位不走），
 * 这一步把它们清出去，Worker 才能回到干净态。判据单向（Master 说不活才杀）⇒ 绝不误杀活查询；
 * 回信失败（网络抖动）→ 本拍不回收，下拍再试。</p>
 *
 * @author jiaxin
 * @version 1.1
 * @date 2026/9/6
 */
public final class WorkerQueryTelemetry implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(WorkerQueryTelemetry.class);

    /** 遥测上抛间隔：-DDojaoQueryTelemetryMs 覆盖（默认 300ms，与 UI DAG 轮询同频 → Scan 进度条平滑渐变）。 */
    private static final long INTERVAL_MS = Math.max(100L, Long.getLong("DojaoQueryTelemetryMs", 300L));

    /**
     * 僵尸片段回收的询问间隔（-DDojaoQueryReapMs 覆盖，默认 30s）：不是每拍都带
     * {@code running_query_ids} —— Master 侧要遍历活跃查询匹配 operator queryId，没必要 300ms 一次。
     */
    private static final long REAP_INTERVAL_MS = Math.max(1000L, Long.getLong("DojaoQueryReapMs", 30_000L));

    private final MasterGateway gateway;
    private final WorkerNode node;
    private final ScheduledExecutorService scheduler;
    /** queryId → taskId → 实时采样器（非终端片段尾部 HashPartitionOperator 的 rowsProcessed）。 */
    private final Map<String, Map<String, Sampler>> byQuery = new ConcurrentHashMap<>();
    /**
     * queryId → taskId → 收尾终值。片段结束时采样器即被注销：若终值不补发，最后一拍的增量
     * 会落在两次 tick 之间被永久丢失（短命片段一个采样帧都发不出去 → Master 侧恒 0 行）。
     * 终值随下一拍以 {@code done=true} 上抛后清除；上抛失败则留待下拍重发。
     */
    private final Map<String, Map<String, Sample>> pendingFinal = new ConcurrentHashMap<>();
    /**
     * 本节点任务池（僵尸片段回收的执行者）：tick 拿到 Master 回的 stale jobId 即调其
     * {@link WorkerManager#cancelTasksOfJob}。装配前（伪分布式 / 未接线）为 null → 回收功能静默关闭，
     * 遥测上行照常。
     */
    private volatile WorkerManager workerManager;
    /** 上一次携带 {@code running_query_ids} 的时刻（ms），按 {@link #REAP_INTERVAL_MS} 节流。 */
    private volatile long lastReapMs;
    private volatile boolean closed;

    /**
     * 一个采样器：行数 + 进出字节 + 溢写字节（均为单调累计值）+ 可选的「该片段已收尾」判定。后者用于片段
     * <b>内部</b>按分区暴露的子任务（如 hash join 的分区线程）——它们比片段本身更早收尾，命中后
     * 该拍的帧里该条目标 {@code done=true}，Master 逐分区独立变绿（片段自身的收尾仍走 {@link #finish}）。
     *
     * <p><b>通道相互独立</b>：{@code rows} 为 null = 本采样器只上报溢写（见 {@link #trackSpill}）——
     * 溢写发生在 Worker 进程内的算子对象上，与「该片段行数是否由数据面回传」无关；终端 Consumer
     * （窗口排序 / 聚合 / 排序归并 join）恰恰是溢写主力，故必须能<b>单独</b>登记而不碰行数通道。</p>
     */
    private record Sampler(LongSupplier rows, LongSupplier bytesIn, LongSupplier bytesOut,
                           LongSupplier spilled, BooleanSupplier done) {
    }

    /** 一拍的采样值；各通道 {@code -1} = 该通道本拍不可读（Master 不覆盖该表）。 */
    private record Sample(long rows, long bytesIn, long bytesOut, long spilled) {

        /** 整条不可读（无任何有效通道）→ 本拍跳过。 */
        boolean empty() {
            return rows < 0L && spilled < 0L;
        }
    }

    public WorkerQueryTelemetry(MasterGateway gateway, WorkerNode node) {
        this.gateway = gateway;
        this.node = node;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "dojao-query-telemetry-" + shortId(node.workerId()));
            t.setDaemon(true);
            return t;
        });
    }

    /** 登记一个非终端片段的实时采样器（任务线程调用；tick 线程并发读安全）。 */
    public void track(String queryId, String taskId, LongSupplier rowsSupplier,
                      LongSupplier bytesInSupplier, LongSupplier bytesOutSupplier) {
        track(queryId, taskId, rowsSupplier, bytesInSupplier, bytesOutSupplier, null);
    }

    /** 登记采样器（带收尾判定）：{@code doneSupplier} 返回 true 时该拍以 {@code done=true} 上报后即注销。 */
    public void track(String queryId, String taskId, LongSupplier rowsSupplier,
                      LongSupplier bytesInSupplier, LongSupplier bytesOutSupplier,
                      BooleanSupplier doneSupplier) {
        if (queryId == null || taskId == null || rowsSupplier == null) {
            return;
        }
        byQuery.computeIfAbsent(queryId, k -> new ConcurrentHashMap<>())
                .put(taskId, new Sampler(rowsSupplier, bytesInSupplier, bytesOutSupplier, null, doneSupplier));
    }

    /**
     * 登记一个<b>纯溢写</b>采样器：只上报累计溢写字节，行数 / 字节通道不可读。
     *
     * <p>用于终端 Consumer（窗口排序 / 聚合 / 排序归并 join）——它们的行数由数据面回传 Master
     * 属权威值（不进遥测，避免双计），但<b>溢写只发生在 Worker 进程内的算子对象上</b>，Master
     * 无从得知。若不走这条通道，监控页「溢写总量」对分布式查询恒为空。</p>
     *
     * <p>与 {@link #track} <b>可叠加</b>：同一 taskId 已登记行数 / 字节时（中间层 join / 聚合），
     * 本方法在原采样器上补挂溢写通道而不是覆盖它 —— 否则会把行数通道一并抹掉。</p>
     *
     * <p>一个片段有<b>多个</b>溢写算子时（链式 join→聚合 / join→窗口：join 与后续算子同片段各溢各的）
     * 再次调用是<b>累加</b>而非覆盖 —— 否则后登记的会把先登记的溢写量抹掉，监控上只剩半个数。</p>
     */
    public void trackSpill(String queryId, String taskId, LongSupplier spilledSupplier) {
        if (queryId == null || taskId == null || spilledSupplier == null) {
            return;
        }
        byQuery.computeIfAbsent(queryId, k -> new ConcurrentHashMap<>())
                .compute(taskId, (k, prev) -> prev == null
                        ? new Sampler(null, null, null, spilledSupplier, null)
                        : new Sampler(prev.rows(), prev.bytesIn(), prev.bytesOut(),
                                sumSuppliers(prev.spilled(), spilledSupplier), prev.done()));
    }

    /** 两路溢写采样器相加（任一为 null 取另一路；两路都不可读 → -1）。 */
    private static LongSupplier sumSuppliers(LongSupplier a, LongSupplier b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        return () -> {
            long x = safeRows(a);
            long y = safeRows(b);
            if (x < 0L && y < 0L) {
                return -1L;
            }
            return Math.max(0L, x) + Math.max(0L, y);
        };
    }

    /**
     * 登记一个<b>存活心跳</b>采样器：行数恒 0、字节通道不可读。本帧只表达「这个任务还活着」，
     * 供 Master 侧 {@code DistributedScanOperator.markAlive} 刷新看门狗的接触时刻 —— 独立 Scan
     * 叶子读慢时（如 ORC 最小读单元 = 整条 stripe）可以长时间不吐页，但不得被判 LOST。
     *
     * <p><b>必须在派发受理后立刻登记，而不是等任务体真正开跑</b>：扫描任务与 Producer 片段共用
     * 叶子池，池满时任务会<b>排队</b>数秒乃至更久。若登记写在任务体里，排队期间既无数据页、也无
     * 心跳，Master 侧 30s 看门狗会把「还在排队的活任务」判 LOST（空闲集群偶发
     * {@code scan task timeout} 的根因）。</p>
     *
     * @param taskActive 任务是否仍在 Worker 任务表中；返回 false 时本拍上报 {@code done=true} 后注销
     *                   （采样器不会滞留到任务结束之后）
     */
    public void trackScanLiveness(String queryId, String taskId, BooleanSupplier taskActive) {
        track(queryId, taskId, () -> 0L, null, null,
                taskActive == null ? null : () -> !taskActive.getAsBoolean());
    }

    /**
     * 注销一个片段的采样器（任务结束 / 失败 / 取消时调用，防泄漏）：仅移除，不补发终值 ——
     * 失败 / 取消的片段不该被呈现为「正常跑完」。
     */
    public void untrack(String queryId, String taskId) {
        removeSampler(queryId, taskId);
    }

    /**
     * 片段<b>正常跑完</b>的收尾：先读一次采样器的终值存入 {@link #pendingFinal}，由下一拍以
     * {@code done=true} 上抛 —— 采样器一旦移除就再也读不到值，不补发则终值（尤其短命片段，
     * 整条命内一个采样帧都发不出）会永久丢失，Master 侧恒 0 行且状态悬挂到整条查询收尾。
     */
    public void finish(String queryId, String taskId) {
        Sampler last = removeSampler(queryId, taskId);
        if (last == null) {
            return;
        }
        Sample sample = sample(last);
        if (!sample.empty()) {
            pendingFinal.computeIfAbsent(queryId, k -> new ConcurrentHashMap<>()).put(taskId, sample);
        }
    }

    /** 从采样表移除一个片段，返回其采样器（不存在 → null）。 */
    private Sampler removeSampler(String queryId, String taskId) {
        if (queryId == null || taskId == null) {
            return null;
        }
        Map<String, Sampler> frags = byQuery.get(queryId);
        if (frags == null) {
            return null;
        }
        Sampler removed = frags.remove(taskId);
        if (frags.isEmpty()) {
            byQuery.remove(queryId);
        }
        return removed;
    }

    /**
     * 装配任务池引用（启用僵尸片段回收）：必须在 {@link #start()} 之前调用。未装配 → 遥测照发但
     * 不带 {@code running_query_ids}，回收功能静默关闭。
     */
    public void setWorkerManager(WorkerManager workerManager) {
        this.workerManager = workerManager;
    }

    /** 启动定时上抛（Worker 注册后调用；空表不发送）。 */
    public void start() {
        scheduler.scheduleWithFixedDelay(this::tick, INTERVAL_MS, INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        closed = true;
        scheduler.shutdownNow();
        byQuery.clear();
        pendingFinal.clear();
    }

    // ==================== 内部 ====================

    /** 单拍：把本节点所有运行查询的片段采样值打成一条遥测帧上抛（空表 / 采样失败 → 不发）。 */
    private void tick() {
        if (closed) {
            return;
        }
        // 本拍要发的终值快照（发送成功后才从 pendingFinal 清除，失败留待下拍重发）
        Map<String, Map<String, Sample>> finals = new LinkedHashMap<>();
        pendingFinal.forEach((q, m) -> {
            if (!m.isEmpty()) {
                finals.put(q, new LinkedHashMap<>(m));
            }
        });
        // 僵尸回收询问（按 REAP_INTERVAL_MS 节流）：带本节点正在跑的 jobId 集，Master 回信已不认的子集
        List<String> runningIds = List.of();
        WorkerManager wm = this.workerManager;
        long nowMs = System.currentTimeMillis();
        if (wm != null && nowMs - lastReapMs >= REAP_INTERVAL_MS) {
            runningIds = new ArrayList<>(wm.runningJobIds());
            lastReapMs = nowMs;
        }
        if (byQuery.isEmpty() && finals.isEmpty() && runningIds.isEmpty()) {
            return;
        }
        // 本拍采样器自报已收尾的子任务（发送成功后注销采样器）
        Map<String, Set<String>> sampledDone = new LinkedHashMap<>();
        try {
            QueryTelemetryMsg.Builder msg = QueryTelemetryMsg.newBuilder()
                    .setNode(ControlSerde.toNode(node));
            AtomicInteger fragCount = new AtomicInteger();
            // queryId → taskId → 采样值：实时采样 + 终值覆盖（终值即该片段的真实完成值）
            Map<String, Map<String, Sample>> samples = new LinkedHashMap<>();
            byQuery.forEach((queryId, frags) -> frags.forEach((taskId, sampler) -> {
                Sample s = sample(sampler);
                if (s.empty()) {    // 采样器暂不可读（未初始化 / 已关）：本拍跳过，下拍补上
                    return;
                }
                samples.computeIfAbsent(queryId, k -> new LinkedHashMap<>()).put(taskId, s);
                if (sampler.done() != null && safeDone(sampler.done())) {
                    sampledDone.computeIfAbsent(queryId, k -> new LinkedHashSet<>()).add(taskId);
                }
            }));
            finals.forEach((queryId, frags) -> samples
                    .computeIfAbsent(queryId, k -> new LinkedHashMap<>()).putAll(frags));
            samples.forEach((queryId, frags) -> {
                if (frags.isEmpty()) {
                    return;
                }
                Map<String, Sample> doneSet = finals.getOrDefault(queryId, Map.of());
                Set<String> doneSampled = sampledDone.getOrDefault(queryId, Set.of());
                QueryTelemetryGroup.Builder group = QueryTelemetryGroup.newBuilder().setQueryId(queryId);
                frags.forEach((taskId, s) -> {
                    group.addFragments(QueryFragmentTelemetryMsg.newBuilder()
                            .setTaskId(taskId).setRowsEmitted(s.rows())
                            .setBytesIn(Math.max(-1L, s.bytesIn()))
                            .setBytesOut(Math.max(-1L, s.bytesOut()))
                            .setSpilledBytes(s.spilled())
                            .setDone(doneSet.containsKey(taskId) || doneSampled.contains(taskId)));
                    fragCount.incrementAndGet();
                });
                if (group.getFragmentsCount() > 0) {
                    msg.addQueries(group);
                }
            });
            if (msg.getQueriesCount() == 0 && runningIds.isEmpty()) {
                return;
            }
            msg.addAllRunningQueryIds(runningIds);
            List<String> stale = gateway.telemetry(msg.build());
            // 僵尸回收：Master 已不认的 jobId → 就地取消其本地片段，释放被残留片段占住的池槽位
            if (wm != null && !stale.isEmpty()) {
                for (String jobId : stale) {
                    int killed = wm.cancelTasksOfJob(jobId);
                    if (killed > 0) {
                        log.warn("僵尸片段回收：Master 已不认查询 {} → 就地取消本节点 {} 个残留片段", shortId(jobId), killed);
                    }
                }
            }
            // 上抛成功 → 清除本拍已发的终值（期间新登记的终值不在快照内，不受影响）
            finals.forEach((queryId, frags) -> {
                Map<String, Sample> pending = pendingFinal.get(queryId);
                if (pending == null) {
                    return;
                }
                frags.keySet().forEach(pending::remove);
                if (pending.isEmpty()) {
                    pendingFinal.remove(queryId);
                }
            });
            // 上抛成功 → 注销自报收尾的采样器（done 只发一次；期间新登记的采样器不在快照内）
            sampledDone.forEach((queryId, taskIds) -> taskIds.forEach(id -> removeSampler(queryId, id)));
        } catch (Throwable t) {
            log.warn("查询遥测打包/上抛异常: {}", String.valueOf(t.getMessage()));
        }
    }

    private static long safeRows(LongSupplier supplier) {
        if (supplier == null) {
            return -1L;
        }
        try {
            return supplier.getAsLong();
        } catch (Throwable t) {
            return -1L;
        }
    }

    /**
     * 读一次采样器：<b>各通道独立</b> —— 不可读记 -1（Master 不覆盖该表）。行数 / 溢写两个
     * 主通道都不可读（{@link Sample#empty()}）时整条作废；纯溢写采样器（{@code rows == null}）
     * 行数恒 -1 但仍有效。
     */
    private static Sample sample(Sampler s) {
        return new Sample(
                s.rows() == null ? -1L : safeRows(s.rows()),
                safeRows(s.bytesIn()),
                safeRows(s.bytesOut()),
                s.spilled() == null ? -1L : safeRows(s.spilled()));
    }

    private static boolean safeDone(BooleanSupplier supplier) {
        try {
            return supplier.getAsBoolean();
        } catch (Throwable t) {
            return false;
        }
    }

    private static String shortId(String id) {
        return id.substring(0, Math.min(8, id.length()));
    }
}
