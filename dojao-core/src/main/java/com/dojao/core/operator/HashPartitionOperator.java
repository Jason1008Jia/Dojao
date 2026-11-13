package com.dojao.core.operator;

import com.dojao.common.data.Page;
import com.dojao.common.memory.NodeMemory;
import com.dojao.core.config.ResourceConfig;
import com.dojao.core.runtime.worker.ExchangeChannel;
import com.dojao.core.runtime.worker.ExchangeLease;
import com.dojao.spi.util.NestedTypeSupport;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Hash 分区算子（Shuffle 端）—— 恒定并行执行：N 个后台分发线程流式读取上游 Page，
 * 逐行计算 {@code hash(keys) % numPartitions}，拷贝到对应分区的微批缓冲区中，
 * 缓冲区满（{@link #BATCH_SIZE}）即发射到该分区独立的有界输出队列。
 *
 * <p><b>设计理念：分区即并行。</b>全链路并行：
 * <ul>
 *   <li><b>分发端</b>：{@code numDistributors} 个分发线程（通常 = 上游并行度），
 *       每个线程持有独立的 {@link PartitionBuffer} + {@link BufferAllocator}，
 *       共享输出队列，无锁竞争仅限队列的 put/take。</li>
 *   <li>并行 join（{@link HashJoinOperator}）：N 个工作线程通过 {@link #partitionQueue(int)}
 *       各消费自己的分区流，独立 build/probe/send。</li>
 *   <li>单线程分区感知算子（聚合/窗口/排序归并）：通过 {@code next()} 合并拉取各分区队列
 *       （保留 {@code partitionId}、丢弃分区结束标记），内部仍是每分区并行填充，无旧串行缓冲。</li>
 * </ul>
 *
 * <p>上游耗尽后各分发线程刷尽残留行，最后一个完活的线程给每个分区发结束标记：单机 → 本地队列塞
 * {@link Page#partitionEnd(int)}；分布式 → 经 {@code ExchangeChannel} 通知归属 Worker。分发异常时
 * 单机塞结束标记让消费线程退出，分布式通知接收端失败（fail-fast），并记录 {@link #distributorFailure}
 * 供下游 rethrow。
 *
 * <p><b>背压</b>：每分区队列有界（{@link #PARALLEL_QUEUE_CAPACITY}），分发线程在队列满时阻塞，
 * 上游扫描随之阻塞 —— 任一环节内存都被队列容量约束，不会无界膨胀。
 *
 * <p><b>单机 / 分布式双模</b>：本算子同时是 {@link PartitionedSource}。不传
 * {@code ExchangeChannel} 时输出到本地分区队列（单机，行为不变）；传入时每个分区的数据经
 * {@code ExchangeChannel} 推到归属 Worker 的 {@link ShuffleReader}（分布式，4 个逻辑分区的内存
 * 物理分布在各个 Worker）。哈希 / gather / 批处理逻辑完全复用，只换输出目的地（{@link #emitTo}）。
 *
 * @author jiaxin
 * @version 4.1
 * @date 2026/8/14
 */
public class HashPartitionOperator extends AbstractMeasurableOperator implements PartitionedSource {

    /** 每个分区缓冲区的行数上限，达到后立即发射。 */
    static final int BATCH_SIZE = 10000;
    /** 每分区输出队列容量（有界 put 提供背压，防止任意一侧缓冲过大）。 */
    private static final int PARALLEL_QUEUE_CAPACITY = 8;

    private Operator upstream;
    private final List<String> keyColumns;
    private final int numPartitions;
    private final int numDistributors;

    /** 分布式模式：Worker↔Worker 交换通道；null → 单机模式（本地分区队列，行为不变）。 */
    private final ExchangeChannel exchangeChannel;
    /** 分布式模式：每个分区的归属 WorkerId（owner 映射，全查询一致 —— co-partitioning 不变量）。 */
    private final String[] ownerWorkerIds;
    /**
     * 分布式模式：每分区的交换标识（{@code exchangeIds[pid]} = 分区 pid 的 exchangeId）。
     * join / window 各分区恒同；聚合每分区一个消费者时各异 —— 发送端据此把各桶路由到
     * 对应分区的独立接收端（消费端并行度与分区数一致，不与 Worker 数绑定）。
     */
    private final String[] exchangeIds;
    /** 分布式模式：本算子所在 WorkerId（单机为 null，供统计 / isSelf 判定）。 */
    private final String selfWorkerId;
    /** 分区方式：{@code HASH}（默认，co-partitioning）/ 广播的 {@code REPLICATE}（build 侧）/ {@code FIXED}（probe 侧）。 */
    private final FragmentSpec.Distribution distribution;
    /** {@code FIXED} 的目标桶号（= 生产者所在 Worker 的槽位）；其余模式无用。 */
    private final int fixedBucket;
    /** 分布式模式：全部分发线程完成且已通知所有归属 Worker 后释放（执行器据此收尾）。 */
    private final CountDownLatch distributedDone;

    /** 每分区的有界输出队列（单机模式：所有分发线程共享填充、消费线程取；分布式模式不使用）。 */
    private final BlockingQueue<Page>[] partitionQueues;
    /** 后台分发线程池。 */
    private ExecutorService distributorExecutor;
    /** 分发线程已完成的计数（最后一个线程负责塞 partitionEnd）。 */
    private final AtomicInteger distributorsDone = new AtomicInteger();
    /**
     * 正在运行的分发线程数（进入 {@link #distributeLoop} 即 +1，_finally_ 里 -1）。
     *
     * <p>与 {@link #distributorsDone} 的区别：{@code shutdownNow()} 会取消<b>尚未启动</b>的任务，
     * 这些任务永远走不到 finally，done 永远达不到 {@code numDistributors}。判断「不可能再有
     * send/complete」只能靠「运行中的线程数归零」。</p>
     */
    private final AtomicInteger distributorsRunning = new AtomicInteger();
    /** partitionEnd 是否已发送（确保只发一次）。 */
    private final AtomicBoolean partitionEndSent = new AtomicBoolean();
    /** 本生产端已向交换通道释放过发送流（close 幂等 + 引用计数与 {@link #doOpen} 的 acquire 严格配对）。 */
    private final AtomicBoolean exchangeReleased = new AtomicBoolean();
    /**
     * 是否<b>成功</b>在通道上登记过本生产端（{@link #doOpen} 的 acquire 循环走完才置 true）。
     *
     * <p>{@code close()} 可能发生在 {@code open()} 之前（open 抛错 / 取消路径），此时绝不能
     * {@code release}：同一 Worker 上多个 Producer 片段共用一条 (目标Worker, exchangeId) 发送流，
     * 未登记却减引用计数会把仍在发送的邻居片段计数减到 0 → 提前关流，邻居的
     * {@code send/complete} 撞上 "发送流已释放"。</p>
     */
    private volatile boolean exchangeAcquired;
    /**
     * 每分区的发送租约（{@code exchangeIds[pid]} 相同的分区共享同一租约）。
     *
     * <p>send/complete/fail 全部经句柄路由，不再按 exchangeId 去通道的全局流表里查 ——
     * 句柄固化了「使用哪一代发送流」，避免同一 Worker 上先后执行的片段复用上一代已收尾的流。</p>
     */
    private volatile ExchangeLease[] exchangeLeases;
    /** 已获取的租约（按 exchangeId 去重，收尾时逐个关闭）。 */
    private final List<ExchangeLease> acquiredLeases = new ArrayList<>();
    /** 已请求收尾（{@link #doClose} 进入）：最后一个分发线程退出时补做发送流释放（见超时路径）。 */
    private final AtomicBoolean closing = new AtomicBoolean();
    /** 分发线程的异常（消费端 rethrow；正常为 null）。 */
    private volatile Throwable distributorFailure;
    /** 各分发线程的独立 allocator（doClose 时统一释放）。 */
    private final List<BufferAllocator> distributorAllocators = new ArrayList<>();

    /**
     * 排障计时：全部分发线程在 {@link #emitTo} 里<b>阻塞等待发出去</b>的累计时长（多线程求和）。
     * <p>用于把 327s 的「分发收尾」拆成两半：该值接近总时长 ⇒ 卡在<b>发送/下游接收</b>（队列满或
     * Flight {@code putNext} 慢）；该值很小 ⇒ 卡在本机的字节拷贝/哈希。默认不打印，仅随
     * {@code [SHUFFLE-PROF] producer 分发收尾} 一行输出。
     */
    private final AtomicLong emitBlockNanos = new AtomicLong();

    // 合并 next() 状态（供单线程分区感知消费者）
    private int nextDrainPid;
    private int endMarkerCount;
    private boolean mergedDrained;

    /**
     * @param upstream       上游算子
     * @param keyColumns     用于计算哈希分区的键列名
     * @param numPartitions  目标分区数
     */
    public HashPartitionOperator(Operator upstream, List<String> keyColumns, int numPartitions) {
        this(upstream, keyColumns, numPartitions, 1);
    }

    /**
     * @param upstream        上游算子
     * @param keyColumns      用于计算哈希分区的键列名
     * @param numPartitions   目标分区数
     * @param numDistributors 并行分发线程数（通常 = 上游并行度以达全链路并行）
     */
    public HashPartitionOperator(Operator upstream, List<String> keyColumns,
                                  int numPartitions, int numDistributors) {
        // 显式 cast 到 (String, String) 解析为单 exchangeId 构造（null 与 String[] 重载有歧义）
        this(upstream, keyColumns, numPartitions, numDistributors,
                (ExchangeChannel) null, (String[]) null, (String) null, (String) null);
    }

    /**
     * 分布式构造：把各分区输出到归属 Worker（{@code ownerWorkerIds[pid]}），经 ExchangeChannel
     * 推送。{@code exchangeChannel} 为 null 时退化为单机行为（本地分区队列）—— 与现有构造等价。
     *
     * <p><b>co-partitioning 不变量</b>：同一 join 的 build / probe 两侧必须传入相同的
     * 哈希键（{@code keyColumns}）与相同的 owner 映射（{@code ownerWorkerIds}），否则
     * 同 key 的 build 行和 probe 行会落在不同 Worker → join 结果错误。</p>
     *
     * @param upstream        上游算子（分布式模式下是 Worker 本地扫描）
     * @param keyColumns      用于计算哈希分区的键列名
     * @param numPartitions   目标分区数（应 ≥ 参与 Worker 数，保证每台至少一个桶）
     * @param numDistributors 并行分发线程数
     * @param exchangeChannel Worker↔Worker 交换通道；null → 单机
     * @param ownerWorkerIds  ownerWorkerIds[pid] = 分区 pid 归属的 WorkerId
     * @param exchangeId      本次 shuffle 的标识
     * @param selfWorkerId    本算子所在 WorkerId
     */
    public HashPartitionOperator(Operator upstream, List<String> keyColumns, int numPartitions, int numDistributors,
                                 ExchangeChannel exchangeChannel, String[] ownerWorkerIds,
                                 String exchangeId, String selfWorkerId) {
        // 兼容单 exchangeId 形态（join / window）：所有分桶共用同一交换标识
        this(upstream, keyColumns, numPartitions, numDistributors, exchangeChannel, ownerWorkerIds,
                Collections.nCopies(Math.max(1, numPartitions), exchangeId).toArray(new String[0]),
                selfWorkerId);
    }

    /**
     * 分布式构造（per-partition exchangeId）：{@code exchangeIds[pid]} = 分区 pid 的交换标识。
     * 聚合每分区一个消费者时，发送端按桶路由到对应分区的独立接收端（消费端并行度 = 分区数）；
     * join / window 传 nCopies 恒同，行为与单 exchangeId 完全一致。
     */
    public HashPartitionOperator(Operator upstream, List<String> keyColumns, int numPartitions, int numDistributors,
                                 ExchangeChannel exchangeChannel, String[] ownerWorkerIds,
                                 String[] exchangeIds, String selfWorkerId) {
        this(upstream, keyColumns, numPartitions, numDistributors, exchangeChannel, ownerWorkerIds,
                exchangeIds, selfWorkerId, FragmentSpec.Distribution.HASH, 0);
    }

    /**
     * 广播构造（{@link FragmentSpec.Distribution}）：{@code numPartitions} 应取 <b>Worker 数 w</b>
     * （每台恰好 own 一个桶），build 侧传 {@code REPLICATE}（整页复制到全部桶）、probe 侧传
     * {@code FIXED}（{@code fixedBucket} = 本生产者的槽位，数据就地入桶、零跨机）。
     * 其余行为（完成标记 / 失败传播 / 背压）与哈希模式完全一致 —— 只换「行去哪个桶」。
     */
    public HashPartitionOperator(Operator upstream, List<String> keyColumns, int numPartitions, int numDistributors,
                                 ExchangeChannel exchangeChannel, String[] ownerWorkerIds,
                                 String[] exchangeIds, String selfWorkerId,
                                 FragmentSpec.Distribution distribution, int fixedBucket) {
        super("HashPartition", "HashPartition", numPartitions);
        this.upstream = upstream;
        this.keyColumns = keyColumns;
        this.numPartitions = Math.max(1, numPartitions);
        this.numDistributors = Math.max(1, numDistributors);
        this.exchangeChannel = exchangeChannel;
        this.ownerWorkerIds = ownerWorkerIds;
        this.exchangeIds = exchangeIds;
        this.selfWorkerId = selfWorkerId;
        this.distribution = distribution;
        this.fixedBucket = fixedBucket;
        this.distributedDone = exchangeChannel != null ? new CountDownLatch(1) : null;
        @SuppressWarnings("unchecked")
        BlockingQueue<Page>[] qs = (BlockingQueue<Page>[]) new BlockingQueue[this.numPartitions];
        this.partitionQueues = qs;
        for (int i = 0; i < this.numPartitions; i++) {
            partitionQueues[i] = new LinkedBlockingQueue<>(PARALLEL_QUEUE_CAPACITY);
        }
    }

    /** 更新上游算子引用（列裁剪优化使用，须在 {@link #doOpen()} 前调用）。 */
    public void setUpstream(Operator upstream) { this.upstream = upstream; }

    public List<String> getPartitionKeys() { return keyColumns; }

    /** 分区数。 */
    public int numPartitions() { return numPartitions; }

    /** 分发线程数。 */
    public int numDistributors() { return numDistributors; }

    /** 返回指定分区的输出队列（各分区独立，供并行 join 工作线程按分区号消费）。 */
    public BlockingQueue<Page> partitionQueue(int pid) {
        return partitionQueues[pid];
    }

    /** 分发线程的异常（无异常返回 null）。 */
    public Throwable distributorFailure() { return distributorFailure; }

    /** 是否为分布式模式（分区输出到远程 Worker）。 */
    public boolean isDistributed() { return exchangeChannel != null; }

    /** 分布式模式下等待全部分发线程完成并已通知所有归属 Worker（执行器据此收尾）。 */
    public void awaitDistributedDone() throws InterruptedException {
        if (distributedDone != null) distributedDone.await();
    }

    /**
     * 发射一个数据页到指定分区：单机 → 本地分区队列；分布式 → 推送该分区的归属 Worker。
     * 这是"单机 / 分布式双模"的分界点：哈希 / gather / 批处理逻辑完全复用，只换输出目的地。
     */
    private void emitTo(int pid, Page page) throws InterruptedException {
        long t0 = System.nanoTime();
        try {
            if (exchangeChannel == null) {
                partitionQueues[pid].put(page);
            } else {
                exchangeLease(pid).send(ownerWorkerIds[pid], pid, page);
            }
        } finally {
            emitBlockNanos.addAndGet(System.nanoTime() - t0);
        }
    }

    /** 标记指定分区发完：单机 → 本地队列塞 partitionEnd；分布式 → 通知归属 Worker 收尾。 */
    private void endPartitionTo(int pid) throws InterruptedException {
        if (exchangeChannel == null) {
            partitionQueues[pid].put(Page.partitionEnd(pid));
        } else {
            exchangeLease(pid).complete(ownerWorkerIds[pid], pid);
        }
    }

    /** 分布式：通知所有桶的归属 Worker 本发送方失败（接收端据此 fail-fast，防止其挂起）。 */
    private void notifyRemoteFail(Throwable t) {
        for (int i = 0; i < numPartitions; i++) {
            try {
                exchangeLease(i).fail(ownerWorkerIds[i], i, t);
            } catch (Throwable ignored) {
                // 目标已死 → 忽略，由 Master 看门狗兜底（Phase C）
            }
        }
    }

    /** 取分区对应的发送租约；缺失说明 open 时序错配（分发线程不该在租约就绪前启动）。 */
    private ExchangeLease exchangeLease(int pid) {
        ExchangeLease[] leases = exchangeLeases;
        ExchangeLease lease = leases == null ? null : leases[pid];
        if (lease == null) {
            throw new IllegalStateException("分区 " + pid + " 没有发送租约（doOpen 未完成 / exchangeId 为空）");
        }
        return lease;
    }

    @Override
    protected void doOpen() throws Exception {
        upstream.open();
        // 先登记本生产端再启动分发线程：同一 Worker 上多个 Producer 片段共用一条
        // (目标 Worker, exchangeId) 发送流，先跑完的片段释放时不得关掉仍在用的共享流。
        // 登记返回「发送租约」—— send/complete/fail 全部经它路由（代际固化在句柄里）。
        if (exchangeChannel != null && exchangeIds != null) {
            Map<String, ExchangeLease> byId = new HashMap<>();
            ExchangeLease[] leases = new ExchangeLease[numPartitions];
            List<ExchangeLease> acquired = new ArrayList<>();
            try {
                for (int pid = 0; pid < numPartitions; pid++) {
                    String id = exchangeIds[pid];
                    if (id == null) {
                        continue;
                    }
                    ExchangeLease lease = byId.get(id);
                    if (lease == null) {
                        lease = exchangeChannel.acquireLease(id);
                        byId.put(id, lease);
                        acquired.add(lease);
                    }
                    leases[pid] = lease;
                }
            } catch (RuntimeException e) {
                // 半途失败：归还已获取的租约（此时还不能置 exchangeAcquired，releaseExchange 不适用）
                for (ExchangeLease lease : acquired) {
                    try { lease.close(); } catch (Throwable ignored) { }
                }
                throw e;
            }
            exchangeLeases = leases;
            acquiredLeases.addAll(acquired);
            exchangeAcquired = true;
        }
        startDistributors();
    }

    /**
     * 合并拉取：轮询各分区队列，原样返回带 partitionId 的数据页、丢弃分区结束标记，
     * 全部分区排空后返回全局 EOF。供单线程分区感知消费者（聚合/窗口/排序归并）使用；
     * 并行 join 不经过这里，直接消费 {@link #partitionQueue(int)}。
     */
    @Override
    protected Page doNext() throws Exception {
        checkDistributorFailure();
        if (mergedDrained) {
            // 分发线程可能在「已发出全部结束标记」之后才记录失败（例如收尾阶段才抛）——
            // EOF 是消费端能看到的最后一次 next()，必须在这里再查一次，否则失败被静默吞掉、结果残缺。
            checkDistributorFailure();
            return Page.eof();
        }

        while (true) {
            boolean anySeen = false;
            for (int i = 0; i < numPartitions; i++) {
                int pid = (nextDrainPid + i) % numPartitions;
                Page p = partitionQueues[pid].poll();
                if (p == null) continue;
                anySeen = true;
                if (p.isEndOfPartition()) {
                    if (++endMarkerCount >= numPartitions) mergedDrained = true;
                    continue;
                }
                nextDrainPid = (pid + 1) % numPartitions;
                return p;
            }
            if (mergedDrained) {
                checkDistributorFailure();
                return Page.eof();
            }
            if (!anySeen) {
                try { Thread.sleep(1); } catch (InterruptedException e) {
                    // ★取消必须原样抛出：旧实现返回 EOF，把「取消」伪装成「上游正常结束」，
                    //   下游溢写算子会立刻起一整轮 Phase 2 归并（纯 CPU），取消后反而更忙。
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
        }
    }

    @Override
    protected void doClose() throws Exception {
        // 1. 先停掉所有后台分发线程
        closing.set(true);
        boolean stopConfirmed = true;
        if (distributorExecutor != null) {
            distributorExecutor.shutdownNow();
            try { distributorExecutor.awaitTermination(5, TimeUnit.SECONDS); } catch (InterruptedException ignored) {}
            // 已 shutdown 的池：isTerminated = 全部任务（含被 shutdownNow 取消的未启动任务）都已结束。
            // 超时未停 ⇒ 还有线程活着，release 推迟到最后一个分发线程退出（见 distributeLoop finally）。
            stopConfirmed = distributorExecutor.isTerminated();
        }
        // 2. 清理各分区队列中未消费的 Page
        for (BlockingQueue<Page> q : partitionQueues) {
            Page qp;
            while ((qp = q.poll()) != null) {
                try { qp.close(); } catch (Exception ignored) {}
            }
        }
        // 3. 通知交换通道释放本 exchange 的发送侧资源（网络流 / 流复用根 / flusher 线程）。
        //    ★只有确认分发线程<b>全部退出</b>才释放：awaitTermination 超时（分发线程卡在
        //    upstream.next() / gRPC putNext 上，中断不会立刻生效）时它稍后仍会 send/complete，
        //    提前关共享流会让它撞上 "发送流已释放"。此时把释放交给最后一个退出的分发线程
        //    （见 distributeLoop 的 finally）。exchangeReleased 保证只减一次引用计数。
        //    distributorExecutor == null = 分发线程从未启动（open 在启动前就失败）⇒ 不可能再有发送，
        //    直接释放本次已登记的引用计数（未 open 则 exchangeAcquired=false，releaseExchange 空转）。
        if (distributorExecutor == null || stopConfirmed) {
            releaseExchange();
        }
        // 4. 关闭上游
        upstream.close();
        // 5. 释放各分发线程的独立 allocator
        for (BufferAllocator a : distributorAllocators) {
            try { a.close(); } catch (Exception ignored) {}
        }
        distributorAllocators.clear();
    }

    /**
     * 关闭本生产端持有的全部发送租约（引用计数 -1，最后一个租约才真正收尾共享流）。
     *
     * <p>仅当 {@link #doOpen} 确实登记过（{@code exchangeAcquired}）才执行 —— 未 open 就 close
     * 不能减计数，否则会把同 Worker 上仍在发送的邻居片段的计数误减到 0。重入由
     * {@code exchangeReleased} 兜住；每个租约对象自身也幂等。</p>
     */
    private void releaseExchange() {
        if (exchangeChannel == null || !exchangeAcquired) {
            return;
        }
        if (!exchangeReleased.compareAndSet(false, true)) {
            return;
        }
        for (ExchangeLease lease : acquiredLeases) {
            try { lease.close(); } catch (Throwable ignored) { }
        }
        acquiredLeases.clear();
    }

    // ==================== 分发 ====================

    /** 启动 N 个后台分发线程（open 时调用）。 */
    private void startDistributors() {
        distributorExecutor = Executors.newFixedThreadPool(numDistributors, r -> {
            Thread t = new Thread(r, "hash-partition-distributor");
            t.setDaemon(true);
            return t;
        });
        for (int d = 0; d < numDistributors; d++) {
            distributorExecutor.submit(this::distributeLoop);
        }
        System.err.printf("[HashPartition] %d distributor(s) started, %d partition(s)%n",
                numDistributors, numPartitions);
    }

    /**
     * 分发线程主循环：拉上游 → 逐行分区 → 填充每分区队列 → 上游耗尽后刷尽残留。
     * 各线程持有独立的 PartitionBuffer + BufferAllocator，线程间零共享（除输出队列）。
     */
    private void distributeLoop() {
        distributorsRunning.incrementAndGet();
        // 每个分发线程的分配器也是共享根的子：分区缓冲的堆外占用进统一账本，受共享根硬上限约束。
        BufferAllocator myAlloc = NodeMemory.shared()
                .newOperatorAllocator("hashpartition-dist", ResourceConfig.queryDirectLimit());
        synchronized (distributorAllocators) { distributorAllocators.add(myAlloc); }

        PartitionBuffer[] myBufs = new PartitionBuffer[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            myBufs[i] = new PartitionBuffer(i, myAlloc);
        }
        Schema mySchema = null;
        // 缓存 key 列索引，供批量哈希计算时按索引取列（避免每行 name lookup）
        final int[] keyIndices = new int[keyColumns.size()];
        boolean keyIndicesResolved = false;
        // 排障时间锚（纯日志）：分发循环起点 + 累计输入行数 —— 收尾时打印「读+发」总共多久，
        // 与 ShuffleReader 的首帧/收齐时刻对表，即可判定阻塞发生在「发不出去」还是「下游不消费」。
        final long profStartNanos = System.nanoTime();
        long profRowsIn = 0L;
        // 细分计时（纯日志）：acc[0]=上游取页 acc[1]=哈希/分行 acc[2]=gather 拷贝入桶
        // acc[3]=detach+recordOutput acc[4]=重建缓冲 acc[5]=emitTo（与全局 emitBlockNanos 同源）
        final long[] acc = new long[6];

        try {
            while (true) {
                if (distributorFailure != null) break;
                long tNext = System.nanoTime();
                Page input = upstream.next();
                acc[0] += System.nanoTime() - tNext;
                if (input.isEOF()) {
                    flushMyBuffers(myBufs, mySchema, acc);
                    return;
                }
                recordInput(input);
                // ★失败路径（例如键列解析时撞到嵌套类型闸门）也必须关页 + 归还内存，
                //   否则分发线程抛出后这张页永远不释放，分配器关闭时报 Memory was leaked。
                try {
                    profRowsIn += input.getRowCount();
                    if (mySchema == null) {
                        mySchema = input.getRoot().getSchema();
                        for (PartitionBuffer buf : myBufs) {
                            buf.initialize(mySchema);
                        }
                    }
                    for (PartitionBuffer buf : myBufs) {
                        buf.ensureTransfers(input.getRoot());
                    }
                    if (!keyIndicesResolved) {
                        resolveKeyIndices(input.getRoot().getSchema(), keyIndices);
                        keyIndicesResolved = true;
                    }
                    distributePageBulk(input, myBufs, mySchema, keyIndices, acc);
                } finally {
                    input.close();
                    releaseInput(input);
                }
            }
        } catch (InterruptedException e) {
            // 中断 = 任务被取消 / 线程池收尾：数据可能不完整，绝不能当作「正常跑完」——
            // 否则收尾时仍会补发 partitionEnd，接收端把残缺数据当完整结果。按失败传播。
            if (distributorFailure == null) distributorFailure = e;
            if (exchangeChannel != null) {
                notifyRemoteFail(e);   // 先尽力通知（此时中断标志尚可能已被清除，push 有机会发出）
            }
            Thread.currentThread().interrupt();
        } catch (Throwable t) {
            if (distributorFailure == null) distributorFailure = t;
            if (exchangeChannel != null) {
                notifyRemoteFail(t);   // 分布式：通知接收端失败（而非完成），防止其挂起
            } else {
                sendPartitionEndOnce();   // 单机：塞结束标记让消费端退出（保持 v1 行为）
            }
        } finally {
            closeMyBuffers(myBufs);
            if (distributorsDone.incrementAndGet() >= numDistributors) {
                // 最后一个分发线程收尾。sendPartitionEndOnce 可能抛（exchangeChannel.complete →
                // SenderStream.push 在流已失败时抛 RuntimeException）——必须兜住，否则 countDown 被跳过、
                // awaitDistributedDone 永久挂起 → 本发送方 COMPLETE 永不发出 → 接收端 partitionEnd 永不就位 →
                // 级联死锁。失败同样要 notifyRemoteFail 让接收端 fail-fast。
                try {
                    if (distributorFailure == null) {
                        sendPartitionEndOnce();
                    }
                } catch (Throwable t) {
                    if (distributorFailure == null) distributorFailure = t;
                    if (exchangeChannel != null) {
                        notifyRemoteFail(t);   // 发送完成标记失败 → 按失败传播给接收端
                    }
                } finally {
                    if (distributedDone != null) distributedDone.countDown();
                    System.err.printf("[SHUFFLE-PROF] producer 分发收尾：%d 行 / %d 桶，用时 %dms"
                                    + "（上游取页 %d / 哈希分行 %d / gather拷贝 %d / detach %d / 重建缓冲 %d / 发射 %d）ms%n",
                            profRowsIn, numPartitions, (System.nanoTime() - profStartNanos) / 1_000_000,
                            acc[0] / 1_000_000, acc[1] / 1_000_000, acc[2] / 1_000_000,
                            acc[3] / 1_000_000, acc[4] / 1_000_000, acc[5] / 1_000_000);
                }
            }
            // doClose 的 awaitTermination 超时 = 本线程当时还活着，释放被推迟到这里：
            // 最后一个<b>运行过</b>的分发线程退出后才不会再有任何 send/complete/fail
            //（正常路径已释放 → no-op）。用 running 而非 done：shutdownNow 取消掉的未启动任务
            // 永远走不到这里，done 永远差数，会让发送流引用计数泄漏、接收端永远收不到 COMPLETE。
            int remaining = distributorsRunning.decrementAndGet();
            if (closing.get() && remaining == 0) {
                releaseExchange();
            }
        }
    }

    /**
     * 给<b>每个</b>分区发结束标记（或失败），只发一次。
     *
     * <p><b>绝不静默中断</b>：旧实现在 {@code InterruptedException} 时 {@code break} —— 后续桶永远
     * 收不到 COMPLETE，接收端 {@code ShuffleReader.completedSenders} 永远差几个发送方，终端
     * Aggregate/Join 消费线程永久阻塞在 {@code partitionEnd} 上（线上表现为「已收 4/6 个完成」
     * 跑满 10 分钟看门狗）。现在改为：清除中断标志后<b>重试同一桶</b>（{@code put}/{@code push}
     * 被中断时消息确定未入队，重试不会重复计数），单个桶的运行时失败不阻止其余桶收尾（接收端
     * 仍需知道其它桶的结果），循环结束后恢复中断标志并抛第一个失败 —— 由调用方的
     * {@code catch (Throwable)} 记入 {@link #distributorFailure} 并 {@link #notifyRemoteFail}。</p>
     */
    private void sendPartitionEndOnce() {
        if (!partitionEndSent.getAndSet(true)) {
            boolean interrupted = false;
            Throwable firstFailure = null;
            for (int i = 0; i < numPartitions; i++) {
                while (true) {
                    try {
                        endPartitionTo(i);
                        break;
                    } catch (InterruptedException e) {
                        // 中断只发生在入队/写入被中断时（消息确定未交付）→ 清除标志后重试该桶
                        interrupted = true;
                    } catch (RuntimeException e) {
                        if (firstFailure == null) firstFailure = e;
                        break;   // 该桶彻底失败：继续给其它桶收尾，最后统一上抛
                    }
                }
            }
            if (interrupted) Thread.currentThread().interrupt();
            if (firstFailure instanceof RuntimeException re) throw re;
        }
    }

    /** 解析 key 列在 schema 中的索引（一次解析，后续按索引取列，避免 name lookup）。 */
    private void resolveKeyIndices(Schema schema, int[] keyIndices) {
        for (int i = 0; i < keyColumns.size(); i++) {
            for (int j = 0; j < schema.getFields().size(); j++) {
                if (schema.getFields().get(j).getName().equals(keyColumns.get(i))) {
                    NestedTypeSupport.requireSupported(schema.getFields().get(j), "哈希分布键");
                    keyIndices[i] = j;
                    break;
                }
            }
        }
    }

    /** 批量分区：先算所有行的分区号，再按分区 gather 拷贝（消除逐行 copyValueSafe 虚调用）。 */
    private void distributePageBulk(Page input, PartitionBuffer[] bufs,
                                     Schema schema, int[] keyIndices, long[] acc)
            throws InterruptedException {
        VectorSchemaRoot root = input.getRoot();
        int rowCount = input.getRowCount();

        // REPLICATE 不是「每行一个分区」（整页发往全部桶），走专门路径
        if (distribution == FragmentSpec.Distribution.REPLICATE) {
            replicatePageBulk(root, rowCount, bufs, schema, acc);
            return;
        }

        // 1. 批量计算每行的分区号（FIXED = 整页恒定一个桶，不做哈希）
        long tHash = System.nanoTime();
        int[] pids = distribution == FragmentSpec.Distribution.FIXED
                ? fixedPartitions(rowCount)
                : computePartitionsBulk(root, rowCount, keyIndices);
        // 2. 统计每个分区收多少行
        int[] counts = new int[numPartitions];
        for (int pid : pids) counts[pid]++;

        // 3. 收集每分区的源行号
        int[][] groups = new int[numPartitions][];
        int[] cursors = new int[numPartitions];
        for (int pid = 0; pid < numPartitions; pid++) {
            groups[pid] = new int[counts[pid]];
        }
        for (int r = 0; r < rowCount; r++) {
            int pid = pids[r];
            groups[pid][cursors[pid]++] = r;
        }
        acc[1] += System.nanoTime() - tHash;

        // 4. 按分区分组逐行拷贝（copyValueSafe 已验证正确，分组后顺序写缓存友好）
        long mark = System.nanoTime();
        for (int pid = 0; pid < numPartitions; pid++) {
            int[] srcRows = groups[pid];
            if (srcRows.length == 0) continue;
            PartitionBuffer buf = bufs[pid];

            for (int r = 0; r < srcRows.length; r++) {
                buf.appendRow(root, srcRows[r]);
                if (buf.rowCount >= BATCH_SIZE) {
                    acc[2] += System.nanoTime() - mark;          // 拷贝段
                    long tB = System.nanoTime();
                    Page out = Page.partitioned(buf.detach(schema), pid);
                    recordOutput(out);   // 页面仍存活时记录；emitTo 的 send 内 transferTo 会关闭源页
                    acc[3] += System.nanoTime() - tB;            // detach + record
                    long tE = System.nanoTime();
                    emitTo(pid, out);
                    acc[5] += System.nanoTime() - tE;            // 发射（与全局 emitBlockNanos 相互印证）
                    long tI = System.nanoTime();
                    buf.initialize(schema);
                    buf.ensureTransfers(root);
                    acc[4] += System.nanoTime() - tI;            // 重建 12 个 Arrow 向量 + transfer pair
                    mark = System.nanoTime();
                }
            }
        }
        acc[2] += System.nanoTime() - mark;                       // 尾段
    }

    /** FIXED（广播 probe 侧）：整页恒定发往一个桶 —— 切片在本机就地入桶，大表零跨机移动。 */
    private int[] fixedPartitions(int rowCount) {
        int[] pids = new int[rowCount];
        java.util.Arrays.fill(pids, Math.floorMod(fixedBucket, numPartitions));
        return pids;
    }

    /**
     * REPLICATE（广播 build 侧）：整页复制到<b>每一个</b>桶 —— 每台 Worker 各得一份完整副本，
     * 于是任意 probe 行都能在本机找到匹配。代价 = 右表数据量 × numPartitions 的网络与内存，
     * 只在右表「够小」时由 {@code JoinDecider} 选中。
     */
    private void replicatePageBulk(VectorSchemaRoot root, int rowCount, PartitionBuffer[] bufs, Schema schema,
                                    long[] acc) throws InterruptedException {
        long mark = System.nanoTime();
        for (int pid = 0; pid < numPartitions; pid++) {
            PartitionBuffer buf = bufs[pid];
            for (int r = 0; r < rowCount; r++) {
                buf.appendRow(root, r);
                if (buf.rowCount >= BATCH_SIZE) {
                    acc[2] += System.nanoTime() - mark;
                    long tB = System.nanoTime();
                    Page out = Page.partitioned(buf.detach(schema), pid);
                    recordOutput(out);   // 页面仍存活时记录；emitTo 的 send 内 transferTo 会关闭源页
                    acc[3] += System.nanoTime() - tB;
                    long tE = System.nanoTime();
                    emitTo(pid, out);
                    acc[5] += System.nanoTime() - tE;
                    long tI = System.nanoTime();
                    buf.initialize(schema);
                    buf.ensureTransfers(root);
                    acc[4] += System.nanoTime() - tI;
                    mark = System.nanoTime();
                }
            }
        }
        acc[2] += System.nanoTime() - mark;
    }

    /** 当前分发线程上游耗尽：刷尽所有分区残留行（不塞 partitionEnd）。 */
    private void flushMyBuffers(PartitionBuffer[] bufs, Schema schema, long[] acc) throws InterruptedException {
        if (schema == null) return;
        for (PartitionBuffer buf : bufs) {
            if (buf.rowCount > 0) {
                long tB = System.nanoTime();
                Page out = Page.partitioned(buf.detach(schema), buf.partitionId);
                recordOutput(out);   // 页面仍存活时记录；emitTo 的 send 内 transferTo 会关闭源页
                acc[3] += System.nanoTime() - tB;
                long tE = System.nanoTime();
                emitTo(buf.partitionId, out);
                acc[5] += System.nanoTime() - tE;
                long tI = System.nanoTime();
                buf.initialize(schema);
                acc[4] += System.nanoTime() - tI;
            }
        }
    }

    private static void closeMyBuffers(PartitionBuffer[] bufs) {
        for (PartitionBuffer buf : bufs) {
            buf.close();
        }
    }

    // ==================== 哈希 ====================

    /** 批量计算所有行的分区号（按预解析的列索引，避免 name lookup）。 */
    private int[] computePartitionsBulk(VectorSchemaRoot root, int rowCount, int[] keyIndices) {
        int[] pids = new int[rowCount];
        FieldVector[] keyVecs = new FieldVector[keyIndices.length];
        int[] keyTws = new int[keyIndices.length];
        for (int i = 0; i < keyIndices.length; i++) {
            keyVecs[i] = root.getVector(keyIndices[i]);
            keyTws[i] = keyVecs[i] != null ? RowAccumulator.typeWidthOf(keyVecs[i].getField().getType()) : -1;
        }
        for (int r = 0; r < rowCount; r++) {
            int hash = 0;
            for (int i = 0; i < keyIndices.length; i++) {
                FieldVector vec = keyVecs[i];
                if (vec == null || vec.isNull(r)) {
                    hash = 31 * hash;
                    continue;
                }
                int tw = keyTws[i];
                if (tw == 8) {
                    hash = 31 * hash + Long.hashCode(vec.getDataBuffer().getLong((long) r * 8));
                } else if (tw == 4) {
                    hash = 31 * hash + vec.getDataBuffer().getInt(r * 4);
                } else if (tw == 2) {
                    hash = 31 * hash + (int) vec.getDataBuffer().getShort((long) r * 2);
                } else if (tw == 1) {
                    hash = 31 * hash + (int) vec.getDataBuffer().getByte(r);
                } else {
                    hash = 31 * hash + vec.getObject(r).hashCode();
                }
            }
            pids[r] = (hash & Integer.MAX_VALUE) % numPartitions;
        }
        return pids;
    }

    // ==================== 失败传播 ====================

    /** 分发线程失败则抛出，避免消费端静默缺数据。 */
    private void checkDistributorFailure() {
        Throwable f = distributorFailure;
        if (f != null) {
            if (f instanceof RuntimeException re) throw re;
            if (f instanceof Error err) throw err;
            throw new RuntimeException("HashPartition distributor failed", f);
        }
    }

    // ==================== 分区缓冲内部类 ====================

    /**
     * 单个分区的行累加器 —— 持有该分区待发射的 Arrow 向量。
     * 每个分发线程持有自己的一套 PartitionBuffer，线程间不共享。
     */
    static class PartitionBuffer {
        final int partitionId;
        final BufferAllocator allocator;
        List<FieldVector> vectors;
        List<TransferPair> transfers;
        int rowCount;
        private VectorSchemaRoot lastSrcRoot;

        PartitionBuffer(int partitionId, BufferAllocator allocator) {
            this.partitionId = partitionId;
            this.allocator = allocator;
            this.vectors = new ArrayList<>();
            this.transfers = new ArrayList<>();
            this.rowCount = 0;
        }

        /** 根据 schema 创建新的累加器向量（丢弃旧数据）。 */
        void initialize(Schema schema) {
            close();
            vectors.clear();
            transfers.clear();
            rowCount = 0;
            lastSrcRoot = null;
            for (Field field : schema.getFields()) {
                FieldVector vec = field.createVector(allocator);
                vec.setInitialCapacity(BATCH_SIZE);
                vec.allocateNew();
                vectors.add(vec);
            }
        }

        /** 当输入页切换时刷新 TransferPair 映射。 */
        void ensureTransfers(VectorSchemaRoot srcRoot) {
            if (srcRoot == lastSrcRoot) return;
            transfers.clear();
            for (int i = 0; i < srcRoot.getFieldVectors().size(); i++) {
                transfers.add(srcRoot.getVector(i).makeTransferPair(vectors.get(i)));
            }
            lastSrcRoot = srcRoot;
        }

        /** 从源页拷贝一行到累加器末尾。 */
        void appendRow(VectorSchemaRoot src, int srcRow) {
            for (int col = 0; col < transfers.size(); col++) {
                transfers.get(col).copyValueSafe(srcRow, rowCount);
            }
            rowCount++;
        }

        /**
         * 批量 gather 拷贝：将源页中散布的 {@code srcRows} 行按列批量写入累加器。
         * 定长列直接 buffer setBytes + validity，变长列单遍 build offset/data，
         * 消除逐行 copyValueSafe 虚调用开销。
         */
        void appendRowsGather(VectorSchemaRoot src, int[] srcRows, int count) {
            if (count == 0) return;
            int dstStart = rowCount;

            // 确保容量
            for (FieldVector vec : vectors) {
                while (vec.getValueCapacity() < dstStart + count) {
                    vec.reAlloc();
                }
            }

            for (int col = 0; col < vectors.size(); col++) {
                FieldVector dst = vectors.get(col);
                FieldVector srcVec = src.getVector(col);
                int tw = RowAccumulator.typeWidthOf(dst.getField().getType());

                if (tw > 0) {
                    // 定长列：gather 拷贝
                    ArrowBuf srcBuf = srcVec.getDataBuffer();
                    ArrowBuf dstBuf = dst.getDataBuffer();
                    ArrowBuf dstVal = dst.getValidityBuffer();
                    boolean srcHasNulls = srcVec.getNullCount() > 0;
                    for (int i = 0; i < count; i++) {
                        int sr = srcRows[i];
                        int dr = dstStart + i;
                        dstBuf.setBytes((long) dr * tw, srcBuf, (long) sr * tw, tw);
                        if (srcHasNulls && srcVec.isNull(sr)) {
                            BitVectorHelper.unsetBit(dstVal, dr);
                        }
                    }
                } else if (dst instanceof BaseVariableWidthVector dv) {
                    // 变长列：gather 拷贝 offset + data
                    BaseVariableWidthVector sv = (BaseVariableWidthVector) srcVec;
                    ArrowBuf srcOff = sv.getOffsetBuffer();
                    ArrowBuf srcData = sv.getDataBuffer();
                    ArrowBuf dstData = dv.getDataBuffer();
                    ArrowBuf dstOff = dv.getOffsetBuffer();
                    ArrowBuf dstVal = dv.getValidityBuffer();
                    boolean srcHasNulls = srcVec.getNullCount() > 0;

                    long pos = dstStart > 0 ? dstOff.getInt((long) dstStart * 4) : 0;
                    for (int i = 0; i < count; i++) {
                        int sr = srcRows[i];
                        int dr = dstStart + i;
                        if (srcHasNulls && sv.isNull(sr)) {
                            BitVectorHelper.unsetBit(dstVal, dr);
                            dstOff.setInt((long) (dr + 1) * 4, (int) pos);
                        } else {
                            long start = srcOff.getInt((long) sr * 4);
                            long len = srcOff.getInt((long) (sr + 1) * 4) - start;
                            while (dstData.capacity() < pos + len) {
                                dst.reAlloc();
                                dstData = dv.getDataBuffer();
                                dstOff = dv.getOffsetBuffer();
                                dstVal = dv.getValidityBuffer();
                            }
                            dstData.setBytes(pos, srcData, start, len);
                            pos += len;
                            dstOff.setInt((long) (dr + 1) * 4, (int) pos);
                        }
                    }
                } else {
                    // FixedSizeList（Vector 类型）/List/Struct/Map 等复合列：copyValueSafe 逐行，
                    // 内部递归处理 child（appendRow 走同一机制）。
                    TransferPair tp = srcVec.makeTransferPair(dst);
                    for (int i = 0; i < count; i++) tp.copyValueSafe(srcRows[i], dstStart + i);
                }
            }
            // 关键：直接写 buffer 后 lastSet 不推进（alloc 后恒为 -1），
            // 之后 detach 的 setValueCount fillHoles 会从 lastSet+1 起清零所有 offset → var 全空。
            // 这里先把 valueCount 设好，让 fillHoles 变 no-op。同时更新 rowCount。
            rowCount += count;
            for (FieldVector vec : vectors) {
                if (vec instanceof BaseVariableWidthVector dv) {
                    dv.setLastSet(rowCount - 1);
                }
                vec.setValueCount(rowCount);
            }
        }

        /**
         * 将累加器数据"剥离"为一个独立的 VectorSchemaRoot，调用者接管其生命周期。
         * 调用后此 PartitionBuffer 的向量置空，需要重新 {@link #initialize}。
         */
        VectorSchemaRoot detach(Schema schema) {
            List<Field> fields = schema.getFields();
            for (int i = 0; i < vectors.size(); i++) {
                vectors.get(i).setValueCount(rowCount);
            }
            VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors, rowCount);
            vectors = new ArrayList<>();
            transfers.clear();
            rowCount = 0;
            lastSrcRoot = null;
            return root;
        }

        /** 释放此缓冲区的 Arrow 向量。 */
        void close() {
            for (FieldVector v : vectors) {
                try { v.close(); } catch (Exception ignored) {}
            }
            vectors.clear();
            transfers.clear();
            rowCount = 0;
        }
    }
}
