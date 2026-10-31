package com.dojao.core.operator;

import com.dojao.common.data.Page;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Shuffle 接收端 —— 运行在归属 Worker 上，接收各发送方经 {@code ExchangeChannel} 推来的
 * 数据页，缓冲进本地有界队列。它是"4 个逻辑分区实际内存分布在各个 Worker"的落点：
 * 每个 Worker 只为它拥有的桶持有缓冲（非归属桶零内存、立即结束）。
 *
 * <p>实现 {@link PartitionedSource}，对外暴露 {@link #partitionQueue(int)} —— 签名与
 * {@link HashPartitionOperator} 完全一致，下游 {@link HashJoinOperator} 零改动消费
 * （数据来自本地分区器还是跨机 shuffle，对 join 不可见）。</p>
 *
 * <p><b>归属桶 vs 非归属桶</b>：发送方按 owner 映射只把数据推给归属 Worker。
 * 每个桶有 {@code numSenders} 个发送方，全部 {@link #onComplete} 后在该桶队列尾放
 * {@link Page#partitionEnd(int)}（与单机分区结束语义一致）；非归属桶构造时立即置空并放
 * 结束标记，消费者立刻结束。</p>
 *
 * <p><b>背压</b>：有界队列 put 阻塞 = 发送方阻塞 = 上游扫描阻塞，与单机分区队列语义一致。</p>
 *
 * @author jiaxin
 * @version 2.0
 * @date 2026/8/14
 */
public final class ShuffleReader implements PartitionedSource {

    /** 每桶入队缓冲容量（与 HashPartitionOperator 的 PARALLEL_QUEUE_CAPACITY 同量级，有界 put 提供背压）。 */
    private static final int QUEUE_CAPACITY = 8;

    private final String exchangeId;
    private final int numPartitions;
    private final int numSenders;

    /** 每个桶的本地有界队列。 */
    private final BlockingQueue<Page>[] bucketQueues;
    /**
     * 收到数据页总数（所有归属桶累计）。Worker 心跳线程据此判断「输入仍在流动 = 任务在推进」：
     * 单调递增 → 阻塞式算子（聚合 Phase 1 / 溢写 / 排序）虽不吐结果但仍在消费输入，Master 结果
     * 看门狗不应误杀；长时间不变 → 真挂起（无输入流动），不发心跳，看门狗照常 fail-fast。
     */
    private final AtomicLong pagesReceived = new AtomicLong();
    /** 每个桶已完成的发送方计数。 */
    private final AtomicInteger[] completedSenders;
    /** 每个桶收到的数据页数（停滞诊断：区分「没数据」与「缺 COMPLETE」）。 */
    private final AtomicLong[] bucketPages;
    /** 最近一次收到页 / 完成 / 失败的时刻（ns）：停滞诊断据此给出「多久没有任何动静」。 */
    private final AtomicLong lastActivityNanos = new AtomicLong(System.nanoTime());
    /** 每个桶是否已收齐（发过 partitionEnd）。 */
    private final boolean[] bucketEnded;
    /** 发送方失败记录（消费端 / 执行器检查，非 null 即 fail-fast）。 */
    private volatile Throwable failure;

    /**
     * 排障时间锚（纯日志，正常路径三行）：接收端构建时刻 + 首帧数据页到达时刻。二者相减即
     * 「发送方多久才把第一页送到」，用来把「页到得慢（交换/上游）」与「消费得慢（本地下游算子）」
     * 分开 —— 只看总耗时无法区分这两类根因。
     */
    private final long createdNanos = System.nanoTime();
    private final AtomicLong firstPageNanos = new AtomicLong(-1L);

    /**
     * 停滞诊断（挂起排障用，正常完成零输出）：构造 8s/30s 各检查一次，任一归属桶未收齐
     * partitionEnd 即打印 [exchange, bucket, 已完成发送方/总, 队列余量]。完成&lt;总且队列=0 →
     * 缺 COMPLETE（发送方没到）；完成&lt;总且队列=8 → 发送方被有界队列阻塞（下游 join 没消费 = 死锁）。
     */
    private final ScheduledExecutorService stallTimer;
    /** 收齐全部归属桶后回调（用于从 ExchangeManager 路由表摘除自己）；可为 null。 */
    private final Runnable onDone;
    /** 看门狗线程是否已停（幂等）。 */
    private final AtomicBoolean watchdogStopped = new AtomicBoolean();
    /** 收齐回调是否已触发（幂等）。 */
    private final AtomicBoolean done = new AtomicBoolean();

    /**
     * @param exchangeId   本次 shuffle 的标识（查询内唯一，与发送端一致）
     * @param numPartitions 逻辑分区数
     * @param numSenders    每个桶的发送方数量（= 参与该 shuffle 的 Worker 数）
     * @param owned         owned[pid] = 本 Worker 是否拥有该桶（拥有才收数据）
     */
    /** 兼容旧构造：不注册完成回调（探针 / 手工构造场景）。 */
    public ShuffleReader(String exchangeId, int numPartitions, int numSenders, boolean[] owned) {
        this(exchangeId, numPartitions, numSenders, owned, null);
    }

    /**
     * @param onDone 全部归属桶收齐（或失败兜底结束）后回调一次，用于从交换注册表摘除接收端；
     *               可为 null。回调在调用 {@link #onComplete}/{@link #onFail} 的发送方线程执行。
     */
    @SuppressWarnings("unchecked")
    public ShuffleReader(String exchangeId, int numPartitions, int numSenders, boolean[] owned, Runnable onDone) {
        this.exchangeId = exchangeId;
        this.onDone = onDone;
        this.numPartitions = Math.max(1, numPartitions);
        this.numSenders = Math.max(1, numSenders);
        this.bucketQueues = new BlockingQueue[this.numPartitions];
        this.completedSenders = new AtomicInteger[this.numPartitions];
        this.bucketPages = new AtomicLong[this.numPartitions];
        this.bucketEnded = new boolean[this.numPartitions];
        for (int i = 0; i < this.numPartitions; i++) {
            bucketQueues[i] = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
            completedSenders[i] = new AtomicInteger();
            bucketPages[i] = new AtomicLong();
            if (owned != null && !owned[i]) {
                // 非归属桶：没有发送方，立即置空并放结束标记（消费者立刻结束，零内存）
                bucketEnded[i] = true;
                bucketQueues[i].offer(Page.partitionEnd(i));
            }
        }
        // 停滞诊断看门狗：只在有归属桶长时间未收齐时打印（成功路径静默）
        this.stallTimer = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "shuffle-stall-watchdog");
            t.setDaemon(true);
            return t;
        });
        // 周期性而非一次性：挂起可能远超 30s（线上曾 RUNNING 100 分钟），两次一次性日志之后
        // 就再无任何线索。每 30s 打印一次直到收齐（收齐即 shutdown，成功路径仍静默）。
        stallTimer.scheduleWithFixedDelay(this::reportStall, 8, 30, TimeUnit.SECONDS);
        // 一次性打印接收端配置：验证 numSenders / 归属桶掩码与发送端 owner 映射一致
        System.err.printf("[ShuffleReader] exchange=%s partitions=%d senders=%d owned=%s%n",
                exchangeId, numPartitions, numSenders, java.util.Arrays.toString(owned));
        finishIfDone();   // 全部桶都非归属（本节点不接收该 exchange）→ 立即停看门狗并摘除
    }

    /**
     * 打印仍未收齐的归属桶状态（定位挂起：缺数据 vs 缺 COMPLETE vs 发送方死锁）。
     *
     * <p>每桶给出：已完成发送方 / 总、队列余量、已收页数、距离上一次任何动静的毫秒数。
     * 组合判读：{@code 完成<总 且 页数不再增长 且 队列=0} → 发送方没到（丢 COMPLETE / 发送端死锁）；
     * {@code 完成<总 且 队列=8} → 发送方被有界队列阻塞（下游没消费 = 死锁）。</p>
     */
    private void reportStall() {
        try {
            if (stallTimer.isShutdown()) {
                return;
            }
            long idleMs = (System.nanoTime() - lastActivityNanos.get()) / 1_000_000;
            for (int i = 0; i < numPartitions; i++) {
                if (!bucketEnded[i]) {
                    int done = completedSenders[i].get();
                    System.err.printf("[SHUFFLE-STALL] exchange=%s bucket=%d 完成=%d/%d 队列余量=%d 已收页=%d 最后活动=%dms前 未收齐%n",
                            exchangeId, i, done, numSenders, bucketQueues[i].size(),
                            bucketPages[i].get(), idleMs);
                }
            }
        } catch (Throwable ignored) {
        }
    }

    // ==================== 发送方回调（ExchangeChannel 调用） ====================

    /** 接收一个数据页（发送方线程调用；有界 put 阻塞 = 背压）。 */
    public void onPage(int bucketId, Page page) throws InterruptedException {
        long now = System.nanoTime();
        if (firstPageNanos.compareAndSet(-1L, now)) {
            System.err.printf("[SHUFFLE-PROF] exchange=%s bucket=%d 首帧到达 +%dms%n",
                    exchangeId, bucketId, (now - createdNanos) / 1_000_000);
        }
        pagesReceived.incrementAndGet();
        bucketPages[bucketId].incrementAndGet();
        lastActivityNanos.set(now);
        bucketQueues[bucketId].put(page);
    }

    /** 一个发送方完成该桶（累计；收齐全部 numSenders 后在队列尾放 partitionEnd）。 */
    public void onComplete(int bucketId) throws InterruptedException {
        lastActivityNanos.set(System.nanoTime());
        boolean endedNow = false;
        synchronized (this) {
            // incrementAndGet 原子递增，恰好返回 numSenders 的那次才放结束标记，只放一次
            if (completedSenders[bucketId].incrementAndGet() >= numSenders && !bucketEnded[bucketId]) {
                bucketEnded[bucketId] = true;
                endedNow = true;
            }
        }
        if (endedNow) {
            System.err.printf("[SHUFFLE-PROF] exchange=%s bucket=%d 收齐 %d 个发送方：%d 页，+%dms%n",
                    exchangeId, bucketId, numSenders, pagesReceived.get(),
                    (System.nanoTime() - createdNanos) / 1_000_000);
            // 结束标记可能阻塞（队列满）→ 不持锁放，避免拖住其它桶的完成判定
            bucketQueues[bucketId].put(Page.partitionEnd(bucketId));
            finishIfDone();
        }
    }

    /**
     * 一个发送方失败：记录失败，并尽力唤醒阻塞的消费线程（offer partitionEnd 到未结束桶）。
     * 消费线程被唤醒后经 {@link #distributorFailure()} 检查到失败即 rethrow（fail-fast）。
     * 完整兜底（Master 看门狗 / 反向 cancel）由 fragment 生命周期层负责（Phase C）。
     */
    public void onFail(int bucketId, Throwable cause) {
        this.failure = cause;
        synchronized (this) {
            for (int i = 0; i < numPartitions; i++) {
                if (!bucketEnded[i]) {
                    bucketEnded[i] = true;
                    try {
                        bucketQueues[i].offer(Page.partitionEnd(i));
                    } catch (Exception ignored) {
                        // 尽力而为：队列满则依赖 fragment 层反向 cancel 兜底
                    }
                }
            }
        }
        finishIfDone();
    }

    // ==================== PartitionedSource ====================

    @Override
    public int numPartitions() {
        return numPartitions;
    }

    @Override
    public BlockingQueue<Page> partitionQueue(int bucketId) {
        return bucketQueues[bucketId];
    }

    @Override
    public Throwable distributorFailure() {
        return failure;
    }

    @Override
    public void open() {
        // 接收由发送方经 ExchangeChannel 驱动，无需主动启动
    }

    @Override
    public void close() {
        // 桶缓冲随 fragment 关闭统一释放；这里只负责停掉停滞诊断线程
        // （单线程 ScheduledExecutor 的核心线程不会因任务结束而退出，必须显式 shutdown）。
        if (watchdogStopped.compareAndSet(false, true)) {
            stallTimer.shutdownNow();
        }
    }

    /**
     * 全部归属桶都收齐（或失败兜底）后收尾：停看门狗线程 + 回调摘除接收端。
     * 由 {@link #onComplete}/{@link #onFail}/{@link #close()} 触发，幂等。
     */
    private synchronized void finishIfDone() {
        for (boolean ended : bucketEnded) {
            if (!ended) return;
        }
        close();
        if (done.compareAndSet(false, true) && onDone != null) {
            try {
                onDone.run();
            } catch (Throwable ignored) {
                // 摘除失败不应影响收尾（最多保留一个空接收端到 Worker 停止）
            }
        }
    }

    // ==================== Getter ====================

    public String exchangeId() {
        return exchangeId;
    }

    /** 收到数据页总数（心跳 / 停滞诊断用：单调递增 = 输入在流动）。 */
    public long pagesReceived() {
        return pagesReceived.get();
    }

    /** 桶是否已收齐（已放 partitionEnd）。 */
    public boolean bucketEnded(int bucketId) {
        return bucketEnded[bucketId];
    }

    /** 指定桶已收到的数据页数（停滞诊断用；探针据此验证按桶计量）。 */
    public long bucketPages(int bucketId) {
        return bucketPages[bucketId].get();
    }
}
