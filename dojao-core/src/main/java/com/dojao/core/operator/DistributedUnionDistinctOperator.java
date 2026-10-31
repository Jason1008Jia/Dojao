package com.dojao.core.operator;

import com.dojao.core.runtime.master.StageScheduler;
import com.dojao.core.runtime.master.WorkerRegistration;
import com.dojao.core.runtime.worker.ExchangeChannel;
import com.dojao.core.runtime.worker.WorkerGateway;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.List;

/**
 * 分布式 UNION distinct 算子（Master 侧）—— 把 UNION 去重的哈希表从 Master 挪到 Worker：
 * Master 只保留 {@link UnionAllOperator}（把两腿对齐成统一规范列名，无状态流式路由）+
 * {@link HashPartitionOperator} 按<b>全部输出列</b>哈希分区，逐行经 Master→Worker 数据直推
 * （{@code gateway.exchangeChannel()}）路由到每分区的独立接收端；Worker 侧每分区一个
 * {@link FragmentSpec.AggregateConsumer}（空聚合 = 全列 DISTINCT，每组一行）本地去重后回传
 * Master —— 全查询最后一处 Master 单机聚合（{@code HashAggregation[Distinct]}）被消除。
 *
 * <pre>
 *   doOpen → dispatchStage：
 *     StageScheduler.planUnionDistinct()   切分：N 个每分区 AggregateConsumer（无 Producer）
 *     dispatch Consumer（先）              同步注册各 Worker 的 ShuffleReader（本 op 是唯一发送方）
 *     开 Master→Worker exchangeChannel + 起后台 pump 线程：
 *       HashPartitionOperator(UnionAll, numDistributors=1) → 按全列哈希 → 逐桶直推 Worker
 *   doNext：从结果队列拉各分区去重输出页；全部 Consumer 完成 → EOF；任一失败 → fail-fast
 *   doClose：反向取消片段，排空队列，停 pump 线程并释放交换通道，关闭 UnionAll 子树
 * </pre>
 *
 * <p><b>为什么 Master 必须留在数据路径</b>：UNION 两腿列名可能不同（{@code SELECT id ... UNION
 * SELECT user_id ...}），分区按<b>列名</b>哈希 —— 只有 UnionAllOperator 先把每页对齐成统一规范
 * 列名，后续全列哈希才对"同一语义列"一致。但 Master 只做无状态转发（有界在途批缓冲），
 * 去重哈希表建在 Worker（内存可测可溢写），不持有去重态。</p>
 *
 * <p><b>正确性</b>：同一元组按全列哈希恒落唯一桶（co-partitioning 不变量）→ 唯一
 * AggregateConsumer 本地去重即该元组的完整去重，各分区去重结果并集 = 全局 distinct。</p>
 *
 * <p><b>单分发线程</b>：pump 用 {@code numDistributors=1} —— {@link UnionAllOperator#doNext}
 * 有状态（emitChild / carried / firstPages）非线程安全，多线程并发拉取会竞态。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/5
 */
public class DistributedUnionDistinctOperator extends AbstractDistributedOperator {

    /** Master 端对齐后的 UNION 单流（两腿列名在此统一，pid 归 0）。 */
    private final Operator child;
    /** 逐行去重键 = 全部 union 输出列。 */
    private final List<String> keyColumns;
    /** 每个 Worker 侧 AggregateConsumer 的溢写内存预算。 */
    private final long memoryLimit;

    /** Master→Worker 数据直推通道（派发后打开，closeProbes 释放）。 */
    private volatile ExchangeChannel exchangeChannel;
    /** 后台 pump 线程：跑 HashPartitionOperator 把 UnionAll 输出推给各 Worker。 */
    private volatile Thread pumpThread;

    public DistributedUnionDistinctOperator(String queryId, Operator child, List<String> keyColumns,
                                            long memoryLimit, int numPartitions,
                                            WorkerGateway gateway, List<WorkerRegistration> workers) {
        super("DistributedUnionDistinct", queryId, numPartitions, gateway, workers);
        this.child = child;
        this.keyColumns = List.copyOf(keyColumns);
        this.memoryLimit = memoryLimit;
    }

    @Override
    public List<ScanColumn> scanColumnSets() {
        // UNION 两腿扫描在 Master 侧的 child 子树内 —— 本算子不直接派发任何基表扫描
        return List.of();
    }

    @Override
    protected int dispatchStage() throws Exception {
        StageScheduler.UnionDistinctStage stage = StageScheduler.planUnionDistinct(
                queryId, workers, numPartitions, keyColumns, memoryLimit);

        // 先派发全部每分区 AggregateConsumer（同步注册各 Worker 的 ShuffleReader 接收端），
        // 再起 pump 推送 —— Master 泵 push 前所有接收端就绪（无竞态）。
        for (FragmentSpec consumer : withTerminal(stage.fragments())) {   // 分布式写 SinkStep + top-N 截断尾

            dispatch(consumer);
        }

        // Master→Worker 直推通道：LocalWorkerGateway 等不支持推送的网关 → 抛错 fail-fast
        // （Planner 门控已保证能走到这里的 gateway 必支持，此检查为防御）。
        ExchangeChannel channel = gateway.exchangeChannel();
        if (channel == null) {
            throw new IllegalStateException("网关不支持 Master→Worker 数据推送（exchangeChannel 返回 null）");
        }
        this.exchangeChannel = channel;

        // 后台 pump：单分发线程拉 UnionAll 对齐单流 → 按全列哈希 → 直推各桶归属 Worker。
        // 上游（UnionAll 及其两腿扫描）在 pump 线程内 open —— 与 Master 主线程无共享状态。
        String[] owners = stage.ownerWorkerIds();
        String[] udExchanges = stage.perPartitionExchanges().toArray(new String[0]);
        Thread pump = new Thread(() -> pump(owners, udExchanges, channel),
                "master-uniondistinct-" + queryId);
        pump.setDaemon(true);
        this.pumpThread = pump;
        pump.start();

        return stage.finalConsumerCount();
    }

    /** pump 线程体：HashPartitionOperator 分布式形态（发送方 = Master），等全部分发完成后收尾。 */
    private void pump(String[] owners, String[] udExchanges, ExchangeChannel channel) {
        HashPartitionOperator part = null;
        try {
            part = new HashPartitionOperator(child, keyColumns, numPartitions, /*numDistributors=*/1,
                    channel, owners, udExchanges, /*selfWorkerId=*/"master");
            part.open();
            part.awaitDistributedDone();   // 等全部分发线程完成并已 complete 所有桶
            if (part.distributorFailure() != null) {
                recordFailure(new RuntimeException("DistributedUnionDistinct 分发失败: "
                        + part.distributorFailure(), part.distributorFailure()));
            }
        } catch (Throwable t) {
            recordFailure(t);   // doNext 侧 fail-fast
        } finally {
            if (part != null) {
                try {
                    part.close();   // 关闭分发线程 + 上游 UnionAll（含两腿扫描）
                } catch (Throwable ignored) {
                }
            }
        }
    }

    @Override
    protected void closeProbes() {
        // 1. 停 pump：正常完成时 pump 已自然退出（awaitDistributedDone 返回）；中断兜底提前关闭
        Thread p = pumpThread;
        if (p != null && p != Thread.currentThread()) {
            p.interrupt();
            try {
                p.join(3_000);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
        // 2. 释放 Master→Worker 交换通道（停 flusher + 关到各 Worker 的 FlightClient，幂等）
        ExchangeChannel ch = exchangeChannel;
        if (ch != null) {
            exchangeChannel = null;
            try {
                gateway.releaseExchangeChannel(ch);
            } catch (Throwable ignored) {
            }
        }
    }

    @Override
    public List<Field> outputFields() {
        // UNION distinct 的终端输出列类型只存在于运行期数据页（UnionAll 对齐后的 Arrow schema）；
        // Master 派发前没有任何静态元数据可推导（两侧列名/类型可能不同），无法先建对类型的目标表。
        // 诚实地拒绝：这类形态的分布式写请先把 UNION 包成中间层（物化/视图）再 CTAS。
        throw new UnsupportedOperationException(
                "CTAS/INSERT OVERWRITE 暂不支持 UNION distinct 内层查询：其输出列类型仅在运行期数据页上，"
                        + "Master 无法在派发前静态推导目标表 schema");
    }
}
