package com.dojao.core.operator;

import com.dojao.common.data.Page;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 分区数据源 → 拉取模型算子适配器 —— 把 {@link PartitionedSource}（如跨机 {@link ShuffleReader}）
 * 包装成下游 spillable 算子（{@link HashAggregationOperator} / {@link WindowSortOperator}）
 * 能消费的 {@link Operator}。
 *
 * <p>下游 spillable 算子通过 {@code upstream.next()} + {@code input.getPartitionId()} 路由到
 * 分区槽位，而 {@link ShuffleReader} 只实现 {@link PartitionedSource}（并行 join 按
 * {@code partitionQueue(pid)} 直接取流），不是 Operator。本适配器补齐这一缺口：把各桶有界
 * 队列<b>合并拉取</b>成单条 next() 流，数据页原样返回（保留 partitionId），丢弃
 * {@link Page#isEndOfPartition()} 结束标记，全部桶排空后返回全局 EOF —— 与
 * {@link HashPartitionOperator#doNext()} 的合并拉取完全同构（见其注释）。</p>
 *
 * <p><b>失败传导</b>：{@link ShuffleReader#onFail} 会把失败桶补上结束标记唤醒消费者，
 * 本算子每次拉取前检查 {@link PartitionedSource#distributorFailure()}，非 null 即 rethrow
 * （fail-fast），杜绝"上游失败被当成正常 EOF 静默吞掉"。</p>
 *
 * @author jiaxin
 * @version 3.0
 * @date 2026/8/20
 */
public final class PartitionedSourceOperator extends AbstractMeasurableOperator {

    private final PartitionedSource source;
    private final int numPartitions;
    @SuppressWarnings("unchecked")
    private BlockingQueue<Page>[] partitionQueues;
    private int endMarkerCount;
    private boolean mergedDrained;
    private int nextDrainPid;

    public PartitionedSourceOperator(PartitionedSource source, int numPartitions) {
        super("PartitionedSource", "PartitionedSource", 1);
        this.source = source;
        this.numPartitions = Math.max(1, numPartitions);
    }

    @Override
    protected void doOpen() throws Exception {
        source.open();
        @SuppressWarnings("unchecked")
        BlockingQueue<Page>[] q = new BlockingQueue[numPartitions];
        for (int pid = 0; pid < numPartitions; pid++) {
            q[pid] = source.partitionQueue(pid);
        }
        this.partitionQueues = q;
    }

    @Override
    protected Page doNext() throws Exception {
        checkSourceFailure();
        if (mergedDrained) return Page.eof();

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
                // 强制重标分区号 = 桶下标：本地 self 桶页自带 pid，跨机页经 copyPage 重建后丢失
                // partitionId（orElse 兜底成 0）→ 同一 key 会被分进 pid 与 0 两个槽重复聚合。
                // 桶下标才是真正的逻辑分区，对本地/网络一视同仁（本页 root 生命周期移交新页）。
                return p.withPartitionId(pid);
            }
            if (mergedDrained) {
                // 收齐结束标记 ≠ 正常结束：上游可能「先发完标记再失败」。返回 EOF 前必须复查一次，
                // 否则失败会被呈现成「成功、0 行」（P3：失败被当成功）。
                checkSourceFailure();
                return Page.eof();
            }
            if (!anySeen) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    mergedDrained = true;
                    checkSourceFailure();   // 取消不是失败：无失败时 checkSourceFailure 是 no-op
                    return Page.eof();
                }
            }
        }
    }

    @Override
    protected void doClose() throws Exception {
        // 排空各桶队列中未消费的 Page，再关闭数据源
        if (partitionQueues != null) {
            for (BlockingQueue<Page> q : partitionQueues) {
                Page p;
                while ((p = q.poll()) != null) {
                    try { p.close(); } catch (Exception ignored) { }
                }
            }
        }
        try {
            source.close();
        } catch (Throwable ignored) { }
    }

    /** 源 / 发送方失败即 rethrow（fail-fast，防止失败被当成 EOF 静默吞掉）。 */
    private void checkSourceFailure() {
        Throwable f = source.distributorFailure();
        if (f != null) {
            if (f instanceof RuntimeException re) throw re;
            if (f instanceof Error err) throw err;
            throw new RuntimeException("PartitionedSource failed", f);
        }
    }

    // ==================== 可观测 ====================

    public int numPartitions() { return numPartitions; }
}
