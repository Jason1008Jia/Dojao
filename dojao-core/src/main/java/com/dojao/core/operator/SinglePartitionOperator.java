package com.dojao.core.operator;

import com.dojao.common.data.Page;

/**
 * 分区归一算子 —— 把上游输出页的分区号强制归一到 pid=0。
 *
 * <p>全局 ORDER BY 排序复用 {@link WindowSortOperator}（空 groupKeyColumns = 全局单分组）。
 * 但 {@link AbstractSpillableOperator#accumulatePage} 按输入页的 {@link Page#getPartitionId()}
 * 分槽：若上游页带非 0 分区号（单机 {@code HashAggregationOperator} 输出
 * {@code Page.partitioned(..., pid)}、join 分区输出等），全局排序会被切成多段<b>各自排序</b>
 * 再拼起来 → 顺序错乱（跨 JVM 回传 Master 的结果页经 {@code Page.fromRoot} 重建无 pid，不受影响；
 * 单机路径是本算子的主要防护对象）。归一化后全部行恒进 partition 0，排序正确。</p>
 *
 * <p><b>页所有权</b>：{@link Page#withPartitionId} 是共享同一 root 的浅包装，源页所有权移交新页
 * （勿 close 源，只 close 新页一次）。EOF / 分区结束标记（root 为 null）原样透传。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/6
 */
public class SinglePartitionOperator extends AbstractMeasurableOperator implements MergeProgress {

    private final Operator upstream;

    public SinglePartitionOperator(Operator upstream) {
        super("SinglePartition", "SinglePartition", 1);
        this.upstream = upstream;
    }

    /** 归并进度透传上游：本算子只归一分区号，真正在归并的是下游的排序 / 聚合算子。 */
    @Override
    public long mergeProgressTicks() {
        return upstream instanceof MergeProgress mp ? mp.mergeProgressTicks() : 0L;
    }

    @Override
    protected void doOpen() throws Exception {
        upstream.open();
    }

    @Override
    protected Page doNext() throws Exception {
        while (true) {
            Page p = upstream.next();
            recordInput(p);
            // EOF / partitionEnd（root null）无数据 buffer，原样透传
            if (p.getRoot() == null) return p;
            // 已是 0 号分区或无分区号（accumulatePage 按 orElse(0) 归 0）→ 原样
            if (p.getPartitionId().map(id -> id == 0).orElse(true)) return p;
            // 换 0 号分区号：浅包装共享同一 root，源页丢弃不 close（root 由新页独占）
            return p.withPartitionId(0);
        }
    }

    @Override
    protected void doClose() throws Exception {
        upstream.close();
    }
}
