package com.dojao.core.operator;

import com.dojao.common.data.Page;

/**
 * shuffle 拉取端（{@link ShuffleReader} / {@link HashPartitionOperator} 的分区队列）的
 * <b>页数 + 字节</b>双上界队列。
 *
 * <p>只按页数（8 页）限制不够：宽行页单页可达几十 MB，8 页 × 6 桶 = 数 GB 常驻，
 * 下游 join / 聚合还没开始就把 Worker 分配器吃穿。这里复用 Master 接收端的
 * {@link ByteBoundedQueue} 记账口径（入队加、出队减、单页超预算时队空放行）。</p>
 */
final class PageBoundedQueue extends ByteBoundedQueue<Page> {

    PageBoundedQueue(int capacity, long maxBytes) {
        super(capacity, maxBytes);
    }

    @Override
    protected long bytesOf(Page p) {
        // 结束标记 / EOF 不占数据预算（否则 6 个发送方的 COMPLETE 会被宽页堵住）
        if (p == null || p.isEOF() || p.isEndOfPartition()) {
            return 0;
        }
        return p.getSizeInBytes();
    }
}
