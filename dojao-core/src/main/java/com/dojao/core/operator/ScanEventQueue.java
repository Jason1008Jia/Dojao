package com.dojao.core.operator;

import com.dojao.core.runtime.worker.ScanEvent;

/**
 * 扫描事件队列：<b>页数 + 字节</b>双上界（{@link DistributedScanOperator} 的 Master 端接收队列）。
 *
 * <p>只按页数限制不够：单页字节随行宽相差两个数量级（窄行长页几 MB，宽 JSON 页几十 MB）。
 * 64 页 × 几十 MB 足以吃穿 Master 的 Arrow 预算 —— 线上表现为结果流接收路径抛
 * {@code OutOfMemoryException: Unable to allocate buffer ...}，被包装成「结果流异常中断且未收到
 * FINISHED/FAILED」（{@code staging_es_to_skb_info} 宽 JSON 扫描，分配 32MB 时已占 1.1GB）。
 * 字节记账与背压语义见 {@link ByteBoundedQueue}。</p>
 *
 * @author jiaxin
 * @version 1.1
 * @date 2026/11/20
 */
final class ScanEventQueue extends ByteBoundedQueue<ScanEvent> {

    ScanEventQueue(int capacity, long maxBytes) {
        super(capacity, maxBytes);
    }

    @Override
    protected long bytesOf(ScanEvent e) {
        if (e instanceof ScanEvent.ScanPage p && p.page() != null) {
            return Math.max(0L, p.page().getSizeInBytes());
        }
        return 0L;
    }

    /** 队列已关闭后的迟到页：立即释放，避免滞留在无人消费的队列里（Master 堆外泄漏）。 */
    @Override
    protected void reject(ScanEvent e) {
        if (e instanceof ScanEvent.ScanPage p && p.page() != null) {
            try {
                p.page().close();
            } catch (Throwable ignored) {
            }
        }
    }
}
