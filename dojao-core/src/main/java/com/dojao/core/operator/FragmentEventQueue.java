package com.dojao.core.operator;

import com.dojao.core.runtime.worker.FragmentEvent;

/**
 * 片段结果回传队列：<b>页数 + 字节</b>双上界（{@link AbstractDistributedOperator} 的 Master 端接收队列）。
 *
 * <p>与 {@link ScanEventQueue} 同因同修：窗口 / join / 聚合等分布式片段把输出页回传 Master，
 * 旧实现只限 32 页、不记字节。窗口阶段整表回传时，几十页宽页足以吃穿 Master 的 Arrow 预算
 * （线上 {@code row_number()} 派生表查询：接收线程分配 2MB 时已占 1.1GB，Worker 结果流被判
 * 「异常中断」→ 片段失败 → 上层聚合/limit 级联失败）。</p>
 *
 * <p>FINISHED / FAILED / PROGRESS 字节为 0 → 不受字节预算约束（失败必须立刻送达）。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/11/20
 */
final class FragmentEventQueue extends ByteBoundedQueue<FragmentEvent> {

    FragmentEventQueue(int capacity, long maxBytes) {
        super(capacity, maxBytes);
    }

    @Override
    protected long bytesOf(FragmentEvent e) {
        if (e instanceof FragmentEvent.FragmentData d && d.page() != null) {
            return Math.max(0L, d.page().getSizeInBytes());
        }
        return 0L;
    }

    /** 队列已关闭后的迟到页：立即释放，避免滞留在无人消费的队列里（Master 堆外泄漏）。 */
    @Override
    protected void reject(FragmentEvent e) {
        if (e instanceof FragmentEvent.FragmentData d && d.page() != null) {
            try {
                d.page().close();
            } catch (Throwable ignored) {
            }
        }
    }
}
