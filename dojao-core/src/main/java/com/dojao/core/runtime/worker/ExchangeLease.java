package com.dojao.core.runtime.worker;

import com.dojao.common.data.Page;

/**
 * 一次发送端登记的<b>发送租约（handle）</b>：{@link ExchangeChannel#acquireLease} 返回，
 * 之后的 {@code send}/{@code complete}/{@code fail} 全部经由本对象路由，不再按 exchangeId
 * 去通道的全局表里查流。
 *
 * <p><b>为什么需要句柄</b>：同一 Worker 上的多个 Producer 片段是<b>独立任务</b>，可能先后执行、
 * 生命周期完全不重叠。若 send/complete 只能按名字查全局表，上一代已收尾的流（flusher 尚未退出、
 * 仍在表里）会被下一代误复用。租约把「使用哪一代」固化在句柄里：每次 0→1 登记创建新的一代
 * （epoch），上一代的流即使还在收尾也不会被新一代看见。</p>
 *
 * <p><b>契约</b>：租约关闭（{@link #close()}）前必须完成本生产端的全部 send/complete/fail；
 * 关闭后再调用会 fail-fast（而不是把消息塞进无消费者的队列）。{@link #close()} 幂等。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/11/05
 */
public interface ExchangeLease extends AutoCloseable {

    /** 经本租约向目标 Worker 的指定桶发送一个数据页（页所有权移交）。 */
    void send(String toWorkerId, int bucketId, Page page) throws InterruptedException;

    /** 经本租约通知目标 Worker：本发送方的该桶数据已发完。 */
    void complete(String toWorkerId, int bucketId) throws InterruptedException;

    /** 经本租约通知目标 Worker：本发送方的该桶发送失败。 */
    void fail(String toWorkerId, int bucketId, Throwable cause);

    /**
     * 归还本生产端占用的发送资源（引用计数 -1，最后一个租约才真正收尾共享流）。
     * 幂等；关闭后不得再 send/complete/fail。
     */
    @Override
    void close();
}
