package com.dojao.core.runtime.worker;

import com.dojao.common.data.Page;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Worker↔Worker 的数据交换通道 —— 分布式 Hash 分区的发送端通过它把数据页推到
 * 该分区的归属 Worker（{@link ShuffleReader}）。
 *
 * <p>与 {@link WorkerGateway}（Master→Worker）、{@link MasterGateway}（Worker→Master）
 * 并列的第三种传输抽象，但这次是 <b>Worker↔Worker 对称</b>：本地实现
 * {@link LocalExchangeChannel}（同 JVM 直接 put 对端有界队列）；跨进程实现
 * {@code NetworkExchangeChannel}（Arrow Flight doExchange）。实现 {@link #isSelf}
 * 让自身/同进程桶走本地直写免网络跳。</p>
 *
 * <p><b>背压</b>：{@link #send} 是有界阻塞（实现方把数据推入对端有界缓冲 / Flight 流控）。
 * 对端消费慢 → 发送端阻塞 → 上游扫描随之阻塞，与 v1 单机分区队列语义一致。</p>
 *
 * <p><b>生命周期</b>：发送方正常扫完一个桶 → {@link #complete}；发送方失败 →
 * {@link #fail}（接收端据此 fail-fast，防止静默缺数据）。</p>
 *
 * @author jiaxin
 * @version 2.0
 * @date 2026/8/14
 */
public interface ExchangeChannel {

    /**
     * 向目标 Worker 的指定交换桶发送一个数据页（有界阻塞 = 背压）。
     *
     * <p>发送方 detach 后该页的所有权移交给接收方（接收方负责 close）。</p>
     *
     * @param toWorkerId 目标 Worker（拥有该桶的节点）
     * @param exchangeId 本次 shuffle 的标识（查询内唯一）
     * @param bucketId   逻辑分区号
     * @param page       数据页
     */
    void send(String toWorkerId, String exchangeId, int bucketId, Page page) throws InterruptedException;

    /**
     * 通知目标 Worker：本发送方的该桶数据已发完。
     * 目标侧据此累计每个桶的完成发送方数量，收齐全部后放 partitionEnd。
     */
    void complete(String toWorkerId, String exchangeId, int bucketId) throws InterruptedException;

    /**
     * 通知目标 Worker：本发送方的该桶发送失败。
     *
     * <p>接收端据此 fail-fast（不得让消费线程挂起等待永远不会来的 partitionEnd）。
     * 失败事件丢失时由 Master 侧看门狗兜底判 LOST（与 v1.1 扫描失败双保险同款）。</p>
     */
    void fail(String toWorkerId, String exchangeId, int bucketId, Throwable cause);

    /** 目标 Worker 是否就是本节点（自身桶可走本地直写免网络跳）。默认 false（远端）。 */
    default boolean isSelf(String toWorkerId) {
        return false;
    }

    /**
     * 获取一个<b>发送租约</b>（推荐入口）：{@code HashPartitionOperator.doOpen} 每个唯一的
     * exchangeId 调用一次，之后的 send/complete/fail 全部经返回的句柄路由。
     *
     * <p>网络实现把「使用哪一代发送流」固化在租约里：每次 0→1 登记创建新的一代（epoch），
     * 上一代已收尾但 flusher 尚未退出的流不会被新一代复用。同一 Worker 上的多个 Producer
     * 片段并发登记时共享当前代（引用计数），只有最后一个租约关闭才真正收尾共享流。</p>
     *
     * <p>默认实现退化为 {@link #acquire}/{@link #release} 配对，供无发送侧资源的本地实现
     * （{@code LocalExchangeChannel}）与测试 / 工具使用。</p>
     */
    default ExchangeLease acquireLease(String exchangeId) {
        acquire(exchangeId);
        return new ExchangeLease() {
            private final AtomicBoolean closed = new AtomicBoolean();

            @Override
            public void send(String toWorkerId, int bucketId, Page page) throws InterruptedException {
                requireOpen();
                ExchangeChannel.this.send(toWorkerId, exchangeId, bucketId, page);
            }

            @Override
            public void complete(String toWorkerId, int bucketId) throws InterruptedException {
                requireOpen();
                ExchangeChannel.this.complete(toWorkerId, exchangeId, bucketId);
            }

            @Override
            public void fail(String toWorkerId, int bucketId, Throwable cause) {
                requireOpen();
                ExchangeChannel.this.fail(toWorkerId, exchangeId, bucketId, cause);
            }

            @Override
            public void close() {
                if (closed.compareAndSet(false, true)) {
                    ExchangeChannel.this.release(exchangeId);
                }
            }

            private void requireOpen() {
                if (closed.get()) {
                    throw new IllegalStateException("exchange 发送租约已关闭 exchangeId=" + exchangeId
                            + "：send/complete 必须在 lease.close() 之前");
                }
            }
        };
    }

    /**
     * 登记一个即将使用该 exchange 发送流的<b>生产端</b>（旧接口，等价于丢弃
     * {@link #acquireLease} 返回的句柄）。
     *
     * <p><b>为什么需要</b>：同一 Worker 上的多个 Producer 片段共用「(目标 Worker, exchangeId)」
     * 这一条底层流（{@code NetworkExchangeChannel} 按 Worker 缓存的单例通道）。若每个片段各自
     * {@link #release}，先跑完的那个会在其它片段仍在发送时关闭共享流 —— 后续 {@code send} 落进
     * 无消费者的待发队列（容量满即永久阻塞），{@code complete} 静默丢失，接收端永远等不齐发送方
     * ⇒ 终端 Consumer 挂起。按引用计数登记后，只有最后一个生产端释放才真正关闭流。</p>
     *
     * <p>与 {@link #release} 成对调用；本地实现无资源（默认 no-op）。新代码应使用
     * {@link #acquireLease}：句柄方式不会再按名字查全局流表，天然隔离代际。</p>
     */
    default void acquire(String exchangeId) {
    }

    /**
     * 释放该 exchange 在本通道上的发送侧资源（本生产端已全部 {@code send}/{@code complete}/{@code fail}）。
     *
     * <p>网络实现据此在待发队列排空后关闭 doExchange 流、释放流复用根（Arrow buffer）并退出
     * flusher 线程 —— 否则每个查询的每条 (目标 Worker, exchangeId) 流都会永久留在通道里，
     * 随查询次数累积直接内存与线程。</p>
     *
     * <p><b>引用计数</b>：共享流要被<b>全部</b>生产端释放（登记数归零）才关闭；
     * 未登记过的 exchange 保持旧语义（立即释放，兼容直接使用通道的测试 / 工具）。本地实现无
     * 资源可释放（默认 no-op）。</p>
     */
    default void release(String exchangeId) {
    }
}
