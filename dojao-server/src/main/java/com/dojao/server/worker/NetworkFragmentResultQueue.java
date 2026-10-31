package com.dojao.server.worker;

import com.dojao.core.runtime.worker.FragmentEvent;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractQueue;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 片段结果的<b>网络回传队列</b> —— 适配 {@link FragmentExecutor} 的 {@link BlockingQueue} 契约：
 * 只实现生产者侧操作（{@code put} 数据页 / {@code offer} 终止事件），把每个事件经
 * {@link ResultStream}（doExchange "result/{taskId}"）推给 Master；读侧操作（take/poll 等）
 * 一律不支持 —— Worker 是纯生产者，消费者在 Master 侧（{@code NetworkWorkerGateway} 建的真实队列）。
 *
 * <p>{@code put} 同步阻塞 = 背压（Master 消费慢 → gRPC 流控阻塞 → 本任务线程阻塞 → 上游算子阻塞）；
 * {@code offer} 超时无实际意义（网络队列容量=流控窗口），返回发送是否成功。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/29
 */
public final class NetworkFragmentResultQueue extends AbstractQueue<FragmentEvent>
        implements BlockingQueue<FragmentEvent> {

    private static final Logger log = LoggerFactory.getLogger(NetworkFragmentResultQueue.class);

    private final ResultStream stream;

    public NetworkFragmentResultQueue(FlightClient masterClient, String taskId, BufferAllocator allocator) {
        this.stream = new ResultStream(masterClient, taskId, allocator);
    }

    @Override
    public void put(FragmentEvent e) {
        send(e);   // 数据页同步发送 = 背压（Master 消费慢 → gRPC 流控阻塞 → 本任务线程阻塞 → 上游算子阻塞）
    }

    @Override
    public boolean offer(FragmentEvent e) {
        return sendQuiet(e);
    }

    @Override
    public boolean offer(FragmentEvent e, long timeout, TimeUnit unit) {
        return sendQuiet(e);
    }

    /** 关闭底层回传流（任务线程收尾时调用，被取消也能主动完成流避免悬挂）。 */
    public void close() {
        stream.close();
    }

    private void send(FragmentEvent e) {
        switch (e) {
            case FragmentEvent.FragmentData d -> stream.sendPage(d.page());
            case FragmentEvent.FragmentProgress p -> stream.progress();
            case FragmentEvent.FragmentFinished f -> stream.finished(f.rowCount());   // 分布式写行数经 FINISHED 回传
            case FragmentEvent.FragmentFailed f -> stream.failed(f.cause());
        }
    }

    private boolean sendQuiet(FragmentEvent e) {
        try {
            send(e);
            return true;
        } catch (RuntimeException ex) {
            log.warn("片段结果网络回传失败: {}", e, ex);
            return false;
        }
    }

    // ==================== 读侧（消费者在 Master，Worker 不读） ====================

    @Override
    public FragmentEvent take() {
        throw new UnsupportedOperationException("network result queue 是生产者");
    }

    @Override
    public int remainingCapacity() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int drainTo(java.util.Collection<? super FragmentEvent> c) {
        throw new UnsupportedOperationException("network result queue 是生产者");
    }

    @Override
    public int drainTo(java.util.Collection<? super FragmentEvent> c, int maxElements) {
        throw new UnsupportedOperationException("network result queue 是生产者");
    }

    @Override
    public FragmentEvent poll(long timeout, TimeUnit unit) {
        throw new UnsupportedOperationException("network result queue 是生产者");
    }

    @Override
    public FragmentEvent poll() {
        throw new UnsupportedOperationException("network result queue 是生产者");
    }

    @Override
    public FragmentEvent peek() {
        throw new UnsupportedOperationException("network result queue 是生产者");
    }

    @Override
    public Iterator<FragmentEvent> iterator() {
        return Collections.emptyIterator();
    }

    @Override
    public int size() {
        return 0;
    }
}
