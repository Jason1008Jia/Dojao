package com.dojao.server.worker;

import com.dojao.core.runtime.worker.ScanEvent;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.memory.BufferAllocator;

import java.util.AbstractQueue;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 分区扫描结果的<b>网络回传队列</b> —— 与 {@link NetworkFragmentResultQueue} 对称，适配
 * 扫描任务的 {@link BlockingQueue<ScanEvent>} 契约（{@code ScanPage} / {@code ScanFinished} /
 * {@code ScanFailed}），经 {@link ResultStream} 推给 Master。读侧操作不支持（Worker 纯生产者）。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/29
 */
public final class NetworkScanResultQueue extends AbstractQueue<ScanEvent>
        implements BlockingQueue<ScanEvent> {

    private final ResultStream stream;

    public NetworkScanResultQueue(FlightClient masterClient, String taskId, BufferAllocator allocator) {
        this.stream = new ResultStream(masterClient, taskId, allocator);
    }

    @Override
    public void put(ScanEvent e) {
        send(e);   // 数据页同步发送 = 背压（Master 消费慢 → gRPC 流控阻塞 → 本任务线程阻塞 → 扫描算子阻塞）
    }

    @Override
    public boolean offer(ScanEvent e) {
        return sendQuiet(e);
    }

    @Override
    public boolean offer(ScanEvent e, long timeout, TimeUnit unit) {
        return sendQuiet(e);
    }

    /** 关闭底层回传流（任务线程收尾时调用）。 */
    public void close() {
        stream.close();
    }

    private void send(ScanEvent e) {
        switch (e) {
            case ScanEvent.ScanPage p -> stream.sendPage(p.page());
            case ScanEvent.ScanFinished f -> stream.finished();
            case ScanEvent.ScanFailed f -> stream.failed(f.cause());
        }
    }

    private boolean sendQuiet(ScanEvent e) {
        try {
            send(e);
            return true;
        } catch (RuntimeException ex) {
            return false;
        }
    }

    // ==================== 读侧（消费者在 Master，Worker 不读） ====================

    @Override
    public ScanEvent take() {
        throw new UnsupportedOperationException("network result queue 是生产者");
    }

    @Override
    public int remainingCapacity() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int drainTo(java.util.Collection<? super ScanEvent> c) {
        throw new UnsupportedOperationException("network result queue 是生产者");
    }

    @Override
    public int drainTo(java.util.Collection<? super ScanEvent> c, int maxElements) {
        throw new UnsupportedOperationException("network result queue 是生产者");
    }

    @Override
    public ScanEvent poll(long timeout, TimeUnit unit) {
        throw new UnsupportedOperationException("network result queue 是生产者");
    }

    @Override
    public ScanEvent poll() {
        throw new UnsupportedOperationException("network result queue 是生产者");
    }

    @Override
    public ScanEvent peek() {
        throw new UnsupportedOperationException("network result queue 是生产者");
    }

    @Override
    public Iterator<ScanEvent> iterator() {
        return Collections.emptyIterator();
    }

    @Override
    public int size() {
        return 0;
    }
}
