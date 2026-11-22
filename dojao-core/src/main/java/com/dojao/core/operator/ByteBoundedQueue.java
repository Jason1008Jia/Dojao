package com.dojao.core.operator;

import com.dojao.core.config.ResourceConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Master 端接收队列的<b>页数 + 字节</b>双上界基类（扫描 / 分布式片段回传共用）。
 *
 * <p>只按页数限制不够：单页字节随行宽相差两个数量级，宽 JSON 页可达几十 MB，
 * 几十页足以吃穿 Master 的 Arrow 预算（线上表现为接收线程
 * {@code OutOfMemoryException: Unable to allocate buffer ...}，被包装成「结果流异常中断」）。
 * 入队前按 {@link #bytesOf(Object)} 记账，超预算即阻塞生产线程 → gRPC 流控 → Worker 停读。</p>
 *
 * <p><b>记账口径</b>：入队加、出队减 —— 已出队（下游正在消费）的那一页不再计入。
 * 非数据事件（FINISHED/FAILED/PROGRESS，字节 0）永不因字节上限阻塞：失败必须立刻送达。
 * 单页自身就超预算时，只要队列当前没有数据页就放行。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/11/20
 */
abstract class ByteBoundedQueue<T> extends LinkedBlockingQueue<T> {

    /** 数据页入队总字节预算。 */
    private final long maxBytes;
    private final Object bytesLock = new Object();
    /** 队列中（未出队）数据页的字节数。 */
    private long queuedBytes;

    ByteBoundedQueue(int capacity, long maxBytes) {
        super(capacity);
        this.maxBytes = Math.max(1L, maxBytes);
    }

    /** 事件占用的堆外字节数；非数据事件返回 0（不受字节预算约束）。 */
    protected abstract long bytesOf(T e);

    /** 接收队列字节预算：节点 Arrow 预算的 1/4，夹在 [64MB, 512MB]。 */
    static long defaultBudgetBytes() {
        long quarter = ResourceConfig.arrowDirectBudgetBytes() / 4;
        return Math.max(64L << 20, Math.min(512L << 20, quarter));
    }

    /** 队列中数据页的字节数（已出队的不计）。 */
    long queuedBytes() {
        synchronized (bytesLock) {
            return queuedBytes;
        }
    }

    @Override
    public void put(T e) throws InterruptedException {
        reserveUntil(e, Long.MAX_VALUE);
        try {
            super.put(e);
        } catch (InterruptedException | RuntimeException ex) {
            release(e);
            throw ex;
        }
    }

    @Override
    public boolean offer(T e) {
        if (!tryReserve(e)) return false;
        if (super.offer(e)) return true;
        release(e);
        return false;
    }

    @Override
    public boolean offer(T e, long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + Math.max(0L, unit.toNanos(timeout));
        if (!reserveUntil(e, deadline)) return false;
        try {
            if (super.offer(e, Math.max(0L, deadline - System.nanoTime()), TimeUnit.NANOSECONDS)) {
                return true;
            }
        } catch (InterruptedException | RuntimeException ex) {
            release(e);
            throw ex;
        }
        release(e);
        return false;
    }

    @Override
    public T take() throws InterruptedException {
        T e = super.take();
        release(e);
        return e;
    }

    @Override
    public T poll() {
        T e = super.poll();
        release(e);
        return e;
    }

    @Override
    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        T e = super.poll(timeout, unit);
        release(e);
        return e;
    }

    @Override
    public int drainTo(Collection<? super T> c) {
        List<T> tmp = new ArrayList<>();
        int n = super.drainTo(tmp);
        for (T e : tmp) release(e);
        c.addAll(tmp);
        return n;
    }

    @Override
    public int drainTo(Collection<? super T> c, int maxElements) {
        List<T> tmp = new ArrayList<>(Math.min(maxElements, 64));
        int n = super.drainTo(tmp, maxElements);
        for (T e : tmp) release(e);
        c.addAll(tmp);
        return n;
    }

    @Override
    public void clear() {
        List<T> tmp = new ArrayList<>();
        super.drainTo(tmp);
        for (T e : tmp) release(e);
    }

    // ==================== 字节记账 ====================

    private boolean tryReserve(T e) {
        long bytes = bytesOf(e);
        if (bytes <= 0) return true;
        synchronized (bytesLock) {
            if (queuedBytes > 0 && queuedBytes + bytes > maxBytes) return false;
            queuedBytes += bytes;
            return true;
        }
    }

    /** @return false = 到 deadline 还没腾出预算（{@code Long.MAX_VALUE} = 一直等） */
    private boolean reserveUntil(T e, long deadlineNanos) throws InterruptedException {
        long bytes = bytesOf(e);
        if (bytes <= 0) return true;
        synchronized (bytesLock) {
            while (queuedBytes > 0 && queuedBytes + bytes > maxBytes) {
                if (deadlineNanos == Long.MAX_VALUE) {
                    bytesLock.wait();
                } else {
                    long remainNanos = deadlineNanos - System.nanoTime();
                    if (remainNanos <= 0) return false;
                    bytesLock.wait(Math.max(1L, remainNanos / 1_000_000L));
                }
            }
            queuedBytes += bytes;
            return true;
        }
    }

    private void release(T e) {
        long bytes = bytesOf(e);
        if (bytes <= 0) return;
        synchronized (bytesLock) {
            queuedBytes = Math.max(0L, queuedBytes - bytes);
            bytesLock.notifyAll();
        }
    }
}
