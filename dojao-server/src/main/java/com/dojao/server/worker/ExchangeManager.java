package com.dojao.server.worker;

import com.dojao.common.memory.NodeMemory;
import com.dojao.core.operator.ShuffleReader;
import com.dojao.core.runtime.worker.ExchangeChannel;
import org.apache.arrow.memory.BufferAllocator;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Worker 侧交换注册表 —— 把"本 Worker 是哪些 shuffle 的接收端"集中管理。
 *
 * <p>每个 Worker 进程一个实例，持有共享路由表 {@code workerId → (exchangeId → ShuffleReader)}：
 * <ul>
 *   <li><b>接收端注册</b>：{@link #registerReader} 把本 Worker 收到某 exchange 数据的
 *       {@link ShuffleReader} 登记进路由表（发送方 push 前必须完成）；</li>
 *   <li><b>发送通道</b>：{@link #channelFor} 返回本 Worker 的 {@link LocalExchangeChannel}，
 *       经同一路由表把页推到目标 Worker 的接收端。</li>
 * </ul>
 *
 * <p>这是 {@link LocalExchangeChannel} 私有的 {@code bus} 的正式化：v2 起由本类统一持有，
 * 伪分布式（单 JVM）下多个 Worker 的 ExchangeManager 共享同一个 {@code bus}，实现
 * Worker↔Worker 直连；跨进程版每个 Worker 进程一个独立 bus，通道实现换 Flight 网络传输。</p>
 *
 * @author jiaxin
 * @version 2.1
 * @date 2026/8/16
 */
public class ExchangeManager {

    private final String selfWorkerId;
    /** 共享路由表：workerId → (exchangeId → ShuffleReader)。子类（网络模式）需访问以构造发送通道。 */
    protected final ConcurrentHashMap<String, ConcurrentHashMap<String, ShuffleReader>> bus;
    /**
     * 本 Worker 交换层的共享分配器：跨线程交接的页（Producer 分区页、Consumer join 输出页）统一
     * 把 buffer 所有权移交到这里，避免接收端 / Master 延迟 close 的页挂在创建它的算子分配器上，
     * 算子关闭时误报内存泄漏。随 Worker 停止而关闭（此时所有页早已 close）。
     */
    private final BufferAllocator allocator;
    /** bus 读写互斥（注册 / 摘除 / 累计页数需要一致视图，避免摘除瞬间漏计或重复计）。 */
    private final Object busLock = new Object();
    /** 已摘除接收端的累计收页数（摘除后仍计入 {@link #totalPagesReceived()}，保证单调不减）。 */
    private long retiredPages;

    /** 独立 bus（生产：每 Worker 进程一个）。 */
    public ExchangeManager(String selfWorkerId) {
        this(selfWorkerId, new ConcurrentHashMap<>());
    }

    /** 共享 bus（伪分布式：单 JVM 多个 Worker 共用同一路由表）。 */
    public ExchangeManager(String selfWorkerId,
                           ConcurrentHashMap<String, ConcurrentHashMap<String, ShuffleReader>> sharedBus) {
        this(selfWorkerId, sharedBus, NodeMemory.shared().newChildAllocator("exchange/manager", 0));
    }

    /** 共享 bus + 注入分配器（测试可复用外部 RootAllocator 统一关闭）。 */
    public ExchangeManager(String selfWorkerId,
                           ConcurrentHashMap<String, ConcurrentHashMap<String, ShuffleReader>> sharedBus,
                           BufferAllocator allocator) {
        this.selfWorkerId = selfWorkerId;
        this.bus = sharedBus;
        this.allocator = allocator;
    }

    public String selfWorkerId() { return selfWorkerId; }

    /** 交换层共享分配器（跨线程页交接的 buffer 归属地）。 */
    public BufferAllocator allocator() { return allocator; }

    /** 关闭共享分配器（Worker 停止时调用；此时所有交接页应已 close）。 */
    public void close() {
        try { allocator.close(); } catch (Throwable ignored) { }
    }

    /**
     * 本 Worker 的发送通道（路由到共享 bus 中目标 Worker 的接收端）。
     *
     * <p>按部署形态返回实现：单 JVM（伪分布式）默认 {@link LocalExchangeChannel}；
     * 多 JVM（真分布式）子类 {@code NetworkExchangeManager} 覆写返回 {@link NetworkExchangeChannel}
     * （内嵌 local 快路径），算子只依赖 {@link ExchangeChannel} 接口，零改动。</p>
     */
    public ExchangeChannel channelFor() {
        return new LocalExchangeChannel(selfWorkerId, bus, allocator);
    }

    /** 注册本 Worker 收到某 exchange 数据的 ShuffleReader（发送方 push 前必须完成）。 */
    public void registerReader(String exchangeId, ShuffleReader reader) {
        synchronized (busLock) {
            bus.computeIfAbsent(selfWorkerId, k -> new ConcurrentHashMap<>()).put(exchangeId, reader);
        }
    }

    /**
     * 摘除已收齐的接收端 —— 查询结束后必须移除，否则每个 exchange 的 ShuffleReader（及其
     * 每桶阻塞队列、完成计数数组）会永久留在 bus 里随查询次数累积。
     *
     * <p>摘除前把其收页数并入 {@link #retiredPages}，保证 {@link #totalPagesReceived()} 仍是
     * 单调递增的「累计」口径（池看门狗依赖它判断还有没有数据在流动）。</p>
     */
    public void unregisterReader(String exchangeId) {
        synchronized (busLock) {
            Map<String, ShuffleReader> readers = bus.get(selfWorkerId);
            if (readers == null) return;
            ShuffleReader removed = readers.remove(exchangeId);
            if (removed != null) {
                retiredPages += removed.pagesReceived();
            }
            if (readers.isEmpty()) {
                bus.remove(selfWorkerId, readers);
            }
        }
    }

    /**
     * 本 Worker 所有 exchange 接收端<b>累计</b>收到的数据页数（单调递增）—— 池看门狗判断「还有没有
     * 数据在流动」的进度信号：页面仍在流入 ⇒ 阻塞中的算子其实在正常消费输入（聚合 Phase 1 / 溢写 /
     * 排序期间的合法沉默），绝不能判死锁；长时间一动不动才是真停滞。
     *
     * <p>语义上可行是因为它只增不减：接收端收齐后会被摘除，但其收页数在摘除时并入
     * {@link #retiredPages}，故「与上一次采样相同」恰等于「这段时间一页都没进」。</p>
     */
    public long totalPagesReceived() {
        synchronized (busLock) {
            long total = retiredPages;
            Map<String, ShuffleReader> readers = bus.get(selfWorkerId);
            if (readers != null) {
                for (ShuffleReader r : readers.values()) {
                    total += r.pagesReceived();
                }
            }
            return total;
        }
    }

    /** 取本 Worker 的某 exchange 接收端。 */
    public ShuffleReader readerFor(String exchangeId) {
        Map<String, ShuffleReader> byWorker = bus.get(selfWorkerId);
        if (byWorker == null) {
            throw new IllegalStateException(
                    "Worker 未注册任何交换接收端: workerId=" + selfWorkerId);
        }
        ShuffleReader reader = byWorker.get(exchangeId);
        if (reader == null) {
            throw new IllegalStateException(
                    "Worker 未注册该 exchange 接收端: workerId=" + selfWorkerId + ", exchangeId=" + exchangeId);
        }
        return reader;
    }
}
