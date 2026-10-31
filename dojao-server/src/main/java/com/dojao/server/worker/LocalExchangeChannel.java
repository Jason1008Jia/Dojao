package com.dojao.server.worker;

import com.dojao.common.data.Page;
import com.dojao.common.memory.NodeMemory;
import com.dojao.core.operator.ShuffleReader;
import com.dojao.core.runtime.worker.ExchangeChannel;
import org.apache.arrow.memory.BufferAllocator;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Worker↔Worker 交换通道的伪分布式实现 —— 单 JVM 内直接 put 目标 Worker 的
 * {@link ShuffleReader} 有界队列，背压天然保留（有界 put 阻塞 = 发送端阻塞 = 上游扫描阻塞）。
 *
 * <p>所有 Worker 共享同一个 {@code bus}（workerId → exchangeId → ShuffleReader），
 * 路由只按目标 workerId + exchangeId 查表，自身桶也走同一路径（单 JVM 内无网络差别）。
 * 用于伪分布式测试 / 本地多 Worker 模拟；跨进程版本见 {@code NetworkExchangeChannel}（Flight）。</p>
 *
 * <p><b>页所有权</b>：{@link #send} 把页的 Arrow buffer 所有权移交到 {@code allocator}
 * （零拷贝 transfer，见 {@link Page#transferTo}），源页随即关闭 —— 与生产 Flight 传输语义一致
 * （发送端序列化后即释放本地页）。否则接收端延迟 close 的页仍挂在发送端算子的分配器上，
 * 发送端算子关闭时 Arrow 会误报内存泄漏。</p>
 *
 * @author jiaxin
 * @version 2.1
 * @date 2026/8/16
 */
public final class LocalExchangeChannel implements ExchangeChannel {

    private final String selfWorkerId;
    /** 共享路由表：workerId → (exchangeId → ShuffleReader)。 */
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, ShuffleReader>> bus;
    /** 页 buffer 所有权移交的目标分配器（通常 = 本 Worker ExchangeManager 的共享分配器）。 */
    private final BufferAllocator allocator;

    public LocalExchangeChannel(String selfWorkerId,
                                ConcurrentHashMap<String, ConcurrentHashMap<String, ShuffleReader>> bus) {
        this(selfWorkerId, bus, NodeMemory.shared().newChildAllocator("exchange/local", 0));
    }

    public LocalExchangeChannel(String selfWorkerId,
                                ConcurrentHashMap<String, ConcurrentHashMap<String, ShuffleReader>> bus,
                                BufferAllocator allocator) {
        this.selfWorkerId = selfWorkerId;
        this.bus = bus;
        this.allocator = allocator;
    }

    /** 目标 Worker 注册其 ShuffleReader（收到该 exchange 数据的地方）。 */
    public void register(String workerId, String exchangeId, ShuffleReader reader) {
        bus.computeIfAbsent(workerId, k -> new ConcurrentHashMap<>()).put(exchangeId, reader);
    }

    private ShuffleReader readerFor(String workerId, String exchangeId) {
        Map<String, ShuffleReader> byWorker = bus.get(workerId);
        if (byWorker == null) {
            throw new IllegalStateException("Worker 未注册交换接收端: workerId=" + workerId + ", exchangeId=" + exchangeId);
        }
        ShuffleReader reader = byWorker.get(exchangeId);
        if (reader == null) {
            throw new IllegalStateException("Worker 未注册该 exchange 接收端: workerId=" + workerId + ", exchangeId=" + exchangeId);
        }
        return reader;
    }

    @Override
    public void send(String toWorkerId, String exchangeId, int bucketId, Page page) throws InterruptedException {
        // 零拷贝移交 buffer 所有权到共享分配器并关闭源页：跨线程交接的页与创建它的算子分配器解耦
        Page owned = page.transferTo(allocator);
        readerFor(toWorkerId, exchangeId).onPage(bucketId, owned);
    }

    @Override
    public void complete(String toWorkerId, String exchangeId, int bucketId) throws InterruptedException {
        readerFor(toWorkerId, exchangeId).onComplete(bucketId);
    }

    @Override
    public void fail(String toWorkerId, String exchangeId, int bucketId, Throwable cause) {
        readerFor(toWorkerId, exchangeId).onFail(bucketId, cause);
    }

    @Override
    public boolean isSelf(String toWorkerId) {
        return selfWorkerId.equals(toWorkerId);
    }
}
