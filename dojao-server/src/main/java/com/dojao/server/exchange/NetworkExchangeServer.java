package com.dojao.server.exchange;

import com.dojao.common.memory.NodeMemory;
import com.dojao.server.worker.ExchangeManager;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Worker↔Worker 交换通道的<b>网络接收端</b> —— 运行在归属 Worker 进程，接收各发送方经
 * {@link NetworkExchangeChannel} 推来的 Arrow Flight doExchange 流，路由到本 Worker 的
 * {@link com.dojao.core.operator.ShuffleReader}（见 {@link ExchangeFlightProducer}）。
 *
 * <p>基于 {@link FlightServer}（gRPC 底座），监听 {selfNode.port()}，与发送方复用同一
 * 进程边界判定（同 JVM 走 {@code LocalExchangeChannel} 快路径，跨 JVM 走本服务器）。</p>
 *
 * <p><b>producer 覆盖</b>：真分布式下本服务器同时承载 Worker 控制面（派发 doAction），
 * 由调用方传入合并 producer（{@code com.dojao.server.worker.WorkerControlProducer}）覆写默认
 * 纯交换 producer —— 一个端口、一个 FlightServer，exchange 与派发并存。</p>
 *
 * @author jiaxin
 * @version 1.1
 * @date 2026/8/24
 */
public final class NetworkExchangeServer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(NetworkExchangeServer.class);

    private final ExchangeManager exchangeManager;
    private final BufferAllocator allocator;
    private final FlightProducer producer;
    private final FlightServer server;

    /**
     * @param exchangeManager 路由目标（readerFor 定位接收端、allocator 承接页 buffer）
     * @param port            监听端口（0 = 操作系统分配，之后经 {@link #port()} 取实际值）
     */
    public NetworkExchangeServer(ExchangeManager exchangeManager, int port) throws IOException {
        this(exchangeManager, port, null);
    }

    /**
     * @param exchangeManager 路由目标（readerFor 定位接收端、allocator 承接页 buffer）
     * @param port            监听端口（0 = 操作系统分配，之后经 {@link #port()} 取实际值）
     * @param producer        FlightProducer 覆盖（真分布式合并控制面）；null → 默认纯交换 producer
     */
    public NetworkExchangeServer(ExchangeManager exchangeManager, int port, FlightProducer producer) throws IOException {
        this.exchangeManager = exchangeManager;
        this.allocator = NodeMemory.shared().newChildAllocator("exchange/server", 0);
        this.producer = producer != null ? producer : new ExchangeFlightProducer(exchangeManager, allocator);
        this.server = FlightServer.builder()
                .allocator(allocator)
                .location(Location.forGrpcInsecure("0.0.0.0", port))
                .producer(this.producer)
                .build();
        this.server.start();
        log.info("Arrow Flight 交换服务器已启动: 0.0.0.0:{}", port());
    }

    /** 实际绑定端口（port=0 时由 OS 分配）。 */
    public int port() {
        return server.getPort();
    }

    /** 优雅关闭 Flight 服务器并释放其分配器（Worker 停止时调用）。 */
    public void shutdown() {
        try {
            server.shutdown();
            // 等 gRPC 把在途消息全部投递完再关分配器，避免「边关 allocator 边有消息释放」
            // 触发 outstanding buffers / 泄漏告警。
            try {
                server.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } finally {
            allocator.close();
        }
        log.info("Arrow Flight 交换服务器已关闭");
    }

    public FlightProducer producer() {
        return producer;
    }

    public ExchangeManager exchangeManager() {
        return exchangeManager;
    }

    @Override
    public void close() {
        shutdown();
    }
}
