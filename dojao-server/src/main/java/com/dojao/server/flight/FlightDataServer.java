package com.dojao.server.flight;

import com.dojao.common.memory.NodeMemory;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 对 Apache Arrow FlightServer 的封装，用于 Worker 之间的数据交换。
 *
 * <p>Worker 使用此服务器交换中间数据（shuffle/exchange），
 * 无需经过 Master 中转。Master 提供 shuffle 分区位置信息，
 * Worker 可直接连接彼此的 Flight 服务器。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/07 15:30
 */
public class FlightDataServer {

    private static final Logger log = LoggerFactory.getLogger(FlightDataServer.class);

    private final String host;
    private final int port;
    private final BufferAllocator allocator;
    private final FlightProducer producer;
    private FlightServer server;

    public FlightDataServer(String host, int port) {
        this.host = host;
        this.port = port;
        this.allocator = NodeMemory.shared().newChildAllocator("flight", 0);
        this.producer = new FlightProducer(allocator);
    }

    /**
     * 在配置的 host:port 上启动 Arrow Flight 服务器。
     *
     * @throws IOException 端口绑定失败时抛出
     */
    public void start() throws IOException {
        Location location = Location.forGrpcInsecure(host, port);
        server = FlightServer.builder()
                .allocator(allocator)
                .location(location)
                .producer(producer)
                .build();
        server.start();
        log.info("Arrow Flight 服务器已启动: {}:{}", host, port);
    }

    /** 优雅关闭 Flight 服务器并释放 Arrow 内存。 */
    public void shutdown() throws Exception {
        if (server != null) {
            server.shutdown();
            log.info("Arrow Flight 服务器已关闭");
        }
        allocator.close();
        log.info("Flight BufferAllocator 已关闭");
    }

    public Location getLocation() {
        return Location.forGrpcInsecure(host, port);
    }

    public FlightProducer getProducer() {
        return producer;
    }
}
