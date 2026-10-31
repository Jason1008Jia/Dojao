package com.dojao.server.flight;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

/**
 * 用于 Worker 之间数据交换的 Arrow Flight 生产者。
 *
 * <p><b>Ticket 格式：</b> {@code <taskId>|<partitionId>}，使用 UTF-8 编码。</p>
 *
 * <p>Worker 通过 {@code acceptPut()} 写入 shuffle 数据，
 * 其他 Worker 通过 {@code getStream()} 读取。
 * Flight 服务器在每个 Worker 节点上运行，让节点间可直接交换中间数据，
 * 无需经过 Master。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/07 15:30
 */
public class FlightProducer implements org.apache.arrow.flight.FlightProducer {

    private static final Logger log = LoggerFactory.getLogger(FlightProducer.class);

    private final BufferAllocator allocator;
    /** Flight 分区数据的内存存储，以 {@code taskId|partitionId} 为键。 */
    private final Map<String, VectorSchemaRoot> dataStore = new ConcurrentHashMap<>();

    public FlightProducer(BufferAllocator allocator) {
        this.allocator = allocator;
    }

    /**
     * 从此 worker 读取数据（由另一个 worker 通过 Flight 客户端调用）。
     */
    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        String key = new String(ticket.getBytes(), StandardCharsets.UTF_8);
        log.info("getStream: ticket={}", key);

        VectorSchemaRoot root = dataStore.get(key);
        if (root == null) {
            log.warn("getStream: 未找到 ticket={} 的数据", key);
            listener.error(CallStatus.NOT_FOUND
                    .withDescription("No data for ticket: " + key)
                    .toRuntimeException());
            return;
        }

        listener.start(root);
        try {
            listener.putNext();
        } finally {
            listener.completed();
        }
    }

    /**
     * 向此 worker 写入数据（由生产 shuffle 输出的 worker 调用）。
     */
    @Override
    public Runnable acceptPut(CallContext context, FlightStream flightStream,
                              StreamListener<PutResult> ackStream) {
        return () -> {
            try {
                FlightDescriptor descriptor = flightStream.getDescriptor();
                String key = descriptor.getPath().isEmpty()
                        ? "unknown"
                        : String.join("|", descriptor.getPath());
                log.info("acceptPut: key={}", key);

                VectorSchemaRoot root = flightStream.getRoot();
                // 存储副本：从 schema 创建新的 root 并逐批拷贝数据
                VectorSchemaRoot stored = VectorSchemaRoot.create(root.getSchema(), allocator);
                while (flightStream.next()) {
                    // 将当前批的数据逐行拷贝到存储的 root 中
                    stored.allocateNew();
                    int rowCount = root.getRowCount();
                    for (int col = 0; col < root.getFieldVectors().size(); col++) {
                        var srcVec = root.getVector(col);
                        var dstVec = stored.getVector(col);
                        dstVec.allocateNew();
                        for (int row = 0; row < rowCount; row++) {
                            dstVec.copyFrom(row, row, srcVec);
                        }
                        dstVec.setValueCount(rowCount);
                    }
                    stored.setRowCount(rowCount);
                }
                dataStore.put(key, stored);
                log.info("acceptPut: 已存储 {} 行，key={}", stored.getRowCount(), key);
                ackStream.onCompleted();
            } catch (Exception e) {
                log.error("acceptPut 失败: {}", e.getMessage(), e);
                ackStream.onError(e);
            }
        };
    }

    /**
     * 列出可用的数据分区。
     */
    @Override
    public void listFlights(CallContext context, Criteria criteria,
                            StreamListener<FlightInfo> listener) {
        for (Map.Entry<String, VectorSchemaRoot> entry : dataStore.entrySet()) {
            Ticket ticket = new Ticket(entry.getKey().getBytes(StandardCharsets.UTF_8));
            FlightDescriptor descriptor = FlightDescriptor.path(entry.getKey());
            FlightEndpoint endpoint = new FlightEndpoint(ticket);
            FlightInfo info = new FlightInfo(entry.getValue().getSchema(), descriptor,
                    Collections.singletonList(endpoint), -1, entry.getValue().getRowCount());
            listener.onNext(info);
        }
        listener.onCompleted();
    }

    /**
     * 获取指定 ticket 的 flight 信息。
     */
    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        String key = String.join("|", descriptor.getPath());
        VectorSchemaRoot root = dataStore.get(key);
        if (root == null) {
            throw CallStatus.NOT_FOUND
                    .withDescription("No data for: " + key)
                    .toRuntimeException();
        }
        Ticket ticket = new Ticket(key.getBytes(StandardCharsets.UTF_8));
        FlightEndpoint endpoint = new FlightEndpoint(ticket);
        return new FlightInfo(root.getSchema(), descriptor,
                Collections.singletonList(endpoint), -1, root.getRowCount());
    }

    /**
     * 列出可用的自定义操作（FlightProducer 接口要求实现）。
     */
    @Override
    public void listActions(CallContext context, StreamListener<ActionType> listener) {
        // 当前实现不提供自定义操作
        listener.onCompleted();
    }

    /**
     * 执行自定义操作（FlightProducer 接口要求实现）。
     */
    @Override
    public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
        // 当前实现不提供自定义操作
        listener.onCompleted();
    }

    /** 从存储中移除过期数据。 */
    public void evict(String key) {
        VectorSchemaRoot removed = dataStore.remove(key);
        if (removed != null) {
            removed.close();
        }
    }
}
