package com.dojao.server.exchange;

import com.dojao.common.data.Page;
import com.dojao.common.memory.NodeMemory;
import com.dojao.core.operator.RowAccumulator;
import com.dojao.core.operator.ShuffleReader;
import com.dojao.core.rpc.exchange.ExchangeControl;
import com.dojao.server.worker.ExchangeManager;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 交换通道的 Arrow Flight 生产者 —— 运行在归属 Worker 进程，接收各发送方经
 * {@link NetworkExchangeChannel} 推来的 doExchange 流，路由到本 Worker 的 {@link ShuffleReader}。
 *
 * <p>每 (worker, exchangeId) 一条流（descriptor path = {@code ["exchange", exchangeId]}，
 * Flight 要求每条流 schema 一致 → 按 exchangeId 分流的代价）。每批：Arrow 原生 batch 承载
 * 数据页（COMPLETE/FAIL 为 0 行空 batch），metadata={@link ExchangeControl} 承载控制字段。</p>
 *
 * <p>每接收一个 DATA 批：逐列拷入 {@link ExchangeManager#allocator()} 共享分配器（原生 batch
 * 的 buffer 归属 Flight 内部 allocator，须拷贝使页生命周期归调用方）后 {@code reader.onPage}
 * 有界 put —— 队列满时本线程阻塞，Flight（gRPC 底座）流控窗口随之满，发送端 {@code putNext}
 * 阻塞 = 背压链（与 local 语义一致）。COMPLETE / FAIL 分别路由到 {@code reader.onComplete} /
 * {@code onFail}（fail-fast）。</p>
 *
 * <p>v1 响应流无 per-batch ack（客户端不依赖确认，仅靠 Flight 流控保证有序传输），
 * 只声明空 schema 后收尾。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/24
 */
public final class ExchangeFlightProducer implements FlightProducer {

    private static final Logger log = LoggerFactory.getLogger(ExchangeFlightProducer.class);

    private final ExchangeManager exchangeManager;
    private final BufferAllocator serverAllocator;

    public ExchangeFlightProducer(ExchangeManager exchangeManager, BufferAllocator serverAllocator) {
        this.exchangeManager = exchangeManager;
        this.serverAllocator = serverAllocator;
    }

    @Override
    public void doExchange(CallContext context, FlightStream flightStream, ServerStreamListener listener) {
        List<String> path = flightStream.getDescriptor().getPath();
        if (path.size() != 2 || !"exchange".equals(path.get(0))) {
            listener.error(CallStatus.INVALID_ARGUMENT
                    .withDescription("期望 path=['exchange', exchangeId]，实际 " + path).toRuntimeException());
            return;
        }
        String exchangeId = path.get(1);
        try {
            // 接收端查找放在 try 内：注册缺失 / 已摘除时以 error 收尾响应，
            // 发送端 getResult() 才能把「这条流没被处理」变成明确失败（而不是单向静默丢消息）。
            ShuffleReader reader = exchangeManager.readerFor(exchangeId);
            while (flightStream.next()) {
                // 发送方惰性建流后首条 descriptor/schema 握手消息无 metadata（HeaderType.NONE/SCHEMA），
                // 只用于建流不承载控制，须跳过 —— 否则 metadataBytes(null) 抛 NPE。
                // DATA/COMPLETE/FAIL 都带 ExchangeControl metadata，绝不在此列。
                ArrowBuf metaBuf = flightStream.getLatestMetadata();
                if (metaBuf == null) {
                    continue;
                }
                ExchangeControl control = ExchangeControl.parseFrom(metadataBytes(metaBuf));
                switch (control.getKind()) {
                    case DATA -> {
                        VectorSchemaRoot root = flightStream.getRoot();
                        // 0 行批（防御性跳过）：send 从不发空数据页
                        if (root.getRowCount() > 0) {
                            // 接收端「即将占用更多堆外」——先过背压闸门；高水位时等待消费/溢写
                            // 把用量压下来，避免 shuffle 洪峰把 Arrow 预算吃穿。
                            NodeMemory.shared().throttle();
                            reader.onPage(control.getBucketId(), copyPage(root, exchangeManager.allocator()));
                        }
                    }
                    case COMPLETE -> reader.onComplete(control.getBucketId());
                    case FAIL -> reader.onFail(control.getBucketId(), new RuntimeException(control.getCause()));
                    default -> {
                    }
                }
            }
            // v1 无 per-batch ack：仅声明空 schema + 正常收尾，客户端忽略响应流。
            // ★Flight 不接管 start() 传入 root 的所有权（VectorUnloader 只引用、不释放），
            //   须由调用方关闭，否则响应 root 的 buffer 会滞留在服务器 allocator 上。
            VectorSchemaRoot response = emptyRoot(serverAllocator);
            try {
                listener.start(response);
                listener.completed();
            } finally {
                response.close();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            listener.error(e);
        } catch (Exception e) {
            listener.error(e);
        }
    }

    // ---- 未使用：交换走 doExchange（见类 javadoc） ----

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        listener.error(CallStatus.UNIMPLEMENTED.withDescription("交换走 doExchange，不用 getStream").toRuntimeException());
    }

    @Override
    public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
        return () -> ackStream.onError(CallStatus.UNIMPLEMENTED
                .withDescription("交换走 doExchange，不用 acceptPut").toRuntimeException());
    }

    @Override
    public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
        listener.onCompleted();
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        throw CallStatus.UNIMPLEMENTED.withDescription("交换走 doExchange，不用 getFlightInfo").toRuntimeException();
    }

    @Override
    public void listActions(CallContext context, StreamListener<ActionType> listener) {
        listener.onCompleted();
    }

    @Override
    public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
        listener.onCompleted();
    }

    // ==================== 内部 ====================

    /** 原生 batch → 独立 allocator 的页（逐列拷贝，var 列安全）。 */
    private static Page copyPage(VectorSchemaRoot src, BufferAllocator allocator) {
        int rc = src.getRowCount();
        List<FieldVector> copied = new ArrayList<>(src.getFieldVectors().size());
        for (int col = 0; col < src.getFieldVectors().size(); col++) {
            FieldVector s = src.getVector(col);
            FieldVector d = s.getField().createVector(allocator);
            d.setInitialCapacity(rc);
            d.allocateNew();
            RowAccumulator.copyColumnBatch(s, d, 0, 0, rc);
            d.setValueCount(rc);
            copied.add(d);
        }
        return Page.fromRoot(new VectorSchemaRoot(src.getSchema().getFields(), copied, rc));
    }

    private static VectorSchemaRoot emptyRoot(BufferAllocator allocator) {
        return VectorSchemaRoot.create(new Schema(List.of()), allocator);
    }

    private static byte[] metadataBytes(ArrowBuf buf) {
        // readableBytes() 返回 long，数组长度需 int（metadata 体积极小，直接截断）
        byte[] out = new byte[Math.toIntExact(buf.readableBytes())];
        buf.readBytes(out);
        return out;
    }
}
