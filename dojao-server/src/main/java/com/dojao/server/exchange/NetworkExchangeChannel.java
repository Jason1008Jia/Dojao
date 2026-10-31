package com.dojao.server.exchange;

import com.dojao.common.data.Page;
import com.dojao.core.operator.RowAccumulator;
import com.dojao.core.operator.ShuffleReader;
import com.dojao.core.rpc.exchange.ExchangeControl;
import com.dojao.core.runtime.worker.ExchangeChannel;
import com.dojao.core.runtime.worker.WorkerNode;
import com.dojao.server.worker.LocalExchangeChannel;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Worker↔Worker 数据交换通道的<b>网络实现</b> —— 跨 JVM 进程的数据页传输（Arrow Flight）。
 *
 * <p><b>进程边界路由</b>：{@link #isSelf} 判真（目标 worker 与发送方共享同一 JVM 内存）时走
 * <b>local 快路径</b>（委托 {@link LocalExchangeChannel} 交接，零网络）；否则把页经
 * <b>Flight doExchange</b> 推到目标 worker（见 {@link ExchangeFlightProducer}）。这与
 * {@code LocalExchangeChannel}（同 JVM 内 Worker 桥梁）形成双模式：本类即"真正的分布式计算"
 * 所用实现，local 能力内嵌其中。</p>
 *
 * <p><b>每 (worker, exchangeId) 一条流</b>：Flight 要求每条流 schema 一致，无法像 gRPC 版
 * （protobuf 包 bytes）那样按 exchangeId 复用一条流。流控反而更干净 —— 背压按单个 shuffle
 * 隔离。descriptor path = {@code ["exchange", exchangeId]}。</p>
 *
 * <p><b>背压</b>：Flight 客户端流式 {@code putNext} 在 gRPC 出站流控窗口耗尽时<b>阻塞</b>
 * （区别于裸 gRPC 非阻塞 onNext），因此 {@code SenderStream} 用<b>有界待发队列（容量 16）+
 * 单 flusher 线程</b>恢复 {@code ExchangeChannel.send} 的阻塞语义：{@code push} 向有界队列
 * {@code put}（队满即阻塞发送端 = HashPartition 分发线程阻塞 = 上游扫描阻塞），flusher 每次
 * {@code putNext} 随窗口阻塞 = 反映对端消费速率，与 local 有界队列语义一致。</p>
 *
 * <p><b>页所有权</b>：网络路径 {@code send} 将页所有权<b>移交 flusher</b>（拷贝进流根后即
 * {@code page.close()}），调用方不再 close —— 否则异步 flusher 与调用方 close 竞态。
 * 与 local 路径（{@code transferTo} 已关闭源页）语义一致。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/24
 */
public final class NetworkExchangeChannel implements ExchangeChannel {

    private static final Logger log = LoggerFactory.getLogger(NetworkExchangeChannel.class);

    /** local 快路径：self 桶 / 同 JVM worker 桶的本地交接（复用伪分布式通道）。 */
    private final LocalExchangeChannel local;
    /** 本 JVM 进程内的 workerId 集合（isSelf 判真：self + 同进程 worker）。 */
    private final Set<String> localWorkerIds;
    /** 远程 worker 地址表：workerId → host:port（不含本 JVM worker）。 */
    private final Map<String, WorkerNode> remoteWorkers;
    /** 每个远程 worker 一个 FlightClient（channel 复用）。 */
    private final ConcurrentHashMap<String, FlightClient> clients = new ConcurrentHashMap<>();
    /** 每个 (worker#exchangeId) 一条懒建立的 doExchange 流。 */
    private final ConcurrentHashMap<String, SenderStream> streams = new ConcurrentHashMap<>();
    /**
     * exchangeId → 仍在用该 exchange 发送流的<b>生产端</b>数量（同 Worker 上多个 Producer 片段
     * 共用一条流）。释放必须等最后一个生产端：先跑完的片段若直接关流，并行片段后续的
     * {@code send}/{@code complete} 会被静默吞掉 → 接收端永远等不齐发送方。
     */
    private final ConcurrentHashMap<String, AtomicInteger> activeSenders = new ConcurrentHashMap<>();
    private final BufferAllocator allocator;

    public NetworkExchangeChannel(String selfWorkerId,
                                  Set<String> localWorkerIds,
                                  Map<String, WorkerNode> remoteWorkers,
                                  ConcurrentHashMap<String, ConcurrentHashMap<String, ShuffleReader>> bus,
                                  BufferAllocator allocator) {
        this.local = new LocalExchangeChannel(selfWorkerId, bus, allocator);
        this.localWorkerIds = localWorkerIds;
        this.remoteWorkers = remoteWorkers;
        this.allocator = allocator;
    }

    // ==================== ExchangeChannel ====================

    @Override
    public void send(String toWorkerId, String exchangeId, int bucketId, Page page) throws InterruptedException {
        if (isSelf(toWorkerId)) {
            local.send(toWorkerId, exchangeId, bucketId, page);
            return;
        }
        // 先转到本 Worker 交换共享分配器再入队：发送端算子（HashPartition 分发线程）的分配器可能
        // 在 flusher 拷贝前被关闭（awaitDistributedDone → doClose 时序），在途页若仍挂在算子分配器上
        // 会被 Arrow 误报内存泄漏（与 local 路径 transferTo 同款）。transferTo 跨 root 退化为逐格拷贝
        // 并同步关闭源页，此后 flusher 只与共享分配器打交道。
        Page owned = page.transferTo(allocator);
        streamFor(toWorkerId, exchangeId).push(SenderStream.Msg.data(
                ExchangeControl.newBuilder()
                        .setExchangeId(exchangeId)
                        .setBucketId(bucketId)
                        .setKind(ExchangeControl.Kind.DATA)
                        .setBatchIndex(owned.getBatchIndex())
                        .setIsSpilled(owned.isSpilled())
                        .build(),
                owned));
    }

    @Override
    public void complete(String toWorkerId, String exchangeId, int bucketId) throws InterruptedException {
        if (isSelf(toWorkerId)) {
            local.complete(toWorkerId, exchangeId, bucketId);
            return;
        }
        streamFor(toWorkerId, exchangeId).push(SenderStream.Msg.control(
                ExchangeControl.newBuilder()
                        .setExchangeId(exchangeId)
                        .setBucketId(bucketId)
                        .setKind(ExchangeControl.Kind.COMPLETE)
                        .build()));
    }

    @Override
    public void fail(String toWorkerId, String exchangeId, int bucketId, Throwable cause) {
        if (isSelf(toWorkerId)) {
            local.fail(toWorkerId, exchangeId, bucketId, cause);
            return;
        }
        try {
            streamFor(toWorkerId, exchangeId).push(SenderStream.Msg.control(
                    ExchangeControl.newBuilder()
                            .setExchangeId(exchangeId)
                            .setBucketId(bucketId)
                            .setKind(ExchangeControl.Kind.FAIL)
                            .setCause(String.valueOf(cause))
                            .build()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public boolean isSelf(String toWorkerId) {
        return localWorkerIds.contains(toWorkerId);
    }

    /**
     * 登记一个使用该 exchange 发送流的生产端（见 {@link #release} 的引用计数说明）。
     */
    @Override
    public void acquire(String exchangeId) {
        if (exchangeId == null) {
            return;
        }
        activeSenders.computeIfAbsent(exchangeId, k -> new AtomicInteger()).incrementAndGet();
    }

    /**
     * 释放该 exchange 的发送流：<b>仅当本通道上最后一个生产端释放时</b>才通知各 flusher 在待发
     * 队列排空后关闭 doExchange 读端、释放流复用根并从 {@link #streams} 摘除自己。
     *
     * <p>未 {@link #acquire} 登记过的 exchange（直接使用通道的测试 / 工具）保持旧语义：立即释放。</p>
     */
    @Override
    public void release(String exchangeId) {
        if (exchangeId == null) {
            return;
        }
        AtomicInteger count = activeSenders.get(exchangeId);
        if (count != null) {
            int left = count.decrementAndGet();
            if (left > 0) {
                return;   // 还有生产端在用这条共享流
            }
            activeSenders.remove(exchangeId, count);
        }
        for (SenderStream s : streams.values()) {
            if (s.exchangeId.equals(exchangeId)) {
                s.release();
            }
        }
    }

    /** 当前仍存活的发送流数（探针断言「查询结束后流已回收」用）。 */
    public int openStreams() {
        return streams.size();
    }

    /** 关闭全部远程流及其底层通道（Worker 停止时调用）。 */
    public void close() {
        streams.forEach((key, stream) -> stream.close());
        streams.clear();
        activeSenders.clear();
        clients.forEach((workerId, client) -> {
            try {
                client.close();
            } catch (Exception ignored) {
            }
        });
        clients.clear();
    }

    // ==================== 内部 ====================

    private SenderStream streamFor(String toWorkerId, String exchangeId) throws InterruptedException {
        return streams.computeIfAbsent(key(toWorkerId, exchangeId),
                k -> new SenderStream(toWorkerId, exchangeId, clientFor(toWorkerId), allocator,
                        s -> streams.remove(k, s)));
    }

    private FlightClient clientFor(String toWorkerId) {
        return clients.computeIfAbsent(toWorkerId, this::openClient);
    }

    private FlightClient openClient(String toWorkerId) {
        WorkerNode node = remoteWorkers.get(toWorkerId);
        if (node == null) {
            throw new IllegalStateException("未知远程 Worker（不在地址表）: " + toWorkerId);
        }
        // Arrow 15：无 builder(Location)，用 builder().location().allocator().build()
        return FlightClient.builder()
                .location(Location.forGrpcInsecure(node.host(), node.port()))
                .allocator(allocator)
                .build();
    }

    private static String key(String toWorkerId, String exchangeId) {
        return toWorkerId + "\u0000" + exchangeId;
    }

    /**
     * 到单个远程 worker 的单个 exchange 的一条 Flight doExchange 流，带 <b>有界队列背压</b>。
     *
     * <p>Flight 客户端流式 {@code putNext} 在 gRPC 出站流控窗口耗尽时阻塞（对流控自动感知），
     * 因此不需要 gRPC 版那套 isReady 闸门。仍保留<b>有界待发队列 + 单 flusher 线程</b>：
     * {@code push} 队满阻塞发送端（= 上游扫描阻塞）；flusher 单线程串行 {@code putNext}
     * （Flight 客户端流非线程安全），保持单流消息顺序（数据先于 complete / fail）。</p>
     */
    private static final class SenderStream {
        private static final int PENDING_CAPACITY = 16;
        /**
         * 控制消息（COMPLETE/FAIL）等待「真正交给 Flight」的上限。COMPLETE/FAIL 只随已流动的数据
         * 排在后面（待发队列 ≤16 条 + gRPC 窗口），正常毫秒级；但<b>接收端消费慢不是故障</b>：
         * {@code ShuffleReader.onPage} 是无超时的有界队列 {@code put}（每桶 8 页）+ 接收侧
         * {@code NodeMemory.throttle()}，下游在排序 / 溢写 / 压缩时可以合法地停住读循环数分钟，
         * gRPC 窗口因此把 flusher 挡在 {@code putNext} 上。旧默认 60s 会把这种「慢」当成「对端卡死」，
         * 提前让 COMPLETE 投递失败（查询直接红，而数据面其实还在推进）—— 故默认与结果看门狗同量级
         * （{@code query.result.watchdogMs} = 10 分钟），可用 {@code exchange.controlAckTimeoutMs} 调整。
         */
        private static final long CONTROL_ACK_TIMEOUT_MS =
                timeoutMs("exchange.controlAckTimeoutMs", 600_000);
        /**
         * 收尾时等接收端响应流结束的上限。Flight 的 {@code FlightStream.completed} 无超时 API，
         * 只能在辅助线程上等（见 {@link #awaitServerDoneThenClose()}）。
         *
         * <p>它等的不是「网络传完」，而是<b>接收端读完并路由完本流全部消息</b>——服务端的读循环
         * 会阻塞在 {@code reader.onPage} 的有界队列上，所以这个等待天然等于「下游消费速度」。
         * 大 shuffle 的下游排序 / 溢写超过 60s 是常态；此时若按 60s 超时关流，{@code close()}
         * 会 cancel 整条 RPC，把尚未送达的尾部 COMPLETE 丢掉，正好复现「已收 5/6」挂死。
         * 默认与结果看门狗同量级（10 分钟），可用 {@code exchange.serverDoneTimeoutMs} 调整。
         */
        private static final long SERVER_DONE_TIMEOUT_MS =
                timeoutMs("exchange.serverDoneTimeoutMs", 600_000);

        /** 读一个毫秒级超时系统属性；非法 / 过小（&lt;1s）回退默认值。 */
        private static long timeoutMs(String key, long defaultValue) {
            long v = Long.getLong(key, defaultValue);
            return v >= 1_000 ? v : defaultValue;
        }
        /**
         * 等接收端响应收尾 + 随后关读端的辅助线程池。<b>绝不能在 flusher 线程上等</b>：接收端可能
         * 长期被下游背压卡住（队列满时它停在 {@code reader.onPage}），而生产端收尾不能被它拖住；
         * 也绝不能在 {@code completed()} 之后<b>立即</b>关流 —— 见 {@link #awaitServerDoneThenClose()}。
         */
        private static final ExecutorService SERVER_DONE_WAITERS = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "exchange-server-done");
            t.setDaemon(true);
            return t;
        });

        /**
         * 一条待发消息：control + 数据页（COMPLETE/FAIL 的 page 为 null）+ 送达确认。
         *
         * <p>{@code ack == null} = 数据页：入队即返回（有界队列 = 背压）。控制消息带 latch：
         * {@link #push} 只有在 flusher 真正 {@code putNext} 之后才返回 —— 否则 putNext 失败
         * （对端已取消 / gRPC 断链）会把结束标记静默丢弃，接收端永远等不齐发送方。</p>
         */
        record Msg(ExchangeControl control, Page page, CountDownLatch ack) {
            static Msg data(ExchangeControl control, Page page) {
                return new Msg(control, page, null);
            }

            static Msg control(ExchangeControl control) {
                return new Msg(control, null, new CountDownLatch(1));
            }
        }

        private final String toWorkerId;
        private final String exchangeId;
        private final BufferAllocator allocator;
        /** 有界待发队列：容量满时 push 阻塞发送端（= 上游扫描阻塞，与 local 语义一致）。 */
        private final BlockingQueue<Msg> pending = new LinkedBlockingQueue<>(PENDING_CAPACITY);
        private final AtomicBoolean closed = new AtomicBoolean();
        /** 读写双子是否已关闭（flusher 收尾与 Worker 停止可能并发，只关一次）。 */
        private final AtomicBoolean streamClosed = new AtomicBoolean();
        /** 生产端已收尾（不会再有新的 send/complete/fail）：队列排空后 flusher 自行退出并清理。 */
        private volatile boolean released;
        /** flusher 收尾后回调（从通道的 streams 表摘除自己，避免按查询次数累积流 / root / 线程）。 */
        private final java.util.function.Consumer<SenderStream> onClosed;
        private final Thread flusher;
        /**
         * doExchange 的读写双子。写端负责发数据；<b>响应读端也必须保留并在关闭时释放</b> ——
         * 否则服务端回传的空 schema 响应消息（每条流 8B 级 buffer）会一直挂在 gRPC 流上，
         * 接收端关闭 allocator 时抛 {@code Memory was leaked}。
         */
        private final FlightClient.ExchangeReaderWriter stream;
        private final FlightClient.ClientStreamListener listener;
        /** 流的复用根（拷贝各页数据进入；首条 DATA 定 schema，无数据的流用空 schema）。 */
        private volatile VectorSchemaRoot streamRoot;
        /** 发送侧失败（putNext 抛错 / 流级错误）→ push 立即抛。 */
        private volatile Throwable failure;
        /**
         * 流启动前暂存的 COMPLETE/FAIL 控制消息。共享流被第一条真实 DATA 定 schema；
         * 空 slice 的 producer 只发 COMPLETE，若在流未启动时直接发会以空 schema 建流，
         * 后续并发 DATA 拷入 0 列根崩溃（IllegalArgumentException）。故暂存，等首个 DATA
         * 定 schema 后补发；确无 DATA 时 2s 宽限兜底。仅 flusher 线程访问，无需同步。
         */
        private final List<Msg> heldControls = new ArrayList<>();
        /** 流未启动且持有暂存控制消息的计时起点（-1 = 未计时）。 */
        private long emptySince = -1;
        /** 兜底已用空 schema 启动流（此时迟到 DATA 无法复用流，须 fail-fast）。 */
        private boolean emptySchemaStarted;

        SenderStream(String toWorkerId, String exchangeId, FlightClient client, BufferAllocator allocator,
                     java.util.function.Consumer<SenderStream> onClosed) {
            this.toWorkerId = toWorkerId;
            this.exchangeId = exchangeId;
            this.allocator = allocator;
            this.onClosed = onClosed;
            // Arrow 15：doExchange 返回 ExchangeReaderWriter，getWriter() 取写端（读写双子）
            this.stream = client.doExchange(FlightDescriptor.path("exchange", exchangeId));
            this.listener = stream.getWriter();
            this.flusher = new Thread(this::drain, "exchange-sender-" + toWorkerId + "#" + exchangeId);
            flusher.setDaemon(true);
            flusher.start();
        }

        /**
         * 推一条消息。有界队列 put（队满阻塞 = 背压）；流已失败则立即抛。调用方为发送端线程
         * （HashPartition 分发线程），可与多个 Producer 片段并发调用。
         *
         * <p><b>控制消息（COMPLETE/FAIL）送达确认</b>：入队后继续等待 flusher 确认「已 putNext」
         * （或已判定丢弃）才返回。旧实现入队即返回 —— flusher 后续 {@code putNext} 失败时消息被
         * 静默丢弃，生产端却已按「全部桶已通知」收尾，接收端 {@code completedSenders} 永远差一个，
         * 终端 Aggregate 挂起 10 分钟才被看门狗发现（线上「已收 4/6 个完成」）。</p>
         */
        void push(Msg msg) throws InterruptedException {
            while (true) {
                Throwable f = failure;
                if (f != null) {
                    closePage(msg);
                    throw new RuntimeException("exchange 流已失败 toWorkerId=" + toWorkerId + "#" + exchangeId, f);
                }
                // 流已释放 = 最后一个生产端已收尾；此处仍有 send/complete 说明生产端生命周期错配
                // （另一个片段提前释放了共享流 / 取消路径与发送竞态）。fail-fast 优于把消息塞进
                // 无 flusher 的有界队列后永久阻塞（接收端也因此永远等不齐发送方）。
                if (released) {
                    closePage(msg);
                    throw new IllegalStateException("exchange 发送流已释放 toWorkerId=" + toWorkerId
                            + "#" + exchangeId + "：send/complete 必须在 release 之前");
                }
                if (pending.offer(msg, 100, TimeUnit.MILLISECONDS)) {
                    break;
                }
            }
            CountDownLatch ack = msg.ack();
            if (ack == null) {
                return;   // 数据页：入队即返回（有界队列提供背压）
            }
            boolean delivered;
            try {
                delivered = ack.await(CONTROL_ACK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // 中断时投递结果未知（可能已交付）→ 不能由调用方重试，转成运行时失败终止该桶
                Thread.currentThread().interrupt();
                throw new IllegalStateException("等待 " + msg.control().getKind() + " 送达确认时被中断"
                        + "（投递结果未知）toWorkerId=" + toWorkerId + "#" + exchangeId, e);
            }
            Throwable f = failure;
            if (f != null) {
                throw new RuntimeException("exchange 流已失败（" + msg.control().getKind() + " 未送达）"
                        + " toWorkerId=" + toWorkerId + "#" + exchangeId, f);
            }
            if (!delivered) {
                throw new IllegalStateException("exchange 控制消息 " + msg.control().getKind()
                        + " 在 " + CONTROL_ACK_TIMEOUT_MS + "ms 内未被 flusher 送达"
                        + " toWorkerId=" + toWorkerId + "#" + exchangeId + "（接收端停滞 / 流断裂）");
            }
        }

        /** 单 flusher 线程：串行从队列取消息，拷贝进流根后 putNext。 */
        private void drain() {
            try {
                while (true) {
                    Msg msg = pending.poll(100, TimeUnit.MILLISECONDS);
                    if (msg == null) {
                        // 生产端已收尾且队列排空 → flusher 退出（finally 里收尾流、释放 root、摘除自己）。
                        if (pending.isEmpty() && (released || closed.get())) {
                            break;
                        }
                        // 2s 宽限兜底：流一直未启动（只有 COMPLETE 的空 slice 流）且队列已空持续超时
                        // → 用空 schema 启动流并补发暂存 COMPLETE，防接收端 ShuffleReader 收不到
                        // 结束标记而挂起。正常路径首个 DATA 会在毫秒级定 schema，宽限不触发。
                        if (streamRoot == null && !heldControls.isEmpty()) {
                            if (emptySince == -1) {
                                emptySince = System.currentTimeMillis();
                            } else if (System.currentTimeMillis() - emptySince > 2_000) {
                                emptySchemaStarted = true;
                                streamRoot = VectorSchemaRoot.create(new Schema(List.of()), allocator);
                                listener.start(streamRoot);
                                flushHeldControls();
                            }
                        }
                        continue;
                    }
                    if (failure != null) {
                        // 流已失败：丢弃未发消息，发送端由 push 的失败检查唤醒。
                        // 页的 buffer 已挂在本通道共享分配器上 → 必须主动 close，否则失败路径会漏。
                        // 控制消息必须确认（否则等待送达的生产端会白等满超时）。
                        closePage(msg);
                        ack(msg);
                        continue;
                    }
                    try {
                        if (msg.page() != null) {
                            sendData(msg);
                        } else {
                            sendControl(msg);
                        }
                    } catch (RuntimeException e) {
                        failure = e;   // putNext 抛错 → 记录失败，发送端 push 感知
                        closePage(msg); // 未成功移交 Flight 的页同样要归还 buffer
                        ack(msg);       // 控制消息等待方立即感知（随后在 push 里读到 failure 并抛）
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                boolean halfClosed = false;
                try {
                    // 流关闭兜底：仍有未启动流的暂存 COMPLETE（本 exchange 全程无 DATA）→ 空 schema 补发
                    if (streamRoot == null && !heldControls.isEmpty()) {
                        emptySchemaStarted = true;
                        streamRoot = VectorSchemaRoot.create(new Schema(List.of()), allocator);
                        listener.start(streamRoot);
                        flushHeldControls();
                    }
                    listener.completed();
                    halfClosed = true;
                } catch (Throwable t) {
                    // 结束标记无法送达 → 必须让等待确认的生产端 fail-fast（旧实现 catch 后静默吞掉，
                    // 生产端以为已通知、接收端永远等不齐发送方 → 终端 Consumer 挂起 10 分钟）。
                    if (failure == null) failure = t;
                } finally {
                    abortPending();
                }
                // 释放流复用根：每条 (目标, exchangeId) 流一个 VectorSchemaRoot，查询结束后不关
                // 会随查询次数累积在受管直接内存里。completed() 已发完所有消息，Flight 不再引用 root。
                VectorSchemaRoot root = streamRoot;
                streamRoot = null;
                if (root != null) {
                    try { root.close(); } catch (Throwable ignored) { }
                }
                // 立即从 streams 摘除：生产端已收尾，这条流对它们已不可见。**不能**因为接收端还没
                // 消费完（下游背压，正常现象）就把流钉在表里 —— 那会让 openStreams / 后续 send 卡住。
                if (onClosed != null) {
                    try { onClosed.accept(this); } catch (Throwable ignored) { }
                }
                // ★必须等接收端把响应流跑完（= 它已读完并路由完本流全部消息）再关读端：
                // completed() 只是异步 half-close，紧接着的 close() 会走 ClientCall.cancel，
                // 把还没写到线上的最后几条消息（典型就是 COMPLETE）直接丢弃。
                // 等待放到辅助线程（对端可能长期被下游背压卡住），流本身已不再对生产端可见。
                if (halfClosed && failure == null) {
                    SERVER_DONE_WAITERS.execute(this::awaitServerDoneThenClose);
                } else {
                    closeStream();
                }
            }
        }

        /** 确认一条消息的投递结论（控制消息唤醒等待方；数据页 ack 为 null，无操作）。 */
        private static void ack(Msg msg) {
            CountDownLatch a = msg.ack();
            if (a != null) {
                a.countDown();
            }
        }

        /**
         * flusher 退出时唤醒所有仍在等确认的生产端，并归还尚未发送的数据页。
         *
         * <p>旧实现在 flusher 退出后队列里残留的控制消息永远不会被 {@code putNext}，等待方只能
         * 白等满超时（或永久阻塞）；这里逐条 ack 并标记流失败，让 {@code push} 立即以流失败上抛。</p>
         *
         * <p><b>顺序铁律：先置 failure，后 ack。</b>反序时等待方可能在 {@code countDown} 与
         * {@code failure = ...} 之间醒来，读到 {@code failure == null} 且 {@code delivered == true}，
         * 把「已被丢弃」误判成「已送达」—— 线上表现为 6 个发送方里总有一个 COMPLETE 静默消失、
         * 接收端 {@code completedSenders} 永远停在 5/6，而发送端毫无异常。</p>
         */
        private void abortPending() {
            List<Msg> dropped = new ArrayList<>();
            Msg m;
            while ((m = pending.poll()) != null) {
                closePage(m);
                dropped.add(m);
            }
            if (!heldControls.isEmpty()) {
                dropped.addAll(heldControls);
                heldControls.clear();
            }
            if (dropped.isEmpty()) {
                return;
            }
            if (failure == null) {
                failure = new RuntimeException("exchange 发送流中止，丢弃 " + dropped.size() + " 条未送达消息"
                        + " toWorkerId=" + toWorkerId + "#" + exchangeId);
            }
            for (Msg d : dropped) {
                ack(d);
            }
        }

        /**
         * 等接收端把本流的响应跑完（服务端读完并路由完本流全部消息后才收尾响应），随后关读端。
         *
         * <p><b>为什么必须等</b>：{@code listener.completed()} 只把「发送完成」放进 gRPC 队列，
         * 是异步 half-close；随后 {@code FlightStream.close()}（{@code ExchangeReaderWriter.close()}）
         * 在响应流尚未结束时直接 {@code ClientCall.cancel("Stream closed before end")}，
         * gRPC 会丢弃尚未写到线上的消息 —— 排在最末尾的 COMPLETE 首当其冲。于是发送端
         * {@code putNext} 返回（ack 显示「已交付」），接收端却永远等不到这个发送方：
         * 线上「已收 5/6 个完成、最后 1 个 Aggregate 挂到看门狗」。</p>
         *
         * <p>服务端 {@code ExchangeFlightProducer.doExchange} 的响应在其读循环结束后立即发出，
         * 无背压时毫秒级返回；但读循环会阻塞在接收端的 {@code reader.onPage}（有界队列 put +
         * {@code NodeMemory.throttle}），所以等待时长等于下游消费速度，慢是常态而非故障。
         * 只有超过 {@link #SERVER_DONE_TIMEOUT_MS}（默认 10 分钟，与结果看门狗同量级）才取消。
         * 本方法在 {@link #SERVER_DONE_WAITERS} 上执行，不阻塞 flusher / 生产端收尾。</p>
         */
        private void awaitServerDoneThenClose() {
            try {
                Future<?> wait = SERVER_DONE_WAITERS.submit(listener::getResult);
                try {
                    wait.get(SERVER_DONE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                } catch (TimeoutException e) {
                    wait.cancel(true);
                    log.warn("交换流 {}s 内未收到接收端收尾，按超时取消（接收端下游可能长期背压："
                                    + "排序/溢写/队列满；可调 -Dexchange.serverDoneTimeoutMs）toWorkerId={}#{}",
                            SERVER_DONE_TIMEOUT_MS / 1000, toWorkerId, exchangeId);
                } catch (InterruptedException e) {
                    wait.cancel(true);
                    Thread.currentThread().interrupt();
                } catch (ExecutionException e) {
                    // 接收端处理本流报错（未注册接收端 / 路由失败等）：接收端自身也有失败传播
                    log.warn("交换流在接收端处理失败 toWorkerId={}#{}: {}", toWorkerId, exchangeId, e.getCause());
                }
            } finally {
                // 释放响应读端（每条流一个 FlightStream，不关会随查询次数在客户端 gRPC 通道上累积）
                closeStream();
            }
        }

        /** 丢弃一条未发送的数据页并归还其 buffer（COMPLETE/FAIL 的 page 为 null）。 */
        private static void closePage(Msg msg) {
            if (msg.page() != null) {
                try {
                    msg.page().close();
                } catch (Throwable ignored) {
                    // 已关闭 / 已被下游接管：忽略
                }
            }
        }

        /** 幂等关闭 doExchange 读写双子（释放响应读端 / gRPC 流资源）。 */
        private void closeStream() {
            if (streamClosed.compareAndSet(false, true)) {
                try {
                    stream.close();
                } catch (Throwable ignored) {
                    // 关闭期异常不应阻断整体收尾（流可能已被远端取消）
                }
            }
        }

        /** 数据页：拷贝进流根（复用根，首条定 schema）→ putNext(meta) → 关闭源页（所有权移交）。 */
        private void sendData(Msg msg) {
            Page page = msg.page();
            VectorSchemaRoot src = page.getRoot();
            if (streamRoot == null) {
                streamRoot = VectorSchemaRoot.create(src.getSchema(), allocator);
                streamRoot.allocateNew();
                listener.start(streamRoot);
                flushHeldControls();   // 真实 schema 已定：补发暂存的 COMPLETE/FAIL（顺序在首条 DATA 前）
            } else if (emptySchemaStarted && !src.getFieldVectors().isEmpty()) {
                // 兜底已用空 schema 建流（补偿型 COMPLETE 已发出），此时到流的 DATA 无法复用流：
                // Flight 流 schema 固定不可改，只能 fail-fast（罕见：2s 宽限后才来数据的慢 slice）
                throw new RuntimeException("exchange 流 " + toWorkerId + "#" + exchangeId
                        + " 已用空 schema 兜底启动（COMPLETE 已补发），迟到 DATA 无法复用流，须重跑查询");
            }
            int rc = page.getRowCount();
            for (int col = 0; col < src.getFieldVectors().size(); col++) {
                FieldVector s = src.getVector(col);
                FieldVector d = streamRoot.getVector(col);
                d.setInitialCapacity(rc);
                d.allocateNew();
                RowAccumulator.copyColumnBatch(s, d, 0, 0, rc);
                d.setValueCount(rc);
            }
            streamRoot.setRowCount(rc);
            putNext(msg.control());
            page.close();   // 拷贝完成，源页释放
        }

        /** 控制消息（COMPLETE/FAIL）：0 行 body，仅 metadata 携带 kind/桶号。 */
        private void sendControl(Msg msg) {
            if (streamRoot == null) {
                // 流未启动：暂存而非以空 schema 建流 —— 空 schema 流会令并发 DATA 拷入 0 列根崩溃。
                // 等首个 DATA 定真实 schema 后由 flushHeldControls 补发；全 exchange 无 DATA 时
                // 由 2s 宽限 / 关闭兜底以空 schema 补发，保证接收端一定收到结束标记。
                heldControls.add(msg);
                return;
            }
            streamRoot.setRowCount(0);
            putNext(msg.control());
            ack(msg);   // 已真正交给 Flight：等待送达确认的生产端可以返回
        }

        /**
         * 流已启动（真实或兜底空 schema）后，按暂存顺序补发全部控制消息。
         *
         * <p>每条发送后立即 ack；中途失败时把剩余未发消息一并 ack（它们确定未送达），
         * 由等待方在 {@code push} 里读到 {@code failure} 后上抛 —— 绝不静默丢弃。</p>
         */
        private void flushHeldControls() {
            if (heldControls.isEmpty()) return;
            List<Msg> batch = new ArrayList<>(heldControls);
            heldControls.clear();
            emptySince = -1;
            for (int i = 0; i < batch.size(); i++) {
                Msg c = batch.get(i);
                try {
                    streamRoot.setRowCount(0);
                    putNext(c.control());
                    ack(c);
                } catch (RuntimeException e) {
                    ack(c);
                    for (int j = i + 1; j < batch.size(); j++) {
                        ack(batch.get(j));
                    }
                    throw e;
                }
            }
        }

        private void putNext(ExchangeControl control) {
            byte[] ctrl = control.toByteArray();
            ArrowBuf metaBuf = allocator.buffer(ctrl.length);
            metaBuf.setBytes(0, ctrl);
            metaBuf.writerIndex(ctrl.length);
            // putNext 接管 metaBuf 所有权（写入后由 Flight 负责释放），勿再 close —— 否则双释放 RefCnt 为负
            listener.putNext(metaBuf);   // 同步序列化 + gRPC 流控背压（阻塞）
        }

        /** 生产端收尾：待发队列排空后 flusher 自行完成流、释放 root 并从通道摘除。 */
        void release() {
            released = true;
        }

        void close() {
            closed.set(true);
            try {
                flusher.join(3_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            // 释放响应读端（v1 无 per-batch ack，响应只有空 schema，无需消费）
            closeStream();
        }
    }
}
