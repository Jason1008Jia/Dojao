package com.dojao.server.master;

import com.dojao.common.data.Page;
import com.dojao.common.memory.NodeMemory;
import com.dojao.core.operator.FragmentSpec;
import com.dojao.core.rpc.control.CancelTaskRequest;
import com.dojao.core.rpc.control.CatalogDefMsg;
import com.dojao.core.rpc.control.ControlActions;
import com.dojao.core.rpc.control.ControlSerde;
import com.dojao.core.rpc.control.DispatchAck;
import com.dojao.core.rpc.control.DispatchFragmentRequest;
import com.dojao.core.rpc.control.DispatchScanRequest;
import com.dojao.core.runtime.master.WorkerRegistration;
import com.dojao.core.runtime.master.WorkerRegistry;
import com.dojao.core.runtime.worker.ExchangeChannel;
import com.dojao.core.runtime.worker.FragmentEvent;
import com.dojao.core.runtime.worker.ScanEvent;
import com.dojao.core.runtime.worker.WorkerGateway;
import com.dojao.core.runtime.worker.WorkerNode;
import com.dojao.metastore.MetaStore;
import com.dojao.server.exchange.NetworkExchangeChannel;
import com.dojao.server.function.FunctionManager;
import com.dojao.spi.bean.Catalog;
import com.dojao.spi.bean.FunctionDefinition;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Result;
import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Master → Worker 的<b>真实网络</b>派发网关 —— Arrow Flight doAction 实现 {@link WorkerGateway}：
 * 分区扫描 / 执行片段派发 + 任务取消。与伪分布式的内存版对称，跨 JVM 进程。</p>
 *
 * <p><b>结果回传</b>：派发时先在 {@link ResultRouter} 注册 taskId → 结果汇（把 Worker 推来的
 * 页 / 终止事件落进调用方给的真实回传队列），再发 doAction 派发请求；Worker 执行完后经
 * doExchange "result/{taskId}" 推结果，Master 侧 {@link MasterControlFlightProducer} 查表路由。
 * 派发 ack 失败 → 立即摘除路由并往回传队列投递失败事件（调用方 fail-fast）。</p>
 *
 * <p><b>拓扑附带</b>：每次派发载荷携带当前集群节点表（{@link WorkerRegistry} 全部注册节点），
 * Worker 侧增量学习，解决"先注册的 Worker 不知道后注册的兄弟"的交换路由问题。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/29
 */
public final class NetworkWorkerGateway implements WorkerGateway {

    private static final Logger log = LoggerFactory.getLogger(NetworkWorkerGateway.class);

    private final ResultRouter router;
    private final WorkerRegistry registry;
    private final MetaStore metaStore;
    private final BufferAllocator allocator = NodeMemory.shared().newChildAllocator("gateway/worker", 0);
    private final ConcurrentHashMap<String, FlightClient> clients = new ConcurrentHashMap<>();

    public NetworkWorkerGateway(ResultRouter router, WorkerRegistry registry, MetaStore metaStore) {
        this.router = router;
        this.registry = registry;
        this.metaStore = metaStore;
    }

    @Override
    public void dispatchScan(String workerId, String jobId, String taskId,
                             String catalog, com.dojao.spi.bean.ScanSlice slice,
                             FragmentSpec.SortLimitStep tail,
                             BlockingQueue<ScanEvent> resultQueue) {
        dispatchScan(workerId, jobId, taskId, catalog, slice, tail, null, null, resultQueue);
    }

    @Override
    public void dispatchScan(String workerId, String jobId, String taskId,
                             String catalog, com.dojao.spi.bean.ScanSlice slice,
                             FragmentSpec.SortLimitStep tail,
                             FragmentSpec.ScanProjection projection,
                             FragmentSpec.SinkStep sink,
                             BlockingQueue<ScanEvent> resultQueue) {
        ResultSink resultSink = new ScanSink(taskId, resultQueue);
        router.register(taskId, resultSink);
        DispatchScanRequest.Builder b = DispatchScanRequest.newBuilder()
                .setWorkerId(workerId)
                .setJobId(jobId)
                .setTaskId(taskId)
                .setCatalog(catalog)
                .setRequest(ControlSerde.toRead(slice.request()))
                .addAllClusterNodes(clusterNodes())
                .addAllCatalogs(catalogDefsFor(
                        sink == null || sink.catalog().equals(catalog)
                                ? List.of(catalog)
                                : List.of(catalog, sink.catalog())));
        // 切片私有载荷（连接器自定义字节，引擎只搬运不解释）；空 = 整表切片
        if (slice.hasSplit()) {
            b.setSplitPayload(com.google.protobuf.ByteString.copyFrom(slice.split().payload()));
        }
        if (tail != null) {
            b.setTail(ControlSerde.toSortLimitStep(tail));
        }
        if (projection != null) {
            b.setProjection(ControlSerde.toScanProjection(projection));
        }
        if (sink != null) {
            b.setSink(ControlSerde.toSinkStep(sink));
        }
        DispatchScanRequest req = b.build();
        if (!dispatch(workerId, ControlActions.DISPATCH_SCAN, req)) {
            router.unregister(taskId);
            offer(resultQueue, new ScanEvent.ScanFailed(taskId,
                    "派发失败 worker=" + shortId(workerId) + "（Worker 不可达或已停止）"));
        }
    }

    @Override
    public void dispatchFragment(String workerId, String jobId, String taskId,
                                 FragmentSpec spec, BlockingQueue<FragmentEvent> resultQueue) {
        dispatchFragment(workerId, jobId, taskId, spec, resultQueue, null);
    }

    @Override
    public void dispatchFragment(String workerId, String jobId, String taskId,
                                 FragmentSpec spec, BlockingQueue<FragmentEvent> resultQueue,
                                 BlockingQueue<FragmentEvent> controlQueue) {
        // Producer 片段从不经 doExchange 回传 Master（数据走 exchange 直连下一层），注册 sink 只会
        // 因 Worker 侧惰性建流（见 ResultStream）而永不 unregister → 泄漏。Consumer 才需要路由。
        if (!spec.isProducer()) {
            router.register(taskId, new FragmentSink(taskId, resultQueue, controlQueue));
        }
        DispatchFragmentRequest req = DispatchFragmentRequest.newBuilder()
                .setWorkerId(workerId)
                .setJobId(jobId)
                .setTaskId(taskId)
                .setSpec(ControlSerde.toFragment(spec))
                .addAllClusterNodes(clusterNodes())
                .addAllFunctions(referencedDefinitions(spec).stream()
                        .map(ControlSerde::toFunctionDefinition).toList())
                .addAllCatalogs(catalogDefsFor(FragmentSpec.catalogsOf(spec)))
                .build();
        if (!dispatch(workerId, ControlActions.DISPATCH_FRAGMENT, req)) {
            router.unregister(taskId);
            FragmentEvent failed = new FragmentEvent.FragmentFailed(taskId,
                    "派发失败 worker=" + shortId(workerId) + "（Worker 不可达或已停止）");
            // 控制队列无界 → 失败原因绝不因数据队列满而丢
            if (controlQueue != null) controlQueue.offer(failed);
            else offer(resultQueue, failed);
        }
    }

    @Override
    public void cancelTask(String workerId, String taskId) {
        // 先标记：取消后 Worker 侧片段通常「静默退出」（中断 ≠ 失败，不回传终止事件），
        // 结果流随之半关闭 —— 接收端不得把它当成「终止事件丢失」上报失败。
        router.markCancelled(taskId);
        CancelTaskRequest req = CancelTaskRequest.newBuilder()
                .setWorkerId(workerId).setTaskId(taskId).build();
        dispatch(workerId, ControlActions.CANCEL_TASK, req);
    }

    // ==================== Master→Worker 数据直推（exchange 数据面） ====================

    @Override
    public boolean supportsExchangePush() {
        return true;
    }

    /**
     * 新建一条 Master→Worker 数据交换通道：Master 作发送方、不 own 任何 worker 桶 ——
     * localWorkerIds 置空 + bus 置空 → {@code isSelf} 恒 false，所有桶全部走网络 Flight doExchange。
     * remote 地址表 = 当前注册表全部 worker（{@code WorkerNode} 带 host:port），复用
     * worker↔worker 通道类（{@link NetworkExchangeChannel}）与其共享分配器（同 root → 零拷贝）。
     * 生命周期归调用算子所有，用毕须 {@link #releaseExchangeChannel} 关闭。
     */
    @Override
    public ExchangeChannel exchangeChannel() {
        Map<String, WorkerNode> remotes = new java.util.LinkedHashMap<>();
        for (WorkerRegistration r : registry.all()) {
            remotes.put(r.info().workerId(), r.info());
        }
        return new NetworkExchangeChannel("master", java.util.Set.of(), remotes,
                new ConcurrentHashMap<>(), allocator);
    }

    @Override
    public void releaseExchangeChannel(ExchangeChannel channel) {
        if (channel instanceof NetworkExchangeChannel nc) {
            try {
                nc.close();   // 停 flusher + 关到各 Worker 的 FlightClient（幂等）
            } catch (Throwable ignored) {
            }
        }
    }

    /** 关闭全部到 Worker 的 FlightClient（Master 停止时调用）。 */
    public void close() {
        for (FlightClient c : clients.values()) {
            try {
                c.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        clients.clear();
        allocator.close();
    }

    // ==================== 内部 ====================

    private boolean dispatch(String workerId, String type, Message req) {
        try {
            WorkerNode node = nodeOf(workerId);
            if (node == null) {
                log.warn("派发失败 {} → 未知 Worker: {}", type, workerId);
                return false;
            }
            FlightClient client = clientFor(node);
            Iterator<Result> results = client.doAction(new Action(type, req.toByteArray()));
            if (!results.hasNext()) {
                log.warn("派发失败 {} → {}: 无 ack", type, node.host() + ":" + node.port());
                return false;
            }
            DispatchAck ack = DispatchAck.parseFrom(results.next().getBody());
            if (!ack.getOk()) {
                log.warn("派发失败 {} → {}: {}", type, node.host() + ":" + node.port(), ack.getError());
                return false;
            }
            return true;
        } catch (FlightRuntimeException e) {
            log.warn("派发失败 {} → {}: {}", type, shortId(workerId), e.status().description());
            return false;
        } catch (InvalidProtocolBufferException e) {
            log.warn("派发失败 {} → {}: 非法 ack", type, shortId(workerId));
            return false;
        } catch (Exception e) {
            log.warn("派发异常 {} → {}: {}", type, shortId(workerId), e.toString());
            return false;
        }
    }

    private WorkerNode nodeOf(String workerId) {
        var reg = registry.get(workerId);
        return reg != null ? reg.info() : null;
    }

    private FlightClient clientFor(WorkerNode node) {
        return clients.computeIfAbsent(node.workerId(), id -> FlightClient.builder()
                .location(Location.forGrpcInsecure(node.host(), node.port()))
                .allocator(allocator)
                .build());
    }

    /** 当前集群全部注册节点表（派发附带，Worker 增量学习交换拓扑）。 */
    private List<com.dojao.core.rpc.control.WorkerNodeMsg> clusterNodes() {
        return registry.all().stream()
                .map(r -> ControlSerde.toNode(r.info()))
                .toList();
    }

    /**
     * 本片段聚合引用的<b>自定义</b>函数定义（L2：真分布式下 Worker 独立 JVM，须随载荷带过去）。
     * 内置 count/sum/avg/min/max 不在 {@link FunctionManager} 定义表里（get 返回 null，自然排除）。
     * 目前自定义函数只经聚合片段（AggregateConsumer / ChainedJoinConsumer 的聚合后续 /
     * MiddleAggregateConsumer）与窗口片段（MiddleWindowConsumer 的 AGGREGATE 型窗口函数）引用。
     */
    private static List<FunctionDefinition> referencedDefinitions(FragmentSpec spec) {
        List<String> names = new java.util.ArrayList<>();
        if (spec instanceof FragmentSpec.AggregateConsumer a) {
            a.aggregate().aggregations().forEach(dto -> names.add(dto.aggregateName()));
        } else if (spec instanceof FragmentSpec.ChainedJoinConsumer cj
                && cj.followUp() instanceof FragmentSpec.AggregateStep agg) {
            agg.aggregations().forEach(dto -> names.add(dto.aggregateName()));
        } else if (spec instanceof FragmentSpec.MiddleAggregateConsumer ma) {
            ma.aggregate().aggregations().forEach(dto -> names.add(dto.aggregateName()));
        } else if (spec instanceof FragmentSpec.MiddleWindowConsumer mw) {
            mw.window().functions().forEach(f -> {
                if (f.functionName() != null) names.add(f.functionName());
            });
        }
        return names.stream()
                .map(FunctionManager::get)
                .filter(java.util.Objects::nonNull)
                .distinct()
                .toList();
    }

    /**
     * 把任务/片段会用到的 catalog <b>定义</b>（type + 连接属性）附进派发载荷 —— 与 cluster_nodes /
     * functions 同款：真分布式下 Worker 是独立 JVM，不该假设它与 Master 共享元数据库（甚至未必同机），
     * 也不该要求它预置 catalog。带上定义后，Worker 凭载荷即可现场构造连接器。
     *
     * <p>名字取不到定义 = 规划期与派发期的内部不一致（规划期已确认过 catalog 存在），
     * 直接抛出 fail-fast，好过让 Worker 端报一个没头没尾的「未知 catalog」。</p>
     */
    private List<CatalogDefMsg> catalogDefsFor(Collection<String> names) {
        if (names.isEmpty()) return List.of();
        List<CatalogDefMsg> out = new ArrayList<>(names.size());
        for (String name : names) {
            // 物化 CTE 的哨兵 catalog 不是注册表里的数据源：Worker 用切片载荷自带的无状态解码
            // 连接器读取（MaterializedCteConnector.wire()），无需（也无法）下发连接定义。
            if (com.dojao.core.operator.MaterializedCteConnector.isCteCatalog(name)) {
                continue;
            }
            Catalog def = metaStore.getRegisteredCatalog(name).orElseThrow(() ->
                    new IllegalStateException("catalog '" + name + "' 未注册，无法随任务派发其定义"
                            + "（规划期应已确认存在，此处不一致属内部错误）"));
            out.add(CatalogDefMsg.newBuilder()
                    .setName(def.name())
                    .setType(def.type())
                    .putAllProperties(def.properties())
                    .build());
        }
        return out;
    }

    private static void offer(BlockingQueue<?> queue, Object event) {
        try {
            @SuppressWarnings("unchecked")
            BlockingQueue<Object> q = (BlockingQueue<Object>) queue;
            q.offer(event, 5, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 投递<b>终止事件</b>（FINISHED / FAILED）并确认已入队。
     *
     * <p>旧实现直接 {@code queue.offer(...)} 且忽略返回值 —— 队列满或消费方已离开时，成功/失败
     * 通知被静默丢弃，Master 侧算子既看不到完成也看不到失败，只能等结果看门狗 10 分钟
     * （线上「已收 4/6 个完成、挂起 600069ms」）。失败即抛，由 doExchange 统一记 ERROR。</p>
     */
    static void deliverTerminal(BlockingQueue<?> queue, Object event, String what) {
        boolean delivered;
        try {
            @SuppressWarnings("unchecked")
            BlockingQueue<Object> q = (BlockingQueue<Object>) queue;
            delivered = q.offer(event, 5, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("投递「" + what + "」被中断", ie);
        }
        if (!delivered) {
            throw new IllegalStateException("投递「" + what + "」失败：回传队列 5s 内未被消费"
                    + "（算子可能已终止）: " + event);
        }
    }

    /** 结果流结束却无终止事件：尽力把该任务转成失败（消费方已离开时只记 ERROR，不阻塞 Flight 线程）。 */
    static void endAsFailed(BlockingQueue<?> queue, Object event, String reason) {
        boolean delivered;
        try {
            @SuppressWarnings("unchecked")
            BlockingQueue<Object> q = (BlockingQueue<Object>) queue;
            delivered = q.offer(event, 1, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            delivered = false;
        }
        if (!delivered) {
            log.error("结果流无终止事件结束，且失败事件无法投递（消费方已结束）: {} cause={}",
                    event, reason);
        }
    }

    private static String shortId(String workerId) {
        return workerId.substring(0, Math.min(8, workerId.length()));
    }

    /** 扫描任务的结果汇：Worker 推来的页 / 终止事件 → 落进 ScanEvent 队列（阻塞背压）。 */
    record ScanSink(String taskId, BlockingQueue<ScanEvent> queue) implements ResultSink {
        @Override
        public void data(Page page) throws Exception {
            queue.put(new ScanEvent.ScanPage(taskId, page));
        }

        @Override
        public void finished(long rowCount) throws Exception {
            // 普通扫描 rowCount 恒 0；scan-sink 携带目标表写入行数。终止事件必须确认交付
            // （静默丢弃 = 扫描 Task 永远 RUNNING）
            deliverTerminal(queue, new ScanEvent.ScanFinished(taskId, rowCount), "扫描完成事件");
        }

        @Override
        public void failed(String cause) throws Exception {
            deliverTerminal(queue, new ScanEvent.ScanFailed(taskId, cause), "扫描失败事件");
        }

        @Override
        public void streamEnded(String reason) throws Exception {
            endAsFailed(queue, new ScanEvent.ScanFailed(taskId, reason), reason);
        }
    }

    /**
     * 片段任务的结果汇：数据页 → 有界队列（背压）；终止/心跳 → 无界<b>控制队列</b>。
     *
     * <p>{@code queue} 有界（32），消费方可能合法地长时间不读（UNION 逐腿预取）。终止事件若进
     * {@code queue}，只能 5s 超时后抛错丢弃 —— 算子永远等不到该片段完成。{@code controlQueue}
     * 非 null 时终止事件永不丢；null（旧签名 / 测试桩）保持原「5s 未送达即抛」语义。</p>
     */
    record FragmentSink(String taskId, BlockingQueue<FragmentEvent> queue,
                        BlockingQueue<FragmentEvent> controlQueue) implements ResultSink {

        FragmentSink(String taskId, BlockingQueue<FragmentEvent> queue) {
            this(taskId, queue, null);
        }

        @Override
        public void data(Page page) throws Exception {
            queue.put(new FragmentEvent.FragmentData(taskId, page));
        }

        @Override
        public void finished(long rowCount) throws Exception {
            terminal(new FragmentEvent.FragmentFinished(taskId, rowCount), "片段完成事件");
        }

        @Override
        public void failed(String cause) throws Exception {
            terminal(new FragmentEvent.FragmentFailed(taskId, cause), "片段失败事件");
        }

        @Override
        public void streamEnded(String reason) throws Exception {
            FragmentEvent ev = new FragmentEvent.FragmentFailed(taskId, reason);
            // 无终止事件结束的兜底：有控制队列则必达；旧路径尽力投递（不阻塞 Flight 线程）
            if (controlQueue != null) controlQueue.offer(ev);
            else endAsFailed(queue, ev, reason);
        }

        @Override
        public void progress() throws Exception {
            // 心跳：只刷新看门狗。控制队列无界 → 不阻塞 Flight 线程；旧路径沿用 offer。
            if (controlQueue != null) controlQueue.offer(new FragmentEvent.FragmentProgress(taskId));
            else queue.offer(new FragmentEvent.FragmentProgress(taskId), 5, TimeUnit.SECONDS);
        }

        /** 终止事件：有控制队列则必达（无界）；否则退回「5s 未送达即抛」的 fail-fast 语义。 */
        private void terminal(FragmentEvent event, String what) {
            if (controlQueue != null) {
                controlQueue.offer(event);
                return;
            }
            deliverTerminal(queue, event, what);
        }
    }
}
