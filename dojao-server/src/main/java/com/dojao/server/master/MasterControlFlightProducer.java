package com.dojao.server.master;

import com.dojao.common.data.Page;
import com.dojao.common.memory.NodeMemory;
import com.dojao.core.operator.RowAccumulator;
import com.dojao.core.rpc.FlightActions;
import com.dojao.core.rpc.QueryRequest;
import com.dojao.core.rpc.QueryResponse;
import com.dojao.core.rpc.control.ControlActions;
import com.dojao.core.rpc.control.ControlSerde;
import com.dojao.core.rpc.control.HeartbeatRequest;
import com.dojao.core.rpc.control.HeartbeatResponse;
import com.dojao.core.rpc.control.QueryFragmentTelemetryMsg;
import com.dojao.core.rpc.control.QueryTelemetryGroup;
import com.dojao.core.rpc.control.QueryTelemetryMsg;
import com.dojao.core.rpc.control.QueryTelemetryResponse;
import com.dojao.core.rpc.control.RegisterRequest;
import com.dojao.core.rpc.control.RegisterResponse;
import com.dojao.core.rpc.control.ResultEventMsg;
import com.dojao.core.rpc.control.UnregisterRequest;
import com.dojao.core.rpc.control.UnregisterResponse;
import com.dojao.core.runtime.master.WorkerRegistry;
import com.dojao.grammar.analyzer.exception.SemanticException;
import com.google.protobuf.InvalidProtocolBufferException;
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
 * Master 侧<b>合并控制面</b> FlightProducer —— 一个端口同时承载：
 * <ul>
 *   <li><b>查询入口</b>（doAction "execute_query"）：客户端提交 SQL，与旧
 *       {@link MasterQueryFlightProducer} 语义一致（请求 QueryRequest → 响应 QueryResponse）；</li>
 *   <li><b>Worker 注册/心跳/注销</b>（doAction "register"/"heartbeat"/"unregister"）：维护
 *       {@link WorkerRegistry}，register 响应回传当前集群节点表（Worker 据此构建交换拓扑）；</li>
 *   <li><b>结果回传</b>（doExchange "result/{taskId}"）：接收 Worker 推来的数据页 / 终止事件，
 *       按 taskId 查 {@link ResultRouter} 路由到对应任务的真实回传队列（背压：队列满 → gRPC
 *       流控 → Worker 生产阻塞）。</li>
 * </ul>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/29
 */
public final class MasterControlFlightProducer implements FlightProducer {

    private static final Logger log = LoggerFactory.getLogger(MasterControlFlightProducer.class);

    private final QueryServiceImpl queryService;
    private final WorkerRegistry registry;
    private final ResultRouter router;
    private final BufferAllocator allocator;

    public MasterControlFlightProducer(QueryServiceImpl queryService,
                                       WorkerRegistry registry,
                                       ResultRouter router,
                                       BufferAllocator allocator) {
        this.queryService = queryService;
        this.registry = registry;
        this.router = router;
        this.allocator = allocator;
    }

    // ==================== doAction：查询 + 注册/心跳/注销 ====================

    @Override
    public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
        switch (action.getType()) {
            case FlightActions.EXECUTE_QUERY -> executeQuery(action, listener);
            case ControlActions.REGISTER -> register(action, listener);
            case ControlActions.HEARTBEAT -> heartbeat(action, listener);
            case ControlActions.UNREGISTER -> unregister(action, listener);
            case ControlActions.QUERY_TELEMETRY -> queryTelemetry(action, listener);
            default -> listener.onError(CallStatus.UNIMPLEMENTED
                    .withDescription("未知 action: " + action.getType()).toRuntimeException());
        }
    }

    private void executeQuery(Action action, StreamListener<Result> listener) {
        try {
            QueryRequest request = QueryRequest.parseFrom(action.getBody());
            // SELECT 分块实时流式回传：executeQuery 每攒满一块就回调一次 onNext（master 内存有界 =
            // 有界结果队列 + 当前一块，客户端逐块消费即背压）；EXPLAIN / DDL / 写语句为单块。
            queryService.executeQuery(request, response ->
                    listener.onNext(new Result(response.toByteArray())));
            listener.onCompleted();
        } catch (SemanticException e) {
            log.warn("SQL 语义错误: {}", e.getMessage());
            listener.onError(CallStatus.INVALID_ARGUMENT
                    .withDescription(e.getMessage()).toRuntimeException());
        } catch (Exception e) {
            log.error("查询执行失败", e);
            listener.onError(CallStatus.INTERNAL
                    .withDescription(String.valueOf(e.getMessage())).toRuntimeException());
        }
    }

    private void register(Action action, StreamListener<Result> listener) {
        try {
            RegisterRequest req = RegisterRequest.parseFrom(action.getBody());
            var node = ControlSerde.fromNode(req.getNode());
            registry.register(node);
            log.info("Worker 注册: {}:{} id={}", node.host(), node.port(), shortId(node.workerId()));
            RegisterResponse resp = RegisterResponse.newBuilder()
                    .setOk(true)
                    .addAllClusterNodes(registry.all().stream()
                            .map(r -> ControlSerde.toNode(r.info())).toList())
                    .build();
            listener.onNext(new Result(resp.toByteArray()));
            listener.onCompleted();
        } catch (InvalidProtocolBufferException e) {
            onInvalid(listener, e);
        } catch (Throwable t) {
            onInternal(listener, t);
        }
    }

    private void heartbeat(Action action, StreamListener<Result> listener) {
        try {
            HeartbeatRequest req = HeartbeatRequest.parseFrom(action.getBody());
            String workerId = req.getNode().getWorkerId();
            boolean known = registry.get(workerId) != null;
            if (known) {
                registry.updateHeartbeat(workerId,
                        ControlSerde.fromState(req.getState()),
                        ControlSerde.fromResource(req.getResource()));
            }
            HeartbeatResponse resp = HeartbeatResponse.newBuilder()
                    .setOk(known)
                    .setError(known ? "" : "未知 Worker（未注册或已超时剔除）")
                    .build();
            listener.onNext(new Result(resp.toByteArray()));
            listener.onCompleted();
        } catch (InvalidProtocolBufferException e) {
            onInvalid(listener, e);
        } catch (Throwable t) {
            onInternal(listener, t);
        }
    }

    private void unregister(Action action, StreamListener<Result> listener) {
        try {
            UnregisterRequest req = UnregisterRequest.parseFrom(action.getBody());
            String workerId = req.getNode().getWorkerId();
            registry.unregister(workerId);
            log.info("Worker 注销: id={}", shortId(workerId));
            listener.onNext(new Result(UnregisterResponse.newBuilder().setOk(true).build().toByteArray()));
            listener.onCompleted();
        } catch (InvalidProtocolBufferException e) {
            onInvalid(listener, e);
        } catch (Throwable t) {
            onInternal(listener, t);
        }
    }

    /**
     * Worker → Master 查询遥测归并：按 queryId 路由到该查询的 {@link QueryManager}（一 query 一
     * 实例，统一管控），把「数据不回传 Master」片段的实时行数 / 收发字节写进它的覆盖表 —— 只覆盖
     * Producer/中间层（Worker 也只上报这些），终端 Consumer / 独立 Scan 叶子的权威值不受影响。
     * 查询已结束（Registry 查不到）→ 整组丢弃。best-effort：均为单调累计值、按 taskId 覆盖。
     *
     * <p><b>僵尸片段回收（本帧的第二个职责）</b>：Worker 在 {@code running_query_ids} 里带上它「正在跑」
     * 的 jobId，Master 逐一对 {@link QueryManagerRegistry#isOperatorQueryActive} 判定，把<b>已不认</b>
     * 的子集经 {@code stale_query_ids} 回信 —— Worker 收到即取消这些 jobId 的全部本地片段。判据是单向的
     * （Master 说不活才杀），所以绝不会误杀活跃查询；这正是「上一次查询的残留片段占住池槽位、把后续查询
     * 拖入死锁」那条链的药（见 {@code WorkerManager} 的池看门狗说明）。</p>
     */
    private void queryTelemetry(Action action, StreamListener<Result> listener) {
        try {
            QueryTelemetryMsg msg = QueryTelemetryMsg.parseFrom(action.getBody());
            int appliedGroups = 0;
            int appliedFrags = 0;
            QueryManagerRegistry registry = QueryManagerRegistry.get();
            for (QueryTelemetryGroup group : msg.getQueriesList()) {
                // 分组键 = Worker 端片段派发 jobId = Master 分布式算子的 queryId（"query-<hex>"，见
                // AbstractDistributedOperator.dispatch），不是 QueryManager.queryId（yyyyMMddHHmmss_UUID）。
                // 直接 get() 恒 miss → 先按 operator queryId 反查归属的 QueryManager（active 优先 recent 兜底）。
                QueryManager qm = registry.get(group.getQueryId());
                if (qm == null) {
                    qm = registry.findByOperatorQueryId(group.getQueryId());
                }
                if (qm == null) {
                    continue;   // 已结束 / 未知查询：丢弃该组
                }
                appliedGroups++;
                for (QueryFragmentTelemetryMsg frag : group.getFragmentsList()) {
                    qm.acceptFragmentTelemetry(frag.getTaskId(), frag.getRowsEmitted(),
                            frag.getBytesIn(), frag.getBytesOut(), frag.getSpilledBytes(), frag.getDone());
                    appliedFrags++;
                }
            }
            // 僵尸片段回收回信：Worker 报上来的「在跑」jobId 里，Master 已不认的那些 → 交回 Worker 就地取消
            List<String> stale = new ArrayList<>();
            for (String jobId : msg.getRunningQueryIdsList()) {
                if (!registry.isOperatorQueryActive(jobId)) {
                    stale.add(jobId);
                }
            }
            if (!stale.isEmpty()) {
                log.warn("Worker {} 上报 {} 个仍在跑的查询中 {} 个 Master 已不认 → 回信令其回收残留片段: {}",
                        shortId(msg.getNode().getWorkerId()), msg.getRunningQueryIdsCount(),
                        stale.size(), stale.stream().map(MasterControlFlightProducer::shortId).toList());
            }
            log.debug("查询遥测接收: workerId={}, groups={}, fragments={}, applied={}, stale={}",
                    shortId(msg.getNode().getWorkerId()), msg.getQueriesCount(),
                    msg.getQueriesList().stream().mapToInt(QueryTelemetryGroup::getFragmentsCount).sum(),
                    appliedFrags, stale.size());
            QueryTelemetryResponse resp = QueryTelemetryResponse.newBuilder()
                    .setOk(true).setError("")
                    .addAllStaleQueryIds(stale)
                    .build();
            listener.onNext(new Result(resp.toByteArray()));
            listener.onCompleted();
        } catch (InvalidProtocolBufferException e) {
            onInvalid(listener, e);
        } catch (Throwable t) {
            onInternal(listener, t);
        }
    }

    // ==================== doExchange：结果回传 ====================

    @Override
    public void doExchange(CallContext context, FlightStream flightStream, ServerStreamListener listener) {
        List<String> path = flightStream.getDescriptor().getPath();
        if (path.size() != 2 || !ControlActions.RESULT_PREFIX.equals(path.get(0))) {
            listener.error(CallStatus.INVALID_ARGUMENT
                    .withDescription("期望 path=['result', taskId]，实际 " + path).toRuntimeException());
            return;
        }
        String taskId = path.get(1);
        ResultSink sink = router.get(taskId);
        log.info("doExchange 开始: taskId={} sink存在={}", taskId, sink != null);
        boolean terminalDelivered = false;
        try {
            while (flightStream.next()) {
                // Worker 惰性建流后连发 descriptor/schema（HeaderType.NONE/SCHEMA，均无 metadata），
                // 它们只用于建流握手、不承载事件，须跳过 —— 否则 metadataBytes(null) 抛 NPE（"buf is null"）。
                // 数据页 / FINISHED / FAILED / PROGRESS 都带 ResultEventMsg metadata。注意 PROGRESS 是
                // metadata-only 消息（无 body），可能出现在 schema 之前 —— 此时 getRoot() 会阻塞等 schema，
                // 故 PROGRESS 分支不得调用 getRoot()（见下方 case PROGRESS）。
                ArrowBuf metaBuf = flightStream.getLatestMetadata();
                if (metaBuf == null) {
                    // schema / 无 metadata 的握手消息：此时 schema 必已到达（或本消息即 schema），
                    // getRoot() 不阻塞。带行数据却缺失 metadata 的消息（DATA 页 metadata 丢失场景）
                    // → 按 DATA 路由，行数据比 metadata 更可信；0 行握手消息照常跳过。
                    VectorSchemaRoot root = flightStream.getRoot();
                    if (root.getRowCount() > 0 && sink != null) {
                        // 接收端「即将占用更多堆外」——先过背压闸门，把多页滞留压回读取端
                        NodeMemory.shared().throttle();
                        sink.data(copyPage(root, allocator));
                    }
                    continue;
                }
                ResultEventMsg event = ResultEventMsg.parseFrom(metadataBytes(metaBuf));
                switch (event.getKind()) {
                    case DATA -> {
                        // DATA 必有前置 schema（Worker 先 start 后 putNext），getRoot() 不阻塞
                        VectorSchemaRoot root = flightStream.getRoot();
                        if (root.getRowCount() > 0 && sink != null) {
                            // 拷贝进 Master 分配器 → 路由入队；队列满阻塞 = 背压（Worker 生产随之阻塞）。
                            // throttle 在拷贝前施加：堆外高水位时先等溢写/消费把用量压下来，
                            // 避免结果回传洪峰把 Arrow 预算（尤其单机回退的多窗口分组）瞬间吃穿。
                            NodeMemory.shared().throttle();
                            sink.data(copyPage(root, allocator));
                        }
                    }
                    case FINISHED -> {
                        log.info("doExchange 完成: taskId={} rows={} sink存在={}",
                                taskId, event.getRowCount(), sink != null);
                        terminalDelivered = true;
                        if (sink != null) {
                            sink.finished(event.getRowCount());   // 分布式写行数经 FINISHED 路由
                        } else {
                            // 结果汇已不在（任务被摘除 / taskId 不匹配）：绝不能静默丢弃完成通知
                            log.error("FINISHED 无对应结果汇，该任务只能等看门狗超时: taskId={} rows={}",
                                    taskId, event.getRowCount());
                        }
                    }
                    case FAILED -> {
                        log.warn("doExchange 失败: taskId={} cause={}", taskId, event.getCause());
                        terminalDelivered = true;
                        if (sink != null) {
                            sink.failed(event.getCause());
                        } else {
                            log.error("FAILED 无对应结果汇，失败原因将丢失: taskId={} cause={}",
                                    taskId, event.getCause());
                        }
                    }
                    case PROGRESS -> {
                        // 心跳：无 body 的 metadata-only 消息（root 可能为 null —— 首个数据页前的
                        // 心跳），只重置 Master 看门狗，不路由数据 / 不触发完成。
                        if (sink != null) sink.progress();
                    }
                    default -> {
                    }
                }
            }
            // 流正常半关闭但从未收到 FINISHED/FAILED：Worker 侧片段被中断（取消）/ 被池判死 /
            // 终止事件丢失。旧实现静默 unregister → 算子只能等结果看门狗 10 分钟
            // （线上「已收 4/6 个完成、挂起 600069ms」）。取消路径已被 markCancelled 标记，跳过。
            if (!terminalDelivered && sink != null && !router.isCancelled(taskId)) {
                String reason = "结果流已结束但未收到 FINISHED/FAILED 终止事件"
                        + "（Worker 片段可能被中断/判死，或终止事件丢失）";
                log.error("{}: taskId={}", reason, taskId);
                sink.streamEnded(reason);
            }
            // v1 无 per-batch ack：仅声明空 schema + 正常收尾，Worker 忽略响应流。
            // ★Flight 不接管 start() 传入 root 的所有权（VectorUnloader 只引用、不释放），
            //   须由调用方关闭，否则响应 root 的 buffer 会滞留在 Master allocator 上。
            VectorSchemaRoot response = emptyRoot(allocator);
            try {
                listener.start(response);
                listener.completed();
            } finally {
                response.close();
            }
        } catch (Exception e) {
            // 关键：不吞异常 —— Worker 全部 FINISHED 未送达时这里是最先暴露的地方
            log.error("doExchange 处理异常: taskId={}", taskId, e);
            if (!terminalDelivered && sink != null && !router.isCancelled(taskId)) {
                try {
                    sink.streamEnded("结果流异常中断且未收到 FINISHED/FAILED: " + e);
                } catch (Exception sinkError) {
                    e.addSuppressed(sinkError);
                }
            }
            listener.error(e);
        } finally {
            router.unregister(taskId);
        }
    }

    // ==================== 内部 ====================

    private static void onInvalid(StreamListener<Result> listener, InvalidProtocolBufferException e) {
        listener.onError(CallStatus.INVALID_ARGUMENT
                .withDescription("非法载荷: " + e.getMessage()).toRuntimeException());
    }

    private static void onInternal(StreamListener<Result> listener, Throwable t) {
        log.error("Master 控制面执行失败", t);
        listener.onError(CallStatus.INTERNAL
                .withDescription(String.valueOf(t)).toRuntimeException());
    }

    private static String shortId(String workerId) {
        return workerId.substring(0, Math.min(8, workerId.length()));
    }

    /** 原生 batch → 独立 allocator 的页（逐列拷贝，var 列安全；与交换接收端同构）。 */
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
        byte[] out = new byte[Math.toIntExact(buf.readableBytes())];
        buf.readBytes(out);
        return out;
    }

    // ==================== 未使用 ====================

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        listener.error(CallStatus.UNIMPLEMENTED.withDescription("Master 走 doAction / doExchange，不用 getStream").toRuntimeException());
    }

    @Override
    public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
        return () -> ackStream.onError(CallStatus.UNIMPLEMENTED
                .withDescription("Master 走 doAction / doExchange，不用 acceptPut").toRuntimeException());
    }

    @Override
    public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
        listener.onCompleted();
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        throw CallStatus.UNIMPLEMENTED.withDescription("Master 走 doAction / doExchange，不用 getFlightInfo").toRuntimeException();
    }

    @Override
    public void listActions(CallContext context, StreamListener<ActionType> listener) {
        listener.onCompleted();
    }
}
