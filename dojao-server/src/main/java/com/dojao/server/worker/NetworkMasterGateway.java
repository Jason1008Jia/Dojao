package com.dojao.server.worker;

import com.dojao.common.memory.NodeMemory;
import com.dojao.core.rpc.control.ControlActions;
import com.dojao.core.rpc.control.ControlSerde;
import com.dojao.core.rpc.control.HeartbeatRequest;
import com.dojao.core.rpc.control.HeartbeatResponse;
import com.dojao.core.rpc.control.QueryTelemetryMsg;
import com.dojao.core.rpc.control.QueryTelemetryResponse;
import com.dojao.core.rpc.control.RegisterRequest;
import com.dojao.core.rpc.control.RegisterResponse;
import com.dojao.core.rpc.control.UnregisterRequest;
import com.dojao.core.rpc.control.UnregisterResponse;
import com.dojao.core.runtime.worker.ResourceSummary;
import com.dojao.core.runtime.worker.WorkerNode;
import com.dojao.core.runtime.worker.WorkerState;
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

import java.util.Iterator;
import java.util.List;

/**
 * Worker → Master 的<b>真实网络</b>通信网关 —— Arrow Flight doAction 实现 {@link MasterGateway}：
 * 注册 / 心跳 / 注销。与伪分布式的内存版对称，跨 JVM 进程。</p>
 *
 * <p><b>集群拓扑引导</b>：{@link #register} 的响应 {@code RegisterResponse} 携带 Master 当前
 * 已知的全量集群节点表，本网关缓存为 {@link #clusterNodes()}，Worker 据此构建
 * {@code NetworkExchangeManager}（worker↔worker 直连拓扑）。后续 Worker 逐一注册后 Master 还会
 * 在每次派发请求里附带最新节点表，Worker 侧 {@code learnNodes} 增量学习（见
 * {@code NetworkExchangeManager.learnNodes}），解决"先注册的 Worker 不知道后注册的兄弟"问题。</p>
 *
 * <p>本网关暴露 {@link #masterClient()} 供注册/心跳/注销 doAction 复用；结果回传
 * （doExchange "result/{taskId}"，见 {@link NetworkFragmentResultQueue}）则用
 * {@link #openResultClient(BufferAllocator)} 建<b>专用独立客户端</b>——必须用 Worker 交换层
 * 共享分配器（与结果流根同 root，zero-copy 安全；镜像 {@code NetworkExchangeChannel} 的独立
 * 客户端模式）。若复用本网关自带 root 的 {@code masterClient}，跨 root 序列化会抛异常并把
 * gRPC 流判死（Master 侧表现为 "client cancelled"）。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/29
 */
public final class NetworkMasterGateway implements MasterGateway {

    private static final Logger log = LoggerFactory.getLogger(NetworkMasterGateway.class);

    private final String masterHost;
    private final int masterPort;
    private final BufferAllocator allocator = NodeMemory.shared().newChildAllocator("gateway/master", 0);
    private volatile FlightClient client;
    /** 结果回传专用缓存客户端（本 Worker 所有 result 流共享一条；见 {@link #openResultClient}）。 */
    private volatile FlightClient resultClient;
    private volatile List<WorkerNode> clusterNodes = List.of();

    public NetworkMasterGateway(String masterHost, int masterPort) {
        this.masterHost = masterHost;
        this.masterPort = masterPort;
    }

    @Override
    public boolean register(WorkerNode info) {
        RegisterRequest req = RegisterRequest.newBuilder()
                .setNode(ControlSerde.toNode(info))
                .build();
        RegisterResponse resp = doAction(ControlActions.REGISTER, req, RegisterResponse::parseFrom);
        if (resp.getOk()) {
            clusterNodes = resp.getClusterNodesList().stream()
                    .map(ControlSerde::fromNode).toList();
            log.info("注册成功，当前集群 {} 个节点: {}",
                    clusterNodes.size(), clusterNodes.stream().map(n -> n.host() + ":" + n.port()).toList());
        } else {
            log.error("注册被 Master 拒绝: {}", resp.getError());
        }
        return resp.getOk();
    }

    @Override
    public boolean heartbeat(WorkerNode info, WorkerState state, ResourceSummary summary) {
        HeartbeatRequest req = HeartbeatRequest.newBuilder()
                .setNode(ControlSerde.toNode(info))
                .setState(ControlSerde.toState(state))
                .setResource(ControlSerde.toResource(summary))
                .build();
        HeartbeatResponse resp = doAction(ControlActions.HEARTBEAT, req, HeartbeatResponse::parseFrom);
        return resp.getOk();
    }

    @Override
    public boolean unregister(WorkerNode info) {
        UnregisterRequest req = UnregisterRequest.newBuilder()
                .setNode(ControlSerde.toNode(info))
                .build();
        UnregisterResponse resp = doAction(ControlActions.UNREGISTER, req, UnregisterResponse::parseFrom);
        return resp.getOk();
    }

    /**
     * 上抛查询遥测（非终端片段累计行数，~1s 一帧；best-effort —— 失败只降级日志，
     * Master 看门狗 / 任务事件兜底，遥测丢帧下拍自愈）。走注册/心跳同一条控制 client。
     *
     * @return Master 判定的「已不认」jobId 列表（请求 {@code running_query_ids} 的子集，= 可回收的
     *         残留片段）；上抛失败 / 被拒时返回<b>空表</b> —— 判据只会「少杀」不会「误杀」，
     *         网络抖动绝不会导致 Worker 取消活查询。
     */
    @Override
    public List<String> telemetry(QueryTelemetryMsg msg) {
        try {
            QueryTelemetryResponse resp = doAction(ControlActions.QUERY_TELEMETRY, msg,
                    QueryTelemetryResponse::parseFrom);
            if (!resp.getOk()) {
                log.warn("查询遥测被 Master 拒绝: {}", resp.getError());
                return List.of();
            }
            return resp.getStaleQueryIdsList();
        } catch (Throwable t) {
            log.warn("查询遥测上抛失败: {}", String.valueOf(t.getMessage()));
            return List.of();
        }
    }

    /** 注册后缓存的全量集群节点表（含 self 与当时已注册的兄弟）。 */
    public List<WorkerNode> clusterNodes() {
        return clusterNodes;
    }

    /** 通往 Master 的 FlightClient（注册/心跳/注销 doAction 共用）。 */
    public FlightClient masterClient() {
        return client();
    }

    /**
     * 通往 Master 的<b>结果回传专用</b> FlightClient（doExchange "result/{taskId}"），
     * <b>本 Worker 缓存唯一一条</b>，所有 result 流共享，Worker 停止时由 {@link #close()} 统一关闭。
     *
     * <p>必须建在 Worker 交换层共享分配器（{@code em.allocator()}）上——结果流的 streamRoot / 页缓冲
     * 都建在该分配器，客户端分配器与之同 root 才能 zero-copy 序列化；用本网关自带 root 的
     * {@link #masterClient()} 会跨 root 序列化失败 → gRPC 流被客户端取消（Master 侧 "client cancelled"）。
     *
     * <p><b>为什么缓存而非每任务新建</b>：镜像 worker↔worker 交换
     * （{@code NetworkExchangeChannel.clients} 按目标缓存单条客户端）的成熟模式。每任务新建 + 每任务
     * {@code client.close()} 的短命客户端在流开启后约 10ms 被 gRPC 静默取消（Master 侧 "client cancelled"），
     * 结果数据一页未送即断流；长命共享客户端则与交换层同构、行为稳定。任务结束不再 per-stream 关闭
     * 客户端（见 {@code ResultStream.close}），只 half-close 流。
     */
    public FlightClient openResultClient(BufferAllocator allocator) {
        FlightClient c = resultClient;
        if (c == null) {
            synchronized (this) {
                c = resultClient;
                if (c == null) {
                    c = FlightClient.builder()
                            .location(Location.forGrpcInsecure(masterHost, masterPort))
                            .allocator(allocator)
                            .build();
                    resultClient = c;
                }
            }
        }
        return c;
    }

    /** 关闭底层连接（Worker 停止时调用）。 */
    public void close() {
        FlightClient c = client;
        if (c != null) {
            try {
                c.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        FlightClient rc = resultClient;
        if (rc != null) {
            try {
                rc.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        allocator.close();
    }

    // ==================== 内部 ====================

    private synchronized FlightClient client() {
        FlightClient c = client;
        if (c == null) {
            c = FlightClient.builder()
                    .location(Location.forGrpcInsecure(masterHost, masterPort))
                    .allocator(allocator)
                    .build();
            client = c;
        }
        return c;
    }

    /** 发一个一元 doAction（请求/响应均 protobuf）。 */
    private <T extends Message> T doAction(String type, Message req, Parser<T> parser) {
        try {
            Iterator<Result> results = client().doAction(new Action(type, req.toByteArray()));
            if (!results.hasNext()) {
                throw new IllegalStateException("Master 未返回结果: action=" + type);
            }
            return parser.parse(results.next().getBody());
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("Master 返回非法响应: action=" + type, e);
        } catch (FlightRuntimeException e) {
            throw new RuntimeException("action 失败 " + type + ": " + e.status().description(), e);
        }
    }

    @FunctionalInterface
    private interface Parser<T> {
        T parse(byte[] body) throws InvalidProtocolBufferException;
    }
}
