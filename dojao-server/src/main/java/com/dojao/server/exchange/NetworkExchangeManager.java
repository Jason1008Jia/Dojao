package com.dojao.server.exchange;

import com.dojao.core.operator.ShuffleReader;
import com.dojao.server.worker.ExchangeManager;
import com.dojao.core.runtime.worker.WorkerNode;
import com.dojao.core.runtime.worker.ExchangeChannel;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.BufferAllocator;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Worker 侧交换注册表的<b>网络扩展</b> —— 跨 JVM 部署形态的 {@link ExchangeManager}。
 *
 * <p>在基类（注册路由表 + 共享分配器 + 本地交接）之上叠加网络能力：
 * <ul>
 *   <li>启动本 Worker 的 {@link NetworkExchangeServer}（Arrow Flight）监听 {selfNode.port()}，
 *       接收远程发送方经 doExchange 推来的数据页并路由到本 Worker 的 {@link ShuffleReader}；</li>
 *   <li>按进程边界构造 local/remote 视图：{@code localWorkerIds} = 与 selfWorkerId 同 JVM 的
 *       worker 集合（走 local 快路径），其余 worker 汇成 {@code remoteWorkers} 地址表；</li>
 *   <li>覆写 {@link #channelFor()} 返回缓存单例 {@link NetworkExchangeChannel}（连接复用，
 *       每条远程流懒建立一次）。</li>
 * </ul>
 *
 * <p><b>进程边界不变式</b>：isSelf 判的是"目标 worker 是否与本发送方同 JVM 进程"，而非
 * workerId 相等 / 机器相等。单 JVM 多 Worker（伪分布式 + 本类混搭）时 localWorkerIds 含全部
 * 同进程 worker，跨进程走网络。装配点唯一（channelFor），算子零改动。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/16
 */
public class NetworkExchangeManager extends ExchangeManager {

    /** 与 selfWorkerId 同 JVM 进程的 workerId 集合（isSelf 判真 → local 快路径）。 */
    private final Set<String> localWorkerIds;
    /** 远程 worker 地址表：workerId → host:port（不含同 JVM worker）。并发安全：派发时增量学习。 */
    private final ConcurrentHashMap<String, WorkerNode> remoteWorkers;
    /** 本 Worker 的 Flight 接收端。 */
    private final NetworkExchangeServer server;
    /** 缓存单例发送通道（每条远程流内部懒建立，连接复用）。 */
    private volatile NetworkExchangeChannel channel;

    /**
     * @param selfWorkerId   本 Worker 身份
     * @param selfNode       本 Worker 节点（port 即 Flight 接收端监听端口）
     * @param localWorkerIds 与本 Worker 同 JVM 进程的 workerId 集合（含自身）
     * @param allNodes       全部参与该 shuffle 的 worker 节点表
     * @param bus            共享路由表（与基类语义一致；生产每进程独立 bus，伪分布式共享）
     * @param allocator      交换层共享分配器（跨线程页交接的 buffer 归属地）
     */
    public NetworkExchangeManager(String selfWorkerId,
                                  WorkerNode selfNode,
                                  Set<String> localWorkerIds,
                                  Map<String, WorkerNode> allNodes,
                                  ConcurrentHashMap<String, ConcurrentHashMap<String, ShuffleReader>> bus,
                                  BufferAllocator allocator) throws IOException {
        this(selfWorkerId, selfNode, localWorkerIds, allNodes, bus, allocator, null);
    }

    /**
     * 全参构造：{@code producer} 非空时用自定义 FlightProducer（真分布式：Worker 控制面合并
     * producer，同时处理 exchange doExchange + 派发 doAction），否则默认纯交换 producer。
     */
    public NetworkExchangeManager(String selfWorkerId,
                                  WorkerNode selfNode,
                                  Set<String> localWorkerIds,
                                  Map<String, WorkerNode> allNodes,
                                  ConcurrentHashMap<String, ConcurrentHashMap<String, ShuffleReader>> bus,
                                  BufferAllocator allocator,
                                  FlightProducer producer) throws IOException {
        super(selfWorkerId, bus, allocator);
        this.localWorkerIds = localWorkerIds;
        this.remoteWorkers = new ConcurrentHashMap<>();
        for (Map.Entry<String, WorkerNode> e : allNodes.entrySet()) {
            if (!localWorkerIds.contains(e.getKey())) {
                remoteWorkers.put(e.getKey(), e.getValue());
            }
        }
        this.server = new NetworkExchangeServer(this, selfNode.port(), producer);
    }

    /**
     * 增量学习新节点（workerId → 地址），真分布式集群节点陆续注册时由派发侧驱动：
     * Master 在每次派发请求里附带最新集群节点表，本 Worker 据此补齐"后注册的兄弟"，
     * 使跨 JVM 交换通道能路由到新加入的 Worker（先注册者不知道后注册者的拓扑问题）。
     */
    public void learnNodes(List<WorkerNode> nodes) {
        for (WorkerNode n : nodes) {
            if (n == null || localWorkerIds.contains(n.workerId())) {
                continue;
            }
            remoteWorkers.putIfAbsent(n.workerId(), n);
        }
    }

    /** 本 Worker 的 Flight 接收端实际绑定端口。 */
    public int serverPort() {
        return server.port();
    }

    @Override
    public ExchangeChannel channelFor() {
        NetworkExchangeChannel c = channel;
        if (c == null) {
            synchronized (this) {
                c = channel;
                if (c == null) {
                    c = new NetworkExchangeChannel(selfWorkerId(), localWorkerIds, remoteWorkers,
                            bus, allocator());
                    channel = c;
                }
            }
        }
        return c;
    }

    @Override
    public void close() {
        NetworkExchangeChannel c = channel;
        if (c != null) {
            c.close();
        }
        server.shutdown();
        super.close();
    }

    /** 便捷构造：无共享 bus 时分配独立路由表（生产：每 Worker 进程一个）。 */
    public static NetworkExchangeManager create(String selfWorkerId,
                                                WorkerNode selfNode,
                                                Set<String> localWorkerIds,
                                                List<WorkerNode> allNodes,
                                                BufferAllocator allocator) throws IOException {
        return create(selfWorkerId, selfNode, localWorkerIds, allNodes, allocator, null);
    }

    /** 便捷构造 + 自定义 FlightProducer（真分布式 Worker 控制面合并 producer）。 */
    public static NetworkExchangeManager create(String selfWorkerId,
                                                WorkerNode selfNode,
                                                Set<String> localWorkerIds,
                                                List<WorkerNode> allNodes,
                                                BufferAllocator allocator,
                                                FlightProducer producer) throws IOException {
        Map<String, WorkerNode> nodeMap = new HashMap<>();
        for (WorkerNode n : allNodes) {
            nodeMap.put(n.workerId(), n);
        }
        return new NetworkExchangeManager(selfWorkerId, selfNode, localWorkerIds, nodeMap,
                new ConcurrentHashMap<>(), allocator, producer);
    }
}
