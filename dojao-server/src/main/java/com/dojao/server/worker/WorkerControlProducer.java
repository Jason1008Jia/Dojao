package com.dojao.server.worker;

import com.dojao.common.data.Page;
import com.dojao.core.operator.ConnectorScanOperator;
import com.dojao.core.operator.FragmentSpec;
import com.dojao.core.operator.OrderByTail;
import com.dojao.core.operator.Operator;
import com.dojao.core.operator.ShuffleReader;
import com.dojao.core.planner.PlanningContext;
import com.dojao.core.rpc.control.CancelTaskRequest;
import com.dojao.core.rpc.control.CatalogDefMsg;
import com.dojao.core.rpc.control.ControlActions;
import com.dojao.core.rpc.control.ControlSerde;
import com.dojao.core.rpc.control.DispatchFragmentRequest;
import com.dojao.core.rpc.control.DispatchScanRequest;
import com.dojao.core.rpc.control.WorkerNodeMsg;
import com.dojao.core.runtime.worker.FragmentEvent;
import com.dojao.core.runtime.worker.ScanEvent;
import com.dojao.core.runtime.worker.WorkerNode;
import com.dojao.metastore.CatalogDef;
import com.dojao.metastore.MetaStore;
import com.dojao.server.exchange.ExchangeFlightProducer;
import com.dojao.server.exchange.NetworkExchangeManager;
import com.dojao.server.function.FunctionManager;
import com.dojao.server.util.ExternalProcessRegistry;
import com.dojao.spi.bean.FunctionDefinition;
import com.dojao.spi.bean.ReadRequest;
import com.dojao.spi.bean.ScanSlice;
import com.dojao.spi.bean.Split;
import com.dojao.spi.service.DojaoConnector;
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
import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Worker 侧<b>合并控制面</b> FlightProducer —— 一个端口同时承载两类职责：
 * <ul>
 *   <li><b>数据面</b>（doExchange）：{@code ["exchange", exchangeId]} 路由到本 Worker 的
 *       {@link ShuffleReader}（委托 {@link ExchangeFlightProducer}，与纯交换版一致）；</li>
 *   <li><b>控制面</b>（doAction）：{@code dispatch_scan} / {@code dispatch_fragment} /
 *       {@code cancel_task} —— Master 派发分区扫描 / 执行片段 / 取消任务。</li>
 * </ul>
 *
 * <p><b>派发即执行</b>：dispatch 处理器解析载荷 → 建网络结果回传队列（doExchange
 * "result/{taskId}" 推到 Master）→ {@code WorkerManager.submitTask} 提交任务执行 →
 * 立即 ack 受理。片段执行（{@link FragmentExecutor}）与扫描回传（{@code scanAndSend}）复用
 * 伪分布式 {@code LocalWorkerGateway} 的既有逻辑，只把内存结果队列换成网络队列。</p>
 *
 * <p><b>拓扑增量学习</b>：每次派发载荷携带集群节点表，处理器先 {@code learnNodes} 补齐
 * worker↔worker 交换地址，再提交任务 —— 保证后续 Producer 跨 JVM 发送时路由表已就绪。</p>
 *
 * <p><b>装配顺序</b>：producer 先于 {@code NetworkExchangeManager} 创建（后者需要 producer 起
 * 服务器），故 exchangeManager / workerManager 用 volatile 引用后置注入（见
 * {@link #setExchangeManager} / {@link #setWorkerManager}）。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/29
 */
public final class WorkerControlProducer implements FlightProducer {

    private static final Logger log = LoggerFactory.getLogger(WorkerControlProducer.class);

    private final NetworkMasterGateway masterGateway;
    private final PlanningContext.ConnectorProvider provider;
    private final ExternalProcessRegistry registry;
    private final BufferAllocator allocator;
    /** 本地元数据中心：接收 Master 随载荷送来的 catalog 定义（{@link MetaStore#installCatalog}）。 */
    private final MetaStore metaStore;

    /** 后置注入（装配循环：producer → NetworkExchangeManager → WorkerManager）。 */
    private volatile WorkerManager workerManager;
    private volatile NetworkExchangeManager exchangeManager;
    private volatile ExchangeFlightProducer exchangeDelegate;
    /** 查询遥测表（Worker 启动后注入）：非终端片段登记 + ~1s 上抛 Master。 */
    private volatile WorkerQueryTelemetry telemetry;

    public WorkerControlProducer(NetworkMasterGateway masterGateway,
                                 PlanningContext.ConnectorProvider provider,
                                 ExternalProcessRegistry registry,
                                 MetaStore metaStore,
                                 BufferAllocator allocator) {
        this.masterGateway = masterGateway;
        this.provider = provider;
        this.registry = registry;
        this.metaStore = metaStore;
        this.allocator = allocator;
    }

    public void setWorkerManager(WorkerManager workerManager) {
        this.workerManager = workerManager;
    }

    public void setExchangeManager(NetworkExchangeManager exchangeManager) {
        this.exchangeManager = exchangeManager;
    }

    /** 注入查询遥测表（Worker 启动完成后由 DojaoWorker 调用；null = 不上抛）。 */
    public void setTelemetry(WorkerQueryTelemetry telemetry) {
        this.telemetry = telemetry;
    }

    // ==================== 数据面：exchange doExchange（委托） ====================

    @Override
    public void doExchange(CallContext context, FlightStream flightStream, ServerStreamListener listener) {
        exchangeDelegate().doExchange(context, flightStream, listener);
    }

    // ==================== 控制面：派发 / 取消 doAction ====================

    @Override
    public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
        try {
            switch (action.getType()) {
                case ControlActions.DISPATCH_SCAN -> dispatchScan(DispatchScanRequest.parseFrom(action.getBody()), listener);
                case ControlActions.DISPATCH_FRAGMENT -> dispatchFragment(DispatchFragmentRequest.parseFrom(action.getBody()), listener);
                case ControlActions.CANCEL_TASK -> cancelTask(CancelTaskRequest.parseFrom(action.getBody()), listener);
                default -> {
                    listener.onError(CallStatus.UNIMPLEMENTED
                            .withDescription("未知 action: " + action.getType()).toRuntimeException());
                    return;
                }
            }
        } catch (InvalidProtocolBufferException e) {
            listener.onError(CallStatus.INVALID_ARGUMENT
                    .withDescription("非法载荷: " + action.getType()).toRuntimeException());
        } catch (Throwable t) {
            log.error("Worker 控制面执行失败 action={}", action.getType(), t);
            listener.onError(CallStatus.INTERNAL
                    .withDescription(String.valueOf(t)).toRuntimeException());
        }
    }

    private void dispatchScan(DispatchScanRequest req, StreamListener<Result> listener) {
        WorkerManager wm = workerManager();
        NetworkExchangeManager em = exchangeManager();
        em.learnNodes(nodesOf(req.getClusterNodesList()));
        installCatalogs(req.getCatalogsList());
        ReadRequest read = ControlSerde.fromRead(req.getRequest());
        // 切片私有载荷（连接器自定义字节，原样交回给连接器解读）；空 = 整表切片
        Split split = Split.of(req.getSplitPayload().toByteArray());
        ScanSlice slice = new ScanSlice(read, split);
        String taskId = req.getTaskId();
        String desc = "scan " + req.getCatalog() + "." + read.schema() + "." + read.table();
        // 片内 top-N 截断尾（Master 下推）：空 = 全量回传（无顶层 ORDER BY LIMIT 下推）
        FragmentSpec.SortLimitStep tail = req.hasTail() ? ControlSerde.fromSortLimitStep(req.getTail()) : null;
        // 结果回传专用独立客户端（用交换层共享分配器，与流根同 root → 零拷贝安全）；
        // 复用 masterClient 会跨 root 序列化失败 → 流被取消（Master 侧 "client cancelled"）。
        NetworkScanResultQueue queue = new NetworkScanResultQueue(
                masterGateway.openResultClient(em.allocator()), taskId, em.allocator());
        // 独立 Scan 是叶子任务：下游（或 Master）的推进完全靠它读数据 → 走叶子专线，绝不排在
        // 阻塞中的消费者片段后面（同 §submitLeafTask 的死锁说明）
        wm.submitLeafTask(new WorkerTask(taskId, req.getJobId(), desc, 1), (task, worker) -> {
            try {
                scanAndSend(taskId, req.getCatalog(), slice, tail, queue);
            } finally {
                queue.close();                    // 被取消也主动完成流，避免 Master 侧 doExchange 悬挂
                registry.killByQuery(req.getJobId());   // 任务结束：杀该 query 绑定的外部进程（幂等）
            }
        }, cause -> queue.offer(new ScanEvent.ScanFailed(taskId, cause), 3, TimeUnit.SECONDS));
        // 存活心跳（P1）：独立 Scan 叶子没有「数据面回 Master」的页，读慢（ORC 最小读单元 =
        // 整条 stripe，无 SARG 时 stripe 级 pread）可以合法沉默很久 —— 由此上抛 rows=0 控制面
        // 帧，Master 侧刷新 DistributedScanOperator 的看门狗接触时刻，LOST 只表示 Worker 失联。
        //
        // ★必须登记在 submit 之后（此处）、而不是任务体内部：叶子池满时任务会在队列里排队，
        //   排队期间同样一页不吐 —— 写在任务体里等于「只有抢到线程才开始心跳」，Master 的 30s
        //   时钟从派发即开始走，排队稍久就把活任务判成 LOST（空闲集群偶发 scan timeout 的根因）。
        //   采样器自带 done 判定（任务摘槽 = 收尾），无需在任务体 finally 里手动注销。
        trackScanLiveness(req.getJobId(), taskId, wm);
        ack(listener, true, null);
    }

    private void dispatchFragment(DispatchFragmentRequest req, StreamListener<Result> listener) {
        WorkerManager wm = workerManager();
        NetworkExchangeManager em = exchangeManager();
        em.learnNodes(nodesOf(req.getClusterNodesList()));
        installCatalogs(req.getCatalogsList());
        FragmentSpec spec = ControlSerde.fromFragment(req.getSpec());
        String taskId = req.getTaskId();
        // L2：真分布式下本 Worker 是独立 JVM，注册表不与 Master 共享。Master 把片段聚合引用的
        // 自定义函数定义附在载荷里，先注册 + 加载成可执行函数（幂等），再执行片段按名重建。
        // 加载失败（后端未装 / 本机无 python）→ 异常向上抛，doAction 返回 INTERNAL → Master fail-fast。
        for (FunctionDefinition def : req.getFunctionsList().stream()
                .map(ControlSerde::fromFunctionDefinition).toList()) {
            FunctionManager.registerExternal(def);
            FunctionManager.ensureLoadedFor(def);
        }
        // 非 producer 片段：同步注册 ShuffleReader 接收端（任何 Producer 推送前必须就绪，
        // 时序由 Master 调度保证：先 Consumer 后 Producer）。
        if (!spec.isProducer()) {
            registerReaders(wm, spec);
        }
        // 结果回传专用独立客户端（同上，用交换层共享分配器与流根同 root）。
        NetworkFragmentResultQueue queue = new NetworkFragmentResultQueue(
                masterGateway.openResultClient(em.allocator()), taskId, em.allocator());
        WorkerTask task = new WorkerTask(taskId, req.getJobId(),
                "fragment " + spec.fragmentId() + " " + spec.describe(), 1);
        WorkerTaskExecution body = (t, worker) -> {
            try {
                // queryId = req.getJobId()（Master 派发即 queryId）：非终端片段据此分组上抛
                // 行数遥测（FragmentExecutor 内部 track/untrack，terminal/scan 不登记 → 无双计）
                new FragmentExecutor(worker.exchangeManager(), provider)
                        .execute(req.getJobId(), spec, queue, telemetry);
            } finally {
                queue.close();                    // 被取消也主动完成流
                registry.killByQuery(req.getJobId());   // 任务结束：杀该 query 绑定的外部进程（幂等）
            }
        };
        // 池死锁判死钩子（仅非 producer）：消费者 / 中间层片段有 Master 侧结果汇，未及执行即被判死时
        // 能把 FragmentFailed 回传 → Master fail-fast → doClose 连带取消整条查询（含在跑的阻塞片段），
        // 把「假死」变成明确错误。Producer 从不回传 Master（数据走 exchange 直连下一层）→ 无汇可投，
        // 其失败由同查询消费者片段的判死连带取消间接体现。
        java.util.function.Consumer<String> aborter = spec.isProducer() ? null
                : cause -> queue.offer(new FragmentEvent.FragmentFailed(taskId, cause), 3, TimeUnit.SECONDS);
        // Producer（叶子）走专线：消费者/MiddleJoinConsumer 会整段阻塞等 shuffle 收齐，与它们
        // 共用一池时生产者会被永久饿死（详见 WorkerManager#leafExecutor）
        if (spec.isProducer()) {
            wm.submitLeafTask(task, body);
        } else {
            wm.submitTask(task, body, aborter);
        }
        ack(listener, true, null);
    }

    private void cancelTask(CancelTaskRequest req, StreamListener<Result> listener) {
        workerManager().cancelTask(req.getTaskId());
        ack(listener, true, null);
    }

    /**
     * 装入 Master 随派发载荷送来的 catalog <b>定义</b>（type + 连接属性）—— 与 cluster_nodes
     * （拓扑）/ functions（函数定义）同款，让 Worker 这侧「凭载荷即可连」。
     *
     * <p>装入后本 Worker 的内存表就能解析这些 catalog：不依赖与 Master 共享元数据库
     * （Master 与 Worker 未必同机，SQLite 文件未必看得到），也不需要 Worker 重启
     * —— 运行时新建的 catalog 下一个任务派发过来即刻可用。</p>
     *
     * <p>{@link MetaStore#installCatalog} 幂等（定义未变直接返回），所以这里放在派发热路径上
     * 也没有额外开销。类型未知 / 属性写错 → 异常上抛 → doAction 返回 INTERNAL → Master fail-fast。</p>
     */
    private void installCatalogs(List<CatalogDefMsg> defs) {
        for (CatalogDefMsg d : defs) {
            metaStore.installCatalog(new CatalogDef(d.getName(), d.getType(), d.getPropertiesMap()));
        }
    }

    // ==================== 内部 ====================

    /**
     * 扫描任务存活心跳登记（telemetry 未注入时 no-op，伪分布式场景行为不变）。
     *
     * @param wm 本 Worker 任务池 —— 采样器用它判断任务是否仍在（排队中 = 仍活；摘槽 = 上报 done 后注销）
     */
    private void trackScanLiveness(String queryId, String taskId, WorkerManager wm) {
        if (telemetry != null) {
            telemetry.trackScanLiveness(queryId, taskId, () -> wm.isTaskActive(taskId));
        }
    }

    /**
     * Worker 侧执行体：独立连接器副本读分区直到 EOF → 逐页回传 → 成功 / 失败各发对应终止事件。
     * {@code tail} 非 null（顶层 ORDER BY LIMIT 下推命中）时把扫描源包成截断尾链 —— 本地排序 +
     * 截断到 offset+limit 行后再逐页回传（回传候选集而非全量，Master 在候选集上全局排序取区间）。
     */
    private void scanAndSend(String taskId, String catalog, ScanSlice slice,
                             FragmentSpec.SortLimitStep tail,
                             NetworkScanResultQueue queue) {
        DojaoConnector conn = null;
        Operator op = null;
        try {
            conn = provider.get(catalog);   // 每个任务独立副本，游标互不干扰
            // 谓词在数据所在地执行：连接器认识的推给数据源，引擎注册函数谓词由引擎在本节点补 Filter
            // （必须在 top-N 截断尾之前 —— 语义是「先过滤再取前 N 行」）
            Operator source = ConnectorScanOperator.openWithEngineFilter(conn, slice.request(), slice.split());
            op = tail != null ? OrderByTail.wrap(source, tail) : source;
            op.open();
            Page page;
            while (!(page = op.next()).isEOF()) {
                // put 已接管页生命周期（发送/丢弃都会 close）—— 此处不得再 close
                queue.put(new ScanEvent.ScanPage(taskId, page));   // 网络回传即背压
            }
            queue.offer(new ScanEvent.ScanFinished(taskId), 1, TimeUnit.SECONDS);
        } catch (Throwable t) {
            // 显式失败回传（尽力送达）：Master 收到即 fail-fast；万一丢失，Master 侧看门狗兜底判 LOST
            queue.offer(new ScanEvent.ScanFailed(taskId, String.valueOf(t)), 3, TimeUnit.SECONDS);
        } finally {
            if (op != null) {
                try {
                    op.close();       // 释放算子链（ConnectorScanOperator.closeRead 幂等）
                } catch (Throwable ignored) {
                }
            }
            if (conn != null) {
                try {
                    conn.closeRead();       // 释放本分区游标
                } catch (Throwable ignored) {
                }
            }
        }
    }

    /** 为某个 exchange 建接收端并登记进 Worker 的交换注册表（与 LocalWorkerGateway 同构）。 */
    private void registerReaders(WorkerManager wm, FragmentSpec spec) {
        if (spec instanceof FragmentSpec.Consumer c) {
            registerReader(wm, c.leftIn());
            registerReader(wm, c.rightIn());
        } else if (spec instanceof FragmentSpec.ChainedJoinConsumer c) {
            registerReader(wm, c.leftIn());
            registerReader(wm, c.rightIn());
        } else if (spec instanceof FragmentSpec.AggregateConsumer a) {
            registerReader(wm, a.in());
        } else if (spec instanceof FragmentSpec.WindowConsumer w) {
            registerReader(wm, w.in());
        } else if (spec instanceof FragmentSpec.MiddleJoinConsumer m) {
            registerReader(wm, m.leftIn());
            registerReader(wm, m.rightIn());
        } else if (spec instanceof FragmentSpec.MiddleAggregateConsumer ma) {
            registerReader(wm, ma.in());
        } else if (spec instanceof FragmentSpec.MiddleWindowConsumer mw) {
            registerReader(wm, mw.in());
        }
    }

    private void registerReader(WorkerManager wm, FragmentSpec.ExchangeInStep in) {
        // 收齐后从路由表摘除：否则每个 exchange 的 ShuffleReader（每桶队列 + 计数数组 + 看门狗
        // 线程）会永久留在 bus 里，随查询次数累积。exchangeId 查询内唯一，按 id 摘除安全。
        ShuffleReader reader = new ShuffleReader(in.exchangeId(), in.numPartitions(), in.numSenders(),
                in.owned(), () -> wm.exchangeManager().unregisterReader(in.exchangeId()));
        wm.exchangeManager().registerReader(in.exchangeId(), reader);
    }

    private static List<WorkerNode> nodesOf(List<WorkerNodeMsg> msgs) {
        return msgs.stream().map(ControlSerde::fromNode).toList();
    }

    private static void ack(StreamListener<Result> listener, boolean ok, String error) {
        DispatchAckHolder ack = DispatchAckHolder.of(ok, error);
        listener.onNext(new Result(ack.bytes()));
        listener.onCompleted();
    }

    /** doAction ack 直接回 protobuf bytes（DispatchAck 无需单独消息类，这里内联构造）。 */
    private record DispatchAckHolder(boolean ok, String error) {
        static DispatchAckHolder of(boolean ok, String error) {
            return new DispatchAckHolder(ok, error);
        }

        byte[] bytes() {
            return com.dojao.core.rpc.control.DispatchAck.newBuilder()
                    .setOk(ok).setError(error == null ? "" : error).build().toByteArray();
        }
    }

    private WorkerManager workerManager() {
        WorkerManager wm = workerManager;
        if (wm == null) {
            throw new IllegalStateException("WorkerControlProducer 未装配 workerManager（派发先于启动）");
        }
        return wm;
    }

    private NetworkExchangeManager exchangeManager() {
        NetworkExchangeManager em = exchangeManager;
        if (em == null) {
            throw new IllegalStateException("WorkerControlProducer 未装配 exchangeManager");
        }
        return em;
    }

    private ExchangeFlightProducer exchangeDelegate() {
        ExchangeFlightProducer d = exchangeDelegate;
        if (d == null) {
            synchronized (this) {
                d = exchangeDelegate;
                if (d == null) {
                    d = new ExchangeFlightProducer(exchangeManager(), allocator);
                    exchangeDelegate = d;
                }
            }
        }
        return d;
    }

    // ==================== 未使用：交换走 doExchange，控制走 doAction ====================

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
}
