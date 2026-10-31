package com.dojao.server.worker;

import com.dojao.common.data.Page;
import com.dojao.core.operator.AggregationSpec;
import com.dojao.core.operator.CaseProjectOperator;
import com.dojao.core.operator.ConnectorScanOperator;
import com.dojao.core.operator.DistributedTask;
import com.dojao.core.operator.ExpandOperator;
import com.dojao.core.operator.ExpressionProjectOperator;
import com.dojao.core.operator.FilterOperator;
import com.dojao.core.planner.SqlTextUtil;
import com.dojao.core.operator.FragmentSpec;
import com.dojao.core.operator.HashAggregationOperator;
import com.dojao.core.operator.HashJoinOperator;
import com.dojao.core.operator.HashPartitionOperator;
import com.dojao.core.operator.MeasurableOperator;
import com.dojao.core.operator.MergeProgress;
import com.dojao.core.operator.Operator;
import com.dojao.core.operator.OperatorStats;
import com.dojao.core.operator.OrderByTail;
import com.dojao.core.operator.PartitionedSource;
import com.dojao.core.operator.PartitionedSourceOperator;
import com.dojao.core.operator.ShuffleReader;
import com.dojao.core.operator.SortMergeJoinOperator;
import com.dojao.core.operator.WindowFilterConfig;
import com.dojao.core.operator.WindowFunctionOperator;
import com.dojao.core.operator.WindowFunctionType;
import com.dojao.core.operator.WindowSortOperator;
import com.dojao.core.function.aggregate.AvgAggregateFunction;
import com.dojao.core.function.aggregate.CountAggregateFunction;
import com.dojao.core.function.aggregate.CountDistinctAggregateFunction;
import com.dojao.core.function.aggregate.MaxAggregateFunction;
import com.dojao.core.function.aggregate.MinAggregateFunction;
import com.dojao.core.function.aggregate.SumAggregateFunction;
import com.dojao.core.planner.PlanningContext;
import com.dojao.core.runtime.worker.ExchangeChannel;
import com.dojao.core.runtime.worker.FragmentEvent;
import com.dojao.spi.bean.WriteRequest;
import com.dojao.spi.service.DojaoConnector;
import com.dojao.spi.function.AggregateFunction;
import com.dojao.spi.function.AggregateFunctionRegistry;
import com.dojao.spi.function.CollectAggregateFunction;
import com.dojao.spi.function.CollectAggregateRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * Worker 侧片段执行器 —— 用现有算子（{@link HashPartitionOperator} / {@link HashJoinOperator} /
 * {@link HashAggregationOperator} / {@link WindowSortOperator} / {@link WindowFunctionOperator} /
 * {@link ShuffleReader}）重建一个片段的算子链并运行。<b>不新增算子</b>，只做编排。
 *
 * <ul>
 *   <li><b>Producer</b>：{@code ScanStep → PartitionStep}。本地扫切片 → 分布式 HashPartition
 *       （经 ExchangeChannel 推给归属 Worker）→ {@link HashPartitionOperator#awaitDistributedDone()}
 *       等所有桶发送完成（含失败时的 fail 通知），无需拉取 next()；</li>
 *   <li><b>Consumer</b>：own 桶数据 → 本地算子 → 回传 Master：
 *       <ul>
 *         <li>join：两个 {@link ShuffleReader} 作 {@code PartitionedSource} 灌给本地
 *             {@link HashJoinOperator}（与单机 join 同一算子代码）；</li>
 *         <li>聚合：{@link ShuffleReader} → {@link PartitionedSourceOperator} → 本地
 *             {@link HashAggregationOperator}（聚合函数按名重建）；</li>
 *         <li>窗口：{@link ShuffleReader} → {@link PartitionedSourceOperator} → 本地
 *             {@link WindowSortOperator} + {@link WindowFunctionOperator}；</li>
 *         <li>链式 join→后续：本地 {@link HashJoinOperator} 输出<b>直接</b>喂给同 key 的窗口 /
 *             聚合（{@link FragmentSpec.ChainedJoinConsumer}，零二次交换，cowork without master）。</li>
 *       </ul>
 *       输出页逐页以 {@link FragmentEvent.FragmentData} 回传 Master，收尾发
 *       {@link FragmentEvent.FragmentFinished} / {@link FragmentEvent.FragmentFailed}。</li>
 * </ul>
 *
 * @author jiaxin
 * @version 3.0
 * @date 2026/8/20
 */
public final class FragmentExecutor {

    private final ExchangeManager exchangeManager;
    private final PlanningContext.ConnectorProvider provider;

    /** 本片段所属查询 queryId（= Master 派发载荷的 jobId）；null = 伪分布式路径（不回传遥测）。 */
    private String queryId;
    /** Worker 侧查询遥测（非终端片段行数采样登记表）；null = 不登记（同 JVM 伪分布式默认关闭）。 */
    private WorkerQueryTelemetry telemetry;

    /**
     * 心跳间隔：超过此间隔无任何结果回传、且上游 shuffle 仍在收页（任务在推进）时，发
     * {@link FragmentEvent.FragmentProgress} 重置 Master 结果看门狗 —— 阻塞式算子（聚合 Phase 1 /
     * 溢写 / 排序 / join 慢 build）长时间不吐结果也不被误杀（可用 -DDojaoHeartbeatMs 覆盖）。
     */
    private static final long HEARTBEAT_INTERVAL_MS = Long.getLong("DojaoHeartbeatMs", 10_000);

    /**
     * 窗口 AGGREGATE 重建用注册表。{@link AggregateFunctionRegistry} 的 GLOBAL 是进程内静态 Map，
     * 任意实例均可查全局 —— 伪分布式（同 JVM 多 Worker）下 worker 与 master 共享该注册表，
     * 自定义聚合（含 Python 桥）按名即命中；真分布式独立 JVM 需 worker 启动时自行装载函数。
     */
    private static final AggregateFunctionRegistry AGGREGATE_REGISTRY = new AggregateFunctionRegistry();

    public FragmentExecutor(ExchangeManager exchangeManager, PlanningContext.ConnectorProvider provider) {
        this.exchangeManager = exchangeManager;
        this.provider = provider;
    }

    /** 执行一个片段（伪分布式 / 无遥测接线）。抛出的异常由 WorkerManager 记录为任务失败。 */
    public void execute(FragmentSpec spec, BlockingQueue<FragmentEvent> resultQueue) throws Exception {
        execute(null, spec, resultQueue, null);
    }

    /**
     * 执行一个片段（真分布式入口：带 queryId 供遥测分组）。抛出的异常由 WorkerManager 记录为任务失败。
     *
     * @param queryId   所属查询 queryId（= Master 派发 jobId）；非终端片段按其分组上抛行数遥测
     * @param telemetry Worker 侧查询遥测表；null = 不上抛（伪分布式同 JVM 或未装配）
     */
    public void execute(String queryId, FragmentSpec spec, BlockingQueue<FragmentEvent> resultQueue,
                        WorkerQueryTelemetry telemetry) throws Exception {
        this.queryId = queryId;
        this.telemetry = telemetry;
        if (spec.isProducer()) {
            executeProducer((FragmentSpec.Producer) spec, resultQueue);
        } else if (spec instanceof FragmentSpec.MiddleAggregateConsumer ma) {
            executeMiddleAggregateConsumer(ma);
        } else if (spec instanceof FragmentSpec.MiddleJoinConsumer mid) {
            executeMiddleJoinConsumer(mid);
        } else if (spec instanceof FragmentSpec.MiddleWindowConsumer mw) {
            executeMiddleWindowConsumer(mw);
        } else if (spec instanceof FragmentSpec.AggregateConsumer agg) {
            executeAggregateConsumer(agg, resultQueue);
        } else if (spec instanceof FragmentSpec.WindowConsumer win) {
            executeWindowConsumer(win, resultQueue);
        } else if (spec instanceof FragmentSpec.ChainedJoinConsumer chain) {
            executeChainedJoinConsumer(chain, resultQueue);
        } else {
            executeConsumer((FragmentSpec.Consumer) spec, resultQueue);
        }
    }

    // ==================== 查询遥测：非终端片段行数 / 流量登记 ====================

    /**
     * 登记非终端片段的实时采样器：读尾部 {@link HashPartitionOperator} 的
     * {@link OperatorStats#getRowsProcessed()}（= 进入分区的行数 = 该段实际发出的行数：
     * Producer = 扫描行数、中间层 join = join 输出、派生中间聚合 = HAVING 后聚合输出），
     * 以及同一算子的 {@code getInputBytes()} / {@code getOutputBytes()}（= 该段实际收发的字节：
     * 输入 = 从连接器 / 上游读入的页大小之和，输出 = 写入 shuffle 的数据量）。
     * 只登记非终端片段 —— 终端 Consumer / 独立 Scan 的行数由数据面回传天然到 Master，不在此列
     * （无双计）。未接线遥测（queryId 或 telemetry 为 null，如伪分布式）→ no-op。
     */
    private void track(FragmentSpec spec, HashPartitionOperator part) {
        if (queryId == null || telemetry == null || part == null) {
            return;
        }
        // 不能捕获被重新赋值的局部 part（lambda 要求 effectively final）——只捕获其 stats 引用，
        // stats 是 long 累加器（AtomicLong），tick 线程并发读安全
        final OperatorStats stats = part.getStats();
        telemetry.track(queryId, spec.fragmentId(),
                stats::getRowsProcessed, stats::getInputBytes, stats::getOutputBytes);
    }

    /** 注销片段的采样器（任务失败 / 取消时调用，防泄漏；不补发终值）。 */
    private void untrack(String fragmentId) {
        if (queryId == null || telemetry == null) {
            return;
        }
        telemetry.untrack(queryId, fragmentId);
    }

    /**
     * 片段<b>正常跑完</b>的遥测收尾：补发终值 + {@code done=true}，让 Master 立刻把该片段任务置
     * FINISHED 并拿到真实终值（短命片段整条命内一个采样帧都发不出，不补发则恒 0 行 + 状态悬挂）。
     */
    private void finishTelemetry(String fragmentId) {
        if (queryId == null || telemetry == null) {
            return;
        }
        telemetry.finish(queryId, fragmentId);
    }

    /**
     * 登记片段内<b>溢写算子</b>的实时溢写字节采样器（{@link OperatorStats#getSpilledBytes()}）。
     *
     * <p>溢写只发生在 Worker 进程内的算子对象上 —— Master 侧同名算子的 {@code getSpilledBytes()} 恒 0，
     * 监控页「溢写总量」因此对分布式查询一直为空。这里把溢写挂到「片段 → Master」的既有遥测通道上：
     * 与行数 / 字节通道<b>互相独立</b>（{@link WorkerQueryTelemetry#trackSpill}），行数仍由数据面 / 既有
     * 采样器提供，不双计。</p>
     *
     * <p>终端 Consumer（窗口排序 / 聚合 / 排序归并 join）恰恰是溢写主力，且它的行数由数据面回传
     * Master（不进遥测）—— 故这里只为溢写建一条<b>纯溢写</b>采样器（rows=null），并由
     * {@link #runAndStream}/{@link #runAndSink} 收尾。</p>
     */
    private void trackSpill(String fragmentId, Operator op) {
        if (queryId == null || telemetry == null || !(op instanceof MeasurableOperator mo)) {
            return;
        }
        final OperatorStats stats = mo.getStats();
        telemetry.trackSpill(queryId, fragmentId, stats::getSpilledBytes);
    }

    /**
     * 中间层 join 的<b>分区并行度</b>登记：本算子片段内部恒有 {@code numPartitions} 个分区线程
     * （{@link HashJoinOperator}），但片段任务只有 1 个 —— 监控看到的「Join 只有 1 个 task」由此而来。
     * 这里把每个分区登记为独立子任务 {@code <fragmentId>#p<k>}：行数 = 该分区真实 join 输出行数、
     * 字节 = 该分区从 shuffle 读入 / 发出 join 输出的字节，收尾 = 该分区线程做完（早于片段整体收尾）
     * → Master 逐分区变绿，Join 框进度条按分区推进。仍是同一片段的内部视图，不新增派发、不改数据面。
     */
    private void trackJoinPartitions(String fragmentId, HashJoinOperator join) {
        if (queryId == null || telemetry == null || join == null) {
            return;
        }
        int n = join.numPartitions();
        for (int k = 0; k < n; k++) {
            final int p = k;
            telemetry.track(queryId, DistributedTask.partitionTaskId(fragmentId, p),
                    () -> join.partitionRowsOut(p),
                    () -> join.partitionBytesIn(p), () -> join.partitionBytesOut(p),
                    () -> join.partitionDone(p));
        }
    }

    /** 中间层 join 收尾：逐分区补发终值 + done（采样器已被 done 判定注销的为 no-op）。 */
    private void finishJoinPartitions(String fragmentId, HashJoinOperator join) {
        if (queryId == null || telemetry == null || join == null) {
            return;
        }
        for (int k = 0; k < join.numPartitions(); k++) {
            telemetry.finish(queryId, DistributedTask.partitionTaskId(fragmentId, k));
        }
    }

    /** 中间层 join 分区子任务注销（失败 / 取消：不补发终值，防泄漏）。 */
    private void untrackJoinPartitions(String fragmentId, HashJoinOperator join) {
        if (queryId == null || telemetry == null || join == null) {
            return;
        }
        for (int k = 0; k < join.numPartitions(); k++) {
            telemetry.untrack(queryId, DistributedTask.partitionTaskId(fragmentId, k));
        }
    }

    // ==================== Producer：扫切片 → 分区 → 推送 ====================

    /**
     * Producer 片段（扫切片 → 哈希分区 → 推送各归属 Worker）。
     *
     * <p><b>终止状态必报</b>：producer 失败原先只落进 {@code WorkerManager} 的任务失败表（仅日志），
     * Master 侧完全看不见 —— 只能等 60s 结果看门狗抛「疑似 result 回传链路故障」这种指错方向的
     * 文案。这里失败即回传 {@link FragmentEvent.FragmentFailed}，Master 当场 fail-fast 并拿到真因。</p>
     *
     * <p><b>成功路径刻意不发 {@code FragmentFinished}</b>：producer 的收尾协议是「下游 Consumer 收齐
     * 数据后各自 complete」；多发一个完成事件会把 Master 侧 {@code finishedCount} 顶到 {@code numConsumers}
     * 之上，导致终端片段提前 EOF、结果被截断。</p>
     */
    private void executeProducer(FragmentSpec.Producer spec,
                                 BlockingQueue<FragmentEvent> resultQueue) throws Exception {
        FragmentSpec.ScanStep scanStep = spec.scan();
        FragmentSpec.PartitionStep partStep = spec.partition();
        DojaoConnector conn = provider.get(scanStep.catalog());
        HashPartitionOperator part = null;
        boolean completed = false;
        try {
            // 切片 = 读请求（范围谓词）+ 连接器私有载荷（Hive 的文件清单等，引擎只搬运）
            // 谓词在数据所在地执行：连接器认识的推给数据源，引擎注册函数谓词在<b>本节点</b>补 Filter
            // ——位置在派生列 / 哈希分区之前（少过滤 = 多 shuffle，且分区前过滤语义与单机一致）
            Operator scan = ConnectorScanOperator.openWithEngineFilter(conn, scanStep.request(), scanStep.split());
            // GROUP BY 表达式派生列：分组键是分区键，须在哈希分区前先物化派生列，否则同值行散到不同桶
            if (!spec.groupKeyExpressions().isEmpty()) {
                scan = new ExpressionProjectOperator(scan, spec.groupKeyExpressions());
            }
            // 多维聚合展开（ROLLUP/CUBE/GROUPING SETS）：扫描之后、按 (维度…, gid) 哈希分区之前
            // 把每行复制成 N 份 —— 基表只扫这一次，各维度组合靠 __grouping_id 在聚合端分开
            if (spec.expand() != null) {
                scan = new ExpandOperator(scan, spec.expand().dimColumns(), spec.expand().groupingSets());
            }
            ExchangeChannel channel = exchangeManager.channelFor();
            part = new HashPartitionOperator(scan, partStep.keyColumns(), partStep.numPartitions(), 1,
                    channel, partStep.ownerWorkerIds().toArray(new String[0]),
                    partStep.exchangeIds().toArray(new String[0]),
                    exchangeManager.selfWorkerId(),
                    partStep.distribution(), partStep.fixedBucket());
            track(spec, part);   // 采样器先登记（open 前）：tick 读一次当前值，短任务也可能抓到若干拍
            part.open();
            part.awaitDistributedDone();   // 等所有分发线程完成并已通知所有归属 Worker
            if (part.distributorFailure() != null) {
                // 失败已通过 channel.fail 传导给接收端（fail-fast），这里再登记任务失败
                throw new RuntimeException("Producer 分发失败 fragment=" + spec.fragmentId(),
                        part.distributorFailure());
            }
            completed = true;
        } catch (Throwable t) {
            // P4：失败必须让 Master 看见。取消 ≠ 失败 —— 中断标记在 = 上层已放弃，静默上抛即可。
            if (!Thread.currentThread().isInterrupted()) {
                emitFailure(resultQueue, new FragmentEvent.FragmentFailed(spec.fragmentId(), fullStack(t)), t, 5);
            }
            throw t;
        } finally {
            if (completed) {
                finishTelemetry(spec.fragmentId());
            } else {
                untrack(spec.fragmentId());
            }
            if (part != null) {
                try { part.close(); } catch (Throwable ignored) { }
            }
        }
    }

    // ==================== Consumer：接桶 → 本地 join → 回传 ====================

    private void executeConsumer(FragmentSpec.Consumer spec, BlockingQueue<FragmentEvent> resultQueue) {
        FragmentSpec.JoinStep joinStep = spec.join();
        // 接收端已在派发阶段注册进本 Worker 的 ExchangeManager
        ShuffleReader left = exchangeManager.readerFor(spec.leftIn().exchangeId());
        ShuffleReader right = exchangeManager.readerFor(spec.rightIn().exchangeId());
        Operator join = localJoin(left, right, joinStep);
        trackSpill(spec.fragmentId(), join);   // 排序归并 / 哈希 join 溢写（终端片段：行数走数据面，此处只报溢写）
        // 顶层 ORDER BY/LIMIT 下推：终端缝挂截断尾（本地排序 + 截到 offset+limit 候选）
        Operator terminal = spec.tail() != null ? OrderByTail.wrap(join, spec.tail()) : join;
        // 心跳进度源 = 两侧 shuffle 收页 + 归并进度：join build（右表）慢但仍在收页时 Keepalive；
        // 输入收齐后卡在归并（如排序归并 Phase 1）时，靠归并 tick 续命
        if (spec.sink() != null) {
            runAndSink(spec.fragmentId(), terminal, spec.sink(), resultQueue,
                    () -> livenessProgress(() -> left.pagesReceived() + right.pagesReceived(), terminal));
        } else {
            runAndStream(spec.fragmentId(), terminal, resultQueue,
                    () -> livenessProgress(() -> left.pagesReceived() + right.pagesReceived(), terminal));
        }
    }

    // ==================== MiddleJoinConsumer：own 桶 → 本地 join → 重哈希推给下一层（不回传） ====================

    /**
     * 多 join 链中间层：{@code executeConsumer} 的头（两个 ShuffleReader → 本地 HashJoin）
     * + {@code executeProducer} 的尾（HashPartition 把 join 输出按下一层 join 左键重新哈希，
     * 经 ExchangeChannel 推给下一层交换）。<b>不回传 Master</b> —— join 输出留在 Worker 侧
     * 继续流向下一层 join，靠 exchange 的 complete/onComplete 协议让下一层 {@link ShuffleReader}
     * 收齐 {@code partitionEnd}；只有最末层 join（普通 Consumer）才回传 Master。
     *
     * <p>失败传导：本片段异常会传播到上层由 WorkerManager 记录任务失败；若 join / 分发失败，
     * HashPartitionOperator 已把 {@code distributorFailure} 经 {@code channel.fail} 通知
     * exchange#(k+1)-L 的接收端（fail-fast），下一层 join 的 ShuffleReader 据此抛错 → 逐级
     * 传导到最末层 Consumer → 最终以 FragmentFailed 回传 Master。</p>
     */
    private void executeMiddleJoinConsumer(FragmentSpec.MiddleJoinConsumer spec) throws Exception {
        FragmentSpec.JoinStep joinStep = spec.join();
        FragmentSpec.PartitionStep nextPartition = spec.nextPartition();
        // 接收端已在派发阶段注册进本 Worker 的 ExchangeManager
        ShuffleReader left = exchangeManager.readerFor(spec.leftIn().exchangeId());
        ShuffleReader right = exchangeManager.readerFor(spec.rightIn().exchangeId());
        Operator join = localJoin(left, right, joinStep);
        trackSpill(spec.fragmentId(), join);   // 中间层 join 溢写（与下方行数采样器叠加，通道独立）
        // 多维聚合展开（最末层才有）：join 输出之后、按 (维度…, gid) 重哈希之前复制行 ——
        // 整条 join 链只跑一次，基表只扫一次
        Operator expand = spec.expand() == null ? null
                : new ExpandOperator(join, spec.expand().dimColumns(), spec.expand().groupingSets());
        Operator downstream = expand != null ? expand : join;
        // 分区级实时遥测（Join 框 n 个分区任务）只有 HashJoinOperator 有；排序归并走碎片统计
        HashJoinOperator hashJoin = join instanceof HashJoinOperator h ? h : null;
        HashPartitionOperator part = null;
        boolean completed = false;
        try {
            ExchangeChannel channel = exchangeManager.channelFor();
            // distribution / fixedBucket 必须透传：链上广播（P1-a）喂给末层的这一层是 FIXED
            //   （本机 join 输出就地入本机桶）；其余层 HASH。忽略它会静默退回重哈希 → 广播白做、
            //   且分区数与 owner 映射不一致 → 结果错。
            part = new HashPartitionOperator(downstream, nextPartition.keyColumns(), nextPartition.numPartitions(), 1,
                    channel, nextPartition.ownerWorkerIds().toArray(new String[0]),
                    nextPartition.exchangeIds().toArray(new String[0]),
                    exchangeManager.selfWorkerId(),
                    nextPartition.distribution(), nextPartition.fixedBucket());
            track(spec, part);   // 采样器先登记：中间层 join 输出行数（Join 框实时行数来源）
            trackJoinPartitions(spec.fragmentId(), hashJoin);   // 片段内部分区并行度（Join 框 n 个分区任务）
            part.open();
            part.awaitDistributedDone();   // 等所有分发线程完成并已通知下一层所有归属 Worker
            if (part.distributorFailure() != null) {
                throw new RuntimeException("MiddleJoinConsumer 分发失败 fragment=" + spec.fragmentId(),
                        part.distributorFailure());
            }
            completed = true;
        } finally {
            if (completed) {
                finishTelemetry(spec.fragmentId());
                finishJoinPartitions(spec.fragmentId(), hashJoin);
            } else {
                untrack(spec.fragmentId());
                untrackJoinPartitions(spec.fragmentId(), hashJoin);
            }
            if (part != null) {
                try { part.close(); } catch (Throwable ignored) { }
            }
            // downstream 关闭会级联关闭上游 join（AbstractMeasurableOperator.doClose 递归）
            try { downstream.close(); } catch (Throwable ignored) { }
        }
    }

    // ==================== MiddleAggregateConsumer：own 桶 → 本地聚合 → 重哈希推给 join（不回传） ====================

    /**
     * 派生表分布式 join 的聚合中间层：{@code executeAggregateConsumer} 的头（ShuffleReader →
     * 本地 HashAggregation）+ {@code executeProducer} 的尾（HashPartition 把聚合输出按派生表
     * 一侧 join 键重新哈希，经 ExchangeChannel 推给派生 join 交换）。<b>不回传 Master</b> ——
     * 派生体输出留在 Worker 侧继续流向 join，靠 exchange 的 complete/onComplete 协议让最终
     * join 的 {@link ShuffleReader} 收齐 {@code partitionEnd}；只有最终 join（普通
     * {@link FragmentSpec.Consumer}）才回传 Master。
     *
     * <p>失败传导：本片段异常上抛由 WorkerManager 记录任务失败；若聚合 / 分发失败，
     * HashPartitionOperator 已把 {@code distributorFailure} 经 {@code channel.fail} 通知 join
     * 交换的接收端（fail-fast）→ 最终 join 的 ShuffleReader 据此抛错 → 以 FragmentFailed 回传
     * Master（与 MiddleJoinConsumer 同构）。</p>
     */
    private void executeMiddleAggregateConsumer(FragmentSpec.MiddleAggregateConsumer spec) throws Exception {
        FragmentSpec.AggregateStep aggStep = spec.aggregate();
        FragmentSpec.PartitionStep nextPartition = spec.nextPartition();
        // 接收端已在派发阶段注册进本 Worker 的 ExchangeManager
        ShuffleReader reader = exchangeManager.readerFor(spec.in().exchangeId());
        Operator src = new PartitionedSourceOperator(reader, spec.in().numPartitions());
        // SUM(CASE WHEN ...) / SUM(IF(...)) 派生列：CASE 逐行、与分区无关，Worker 本地先算派生列再
        // 聚合（与单机一致；聚合输入列被 CaseProjectOperator 追加在页尾，聚合按列名引用派生列）
        if (!aggStep.caseProjections().isEmpty()) {
            src = new CaseProjectOperator(src, aggStep.caseProjections());
        }
        List<AggregationSpec> aggs = new ArrayList<>();
        for (FragmentSpec.AggregationDto dto : aggStep.aggregations()) {
            aggs.add(aggregationSpecFor(dto));
        }
        HashAggregationOperator agg = new HashAggregationOperator(
                src, aggStep.keyColumns(), aggs, aggStep.memoryLimit(), aggStep.numPartitions());
        trackSpill(spec.fragmentId(), agg);   // 派生聚合中间层溢写（与下方行数采样器叠加）
        // 派生内层 HAVING：聚合输出行级过滤（每 key 整组都在本分区聚齐 → 按聚合输出行过滤语义与单机一致）。
        // FilterOperator 按列名引用聚合输出列（keys + agg 输出列）；进 HashPartitionOperator 前逐行过滤。
        Operator feed = agg;
        String postAggregate = spec.filterAfterAggregate();
        if (postAggregate != null && !postAggregate.isBlank()) {
            // 按顶层 AND 拆成多个 FilterOperator：单个 FilterOperator 只编译「单个简单谓词」，
            // 整串合取会落 UNKNOWN → <b>静默放行全部行</b>（派生内层 HAVING 与「挂在派生关系上的
            // 谓词」以 AND 相连时必然出现这种合取串）。
            for (String cond : SqlTextUtil.splitConjuncts(postAggregate)) {
                if (!cond.isBlank()) {
                    feed = new FilterOperator(feed, cond);
                }
            }
        }
        HashPartitionOperator part = null;
        boolean completed = false;
        try {
            ExchangeChannel channel = exchangeManager.channelFor();
            // distribution / fixedBucket 必须透传：全局聚合派生源（非相关标量子查询的单值广播 build 侧）
            //   的 nextPartition 是 REPLICATE（把恒 1 行整份复制到全部 n 个桶）；其余层 HASH。
            //   忽略它会静默退回「按空键哈希」→ 1 行只落 0 号桶 → 其余分区 build 侧为空 ⇒ 静默丢行。
            part = new HashPartitionOperator(feed, nextPartition.keyColumns(), nextPartition.numPartitions(), 1,
                    channel, nextPartition.ownerWorkerIds().toArray(new String[0]),
                    nextPartition.exchangeIds().toArray(new String[0]),
                    exchangeManager.selfWorkerId(),
                    nextPartition.distribution(), nextPartition.fixedBucket());
            track(spec, part);   // 采样器先登记：派生聚合中间层输出行数（HAVING 后，Aggregate→Shuffle 框行数来源）
            part.open();
            part.awaitDistributedDone();   // 等所有分发线程完成并已通知最终 join 的归属 Worker
            if (part.distributorFailure() != null) {
                throw new RuntimeException("MiddleAggregateConsumer 分发失败 fragment=" + spec.fragmentId(),
                        part.distributorFailure());
            }
            completed = true;
        } finally {
            if (completed) {
                finishTelemetry(spec.fragmentId());
            } else {
                untrack(spec.fragmentId());
            }
            if (part != null) {
                try { part.close(); } catch (Throwable ignored) { }
            }
            try { agg.close(); } catch (Throwable ignored) { }
        }
    }

    // ==================== MiddleWindowConsumer：own 桶 → 本地窗口 → 重哈希推给 join（不回传） ====================

    /**
     * 派生表分布式 join 的窗口中间层：{@code executeWindowConsumer} 的头（ShuffleReader →
     * 本地 WindowSort + WindowFunction）+ {@code executeProducer} 的尾（HashPartition 把窗口输出
     * 按派生表一侧 join 键重新哈希，经 ExchangeChannel 推给派生 join 交换）。<b>不回传 Master</b> ——
     * 派生体输出留在 Worker 侧继续流向 join，靠 exchange 的 complete/onComplete 协议让最终 join 的
     * {@link ShuffleReader} 收齐 {@code partitionEnd}；只有最终 join（普通 {@link FragmentSpec.Consumer}）
     * 才回传 Master。
     *
     * <p>失败传导：与 {@code MiddleAggregateConsumer} 同构 —— 分发失败经 {@code channel.fail}
     * 通知 join 交换接收端（fail-fast），最终以 FragmentFailed 回传 Master。</p>
     */
    private void executeMiddleWindowConsumer(FragmentSpec.MiddleWindowConsumer spec) throws Exception {
        FragmentSpec.WindowStep w = spec.window();
        FragmentSpec.PartitionStep nextPartition = spec.nextPartition();
        // 接收端已在派发阶段注册进本 Worker 的 ExchangeManager
        ShuffleReader reader = exchangeManager.readerFor(spec.in().exchangeId());
        Operator src = new PartitionedSourceOperator(reader, spec.in().numPartitions());
        WindowSortOperator sort = new WindowSortOperator(
                src, w.partitionBy(), w.orderBy(), w.ascending(), w.memoryLimit(), w.numPartitions());
        WindowFunctionOperator win = new WindowFunctionOperator(
                sort, w.partitionBy(), w.functions(), w.orderBy(), AGGREGATE_REGISTRY);
        applyWindowFilter(w, sort, win);
        trackSpill(spec.fragmentId(), sort);   // 派生窗口中间层溢写（WindowSort 是溢写方，与行数采样器叠加）
        trackSpill(spec.fragmentId(), win);    // 窗口函数「整分区缓冲」溢写（另一路写盘，累加）
        HashPartitionOperator part = null;
        boolean completed = false;
        try {
            ExchangeChannel channel = exchangeManager.channelFor();
            part = new HashPartitionOperator(win, nextPartition.keyColumns(), nextPartition.numPartitions(), 1,
                    channel, nextPartition.ownerWorkerIds().toArray(new String[0]),
                    nextPartition.exchangeIds().toArray(new String[0]),
                    exchangeManager.selfWorkerId(),
                    nextPartition.distribution(), nextPartition.fixedBucket());
            track(spec, part);   // 采样器先登记：派生窗口中间层输出行数
            part.open();
            part.awaitDistributedDone();   // 等所有分发线程完成并已通知最终 join 的归属 Worker
            if (part.distributorFailure() != null) {
                throw new RuntimeException("MiddleWindowConsumer 分发失败 fragment=" + spec.fragmentId(),
                        part.distributorFailure());
            }
            completed = true;
        } finally {
            if (completed) {
                finishTelemetry(spec.fragmentId());
            } else {
                untrack(spec.fragmentId());
            }
            if (part != null) {
                try { part.close(); } catch (Throwable ignored) { }
            }
            try { win.close(); } catch (Throwable ignored) { }
        }
    }

    // ==================== Consumer：own 桶 → 本地聚合 / 窗口 → 回传 ====================

    private void executeAggregateConsumer(FragmentSpec.AggregateConsumer spec,
                                          BlockingQueue<FragmentEvent> resultQueue) {
        // 接收端已在派发阶段注册进本 Worker 的 ExchangeManager
        ShuffleReader reader = exchangeManager.readerFor(spec.in().exchangeId());
        Operator src = new PartitionedSourceOperator(reader, spec.in().numPartitions());
        // SUM(CASE WHEN ...) 派生列：CASE 逐行、与分区无关，Worker 本地先算派生列再聚合（结果与单机一致）
        if (!spec.aggregate().caseProjections().isEmpty()) {
            src = new CaseProjectOperator(src, spec.aggregate().caseProjections());
        }
        List<AggregationSpec> aggs = new ArrayList<>();
        for (FragmentSpec.AggregationDto dto : spec.aggregate().aggregations()) {
            aggs.add(aggregationSpecFor(dto));
        }
        HashAggregationOperator agg = new HashAggregationOperator(
                src, spec.aggregate().keyColumns(), aggs,
                spec.aggregate().memoryLimit(), spec.aggregate().numPartitions());
        trackSpill(spec.fragmentId(), agg);   // 终端聚合溢写（行数走数据面，此处只报溢写）
        // 二级聚合（外层全局聚合下沉）：本地聚合输出直接喂第二次 HashAggregation（同 Worker、
        // 零交换），每分区只回传 1 行 partial —— Master 侧只需合并 N 行，不再单机扫全部内层结果。
        Operator result = spec.followUp() != null
                ? buildFollowUp(spec.fragmentId(), agg, spec.followUp())
                : agg;
        // 顶层 ORDER BY/LIMIT 下推：终端缝挂截断尾（本地排序 + 截到 offset+limit 候选）
        Operator terminal = spec.tail() != null ? OrderByTail.wrap(result, spec.tail()) : result;
        // 心跳进度源 = shuffle 收页 + 聚合 Phase 2 归并进度：Phase 1/归并长时间不吐结果时 Keepalive
        if (spec.sink() != null) {
            runAndSink(spec.fragmentId(), terminal, spec.sink(), resultQueue,
                    () -> livenessProgress(() -> reader.pagesReceived(), terminal));
        } else {
            runAndStream(spec.fragmentId(), terminal, resultQueue,
                    () -> livenessProgress(() -> reader.pagesReceived(), terminal));
        }
    }

    private void executeWindowConsumer(FragmentSpec.WindowConsumer spec,
                                       BlockingQueue<FragmentEvent> resultQueue) {
        // 接收端已在派发阶段注册进本 Worker 的 ExchangeManager
        ShuffleReader reader = exchangeManager.readerFor(spec.in().exchangeId());
        PartitionedSourceOperator src = new PartitionedSourceOperator(reader, spec.in().numPartitions());
        FragmentSpec.WindowStep w = spec.window();
        WindowSortOperator sort = new WindowSortOperator(
                src, w.partitionBy(), w.orderBy(), w.ascending(), w.memoryLimit(), w.numPartitions());
        WindowFunctionOperator win = new WindowFunctionOperator(
                sort, w.partitionBy(), w.functions(), w.orderBy(), AGGREGATE_REGISTRY);
        applyWindowFilter(w, sort, win);
        trackSpill(spec.fragmentId(), sort);   // 终端窗口排序溢写（行数走数据面，此处只报溢写）
        trackSpill(spec.fragmentId(), win);    // 窗口函数「整分区缓冲」溢写（另一路写盘，累加）
        // 顶层 ORDER BY/LIMIT 下推：终端缝挂截断尾（本地排序 + 截到 offset+limit 候选）
        Operator terminal = spec.tail() != null ? OrderByTail.wrap(win, spec.tail()) : win;
        // 心跳进度源 = shuffle 收页 + 窗口排序 Phase 2 归并进度：
        // 「收齐 → 长归并（K 路归并压缩 + 逐组输出）」期间收页数不动，靠归并 tick 续命
        if (spec.sink() != null) {
            runAndSink(spec.fragmentId(), terminal, spec.sink(), resultQueue,
                    () -> livenessProgress(() -> reader.pagesReceived(), terminal));
        } else {
            runAndStream(spec.fragmentId(), terminal, resultQueue,
                    () -> livenessProgress(() -> reader.pagesReceived(), terminal));
        }
    }

    // ==================== Consumer：own 桶 → 本地 join → 同 key 后续计算（零二次交换） ====================

    /**
     * 链式片段（Phase 2）：本地 HashJoin 输出<b>直接</b>喂给同 key 的窗口 / 聚合（零二次交换）。
     * join 输出已在归属 Worker 上按 key 分布（co-partitioning），后续计算无需再 shuffle，
     * 数据全程留在本机，只有最终结果回传 Master —— 即 "同 key 只需一次分区、所有操作本地执行"。
     */
    private void executeChainedJoinConsumer(FragmentSpec.ChainedJoinConsumer spec,
                                            BlockingQueue<FragmentEvent> resultQueue) {
        // 接收端已在派发阶段注册进本 Worker 的 ExchangeManager
        ShuffleReader left = exchangeManager.readerFor(spec.leftIn().exchangeId());
        ShuffleReader right = exchangeManager.readerFor(spec.rightIn().exchangeId());
        Operator join = localJoin(left, right, spec.join());
        trackSpill(spec.fragmentId(), join);   // 链式 join 溢写（与后续窗口 / 聚合溢写叠加，各算子各报一路）
        Operator op = buildFollowUp(spec.fragmentId(), join, spec.followUp());
        // 顶层 ORDER BY/LIMIT 下推：终端缝挂截断尾（本地排序 + 截到 offset+limit 候选）
        Operator terminal = spec.tail() != null ? OrderByTail.wrap(op, spec.tail()) : op;
        // 心跳进度源 = 两侧 shuffle 收页 + 后续窗口/聚合的归并进度：
        // 链式 join→聚合/窗口的最末层 Phase 1/归并长时间不吐结果时 Keepalive
        if (spec.sink() != null) {
            runAndSink(spec.fragmentId(), terminal, spec.sink(), resultQueue,
                    () -> livenessProgress(() -> left.pagesReceived() + right.pagesReceived(), terminal));
        } else {
            runAndStream(spec.fragmentId(), terminal, resultQueue,
                    () -> livenessProgress(() -> left.pagesReceived() + right.pagesReceived(), terminal));
        }
    }

    /**
     * 按 Master 的选型结论重建本地 join 算子 —— 两侧输入都是本机 own 桶
     * （{@link com.dojao.core.operator.PartitionedSource}），三种算法共用同一套算子代码，只换实现。
     *
     * <p><b>{@code BROADCAST} 落到 {@link HashJoinOperator}</b>：广播由<b>分区方式</b>而非算子形态实现
     * （build 侧 {@code REPLICATE} 复制到每台、probe 侧 {@code FIXED} 就地不动，见
     * {@code StageScheduler.planBroadcastJoin}）—— 每台的右桶恰好是完整右表，分区哈希 join 直接可用。</p>
     *
     * <p><b>{@code SORT_MERGE} 需要把桶队列接回拉取模型</b>：它两侧上游是 {@link Operator}（自己按
     * {@code next()} 拉、按页 {@code partitionId} 分槽排序），而 {@link ShuffleReader} 只实现
     * {@link PartitionedSource}（推送进桶有界队列）→ 用 {@link PartitionedSourceOperator} 适配
     * （合并拉取各桶、重标分区号、丢弃 {@code partitionEnd}），与单机路径「HashPartitionOperator
     * 合并拉取 → SortMergeJoin」完全同构 —— 区别只是上游从本地分发线程换成跨机 shuffle。</p>
     *
     * <p>选型由 Master 在规划期完成（只有它拿得到连接器统计）：Worker 只按结论重建，不重复判断。</p>
     */
    private static Operator localJoin(PartitionedSource left, PartitionedSource right,
                                      FragmentSpec.JoinStep joinStep) {
        return switch (joinStep.algorithm()) {
            case SORT_MERGE -> new SortMergeJoinOperator(
                    new PartitionedSourceOperator(left, left.numPartitions()),
                    new PartitionedSourceOperator(right, right.numPartitions()),
                    joinStep.leftKeyColumns(), joinStep.rightKeyColumns(),
                    joinStep.joinType(), joinStep.memoryLimit(), joinStep.numPartitions(),
                    // 空 build 侧（右表某分区一页未收）时用声明列造输出 schema：
                    // LEFT/FULL 要吐「左行 + 右列全 NULL」，没有右列类型就造不出来（曾静默 0 行）。
                    // 空 probe 侧（左表某分区一页未收）同理：RIGHT/FULL 要吐「右行 + 左列全 NULL」，
                    // 没有左列类型 SortMergeJoin 直接抛错、Hash 路径静默丢孤儿行。
                    joinStep.declaredRightFields(), joinStep.declaredLeftFields());
            // HASH / BROADCAST：同一个分区哈希 join（广播只改分区方式，不改算子）
            // declaredRightFields：Master 规划期下发的右表声明列。哈希分区对空桶一页不发，没有它
            // 该分区就会吐「只有左列」的短 schema（详见 ArrowSchemaIpc / HashJoinOperator）。
            case HASH, BROADCAST -> new HashJoinOperator(left, right,
                    joinStep.leftKeyColumns(), joinStep.rightKeyColumns(),
                    joinStep.joinType(), joinStep.memoryLimit(), joinStep.numPartitions(),
                    joinStep.declaredRightFields(), joinStep.declaredLeftFields());
        };
    }

    /**
     * 提前过滤下推（Master 规划期下发）：把 {@code WHERE rn <= N} 这类外层窗口过滤挂到
     * {@link WindowSortOperator}，在 Phase 1 末尾就地裁剪，避免整组数据回传 Master 再过滤。
     *
     * <p>等值场景与单机 {@code Planner.tryPushDownWindowFilter} 同口径：给
     * {@link WindowFunctionOperator} 设值覆写（跳过逐行计算）。<b>SUM 不设</b> —— per-key 符号检查下
     * 混合符号分组不过滤，覆写常量会出错。
     */
    private static void applyWindowFilter(FragmentSpec.WindowStep w, WindowSortOperator sort,
                                          WindowFunctionOperator win) {
        WindowFilterConfig f = w.filter();
        if (f == null) return;
        sort.setFilterConfig(f);
        if (f.operator() == WindowFilterConfig.FilterOp.EQ && f.functionType() != WindowFunctionType.SUM) {
            win.setValueOverride(f.functionType(), f.targetValue());
        }
    }

    /**
     * 同 key 后续计算：把 join 输出接到本地窗口 / 聚合上（join 是 spillable 上游，open/close 自动传播）。
     *
     * <p>后续算子（窗口排序 / 聚合）同样是溢写方，按 {@code fragmentId} 登记溢写采样器 —— 与 join 那路
     * 叠加（同一片段内多个溢写算子各溢各的）。</p>
     */
    private Operator buildFollowUp(String fragmentId, Operator upstream, FragmentSpec.FollowUp followUp) {
        return switch (followUp) {
            case FragmentSpec.WindowStep w -> {
                WindowSortOperator sort = new WindowSortOperator(
                        upstream, w.partitionBy(), w.orderBy(), w.ascending(), w.memoryLimit(), w.numPartitions());
                WindowFunctionOperator win = new WindowFunctionOperator(
                        sort, w.partitionBy(), w.functions(), w.orderBy(), AGGREGATE_REGISTRY);
                applyWindowFilter(w, sort, win);
                trackSpill(fragmentId, sort);
                trackSpill(fragmentId, win);   // 窗口函数缓冲溢写（与排序溢写累加）
                yield win;
            }
            case FragmentSpec.AggregateStep a -> {
                // SUM(CASE WHEN ...) / SUM(IF(...)) 派生列：CASE 逐行、与分区无关，Worker 本地
                // 先算派生列再聚合（与单机一致；join 输出即上游，派生列按 join 输出 schema 解析）
                Operator aggInput = upstream;
                if (!a.caseProjections().isEmpty()) {
                    aggInput = new CaseProjectOperator(upstream, a.caseProjections());
                }
                List<AggregationSpec> aggs = new ArrayList<>();
                for (FragmentSpec.AggregationDto dto : a.aggregations()) {
                    aggs.add(aggregationSpecFor(dto));
                }
                HashAggregationOperator aggOp = new HashAggregationOperator(
                        aggInput, a.keyColumns(), aggs, a.memoryLimit(), a.numPartitions());
                trackSpill(fragmentId, aggOp);
                yield aggOp;
            }
        };
    }

    /**
     * 通用回传循环：拉取算子输出页 → 移交到共享分配器 → 入队回传 → 终止事件（成功/失败）。
     * 同时起<b>心跳线程</b>：阻塞式算子（聚合 Phase 1 / 溢写 / 排序 / join 慢 build）长时间不吐
     * 结果时，只要上游 shuffle 仍在收页（{@code progressCounter} 递增）就周期发 FragmentProgress，
     * 防止 Master 结果看门狗误杀慢查询；无新收页 → 不发心跳，真挂起的任务仍被看门狗 fail-fast。
     *
     * @param progressCounter 心跳进度源：上游 ShuffleReader 收到页数 + 算子链归并进度 tick
     *                        （{@link #livenessProgress}，单调递增 = 任务在推进）。
     */
    private void runAndStream(String fragmentId, Operator op, BlockingQueue<FragmentEvent> resultQueue,
                              LongSupplier progressCounter) {
        AtomicBoolean done = new AtomicBoolean();
        AtomicLong lastEmitNanos = new AtomicLong(System.nanoTime());
        Thread heartbeat = startHeartbeat(fragmentId, resultQueue, progressCounter, lastEmitNanos, done);
        // 排障时间锚（纯日志）：终端片段 open → EOF 的墙钟 + 输出页数。「下游算子消费多久」
        // 与 ShuffleReader 的「页多久到齐」相减即「算子在页到齐后又跑了多久」。
        final long profStartNanos = System.nanoTime();
        int profPages = 0;
        boolean completed = false;
        try {
            op.open();
            while (true) {
                Page p = op.next();
                if (p.isEOF()) break;
                profPages++;
                // 输出页的 buffer 所有权移交到本 Worker 交换共享分配器再入队：Master 可能延迟
                // close 该页，若仍挂在算子内部分配器上，op.close() 关闭分配器会误报内存泄漏
                Page owned = p.transferTo(exchangeManager.allocator());
                lastEmitNanos.set(System.nanoTime());
                // 数据页有界阻塞入队 = 背压（Master 消费慢 → 算子阻塞 → 分区队列 → 发送端）
                resultQueue.put(new FragmentEvent.FragmentData(fragmentId, owned));
            }
            emitTerminal(resultQueue, new FragmentEvent.FragmentFinished(fragmentId), 5);
            completed = true;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();   // 被取消：直接退出，不发终止事件（调用方已放弃）
        } catch (Throwable t) {
            // 取消 ≠ 失败：算子包装层（AbstractMeasurableOperator"… next failed" /
            // AbstractSpillableOperator"Phase 2 分区归并失败"）会把中断信号裹进异常链，
            // 但线程中断标记已由包装层补回。此处认标记 → 静默退出（不发 FragmentFailed、
            // 不打失败日志），否则 Master 侧会把这当成真失败刷 WARN 堆栈。
            if (Thread.currentThread().isInterrupted()) return;
            // 显式失败回传：PartitionedSourceOperator 已把 ShuffleReader 的 distributorFailure
            // rethrow（fail-fast），cause 带完整堆栈链，Master 侧可精确定位根因。
            emitFailure(resultQueue, new FragmentEvent.FragmentFailed(fragmentId, fullStack(t)), t, 5);
        } finally {
            System.err.printf("[SHUFFLE-PROF] terminal 消费完成 taskId=%s：输出 %d 页，用时 %dms%n",
                    fragmentId, profPages, (System.nanoTime() - profStartNanos) / 1_000_000);
            done.set(true);
            if (heartbeat != null) {
                try { heartbeat.join(3_000); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
            try { op.close(); } catch (Throwable ignored) { }
            // 终端片段的溢写采样器收尾（未登记溢写采样器 → no-op）：正常跑完补发终值 + done，
            // 失败 / 取消只注销不补发 —— 失败品不该被呈现成「正常跑完」。
            if (completed) {
                finishTelemetry(fragmentId);
            } else {
                untrack(fragmentId);
            }
        }
    }

    /**
     * 分布式写回传循环：拉取算子输出页 → 本地 {@code connector.write} 写走 + 计数，最后只回传
     * {@code FragmentFinished(fragmentId, rows)}（完成 ack + 行数，<b>无数据页 —— 数据不回
     * Master</b>）。与 {@link #runAndStream} 同构：异常 → {@code FragmentFailed}（fail-fast）；
     * 心跳复用 {@link #startHeartbeat}（慢 shuffle / 写前积累不误杀）。
     *
     * <p><b>关键差异（sink liveness）</b>：drain 写外部数据源时输入早已收齐、{@code progressCounter}
     * 静止，但写走可能要数十秒/数分钟 —— 若只靠它判存活，Master 结果看门狗会把长写误判为链路
     * 故障 fail-fast。故心跳进度 = {@code progressCounter + 已写出行数}：写走每前进一页，存活信号就
     * 递增一次，Master 看门狗据此续命；真挂起（无新输入也无新写出）仍会被看门狗兜住。</p>
     *
     * @param progressCounter 心跳进度源（同 {@link #runAndStream}：上游 ShuffleReader 收到页数）。
     */
    private void runAndSink(String fragmentId, Operator op, FragmentSpec.SinkStep sink,
                            BlockingQueue<FragmentEvent> resultQueue, LongSupplier progressCounter) {
        AtomicBoolean done = new AtomicBoolean();
        AtomicLong lastEmitNanos = new AtomicLong(System.nanoTime());
        AtomicLong sunkRows = new AtomicLong();   // 已写出行数：drain 阶段的心跳存活信号
        Thread heartbeat = startHeartbeat(fragmentId, resultQueue,
                () -> progressCounter.getAsLong() + sunkRows.get(), lastEmitNanos, done);
        long rows = 0;
        boolean completed = false;
        // 排障计时（纯日志）：把「上游产出（= 窗口 Phase 2 流式读取）」与「connector.write（Hive ORC 落盘）」
        // 分开累计 —— 若 writeMs ≫ nextMs，Phase 2 的慢就不是本算子读/归并，而是被下游 sink 背压拖住。
        long nextNanos = 0L;
        long writeNanos = 0L;
        final long sinkStartNanos = System.nanoTime();
        try {
            // 目标 catalog 未在本 Worker 注册 → IllegalStateException → FragmentFailed（明确报错）
            DojaoConnector conn = provider.get(sink.catalog());
            op.open();
            while (true) {
                long t0 = System.nanoTime();
                Page p = op.next();
                nextNanos += System.nanoTime() - t0;
                if (p.isEOF()) break;
                // 跳过 partition-end 标记页（无 Arrow root、零数据），只写真实数据页
                if (p.getRoot() != null) {
                    long t1 = System.nanoTime();
                    conn.write(new WriteRequest(sink.catalog(), sink.schema(), sink.table(),
                            p, sink.targetColumns()));
                    writeNanos += System.nanoTime() - t1;
                    rows += p.getRowCount();
                    sunkRows.addAndGet(p.getRowCount());
                }
                p.close();
            }
            conn.closeWrite();
            emitTerminal(resultQueue, new FragmentEvent.FragmentFinished(fragmentId, rows), 5);
            completed = true;
        } catch (Throwable t) {
            // 同 runAndStream：中断标记在 = 取消，不是失败 → 静默退出，不发 FragmentFailed。
            // 本路径无裸 InterruptedException（write/next 均不声明受检异常），中断只会以
            // 包装异常形式出现，故统一靠标记判定。
            if (Thread.currentThread().isInterrupted()) return;
            emitFailure(resultQueue, new FragmentEvent.FragmentFailed(fragmentId, fullStack(t)), t, 5);
        } finally {
            System.err.printf("[SHUFFLE-PROF] sink 写完 taskId=%s：%d 行，op.next() %dms + connector.write %dms，总用时 %dms%n",
                    fragmentId, rows, nextNanos / 1_000_000, writeNanos / 1_000_000,
                    (System.nanoTime() - sinkStartNanos) / 1_000_000);
            done.set(true);
            if (heartbeat != null) {
                try { heartbeat.join(3_000); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
            try { op.close(); } catch (Throwable ignored) { }
            // 同 runAndStream：终端写片段的溢写采样器收尾（未登记 → no-op）
            if (completed) {
                finishTelemetry(fragmentId);
            } else {
                untrack(fragmentId);
            }
        }
    }

    /**
     * 心跳进度源 = 上游 shuffle 收页数 + 算子链的<b>归并进度</b>（{@link MergeProgress#mergeProgressTicks()}）。
     *
     * <p><b>为什么必须加后半截</b>：溢写算子的 Phase 2（分区归并）在<b>产出任何页之前</b>可能持续很久，
     * 而此时输入早已收齐 —— 只看收页数会判定「无进展」，Master 结果看门狗（默认 60s）遂把长归并误判为
     * 「doExchange result 回传链路故障」并 fail-fast。归并 tick 恰在归并推进一步（写完一个输出批 /
     * 吐出一页 / 处理完一批 key）时自增，把它并进进度源即可续命；<b>真卡死</b>（归并线程阻塞/死锁）时
     * 两者都不动，看门狗照常兜底。</p>
     *
     * @param upstreamPages 上游收页数（原有通道）
     * @param op            该缝的最终算子；{@code terminal} 链上任何溢写算子的归并 tick 都会被算进来
     */
    private static long livenessProgress(LongSupplier upstreamPages, Operator op) {
        long pages = upstreamPages.getAsLong();
        return pages + (op instanceof MergeProgress mp ? mp.mergeProgressTicks() : 0L);
    }

    /**
     * 心跳线程：每 {@link #HEARTBEAT_INTERVAL_MS} 检查一次。条件 = ① 超过心跳间隔无任何结果回传
     * （{@code lastEmitNanos} 很久未更新，即非数据流动期）且 ② 上游 shuffle 有新收页（进度源递增）。
     * 两者都满足 → 发 {@link FragmentEvent.FragmentProgress}（Master 看门狗重置）。② 不满足 = 输入
     * 没在流动（真挂起 / 死锁 / 发送方全停），不发心跳，Master 看门狗照常 fail-fast。
     */
    private static Thread startHeartbeat(String fragmentId, BlockingQueue<FragmentEvent> resultQueue,
                                         LongSupplier progressCounter, AtomicLong lastEmitNanos,
                                         AtomicBoolean done) {
        Thread t = new Thread(() -> {
            long lastRows = progressCounter.getAsLong();
            while (!done.get()) {
                try {
                    Thread.sleep(HEARTBEAT_INTERVAL_MS);
                } catch (InterruptedException e) {
                    return;
                }
                if (done.get()) return;
                if ((System.nanoTime() - lastEmitNanos.get()) / 1_000_000 < HEARTBEAT_INTERVAL_MS) {
                    continue;   // 最近有数据/心跳回传，无需额外心跳
                }
                long rows = progressCounter.getAsLong();
                if (rows == lastRows) {
                    continue;   // 无新输入 shuffle 收页 → 真挂起，不发心跳（保留看门狗死锁检测）
                }
                lastRows = rows;
                emit(resultQueue, new FragmentEvent.FragmentProgress(fragmentId), 5);
            }
        }, "fragment-heartbeat-" + fragmentId);
        t.setDaemon(true);
        t.start();
        return t;
    }

    /**
     * 按名重建聚合 {@link AggregationSpec}（FragmentSpec 只传 aggregateName）。
     * <b>collect-evaluate 型优先</b>：先查 {@link CollectAggregateRegistry}（框架攒值，外部只写
     * evaluate），命中 → 用 collect 工厂；否则按增量型 {@link #aggregateFor} 重建。伪分布式
     * （同 JVM）下静态注册表跨 Worker/Master 共享，collect 桥实例按名即命中。
     */
    private static AggregationSpec aggregationSpecFor(FragmentSpec.AggregationDto dto) {
        CollectAggregateFunction collectFn = CollectAggregateRegistry.getGlobal(dto.aggregateName());
        if (collectFn != null) {
            return AggregationSpec.collect(dto.outputColumn(), collectFn, dto.collectInputColumns());
        }
        return new AggregationSpec(dto.inputColumn(), dto.outputColumn(), aggregateFor(dto.aggregateName()));
    }

    /**
     * 按名重建聚合函数实例（FragmentSpec 只传 aggregateName）。内置五函数用单例（与单机 Planner
     * switch 内置优先一致）；自定义函数查全局注册表 —— CREATE FUNCTION 后由 FunctionManager.
     * ensureLoaded 灌入（同 JVM 即可命中），聚合多阶段（createState / accumulate / merge / evaluate）
     * 由该实例在 worker 侧本地调用，Python 聚合经桥代理回 sidecar。
     */
    private static AggregateFunction<?> aggregateFor(String name) {
        AggregateFunction<?> builtin = switch (name.toLowerCase()) {
            case "count" -> CountAggregateFunction.INSTANCE;
            case "count_distinct" -> CountDistinctAggregateFunction.INSTANCE;
            case "sum" -> SumAggregateFunction.INSTANCE;
            case "avg" -> AvgAggregateFunction.INSTANCE;
            case "min" -> MinAggregateFunction.INSTANCE;
            case "max" -> MaxAggregateFunction.INSTANCE;
            default -> null;
        };
        if (builtin != null) return builtin;
        AggregateFunction<?> custom = AggregateFunctionRegistry.getGlobal(name);
        if (custom != null) return custom;
        throw new IllegalStateException("分布式聚合不支持的函数: " + name);
    }

    /** 有界超时投放<b>进度</b>事件：丢失只影响存活判定，不影响正确性 → 尽力而为。 */
    private static void emit(BlockingQueue<FragmentEvent> queue, FragmentEvent event, long timeoutSec) {
        try {
            queue.offer(event, timeoutSec, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 投放<b>终止事件</b>（FragmentFinished / FragmentFailed）：必须确认已交付。
     *
     * <p>旧实现用 {@link #emit} 且忽略 {@code offer} 的返回值 —— 队列满或网络结果链路已断时，
     * 成功/失败通知被<b>静默丢弃</b>：Master 既看不到完成也看不到失败，只能等结果看门狗
     * 10 分钟超时（线上「已收 4/6 个完成、挂起 600069ms」）。宁可在这里 fail-fast。</p>
     */
    private static void emitTerminal(BlockingQueue<FragmentEvent> queue, FragmentEvent event, long timeoutSec) {
        boolean delivered;
        try {
            delivered = queue.offer(event, timeoutSec, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("投放终止事件时被中断，Master 无法得知该片段结果: "
                    + event.getClass().getSimpleName(), ie);
        }
        if (!delivered) {
            throw new IllegalStateException("终止事件 " + timeoutSec + "s 内未能交付 Master（结果回传链路已断）: "
                    + event.getClass().getSimpleName());
        }
    }

    /**
     * 失败路径投放 FragmentFailed：自身投递失败时挂到原始异常上（suppressed）而不是把它吞掉，
     * 原始异常仍由调用方上抛 → 该片段的失败至少能在 Worker 日志里看到。
     */
    private static void emitFailure(BlockingQueue<FragmentEvent> queue, FragmentEvent.FragmentFailed event,
                                    Throwable cause, long timeoutSec) {
        try {
            emitTerminal(queue, event, timeoutSec);
        } catch (RuntimeException deliveryFailure) {
            cause.addSuppressed(deliveryFailure);
        }
    }

    /** 格式化完整 cause 链堆栈（含根因），替代 String.valueOf 只留顶层信息。 */
    private static String fullStack(Throwable t) {
        StringBuilder sb = new StringBuilder();
        Throwable cur = t;
        while (cur != null) {
            sb.append(cur).append('\n');
            for (StackTraceElement e : cur.getStackTrace()) {
                sb.append("    at ").append(e).append('\n');
            }
            cur = cur.getCause();
            if (cur != null) sb.append("Caused by: ");
        }
        return sb.toString();
    }

}
