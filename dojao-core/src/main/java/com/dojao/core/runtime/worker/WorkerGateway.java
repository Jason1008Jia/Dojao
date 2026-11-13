package com.dojao.core.runtime.worker;

import com.dojao.core.operator.FragmentSpec;
import com.dojao.spi.bean.ReadRequest;
import com.dojao.spi.bean.ScanSlice;

import java.util.concurrent.BlockingQueue;

/**
 * Master → Worker 的任务派发通道 —— 分布式扫描把分区 {@link ReadRequest} 派发给 Worker 执行。
 *
 * <p>与 {@link MasterGateway}（Worker→Master：注册 / 心跳 / 注销）对称，二者都是传输抽象：
 * 生产环境由真实网络传输（Arrow Flight）实现，测试 / 单 JVM 模拟用内存实现。v1 仅支持扫描派发。</p>
 *
 * <p><b>回传契约</b>：实现方须在 Worker 侧独立执行 {@code connector.openSplit(request, split)}
 * 拿到的游标直到 EOF，逐页以 {@link ScanEvent.ScanPage} 放入 {@code resultQueue}（队列有界，回传即背压），随后二选一：
 * <ul>
 *   <li>正常扫完 → 放 {@link ScanEvent.ScanFinished}；</li>
 *   <li>读取抛异常 → 捕获并以 {@link ScanEvent.ScanFailed} 显式回传（<b>禁止吞掉</b>），
 *       失败事件允许阻塞直投（Master 必收，收到即 fail-fast；即使丢失，Master 侧看门狗兜底判 LOST）；</li>
 *   <li>任务被 {@link #cancelTask} 取消 / 线程中断 → 直接退出，<b>不发任何终止事件</b>（调用方已放弃）。</li>
 * </ul></p>
 *
 * @author jiaxin
 * @version 1.1
 * @date 2026/8/11
 */
public interface WorkerGateway {

    /**
     * 派发一个分区的扫描任务给指定 Worker。
     *
     * <p>同步 void 返回（v1）：返回即表示派发已受理，执行结果异步经 {@code resultQueue} 送达
     * （成功 / 失败事件见接口 javadoc）。派发本身失败（如目标 Worker 不存在 / 已停止）抛
     * {@link RuntimeException}，调用方须立即登记该分区失败。</p>
     *
     * @param workerId    目标 Worker（Master 侧 {@code WorkerRegistry} 中的节点）
     * @param jobId       所属查询（queryId）
     * @param taskId      分区任务 ID（Master 侧保证查询内唯一）
     * @param catalog     数据源 catalog（Worker 据此取独立连接器副本）
     * @param slice       自完备的切片描述符（读请求：列裁剪 / 谓词下推 / 分区范围已内置；
     *                    + 连接器私有切片载荷，引擎只搬运不解释）
     * @param tail        片内 top-N 截断尾（可空 = 不下推）：Worker 扫完本分区后按
     *                    ORDER BY 键本地排序 + 截断到 offset+limit 行再回传候选集
     * @param resultQueue 回传 {@link ScanEvent} 的有界队列（Worker 生产，Master 消费）
     */
    void dispatchScan(String workerId, String jobId, String taskId,
                      String catalog, ScanSlice slice,
                      FragmentSpec.SortLimitStep tail,
                      BlockingQueue<ScanEvent> resultQueue);

    /**
     * 派发一个分区扫描任务，可选<b>分片侧投影 + 就地写</b>（scan-sink：CTAS / INSERT 的纯扫描+投影形态）。
     *
     * <p>与 6 参重载的唯一区别是 {@code projection} / {@code sink}：</p>
     * <ul>
     *   <li>{@code projection} 非空 → Worker 扫完切片先投影（纯列投影 / 标量表达式投影）再处理；</li>
     *   <li>{@code sink} 非空 → Worker 投影后<b>就地写目标表</b>，数据页<b>不回传 Master</b>，
     *       只在完成时回传 {@link ScanEvent.ScanFinished#rows()}（写入行数）。</li>
     * </ul>
     *
     * <p>默认实现抛 {@link UnsupportedOperationException}（调用方 Planner 据此回退 Master 写路径），
     * 生产网关与本地测试网关须覆盖。</p>
     */
    default void dispatchScan(String workerId, String jobId, String taskId,
                              String catalog, ScanSlice slice,
                              FragmentSpec.SortLimitStep tail,
                              FragmentSpec.ScanProjection projection,
                              FragmentSpec.SinkStep sink,
                              BlockingQueue<ScanEvent> resultQueue) {
        if (projection == null && sink == null) {
            dispatchScan(workerId, jobId, taskId, catalog, slice, tail, resultQueue);
            return;
        }
        throw new UnsupportedOperationException("该 WorkerGateway 不支持扫描分片就地写");
    }

    /**
     * 取消一个已派发的分区任务（Master 主动放弃：查询失败 / 算子关闭）。
     *
     * <p>Worker 侧须中断该任务的执行线程并停止放事件；被取消的任务不再发终止事件。
     * v1 默认 no-op，实现方按需覆盖。</p>
     *
     * @param workerId 目标 Worker
     * @param taskId   分区任务 ID（派发时的 taskId）
     */
    default void cancelTask(String workerId, String taskId) {
    }

    /**
     * 派发一个执行片段（分布式 join 的最小执行单元）给指定 Worker。
     *
     * <p>{@link FragmentSpec} 是自完备的可序列化片段描述（Producer：扫切片→哈希分区→推送
     * 归属 Worker；Consumer：接 own 桶→本地 HashJoin→回传 Master）。本方法负责把片段投递到
     * Worker 侧执行，执行结果经 {@code resultQueue} 以 {@link FragmentEvent} 送达（仅 Consumer
     * 使用：数据页 + Finished / Failed 终止事件）。</p>
     *
     * <p><b>时序约束</b>：Consumer 片段的 {@link ShuffleReader} 接收端必须在任何 Producer
     * 推送数据<b>之前</b>注册完成。实现方须在 {@code dispatchFragment} 内同步注册接收端
     * （投入任务队列前完成），Master 侧调度须先派发全部 Consumer 再派发 Producer。</p>
     *
     * @param workerId    目标 Worker
     * @param jobId       所属查询（queryId）
     * @param taskId      片段任务 ID（查询内唯一）
     * @param spec        自完备的片段描述
     * @param resultQueue 回传 {@link FragmentEvent} 的有界队列（Consumer 生产，Master 消费）
     */
    void dispatchFragment(String workerId, String jobId, String taskId,
                          FragmentSpec spec, BlockingQueue<FragmentEvent> resultQueue);

    /**
     * 同 {@link #dispatchFragment(String, String, String, FragmentSpec, BlockingQueue)}，额外提供
     * <b>控制事件</b>（FINISHED / FAILED / PROGRESS）的<b>无界</b>队列。
     *
     * <p>为什么需要：{@code resultQueue} 有界（背压），而 Master 侧消费方可能合法地长时间不读本算子
     * —— 典型是 {@code UnionAllOperator} 逐腿预取首屏（腿 0 等 60s shuffle 期间，腿 1..n 已 open 但
     * 没人调 {@code next()}）。数据页/心跳会把这 32 容量的队列灌满，终止事件若也走它就只能超时丢弃，
     * 算子永远等不到该片段完成（线上 ROLLUP 整条查询挂住）。控制事件走独立无界队列后，慢消费者只
     * 影响数据吞吐，不影响完成通知。</p>
     *
     * <p>默认实现回退到单队列版本（测试桩 / 本地网关），生产网关应覆盖本方法。</p>
     *
     * @param controlQueue 控制事件无界队列（可为 null → 回退单队列语义）
     */
    default void dispatchFragment(String workerId, String jobId, String taskId,
                                  FragmentSpec spec, BlockingQueue<FragmentEvent> resultQueue,
                                  BlockingQueue<FragmentEvent> controlQueue) {
        dispatchFragment(workerId, jobId, taskId, spec, resultQueue);
    }

    /**
     * 网关是否支持 <b>Master → Worker 的数据直推</b>（exchange 数据面）。
     *
     * <p>分布式执行下存在"发送方是 Master"的场景：Master 侧流式算子（如 UNION 的去重上游
     * UnionAllOperator）需要按哈希把数据页直接推给 Worker 侧已注册的交换接收端。支持该能力的
     * 网关实现应同时实现 {@link #exchangeChannel()}（返回一条新建的 Master→Worker 通道）与
     * {@link #releaseExchangeChannel}（用毕关闭）。实现方不支持（默认 false）时，Planner 应
     * 回退到 Master 本地聚合（行为不变）。</p>
     *
     * @return true → 本网关可用作 Master→Worker 数据推送通道的提供者
     */
    default boolean supportsExchangePush() {
        return false;
    }

    /**
     * 新建一条 Master→Worker 的数据交换通道（Master 作发送方、不拥有任何桶）。
     *
     * <p>仅当 {@link #supportsExchangePush()} 为 true 时调用。通道生命周期归调用算子所有：
     * 用完必须 {@link #releaseExchangeChannel} 关闭（释放到各 Worker 的 FlightClient / 流）。</p>
     *
     * @throws UnsupportedOperationException 本网关不支持 Master→Worker 数据推送
     */
    default ExchangeChannel exchangeChannel() {
        throw new UnsupportedOperationException(
                "本网关不支持 Master→Worker 交换数据推送（supportsExchangePush=false）");
    }

    /** 释放一条 {@link #exchangeChannel()} 返回的通道（幂等；默认 no-op）。 */
    default void releaseExchangeChannel(ExchangeChannel channel) {
    }
}
