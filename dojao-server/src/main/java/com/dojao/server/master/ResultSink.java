package com.dojao.server.master;

import com.dojao.common.data.Page;

/**
 * Master 侧单个任务的结果汇 —— Worker 经 doExchange "result/{taskId}" 推来的页 / 终止事件
 * 由 Master Flight 生产者路由到这里，本接口把它们落进调用方（{@code NetworkWorkerGateway}）
 * 创建的真实回传队列（{@code BlockingQueue<ScanEvent>} / {@code BlockingQueue<FragmentEvent>}）。
 *
 * <p>与伪分布式同构：Worker 侧生产、Master 消费，队列有界 → 汇实现必须<b>阻塞</b>
 * （put / offer 满则等），使背压经 gRPC 流控传导回 Worker（Worker 生产慢 → 上游算子阻塞）。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/29
 */
public interface ResultSink {

    /** 一页数据（Arrow batch 已拷贝进 Master 分配器，本方法须接管页生命周期：入队即持有）。 */
    void data(Page page) throws Exception;

    /**
     * 正常完成（所有数据页已先到达）。
     *
     * @param rowCount 分布式写路径本分区写走的行数（经 FINISHED metadata 传输；SELECT / 扫描恒 0）。
     */
    void finished(long rowCount) throws Exception;

    /** 执行失败（Worker fail-fast 显式回传）。 */
    void failed(String cause) throws Exception;

    /**
     * 心跳：任务仍在推进但无结果回传（阻塞式聚合 / 溢写 / 排序的慢查询）。默认 no-op ——
     * 只有需要防 Master 结果看门狗误杀的片段 sink（{@code FragmentSink}）才路由到回传队列。
     */
    default void progress() throws Exception {
    }

    /**
     * 结果流结束（正常半关闭 / 异常中断）却<b>从未</b>收到 {@link #finished}/{@link #failed}：
     * Worker 侧任务被中断（取消）、被池判死、或终止事件在链路上丢失。实现应把该任务转成失败投递，
     * 让算子当场 fail-fast —— 旧实现静默 {@code unregister}，算子只能等结果看门狗 10 分钟
     * （线上「已收 4/6 个完成、挂起 600069ms」）。取消路径由 {@link ResultRouter#markCancelled}
     * 标记，此类流不会走到这里。
     */
    default void streamEnded(String reason) throws Exception {
    }
}
