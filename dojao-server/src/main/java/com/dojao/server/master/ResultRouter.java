package com.dojao.server.master;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Master 侧结果路由表 —— taskId → 结果汇，连接 Master→Worker 派发端（{@link NetworkWorkerGateway}
 * 派发时注册）与 Master Flight 生产者的 doExchange "result/{taskId}" 接收端（读流时查表路由）。
 *
 * <p>两个组件共享同一实例（经 {@code MasterServer} 装配）：派发端注册 sink，接收端按
 * worker 推来的 taskId 查表并投递。流结束后（FINISHED / FAILED / 取消）由接收端摘除。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/29
 */
public final class ResultRouter {

    private final ConcurrentHashMap<String, ResultSink> sinks = new ConcurrentHashMap<>();
    /** 已被 Master 主动取消的任务：其结果流「无终止事件地结束」属预期，不得报成失败。 */
    private final Set<String> cancelled = ConcurrentHashMap.newKeySet();

    /** 派发时注册该任务的汇（Worker 的结果将投递到这里）。 */
    public void register(String taskId, ResultSink sink) {
        sinks.put(taskId, sink);
    }

    /** 读流时按 taskId 取汇。 */
    public ResultSink get(String taskId) {
        return sinks.get(taskId);
    }

    /**
     * 标记该任务已被主动取消（{@code WorkerGateway.cancelTask} 调用）。只对仍在路由表中的任务生效
     * —— 否则迟到的取消会在表里留下永不清理的标记。
     */
    public void markCancelled(String taskId) {
        if (sinks.containsKey(taskId)) {
            cancelled.add(taskId);
        }
    }

    /** 该任务是否已被主动取消（结果流无终止事件结束属预期）。 */
    public boolean isCancelled(String taskId) {
        return cancelled.contains(taskId);
    }

    /** 流结束 / 任务放弃时摘除（防 taskId 复用残留）。 */
    public void unregister(String taskId) {
        sinks.remove(taskId);
        cancelled.remove(taskId);
    }

    public int size() {
        return sinks.size();
    }
}
