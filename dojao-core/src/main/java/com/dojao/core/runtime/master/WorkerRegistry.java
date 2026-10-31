package com.dojao.core.runtime.master;

import com.dojao.core.runtime.worker.ResourceSummary;
import com.dojao.core.runtime.worker.WorkerNode;
import com.dojao.core.runtime.worker.WorkerState;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Worker 全局注册表（Master 单例）—— 收集各节点心跳，维护每个 Worker 的最新状态与资源摘要。
 *
 * <p>Master 依据本注册表做任务分配：{@link #healthy} 剔除心跳超时 / 已停止节点，
 * 再交给 {@link WorkerSelector} 按节点资源情况选出落点。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/11
 */
public final class WorkerRegistry {

    private static final WorkerRegistry INSTANCE = new WorkerRegistry();

    /** workerId → 注册信息。 */
    private final ConcurrentHashMap<String, WorkerRegistration> workers = new ConcurrentHashMap<>();

    /** 各节点资源时间序列（监控页折线图用）；Master 自身以 "master" 为 nodeId 也记在这里。 */
    private final ResourceHistory history = new ResourceHistory();

    private WorkerRegistry() {}

    public static WorkerRegistry get() {
        return INSTANCE;
    }

    /** 资源时间序列（同一实例，可被监控页读取；Master 自身采样也写这里）。 */
    public ResourceHistory history() {
        return history;
    }

    /** Worker 注册（或重启后覆盖）。 */
    public void register(WorkerNode info) {
        workers.put(info.workerId(),
                new WorkerRegistration(info, WorkerState.REGISTERED, null, System.nanoTime()));
    }

    /** 收到心跳：更新状态、资源摘要与最近心跳时刻，并追加一个历史采样点。 */
    public void updateHeartbeat(String workerId, WorkerState state, ResourceSummary summary) {
        workers.computeIfPresent(workerId, (id, reg) ->
                new WorkerRegistration(reg.info(), state, summary, System.nanoTime()));
        history.append(workerId, summary);
    }

    /** Worker 优雅停止时注销（连带清掉它的历史序列）。 */
    public void unregister(String workerId) {
        workers.remove(workerId);
        history.remove(workerId);
    }

    // ==================== 查询 ====================

    public WorkerRegistration get(String workerId) {
        return workers.get(workerId);
    }

    public List<WorkerRegistration> all() {
        return List.copyOf(workers.values());
    }

    /** 存活节点：心跳在 timeoutMs 内且非 STOPPED / FAILED。 */
    public List<WorkerRegistration> healthy(long timeoutMs) {
        return workers.values().stream()
                .filter(r -> r.isFresh(timeoutMs))
                .filter(r -> r.state() != WorkerState.STOPPED && r.state() != WorkerState.FAILED)
                .toList();
    }

    public int size() {
        return workers.size();
    }
}
