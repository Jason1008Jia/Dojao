package com.dojao.core.runtime.master;

import com.dojao.core.runtime.worker.ResourceSummary;
import com.dojao.core.runtime.worker.WorkerNode;
import com.dojao.core.runtime.worker.WorkerState;

/**
 * Master 侧对单个 Worker 的注册信息 —— 身份 + 最近心跳携带的状态与资源摘要。
 *
 * @param info               Worker 身份（workerId / host / port / 版本）
 * @param state              Worker 最近心跳上报的状态（IDLE / BUSY / ...）
 * @param lastResource       Worker 最近一次心跳携带的资源摘要（未收到过为 null）
 * @param lastHeartbeatNanos 最近一次心跳时刻（nanoTime；注册即记一次）
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/11
 */
public record WorkerRegistration(WorkerNode info, WorkerState state,
                                 ResourceSummary lastResource, long lastHeartbeatNanos) {

    /** 心跳是否在 timeoutMs 内（据此判定节点存活，超时剔除）。 */
    public boolean isFresh(long timeoutMs) {
        return lastHeartbeatNanos > 0L && System.nanoTime() - lastHeartbeatNanos < timeoutMs * 1_000_000L;
    }
}
