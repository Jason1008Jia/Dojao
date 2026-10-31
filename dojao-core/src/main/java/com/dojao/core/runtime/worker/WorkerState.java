package com.dojao.core.runtime.worker;

/**
 * Worker 生命周期状态 —— Worker 侧自行维护，并通过心跳汇报给 Master。
 *
 * <p>INITIALIZING 启动未注册 → REGISTERED 注册成功 → IDLE 空闲 / BUSY 有任务运行（互斥切换）→
 * STOPPED 优雅停止（终态）；注册或心跳失败 → FAILED。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/11
 */
public enum WorkerState {
    INITIALIZING,
    REGISTERED,
    IDLE,
    BUSY,
    FAILED,
    STOPPED
}
