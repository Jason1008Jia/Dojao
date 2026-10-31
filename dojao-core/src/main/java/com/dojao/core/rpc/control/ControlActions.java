package com.dojao.core.rpc.control;

/**
 * 真分布式控制面 action 常量（Master ↔ Worker，Arrow Flight 控制面）。
 *
 * <p>与 {@code control.proto} 的载荷一一对应：
 * <ul>
 *   <li>Worker → Master（doAction）：{@link #REGISTER} / {@link #HEARTBEAT} / {@link #UNREGISTER}
 *       —— 节点注册、心跳保活、优雅注销；</li>
 *   <li>Master → Worker（doAction）：{@link #DISPATCH_SCAN} / {@link #DISPATCH_FRAGMENT} /
 *       {@link #CANCEL_TASK} —— 分区扫描 / 执行片段派发与取消；</li>
 *   <li>Worker → Master（doExchange）：descriptor path = {@code ["result", taskId]}
 *       （{@link #RESULT_PREFIX}）—— 结果数据页 + 终止事件回传。</li>
 * </ul>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/29
 */
public final class ControlActions {

    /** Worker → Master：节点注册（请求 RegisterRequest，响应 RegisterResponse 含集群节点表）。 */
    public static final String REGISTER = "register";
    /** Worker → Master：定时心跳（请求 HeartbeatRequest，响应 HeartbeatResponse）。 */
    public static final String HEARTBEAT = "heartbeat";
    /** Worker → Master：优雅注销（请求 UnregisterRequest）。 */
    public static final String UNREGISTER = "unregister";
    /** Worker → Master：查询遥测 ~1s 上抛（请求 QueryTelemetryMsg —— 非终端片段累计行数，
     *  供 Master 侧 QueryManager 归并填 Scan/Join 框真实行数；响应 QueryTelemetryResponse）。 */
    public static final String QUERY_TELEMETRY = "query_telemetry";

    /** Master → Worker：派发分区扫描（请求 DispatchScanRequest）。 */
    public static final String DISPATCH_SCAN = "dispatch_scan";
    /** Master → Worker：派发执行片段（请求 DispatchFragmentRequest）。 */
    public static final String DISPATCH_FRAGMENT = "dispatch_fragment";
    /** Master → Worker：取消任务（请求 CancelTaskRequest）。 */
    public static final String CANCEL_TASK = "cancel_task";

    /** 结果回传 doExchange 的 path 前缀（完整 path = ["result", taskId]）。 */
    public static final String RESULT_PREFIX = "result";

    private ControlActions() {
    }
}
