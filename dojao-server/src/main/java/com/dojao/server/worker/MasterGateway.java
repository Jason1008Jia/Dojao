package com.dojao.server.worker;

import com.dojao.core.rpc.control.QueryTelemetryMsg;
import com.dojao.core.runtime.worker.ResourceSummary;
import com.dojao.core.runtime.worker.WorkerNode;
import com.dojao.core.runtime.worker.WorkerState;

import java.util.List;

/**
 * Worker → Master 的远程通信抽象 —— 心跳 / 注册 / 注销 / 查询遥测上抛。
 *
 * <p>生产环境由真实网络传输（Arrow Flight）实现；测试 / 多节点模拟用内存实现。
 * WorkerManager 只依赖此接口，不感知传输细节。
 *
 * @author jiaxin
 * @version 1.1
 * @date 2026/8/11
 */
public interface MasterGateway {

    /** Worker 启动时注册，返回 Master 是否接受。 */
    boolean register(WorkerNode info);

    /**
     * 定时心跳：证明 Worker 存活 + 汇报当前状态与资源摘要。
     * 返回 false 表示 Master 已不认这个节点（超时 / 被剔除），Worker 应标记 FAILED。
     */
    boolean heartbeat(WorkerNode info, WorkerState state, ResourceSummary summary);

    /** Worker 优雅停止时注销，让 Master 立即摘除（不再等心跳超时）。 */
    boolean unregister(WorkerNode info);

    /**
     * 查询遥测上抛（非终端片段行数 / 字节 + 溢写，见 {@link WorkerQueryTelemetry}；best-effort）。
     *
     * @return Master 判定的「已不认」jobId 列表（请求 {@code running_query_ids} 的子集，= 可回收的
     *         残留片段）。默认实现不支持该通道（内存 / 测试网关）→ 空表（僵尸回收静默关闭）。
     */
    default List<String> telemetry(QueryTelemetryMsg msg) {
        return List.of();
    }
}
