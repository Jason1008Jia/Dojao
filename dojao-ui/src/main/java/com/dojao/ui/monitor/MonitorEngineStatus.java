package com.dojao.ui.monitor;

import java.util.List;
import java.util.Map;

/**
 * 引擎状态总览 —— 系统层面与查询层面的合并视图（{@code GET /api/engine/status}）。
 *
 * <p>{@code cluster} 是系统层面：Master + 健康 Worker 的 CPU / 内存 / 堆 / 磁盘 / 线程 /
 * 受管直接内存 / 运行任务 / 槽位；{@code queryStates} 是查询层面：按状态计数
 * （固定包含 RUNNING / SUBMITTED / FINISHED / FAILED / CANCELED 五个键，值为 0 也保留，
 * 另有 UNKNOWN / REJECTED 等时追加）。</p>
 *
 * <p>{@code totalQueries} = 全部查询条数；{@code matchedQueries} = 按 {@code state} 过滤后的条数；
 * {@code queries} = 过滤后再按 {@code limit} 截断的列表（按提交时间倒序）。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/21
 */
public record MonitorEngineStatus(
        long sampledAt,
        int totalQueries,
        int matchedQueries,
        Map<String, Integer> queryStates,
        MonitorCluster cluster,
        List<MonitorJob> queries) {
}
