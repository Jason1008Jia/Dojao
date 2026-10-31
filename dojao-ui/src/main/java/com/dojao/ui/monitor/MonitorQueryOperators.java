package com.dojao.ui.monitor;

import java.util.List;
import java.util.Map;

/**
 * 单查询的算子执行明细 —— 查询概要 + 全部算子（含每个算子的分布式任务进度）。
 *
 * <p>{@code found=false} 表示 queryId 不存在（{@link QueryMonitor#snapshot} 对未知 id 返回
 * state=UNKNOWN 的占位快照），HTTP 层据此返回 404；此时 {@code operators} 为空。</p>
 *
 * <p>{@code operators} 下标即执行 DAG 的节点下标（第 0 个为根/最下游算子），每个算子自带
 * {@code children}（上游算子下标）与 {@code tasks}（各 Worker 任务进度）；
 * {@code operatorFailures} 为「算子 → 失败原因」映射。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/21
 */
public record MonitorQueryOperators(
        String queryId,
        String sql,
        String state,
        long wallMs,
        long totalRowsOut,
        String failureMessage,
        boolean found,
        int operatorCount,
        List<MonitorOperator> operators,
        Map<String, String> operatorFailures) {
}
