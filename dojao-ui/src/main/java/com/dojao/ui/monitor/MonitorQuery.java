package com.dojao.ui.monitor;

import java.util.List;
import java.util.Map;

/**
 * 单查询完整执行快照 —— 监控页点开一个任务后渲染「执行 DAG」的全部数据。
 *
 * <p>{@code operators} 下标即 DAG 节点下标（第 0 个为根/最下游算子），每个算子
 * 自带 {@code children}（上游输入算子下标）与 {@code tasks}（分布式任务进度）。
 * 顶层 {@code state} 为查询状态（SUBMITTED / RUNNING / FINISHED / FAILED / CANCELED）。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/6
 */
public record MonitorQuery(
        String queryId,
        String sql,
        String state,
        long wallMs,
        long totalRowsOut,
        String failureMessage,
        List<MonitorOperator> operators,
        Map<String, String> operatorFailures) {
}
