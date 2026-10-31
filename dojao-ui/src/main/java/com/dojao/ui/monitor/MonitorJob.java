package com.dojao.ui.monitor;

import java.util.List;

/**
 * 任务列表行 —— 监控页「任务列表」一条查询的可观测摘要。
 *
 * <p>查询完成/失败后仍保留（最近 N 条）。{@code state} 为上层查询状态字符串
 * （SUBMITTED / RUNNING / FINISHED / FAILED / CANCELED），时间可由 queryId 的
 * {@code yyyyMMddHHmmss} 前缀还原。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/6
 */
public record MonitorJob(String queryId, String sql, String state,
                         long wallMs, long totalRowsOut) {
}
