package com.dojao.ui.monitor;

import java.util.List;
import java.util.Map;

/**
 * 资源时间序列（按节点 id 分组）—— {@code GET /api/cluster/history} 的响应体。
 *
 * <p>{@code series} 的 key 为节点 id（Worker 的 workerId，Master 自身为 {@code "master"}），
 * value 为该节点按时间升序的采样点列表。前端每个 key 画一条折线。
 *
 * @param series 节点 id → 采样点序列（时间升序）
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/29
 */
public record MonitorHistory(Map<String, List<MonitorSample>> series) {
}
