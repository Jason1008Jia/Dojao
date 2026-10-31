package com.dojao.ui;

import java.util.List;

/**
 * DAG 数据结构（vis-network 格式）。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/5
 */
public record DagData(String jobLabel, List<Node> nodes, List<Edge> edges) {

    public record Node(String id, String label, String group) {}
    public record Edge(String from, String to) {}
}
