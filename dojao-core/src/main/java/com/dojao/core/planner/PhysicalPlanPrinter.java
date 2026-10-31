package com.dojao.core.planner;

import com.dojao.core.operator.Explainable;

import java.util.List;
import java.util.Map;

/**
 * 物理计划打印器 —— 把 {@link PhysicalNode} 树渲染成文本（EXPLAIN 展示用）。
 *
 * <p>每个节点一行：显示名 + 输出列 + 估算/实际行数（有则显示）。算子若实现
 * {@link Explainable}，其内部结构（扫描表 / 逐层 join 算法与键 / 聚合与窗口规格）逐条
 * 渲染在该节点行下方，使分布式算子不再是「一个看不懂的叶子」。</p>
 *
 * <p>两种入口：</p>
 * <ul>
 *   <li>{@link #print(PhysicalNode)}：普通 EXPLAIN —— 只显示规划期估算行数；</li>
 *   <li>{@link #print(PhysicalNode, Map)}：EXPLAIN ANALYZE —— 额外显示每个节点的实际输出行数
 *       （键为节点对象本身，按对象身份匹配）。</li>
 * </ul>
 *
 * @author jiaxin
 * @version 2.0
 * @date 2026/8/2
 */
public final class PhysicalPlanPrinter {

    private PhysicalPlanPrinter() {}

    /** 渲染整棵计划树（只含估算行数）。 */
    public static String print(PhysicalNode root) {
        return print(root, null);
    }

    /**
     * 渲染整棵计划树，并回填每个节点的实际输出行数。
     *
     * @param actualRows 节点对象 → 实际输出行数（EXPLAIN ANALYZE 执行后采集）；
     *                   {@code null} / 未命中 = 该节点不显示实际行数
     */
    public static String print(PhysicalNode root, Map<PhysicalNode, Long> actualRows) {
        StringBuilder sb = new StringBuilder();
        render(root, "", true, sb, actualRows);
        return sb.toString();
    }

    private static void render(PhysicalNode node, String prefix, boolean isLast, StringBuilder sb,
                               Map<PhysicalNode, Long> actualRows) {
        sb.append(prefix);
        sb.append(isLast ? "└── " : "├── ");
        sb.append(describe(node, actualRows)).append('\n');

        // 算子内部结构（分布式算子展开为「扫描 / 逐层 join / 聚合 / 窗口」明细）
        String childPrefix = prefix + (isLast ? "    " : "│   ");
        if (node.operator() instanceof Explainable explainable) {
            for (String line : explainable.explainLines()) {
                sb.append(childPrefix).append("- ").append(line).append('\n');
            }
        }

        List<PhysicalNode> children = node.children();
        for (int i = 0; i < children.size(); i++) {
            render(children.get(i), childPrefix, i == children.size() - 1, sb, actualRows);
        }
    }

    private static String describe(PhysicalNode node, Map<PhysicalNode, Long> actualRows) {
        String desc = node.name();
        if (!node.outputColumns().isEmpty()) {
            desc += " [columns=" + String.join(", ", node.outputColumns()) + "]";
        }
        long est = node.estimatedRows();
        Long actual = actualRows == null ? null : actualRows.get(node);
        if (est >= 0L || actual != null) {
            desc += " (";
            if (est >= 0L) {
                desc += "est=" + formatRows(est);
            }
            if (actual != null) {
                if (est >= 0L) desc += ", ";
                desc += "actual=" + formatRows(actual);
            }
            desc += ")";
        }
        return desc;
    }

    /** 行数千分位（EXPLAIN 面向人阅读；估算值本就非精确值）。 */
    static String formatRows(long rows) {
        return String.format("%,d", rows);
    }
}
