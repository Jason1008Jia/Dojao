package com.dojao.core.planner;

import com.dojao.core.operator.Operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * 物理计划节点 —— 封装一个物理算子及其输出列。
 *
 * <p>输出列（{@link #outputColumns()}）是按物理算子输出顺序排列的列名清单，
 * Planner 用它把逻辑列引用（含别名限定符）解析到下游算子可直接消费的裸列名，
 * 并为最终的 {@code ProjectOperator} 提供投影列清单。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/2
 */
public final class PhysicalNode {

    /** 估算行数未知时的哨兵（与 {@code JoinCardinality.UNKNOWN} 同值）。 */
    public static final long UNKNOWN_ROWS = -1L;

    private final String name;                 // 算子在计划树中的显示名
    private final Operator operator;
    private final List<String> outputColumns;  // 算子输出的列名（有序，与 Arrow schema 一致）
    private final List<PhysicalNode> children;
    private final long estimatedRows;          // 规划期估算输出行数；UNKNOWN_ROWS = 未知

    public PhysicalNode(String name, Operator operator,
                        List<String> outputColumns, List<PhysicalNode> children) {
        this(name, operator, outputColumns, children, UNKNOWN_ROWS);
    }

    public PhysicalNode(String name, Operator operator,
                        List<String> outputColumns, List<PhysicalNode> children, long estimatedRows) {
        this.name = name;
        this.operator = operator;
        this.outputColumns = List.copyOf(outputColumns);
        this.children = children != null ? List.copyOf(children) : List.of();
        this.estimatedRows = estimatedRows;
    }

    public PhysicalNode(String name, Operator operator,
                        List<String> outputColumns, PhysicalNode... children) {
        this(name, operator, outputColumns,
                children != null ? Arrays.asList(children) : List.of());
    }

    public String name() { return name; }
    /** 算子类型名（用于优化器分支判断）。 */
    public String type() { return name.replaceAll("\\[.*", ""); }
    public Operator operator() { return operator; }
    public List<String> outputColumns() { return outputColumns; }
    public List<PhysicalNode> children() { return children; }

    /** 规划期估算输出行数；{@link #UNKNOWN_ROWS} = 无法估算（统计缺失 / 中间结果不可知）。 */
    public long estimatedRows() { return estimatedRows; }

    /** 复制本节点并替换估算行数（算子 / 子节点 / 列不变）。 */
    public PhysicalNode withEstimate(long rows) {
        return new PhysicalNode(name, operator, outputColumns, children, rows);
    }

    /** 输出列是否包含指定裸列名（大小写不敏感）。 */
    public boolean hasColumn(String columnName) {
        for (String c : outputColumns) {
            if (c.equalsIgnoreCase(columnName)) return true;
        }
        return false;
    }

    /** 便捷：把当前节点包装为叶子（无子节点）。 */
    public static PhysicalNode leaf(String name, Operator operator, List<String> outputColumns) {
        return new PhysicalNode(name, operator, outputColumns, List.<PhysicalNode>of());
    }
}
