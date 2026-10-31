package com.dojao.core.planner;

import com.dojao.core.operator.Operator;

import java.util.List;

/**
 * 物理执行计划 —— 由 {@link Planner} 生成，承载算子树根节点与可读的 EXPLAIN 文本。
 *
 * <p>执行入口：拿到 {@link #operator()} 后依次调用 {@code open()} / {@code next()} / {@code close()}。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/2
 */
public record PhysicalPlan(PhysicalNode root, String explainText) {

    /**
     * 物理计划根节点（即最顶层的算子）。
     */
    @Override
    public PhysicalNode root() {
        return root;
    }

    /**
     * 根算子 —— 调用其 {@code next()} 逐批拉取结果。
     */
    public Operator operator() {
        return root.operator();
    }

    /**
     * 结果列名（根算子输出的列，与 Arrow schema 顺序一致）。
     */
    public List<String> outputColumns() {
        return root.outputColumns();
    }

    /**
     * EXPLAIN 文本（PlanTextFormatter 风格的物理计划树）。
     */
    @Override
    public String explainText() {
        return explainText;
    }
}
