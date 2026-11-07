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
public record PhysicalPlan(PhysicalNode root, String explainText, Runnable cleanup) {

    /** 无附加资源的计划（测试 / 手工构造）。 */
    public PhysicalPlan(PhysicalNode root, String explainText) {
        this(root, explainText, () -> { });
    }

    /**
     * 释放规划期申请、需随查询生命周期回收的资源（当前为物化 CTE 的落盘目录与 wire 载荷）。
     *
     * <p>幂等，由执行入口在算子链 {@code close()} 之后调用；规划失败 / 取消路径也必须调用，
     * 否则物化文件只能等 JVM 退出时由 {@code SpillDirs} 兜底清理。</p>
     */
    public void close() {
        try {
            cleanup.run();
        } catch (Throwable ignored) {
            // 资源回收失败不应掩盖查询本身的结果 / 异常
        }
    }

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
