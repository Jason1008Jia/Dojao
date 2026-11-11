package com.dojao.core.planner;

import com.dojao.core.operator.Operator;

import java.util.List;
import java.util.Set;

/**
 * 物理执行计划 —— 由 {@link Planner} 生成，承载算子树根节点与可读的 EXPLAIN 文本。
 *
 * <p>执行入口：拿到 {@link #operator()} 后依次调用 {@code open()} / {@code next()} / {@code close()}。
 *
 * <p>{@code auxiliaryOperatorIds} 是主计划树之外、执行期惰性规划的分布式算子 jobId（物化 CTE /
 * 物化派生体查询体），由 {@link Planner} 持有的<b>实时可变视图</b>提供：查询体在派发前才规划，
 * 登记随执行推进追加。查询归属判定（QueryManager）据此认可这些 jobId，避免僵尸片段回收误杀。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/2
 */
public record PhysicalPlan(PhysicalNode root, String explainText, Runnable cleanup,
                           Set<String> auxiliaryOperatorIds) {

    public PhysicalPlan {
        auxiliaryOperatorIds = auxiliaryOperatorIds == null ? Set.of() : auxiliaryOperatorIds;
    }

    /** 无附加资源的计划（测试 / 手工构造）。 */
    public PhysicalPlan(PhysicalNode root, String explainText) {
        this(root, explainText, () -> { });
    }

    public PhysicalPlan(PhysicalNode root, String explainText, Runnable cleanup) {
        this(root, explainText, cleanup, Set.of());
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
