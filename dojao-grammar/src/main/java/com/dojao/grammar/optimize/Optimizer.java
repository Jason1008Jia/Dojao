package com.dojao.grammar.optimize;

import com.dojao.grammar.analyzer.plan.LogicalPlan;
import java.util.List;

/**
 * Dojao SQL 优化器：对 LogicalPlan 按顺序应用优化规则。
 *
 * <p>默认规则顺序：
 * <ol>
 *   <li>{@link PredicatePushdown 谓词下推} —— 把 WHERE 条件下推到最底层的表扫描</li>
 *   <li>{@link ColumnPruning 列裁剪} —— 只保留查询实际需要的列</li>
 * </ol>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/5
 */
public final class Optimizer {

    private final List<OptimizerRule> rules;

    public Optimizer() {
        this.rules = List.of(
            new PredicatePushdown(),
            new ColumnPruning()
        );
    }

    public Optimizer(List<OptimizerRule> rules) {
        this.rules = List.copyOf(rules);
    }

    /** 规则轮次上限（列裁剪只做加法性收窄 → 单调收敛，通常 2 轮即稳定）。 */
    private static final int MAX_ROUNDS = 5;

    /**
     * 应用所有已注册的规则，返回优化后的计划。
     *
     * <p>有界不动点：重复执行规则列表，直到某一轮不再改变计划（{@link LogicalPlan} 是
     * {@code record}，结构相等即收敛）或达到 {@link #MAX_ROUNDS} 上限。这让
     * 「先裁 CTE 再裁主查询」的级联需求能传播到最内层扫描。
     */
    public LogicalPlan optimize(LogicalPlan plan) {
        LogicalPlan current = plan;
        for (int round = 0; round < MAX_ROUNDS; round++) {
            LogicalPlan next = current;
            for (OptimizerRule rule : rules) {
                next = rule.apply(next);
            }
            if (next.equals(current)) {
                return next;
            }
            current = next;
        }
        return current;
    }
}
