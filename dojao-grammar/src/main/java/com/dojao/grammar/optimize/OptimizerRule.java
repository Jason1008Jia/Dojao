package com.dojao.grammar.optimize;

import com.dojao.grammar.analyzer.plan.LogicalPlan;

/**
 * 优化规则：对 LogicalPlan 应用一次重写，返回优化后的计划。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/5
 */
public sealed interface OptimizerRule
    permits PredicatePushdown, ColumnPruning {

    /** Apply this rule to a LogicalPlan, returning the optimized plan. */
    LogicalPlan apply(LogicalPlan plan);
}
