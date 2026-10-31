package com.dojao.core.operator;

/**
 * Join 类型枚举 —— 与 {@code LogicalPlan.JoinType} 对应，
 * 物理算子层独立定义，避免耦合语法分析层。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/17
 */
public enum JoinType {
    INNER,
    LEFT,
    RIGHT,
    FULL,
    CROSS
}
