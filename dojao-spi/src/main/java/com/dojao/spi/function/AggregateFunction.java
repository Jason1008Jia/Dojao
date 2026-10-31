package com.dojao.spi.function;

/**
 * 聚合函数接口 —— GROUP BY 聚合操作的基础抽象。
 *
 * <h3>核心语义</h3>
 * <p>每个 key 维护一个状态 S，逐行 {@link #accumulate} 累加，
 * 可跨分区 {@link #merge} 合并，最终 {@link #evaluate} 求值。</p>
 *
 * <h3>生命周期（框架调度）</h3>
 * <pre>
 *   GROUP BY: createState → [accumulate × N] → evaluate
 * </pre>
 *
 * @param <S> 累加状态类型（COUNT = long[], SUM = double[], AVG = double[2]…）
 * @author jiaxin
 * @since 2.0
 */
public interface AggregateFunction<S> extends Function {

    /** 创建空累加状态 */
    S createState();

    /** 累加一行数据到状态中（原地修改 state） */
    void accumulate(S state, Object[] inputs);

    /** 将 source 的状态合并到 target（target 原地修改） */
    void merge(S target, S source);

    /** 从状态中求最终值（只读） */
    Object evaluate(S state);

    /** 重置状态（分区边界调用），默认 no-op */
    default void reset(S state) {}
}
