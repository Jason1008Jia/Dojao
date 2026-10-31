package com.dojao.spi.function;

/**
 * 窗口函数接口 —— OVER window 操作的基础抽象。
 *
 * <h3>与 {@link AggregateFunction} 的本质区别</h3>
 * <p>AggregateFunction 对每个 key 只输出一行（GROUP BY 归约）。</p>
 * <p>WindowFunction 对每个 key 内的每一行都输出一行（OVER 窗口）。</p>
 *
 * <h3>内置窗口函数分类</h3>
 * <ul>
 *   <li><b>排序型</b>：ROW_NUMBER, RANK, DENSE_RANK, NTILE</li>
 *   <li><b>位移型</b>：LAG, LEAD, FIRST_VALUE, LAST_VALUE</li>
 *   <li><b>聚合型</b>：SUM OVER, AVG OVER, COUNT OVER（可委托 {@link AggregateFunction} 实现）</li>
 * </ul>
 *
 * <p>TODO：接口方法在窗口函数重构时细化（当前 WindowFunctionOperator 为硬编码 switch-case）。</p>
 *
 * @author jiaxin
 * @since 2.0
 */
public interface WindowFunction extends Function {
    // TODO: define window-specific methods when WindowFunctionOperator is refactored
    // e.g.: evaluate(PartitionRows rows, FrameSpec frame, int currentRow) → Object
}
