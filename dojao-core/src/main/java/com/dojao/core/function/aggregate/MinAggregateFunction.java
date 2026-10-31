package com.dojao.core.function.aggregate;

import com.dojao.spi.function.AggregateFunction;

/**
 * MIN —— State = Object[1]（当前最小值；{@code null} = 尚未见到非 NULL 值）。
 *
 * <p>★旧实现只接受 {@code Number}，日期时间 / 字符串 / 布尔等列的值全被忽略 ⇒ 状态保持初值
 * ⇒ {@code MIN(order_time)} 恒为 NULL（即使列里没有 NULL）。现按 {@link MinMaxSupport} 的规范化
 * 口径比较任意可比较类型；输出列类型由聚合表按输入列 Arrow 类型决定。</p>
 */
public class MinAggregateFunction implements AggregateFunction<Object[]> {

    public static final MinAggregateFunction INSTANCE = new MinAggregateFunction();

    @Override public Object[] createState() { return new Object[]{null}; }

    @Override public void accumulate(Object[] s, Object[] inputs) {
        Object v = MinMaxSupport.normalize(inputs == null || inputs.length == 0 ? null : inputs[0]);
        if (v != null && (s[0] == null || MinMaxSupport.compare(v, s[0]) < 0)) {
            s[0] = v;
        }
    }

    @Override public void merge(Object[] target, Object[] source) {
        if (source == null || source.length == 0 || source[0] == null) return;
        if (target[0] == null || MinMaxSupport.compare(source[0], target[0]) < 0) {
            target[0] = source[0];
        }
    }

    @Override public Object evaluate(Object[] s) { return s[0]; }

    @Override public void reset(Object[] s) { s[0] = null; }

    @Override public String functionName() { return "min"; }

    @Override public String display() { return "最小值：任意可比较列（数值 / 日期时间 / 字符串）的最小值 MIN(col)"; }
}
