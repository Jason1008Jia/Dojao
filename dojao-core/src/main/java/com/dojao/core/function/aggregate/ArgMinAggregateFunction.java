package com.dojao.core.function.aggregate;

import com.dojao.spi.function.AggregateFunction;

/**
 * {@code ARG_MIN(value, order_key)} —— 组内按 {@code order_key} 取<b>最小</b>那一行的 {@code value}。
 *
 * <p>语义与 {@link ArgMaxAggregateFunction} 对称：{@code order_key} 为 NULL 的行跳过、
 * {@code value} 可为 NULL、并列最小值取先出现的那行、空组 → NULL。</p>
 *
 * @author jiaxin
 * @date 2026/11/25
 */
public final class ArgMinAggregateFunction implements AggregateFunction<Object[]> {

    public static final ArgMinAggregateFunction INSTANCE = new ArgMinAggregateFunction();

    @Override
    public Object[] createState() {
        return new Object[]{null, null};
    }

    @Override
    public void accumulate(Object[] state, Object[] inputs) {
        if (inputs == null || inputs.length < 2) return;
        Object key = MinMaxSupport.normalize(inputs[1]);
        if (key == null) return;
        if (state[1] == null || MinMaxSupport.compare(key, state[1]) < 0) {
            state[0] = inputs[0];
            state[1] = key;
        }
    }

    @Override
    public void merge(Object[] target, Object[] source) {
        if (source == null || source.length < 2 || source[1] == null) return;
        if (target[1] == null || MinMaxSupport.compare(source[1], target[1]) < 0) {
            target[0] = source[0];
            target[1] = source[1];
        }
    }

    @Override
    public Object evaluate(Object[] state) {
        return state[0];
    }

    @Override
    public void reset(Object[] state) {
        state[0] = null;
        state[1] = null;
    }

    @Override
    public String functionName() {
        return "arg_min";
    }

    @Override
    public String display() {
        return "按序取最小：arg_min(value, order_key) —— 组内 order_key 最小的那一行的 value";
    }
}
