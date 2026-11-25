package com.dojao.core.function.aggregate;

import com.dojao.spi.function.AggregateFunction;

/**
 * {@code ARG_MAX(value, order_key)} —— 组内按 {@code order_key} 取<b>最大</b>那一行的 {@code value}。
 *
 * <p>语义与 Hive {@code max_by} / Presto {@code max_by} / ClickHouse {@code argMax} 一致：</p>
 * <ul>
 *   <li>{@code order_key} 为 NULL 的行<b>跳过</b>（无法比较大小）；</li>
 *   <li>{@code value} 允许为 NULL —— 胜出行的值就是 NULL，结果即 NULL；</li>
 *   <li>并列最大值时取<b>先出现</b>的那行（与 {@code collect_ordered_list} 的稳定排序同口径）；</li>
 *   <li>空组 → NULL。</li>
 * </ul>
 *
 * <p>状态 = {@code Object[]{value, orderKey}}：{@code orderKey == null} 表示尚无胜出行。
 * 比较走 {@link MinMaxSupport} 的规范化口径（数值 / 日期时间 / 字符串 / 布尔 / Decimal 均可）。</p>
 *
 * @author jiaxin
 * @date 2026/11/25
 */
public final class ArgMaxAggregateFunction implements AggregateFunction<Object[]> {

    public static final ArgMaxAggregateFunction INSTANCE = new ArgMaxAggregateFunction();

    @Override
    public Object[] createState() {
        return new Object[]{null, null};
    }

    @Override
    public void accumulate(Object[] state, Object[] inputs) {
        if (inputs == null || inputs.length < 2) return;
        Object key = MinMaxSupport.normalize(inputs[1]);
        if (key == null) return;                       // 排序列 NULL → 该行不参与
        if (state[1] == null || MinMaxSupport.compare(key, state[1]) > 0) {
            state[0] = inputs[0];
            state[1] = key;
        }
    }

    @Override
    public void merge(Object[] target, Object[] source) {
        if (source == null || source.length < 2 || source[1] == null) return;
        if (target[1] == null || MinMaxSupport.compare(source[1], target[1]) > 0) {
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
        return "arg_max";
    }

    @Override
    public String display() {
        return "按序取最大：arg_max(value, order_key) —— 组内 order_key 最大的那一行的 value";
    }
}
