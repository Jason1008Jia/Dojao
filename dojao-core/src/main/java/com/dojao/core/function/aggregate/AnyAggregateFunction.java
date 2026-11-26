package com.dojao.core.function.aggregate;

import com.dojao.spi.function.AggregateFunction;

/**
 * {@code ANY(value)} / {@code ANY_VALUE(value)} —— 取组内<b>任意一个非 NULL</b> 值。
 *
 * <p>语义对齐 Hive/Spark 的 {@code any_value}、Presto 的 {@code arbitrary}：</p>
 * <ul>
 *   <li>实现上取<b>第一个遇到的非 NULL</b> 值（结果确定、可复现，不会随执行顺序漂移）；</li>
 *   <li>整组全是 NULL / 空组 → NULL；</li>
 *   <li>输出类型 = 输入列类型（与 MIN/MAX 同型，不做数值提升）。</li>
 * </ul>
 *
 * <p>典型用途：已知组内某列取值唯一或「取哪个都行」时（如按 id 分组取冗余描述列），
 * 避免为了拿一列而写 {@code MAX(..)} 触发不必要的比较。</p>
 *
 * @author jiaxin
 * @date 2026/11/25
 */
public final class AnyAggregateFunction implements AggregateFunction<Object[]> {

    public static final AnyAggregateFunction INSTANCE = new AnyAggregateFunction();

    @Override
    public Object[] createState() {
        return new Object[]{null};
    }

    @Override
    public void accumulate(Object[] state, Object[] inputs) {
        if (state[0] != null) return;                    // 已经拿到值：后续行不再看
        if (inputs == null || inputs.length == 0) return;
        if (inputs[0] != null) state[0] = inputs[0];
    }

    @Override
    public void merge(Object[] target, Object[] source) {
        if (target[0] != null) return;
        if (source != null && source.length > 0 && source[0] != null) {
            target[0] = source[0];
        }
    }

    @Override
    public Object evaluate(Object[] state) {
        return state[0];
    }

    @Override
    public void reset(Object[] state) {
        state[0] = null;
    }

    @Override
    public String functionName() {
        return "any";
    }

    @Override
    public String display() {
        return "任意值：ANY(col) —— 组内第一个非 NULL 值（全 NULL / 空组 → NULL）";
    }
}
