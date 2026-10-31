package com.dojao.core.function.aggregate;

import com.dojao.spi.function.AggregateFunction;

/** COUNT —— State = long[1]。 */
public class CountAggregateFunction implements AggregateFunction<long[]> {

    public static final CountAggregateFunction INSTANCE = new CountAggregateFunction();

    @Override public long[] createState() { return new long[]{0L}; }

    @Override public void accumulate(long[] s, Object[] inputs) {
        // SQL 语义：COUNT(col) 只统计非 NULL；COUNT(*) 以空参数数组调用，统计全部行。
        if (inputs.length == 0 || inputs[0] != null) s[0]++;
    }

    @Override public void merge(long[] target, long[] source) { target[0] += source[0]; }

    @Override public Object evaluate(long[] s) { return s[0]; }

    @Override public void reset(long[] s) { s[0] = 0L; }

    @Override public String functionName() { return "count"; }

    @Override public String display() { return "计数：COUNT(*)/COUNT(col)"; }
}
