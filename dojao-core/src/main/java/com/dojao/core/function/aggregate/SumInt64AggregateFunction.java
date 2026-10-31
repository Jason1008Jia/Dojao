package com.dojao.core.function.aggregate;

import com.dojao.spi.function.AggregateFunction;

/**
 * SUM_INT64 —— 内部聚合：按 BIGINT 精度累加（State = long[1]）。
 *
 * <p>只供规划期二级 COUNT(DISTINCT) 改写的外层合并使用（把内层每子桶的
 * {@code count} / {@code count_distinct} 相加），<b>不注册进 SQL 函数目录</b>。
 * 与普通 {@code sum} 的区别是输出类型恒为 BIGINT —— 普通 sum 会把 BIGINT 漂成
 * DOUBLE（结果渲染成 {@code "2.0"}、CTAS 类型错）。NULL 输入跳过。
 */
public class SumInt64AggregateFunction implements AggregateFunction<long[]> {

    public static final SumInt64AggregateFunction INSTANCE = new SumInt64AggregateFunction();

    @Override public long[] createState() { return new long[]{0L}; }

    @Override public void accumulate(long[] s, Object[] inputs) {
        if (inputs.length > 0 && inputs[0] != null) s[0] += toLong(inputs[0]);
    }

    @Override public void merge(long[] target, long[] source) { target[0] += source[0]; }

    @Override public Object evaluate(long[] s) { return s[0]; }

    @Override public void reset(long[] s) { s[0] = 0L; }

    @Override public String functionName() { return "sum_int64"; }

    @Override public String display() { return "内部 BIGINT 求和（二级 COUNT DISTINCT 合并）"; }

    private static long toLong(Object v) {
        if (v instanceof Number n) return n.longValue();
        String s = v.toString().trim();
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            return (long) Double.parseDouble(s);
        }
    }
}
