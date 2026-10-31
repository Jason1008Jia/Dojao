package com.dojao.core.function.aggregate;

import com.dojao.spi.function.AggregateFunction;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * AVG —— State = {@code Object[2]} = [sum, count]。
 * evaluate 时才做除法，保证溢写/merge 的正确性。
 *
 * <p>与 {@link SumAggregateFunction} 相同：DECIMAL 输入切换为 BigDecimal 精确累加
 * （避免 {@code avg(amount)} 带浮点尾差），纯数值列仍走 double 历史口径。</p>
 */
public class AvgAggregateFunction implements AggregateFunction<Object[]> {

    public static final AvgAggregateFunction INSTANCE = new AvgAggregateFunction();
    private static final int SUM = 0, CNT = 1;

    @Override public Object[] createState() { return new Object[]{0.0d, 0L}; }

    @Override public void accumulate(Object[] s, Object[] inputs) {
        if (inputs.length == 0) return;
        add(s, inputs[0], 1L);
    }

    @Override public void merge(Object[] target, Object[] source) {
        if (source.length < 2) return;
        add(target, source[SUM], toLong(source[CNT]));
    }

    /** sum += value；cnt += count。BigDecimal 参与时走精确十进制加法。 */
    private static void add(Object[] s, Object value, long count) {
        if (value instanceof BigDecimal bd) {
            s[SUM] = sumValue(s[SUM]).add(bd);
        } else if (value instanceof Number n) {
            Object cur = s[SUM];
            if (cur instanceof BigDecimal bd) s[SUM] = bd.add(BigDecimal.valueOf(n.doubleValue()));
            else s[SUM] = ((Number) cur).doubleValue() + n.doubleValue();
        } else if (value != null) {
            return;   // 非数值输入不计入（与旧行为一致）
        }
        s[CNT] = toLong(s[CNT]) + count;
    }

    private static BigDecimal sumValue(Object v) {
        if (v instanceof BigDecimal bd) return bd;
        return v instanceof Number n ? BigDecimal.valueOf(n.doubleValue()) : BigDecimal.ZERO;
    }

    private static long toLong(Object v) {
        return v instanceof Number n ? n.longValue() : 0L;
    }

    @Override public Object evaluate(Object[] s) {
        long cnt = toLong(s[CNT]);
        if (cnt == 0) return null;
        Object sum = s[SUM];
        if (sum instanceof BigDecimal bd) {
            // 与输入同 scale（DECIMAL 列通常是 2）→ 「精确到分」；除法至少 2 位小数
            int scale = Math.max(bd.scale(), 2);
            return bd.divide(BigDecimal.valueOf(cnt), scale, RoundingMode.HALF_UP);
        }
        return ((Number) sum).doubleValue() / cnt;
    }

    @Override public void reset(Object[] s) { s[SUM] = 0.0d; s[CNT] = 0L; }

    @Override public String functionName() { return "avg"; }

    @Override public String display() { return "平均值：数值列求和/计数 AVG(col)"; }
}
