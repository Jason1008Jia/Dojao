package com.dojao.core.function.aggregate;

import com.dojao.spi.function.AggregateFunction;

import java.math.BigDecimal;

/**
 * SUM —— State = {@code Object[1]}（累计和）。
 *
 * <p>历史上状态是 {@code double[1]}，DECIMAL 列会被转成 double 累加 ⇒
 * {@code sum(amount)} 出现 {@code 157498.08000000002} 之类的浮点尾差。
 * 现在：一旦输入出现 {@link BigDecimal}（Arrow DECIMAL 列 / DECIMAL 表达式），状态切换为
 * BigDecimal 精确累加；纯整数/浮点列仍沿用 double（历史口径与性能不变）。</p>
 *
 * <p>★空集 / 全 NULL 语义：状态初值是 {@code null}（「还没见过任何非 NULL 值」），
 * {@link #evaluate} 原样返回 ⇒ SUM 在空集或全 NULL 上返回 NULL（SQL 标准；与
 * min/max/avg 一致）。旧实现初值 {@code 0.0d}，导致「该地区本周期没有销售」被报成
 * {@code 0 元}。{@code sum_int64}（count 合并等内部用途）不受影响。</p>
 */
public class SumAggregateFunction implements AggregateFunction<Object[]> {

    public static final SumAggregateFunction INSTANCE = new SumAggregateFunction();

    @Override public Object[] createState() { return new Object[]{null}; }

    @Override public void accumulate(Object[] s, Object[] inputs) {
        if (inputs.length == 0) return;
        add(s, inputs[0]);
    }

    @Override public void merge(Object[] target, Object[] source) {
        if (source.length == 0) return;
        add(target, source[0]);
    }

    /**
     * 把 value 并入 {@code s[0]}。
     *
     * <p>三种精确性口径：</p>
     * <ul>
     *   <li>BigDecimal（DECIMAL 列 / 表达式）→ 精确十进制累加；</li>
     *   <li><b>整数</b>（BIGINT/INT 列、{@code CAST(x AS BIGINT)} 派生列）→ 以 {@code Long} 精确累加，
     *       状态保持 Long ⇒ 输出 BIGINT（不回退 double，避免 {@code 8.076270371E9} 与 &gt;2^53 丢位）；</li>
     *   <li>浮点 → double（历史口径）。</li>
     * </ul>
     * 整数累加溢出（超过 Long.MAX_VALUE）时升级为 BigDecimal，结果至少不静默回绕。
     */
    private static void add(Object[] s, Object value) {
        if (value instanceof BigDecimal bd) {
            s[0] = sumValue(s[0]).add(bd);
        } else if (value instanceof Number n) {
            Object cur = s[0];
            if (cur instanceof BigDecimal bd) {
                s[0] = bd.add(toDecimal(n));
            } else if (isIntegral(n) && (cur == null || cur instanceof Long)) {
                long add = n.longValue();
                if (cur == null) {
                    s[0] = add;
                } else {
                    try {
                        s[0] = Math.addExact((Long) cur, add);
                    } catch (ArithmeticException overflow) {
                        s[0] = BigDecimal.valueOf((Long) cur).add(BigDecimal.valueOf(add));
                    }
                }
            } else if (cur == null) {
                s[0] = n.doubleValue();
            } else {
                s[0] = ((Number) cur).doubleValue() + n.doubleValue();
            }
        }
        // 非数值输入（字符串等）按旧行为忽略
    }

    /** 整数型 Number（Arrow 整型列 getObject 得到 Long/Integer/Short/Byte）。 */
    private static boolean isIntegral(Number n) {
        return n instanceof Long || n instanceof Integer || n instanceof Short || n instanceof Byte;
    }

    private static BigDecimal sumValue(Object v) {
        if (v instanceof BigDecimal bd) return bd;
        return v instanceof Number n ? toDecimal(n) : BigDecimal.ZERO;
    }

    /** Number → BigDecimal（整型不引入多余小数位；浮点按十进制字面值）。 */
    private static BigDecimal toDecimal(Number n) {
        if (n instanceof Long || n instanceof Integer || n instanceof Short || n instanceof Byte) {
            return BigDecimal.valueOf(n.longValue());
        }
        return BigDecimal.valueOf(n.doubleValue());
    }

    @Override public Object evaluate(Object[] s) { return s[0]; }

    @Override public void reset(Object[] s) { s[0] = null; }

    @Override public String functionName() { return "sum"; }

    @Override public String display() { return "求和：数值列累加 SUM(col)"; }
}
