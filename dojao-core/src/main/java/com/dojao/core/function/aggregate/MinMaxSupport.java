package com.dojao.core.function.aggregate;

import com.dojao.core.operator.ArrowComparators;
import org.apache.arrow.vector.util.Text;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

/**
 * MIN / MAX 的状态值与比较工具。
 *
 * <p><b>为什么需要它</b>：旧实现只接受 {@code Number}（{@code if (v instanceof Number)}），
 * 日期时间（{@code LocalDateTime}）、字符串（{@code Text}）、布尔等输入被直接忽略 ⇒
 * 状态永远是初值 ⇒ {@code MIN(order_time)} / {@code MAX(name)} 恒返回 NULL，即使列里全是非空值。</p>
 *
 * <p><b>取值规范化</b>：整数族（Byte/Short/Integer/Long）统一成 {@link Long}，浮点族统一成
 * {@link Double}，{@code Text} 转 {@link String}，其余（BigDecimal / LocalDateTime / Boolean /
 * byte[] / Duration / List…）原样保留。同一列的类型是一致的，因此状态里的值始终同类、可直接比较。</p>
 *
 * <p>输出列的类型不在这里决定 —— 由 {@code AggregationHashTable} 按<b>输入列的 Arrow 类型</b>
 * 解析（日期列输出日期，不再是 DOUBLE）。</p>
 */
public final class MinMaxSupport {

    private MinMaxSupport() {
    }

    /** 规范化一个原始取值（来自 {@code FieldVector.getObject}）以便比较与存储。 */
    public static Object normalize(Object v) {
        if (v == null) return null;
        if (v instanceof Byte || v instanceof Short || v instanceof Integer || v instanceof Long) {
            return ((Number) v).longValue();
        }
        if (v instanceof Float || v instanceof Double) {
            return ((Number) v).doubleValue();
        }
        if (v instanceof Text t) return t.toString();
        return v;   // BigDecimal / LocalDateTime / Boolean / byte[] / Duration / List … 原样保留
    }

    /**
     * 两个规范化后的值比较（负 = a 更小）。同列类型一致，因此这里覆盖的都是实际会出现的组合。
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static int compare(Object a, Object b) {
        if (a == b) return 0;
        if (a == null) return -1;
        if (b == null) return 1;
        if (a instanceof Long la && b instanceof Long lb) return Long.compare(la, lb);
        if (a instanceof BigDecimal || b instanceof BigDecimal) {
            return toBigDecimal(a).compareTo(toBigDecimal(b));
        }
        if (a instanceof Number na && b instanceof Number nb) {
            return Double.compare(na.doubleValue(), nb.doubleValue());
        }
        if (a instanceof String sa && b instanceof String sb) return sa.compareTo(sb);
        if (a instanceof byte[] ba && b instanceof byte[] bb) return Arrays.compareUnsigned(ba, bb);
        if (a instanceof List<?> la && b instanceof List<?> lb) return ArrowComparators.compareLists(la, lb);
        if (a instanceof Comparable ca) {
            try {
                return ca.compareTo(b);
            } catch (ClassCastException ignored) {
                // 不同类（理论上同列不会出现）→ 退回文本序，至少不崩
            }
        }
        return a.toString().compareTo(b.toString());
    }

    private static BigDecimal toBigDecimal(Object v) {
        if (v instanceof BigDecimal b) return b;
        if (v instanceof Long l) return BigDecimal.valueOf(l);
        return new BigDecimal(v.toString());
    }
}
