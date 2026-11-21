package com.dojao.core.function.aggregate;

import com.dojao.spi.function.AggregateFunction;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * COLLECT_ORDERED_LIST —— 按指定排序列把分组内的值收集成有序数组（Arrow {@code list<值类型>}）。
 *
 * <ul>
 *   <li>签名：{@code collect_ordered_list(value, order_key[, flag])}；第三参数缺省 = 升序，
 *       字面量 {@code FALSE}（或 {@code DESC}）→ 降序；</li>
 *   <li>状态 = 两条并行列表（值 + 排序列），累加时按行追加；{@code evaluate} 时<b>稳定排序</b>
 *       —— 排序键相同的值保持出现顺序；</li>
 *   <li>NULL 值跳过（与 collect_list 一致）；排序列为 NULL 视为最小（升序最前 / 降序最后）；</li>
 *   <li>空组 / 全 NULL 组 → {@code NULL}。溢写 / 分布式合并后，同排序键的相对顺序与原表物理顺序
 *       不保证一致（collect_list 的固有语义）。</li>
 * </ul>
 *
 * @author jiaxin
 * @date 2026/11/20
 */
public class CollectOrderedListAggregateFunction
        implements AggregateFunction<CollectOrderedListAggregateFunction.OrderedState> {

    public static final CollectOrderedListAggregateFunction ASCENDING =
            new CollectOrderedListAggregateFunction(false);
    public static final CollectOrderedListAggregateFunction DESCENDING =
            new CollectOrderedListAggregateFunction(true);

    private final boolean descending;

    public CollectOrderedListAggregateFunction(boolean descending) {
        this.descending = descending;
    }

    public boolean descending() {
        return descending;
    }

    /** 状态：{@code values[i]} 与 {@code orderKeys[i]} 按下标对齐，evaluate 时按排序键重排。 */
    public static final class OrderedState {
        final ArrayList<Object> values = new ArrayList<>();
        final ArrayList<Object> orderKeys = new ArrayList<>();

        public int size() { return values.size(); }
        public List<Object> values() { return values; }
        public List<Object> orderKeys() { return orderKeys; }
    }

    @Override
    public OrderedState createState() {
        return new OrderedState();
    }

    @Override
    public void accumulate(OrderedState state, Object[] inputs) {
        if (inputs == null || inputs.length == 0 || inputs[0] == null) return;
        Object value = CollectSupport.normalize(inputs[0]);
        if (value == null) return;
        // 排序列为 NULL 的行保留（按最小键参与排序），不能丢
        Object orderKey = inputs.length > 1 ? CollectSupport.normalize(inputs[1]) : null;
        state.values.add(value);
        state.orderKeys.add(orderKey);
    }

    @Override
    public void merge(OrderedState target, OrderedState source) {
        if (source == null || source.values.isEmpty()) return;
        target.values.addAll(source.values);
        target.orderKeys.addAll(source.orderKeys);
    }

    @Override
    public Object evaluate(OrderedState state) {
        if (state == null || state.values.isEmpty()) return null;
        int n = state.values.size();
        int[] idx = new int[n];
        for (int i = 0; i < n; i++) idx[i] = i;
        sortIndices(idx, state.orderKeys, descending);
        ArrayList<Object> out = new ArrayList<>(n);
        for (int i : idx) out.add(CollectSupport.export(state.values.get(i)));
        return out;
    }

    @Override
    public void reset(OrderedState state) {
        state.values.clear();
        state.orderKeys.clear();
    }

    @Override
    public String functionName() {
        return "collect_ordered_list";
    }

    @Override
    public String display() {
        return "有序收集：按第二参数列排序后把第一参数列收集为数组 "
                + "COLLECT_ORDERED_LIST(value, order_key[, FALSE])（第三参数 FALSE / DESC 降序，缺省升序）";
    }

    /**
     * 从整组序列化的值重建状态（仅供整根溢写调试 / 探针）：序列化顺序即已排好的顺序，
     * 故排序键全部置 NULL —— 稳定排序会原样保留该顺序。
     */
    public static OrderedState fromValues(Collection<?> values) {
        OrderedState state = new OrderedState();
        if (values == null) return state;
        for (Object v : values) {
            Object n = CollectSupport.normalize(v);
            if (n == null) continue;
            state.values.add(n);
            state.orderKeys.add(null);
        }
        return state;
    }

    /**
     * 排序键全序：NULL 最小、数值按数值（含 BigDecimal 精确比较）、同类型可比对象用 compareTo、
     * 其余退回 toString（日期时间 / 布尔等）。
     */
    static int compareOrderKeys(Object a, Object b) {
        if (a == null && b == null) return 0;
        if (a == null) return -1;
        if (b == null) return 1;
        if (a instanceof Number na && b instanceof Number nb) {
            if (a instanceof BigDecimal || b instanceof BigDecimal) {
                return toBigDecimal(na).compareTo(toBigDecimal(nb));
            }
            if (a instanceof Double || a instanceof Float
                    || b instanceof Double || b instanceof Float) {
                return Double.compare(na.doubleValue(), nb.doubleValue());
            }
            return Long.compare(na.longValue(), nb.longValue());
        }
        if (a.getClass() == b.getClass() && a instanceof Comparable<?>) {
            @SuppressWarnings("unchecked")
            Comparable<Object> c = (Comparable<Object>) a;
            return c.compareTo(b);
        }
        return a.toString().compareTo(b.toString());
    }

    private static BigDecimal toBigDecimal(Number n) {
        if (n instanceof BigDecimal bd) return bd;
        if (n instanceof Double || n instanceof Float) return BigDecimal.valueOf(n.doubleValue());
        return BigDecimal.valueOf(n.longValue());
    }

    /** 稳定归并排序（同键保持出现顺序），避免 Integer 装箱的额外堆开销。 */
    private static void sortIndices(int[] a, List<Object> keys, boolean descending) {
        int[] tmp = new int[a.length];
        mergeSort(a, tmp, 0, a.length, keys, descending);
    }

    private static void mergeSort(int[] a, int[] tmp, int lo, int hi,
                                  List<Object> keys, boolean descending) {
        if (hi - lo < 2) return;
        int mid = (lo + hi) >>> 1;
        mergeSort(a, tmp, lo, mid, keys, descending);
        mergeSort(a, tmp, mid, hi, keys, descending);
        int i = lo, j = mid, k = lo;
        while (i < mid && j < hi) {
            int c = compareOrderKeys(keys.get(a[i]), keys.get(a[j]));
            if (descending) c = -c;
            if (c <= 0) tmp[k++] = a[i++];
            else tmp[k++] = a[j++];
        }
        while (i < mid) tmp[k++] = a[i++];
        while (j < hi) tmp[k++] = a[j++];
        System.arraycopy(tmp, lo, a, lo, hi - lo);
    }
}
