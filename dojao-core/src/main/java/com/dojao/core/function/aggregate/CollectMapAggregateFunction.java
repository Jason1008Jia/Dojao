package com.dojao.core.function.aggregate;

import com.dojao.spi.function.AggregateFunction;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * COLLECT_MAP / MAP_AGG —— 把分组内的 {@code (key, value)} 对收集成 Arrow {@code map<key, value>}。
 *
 * <ul>
 *   <li><b>默认（覆盖）</b>：相同 key 后到的 value 覆盖先到的，条目按首次出现顺序排列；</li>
 *   <li><b>追加</b>（{@code collect_map(k, v, TRUE)}）：相同 key 的 value 追加进列表，
 *       结果类型 {@code map<key, list<value>>}；</li>
 *   <li>key / value 先经 {@link CollectSupport#normalize} 规范化（Text→String、整数→Long、
 *       浮点→Double、byte[]→按内容比较的 ByteBuffer），保证内存与溢写回读路径的「同 key」判定一致；</li>
 *   <li>key 或 value 为 NULL 的行跳过；空组 / 全 NULL 组 → {@code NULL}；
 *       并行执行下「后者覆盖」的「后者」由归并顺序决定（分布式聚合的固有语义）。</li>
 * </ul>
 *
 * @author jiaxin
 * @date 2026/11/20
 */
public class CollectMapAggregateFunction implements AggregateFunction<CollectMapAggregateFunction.MapState> {

    /** 覆盖模式（默认）：相同 key 后者覆盖。 */
    public static final CollectMapAggregateFunction OVERWRITE = new CollectMapAggregateFunction(false);
    /** 追加模式：相同 key 的值追加为列表。 */
    public static final CollectMapAggregateFunction APPEND = new CollectMapAggregateFunction(true);

    private final boolean append;

    public CollectMapAggregateFunction(boolean append) {
        this.append = append;
    }

    public boolean appendValues() {
        return append;
    }

    /**
     * 状态：{@link LinkedHashMap} 保留 key 首次出现顺序；{@code elements} 是值的<b>总个数</b>
     * （追加模式下 = 各列表元素数之和），供内存估算 O(1) 读取，不必遍历 map。
     */
    public static final class MapState {
        final LinkedHashMap<Object, Object> map = new LinkedHashMap<>();
        long elements;

        public int mapSize() { return map.size(); }
        public long elementCount() { return elements; }

        /** 只读视图（溢写侧遍历 / 测试用），键按首次出现顺序。 */
        public java.util.Map<Object, Object> entries() { return map; }
    }

    @Override
    public MapState createState() {
        return new MapState();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void accumulate(MapState state, Object[] inputs) {
        if (inputs == null || inputs.length < 2 || inputs[0] == null || inputs[1] == null) return;
        Object key = CollectSupport.normalize(inputs[0]);
        Object value = CollectSupport.normalize(inputs[1]);
        if (key == null || value == null) return;
        if (append) {
            ArrayList<Object> list = (ArrayList<Object>) state.map.computeIfAbsent(
                    key, k -> new ArrayList<>(2));
            list.add(value);
            state.elements++;
        } else {
            state.map.put(key, value);
            state.elements = state.map.size();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void merge(MapState target, MapState source) {
        if (source == null || source.map.isEmpty()) return;
        if (append) {
            for (Map.Entry<Object, Object> e : source.map.entrySet()) {
                List<Object> src = (List<Object>) e.getValue();
                ArrayList<Object> dst = (ArrayList<Object>) target.map.computeIfAbsent(
                        e.getKey(), k -> new ArrayList<>(Math.max(2, src.size())));
                dst.addAll(src);
                target.elements += src.size();
            }
        } else {
            target.map.putAll(source.map);
            target.elements = target.map.size();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object evaluate(MapState state) {
        if (state == null || state.map.isEmpty()) return null;
        LinkedHashMap<Object, Object> out = new LinkedHashMap<>(Math.max(4, state.map.size() * 2));
        for (Map.Entry<Object, Object> e : state.map.entrySet()) {
            if (append) {
                List<Object> vals = (List<Object>) e.getValue();
                ArrayList<Object> exported = new ArrayList<>(vals.size());
                for (Object v : vals) exported.add(CollectSupport.export(v));
                out.put(CollectSupport.export(e.getKey()), exported);
            } else {
                out.put(CollectSupport.export(e.getKey()), CollectSupport.export(e.getValue()));
            }
        }
        return out;
    }

    @Override
    public void reset(MapState state) {
        state.map.clear();
        state.elements = 0;
    }

    @Override
    public String functionName() {
        return "collect_map";
    }

    @Override
    public String display() {
        return "收集映射：把分组内 (key, value) 对收集为 Map COLLECT_MAP(k, v)（MAP_AGG 等价；"
                + "第三参数 TRUE 时同 key 值追加为数组）";
    }
}
