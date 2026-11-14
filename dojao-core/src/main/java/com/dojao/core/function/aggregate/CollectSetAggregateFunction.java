package com.dojao.core.function.aggregate;

import com.dojao.spi.function.AggregateFunction;

import java.util.ArrayList;
import java.util.LinkedHashSet;

/**
 * COLLECT_SET —— 把分组内非 NULL 值去重后收集成数组（Arrow {@code list<输入类型>}）。
 *
 * <ul>
 *   <li>状态 = {@link LinkedHashSet}，保留首次出现顺序；合并 = 集合并；</li>
 *   <li>值先经 {@link CollectSupport#normalize} 规范化（Text→String、整数→Long、浮点→Double、
 *       byte[]→按内容比较的 ByteBuffer），保证内存与溢写回读路径的“同值”判定一致；</li>
 *   <li>NULL 值跳过；空组 / 全 NULL 组 → {@code NULL}；</li>
 *   <li>结果列是嵌套类型：只能作为最终投影 / CTAS 写出。</li>
 * </ul>
 *
 * @author jiaxin
 * @date 2026/11/14
 */
public class CollectSetAggregateFunction implements AggregateFunction<LinkedHashSet<Object>> {

    public static final CollectSetAggregateFunction INSTANCE = new CollectSetAggregateFunction();

    @Override
    public LinkedHashSet<Object> createState() {
        return new LinkedHashSet<>();
    }

    @Override
    public void accumulate(LinkedHashSet<Object> state, Object[] inputs) {
        if (inputs == null || inputs.length == 0 || inputs[0] == null) return;
        state.add(CollectSupport.normalize(inputs[0]));
    }

    @Override
    public void merge(LinkedHashSet<Object> target, LinkedHashSet<Object> source) {
        if (source == null || source.isEmpty()) return;
        target.addAll(source);
    }

    @Override
    public Object evaluate(LinkedHashSet<Object> state) {
        if (state == null || state.isEmpty()) return null;
        ArrayList<Object> out = new ArrayList<>(state.size());
        for (Object v : state) out.add(CollectSupport.export(v));
        return out;
    }

    @Override
    public void reset(LinkedHashSet<Object> state) {
        state.clear();
    }

    @Override
    public String functionName() {
        return "collect_set";
    }

    @Override
    public String display() {
        return "收集去重集合：把分组内非 NULL 值去重后收集为数组 COLLECT_SET(col)";
    }
}
