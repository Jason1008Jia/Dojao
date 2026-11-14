package com.dojao.core.function.aggregate;

import com.dojao.spi.function.AggregateFunction;

import java.util.ArrayList;

/**
 * COLLECT_LIST —— 把分组内非 NULL 值按出现顺序收集成数组（Arrow {@code list<输入类型>}）。
 *
 * <ul>
 *   <li>状态 = {@link ArrayList}，逐行追加；合并 = 拼接（顺序 = 先到先得，跨分区/溢写合并后
 *       组内顺序不保证与原表物理顺序一致，这是分布式聚合的固有语义）；</li>
 *   <li>NULL 值跳过（Spark 语义）；空组 / 全 NULL 组 → {@code NULL}（不是空数组）；</li>
 *   <li>结果列是嵌套类型：只能作为最终投影 / CTAS 写出，不能再参与分组、排序、JOIN、窗口或表达式求值。</li>
 * </ul>
 *
 * @author jiaxin
 * @date 2026/11/14
 */
public class CollectListAggregateFunction implements AggregateFunction<ArrayList<Object>> {

    public static final CollectListAggregateFunction INSTANCE = new CollectListAggregateFunction();

    @Override
    public ArrayList<Object> createState() {
        return new ArrayList<>();
    }

    @Override
    public void accumulate(ArrayList<Object> state, Object[] inputs) {
        if (inputs == null || inputs.length == 0 || inputs[0] == null) return;
        state.add(CollectSupport.normalize(inputs[0]));
    }

    @Override
    public void merge(ArrayList<Object> target, ArrayList<Object> source) {
        if (source == null || source.isEmpty()) return;
        target.addAll(source);
    }

    @Override
    public Object evaluate(ArrayList<Object> state) {
        if (state == null || state.isEmpty()) return null;
        ArrayList<Object> out = new ArrayList<>(state.size());
        for (Object v : state) out.add(CollectSupport.export(v));
        return out;
    }

    @Override
    public void reset(ArrayList<Object> state) {
        state.clear();
    }

    @Override
    public String functionName() {
        return "collect_list";
    }

    @Override
    public String display() {
        return "收集列表：把分组内非 NULL 值按出现顺序收集为数组 COLLECT_LIST(col)（ARRAY_AGG 等价）";
    }
}
