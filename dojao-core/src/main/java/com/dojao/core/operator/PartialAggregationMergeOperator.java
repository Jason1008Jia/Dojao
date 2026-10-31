package com.dojao.core.operator;

import com.dojao.common.data.Page;
import com.dojao.common.memory.NodeMemory;
import com.dojao.core.config.ResourceConfig;
import com.dojao.core.function.aggregate.MinMaxSupport;
import com.dojao.spi.function.AggregateFunction;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.ArrayList;
import java.util.List;

/**
 * 分布式全局聚合的 <b>partial → final</b> 合并算子（Master 侧，单行输出）。
 *
 * <p><b>解决什么</b>：内层已经是分布式聚合（结果全部留在 Worker），外层再做一次<b>无 GROUP BY</b>
 * 的聚合（如 {@code SELECT count(*) FROM (分组聚合) t}）时，旧路径会把内层的<b>全部结果行</b>
 * 回传 Master，再由单机 {@link HashAggregationOperator} 逐行扫一遍 —— 千万级内层结果就是
 * 「Aggregate 完成后卡在 505 行/秒」。现在：
 *
 * <pre>
 *   Worker 每分区：内层聚合输出 → CaseProject（外层 CASE 派生列）→ 二级 HashAggregation（全局）
 *                  ⇒ 每分区只回传 <b>1 行 partial</b>（count / sum / min / max）
 *   Master：本算子把 N 行 partial 用各聚合函数的 {@code merge} 合成 1 行最终结果
 * </pre>
 *
 * <p><b>为什么不用 HashAggregationOperator 重写聚合规格</b>：那样要把 partial 的 count 当 sum 再算，
 * 输出类型会从 BIGINT 漂成 DOUBLE（Decimal 的 sum 还会被二次放宽精度）—— 结果类型与单机路径不一致。
 * 本算子直接复用原聚合函数的状态/合并/求值（{@code count=long[]}、{@code sum/min/max=Object[]}），
 * 输出列类型取<b>第一页 partial 的实际 schema</b>（= Worker 侧结果列类型），逐位保真。</p>
 *
 * <p><b>支持的聚合</b>：count / sum / min / max（partial 形态可合并）。avg 需要 和+计数、
 * count(distinct)/collect/自定义 UDAF 的 partial 不可合并 —— Planner 不会为它们下沉。</p>
 *
 * @author jiaxin
 * @date 2026/10/20
 */
public final class PartialAggregationMergeOperator extends AbstractMeasurableOperator implements Explainable {

    private Operator upstream;
    private final List<AggregationSpec> aggregations;

    /** 各聚合的累加状态（与 {@link #aggregations} 同序）。 */
    private final List<Object> states = new ArrayList<>();

    /** 输出列（取第一页 partial 的 schema；零页时才用静态口径兜底）。 */
    private List<Field> outputFields;
    private BufferAllocator allocator;
    private boolean emitted;

    public PartialAggregationMergeOperator(Operator upstream, List<AggregationSpec> aggregations) {
        super("PartialAggregationMerge", "PartialAggregationMerge", 1);
        this.upstream = upstream;
        this.aggregations = List.copyOf(aggregations);
        for (AggregationSpec spec : this.aggregations) {
            if (spec.isCollect()) {
                throw new IllegalArgumentException("collect 型聚合不支持 partial 合并: " + spec.outputColumn());
            }
            states.add(spec.aggregate().createState());
        }
    }

    /** 列裁剪插入 Project 后更换上游（与其它单上游算子同一套 fix 钩子）。 */
    public void setUpstream(Operator upstream) {
        this.upstream = upstream;
    }

    /** 合并的聚合规格（供列裁剪推算子节点需求 / EXPLAIN）。 */
    public List<AggregationSpec> getAggregations() {
        return aggregations;
    }

    // ==================== 生命周期 ====================

    @Override
    protected void doOpen() {
        upstream.open();
        allocator = NodeMemory.shared().newOperatorAllocator(getStats().getOperatorType(),
                ResourceConfig.queryDirectLimit());
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    protected Page doNext() throws Exception {
        if (emitted) return Page.eof();
        while (true) {
            Page input = upstream.next();
            if (input.isEOF()) break;
            try {
                recordInput(input);
                mergePage(input);
            } finally {
                releaseInput(input);
                input.close();
            }
        }
        emitted = true;
        if (outputFields == null) {
            // 上游一页都没吐（理论上不可达：全局聚合每个分区都会补 1 行 partial）→ 静态口径兜底
            outputFields = fallbackFields();
        }
        return buildResultPage();
    }

    @Override
    protected void doClose() throws Exception {
        upstream.close();
        if (allocator != null) {
            allocator.close();
            allocator = null;
        }
    }

    // ==================== 合并 ====================

    private void mergePage(Page input) {
        if (input.getRowCount() <= 0 || input.getRoot() == null) return;
        if (outputFields == null) {
            outputFields = input.getSchema().getFields();
        }
        int rows = input.getRowCount();
        for (int r = 0; r < rows; r++) {
            for (int i = 0; i < aggregations.size(); i++) {
                AggregationSpec spec = aggregations.get(i);
                FieldVector vec = input.getVector(spec.outputColumn());
                Object raw = vec.isNull(r) ? null : vec.getObject(r);
                mergeOne(states.get(i), spec.aggregate(), raw);
            }
        }
    }

    /** 把一行 partial 值并入状态（各聚合函数自己的 merge 语义）。 */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void mergeOne(Object state, AggregateFunction fn, Object raw) {
        String name = fn.functionName();
        Object partial = switch (name) {
            // COUNT partial = 该分区计数（BIGINT），合并即相加
            case "count" -> raw == null ? null : new long[]{((Number) raw).longValue()};
            // SUM partial = 该分区小计（DOUBLE / DECIMAL），用函数自身的精确加法合并
            case "sum" -> raw == null ? null : new Object[]{raw};
            // MIN/MAX partial = 该分区极值（与输入同型），规范化后按函数口径比较
            case "min", "max" -> {
                Object v = MinMaxSupport.normalize(raw);
                yield v == null ? null : new Object[]{v};
            }
            default -> throw new IllegalStateException(
                    "不支持 partial 合并的聚合函数: " + name + "（仅 count / sum / min / max）");
        };
        if (partial != null) {
            ((AggregateFunction) fn).merge(state, partial);
        }
    }

    // ==================== 输出 ====================

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Page buildResultPage() {
        List<FieldVector> vectors = new ArrayList<>(outputFields.size());
        for (int i = 0; i < outputFields.size(); i++) {
            Field field = outputFields.get(i);
            FieldVector vec = field.createVector(allocator);
            vec.allocateNew();
            Object result = i < aggregations.size()
                    ? ((AggregateFunction) aggregations.get(i).aggregate()).evaluate(states.get(i))
                    : null;
            // 复用溢写/聚合共用的写值工具（同包：全类型覆盖，Decimal 按向量 scale 归一）
            AbstractSpillableOperator.setVectorValue(vec, 0, result);
            vec.setValueCount(1);
            vectors.add(vec);
        }
        VectorSchemaRoot root = new VectorSchemaRoot(outputFields, vectors, 1);
        return Page.fromRoot(root);
    }

    /** 零页兜底：按聚合输出列的静态 Arrow 类型合成（与 Worker 侧口径一致的部分）。 */
    private List<Field> fallbackFields() {
        List<Field> fields = new ArrayList<>(aggregations.size());
        for (AggregationSpec spec : aggregations) {
            fields.add(new Field(spec.outputColumn(),
                    FieldType.nullable(spec.outputArrowType()), null));
        }
        return fields;
    }

    // ==================== 可观测 ====================

    @Override
    public List<String> explainLines() {
        List<String> out = new ArrayList<>();
        out.add("merge partials from " + aggregations.size() + " aggregates into 1 row: "
                + aggregations.stream().map(a -> a.aggregate().functionName()
                        + "(" + (a.inputColumn() == null ? "*" : a.inputColumn()) + ") AS "
                        + a.outputColumn()).toList());
        return out;
    }
}
