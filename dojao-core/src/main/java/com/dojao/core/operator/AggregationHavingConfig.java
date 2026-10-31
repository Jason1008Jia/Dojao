package com.dojao.core.operator;

/**
 * HAVING 谓词下推配置：将 {@code HAVING agg_col op value} 条件下推到
 * {@link HashAggregationOperator}，在 Phase 2 输出组结果时直接跳过不满足条件的组。
 *
 * <h3>支持</h3>
 * COUNT / SUM / AVG / MIN / MAX，操作符 = / <= / < / >= / >。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/6
 */
public class AggregationHavingConfig {

    /** 聚合输出列在 output root 中的索引（keyColumns.size() + specIndex）。 */
    private final int columnIndex;
    private final WindowFilterConfig.FilterOp op;
    private final long targetValue;

    public AggregationHavingConfig(int columnIndex,
                                   WindowFilterConfig.FilterOp op, long targetValue) {
        this.columnIndex = columnIndex;
        this.op = op;
        this.targetValue = targetValue;
    }

    public int columnIndex() { return columnIndex; }

    /** 从 Arrow 向量读取行值并比对。 */
    public boolean accept(org.apache.arrow.vector.FieldVector vec, int row) {
        if (vec.isNull(row)) return false;
        Object obj = vec.getObject(row);
        long value = obj instanceof Number n ? n.longValue() : 0;
        return match(value);
    }

    /** 直接用原始值比对（溢写路径，已从聚合函数 state 求值）。 */
    public boolean accept(Object obj, long fallback) {
        long value = obj instanceof Number n ? n.longValue() : fallback;
        return match(value);
    }

    private boolean match(long value) {
        return switch (op) {
            case EQ -> value == targetValue;
            case LE -> value <= targetValue;
            case LT -> value < targetValue;
            case GE -> value >= targetValue;
            case GT -> value > targetValue;
        };
    }
}
