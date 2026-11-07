package com.dojao.core.operator;

/**
 * Phase 2 Filter → Sort → Output 配置：把外层 WHERE 窗口函数(列) op 值 的过滤条件下推到
 * {@link WindowSortOperator}，在 Phase 2 做分组级预过滤，避免全量排序+拷贝。
 *
 * <h3>适用条件</h3>
 * <p>仅支持<b>单调递增</b>的窗口函数：ROW_NUMBER / COUNT / RANK / DENSE_RANK。
 *   等值 = N 和区间 <= / < / >= / > N 全部支持。</p>
 *
 * <h3>不支持</h3>
 * SUM / AVG / MIN / MAX：非单调或 ORDER BY 下无法预判最终值，必须输出全组后过滤。
 *
 * @author jiaxin
 * @version 2.2
 * @date 2026/8/6
 */
public class WindowFilterConfig {

    public enum FilterOp { EQ, LE, LT, GE, GT }

    private final WindowFunctionType functionType;
    private final long targetValue;
    private final int aggregateColIndex;     // -1 for positional/rank; >=0 for MIN/MAX/SUM/AVG
    private final boolean noOrderBy;         // true=全量窗口(分区级聚合)，false=有排序(逐行运行值)
    private final FilterOp operator;

    // ---- 构造器 ----

    /** 位置型、等值过滤（向后兼容）。 */
    public WindowFilterConfig(int rowNumber) {
        this(WindowFunctionType.ROW_NUMBER, rowNumber, -1, true, FilterOp.EQ);
    }

    public WindowFilterConfig(WindowFunctionType functionType, long targetValue,
                              int aggregateColIndex, boolean noOrderBy, FilterOp operator) {
        this.functionType = functionType;
        this.targetValue = targetValue;
        this.aggregateColIndex = aggregateColIndex;
        this.noOrderBy = noOrderBy;
        this.operator = operator;
    }

    public WindowFilterConfig(WindowFunctionType functionType, long targetValue, FilterOp operator) {
        this(functionType, targetValue, -1, true, operator);
    }

    // ---- 查询 ----

    public boolean isActive() { return targetValue > 0; }
    public FilterOp operator() { return operator; }
    public boolean isRange() { return operator != FilterOp.EQ; }

    /** 等值过滤：只输出一行（= N），可启用 WindowFunction 值覆写。 */
    public boolean isSingleRow() { return operator == FilterOp.EQ; }

    /** 位置型过滤：直接按行号跳转取第 targetValue 行，不需要逐行计算。 */
    public boolean isPositional() {
        return functionType == WindowFunctionType.ROW_NUMBER
                || functionType == WindowFunctionType.COUNT;
    }

    /** 排名型：RANK / DENSE_RANK，需 ORDER BY 比对。 */
    public boolean isRank() {
        return functionType == WindowFunctionType.RANK
                || functionType == WindowFunctionType.DENSE_RANK;
    }

    /** 全量窗口分区级聚合（所有行同值）：MIN / MAX / SUM / AVG，无 ORDER BY。 */
    public boolean isAggregate() {
        return aggregateColIndex >= 0 && noOrderBy;
    }

    /** 有排序窗口逐行运行值（单调不降/不增）：MIN / MAX。 */
    public boolean isValueMonotonic() {
        return aggregateColIndex >= 0 && !noOrderBy
                && (functionType == WindowFunctionType.MIN || functionType == WindowFunctionType.MAX);
    }

    /** 有排序窗口 SUM：需 per-key 符号检查（全非负→不降，全非正→不升，混合→不过滤）。 */
    public boolean isSumWithOrderBy() {
        return aggregateColIndex >= 0 && !noOrderBy
                && functionType == WindowFunctionType.SUM;
    }

    public int aggregateColIndex() { return aggregateColIndex; }

    /** 全量窗口（无 ORDER BY / 分区级聚合值）标记 —— 线格式往返需原样携带。 */
    public boolean noOrderBy() { return noOrderBy; }

    /** 分组须达到的最少行数才可能满足条件（用于 Phase 1 预过滤）。 */
    public long minGroupSize() {
        // SUM: 单行大值即可满足任意操作符 → 至少 1 行
        if (functionType == WindowFunctionType.SUM) return 1;
        return switch (operator) {
            case EQ -> targetValue;
            case LE, LT -> 1;
            case GE -> targetValue;
            case GT -> targetValue + 1;
        };
    }

    public WindowFunctionType functionType() { return functionType; }
    public long targetValue() { return targetValue; }

    /** EXPLAIN 展示（如 {@code RANK EQ 1}），供用户确认提前过滤确实下推生效。 */
    public String describe() {
        return functionType + " " + operator + " " + targetValue;
    }

    /** 位置型 + 等值时的 0-indexed 输出位置。 */
    public int outputPosition() { return (int) targetValue - 1; }
}
