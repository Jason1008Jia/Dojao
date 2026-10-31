package com.dojao.core.operator;

/**
 * 窗口函数规格 —— 描述一个窗口函数表达式：类型 + 输入列 + 输出列名 + 偏移量。
 *
 * <p>例如 SQL 中的 {@code ROW_NUMBER() OVER (...) AS rn} 对应：
 * {@code new WindowFunctionSpec(WindowFunctionType.ROW_NUMBER, null, "rn")}
 *
 * <p>{@code SUM(amount) OVER (...) AS total} 对应：
 * {@code new WindowFunctionSpec(WindowFunctionType.SUM, "amount", "total")}
 *
 * <p>{@code LAG(amount, 1, 0) OVER (...) AS prev_amount} 对应：
 * {@code new WindowFunctionSpec(WindowFunctionType.LAG, "amount", "prev_amount", 1, 0L)}
 *
 * <p>{@code SUM(amount) OVER (... ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS s} 对应：
 * {@code new WindowFunctionSpec(WindowFunctionType.SUM, "amount", "s").withFrame(WindowFrame.rows(2, 0))}
 *
 * @author jiaxin
 * @version 1.2
 * @date 2026/7/18
 */
public record WindowFunctionSpec(
        WindowFunctionType type,
        String inputColumn,   // ROW_NUMBER / RANK / DENSE_RANK / AGGREGATE 时为 null
        String outputColumn,
        int offset,           // LAG / LEAD 的偏移量，默认 1
        Object defaultValue,  // LAG / LEAD 越界时的默认值，null 表示 NULL
        String functionName,  // 聚合函数名，非 AGGREGATE 时为 null
        WindowFrame frame     // ROWS 帧；null = 运行帧（默认）
) {

    /** 偏移无关的窗口函数（SUM, AVG, COUNT, RANK, ROW_NUMBER 等），无帧。 */
    public WindowFunctionSpec(WindowFunctionType type, String inputColumn, String outputColumn) {
        this(type, inputColumn, outputColumn, 1, null, null, null);
    }

    /** 兼容构造：无帧（运行/默认帧）。 */
    public WindowFunctionSpec(WindowFunctionType type, String inputColumn, String outputColumn,
                              int offset, Object defaultValue, String functionName) {
        this(type, inputColumn, outputColumn, offset, defaultValue, functionName, null);
    }

    /** 返回带指定帧的副本（仅内置聚合 SUM/AVG/COUNT/MIN/MAX 有意义）。 */
    public WindowFunctionSpec withFrame(WindowFrame f) {
        return new WindowFunctionSpec(type, inputColumn, outputColumn, offset, defaultValue, functionName, f);
    }

    // ==================== 工厂方法 ====================

    public static WindowFunctionSpec rowNumber(String outputColumn) {
        return new WindowFunctionSpec(WindowFunctionType.ROW_NUMBER, null, outputColumn);
    }

    public static WindowFunctionSpec rank(String outputColumn) {
        return new WindowFunctionSpec(WindowFunctionType.RANK, null, outputColumn);
    }

    public static WindowFunctionSpec denseRank(String outputColumn) {
        return new WindowFunctionSpec(WindowFunctionType.DENSE_RANK, null, outputColumn);
    }

    public static WindowFunctionSpec sum(String inputColumn, String outputColumn) {
        return new WindowFunctionSpec(WindowFunctionType.SUM, inputColumn, outputColumn);
    }

    public static WindowFunctionSpec avg(String inputColumn, String outputColumn) {
        return new WindowFunctionSpec(WindowFunctionType.AVG, inputColumn, outputColumn);
    }

    public static WindowFunctionSpec count(String inputColumn, String outputColumn) {
        return new WindowFunctionSpec(WindowFunctionType.COUNT, inputColumn, outputColumn);
    }

    public static WindowFunctionSpec min(String inputColumn, String outputColumn) {
        return new WindowFunctionSpec(WindowFunctionType.MIN, inputColumn, outputColumn);
    }

    public static WindowFunctionSpec max(String inputColumn, String outputColumn) {
        return new WindowFunctionSpec(WindowFunctionType.MAX, inputColumn, outputColumn);
    }

    public static WindowFunctionSpec lag(String inputColumn, String outputColumn) {
        return new WindowFunctionSpec(WindowFunctionType.LAG, inputColumn, outputColumn, 1, null, null);
    }

    public static WindowFunctionSpec lag(String inputColumn, String outputColumn, int offset) {
        return new WindowFunctionSpec(WindowFunctionType.LAG, inputColumn, outputColumn, offset, null, null);
    }

    public static WindowFunctionSpec lag(String inputColumn, String outputColumn, int offset, Object defaultValue) {
        return new WindowFunctionSpec(WindowFunctionType.LAG, inputColumn, outputColumn, offset, defaultValue, null);
    }

    public static WindowFunctionSpec lead(String inputColumn, String outputColumn) {
        return new WindowFunctionSpec(WindowFunctionType.LEAD, inputColumn, outputColumn, 1, null, null);
    }

    public static WindowFunctionSpec lead(String inputColumn, String outputColumn, int offset) {
        return new WindowFunctionSpec(WindowFunctionType.LEAD, inputColumn, outputColumn, offset, null, null);
    }

    public static WindowFunctionSpec lead(String inputColumn, String outputColumn, int offset, Object defaultValue) {
        return new WindowFunctionSpec(WindowFunctionType.LEAD, inputColumn, outputColumn, offset, defaultValue, null);
    }

    // ==================== 聚合（作为窗口函数） ====================

    /** 注册用户自定义聚合函数为窗口函数 */
    public static WindowFunctionSpec aggregate(String functionName, String inputColumn, String outputColumn) {
        return new WindowFunctionSpec(WindowFunctionType.AGGREGATE, inputColumn, outputColumn, 1, null, functionName);
    }
}
