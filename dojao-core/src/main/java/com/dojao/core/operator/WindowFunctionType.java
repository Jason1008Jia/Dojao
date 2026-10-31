package com.dojao.core.operator;

/**
 * 窗口函数类型枚举。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/18
 */
public enum WindowFunctionType {

    /** 分区内行号，从 1 开始递增，相同 ORDER BY 值的行顺序不确定 */
    ROW_NUMBER,

    /** 排名，相同值排名相同，下一排名跳过（如 1,1,3,4） */
    RANK,

    /** 密集排名，相同值排名相同，下一排名连续（如 1,1,2,3） */
    DENSE_RANK,

    /** 分区内运行求和（默认 frame: UNBOUNDED PRECEDING ~ CURRENT ROW） */
    SUM,

    /** 分区内运行平均（默认 frame: UNBOUNDED PRECEDING ~ CURRENT ROW） */
    AVG,
    /** 分区内运行统计  */
    COUNT,


    //MAX 统计分组内最大值
    MAX,

    //MIN 统计分组内最小值
    MIN,

    /** 滞后 N 行取值（默认 offset=1），首 N 行返回 defaultValue 或 NULL */
    LAG,

    /** 前导 N 行取值（默认 offset=1），末 N 行返回 defaultValue 或 NULL */
    LEAD,

    /** 用户自定义聚合函数（通过 {@link com.dojao.spi.function.AggregateFunction} 接口注册），内建 SUM/AVG/COUNT/MIN/MAX 仍走原生快速路径 */
    AGGREGATE
}
