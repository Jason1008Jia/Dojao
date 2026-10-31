package com.dojao.core.operator;

/**
 * 算子运行状态 —— 由 {@link OperatorStats} 维护，供 QueryManager / WebUI 实时展示。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/11
 */
public enum OperatorState {
    /** 尚未 open（未进入执行）。 */
    NOT_STARTED,
    /** 正在执行（open 之后、close 之前，含后台任务线程活跃期）。 */
    RUNNING,
    /** 正常执行完毕（close 后标记）。 */
    FINISHED,
    /** 执行失败（open/next/close 或后台任务抛异常）。 */
    FAILED
}
