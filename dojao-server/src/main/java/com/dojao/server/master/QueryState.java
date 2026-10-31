package com.dojao.server.master;

/**
 * 查询生命周期状态 —— 由 {@link QueryManager} 维护。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/11
 */
public enum QueryState {
    /** 已提交，尚未开始执行（仍在校验/规划阶段）。 */
    SUBMITTED,
    /** 正在执行算子链。 */
    RUNNING,
    /** 执行完成（读到 EOF，或写语句返回）。 */
    FINISHED,
    /** 执行失败（语义错误 / 算子异常 / 后台任务失败）。 */
    FAILED,
    /** 被用户取消。 */
    CANCELED
}
