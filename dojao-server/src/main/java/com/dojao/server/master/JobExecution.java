package com.dojao.server.master;

/**
 * Job 实际执行体 —— 由提交方提供（生产环境为 QueryServiceImpl / 各渠道网关，测试为 demo）。
 *
 * <p>典型流程：校验 SQL → 生成物理计划 → 创建并挂载 {@link QueryManager} →
 * 执行算子链 → 标记完成/失败。执行过程中抛出的异常会统一转成 Job FAILED。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/11
 */
@FunctionalInterface
public interface JobExecution {

    /** 执行 Job；抛出的异常由 JobManager 捕获并标记 FAILED。 */
    void execute(Job job) throws Exception;
}
