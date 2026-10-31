package com.dojao.server.worker;

/**
 * Worker 任务执行体 —— 由 Master 侧 / 执行引擎提供。
 *
 * <p>典型流程：校验 SQL → 生成物理计划 → 执行算子链 → 回传结果/行数。
 * 执行过程中抛出的异常由 WorkerManager 记录为该任务失败（taskFailures）。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/11
 */
@FunctionalInterface
public interface WorkerTaskExecution {

    /** 执行任务；抛出的异常记录为任务失败。 */
    void execute(WorkerTask task, WorkerManager worker) throws Exception;
}
