package com.dojao.core.runtime.master;

import java.util.List;
import java.util.Optional;

/**
 * Master 任务落点选择策略（接口）—— 输入存活节点列表，按节点资源情况选出最优 Worker。
 *
 * <p>后续可插拔：默认（最少负载）/ 加权 / 亲和性 / 队列深度等。
 * v1 由 {@link DefaultWorkerSelector} 占位。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/11
 */
public interface WorkerSelector {

    /** 从存活节点中选择一个任务落点；无可选节点返回空。 */
    Optional<WorkerRegistration> select(List<WorkerRegistration> healthyWorkers);
}
