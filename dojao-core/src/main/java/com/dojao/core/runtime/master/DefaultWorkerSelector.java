package com.dojao.core.runtime.master;

import com.dojao.core.runtime.worker.WorkerState;

import java.util.List;
import java.util.Optional;

/**
 * 默认任务落点策略（占位）：
 * <ol>
 *   <li>IDLE（无运行任务）优先于 BUSY；</li>
 *   <li>其次 CPU 使用率低者优先（未上报按 100 处理）；</li>
 *   <li>再其次内存占用比例低者优先；</li>
 *   <li>最后按 workerId 字典序（确定性兜底）。</li>
 * </ol>
 * 真实分配算法（权重 / 队列深度 / 亲和性等）后续在此演进。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/11
 */
public final class DefaultWorkerSelector implements WorkerSelector {

    @Override
    public Optional<WorkerRegistration> select(List<WorkerRegistration> healthyWorkers) {
        if (healthyWorkers.isEmpty()) {
            return Optional.empty();
        }
        return healthyWorkers.stream().min((a, b) -> {
            int c = Integer.compare(loadScore(a), loadScore(b));
            if (c != 0) return c;
            c = Long.compare(cpuScore(a), cpuScore(b));
            if (c != 0) return c;
            c = Long.compare(memScore(a), memScore(b));
            if (c != 0) return c;
            return a.info().workerId().compareTo(b.info().workerId());
        });
    }

    /** IDLE 得 0 分，BUSY 得 1 分（越小越优）。 */
    private static int loadScore(WorkerRegistration r) {
        return r.state() == WorkerState.IDLE ? 0 : 1;
    }

    /** CPU 使用率；未上报（-1）按 100 分处理。 */
    private static long cpuScore(WorkerRegistration r) {
        if (r.lastResource() == null || r.lastResource().cpuUsagePct() < 0) {
            return 100L;
        }
        return r.lastResource().cpuUsagePct();
    }

    /** 内存占用比例（万分比）；未上报按最大处理。 */
    private static long memScore(WorkerRegistration r) {
        if (r.lastResource() == null || r.lastResource().totalMemoryBytes() <= 0L) {
            return Long.MAX_VALUE;
        }
        return r.lastResource().usedMemoryBytes() * 10_000L / r.lastResource().totalMemoryBytes();
    }
}
