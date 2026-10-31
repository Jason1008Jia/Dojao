package com.dojao.core.resource;

import com.dojao.core.config.ResourceConfig;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 集群并行度预算 —— 多 query 共享 Worker 并发容量时的准入控制。
 *
 * <p>容量来源 {@link CapacitySource#healthyCapacity()}：健康 Worker 的
 * {@code ResourceSummary.maxConcurrency} 之和（Master 视角的集群总容量）。</p>
 *
 * <p><b>准入规则</b>（应需 = 初始并行度计算值，{@code ratio = query.admission.ratio}，默认 0.8）：</p>
 * <ul>
 *   <li>当前可用 {@code C >= 应需 P} → 授予 P；</li>
 *   <li>{@code P * ratio <= C < P} → 降级授予 C（没小太多，就用可用的）；</li>
 *   <li>{@code C < P * ratio} → 阻塞等待其它 query 释放出资源后再授予。</li>
 * </ul>
 *
 * <p><b>每 query 硬上限</b>（{@link #perQueryMax()}）：配置了 {@code system.query.max.parallelism}
 * 就用用户的值；否则默认公式 = 集群总容量 / 健康 Worker 个数 —— 单个 query 最多用「一个 Worker 的平均份额」，
 * 防止单 query 独吞整个集群。</p>
 *
 * <p>无健康 Worker（单机）时不做准入 —— 直接授予计算值，避免单机自阻塞死锁。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/29
 */
public final class ParallelismBudget {

    /** 集群容量来源：由调用方从 Worker 注册表聚合（健康 Worker 的 maxConcurrency 之和）。 */
    @FunctionalInterface
    public interface CapacitySource {
        /** 健康 Worker 并发容量之和；<= 0 表示无集群（单机）。 */
        int healthyCapacity();

        /** 健康 Worker 个数；<= 0 表示未知（默认 0，即不按 worker 数额外收紧）。 */
        default int workerCount() {
            return 0;
        }
    }

    private final CapacitySource capacitySource;
    /** queryId → 已授予并行度。 */
    private final Map<String, Integer> granted = new ConcurrentHashMap<>();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition freed = lock.newCondition();

    public ParallelismBudget(CapacitySource capacitySource) {
        this.capacitySource = capacitySource;
    }

    /** 集群总容量：健康 Worker 之和；无集群时用 {@code system.master.parallelism} 兜底（计算器封顶用）。 */
    public int clusterCapacity() {
        int c = capacitySource.healthyCapacity();
        return c > 0 ? c : ResourceConfig.masterParallelism();
    }

    /** 当前可用并行度（总容量 - 已授予）。 */
    public int available() {
        return Math.max(0, clusterCapacity() - used());
    }

    /**
     * 每 query 并行度硬上限：
     * <ul>
     *   <li>配置了 {@code system.query.max.parallelism}（{@code > 0}）→ 直接用用户值（卡死）；</li>
     *   <li>否则（auto）→ 默认公式 = 集群总容量 / 健康 Worker 个数。workerCount {@code <= 1}
     *       时不额外收紧（单机 / 单 Worker 可占满整机）；{@code > 1} 时单 query 最多用
     *       「一个 Worker 的平均份额」。</li>
     * </ul>
     * 值越小 → 单 query 上限越低，集群并发 query 数越多（吞吐越高）；值越大 → 单 query 能用越多，并发数越少。
     */
    public int perQueryMax() {
        int userCap = ResourceConfig.systemQueryMaxParallelism();
        if (userCap > 0) {
            return userCap;   // 用户显式配置 → 卡死为用户值，不再叠加任何公式
        }
        int cap = clusterCapacity();
        int workers = capacitySource.workerCount();
        return workers > 1 ? Math.max(1, cap / workers) : cap;
    }

    /**
     * 申请 {@code desired} 并行度：够就给，差一点降级给可用量，差太多阻塞等待。
     *
     * @param queryId 查询标识（同一 query 重复调用会覆盖授予）
     * @param desired 应需并行度（初始并行度计算值，通常已由计算器封顶到集群容量）
     * @return 实际授予的并行度
     */
    public int tryAdmit(String queryId, int desired) throws InterruptedException {
        // 硬上限优先：应需超过「每 query 上限」直接卡死到上限（与计算器侧封顶双保险）
        desired = Math.max(1, Math.min(desired, perQueryMax()));
        if (capacitySource.healthyCapacity() <= 0) {
            // 无集群（单机）：不做准入，直接授予，避免自阻塞死锁
            grant(queryId, desired);
            return desired;
        }
        double ratio = ResourceConfig.queryAdmissionRatio();
        lock.lock();
        try {
            while (true) {
                int avail = available();
                if (avail >= desired) {
                    grant(queryId, desired);
                    return desired;
                }
                if (avail >= (int) Math.ceil(desired * ratio)) {
                    grant(queryId, avail);   // 降级：没小太多，用可用量
                    return avail;
                }
                // 可用 < 应需 * ratio → 阻塞等待其它 query 释放
                freed.await();
            }
        } finally {
            lock.unlock();
        }
    }

    /** 释放 query 占用的并行度（执行结束 finally 中调用），唤醒等待中的准入者。 */
    public void release(String queryId) {
        lock.lock();
        try {
            granted.remove(queryId);
            freed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private int used() {
        int sum = 0;
        for (int v : granted.values()) {
            sum += v;
        }
        return sum;
    }

    private void grant(String queryId, int parallelism) {
        granted.put(queryId, parallelism);
    }
}
