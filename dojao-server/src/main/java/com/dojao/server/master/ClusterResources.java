package com.dojao.server.master;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 集群资源视图 —— JobManager 持有的集群级资源总量与已用量。
 *
 * <p>v1 只跟踪并行度 slot；内存 / CPU / IO 等维度后续资源分配策略讨论后再扩展。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/11
 */
public final class ClusterResources {

    /** 集群总并行度 slot。 */
    private final int totalSlots;

    private final AtomicInteger usedSlots = new AtomicInteger();

    public ClusterResources(int totalSlots) {
        this.totalSlots = Math.max(1, totalSlots);
    }

    public int totalSlots() { return totalSlots; }
    public int usedSlots() { return usedSlots.get(); }
    public int availableSlots() { return Math.max(0, totalSlots - usedSlots.get()); }

    /** 占用 n 个 slot。 */
    public void reserve(int n) {
        usedSlots.addAndGet(Math.max(0, n));
    }

    /** 归还 n 个 slot（下限 0）。 */
    public void release(int n) {
        usedSlots.updateAndGet(v -> Math.max(0, v - Math.max(0, n)));
    }
}
