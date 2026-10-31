package com.dojao.core.runtime.master;

import java.util.ArrayList;
import java.util.List;

/**
 * 查询级固定分区布局 —— 分区号与 Worker 的<b>强绑定</b>。
 *
 * <p>核心不变量：一个查询内 {@code owner[pid]} 一旦确定就<b>不再变化</b>（即便后续 shuffle
 * 用了不同的 key）。因为布局是 (workers, numPartitions) 的纯函数（round-robin），同一查询里
 * 所有算子在同一个 worker 列表 + 分区数下算出的布局天然一致，无需共享可变状态即可满足
 * "同 key 阶段零交换、跨 key 阶段仍路由到同一批机器"的规划约束。</p>
 *
 * <ul>
 *   <li><b>同 key 连续阶段</b>：同 hash → 同桶 → 同 Worker，数据全程留在本机，零跨机传输；</li>
 *   <li><b>跨 key 阶段</b>：按新 key 重新哈希 → 仍路由到<b>同一 owner 映射</b>，本机已有
 *       该桶数据时走本地快路径（巧合桶复用），不可控的机器漂移被彻底消除。</li>
 * </ul>
 *
 * @author jiaxin
 * @version 3.0
 * @date 2026/8/20
 */
public record PartitionLayout(int numPartitions, String[] ownerWorkerIds) {

    /** round-robin 布局：{@code owner[pid] = workers[pid % numWorkers]}。 */
    public static PartitionLayout roundRobin(List<WorkerRegistration> workers, int numPartitions) {
        int n = Math.max(1, numPartitions);
        String[] owner = new String[n];
        for (int pid = 0; pid < n; pid++) {
            owner[pid] = workers.get(pid % workers.size()).info().workerId();
        }
        return new PartitionLayout(n, owner);
    }

    /** 桶 pid 归属的 WorkerId。 */
    public String ownerOf(int pid) {
        return ownerWorkerIds[pid];
    }

    /** 全部 owner 的列表（与分区号一一对应，供 PartitionStep 传输）。 */
    public List<String> ownerWorkerIdsList() {
        return List.of(ownerWorkerIds);
    }

    /** 某 Worker 拥有的桶号列表（升序）。 */
    public List<Integer> ownedPartitions(String workerId) {
        List<Integer> out = new ArrayList<>();
        for (int pid = 0; pid < numPartitions; pid++) {
            if (ownerWorkerIds[pid].equals(workerId)) out.add(pid);
        }
        return out;
    }

    /** 某 Worker 的归属掩码（owned[pid]=true 表示该桶归此 Worker），供 ExchangeInStep 使用。 */
    public boolean[] ownedMask(String workerId) {
        boolean[] mask = new boolean[numPartitions];
        for (int pid = 0; pid < numPartitions; pid++) {
            mask[pid] = ownerWorkerIds[pid].equals(workerId);
        }
        return mask;
    }
}
