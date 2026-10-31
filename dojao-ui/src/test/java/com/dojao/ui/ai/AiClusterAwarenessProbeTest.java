package com.dojao.ui.ai;

import com.dojao.ui.monitor.MonitorCluster;
import com.dojao.ui.monitor.MonitorJob;
import com.dojao.ui.monitor.MonitorWorker;

import java.util.List;

/**
 * 探针：Dojao AI「集群感知」——{@code cluster_status} 工具与忙闲护栏（纯函数，离线）。
 *
 * <p>覆盖：{@link AiAgentService#formatClusterStatus} 摘要文本、{@link AiAgentService#clusterLoadLevel}
 * 分级（空闲 / 适中 / 繁忙，分别由「任务/槽位」与「平均 CPU」触发）、{@link AiAgentService#isClusterBusy}
 * 与 {@link AiAgentService#clusterBusyAdvisory} 的一致性、空集群 / 无 CPU 采样（-1）边界。</p>
 *
 * <p><b>判别力</b>：把 {@code clusterLoadLevel} 里 {@code slotRatio >= CLUSTER_BUSY_SLOT_RATIO}
 * 中和成 {@code >= 999} → 「槽位繁忙」用例 FAIL；把 {@code cpu >= CLUSTER_BUSY_CPU_PCT}
 * 中和成 {@code >= 999} → 「CPU 繁忙」用例 FAIL。</p>
 */
public final class AiClusterAwarenessProbeTest {

    private static final long GB = 1024L * 1024 * 1024;
    private static int fail = 0;

    public static void main(String[] args) {
        // ---- 空闲集群：2 worker，0 任务，CPU 10% ----
        MonitorCluster idle = cluster(
                master(15, 0),
                List.of(worker("w1", 10, 5, 0), worker("w2", 12, 5, 0)),
                List.of(job("q1", "RUNNING")),
                List.of());
        check("空闲 level=0", AiAgentService.clusterLoadLevel(idle) == 0);
        check("空闲 label=空闲", "空闲".equals(AiAgentService.clusterLoadLabel(idle)));
        check("空闲 notBusy", !AiAgentService.isClusterBusy(idle));
        check("空闲 advisory=null", AiAgentService.clusterBusyAdvisory(idle) == null);

        String text = AiAgentService.formatClusterStatus(idle);
        check("摘要 含 master 节点", text.contains("[master]"));
        check("摘要 含 worker 节点", text.contains("[worker]") && text.contains("w1"));
        check("摘要 含健康汇总", text.contains("健康 worker 2/2"));
        check("摘要 含总线位 10", text.contains("总槽位 10"));
        check("摘要 含占用率 0%", text.contains("占用率 0%"));
        check("摘要 含活动查询 q1", text.contains("q1") && text.contains("活动查询：1"));
        check("摘要 含负载结论空闲", text.contains("负载结论：空闲"));

        // ---- 适中：任务 6/10 = 60%，CPU 低 ----
        MonitorCluster moderate = cluster(master(15, 0),
                List.of(worker("w1", 10, 5, 3), worker("w2", 10, 5, 3)), List.of(), List.of());
        check("适中 level=1", AiAgentService.clusterLoadLevel(moderate) == 1);
        check("适中 label=适中", "适中".equals(AiAgentService.clusterLoadLabel(moderate)));
        check("适中 notBusy", !AiAgentService.isClusterBusy(moderate));

        // ---- 繁忙（任务/槽位）：10/10 = 100% ----
        MonitorCluster busySlot = cluster(master(15, 0),
                List.of(worker("w1", 10, 5, 5), worker("w2", 10, 5, 5)), List.of(), List.of());
        check("繁忙(槽位) level=2", AiAgentService.clusterLoadLevel(busySlot) == 2);
        check("繁忙(槽位) label=繁忙", "繁忙".equals(AiAgentService.clusterLoadLabel(busySlot)));
        check("繁忙(槽位) isBusy", AiAgentService.isClusterBusy(busySlot));
        String adv = AiAgentService.clusterBusyAdvisory(busySlot);
        check("繁忙(槽位) advisory 非空且含稍后重试",
                adv != null && adv.contains("稍后重试") && adv.contains("10/10"));

        // ---- 繁忙（CPU）：无任务但平均 CPU 90% ----
        MonitorCluster busyCpu = cluster(master(90, 0),
                List.of(worker("w1", 90, 5, 0), worker("w2", 90, 5, 0)), List.of(), List.of());
        check("繁忙(CPU) level=2", AiAgentService.clusterLoadLevel(busyCpu) == 2);
        check("繁忙(CPU) isBusy", AiAgentService.isClusterBusy(busyCpu));
        check("繁忙(CPU) advisory 非空", AiAgentService.clusterBusyAdvisory(busyCpu) != null);

        // isClusterBusy 与 advisory 一致性
        check("一致性 busySlot", AiAgentService.isClusterBusy(busySlot)
                == (AiAgentService.clusterBusyAdvisory(busySlot) != null));
        check("一致性 idle", AiAgentService.isClusterBusy(idle)
                == (AiAgentService.clusterBusyAdvisory(idle) != null));

        // ---- 边界：空集群 / 无 CPU 采样（-1）----
        MonitorCluster empty = cluster(null, List.of(), List.of(), List.of());
        check("空集群 不抛", AiAgentService.formatClusterStatus(empty) != null);
        check("空集群 无节点", AiAgentService.formatClusterStatus(empty).contains("节点：无"));
        check("空集群 level=0", AiAgentService.clusterLoadLevel(empty) == 0);
        check("null 集群 不抛", AiAgentService.formatClusterStatus(null).contains("未获取到"));

        MonitorCluster noCpu = cluster(worker("master", -1, 5, 0),
                List.of(worker("w1", -1, 5, 0)), List.of(), List.of());
        check("无 CPU 采样(-1) notBusy", !AiAgentService.isClusterBusy(noCpu));
        check("无 CPU 采样 摘要含未知", AiAgentService.formatClusterStatus(noCpu).contains("未知"));

        System.out.println(fail == 0 ? "\n全部通过 ✅" : "\n失败 " + fail + " 项 ❌");
        if (fail > 0) {
            System.exit(1);
        }
    }

    private static MonitorCluster cluster(MonitorWorker master, List<MonitorWorker> workers,
                                          List<MonitorJob> active, List<MonitorJob> recent) {
        return new MonitorCluster(master, workers, active, recent);
    }

    private static MonitorWorker master(int cpuPct, int runningQueries) {
        return worker("master", cpuPct, 4, runningQueries);
    }

    /** 便捷构造：role=worker（除非 id=master），带槽位与运行任务数。 */
    private static MonitorWorker worker(String id, int cpuPct, int maxConcurrency, int runningQueries) {
        String role = "master".equals(id) ? "master" : "worker";
        return new MonitorWorker(id, id + ".host", 9000, "RUNNING", true,
                8, cpuPct,
                16L * GB, 8L * GB,
                runningQueries,
                "1.0", role,
                2L * GB, 4L * GB, 100,
                500L * GB, 200L * GB,
                maxConcurrency,
                0L, 0L, List.of(), 0L);
    }

    private static MonitorJob job(String id, String state) {
        return new MonitorJob(id, "select * from t where dt = '2026-10-14'", state, 1234L, 10L);
    }

    private static void check(String name, boolean ok) {
        System.out.println((ok ? "  ✅ " : "  ❌ ") + name);
        if (!ok) {
            fail++;
        }
    }

    private AiClusterAwarenessProbeTest() {
    }
}
