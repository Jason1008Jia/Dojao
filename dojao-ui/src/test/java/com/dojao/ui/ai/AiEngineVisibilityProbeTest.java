package com.dojao.ui.ai;

import com.dojao.ui.monitor.MonitorCluster;
import com.dojao.ui.monitor.MonitorEngineStatus;
import com.dojao.ui.monitor.MonitorJob;
import com.dojao.ui.monitor.MonitorOperator;
import com.dojao.ui.monitor.MonitorQuery;
import com.dojao.ui.monitor.MonitorQueryOperators;
import com.dojao.ui.monitor.MonitorTask;
import com.dojao.ui.monitor.MonitorViews;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 探针：Dojao AI「引擎可见性」——查询列表 / 查询详情 / 算子详情的纯函数与状态过滤（离线）。
 *
 * <p>覆盖：{@link MonitorViews} 的状态规格解析（别名 / 中文 / 并集）、过滤与计数、limit 夹紧、
 * 引擎状态组装、算子按序号 / operatorId 定位；{@link AiAgentService} 的
 * {@code formatQueryList / formatQueryStatus / formatOperatorList / formatOperatorDetail}
 * 输出（含未找到查询、失败算子、任务明细、截断提示）。</p>
 *
 * <p><b>判别力</b>：把 {@code MonitorViews.statesOf} 的 {@code ACTIVE} 分支只加 RUNNING
 * → active 相关用例 FAIL；把 {@code AiAgentService.operatorLine} 的溢写段删掉 → 详情用例 FAIL；
 * 把 {@code formatQueryStatus} 的未找到分支改成正常输出 → 未找到用例 FAIL。</p>
 */
public final class AiEngineVisibilityProbeTest {

    private static final long KB = 1024L;
    private static final long MB = 1024L * KB;
    private static final long GB = 1024L * MB;

    private static int fail = 0;

    public static void main(String[] args) {
        List<MonitorJob> jobs = jobs();
        List<MonitorJob> withUnknown = new ArrayList<>(jobs);
        withUnknown.add(new MonitorJob("q20261021112000", "explain select 1", "UNKNOWN", 0L, 0L));

        // ==================== 状态规格解析 ====================
        check("statesOf(null) = 不过滤", MonitorViews.statesOf(null) == null);
        check("statesOf(all) = 不过滤", MonitorViews.statesOf("all") == null);
        check("statesOf(running) = {RUNNING}", Set.of("RUNNING").equals(MonitorViews.statesOf("running")));
        check("statesOf(active) = {SUBMITTED,RUNNING}",
                Set.of("SUBMITTED", "RUNNING").equals(MonitorViews.statesOf("active")));
        Set<String> multi = MonitorViews.statesOf("failed,canceled");
        check("statesOf(failed,canceled) 两个状态", multi != null && multi.size() == 2
                && multi.contains("FAILED") && multi.contains("CANCELED"));
        check("statesOf(运行中) 中文别名", Set.of("RUNNING").equals(MonitorViews.statesOf("运行中")));
        check("statesOf(已完成) 中文别名", Set.of("FINISHED").equals(MonitorViews.statesOf("已完成")));
        check("statesOf(未知状态) 原样保留", Set.of("WEIRD").equals(MonitorViews.statesOf("weird")));

        // ==================== 过滤 / 计数 / 截断 ====================
        check("filterJobs(all) 全量", MonitorViews.filterJobs(jobs, null).size() == 6);
        check("filterJobs(running) 2 条", MonitorViews.filterJobs(jobs, "running").size() == 2);
        check("filterJobs(active) 3 条", MonitorViews.filterJobs(jobs, "active").size() == 3);
        List<MonitorJob> failed = MonitorViews.filterJobs(jobs, "failed");
        check("filterJobs(failed) 1 条且 id 正确",
                failed.size() == 1 && failed.get(0).queryId().equals("q20261021114000"));
        check("filterJobs(无法识别状态) 空", MonitorViews.filterJobs(jobs, "nonsense").isEmpty());

        Map<String, Integer> counts = MonitorViews.countByState(jobs);
        check("countByState 固定五个键", counts.keySet().containsAll(MonitorViews.CANONICAL_STATES));
        check("countByState RUNNING=2", counts.get("RUNNING") == 2);
        check("countByState SUBMITTED=1", counts.get("SUBMITTED") == 1);
        check("countByState FINISHED=1", counts.get("FINISHED") == 1);
        check("countByState FAILED=1", counts.get("FAILED") == 1);
        check("countByState CANCELED=1", counts.get("CANCELED") == 1);
        Map<String, Integer> countsUnknown = MonitorViews.countByState(withUnknown);
        check("countByState 额外 UNKNOWN 追加", countsUnknown.get("UNKNOWN") == 1);

        check("head(2) 两条", MonitorViews.head(jobs, 2).size() == 2);
        check("head(0) 不截断", MonitorViews.head(jobs, 0).size() == 6);
        check("clampLimit(null)=默认", MonitorViews.clampLimit(null, 20, 100) == 20);
        check("clampLimit(5)", MonitorViews.clampLimit("5", 20, 100) == 5);
        check("clampLimit(500)→上限", MonitorViews.clampLimit("500", 20, 100) == 100);
        check("clampLimit(abc)=默认", MonitorViews.clampLimit("abc", 20, 100) == 20);
        check("clampLimit(-3)=默认", MonitorViews.clampLimit("-3", 20, 100) == 20);

        // ==================== 引擎状态组装 ====================
        MonitorCluster cluster = new MonitorCluster(null, List.of(), List.of(), List.of());
        MonitorEngineStatus status = MonitorViews.engineStatus(jobs, cluster, "active", 2, 987654321L);
        check("engineStatus sampledAt", status.sampledAt() == 987654321L);
        check("engineStatus totalQueries=6", status.totalQueries() == 6);
        check("engineStatus matchedQueries=3", status.matchedQueries() == 3);
        check("engineStatus queries 截断为 2", status.queries().size() == 2);
        check("engineStatus queryStates RUNNING=2", status.queryStates().get("RUNNING") == 2);
        check("engineStatus cluster 透传", status.cluster() == cluster);

        // ==================== 查询 / 算子定位 ====================
        MonitorQuery q = query();
        MonitorQuery unknown = new MonitorQuery("q-missing", "", "UNKNOWN", 0L, 0L,
                "no such query", List.of(), Map.of());
        check("exists(真实查询)", MonitorViews.exists(q));
        check("exists(UNKNOWN 占位) = false", !MonitorViews.exists(unknown));

        MonitorQueryOperators dto = MonitorViews.toQueryOperators(q);
        check("toQueryOperators found=true", dto.found());
        check("toQueryOperators operatorCount=3", dto.operatorCount() == 3);
        check("toQueryOperators 保留失败映射", dto.operatorFailures().containsKey("HashJoin"));
        check("toQueryOperators(UNKNOWN) found=false", !MonitorViews.toQueryOperators(unknown).found());

        check("findOperator(1)", idxOf(q, "1") == 1);
        check("findOperator(#0)", idxOf(q, "#0") == 0);
        check("findOperator(operatorId 大小写不敏感)", idxOf(q, "OP-JOIN") == 2);
        check("findOperator(不存在序号) = null", MonitorViews.findOperator(q, "9") == null);
        check("findOperator(不存在 id) = null", MonitorViews.findOperator(q, "nope") == null);

        // ==================== formatQueryList ====================
        String listText = AiAgentService.formatQueryList(null, jobs, 20);
        check("列表 含筛选口径全部", listText.contains("筛选：全部"));
        check("列表 含总数与命中数", listText.contains("共 6 条，命中 6 条"));
        check("列表 含 queryId 与状态", listText.contains("q20261021120000")
                && listText.contains("运行中 / RUNNING"));
        check("列表 含 SQL 摘要", listText.contains("select province"));
        check("列表 含下钻提示", listText.contains("query_status(queryId)"));

        String listLimited = AiAgentService.formatQueryList("active", jobs, 2);
        check("列表 截断提示", listLimited.contains("命中 3 条，显示最近 2 条"));
        String listFailed = AiAgentService.formatQueryList("failed", jobs, 20);
        check("列表 失败筛选含失败查询", listFailed.contains("q20261021114000")
                && listFailed.contains("失败 / FAILED"));
        check("列表 失败筛选不含运行查询", !listFailed.contains("q20261021120000"));
        check("列表 空结果提示",
                AiAgentService.formatQueryList("nonsense", jobs, 20).contains("(没有符合条件的查询)"));

        // ==================== formatQueryStatus ====================
        String unknownText = AiAgentService.formatQueryStatus(unknown);
        check("查询详情 未找到提示", unknownText.contains("未找到查询 q-missing")
                && unknownText.contains("list_queries"));
        check("查询详情 null 不抛", AiAgentService.formatQueryStatus(null).contains("未获取到"));

        String qText = AiAgentService.formatQueryStatus(q);
        check("查询详情 含状态", qText.contains("查询 q1：运行中（RUNNING）"));
        check("查询详情 含进行中耗时说明", qText.contains("仍在进行"));
        check("查询详情 含算子数", qText.contains("算子：3 个"));
        check("查询详情 含根算子行", qText.contains("#0 HashAggregation"));
        check("查询详情 含溢写", qText.contains("溢写 1.0G"));
        check("查询详情 含上游序号", qText.contains("上游 #1"));
        check("查询详情 含失败算子", qText.contains("失败算子")
                && qText.contains("HashJoin: join build failed"));
        check("查询详情 含下钻提示", qText.contains("operator_status(queryId, operator=\"#序号\")"));

        // ==================== formatOperatorList ====================
        String opList = AiAgentService.formatOperatorList(q);
        check("算子列表 含数量", opList.contains("的算子（3 个）"));
        check("算子列表 含任务汇总", opList.contains("任务 2（运行 1 / 完成 1 / 失败 0）"));
        check("算子列表 含扫描任务数", opList.contains("任务 3（运行 1 / 完成 2 / 失败 0）"));

        // ==================== formatOperatorDetail ====================
        MonitorOperator agg = MonitorViews.findOperator(q, "0");
        String aggText = agg == null ? "" : AiAgentService.formatOperatorDetail(q, agg, true);
        check("算子详情 含 operatorId", aggText.contains("op-agg"));
        check("算子详情 含并行度", aggText.contains("并行度：6"));
        check("算子详情 含行数", aggText.contains("行数：输入 1,000,000 / 输出 12"));
        check("算子详情 含内存峰值", aggText.contains("内存：当前 50M（峰值 200M）"));
        check("算子详情 含溢写", aggText.contains("溢写：1.0G"));
        check("算子详情 含速率", aggText.contains("瞬时 5,000 行/秒"));
        check("算子详情 含任务明细", aggText.contains("task-a-1 @ w1")
                && aggText.contains("（运行 1 / 完成 1 / 失败 0）"));

        MonitorOperator join = MonitorViews.findOperator(q, "2");
        String joinText = join == null ? "" : AiAgentService.formatOperatorDetail(q, join, true);
        check("算子详情 失败原因", joinText.contains("失败原因：join build failed"));
        check("算子详情 失败任务与源表", joinText.contains("task-c-1 @ w3")
                && joinText.contains("失败: worker lost") && joinText.contains("源=hive.db.orders"));
        check("算子详情 预计行数", joinText.contains("（预计 500,000 行）"));
        String joinNoTasks = join == null ? "" : AiAgentService.formatOperatorDetail(q, join, false);
        check("算子详情 省略任务", !joinNoTasks.contains("task-c-1"));
        check("算子详情 省略提示", joinNoTasks.contains("明细已省略"));

        // ==================== 任务状态汇总边界 ====================
        check("taskSummary 空列表 = 空串", AiAgentService.taskSummary(List.of()).isEmpty());
        check("taskSummary 含失联", AiAgentService.taskSummary(List.of(
                task("t1", "w1", "LOST", 0L, null, null, -1L))).contains("失联 1"));

        // ==================== 系统提示词：可用 HTTP 接口清单 ====================
        String api = AiAgentService.DOJAO_HTTP_API;
        check("接口清单 明确不能自行发起 HTTP", api.contains("不能发起 HTTP 请求"));
        check("接口清单 引擎总览", api.contains("GET /api/engine/status?state=&limit="));
        check("接口清单 查询列表", api.contains("GET /api/queries?state=&limit="));
        check("接口清单 算子列表", api.contains("GET /api/queries/{id}/operators"));
        check("接口清单 单算子", api.contains("GET /api/queries/{id}/operators/{op}"));
        check("接口清单 集群快照", api.contains("GET /api/cluster"));
        check("接口清单 日志", api.contains("GET /api/logs?node=&tail="));
        check("接口清单 异步查询", api.contains("POST /api/query"));
        check("接口清单 结果分页", api.contains("GET /api/query/{id}/rows?offset=&limit="));
        check("接口清单 元数据", api.contains("GET /api/metadata/catalogs"));
        check("接口清单 鉴权说明", api.contains("401"));
        check("系统提示词 已注入接口清单", AiAgentService.SYSTEM_PROMPT.contains("Dojao HTTP 接口清单")
                && AiAgentService.SYSTEM_PROMPT.contains("/api/engine/status"));

        System.out.println(fail == 0 ? "\n全部通过 ✅" : "\n失败 " + fail + " 项 ❌");
        if (fail > 0) {
            System.exit(1);
        }
    }

    private static List<MonitorJob> jobs() {
        return List.of(
                new MonitorJob("q20261021120000", "select province, count(*) from t group by province",
                        "RUNNING", 1200L, 0L),
                new MonitorJob("q20261021115900",
                        "select * from orders where dt = '2026-10-21'", "RUNNING", 3400L, 120L),
                new MonitorJob("q20261021115800", "select count(*) from users", "SUBMITTED", 0L, 0L),
                new MonitorJob("q20261021115000", "select 1", "FINISHED", 15L, 1L),
                new MonitorJob("q20261021114000", "select * from missing_table", "FAILED", 88L, 0L),
                new MonitorJob("q20261021113000", "select * from huge", "CANCELED", 500L, 0L));
    }

    private static MonitorQuery query() {
        MonitorOperator agg = new MonitorOperator(0, "op-agg", "HashAggregation", "RUNNING", 6,
                1_000_000L, 12L, 1_500 * MB, 4 * KB, 50 * MB, 200 * MB, 1 * GB,
                8_000L, 12_000L, 5_000L, 4_200L, 3 * MB, 2 * MB, null,
                List.of(1),
                List.of(task("task-a-1", "w1", "RUNNING", 400_000L, null, null, -1L),
                        task("task-a-2", "w2", "FINISHED", 600_000L, null, null, -1L)));
        MonitorOperator scan = new MonitorOperator(1, "op-scan", "Scan", "FINISHED", 6,
                1_000_000L, 1_000_000L, 1_500 * MB, 1_500 * MB, 0L, 0L, 0L,
                1_000L, 3_000L, 0L, 333_000L, 0L, 0L, null,
                List.of(),
                List.of(task("task-b-1", "w1", "RUNNING", 300_000L, null, "hive.db.orders", 500_000L),
                        task("task-b-2", "w2", "FINISHED", 350_000L, null, "hive.db.orders", 500_000L),
                        task("task-b-3", "w3", "FINISHED", 350_000L, null, "hive.db.orders", 500_000L)));
        MonitorOperator join = new MonitorOperator(2, "op-join", "HashJoin", "FAILED", 6,
                1_000_000L, 0L, 1_500 * MB, 0L, 0L, 0L, 0L, 200L, 400L,
                0L, 0L, 0L, 0L, "join build failed",
                List.of(0, 1),
                List.of(task("task-c-1", "w3", "FAILED", 0L, "worker lost", "hive.db.orders", 500_000L)));
        Map<String, String> failures = new LinkedHashMap<>();
        failures.put("HashJoin", "join build failed");
        return new MonitorQuery("q1",
                "select o.province, count(*) from orders o join users u on o.user_id = u.id group by o.province",
                "RUNNING", 12_345L, 12L, null, List.of(agg, scan, join), failures);
    }

    /** 定位算子并返回下标；找不到 → -1（避免判别力实验里 NPE 顶掉断言）。 */
    private static int idxOf(MonitorQuery q, String key) {
        MonitorOperator op = MonitorViews.findOperator(q, key);
        return op == null ? -1 : op.idx();
    }

    private static MonitorTask task(String id, String worker, String state, long rows,
                                    String failure, String source, long expectedRows) {
        return new MonitorTask(id, worker, "fragment", false, state, rows, 800L,
                failure, source, expectedRows);
    }

    private static void check(String name, boolean ok) {
        if (ok) {
            System.out.println("[PASS] " + name);
        } else {
            fail++;
            System.out.println("[FAIL] " + name);
        }
    }
}
