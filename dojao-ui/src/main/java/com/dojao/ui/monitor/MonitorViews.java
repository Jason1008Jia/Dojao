package com.dojao.ui.monitor;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * 监控视图的纯函数集合 —— 状态过滤 / 状态计数 / 引擎状态组装 / 算子定位。
 *
 * <p>HTTP 处理器（{@code DojaoUiApp}）与 AI 工具（{@code AiAgentService}）共用同一套口径，
 * 全部无副作用、<b>离线可测</b>。状态名统一大写；对外的中文别名见 {@link #statesOf(String)}。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/21
 */
public final class MonitorViews {

    /** 计数结果里固定出现（值为 0 也保留）的状态，顺序即展示顺序。 */
    public static final List<String> CANONICAL_STATES =
            List.of("RUNNING", "SUBMITTED", "FINISHED", "FAILED", "CANCELED");

    /** {@code /api/engine/status} 未显式给 limit 时返回的查询条数。 */
    public static final int DEFAULT_ENGINE_QUERIES = 20;

    /** {@code /api/engine/status} 单次最多返回的查询条数。 */
    public static final int MAX_ENGINE_QUERIES = 200;

    /** {@code /api/queries} 单次最多返回的查询条数（{@code limit=0} = 不限）。 */
    public static final int MAX_LIST_QUERIES = 1000;

    private static final Set<String> ALL_TOKENS = Set.of("ALL", "*", "ANY", "全部", "所有");

    private MonitorViews() {
    }

    /**
     * 把状态规格解析成规范状态集合。支持逗号 / 分号 / 竖线 / 空白分隔，多个状态取并集。
     *
     * <p>别名：{@code active}=排队+运行；{@code running}、{@code submitted/queued/pending}、
     * {@code finished/success/completed/done}、{@code failed/error}、{@code canceled/cancelled}；
     * 也接受中文（运行中 / 排队中 / 已完成 / 失败 / 已取消）。其余原样大写保留（如 UNKNOWN）。</p>
     *
     * @return {@code null} = 不过滤（规格为空 / all / 只含无法识别的「全部」词）
     */
    public static Set<String> statesOf(String stateSpec) {
        if (stateSpec == null || stateSpec.isBlank()) {
            return null;
        }
        Set<String> out = new LinkedHashSet<>();
        boolean all = false;
        for (String token : stateSpec.split("[,;|\\s]+")) {
            String t = token.strip().toUpperCase(Locale.ROOT);
            if (t.isEmpty()) {
                continue;
            }
            if (ALL_TOKENS.contains(t)) {
                all = true;
                continue;
            }
            switch (t) {
                case "RUNNING", "运行", "运行中" -> out.add("RUNNING");
                case "SUBMITTED", "QUEUED", "PENDING", "排队", "排队中" -> out.add("SUBMITTED");
                case "ACTIVE", "ONGOING", "活跃" -> {
                    out.add("SUBMITTED");
                    out.add("RUNNING");
                }
                case "FINISHED", "SUCCESS", "SUCCEEDED", "COMPLETED", "DONE", "完成", "已完成" ->
                        out.add("FINISHED");
                case "FAILED", "FAILURE", "ERROR", "失败" -> out.add("FAILED");
                case "CANCELED", "CANCELLED", "取消", "已取消" -> out.add("CANCELED");
                default -> out.add(t);
            }
        }
        return all || out.isEmpty() ? null : out;
    }

    /** 按状态规格过滤查询列表（保持原有顺序）；{@code stateSpec} 为 all/空 → 原样返回副本。 */
    public static List<MonitorJob> filterJobs(List<MonitorJob> jobs, String stateSpec) {
        if (jobs == null || jobs.isEmpty()) {
            return List.of();
        }
        Set<String> states = statesOf(stateSpec);
        if (states == null) {
            return List.copyOf(jobs);
        }
        List<MonitorJob> out = new ArrayList<>();
        for (MonitorJob j : jobs) {
            if (j != null && j.state() != null
                    && states.contains(j.state().toUpperCase(Locale.ROOT))) {
                out.add(j);
            }
        }
        return out;
    }

    /**
     * 按状态计数：固定包含 {@link #CANONICAL_STATES} 五个键（含 0），
     * 另有其他状态（如 UNKNOWN / REJECTED）时按首次出现顺序追加。
     */
    public static Map<String, Integer> countByState(List<MonitorJob> jobs) {
        Map<String, Integer> counts = new LinkedHashMap<>();
        for (String s : CANONICAL_STATES) {
            counts.put(s, 0);
        }
        if (jobs != null) {
            for (MonitorJob j : jobs) {
                String s = j == null || j.state() == null
                        ? "UNKNOWN" : j.state().toUpperCase(Locale.ROOT);
                counts.merge(s, 1, Integer::sum);
            }
        }
        return counts;
    }

    /** 取列表前 {@code limit} 条；{@code limit} ≤ 0 或 ≥ 长度 → 原样返回副本。 */
    public static <T> List<T> head(List<T> items, int limit) {
        if (items == null || items.isEmpty()) {
            return List.of();
        }
        if (limit <= 0 || limit >= items.size()) {
            return List.copyOf(items);
        }
        return List.copyOf(items.subList(0, limit));
    }

    /**
     * 解析 limit 参数：空 / 非整数 / ≤0 → {@code def}；否则夹到 {@code [1, max]}。
     * （{@code /api/queries} 的 {@code limit=0} 语义为「不限」，在处理器里单独处理。）
     */
    public static int clampLimit(String raw, int def, int max) {
        if (raw == null || raw.isBlank()) {
            return def;
        }
        try {
            int n = Integer.parseInt(raw.strip());
            if (n <= 0) {
                return def;
            }
            return Math.min(n, max);
        } catch (NumberFormatException e) {
            return def;
        }
    }

    /**
     * 组装引擎状态总览：状态计数取全量，查询列表按 {@code stateSpec} 过滤后截断到
     * {@code limit}（≤0 → {@value #DEFAULT_ENGINE_QUERIES}，上限 {@value #MAX_ENGINE_QUERIES}）。
     */
    public static MonitorEngineStatus engineStatus(List<MonitorJob> allJobs, MonitorCluster cluster,
                                                   String stateSpec, int limit, long sampledAt) {
        List<MonitorJob> all = allJobs == null ? List.of() : allJobs;
        List<MonitorJob> matched = filterJobs(all, stateSpec);
        int cap = limit <= 0 ? DEFAULT_ENGINE_QUERIES : Math.min(limit, MAX_ENGINE_QUERIES);
        return new MonitorEngineStatus(sampledAt, all.size(), matched.size(),
                countByState(all), cluster, head(matched, cap));
    }

    /** 查询是否真实存在（{@link QueryMonitor#snapshot} 对未知 id 返回 state=UNKNOWN 的占位快照）。 */
    public static boolean exists(MonitorQuery q) {
        return q != null && q.state() != null && !"UNKNOWN".equalsIgnoreCase(q.state());
    }

    /** 查询快照 → 算子明细 DTO（未知查询 found=false）。 */
    public static MonitorQueryOperators toQueryOperators(MonitorQuery q) {
        if (q == null) {
            return new MonitorQueryOperators("", "", "UNKNOWN", 0L, 0L, "no such query",
                    false, 0, List.of(), Map.of());
        }
        List<MonitorOperator> ops = q.operators() == null ? List.of() : q.operators();
        Map<String, String> failures = q.operatorFailures() == null ? Map.of() : q.operatorFailures();
        return new MonitorQueryOperators(q.queryId(), q.sql(), q.state(), q.wallMs(),
                q.totalRowsOut(), q.failureMessage(), exists(q), ops.size(), ops, failures);
    }

    /**
     * 在执行 DAG 里定位一个算子：{@code key} 可为节点下标（{@code "3"} / {@code "#3"}）
     * 或 {@code operatorId}（大小写不敏感）。
     *
     * @return 找不到 → {@code null}
     */
    public static MonitorOperator findOperator(MonitorQuery q, String key) {
        if (q == null || q.operators() == null || key == null || key.isBlank()) {
            return null;
        }
        String k = key.strip();
        if (k.startsWith("#")) {
            k = k.substring(1);
        }
        try {
            int idx = Integer.parseInt(k);
            for (MonitorOperator op : q.operators()) {
                if (op.idx() == idx) {
                    return op;
                }
            }
            // idx 与列表下标不连续时的兜底（按 DAG 节点序号即列表下标约定）
            if (idx >= 0 && idx < q.operators().size()) {
                return q.operators().get(idx);
            }
            return null;
        } catch (NumberFormatException ignored) {
            // 非数字 → 按 operatorId 匹配
        }
        for (MonitorOperator op : q.operators()) {
            if (op.operatorId() != null && op.operatorId().equalsIgnoreCase(k)) {
                return op;
            }
        }
        return null;
    }
}
