package com.dojao.server.master;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * QueryManager 全局注册表（Master 单例）。
 *
 * <p>每次查询提交时 {@link #register(QueryManager)}，执行完成后
 * {@link #complete(QueryManager)} 把实例从活跃表移入最近完成缓存（LRU，有界），
 * 保证客户端 / WebUI 随时可通过 queryId 取到查询快照。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/11
 */
public final class QueryManagerRegistry {

    /** 最近完成缓存上限：超出按最近访问淘汰（LRU）。 */
    private static final int RECENT_CAPACITY = 100;

    private static final QueryManagerRegistry INSTANCE = new QueryManagerRegistry();

    /** 正在执行的查询。 */
    private final ConcurrentHashMap<String, QueryManager> active = new ConcurrentHashMap<>();

    /** 最近完成的查询（LRU，容量受 RECENT_CAPACITY 约束）。 */
    private final Map<String, QueryManager> recent =
            Collections.synchronizedMap(new LinkedHashMap<>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, QueryManager> eldest) {
                    return size() > RECENT_CAPACITY;
                }
            });

    private QueryManagerRegistry() {}

    public static QueryManagerRegistry get() {
        return INSTANCE;
    }

    /** 查询提交时注册。 */
    public void register(QueryManager qm) {
        active.put(qm.queryId(), qm);
    }

    /** 查询执行完成（FINISHED / FAILED / CANCELED）后调用：移出活跃，保留最近记录。 */
    public void complete(QueryManager qm) {
        active.remove(qm.queryId());
        recent.put(qm.queryId(), qm);
    }

    /** 按 queryId 取查询（先活跃，后最近完成）。 */
    public QueryManager get(String queryId) {
        QueryManager qm = active.get(queryId);
        if (qm != null) return qm;
        synchronized (recent) {
            return recent.get(queryId);
        }
    }

    /**
     * 按 <b>operator queryId</b>（Worker 遥测分组键，格式 "query-&lt;hex&gt;"，= 片段派发 jobId）反查归属的
     * QueryManager。{@link #get(String)} 按 QueryManager.queryId（yyyyMMddHHmmss_UUID）索引，对遥测分组键
     * 恒 miss —— 须改遍历物理计划匹配（{@link QueryManager#ownsOperatorQueryId}）。active 优先、recent 兜底
     * （查询已完成的迟到尾部帧仍尽力落表，行数冻结不覆盖权威值）。调用频率低（~1 帧/s/worker × 组数 ×
     * 并发查询数），遍历开销可忽略。
     */
    public QueryManager findByOperatorQueryId(String operatorQueryId) {
        if (operatorQueryId == null) {
            return null;
        }
        for (QueryManager qm : active.values()) {
            if (qm.ownsOperatorQueryId(operatorQueryId)) {
                return qm;
            }
        }
        synchronized (recent) {
            for (QueryManager qm : recent.values()) {
                if (qm.ownsOperatorQueryId(operatorQueryId)) {
                    return qm;
                }
            }
        }
        return null;
    }

    /**
     * 某 operator queryId 归属的查询<b>是否仍活着</b>（僵尸片段回收的判据）。
     *
     * <p>「活着」= 该 operator queryId 命中某个仍在 {@link #active} 表里的 QueryManager，且该查询状态
     * 为 {@link QueryState#SUBMITTED} / {@link QueryState#RUNNING}。只在 active 表里查（不进 recent）：
     * 落了 recent 说明查询已收尾 → 其 Worker 片段即便还挂着也是残留，可以回收。状态判定再保守一层：
     * 状态已进入终态但尚未 {@code complete()} 的查询（算子链正在 doClose）同样算「不活」，
     * 它自己的取消正在路上，Worker 先动手取消也只是提前，不会误杀活查询。</p>
     *
     * <p><b>只用于「Master 说已死不认」的单向判定</b>：查不到 / 已终态 → 返回 false（= 可回收）。
     * 与本类 {@link #get(String)} 的「最近完成也返回」语义刻意不同。</p>
     */
    public boolean isOperatorQueryActive(String operatorQueryId) {
        if (operatorQueryId == null) {
            return false;
        }
        for (QueryManager qm : active.values()) {
            if (qm.ownsOperatorQueryId(operatorQueryId)) {
                QueryState st = qm.state();
                return st == QueryState.SUBMITTED || st == QueryState.RUNNING;
            }
        }
        return false;
    }

    /** 正在执行的查询。 */
    public List<QueryManager> activeQueries() {
        return List.copyOf(active.values());
    }

    /** 最近完成的查询（LRU 顺序）。 */
    public List<QueryManager> recentlyFinished() {
        synchronized (recent) {
            return List.copyOf(recent.values());
        }
    }

    public int activeCount() {
        return active.size();
    }
}
