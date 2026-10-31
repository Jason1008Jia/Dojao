package com.dojao.ui.ai;

import com.dojao.ui.monitor.MonitorCluster;
import com.dojao.ui.monitor.MonitorColumn;
import com.dojao.ui.monitor.MonitorFunction;
import com.dojao.ui.monitor.MonitorJob;
import com.dojao.ui.monitor.MonitorQuery;
import com.dojao.ui.monitor.MonitorResult;
import com.dojao.ui.monitor.MonitorSubmit;
import com.dojao.ui.monitor.QueryMonitor;

import java.util.ArrayList;
import java.util.List;

/**
 * <b>AI 提交任务 LIMIT 500 硬上限探针</b>（纯计算 + 假 QueryMonitor，离线）。
 *
 * <p>钉住两件事：</p>
 * <ol>
 *   <li>{@link AiAgentService#applyAiRowLimit} 的改写规则：无 LIMIT 追加；已有更小 LIMIT 保持；
 *       更大 LIMIT 收紧到 500（OFFSET 保留）；子查询 / 字符串 / 注释里的 limit 不算；
 *       DDL / INSERT / SHOW / DESCRIBE / EXPLAIN 与多语句原样不动。</li>
 *   <li>AI 的两条提交路径（execute_sql、render_chart 的 sql 取数）<b>真的</b>把改写后的 SQL
 *       提交给集群 —— 用假 QueryMonitor 记录实际提交语句。</li>
 * </ol>
 *
 * <p><b>判别力</b>：把 {@link AiAgentService#applyAiRowLimit} 中和成直接返回原 SQL，
 * 追加 / 收紧用例立即 FAIL；把 execute_sql / render_chart 的调用改回原 SQL，集成用例 FAIL。</p>
 *
 * <p>跑法：{@code mvn -o -q -pl dojao-ui -am test-compile} 后直接 Run main()。</p>
 *
 * @author jiaxin
 * @date 2026/10/18
 */
public final class AiRowLimitProbeTest {

    private static int fail = 0;

    public static void main(String[] args) {
        testAppendWhenAbsent();
        testKeepOrTightenExisting();
        testQuotesCommentsSubqueries();
        testNonSelectUnchanged();
        testIntegrationThroughTools();
        System.out.println(fail == 0 ? "\n全部通过 ✅" : "\n失败 " + fail + " 项 ❌");
        if (fail > 0) {
            System.exit(1);
        }
    }

    /** 没有顶层 LIMIT：末尾追加 LIMIT 500。 */
    private static void testAppendWhenAbsent() {
        expect("普通 SELECT 追加", "select * from t LIMIT 500",
                AiAgentService.applyAiRowLimit("select * from t"));
        expect("聚合查询追加", "select count(*) from t LIMIT 500",
                AiAgentService.applyAiRowLimit("select count(*) from t"));
        expect("ORDER BY 之后追加", "select a from t order by a desc LIMIT 500",
                AiAgentService.applyAiRowLimit("select a from t order by a desc"));
        expect("CTE 查询追加", "with c as (select 1 as x) select * from c LIMIT 500",
                AiAgentService.applyAiRowLimit("with c as (select 1 as x) select * from c"));
        expect("大小写不敏感（SeLeCt）", "SeLeCt a FROM t LIMIT 500",
                AiAgentService.applyAiRowLimit("SeLeCt a FROM t"));
        expect("行首注释不影响判断", "-- 注释\nselect a from t LIMIT 500",
                AiAgentService.applyAiRowLimit("-- 注释\nselect a from t"));
        expect("块注释不影响判断", "/* c */ select a from t LIMIT 500",
                AiAgentService.applyAiRowLimit("/* c */ select a from t"));
        expect("收尾分号被剥掉后再追加", "select a from t LIMIT 500",
                AiAgentService.applyAiRowLimit("select a from t;  "));
    }

    /** 已有顶层 LIMIT：不超过 500 保持，超过 500 改成 500。 */
    private static void testKeepOrTightenExisting() {
        expect("已有更小 LIMIT 保持", "select * from t limit 10",
                AiAgentService.applyAiRowLimit("select * from t limit 10"));
        expect("已有 LIMIT 500 保持", "select * from t LIMIT 500",
                AiAgentService.applyAiRowLimit("select * from t LIMIT 500"));
        expect("更大 LIMIT 收紧到 500", "select * from t LIMIT 500",
                AiAgentService.applyAiRowLimit("select * from t LIMIT 1000"));
        expect("超大 LIMIT 收紧到 500", "select * from t LIMIT 500",
                AiAgentService.applyAiRowLimit("select * from t LIMIT 100000000"));
        expect("收紧时保留 OFFSET", "select * from t LIMIT 500 OFFSET 20",
                AiAgentService.applyAiRowLimit("select * from t LIMIT 1000 OFFSET 20"));
        expect("收紧时不改关键字大小写", "select * from t limit 500",
                AiAgentService.applyAiRowLimit("select * from t limit 999"));
        expect("分号 + 大 LIMIT 也收紧", "select * from t LIMIT 500",
                AiAgentService.applyAiRowLimit("select * from t LIMIT 1000;"));
        expect("CTE 外层大 LIMIT 收紧", "with c as (select 1 as x) select * from c LIMIT 500",
                AiAgentService.applyAiRowLimit("with c as (select 1 as x) select * from c LIMIT 9000"));
    }

    /** 字符串 / 注释 / 子查询里的 limit 不算外层限制。 */
    private static void testQuotesCommentsSubqueries() {
        expect("字符串里的 limit 不算", "select 'limit 9999' as s from t LIMIT 500",
                AiAgentService.applyAiRowLimit("select 'limit 9999' as s from t"));
        expect("行注释里的 limit 不算", "-- limit 9999\nselect a from t LIMIT 500",
                AiAgentService.applyAiRowLimit("-- limit 9999\nselect a from t"));
        expect("块注释里的 limit 不算", "/* limit 9999 */ select a from t LIMIT 500",
                AiAgentService.applyAiRowLimit("/* limit 9999 */ select a from t"));
        expect("子查询 LIMIT 不算外层限制", "select * from (select * from t limit 10) x LIMIT 500",
                AiAgentService.applyAiRowLimit("select * from (select * from t limit 10) x"));
        expect("反引号标识符 limit 不算", "select `limit` from t LIMIT 500",
                AiAgentService.applyAiRowLimit("select `limit` from t"));
    }

    /** 非 SELECT/WITH 与多语句：原样返回（不改写，避免破坏语句）。 */
    private static void testNonSelectUnchanged() {
        expect("INSERT 不动", "insert into t values (1)",
                AiAgentService.applyAiRowLimit("insert into t values (1)"));
        expect("DDL 不动", "create table t(x int)",
                AiAgentService.applyAiRowLimit("create table t(x int)"));
        expect("SHOW 不动", "show catalogs",
                AiAgentService.applyAiRowLimit("show catalogs"));
        expect("DESCRIBE 不动", "describe orders",
                AiAgentService.applyAiRowLimit("describe orders"));
        expect("EXPLAIN 不动", "explain select * from t",
                AiAgentService.applyAiRowLimit("explain select * from t"));
        expect("多语句不动", "select 1; select 2",
                AiAgentService.applyAiRowLimit("select 1; select 2"));
        expect("null → 空串", "", AiAgentService.applyAiRowLimit(null));
    }

    /** 集成：AI 的提交路径确实把 LIMIT 500 发给集群。 */
    private static void testIntegrationThroughTools() {
        FakeMonitor m = new FakeMonitor();
        AiAgentService svc = new AiAgentService(null, m, null);

        svc.executeSql("select city, amount from sales", false, "alice");
        check("execute_sql 实际提交带 LIMIT 500",
                m.submitted.size() == 1 && m.submitted.get(0).endsWith("LIMIT 500"));

        svc.executeSql("select * from t limit 10", false, "alice");
        check("execute_sql 已有限制时保持原样",
                m.submitted.size() == 2 && m.submitted.get(1).equals("select * from t limit 10"));

        svc.executeSql("select * from t limit 99999", false, "alice");
        check("execute_sql 收紧大 LIMIT",
                m.submitted.size() == 3 && m.submitted.get(2).equals("select * from t limit 500"));

        svc.executeSql("show catalogs", false, "alice");
        check("execute_sql 的 SHOW 不追加 LIMIT",
                m.submitted.size() == 4 && m.submitted.get(3).equals("show catalogs"));

        final String[] spec = {null};
        svc.renderChart("bar", "select city, amount from sales", null,
                "city", List.of("amount"), null, null, "t", "alice", s -> spec[0] = s);
        check("render_chart 实际提交带 LIMIT 500",
                m.submitted.size() == 5 && m.submitted.get(4).endsWith("LIMIT 500"));
        check("render_chart 正常产出 spec", spec[0] != null && spec[0].contains("\"bar\""));
    }

    private static void expect(String name, String want, String got) {
        check(name + "（实际 " + got + "）", want.equals(got));
    }

    private static void check(String name, boolean ok) {
        System.out.println((ok ? "  ✅ " : "  ❌ ") + name);
        if (!ok) {
            fail++;
        }
    }

    /** 假监控：记录提交的 SQL，返回一份两条数据的结果。 */
    private static final class FakeMonitor implements QueryMonitor {
        final List<String> submitted = new ArrayList<>();

        @Override
        public List<MonitorJob> listQueries() {
            return List.of();
        }

        @Override
        public MonitorQuery snapshot(String queryId) {
            return null;
        }

        @Override
        public MonitorCluster cluster() {
            return new MonitorCluster(null, List.of(), List.of(), List.of());
        }

        @Override
        public com.dojao.ui.monitor.MonitorHistory history(int maxPoints) {
            return null;
        }

        @Override
        public MonitorSubmit submit(String sql, String owner) {
            submitted.add(sql);
            return new MonitorSubmit("q" + submitted.size(), "SUBMITTED", "ok");
        }

        @Override
        public void cancel(String queryId) {
        }

        @Override
        public MonitorResult result(String queryId, int offset, int limit) {
            List<String> columns = List.of("city", "amount");
            List<List<String>> rows = List.of(List.of("北京", "10"), List.of("上海", "20"));
            return new MonitorResult(queryId, "FINISHED", columns, rows,
                    rows.size(), 0, false, null, null, 1);
        }

        @Override
        public List<String> listCatalogs(String viewer) {
            return List.of();
        }

        @Override
        public List<String> listSchemas(String catalog) {
            return List.of();
        }

        @Override
        public List<String> listTables(String catalog, String schema) {
            return List.of();
        }

        @Override
        public List<MonitorColumn> listColumns(String catalog, String schema, String table) {
            return List.of();
        }

        @Override
        public List<MonitorFunction> listFunctions(String viewer) {
            return List.of();
        }
    }
}
