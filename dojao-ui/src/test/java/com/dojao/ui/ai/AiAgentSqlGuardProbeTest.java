package com.dojao.ui.ai;

/**
 * 探针：{@link AiAgentService#isReadOnlySql} 只读 SQL 守卫（安全关键，离线）。
 *
 * <p>放行 SELECT/SHOW/DESCRIBE/EXPLAIN（含大小写、前后空白、行首注释）；拒绝一切 DML/DDL。
 * 另覆盖 {@link AiAgentService#isDdlSql} 的 execute_ddl 守卫：只放行 CREATE/DROP/ALTER/TRUNCATE。</p>
 *
 * <p><b>判别力</b>：把 {@link AiAgentService} 的 READ_ONLY_KEYWORDS 改成包含 "DELETE"，或让判据
 * 恒返回 true，「拒绝 DELETE/DROP/…」用例立即 FAIL；DDL 侧同理（放开 DDL_KEYWORDS 时拒绝用例 FAIL）。</p>
 */
public final class AiAgentSqlGuardProbeTest {

    private static int fail = 0;

    public static void main(String[] args) {
        // ---- 放行（只读）----
        allow("select * from t", "select * from t");
        allow("小写 + 前导空白", "   select 1");
        allow("行注释后是 select", "-- 注释\nselect 1");
        allow("块注释后是 select", "/* c */ select 1");
        allow("SHOW", "show catalogs");
        allow("DESCRIBE", "describe orders");
        allow("DESC", "desc orders");
        allow("EXPLAIN", "explain select 1");
        // CTE（WITH ... SELECT）是只读的 —— 复杂分析查询常用，不能因首词不是 SELECT 被误拒。
        allow("WITH ... SELECT（CTE）", "with c as (select 1 as x) select * from c");
        allow("WITH 大写", "WITH c AS (SELECT 1 AS x) SELECT * FROM c");
        allow("行注释后是 WITH", "-- 注释\nwith c as (select 1 as x) select * from c");

        // ---- 拒绝（写入/DDL）----
        deny("DELETE", "delete from t");
        deny("UPDATE", "update t set a = 1");
        deny("INSERT", "insert into t values (1)");
        deny("DROP", "drop table t");
        deny("CREATE", "create table t(a int)");
        deny("TRUNCATE", "truncate table t");
        deny("ALTER", "alter table t add c int");
        deny("注释伪装 DROP", "-- hello\ndrop table t");
        deny("空串", "   ");
        deny("null", null);
        // 批量提交（分号分隔）不得绕过只读守卫：AI 工具每次只允许一条只读语句。
        deny("多语句只读也不放行（AI 单条约定）", "select 1; select 2");
        deny("SELECT 夹带 INSERT", "select 1; insert into t values (1)");
        deny("SELECT 夹带 DROP", "select 1; drop table t");
        allow("字符串内分号仍是单条", "select 'a;b'");

        // ---- execute_ddl 守卫：只放行 CREATE/DROP/ALTER/TRUNCATE，拒绝只读与 DML ----
        ddlAllow("CREATE TABLE", "create table t(a int)");
        ddlAllow("CREATE CATALOG", "CREATE CATALOG hive");
        ddlAllow("DROP TABLE", "drop table t");
        ddlAllow("ALTER", "alter table t add c int");
        ddlAllow("TRUNCATE", "truncate table t");
        ddlAllow("行注释后是 DROP", "-- c\ndrop table t");
        ddlDeny("SELECT", "select 1");
        ddlDeny("SHOW", "show catalogs");
        ddlDeny("DELETE", "delete from t");
        ddlDeny("INSERT", "insert into t values (1)");
        ddlDeny("UPDATE", "update t set a = 1");
        ddlDeny("空串", "  ");
        ddlDeny("null", null);
        ddlDeny("多语句 DDL 也不放行（AI 单条约定）", "create table a(x int); drop table b");

        System.out.println(fail == 0 ? "\n全部通过 ✅" : "\n失败 " + fail + " 项 ❌");
        if (fail > 0) {
            System.exit(1);
        }
    }

    private static void allow(String name, String sql) {
        check("放行 " + name, AiAgentService.isReadOnlySql(sql));
    }

    private static void deny(String name, String sql) {
        check("拒绝 " + name, !AiAgentService.isReadOnlySql(sql));
    }

    private static void ddlAllow(String name, String sql) {
        check("DDL 放行 " + name, AiAgentService.isDdlSql(sql));
    }

    private static void ddlDeny(String name, String sql) {
        check("DDL 拒绝 " + name, !AiAgentService.isDdlSql(sql));
    }

    private static void check(String name, boolean ok) {
        System.out.println((ok ? "  ✅ " : "  ❌ ") + name);
        if (!ok) {
            fail++;
        }
    }
}
