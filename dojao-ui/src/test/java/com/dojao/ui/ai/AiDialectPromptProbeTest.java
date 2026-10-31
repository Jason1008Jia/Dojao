package com.dojao.ui.ai;

import java.util.List;

/**
 * 探针：提交给大模型的系统提示词里<b>确实带上了 Dojao SQL 方言约束</b>（离线，纯字符串检查）。
 *
 * <p>动机：模型常按 MySQL/PostgreSQL 习惯写 SQL 导致引擎语法错误。约束必须真正拼进
 * {@link AiAgentService#SYSTEM_PROMPT}（而非只定义未使用），且在使用 Skill 时也不能丢。</p>
 *
 * <p><b>判别力</b>：若把 {@code SYSTEM_PROMPT} 末尾的 {@code + "\n\n" + DOJAO_SQL_DIALECT}
 * 拼接去掉（方言段不再随提示词下发），下面的「方言段已下发 / 关键约束存在」用例立即 FAIL。</p>
 */
public final class AiDialectPromptProbeTest {

    private static int fail = 0;

    public static void main(String[] args) {
        String p = AiAgentService.SYSTEM_PROMPT;

        // ---- 方言段确实被拼进最终提示词 ----
        check("方言段已下发（含标题）", p.contains("Dojao SQL 方言约定"));
        check("方言段与常量逐字一致", p.contains(AiAgentService.DOJAO_SQL_DIALECT));
        check("原有工具说明仍在（未被覆盖）", p.contains("execute_sql") && p.contains("render_chart"));

        // ---- 关键约束逐条存在 ----
        check("单引号字符串 / 双引号是标识符", p.contains("字符串只能用单引号"));
        check("表名三段式", p.contains("catalog.schema.table"));
        check("无 UPDATE / DELETE", p.contains("没有 UPDATE / DELETE"));
        check("无 || 拼接", p.contains("没有 || 字符串拼接"));
        check("无类型前缀字面量", p.contains("类型前缀字面量"));
        check("LIMIT m,n 不支持", p.contains("LIMIT m, n"));
        check("去重计数写法 COUNT(DISTINCT x)", p.contains("COUNT(DISTINCT"));
        check("CASE 强制搜索式模板", p.contains("CASE WHEN <条件表达式> THEN <值>"));
        check("CASE 明确禁止简单式", p.contains("严禁写简单式 CASE"));
        check("CASE 简单式改写指引", p.contains("CASE WHEN <表达式> = <值> THEN"));
        check("CASE WHEN 必须是完整布尔条件", p.contains("不能只写列名或字面量"));
        check("JOIN 必须带 ON", p.contains("连接必须带 ON"));
        check("窗口内禁聚合", p.contains("不得出现聚合函数"));
        check("DDL 写法要点", p.contains("CREATE CATALOG c USING"));
        check("提交前自查提示", p.contains("自查一遍"));

        // ---- 图表能力边界（只支持 11 种；越界礼貌致歉，不得偷换类型） ----
        String base = AiAgentService.composeBasePrompt(p, List.of());

        // ---- D1：DDL 能力口径必须与实现一致（报告 v4 §9） ----
        // 实现只认 CTAS / CATALOG / FUNCTION / DROP TABLE / TRUNCATE；CREATE TABLE(列定义)、
        // CREATE/DROP SCHEMA、ALTER TABLE、CREATE/DROP VIEW 都会抛「暂不支持执行的语句类型」。
        check("D1 方言段声明「DDL 能力（第一版）」", p.contains("DDL 能力（第一版）"));
        check("D1 明确可用集合（CTAS / DROP TABLE / TRUNCATE TABLE）",
                p.contains("CREATE TABLE c.s.t AS SELECT") && p.contains("DROP TABLE c.s.t")
                        && p.contains("TRUNCATE TABLE c.s.t"));
        check("D1 明确不支持 CREATE TABLE 显式列定义", p.contains("CREATE TABLE t (列定义 ...)"));
        check("D1 明确不支持 CREATE / DROP SCHEMA", p.contains("`CREATE SCHEMA` / `DROP SCHEMA`"));
        check("D1 明确不支持 ALTER TABLE", p.contains("`ALTER TABLE`（ADD / DROP / RENAME COLUMN）"));
        check("D1 明确不支持 CREATE / DROP VIEW", p.contains("`CREATE VIEW` / `DROP VIEW`"));
        check("D1 报错文案与实现一致", p.contains("暂不支持执行的语句类型"));
        check("D1 能力段同样声明未实现集合",
                base.contains("未实现") && base.contains("`CREATE / DROP SCHEMA`")
                        && base.contains("`ALTER TABLE`") && base.contains("`CREATE / DROP VIEW`"));

        check("图表能力边界段已下发", p.contains("图表能力边界"));
        check("能力段也提到图表限制", base.contains(AiAgentService.DOJAO_CAPABILITIES)
                && base.contains("radar / sankey / gauge / map / 3D 等**不支持**"));
        check("11 种支持类型逐项列出", p.contains("bar（柱状图）") && p.contains("line（折线图）")
                && p.contains("area（面积图）") && p.contains("pie（饼图）") && p.contains("scatter（散点图）")
                && p.contains("funnel（漏斗图）") && p.contains("treemap（矩形树图）") && p.contains("tree（树图）")
                && p.contains("heatmap（热力图）") && p.contains("graph（关系图）") && p.contains("dataset（数据集表格）"));
        check("明确列出不支持的类型", p.contains("radar（雷达图）") && p.contains("sankey（桑基图）")
                && p.contains("gauge（仪表盘）") && p.contains("map（地图）") && p.contains("3D 图表"));
        check("越界时禁止调用 render_chart", p.contains("不要调用 render_chart"));
        check("越界时禁止用其他类型顶替", p.contains("也不要用其他类型顶替"));
        check("越界固定致歉话术", p.contains("抱歉，Dojao 目前只支持"));
        check("支持清单与常量逐字一致", p.contains(AiAgentService.CHART_SUPPORT_SENTENCE));
        check("后端越界错误同口径", AiAgentService.unsupportedChartType("雷达图")
                .equals("抱歉，" + AiAgentService.CHART_SUPPORT_SENTENCE + "，暂不支持 雷达图。"));

        // ---- 用 Skill 时方言仍在（composeSystemPrompt 不吞掉基础约束） ----
        AiAgentService.Skill skill = new AiAgentService.Skill("财务分析", "只回答金额相关问题。", List.of());
        String composed = AiAgentService.composeSystemPrompt(p, skill);
        check("带 Skill 时方言仍在", composed.contains("Dojao SQL 方言约定"));
        check("带 Skill 时技能段也加上", composed.contains("财务分析"));

        System.out.println(fail == 0 ? "\n全部通过 ✅" : "\n失败 " + fail + " 项 ❌");
        if (fail > 0) {
            System.exit(1);
        }
    }

    private static void check(String name, boolean ok) {
        System.out.println((ok ? "  ✅ " : "  ❌ ") + name);
        if (!ok) {
            fail++;
        }
    }
}
