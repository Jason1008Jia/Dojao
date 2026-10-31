package com.dojao.ui.ai;

import com.dojao.ai.AiTool;
import dev.langchain4j.model.chat.request.json.JsonObjectSchema;

import java.util.List;

/**
 * 探针：{@link AiAgentService} 的 Skill 纯函数（离线，无需模型 / 集群）——
 * 系统提示词合成 {@code composeSystemPrompt} + 工具白名单过滤 {@code filterTools}。
 *
 * <p><b>判别力</b>：把 {@code filterTools} 的 {@code names.isEmpty()} 分支去掉，
 * 「空白名单返回全部」用例立即 FAIL；把 {@code composeSystemPrompt} 对空提示词的短路去掉，
 * 「空技能提示词不追加」用例 FAIL。</p>
 */
public final class AiSkillComposeProbeTest {

    private static int fail = 0;

    public static void main(String[] args) {
        String base = "BASE";
        AiAgentService.Skill noPrompt = new AiAgentService.Skill("报表", "   ", List.of());
        AiAgentService.Skill withPrompt = new AiAgentService.Skill("报表", "只统计金额", List.of());
        AiAgentService.Skill noName = new AiAgentService.Skill(null, "自定义指令", null);

        // ---- composeSystemPrompt ----
        check("null 技能 → 基础提示词原样", "BASE".equals(AiAgentService.composeSystemPrompt(base, null)));
        check("空白提示词 → 不追加", "BASE".equals(AiAgentService.composeSystemPrompt(base, noPrompt)));
        String composed = AiAgentService.composeSystemPrompt(base, withPrompt);
        check("有提示词 → 追加技能段", composed.startsWith("BASE\n\n") && composed.contains("【当前技能：报表】")
                && composed.endsWith("只统计金额"));
        String composedNoName = AiAgentService.composeSystemPrompt(base, noName);
        check("无名称 → 通用表头", composedNoName.contains("【当前技能】") && composedNoName.endsWith("自定义指令"));

        // ---- filterTools ----
        List<AiTool> all = List.of(tool("execute_sql"), tool("execute_ddl"), tool("render_chart"),
                tool("list_catalogs"), tool("list_tables"), tool("describe_table"));

        check("null 白名单 → 全部", AiAgentService.filterTools(all, null) == all);
        check("空白名单 → 全部", AiAgentService.filterTools(all, List.of()) == all);

        List<AiTool> one = AiAgentService.filterTools(all, List.of("execute_sql"));
        check("单选白名单 → 恰 1 个", one.size() == 1 && "execute_sql".equals(one.get(0).name()));

        List<AiTool> two = AiAgentService.filterTools(all, List.of("execute_sql", "render_chart"));
        check("多选白名单 → 2 个且保序",
                two.size() == 2 && "execute_sql".equals(two.get(0).name())
                        && "render_chart".equals(two.get(1).name()));

        check("未知工具名被忽略", AiAgentService.filterTools(all, List.of("no_such_tool")) == all);
        check("部分命中：忽略未知、保留已知",
                AiAgentService.filterTools(all, List.of("no_such_tool", "describe_table")).size() == 1);

        System.out.println(fail == 0 ? "\n全部通过 ✅" : "\n失败 " + fail + " 项 ❌");
        if (fail > 0) {
            System.exit(1);
        }
    }

    private static AiTool tool(String name) {
        return new AiTool(name, "", JsonObjectSchema.builder().build(), args -> "");
    }

    private static void check(String name, boolean ok) {
        System.out.println((ok ? "  ✅ " : "  ❌ ") + name);
        if (!ok) {
            fail++;
        }
    }
}
