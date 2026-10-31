package com.dojao.ui.ai;

import com.dojao.ui.monitor.MonitorFunction;

import java.util.List;

/**
 * 探针：Dojao AI「能力边界 + 函数清单」前置告知（纯函数 / 常量，离线）。
 *
 * <p>覆盖：①{@link AiAgentService#DOJAO_CAPABILITIES} 含关键能力陈述（分布式 / 磁盘溢写 / 联邦 / 边界）；
 * ②{@link AiAgentService#buildFunctionCatalog} 按来源与 kind 分组（标量 / 聚合 / 窗口 / 自定义 UDF）、内置只列名、
 * UDF 带说明、未传入者不出现、空集返回空串；③{@link AiAgentService#composeBasePrompt} 确实把能力段与函数清单拼进
 * 系统提示词（空清单时只拼能力段）。</p>
 *
 * <p><b>判别力</b>：把 {@code buildFunctionCatalog} 的分组标题中和掉（只拼名字、无「标量/聚合/窗口」标签）→
 * 「分组标题存在」用例 FAIL；把 {@code composeBasePrompt} 中和成 {@code return systemPrompt;}（不拼能力段与清单）→
 * 「能力段已下发 / 清单已下发」用例 FAIL。</p>
 */
public final class AiCapabilitiesProbeTest {

    private static int fail = 0;

    public static void main(String[] args) {
        // ---- ① 能力/边界块 ----
        String cap = AiAgentService.DOJAO_CAPABILITIES;
        check("能力块非空", cap != null && !cap.isBlank());
        check("能力块：分布式定位", cap.contains("分布式"));
        check("能力块：磁盘溢写强项", cap.contains("磁盘溢写"));
        check("能力块：联邦查询", cap.contains("联邦"));
        check("能力块：跨库对数", cap.contains("跨库对数") && cap.contains("对账"));
        check("能力块：差集不支持 INTERSECT/EXCEPT", cap.contains("INTERSECT / EXCEPT / MINUS"));
        check("提示词：对数流程", AiAgentService.SYSTEM_PROMPT.contains("跨库对数")
                && AiAgentService.SYSTEM_PROMPT.contains("仅左有")
                && AiAgentService.SYSTEM_PROMPT.contains("FULL JOIN"));
        check("提示词：开场白含对数", AiAgentService.SYSTEM_PROMPT.contains("查数、对数"));
        check("能力块：无 UPDATE / DELETE", cap.contains("没有 UPDATE / DELETE"));
        check("能力块：无事务", cap.contains("事务"));
        check("能力块：指向函数清单", cap.contains("可用 SQL 函数"));

        // ---- 构造「张三可见集」：内置（标量/聚合/窗口）+ 自定义 UDF ----
        List<MonitorFunction> funcs = List.of(
                new MonitorFunction("upper", "builtin", "scalar", "", "upper(str) → string"),
                new MonitorFunction("regexp_extract", "builtin", "scalar", "", "regexp_extract(str, pattern, idx) → string"),
                new MonitorFunction("sum", "builtin", "aggregate", "", ""),
                new MonitorFunction("count_distinct", "builtin", "aggregate", "", ""),
                new MonitorFunction("row_number", "builtin", "window", "", ""),
                new MonitorFunction("s_extract_img_keywords", "custom", "", "python",
                        "从图片中提取关键字，输入图片二进制，返回空格分隔的关键词"));

        // ---- ② buildFunctionCatalog ----
        check("空集→空串", AiAgentService.buildFunctionCatalog(List.of()).isEmpty());
        check("null→空串", AiAgentService.buildFunctionCatalog(null).isEmpty());

        String cat = AiAgentService.buildFunctionCatalog(funcs);
        check("目录含标题", cat.contains("可用 SQL 函数"));
        check("目录含标量分组", cat.contains("标量函数"));
        check("目录含聚合分组", cat.contains("聚合函数"));
        check("目录含窗口分组", cat.contains("窗口函数"));
        check("目录含自定义分组", cat.contains("自定义函数"));
        check("标量组含内置名", cat.contains("upper") && cat.contains("regexp_extract"));
        check("聚合组含 sum", cat.contains("sum"));
        check("窗口组含 row_number", cat.contains("row_number"));
        // ★内部聚合键 count_distinct 必须改写成真实写法 count(distinct x)，绝不能当成函数名下发
        check("count_distinct 改写成 count(distinct x)", cat.contains("count(distinct x)"));
        check("目录不含内部键名 count_distinct", !cat.contains("count_distinct"));
        check("UDF 名与说明均出现",
                cat.contains("s_extract_img_keywords") && cat.contains("从图片中提取关键字"));
        check("未传入的他人私有函数不出现", !cat.contains("s_lisi_secret"));

        // ---- ③ composeBasePrompt 拼接 ----
        String baseWith = AiAgentService.composeBasePrompt(AiAgentService.SYSTEM_PROMPT, funcs);
        check("能力段已下发", baseWith.contains(AiAgentService.DOJAO_CAPABILITIES));
        check("函数清单已下发", baseWith.contains("## 可用 SQL 函数"));
        check("基础提示词仍在", baseWith.contains("execute_sql"));

        String baseEmpty = AiAgentService.composeBasePrompt(AiAgentService.SYSTEM_PROMPT, List.of());
        check("空清单仍下发能力段", baseEmpty.contains(AiAgentService.DOJAO_CAPABILITIES));
        check("空清单不下发函数清单标题", !baseEmpty.contains("## 可用 SQL 函数"));

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

    private AiCapabilitiesProbeTest() {
    }
}
