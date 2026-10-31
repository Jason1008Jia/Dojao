package com.dojao.ui.ai;

import com.dojao.ai.AiTool;
import dev.langchain4j.model.chat.request.json.JsonObjectSchema;

import java.lang.reflect.Method;
import java.util.List;

/**
 * 探针：{@code execute_sql(includeTiming=true)} 的耗时回传（离线）。
 *
 * <p>覆盖：①{@link AiAgentService#timingHint} 的格式化（毫秒 / 输出行数 / 行每秒的千分位、零耗时、零行）；
 * ②工具 JSON Schema 确实暴露可选布尔参数 {@code includeTiming}，且只有 {@code sql} 是必填 ——
 * 模型能看见才可能显式开启；③提示词里写明了开启方式（否则模型不会用）。</p>
 *
 * <p><b>判别力</b>：把 {@code timingHint} 中和成 {@code return "";} → 全部格式化用例 FAIL；
 * 从 schema 删掉 {@code addBooleanProperty("includeTiming", ...)} → schema 用例 FAIL。</p>
 */
public final class AiTimingProbeTest {

    private static int fail = 0;

    public static void main(String[] args) throws Exception {
        // ---- ① 耗时摘要格式化 ----
        String normal = AiAgentService.timingHint(1234L, 10_000L);
        check("含毫秒（千分位）: " + normal, normal.contains("1,234 ms"));
        check("含输出行数（千分位）", normal.contains("输出 10,000 行"));
        check("含吞吐（10000/1234ms ≈ 8104 行/秒）", normal.contains("约 8,104 行/秒"));
        check("单行摘要（无换行嵌套）", normal.indexOf('\n') == normal.lastIndexOf('\n'));

        String zeroRows = AiAgentService.timingHint(2000L, 0L);
        check("零输出行 → 0 行/秒", zeroRows.contains("约 0 行/秒"));

        String slow = AiAgentService.timingHint(2_000_000L, 1L);
        check("大耗时千分位: " + slow, slow.contains("2,000,000 ms"));

        String unknown = AiAgentService.timingHint(0L, 100L);
        check("零耗时 → 明确不可用而非 0 ms", unknown.contains("不可用"));
        check("零耗时不出现 0 ms", !unknown.contains("0 ms"));

        // ---- ② 工具 schema：includeTiming 是可选布尔参数 ----
        List<AiTool> tools = buildToolsReflectively();
        AiTool executeSql = tools.stream()
                .filter(t -> "execute_sql".equals(t.name()))
                .findFirst().orElse(null);
        check("存在 execute_sql 工具", executeSql != null);
        if (executeSql != null) {
            JsonObjectSchema schema = executeSql.parameters();
            check("schema 暴露 includeTiming", schema.properties().containsKey("includeTiming"));
            check("includeTiming 是可选参数（required 只有 sql）",
                    schema.required().size() == 1 && schema.required().contains("sql"));
            check("工具描述提到 includeTiming",
                    executeSql.description() != null && executeSql.description().contains("includeTiming"));
        }

        // ---- ③ 提示词告知模型何时开启 ----
        check("SYSTEM_PROMPT 提到 includeTiming",
                AiAgentService.SYSTEM_PROMPT.contains("includeTiming"));
        check("SYSTEM_PROMPT 说明用于性能评估",
                AiAgentService.SYSTEM_PROMPT.contains("性能"));

        System.out.println(fail == 0 ? "\n全部通过 ✅" : "\n失败 " + fail + " 项 ❌");
        if (fail > 0) {
            System.exit(1);
        }
    }

    private static List<AiTool> buildToolsReflectively() throws Exception {
        AiAgentService service = new AiAgentService(null, null, null);
        Method m = AiAgentService.class.getDeclaredMethod("buildTools", String.class,
                AiAgentService.DdlConfirm.class, AiAgentService.ChartSink.class, AiAgentService.FileSink.class);
        m.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<AiTool> tools = (List<AiTool>) m.invoke(service, "probe", null, null, null);
        return tools;
    }

    private static void check(String name, boolean ok) {
        System.out.println((ok ? "  ✅ " : "  ❌ ") + name);
        if (!ok) {
            fail++;
        }
    }

    private AiTimingProbeTest() {
    }
}
