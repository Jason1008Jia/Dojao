package com.dojao.ui.ai;

import com.dojao.metastore.MetaStore;
import com.dojao.spi.bean.FunctionDefinition;
import com.dojao.ui.monitor.MonitorFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 探针：AI 的 dojao 函数能力（{@link AiAgentService#describeFunctionFrom}，离线纯函数）+ 用户级隔离。
 *
 * <p>覆盖：①describe 命中自定义 / 内置（大小写不敏感、给调用提示）；②describe 未命中（他人私有 / 不存在 / 空名）→ 明确拒绝文案。</p>
 *
 * <p>★函数「检索」能力已改为<b>前置注入系统提示词</b>（{@link AiAgentService#buildFunctionCatalog}，见
 * {@code AiCapabilitiesProbeTest}），不再有 {@code list_functions} 工具；本类只保留 describe_function 与隔离语义。</p>
 *
 * <p><b>用户级隔离</b>：本类用真实的 {@link MetaStore#visibleTo} 语义构造「张三可见集」，证明公共函数与张三自己的私有函数可见、
 * 李四的私有函数不可见；且本服务只信任传入的可见集（未命中即拒绝），绝不越权。</p>
 *
 * <p><b>判别力</b>：把 {@code describeFunctionFrom} 的未命中分支改成返回空串（或恒命中）→ 用例 ②「拒绝他人私有 / 不存在」FAIL。</p>
 */
public final class AiFunctionRetrievalProbeTest {

    private static int fail = 0;

    public static void main(String[] args) {
        // ---- 隔离语义：用真实的 MetaStore.visibleTo 判定「谁可见」 ----
        // 公共函数（未标 is_private）
        FunctionDefinition pub = def("s_normalize", null, false);
        // 张三的私有函数（用户场景：识别图片关键字的小模型）
        FunctionDefinition zhangsanPrivate =
                def("s_extract_img_keywords", "张三", true);
        // 李四的私有函数
        FunctionDefinition lisiPrivate = def("s_lisi_secret", "李四", true);

        check("隔离：公共函数对张三可见", MetaStore.visibleTo("张三", pub.owner(), pub.properties()));
        check("隔离：张三自己的私有函数可见",
                MetaStore.visibleTo("张三", zhangsanPrivate.owner(), zhangsanPrivate.properties()));
        check("隔离：李四的私有函数对张三不可见",
                !MetaStore.visibleTo("张三", lisiPrivate.owner(), lisiPrivate.properties()));

        // 模拟 server 侧按 viewer 过滤后的「张三可见集」（= monitor.listFunctions("张三")）
        List<MonitorFunction> viewerFuncs = new ArrayList<>();
        viewerFuncs.add(new MonitorFunction("s_normalize", "custom", "", "python", "把字符串做归一化"));
        viewerFuncs.add(new MonitorFunction("s_extract_img_keywords", "custom", "", "python",
                "从图片中提取关键字，输入图片二进制，返回空格分隔的关键词"));
        // builtin 采样
        viewerFuncs.add(new MonitorFunction("upper", "builtin", "scalar", "", "upper(str) → string"));
        viewerFuncs.add(new MonitorFunction("regexp_extract", "builtin", "scalar", "",
                "regexp_extract(str, pattern, idx) → string"));
        // 注意：李四的私有函数不在可见集里

        // ---- ① describe 命中 ----
        String d1 = AiAgentService.describeFunctionFrom(viewerFuncs, "s_extract_img_keywords");
        check("① describe 自定义：含名", d1.contains("s_extract_img_keywords"));
        check("① describe 自定义：来源为自定义", d1.contains("自定义"));
        check("① describe 自定义：含说明", d1.contains("从图片中提取关键字"));
        check("① describe 自定义：给调用提示", d1.contains("SELECT s_extract_img_keywords("));
        check("① describe 大小写不敏感", AiAgentService.describeFunctionFrom(viewerFuncs, "UPPER").contains("内置"));

        // ---- ② describe 未命中（他人私有 / 不存在）→ 拒绝 ----
        String d2 = AiAgentService.describeFunctionFrom(viewerFuncs, "s_lisi_secret");
        check("② describe 他人私有 → 拒绝（无权使用）", d2.contains("无权使用"));
        check("② describe 不存在 → 拒绝", AiAgentService.describeFunctionFrom(viewerFuncs, "nope").contains("未找到"));
        check("② describe 空名 → 提示补名",
                AiAgentService.describeFunctionFrom(viewerFuncs, "  ").contains("需要提供函数名"));

        System.out.println(fail == 0 ? "\n全部通过 ✅" : "\n失败 " + fail + " 项 ❌");
        if (fail > 0) {
            System.exit(1);
        }
    }

    /** 构造函数定义（properties 仅按需带 is_private）。 */
    private static FunctionDefinition def(String name, String owner, boolean isPrivate) {
        Map<String, String> props = isPrivate ? Map.of("is_private", "true") : Map.of();
        return new FunctionDefinition(name, "python", "/tmp/" + name + ".py", null,
                props, "", null, null, owner);
    }

    private static void check(String name, boolean ok) {
        System.out.println((ok ? "  ✅ " : "  ❌ ") + name);
        if (!ok) {
            fail++;
        }
    }
}
