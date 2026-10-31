package com.dojao.ui.ai;

import java.util.List;

/**
 * <b>AI 回答边界探针</b>（纯字符串，离线）：钉住「非数据实质问题回固定模板」这条系统提示词约定。
 *
 * <p>用户要求：问数据之外的<b>实质性</b>问题，必须回复
 * 「Dojao 您这个问题 Dojao 还不太明白，请您找一个其他问题吧，比如&lt;结合上下文推测的具体数据问题&gt;」，
 * 不得调用工具、不得展开。但「你好」「hi」「谢谢」这类<b>问候寒暄</b>与「你是谁 / 你能做什么」属于职责范围，
 * 必须正常友好回应，<b>不得套用拒绝模板</b>（否则用户说「你好 dojao」都会被拒，体验极差）。</p>
 *
 * <p><b>判别力</b>：删掉 {@link AiAgentService#SYSTEM_PROMPT} 里的「★回答边界」段（或把模板句 / 问候例外改掉），
 * 对应用例立即 FAIL。</p>
 *
 * <p>跑法：{@code mvn -o -q -pl dojao-ui -am test-compile} 后直接 Run main()。</p>
 *
 * @author jiaxin
 * @date 2026/10/18
 */
public final class AiBoundaryPromptProbeTest {

    private static int fail = 0;

    public static void main(String[] args) {
        String prompt = AiAgentService.SYSTEM_PROMPT;

        check("提示词含固定拒绝模板", prompt.contains("Dojao 您这个问题 Dojao 还不太明白"));
        check("提示词含引导语「请您找一个其他问题吧」", prompt.contains("请您找一个其他问题吧，比如"));
        check("拒绝时不得调用工具", prompt.contains("不要调用任何工具"));
        check("要求结合上下文推测示例", prompt.contains("结合上文") || prompt.contains("上下文中"));
        check("职责范围定义（数据类问题）",
                prompt.contains("你的职责是处理与 Dojao") && prompt.contains("数据"));
        check("给出上下文示例（orders）", prompt.contains("orders"));
        check("非数据实质问题坚持同一句，不追问", prompt.contains("只回这一句"));

        // ★问候 / 寒暄必须正常回应，不得套拒绝模板（「你好 dojao」被拒的回归）。
        check("问候寒暄属职责范围、正常友好回应",
                prompt.contains("问候与寒暄") && prompt.contains("属于职责范围，正常友好地回应"));
        check("问候寒暄明确列出示例（你好 / hi / 谢谢）",
                prompt.contains("你好") && prompt.contains("hi") && prompt.contains("谢谢"));
        check("问候寒暄不得套用拒绝模板",
                prompt.contains("不要套用下面的拒绝模板"));
        check("「你是谁 / 你能做什么」不算闲聊",
                prompt.contains("你是谁") && prompt.contains("你能做什么"));
        check("拒绝模板只针对「与数据无关的实质性问题」",
                prompt.contains("与数据无关的实质性问题")
                        && !prompt.contains("任何闲聊、问候、与数据无关的提问"));

        // 最终发给模型的提示词 = SYSTEM_PROMPT + 能力段 +（函数清单），边界段必须原样保留。
        String composed = AiAgentService.composeBasePrompt(prompt, List.of());
        check("composeBasePrompt 后仍含拒绝模板", composed.contains("Dojao 您这个问题 Dojao 还不太明白"));
        check("SYSTEM_PROMPT 仍含方言段（未被破坏）",
                composed.contains("Dojao SQL 方言约定"));

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
