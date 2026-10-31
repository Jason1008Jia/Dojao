package com.dojao.ai;

import dev.langchain4j.agent.tool.ToolSpecification;
import dev.langchain4j.model.chat.request.json.JsonObjectSchema;

/**
 * 一个可被 LLM 调用的工具：名称 + 描述 + 参数的 JSON Schema + 执行器。
 *
 * <p>{@code dojao-ai} 只认这个抽象，不关心工具后端是 SQL 引擎还是别的；具体工具（执行 SQL / 查目录）
 * 由上层模块（{@code dojao-ui}）构造注入，从而让 {@code dojao-ai} 零 dojao-metastore/ui 依赖。</p>
 *
 * @param name        工具名（LLM 用它发起调用，须唯一）
 * @param description 自然语言描述（LLM 据此决定何时调用）
 * @param parameters  参数 JSON Schema
 * @param executor    执行器：入参 = 模型给的 JSON 字符串，返回 = 结果文本
 */
public record AiTool(String name, String description, JsonObjectSchema parameters, ToolExecutor executor) {

    /** 工具执行器：{@code argumentsJson} 是模型生成的参数对象 JSON。 */
    @FunctionalInterface
    public interface ToolExecutor {
        String execute(String argumentsJson) throws Exception;
    }

    /** 转成 LangChain4j 的工具规格（放进 {@code ChatRequest.toolSpecifications}）。 */
    public ToolSpecification toSpecification() {
        return ToolSpecification.builder()
                .name(name)
                .description(description)
                .parameters(parameters)
                .build();
    }
}
