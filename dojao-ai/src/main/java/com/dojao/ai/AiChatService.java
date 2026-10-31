package com.dojao.ai;

import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.openai.OpenAiChatModel;

import java.time.Duration;

/**
 * 单轮对话服务：把一条用户消息交给 LLM，整段返回回复文本（同步，非流式）。
 *
 * <p>里程碑 1 直接用低层 {@link OpenAiChatModel} + {@link ChatModel#chat(String)}，
 * 暂不上 AiServices（上下文记忆 / ReAct / 工具 / MCP 留待后续里程碑）。</p>
 */
public class AiChatService {

    private final AiConfig config;
    private volatile ChatModel model;

    public AiChatService(AiConfig config) {
        this.config = config;
    }

    /** 惰性构造 LangChain4j 模型（首次调用时建，之后复用）。 */
    private ChatModel model() {
        ChatModel m = model;
        if (m == null) {
            synchronized (this) {
                m = model;
                if (m == null) {
                    m = OpenAiChatModel.builder()
                            .baseUrl(config.baseUrl())
                            .apiKey(config.apiKey())
                            .modelName(config.model())
                            .temperature(config.temperature())
                            .timeout(Duration.ofSeconds(60))
                            .build();
                    model = m;
                }
            }
        }
        return m;
    }

    /**
     * 发送一条消息并返回整段回复。
     *
     * @throws IllegalStateException 未配置密钥
     * @throws RuntimeException      网络 / 鉴权等异常向上抛（UI 层转 500 文案）
     */
    public String chat(String message) {
        if (!config.configured()) {
            throw new IllegalStateException("AI 未配置：请在「AI 模型管理」填写服务地址与密钥");
        }
        return model().chat(message);
    }
}
