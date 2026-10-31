package com.dojao.ai;

/**
 * AI 对话服务配置值对象。
 *
 * <p>配置单一来源 = UI 的 SQLite（{@code ui_ai_config_user}，一账号一行；未单独配置的账号
 * 回退管理员配置）。{@code dojao-ai} 只接收这个不可变值对象，自身不持久化、不依赖 dojao-core。</p>
 *
 * @param baseUrl     OpenAI 兼容端点基址（如 {@code https://api.openai.com/v1}）
 * @param apiKey      密钥（可为 null/空 = 未配置）
 * @param model       模型名（如 {@code gpt-4o-mini}）
 * @param temperature 采样温度
 */
public record AiConfig(String baseUrl, String apiKey, String model, double temperature) {

    /** 是否已配置（密钥非空即视为可用）。 */
    public boolean configured() {
        return apiKey != null && !apiKey.isBlank();
    }
}
