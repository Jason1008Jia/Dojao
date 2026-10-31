package com.dojao.ai;

/**
 * ReAct 推理过程的流式回调 —— 每类事件到达即触发，供上层（UI）实时「展开」给用户看。
 *
 * <p>回调的执行线程 = LangChain4j 流式线程（非调用线程），实现方自行保证线程安全 / 转交。
 * 除 {@link #onText}/{@link #onThinking} 会多次触发外，{@link #onComplete}/{@link #onError} 恰好其一、且必触发一次。</p>
 */
public interface ReActListener {

    /** 正文流式片段（模型的可见输出）。 */
    default void onText(String text) {
    }

    /** 推理模型的「思考」流（如 DeepSeek-R1 / o 系列；普通模型不会触发）。 */
    default void onThinking(String text) {
    }

    /** 模型决定调用某工具（在真正执行前触发）。{@code argumentsJson} = 模型给的参数 JSON。 */
    default void onToolCall(String name, String argumentsJson) {
    }

    /** 工具执行返回结果（截断后的文本）。 */
    default void onToolResult(String name, String resultText) {
    }

    /** 整轮推理正常结束。 */
    default void onComplete() {
    }

    /** 整轮推理出错（网络 / 鉴权 / 超时等）。 */
    default void onError(Throwable t) {
    }
}
