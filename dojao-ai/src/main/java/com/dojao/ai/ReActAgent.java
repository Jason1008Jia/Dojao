package com.dojao.ai;

import dev.langchain4j.agent.tool.ToolExecutionRequest;
import dev.langchain4j.agent.tool.ToolSpecification;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.data.message.SystemMessage;
import dev.langchain4j.data.message.ToolExecutionResultMessage;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.chat.StreamingChatModel;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.response.ChatResponse;
import dev.langchain4j.model.chat.response.PartialThinking;
import dev.langchain4j.model.chat.response.StreamingChatResponseHandler;
import dev.langchain4j.model.openai.OpenAiStreamingChatModel;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 手写的 ReAct（Reason + Act）推理循环 —— 执行一条用户消息的完整「思考 → 工具 → 观察 → 再思考 → 结论」，
 * 并把过程中每个事件<b>流式</b>回调给 {@link ReActListener}（上层据此像 Claude Code 那样「展开」推理轨迹）。
 *
 * <p><b>为什么手写而非 LangChain4j AiServices</b>：需要逐事件（正文 / 思考 / 工具调用 / 工具结果）向外吐，
 * 且对任意 OpenAI 兼容端点的兼容性最好。</p>
 *
 * <p><b>阻塞语义</b>：{@link #run} 是<b>阻塞</b>方法（内部用 {@link CountDownLatch} 把异步流式调用转成同步等待），
 * 调用方须放在工作线程执行。</p>
 */
public final class ReActAgent {

    /** 单轮 LLM 调用的最长等待（含流式输出）。 */
    private static final Duration CALL_TIMEOUT = Duration.ofSeconds(120);

    /** 单条工具结果回填给模型的最大长度（防止观测撑爆上下文）。 */
    private static final int MAX_TOOL_RESULT_CHARS = 4000;

    /** 未显式指定时的最大推理步数（达到后仍会强制模型收尾作答，不会缺答案就停）。 */
    private static final int DEFAULT_MAX_ITERATIONS = 16;

    private final StreamingChatModel model;
    private final List<AiTool> tools;
    private final Map<String, AiTool> toolByName = new HashMap<>();
    private final String systemPrompt;
    private final int maxIterations;

    public ReActAgent(StreamingChatModel model, List<AiTool> tools, String systemPrompt, int maxIterations) {
        this.model = model;
        this.tools = tools == null ? List.of() : tools;
        for (AiTool t : this.tools) {
            this.toolByName.put(t.name(), t);
        }
        this.systemPrompt = systemPrompt;
        this.maxIterations = maxIterations <= 0 ? DEFAULT_MAX_ITERATIONS : maxIterations;
    }

    /** 从配置建一个开箱即用的 Agent（内部构造 {@link OpenAiStreamingChatModel}）。 */
    public static ReActAgent fromConfig(AiConfig cfg, List<AiTool> tools, String systemPrompt) {
        StreamingChatModel model = OpenAiStreamingChatModel.builder()
                .baseUrl(cfg.baseUrl())
                .apiKey(cfg.apiKey())
                .modelName(cfg.model())
                .temperature(cfg.temperature())
                .timeout(CALL_TIMEOUT)
                .build();
        return new ReActAgent(model, tools, systemPrompt, DEFAULT_MAX_ITERATIONS);
    }

    /**
     * 跑一轮完整推理，事件流式回调给 {@code listener}（阻塞直到结束或出错）。
     *
     * @param userMessage 用户提问
     * @param listener    流式事件接收器
     */
    public void run(String userMessage, ReActListener listener) {
        run(null, userMessage, listener);
    }

    /**
     * 跑一轮完整推理（带多轮对话历史），事件流式回调给 {@code listener}（阻塞直到结束或出错）。
     *
     * <p>{@code history} 是本次提问之前的历史会话消息（UserMessage / AiMessage 交替，<b>不含</b>
     * SystemMessage —— SystemMessage 由本方法按 {@code systemPrompt} 统一下发）。历史会作为
     * 前缀拼在新用户消息之前，让模型「记得」前文，从而实现长对话。</p>
     *
     * @param history     历史消息（可为 null / 空 = 单轮）
     * @param userMessage 本轮用户提问
     * @param listener    流式事件接收器
     */
    public void run(List<ChatMessage> history, String userMessage, ReActListener listener) {
        try {
            List<ChatMessage> messages = new ArrayList<>();
            if (systemPrompt != null && !systemPrompt.isBlank()) {
                messages.add(SystemMessage.systemMessage(systemPrompt));
            }
            if (history != null && !history.isEmpty()) {
                messages.addAll(history);
            }
            messages.add(UserMessage.userMessage(userMessage));

            List<ToolSpecification> specs = tools.stream().map(AiTool::toSpecification).toList();

            // 已出现过的工具调用签名（名 + 归一化参数）：用于识别「重复调用」（模型卡住反复跑同一 SQL）。
            Set<String> seenCalls = new HashSet<>();
            // 收敛提醒点：用了约 2/3 步数后，注入一次「尽快作答」提示，逼模型收敛（避免一直跑 SQL 不出结论）。
            int nudgeAt = Math.max(2, maxIterations * 2 / 3);

            for (int i = 0; i < maxIterations; i++) {
                if (i == nudgeAt) {
                    messages.add(UserMessage.userMessage(
                            "（系统提示：你已完成多步探查。请尽快基于已获得的信息收敛出最终回答，"
                                    + "不要重复执行相同的查询。）"));
                }
                AiMessage ai = callOnce(messages, specs, listener);
                messages.add(ai);

                if (!ai.hasToolExecutionRequests()) {
                    // 模型收敛：正常情况下正文已由流式回调逐字吐出；若最终一条正文为空（少数端点/模型），
                    // 补一句兜底文案，避免前端「有推理链、无回答」的空白。
                    if (ai.text() == null || ai.text().isBlank()) {
                        listener.onText("（模型未给出文本回答，请重试或换一种问法。）");
                    }
                    listener.onComplete();
                    return;
                }
                boolean progressed = false;
                for (ToolExecutionRequest req : ai.toolExecutionRequests()) {
                    String sig = callSignature(req);
                    if (seenCalls.add(sig)) {
                        progressed = true;
                        String result = executeTool(req, listener);
                        // ★ 关键：把工具结果作为 ToolExecutionResultMessage 回填，下一轮模型才能「看到」观察结果。
                        messages.add(ToolExecutionResultMessage.from(req, result));
                    } else {
                        // 与之前完全相同的调用：不再重复执行（否则就是死循环跑同一条 SQL），
                        // 只回填提示，让模型改用其它查询或直接作答。
                        String dup = "（该调用与之前完全相同，结果同上，已在前面给出；"
                                + "请勿重复相同查询，请基于已有结果作答，或改用不同的查询。）";
                        listener.onToolResult(req.name(), dup);
                        messages.add(ToolExecutionResultMessage.from(req, dup));
                    }
                }
                if (!progressed) {
                    // 本轮所有调用都是重复的 → 模型已卡住，直接跳出收尾作答（不空耗到步数上限）。
                    break;
                }
            }
            // ★ 步数用尽 / 模型卡住：不直接停止 —— 禁用工具再调一次，逼模型用已获得的观察信息收敛出最终答案。
            forceFinalAnswer(messages, listener);
            listener.onComplete();
        } catch (Exception e) {
            listener.onError(e);
        }
    }

    /** 工具调用签名：工具名 + 归一化参数（去首尾空白、内部连续空白压成单空格）—— 识别「重复调用」。 */
    private static String callSignature(ToolExecutionRequest req) {
        String args = req.arguments() == null ? "" : req.arguments().trim().replaceAll("\\s+", " ");
        return req.name() + "\u0000" + args;
    }

    /**
     * 达到最大推理步数后的收尾：<b>禁用工具</b>再调一次模型，并在消息里明确要求「基于已有信息立即作答」，
     * 让模型无法再靠调工具拖延，必须给出最终答复。
     *
     * <p>仅当模型在<b>禁用工具</b>的情况下仍返回工具调用请求（少数端点会「硬塞」）才兜底补一句提示 ——
     * 正常情况正文已经由流式回调逐字吐出，这里不再重复输出，避免与 {@code onPartialResponse} 的内容重复。</p>
     */
    private void forceFinalAnswer(List<ChatMessage> messages, ReActListener listener) throws InterruptedException {
        List<ChatMessage> finalMessages = new ArrayList<>(messages);
        finalMessages.add(UserMessage.userMessage(
                "（系统提示：工具调用步数已达上限，请立即基于以上已获得的信息给出最终回答，不要再请求调用任何工具。）"));
        AiMessage ai = callOnce(finalMessages, List.of(), listener);
        if (ai.hasToolExecutionRequests()) {
            listener.onText("\n\n（推理步数已达上限，且模型仍尝试调用工具，已停止。）");
        } else if (ai.text() == null || ai.text().isBlank()) {
            listener.onText("\n\n（模型未给出文本回答，请重试或换一种问法。）");
        }
    }

    /** 单轮流式调用：边流边回调，阻塞等整条响应完成后返回 {@link AiMessage}。 */
    private AiMessage callOnce(List<ChatMessage> messages, List<ToolSpecification> specs, ReActListener listener)
            throws InterruptedException {
        ChatRequest request = ChatRequest.builder()
                .messages(messages)
                .toolSpecifications(specs)
                .build();

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<AiMessage> aiRef = new AtomicReference<>();
        AtomicReference<Throwable> errRef = new AtomicReference<>();

        model.chat(request, new StreamingChatResponseHandler() {
            @Override
            public void onPartialResponse(String text) {
                listener.onText(text);
            }

            @Override
            public void onPartialThinking(PartialThinking thinking) {
                listener.onThinking(thinking.text());
            }

            @Override
            public void onCompleteResponse(ChatResponse response) {
                aiRef.set(response.aiMessage());
                latch.countDown();
            }

            @Override
            public void onError(Throwable t) {
                errRef.set(t);
                latch.countDown();
            }
        });

        if (!latch.await(CALL_TIMEOUT.toSeconds() + 10, TimeUnit.SECONDS)) {
            throw new IllegalStateException("LLM 调用超时（" + CALL_TIMEOUT.toSeconds() + "s）");
        }
        if (errRef.get() != null) {
            Throwable t = errRef.get();
            throw new IllegalStateException("LLM 调用失败: " + (t.getMessage() == null ? t.toString() : t.getMessage()), t);
        }
        AiMessage ai = aiRef.get();
        if (ai == null) {
            throw new IllegalStateException("LLM 未返回任何内容");
        }
        return ai;
    }

    /** 执行一次工具调用：先回调 onToolCall，执行后回调 onToolResult，返回（截断后的）结果文本。 */
    private String executeTool(ToolExecutionRequest req, ReActListener listener) {
        String name = req.name();
        listener.onToolCall(name, req.arguments());

        String result;
        AiTool tool = toolByName.get(name);
        if (tool == null) {
            result = "未知工具: " + name;
        } else {
            try {
                result = tool.executor().execute(req.arguments());
            } catch (Exception e) {
                result = "工具执行异常: " + (e.getMessage() == null ? e.toString() : e.getMessage());
            }
        }
        if (result == null) {
            result = "";
        }
        if (result.length() > MAX_TOOL_RESULT_CHARS) {
            result = result.substring(0, MAX_TOOL_RESULT_CHARS) + "\n...(已截断)";
        }
        listener.onToolResult(name, result);
        return result;
    }
}
