package com.dojao.ui.ai;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;

/**
 * AI 会话「轮次」的 JSON 纯函数操作：把「写前落库（append pending）→ 生成结束回填（fill by turnId）」
 * 这两步的 JSON 逻辑从 {@code DojaoUiApp} 抽出，便于离线探针；本身不碰数据库。
 *
 * <p>一轮的形状：{@code {turnId, user, assistant, ops:[…], charts:[…], files:[…], trace:[…],
 * attachments:[…], skill?, pending?}}（{@code attachments} 为用户上传并解析后的参考文件，
 * 形状见 {@link FileParser#toJson}）。
 * 发送提问时先 {@link #appendPending} 落一条 {@code pending:true} 的轮次（此后即便用户刷新页面 /
 * 断线，提问也已保存）；生成过程中 {@link #progress} 把思考 / 工具调用 / 工具结果与半成品回答
 * <b>增量写回同一条轮次</b>（刷新后仍能看到已发生的思考过程）；答案生成结束后 {@link #fill}
 * 按 {@code turnId} 回填最终 assistant / ops / charts / trace 并去掉 {@code pending}。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/17
 */
public final class AiTurns {

    private AiTurns() {
    }

    /** 独立于引擎配置的树操作 mapper（只做 JSON 结构变换，不参与类型绑定）。 */
    private static final ObjectMapper JSON = new ObjectMapper();

    /** 单轮最多保留的推理事件数（防止超长会话把 turns_json / 数据库撑爆）。 */
    public static final int MAX_TRACE_EVENTS = 400;

    /** 单轮推理事件原文总字符上限（超出后截断，并在末尾留一条提示事件）。 */
    public static final int MAX_TRACE_CHARS = 200_000;

    /** turns_json 是否为「没有轮次」（空串、非数组、解析失败一概视作空）。 */
    public static boolean isEmpty(String turnsJson) {
        try {
            JsonNode n = JSON.readTree(turnsJson);
            return !(n instanceof ArrayNode a) || a.isEmpty();
        } catch (Exception e) {
            return true;
        }
    }

    /** 追加一条 pending 轮次（提问已落库、答案待生成）；返回新的 turns_json。 */
    public static String appendPending(String turnsJson, String turnId, String userMsg, String skillName) {
        return appendPending(turnsJson, turnId, userMsg, skillName, null);
    }

    /**
     * 追加一条 pending 轮次，并记录本轮携带的<b>附件</b>（解析后的文本）。
     *
     * <p>附件随轮次落库有两个用途：①刷新页面后用户气泡仍能看到「附了哪些文件」；
     * ②后续追问时 {@code buildHistory} 能把附件内容一起喂回模型（不然模型会「忘记」文件）。</p>
     *
     * @param attachments 附件 JSON 数组（{@code [{name,format,chars,truncated,text}]}），可为 null
     */
    public static String appendPending(String turnsJson, String turnId, String userMsg, String skillName,
                                      JsonNode attachments) {
        ArrayNode turns = parse(turnsJson);
        ObjectNode turn = JSON.createObjectNode();
        turn.put("turnId", turnId);
        turn.put("user", userMsg == null ? "" : userMsg);
        turn.put("assistant", "");
        turn.put("pending", true);
        if (skillName != null && !skillName.isBlank()) {
            turn.put("skill", skillName);
        }
        turn.putArray("ops");
        turn.putArray("charts");
        turn.putArray("files");
        if (attachments != null && attachments.isArray() && !attachments.isEmpty()) {
            turn.set("attachments", attachments);
        }
        turns.add(turn);
        return write(turns);
    }

    /**
     * <b>增量写回推理进度</b>：把当前已发生的思考 / 工具调用 / 工具结果（{@code trace}）、
     * 已渲染的图表 / 导出文件、以及<b>半成品回答</b>写进 {@code turnId} 对应的 pending 轮次。
     * 这样用户在模型思考途中刷新页面，仍能看到已经产生的推理链，而不是只剩「正在生成回答…」。
     *
     * <p>已完成（{@code pending} 已清除）或 {@code turnId} 不存在时原样返回，绝不覆盖最终答案；
     * {@code trace} / {@code charts} / {@code files} 为 {@code null} 表示本次不更新该字段。</p>
     *
     * <p>事件 / 字符数受 {@link #MAX_TRACE_EVENTS}、{@link #MAX_TRACE_CHARS} 约束。</p>
     */
    public static String progress(String turnsJson, String turnId, List<String> trace,
                                  List<String> charts, List<String> files, String partialAssistant) {
        ArrayNode turns = parse(turnsJson);
        ObjectNode turn = find(turns, turnId);
        if (turn == null || !turn.path("pending").asBoolean(false)) {
            return write(turns);
        }
        if (trace != null) {
            putJsonArray(turn, "trace", trace, MAX_TRACE_EVENTS, MAX_TRACE_CHARS, true);
        }
        if (charts != null) {
            putJsonArray(turn, "charts", charts, MAX_TRACE_EVENTS, MAX_TRACE_CHARS, false);
        }
        if (files != null) {
            putJsonArray(turn, "files", files, MAX_TRACE_EVENTS, MAX_TRACE_CHARS, false);
        }
        if (partialAssistant != null) {
            turn.put("assistant", partialAssistant);
        }
        return write(turns);
    }

    /** {@link #fill} 的结果：新的 turns_json + 是否走了「追加」兜底（即没找到 turnId）。 */
    public record FillResult(String turnsJson, boolean appended) {
    }

    /**
     * 按 {@code turnId} 找到 pending 轮次并回填答案（不含导出文件，兼容旧调用方）。
     */
    public static FillResult fill(String turnsJson, String turnId, String userMsg, String assistant,
                                  List<String> ops, List<String> charts, String skillName) {
        return fill(turnsJson, turnId, userMsg, assistant, ops, charts, null, skillName);
    }

    /**
     * 按 {@code turnId} 找到 pending 轮次并回填答案（正常完成、出错保留部分都走这里）；
     * 找不到（如 beginTurn 失败 / turnId 为空）则退化为追加一条完整轮次，确保本轮问答不丢。
     *
     * @param files 本轮导出文件的元数据 JSON（{@code {id,name,format,size,url}}），可为 null
     */
    public static FillResult fill(String turnsJson, String turnId, String userMsg, String assistant,
                                  List<String> ops, List<String> charts, List<String> files, String skillName) {
        return fill(turnsJson, turnId, userMsg, assistant, ops, charts, files, null, skillName);
    }

    /**
     * 按 {@code turnId} 找到 pending 轮次并回填最终答案与推理链；{@code trace} 为 {@code null}
     * 时保留生成过程中已增量写入的 trace。
     *
     * @param trace 推理事件 JSON（{@code {"k":"think"|"call"|"result",…}}），可为 null
     */
    public static FillResult fill(String turnsJson, String turnId, String userMsg, String assistant,
                                  List<String> ops, List<String> charts, List<String> files,
                                  List<String> trace, String skillName) {
        ArrayNode turns = parse(turnsJson);
        ObjectNode turn = find(turns, turnId);
        boolean appended = turn == null;
        if (appended) {
            turn = JSON.createObjectNode();
            turn.put("user", userMsg == null ? "" : userMsg);
            turns.add(turn);
        }
        turn.put("assistant", assistant == null ? "" : assistant);
        turn.remove("pending");
        if (skillName != null && !skillName.isBlank()) {
            turn.put("skill", skillName);
        }
        ArrayNode opsArr = turn.putArray("ops");
        if (ops != null) {
            for (String o : ops) {
                opsArr.add(o);
            }
        }
        putJsonArray(turn, "charts", charts, MAX_TRACE_EVENTS, MAX_TRACE_CHARS, false);
        putJsonArray(turn, "files", files, MAX_TRACE_EVENTS, MAX_TRACE_CHARS, false);
        if (trace != null) {
            putJsonArray(turn, "trace", trace, MAX_TRACE_EVENTS, MAX_TRACE_CHARS, true);
        }
        return new FillResult(write(turns), appended);
    }

    /** 按 {@code turnId} 找轮次节点（未命中 / turnId 为空返回 null）。 */
    private static ObjectNode find(ArrayNode turns, String turnId) {
        if (turnId == null) {
            return null;
        }
        for (JsonNode n : turns) {
            if (n.isObject() && turnId.equals(n.path("turnId").asText(null))) {
                return (ObjectNode) n;
            }
        }
        return null;
    }

    /**
     * 把 JSON 数组文本列表写进轮次字段（替换同名数组）；单条损坏跳过。
     *
     * @param addTruncationNote 达到条数 / 字符上限时是否追加一条提示事件
     */
    private static void putJsonArray(ObjectNode turn, String field, List<String> items,
                                     int maxItems, int maxChars, boolean addTruncationNote) {
        ArrayNode arr = turn.putArray(field);
        if (items == null) {
            return;
        }
        int added = 0;
        int chars = 0;
        boolean truncated = false;
        for (String item : items) {
            if (item == null) {
                continue;
            }
            if (added >= maxItems || chars + item.length() > maxChars) {
                truncated = true;
                break;
            }
            try {
                arr.add(JSON.readTree(item));
                chars += item.length();
                added++;
            } catch (Exception ignored) {
                // 单条损坏（如图表规格 / 文件元数据）不影响整轮落库
            }
        }
        if (truncated && addTruncationNote) {
            ObjectNode note = JSON.createObjectNode();
            note.put("k", "note");
            note.put("t", "推理记录过长，后续步骤未保存");
            arr.add(note);
        }
    }

    private static ArrayNode parse(String turnsJson) {
        try {
            JsonNode n = JSON.readTree(turnsJson);
            return n instanceof ArrayNode a ? a : JSON.createArrayNode();
        } catch (Exception e) {
            return JSON.createArrayNode();
        }
    }

    private static String write(ArrayNode turns) {
        try {
            return JSON.writeValueAsString(turns);
        } catch (Exception e) {
            return "[]";
        }
    }
}
