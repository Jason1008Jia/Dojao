package com.dojao.ui.ai;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

/**
 * 探针：{@link AiTurns}「写前落库 → 回填」的纯函数逻辑（离线）。
 *
 * <p>核心不变量：<b>「刷新也不丢问答，也不丢思考过程」</b> —— 发送提问时先 {@code appendPending}，
 * 此后即便答案尚未生成（{@code fill} 从未被调用）甚至用户已刷新页面，提问也已在 {@code turns_json} 里；
 * 生成过程中 {@code progress} 把思考 / 工具调用 / 结果 / 半成品回答增量写回同一条轮次；答案生成后
 * {@code fill} 按 {@code turnId} <b>原地回填</b>（不新增轮次），且不会覆盖已完成轮次。</p>
 *
 * <p><b>判别力</b>：把 {@code fill} 的 {@code turnId} 匹配中和（永远走追加兜底）⇒「原地回填仍 1 条」等用例立即
 * FAIL；把 {@code turn.remove("pending")} 删掉 ⇒「pending 已清除」FAIL；把 {@code progress} 改成原样返回
 * ⇒「trace 3 条 / 半成品回答 / 图表文件增量」FAIL；去掉 {@code pending} 校验 ⇒「完成后不被迟到事件污染」FAIL。</p>
 */
public final class AiTurnsProbeTest {

    private static int fail = 0;
    private static final ObjectMapper JSON = new ObjectMapper();

    public static void main(String[] args) {
        // 场景 0：isEmpty 判定
        check("isEmpty: 空串", AiTurns.isEmpty(""));
        check("isEmpty: []", AiTurns.isEmpty("[]"));
        check("isEmpty: 非数组对象", AiTurns.isEmpty("{\"a\":1}"));
        check("isEmpty: 解析失败", AiTurns.isEmpty("not json"));
        check("isEmpty: 有轮次 -> false", !AiTurns.isEmpty("[{\"user\":\"u\"}]"));

        // 场景 1：appendPending 落一条 pending 轮次（提问即刻可持久化）
        String s = AiTurns.appendPending("[]", "turn-1", "销量多少", "数据分析");
        JsonNode t0 = at(s, 0);
        check("appendPending: 1 条", size(s) == 1);
        check("appendPending: user 已保存", "销量多少".equals(t0.path("user").asText(null)));
        check("appendPending: turnId", "turn-1".equals(t0.path("turnId").asText(null)));
        check("appendPending: pending=true", t0.path("pending").asBoolean(false));
        check("appendPending: assistant 暂空", "".equals(t0.path("assistant").asText(null)));
        check("appendPending: skill 记录", "数据分析".equals(t0.path("skill").asText(null)));
        check("★刷新不丢提问（未 fill 也已在库）", s.contains("销量多少"));
        check("isEmpty: 追加后 -> false", !AiTurns.isEmpty(s));

        // 场景 2：fill 按 turnId 原地回填（不新增轮次、清除 pending）
        AiTurns.FillResult r = AiTurns.fill(s, "turn-1", "销量多少", "本月 1234 件",
                List.of("execute_sql", "render_chart"),
                List.of("{\"type\":\"bar\",\"category\":\"d\",\"values\":[\"n\"]}"), "数据分析");
        JsonNode f0 = at(r.turnsJson(), 0);
        check("fill: 仍只 1 条（原地回填）", size(r.turnsJson()) == 1);
        check("fill: appended=false", !r.appended());
        check("fill: assistant 回填", "本月 1234 件".equals(f0.path("assistant").asText(null)));
        check("fill: pending 已清除", !f0.has("pending"));
        check("fill: ops 记录", f0.path("ops").size() == 2
                && "execute_sql".equals(f0.path("ops").path(0).asText()));
        check("fill: charts 记录", f0.path("charts").size() == 1
                && "bar".equals(f0.path("charts").path(0).path("type").asText(null)));
        check("fill: user 保留", "销量多少".equals(f0.path("user").asText(null)));

        // 场景 3：fill 未知 turnId -> 追加兜底（不丢本轮），已有轮次保留
        AiTurns.FillResult r2 = AiTurns.fill(r.turnsJson(), "no-such", "第二个问题", "答案二",
                List.of(), List.of(), null);
        check("fill 未知 turnId: appended=true", r2.appended());
        check("fill 未知 turnId: 追加后 2 条", size(r2.turnsJson()) == 2);
        check("fill 未知 turnId: 首轮保留", "本月 1234 件".equals(at(r2.turnsJson(), 0).path("assistant").asText(null)));
        check("fill 未知 turnId: 新轮 user", "第二个问题".equals(at(r2.turnsJson(), 1).path("user").asText(null)));

        // 场景 4：多轮 —— 第二轮 appendPending 后按 turnId 回填，首轮不受影响
        String s2 = AiTurns.appendPending(r.turnsJson(), "turn-2", "再问一个", null);
        check("第二轮 appendPending: 2 条", size(s2) == 2);
        AiTurns.FillResult r3 = AiTurns.fill(s2, "turn-2", "再问一个", "答案二",
                List.of("list_tables"), List.of(), null);
        check("回填第二轮 turnId: 仍 2 条", size(r3.turnsJson()) == 2);
        check("首轮未被改动", "本月 1234 件".equals(at(r3.turnsJson(), 0).path("assistant").asText(null)));
        check("第二轮已回填", "答案二".equals(at(r3.turnsJson(), 1).path("assistant").asText(null)));

        // 场景 5：损坏的图表规格被跳过，不影响整轮落库
        AiTurns.FillResult r4 = AiTurns.fill("[]", null, "q", "a", List.of(), List.of("{bad json"), null);
        check("损坏图表被跳过", at(r4.turnsJson(), 0).path("charts").size() == 0);

        // 场景 6：turnId 为 null 的 fill -> 兜底追加且 user 正确（beginTurn 失败时的退化路径）
        AiTurns.FillResult r5 = AiTurns.fill("[]", null, "无 turnId 的问", "答", List.of(), List.of(), null);
        check("turnId null: appended=true", r5.appended());
        check("turnId null: user 正确", "无 turnId 的问".equals(at(r5.turnsJson(), 0).path("user").asText(null)));

        // 场景 7：生成中的增量进度 —— 刷新后仍能看到思考 / 工具过程与半成品回答
        String p = AiTurns.appendPending("[]", "turn-p", "分析一下", null);
        String p2 = AiTurns.progress(p, "turn-p",
                List.of("{\"k\":\"think\",\"t\":\"先看表结构\"}",
                        "{\"k\":\"call\",\"name\":\"list_tables\",\"args\":\"{}\"}",
                        "{\"k\":\"result\",\"name\":\"list_tables\",\"t\":\"orders\"}"),
                List.of("{\"type\":\"bar\"}"), List.of("{\"id\":\"f1\",\"name\":\"a.md\"}"), "部分回答");
        JsonNode pj = at(p2, 0);
        check("progress: trace 3 条", pj.path("trace").size() == 3);
        check("★progress: 思考内容保留", "先看表结构".equals(pj.path("trace").path(0).path("t").asText(null)));
        check("★progress: 工具调用保留", "list_tables".equals(pj.path("trace").path(1).path("name").asText(null)));
        check("★progress: 半成品回答落库", "部分回答".equals(pj.path("assistant").asText(null)));
        check("progress: 仍为 pending", pj.path("pending").asBoolean(false));
        check("★progress: 图表/文件增量可见", pj.path("charts").size() == 1 && pj.path("files").size() == 1);
        check("progress: 不新增轮次", size(p2) == 1);
        check("progress: 未知 turnId 原样",
                size(AiTurns.progress("[]", "nope", List.of("{\"k\":\"think\"}"), null, null, "x")) == 0);

        // 场景 8：fill 完成时 trace=null 保留增量 trace；完成后迟到的 progress 不得覆盖最终答案
        AiTurns.FillResult pDone = AiTurns.fill(p2, "turn-p", "分析一下", "最终回答",
                List.of("list_tables"), List.of(), null);
        String late = AiTurns.progress(pDone.turnsJson(), "turn-p",
                List.of("{\"k\":\"think\",\"t\":\"迟到的事件\"}"), null, null, "迟到回答");
        JsonNode lj = at(late, 0);
        check("★fill: 保留生成中已写入的 trace", lj.path("trace").size() == 3);
        check("★progress: 完成后不被半成品覆盖", "最终回答".equals(lj.path("assistant").asText(null)));
        check("★progress: 完成后不被迟到事件污染", lj.path("trace").size() == 3);

        // 场景 9：fill 显式 trace（正常完成时由服务端传入完整推理链）
        AiTurns.FillResult r6 = AiTurns.fill("[]", null, "q", "a", List.of(), List.of(), null,
                List.of("{\"k\":\"think\",\"t\":\"完整链路\"}"), null);
        check("fill: 显式 trace 落库", at(r6.turnsJson(), 0).path("trace").size() == 1);

        // 场景 10：事件数超限截断（防超长会话撑爆 turns_json），并留一条提示
        List<String> many = new ArrayList<>();
        for (int i = 0; i < AiTurns.MAX_TRACE_EVENTS + 20; i++) {
            many.add("{\"k\":\"think\",\"t\":\"e\"}");
        }
        JsonNode trunc = at(AiTurns.progress(p, "turn-p", many, null, null, null), 0);
        check("progress: 事件数超限截断", trunc.path("trace").size() == AiTurns.MAX_TRACE_EVENTS + 1);
        check("progress: 截断留提示",
                "note".equals(trunc.path("trace").path(AiTurns.MAX_TRACE_EVENTS).path("k").asText(null)));

        System.out.println(fail == 0 ? "\n全部通过 ✅" : "\n失败 " + fail + " 项 ❌");
        if (fail > 0) {
            System.exit(1);
        }
    }

    private static JsonNode at(String turnsJson, int i) {
        try {
            JsonNode n = JSON.readTree(turnsJson);
            return n.isArray() ? n.path(i) : JSON.createObjectNode();
        } catch (Exception e) {
            return JSON.createObjectNode();
        }
    }

    private static int size(String turnsJson) {
        try {
            JsonNode n = JSON.readTree(turnsJson);
            return n.isArray() ? n.size() : -1;
        } catch (Exception e) {
            return -1;
        }
    }

    private static void check(String name, boolean ok) {
        System.out.println((ok ? "  ✅ " : "  ❌ ") + name);
        if (!ok) {
            fail++;
        }
    }
}
