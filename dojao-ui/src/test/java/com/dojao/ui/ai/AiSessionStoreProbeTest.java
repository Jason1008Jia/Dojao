package com.dojao.ui.ai;

import java.util.List;

/**
 * 探针：{@link AiSessionStore} 会话持久化（离线，内存库）—— 增 / 列 / 查 / 存 / 删 + 按用户隔离。
 *
 * <p><b>判别力</b>：把 {@code get}/{@code list}/{@code delete} 的 {@code owner} 条件去掉，
 * 「他人看不到 / 删不掉」用例立即 FAIL；把 {@code save} 的 upsert 退化成 insert，
 * 「save 覆盖标题与轮次」用例 FAIL。</p>
 */
public final class AiSessionStoreProbeTest {

    private static int fail = 0;

    public static void main(String[] args) {
        try (AiSessionStore store = AiSessionStore.open(":memory:")) {
            String alice = "alice";
            String bob = "bob";

            // ---- create ----
            AiSessionStore.SessionFull s1 = store.create(alice);
            check("create 返回非空 id", s1.id() != null && !s1.id().isBlank());
            check("create 默认标题", AiSessionStore.DEFAULT_TITLE.equals(s1.title()));
            check("create 空轮次", "[]".equals(s1.turnsJson()));

            // ---- get / list ----
            check("get 命中本人", store.get(alice, s1.id()) != null);
            List<AiSessionStore.SessionMeta> aliceList = store.list(alice);
            check("list 含本人会话", aliceList.size() == 1 && s1.id().equals(aliceList.get(0).id()));

            // ---- save：覆盖标题与轮次 ----
            store.save(alice, s1.id(), "统计销量", "[{\"user\":\"u\",\"assistant\":\"a\",\"ops\":[\"execute_sql\"]}]");
            AiSessionStore.SessionFull after = store.get(alice, s1.id());
            check("save 覆盖标题", after != null && "统计销量".equals(after.title()));
            check("save 覆盖轮次", after != null && after.turnsJson().contains("execute_sql"));
            check("list 回显新标题", "统计销量".equals(store.list(alice).get(0).title()));

            // ---- 按用户隔离 ----
            check("他人 get 不到", store.get(bob, s1.id()) == null);
            check("他人 list 为空", store.list(bob).isEmpty());
            check("他人删不掉", !store.delete(bob, s1.id()));
            check("隔离后本人仍在", store.get(alice, s1.id()) != null);

            // ---- 排序：新建的会话排在前（updated_at 倒序）----
            sleep(5);
            AiSessionStore.SessionFull s2 = store.create(alice);
            List<AiSessionStore.SessionMeta> l2 = store.list(alice);
            check("list 倒序（新会话在前）", l2.size() == 2 && s2.id().equals(l2.get(0).id()));

            // ---- 模拟「生成中刷新页面」：pending + 增量 trace 落库 → 重新读取 → 最终回填 ----
            String thinkingTurns = AiTurns.appendPending("[]", "turn-x", "正在思考的问题", null);
            store.save(alice, s2.id(), "生成中", thinkingTurns);
            String progressed = AiTurns.progress(store.get(alice, s2.id()).turnsJson(), "turn-x",
                    List.of("{\"k\":\"think\",\"t\":\"先查表\"}",
                            "{\"k\":\"call\",\"name\":\"execute_sql\",\"args\":\"{}\"}"),
                    null, null, "部分回答");
            store.save(alice, s2.id(), "生成中", progressed);
            AiSessionStore.SessionFull mid = store.get(alice, s2.id());
            check("★刷新场景：重新读取仍是 pending", mid.turnsJson().contains("\"pending\":true"));
            check("★刷新场景：思考过程已落库", mid.turnsJson().contains("先查表"));
            check("★刷新场景：半成品回答已落库", mid.turnsJson().contains("部分回答"));
            AiTurns.FillResult fin = AiTurns.fill(mid.turnsJson(), "turn-x", "正在思考的问题", "最终回答",
                    List.of("execute_sql"), List.of(), null);
            store.save(alice, s2.id(), "完成", fin.turnsJson());
            AiSessionStore.SessionFull end = store.get(alice, s2.id());
            check("★刷新场景：完成后 pending 清除", !end.turnsJson().contains("\"pending\":true"));
            check("★刷新场景：完整 trace 保留", end.turnsJson().contains("先查表"));

            // ---- delete ----
            check("delete 命中", store.delete(alice, s1.id()));
            check("delete 后 get 为空", store.get(alice, s1.id()) == null);
            check("delete 后 list 只剩 1", store.list(alice).size() == 1);
            check("重复 delete 返回 false", !store.delete(alice, s1.id()));
        }

        System.out.println(fail == 0 ? "\n全部通过 ✅" : "\n失败 " + fail + " 项 ❌");
        if (fail > 0) {
            System.exit(1);
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void check(String name, boolean ok) {
        System.out.println((ok ? "  ✅ " : "  ❌ ") + name);
        if (!ok) {
            fail++;
        }
    }
}
