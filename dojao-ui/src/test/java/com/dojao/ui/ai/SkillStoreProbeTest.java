package com.dojao.ui.ai;

import java.util.List;

/**
 * 探针：{@link SkillStore} 技能持久化（离线，内存库）—— 增 / 列 / 查 / 存 / 删 + 按用户隔离 + 字段往返。
 *
 * <p><b>判别力</b>：把 {@code get}/{@code list}/{@code delete} 的 {@code owner} 条件去掉，
 * 「他人看不到 / 删不掉」用例立即 FAIL；把 {@code save} 的 upsert 退化成 insert，
 * 「save 覆盖字段」用例 FAIL。</p>
 */
public final class SkillStoreProbeTest {

    private static int fail = 0;

    public static void main(String[] args) {
        try (SkillStore store = SkillStore.open(":memory:")) {
            String alice = "alice";
            String bob = "bob";

            // ---- create ----
            SkillStore.SkillFull s1 = store.create(alice, "日活报表", "统计每日活跃用户",
                    "只统计最近 7 天，输出 Markdown 表格", "[\"execute_sql\",\"render_chart\"]");
            check("create 返回非空 id", s1.id() != null && !s1.id().isBlank());
            check("create name 往返", "日活报表".equals(s1.name()));
            check("create description 往返", "统计每日活跃用户".equals(s1.description()));
            check("create prompt 往返", s1.prompt().contains("最近 7 天"));
            check("create tools 往返", s1.toolsJson().contains("execute_sql") && s1.toolsJson().contains("render_chart"));

            // ---- get / list ----
            SkillStore.SkillFull got = store.get(alice, s1.id());
            check("get 命中本人", got != null && "日活报表".equals(got.name()));
            List<SkillStore.SkillMeta> list = store.list(alice);
            check("list 含本人技能", list.size() == 1 && s1.id().equals(list.get(0).id()));
            check("list 摘要带 tools", list.get(0).toolsJson().contains("render_chart"));

            // ---- save：覆盖字段 ----
            store.save(alice, s1.id(), "日活报表 v2", "改过说明", "改为按月统计", "[\"execute_sql\"]");
            SkillStore.SkillFull after = store.get(alice, s1.id());
            check("save 覆盖 name", after != null && "日活报表 v2".equals(after.name()));
            check("save 覆盖 description", after != null && "改过说明".equals(after.description()));
            check("save 覆盖 prompt", after != null && "改为按月统计".equals(after.prompt()));
            check("save 覆盖 tools", after != null && "[\"execute_sql\"]".equals(after.toolsJson()));
            check("list 回显新名", "日活报表 v2".equals(store.list(alice).get(0).name()));

            // ---- 空 tools 归一为 [] ----
            store.save(alice, s1.id(), "全工具", "", "p", null);
            check("空 tools 归一为 []", "[]".equals(store.get(alice, s1.id()).toolsJson()));

            // ---- 按用户隔离 ----
            check("他人 get 不到", store.get(bob, s1.id()) == null);
            check("他人 list 为空", store.list(bob).isEmpty());
            check("他人删不掉", !store.delete(bob, s1.id()));
            check("隔离后本人仍在", store.get(alice, s1.id()) != null);

            // ---- 排序：新建的技能排在前（updated_at 倒序）----
            sleep(5);
            SkillStore.SkillFull s2 = store.create(alice, "留存分析", "", "", "[]");
            List<SkillStore.SkillMeta> l2 = store.list(alice);
            check("list 倒序（新技能在前）", l2.size() == 2 && s2.id().equals(l2.get(0).id()));

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
