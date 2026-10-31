package com.dojao.ui.ai;

import com.dojao.ai.AiConfig;
import com.dojao.ai.AiTool;
import com.dojao.ui.auth.AuthStore;
import com.dojao.ui.auth.Role;
import com.dojao.ui.auth.User;
import dev.langchain4j.model.chat.request.json.JsonObjectSchema;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 探针：用户角色（管理员 / 开发者 / 分析师）的权限边界与「一账号一个模型」。
 *
 * <p>覆盖：①{@link Role} 的宽松 / 严格解析与旧 {@code USER} 迁移；②AI 工具按角色过滤
 * （分析师剔除 {@link AiAgentService#DEVELOPER_TOOLS}，含日志工具 {@code read_logs}）；
 * ③三类角色的提示词差异与 {@link AiAgentService#composeBasePrompt} 拼接；④
 * {@link AuthStore} 的用户列表 / 修改 / 删除（含旧库角色迁移、删号清会话）；
 * ⑤{@link AiConfigStore} 按账号隔离 + 回退管理员配置 + 旧全局配置迁移。</p>
 *
 * <p><b>判别力</b>：把 {@code DEVELOPER_TOOLS} 里的 {@code read_logs} 去掉 → 「分析师工具集已剔除日志」
 * 与「技能白名单不能回授开发者工具」用例 FAIL；把 {@code filterToolsForRole} 中和成原样返回 →
 * 分析师工具集用例 FAIL；把 {@code AiConfigStore.load} 的「回退管理员配置」去掉 → 继承用例 FAIL；
 * 把 {@code AuthStore.initSchema} 的 USER→ANALYST 迁移去掉 → 旧库迁移用例 FAIL。</p>
 */
public final class AiRoleAccessProbeTest {

    private static int fail = 0;

    public static void main(String[] args) throws Exception {
        roleParsing();
        toolGating();
        rolePrompts();
        authStoreCrud();
        legacyRoleMigration();
        aiConfigPerOwner();
        legacyAiConfigMigration();

        System.out.println(fail == 0 ? "\n全部通过 ✅" : "\n失败 " + fail + " 项 ❌");
        if (fail > 0) {
            System.exit(1);
        }
    }

    // ==================== ① 角色解析 ====================

    private static void roleParsing() {
        check("of(null) → 分析师", Role.of(null) == Role.ANALYST);
        check("of(\"USER\") → 分析师", Role.of("USER") == Role.ANALYST);
        check("of(\" developer \") → 开发者", Role.of(" developer ") == Role.DEVELOPER);
        check("of(未知) → 分析师（不抛）", Role.of("owner") == Role.ANALYST);
        check("parse(\"ADMIN\") → 管理员", Role.parse("ADMIN") == Role.ADMIN);
        check("parse(\"user\") → 分析师", Role.parse("user") == Role.ANALYST);
        check("parse(null) 抛异常", throwsIae(() -> Role.parse(null)));
        check("parse(\"\") 抛异常", throwsIae(() -> Role.parse(" ")));
        check("parse(未知) 抛异常", throwsIae(() -> Role.parse("OWNER")));
        check("canUseClusterTools：管理员", Role.ADMIN.canUseClusterTools());
        check("canUseClusterTools：开发者", Role.DEVELOPER.canUseClusterTools());
        check("canUseClusterTools：分析师", !Role.ANALYST.canUseClusterTools());
        check("isAdmin：仅管理员", Role.ADMIN.isAdmin() && !Role.DEVELOPER.isAdmin()
                && !Role.ANALYST.isAdmin());
    }

    // ==================== ② 工具按角色过滤 ====================

    private static void toolGating() {
        check("工具清单含 read_logs", AiAgentService.AVAILABLE_TOOLS.contains("read_logs"));
        check("开发工具集含 read_logs", AiAgentService.DEVELOPER_TOOLS.contains("read_logs"));
        check("开发工具集 = 集群状态/日志/查询/算子",
                AiAgentService.DEVELOPER_TOOLS.equals(java.util.Set.of(
                        "cluster_status", "read_logs", "list_queries", "query_status", "operator_status")));

        List<AiTool> all = new ArrayList<>();
        for (String name : AiAgentService.AVAILABLE_TOOLS) {
            all.add(new AiTool(name, "", JsonObjectSchema.builder().build(), a -> ""));
        }

        List<String> analystNames = names(AiAgentService.filterToolsForRole(all, Role.ANALYST));
        check("分析师保留数据类工具",
                analystNames.contains("execute_sql") && analystNames.contains("render_chart")
                        && analystNames.contains("export_file") && analystNames.contains("list_tables"));
        boolean removedAll = true;
        for (String dev : AiAgentService.DEVELOPER_TOOLS) {
            if (analystNames.contains(dev)) {
                removedAll = false;
            }
        }
        check("分析师工具集已剔除全部开发者工具（含日志）", removedAll);
        check("分析师工具集不含 read_logs", !analystNames.contains("read_logs"));

        check("开发者工具集原样",
                AiAgentService.filterToolsForRole(all, Role.DEVELOPER).size() == all.size());
        check("管理员工具集原样",
                AiAgentService.filterToolsForRole(all, Role.ADMIN).size() == all.size());
        check("role=null 按原样（防御性）",
                AiAgentService.filterToolsForRole(all, null).size() == all.size());

        check("toolsForRole(分析师) 不含 read_logs",
                !AiAgentService.toolsForRole(Role.ANALYST).contains("read_logs"));
        check("toolsForRole(开发者) 含 read_logs",
                AiAgentService.toolsForRole(Role.DEVELOPER).contains("read_logs"));
        check("toolsForRole(管理员) = 全部",
                AiAgentService.toolsForRole(Role.ADMIN).equals(AiAgentService.AVAILABLE_TOOLS));

        // 技能白名单不能把开发者工具「重新授予」分析师
        List<AiTool> skillOverride = AiAgentService.filterTools(
                AiAgentService.filterToolsForRole(all, Role.ANALYST), List.of("read_logs"));
        check("技能白名单不能回授开发者工具",
                names(skillOverride).stream().noneMatch(AiAgentService.DEVELOPER_TOOLS::contains));
    }

    // ==================== ③ 角色提示词 ====================

    private static void rolePrompts() {
        String admin = AiAgentService.rolePrompt(Role.ADMIN);
        String dev = AiAgentService.rolePrompt(Role.DEVELOPER);
        String analyst = AiAgentService.rolePrompt(Role.ANALYST);

        check("管理员提示词：角色名", admin.contains("管理员") && admin.contains("ADMIN"));
        check("管理员提示词：能力并集", admin.contains("数据探查") && admin.contains("集群测试")
                && admin.contains("压测") && admin.contains("日志分析"));
        check("管理员提示词：工具清单", admin.contains("read_logs"));
        check("管理员提示词：用户管理指引", admin.contains("用户管理"));

        check("开发者提示词：角色名", dev.contains("开发者") && dev.contains("DEVELOPER"));
        check("开发者提示词：压测 + 日志分析", dev.contains("压测") && dev.contains("日志分析"));
        check("开发者提示词：read_logs 工具", dev.contains("read_logs"));
        check("开发者提示词：不能管理账号", dev.contains("管理员") && dev.contains("用户管理"));

        check("分析师提示词：角色名", analyst.contains("分析师") && analyst.contains("ANALYST"));
        check("分析师提示词：固定拒绝话术", analyst.contains("当前账号是「分析师」角色")
                && analyst.contains("请联系管理员或使用开发者账号"));
        check("分析师提示词：明确不得调用集群工具",
                analyst.contains("不要调用这些工具") && analyst.contains("read_logs")
                        && analyst.contains("cluster_status"));
        check("分析师提示词：SQL 报错诊断仍属职责", analyst.contains("这条 SQL 为什么报错"));
        check("rolePrompt(null) = 分析师", AiAgentService.rolePrompt(null).equals(analyst));

        String composedAnalyst = AiAgentService.composeBasePrompt(
                AiAgentService.SYSTEM_PROMPT, List.of(), Role.ANALYST);
        check("合成提示词：分析师段已下发", composedAnalyst.contains(analyst));
        check("合成提示词：能力段仍在", composedAnalyst.contains(AiAgentService.DOJAO_CAPABILITIES));
        check("合成提示词：基础提示词仍在", composedAnalyst.contains("execute_sql"));

        String composedAdmin = AiAgentService.composeBasePrompt(
                AiAgentService.SYSTEM_PROMPT, List.of(), Role.ADMIN);
        check("合成提示词：管理员不套分析师段",
                !composedAdmin.contains("【当前账号角色：分析师（ANALYST）】"));

        check("基础提示词列出 read_logs", AiAgentService.SYSTEM_PROMPT.contains("read_logs"));
        check("HTTP 清单含日志接口", AiAgentService.DOJAO_HTTP_API.contains("GET /api/logs"));
    }

    // ==================== ④ AuthStore 用户管理 ====================

    private static void authStoreCrud() {
        try (AuthStore store = AuthStore.open(null, 3_600_000L)) {
            store.ensureBootstrapAdmin(AuthStore.DEFAULT_ADMIN_PASSWORD);
            check("引导管理员", store.countUsers() == 1 && store.countAdmins() == 1
                    && store.authenticate("admin", AuthStore.DEFAULT_ADMIN_PASSWORD).isPresent());

            store.createUser("dev01", "DevPass123", Role.DEVELOPER, "开发者一号");
            store.createUser("qa01", "QaPass123", Role.ANALYST, null);
            List<AuthStore.UserEntry> list = store.listUsers();
            check("列表按用户名排序", list.size() == 3
                    && list.get(0).username().equals("admin")
                    && list.get(1).username().equals("dev01")
                    && list.get(2).username().equals("qa01"));
            check("列表角色正确", list.get(1).role() == Role.DEVELOPER
                    && list.get(2).role() == Role.ANALYST);
            check("列表不含口令哈希（结构天然）", list.get(0).displayName().equals("Administrator"));

            check("修改：存在", store.updateUser("dev01", Role.ANALYST, "改名开发者", "NewPass456"));
            Optional<User> dev = store.findUser("dev01");
            check("修改后角色 / 显示名", dev.isPresent() && dev.get().role() == Role.ANALYST
                    && "改名开发者".equals(dev.get().displayName()));
            check("修改后新口令可用", store.authenticate("dev01", "NewPass456").isPresent());
            check("修改后旧口令失效", store.authenticate("dev01", "DevPass123").isEmpty());
            check("修改：不存在返回 false",
                    !store.updateUser("ghost", Role.ANALYST, null, null));
            check("修改：口令留空不改",
                    store.updateUser("dev01", Role.DEVELOPER, "改名开发者", "")
                            && store.authenticate("dev01", "NewPass456").isPresent());

            String token = store.newSession("qa01");
            check("删除前会话有效", store.resolve(token).isPresent());
            check("删除：存在返回 true", store.deleteUser("qa01"));
            check("删除：再次返回 false", !store.deleteUser("qa01"));
            check("删除后用户不存在", store.findUser("qa01").isEmpty());
            check("删除后会话失效", store.resolve(token).isEmpty());
            check("管理员数量仍为 1", store.countAdmins() == 1);
        }
    }

    /** 旧库里的 {@code role='USER'} 应在初始化时迁移为 {@code ANALYST}。 */
    private static void legacyRoleMigration() {
        Path dir = null;
        try {
            dir = Files.createTempDirectory("dojao-role-migrate");
            Path db = dir.resolve("ui.db");
            try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db);
                 Statement st = c.createStatement()) {
                st.execute("""
                        CREATE TABLE ui_user (
                            username TEXT NOT NULL PRIMARY KEY,
                            password TEXT NOT NULL,
                            role     TEXT NOT NULL,
                            display_name TEXT,
                            created_at INTEGER NOT NULL,
                            updated_at INTEGER NOT NULL
                        )""");
                st.executeUpdate("INSERT INTO ui_user VALUES ('legacy', 'x', 'USER', '旧用户', 1, 1)");
            }
            try (AuthStore store = AuthStore.open(db.toString(), 3_600_000L)) {
                Optional<User> u = store.findUser("legacy");
                check("旧 USER 行已迁移为分析师", u.isPresent() && u.get().role() == Role.ANALYST);
                check("迁移后列表可见", store.listUsers().stream()
                        .anyMatch(e -> e.username().equals("legacy") && e.role() == Role.ANALYST));
            }
        } catch (Exception e) {
            check("旧角色迁移探针未抛异常: " + e, false);
        } finally {
            deleteDb(dir);
        }
    }

    // ==================== ⑤ AI 配置一账号一份 ====================

    private static void aiConfigPerOwner() {
        try (AiConfigStore store = AiConfigStore.open(null)) {
            check("初始：admin 未保存自己的配置", !store.hasOwn("admin"));
            AiConfig defaults = store.load("anyone");
            check("初始：回退默认值（无 apiKey）",
                    AiConfigStore.DEFAULT_BASE_URL.equals(defaults.baseUrl())
                            && defaults.apiKey() == null
                            && AiConfigStore.DEFAULT_MODEL.equals(defaults.model()));

            AiConfig adminCfg = new AiConfig("https://admin.example/v1", "admin-key", "admin-model", 0.1);
            store.save("admin", adminCfg);
            check("管理员保存后有自己的配置", store.hasOwn("admin"));
            check("管理员读取自己的配置", store.load("admin").equals(adminCfg));
            check("普通账号继承管理员配置", store.load("alice").equals(adminCfg));
            check("普通账号未保存过配置", !store.hasOwn("alice"));

            AiConfig aliceCfg = new AiConfig("https://alice.example/v1", "alice-key", "alice-model", 0.7);
            store.save("alice", aliceCfg);
            check("账号保存自己的模型", store.load("alice").equals(aliceCfg));
            check("账号配置不影响管理员", store.load("admin").equals(adminCfg));
            check("账号配置不影响其他账号", store.load("bob").equals(adminCfg));

            check("删除账号配置", store.delete("alice") && !store.hasOwn("alice"));
            check("删除后重新继承管理员", store.load("alice").equals(adminCfg));

            AiConfig rootCfg = new AiConfig("https://root.example/v1", "root-key", "root-model", 0.3);
            store.save(null, rootCfg);   // owner 为 null → 落在 admin
            check("owner=null 落到管理员", store.load(AiConfigStore.ADMIN_OWNER).equals(rootCfg));
        }
    }

    /** 旧版全局单行表 {@code ui_ai_config(id=1)} → 管理员账号配置。 */
    private static void legacyAiConfigMigration() {
        Path dir = null;
        try {
            dir = Files.createTempDirectory("dojao-ai-config-migrate");
            Path db = dir.resolve("ui.db");
            try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db);
                 Statement st = c.createStatement()) {
                st.execute("""
                        CREATE TABLE ui_ai_config (
                            id          INTEGER NOT NULL PRIMARY KEY,
                            base_url    TEXT NOT NULL,
                            api_key     TEXT,
                            model       TEXT NOT NULL,
                            temperature REAL NOT NULL
                        )""");
                st.executeUpdate("INSERT INTO ui_ai_config VALUES "
                        + "(1, 'https://legacy.example/v1', 'legacy-key', 'legacy-model', 0.4)");
            }
            try (AiConfigStore store = AiConfigStore.open(db.toString())) {
                AiConfig admin = store.load("admin");
                check("旧全局配置迁移到管理员",
                        "https://legacy.example/v1".equals(admin.baseUrl())
                                && "legacy-key".equals(admin.apiKey())
                                && "legacy-model".equals(admin.model())
                                && Math.abs(admin.temperature() - 0.4) < 1e-9);
                check("迁移后账号继承该配置", store.load("alice").equals(admin));
            }
        } catch (Exception e) {
            check("旧 AI 配置迁移探针未抛异常: " + e, false);
        } finally {
            deleteDb(dir);
        }
    }

    // ==================== 工具 ====================

    private static List<String> names(List<AiTool> tools) {
        List<String> out = new ArrayList<>();
        for (AiTool t : tools) {
            out.add(t.name());
        }
        return out;
    }

    private static boolean throwsIae(Runnable r) {
        try {
            r.run();
            return false;
        } catch (IllegalArgumentException e) {
            return true;
        }
    }

    private static void deleteDb(Path dir) {
        if (dir == null) {
            return;
        }
        try (var files = Files.list(dir)) {
            files.forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (Exception ignored) {
                    // 清理失败不影响探针结论
                }
            });
        } catch (Exception ignored) {
            // ignore
        }
        try {
            Files.deleteIfExists(dir);
        } catch (Exception ignored) {
            // ignore
        }
    }

    private static void check(String name, boolean ok) {
        System.out.println((ok ? "  ✅ " : "  ❌ ") + name);
        if (!ok) {
            fail++;
        }
    }

    private AiRoleAccessProbeTest() {
    }
}
