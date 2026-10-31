package com.dojao.ui.ai;

import com.dojao.ui.auth.AuthStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Dojao AI「技能（Skill）」的持久化存储 —— 与账号体系同库（{@link AuthStore#defaultDbPath()}，
 * 默认 {@code ./data/dojao-ui.db}），独立连接、独立表 {@code ui_ai_skill}。
 *
 * <p><b>什么是 Skill</b>：一段可复用的分析角色 —— 名称 + 说明 + 附加系统提示词（业务口径 / 输出规范）
 * + 允许使用的工具白名单。用户在 AI 对话里选中某 Skill，本轮推理即按其指令与工具集运行。</p>
 *
 * <p><b>按用户隔离</b>：每行归一个 {@code owner}（登录用户名），列表 / 读取 / 删除都带 owner 条件，
 * 用户之间互不可见。{@code tools_json} 存工具名数组的 JSON 文本（空数组 = 不限制，用全部工具）。</p>
 *
 * <p><b>并发</b>：与 {@link AiSessionStore} 同款取舍 —— SQLite 单写者，持一个常驻连接由方法级
 * {@code synchronized} 串行化。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/14
 */
public final class SkillStore implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SkillStore.class);

    private static final String SQLITE_PREFIX = "jdbc:sqlite:";

    private static final String CREATE_TABLE_DDL = """
        CREATE TABLE IF NOT EXISTS ui_ai_skill (
            id          TEXT    NOT NULL PRIMARY KEY,
            owner       TEXT    NOT NULL,
            name        TEXT    NOT NULL,
            description TEXT    NOT NULL,
            prompt      TEXT    NOT NULL,
            tools_json  TEXT    NOT NULL,
            created_at  INTEGER NOT NULL,
            updated_at  INTEGER NOT NULL
        )
        """;

    private static final String CREATE_INDEX_DDL =
            "CREATE INDEX IF NOT EXISTS idx_ui_ai_skill_owner ON ui_ai_skill (owner, updated_at)";

    /** 技能摘要（列表用，不含提示词正文）。{@code toolsJson} 为工具名数组的 JSON 文本。 */
    public record SkillMeta(String id, String name, String description, String toolsJson, long updatedAt) {
    }

    /** 技能全文（编辑 / 应用时用）。 */
    public record SkillFull(String id, String name, String description, String prompt,
                            String toolsJson, long updatedAt) {
    }

    private final String jdbcUrl;
    private Connection conn;

    private SkillStore(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    /**
     * 打开（必要时创建）技能库并建表。
     *
     * @param dbPath db 文件路径；{@code null} / 空白 / {@code :memory:} = 内存库（测试用）
     */
    public static SkillStore open(String dbPath) {
        boolean inMemory = dbPath == null || dbPath.isBlank() || ":memory:".equals(dbPath);
        if (!inMemory) {
            createParentDirIfNeeded(dbPath);
        }
        SkillStore store = new SkillStore(SQLITE_PREFIX + (inMemory ? ":memory:" : dbPath));
        store.initSchema();
        return store;
    }

    private void initSchema() {
        try {
            conn = DriverManager.getConnection(jdbcUrl);
            try (Statement st = conn.createStatement()) {
                st.execute("PRAGMA journal_mode=WAL");
                st.execute("PRAGMA busy_timeout=5000");
                st.execute(CREATE_TABLE_DDL);
                st.execute(CREATE_INDEX_DDL);
            }
            log.info("AI 技能库已就绪: {}", jdbcUrl);
        } catch (SQLException e) {
            throw new IllegalStateException("初始化 AI 技能库失败: " + jdbcUrl, e);
        }
    }

    /** 某用户的技能清单（按更新时间倒序）。 */
    public synchronized List<SkillMeta> list(String owner) {
        String sql = "SELECT id, name, description, tools_json, updated_at FROM ui_ai_skill"
                + " WHERE owner = ? ORDER BY updated_at DESC";
        List<SkillMeta> out = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, owner);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new SkillMeta(rs.getString("id"), rs.getString("name"),
                            rs.getString("description"), rs.getString("tools_json"), rs.getLong("updated_at")));
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("读取 AI 技能列表失败", e);
        }
        return out;
    }

    /** 读取某技能全文；不存在（或不属于该 owner）返回 null。 */
    public synchronized SkillFull get(String owner, String id) {
        if (id == null || id.isBlank()) {
            return null;
        }
        String sql = "SELECT id, name, description, prompt, tools_json, updated_at FROM ui_ai_skill"
                + " WHERE owner = ? AND id = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, owner);
            ps.setString(2, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return new SkillFull(rs.getString("id"), rs.getString("name"), rs.getString("description"),
                        rs.getString("prompt"), rs.getString("tools_json"), rs.getLong("updated_at"));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("读取 AI 技能失败", e);
        }
    }

    /** 新建一个技能（归属 owner），返回其全文。 */
    public synchronized SkillFull create(String owner, String name, String description,
                                         String prompt, String toolsJson) {
        String id = UUID.randomUUID().toString();
        long now = System.currentTimeMillis();
        String sql = """
                INSERT INTO ui_ai_skill
                    (id, owner, name, description, prompt, tools_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, id);
            ps.setString(2, owner);
            ps.setString(3, name);
            ps.setString(4, description == null ? "" : description);
            ps.setString(5, prompt == null ? "" : prompt);
            ps.setString(6, toolsJson == null || toolsJson.isBlank() ? "[]" : toolsJson);
            ps.setLong(7, now);
            ps.setLong(8, now);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("新建 AI 技能失败", e);
        }
        return new SkillFull(id, name, description == null ? "" : description,
                prompt == null ? "" : prompt, toolsJson == null || toolsJson.isBlank() ? "[]" : toolsJson, now);
    }

    /**
     * 覆盖写入某技能（upsert：名称 + 说明 + 提示词 + 工具 + 更新时间）。
     *
     * <p>调用方须先用 {@link #get(String, String)} 校验归属，避免越权覆盖他人技能。</p>
     */
    public synchronized boolean save(String owner, String id, String name, String description,
                                     String prompt, String toolsJson) {
        long now = System.currentTimeMillis();
        String sql = """
                INSERT INTO ui_ai_skill
                    (id, owner, name, description, prompt, tools_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    name = excluded.name,
                    description = excluded.description,
                    prompt = excluded.prompt,
                    tools_json = excluded.tools_json,
                    updated_at = excluded.updated_at
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, id);
            ps.setString(2, owner);
            ps.setString(3, name);
            ps.setString(4, description == null ? "" : description);
            ps.setString(5, prompt == null ? "" : prompt);
            ps.setString(6, toolsJson == null || toolsJson.isBlank() ? "[]" : toolsJson);
            ps.setLong(7, now);
            ps.setLong(8, now);
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new IllegalStateException("保存 AI 技能失败", e);
        }
    }

    /** 删除某技能（须归属该 owner）；返回是否确有删除。 */
    public synchronized boolean delete(String owner, String id) {
        String sql = "DELETE FROM ui_ai_skill WHERE owner = ? AND id = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, owner);
            ps.setString(2, id);
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new IllegalStateException("删除 AI 技能失败", e);
        }
    }

    /** 删除某账号的全部技能（删除用户时清理）；返回删除条数。 */
    public synchronized int deleteAll(String owner) {
        try (PreparedStatement ps = conn.prepareStatement("DELETE FROM ui_ai_skill WHERE owner = ?")) {
            ps.setString(1, owner);
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("清空 AI 技能失败: " + owner, e);
        }
    }

    @Override
    public synchronized void close() {
        if (conn == null) {
            return;
        }
        try {
            conn.close();
        } catch (SQLException e) {
            log.warn("关闭 AI 技能库失败: {}", e.getMessage());
        } finally {
            conn = null;
        }
    }

    private static void createParentDirIfNeeded(String dbPath) {
        Path parent = Path.of(dbPath).toAbsolutePath().getParent();
        if (parent == null) {
            return;
        }
        try {
            Files.createDirectories(parent);
        } catch (IOException e) {
            throw new IllegalStateException("创建 db 目录失败: " + parent, e);
        }
    }
}
