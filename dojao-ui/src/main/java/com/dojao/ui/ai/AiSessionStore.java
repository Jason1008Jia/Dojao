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
 * Dojao AI 长对话（session）的持久化存储 —— 与账号体系同库（{@link AuthStore#defaultDbPath()}，
 * 默认 {@code ./data/dojao-ui.db}），独立连接、独立表 {@code ui_ai_session}。
 *
 * <p><b>按用户隔离</b>：每行归一个 {@code owner}（登录用户名），列表 / 读取 / 写入 / 删除都带
 * owner 条件，用户之间互不可见。</p>
 *
 * <p><b>消息形状</b>：一轮 = {@code {user, assistant, ops:[工具名…]}}。整段会话的轮次序列以 JSON
 * 数组落在 {@code turns_json} 文本列里（schema 简单，读写一次到位；无需 join）。</p>
 *
 * <p><b>并发</b>：与 {@link AiConfigStore} 同款取舍 —— SQLite 单写者，持一个常驻连接由方法级
 * {@code synchronized} 串行化。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/14
 */
public final class AiSessionStore implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AiSessionStore.class);

    private static final String SQLITE_PREFIX = "jdbc:sqlite:";

    /** 新建会话的默认标题（首轮问答后由调用方改写为问题摘要）。 */
    public static final String DEFAULT_TITLE = "新对话";

    private static final String CREATE_TABLE_DDL = """
        CREATE TABLE IF NOT EXISTS ui_ai_session (
            id          TEXT    NOT NULL PRIMARY KEY,
            owner       TEXT    NOT NULL,
            title       TEXT    NOT NULL,
            turns_json  TEXT    NOT NULL,
            created_at  INTEGER NOT NULL,
            updated_at  INTEGER NOT NULL
        )
        """;

    private static final String CREATE_INDEX_DDL =
            "CREATE INDEX IF NOT EXISTS idx_ui_ai_session_owner ON ui_ai_session (owner, updated_at)";

    /** 会话摘要（列表用，不带轮次正文）。 */
    public record SessionMeta(String id, String title, long updatedAt) {
    }

    /** 会话全文（打开某会话用，{@code turnsJson} 为轮次数组的 JSON 文本）。 */
    public record SessionFull(String id, String title, String turnsJson, long updatedAt) {
    }

    private final String jdbcUrl;
    private Connection conn;

    private AiSessionStore(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    /**
     * 打开（必要时创建）会话库并建表。
     *
     * @param dbPath db 文件路径；{@code null} / 空白 / {@code :memory:} = 内存库（测试用）
     */
    public static AiSessionStore open(String dbPath) {
        boolean inMemory = dbPath == null || dbPath.isBlank() || ":memory:".equals(dbPath);
        if (!inMemory) {
            createParentDirIfNeeded(dbPath);
        }
        AiSessionStore store = new AiSessionStore(SQLITE_PREFIX + (inMemory ? ":memory:" : dbPath));
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
            log.info("AI 会话库已就绪: {}", jdbcUrl);
        } catch (SQLException e) {
            throw new IllegalStateException("初始化 AI 会话库失败: " + jdbcUrl, e);
        }
    }

    /** 某用户的会话清单（按更新时间倒序）。 */
    public synchronized List<SessionMeta> list(String owner) {
        String sql = "SELECT id, title, updated_at FROM ui_ai_session WHERE owner = ? ORDER BY updated_at DESC";
        List<SessionMeta> out = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, owner);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new SessionMeta(rs.getString("id"), rs.getString("title"), rs.getLong("updated_at")));
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("读取 AI 会话列表失败", e);
        }
        return out;
    }

    /** 读取某会话全文；不存在（或不属于该 owner）返回 null。 */
    public synchronized SessionFull get(String owner, String id) {
        if (id == null || id.isBlank()) {
            return null;
        }
        String sql = "SELECT id, title, turns_json, updated_at FROM ui_ai_session WHERE owner = ? AND id = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, owner);
            ps.setString(2, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return new SessionFull(rs.getString("id"), rs.getString("title"),
                        rs.getString("turns_json"), rs.getLong("updated_at"));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("读取 AI 会话失败", e);
        }
    }

    /** 新建一个空会话（归属 owner），返回其全文。 */
    public synchronized SessionFull create(String owner) {
        String id = UUID.randomUUID().toString();
        long now = System.currentTimeMillis();
        String sql = """
                INSERT INTO ui_ai_session (id, owner, title, turns_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, id);
            ps.setString(2, owner);
            ps.setString(3, DEFAULT_TITLE);
            ps.setString(4, "[]");
            ps.setLong(5, now);
            ps.setLong(6, now);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("新建 AI 会话失败", e);
        }
        return new SessionFull(id, DEFAULT_TITLE, "[]", now);
    }

    /** 写入会话（upsert：标题 + 轮次 JSON + 更新时间）。 */
    public synchronized void save(String owner, String id, String title, String turnsJson) {
        long now = System.currentTimeMillis();
        String sql = """
                INSERT INTO ui_ai_session (id, owner, title, turns_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    title = excluded.title,
                    turns_json = excluded.turns_json,
                    updated_at = excluded.updated_at
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, id);
            ps.setString(2, owner);
            ps.setString(3, title == null || title.isBlank() ? DEFAULT_TITLE : title);
            ps.setString(4, turnsJson == null ? "[]" : turnsJson);
            ps.setLong(5, now);
            ps.setLong(6, now);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("保存 AI 会话失败", e);
        }
    }

    /** 删除某会话（须归属该 owner）；返回是否确有删除。 */
    public synchronized boolean delete(String owner, String id) {
        String sql = "DELETE FROM ui_ai_session WHERE owner = ? AND id = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, owner);
            ps.setString(2, id);
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new IllegalStateException("删除 AI 会话失败", e);
        }
    }

    /** 删除某账号的全部会话（删除用户时清理）；返回删除条数。 */
    public synchronized int deleteAll(String owner) {
        try (PreparedStatement ps = conn.prepareStatement("DELETE FROM ui_ai_session WHERE owner = ?")) {
            ps.setString(1, owner);
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("清空 AI 会话失败: " + owner, e);
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
            log.warn("关闭 AI 会话库失败: {}", e.getMessage());
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
