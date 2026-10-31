package com.dojao.ui.script;

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
 * 控制台「SQL 脚本」的持久化存储 —— 与账号体系同库（{@link AuthStore#defaultDbPath()}，默认
 * {@code ./data/dojao-ui.db}），独立连接、独立表 {@code ui_sql_script}。
 *
 * <p>脚本<b>按登录用户隔离</b>（owner = 当前会话用户名）：这是「跟随登录状态」的隔离方式 ——
 * 每个用户只看到、只能读写自己的脚本，无需任何 {@code is_private} 显式声明。</p>
 *
 * <p><b>并发</b>：与 {@link AuthStore} 同款取舍 —— SQLite 单写者，持一个常驻连接由方法级
 * {@code synchronized} 串行化；开 {@code journal_mode=WAL} + {@code busy_timeout=5000}
 * （与账号库同文件、各自连接，靠 WAL 的多写者能力 + busy 退避共存）。元数据量极小，非热路径。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/12
 */
public final class SqlScriptStore implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SqlScriptStore.class);

    private static final String SQLITE_PREFIX = "jdbc:sqlite:";

    private static final String CREATE_TABLE_DDL = """
        CREATE TABLE IF NOT EXISTS ui_sql_script (
            id         TEXT    NOT NULL PRIMARY KEY,
            owner      TEXT    NOT NULL,
            name       TEXT    NOT NULL,
            content    TEXT    NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        )
        """;

    /** 同一 owner 下脚本名唯一（保存同名 = 覆盖，不新增）。 */
    private static final String CREATE_INDEX_DDL =
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_ui_sql_script_owner_name ON ui_sql_script(owner, name)";

    private final String jdbcUrl;
    private Connection conn;

    private SqlScriptStore(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    /**
     * 打开（必要时创建）脚本库并建表。
     *
     * @param dbPath db 文件路径；{@code null} / 空白 / {@code :memory:} = 内存库（测试用）
     */
    public static SqlScriptStore open(String dbPath) {
        boolean inMemory = dbPath == null || dbPath.isBlank() || ":memory:".equals(dbPath);
        if (!inMemory) {
            createParentDirIfNeeded(dbPath);
        }
        SqlScriptStore store = new SqlScriptStore(SQLITE_PREFIX + (inMemory ? ":memory:" : dbPath));
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
            log.info("控制台 SQL 脚本库已就绪: {}", jdbcUrl);
        } catch (SQLException e) {
            throw new IllegalStateException("初始化控制台 SQL 脚本库失败: " + jdbcUrl, e);
        }
    }

    // ==================== 脚本 ====================

    /** 脚本元信息（不含正文，列表用；正文见 {@link #content}）。 */
    public record ScriptMeta(String id, String name, long createdAt, long updatedAt) {
    }

    /** 指定用户的脚本清单，按最近更新倒序。 */
    public synchronized List<ScriptMeta> list(String owner) {
        String sql = """
                SELECT id, name, created_at, updated_at
                  FROM ui_sql_script
                 WHERE owner = ?
                 ORDER BY updated_at DESC
                """;
        List<ScriptMeta> out = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, owner);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new ScriptMeta(rs.getString("id"), rs.getString("name"),
                            rs.getLong("created_at"), rs.getLong("updated_at")));
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("查询控制台 SQL 脚本失败: " + owner, e);
        }
        return out;
    }

    /**
     * 保存脚本：同一 owner 下同名则覆盖正文（保留 id 与创建时间），否则新建。
     *
     * @return 落库后的元信息（含 id，供前端后续「加载 / 删除」引用）
     */
    public synchronized ScriptMeta save(String owner, String name, String content) {
        long now = System.currentTimeMillis();
        try {
            String existingId = findIdByName(owner, name);
            if (existingId != null) {
                String sql = "UPDATE ui_sql_script SET content = ?, updated_at = ? WHERE id = ?";
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setString(1, content);
                    ps.setLong(2, now);
                    ps.setString(3, existingId);
                    ps.executeUpdate();
                }
                return new ScriptMeta(existingId, name, createdAtOf(existingId), now);
            }
            String id = UUID.randomUUID().toString();
            String sql = """
                    INSERT INTO ui_sql_script (id, owner, name, content, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """;
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, id);
                ps.setString(2, owner);
                ps.setString(3, name);
                ps.setString(4, content);
                ps.setLong(5, now);
                ps.setLong(6, now);
                ps.executeUpdate();
            }
            return new ScriptMeta(id, name, now, now);
        } catch (SQLException e) {
            throw new IllegalStateException("保存控制台 SQL 脚本失败: " + owner + "/" + name, e);
        }
    }

    /** 读取脚本正文；不存在 / 非本人所有返回 {@code null}。 */
    public synchronized String content(String owner, String id) {
        String sql = "SELECT content FROM ui_sql_script WHERE id = ? AND owner = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, id);
            ps.setString(2, owner);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString("content") : null;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("读取控制台 SQL 脚本失败: " + owner + "/" + id, e);
        }
    }

    /** 读取脚本元信息（含正文）；不存在 / 非本人所有返回 {@code null}。 */
    public synchronized ScriptFull get(String owner, String id) {
        String sql = "SELECT id, name, content, created_at, updated_at "
                + "FROM ui_sql_script WHERE id = ? AND owner = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, id);
            ps.setString(2, owner);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return new ScriptFull(rs.getString("id"), rs.getString("name"), rs.getString("content"),
                        rs.getLong("created_at"), rs.getLong("updated_at"));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("读取控制台 SQL 脚本失败: " + owner + "/" + id, e);
        }
    }

    /** 删除脚本（仅本人所有）。返回是否删除了记录。 */
    public synchronized boolean delete(String owner, String id) {
        try (PreparedStatement ps = conn.prepareStatement(
                "DELETE FROM ui_sql_script WHERE id = ? AND owner = ?")) {
            ps.setString(1, id);
            ps.setString(2, owner);
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new IllegalStateException("删除控制台 SQL 脚本失败: " + owner + "/" + id, e);
        }
    }

    /** 删除某账号的全部脚本（删除用户时清理）；返回删除条数。 */
    public synchronized int deleteAll(String owner) {
        try (PreparedStatement ps = conn.prepareStatement("DELETE FROM ui_sql_script WHERE owner = ?")) {
            ps.setString(1, owner);
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("清空控制台 SQL 脚本失败: " + owner, e);
        }
    }

    /** 脚本完整信息（列表项 + 正文，加载进编辑器用）。 */
    public record ScriptFull(String id, String name, String content, long createdAt, long updatedAt) {
    }

    @Override
    public synchronized void close() {
        if (conn == null) {
            return;
        }
        try {
            conn.close();
        } catch (SQLException e) {
            log.warn("关闭控制台 SQL 脚本库失败: {}", e.getMessage());
        } finally {
            conn = null;
        }
    }

    // ==================== 内部工具 ====================

    private String findIdByName(String owner, String name) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT id FROM ui_sql_script WHERE owner = ? AND name = ?")) {
            ps.setString(1, owner);
            ps.setString(2, name);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString("id") : null;
            }
        }
    }

    private long createdAtOf(String id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT created_at FROM ui_sql_script WHERE id = ?")) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong("created_at") : 0L;
            }
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
