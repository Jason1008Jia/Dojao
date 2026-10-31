package com.dojao.ui.auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;

/**
 * 控制台登录用户与会话的持久化存储 —— 独立 SQLite 单文件（默认 {@code ./data/dojao-ui.db}，
 * {@code -Dsystem.ui.db.path} 覆盖）。
 *
 * <p>为什么不复用 {@code dojao-metastore.db}：那边是 Dojao 自身注册表（catalog + 函数），
 * 这里是 Web 控制台的账号体系。分文件可以各写各的、不必让两个写入者去争同一把 SQLite 写锁，
 * 也不必被 MetaStore 的建表迁移牵连。</p>
 *
 * <p><b>并发</b>：与 {@code SqliteMetaStoreStorage} 同款取舍 —— SQLite 单写者，持一个常驻连接
 * 由方法级 {@code synchronized} 串行化；开 {@code journal_mode=WAL} + {@code busy_timeout=5000}。
 * 元数据量极小，不是热路径。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/17
 */
public final class AuthStore implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AuthStore.class);

    private static final String SQLITE_PREFIX = "jdbc:sqlite:";

    /** 首个管理员的默认口令；{@code -Dsystem.ui.admin.password} 可覆盖（仅在账号表为空时生效）。 */
    public static final String DEFAULT_ADMIN_PASSWORD = "Admin@123456";

    private static final String CREATE_USER_DDL = """
        CREATE TABLE IF NOT EXISTS ui_user (
            username     TEXT    NOT NULL PRIMARY KEY,
            password     TEXT    NOT NULL,
            role         TEXT    NOT NULL,
            display_name TEXT,
            created_at   INTEGER NOT NULL,
            updated_at   INTEGER NOT NULL
        )
        """;

    private static final String CREATE_SESSION_DDL = """
        CREATE TABLE IF NOT EXISTS ui_session (
            token      TEXT    NOT NULL PRIMARY KEY,
            username   TEXT    NOT NULL,
            created_at INTEGER NOT NULL,
            expires_at INTEGER NOT NULL
        )
        """;

    private static final SecureRandom RANDOM = new SecureRandom();

    private final String jdbcUrl;
    private final long sessionTtlMs;
    private Connection conn;

    private AuthStore(String jdbcUrl, long sessionTtlMs) {
        this.jdbcUrl = jdbcUrl;
        this.sessionTtlMs = sessionTtlMs;
    }

    /**
     * 打开（必要时创建）控制台账号库并建表。
     *
     * @param dbPath       db 文件路径；{@code null} / 空白 / {@code :memory:} = 内存库（测试用）
     * @param sessionTtlMs 会话有效期（毫秒）
     */
    public static AuthStore open(String dbPath, long sessionTtlMs) {
        boolean inMemory = dbPath == null || dbPath.isBlank() || ":memory:".equals(dbPath);
        if (!inMemory) {
            createParentDirIfNeeded(dbPath);
        }
        AuthStore store = new AuthStore(SQLITE_PREFIX + (inMemory ? ":memory:" : dbPath), sessionTtlMs);
        store.initSchema();
        return store;
    }

    /** 默认 db 路径：{@code ./data/dojao-ui.db}（与 conf/ 同级，已在 .gitignore 的 data/ 下）。 */
    public static String defaultDbPath() {
        return System.getProperty("system.ui.db.path", "./data/dojao-ui.db");
    }

    /** 默认会话有效期：12 小时，{@code -Dsystem.ui.session.hours} 覆盖。 */
    public static long defaultSessionTtlMs() {
        long hours = Long.getLong("system.ui.session.hours", 12L);
        return Math.max(1L, hours) * 3600_000L;
    }

    private void initSchema() {
        try {
            conn = DriverManager.getConnection(jdbcUrl);
            try (Statement st = conn.createStatement()) {
                st.execute("PRAGMA journal_mode=WAL");
                st.execute("PRAGMA busy_timeout=5000");
                st.execute(CREATE_USER_DDL);
                st.execute(CREATE_SESSION_DDL);
                // 角色迁移：旧版本只有 ADMIN / USER，USER 统一归入分析师（最小权限）。
                st.executeUpdate("UPDATE ui_user SET role = 'ANALYST' WHERE UPPER(role) = 'USER'");
            }
            log.info("控制台账号库已就绪: {}", jdbcUrl);
        } catch (SQLException e) {
            throw new IllegalStateException("初始化控制台账号库失败: " + jdbcUrl, e);
        }
    }

    // ==================== 引导：首个管理员 ====================

    /**
     * 账号表为空时创建首个管理员 {@code admin}。
     *
     * <p>口令来源：{@code -Dsystem.ui.admin.password} 优先，未配置用 {@link #DEFAULT_ADMIN_PASSWORD}。
     * 账号表非空时本方法不做任何事（配置项被忽略）—— 口令只在首次建库时落库，之后改口令需另行处理。</p>
     */
    public synchronized void ensureBootstrapAdmin(String configuredPassword) {
        if (countUsers() > 0) {
            if (configuredPassword != null && !configuredPassword.isBlank()) {
                log.info("控制台已有账号，-Dsystem.ui.admin.password 已忽略（口令只在首次建库时生效）");
            }
            return;
        }
        boolean byDefault = configuredPassword == null || configuredPassword.isBlank();
        String password = byDefault ? DEFAULT_ADMIN_PASSWORD : configuredPassword;
        createUser("admin", password, Role.ADMIN, "Administrator");
        if (byDefault) {
            log.warn("""

                    ============================================================
                      控制台已创建初始管理员账号（默认口令，请尽快修改）
                        用户名 : admin
                        口令   : {}
                      如需固定口令，删库后带 -Dsystem.ui.admin.password=*** 重启
                    ============================================================""", DEFAULT_ADMIN_PASSWORD);
        } else {
            log.info("控制台已按 -Dsystem.ui.admin.password 创建初始管理员账号: admin");
        }
    }

    // ==================== 用户 ====================

    /** 新建/覆盖一个用户（口令即时哈希，明文不落库）。 */
    public synchronized void createUser(String username, String password, Role role, String displayName) {
        long now = System.currentTimeMillis();
        String sql = """
                INSERT INTO ui_user (username, password, role, display_name, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(username) DO UPDATE SET
                    password = excluded.password,
                    role = excluded.role,
                    display_name = excluded.display_name,
                    updated_at = excluded.updated_at
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, username);
            ps.setString(2, PasswordHasher.hash(password));
            ps.setString(3, role.name());
            ps.setString(4, displayName);
            ps.setLong(5, now);
            ps.setLong(6, now);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("写入控制台用户失败: " + username, e);
        }
    }

    /** 校验用户名 + 口令；不通过返回空。 */
    public synchronized Optional<User> authenticate(String username, String password) {
        if (username == null || password == null) {
            return Optional.empty();
        }
        String sql = "SELECT username, password, role, display_name FROM ui_user WHERE username = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, username);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next() || !PasswordHasher.verify(password, rs.getString("password"))) {
                    return Optional.empty();
                }
                return Optional.of(new User(rs.getString("username"),
                        Role.of(rs.getString("role")), rs.getString("display_name")));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("查询控制台用户失败: " + username, e);
        }
    }

    public synchronized int countUsers() {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM ui_user")) {
            return rs.next() ? rs.getInt(1) : 0;
        } catch (SQLException e) {
            throw new IllegalStateException("统计控制台用户失败", e);
        }
    }

    /** 用户管理列表项（含时间戳，供「用户管理」界面展示；不含口令哈希）。 */
    public record UserEntry(String username, Role role, String displayName,
                            long createdAt, long updatedAt) {
    }

    /** 全部账号（按用户名排序）。 */
    public synchronized List<UserEntry> listUsers() {
        String sql = "SELECT username, role, display_name, created_at, updated_at "
                + "FROM ui_user ORDER BY username";
        List<UserEntry> out = new ArrayList<>();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                out.add(new UserEntry(rs.getString("username"),
                        Role.of(rs.getString("role")),
                        rs.getString("display_name"),
                        rs.getLong("created_at"),
                        rs.getLong("updated_at")));
            }
            return out;
        } catch (SQLException e) {
            throw new IllegalStateException("读取控制台用户列表失败", e);
        }
    }

    /** 按用户名查询（不存在返回空）。 */
    public synchronized Optional<User> findUser(String username) {
        if (username == null || username.isBlank()) {
            return Optional.empty();
        }
        String sql = "SELECT username, role, display_name FROM ui_user WHERE username = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, username);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }
                return Optional.of(new User(rs.getString("username"),
                        Role.of(rs.getString("role")), rs.getString("display_name")));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("查询控制台用户失败: " + username, e);
        }
    }

    /** 管理员账号数量（用于「至少保留一个管理员」保护）。 */
    public synchronized int countAdmins() {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM ui_user WHERE role = 'ADMIN'")) {
            return rs.next() ? rs.getInt(1) : 0;
        } catch (SQLException e) {
            throw new IllegalStateException("统计管理员账号失败", e);
        }
    }

    /**
     * 修改用户资料 / 角色；{@code newPassword} 为空 = 不改口令。
     *
     * @return 用户是否存在（不存在返回 false，不做任何修改）
     */
    public synchronized boolean updateUser(String username, Role role, String displayName,
                                           String newPassword) {
        if (username == null || username.isBlank()) {
            return false;
        }
        String sql = (newPassword == null || newPassword.isBlank())
                ? "UPDATE ui_user SET role = ?, display_name = ?, updated_at = ? WHERE username = ?"
                : "UPDATE ui_user SET role = ?, display_name = ?, password = ?, updated_at = ? WHERE username = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, role.name());
            ps.setString(2, displayName);
            int idx = 3;
            if (newPassword != null && !newPassword.isBlank()) {
                ps.setString(idx++, PasswordHasher.hash(newPassword));
            }
            ps.setLong(idx++, System.currentTimeMillis());
            ps.setString(idx, username);
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new IllegalStateException("更新控制台用户失败: " + username, e);
        }
    }

    /** 删除用户及其全部会话；返回用户是否存在（不存在返回 false）。 */
    public synchronized boolean deleteUser(String username) {
        if (username == null || username.isBlank() || findUser(username).isEmpty()) {
            return false;
        }
        try {
            try (PreparedStatement ps = conn.prepareStatement("DELETE FROM ui_session WHERE username = ?")) {
                ps.setString(1, username);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement("DELETE FROM ui_user WHERE username = ?")) {
                ps.setString(1, username);
                return ps.executeUpdate() > 0;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("删除控制台用户失败: " + username, e);
        }
    }

    // ==================== 会话 ====================

    /** 为用户开一个新会话，返回不透明 token（32 字节随机 hex）。 */
    public synchronized String newSession(String username) {
        purgeExpired();
        byte[] raw = new byte[32];
        RANDOM.nextBytes(raw);
        String token = HexFormat.of().formatHex(raw);
        long now = System.currentTimeMillis();
        String sql = "INSERT INTO ui_session (token, username, created_at, expires_at) VALUES (?, ?, ?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, token);
            ps.setString(2, username);
            ps.setLong(3, now);
            ps.setLong(4, now + sessionTtlMs);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("创建控制台会话失败: " + username, e);
        }
        return token;
    }

    /** token → 用户；token 缺失/未知/已过期一律返回空。 */
    public synchronized Optional<User> resolve(String token) {
        if (token == null || token.isBlank()) {
            return Optional.empty();
        }
        String sql = """
                SELECT u.username, u.role, u.display_name
                  FROM ui_session s JOIN ui_user u ON u.username = s.username
                 WHERE s.token = ? AND s.expires_at > ?
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, token);
            ps.setLong(2, System.currentTimeMillis());
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }
                return Optional.of(new User(rs.getString("username"),
                        Role.of(rs.getString("role")), rs.getString("display_name")));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("解析控制台会话失败", e);
        }
    }

    /** 注销单个会话（登出）。 */
    public synchronized void invalidate(String token) {
        if (token == null || token.isBlank()) {
            return;
        }
        try (PreparedStatement ps = conn.prepareStatement("DELETE FROM ui_session WHERE token = ?")) {
            ps.setString(1, token);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("注销控制台会话失败", e);
        }
    }

    private void purgeExpired() {
        try (PreparedStatement ps = conn.prepareStatement("DELETE FROM ui_session WHERE expires_at <= ?")) {
            ps.setLong(1, System.currentTimeMillis());
            ps.executeUpdate();
        } catch (SQLException e) {
            log.warn("清理过期会话失败: {}", e.getMessage());
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
            log.warn("关闭控制台账号库失败: {}", e.getMessage());
        } finally {
            conn = null;
        }
    }

    // ==================== 工具 ====================

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
