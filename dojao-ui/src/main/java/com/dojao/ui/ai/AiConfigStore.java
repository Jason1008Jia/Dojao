package com.dojao.ui.ai;

import com.dojao.ai.AiConfig;
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

/**
 * AI 对话配置的持久化存储 —— 与账号体系同库（{@link AuthStore#defaultDbPath()}，默认
 * {@code ./data/dojao-ui.db}），独立连接、独立表 {@code ui_ai_config_user}。
 *
 * <p><b>按账号隔离</b>：一行一个账号（主键 {@code owner}），每个账号同一时间只保留一份模型配置
 * （一账号一个模型）。账号没有自己的配置时，回退到管理员 {@code admin} 的配置；管理员也没配置时
 * 使用默认值（{@code apiKey=null} = 未配置）。</p>
 *
 * <p><b>旧数据迁移</b>：旧版本使用全局单行表 {@code ui_ai_config}（{@code id=1}）。本类启动时若新表为空，
 * 会把旧行迁移为 {@code owner='admin'} 的配置；旧表保留不删（只读，不再写入）。</p>
 *
 * <p><b>并发</b>：与 {@link AuthStore} / {@code SqlScriptStore} 同款取舍 —— SQLite 单写者，
 * 持一个常驻连接由方法级 {@code synchronized} 串行化；开 {@code journal_mode=WAL} +
 * {@code busy_timeout=5000}。元数据量极小，非热路径。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/13
 */
public final class AiConfigStore implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AiConfigStore.class);

    private static final String SQLITE_PREFIX = "jdbc:sqlite:";

    /** 管理员账号名：未单独配置的账号回退到这份配置。 */
    public static final String ADMIN_OWNER = "admin";

    /** 默认值（空行 / 无行时套用）：DeepSeek OpenAI 兼容端点，首次使用只需填 API Key。 */
    public static final String DEFAULT_BASE_URL = "https://api.deepseek.com/";
    public static final String DEFAULT_MODEL = "deepseek-flash";
    public static final double DEFAULT_TEMPERATURE = 0.2;

    private static final String CREATE_TABLE_DDL = """
        CREATE TABLE IF NOT EXISTS ui_ai_config_user (
            owner       TEXT    NOT NULL PRIMARY KEY,
            base_url    TEXT    NOT NULL,
            api_key     TEXT,
            model       TEXT    NOT NULL,
            temperature REAL    NOT NULL,
            updated_at  INTEGER NOT NULL
        )
        """;

    /** 旧版本全局单行表（只读迁移来源）。 */
    private static final String LEGACY_TABLE = "ui_ai_config";

    private final String jdbcUrl;
    private Connection conn;

    private AiConfigStore(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    /**
     * 打开（必要时创建）配置库并建表 / 迁移旧配置。
     *
     * @param dbPath db 文件路径；{@code null} / 空白 / {@code :memory:} = 内存库（测试用）
     */
    public static AiConfigStore open(String dbPath) {
        boolean inMemory = dbPath == null || dbPath.isBlank() || ":memory:".equals(dbPath);
        if (!inMemory) {
            createParentDirIfNeeded(dbPath);
        }
        AiConfigStore store = new AiConfigStore(SQLITE_PREFIX + (inMemory ? ":memory:" : dbPath));
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
            }
            migrateLegacy();
            log.info("AI 对话配置库已就绪: {}", jdbcUrl);
        } catch (SQLException e) {
            throw new IllegalStateException("初始化 AI 对话配置库失败: " + jdbcUrl, e);
        }
    }

    /** 旧全局单行配置 → {@code admin} 账号配置（仅在新表为空且旧表存在时执行一次）。 */
    private void migrateLegacy() throws SQLException {
        if (!tableExists(LEGACY_TABLE) || !tableIsEmpty("ui_ai_config_user")) {
            return;
        }
        String sql = "SELECT base_url, api_key, model, temperature FROM " + LEGACY_TABLE + " WHERE id = 1";
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            if (!rs.next()) {
                return;
            }
            AiConfig legacy = new AiConfig(rs.getString("base_url"), rs.getString("api_key"),
                    rs.getString("model"), rs.getDouble("temperature"));
            save(ADMIN_OWNER, legacy);
            log.info("已把旧的全局 AI 配置迁移为管理员账号配置（{}）", ADMIN_OWNER);
        }
    }

    private boolean tableExists(String table) throws SQLException {
        String sql = "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, table);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private boolean tableIsEmpty(String table) throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + table)) {
            return !rs.next() || rs.getInt(1) == 0;
        }
    }

    // ==================== 配置 ====================

    /**
     * 读取账号的<b>生效</b>配置：本账号自己的行 → 管理员配置 → 默认值。
     * {@code apiKey=null} 表示未配置。
     */
    public synchronized AiConfig load(String owner) {
        AiConfig own = loadOwn(owner);
        if (own != null) {
            return own;
        }
        if (!ADMIN_OWNER.equals(normalize(owner))) {
            AiConfig admin = loadOwn(ADMIN_OWNER);
            if (admin != null) {
                return admin;
            }
        }
        return defaults();
    }

    /** 读取账号自己保存的配置；没有行返回 {@code null}（用于区分「继承管理员配置」）。 */
    public synchronized AiConfig loadOwn(String owner) {
        String sql = "SELECT base_url, api_key, model, temperature FROM ui_ai_config_user WHERE owner = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, normalize(owner));
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                String baseUrl = rs.getString("base_url");
                String model = rs.getString("model");
                return new AiConfig(
                        baseUrl == null || baseUrl.isBlank() ? DEFAULT_BASE_URL : baseUrl,
                        rs.getString("api_key"),
                        model == null || model.isBlank() ? DEFAULT_MODEL : model,
                        rs.getDouble("temperature"));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("读取 AI 对话配置失败: " + owner, e);
        }
    }

    /** 账号是否保存过自己的配置（false = 正在使用管理员配置 / 默认值）。 */
    public synchronized boolean hasOwn(String owner) {
        String sql = "SELECT 1 FROM ui_ai_config_user WHERE owner = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, normalize(owner));
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("查询 AI 对话配置失败: " + owner, e);
        }
    }

    /** 保存账号配置（按 owner upsert）。 */
    public synchronized void save(String owner, AiConfig cfg) {
        String sql = """
                INSERT INTO ui_ai_config_user (owner, base_url, api_key, model, temperature, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(owner) DO UPDATE SET
                    base_url = excluded.base_url,
                    api_key = excluded.api_key,
                    model = excluded.model,
                    temperature = excluded.temperature,
                    updated_at = excluded.updated_at
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, normalize(owner));
            ps.setString(2, cfg.baseUrl());
            ps.setString(3, cfg.apiKey());
            ps.setString(4, cfg.model());
            ps.setDouble(5, cfg.temperature());
            ps.setLong(6, System.currentTimeMillis());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("保存 AI 对话配置失败: " + owner, e);
        }
    }

    /** 删除账号配置（删除用户时清理；返回是否删除了一行）。 */
    public synchronized boolean delete(String owner) {
        try (PreparedStatement ps = conn.prepareStatement(
                "DELETE FROM ui_ai_config_user WHERE owner = ?")) {
            ps.setString(1, normalize(owner));
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new IllegalStateException("删除 AI 对话配置失败: " + owner, e);
        }
    }

    private static AiConfig defaults() {
        return new AiConfig(DEFAULT_BASE_URL, null, DEFAULT_MODEL, DEFAULT_TEMPERATURE);
    }

    private static String normalize(String owner) {
        return owner == null || owner.isBlank() ? ADMIN_OWNER : owner;
    }

    @Override
    public synchronized void close() {
        if (conn == null) {
            return;
        }
        try {
            conn.close();
        } catch (SQLException e) {
            log.warn("关闭 AI 对话配置库失败: {}", e.getMessage());
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
