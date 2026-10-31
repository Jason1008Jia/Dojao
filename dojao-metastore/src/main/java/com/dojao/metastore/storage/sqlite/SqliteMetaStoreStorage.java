package com.dojao.metastore.storage.sqlite;

import com.dojao.metastore.storage.MetaStoreStorage;
import com.dojao.spi.bean.FunctionDefinition;
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
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 本地 <b>SQLite 单文件</b>元数据存储实现：catalog 注册信息 + 持久化函数定义落在
 * 一个 db 文件里（默认 {@code ./data/dojao-metastore.db}，{@code -Dsystem.metastore.sqlite.path} 覆盖），
 * <b>进程重启后仍在</b> —— 对比 {@link com.dojao.metastore.storage.local.LocalMetaStoreStorage}
 * 的纯内存实现（退出即丢，仅测试用）。
 *
 * <p>与 MySQL/PG/SQLServer 实现的差别：那些走 JDBC 连外部数据库、带内存缓存；这里数据量极小
 * （catalog 与函数各几十行），<b>不做缓存，全部直读库</b> —— 既省事，也避免 Master / Worker
 * 多进程共用同一文件时各自缓存不一致。</p>
 *
 * <p><b>并发</b>：SQLite 单写者模型，故持有一个常驻 {@link Connection} 并由方法级
 * {@code synchronized} 串行化（元数据操作是启动期低频调用，不是热路径）。开
 * {@code journal_mode=WAL} + {@code busy_timeout=5000}，以便 Master 与 N 个 Worker
 * 同时打开同一 db 文件时，写冲突是「等待重试」而不是直接抛 {@code SQLITE_BUSY}。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/17
 */
public class SqliteMetaStoreStorage implements MetaStoreStorage {

    private static final Logger log = LoggerFactory.getLogger(SqliteMetaStoreStorage.class);

    private static final String SQLITE_PREFIX = "jdbc:sqlite:";

    /** 时间格式：yyyy-MM-dd HH:mm:ss.SSS（与 MysqlMetaStoreStorage 一致）。 */
    private static final DateTimeFormatter DT_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private static final String CREATE_CATALOG_DDL = """
        CREATE TABLE IF NOT EXISTS catalog (
            name       TEXT NOT NULL PRIMARY KEY,
            type       TEXT NOT NULL,
            properties TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            owner      TEXT
        )
        """;

    /** CREATE FUNCTION 持久化表（LOCAL 内存版的重启丢失问题在此消除）。 */
    private static final String CREATE_FUNCTION_DDL = """
        CREATE TABLE IF NOT EXISTS dojao_functions (
            name       TEXT NOT NULL PRIMARY KEY,
            type       TEXT NOT NULL,
            jar_path   TEXT,
            class_name TEXT,
            properties TEXT,
            comment    TEXT,
            created_at TEXT,
            updated_at TEXT,
            owner      TEXT
        )
        """;

    private static final String FUNCTION_COLUMNS =
            "name, type, jar_path, class_name, properties, comment, created_at, updated_at, owner";

    private final String jdbcUrl;
    /** db 文件路径；{@code null} = 内存库（{@code :memory:}，不落盘）。 */
    private final String dbFilePath;

    /** 常驻连接（SQLite 无连接池概念，单连接 + 方法级串行化即可）。 */
    private Connection conn;

    private volatile boolean ready = false;

    public SqliteMetaStoreStorage(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl != null ? jdbcUrl : SQLITE_PREFIX + ":memory:";
        String path = this.jdbcUrl.startsWith(SQLITE_PREFIX)
                ? this.jdbcUrl.substring(SQLITE_PREFIX.length())
                : null;
        this.dbFilePath = (path == null || path.isBlank() || ":memory:".equals(path)) ? null : path;
    }

    // ==================== 生命周期 ====================

    @Override
    public synchronized void initialize() {
        if (ready) {
            return;
        }
        createParentDirIfNeeded();
        try {
            conn = DriverManager.getConnection(jdbcUrl);
            try (Statement st = conn.createStatement()) {
                // WAL：读写不互斥（Master 与 Worker 共用同一文件）；busy_timeout：写冲突等待 5s 再报错
                st.execute("PRAGMA journal_mode=WAL");
                st.execute("PRAGMA busy_timeout=5000");
                st.execute(CREATE_CATALOG_DDL);
                st.execute(CREATE_FUNCTION_DDL);
                // 旧库迁移：补 owner 列（CREATE TABLE IF NOT EXISTS 不会给已存在的表加列）
                ensureColumn(st, "catalog", "owner");
                ensureColumn(st, "dojao_functions", "owner");
            }
            ready = true;
            log.info("SQLite 元数据存储已就绪: {}", jdbcUrl);
        } catch (SQLException e) {
            throw new RuntimeException("初始化 SQLite 元数据存储失败: " + jdbcUrl, e);
        }
    }

    @Override
    public synchronized void close() {
        ready = false;
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                log.warn("关闭 SQLite 连接失败: {}", e.getMessage());
            } finally {
                conn = null;
            }
        }
    }

    @Override
    public boolean isReady() {
        return ready;
    }

    // ==================== Catalog 操作 ====================

    @Override
    public synchronized void registerCatalog(String catalogName, String connectorType,
                                             Map<String, String> properties) {
        registerCatalog(catalogName, connectorType, properties, null);
    }

    @Override
    public synchronized void registerCatalog(String catalogName, String connectorType,
                                             Map<String, String> properties, String owner) {
        ensureReady();
        String now = now();
        // ON CONFLICT 不更新 created_at → 重复注册（Master 与 Worker 各注册一次同一 catalog）保留首次创建时间
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO catalog (name, type, properties, created_at, updated_at, owner)"
                + " VALUES (?, ?, ?, ?, ?, ?)"
                + " ON CONFLICT(name) DO UPDATE SET"
                + " type = excluded.type,"
                + " properties = excluded.properties,"
                + " owner = excluded.owner,"
                + " updated_at = excluded.updated_at")) {
            ps.setString(1, catalogName);
            ps.setString(2, connectorType);
            ps.setString(3, propertiesToJson(properties));
            ps.setString(4, now);
            ps.setString(5, now);
            ps.setString(6, owner);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("注册 catalog 失败: " + catalogName, e);
        }
    }

    @Override
    public synchronized Optional<String> getCatalogOwner(String catalogName) {
        return queryString("SELECT owner FROM catalog WHERE name = ?", catalogName)
                .filter(s -> !s.isBlank());
    }

    @Override
    public synchronized void unregisterCatalog(String catalogName) {
        ensureReady();
        try (PreparedStatement ps = conn.prepareStatement("DELETE FROM catalog WHERE name = ?")) {
            ps.setString(1, catalogName);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("注销 catalog 失败: " + catalogName, e);
        }
    }

    @Override
    public synchronized List<String> listCatalogs() {
        ensureReady();
        List<String> result = new ArrayList<>();
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT name FROM catalog ORDER BY name")) {
            while (rs.next()) {
                result.add(rs.getString(1));
            }
        } catch (SQLException e) {
            throw new RuntimeException("读取 catalog 列表失败", e);
        }
        return result;
    }

    @Override
    public synchronized Optional<String> getConnectorType(String catalogName) {
        return queryString("SELECT type FROM catalog WHERE name = ?", catalogName);
    }

    @Override
    public synchronized Map<String, String> getCatalogProperties(String catalogName) {
        return queryString("SELECT properties FROM catalog WHERE name = ?", catalogName)
                .map(SqliteMetaStoreStorage::parseProperties)
                .orElseGet(() -> Collections.emptyMap());
    }

    @Override
    public synchronized boolean catalogExists(String catalogName) {
        return queryString("SELECT name FROM catalog WHERE name = ?", catalogName).isPresent();
    }

    @Override
    public synchronized Optional<String> getCatalogCreatedAt(String catalogName) {
        return queryString("SELECT created_at FROM catalog WHERE name = ?", catalogName);
    }

    @Override
    public synchronized Optional<String> getCatalogUpdatedAt(String catalogName) {
        return queryString("SELECT updated_at FROM catalog WHERE name = ?", catalogName);
    }

    // ==================== Function 操作 ====================

    @Override
    public synchronized void registerFunction(FunctionDefinition def) {
        ensureReady();
        String now = now();
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO dojao_functions (" + FUNCTION_COLUMNS + ")"
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
                + " ON CONFLICT(name) DO UPDATE SET"
                + " type = excluded.type,"
                + " jar_path = excluded.jar_path,"
                + " class_name = excluded.class_name,"
                + " properties = excluded.properties,"
                + " comment = excluded.comment,"
                + " owner = excluded.owner,"
                + " updated_at = excluded.updated_at")) {
            ps.setString(1, def.name());
            ps.setString(2, def.type());
            ps.setString(3, def.jarPath());
            ps.setString(4, def.className());
            ps.setString(5, propertiesToJson(def.properties()));
            ps.setString(6, def.comment());
            ps.setString(7, def.createdAt() != null ? def.createdAt() : now);
            ps.setString(8, def.updatedAt() != null ? def.updatedAt() : now);
            ps.setString(9, def.owner());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("持久化函数失败: " + def.name(), e);
        }
    }

    @Override
    public synchronized void dropFunction(String name) {
        ensureReady();
        try (PreparedStatement ps = conn.prepareStatement("DELETE FROM dojao_functions WHERE name = ?")) {
            ps.setString(1, name);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("删除函数失败: " + name, e);
        }
    }

    @Override
    public synchronized Optional<FunctionDefinition> getFunction(String name) {
        ensureReady();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT " + FUNCTION_COLUMNS + " FROM dojao_functions WHERE name = ?")) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(readFunction(rs)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new RuntimeException("读取函数失败: " + name, e);
        }
    }

    @Override
    public synchronized List<FunctionDefinition> listFunctions() {
        ensureReady();
        List<FunctionDefinition> result = new ArrayList<>();
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT " + FUNCTION_COLUMNS + " FROM dojao_functions ORDER BY name")) {
            while (rs.next()) {
                result.add(readFunction(rs));
            }
        } catch (SQLException e) {
            throw new RuntimeException("读取函数列表失败", e);
        }
        return result;
    }

    // ==================== 私有方法 ====================

    /** 未初始化时自动补一次（与 MysqlMetaStoreStorage 的懒加载行为一致）。 */
    private void ensureReady() {
        if (!ready) {
            initialize();
        }
    }

    /** 建 db 文件所在目录（文件本身由 SQLite 自动创建）。 */
    private void createParentDirIfNeeded() {
        if (dbFilePath == null) {
            return;
        }
        Path parent = Path.of(dbFilePath).toAbsolutePath().getParent();
        if (parent == null) {
            return;
        }
        try {
            Files.createDirectories(parent);
        } catch (IOException e) {
            throw new RuntimeException("创建 SQLite 目录失败: " + parent, e);
        }
    }

    /** 旧库迁移：table 若无 column 列则 ALTER TABLE 补上（列已存在则跳过）。 */
    private static void ensureColumn(Statement st, String table, String column) throws SQLException {
        boolean exists = false;
        try (ResultSet rs = st.executeQuery("PRAGMA table_info(" + table + ")")) {
            while (rs.next()) {
                if (column.equalsIgnoreCase(rs.getString("name"))) {
                    exists = true;
                    break;
                }
            }
        }
        if (!exists) {
            st.execute("ALTER TABLE " + table + " ADD COLUMN " + column + " TEXT");
        }
    }

    /** 单列单行查询：无行返回 {@link Optional#empty()}。 */
    private Optional<String> queryString(String sql, String param) {
        ensureReady();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, param);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.ofNullable(rs.getString(1)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new RuntimeException("SQLite 查询失败: " + sql, e);
        }
    }

    private FunctionDefinition readFunction(ResultSet rs) throws SQLException {
        return new FunctionDefinition(
                rs.getString("name"),
                rs.getString("type"),
                rs.getString("jar_path"),
                rs.getString("class_name"),
                parseProperties(rs.getString("properties")),
                rs.getString("comment"),
                rs.getString("created_at"),
                rs.getString("updated_at"),
                rs.getString("owner"));
    }

    private static String now() {
        return LocalDateTime.now().format(DT_FMT);
    }

    /** 简单的 JSON 序列化（与 MysqlMetaStoreStorage 同款，避免为元数据引入 Jackson）。 */
    private static String propertiesToJson(Map<String, String> props) {
        if (props == null || props.isEmpty()) return "{}";
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, String> e : props.entrySet()) {
            if (!first) sb.append(",");
            sb.append("\"").append(escapeJson(e.getKey())).append("\":\"")
                    .append(escapeJson(e.getValue())).append("\"");
            first = false;
        }
        sb.append("}");
        return sb.toString();
    }

    private static Map<String, String> parseProperties(String json) {
        Map<String, String> result = new LinkedHashMap<>();
        if (json == null || json.isEmpty() || "{}".equals(json)) return result;
        String content = json.substring(1, json.length() - 1).trim();
        if (content.isEmpty()) return result;
        String[] pairs = content.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
        for (String pair : pairs) {
            String[] kv = pair.trim().split(":", 2);
            if (kv.length == 2) {
                result.put(unquote(kv[0].trim()), unquote(kv[1].trim()));
            }
        }
        return result;
    }

    private static String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String unquote(String s) {
        if (s.startsWith("\"") && s.endsWith("\"")) {
            return s.substring(1, s.length() - 1)
                    .replace("\\\"", "\"")
                    .replace("\\\\", "\\");
        }
        return s;
    }
}
