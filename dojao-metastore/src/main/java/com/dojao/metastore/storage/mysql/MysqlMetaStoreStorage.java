package com.dojao.metastore.storage.mysql;

import com.dojao.metastore.storage.MetaStoreStorage;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * MySQL 外接存储实现（默认）。
 *
 * <p>启动流程：
 * <ol>
 *   <li>从 JDBC URL 中提取数据库名</li>
 *   <li>连接 MySQL 服务器（不指定数据库），CREATE DATABASE IF NOT EXISTS</li>
 *   <li>连接目标数据库，执行 init-mysql.sql（建表语句）</li>
 *   <li>加载已有数据到内存缓存</li>
 * </ol>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/6/27
 */
public class MysqlMetaStoreStorage implements MetaStoreStorage {

    private static final String TABLE_NAME = "catalog";

    /** 建表 DDL（与 init-mysql.sql 内容完全一致，内联避免 classpath 依赖问题）。 */
    private static final String CREATE_TABLE_DDL = """
        CREATE TABLE IF NOT EXISTS catalog (
            name         VARCHAR(255)  NOT NULL PRIMARY KEY  COMMENT 'Catalog 名称（唯一标识）',
            type         VARCHAR(100)  NOT NULL              COMMENT '连接器类型：mysql / postgres / sqlserver / hive 等',
            properties   VARCHAR(1000)                       COMMENT 'Catalog 属性（JSON 格式，包含 jdbcUrl 等连接信息）',
            created_at   VARCHAR(23)   NOT NULL              COMMENT '创建时间 yyyy-MM-dd HH:mm:ss.SSS',
            updated_at   VARCHAR(23)   NOT NULL              COMMENT '更新时间 yyyy-MM-dd HH:mm:ss.SSS',
            owner        VARCHAR(255)                        COMMENT '归属创建者（is_private 时用于可见性隔离；空=全局）'
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Catalog 注册表'
        """;

    /** 时间格式：yyyy-MM-dd HH:mm:ss.SSS */
    private static final DateTimeFormatter DT_FMT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private final String jdbcUrl;
    private final String databaseName;
    /** 连接 MySQL 服务器（不指定数据库）的 URL，用于建库操作。 */
    private final String serverUrl;
    private final String username;
    private final String password;

    /** 内存缓存：catalogName → type。 */
    private final Map<String, String> typeCache = new ConcurrentHashMap<>();
    /** 内存缓存：catalogName → properties。 */
    private final Map<String, Map<String, String>> propertiesCache = new ConcurrentHashMap<>();
    /** 内存缓存：catalogName → created_at。 */
    private final Map<String, String> createdAtCache = new ConcurrentHashMap<>();
    /** 内存缓存：catalogName → updated_at。 */
    private final Map<String, String> updatedAtCache = new ConcurrentHashMap<>();
    /** 内存缓存：catalogName → owner（归属创建者；空=全局）。 */
    private final Map<String, String> ownerCache = new ConcurrentHashMap<>();

    private volatile boolean ready = false;

    public MysqlMetaStoreStorage(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.databaseName = extractDatabaseName(jdbcUrl);
        this.serverUrl = buildServerUrl(jdbcUrl);
        this.username = username;
        this.password = password;
    }

    // ==================== 生命周期 ====================

    @Override
    public void initialize() {
        createDatabaseIfNotExists();
        executeInitScript();
        loadFromDb();
        this.ready = true;
    }

    @Override
    public void close() {
        this.ready = false;
        typeCache.clear();
        propertiesCache.clear();
        createdAtCache.clear();
        updatedAtCache.clear();
        ownerCache.clear();
    }

    @Override
    public boolean isReady() {
        return ready;
    }

    // ==================== Catalog 操作 ====================

    @Override
    public void registerCatalog(String catalogName, String connectorType,
                                 Map<String, String> properties) {
        registerCatalog(catalogName, connectorType, properties, null);
    }

    @Override
    public void registerCatalog(String catalogName, String connectorType,
                                 Map<String, String> properties, String owner) {
        String propsJson = propertiesToJson(properties);
        String now = now();

        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                 "INSERT INTO " + TABLE_NAME
                 + " (name, type, properties, created_at, updated_at, owner)"
                 + " VALUES (?, ?, ?, ?, ?, ?)"
                 + " ON DUPLICATE KEY UPDATE"
                 + " type = VALUES(type),"
                 + " properties = VALUES(properties),"
                 + " owner = VALUES(owner),"
                 + " updated_at = VALUES(updated_at)")) {
            ps.setString(1, catalogName);
            ps.setString(2, connectorType);
            ps.setString(3, propsJson);
            ps.setString(4, now);
            ps.setString(5, now);
            ps.setString(6, owner);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to register catalog: " + catalogName, e);
        }

        // 更新缓存
        typeCache.put(catalogName, connectorType);
        propertiesCache.put(catalogName, new LinkedHashMap<>(properties));
        // 保留已有的 created_at（INSERT 时是新值，UPDATE 时保留旧值）
        createdAtCache.putIfAbsent(catalogName, now);
        updatedAtCache.put(catalogName, now);
        if (owner == null || owner.isBlank()) {
            ownerCache.remove(catalogName);
        } else {
            ownerCache.put(catalogName, owner);
        }
    }

    @Override
    public void unregisterCatalog(String catalogName) {
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                 "DELETE FROM " + TABLE_NAME + " WHERE name = ?")) {
            ps.setString(1, catalogName);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to unregister catalog: " + catalogName, e);
        }

        typeCache.remove(catalogName);
        propertiesCache.remove(catalogName);
        createdAtCache.remove(catalogName);
        updatedAtCache.remove(catalogName);
        ownerCache.remove(catalogName);
    }

    @Override
    public List<String> listCatalogs() {
        if (!ready) loadFromDb();
        List<String> result = new ArrayList<>(typeCache.keySet());
        Collections.sort(result);
        return result;
    }

    @Override
    public Optional<String> getConnectorType(String catalogName) {
        if (!ready) loadFromDb();
        return Optional.ofNullable(typeCache.get(catalogName));
    }

    @Override
    public Map<String, String> getCatalogProperties(String catalogName) {
        if (!ready) loadFromDb();
        Map<String, String> props = propertiesCache.get(catalogName);
        return props != null ? new LinkedHashMap<>(props) : Map.of();
    }

    @Override
    public boolean catalogExists(String catalogName) {
        if (!ready) loadFromDb();
        return typeCache.containsKey(catalogName);
    }

    @Override
    public Optional<String> getCatalogCreatedAt(String catalogName) {
        if (!ready) loadFromDb();
        return Optional.ofNullable(createdAtCache.get(catalogName));
    }

    @Override
    public Optional<String> getCatalogUpdatedAt(String catalogName) {
        if (!ready) loadFromDb();
        return Optional.ofNullable(updatedAtCache.get(catalogName));
    }

    @Override
    public Optional<String> getCatalogOwner(String catalogName) {
        if (!ready) loadFromDb();
        return Optional.ofNullable(ownerCache.get(catalogName)).filter(s -> !s.isBlank());
    }

    /** 内部取连接（不再对外暴露：{@link MetaStoreStorage} 是与后端无关的抽象，不应泄漏 JDBC）。 */
    private Connection getConnection() {
        try {
            return DriverManager.getConnection(jdbcUrl, username, password);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get MySQL connection", e);
        }
    }

    // ==================== 初始化私有方法 ====================

    private void createDatabaseIfNotExists() {
        if (databaseName == null || databaseName.isEmpty()) {
            return;
        }

        String createDbSql = "CREATE DATABASE IF NOT EXISTS `" + databaseName + "`"
            + " CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci";

        try (Connection conn = DriverManager.getConnection(serverUrl, username, password);
             Statement stmt = conn.createStatement()) {
            stmt.execute(createDbSql);
        } catch (SQLException e) {
            throw new RuntimeException(
                "Failed to create database '" + databaseName + "' on MySQL server", e);
        }
    }

    private void executeInitScript() {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(CREATE_TABLE_DDL);
            // 旧库迁移：补 owner 列（CREATE TABLE IF NOT EXISTS 不会给已存在的表加列）
            if (!columnExists(conn, TABLE_NAME, "owner")) {
                stmt.execute("ALTER TABLE " + TABLE_NAME
                        + " ADD COLUMN owner VARCHAR(255) COMMENT '归属创建者（is_private 时用于可见性隔离；空=全局）'");
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to create catalog table", e);
        }
    }

    /** information_schema 查询判断列是否存在（兼容旧库迁移）。 */
    private boolean columnExists(Connection conn, String table, String column) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT COUNT(*) FROM information_schema.columns"
                + " WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?")) {
            ps.setString(1, table);
            ps.setString(2, column);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    // ==================== URL 解析 ====================

    static String extractDatabaseName(String jdbcUrl) {
        if (jdbcUrl == null) return null;

        String prefix = "jdbc:mysql://";
        String rest;
        if (jdbcUrl.startsWith(prefix)) {
            rest = jdbcUrl.substring(prefix.length());
        } else {
            return null;
        }

        int slashIdx = rest.lastIndexOf('/');
        if (slashIdx < 0 || slashIdx == rest.length() - 1) {
            return null;
        }

        String dbPart = rest.substring(slashIdx + 1);
        int queryIdx = dbPart.indexOf('?');
        if (queryIdx >= 0) {
            dbPart = dbPart.substring(0, queryIdx);
        }
        return dbPart;
    }

    static String buildServerUrl(String jdbcUrl) {
        if (jdbcUrl == null) return null;

        String prefix = "jdbc:mysql://";
        if (!jdbcUrl.startsWith(prefix)) return jdbcUrl;

        String rest = jdbcUrl.substring(prefix.length());
        int slashIdx = rest.lastIndexOf('/');

        if (slashIdx >= 0) {
            String queryPart = "";
            int queryIdx = jdbcUrl.indexOf('?');
            if (queryIdx >= 0) {
                queryPart = jdbcUrl.substring(queryIdx);
            }
            return prefix + rest.substring(0, slashIdx) + queryPart;
        }
        return jdbcUrl;
    }

    // ==================== 其他私有方法 ====================

    /** 当前时间的格式化字符串。 */
    private static String now() {
        return LocalDateTime.now().format(DT_FMT);
    }

    private void loadFromDb() {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT name, type, properties, created_at, updated_at, owner"
                 + " FROM " + TABLE_NAME)) {
            while (rs.next()) {
                String name = rs.getString("name");
                String type = rs.getString("type");
                String propsStr = rs.getString("properties");
                String createdAt = rs.getString("created_at");
                String updatedAt = rs.getString("updated_at");
                String owner = rs.getString("owner");

                typeCache.put(name, type);
                propertiesCache.put(name, parseProperties(propsStr));
                createdAtCache.put(name, createdAt);
                updatedAtCache.put(name, updatedAt);
                if (owner != null && !owner.isBlank()) {
                    ownerCache.put(name, owner);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to load metastore data from MySQL", e);
        }
    }

    /** 简单的 JSON 序列化（避免引入 Jackson 等重依赖）。 */
    private String propertiesToJson(Map<String, String> props) {
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

    /** 简单的 JSON 反序列化。 */
    private Map<String, String> parseProperties(String json) {
        Map<String, String> result = new LinkedHashMap<>();
        if (json == null || json.isEmpty() || "{}".equals(json)) return result;
        String content = json.substring(1, json.length() - 1).trim();
        if (content.isEmpty()) return result;
        String[] pairs = content.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
        for (String pair : pairs) {
            String[] kv = pair.trim().split(":", 2);
            if (kv.length == 2) {
                String key = unquote(kv[0].trim());
                String value = unquote(kv[1].trim());
                result.put(key, value);
            }
        }
        return result;
    }

    private String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private String unquote(String s) {
        if (s.startsWith("\"") && s.endsWith("\"")) {
            return s.substring(1, s.length() - 1)
                    .replace("\\\"", "\"")
                    .replace("\\\\", "\\");
        }
        return s;
    }
}
