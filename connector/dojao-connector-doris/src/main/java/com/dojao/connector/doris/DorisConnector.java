package com.dojao.connector.doris;

import com.dojao.common.config.CommonConfig;
import com.dojao.common.data.Page;
import com.dojao.common.memory.NodeMemory;
import com.dojao.spi.bean.*;
import com.dojao.spi.service.DojaoConnector;
import com.dojao.spi.service.OverwriteEnable;
import com.dojao.spi.util.JdbcArrowConverter;
import com.dojao.spi.util.JdbcSizeEstimator;
import com.dojao.spi.util.SqlIdentifiers;
import com.dojao.spi.util.VarcharMapping;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Apache Doris 连接器，通过 MySQL 线路协议（Connector/J）实现 {@link DojaoConnector} 接口。
 *
 * <p>Doris 的 FE 提供 MySQL 兼容协议（默认端口 9030），元数据同样走
 * {@code information_schema}，因此读取 / 写入 / 分区并行 / 谓词下推沿用 MySQL 实现。
 * 与 MySQL 的差异集中在下述几处：</p>
 * <ul>
 *   <li><b>建表</b>：Doris 必须显式 {@code DISTRIBUTED BY}，且没有 MySQL 的 65535 行大小上限
 *       —— 字符串列统一 {@code STRING}，见 {@link #createTable} / {@link #arrowTypeToDoris}；
 *       默认模型为 DUPLICATE（不指定 key 时由 Doris 自选排序列）。</li>
 *   <li><b>覆盖写</b>：Doris 没有 {@code RENAME TABLE a TO b, c TO d}，改用原生的
 *       {@code ALTER TABLE t REPLACE WITH TABLE staging PROPERTIES('swap'='false')} 原子替换，
 *       见 {@link #commitOverwrite}。</li>
 *   <li><b>行数估算</b>：Doris 的 {@code EXPLAIN} 返回文本计划而非
 *       {@code rows}/{@code filtered}，取不到时返回 {@code -1} 退回元数据估算，
 *       见 {@link #explainRows}。</li>
 *   <li><b>主键</b>：Doris 没有主键约束（只有 DUPLICATE/UNIQUE/AGGREGATE 数据模型），
 *       并行扫描走数值列范围分区 / LIMIT-OFFSET 兜底，见 {@link #getPartitionKey}。</li>
 * </ul>
 *
 * <p>映射关系：
 * <ul>
 *   <li>catalog → JDBC 连接 URL 对应的 Doris 集群（可配置名称，默认 "doris"）</li>
 *   <li>schema  → Doris 的 DATABASE（即 information_schema.SCHEMATA 中的 SCHEMA_NAME）</li>
 *   <li>table   → Doris 表（information_schema.TABLES）</li>
 *   <li>column  → Doris 列（information_schema.COLUMNS）</li>
 * </ul>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/25
 */
public class DorisConnector implements DojaoConnector, OverwriteEnable {

    /** 此连接器的 catalog 名称（对外暴露的 catalog 标识）。 */
    private final String catalogName;

    /** JDBC 连接信息。 */
    private final String jdbcUrl;
    private final String username;
    private final String password;

    /** 缓存：dbName → List<Table>。 */
    private final Map<String, List<Table>> tableCache = new ConcurrentHashMap<>();
    /** 缓存：dbName.tableName → List<Column>。 */
    private final Map<String, List<Column>> columnCache = new ConcurrentHashMap<>();

    /** 元数据缓存是否已过期。 */
    private volatile boolean cacheDirty = true;

    /** Arrow 内存分配器，用于 JDBC → Arrow 转换。 */
    private final BufferAllocator allocator;

    // ==================== 游标状态（多页读取） ====================
    private Connection readConnection;
    private PreparedStatement readPreparedStatement;
    private ResultSet readResultSet;
    private JdbcArrowConverter readConverter;
    private String activeReadSql;

    public DorisConnector(String catalogName, String jdbcUrl, String username, String password) {
        this(catalogName, jdbcUrl, username, password,
                NodeMemory.shared().newChildAllocator("connector/doris", 0));
    }

    public DorisConnector(String catalogName, String jdbcUrl, String username, String password,
                          BufferAllocator allocator) {
        this.catalogName = catalogName;
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.allocator = allocator;
    }

    public DorisConnector(String jdbcUrl, String username, String password) {
        this("doris", jdbcUrl, username, password);
    }

    // ==================== Public getters ====================

    public String getCatalogName() { return catalogName; }
    public String getJdbcUrl() { return jdbcUrl; }
    public String getUsername() { return username; }
    public String getPassword() { return password; }

    /**
     * 独立副本：JDBC 游标状态在连接器实例上（{@link #read} 连续调用接着上次读），
     * 并行扫描必须每分区一个实例，因此覆写 SPI 的 {@link DojaoConnector#copy()}。
     */
    @Override
    public DorisConnector copy() {
        return new DorisConnector(catalogName, jdbcUrl, username, password,
                NodeMemory.shared().newChildAllocator("connector/doris", 0));
    }

    // ==================== Connection ====================

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, username, password);
    }

    /**
     * 连接性探针：用当前 URL / 账号<b>真的建一次连接</b>（引擎在 CREATE CATALOG 落库前调用，
     * 连不上就不入库）。
     *
     * <p>刻意不复用{@link #listSchemas}的默认探针：那会真的枚举 information_schema.SCHEMATA，
     * 既更重，也会把「连得上但没权限看库列表」误报成连接失败。这里只要 {@code isValid} 一次
     * 往返 —— 连不上（网络不通 / url 写错 / 账号密码错 / 库不存在）在 JDBC 驱动层当场抛
     * {@link SQLException}。</p>
     */
    @Override
    public void probeConnectivity(String catalog) {
        try (Connection conn = getConnection()) {
            if (!conn.isValid(5)) {
                throw new RuntimeException("无法连接到数据源 " + catalog + "（" + jdbcUrl
                        + "）：连接校验失败");
            }
        } catch (SQLException e) {
            throw new RuntimeException("无法连接到数据源 " + catalog + "（" + jdbcUrl + "）: "
                    + e.getMessage(), e);
        }
    }

    // ==================== Catalog ====================

    @Override
    public boolean catalogExists(String catalog) {
        return this.catalogName.equals(catalog);
    }

    @Override
    public Optional<Catalog> getCatalog(String catalog) {
        if (!catalogExists(catalog)) return Optional.empty();
        return Optional.of(new Catalog(catalogName, "doris", Map.of("url", jdbcUrl), null, null));
    }

    @Override
    public List<String> listCatalogs() {
        return List.of(catalogName);
    }

    // ==================== Schema ====================

    @Override
    public boolean schemaExists(String catalog, String schema) {
        if (!catalogExists(catalog)) return false;
        String schemaUrl = buildSchemaUrl(jdbcUrl, schema);
        try (Connection conn = DriverManager.getConnection(schemaUrl, username, password)) {
            return true; // 连接成功即 schema 存在
        } catch (SQLException e) {
            // 报错里带上「实际连接的 URL」：这条路径会按 schema 换库名，若改写出的 URL 不对
            // （见 buildSchemaUrl 的坑），只看 e.getMessage() 会误判成数据源网络问题。
            throw new RuntimeException(
                "Failed to connect to schema '" + catalog + "." + schema
                + "' (url=" + schemaUrl + ") — " + e.getMessage(), e);
        }
    }

    /**
     * 把 jdbcUrl 里的库名换成目标 schema，<b>原样保留 authority（host:port）与 {@code ?} 之后的
     * 全部连接参数</b>。
     *
     * <p><b>踩过的坑（集群实测 {@code Schema check failed … Communications link failure}）。</b>
     * 旧实现想找「host:port 后面的那个斜杠」，写的是 {@code indexOf("/", "jdbc:".length() + 1)} ——
     * {@code "jdbc:mysql:"} 正好 11 个字符，于是命中的是 {@code //} 里的<b>第一个</b>斜杠，
     * 把 host:port 一起截掉，拼出 {@code jdbc:mysql:/metastore}。Connector/J 对这种串<b>不报格式错</b>，
     * 而是把缺失的 authority <b>兜底成 {@code localhost:3306}</b>（已用 8.3.0 实测：连上去后报的是
     * {@code Access denied for user 'x'@'localhost'} —— 服务器是本机的 MySQL，不是 URL 里那台）。
     * 于是：本机有 MySQL 的开发环境完全看不出问题（schema 校验悄悄连了本机），而集群里的 Master
     * 本机没有 3306 → 连接失败。另外 {@code useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=…}
     * 也被一并丢弃，等于换了一套连接语义。</p>
     */
    static String buildSchemaUrl(String jdbcUrl, String schema) {
        String base = jdbcUrl;
        // 1) 摘掉 ? 之后的连接参数（保留，最后接回去）
        String params = "";
        int q = base.indexOf('?');
        if (q >= 0) {
            params = base.substring(q);
            base = base.substring(0, q);
        }
        // 2) 只截掉「// 之后的 authority 结束处」往后的库名路径 —— 绝不能碰 // 本身
        int authority = base.indexOf("//");
        int pathStart = authority >= 0 ? base.indexOf('/', authority + 2) : -1;
        if (pathStart >= 0) base = base.substring(0, pathStart);
        while (base.endsWith("/")) base = base.substring(0, base.length() - 1);
        return base + "/" + schema + params;
    }

    @Override
    public Optional<Schema> getSchema(String catalog, String schema) {
        if (!schemaExists(catalog, schema)) return Optional.empty();
        return Optional.of(new Schema(catalog, schema, Map.of()));
    }

    @Override
    public List<String> listSchemas(String catalog) {
        if (!catalogExists(catalog)) return List.of();
        List<String> schemas = new ArrayList<>();
        try (Connection conn = getConnection()) {
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                     "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA"
                     + " ORDER BY SCHEMA_NAME")) {
                while (rs.next()) {
                    schemas.add(rs.getString("SCHEMA_NAME"));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to list schemas", e);
        }
        return schemas;
    }

    // ==================== Table ====================

    @Override
    public boolean tableExists(String catalog, String schema, String table) {
        if (!catalogExists(catalog)) return false;
        try (Connection conn = getConnection()) {
            try (PreparedStatement ps = conn.prepareStatement(
                "SELECT 1 FROM information_schema.TABLES"
                + " WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?")) {
                ps.setString(1, schema);
                ps.setString(2, table);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next();
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public Optional<Table> getTable(String catalog, String schema, String table) {
        List<Column> columns = getTableColumns(catalog, schema, table);
        if (columns.isEmpty() && !tableExists(catalog, schema, table)) {
            return Optional.empty();
        }
        return Optional.of(new Table(
            catalog, schema, table,
            columns,
            getTableProperties(schema, table),
            getTableComment(schema, table)
        ));
    }

    @Override
    public List<Column> getTableColumns(String catalog, String schema, String tableName) {
        if (!catalogExists(catalog)) return List.of();

        String cacheKey = schema + "." + tableName;
        if (!cacheDirty && columnCache.containsKey(cacheKey)) {
            return columnCache.get(cacheKey);
        }

        List<Column> columns = new ArrayList<>();
        try (Connection conn = getConnection()) {
            try (PreparedStatement ps = conn.prepareStatement(
                "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE,"
                + "       COLUMN_COMMENT, COLUMN_DEFAULT, IS_NULLABLE"
                + " FROM information_schema.COLUMNS"
                + " WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
                + " ORDER BY ORDINAL_POSITION")) {
                ps.setString(1, schema);
                ps.setString(2, tableName);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String name = rs.getString("COLUMN_NAME");
                        String dataType = rs.getString("COLUMN_TYPE"); // 如 INT(11), VARCHAR(255)
                        String comment = rs.getString("COLUMN_COMMENT");
                        String defaultValue = rs.getString("COLUMN_DEFAULT");
                        boolean nullable = "YES".equalsIgnoreCase(
                            rs.getString("IS_NULLABLE"));

                        columns.add(new Column(
                            name, dataType,
                            comment != null && !comment.isEmpty() ? comment : null,
                            defaultValue,
                            nullable
                        ));
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(
                "Failed to get columns for: " + schema + "." + tableName, e);
        }

        columnCache.put(cacheKey, columns);
        return columns;
    }

    // ==================== Dojao 类型映射 ====================

    /** {@code DATA_TYPE} → {@link DojaoType}（大小写不敏感，未知类型兜底字符串）。包级可见：离线探针直接断言。 */
    static DojaoType dojaoType(String dataType) {
        if (dataType == null) return DojaoType.STRING;
        return DORIS_TYPE_MAPPING.getOrDefault(
                dataType.toLowerCase(java.util.Locale.ROOT), DojaoType.STRING);
    }

    private static final Map<String, DojaoType> DORIS_TYPE_MAPPING = new LinkedHashMap<>();

    static {
        // 布尔
        DORIS_TYPE_MAPPING.put("bit", DojaoType.BOOLEAN);
        DORIS_TYPE_MAPPING.put("bool", DojaoType.BOOLEAN);
        DORIS_TYPE_MAPPING.put("boolean", DojaoType.BOOLEAN);
        // 整数
        DORIS_TYPE_MAPPING.put("tinyint", DojaoType.TINYINT);
        DORIS_TYPE_MAPPING.put("smallint", DojaoType.SMALLINT);
        DORIS_TYPE_MAPPING.put("int", DojaoType.INT);
        DORIS_TYPE_MAPPING.put("integer", DojaoType.INT);
        DORIS_TYPE_MAPPING.put("bigint", DojaoType.BIGINT);
        // LARGEINT 是 128 位整数，Dojao 没有对应定长整型 → 按 DECIMAL 精确承载
        DORIS_TYPE_MAPPING.put("largeint", DojaoType.DECIMAL);
        // 浮点
        DORIS_TYPE_MAPPING.put("float", DojaoType.FLOAT);
        DORIS_TYPE_MAPPING.put("double", DojaoType.DOUBLE);
        // 定点
        DORIS_TYPE_MAPPING.put("decimal", DojaoType.DECIMAL);
        DORIS_TYPE_MAPPING.put("decimalv2", DojaoType.DECIMAL);
        DORIS_TYPE_MAPPING.put("decimalv3", DojaoType.DECIMAL);
        DORIS_TYPE_MAPPING.put("numeric", DojaoType.DECIMAL);
        DORIS_TYPE_MAPPING.put("dec", DojaoType.DECIMAL);
        DORIS_TYPE_MAPPING.put("fixed", DojaoType.DECIMAL);
        // 字符串（Doris 的 VARCHAR / STRING / TEXT 都是变长字符串）
        DORIS_TYPE_MAPPING.put("char", DojaoType.STRING);
        DORIS_TYPE_MAPPING.put("varchar", DojaoType.STRING);
        DORIS_TYPE_MAPPING.put("text", DojaoType.STRING);
        DORIS_TYPE_MAPPING.put("tinytext", DojaoType.STRING);
        DORIS_TYPE_MAPPING.put("mediumtext", DojaoType.STRING);
        DORIS_TYPE_MAPPING.put("longtext", DojaoType.STRING);
        DORIS_TYPE_MAPPING.put("string", DojaoType.STRING);
        // 日期/时间（Doris 的 DATETIME 已含时分秒；TIMESTAMP 只是 DATETIME 别名）
        DORIS_TYPE_MAPPING.put("date", DojaoType.DATE);
        DORIS_TYPE_MAPPING.put("datev2", DojaoType.DATE);
        DORIS_TYPE_MAPPING.put("datetime", DojaoType.DATETIME);
        DORIS_TYPE_MAPPING.put("datetimev2", DojaoType.DATETIME);
        DORIS_TYPE_MAPPING.put("timestamp", DojaoType.DATETIME);
        DORIS_TYPE_MAPPING.put("time", DojaoType.STRING);
        // 半结构化
        DORIS_TYPE_MAPPING.put("json", DojaoType.JSON);
        DORIS_TYPE_MAPPING.put("jsonb", DojaoType.JSON);
        // 嵌套 / 特殊类型：JDBC 以文本返回，按 STRING 读取（不做嵌套解析）
        DORIS_TYPE_MAPPING.put("array", DojaoType.STRING);
        DORIS_TYPE_MAPPING.put("map", DojaoType.STRING);
        DORIS_TYPE_MAPPING.put("struct", DojaoType.STRING);
        DORIS_TYPE_MAPPING.put("variant", DojaoType.STRING);
        DORIS_TYPE_MAPPING.put("bitmap", DojaoType.STRING);
        DORIS_TYPE_MAPPING.put("hll", DojaoType.STRING);
        DORIS_TYPE_MAPPING.put("ipv4", DojaoType.STRING);
        DORIS_TYPE_MAPPING.put("ipv6", DojaoType.STRING);
        // 二进制
        DORIS_TYPE_MAPPING.put("binary", DojaoType.STRING);
        DORIS_TYPE_MAPPING.put("varbinary", DojaoType.STRING);
        DORIS_TYPE_MAPPING.put("tinyblob", DojaoType.STRING);
        DORIS_TYPE_MAPPING.put("blob", DojaoType.STRING);
        DORIS_TYPE_MAPPING.put("mediumblob", DojaoType.STRING);
        DORIS_TYPE_MAPPING.put("longblob", DojaoType.STRING);
    }

    @Override
    public Map<String, DojaoType> getColumnTypes(String schema, String table) {
        Map<String, DojaoType> result = new LinkedHashMap<>();
        try (Connection conn = getConnection()) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT COLUMN_NAME, DATA_TYPE"
                            + " FROM information_schema.COLUMNS"
                            + " WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
                            + " ORDER BY ORDINAL_POSITION")) {
                ps.setString(1, schema);
                ps.setString(2, table);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String colName = rs.getString("COLUMN_NAME");
                        String dataType = rs.getString("DATA_TYPE").toLowerCase(java.util.Locale.ROOT);
                        result.put(colName, dojaoType(dataType));
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(
                    "Failed to get column types for: " + schema + "." + table, e);
        }
        return result;
    }

    /**
     * CTAS 源列类型映射（读方向精确实现）：对指定列集跑一条零行探针
     * {@code SELECT <cols> FROM 表 WHERE 1=0}，直接取 JDBC ResultSet 元数据 → {@link JdbcArrowConverter#buildSchema}，
     * 与运行期 {@link #read(ReadRequest)} 走同一条 JDBC→Arrow 映射 —— int 位宽 / decimal(p,s) / tinyint(1)
     * 布尔化 / year 等全由驱动元数据决定，保证 Master 按此建出的目标表一定装得下 Worker 扫描产出的数据。
     *
     * <p>这是"每个连接器写一个具体类型映射实现"的读方向：无精度元数据可用的兜底实现见
     * {@link DojoaConnector#sourceFields}（DojaoType → 宽 Decimal(38,10)）。</p>
     */
    @Override
    public List<Field> sourceFields(String catalog, String schema, String table, List<String> columns) {
        // 列名可能来自 ReadRequest.columns()（已按方言转义）→ 先剥引号，否则拼出的 SQL 双重转义必失败
        List<String> cols = (columns == null || columns.isEmpty())
                ? getTableColumns(catalog, schema, table).stream().map(Column::name).toList()
                : SqlIdentifiers.unquoteAll(columns);
        if (cols.isEmpty()) {
            return List.of();
        }
        StringBuilder selectList = new StringBuilder();
        for (int i = 0; i < cols.size(); i++) {
            if (i > 0) selectList.append(", ");
            selectList.append('`').append(cols.get(i).replace("`", "``")).append('`');
        }
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT " + selectList + " FROM " + q(schema, table) + " WHERE 1=0")) {
            try (ResultSet rs = ps.executeQuery()) {
                return JdbcArrowConverter.buildSchema(rs.getMetaData()).getFields();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to read source column metadata for CTAS: "
                    + schema + "." + table, e);
        }
    }

    @Override
    public Optional<String> getDefaultCharset(String schema) {
        // 新表（CTAS）继承 schema 的默认字符集；探查失败 → 未知 → 调用方 TEXT 兜底
        try (Connection conn = getConnection()) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT DEFAULT_CHARACTER_SET_NAME FROM information_schema.SCHEMATA"
                            + " WHERE SCHEMA_NAME = ?")) {
                ps.setString(1, schema);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) return Optional.ofNullable(rs.getString(1));
                }
            }
        } catch (SQLException e) {
            return Optional.empty();
        }
        return Optional.empty();
    }

    @Override
    public VarcharMapping varcharMapping() {
        return DorisVarcharMapping.INSTANCE;
    }

    @Override
    public List<String> listTables(String catalog, String schema) {
        if (!catalogExists(catalog)) return List.of();
        List<String> tables = new ArrayList<>();
        try (Connection conn = getConnection()) {
            try (PreparedStatement ps = conn.prepareStatement(
                "SELECT TABLE_NAME FROM information_schema.TABLES"
                + " WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'"
                + " ORDER BY TABLE_NAME")) {
                ps.setString(1, schema);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        tables.add(rs.getString("TABLE_NAME"));
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to list tables for: " + schema, e);
        }
        return tables;
    }

    // ==================== Column ====================

    @Override
    public boolean columnExists(String catalog, String schema, String table, String column) {
        if (!catalogExists(catalog)) return false;
        try (Connection conn = getConnection()) {
            try (PreparedStatement ps = conn.prepareStatement(
                "SELECT 1 FROM information_schema.COLUMNS"
                + " WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?")) {
                ps.setString(1, schema);
                ps.setString(2, table);
                ps.setString(3, column);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next();
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(
                "Failed to check column existence: " + schema + "." + table + "." + column, e);
        }
    }

    @Override
    public Optional<Column> getColumn(String catalog, String schema, String table, String column) {
        return getTableColumns(catalog, schema, table).stream()
            .filter(c -> c.name().equals(column))
            .findFirst();
    }

    // ==================== 统计信息 ====================

    /** 采样行数：一个完整 batch，摊薄 Arrow power-of-2 分配带来的字节高估。 */
    private static final int SIZE_SAMPLE_ROWS = CommonConfig.DEFAULT_BATCH_SIZE;

    @Override
    public TableStats getTableStats(String catalog, String schema, String table) {
        if (!catalogExists(catalog)) {
            throw new IllegalArgumentException("Catalog '" + catalog + "' does not exist");
        }
        // ① 元数据真实体积（无权限 → [-1,-1]）→ ② 整表 EXPLAIN 行数 × 采样平均行长 → ③ 未知
        long[] raw = fetchRawStats(schema, table);
        JdbcSizeEstimator.Estimate est = JdbcSizeEstimator.estimate(this, catalog, schema, table, null,
            raw[0], raw[1], f -> explainRows(schema, table, f), SIZE_SAMPLE_ROWS);
        List<ColumnStats> cols;
        try {
            cols = getColumnStats(catalog, schema, table);
        } catch (Exception e) {
            // 列统计权限不足 → 降级为空，不拖垮整表统计（体积/行数才是 join 选型要的）
            cols = List.of();
        }
        return new TableStats(catalog, schema, table, est.rowCount(), est.byteSize(), cols);
    }

    /**
     * 读取元数据真实统计 {@code [rowCount, byteSize]}，任一未知记 {@code -1}。
     *
     * <p>SQL NULL（VIEW 的 TABLE_ROWS / DATA_LENGTH 为 NULL、或新建空表未分析）→ 记为 -1（未知），
     * 不能留 0（0 会被框架当成"空表"，并行度算成 1）。InnoDB 的 TABLE_ROWS 是估算值，非精确行数。</p>
     *
     * <p><b>权限不足 / 查不到 → 返回 {@code [-1,-1]}，不抛异常</b>，交由三级降级继续走 EXPLAIN + 采样。</p>
     */
    private long[] fetchRawStats(String schema, String table) {
        long rowCount = -1;
        long byteSize = -1;
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                 "SELECT TABLE_ROWS, DATA_LENGTH, INDEX_LENGTH"
                 + " FROM information_schema.TABLES"
                 + " WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?")) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    long tableRows = rs.getLong("TABLE_ROWS");
                    if (rs.wasNull()) {
                        tableRows = -1;
                    }
                    long dataLen = rs.getLong("DATA_LENGTH");
                    if (rs.wasNull()) {
                        dataLen = -1;
                    }
                    long indexLen = rs.getLong("INDEX_LENGTH");
                    if (rs.wasNull()) {
                        indexLen = -1;
                    }
                    rowCount = tableRows;
                    // 数据或索引体积任一未知 → 总体积未知（-1）；否则数据 + 索引
                    byteSize = (dataLen < 0 || indexLen < 0) ? -1 : dataLen + indexLen;
                }
            }
        } catch (SQLException e) {
            // 权限不足 / 元数据不可读 → 未知，交给三级降级
        }
        return new long[]{rowCount, byteSize};
    }

    /**
     * 列统计：Doris 的 {@code information_schema.STATISTICS} 不保证提供 MySQL 口径的
     * {@code CARDINALITY}，因此只取列元数据（NULL 约束 / 字符长度），基数（NDV）留未知
     * {@code -1} —— 上层 join 选型会退回更保守的估算，但不会因一条统计 SQL 失败而拖垮整表统计。
     */
    @Override
    public List<ColumnStats> getColumnStats(String catalog, String schema, String table) {
        if (!catalogExists(catalog)) return List.of();
        List<ColumnStats> statsList = new ArrayList<>();
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                "SELECT COLUMN_NAME, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH"
                + " FROM information_schema.COLUMNS"
                + " WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
                + " ORDER BY ORDINAL_POSITION")) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String colName = rs.getString("COLUMN_NAME");
                    long charMaxLen = rs.getLong("CHARACTER_MAXIMUM_LENGTH");
                    statsList.add(new ColumnStats(colName, -1, -1,
                        Optional.empty(), Optional.empty(),
                        charMaxLen > 0 ? Optional.of(charMaxLen) : Optional.empty()));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get column stats: " + schema + "." + table, e);
        }
        return statsList;
    }

    // ==================== 过滤感知行数估算（Scan 进度条分母）====================

    /**
     * 过滤感知估算：对下推谓词跑 {@code EXPLAIN SELECT * FROM t WHERE filter}，取优化器给出的
     * {@code rows × filtered%} 作为过滤后行数估计；无谓词 → 整表 {@code TABLE_ROWS}。
     *
     * <p>MySQL 的 {@code EXPLAIN.rows} 是索引选择率估算（PK 等值/范围通常相当准），{@code filtered}
     * 是谓词额外过滤比例（5.1+ 可用）。EXPLAIN 失败（老版本 / 权限 / 语法）→ 回退整表估计，
     * 绝不因估算失败而报错（进度条分母退化为整表量）。</p>
     */
    @Override
    public long estimateRows(String catalog, String schema, String table, String filter) {
        if (filter == null || filter.isBlank()) {
            return DojaoConnector.super.estimateRows(catalog, schema, table, filter);
        }
        if (!catalogExists(catalog)) {
            return -1L;
        }
        long est = explainRows(schema, table, filter);
        if (est > 0) {
            return est;
        }
        // EXPLAIN 不可用 → 退化为整表估计（进度条分母粗一点，但不报错）
        return DojaoConnector.super.estimateRows(catalog, schema, table, filter);
    }

    /**
     * 方言 EXPLAIN 行数估算：Doris 的 {@code EXPLAIN} 只返回文本执行计划（列名如
     * {@code Explain String}），没有 MySQL 的 {@code rows}/{@code filtered} 数值列可读，
     * 因此这里恒返回 {@code -1} —— 由 {@link #estimateRows} / {@link JdbcSizeEstimator}
     * 退回 {@code information_schema.TABLES.TABLE_ROWS} 的整表估算。
     *
     * <p>刻意不解析计划文本：Doris 各版本的计划格式不稳定，按文本猜行数只会得到看似
     * 精确的错数。宁可「粗但不会错」。</p>
     */
    private long explainRows(String schema, String table, String filter) {
        return -1L;
    }

    /**
     * 过滤感知的<b>字节</b>估算（三级降级）：① 元数据体积（有过滤则按选择率缩放）→
     * ② EXPLAIN 行数 × 采样平均行长 → ③ 未知（{@code -1}，调用方退回最保守的排序归并）。
     *
     * <p>见 {@link JdbcSizeEstimator#estimate}。元数据拿得到就直接用（最准）；账号无权限读不到
     * {@code information_schema} 时，退用 EXPLAIN 估行数 × 采样几行求平均行长。</p>
     */
    @Override
    public long estimateBytes(String catalog, String schema, String table, String filter) {
        if (!catalogExists(catalog)) {
            return -1L;
        }
        long[] raw = fetchRawStats(schema, table);
        return JdbcSizeEstimator.estimate(this, catalog, schema, table, filter,
            raw[0], raw[1], f -> explainRows(schema, table, f), SIZE_SAMPLE_ROWS).byteSize();
    }

    // ==================== 数据读取（JDBC → Arrow，游标模式）====================

    @Override
    public Page read(ReadRequest request) {
        if (!catalogExists(request.catalog())) {
            throw new IllegalArgumentException("Catalog '" + request.catalog() + "' does not exist");
        }

        String sql = buildReadSql(request);

        // 查询变更 → 关闭旧游标，开启新查询
        if (!sql.equals(activeReadSql)) {
            closeRead();
        }

        try {
            // 首次调用 → 打开新游标
            if (readConnection == null || readConnection.isClosed()) {
                readConnection = getConnection();
                readPreparedStatement = readConnection.prepareStatement(sql);
                readPreparedStatement.setFetchSize(Integer.MIN_VALUE);
                readResultSet = readPreparedStatement.executeQuery();
                readConverter = new JdbcArrowConverter(allocator, CommonConfig.DEFAULT_BATCH_SIZE);
                activeReadSql = sql;
            }

            // 从 ResultSet 游标读下一批
            Page page = readConverter.convertNextBatch(readResultSet);
            if (page.isEOF() || (page.getRoot() != null && page.getRoot().getRowCount() == 0)) {
                closeRead();
            }
            return page;

        } catch (SQLException e) {
            closeRead();
            throw new RuntimeException("Failed to read data: "
                    + request.schema() + "." + request.table(), e);
        }
    }

    /** 关闭当前读取游标，释放 DB 资源。 */
    @Override
    public synchronized void closeRead() {
        try {
            if (readResultSet != null) { readResultSet.close(); readResultSet = null; }
        } catch (SQLException ignored) {}
        try {
            if (readPreparedStatement != null) { readPreparedStatement.close(); readPreparedStatement = null; }
        } catch (SQLException ignored) {}
        try {
            if (readConnection != null) { readConnection.close(); readConnection = null; }
        } catch (SQLException ignored) {}
        readConverter = null;
        activeReadSql = null;
    }

    // ==================== 列裁剪 & 谓词下推 ====================

    /** 列裁剪：MySQL 方言对列名做反引号转义，防止保留字/特殊字符破坏 SELECT 列清单。 */
    @Override
    public ReadRequest pruneColumns(ReadRequest request, List<String> columns) {
        if (columns == null || columns.isEmpty()) return request;
        List<String> escaped = new ArrayList<>(columns.size());
        for (String col : columns) {
            // 幂等：入参可能已是转义形态（如从另一个 ReadRequest 复用）→ 先剥引号再转义，避免双重转义
            escaped.add("`" + SqlIdentifiers.unquote(col).replace("`", "``") + "`");
        }
        return request.withColumns(escaped);
    }

    /** 谓词下推：MySQL 方言原样透传（buildReadSql 直接拼进 WHERE）。 */
    @Override
    public ReadRequest pushdownPredicate(ReadRequest request, String predicate) {
        if (predicate == null || predicate.isBlank()) return request;
        String merged = request.filterExpression()
                .map(f -> "(" + f + ") AND (" + predicate + ")")
                .orElse(predicate);
        return request.withFilter(merged);
    }

    /** 谓词下推：MySQL 方言原样透传（buildReadSql 直接拼进 WHERE）。 */
    @Override
    public boolean supportsPredicatePushdown() {
        // buildReadSql 把 filterExpression 直接拼进 WHERE → 过滤真的执行了，引擎可以不下推就不过滤
        return true;
    }

    /** 构建 SELECT SQL。包级可见：离线探针直接断言拼装结果。 */
    String buildReadSql(ReadRequest request) {
        StringBuilder sql = new StringBuilder("SELECT ");
        List<String> selectCols = request.columns().orElse(List.of());
        if (selectCols.isEmpty()) {
            sql.append("*");
        } else {
            sql.append(String.join(", ", selectCols));
        }
        sql.append(" FROM ").append(request.schema()).append(".").append(request.table());

        request.filterExpression().ifPresent(filter -> sql.append(" WHERE ").append(filter));

        if (request.limit() > 0) {
            sql.append(" LIMIT ").append(request.limit());
        }
        if (request.offset() > 0) {
            sql.append(" OFFSET ").append(request.offset());
        }
        return sql.toString();
    }

    // ==================== 分区探针（平行 TableScan 用） ====================

    /**
     * Doris 没有主键约束（只有 DUPLICATE / UNIQUE / AGGREGATE 三种数据模型，
     * UNIQUE KEY 也不保证唯一），因此不做「主键范围分区」——并行扫描由
     * {@link #planNumericColumnPartitions}（数值列 MIN/MAX 均分）或
     * {@link #planLimitOffsetPartitions}（LIMIT/OFFSET 兜底）承担。
     */
    @Override
    public Optional<String> getPartitionKey(String schema, String table) {
        return Optional.empty();
    }

    /** 获取精确行数（SELECT COUNT(*)）。 */
    @Override
    public long getExactRowCount(String schema, String table) {
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                 "SELECT COUNT(*) FROM " + schema + "." + table)) {
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getLong(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to count rows: " + schema + "." + table, e);
        }
        return 0;
    }

    /** 获取主键列的最小/最大值（用于范围分区）。 */
    @Override
    public long[] getPartitionKeyRange(String schema, String table, String pkColumn) {
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                 "SELECT MIN(" + pkColumn + "), MAX(" + pkColumn + ") FROM " + schema + "." + table)) {
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new long[]{rs.getLong(1), rs.getLong(2)};
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get range for: " + schema + "." + table + "." + pkColumn, e);
        }
        return new long[]{0, 0};
    }

    /**
     * 可用作 MIN/MAX 范围分区的数值类型。排除 {@code LARGEINT} / {@code DECIMAL}：
     * {@link #getPartitionKeyRange} 用 {@code long[]} 承载边界，128 位整数与定点数可能溢出，
     * 溢出的分区边界会静默丢行；这类列退回 LIMIT/OFFSET 兜底。
     */
    static boolean isNumericType(String dataType) {
        if (dataType == null) return false;
        String upper = dataType.toUpperCase(Locale.ROOT);
        if (upper.contains("LARGEINT") || upper.contains("DECIMAL")) return false;
        return upper.contains("INT") || upper.contains("FLOAT") || upper.contains("DOUBLE");
    }

    // ==================== 并行扫描分区（主键 → 数值列 → LIMIT/OFFSET 兜底） ====================

    /**
     * 分区策略：主键范围分区优先；无主键时依次回退
     * <ol>
     *   <li><b>数值列范围分区</b>：information_schema 里找一个数值列（优先 NOT NULL，最接近隐式主键），
     *       MIN/MAX 范围均分。该列有索引时每分区走索引范围扫描，总 I/O≈1 倍；无索引则退化为多倍全表扫。</li>
     *   <li><b>LIMIT/OFFSET 均分</b>：无任何数值列的最后兜底，每分区 SELECT ... LIMIT step OFFSET i*step。
     *       offset 跳过是 O(N) 的，大表偏浪费，但保证无主键表也能并行扫。</li>
     * </ol>
     */
    @Override
    public List<ReadRequest> planParallelScan(ReadRequest request, int parallelism) {
        List<ReadRequest> pkPartitions = DojaoConnector.super.planParallelScan(request, parallelism);
        if (!pkPartitions.isEmpty()) return pkPartitions;
        List<ReadRequest> colPartitions = planNumericColumnPartitions(request, parallelism);
        if (!colPartitions.isEmpty()) return colPartitions;
        return planLimitOffsetPartitions(request, parallelism);
    }

    /** 兜底 1：找数值列做 MIN/MAX 范围分区。 */
    private List<ReadRequest> planNumericColumnPartitions(ReadRequest request, int parallelism) {
        if (parallelism <= 1 || request.limit() > 0 || request.offset() > 0) return List.of();
        Optional<String> numericCol = findNumericColumn(request.schema(), request.table());
        if (numericCol.isEmpty()) return List.of();
        long[] range = getPartitionKeyRange(request.schema(), request.table(), numericCol.get());
        if (range[0] >= range[1]) return List.of();
        return DojaoConnector.splitRangePartitions(request, numericCol.get(),
                range[0], range[1], parallelism);
    }

    /** 找第一个数值列：优先 NOT NULL（最接近隐式主键），否则任意数值列。 */
    private Optional<String> findNumericColumn(String schema, String table) {
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                 "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE"
                 + " FROM information_schema.COLUMNS"
                 + " WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
                 + " ORDER BY ORDINAL_POSITION")) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                String firstNumeric = null;
                while (rs.next()) {
                    if (!isNumericType(rs.getString("DATA_TYPE"))) continue;
                    String col = rs.getString("COLUMN_NAME");
                    if ("NO".equalsIgnoreCase(rs.getString("IS_NULLABLE"))) return Optional.of(col);
                    if (firstNumeric == null) firstNumeric = col;
                }
                return firstNumeric == null ? Optional.empty() : Optional.of(firstNumeric);
            }
        } catch (SQLException e) {
            return Optional.empty();
        }
    }

    /** 兜底 2：LIMIT/OFFSET 均分（COUNT 必须带过滤条件，否则过滤后行数偏小导致漏数据）。 */
    private List<ReadRequest> planLimitOffsetPartitions(ReadRequest request, int parallelism) {
        if (parallelism <= 1 || request.limit() > 0 || request.offset() > 0) return List.of();
        long total = countFilteredRows(request);
        if (total <= 0) return List.of();
        long step = total / parallelism + (total % parallelism == 0 ? 0 : 1);
        List<ReadRequest> parts = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            long offset = (long) i * step;
            if (offset >= total) break;
            long limit = Math.min(step, total - offset);
            parts.add(new ReadRequest(request.catalog(), request.schema(), request.table(),
                    request.columns(), request.filterExpression(), offset, limit));
        }
        return parts;
    }

    /** 带过滤条件的行数统计（LIMIT/OFFSET 分区用）。 */
    private long countFilteredRows(ReadRequest request) {
        StringBuilder sql = new StringBuilder("SELECT COUNT(*) FROM ")
                .append(request.schema()).append(".").append(request.table());
        request.filterExpression().ifPresent(f -> sql.append(" WHERE ").append(f));
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql.toString())) {
            if (rs.next()) return rs.getLong(1);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to count rows: "
                    + request.schema() + "." + request.table(), e);
        }
        return 0;
    }

    // ==================== 数据写入（Arrow → JDBC）====================

    /**
     * 持久写会话：每个（写线程 × SQL）一个独立 Connection + PreparedStatement 复用，
     * 避免每页新建连接的开销。URL 自动补 rewriteBatchedStatements=true，把一页的
     * addBatch 行折叠成一条多行 INSERT VALUES(...),(...)，消除逐行网络往返。
     */
    private final ConcurrentHashMap<String, WriteSession> writeSessions = new ConcurrentHashMap<>();

    /** 把 rewriteBatchedStatements=true 追加到 JDBC URL（已含则不动）。 */
    private static String withRewriteBatched(String url) {
        if (url.contains("rewriteBatchedStatements")) return url;
        return url + (url.contains("?") ? "&" : "?") + "rewriteBatchedStatements=true";
    }

    private static final class WriteSession implements AutoCloseable {
        final Connection conn;
        final PreparedStatement ps;
        WriteSession(String url, String user, String pass, String sql) throws SQLException {
            this.conn = DriverManager.getConnection(url, user, pass);
            this.ps = conn.prepareStatement(sql);
        }
        @Override
        public void close() {
            try { ps.close(); } catch (SQLException ignored) {}
            try { conn.close(); } catch (SQLException ignored) {}
        }
    }

    /** 取当前线程 + 该 SQL 的持久写会话（不存在则创建；同名线程无竞争）。 */
    private WriteSession session(String sql) throws SQLException {
        String key = Thread.currentThread().getName() + "#" + sql;
        WriteSession s = writeSessions.get(key);
        if (s != null) return s;
        WriteSession created = new WriteSession(withRewriteBatched(jdbcUrl), username, password, sql);
        WriteSession raced = writeSessions.putIfAbsent(key, created);
        if (raced != null) {
            created.close();
            return raced;
        }
        return created;
    }

    @Override
    public void write(WriteRequest request) {
        if (!catalogExists(request.catalog())) {
            throw new IllegalArgumentException("Catalog '" + request.catalog() + "' does not exist");
        }
        Page data = request.data();
        VectorSchemaRoot root = data.getRoot();
        if (root == null || data.getRowCount() == 0) return;

        // 目标列名：显式 targetColumns 优先（INSERT INTO t(a,b) SELECT x,y），否则用 Page schema 列名
        List<String> cols;
        List<String> targets = request.targetColumns();
        int colCount = root.getSchema().getFields().size();
        if (targets != null && !targets.isEmpty()) {
            if (targets.size() != colCount) {
                throw new IllegalArgumentException("写入列数不匹配：目标列 " + targets.size()
                        + "，数据列 " + colCount);
            }
            cols = new ArrayList<>(targets);
        } else {
            cols = new ArrayList<>();
            for (var field : root.getSchema().getFields()) {
                cols.add(field.getName());
            }
        }
        String placeholders = String.join(", ", Collections.nCopies(colCount, "?"));
        String sql = "INSERT INTO " + request.schema() + "." + request.table()
                + " (" + String.join(", ", cols) + ") VALUES (" + placeholders + ")";

        try {
            PreparedStatement ps = session(sql).ps;   // 持久会话：复用连接 + PreparedStatement
            int rowCount = root.getRowCount();
            int batchSize = Math.max(1, CommonConfig.JDBC_WRITE_BATCH_SIZE);   // 分块行数（默认 1 万行/批）
            for (int row = 0; row < rowCount; row++) {
                for (int col = 0; col < colCount; col++) {
                    FieldVector vector = root.getVector(col);
                    Object value = vector.getObject(row);
                    // Arrow Text → String
                    if (value instanceof Text textValue) {
                        value = textValue.toString();
                    }
                    ps.setObject(col + 1, value);
                }
                ps.addBatch();
                // 每 batchSize 行 flush 一次：单条多行 INSERT 规模可控，均摊 MySQL 解析成本
                if (((row + 1) % batchSize) == 0) {
                    ps.executeBatch();
                }
            }
            if (rowCount % batchSize != 0) {
                ps.executeBatch();   // 收尾剩余不足一批的行
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to write data: "
                    + request.schema() + "." + request.table(), e);
        }
    }

    @Override
    public void closeWrite() {
        for (WriteSession s : writeSessions.values()) {
            s.close();
        }
        writeSessions.clear();
    }

    // ==================== 建表（CTAS）====================

    @Override
    public boolean createTable(String catalog, String schema, String table,
                               List<Field> fields, boolean ifNotExists) {
        // 表已存在：IF NOT EXISTS → 跳过建表（返回 false，调用方不写入）；否则让下方 DDL 抛标准
        // "Table ... already exists" 错误（标准 SQL 语义：不带 IF NOT EXISTS 重复建表即报错）
        if (tableExists(catalog, schema, table)) {
            if (ifNotExists) return false;
        }
        StringBuilder sb = new StringBuilder("CREATE TABLE ");
        if (ifNotExists) sb.append("IF NOT EXISTS ");
        sb.append('`').append(schema).append("`.`").append(table).append("` (");
        List<String> defs = new ArrayList<>();
        for (Field f : fields) {
            defs.add('`' + f.getName() + "` " + arrowTypeToDoris(f.getType()));
        }
        sb.append(String.join(", ", defs)).append(") ENGINE=OLAP");

        // DUPLICATE KEY 的 key 列必须是列定义的前缀；只有首列类型可做 key 时才显式声明，
        // 否则交给 Doris 默认模型（不指定 key 时由它自选排序列）
        if (!fields.isEmpty() && keyCapable(fields.get(0).getType())) {
            sb.append(" DUPLICATE KEY(`").append(fields.get(0).getName()).append("`)");
        }

        // 分桶列必须显式指定；挑第一个可哈希的列，整表没有合适列时 RANDOM 兜底
        String distCol = distributionColumn(fields);
        if (distCol != null) {
            sb.append(" DISTRIBUTED BY HASH(`").append(distCol).append("`) BUCKETS AUTO");
        } else {
            sb.append(" DISTRIBUTED BY RANDOM BUCKETS AUTO");
        }

        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sb.toString());
        } catch (SQLException e) {
            throw new RuntimeException("Failed to create table: "
                    + schema + "." + table, e);
        }
        return true;
    }

    @Override
    public void dropTable(String catalog, String schema, String table) {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("DROP TABLE IF EXISTS " + q(schema, table));
        } catch (SQLException e) {
            throw new RuntimeException("Failed to drop table: " + schema + "." + table, e);
        }
    }

    @Override
    public void truncateTable(String catalog, String schema, String table) {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("TRUNCATE TABLE " + q(schema, table));
        } catch (SQLException e) {
            throw new RuntimeException("Failed to truncate table: " + schema + "." + table, e);
        }
    }

    // ==================== INSERT OVERWRITE（Feature B，实现 OverwriteEnable） ====================
    //
    // Doris 覆盖形态：Master 建一张 staging（CREATE TABLE ... LIKE 克隆目标表结构）→
    // N 个 Worker 分区都写 staging → 全部成功后再用 Doris 原生的原子替换
    // ALTER TABLE target REPLACE WITH TABLE staging PROPERTIES('swap'='false')
    // 一步换掉目标表（staging 被删除，无空窗期）；任一分区失败则 DROP staging，
    // 目标表从未被触碰 —— 天然全成或全不成。

    /**
     * 权限探针：真实执行覆盖所需的三类操作（CREATE LIKE / REPLACE WITH / DROP）各一次，
     * 全程只碰 throwaway 探针表、不触碰目标表 —— 反映角色授权的<b>有效</b>权限
     * （比查权限表可靠）；任一失败 → 清理残留并返回 false（Master 报「暂不支持」）。
     */
    @Override
    public boolean supportsOverwrite(String catalog, String schema, String table) {
        String uuid = UUID.randomUUID().toString().replace("-", "");
        String probe = q(schema, table + "__dojao_probe_" + uuid);
        String probe2 = q(schema, table + "__dojao_probe2_" + uuid);
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE TABLE " + probe + " LIKE " + q(schema, table));
            stmt.executeUpdate("CREATE TABLE " + probe2 + " LIKE " + q(schema, table));
            // swap=false：probe 接收 probe2 的数据，probe2 被删除 —— 与 commitOverwrite 同一路径
            stmt.executeUpdate("ALTER TABLE " + probe + " REPLACE WITH TABLE " + probe2
                    + " PROPERTIES('swap'='false')");
            stmt.executeUpdate("DROP TABLE " + probe);
            return true;
        } catch (SQLException e) {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("DROP TABLE IF EXISTS " + probe);
                stmt.executeUpdate("DROP TABLE IF EXISTS " + probe2);
            } catch (SQLException ignored) {
            }
            return false;
        }
    }

    /** 建覆盖用 staging 表（CREATE TABLE ... LIKE 克隆目标表结构），返回 staging 表名。 */
    @Override
    public String beginOverwrite(String catalog, String schema, String table) {
        String staging = table + "__dojao_tmp_"
                + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE TABLE " + q(schema, staging) + " LIKE " + q(schema, table));
        } catch (SQLException e) {
            throw new RuntimeException("Failed to create staging table for INSERT OVERWRITE: "
                    + schema + "." + table, e);
        }
        return staging;
    }

    /**
     * 收齐全部 N 分区写入后，用 Doris 的原子表替换把 staging 换成目标表：
     * {@code ALTER TABLE target REPLACE WITH TABLE staging PROPERTIES('swap'='false')}
     * —— 内部是「删除旧表 + 重命名 staging」的一次原子元数据操作，查询看不到中间空表；
     * 失败则目标表原样未动（staging 留给 abortOverwrite 清理）。
     */
    @Override
    public void commitOverwrite(String catalog, String schema, String table, String staging) {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("ALTER TABLE " + q(schema, table)
                    + " REPLACE WITH TABLE " + q(schema, staging) + " PROPERTIES('swap'='false')");
        } catch (SQLException e) {
            throw new RuntimeException("Failed to commit INSERT OVERWRITE: "
                    + schema + "." + table, e);
        }
    }

    /** 覆盖失败/取消：删 staging（原表从未被触碰，保持原样）。幂等（IF EXISTS）。 */
    @Override
    public void abortOverwrite(String catalog, String schema, String table, String staging) {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("DROP TABLE IF EXISTS " + q(schema, staging));
        } catch (SQLException e) {
            throw new RuntimeException("Failed to abort INSERT OVERWRITE staging: "
                    + schema + "." + staging, e);
        }
    }

    /** 反引号限定标识符：`schema`.`table`。包级可见：离线探针直接断言拼装结果。 */
    static String q(String schema, String table) {
        return '`' + schema + "`.`" + table + '`';
    }

    /**
     * Arrow 类型 → Doris 列类型。
     *
     * <p>Doris 没有 MySQL 的 65535 行大小上限，字符串统一 {@code STRING}（变长、无长度声明）；
     * 嵌套类型（List / FixedSizeList）以文本承载（Doris 的 ARRAY/MAP 需要精确元素类型，
     * 而 Dojao 的嵌套能力当前只做透传，不做嵌套表达式）。</p>
     */
    static String arrowTypeToDoris(ArrowType type) {
        if (type instanceof ArrowType.Int i) {
            return switch (i.getBitWidth()) {
                case 8 -> "TINYINT";
                case 16 -> "SMALLINT";
                case 32 -> "INT";
                default -> "BIGINT";
            };
        }
        if (type instanceof ArrowType.Utf8 || type instanceof ArrowType.LargeUtf8) return "STRING";
        if (type instanceof ArrowType.FloatingPoint fp) {
            return fp.getPrecision() == FloatingPointPrecision.SINGLE ? "FLOAT" : "DOUBLE";
        }
        // DECIMAL 必须带 (p,s)：裸 "DECIMAL" 落成 DECIMAL(10,0)，会把小数位静默截断
        if (type instanceof ArrowType.Decimal d) {
            return "DECIMAL(" + d.getPrecision() + "," + d.getScale() + ")";
        }
        if (type instanceof ArrowType.Bool) return "BOOLEAN";
        if (type instanceof ArrowType.Date) return "DATE";
        if (type instanceof ArrowType.Timestamp || type instanceof ArrowType.Time) return "DATETIME";
        if (type instanceof ArrowType.FixedSizeList || type instanceof ArrowType.List) return "STRING";
        return "STRING";
    }

    /** 首列能否作为 Doris 的 DUPLICATE KEY（key 列不支持 FLOAT/DOUBLE 与嵌套类型）。 */
    static boolean keyCapable(ArrowType type) {
        return type instanceof ArrowType.Int
                || type instanceof ArrowType.Utf8
                || type instanceof ArrowType.LargeUtf8
                || type instanceof ArrowType.Decimal
                || type instanceof ArrowType.Bool
                || type instanceof ArrowType.Date
                || type instanceof ArrowType.Timestamp
                || type instanceof ArrowType.Time;
    }

    /** 分桶列：第一个可哈希的列（整型 / 字符串 / 定点 / 布尔 / 日期时间 / 浮点）；没有则 null。 */
    static String distributionColumn(List<Field> fields) {
        for (Field f : fields) {
            ArrowType type = f.getType();
            if (type instanceof ArrowType.Int
                    || type instanceof ArrowType.Utf8
                    || type instanceof ArrowType.LargeUtf8
                    || type instanceof ArrowType.Decimal
                    || type instanceof ArrowType.Bool
                    || type instanceof ArrowType.Date
                    || type instanceof ArrowType.Timestamp
                    || type instanceof ArrowType.Time
                    || type instanceof ArrowType.FloatingPoint) {
                return f.getName();
            }
        }
        return null;
    }

    // ==================== 缓存管理 ====================

    /** 使元数据缓存失效（下次查询时重新从 MySQL 加载）。 */
    public void invalidateCache() {
        this.cacheDirty = true;
        this.tableCache.clear();
        this.columnCache.clear();
    }

    // ==================== 私有方法 ====================

    /** 获取表的 COMMENT（MySQL CREATE TABLE 中的注释）。 */
    private String getTableComment(String schema, String table) {
        try (Connection conn = getConnection()) {
            try (PreparedStatement ps = conn.prepareStatement(
                "SELECT TABLE_COMMENT FROM information_schema.TABLES"
                + " WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?")) {
                ps.setString(1, schema);
                ps.setString(2, table);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        String comment = rs.getString("TABLE_COMMENT");
                        return (comment != null && !comment.isEmpty()) ? comment : null;
                    }
                }
            }
        } catch (SQLException e) {
            // ignore
        }
        return null;
    }

    /** 获取表属性（Doris 中主要是 ENGINE、TABLE_ROWS、DATA_LENGTH 等）。 */
    private Map<String, String> getTableProperties(String schema, String table) {
        Map<String, String> props = new LinkedHashMap<>();
        try (Connection conn = getConnection()) {
            // 不查 TABLE_COLLATION：Doris 的 information_schema.TABLES 不保证提供该列，
            // 一旦缺列整条查询会失败、所有属性一起丢
            try (PreparedStatement ps = conn.prepareStatement(
                "SELECT ENGINE, TABLE_ROWS, AVG_ROW_LENGTH, DATA_LENGTH,"
                + "       CREATE_TIME, UPDATE_TIME"
                + " FROM information_schema.TABLES"
                + " WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?")) {
                ps.setString(1, schema);
                ps.setString(2, table);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        putIfNotNull(props, "engine", rs.getString("ENGINE"));
                        putIfNotNull(props, "table_rows", rs.getString("TABLE_ROWS"));
                        putIfNotNull(props, "avg_row_length", rs.getString("AVG_ROW_LENGTH"));
                        putIfNotNull(props, "data_length", rs.getString("DATA_LENGTH"));
                        putIfNotNull(props, "create_time", rs.getString("CREATE_TIME"));
                        putIfNotNull(props, "update_time", rs.getString("UPDATE_TIME"));
                    }
                }
            }
        } catch (SQLException e) {
            // ignore
        }
        return props;
    }

    private static void putIfNotNull(Map<String, String> map, String key, String value) {
        if (value != null && !value.isEmpty()) {
            map.put(key, value);
        }
    }
}
