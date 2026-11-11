package com.dojao.connector.mysql;

import com.dojao.common.config.CommonConfig;
import com.dojao.common.data.Page;
import com.dojao.common.memory.NodeMemory;
import com.dojao.spi.bean.*;
import com.dojao.spi.service.DojaoConnector;
import com.dojao.spi.service.OverwriteEnable;
import com.dojao.spi.util.ArrowJdbcValues;
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
 * MySQL 连接器，通过 JDBC 实现 {@link DojaoConnector} 接口。
 *
 * <p>映射关系：
 * <ul>
 *   <li>catalog → JDBC 连接 URL 对应的 MySQL 实例（可配置名称，默认 "mysql"）</li>
 *   <li>schema  → MySQL 的 DATABASE（即 information_schema.SCHEMATA 中的 SCHEMA_NAME）</li>
 *   <li>table   → MySQL 表（information_schema.TABLES）</li>
 *   <li>column  → MySQL 列（information_schema.COLUMNS）</li>
 * </ul>
 *
 * @author jiaxin
 * @version 2.0
 * @date 2026/6/27
 */
public class MysqlConnector implements DojaoConnector, OverwriteEnable {

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

    public MysqlConnector(String catalogName, String jdbcUrl, String username, String password) {
        this(catalogName, jdbcUrl, username, password,
                NodeMemory.shared().newChildAllocator("connector/mysql", 0));
    }

    public MysqlConnector(String catalogName, String jdbcUrl, String username, String password,
                          BufferAllocator allocator) {
        this.catalogName = catalogName;
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.allocator = allocator;
    }

    public MysqlConnector(String jdbcUrl, String username, String password) {
        this("mysql", jdbcUrl, username, password);
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
    public MysqlConnector copy() {
        return new MysqlConnector(catalogName, jdbcUrl, username, password,
                NodeMemory.shared().newChildAllocator("connector/mysql", 0));
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
        return Optional.of(new Catalog(catalogName, "mysql", Map.of("url", jdbcUrl), null, null));
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

    private static final Map<String, DojaoType> MYSQL_TYPE_MAPPING = new LinkedHashMap<>();

    static {
        // 布尔
        MYSQL_TYPE_MAPPING.put("bit", DojaoType.BOOLEAN);
        MYSQL_TYPE_MAPPING.put("bool", DojaoType.BOOLEAN);
        MYSQL_TYPE_MAPPING.put("boolean", DojaoType.BOOLEAN);
        // 整数
        MYSQL_TYPE_MAPPING.put("tinyint", DojaoType.TINYINT);
        MYSQL_TYPE_MAPPING.put("smallint", DojaoType.SMALLINT);
        MYSQL_TYPE_MAPPING.put("mediumint", DojaoType.INT);
        MYSQL_TYPE_MAPPING.put("int", DojaoType.INT);
        MYSQL_TYPE_MAPPING.put("integer", DojaoType.INT);
        MYSQL_TYPE_MAPPING.put("bigint", DojaoType.BIGINT);
        MYSQL_TYPE_MAPPING.put("year", DojaoType.SMALLINT);
        // 浮点
        MYSQL_TYPE_MAPPING.put("float", DojaoType.FLOAT);
        MYSQL_TYPE_MAPPING.put("real", DojaoType.DOUBLE);
        MYSQL_TYPE_MAPPING.put("double", DojaoType.DOUBLE);
        MYSQL_TYPE_MAPPING.put("double precision", DojaoType.DOUBLE);
        // 定点
        MYSQL_TYPE_MAPPING.put("decimal", DojaoType.DECIMAL);
        MYSQL_TYPE_MAPPING.put("numeric", DojaoType.DECIMAL);
        MYSQL_TYPE_MAPPING.put("dec", DojaoType.DECIMAL);
        MYSQL_TYPE_MAPPING.put("fixed", DojaoType.DECIMAL);
        // 字符串
        MYSQL_TYPE_MAPPING.put("char", DojaoType.STRING);
        MYSQL_TYPE_MAPPING.put("varchar", DojaoType.STRING);
        MYSQL_TYPE_MAPPING.put("tinytext", DojaoType.STRING);
        MYSQL_TYPE_MAPPING.put("text", DojaoType.STRING);
        MYSQL_TYPE_MAPPING.put("mediumtext", DojaoType.STRING);
        MYSQL_TYPE_MAPPING.put("longtext", DojaoType.STRING);
        MYSQL_TYPE_MAPPING.put("enum", DojaoType.STRING);
        MYSQL_TYPE_MAPPING.put("set", DojaoType.STRING);
        // 日期/时间
        MYSQL_TYPE_MAPPING.put("date", DojaoType.DATE);
        MYSQL_TYPE_MAPPING.put("time", DojaoType.STRING);
        MYSQL_TYPE_MAPPING.put("datetime", DojaoType.DATETIME);
        MYSQL_TYPE_MAPPING.put("timestamp", DojaoType.TIMESTAMP);
        // 半结构化
        MYSQL_TYPE_MAPPING.put("json", DojaoType.JSON);
        // 二进制
        MYSQL_TYPE_MAPPING.put("binary", DojaoType.STRING);
        MYSQL_TYPE_MAPPING.put("varbinary", DojaoType.STRING);
        MYSQL_TYPE_MAPPING.put("tinyblob", DojaoType.STRING);
        MYSQL_TYPE_MAPPING.put("blob", DojaoType.STRING);
        MYSQL_TYPE_MAPPING.put("mediumblob", DojaoType.STRING);
        MYSQL_TYPE_MAPPING.put("longblob", DojaoType.STRING);
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
                        DojaoType dojaoType = MYSQL_TYPE_MAPPING.getOrDefault(dataType, DojaoType.STRING);
                        result.put(colName, dojaoType);
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
        return MysqlVarcharMapping.INSTANCE;
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

    @Override
    public List<ColumnStats> getColumnStats(String catalog, String schema, String table) {
        if (!catalogExists(catalog)) return List.of();
        List<ColumnStats> statsList = new ArrayList<>();
        try (Connection conn = getConnection()) {
            // 从 information_schema 获取列元数据 + 基数估算
            try (PreparedStatement ps = conn.prepareStatement(
                "SELECT c.COLUMN_NAME, c.IS_NULLABLE, c.CHARACTER_MAXIMUM_LENGTH,"
                + "       s.CARDINALITY"
                + " FROM information_schema.COLUMNS c"
                + " LEFT JOIN information_schema.STATISTICS s"
                + "   ON s.TABLE_SCHEMA = c.TABLE_SCHEMA"
                + "  AND s.TABLE_NAME = c.TABLE_NAME"
                + "  AND s.COLUMN_NAME = c.COLUMN_NAME"
                + " WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?"
                + " ORDER BY c.ORDINAL_POSITION")) {
                ps.setString(1, schema);
                ps.setString(2, table);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String colName = rs.getString("COLUMN_NAME");
                        boolean nullable = "YES".equalsIgnoreCase(rs.getString("IS_NULLABLE"));
                        long charMaxLen = rs.getLong("CHARACTER_MAXIMUM_LENGTH");
                        long cardinality = rs.getLong("CARDINALITY");
                        statsList.add(new ColumnStats(colName, -1,
                            cardinality > 0 ? cardinality : -1,
                            Optional.empty(), Optional.empty(),
                            charMaxLen > 0 ? Optional.of(charMaxLen) : Optional.empty()));
                    }
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
     * 方言 EXPLAIN 行数估算：对 {@code EXPLAIN SELECT * FROM t [WHERE filter]} 取
     * {@code rows × filtered%}；无谓词（filter 空）→ 整表 {@code rows}。
     *
     * <p>MySQL 的 {@code EXPLAIN.rows} 是索引选择率估算（PK 等值/范围通常相当准），{@code filtered}
     * 是谓词额外过滤比例（5.1+ 可用）。<b>EXPLAIN 失败（老版本 / 权限 / 语法）→ 返回 {@code -1}</b>，
     * 绝不因估算失败而报错。</p>
     */
    private long explainRows(String schema, String table, String filter) {
        String sql = "EXPLAIN SELECT * FROM " + q(schema, table)
                + ((filter != null && !filter.isBlank()) ? " WHERE " + filter : "");
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                long rows = rs.getLong("rows");
                if (!rs.wasNull() && rows > 0) {
                    double filtered = 100.0;
                    try {
                        double f = rs.getDouble("filtered");
                        if (!rs.wasNull() && f > 0) filtered = f;
                    } catch (SQLException ignored) {
                        // 无 filtered 列（极老版本）→ 按 100% 计
                    }
                    long est = (long) Math.ceil(rows * filtered / 100.0);
                    return Math.max(1L, est);
                }
            }
        } catch (SQLException e) {
            // EXPLAIN 不可用（老版本 / 权限 / 语法）→ 未知
        }
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

    /** 构建 SELECT SQL。 */
    private String buildReadSql(ReadRequest request) {
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
     * 获取表的主键列名。仅返回单列数值型主键（适合范围分区）。
     * 若为联合主键或非数值型，返回 {@code Optional.empty()}。
     */
    @Override
    public Optional<String> getPartitionKey(String schema, String table) {
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                 "SELECT kc.COLUMN_NAME, c.DATA_TYPE"
                 + " FROM information_schema.KEY_COLUMN_USAGE kc"
                 + " JOIN information_schema.COLUMNS c"
                 + "   ON c.TABLE_SCHEMA = kc.TABLE_SCHEMA"
                 + "  AND c.TABLE_NAME = kc.TABLE_NAME"
                 + "  AND c.COLUMN_NAME = kc.COLUMN_NAME"
                 + " WHERE kc.TABLE_SCHEMA = ? AND kc.TABLE_NAME = ?"
                 + "   AND kc.CONSTRAINT_NAME = 'PRIMARY'"
                 + " ORDER BY kc.ORDINAL_POSITION")) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                String pkCol = null;
                int count = 0;
                while (rs.next()) {
                    count++;
                    if (count == 1 && isNumericType(rs.getString("DATA_TYPE"))) {
                        pkCol = rs.getString("COLUMN_NAME");
                    }
                }
                return (count == 1 && pkCol != null) ? Optional.of(pkCol) : Optional.empty();
            }
        } catch (SQLException e) {
            return Optional.empty();
        }
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

    private static boolean isNumericType(String dataType) {
        if (dataType == null) return false;
        String upper = dataType.toUpperCase();
        return upper.contains("INT") || upper.contains("DECIMAL") || upper.contains("NUMERIC")
            || upper.contains("FLOAT") || upper.contains("DOUBLE") || upper.contains("BIGINT")
            || upper.contains("TINYINT") || upper.contains("SMALLINT") || upper.contains("MEDIUMINT");
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
                    Object value = ArrowJdbcValues.toJdbc(vector, row);
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
        // 字符串列 DDL 映射：默认字符集（新表继承）→ VARCHAR 上限；未知则 TEXT 兜底
        VarcharMapping vm = varcharMapping();
        String charset = getDefaultCharset(schema).orElse(null);
        // 行大小预算：若所有列按字符集最大 VARCHAR 的声明字节求和超 65535 行上限，
        // 字符串列整体降级 TEXT（TEXT 行内只占指针，保证建表不因行过大失败）
        boolean useTextForStrings = rowExceedsLimit(fields, charset, vm);
        StringBuilder sb = new StringBuilder("CREATE TABLE ");
        if (ifNotExists) sb.append("IF NOT EXISTS ");
        sb.append('`').append(schema).append("`.`").append(table).append("` (");
        List<String> defs = new ArrayList<>();
        for (Field f : fields) {
            defs.add('`' + f.getName() + "` "
                    + arrowTypeToMysql(f.getType(), charset, useTextForStrings, vm));
        }
        sb.append(String.join(", ", defs)).append(')');

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
    // JDBC 覆盖形态：Master 建一张 staging（克隆目标表结构）→ N 个 Worker 分区都写 staging →
    // 全部成功后再一次性把 staging 换成目标表（原子 RENAME 无空窗期）；任一分区失败则 DROP staging，
    // 目标表从未被触碰 —— 天然全成或全不成。

    /**
     * 权限探针：真实执行覆盖所需的三类操作（CREATE LIKE / RENAME / DROP）各一次，
     * 全程只碰 throwaway 探针表、不触碰目标表 —— 反映角色授权的<b>有效</b>权限
     * （比查权限表可靠）；任一失败 → 清理残留并返回 false（Master 报「暂不支持」）。
     */
    @Override
    public boolean supportsOverwrite(String catalog, String schema, String table) {
        String uuid = UUID.randomUUID().toString().replace("-", "");
        String probe = q(schema, table + "__dojao_probe_" + uuid);
        String renamed = q(schema, table + "__dojao_probe_r_" + uuid);
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE TABLE " + probe + " LIKE " + q(schema, table));
            stmt.executeUpdate("RENAME TABLE " + probe + " TO " + renamed);
            stmt.executeUpdate("DROP TABLE " + renamed);
            return true;
        } catch (SQLException e) {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("DROP TABLE IF EXISTS " + probe);
                stmt.executeUpdate("DROP TABLE IF EXISTS " + renamed);
            } catch (SQLException ignored) {
            }
            return false;
        }
    }

    /** 建覆盖用 staging 表（克隆目标表结构），返回 staging 表名（Worker 侧写入目标）。 */
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
     * 收齐全部 N 分区写入后，把 staging 原子切换成目标表（单条 RENAME 双目标，无空窗期）：
     * {@code target → target__dojao_old_<uuid>, staging → target}，随后 DROP 旧表。
     * 相比「TRUNCATE 后再 RENAME」的窗口，多表 RENAME 单语句原子 —— RENAME 失败则 target 原样未动。
     */
    @Override
    public void commitOverwrite(String catalog, String schema, String table, String staging) {
        String targetQ = q(schema, table);
        String stagingQ = q(schema, staging);
        String oldQ = q(schema, table + "__dojao_old_"
                + UUID.randomUUID().toString().replace("-", "").substring(0, 8));
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("RENAME TABLE " + targetQ + " TO " + oldQ
                    + ", " + stagingQ + " TO " + targetQ);
            stmt.executeUpdate("DROP TABLE " + oldQ);
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

    /** 反引号限定标识符：`schema`.`table`。 */
    private static String q(String schema, String table) {
        return '`' + schema + "`.`" + table + '`';
    }

    /** Arrow 类型 → MySQL 列类型。字符集经映射器推算 VARCHAR 上限（未知 → TEXT）。 */
    private static String arrowTypeToMysql(ArrowType type, String charset,
                                           boolean useTextForStrings, VarcharMapping vm) {
        if (type instanceof ArrowType.Int i) {
            return switch (i.getBitWidth()) {
                case 8 -> "TINYINT";
                case 16 -> "SMALLINT";
                case 32 -> "INT";
                default -> "BIGINT";
            };
        }
        if (type instanceof ArrowType.Utf8)
            return useTextForStrings ? "TEXT" : vm.resolveVarcharType(charset);
        if (type instanceof ArrowType.LargeUtf8) return "LONGTEXT";
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
        if (type instanceof ArrowType.FixedSizeList || type instanceof ArrowType.List) return "JSON";
        // 兜底：文本
        return useTextForStrings ? "TEXT" : vm.resolveVarcharType(charset);
    }

    /** 估算整行声明字节是否超过 MySQL 65535 行上限（超则字符串列须降级 TEXT）。 */
    private static boolean rowExceedsLimit(List<Field> fields, String charset, VarcharMapping vm) {
        long total = 0;
        for (Field f : fields) total += arrowColumnRowBytes(f.getType(), charset, vm);
        return total > MysqlVarcharMapping.MAX_ROW_BYTES;
    }

    /** 单列的行内声明字节估算（用于建表行大小预算）。 */
    private static long arrowColumnRowBytes(ArrowType type, String charset, VarcharMapping vm) {
        if (type instanceof ArrowType.Int i) return i.getBitWidth() / 8;
        if (type instanceof ArrowType.Utf8) return vm.varcharMaxBytes(charset);
        if (type instanceof ArrowType.LargeUtf8) return vm.textRowBytes();
        if (type instanceof ArrowType.FloatingPoint fp) {
            return fp.getPrecision() == FloatingPointPrecision.SINGLE ? 4 : 8;
        }
        if (type instanceof ArrowType.Decimal) return 16;
        if (type instanceof ArrowType.Bool) return 1;
        if (type instanceof ArrowType.Date) return 3;
        if (type instanceof ArrowType.Timestamp || type instanceof ArrowType.Time) return 8;
        // FixedSizeList / List（JSON）及未知：按 TEXT 指针小占用计
        return vm.textRowBytes();
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

    /** 获取表属性（MySQL 中主要是 ENGINE、TABLE_ROWS 等）。 */
    private Map<String, String> getTableProperties(String schema, String table) {
        Map<String, String> props = new LinkedHashMap<>();
        try (Connection conn = getConnection()) {
            try (PreparedStatement ps = conn.prepareStatement(
                "SELECT ENGINE, TABLE_ROWS, AVG_ROW_LENGTH, DATA_LENGTH,"
                + "       CREATE_TIME, UPDATE_TIME, TABLE_COLLATION"
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
                        putIfNotNull(props, "table_collation", rs.getString("TABLE_COLLATION"));
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
