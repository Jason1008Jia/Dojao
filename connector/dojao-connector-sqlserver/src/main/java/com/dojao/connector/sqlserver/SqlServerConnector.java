package com.dojao.connector.sqlserver;

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
 * Microsoft SQL Server 连接器，通过 JDBC 实现 {@link DojaoConnector} 接口。
 *
 * <p>三层映射：
 * <ul>
 *   <li>catalog → JDBC 连接串（{@code databaseName}）指向的<b>数据库</b>（实例与库都在 URL 里）</li>
 *   <li>schema  → SQL Server 的 schema（默认 {@code dbo}，见 {@code sys.schemas}）</li>
 *   <li>table   → 表（{@code INFORMATION_SCHEMA.TABLES}，仅 BASE TABLE）</li>
 *   <li>column  → 列（{@code INFORMATION_SCHEMA.COLUMNS}）</li>
 * </ul>
 *
 * <p><b>方言要点</b>：
 * <ul>
 *   <li>标识符用方括号转义（{@code [col]}，内嵌 {@code ]} 翻倍）；{@link SqlIdentifiers#unquote}
 *       已能剥除 {@code [x]}，所以列裁剪 / CTAS 声明列两条链路可以幂等往返。</li>
 *   <li><b>tinyint 必须转 SMALLINT</b>：SQL Server 的 {@code tinyint} 是 0..255 无符号，而
 *       {@link JdbcArrowConverter} 按 JDBC {@code TINYINT} 映射成 Arrow {@code Int(8, true)}
 *       —— 128..255 会在运行期静默溢出成负数。所有读取投影（列裁剪与整表 SELECT）
 *       都把 tinyint 列写成 {@code CAST([c] AS SMALLINT) AS [c]}，同时 {@link #getColumnTypes}
 *       也把它报成 SMALLINT，保证「声明类型 = 实际产出类型」。</li>
 *   <li>{@code timestamp} 在 SQL Server 里是 rowversion（8 字节二进制，不是时间）→ STRING。</li>
 *   <li>分页：无 offset 用 {@code SELECT TOP (n)}；有 offset 用
 *       {@code ORDER BY (SELECT NULL) OFFSET n ROWS [FETCH NEXT m ROWS ONLY]}（T-SQL 要求
 *       OFFSET/FETCH 必须带 ORDER BY）。</li>
 * </ul>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/22
 */
public class SqlServerConnector implements DojaoConnector, OverwriteEnable {

    /** 此连接器的 catalog 名称（对外暴露的 catalog 标识）。 */
    private final String catalogName;

    /** JDBC 连接信息。 */
    private final String jdbcUrl;
    private final String username;
    private final String password;

    /** 缓存：schema.table（原文）→ List<Column>。 */
    private final Map<String, List<Column>> columnCache = new ConcurrentHashMap<>();

    /** 缓存：schema.table（小写）→ tinyint 列名（小写）集合。 */
    private final Map<String, Set<String>> tinyintCache = new ConcurrentHashMap<>();

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

    public SqlServerConnector(String catalogName, String jdbcUrl, String username, String password) {
        this(catalogName, jdbcUrl, username, password,
                NodeMemory.shared().newChildAllocator("connector/sqlserver", 0));
    }

    public SqlServerConnector(String catalogName, String jdbcUrl, String username, String password,
                              BufferAllocator allocator) {
        this.catalogName = catalogName;
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.allocator = allocator;
    }

    public SqlServerConnector(String jdbcUrl, String username, String password) {
        this("sqlserver", jdbcUrl, username, password);
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
    public SqlServerConnector copy() {
        return new SqlServerConnector(catalogName, jdbcUrl, username, password,
                NodeMemory.shared().newChildAllocator("connector/sqlserver", 0));
    }

    // ==================== Connection ====================

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, username, password);
    }

    /**
     * 连接性探针：用当前 URL / 账号<b>真的建一次连接</b>（引擎在 CREATE CATALOG 落库前调用，
     * 连不上就不入库）。只做一次 {@code isValid} 握手，不枚举元数据 —— 避免把
     * 「连得上但没权限」误报成连接失败。
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
        return Optional.of(new Catalog(catalogName, "sqlserver", Map.of("url", jdbcUrl), null, null));
    }

    @Override
    public List<String> listCatalogs() {
        return List.of(catalogName);
    }

    // ==================== Schema ====================

    @Override
    public boolean schemaExists(String catalog, String schema) {
        if (!catalogExists(catalog)) return false;
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT 1 FROM sys.schemas WHERE name = ?")) {
            ps.setString(1, schema);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to check schema existence: " + schema, e);
        }
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
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT name FROM sys.schemas"
                     + " WHERE name NOT IN ('sys', 'guest', 'INFORMATION_SCHEMA')"
                     + " ORDER BY name")) {
            while (rs.next()) {
                schemas.add(rs.getString(1));
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
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT 1 FROM INFORMATION_SCHEMA.TABLES"
                     + " WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?")) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
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
        Map<String, String> comments = getColumnComments(schema, tableName);
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_DEFAULT, IS_NULLABLE"
                     + " FROM INFORMATION_SCHEMA.COLUMNS"
                     + " WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
                     + " ORDER BY ORDINAL_POSITION")) {
            ps.setString(1, schema);
            ps.setString(2, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String name = rs.getString("COLUMN_NAME");
                    columns.add(new Column(
                            name,
                            rs.getString("DATA_TYPE"),
                            comments.get(name),
                            rs.getString("COLUMN_DEFAULT"),
                            "YES".equalsIgnoreCase(rs.getString("IS_NULLABLE"))
                    ));
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

    private static final Map<String, DojaoType> SQLSERVER_TYPE_MAPPING = new LinkedHashMap<>();

    static {
        // SQL Server tinyint 是 0..255 无符号 —— 读取侧统一 CAST 成 SMALLINT，
        // 因此这里也报 SMALLINT（声明类型必须与产出类型一致，见类注释）。
        SQLSERVER_TYPE_MAPPING.put("tinyint", DojaoType.SMALLINT);
        SQLSERVER_TYPE_MAPPING.put("smallint", DojaoType.SMALLINT);
        SQLSERVER_TYPE_MAPPING.put("int", DojaoType.INT);
        SQLSERVER_TYPE_MAPPING.put("bigint", DojaoType.BIGINT);
        // 布尔
        SQLSERVER_TYPE_MAPPING.put("bit", DojaoType.BOOLEAN);
        // 浮点
        SQLSERVER_TYPE_MAPPING.put("real", DojaoType.FLOAT);
        SQLSERVER_TYPE_MAPPING.put("float", DojaoType.DOUBLE);
        // 定点
        SQLSERVER_TYPE_MAPPING.put("decimal", DojaoType.DECIMAL);
        SQLSERVER_TYPE_MAPPING.put("numeric", DojaoType.DECIMAL);
        SQLSERVER_TYPE_MAPPING.put("money", DojaoType.DECIMAL);
        SQLSERVER_TYPE_MAPPING.put("smallmoney", DojaoType.DECIMAL);
        // 字符串
        SQLSERVER_TYPE_MAPPING.put("char", DojaoType.STRING);
        SQLSERVER_TYPE_MAPPING.put("varchar", DojaoType.STRING);
        SQLSERVER_TYPE_MAPPING.put("nchar", DojaoType.STRING);
        SQLSERVER_TYPE_MAPPING.put("nvarchar", DojaoType.STRING);
        SQLSERVER_TYPE_MAPPING.put("text", DojaoType.STRING);
        SQLSERVER_TYPE_MAPPING.put("ntext", DojaoType.STRING);
        SQLSERVER_TYPE_MAPPING.put("sysname", DojaoType.STRING);
        SQLSERVER_TYPE_MAPPING.put("uniqueidentifier", DojaoType.STRING);
        SQLSERVER_TYPE_MAPPING.put("xml", DojaoType.STRING);
        SQLSERVER_TYPE_MAPPING.put("sql_variant", DojaoType.STRING);
        SQLSERVER_TYPE_MAPPING.put("hierarchyid", DojaoType.STRING);
        SQLSERVER_TYPE_MAPPING.put("geography", DojaoType.STRING);
        SQLSERVER_TYPE_MAPPING.put("geometry", DojaoType.STRING);
        // 日期/时间
        SQLSERVER_TYPE_MAPPING.put("date", DojaoType.DATE);
        SQLSERVER_TYPE_MAPPING.put("time", DojaoType.STRING);
        SQLSERVER_TYPE_MAPPING.put("datetime", DojaoType.DATETIME);
        SQLSERVER_TYPE_MAPPING.put("datetime2", DojaoType.DATETIME);
        SQLSERVER_TYPE_MAPPING.put("smalldatetime", DojaoType.DATETIME);
        SQLSERVER_TYPE_MAPPING.put("datetimeoffset", DojaoType.TIMESTAMP_TZ);
        // timestamp = rowversion（8 字节二进制，不是时间）
        SQLSERVER_TYPE_MAPPING.put("timestamp", DojaoType.STRING);
        SQLSERVER_TYPE_MAPPING.put("rowversion", DojaoType.STRING);
        // 二进制
        SQLSERVER_TYPE_MAPPING.put("binary", DojaoType.STRING);
        SQLSERVER_TYPE_MAPPING.put("varbinary", DojaoType.STRING);
        SQLSERVER_TYPE_MAPPING.put("image", DojaoType.STRING);
    }

    /** SQL Server 类型名（{@code DATA_TYPE}）→ Dojao 类型；未知类型按字符串处理。 */
    static DojaoType dojaoType(String dataType) {
        if (dataType == null) return DojaoType.STRING;
        return SQLSERVER_TYPE_MAPPING.getOrDefault(
                dataType.toLowerCase(Locale.ROOT), DojaoType.STRING);
    }

    @Override
    public Map<String, DojaoType> getColumnTypes(String schema, String table) {
        Map<String, DojaoType> result = new LinkedHashMap<>();
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT COLUMN_NAME, DATA_TYPE"
                     + " FROM INFORMATION_SCHEMA.COLUMNS"
                     + " WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
                     + " ORDER BY ORDINAL_POSITION")) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.put(rs.getString("COLUMN_NAME"), dojaoType(rs.getString("DATA_TYPE")));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(
                    "Failed to get column types for: " + schema + "." + table, e);
        }
        return result;
    }

    @Override
    public List<String> listTables(String catalog, String schema) {
        if (!catalogExists(catalog)) return List.of();
        List<String> tables = new ArrayList<>();
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES"
                     + " WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'"
                     + " ORDER BY TABLE_NAME")) {
            ps.setString(1, schema);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tables.add(rs.getString("TABLE_NAME"));
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
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS"
                     + " WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?")) {
            ps.setString(1, schema);
            ps.setString(2, table);
            ps.setString(3, column);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
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

    // ==================== CTAS 声明列（与读取同一条映射） ====================

    /**
     * CTAS 源列类型映射：对指定列集跑一条零行探针
     * {@code SELECT <投影> FROM 表 WHERE 1=0}，直接取 JDBC 元数据 →
     * {@link JdbcArrowConverter#buildSchema}。
     *
     * <p>投影走与运行期完全相同的 {@link #projectColumn}（tinyint → SMALLINT 的 CAST），
     * 因此 Master 按此建出的目标列类型与 Worker 实际扫出的 Arrow 类型逐列一致。</p>
     */
    @Override
    public List<Field> sourceFields(String catalog, String schema, String table, List<String> columns) {
        List<Column> tableCols = getTableColumns(catalog, schema, table);
        Set<String> known = new LinkedHashSet<>();
        for (Column c : tableCols) known.add(c.name());
        List<String> requested = new ArrayList<>();
        if (columns == null || columns.isEmpty()) {
            requested.addAll(known);
        } else {
            for (String fragment : columns) {
                String name = resolveColumnName(fragment, known);
                if (name != null && (known.isEmpty() || known.contains(name))) {
                    requested.add(name);
                }
            }
        }
        if (requested.isEmpty()) return List.of();

        Set<String> tinyint = claimedTinyint(tableCols);
        StringBuilder select = new StringBuilder();
        for (String name : requested) {
            if (select.length() > 0) select.append(", ");
            select.append(projectColumn(name, tinyint));
        }
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT " + select + " FROM " + q(schema, table) + " WHERE 1=0")) {
            try (ResultSet rs = ps.executeQuery()) {
                return JdbcArrowConverter.buildSchema(rs.getMetaData()).getFields();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to read source column metadata for CTAS: "
                    + schema + "." + table, e);
        }
    }

    /**
     * 从「列名 / 已转义的 SQL 片段」还原出元数据里的裸列名。
     *
     * <p>引擎不同链路传进来的形态不同：{@code pruneColumns} 之后是 {@code [col]} 或
     * {@code CAST([col] AS SMALLINT) AS [col]}，规划期可能还是裸名。依次尝试
     * 直接剥引号 → 取最后一个 {@code AS} 别名 → 取最后一段方括号内容。</p>
     */
    static String resolveColumnName(String fragment, Set<String> known) {
        if (fragment == null) return null;
        String t = fragment.trim();
        String direct = SqlIdentifiers.unquote(t);
        if (known.isEmpty() || known.contains(direct)) return direct;
        int as = t.toUpperCase(Locale.ROOT).lastIndexOf(" AS ");
        if (as >= 0) {
            String tail = SqlIdentifiers.unquote(t.substring(as + 4).trim());
            if (known.contains(tail)) return tail;
        }
        int lb = t.lastIndexOf('[');
        int rb = t.lastIndexOf(']');
        if (lb >= 0 && rb > lb) {
            String inner = t.substring(lb + 1, rb).replace("]]", "]");
            if (known.contains(inner)) return inner;
        }
        return direct;
    }

    // ==================== 统计信息 ====================

    /** 采样行数：一个完整 batch，摊薄 Arrow power-of-2 分配带来的字节高估。 */
    private static final int SIZE_SAMPLE_ROWS = CommonConfig.DEFAULT_BATCH_SIZE;

    @Override
    public TableStats getTableStats(String catalog, String schema, String table) {
        if (!catalogExists(catalog)) {
            throw new IllegalArgumentException("Catalog '" + catalog + "' does not exist");
        }
        long[] raw = fetchRawStats(schema, table);
        long rows = raw[0] > 0 ? raw[0] : -1;
        long bytes = estimateBytesInternal(schema, table, null, raw);
        return new TableStats(catalog, schema, table, rows, bytes, List.of());
    }

    /**
     * 读取元数据统计 {@code [rowCount, byteSize]}，任一未知记 {@code -1}。
     *
     * <p>行数 = {@code sys.partitions.rows}（index_id 0/1，堆或聚集索引）；
     * 字节 = {@code sys.allocation_units.total_pages × 8192}。需要
     * {@code VIEW DATABASE STATE} 权限，<b>权限不足 / 查不到 → 返回 {@code [-1,-1]}，不抛异常</b>。</p>
     */
    private long[] fetchRawStats(String schema, String table) {
        long rowCount = -1;
        long byteSize = -1;
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT SUM(CONVERT(BIGINT, p.rows)) AS row_count,"
                     + "       SUM(CONVERT(BIGINT, a.total_pages)) * 8192 AS byte_size"
                     + " FROM sys.tables t"
                     + " JOIN sys.schemas s ON t.schema_id = s.schema_id"
                     + " JOIN sys.partitions p ON t.object_id = p.object_id"
                     + "  AND p.index_id IN (0, 1)"
                     + " JOIN sys.allocation_units a ON p.partition_id = a.container_id"
                     + " WHERE s.name = ? AND t.name = ?"
                     + " GROUP BY t.object_id")) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    long rows = rs.getLong("row_count");
                    long bytes = rs.getLong("byte_size");
                    rowCount = rows > 0 ? rows : -1;
                    byteSize = bytes > 0 ? bytes : -1;
                }
            }
        } catch (SQLException e) {
            // 权限不足 / 元数据不可读 → 未知，交给后续降级
        }
        return new long[]{rowCount, byteSize};
    }

    /**
     * 字节估算。SQL Server 没有 MySQL/PG 那样的 {@code EXPLAIN} 可解析，
     * 因此 {@link JdbcSizeEstimator} 的 EXPLAIN 档传 {@code null}；元数据也拿不到时，
     * 用「元数据行数 × 采样平均行长」补齐（仍拿不到 → {@code -1}，调用方退回最保守的排序归并）。
     */
    private long estimateBytesInternal(String schema, String table, String filter, long[] raw) {
        JdbcSizeEstimator.Estimate est = JdbcSizeEstimator.estimate(
                this, catalogName, schema, table, filter, raw[0], raw[1], null, SIZE_SAMPLE_ROWS);
        if (est.byteSize() > 0) return est.byteSize();
        long rows = est.rowCount() > 0 ? est.rowCount() : raw[0];
        if (rows <= 0) return -1;
        double avg = JdbcSizeEstimator.sampleAvgRowBytes(
                this, catalogName, schema, table, SIZE_SAMPLE_ROWS);
        return avg > 0 ? (long) Math.ceil(rows * avg) : -1;
    }

    @Override
    public long estimateBytes(String catalog, String schema, String table, String filter) {
        if (!catalogExists(catalog)) return -1L;
        long[] raw = fetchRawStats(schema, table);
        return estimateBytesInternal(schema, table, filter, raw);
    }

    /**
     * 列统计：SQL Server 的统计对象（{@code sys.stats}）只记录直方图属性，
     * 不像 PG {@code pg_stats} 那样直接暴露 NDV；猜一个基数比不报更危险，
     * 因此返回空列表（join 选型退回行数/字节估算）。
     */
    @Override
    public List<ColumnStats> getColumnStats(String catalog, String schema, String table) {
        return List.of();
    }

    // ==================== 数据读取（JDBC → Arrow，游标模式） ====================

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
            // 首次调用 → 打开新游标（驱动默认 adaptive response buffering，按 fetchSize 分批拉取）
            if (readConnection == null || readConnection.isClosed()) {
                readConnection = getConnection();
                readPreparedStatement = readConnection.prepareStatement(sql);
                readPreparedStatement.setFetchSize(CommonConfig.DEFAULT_BATCH_SIZE);
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

    /**
     * 列裁剪：SQL Server 方言用方括号转义；<b>tinyint 列额外 CAST 成 SMALLINT</b>
     * （见类注释的溢出说明）。返回的 {@link ReadRequest#columns()} 是可直拼进 SELECT 的片段。
     */
    @Override
    public ReadRequest pruneColumns(ReadRequest request, List<String> columns) {
        if (columns == null || columns.isEmpty()) return request;
        Set<String> tinyint = tinyintColumns(request.schema(), request.table());
        List<String> escaped = new ArrayList<>(columns.size());
        for (String col : columns) {
            escaped.add(projectColumn(col, tinyint));
        }
        return request.withColumns(escaped);
    }

    /** 谓词下推：T-SQL 方言原样透传（buildReadSql 直接拼进 WHERE）。 */
    @Override
    public ReadRequest pushdownPredicate(ReadRequest request, String predicate) {
        if (predicate == null || predicate.isBlank()) return request;
        String merged = request.filterExpression()
                .map(f -> "(" + f + ") AND (" + predicate + ")")
                .orElse(predicate);
        return request.withFilter(merged);
    }

    @Override
    public boolean supportsPredicatePushdown() {
        // buildReadSql 把 filterExpression 直接拼进 WHERE → 过滤真的执行了，引擎可以不下推就不过滤
        return true;
    }

    /** 构建 SELECT SQL（包级可见：离线探针直接断言方言形态）。 */
    String buildReadSql(ReadRequest request) {
        List<String> selectCols = request.columns().orElse(List.of());
        String projection = selectCols.isEmpty()
                ? defaultProjection(request)
                : String.join(", ", selectCols);

        boolean top = request.limit() > 0 && request.offset() <= 0;
        StringBuilder sql = new StringBuilder("SELECT ");
        if (top) sql.append("TOP (").append(request.limit()).append(") ");
        sql.append(projection)
                .append(" FROM ").append(q(request.schema(), request.table()));

        request.filterExpression().ifPresent(filter -> sql.append(" WHERE ").append(filter));

        if (request.offset() > 0) {
            // T-SQL：OFFSET/FETCH 必须带 ORDER BY；(SELECT NULL) = 不承诺顺序（与 MySQL LIMIT/OFFSET 兜底同口径）
            sql.append(" ORDER BY (SELECT NULL) OFFSET ").append(request.offset()).append(" ROWS");
            if (request.limit() > 0) {
                sql.append(" FETCH NEXT ").append(request.limit()).append(" ROWS ONLY");
            }
        }
        return sql.toString();
    }

    /** 无列裁剪时的整表投影：按元数据列出全部列，tinyint 同样 CAST（元数据取不到 → {@code *}）。 */
    private String defaultProjection(ReadRequest request) {
        List<Column> cols = getTableColumns(request.catalog(), request.schema(), request.table());
        if (cols.isEmpty()) return "*";
        Set<String> tinyint = claimedTinyint(cols);
        StringBuilder sb = new StringBuilder();
        for (Column c : cols) {
            if (sb.length() > 0) sb.append(", ");
            sb.append(projectColumn(c.name(), tinyint));
        }
        return sb.toString();
    }

    /**
     * 单列投影片段：
     * <ul>
     *   <li>已经是 SQL 片段（含 {@code (} 或以 {@code CAST} 开头）→ 原样返回（幂等）；</li>
     *   <li>tinyint 列 → {@code CAST([c] AS SMALLINT) AS [c]}（防 128..255 溢出 Arrow Int8）；</li>
     *   <li>其余 → {@code [c]}。</li>
     * </ul>
     */
    static String projectColumn(String rawName, Set<String> tinyintLower) {
        if (rawName == null) return null;
        String t = rawName.trim();
        if (looksLikeFragment(t)) return t;
        String name = SqlIdentifiers.unquote(t);
        String quoted = bracket(name);
        boolean tiny = tinyintLower != null
                && tinyintLower.contains(name.toLowerCase(Locale.ROOT));
        return tiny ? "CAST(" + quoted + " AS SMALLINT) AS " + quoted : quoted;
    }

    /** 判断是否已是 SQL 片段（而非裸标识符 / 带引号标识符）。 */
    static boolean looksLikeFragment(String s) {
        if (s == null) return false;
        String t = s.trim();
        return t.indexOf('(') >= 0 || t.regionMatches(true, 0, "CAST", 0, 4);
    }

    /** 方括号标识符转义（幂等：先剥任何方言引号，再按 SQL Server 规则加括号）。 */
    static String bracket(String ident) {
        String name = SqlIdentifiers.unquote(ident);
        return "[" + name.replace("]", "]]") + "]";
    }

    /** {schema}.{table} 的方括号限定名。 */
    private static String q(String schema, String table) {
        return bracket(schema) + "." + bracket(table);
    }

    /** 表里 tinyint 列（小写）集合；元数据不可读 → 空集合（退化为不 CAST，不阻塞查询）。 */
    private Set<String> tinyintColumns(String schema, String table) {
        String key = (schema + "." + table).toLowerCase(Locale.ROOT);
        Set<String> cached = tinyintCache.get(key);
        if (cached != null && !cacheDirty) return cached;
        Set<String> out = new HashSet<>();
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS"
                     + " WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND DATA_TYPE = 'tinyint'")) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(rs.getString(1).toLowerCase(Locale.ROOT));
                }
            }
        } catch (SQLException e) {
            return Set.of();
        }
        Set<String> frozen = Set.copyOf(out);
        tinyintCache.put(key, frozen);
        return frozen;
    }

    /** 从已加载的列元数据里挑出 tinyint（小写）集合。 */
    private static Set<String> claimedTinyint(List<Column> columns) {
        Set<String> out = new HashSet<>();
        for (Column c : columns) {
            if (c.dataType() != null && "tinyint".equalsIgnoreCase(c.dataType())) {
                out.add(c.name().toLowerCase(Locale.ROOT));
            }
        }
        return out;
    }

    // ==================== 分区探针（平行 TableScan 用） ====================

    /**
     * 单列数值型主键列名（范围分区用）。联合主键 / 非数值 / 无主键 → empty。
     */
    @Override
    public Optional<String> getPartitionKey(String schema, String table) {
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT c.name, ty.name AS type_name"
                     + " FROM sys.indexes i"
                     + " JOIN sys.index_columns ic ON i.object_id = ic.object_id"
                     + "  AND i.index_id = ic.index_id"
                     + " JOIN sys.columns c ON ic.object_id = c.object_id"
                     + "  AND ic.column_id = c.column_id"
                     + " JOIN sys.types ty ON c.user_type_id = ty.user_type_id"
                     + " JOIN sys.tables t ON i.object_id = t.object_id"
                     + " JOIN sys.schemas s ON t.schema_id = s.schema_id"
                     + " WHERE i.is_primary_key = 1 AND s.name = ? AND t.name = ?"
                     + " ORDER BY ic.key_ordinal")) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                String pkCol = null;
                int count = 0;
                while (rs.next()) {
                    count++;
                    if (count == 1 && isNumericType(rs.getString("type_name"))) {
                        pkCol = rs.getString("name");
                    }
                }
                return (count == 1 && pkCol != null) ? Optional.of(pkCol) : Optional.empty();
            }
        } catch (SQLException e) {
            return Optional.empty();
        }
    }

    /** 精确行数（{@code COUNT_BIG(*)}，返回 BIGINT 不溢出）。 */
    @Override
    public long getExactRowCount(String schema, String table) {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT COUNT_BIG(*) FROM " + q(schema, table))) {
            if (rs.next()) return rs.getLong(1);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to count rows: " + schema + "." + table, e);
        }
        return 0;
    }

    /** 主键列 MIN/MAX（范围分区用）。 */
    @Override
    public long[] getPartitionKeyRange(String schema, String table, String pkColumn) {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT MIN(" + bracket(pkColumn) + "), MAX(" + bracket(pkColumn) + ")"
                     + " FROM " + q(schema, table))) {
            if (rs.next()) {
                return new long[]{rs.getLong(1), rs.getLong(2)};
            }
        } catch (SQLException e) {
            throw new RuntimeException(
                    "Failed to get range for: " + schema + "." + table + "." + pkColumn, e);
        }
        return new long[]{0, 0};
    }

    private static boolean isNumericType(String typeName) {
        if (typeName == null) return false;
        String t = typeName.toLowerCase(Locale.ROOT);
        return t.equals("tinyint") || t.equals("smallint") || t.equals("int")
                || t.equals("bigint") || t.equals("decimal") || t.equals("numeric")
                || t.equals("money") || t.equals("smallmoney") || t.equals("real")
                || t.equals("float");
    }

    // ==================== 并行扫描分区（主键 → 数值列 → OFFSET/FETCH 兜底） ====================

    /**
     * 分区策略：
     * <ol>
     *   <li><b>单列数值主键范围</b>：{@code [k] >= a AND [k] <= b} 均分；</li>
     *   <li><b>数值列范围</b>：无主键时找一个数值列（优先 NOT NULL）按 MIN/MAX 均分；</li>
     *   <li><b>OFFSET/FETCH 均分</b>：最后兜底，每分区
     *       {@code ORDER BY (SELECT NULL) OFFSET i*step ROWS FETCH NEXT step ROWS ONLY}。</li>
     * </ol>
     */
    @Override
    public List<ReadRequest> planParallelScan(ReadRequest request, int parallelism) {
        if (parallelism <= 1 || request.limit() > 0 || request.offset() > 0) return List.of();

        Optional<String> pk = getPartitionKey(request.schema(), request.table());
        if (pk.isPresent()) {
            long[] range = getPartitionKeyRange(request.schema(), request.table(), pk.get());
            List<ReadRequest> parts = splitRange(request, pk.get(), range[0], range[1], parallelism);
            if (!parts.isEmpty()) return parts;
        }
        Optional<String> numericCol = findNumericColumn(request.schema(), request.table());
        if (numericCol.isPresent()) {
            long[] range = getPartitionKeyRange(request.schema(), request.table(), numericCol.get());
            List<ReadRequest> parts = splitRange(request, numericCol.get(), range[0], range[1], parallelism);
            if (!parts.isEmpty()) return parts;
        }
        return planOffsetPartitions(request, parallelism);
    }

    /** 按 [min,max] 均分，每段 {@code key >= a AND key <= b}（键按方言加方括号）。 */
    private List<ReadRequest> splitRange(ReadRequest request, String key,
                                         long min, long max, int parallelism) {
        if (min >= max) return List.of();
        long span = max - min + 1;
        if (span <= 0) return List.of();   // long 溢出 → 放弃范围分区，走 OFFSET 兜底
        long step = span / parallelism + (span % parallelism == 0 ? 0 : 1);
        if (step <= 0) step = 1;
        String k = bracket(key);
        List<ReadRequest> parts = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            long start = min + (long) i * step;
            long end = (i == parallelism - 1) ? max : (min + (long) (i + 1) * step - 1);
            if (start > max) break;
            String rangeFilter = k + " >= " + start + " AND " + k + " <= " + end;
            String merged = request.filterExpression()
                    .map(f -> "(" + f + ") AND (" + rangeFilter + ")")
                    .orElse(rangeFilter);
            parts.add(new ReadRequest(request.catalog(), request.schema(), request.table(),
                    request.columns(), Optional.of(merged), request.offset(), request.limit()));
        }
        return parts;
    }

    /** 找第一个数值列：优先 NOT NULL（最接近隐式主键），否则任意数值列。 */
    private Optional<String> findNumericColumn(String schema, String table) {
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE"
                     + " FROM INFORMATION_SCHEMA.COLUMNS"
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

    /** OFFSET/FETCH 均分（COUNT 必须带过滤条件，否则过滤后行数偏小导致漏数据）。 */
    private List<ReadRequest> planOffsetPartitions(ReadRequest request, int parallelism) {
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

    /** 带过滤条件的行数统计（OFFSET 分区用）。 */
    private long countFilteredRows(ReadRequest request) {
        StringBuilder sql = new StringBuilder("SELECT COUNT_BIG(*) FROM ")
                .append(q(request.schema(), request.table()));
        request.filterExpression().ifPresent(f -> sql.append(" WHERE ").append(f));
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql.toString())) {
            if (rs.next()) return rs.getLong(1);
        } catch (SQLException e) {
            return 0;
        }
        return 0;
    }

    // ==================== 数据写入（Arrow → JDBC） ====================

    /**
     * 持久写会话：每个（写线程 × SQL）一个独立 Connection + PreparedStatement 复用，
     * 避免每页新建连接的开销。
     */
    private final ConcurrentHashMap<String, WriteSession> writeSessions = new ConcurrentHashMap<>();

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
        WriteSession created = new WriteSession(jdbcUrl, username, password, sql);
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
            cols = new ArrayList<>(targets.size());
            for (String t : targets) cols.add(bracket(t));
        } else {
            cols = new ArrayList<>();
            for (Field field : root.getSchema().getFields()) {
                cols.add(bracket(field.getName()));
            }
        }
        String placeholders = String.join(", ", Collections.nCopies(colCount, "?"));
        String sql = "INSERT INTO " + q(request.schema(), request.table())
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

    // ==================== 建表（CTAS） ====================

    @Override
    public boolean createTable(String catalog, String schema, String table,
                               List<Field> fields, boolean ifNotExists) {
        // 表已存在：IF NOT EXISTS → 跳过建表（返回 false，调用方不写入）；否则让下方 DDL 抛标准
        // "There is already an object named ..." 错误（标准 SQL 语义）。
        if (tableExists(catalog, schema, table)) {
            if (ifNotExists) return false;
        }
        StringBuilder sb = new StringBuilder("CREATE TABLE ")
                .append(q(schema, table)).append(" (");
        List<String> defs = new ArrayList<>();
        for (Field f : fields) {
            defs.add(bracket(f.getName()) + " " + arrowTypeToSqlServer(f.getType()));
        }
        sb.append(String.join(", ", defs)).append(')');

        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
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
            stmt.executeUpdate("IF OBJECT_ID(N'" + literal(schema + "." + table)
                    + "', N'U') IS NOT NULL DROP TABLE " + q(schema, table));
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

    /**
     * 字符串列 DDL 类型：{@code NVARCHAR(MAX)}。
     *
     * <p>MAX 类型把数据存成行外 LOB，不占 8060 字节的行内上限，也不会因字符集
     * （单字节 vs Unicode）截断 —— 是 SQL Server 侧最安全的通用字符串落点。</p>
     */
    private static final VarcharMapping SQLSERVER_VARCHAR = new VarcharMapping() {
        @Override public String resolveVarcharType(String charset) { return "NVARCHAR(MAX)"; }
        @Override public int varcharMaxBytes(String charset) { return textRowBytes(); }
        @Override public int textRowBytes() { return 16; }
    };

    @Override
    public VarcharMapping varcharMapping() {
        return SQLSERVER_VARCHAR;
    }

    @Override
    public Optional<String> getDefaultCharset(String schema) {
        // SQL Server 没有库级「字符集」概念（由列类型与排序规则决定）→ 未知
        return Optional.empty();
    }

    /** Arrow 类型 → SQL Server 列类型。 */
    static String arrowTypeToSqlServer(ArrowType type) {
        if (type instanceof ArrowType.Int i) {
            return switch (i.getBitWidth()) {
                // 绝不落 TINYINT：SQL Server tinyint 是 0..255 无符号，装不下 Arrow int8 的负数
                case 8, 16 -> "SMALLINT";
                case 32 -> "INT";
                default -> "BIGINT";
            };
        }
        if (type instanceof ArrowType.Utf8 || type instanceof ArrowType.LargeUtf8) {
            return "NVARCHAR(MAX)";
        }
        if (type instanceof ArrowType.FloatingPoint fp) {
            return fp.getPrecision() == FloatingPointPrecision.SINGLE ? "REAL" : "FLOAT";
        }
        // DECIMAL 必须带 (p,s)：裸 "DECIMAL" 落成 DECIMAL(18,0)，会把小数位静默截断
        if (type instanceof ArrowType.Decimal d) {
            return "DECIMAL(" + d.getPrecision() + "," + d.getScale() + ")";
        }
        if (type instanceof ArrowType.Bool) return "BIT";
        if (type instanceof ArrowType.Date) return "DATE";
        if (type instanceof ArrowType.Time) return "TIME";
        if (type instanceof ArrowType.Timestamp) return "DATETIME2";
        if (type instanceof ArrowType.FixedSizeList || type instanceof ArrowType.List) {
            return "NVARCHAR(MAX)";
        }
        // 兜底：文本
        return "NVARCHAR(MAX)";
    }

    // ==================== INSERT OVERWRITE（OverwriteEnable） ====================
    //
    // SQL Server 没有 MySQL 的多表 RENAME，也没有 CREATE TABLE ... LIKE，因此：staging 用
    // SELECT TOP 0 * INTO 克隆结构 → 全部 N 分区写 staging → 成功后 sp_rename 目标→old、
    // staging→目标、再 DROP old。两条 RENAME 不是单语句原子，失败时尽力把 old 改回原名；
    // 但 staging 阶段目标表从未被触碰（全成或全不成）。

    @Override
    public boolean supportsOverwrite(String catalog, String schema, String table) {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String probe = table + "__dojao_probe_" + suffix;
        String renamed = table + "__dojao_probe_r_" + suffix;
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("SELECT TOP 0 * INTO " + q(schema, probe)
                    + " FROM " + q(schema, table));
            stmt.executeUpdate("EXEC sp_rename N'" + literal(schema + "." + probe)
                    + "', N'" + literal(renamed) + "'");
            stmt.executeUpdate("DROP TABLE " + q(schema, renamed));
            return true;
        } catch (SQLException e) {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("IF OBJECT_ID(N'" + literal(schema + "." + probe)
                        + "', N'U') IS NOT NULL DROP TABLE " + q(schema, probe));
                stmt.executeUpdate("IF OBJECT_ID(N'" + literal(schema + "." + renamed)
                        + "', N'U') IS NOT NULL DROP TABLE " + q(schema, renamed));
            } catch (SQLException ignored) {
            }
            return false;
        }
    }

    /** 建覆盖用 staging（{@code SELECT TOP 0 * INTO} 克隆列结构），返回 staging 表名。 */
    @Override
    public String beginOverwrite(String catalog, String schema, String table) {
        String staging = table + "__dojao_tmp_"
                + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("SELECT TOP 0 * INTO " + q(schema, staging)
                    + " FROM " + q(schema, table));
        } catch (SQLException e) {
            throw new RuntimeException("Failed to create staging table for INSERT OVERWRITE: "
                    + schema + "." + table, e);
        }
        return staging;
    }

    /**
     * 收齐全部 N 分区写入后切换：目标→old、staging→目标、DROP old。
     *
     * <p>第二步失败时尽力把 old 改回目标名（保证「失败后目标表还在」这一契约）。</p>
     */
    @Override
    public void commitOverwrite(String catalog, String schema, String table, String staging) {
        String old = table + "__dojao_old_"
                + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        boolean targetRenamed = false;
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("EXEC sp_rename N'" + literal(schema + "." + table)
                    + "', N'" + literal(old) + "'");
            targetRenamed = true;
            stmt.executeUpdate("EXEC sp_rename N'" + literal(schema + "." + staging)
                    + "', N'" + literal(table) + "'");
            stmt.executeUpdate("DROP TABLE " + q(schema, old));
        } catch (SQLException e) {
            if (targetRenamed) {
                try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate("EXEC sp_rename N'" + literal(schema + "." + old)
                            + "', N'" + literal(table) + "'");
                } catch (SQLException ignored) {
                }
            }
            throw new RuntimeException("Failed to commit INSERT OVERWRITE: "
                    + schema + "." + table, e);
        }
    }

    /** 覆盖失败/取消：删 staging（原表从未被触碰，保持原样）。幂等。 */
    @Override
    public void abortOverwrite(String catalog, String schema, String table, String staging) {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("IF OBJECT_ID(N'" + literal(schema + "." + staging)
                    + "', N'U') IS NOT NULL DROP TABLE " + q(schema, staging));
        } catch (SQLException e) {
            throw new RuntimeException("Failed to abort INSERT OVERWRITE staging: "
                    + schema + "." + staging, e);
        }
    }

    /** SQL 字符串字面量转义（sp_rename / OBJECT_ID 的参数）。 */
    static String literal(String value) {
        return value == null ? "" : value.replace("'", "''");
    }

    // ==================== 缓存管理 ====================

    /** 使元数据缓存失效（下次查询时重新从 SQL Server 加载）。 */
    public void invalidateCache() {
        this.cacheDirty = true;
        this.columnCache.clear();
        this.tinyintCache.clear();
    }

    // ==================== 私有方法 ====================

    /** 表注释（sys.extended_properties 的 MS_Description）。 */
    private String getTableComment(String schema, String table) {
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT CAST(ep.value AS NVARCHAR(4000)) FROM sys.tables t"
                     + " JOIN sys.schemas s ON t.schema_id = s.schema_id"
                     + " LEFT JOIN sys.extended_properties ep"
                     + "   ON ep.major_id = t.object_id AND ep.minor_id = 0"
                     + "  AND ep.name = 'MS_Description'"
                     + " WHERE s.name = ? AND t.name = ?")) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String comment = rs.getString(1);
                    return (comment != null && !comment.isEmpty()) ? comment : null;
                }
            }
        } catch (SQLException e) {
            // ignore
        }
        return null;
    }

    /** 列注释（一次查全表，避免逐列往返）。 */
    private Map<String, String> getColumnComments(String schema, String table) {
        Map<String, String> comments = new LinkedHashMap<>();
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT c.name, CAST(ep.value AS NVARCHAR(4000)) FROM sys.columns c"
                     + " JOIN sys.tables t ON c.object_id = t.object_id"
                     + " JOIN sys.schemas s ON t.schema_id = s.schema_id"
                     + " LEFT JOIN sys.extended_properties ep"
                     + "   ON ep.major_id = c.object_id AND ep.minor_id = c.column_id"
                     + "  AND ep.name = 'MS_Description'"
                     + " WHERE s.name = ? AND t.name = ?")) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String comment = rs.getString(2);
                    if (comment != null && !comment.isEmpty()) {
                        comments.put(rs.getString(1), comment);
                    }
                }
            }
        } catch (SQLException e) {
            // 注释是可选信息，读不到不影响元数据
        }
        return comments;
    }

    /** 表属性（行数快照等）。 */
    private Map<String, String> getTableProperties(String schema, String table) {
        Map<String, String> props = new LinkedHashMap<>();
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT SUM(CONVERT(BIGINT, p.rows)) FROM sys.tables t"
                     + " JOIN sys.schemas s ON t.schema_id = s.schema_id"
                     + " JOIN sys.partitions p ON t.object_id = p.object_id"
                     + "  AND p.index_id IN (0, 1)"
                     + " WHERE s.name = ? AND t.name = ?")) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    putIfNotNull(props, "row_count", rs.getString(1));
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
