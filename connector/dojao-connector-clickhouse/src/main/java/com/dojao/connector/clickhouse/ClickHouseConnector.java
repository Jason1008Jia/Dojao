package com.dojao.connector.clickhouse;

import com.dojao.common.config.CommonConfig;
import com.dojao.common.data.Page;
import com.dojao.common.memory.NodeMemory;
import com.dojao.spi.bean.*;
import com.dojao.spi.service.DojaoConnector;
import com.dojao.spi.service.OverwriteEnable;
import com.dojao.spi.util.JdbcArrowConverter;
import com.dojao.spi.util.JdbcSizeEstimator;
import com.dojao.spi.util.NestedTypeSupport;
import com.dojao.spi.util.SqlIdentifiers;
import com.dojao.spi.util.VarcharMapping;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ClickHouse 连接器，通过 ClickHouse JDBC 实现 {@link DojaoConnector} 接口。
 *
 * <p>层级映射：
 * <ul>
 *   <li>catalog → JDBC 连接 URL 对应的 ClickHouse 实例（可配置名称，默认 "clickhouse"）</li>
 *   <li>schema  → ClickHouse 的 DATABASE（system.databases）</li>
 *   <li>table   → ClickHouse 表（system.tables）</li>
 *   <li>column  → ClickHouse 列（system.columns，type 保留 Nullable(...) / Decimal(p, s) 等原始类型）</li>
 * </ul>
 *
 * <p>标识符一律用反引号转义（ClickHouse 支持反引号 / 双引号，反引号与 MySQL 风格一致）。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/1
 */
public class ClickHouseConnector implements DojaoConnector, OverwriteEnable {

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

    public ClickHouseConnector(String catalogName, String jdbcUrl, String username, String password) {
        this(catalogName, jdbcUrl, username, password,
                NodeMemory.shared().newChildAllocator("connector/clickhouse", 0));
    }

    public ClickHouseConnector(String catalogName, String jdbcUrl, String username, String password,
                               BufferAllocator allocator) {
        this.catalogName = catalogName;
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.allocator = allocator;
    }

    public ClickHouseConnector(String jdbcUrl, String username, String password) {
        this("clickhouse", jdbcUrl, username, password);
    }

    // ==================== Public getters ====================

    public String getCatalogName() { return catalogName; }
    public String getJdbcUrl() { return jdbcUrl; }
    public String getUsername() { return username; }
    public String getPassword() { return password; }

    /**
     * 独立副本：JDBC 游标状态挂在连接器实例上（{@link #read} 连续调用接着上次读），
     * 并行扫描必须每分区一个实例，因此覆写 SPI 的 {@link DojaoConnector#copy()}。
     */
    @Override
    public ClickHouseConnector copy() {
        return new ClickHouseConnector(catalogName, jdbcUrl, username, password,
                NodeMemory.shared().newChildAllocator("connector/clickhouse", 0));
    }

    // ==================== Connection ====================

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, username, password);
    }

    /**
     * 连接性探针：用当前 URL / 账号<b>真的建一次连接</b>（引擎在 CREATE CATALOG 落库前调用，
     * 连不上就不入库）。只做一次 {@code isValid} 握手，不去枚举元数据。
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
        return Optional.of(new Catalog(catalogName, "clickhouse", Map.of("url", jdbcUrl), null, null));
    }

    @Override
    public List<String> listCatalogs() {
        return List.of(catalogName);
    }

    // ==================== Schema（= ClickHouse DATABASE） ====================

    @Override
    public boolean schemaExists(String catalog, String schema) {
        if (!catalogExists(catalog)) return false;
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT 1 FROM system.databases WHERE name = ?")) {
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
                     "SELECT name FROM system.databases ORDER BY name")) {
            while (rs.next()) {
                schemas.add(rs.getString("name"));
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
                     "SELECT 1 FROM system.tables WHERE database = ? AND name = ?")) {
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
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT name, type, default_expression, comment"
                     + " FROM system.columns"
                     + " WHERE database = ? AND table = ?"
                     + " ORDER BY position")) {
            ps.setString(1, schema);
            ps.setString(2, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String name = rs.getString("name");
                    String dataType = rs.getString("type");       // 如 Nullable(Int32) / Decimal(10, 2)
                    String defaultValue = rs.getString("default_expression");
                    String comment = rs.getString("comment");
                    // ClickHouse 列类型以 Nullable(...) 标注可空；无 Nullable 包装即 NOT NULL
                    boolean nullable = dataType != null && dataType.startsWith("Nullable(");

                    columns.add(new Column(
                            name, dataType,
                            comment != null && !comment.isEmpty() ? comment : null,
                            defaultValue,
                            nullable
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

    private static final Map<String, DojaoType> CH_TYPE_MAPPING = new LinkedHashMap<>();

    static {
        // 布尔
        CH_TYPE_MAPPING.put("bool", DojaoType.BOOLEAN);
        CH_TYPE_MAPPING.put("boolean", DojaoType.BOOLEAN);
        // 有符号整数
        CH_TYPE_MAPPING.put("int8", DojaoType.TINYINT);
        CH_TYPE_MAPPING.put("int16", DojaoType.SMALLINT);
        CH_TYPE_MAPPING.put("int32", DojaoType.INT);
        CH_TYPE_MAPPING.put("int64", DojaoType.BIGINT);
        CH_TYPE_MAPPING.put("int128", DojaoType.BIGINT);
        CH_TYPE_MAPPING.put("int256", DojaoType.BIGINT);
        // 无符号整数（按位宽升级到有符号，避免溢出）
        CH_TYPE_MAPPING.put("uint8", DojaoType.SMALLINT);
        CH_TYPE_MAPPING.put("uint16", DojaoType.INT);
        CH_TYPE_MAPPING.put("uint32", DojaoType.BIGINT);
        CH_TYPE_MAPPING.put("uint64", DojaoType.BIGINT);
        CH_TYPE_MAPPING.put("uint128", DojaoType.BIGINT);
        CH_TYPE_MAPPING.put("uint256", DojaoType.BIGINT);
        // 浮点
        CH_TYPE_MAPPING.put("float32", DojaoType.FLOAT);
        CH_TYPE_MAPPING.put("float64", DojaoType.DOUBLE);
        // 定点
        CH_TYPE_MAPPING.put("decimal", DojaoType.DECIMAL);
        CH_TYPE_MAPPING.put("decimal32", DojaoType.DECIMAL);
        CH_TYPE_MAPPING.put("decimal64", DojaoType.DECIMAL);
        CH_TYPE_MAPPING.put("decimal128", DojaoType.DECIMAL);
        CH_TYPE_MAPPING.put("decimal256", DojaoType.DECIMAL);
        // 字符串
        CH_TYPE_MAPPING.put("string", DojaoType.STRING);
        CH_TYPE_MAPPING.put("fixedstring", DojaoType.STRING);
        CH_TYPE_MAPPING.put("uuid", DojaoType.STRING);
        CH_TYPE_MAPPING.put("ipv4", DojaoType.STRING);
        CH_TYPE_MAPPING.put("ipv6", DojaoType.STRING);
        CH_TYPE_MAPPING.put("enum8", DojaoType.STRING);
        CH_TYPE_MAPPING.put("enum16", DojaoType.STRING);
        // 日期/时间
        CH_TYPE_MAPPING.put("date", DojaoType.DATE);
        CH_TYPE_MAPPING.put("date32", DojaoType.DATE);
        CH_TYPE_MAPPING.put("datetime", DojaoType.DATETIME);
        CH_TYPE_MAPPING.put("datetime32", DojaoType.DATETIME);
        CH_TYPE_MAPPING.put("datetime64", DojaoType.TIMESTAMP);
        // 复合 / 半结构化
        CH_TYPE_MAPPING.put("array", DojaoType.LIST);
        CH_TYPE_MAPPING.put("map", DojaoType.MAP);
        CH_TYPE_MAPPING.put("tuple", DojaoType.STRING);
        CH_TYPE_MAPPING.put("object", DojaoType.JSON);
        CH_TYPE_MAPPING.put("json", DojaoType.JSON);
    }

    @Override
    public Map<String, DojaoType> getColumnTypes(String schema, String table) {
        Map<String, DojaoType> result = new LinkedHashMap<>();
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT name, type FROM system.columns"
                     + " WHERE database = ? AND table = ?"
                     + " ORDER BY position")) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String colName = rs.getString("name");
                    DojaoType dojaoType = mapChType(rs.getString("type"));
                    result.put(colName, dojaoType);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(
                    "Failed to get column types for: " + schema + "." + table, e);
        }
        return result;
    }

    /**
     * ClickHouse 原生类型 → Dojao 类型。先剥掉 {@code Nullable(...)} / {@code LowCardinality(...)}
     * 包装，再取外层类型名的基数（如 {@code Decimal(10, 2)} → {@code decimal}）。
     */
    static DojaoType mapChType(String chType) {
        if (chType == null) return DojaoType.STRING;
        String t = chType.trim();
        // 去皮：Nullable(...) / LowCardinality(...)（可能嵌套）
        boolean peeled = true;
        while (peeled) {
            peeled = false;
            for (String wrapper : new String[]{"Nullable(", "LowCardinality("}) {
                if (t.startsWith(wrapper) && t.endsWith(")")) {
                    t = t.substring(wrapper.length(), t.length() - 1).trim();
                    peeled = true;
                }
            }
        }
        String base = t.toLowerCase(Locale.ROOT);
        int paren = base.indexOf('(');
        if (paren > 0) base = base.substring(0, paren).trim();
        return CH_TYPE_MAPPING.getOrDefault(base, DojaoType.STRING);
    }

    @Override
    public List<String> listTables(String catalog, String schema) {
        if (!catalogExists(catalog)) return List.of();
        List<String> tables = new ArrayList<>();
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT name FROM system.tables"
                     + " WHERE database = ? AND is_temporary = 0"
                     + "   AND engine NOT IN ('View', 'MaterializedView', 'Dictionary')"
                     + " ORDER BY name")) {
            ps.setString(1, schema);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tables.add(rs.getString("name"));
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
                     "SELECT 1 FROM system.columns"
                     + " WHERE database = ? AND table = ? AND name = ?")) {
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
     * 来自 {@code system.tables} 的 {@code total_rows/total_bytes}（MergeTree 等引擎的统计）。
     * <b>权限不足 / 查不到 → 返回 {@code [-1,-1]}，不抛异常</b>，交由三级降级继续走 EXPLAIN + 采样。
     */
    private long[] fetchRawStats(String schema, String table) {
        long rowCount = -1;
        long byteSize = -1;
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT total_rows, total_bytes FROM system.tables"
                     + " WHERE database = ? AND name = ?")) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    long rows = rs.getLong("total_rows");
                    rowCount = rs.wasNull() || rows < 0 ? -1 : rows;
                    long bytes = rs.getLong("total_bytes");
                    byteSize = rs.wasNull() || bytes < 0 ? -1 : bytes;
                }
            }
        } catch (SQLException e) {
            // 权限不足（读不到 system.tables）/ 元数据不可读 → 未知，交给三级降级
        }
        return new long[]{rowCount, byteSize};
    }

    /**
     * 方言 EXPLAIN 行数估算：{@code EXPLAIN ESTIMATE SELECT * FROM t [WHERE filter]}，结果集带
     * {@code rows} 列（预估要读的行数估算）。
     *
     * <p><b>EXPLAIN 不可用（老版本不支持 {@code EXPLAIN ESTIMATE}）/ 取不到 / 抛异常 → 返回
     * {@code -1}</b>，绝不因估算失败而报错。</p>
     */
    private long explainRows(String schema, String table, String filter) {
        String sql = "EXPLAIN ESTIMATE SELECT * FROM " + quoteIdent(schema) + "." + quoteIdent(table)
                + ((filter != null && !filter.isBlank()) ? " WHERE " + filter : "");
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                long rows = rs.getLong("rows");
                return (!rs.wasNull() && rows > 0) ? rows : -1L;
            }
        } catch (Exception e) {
            // EXPLAIN ESTIMATE 不可用（版本 / 权限 / 语法）→ 未知
        }
        return -1L;
    }

    /**
     * 过滤感知的<b>字节</b>估算（三级降级）：① 元数据体积（有过滤则按选择率缩放）→
     * ② EXPLAIN 行数 × 采样平均行长 → ③ 未知（{@code -1}，调用方退回最保守的排序归并）。
     *
     * <p>见 {@link JdbcSizeEstimator#estimate}。账号读不到 {@code system.tables} 时（无权限），
     * 退用 {@code EXPLAIN ESTIMATE} 估行数 × 采样几行求平均行长。</p>
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

    @Override
    public List<ColumnStats> getColumnStats(String catalog, String schema, String table) {
        if (!catalogExists(catalog)) return List.of();
        List<ColumnStats> statsList = new ArrayList<>();
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT name, type, data_compressed_bytes FROM system.columns"
                     + " WHERE database = ? AND table = ?"
                     + " ORDER BY position")) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String colName = rs.getString("name");
                    String type = rs.getString("type");
                    boolean nullable = type != null && type.startsWith("Nullable(");
                    statsList.add(new ColumnStats(colName, -1, -1,
                            Optional.empty(), Optional.empty(), Optional.empty()));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get column stats: " + schema + "." + table, e);
        }
        return statsList;
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
                // 正数 fetchSize → ClickHouse JDBC 走流式结果集（边读边拉，不全量物化）
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

    /** 列裁剪：ClickHouse 方言对列名做反引号转义，防止保留字/特殊字符破坏 SELECT 列清单。 */
    @Override
    public ReadRequest pruneColumns(ReadRequest request, List<String> columns) {
        if (columns == null || columns.isEmpty()) return request;
        List<String> escaped = new ArrayList<>(columns.size());
        for (String col : columns) {
            escaped.add(quoteIdent(col));
        }
        return request.withColumns(escaped);
    }

    /** 谓词下推：ClickHouse 方言原样透传（buildReadSql 直接拼进 WHERE）。 */
    @Override
    public ReadRequest pushdownPredicate(ReadRequest request, String predicate) {
        if (predicate == null || predicate.isBlank()) return request;
        String merged = request.filterExpression()
                .map(f -> "(" + f + ") AND (" + predicate + ")")
                .orElse(predicate);
        return request.withFilter(merged);
    }

    /** 谓词下推：ClickHouse 方言原样透传（buildReadSql 直接拼进 WHERE）。 */
    @Override
    public boolean supportsPredicatePushdown() {
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
        sql.append(" FROM ").append(quoteIdent(request.schema()))
                .append(".").append(quoteIdent(request.table()));

        request.filterExpression().ifPresent(filter -> sql.append(" WHERE ").append(filter));

        if (request.limit() > 0) {
            sql.append(" LIMIT ").append(request.limit());
        }
        if (request.offset() > 0) {
            sql.append(" OFFSET ").append(request.offset());
        }
        return sql.toString();
    }

    /** ClickHouse 标识符反引号转义（反引号自身翻倍）。 */
    private String quoteIdent(String ident) {
        // 幂等：入参可能已是转义形态（ReadRequest.columns() 复用）→ 先剥引号再转义
        return "`" + SqlIdentifiers.unquote(ident).replace("`", "``") + "`";
    }

    // ==================== CTAS 源列类型（读方向精确映射） ====================

    /**
     * CTAS 源列类型映射：对指定列集跑一条零行探针
     * {@code SELECT <cols> FROM 表 LIMIT 0}，取 JDBC ResultSet 元数据 → ，
     * 与运行期 {@link #read(ReadRequest)} 走同一条 JDBC→Arrow 映射，保证 Master 按此建出的目标表装得下数据。
     */
    @Override
    public List<Field> sourceFields(String catalog, String schema, String table, List<String> columns) {
        List<String> cols = (columns == null || columns.isEmpty())
                ? getTableColumns(catalog, schema, table).stream().map(Column::name).toList()
                : columns;
        if (cols.isEmpty()) {
            return List.of();
        }
        StringBuilder selectList = new StringBuilder();
        for (int i = 0; i < cols.size(); i++) {
            if (i > 0) selectList.append(", ");
            selectList.append(quoteIdent(cols.get(i)));
        }
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT " + selectList + " FROM "
                     + quoteIdent(schema) + "." + quoteIdent(table) + " LIMIT 0")) {
            try (ResultSet rs = ps.executeQuery()) {
                return JdbcArrowConverter.buildSchema(rs.getMetaData()).getFields();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to read source column metadata for CTAS: "
                    + schema + "." + table, e);
        }
    }

    @Override
    public VarcharMapping varcharMapping() {
        return VarcharMapping.fallback();
    }

    // ==================== 分区探针（平行 TableScan 用） ====================

    /**
     * 取表可用的范围分区键：读取 {@code system.tables.primary_key}，仅当其为<b>单一裸数值列</b>
     * （无逗号 / 括号 / 表达式）时返回。ClickHouse 的 MergeTree 主键常是 {@code tuple()}（空）或
     * 复合表达式，此类一律返回空，交由下游兜底。
     */
    @Override
    public Optional<String> getPartitionKey(String schema, String table) {
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT primary_key FROM system.tables WHERE database = ? AND name = ?")) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                String pk = rs.getString(1);
                if (pk == null) return Optional.empty();
                pk = pk.trim();
                if (pk.isEmpty() || pk.indexOf(',') >= 0 || pk.indexOf('(') >= 0
                        || pk.indexOf(' ') >= 0) {
                    return Optional.empty();
                }
                return isNumericColumn(schema, table, pk) ? Optional.of(pk) : Optional.empty();
            }
        } catch (SQLException e) {
            return Optional.empty();
        }
    }

    /** 精确行数（SELECT count()）。 */
    @Override
    public long getExactRowCount(String schema, String table) {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT count() FROM " + quoteIdent(schema) + "." + quoteIdent(table))) {
            if (rs.next()) return rs.getLong(1);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to count rows: " + schema + "." + table, e);
        }
        return 0;
    }

    /** 分区键列的最小/最大值（用于范围分区）。 */
    @Override
    public long[] getPartitionKeyRange(String schema, String table, String pkColumn) {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT MIN(" + quoteIdent(pkColumn) + "), MAX(" + quoteIdent(pkColumn) + ")"
                     + " FROM " + quoteIdent(schema) + "." + quoteIdent(table))) {
            if (rs.next()) {
                return new long[]{rs.getLong(1), rs.getLong(2)};
            }
        } catch (SQLException e) {
            throw new RuntimeException(
                    "Failed to get range for: " + schema + "." + table + "." + pkColumn, e);
        }
        return new long[]{0, 0};
    }

    private boolean isNumericColumn(String schema, String table, String column) {
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT type FROM system.columns"
                     + " WHERE database = ? AND table = ? AND name = ?")) {
            ps.setString(1, schema);
            ps.setString(2, table);
            ps.setString(3, column);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return false;
                return isNumericType(rs.getString(1));
            }
        } catch (SQLException e) {
            return false;
        }
    }

    /** 是否数值类型（剥掉 Nullable(...) 包装后按前缀判定）。 */
    private static boolean isNumericType(String chType) {
        if (chType == null) return false;
        String t = chType.trim();
        while (t.startsWith("Nullable(") && t.endsWith(")")) {
            t = t.substring("Nullable(".length(), t.length() - 1).trim();
        }
        String base = t.toLowerCase(Locale.ROOT);
        int paren = base.indexOf('(');
        if (paren > 0) base = base.substring(0, paren).trim();
        return base.startsWith("int") || base.startsWith("uint")
                || base.startsWith("float") || base.startsWith("decimal")
                || base.equals("bool") || base.equals("boolean");
    }

    // ==================== 并行扫描分区（主键 → 数值列 → LIMIT/OFFSET 兜底） ====================

    /**
     * 分区策略：主键范围分区优先；无主键时依次回退
     * <ol>
     *   <li>数值列范围分区：找第一个数值列（优先 NOT NULL），MIN/MAX 范围均分；</li>
     *   <li>LIMIT/OFFSET 均分：无任何数值列的最后兜底。</li>
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

    /** 找第一个数值列：优先 NOT NULL（type 无 Nullable(...) 包装，最接近隐式主键），否则任意数值列。 */
    private Optional<String> findNumericColumn(String schema, String table) {
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT name, type FROM system.columns"
                     + " WHERE database = ? AND table = ?"
                     + " ORDER BY position")) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                String firstNumeric = null;
                while (rs.next()) {
                    String type = rs.getString("type");
                    if (!isNumericType(type)) continue;
                    String col = rs.getString("name");
                    if (type != null && !type.startsWith("Nullable(")) return Optional.of(col);
                    if (firstNumeric == null) firstNumeric = col;
                }
                return firstNumeric == null ? Optional.empty() : Optional.of(firstNumeric);
            }
        } catch (SQLException e) {
            return Optional.empty();
        }
    }

    /** 兜底 2：LIMIT/OFFSET 均分（COUNT 带过滤条件，否则过滤后行数偏小导致漏数据）。 */
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
        StringBuilder sql = new StringBuilder("SELECT count() FROM ")
                .append(quoteIdent(request.schema())).append(".").append(quoteIdent(request.table()));
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
            cols = targets.stream().map(this::quoteIdent).toList();
        } else {
            cols = new ArrayList<>();
            for (var field : root.getSchema().getFields()) {
                cols.add(quoteIdent(field.getName()));
            }
        }
        String placeholders = String.join(", ", Collections.nCopies(colCount, "?"));
        String sql = "INSERT INTO " + quoteIdent(request.schema()) + "." + quoteIdent(request.table())
                + " (" + String.join(", ", cols) + ") VALUES (" + placeholders + ")";

        try {
            PreparedStatement ps = session(sql).ps;   // 持久会话：复用连接 + PreparedStatement
            int rowCount = root.getRowCount();
            int batchSize = Math.max(1, CommonConfig.JDBC_WRITE_BATCH_SIZE);   // 分块行数
            for (int row = 0; row < rowCount; row++) {
                for (int col = 0; col < colCount; col++) {
                    FieldVector vector = root.getVector(col);
                    Object value;
                    if (vector instanceof StructVector) {
                        // 读侧把 Tuple 渲染成字符串，不会产出 struct 向量；来自 Hive 的 STRUCT
                        // 没有对应的 setObject 表达方式 → 显式拒绝，不静默写成 String
                        throw new UnsupportedOperationException(
                                "ClickHouse 写入暂不支持 STRUCT 类型列 '"
                                        + vector.getField().getName() + "'");
                    } else if (vector instanceof ListVector || vector instanceof MapVector) {
                        // Array / Map：转成 Java 数组 / LinkedHashMap，由 clickhouse-jdbc 递归渲染
                        value = NestedTypeSupport.toJavaValue(vector, row);
                    } else {
                        value = vector.getObject(row);
                    }
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

    // ==================== 建表（CTAS）====================

    @Override
    public boolean createTable(String catalog, String schema, String table,
                               List<Field> fields, boolean ifNotExists) {
        // 表已存在：IF NOT EXISTS → 跳过建表（返回 false，调用方不写入）；否则让下方 DDL 抛出标准错误
        if (tableExists(catalog, schema, table)) {
            if (ifNotExists) return false;
        }
        StringBuilder sb = new StringBuilder("CREATE TABLE ");
        if (ifNotExists) sb.append("IF NOT EXISTS ");
        sb.append(quoteIdent(schema)).append('.').append(quoteIdent(table)).append(" (");
        List<String> defs = new ArrayList<>();
        for (Field f : fields) {
            defs.add(quoteIdent(f.getName()) + " " + arrowTypeToClickHouse(f));
        }
        // MergeTree 必须带 ORDER BY；无排序键用 tuple()（空排序键）
        sb.append(String.join(", ", defs)).append(") ENGINE = MergeTree ORDER BY tuple()");

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

    /**
     * Arrow 字段 → ClickHouse 列类型（含嵌套：{@code Array(T)} / {@code Map(K, V)}）。
     *
     * <p>CTAS 建表必须按嵌套结构声明真实类型：旧实现把 List / Map 统一写成 {@code String}，
     * 后续 INSERT 传 {@code Object[]} 会被驱动拒绝，显式透传又会被当成字符串字面量。</p>
     *
     * <p>元素 / Map 值按可空性包 {@code Nullable(...)}（源列允许 NULL 时才是可空列，否则 CH
     * 会拒绝 NULL）；Map 的 key 生成不允许 Nullable，故不包。</p>
     */
    static String arrowTypeToClickHouse(Field field) {
        ArrowType type = field.getType();
        List<Field> children = field.getChildren();
        if (type instanceof ArrowType.List || type instanceof ArrowType.FixedSizeList) {
            String element = children.isEmpty() ? "String" : nullableIf(children.get(0));
            return "Array(" + element + ")";
        }
        if (type instanceof ArrowType.Map) {
            List<Field> kv = children.isEmpty() ? List.of() : children.get(0).getChildren();
            String key = kv.isEmpty() ? "String" : arrowTypeToClickHouse(kv.get(0));
            String value = kv.size() < 2 ? "String" : nullableIf(kv.get(1));
            return "Map(" + key + ", " + value + ")";
        }
        if (type instanceof ArrowType.Struct) {
            throw new UnsupportedOperationException("ClickHouse 暂不支持 STRUCT 类型列 '"
                    + field.getName() + "'（ClickHouse 无对应 Arrow Struct 的列类型）");
        }
        return arrowTypeToClickHouse(type);
    }

    /**
     * 可空字段包 {@code Nullable(...)}（仅用于嵌套元素 / Map 值，顶层列保持旧口径）。
     *
     * <p>★ClickHouse 不允许 {@code Nullable(Array(...))} / {@code Nullable(Map(...))}
     * （"Nested type cannot be inside Nullable type"），故容器子字段一律不包；
     * 可空的数组元素请用 {@code Array(Nullable(T))} 表达。</p>
     */
    private static String nullableIf(Field child) {
        String type = arrowTypeToClickHouse(child);
        ArrowType t = child.getType();
        boolean container = t instanceof ArrowType.List
                || t instanceof ArrowType.FixedSizeList
                || t instanceof ArrowType.Map;
        return child.isNullable() && !container ? "Nullable(" + type + ")" : type;
    }

    /** Arrow 类型 → ClickHouse 列类型（标量）。 */
    private static String arrowTypeToClickHouse(ArrowType type) {
        if (type instanceof ArrowType.Int i) {
            return switch (i.getBitWidth()) {
                case 8 -> "Int8";
                case 16 -> "Int16";
                case 32 -> "Int32";
                default -> "Int64";
            };
        }
        if (type instanceof ArrowType.Utf8) return "String";
        if (type instanceof ArrowType.LargeUtf8) return "String";
        if (type instanceof ArrowType.FloatingPoint fp) {
            return fp.getPrecision() == FloatingPointPrecision.SINGLE ? "Float32" : "Float64";
        }
        // DECIMAL 必须带 (p,s)：裸 "Decimal" 会落成 Decimal(10,0)，静默截断小数位
        if (type instanceof ArrowType.Decimal d) {
            return "Decimal(" + d.getPrecision() + ", " + d.getScale() + ")";
        }
        if (type instanceof ArrowType.Bool) return "Bool";
        if (type instanceof ArrowType.Date) return "Date";
        if (type instanceof ArrowType.Timestamp || type instanceof ArrowType.Time) {
            return "DateTime64(3)";
        }
        // 复合类型（List / FixedSizeList / Map 等）：以 String 兜底承载
        return "String";
    }

    // ==================== INSERT OVERWRITE（实现 OverwriteEnable） ====================
    //
    // 与 MySQL 同款形态：建 staging（克隆目标表结构）→ N 个分区都写 staging →
    // 全部成功后再一次性把 staging 换成目标表（ClickHouse 多目标 RENAME 对 Atomic 数据库原子生效）；
    // 任一分区失败则 DROP staging，目标表从未被触碰 —— 天然全成或全不成。

    /**
     * 权限探针：真实执行覆盖所需的两类操作（CREATE / DROP）各一次，全程只碰 throwaway 探针表、
     * 不触碰目标表；失败 → 清理残留并返回 false（Master 报「暂不支持」）。
     */
    @Override
    public boolean supportsOverwrite(String catalog, String schema, String table) {
        String uuid = UUID.randomUUID().toString().replace("-", "");
        String probe = q(schema, table + "__dojao_probe_" + uuid);
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE TABLE " + probe + " AS " + q(schema, table));
            stmt.executeUpdate("DROP TABLE " + probe);
            return true;
        } catch (SQLException e) {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("DROP TABLE IF EXISTS " + probe);
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
            stmt.executeUpdate("CREATE TABLE " + q(schema, staging) + " AS " + q(schema, table));
        } catch (SQLException e) {
            throw new RuntimeException("Failed to create staging table for INSERT OVERWRITE: "
                    + schema + "." + table, e);
        }
        return staging;
    }

    /**
     * 收齐全部 N 分区写入后，把 staging 原子切换成目标表：
     * {@code target → target__dojao_old_<uuid>, staging → target}，随后 DROP 旧表。
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
        return "`" + schema + "`.`" + table + "`";
    }

    // ==================== 缓存管理 ====================

    /** 使元数据缓存失效（下次查询时重新从 ClickHouse 加载）。 */
    public void invalidateCache() {
        this.cacheDirty = true;
        this.tableCache.clear();
        this.columnCache.clear();
    }

    // ==================== 私有方法 ====================

    /** 获取表的 COMMENT（system.tables.comment）。 */
    private String getTableComment(String schema, String table) {
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT comment FROM system.tables WHERE database = ? AND name = ?")) {
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

    /** 获取表属性（ClickHouse 的 engine / total_rows 等）。 */
    private Map<String, String> getTableProperties(String schema, String table) {
        Map<String, String> props = new LinkedHashMap<>();
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT engine, total_rows, total_bytes, primary_key"
                     + " FROM system.tables WHERE database = ? AND name = ?")) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    putIfNotNull(props, "engine", rs.getString("engine"));
                    putIfNotNull(props, "total_rows", rs.getString("total_rows"));
                    putIfNotNull(props, "total_bytes", rs.getString("total_bytes"));
                    putIfNotNull(props, "primary_key", rs.getString("primary_key"));
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
