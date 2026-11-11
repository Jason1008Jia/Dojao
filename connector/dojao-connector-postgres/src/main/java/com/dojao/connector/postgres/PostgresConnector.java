package com.dojao.connector.postgres;

import com.dojao.common.config.CommonConfig;
import com.dojao.common.data.Page;
import com.dojao.common.memory.NodeMemory;
import com.dojao.spi.bean.*;
import com.dojao.spi.service.DojaoConnector;
import com.dojao.spi.util.ArrowJdbcValues;
import com.dojao.spi.util.JdbcArrowConverter;
import com.dojao.spi.util.JdbcSizeEstimator;
import com.dojao.spi.util.NestedTypeSupport;
import com.dojao.spi.util.SqlIdentifiers;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * PostgreSQL 连接器，通过 JDBC 实现 {@link DojaoConnector} 接口。
 *
 * <p>三层映射（与 MySQL 连接器区分）：
 * <ul>
 *   <li>catalog → JDBC 连接 URL 对应的 PostgreSQL 实例（默认名称 "postgresql"）</li>
 *   <li>schema  → PostgreSQL 的 SCHEMA（public / 自定义 schema，information_schema.schemata）</li>
 *   <li>table   → PostgreSQL 表（information_schema.tables）</li>
 *   <li>column  → PostgreSQL 列（information_schema.columns，udt_name 保留 vector 等类型名）</li>
 * </ul>
 *
 * <p>pgvector 支持：`vector` 列通过 {@link JdbcArrowConverter} 映射为
 * Arrow FixedSizeList&lt;Float32&gt;，维度由数据自动解析。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/8
 */
public class PostgresConnector implements DojaoConnector {

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

    public PostgresConnector(String catalogName, String jdbcUrl, String username, String password) {
        this(catalogName, jdbcUrl, username, password,
                NodeMemory.shared().newChildAllocator("connector/postgres", 0));
    }

    public PostgresConnector(String catalogName, String jdbcUrl, String username, String password,
                             BufferAllocator allocator) {
        this.catalogName = catalogName;
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.allocator = allocator;
    }

    public PostgresConnector(String jdbcUrl, String username, String password) {
        this("postgresql", jdbcUrl, username, password);
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
    public PostgresConnector copy() {
        return new PostgresConnector(catalogName, jdbcUrl, username, password,
                NodeMemory.shared().newChildAllocator("connector/postgres", 0));
    }

    // ==================== Connection ====================

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, username, password);
    }

    /**
     * 连接性探针：用当前 URL / 账号<b>真的建一次连接</b>（引擎在 CREATE CATALOG 落库前调用，
     * 连不上就不入库）。与 MySQL 连接器同款：只做一次 {@code isValid} 握手，不去枚举元数据
     * （避免把「连得上但没权限」误报成连接失败）。
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
        return Optional.of(new Catalog(catalogName, "postgresql", Map.of("url", jdbcUrl), null, null));
    }

    @Override
    public List<String> listCatalogs() {
        return List.of(catalogName);
    }

    // ==================== Schema ====================

    @Override
    public boolean schemaExists(String catalog, String schema) {
        if (!catalogExists(catalog)) return false;
        try (Connection conn = getConnection()) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT 1 FROM information_schema.schemata WHERE schema_name = ?")) {
                ps.setString(1, schema);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next();
                }
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
                     "SELECT schema_name FROM information_schema.schemata"
                     + " WHERE schema_name NOT LIKE 'pg\\_%' AND schema_name <> 'information_schema'"
                     + " ORDER BY schema_name")) {
            while (rs.next()) {
                schemas.add(rs.getString("schema_name"));
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
                    "SELECT 1 FROM information_schema.tables"
                    + " WHERE table_schema = ? AND table_name = ?")) {
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
                    "SELECT column_name, udt_name, column_default, is_nullable"
                    + " FROM information_schema.columns"
                    + " WHERE table_schema = ? AND table_name = ?"
                    + " ORDER BY ordinal_position")) {
                ps.setString(1, schema);
                ps.setString(2, tableName);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String name = rs.getString("column_name");
                        String dataType = rs.getString("udt_name"); // int8 / text / vector ...
                        String defaultValue = rs.getString("column_default");
                        boolean nullable = "YES".equalsIgnoreCase(
                                rs.getString("is_nullable"));

                        columns.add(new Column(
                                name, dataType,
                                null, // PG 列注释暂不取
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

    private static final Map<String, DojaoType> PG_TYPE_MAPPING = new LinkedHashMap<>();

    static {
        // 布尔
        PG_TYPE_MAPPING.put("bool", DojaoType.BOOLEAN);
        // 整数
        PG_TYPE_MAPPING.put("int2", DojaoType.SMALLINT);
        PG_TYPE_MAPPING.put("int4", DojaoType.INT);
        PG_TYPE_MAPPING.put("int8", DojaoType.BIGINT);
        // 浮点
        PG_TYPE_MAPPING.put("float4", DojaoType.FLOAT);
        PG_TYPE_MAPPING.put("float8", DojaoType.DOUBLE);
        // 定点
        PG_TYPE_MAPPING.put("numeric", DojaoType.DECIMAL);
        PG_TYPE_MAPPING.put("money", DojaoType.DECIMAL);
        // 字符串
        PG_TYPE_MAPPING.put("varchar", DojaoType.STRING);
        PG_TYPE_MAPPING.put("char", DojaoType.STRING);
        PG_TYPE_MAPPING.put("bpchar", DojaoType.STRING);
        PG_TYPE_MAPPING.put("text", DojaoType.STRING);
        PG_TYPE_MAPPING.put("name", DojaoType.STRING);
        PG_TYPE_MAPPING.put("uuid", DojaoType.STRING);
        // 日期/时间
        PG_TYPE_MAPPING.put("date", DojaoType.DATE);
        PG_TYPE_MAPPING.put("time", DojaoType.STRING);
        PG_TYPE_MAPPING.put("timetz", DojaoType.STRING);
        PG_TYPE_MAPPING.put("timestamp", DojaoType.DATETIME);
        PG_TYPE_MAPPING.put("timestamptz", DojaoType.TIMESTAMP_TZ);
        // 半结构化 / 二进制
        PG_TYPE_MAPPING.put("json", DojaoType.JSON);
        PG_TYPE_MAPPING.put("jsonb", DojaoType.JSON);
        PG_TYPE_MAPPING.put("bytea", DojaoType.STRING);
        // 向量（pgvector）
        PG_TYPE_MAPPING.put("vector", DojaoType.VECTOR);
        PG_TYPE_MAPPING.put("halfvec", DojaoType.VECTOR);
    }

    @Override
    public Map<String, DojaoType> getColumnTypes(String schema, String table) {
        Map<String, DojaoType> result = new LinkedHashMap<>();
        try (Connection conn = getConnection()) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT column_name, udt_name"
                    + " FROM information_schema.columns"
                    + " WHERE table_schema = ? AND table_name = ?"
                    + " ORDER BY ordinal_position")) {
                ps.setString(1, schema);
                ps.setString(2, table);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String colName = rs.getString("column_name");
                        String udtName = rs.getString("udt_name").toLowerCase(Locale.ROOT);
                        DojaoType dojaoType = PG_TYPE_MAPPING.getOrDefault(udtName,
                                udtName.startsWith("_") ? DojaoType.LIST : DojaoType.STRING);
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
     * 声明列类型 —— 用 {@code SELECT ... LIMIT 0} 让驱动自己报出类型名。
     *
     * <p>必须走这条路（而不是 {@code DojaoTypeMapper}）：PG 数组的真实元素类型只存在于
     * {@code udt_name}（{@code _int4} / {@code _text}…），按 {@code DojaoType.LIST} 映射
     * 只能得到文本占位，和运行期读出的 {@code List<...>} 对不上 —— 声明类型与实际类型不一致
     * 会让上层按错误宽度取值（历史 P0 的同类问题）。</p>
     */
    @Override
    public List<Field> sourceFields(String catalog, String schema, String table,
                                    List<String> columns) {
        List<String> cols = (columns == null || columns.isEmpty())
                ? getTableColumns(catalog, schema, table).stream().map(Column::name).toList()
                : SqlIdentifiers.unquoteAll(columns);
        if (cols.isEmpty()) return List.of();
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
            throw new RuntimeException("Failed to read source column metadata: "
                    + schema + "." + table, e);
        }
    }

    @Override
    public List<String> listTables(String catalog, String schema) {
        if (!catalogExists(catalog)) return List.of();
        List<String> tables = new ArrayList<>();
        try (Connection conn = getConnection()) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT table_name FROM information_schema.tables"
                    + " WHERE table_schema = ? AND table_type = 'BASE TABLE'"
                    + " ORDER BY table_name")) {
                ps.setString(1, schema);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        tables.add(rs.getString("table_name"));
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
                    "SELECT 1 FROM information_schema.columns"
                    + " WHERE table_schema = ? AND table_name = ? AND column_name = ?")) {
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
     * {@code reltuples}/{@code relpages} 来自 {@code pg_class}（ANALYZE 后的估算快照）。
     * <b>权限不足 / 查不到 → 返回 {@code [-1,-1]}，不抛异常</b>，交由三级降级继续走 EXPLAIN + 采样。
     */
    private long[] fetchRawStats(String schema, String table) {
        long rowCount = -1;
        long byteSize = -1;
        try (Connection conn = getConnection()) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT c.reltuples, c.relpages FROM pg_class c"
                    + " JOIN pg_namespace n ON c.relnamespace = n.oid"
                    + " WHERE n.nspname = ? AND c.relname = ?")) {
                ps.setString(1, schema);
                ps.setString(2, table);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        double reltuples = rs.getDouble("reltuples");
                        rowCount = reltuples > 0 ? (long) reltuples : -1;
                        byteSize = rs.getLong("relpages") * 8192L;
                    }
                }
            }
        } catch (SQLException e) {
            // 权限不足 / 元数据不可读 → 未知，交给三级降级
        }
        return new long[]{rowCount, byteSize};
    }

    /**
     * 方言 EXPLAIN 行数估算：对 {@code EXPLAIN SELECT * FROM t [WHERE filter]} 的<b>文本计划</b>
     * 取首条 {@code rows=N}（顶层节点的总行数估算）；无谓词 → 整表估算。
     *
     * <p><b>EXPLAIN 不可用 / 解析不到 / 抛异常 → 返回 {@code -1}</b>，绝不因估算失败而报错。</p>
     */
    private long explainRows(String schema, String table, String filter) {
        String sql = "EXPLAIN SELECT * FROM " + quoteIdent(schema) + "." + quoteIdent(table)
                + ((filter != null && !filter.isBlank()) ? " WHERE " + filter : "");
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            StringBuilder plan = new StringBuilder();
            while (rs.next()) {
                plan.append(rs.getString(1)).append('\n');
            }
            Matcher m = Pattern.compile("rows=(\\d+)").matcher(plan);
            if (m.find()) {
                long rows = Long.parseLong(m.group(1));
                return rows > 0 ? rows : -1L;
            }
        } catch (Exception e) {
            // EXPLAIN 不可用（权限 / 语法 / 版本）→ 未知
        }
        return -1L;
    }

    /**
     * 过滤感知的<b>字节</b>估算（三级降级）：① 元数据体积（有过滤则按选择率缩放）→
     * ② EXPLAIN 行数 × 采样平均行长 → ③ 未知（{@code -1}，调用方退回最保守的排序归并）。
     * 见 {@link JdbcSizeEstimator#estimate}。
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
        try (Connection conn = getConnection()) {
            // pg_stats 提供 n_distinct 基数估算（NULL 表示未统计）
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT a.attname AS column_name, s.n_distinct"
                    + " FROM pg_attribute a"
                    + " JOIN pg_class c ON a.attrelid = c.oid"
                    + " JOIN pg_namespace n ON c.relnamespace = n.oid"
                    + " LEFT JOIN pg_stats s"
                    + "   ON s.schemaname = n.nspname AND s.tablename = c.relname"
                    + "  AND s.attname = a.attname"
                    + " WHERE n.nspname = ? AND c.relname = ?"
                    + "   AND a.attnum > 0 AND NOT a.attisdropped"
                    + " ORDER BY a.attnum")) {
                ps.setString(1, schema);
                ps.setString(2, table);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String colName = rs.getString("column_name");
                        double nDistinct = rs.getDouble("n_distinct");
                        long cardinality = nDistinct > 0 ? Math.round(nDistinct) : -1;
                        statsList.add(new ColumnStats(colName, -1, cardinality,
                                Optional.empty(), Optional.empty(), Optional.empty()));
                    }
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
                // PG 流式读：关闭自动提交 + 正数 fetchSize
                readConnection.setAutoCommit(false);
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
            if (readConnection != null) {
                readConnection.rollback(); // 释放只读事务
                readConnection.close();
                readConnection = null;
            }
        } catch (SQLException ignored) {}
        readConverter = null;
        activeReadSql = null;
    }

    // ==================== 列裁剪 & 谓词下推 ====================

    /** 列裁剪：Postgres 方言对列名做双引号转义，防止保留字/特殊字符破坏 SELECT 列清单。 */
    @Override
    public ReadRequest pruneColumns(ReadRequest request, List<String> columns) {
        if (columns == null || columns.isEmpty()) return request;
        List<String> escaped = new ArrayList<>(columns.size());
        for (String col : columns) {
            escaped.add(quoteIdent(col));
        }
        return request.withColumns(escaped);
    }

    /** 谓词下推：Postgres 方言原样透传（buildReadSql 直接拼进 WHERE）。 */
    @Override
    public ReadRequest pushdownPredicate(ReadRequest request, String predicate) {
        if (predicate == null || predicate.isBlank()) return request;
        String merged = request.filterExpression()
                .map(f -> "(" + f + ") AND (" + predicate + ")")
                .orElse(predicate);
        return request.withFilter(merged);
    }

    /** 谓词下推：Postgres 方言原样透传（buildReadSql 直接拼进 WHERE）。 */
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

    /** Postgres 标识符双引号转义。 */
    private String quoteIdent(String ident) {
        // 幂等：入参可能已是转义形态（ReadRequest.columns() 复用）→ 先剥引号再转义
        return "\"" + SqlIdentifiers.unquote(ident).replace("\"", "\"\"") + "\"";
    }

    // ==================== 分区探针（平行 TableScan 用） ====================

    /**
     * 获取表的主键列名。仅返回单列数值型主键（适合范围分区）。
     * 若为联合主键或非数值型，返回 {@code Optional.empty()}。
     */
    @Override
    public Optional<String> getPartitionKey(String schema, String table) {
        try (Connection conn = getConnection()) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT kc.column_name, c.udt_name"
                    + " FROM information_schema.table_constraints tc"
                    + " JOIN information_schema.key_column_usage kc"
                    + "   ON tc.constraint_name = kc.constraint_name"
                    + "  AND tc.table_schema = kc.table_schema"
                    + "  AND tc.table_name = kc.table_name"
                    + " JOIN information_schema.columns c"
                    + "   ON c.table_schema = kc.table_schema"
                    + "  AND c.table_name = kc.table_name"
                    + "  AND c.column_name = kc.column_name"
                    + " WHERE tc.constraint_type = 'PRIMARY KEY'"
                    + "   AND tc.table_schema = ? AND tc.table_name = ?"
                    + " ORDER BY kc.ordinal_position")) {
                ps.setString(1, schema);
                ps.setString(2, table);
                try (ResultSet rs = ps.executeQuery()) {
                    String pkCol = null;
                    int count = 0;
                    while (rs.next()) {
                        count++;
                        if (count == 1 && isNumericType(rs.getString("udt_name"))) {
                            pkCol = rs.getString("column_name");
                        }
                    }
                    return (count == 1 && pkCol != null) ? Optional.of(pkCol) : Optional.empty();
                }
            }
        } catch (SQLException e) {
            return Optional.empty();
        }
    }

    /** 获取精确行数（SELECT COUNT(*)）。 */
    @Override
    public long getExactRowCount(String schema, String table) {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT COUNT(*) FROM " + quoteIdent(schema) + "." + quoteIdent(table))) {
            if (rs.next()) return rs.getLong(1);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to count rows: " + schema + "." + table, e);
        }
        return 0;
    }

    /** 获取主键列的最小/最大值（用于范围分区）。 */
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

    private static boolean isNumericType(String udtName) {
        if (udtName == null) return false;
        String u = udtName.toLowerCase(Locale.ROOT);
        return u.equals("int2") || u.equals("int4") || u.equals("int8")
                || u.equals("numeric") || u.equals("float4") || u.equals("float8")
                || u.contains("serial");
    }

    // ==================== 并行扫描分区（主键 → ctid 兜底） ====================

    /**
     * 分区策略：主键范围分区优先（默认实现）；无主键时回退
     * <b>ctid 物理行号范围分区</b>，保证大表无主键也能并行扫描。
     */
    @Override
    public List<ReadRequest> planParallelScan(ReadRequest request, int parallelism) {
        List<ReadRequest> pkPartitions = DojaoConnector.super.planParallelScan(request, parallelism);
        if (!pkPartitions.isEmpty()) return pkPartitions;
        return planCtidPartitions(request, parallelism);
    }

    /**
     * ctid 范围分区：{@code SELECT MIN(ctid), MAX(ctid)} 拿物理行号边界，按 block 均分成 N 段，
     * 每段 {@code ctid >= '(b0,0)' AND ctid < '(b1,0)'}。ctid 是物理位置，天然均匀、零索引依赖，
     * 各分区谓词互斥且并集 = 全表，总 I/O 恰 1 倍。
     */
    private List<ReadRequest> planCtidPartitions(ReadRequest request, int parallelism) {
        if (parallelism <= 1 || request.limit() > 0 || request.offset() > 0) return List.of();
        long[] blockRange;
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT MIN(ctid), MAX(ctid) FROM "
                     + quoteIdent(request.schema()) + "." + quoteIdent(request.table()))) {
            if (!rs.next()) return List.of();
            blockRange = parseCtidBoundaries(rs.getString(1), rs.getString(2));
        } catch (SQLException e) {
            return List.of();   // 探测失败 → 单分区兜底
        }
        if (blockRange == null) return List.of();

        long minBlk = blockRange[0], maxBlk = blockRange[1];
        long span = maxBlk - minBlk + 1;
        long step = span / parallelism + (span % parallelism == 0 ? 0 : 1);
        if (step <= 0) step = 1;

        List<ReadRequest> parts = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            long startBlk = minBlk + (long) i * step;
            long endBlk = (i == parallelism - 1) ? maxBlk + 1 : startBlk + step;   // 半开区间
            if (startBlk > maxBlk) break;
            String rangeFilter = "ctid >= '(" + startBlk + ",0)' AND ctid < '(" + endBlk + ",0)'";
            String merged = request.filterExpression()
                    .map(f -> "(" + f + ") AND (" + rangeFilter + ")")
                    .orElse(rangeFilter);
            parts.add(new ReadRequest(request.catalog(), request.schema(), request.table(),
                    request.columns(), Optional.of(merged), request.offset(), request.limit()));
        }
        return parts;
    }

    /** 解析 min/max ctid 文本 "(block,offset)" → [minBlk, maxBlk]；无法解析 / 单块返回 null。 */
    private static long[] parseCtidBoundaries(String minCtid, String maxCtid) {
        if (minCtid == null || maxCtid == null) return null;
        long minBlk = parseCtidBlock(minCtid);
        long maxBlk = parseCtidBlock(maxCtid);
        if (minBlk < 0 || maxBlk < 0 || minBlk >= maxBlk) return null;
        return new long[]{minBlk, maxBlk};
    }

    private static long parseCtidBlock(String ctid) {
        try {
            String inner = ctid.trim();
            if (inner.startsWith("(") && inner.endsWith(")")) {
                inner = inner.substring(1, inner.length() - 1);
            }
            return Long.parseLong(inner.split(",")[0].trim());
        } catch (Exception e) {
            return -1;
        }
    }

    // ==================== 数据写入（Arrow → JDBC）====================

    /**
     * 持久写会话：每个（写线程 × SQL）一个独立 Connection + PreparedStatement 复用，
     * 避免每页新建连接的开销。URL 自动补 reWriteBatchedInserts=true，把一页的
     * addBatch 行折叠成一条多行 INSERT VALUES(...),(...)，消除逐行网络往返。
     */
    private final ConcurrentHashMap<String, WriteSession> writeSessions = new ConcurrentHashMap<>();

    /** 把 reWriteBatchedInserts=true 追加到 JDBC URL（已含则不动）。 */
    private static String withRewriteBatched(String url) {
        if (url.contains("reWriteBatchedInserts")) return url;
        return url + (url.contains("?") ? "&" : "?") + "reWriteBatchedInserts=true";
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
            int batchSize = Math.max(1, CommonConfig.JDBC_WRITE_BATCH_SIZE);   // 分块行数（默认 1 万行/批）
            for (int row = 0; row < rowCount; row++) {
                for (int col = 0; col < colCount; col++) {
                    FieldVector vector = root.getVector(col);
                    Field field = root.getSchema().getFields().get(col);
                    Object value;
                    if (vector instanceof FixedSizeListVector flv) {
                        // 向量列 → "[v1,v2,...]" 文本，PG 自动转为 vector 类型
                        value = flv.isNull(row) ? null : vectorToString(flv, row);
                    } else if (vector instanceof MapVector || vector instanceof StructVector) {
                        // PG 没有原生 Map / STRUCT 列类型：显式拒绝，避免驱动抛难以理解的错误
                        throw new UnsupportedOperationException("PostgreSQL 不支持写入 Map / STRUCT 类型列 '"
                                + vector.getField().getName() + "'（" + field.getType() + "）");
                    } else if (vector instanceof ListVector) {
                        // PG 数组：转成 Java 数组后由驱动按元素类型编码（多维数组同样适用）
                        value = NestedTypeSupport.toJavaValue(vector, row);
                    } else {
                        value = ArrowJdbcValues.toJdbc(vector, row);
                    }
                    // Arrow Text → String
                    if (value instanceof Text textValue) {
                        value = textValue.toString();
                    }
                    if (value instanceof Object[] arrayValue) {
                        ps.setArray(col + 1, ps.getConnection().createArrayOf(
                                pgArrayElementType(field), arrayValue));
                    } else {
                        ps.setObject(col + 1, value);
                    }
                }
                ps.addBatch();
                // 每 batchSize 行 flush 一次：单条多行 INSERT 规模可控，均摊 PostgreSQL 解析成本
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

    /** 把 FixedSizeList&lt;Float32&gt; 行转成 pgvector 文本 "[v1,v2,...]"。 */
    private static String vectorToString(FixedSizeListVector vec, int row) {
        int listSize = ((ArrowType.FixedSizeList) vec.getField().getType()).getListSize();
        org.apache.arrow.vector.Float4Vector child =
                (org.apache.arrow.vector.Float4Vector) vec.getDataVector();
        StringBuilder sb = new StringBuilder(listSize * 10);
        sb.append('[');
        for (int e = 0; e < listSize; e++) {
            if (e > 0) sb.append(',');
            sb.append(child.get(row * listSize + e));
        }
        sb.append(']');
        return sb.toString();
    }

    // ==================== 建表（CTAS）====================

    @Override
    public boolean createTable(String catalog, String schema, String table,
                               List<Field> fields, boolean ifNotExists) {
        // 表已存在：IF NOT EXISTS → 跳过建表（返回 false，调用方不写入）；否则让下方 DDL 抛标准
        // "relation ... already exists" 错误（标准 SQL 语义：不带 IF NOT EXISTS 重复建表即报错）
        if (tableExists(catalog, schema, table)) {
            if (ifNotExists) return false;
        }
        boolean hasVector = fields.stream()
                .anyMatch(f -> f.getType() instanceof ArrowType.FixedSizeList);
        StringBuilder sb = new StringBuilder("CREATE TABLE ");
        if (ifNotExists) sb.append("IF NOT EXISTS ");
        sb.append(quoteIdent(schema)).append('.').append(quoteIdent(table)).append(" (");
        List<String> defs = new ArrayList<>();
        for (Field f : fields) {
            defs.add(quoteIdent(f.getName()) + " " + arrowTypeToPg(f));
        }
        sb.append(String.join(", ", defs)).append(')');

        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {
            if (hasVector) {
                // pgvector 扩展未装时建表会失败，先确保扩展存在
                stmt.executeUpdate("CREATE EXTENSION IF NOT EXISTS vector");
            }
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
            stmt.executeUpdate("DROP TABLE IF EXISTS "
                    + quoteIdent(schema) + "." + quoteIdent(table));
        } catch (SQLException e) {
            throw new RuntimeException("Failed to drop table: " + schema + "." + table, e);
        }
    }

    @Override
    public void truncateTable(String catalog, String schema, String table) {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("TRUNCATE TABLE "
                    + quoteIdent(schema) + "." + quoteIdent(table));
        } catch (SQLException e) {
            throw new RuntimeException("Failed to truncate table: " + schema + "." + table, e);
        }
    }

    /**
     * Arrow 字段 → PostgreSQL 列类型（嵌套列写出 {@code 元素类型[]}，多维数组逐层加 {@code []}）。
     *
     * <p>PG 没有 Map / STRUCT 列类型 → 显式拒绝，而不是静默写成 TEXT（写进去就读不回来了）。</p>
     */
    static String arrowTypeToPg(Field field) {
        ArrowType type = field.getType();
        if (type instanceof ArrowType.List) {
            List<Field> children = field.getChildren();
            String elem = children.isEmpty() ? "TEXT" : arrowTypeToPg(children.get(0));
            return elem + "[]";
        }
        if (type instanceof ArrowType.Map || type instanceof ArrowType.Struct) {
            throw new UnsupportedOperationException("PostgreSQL 不支持 "
                    + (type instanceof ArrowType.Map ? "Map" : "STRUCT")
                    + " 类型列 '" + field.getName() + "'（PG 只有数组）");
        }
        return arrowTypeToPg(type);
    }

    /** PG 数组的<b>最内层</b>元素类型名（{@code createArrayOf} 用；多维数组也只给基础类型）。 */
    static String pgArrayElementType(Field listField) {
        Field current = listField;
        while (current != null && current.getType() instanceof ArrowType.List) {
            List<Field> children = current.getChildren();
            current = children.isEmpty() ? null : children.get(0);
        }
        if (current == null) return "text";
        ArrowType type = current.getType();
        if (type instanceof ArrowType.Int i) {
            return switch (i.getBitWidth()) {
                case 8, 16 -> "int2";
                case 32 -> "int4";
                default -> "int8";
            };
        }
        if (type instanceof ArrowType.FloatingPoint fp) {
            return fp.getPrecision() == FloatingPointPrecision.SINGLE ? "float4" : "float8";
        }
        if (type instanceof ArrowType.Decimal) return "numeric";
        if (type instanceof ArrowType.Bool) return "bool";
        if (type instanceof ArrowType.Date) return "date";
        if (type instanceof ArrowType.Timestamp || type instanceof ArrowType.Time) return "timestamp";
        if (type instanceof ArrowType.Binary) return "bytea";
        return "text";
    }

    /** Arrow 类型 → PostgreSQL 列类型。 */
    private static String arrowTypeToPg(ArrowType type) {
        if (type instanceof ArrowType.Int i) {
            return switch (i.getBitWidth()) {
                case 8, 16 -> "SMALLINT";
                case 32 -> "INTEGER";
                default -> "BIGINT";
            };
        }
        if (type instanceof ArrowType.Utf8) return "TEXT";
        if (type instanceof ArrowType.LargeUtf8) return "TEXT";
        if (type instanceof ArrowType.FloatingPoint fp) {
            return fp.getPrecision() == FloatingPointPrecision.SINGLE ? "REAL" : "DOUBLE PRECISION";
        }
        if (type instanceof ArrowType.Decimal) return "NUMERIC";
        if (type instanceof ArrowType.Bool) return "BOOLEAN";
        if (type instanceof ArrowType.Date) return "DATE";
        if (type instanceof ArrowType.Timestamp || type instanceof ArrowType.Time) return "TIMESTAMP";
        if (type instanceof ArrowType.FixedSizeList fsl) return "vector(" + fsl.getListSize() + ")";
        // 兜底：文本
        return "TEXT";
    }

    // ==================== 缓存管理 ====================

    /** 使元数据缓存失效（下次查询时重新从 PostgreSQL 加载）。 */
    public void invalidateCache() {
        this.cacheDirty = true;
        this.tableCache.clear();
        this.columnCache.clear();
    }

    // ==================== 私有方法 ====================

    /** 获取表的 COMMENT（pg_description 中 objsubid=0 的注释）。 */
    private String getTableComment(String schema, String table) {
        try (Connection conn = getConnection()) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT d.description FROM pg_description d"
                    + " JOIN pg_class c ON d.objoid = c.oid"
                    + " JOIN pg_namespace n ON c.relnamespace = n.oid"
                    + " WHERE n.nspname = ? AND c.relname = ? AND d.objsubid = 0")) {
                ps.setString(1, schema);
                ps.setString(2, table);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        String comment = rs.getString(1);
                        return (comment != null && !comment.isEmpty()) ? comment : null;
                    }
                }
            }
        } catch (SQLException e) {
            // ignore
        }
        return null;
    }

    /** 获取表属性（PG 的 reltuples / relpages 等）。 */
    private Map<String, String> getTableProperties(String schema, String table) {
        Map<String, String> props = new LinkedHashMap<>();
        try (Connection conn = getConnection()) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT c.reltuples, c.relpages FROM pg_class c"
                    + " JOIN pg_namespace n ON c.relnamespace = n.oid"
                    + " WHERE n.nspname = ? AND c.relname = ?")) {
                ps.setString(1, schema);
                ps.setString(2, table);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        putIfNotNull(props, "reltuples", String.valueOf(rs.getLong("reltuples")));
                        putIfNotNull(props, "relpages", String.valueOf(rs.getLong("relpages")));
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
