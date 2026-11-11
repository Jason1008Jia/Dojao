package com.dojao.connector.oracle;

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
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Oracle 连接器，通过 JDBC 实现 {@link DojaoConnector} 接口。
 *
 * <p>三层映射：
 * <ul>
 *   <li>catalog → JDBC 连接串（{@code jdbc:oracle:thin:@//host:port/service}）指向的<b>数据库实例</b></li>
 *   <li>schema  → Oracle 的<b>用户</b>（即表的 owner；留空 = 当前登录用户的默认 schema）</li>
 *   <li>table / column → 数据字典 {@code ALL_TABLES} / {@code ALL_TAB_COLUMNS}</li>
 * </ul>
 *
 * <p><b>方言要点</b>：
 * <ul>
 *   <li><b>标识符大小写</b>：Oracle 会把不加引号的标识符折叠成大写，而引擎下发的通常是用户原文
 *       （小写）。因此 schema / table / column 一律先按原文精确查字典、再按 {@code UPPER()} 兜底，
 *       最后用字典里的<b>真实名字</b>加双引号下发 —— 既不会因为大小写对不上而找不到表，
 *       也不会把加引号创建的混合大小写对象改错大小写。</li>
 *   <li>标识符用双引号转义（内嵌 {@code "} 翻倍）。</li>
 *   <li><b>不带精度的 NUMBER 必须 CAST</b>：Oracle 的 {@code NUMBER}（无 p/s）在 JDBC 元数据里
 *       precision 为 0，而 {@link JdbcArrowConverter} 会据此构造 {@code Decimal(0, s)} —— 非法的
 *       Arrow 类型。读取投影把它写成 {@code CAST("c" AS NUMBER(38,10)) AS "c"}，同时声明类型仍为
 *       DECIMAL，保证「声明类型 = 实际产出类型」。负 scale / scale 大于 precision 同样走这条 CAST。</li>
 *   <li><b>FLOAT 提升为 BINARY_DOUBLE</b>：Oracle {@code FLOAT} 实际是二进制精度的 NUMBER（默认 126
 *       位），而 JDBC 把它报成 {@code Types.FLOAT} → Arrow Float32，会丢精度；投影里 CAST 成
 *       {@code BINARY_DOUBLE} 换成 Float64。</li>
 *   <li><b>DATE 带时间</b>：Oracle {@code DATE} 精确到秒，JDBC 按 TIMESTAMP 上报 → Arrow
 *       Timestamp(ms)，故声明为 DATETIME 而不是 DATE。</li>
 *   <li>分页：12c+ 的 {@code OFFSET n ROWS [FETCH NEXT m ROWS ONLY]} / {@code FETCH FIRST n ROWS ONLY}。</li>
 * </ul>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/24
 */
public class OracleConnector implements DojaoConnector, OverwriteEnable {

    /** 此连接器的 catalog 名称（对外暴露的 catalog 标识）。 */
    private final String catalogName;

    /** JDBC 连接信息。 */
    private final String jdbcUrl;
    private final String username;
    private final String password;

    /** Oracle 标识符上限（12.2+ 为 128 字节；更早版本为 30，临时表名按 128 截断）。 */
    static final int MAX_IDENTIFIER_LENGTH = 128;

    /** 缓存：OWNER.TABLE（均大写）→ 列元数据。 */
    private final Map<String, List<ColumnMeta>> columnMetaCache = new ConcurrentHashMap<>();

    /** 缓存：OWNER.TABLE（均大写）→ 对外列清单。 */
    private final Map<String, List<Column>> columnCache = new ConcurrentHashMap<>();

    /** 缓存：输入 schema（原文）→ 字典里的真实用户名。 */
    private final Map<String, String> schemaCache = new ConcurrentHashMap<>();

    /** 缓存：OWNER(UPPER)|输入 table(UPPER) → 字典里的真实表名。 */
    private final Map<String, String> tableCache = new ConcurrentHashMap<>();

    /** Arrow 内存分配器，用于 JDBC → Arrow 转换。 */
    private final BufferAllocator allocator;

    // ==================== 游标状态（多页读取） ====================
    private Connection readConnection;
    private PreparedStatement readPreparedStatement;
    private ResultSet readResultSet;
    private JdbcArrowConverter readConverter;
    private String activeReadSql;

    public OracleConnector(String catalogName, String jdbcUrl, String username, String password) {
        this(catalogName, jdbcUrl, username, password,
                NodeMemory.shared().newChildAllocator("connector/oracle", 0));
    }

    public OracleConnector(String catalogName, String jdbcUrl, String username, String password,
                           BufferAllocator allocator) {
        this.catalogName = catalogName;
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.allocator = allocator;
    }

    public OracleConnector(String jdbcUrl, String username, String password) {
        this("oracle", jdbcUrl, username, password);
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
    public OracleConnector copy() {
        return new OracleConnector(catalogName, jdbcUrl, username, password,
                NodeMemory.shared().newChildAllocator("connector/oracle", 0));
    }

    // ==================== Connection ====================

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, username, password);
    }

    /**
     * 连接性探针：用当前 URL / 账号<b>真的建一次连接</b>（引擎在 CREATE CATALOG 落库前调用，
     * 连不上就不入库）。只做一次 {@code isValid} 握手，不枚举元数据。
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
        return Optional.of(new Catalog(catalogName, "oracle", Map.of("url", jdbcUrl), null, null));
    }

    @Override
    public List<String> listCatalogs() {
        return List.of(catalogName);
    }

    // ==================== Schema ====================

    /** Oracle 内部账号，不作为用户 schema 展示。 */
    private static final Set<String> SYSTEM_SCHEMAS = Set.of(
            "SYS", "SYSTEM", "SYSAUX", "OUTLN", "DBSNMP", "APPQOSSYS", "AUDSYS", "ANONYMOUS",
            "XDB", "WMSYS", "ORDSYS", "ORDDATA", "ORDPLUGINS", "CTXSYS", "MDSYS", "OLAPSYS",
            "LBACSYS", "DVSYS", "DVF", "GSMADMIN_INTERNAL", "OJVMSYS", "REMOTE_SCHEDULER_AGENT",
            "SYSBACKUP", "SYSDG", "SYSKM", "SYSRAC", "SYS$UMF", "XS$NULL", "GGSYS", "DBSFWUSER",
            "PUBLIC", "ORACLE_OCM", "MDDATA", "SI_INFORMTN_SCHEMA", "SPATIAL_CSW_ADMIN_USR",
            "SPATIAL_WFS_ADMIN_USR", "APEX_PUBLIC_USER", "FLOWS_FILES", "DIP", "TSMSYS");

    @Override
    public boolean schemaExists(String catalog, String schema) {
        if (!catalogExists(catalog)) return false;
        return lookupSchema(schema).isPresent();
    }

    @Override
    public Optional<Schema> getSchema(String catalog, String schema) {
        if (!schemaExists(catalog, schema)) return Optional.empty();
        String resolved = lookupSchema(schema).orElse(schema);
        return Optional.of(new Schema(catalog, resolved, Map.of()));
    }

    @Override
    public List<String> listSchemas(String catalog) {
        if (!catalogExists(catalog)) return List.of();
        List<String> schemas = new ArrayList<>();
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT username FROM all_users ORDER BY username")) {
            while (rs.next()) {
                String name = rs.getString(1);
                if (name == null) continue;
                String upper = name.toUpperCase(Locale.ROOT);
                if (SYSTEM_SCHEMAS.contains(upper)) continue;
                if (upper.startsWith("APEX_") || upper.startsWith("FLOWS_")) continue;
                schemas.add(name);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to list schemas", e);
        }
        return schemas;
    }

    /** 把用户输入的 schema 解析为字典里的真实用户名（大小写不敏感，空 = 当前 schema）。 */
    private Optional<String> lookupSchema(String schema) {
        if (schema == null || schema.isBlank()) {
            return Optional.ofNullable(currentSchema());
        }
        String trimmed = schema.trim();
        String cached = schemaCache.get(trimmed);
        if (cached != null) return Optional.of(cached);

        Optional<String> found = lookupOne(
                "SELECT username FROM all_users WHERE username = ?", trimmed);
        if (found.isEmpty()) {
            found = lookupOne(
                    "SELECT username FROM all_users WHERE UPPER(username) = UPPER(?)", trimmed);
        }
        found.ifPresent(name -> schemaCache.put(trimmed, name));
        return found;
    }

    /** 解析 schema，查不到时原样返回（让后续 SQL 报出用户看得懂的名字）。 */
    private String resolveSchema(String schema) {
        if (schema == null || schema.isBlank()) {
            String current = currentSchema();
            return current != null ? current : username;
        }
        return lookupSchema(schema).orElseGet(() -> SqlIdentifiers.unquote(schema.trim()));
    }

    private String currentSchema() {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL")) {
            if (rs.next()) return rs.getString(1);
        } catch (SQLException e) {
            // 取不到当前 schema 就退回登录用户名（Oracle 默认 schema 即用户名）
        }
        return username;
    }

    // ==================== Table ====================

    @Override
    public boolean tableExists(String catalog, String schema, String table) {
        if (!catalogExists(catalog)) return false;
        String owner = resolveSchema(schema);
        return lookupTable(owner, table).isPresent();
    }

    @Override
    public Optional<Table> getTable(String catalog, String schema, String table) {
        List<Column> columns = getTableColumns(catalog, schema, table);
        if (columns.isEmpty() && !tableExists(catalog, schema, table)) {
            return Optional.empty();
        }
        String owner = resolveSchema(schema);
        String resolved = lookupTable(owner, table).orElse(table);
        return Optional.of(new Table(
                catalog, owner, resolved,
                columns,
                getTableProperties(owner, resolved),
                getTableComment(owner, resolved)
        ));
    }

    @Override
    public List<Column> getTableColumns(String catalog, String schema, String tableName) {
        if (!catalogExists(catalog)) return List.of();

        String owner = resolveSchema(schema);
        String table = lookupTable(owner, tableName).orElse(SqlIdentifiers.unquote(tableName));
        String cacheKey = cacheKey(owner, table);

        List<Column> cached = columnCache.get(cacheKey);
        if (cached != null) return cached;

        List<Column> columns = new ArrayList<>();
        for (ColumnMeta m : loadColumnMeta(owner, table)) {
            columns.add(new Column(m.name(), m.dataType(), m.comment(), m.defaultValue(),
                    m.nullable()));
        }
        List<Column> frozen = List.copyOf(columns);
        columnCache.put(cacheKey, frozen);
        return frozen;
    }

    @Override
    public List<String> listTables(String catalog, String schema) {
        if (!catalogExists(catalog)) return List.of();
        String owner = resolveSchema(schema);
        List<String> tables = new ArrayList<>();
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT table_name FROM all_tables"
                     + " WHERE owner = ? AND nested = 'NO' AND table_name NOT LIKE 'BIN$%'"
                     + " ORDER BY table_name")) {
            ps.setString(1, owner);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tables.add(rs.getString(1));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to list tables for: " + owner, e);
        }
        return tables;
    }

    // ==================== Column ====================

    @Override
    public boolean columnExists(String catalog, String schema, String table, String column) {
        if (!catalogExists(catalog)) return false;
        return getColumn(catalog, schema, table, column).isPresent();
    }

    @Override
    public Optional<Column> getColumn(String catalog, String schema, String table, String column) {
        if (column == null) return Optional.empty();
        String name = SqlIdentifiers.unquote(column.trim());
        for (Column c : getTableColumns(catalog, schema, table)) {
            if (c.name().equals(name) || c.name().equalsIgnoreCase(name)) {
                return Optional.of(c);
            }
        }
        return Optional.empty();
    }

    // ==================== Dojao 类型映射 ====================

    private static final Map<String, DojaoType> ORACLE_TYPE_MAPPING = new LinkedHashMap<>();

    static {
        // 数值：Oracle 的 INTEGER / DECIMAL 等 ANSI 同义词在字典里都存成 NUMBER
        ORACLE_TYPE_MAPPING.put("number", DojaoType.DECIMAL);
        ORACLE_TYPE_MAPPING.put("numeric", DojaoType.DECIMAL);
        ORACLE_TYPE_MAPPING.put("decimal", DojaoType.DECIMAL);
        ORACLE_TYPE_MAPPING.put("integer", DojaoType.DECIMAL);
        ORACLE_TYPE_MAPPING.put("int", DojaoType.DECIMAL);
        // FLOAT 是二进制精度的 NUMBER → 读取时 CAST 成 BINARY_DOUBLE（见 #castTarget）
        ORACLE_TYPE_MAPPING.put("float", DojaoType.DOUBLE);
        ORACLE_TYPE_MAPPING.put("binary_float", DojaoType.FLOAT);
        ORACLE_TYPE_MAPPING.put("binary_double", DojaoType.DOUBLE);
        // 字符串
        ORACLE_TYPE_MAPPING.put("char", DojaoType.STRING);
        ORACLE_TYPE_MAPPING.put("nchar", DojaoType.STRING);
        ORACLE_TYPE_MAPPING.put("varchar", DojaoType.STRING);
        ORACLE_TYPE_MAPPING.put("varchar2", DojaoType.STRING);
        ORACLE_TYPE_MAPPING.put("nvarchar2", DojaoType.STRING);
        ORACLE_TYPE_MAPPING.put("long", DojaoType.STRING);
        ORACLE_TYPE_MAPPING.put("clob", DojaoType.STRING);
        ORACLE_TYPE_MAPPING.put("nclob", DojaoType.STRING);
        ORACLE_TYPE_MAPPING.put("rowid", DojaoType.STRING);
        ORACLE_TYPE_MAPPING.put("urowid", DojaoType.STRING);
        ORACLE_TYPE_MAPPING.put("xmltype", DojaoType.STRING);
        ORACLE_TYPE_MAPPING.put("json", DojaoType.STRING);
        ORACLE_TYPE_MAPPING.put("sdo_geometry", DojaoType.STRING);
        // 二进制：DojaoType 无二进制枚举，按字符串声明（与 SQL Server 连接器同口径）
        ORACLE_TYPE_MAPPING.put("blob", DojaoType.STRING);
        ORACLE_TYPE_MAPPING.put("raw", DojaoType.STRING);
        ORACLE_TYPE_MAPPING.put("long raw", DojaoType.STRING);
        ORACLE_TYPE_MAPPING.put("bfile", DojaoType.STRING);
        // 日期 / 时间：Oracle DATE 含时分秒，JDBC 报 TIMESTAMP
        ORACLE_TYPE_MAPPING.put("date", DojaoType.DATETIME);
        ORACLE_TYPE_MAPPING.put("timestamp", DojaoType.DATETIME);
        ORACLE_TYPE_MAPPING.put("timestamp with time zone", DojaoType.TIMESTAMP_TZ);
        ORACLE_TYPE_MAPPING.put("timestamp with local time zone", DojaoType.TIMESTAMP_TZ);
        ORACLE_TYPE_MAPPING.put("interval day to second", DojaoType.STRING);
        ORACLE_TYPE_MAPPING.put("interval year to month", DojaoType.STRING);
        // 布尔（23c 起原生 BOOLEAN）
        ORACLE_TYPE_MAPPING.put("boolean", DojaoType.BOOLEAN);
    }

    /** Oracle 类型名（{@code ALL_TAB_COLUMNS.DATA_TYPE}）→ Dojao 类型；未知类型按字符串处理。 */
    static DojaoType dojaoType(String dataType) {
        if (dataType == null) return DojaoType.STRING;
        return ORACLE_TYPE_MAPPING.getOrDefault(
                dataType.trim().toLowerCase(Locale.ROOT), DojaoType.STRING);
    }

    @Override
    public Map<String, DojaoType> getColumnTypes(String schema, String table) {
        String owner = resolveSchema(schema);
        String resolved = lookupTable(owner, table).orElse(SqlIdentifiers.unquote(table));
        Map<String, DojaoType> result = new LinkedHashMap<>();
        for (ColumnMeta m : loadColumnMeta(owner, resolved)) {
            result.put(m.name(), dojaoType(m.dataType()));
        }
        return result;
    }

    // ==================== CTAS 声明列（与读取同一条映射） ====================

    /**
     * CTAS 源列类型映射：对指定列集跑一条零行探针
     * {@code SELECT <投影> FROM 表 WHERE 1=0}，直接取 JDBC 元数据 →
     * {@link JdbcArrowConverter#buildSchema}。
     *
     * <p>投影走与运行期完全相同的 {@link #projectColumn}（NUMBER / FLOAT 的 CAST），
     * 因此 Master 按此建出的目标列类型与 Worker 实际扫出的 Arrow 类型逐列一致。</p>
     */
    @Override
    public List<Field> sourceFields(String catalog, String schema, String table, List<String> columns) {
        String owner = resolveSchema(schema);
        String resolved = lookupTable(owner, table).orElse(SqlIdentifiers.unquote(table));
        List<ColumnMeta> meta = loadColumnMeta(owner, resolved);
        Set<String> known = new LinkedHashSet<>();
        for (ColumnMeta m : meta) known.add(m.name());

        List<String> requested = new ArrayList<>();
        if (columns == null || columns.isEmpty()) {
            requested.addAll(known);
        } else {
            for (String fragment : columns) {
                String name = resolveColumnName(fragment, known);
                if (name != null && (known.isEmpty() || containsIgnoreCase(known, name))) {
                    requested.add(name);
                }
            }
        }
        if (requested.isEmpty()) return List.of();

        Map<String, String> casts = castTargets(meta);
        StringBuilder select = new StringBuilder();
        for (String name : requested) {
            if (select.length() > 0) select.append(", ");
            select.append(projectColumn(name, casts));
        }
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT " + select + " FROM " + q(owner, resolved) + " WHERE 1=0")) {
            try (ResultSet rs = ps.executeQuery()) {
                return JdbcArrowConverter.buildSchema(rs.getMetaData()).getFields();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to read source column metadata for CTAS: "
                    + owner + "." + resolved, e);
        }
    }

    /**
     * 从「列名 / 已转义的 SQL 片段」还原出元数据里的裸列名。
     *
     * <p>引擎不同链路传进来的形态不同：{@code pruneColumns} 之后是 {@code "col"} 或
     * {@code CAST("col" AS NUMBER(38,10)) AS "col"}，规划期可能还是裸名。依次尝试
     * 直接剥引号 → 大小写不敏感匹配 → 取最后一个 {@code AS} 别名 → 取最后一段双引号内容。</p>
     */
    static String resolveColumnName(String fragment, Set<String> known) {
        if (fragment == null) return null;
        String t = fragment.trim();
        String direct = SqlIdentifiers.unquote(t);
        if (known.isEmpty() || known.contains(direct)) return direct;
        for (String k : known) {
            if (k.equalsIgnoreCase(direct)) return k;
        }
        int as = t.toUpperCase(Locale.ROOT).lastIndexOf(" AS ");
        if (as >= 0) {
            String tail = SqlIdentifiers.unquote(t.substring(as + 4).trim());
            if (known.contains(tail)) return tail;
            for (String k : known) {
                if (k.equalsIgnoreCase(tail)) return k;
            }
        }
        Matcher m = QUOTED_IDENTIFIER.matcher(t);
        String last = null;
        while (m.find()) last = m.group(1).replace("\"\"", "\"");
        if (last != null) {
            if (known.contains(last)) return last;
            for (String k : known) {
                if (k.equalsIgnoreCase(last)) return k;
            }
        }
        return direct;
    }

    private static final Pattern QUOTED_IDENTIFIER =
            Pattern.compile("\"((?:[^\"]|\"\")*)\"");

    private static boolean containsIgnoreCase(Set<String> names, String needle) {
        for (String n : names) {
            if (n.equalsIgnoreCase(needle)) return true;
        }
        return false;
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
     * <p>来自 {@code ALL_TABLES.NUM_ROWS / AVG_ROW_LEN}（需表做过统计信息收集）；
     * <b>未收集统计 / 权限不足 → {@code [-1,-1]}，不抛异常</b>。</p>
     */
    private long[] fetchRawStats(String schema, String table) {
        long rowCount = -1;
        long byteSize = -1;
        try {
            String owner = resolveSchema(schema);
            Optional<String> resolved = lookupTable(owner, table);
            if (resolved.isEmpty()) return new long[]{-1, -1};
            try (Connection conn = getConnection();
                 PreparedStatement ps = conn.prepareStatement(
                         "SELECT num_rows, avg_row_len FROM all_tables"
                         + " WHERE owner = ? AND table_name = ?")) {
                ps.setString(1, owner);
                ps.setString(2, resolved.get());
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        long rows = rs.getLong(1);
                        if (rs.wasNull()) rows = -1;
                        long avg = rs.getLong(2);
                        if (rs.wasNull()) avg = -1;
                        rowCount = rows > 0 ? rows : -1;
                        byteSize = (rows > 0 && avg > 0) ? rows * avg : -1;
                    }
                }
            }
        } catch (Exception e) {
            // 统计信息不可读 → 未知，交给后续降级
        }
        return new long[]{rowCount, byteSize};
    }

    /**
     * 字节估算。Oracle 没有轻量的 EXPLAIN 估算（EXPLAIN PLAN 需要写 PLAN_TABLE），
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
     * 列统计：Oracle 的 {@code num_distinct / num_nulls} 来自 {@code ALL_TAB_COLUMNS} 的统计快照，
     * 未收集统计时为 NULL → 记 {@code -1}。min/max 不在字典里，返回空。
     */
    @Override
    public List<ColumnStats> getColumnStats(String catalog, String schema, String table) {
        if (!catalogExists(catalog)) return List.of();
        String owner = resolveSchema(schema);
        String resolved = lookupTable(owner, table).orElse(SqlIdentifiers.unquote(table));
        List<ColumnStats> out = new ArrayList<>();
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT column_name, num_distinct, num_nulls FROM all_tab_columns"
                     + " WHERE owner = ? AND table_name = ? ORDER BY column_id")) {
            ps.setString(1, owner);
            ps.setString(2, resolved);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long distinct = rs.getLong(2);
                    if (rs.wasNull()) distinct = -1;
                    long nulls = rs.getLong(3);
                    if (rs.wasNull()) nulls = -1;
                    if (distinct < 0 && nulls < 0) continue;
                    out.add(new ColumnStats(rs.getString(1), nulls, distinct,
                            Optional.empty(), Optional.empty(), Optional.empty()));
                }
            }
        } catch (SQLException e) {
            return List.of();
        }
        return out;
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
            // 首次调用 → 打开新游标（setFetchSize 打开 Oracle 的 row prefetch）
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
     * 列裁剪：Oracle 方言用双引号转义；<b>无精度 NUMBER / FLOAT</b> 额外 CAST
     * （见类注释）。返回的 {@link ReadRequest#columns()} 是可直拼进 SELECT 的片段。
     */
    @Override
    public ReadRequest pruneColumns(ReadRequest request, List<String> columns) {
        if (columns == null || columns.isEmpty()) return request;
        String owner = resolveSchema(request.schema());
        String table = lookupTable(owner, request.table())
                .orElse(SqlIdentifiers.unquote(request.table()));
        Map<String, String> casts = castTargets(owner, table);
        List<String> escaped = new ArrayList<>(columns.size());
        for (String col : columns) {
            escaped.add(projectColumn(col, casts));
        }
        return request.withColumns(escaped);
    }

    /** 谓词下推：Oracle SQL 方言原样透传（buildReadSql 直接拼进 WHERE）。 */
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
        // buildReadSql 把 filterExpression 直接拼进 WHERE → 过滤真的执行了
        return true;
    }

    /** 构建 SELECT SQL（实例版：先把 schema / table 解析成字典里的真实名字）。 */
    String buildReadSql(ReadRequest request) {
        String owner = resolveSchema(request.schema());
        String table = lookupTable(owner, request.table())
                .orElse(SqlIdentifiers.unquote(request.table()));
        List<String> selectCols = request.columns().orElse(List.of());
        String projection = selectCols.isEmpty()
                ? defaultProjection(owner, table)
                : String.join(", ", selectCols);
        return buildReadSql(owner, table, projection,
                request.filterExpression().orElse(null), request.offset(), request.limit());
    }

    /**
     * 构建 SELECT SQL（纯函数：owner / table 已是字典里的真实名字，由本方法加引号）。
     *
     * <p>分页形态：无 offset → {@code FETCH FIRST n ROWS ONLY}；有 offset →
     * {@code OFFSET n ROWS [FETCH NEXT m ROWS ONLY]}（Oracle 12c+）。</p>
     */
    static String buildReadSql(String owner, String table, String projection, String filter,
                               long offset, long limit) {
        StringBuilder sql = new StringBuilder("SELECT ")
                .append(projection)
                .append(" FROM ").append(q(owner, table));

        if (filter != null && !filter.isBlank()) {
            sql.append(" WHERE ").append(filter);
        }

        if (offset > 0) {
            sql.append(" OFFSET ").append(offset).append(" ROWS");
            if (limit > 0) {
                sql.append(" FETCH NEXT ").append(limit).append(" ROWS ONLY");
            }
        } else if (limit > 0) {
            sql.append(" FETCH FIRST ").append(limit).append(" ROWS ONLY");
        }
        return sql.toString();
    }

    /** 无列裁剪时的整表投影：按元数据列出全部列，NUMBER / FLOAT 同样 CAST（元数据取不到 → {@code *}）。 */
    private String defaultProjection(String owner, String table) {
        List<ColumnMeta> meta = loadColumnMeta(owner, table);
        if (meta.isEmpty()) return "*";
        Map<String, String> casts = castTargets(meta);
        StringBuilder sb = new StringBuilder();
        for (ColumnMeta m : meta) {
            if (sb.length() > 0) sb.append(", ");
            sb.append(projectColumn(m.name(), casts));
        }
        return sb.toString();
    }

    /**
     * 单列投影片段：
     * <ul>
     *   <li>已经是 SQL 片段（含 {@code (} 或以 {@code CAST} 开头）→ 原样返回（幂等）；</li>
     *   <li>命中 casts → {@code CAST("c" AS <目标类型>) AS "c"}；</li>
     *   <li>其余 → {@code "c"}。</li>
     * </ul>
     */
    static String projectColumn(String rawName, Map<String, String> castTargets) {
        if (rawName == null) return null;
        String t = rawName.trim();
        if (looksLikeFragment(t)) return t;
        String name = SqlIdentifiers.unquote(t);
        String quoted = quote(name);
        String target = castTargets == null ? null
                : castTargets.get(name.toUpperCase(Locale.ROOT));
        return target == null ? quoted
                : "CAST(" + quoted + " AS " + target + ") AS " + quoted;
    }

    /** 判断是否已是 SQL 片段（而非裸标识符 / 带引号标识符）。 */
    static boolean looksLikeFragment(String s) {
        if (s == null) return false;
        String t = s.trim();
        return t.indexOf('(') >= 0 || t.regionMatches(true, 0, "CAST", 0, 4);
    }

    /** 双引号标识符转义（幂等：先剥任何方言引号，再按 Oracle 规则加双引号）。 */
    static String quote(String ident) {
        String name = SqlIdentifiers.unquote(ident);
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }

    /** {schema}.{table} 的双引号限定名。 */
    private static String q(String owner, String table) {
        return quote(owner) + "." + quote(table);
    }

    /**
     * 需要 CAST 的列（大写列名 → 目标 SQL 类型）。
     *
     * <ul>
     *   <li>无精度 NUMBER / 负 scale / scale &gt; precision → {@code NUMBER(38,10)}；</li>
     *   <li>FLOAT → {@code BINARY_DOUBLE}（避免 JDBC 报 Types.FLOAT 被映射成 Float32 丢精度）。</li>
     * </ul>
     */
    private Map<String, String> castTargets(String owner, String table) {
        return castTargets(loadColumnMeta(owner, table));
    }

    private static Map<String, String> castTargets(List<ColumnMeta> meta) {
        Map<String, String> out = new LinkedHashMap<>();
        for (ColumnMeta m : meta) {
            String target = castTarget(m.dataType(), m.precision(), m.scale());
            if (target != null) {
                out.put(m.name().toUpperCase(Locale.ROOT), target);
            }
        }
        return out;
    }

    /** 单列 CAST 目标类型；不需要 CAST 返回 {@code null}。 */
    static String castTarget(String dataType, int precision, int scale) {
        if (dataType == null) return null;
        String t = dataType.trim().toUpperCase(Locale.ROOT);
        if (t.equals("NUMBER") || t.equals("NUMERIC") || t.equals("DECIMAL")
                || t.equals("INTEGER")) {
            return needsNumberCast(precision, scale) ? "NUMBER(38,10)" : null;
        }
        if (t.equals("FLOAT")) return "BINARY_DOUBLE";
        return null;
    }

    /** JDBC 元数据是否给不出合法的 Arrow Decimal 精度 / scale。 */
    static boolean needsNumberCast(int precision, int scale) {
        return precision <= 0 || scale < 0 || scale > precision;
    }

    // ==================== 分区探针（平行 TableScan 用） ====================

    /**
     * 单列数值主键列名（范围分区用）。仅接受 {@code NUMBER} 且 scale = 0 的单列主键 ——
     * 带小数的主键用整数区间切分会出现「两个区间之间的值落空」的静默丢行。
     * 联合主键 / 非数值 / 无主键 → empty。
     */
    @Override
    public Optional<String> getPartitionKey(String schema, String table) {
        String owner = resolveSchema(schema);
        Optional<String> resolved = lookupTable(owner, table);
        if (resolved.isEmpty()) return Optional.empty();
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT cc.column_name, tc.data_type, tc.data_scale"
                     + " FROM all_constraints c"
                     + " JOIN all_cons_columns cc ON c.owner = cc.owner"
                     + "   AND c.constraint_name = cc.constraint_name"
                     + " JOIN all_tab_columns tc ON tc.owner = cc.owner"
                     + "   AND tc.table_name = cc.table_name"
                     + "   AND tc.column_name = cc.column_name"
                     + " WHERE c.constraint_type = 'P' AND c.owner = ? AND c.table_name = ?"
                     + " ORDER BY cc.position")) {
            ps.setString(1, owner);
            ps.setString(2, resolved.get());
            try (ResultSet rs = ps.executeQuery()) {
                String pkCol = null;
                int count = 0;
                boolean rangeable = false;
                while (rs.next()) {
                    count++;
                    if (count == 1) {
                        long scale = rs.getLong(3);
                        if (rs.wasNull()) scale = 0;
                        rangeable = isRangeableKey(rs.getString(2), scale);
                        if (rangeable) pkCol = rs.getString(1);
                    }
                }
                return (count == 1 && rangeable) ? Optional.of(pkCol) : Optional.empty();
            }
        } catch (SQLException e) {
            return Optional.empty();
        }
    }

    /** 只有 NUMBER(scale = 0) 才适合整数区间切分。 */
    private static boolean isRangeableKey(String typeName, long scale) {
        if (typeName == null) return false;
        String t = typeName.trim().toUpperCase(Locale.ROOT);
        return (t.equals("NUMBER") || t.equals("INTEGER") || t.equals("INT")) && scale == 0;
    }

    /** 精确行数（{@code COUNT(*)}）。 */
    @Override
    public long getExactRowCount(String schema, String table) {
        String owner = resolveSchema(schema);
        String resolved = lookupTable(owner, table).orElse(SqlIdentifiers.unquote(table));
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT COUNT(*) FROM " + q(owner, resolved))) {
            if (rs.next()) return rs.getLong(1);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to count rows: " + owner + "." + resolved, e);
        }
        return 0;
    }

    /** 主键列 MIN/MAX（范围分区用）。 */
    @Override
    public long[] getPartitionKeyRange(String schema, String table, String pkColumn) {
        String owner = resolveSchema(schema);
        String resolved = lookupTable(owner, table).orElse(SqlIdentifiers.unquote(table));
        String key = quote(pkColumn);
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT MIN(" + key + "), MAX(" + key + ") FROM " + q(owner, resolved))) {
            if (rs.next()) {
                return new long[]{rs.getLong(1), rs.getLong(2)};
            }
        } catch (SQLException e) {
            throw new RuntimeException(
                    "Failed to get range for: " + owner + "." + resolved + "." + pkColumn, e);
        }
        return new long[]{0, 0};
    }

    // ==================== 并行扫描分区（主键 → 数值列 → OFFSET/FETCH 兜底） ====================

    /**
     * 分区策略：
     * <ol>
     *   <li><b>单列整数主键范围</b>：{@code "k" >= a AND "k" <= b} 均分；</li>
     *   <li><b>整数 NUMBER 列范围</b>：无主键时找一个 scale = 0 的 NUMBER 列按 MIN/MAX 均分；</li>
     *   <li><b>OFFSET/FETCH 均分</b>：最后兜底，每分区
     *       {@code OFFSET i*step ROWS FETCH NEXT step ROWS ONLY}（Oracle 12c+）。</li>
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

    /** 按 [min,max] 均分，每段 {@code key >= a AND key <= b}（键按 Oracle 方言加双引号）。 */
    private List<ReadRequest> splitRange(ReadRequest request, String key,
                                         long min, long max, int parallelism) {
        if (min >= max) return List.of();
        long span = max - min + 1;
        if (span <= 0) return List.of();   // long 溢出 → 放弃范围分区，走 OFFSET 兜底
        long step = span / parallelism + (span % parallelism == 0 ? 0 : 1);
        if (step <= 0) step = 1;
        String k = quote(key);
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

    /** 找第一个整数 NUMBER 列：优先 NOT NULL（最接近隐式主键），否则任意整数列。 */
    private Optional<String> findNumericColumn(String schema, String table) {
        String owner = resolveSchema(schema);
        Optional<String> resolved = lookupTable(owner, table);
        if (resolved.isEmpty()) return Optional.empty();
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT column_name FROM all_tab_columns"
                     + " WHERE owner = ? AND table_name = ? AND data_type = 'NUMBER'"
                     + "   AND (data_scale IS NULL OR data_scale = 0)"
                     + " ORDER BY CASE WHEN nullable = 'N' THEN 0 ELSE 1 END, column_id")) {
            ps.setString(1, owner);
            ps.setString(2, resolved.get());
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(rs.getString(1)) : Optional.empty();
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
        String owner = resolveSchema(request.schema());
        String resolved = lookupTable(owner, request.table())
                .orElse(SqlIdentifiers.unquote(request.table()));
        StringBuilder sql = new StringBuilder("SELECT COUNT(*) FROM ")
                .append(q(owner, resolved));
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
            for (String t : targets) cols.add(quote(t));
        } else {
            cols = new ArrayList<>();
            for (Field field : root.getSchema().getFields()) {
                cols.add(quote(field.getName()));
            }
        }
        String placeholders = String.join(", ", Collections.nCopies(colCount, "?"));
        String owner = resolveSchema(request.schema());
        String table = lookupTable(owner, request.table())
                .orElse(SqlIdentifiers.unquote(request.table()));
        String sql = "INSERT INTO " + q(owner, table)
                + " (" + String.join(", ", cols) + ") VALUES (" + placeholders + ")";

        try {
            PreparedStatement ps = session(sql).ps;   // 持久会话：复用连接 + PreparedStatement
            int rowCount = root.getRowCount();
            int batchSize = Math.max(1, CommonConfig.JDBC_WRITE_BATCH_SIZE);
            for (int row = 0; row < rowCount; row++) {
                for (int col = 0; col < colCount; col++) {
                    FieldVector vector = root.getVector(col);
                    ps.setObject(col + 1, jdbcValue(ArrowJdbcValues.toJdbc(vector, row)));
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
                    + owner + "." + table, e);
        }
    }

    /**
     * Arrow 值 → JDBC 可绑定的值。
     *
     * <p>Oracle 没有 BOOLEAN 列类型（23c 前）、也没有 TIME 类型，而 Arrow 的
     * Boolean / Time 向量会给出 {@link Boolean} / {@link LocalTime} —— 直接 {@code setObject}
     * 会绑定失败或落进错误类型。这里统一收敛成数值 / 字符串（与我们建表时的 DDL 映射一致）。</p>
     */
    static Object jdbcValue(Object value) {
        if (value instanceof Text text) return text.toString();
        if (value instanceof Boolean b) return b ? 1 : 0;
        if (value instanceof LocalTime t) return t.toString();
        if (value instanceof java.sql.Time t) return t.toString();   // Oracle 无 TIME 类型，按文本写入
        return value;
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
        // 表已存在：IF NOT EXISTS → 跳过建表（返回 false，调用方不写入）；否则让 DDL 抛
        // 标准的 ORA-00955 "name is already used by an existing object"。
        if (tableExists(catalog, schema, table)) {
            if (ifNotExists) return false;
        }
        StringBuilder sb = new StringBuilder("CREATE TABLE ")
                .append(q(resolveSchema(schema), SqlIdentifiers.unquote(table))).append(" (");
        List<String> defs = new ArrayList<>();
        for (Field f : fields) {
            defs.add(quote(f.getName()) + " " + arrowTypeToOracle(f.getType()));
        }
        sb.append(String.join(", ", defs)).append(')');

        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sb.toString());
        } catch (SQLException e) {
            throw new RuntimeException("Failed to create table: "
                    + schema + "." + table, e);
        } finally {
            invalidateCache();
        }
        return true;
    }

    @Override
    public void dropTable(String catalog, String schema, String table) {
        String owner = resolveSchema(schema);
        String resolved = lookupTable(owner, table).orElse(SqlIdentifiers.unquote(table));
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("DROP TABLE " + q(owner, resolved));
        } catch (SQLException e) {
            if (!isMissingTable(e)) {
                throw new RuntimeException("Failed to drop table: " + owner + "." + resolved, e);
            }
        } finally {
            invalidateCache();
        }
    }

    @Override
    public void truncateTable(String catalog, String schema, String table) {
        String owner = resolveSchema(schema);
        String resolved = lookupTable(owner, table).orElse(SqlIdentifiers.unquote(table));
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("TRUNCATE TABLE " + q(owner, resolved));
        } catch (SQLException e) {
            throw new RuntimeException("Failed to truncate table: " + owner + "." + resolved, e);
        }
    }

    /**
     * 字符串列 DDL 类型：{@code CLOB}。
     *
     * <p>Oracle 标准 {@code VARCHAR2} 上限 4000 字节（未开启 extended 时），大文本会撞
     * {@code ORA-01461}；CLOB 存行外 LOB、不占行内字节上限，是通用字符串落点的安全选择。</p>
     */
    private static final VarcharMapping ORACLE_VARCHAR = new VarcharMapping() {
        @Override public String resolveVarcharType(String charset) { return "CLOB"; }
        @Override public int varcharMaxBytes(String charset) { return textRowBytes(); }
        @Override public int textRowBytes() { return 16; }
    };

    @Override
    public VarcharMapping varcharMapping() {
        return ORACLE_VARCHAR;
    }

    @Override
    public Optional<String> getDefaultCharset(String schema) {
        // Oracle 的字符集是库级 / 列级 NLS 设置，没有「schema 默认字符集」概念 → 未知
        return Optional.empty();
    }

    /** Arrow 类型 → Oracle 列类型。 */
    static String arrowTypeToOracle(ArrowType type) {
        if (type instanceof ArrowType.Int i) {
            return switch (i.getBitWidth()) {
                case 8, 16 -> "NUMBER(5)";
                case 32 -> "NUMBER(10)";
                default -> "NUMBER(19)";
            };
        }
        if (type instanceof ArrowType.Utf8 || type instanceof ArrowType.LargeUtf8) {
            return "CLOB";
        }
        if (type instanceof ArrowType.FloatingPoint fp) {
            return fp.getPrecision() == FloatingPointPrecision.SINGLE
                    ? "BINARY_FLOAT" : "BINARY_DOUBLE";
        }
        // DECIMAL 必须带 (p,s)：Oracle 裸 "NUMBER" 是浮点式的无约束 NUMBER，会丢定点的精度保证
        if (type instanceof ArrowType.Decimal d) {
            int precision = Math.min(38, Math.max(1, d.getPrecision()));
            int scale = Math.min(precision, Math.max(0, d.getScale()));
            return "NUMBER(" + precision + "," + scale + ")";
        }
        if (type instanceof ArrowType.Bool) return "NUMBER(1)";
        if (type instanceof ArrowType.Date) return "DATE";
        if (type instanceof ArrowType.Time) return "VARCHAR2(32)";
        if (type instanceof ArrowType.Timestamp ts) {
            return ts.getTimezone() != null ? "TIMESTAMP WITH TIME ZONE" : "TIMESTAMP";
        }
        if (type instanceof ArrowType.FixedSizeList || type instanceof ArrowType.List
                || type instanceof ArrowType.Map || type instanceof ArrowType.Struct) {
            return "CLOB";
        }
        // 兜底：文本
        return "CLOB";
    }

    // ==================== INSERT OVERWRITE（OverwriteEnable） ====================
    //
    // Oracle 没有 MySQL 的多表 RENAME，也没有 CREATE TABLE ... LIKE 之外的克隆捷径，因此：
    // staging 用 CREATE TABLE ... AS SELECT * FROM 目标 WHERE 1=0 克隆结构 → 全部 N 分区写
    // staging → 成功后把 目标→old、staging→目标、DROP old。两条 RENAME 不是单语句原子，
    // 失败时尽力把 old 改回原名；但 staging 阶段目标表从未被触碰（全成或全不成）。

    @Override
    public boolean supportsOverwrite(String catalog, String schema, String table) {
        if (!tableExists(catalog, schema, table)) return false;
        String owner = resolveSchema(schema);
        String target = lookupTable(owner, table).orElse(SqlIdentifiers.unquote(table));
        String probe = tempName(target, "__dojao_probe_");
        String renamed = tempName(target, "__dojao_pr_");
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE TABLE " + q(owner, probe)
                    + " AS SELECT * FROM " + q(owner, target) + " WHERE 1=0");
            stmt.executeUpdate("ALTER TABLE " + q(owner, probe)
                    + " RENAME TO " + quote(renamed));
            stmt.executeUpdate("DROP TABLE " + q(owner, renamed));
            return true;
        } catch (SQLException e) {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("DROP TABLE " + q(owner, renamed));
            } catch (SQLException ignored) {
            }
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("DROP TABLE " + q(owner, probe));
            } catch (SQLException ignored) {
            }
            return false;
        }
    }

    /** 建覆盖用 staging（{@code CREATE TABLE ... AS SELECT ... WHERE 1=0} 克隆列结构），返回 staging 表名。 */
    @Override
    public String beginOverwrite(String catalog, String schema, String table) {
        String owner = resolveSchema(schema);
        String target = lookupTable(owner, table).orElse(SqlIdentifiers.unquote(table));
        String staging = tempName(target, "__dojao_tmp_");
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE TABLE " + q(owner, staging)
                    + " AS SELECT * FROM " + q(owner, target) + " WHERE 1=0");
        } catch (SQLException e) {
            throw new RuntimeException("Failed to create staging table for INSERT OVERWRITE: "
                    + owner + "." + target, e);
        } finally {
            invalidateCache();
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
        String owner = resolveSchema(schema);
        String target = lookupTable(owner, table).orElse(SqlIdentifiers.unquote(table));
        String stagingName = SqlIdentifiers.unquote(staging);
        String old = tempName(target, "__dojao_old_");
        boolean targetRenamed = false;
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("ALTER TABLE " + q(owner, target)
                    + " RENAME TO " + quote(old));
            targetRenamed = true;
            stmt.executeUpdate("ALTER TABLE " + q(owner, stagingName)
                    + " RENAME TO " + quote(target));
            stmt.executeUpdate("DROP TABLE " + q(owner, old));
        } catch (SQLException e) {
            if (targetRenamed) {
                try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate("ALTER TABLE " + q(owner, old)
                            + " RENAME TO " + quote(target));
                } catch (SQLException ignored) {
                }
            }
            throw new RuntimeException("Failed to commit INSERT OVERWRITE: "
                    + owner + "." + target, e);
        } finally {
            invalidateCache();
        }
    }

    /** 覆盖失败/取消：删 staging（原表从未被触碰，保持原样）。幂等。 */
    @Override
    public void abortOverwrite(String catalog, String schema, String table, String staging) {
        String owner = resolveSchema(schema);
        String stagingName = SqlIdentifiers.unquote(staging);
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("DROP TABLE " + q(owner, stagingName));
        } catch (SQLException e) {
            if (!isMissingTable(e)) {
                throw new RuntimeException("Failed to abort INSERT OVERWRITE staging: "
                        + owner + "." + stagingName, e);
            }
        } finally {
            invalidateCache();
        }
    }

    /** ORA-00942 = table or view does not exist。 */
    private static boolean isMissingTable(SQLException e) {
        return e.getErrorCode() == 942;
    }

    /** SQL 字符串字面量转义（单引号翻倍）。 */
    static String literal(String value) {
        return value == null ? "" : value.replace("'", "''");
    }

    /**
     * 生成 ≤ {@link #MAX_IDENTIFIER_LENGTH} 的临时表名：{@code 目标名前缀 + tag + 8 位随机}。
     * 目标名过长时截断前缀，保证生成的标识符仍可被 Oracle 接受。
     */
    static String tempName(String table, String tag) {
        String suffix = tag + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        int keep = Math.max(1, MAX_IDENTIFIER_LENGTH - suffix.length());
        String base = table == null ? "t" : table;
        if (base.length() > keep) base = base.substring(0, keep);
        return base + suffix;
    }

    // ==================== 缓存管理 ====================

    /** 使元数据缓存失效（本连接器执行 DDL 后调用；schema 解析不受影响）。 */
    public void invalidateCache() {
        this.columnCache.clear();
        this.columnMetaCache.clear();
        this.tableCache.clear();
    }

    // ==================== 私有方法 ====================

    /** 列元数据（含精度 / scale，用于判断是否需要 CAST）。 */
    private record ColumnMeta(String name, String dataType, int precision, int scale,
                              boolean nullable, String comment, String defaultValue) {
    }

    /** 加载列元数据（缓存；列注释一次批量查，避免逐列往返）。 */
    private List<ColumnMeta> loadColumnMeta(String owner, String table) {
        String key = cacheKey(owner, table);
        List<ColumnMeta> cached = columnMetaCache.get(key);
        if (cached != null) return cached;

        Map<String, String> comments = getColumnComments(owner, table);
        List<ColumnMeta> out = new ArrayList<>();
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT column_name, data_type, data_precision, data_scale,"
                     + " nullable, data_default"
                     + " FROM all_tab_columns"
                     + " WHERE owner = ? AND table_name = ? ORDER BY column_id")) {
            ps.setString(1, owner);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String name = rs.getString("COLUMN_NAME");
                    String dataType = rs.getString("DATA_TYPE");
                    int precision = rs.getInt("DATA_PRECISION");
                    if (rs.wasNull()) precision = 0;
                    int scale = rs.getInt("DATA_SCALE");
                    if (rs.wasNull()) scale = 0;
                    boolean nullable = "Y".equalsIgnoreCase(rs.getString("NULLABLE"));
                    String defaultValue = null;
                    try {
                        defaultValue = rs.getString("DATA_DEFAULT");
                    } catch (SQLException ignored) {
                        // DATA_DEFAULT 是 LONG 列，个别驱动/权限下读不到 → 留空
                    }
                    out.add(new ColumnMeta(name, dataType, precision, scale, nullable,
                            comments.get(name), defaultValue));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(
                    "Failed to get columns for: " + owner + "." + table, e);
        }
        List<ColumnMeta> frozen = List.copyOf(out);
        columnMetaCache.put(key, frozen);
        return frozen;
    }

    /** 表注释（ALL_TAB_COMMENTS.COMMENTS）。 */
    private String getTableComment(String owner, String table) {
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT comments FROM all_tab_comments"
                     + " WHERE owner = ? AND table_name = ?")) {
            ps.setString(1, owner);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String comment = rs.getString(1);
                    return (comment != null && !comment.isEmpty()) ? comment : null;
                }
            }
        } catch (SQLException e) {
            // 注释是可选信息，读不到不影响元数据
        }
        return null;
    }

    /** 列注释（一次查全表，避免逐列往返）。 */
    private Map<String, String> getColumnComments(String owner, String table) {
        Map<String, String> comments = new LinkedHashMap<>();
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT column_name, comments FROM all_col_comments"
                     + " WHERE owner = ? AND table_name = ?")) {
            ps.setString(1, owner);
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
    private Map<String, String> getTableProperties(String owner, String table) {
        Map<String, String> props = new LinkedHashMap<>();
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT num_rows FROM all_tables WHERE owner = ? AND table_name = ?")) {
            ps.setString(1, owner);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String rows = rs.getString(1);
                    if (rows != null && !rows.isEmpty()) {
                        props.put("row_count", rows);
                    }
                }
            }
        } catch (SQLException e) {
            // ignore
        }
        return props;
    }

    /** 字典里查一个名字（单参数）。 */
    private Optional<String> lookupOne(String sql, String value) {
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, value);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return Optional.ofNullable(rs.getString(1));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to resolve Oracle name: " + value, e);
        }
        return Optional.empty();
    }

    /** 解析 table 在字典里的真实名字（大小写不敏感；查不到 → empty）。 */
    private Optional<String> lookupTable(String owner, String table) {
        if (table == null || table.isBlank()) return Optional.empty();
        String input = SqlIdentifiers.unquote(table.trim());
        String cacheKey = owner.toUpperCase(Locale.ROOT) + "|" + input.toUpperCase(Locale.ROOT);
        String cached = tableCache.get(cacheKey);
        if (cached != null) return Optional.of(cached);

        Optional<String> found = lookupTableName(
                "SELECT table_name FROM all_tables WHERE owner = ? AND table_name = ?",
                owner, input);
        if (found.isEmpty()) {
            found = lookupTableName(
                    "SELECT table_name FROM all_tables"
                            + " WHERE owner = ? AND UPPER(table_name) = UPPER(?)",
                    owner, input);
        }
        found.ifPresent(name -> tableCache.put(cacheKey, name));
        return found;
    }

    private Optional<String> lookupTableName(String sql, String owner, String table) {
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, owner);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return Optional.ofNullable(rs.getString(1));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to resolve table: " + owner + "." + table, e);
        }
        return Optional.empty();
    }

    private static String cacheKey(String owner, String table) {
        return owner.toUpperCase(Locale.ROOT) + "." + table.toUpperCase(Locale.ROOT);
    }
}
