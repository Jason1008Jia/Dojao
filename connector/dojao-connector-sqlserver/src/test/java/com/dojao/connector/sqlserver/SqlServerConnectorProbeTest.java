package com.dojao.connector.sqlserver;

import com.dojao.spi.bean.DojaoType;
import com.dojao.spi.bean.ReadRequest;
import com.dojao.spi.service.ConnectorFactory;
import com.dojao.spi.service.DojaoConnector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;

/**
 * SQL Server 连接器离线探针 —— <b>不连任何数据库</b>，只断言方言拼装与类型契约。
 *
 * <p>覆盖：ServiceLoader 发现 / 工厂校验与默认值 / catalog 身份 / 方括号转义（含 {@code ]} 翻倍与
 * 幂等）/ 列投影片段（tinyint → SMALLINT）/ 片段识别 / 列名还原 / {@code buildReadSql}
 * （TOP、OFFSET-FETCH、WHERE、限定名）/ 谓词合并 / 类型映射表 / Arrow 类型 → DDL /
 * 字面量转义 / JDBC 驱动存在。</p>
 *
 * <p>运行（模块目录下）：{@code java -cp ... SqlServerConnectorProbeTest}。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/22
 */
public final class SqlServerConnectorProbeTest {

    private static int pass;
    private static int fail;

    private static final String URL =
            "jdbc:sqlserver://db.example:1433;databaseName=warehouse;encrypt=false";

    public static void main(String[] args) {
        scenario("S1  ServiceLoader 能发现 sqlserver 工厂", SqlServerConnectorProbeTest::s1Discovery);
        scenario("S2  工厂参数校验 / 默认账号 / 创建", SqlServerConnectorProbeTest::s2Factory);
        scenario("S3  catalog 身份 / 副本独立", SqlServerConnectorProbeTest::s3Catalog);
        scenario("S4  方括号转义（] 翻倍 / 各引号幂等）", SqlServerConnectorProbeTest::s4Bracket);
        scenario("S5  列投影片段（tinyint → SMALLINT、片段幂等）", SqlServerConnectorProbeTest::s5ProjectColumn);
        scenario("S6  片段识别 / 列名还原", SqlServerConnectorProbeTest::s6Resolve);
        scenario("S7  buildReadSql（TOP / OFFSET-FETCH / WHERE / 限定名）",
                SqlServerConnectorProbeTest::s7ReadSql);
        scenario("S8  谓词下推合并", SqlServerConnectorProbeTest::s8Pushdown);
        scenario("S9  SQL Server 类型 → DojaoType", SqlServerConnectorProbeTest::s9TypeMapping);
        scenario("S10 Arrow 类型 → DDL 类型", SqlServerConnectorProbeTest::s10ArrowToDdl);
        scenario("S11 字面量转义", SqlServerConnectorProbeTest::s11Literal);
        scenario("S12 JDBC 驱动随模块装配", SqlServerConnectorProbeTest::s12Driver);

        System.out.println();
        System.out.println("==== SQL Server 连接器探针: " + pass + " PASS / " + fail + " FAIL ====");
        if (fail > 0) System.exit(1);
    }

    // ==================== 场景 ====================

    private static void s1Discovery() {
        ConnectorFactory found = null;
        Iterator<ConnectorFactory> it = ServiceLoader.load(ConnectorFactory.class).iterator();
        while (it.hasNext()) {
            ConnectorFactory f = it.next();
            if ("sqlserver".equals(f.type())) found = f;
        }
        check("ServiceLoader 发现 type()=sqlserver 的工厂", found != null);
        if (found == null) return;

        check("工厂实现类正确",
                found instanceof SqlServerConnectorFactory);
        check("displayName = SQL Server", "SQL Server".equals(found.displayName()));

        Map<String, String> props = found.describeProperties();
        check("声明 3 个属性", props != null && props.size() == 3);
        check("属性键带 catalog.sqlserver. 前缀", props != null
                && props.containsKey("catalog.sqlserver.jdbcUrl")
                && props.containsKey("catalog.sqlserver.username")
                && props.containsKey("catalog.sqlserver.password"));
        check("jdbcUrl 标注必填", props != null
                && props.get("catalog.sqlserver.jdbcUrl").contains("必填"));
        check("说明里给出 encrypt 连接串提示", props != null
                && props.get("catalog.sqlserver.jdbcUrl").contains("encrypt=trustServerCertificate"));
    }

    private static void s2Factory() {
        ConnectorFactory factory = new SqlServerConnectorFactory();

        IllegalArgumentException missing = expectThrows(IllegalArgumentException.class,
                () -> factory.validate("srv", Map.of()),
                "空属性缺少 jdbcUrl → 拒绝");
        check("缺少 jdbcUrl 的报错点名属性键", missing != null
                && missing.getMessage().contains("catalog.sqlserver.jdbcUrl"));

        expectThrows(IllegalArgumentException.class,
                () -> factory.validate("srv", Map.of("catalog.sqlserver.jdbcUrl", "   ")),
                "空白 jdbcUrl → 拒绝");

        factory.validate("srv", Map.of("catalog.sqlserver.jdbcUrl", URL));

        SqlServerConnector conn = (SqlServerConnector) factory.create("srv",
                Map.of("catalog.sqlserver.jdbcUrl", URL));
        check("create 返回 SqlServerConnector", conn != null);
        check("catalogName 透传", "srv".equals(conn.getCatalogName()));
        check("jdbcUrl 透传", URL.equals(conn.getJdbcUrl()));
        check("username 默认 sa", "sa".equals(conn.getUsername()));
        check("password 默认空串", "".equals(conn.getPassword()));

        SqlServerConnector explicit = (SqlServerConnector) factory.create("srv", Map.of(
                "catalog.sqlserver.jdbcUrl", URL,
                "catalog.sqlserver.username", "reader",
                "catalog.sqlserver.password", " p@ss "));
        check("显式 username 生效", "reader".equals(explicit.getUsername()));
        check("显式 password 不 trim", " p@ss ".equals(explicit.getPassword()));

        SqlServerConnector bare = (SqlServerConnector) factory.create("srv", Map.of("jdbcUrl", URL));
        check("裸键 jdbcUrl 兼容（旧写法）", URL.equals(bare.getJdbcUrl()));
    }

    private static void s3Catalog() {
        SqlServerConnector conn = new SqlServerConnector("srv", URL, "sa", "");
        check("catalogExists 命中自身", conn.catalogExists("srv"));
        check("catalogExists 拒绝其他名字", !conn.catalogExists("other"));
        check("listCatalogs 只有自身", conn.listCatalogs().equals(List.of("srv")));
        check("getCatalog 返回类型 sqlserver",
                conn.getCatalog("srv").map(c -> "sqlserver".equals(c.type())).orElse(false));
        check("getCatalog 未知名字为空", conn.getCatalog("other").isEmpty());

        DojaoConnector copy = conn.copy();
        check("copy 返回新实例", copy != conn);
        check("copy 保留连接信息", copy instanceof SqlServerConnector s
                && URL.equals(s.getJdbcUrl()) && "srv".equals(s.getCatalogName()));
        check("copy 是独立游标实例", copy instanceof SqlServerConnector
                && conn.copy() != conn);
    }

    private static void s4Bracket() {
        eq("[id]", SqlServerConnector.bracket("id"), "普通标识符");
        eq("[my col]", SqlServerConnector.bracket("my col"), "含空格");
        eq("[a]]b]", SqlServerConnector.bracket("a]b"), "] 翻倍");
        eq("[id]", SqlServerConnector.bracket("[id]"), "[id] 幂等");
        eq("[a]]b]", SqlServerConnector.bracket("[a]]b]"), "已转义形态幂等");
        eq("[id]", SqlServerConnector.bracket("`id`"), "反引号输入先剥引号");
        eq("[id]", SqlServerConnector.bracket("\"id\""), "双引号输入先剥引号");
    }

    private static void s5ProjectColumn() {
        eq("[id]", SqlServerConnector.projectColumn("id", Set.of()), "普通列加方括号");
        eq("[id]", SqlServerConnector.projectColumn("[id]", Set.of()), "已带方括号不重复加");
        eq("CAST([tiny] AS SMALLINT) AS [tiny]",
                SqlServerConnector.projectColumn("tiny", Set.of("tiny")),
                "tinyint 列 CAST 成 SMALLINT");
        eq("CAST([TINY] AS SMALLINT) AS [TINY]",
                SqlServerConnector.projectColumn("TINY", Set.of("tiny")),
                "tinyint 判定大小写不敏感（标识符原样保留大小写）");
        eq("[tiny]", SqlServerConnector.projectColumn("tiny", Set.of("other")),
                "非 tinyint 列不加 CAST");
        eq("CAST([tiny] AS SMALLINT) AS [tiny]",
                SqlServerConnector.projectColumn("CAST([tiny] AS SMALLINT) AS [tiny]", Set.of("tiny")),
                "已是片段 → 原样返回（幂等）");
        eq("sum(x)", SqlServerConnector.projectColumn("sum(x)", Set.of()), "表达式片段原样返回");
    }

    private static void s6Resolve() {
        Set<String> known = Set.of("id", "tiny", "a]b");
        eq("id", SqlServerConnector.resolveColumnName("id", known), "裸名");
        eq("id", SqlServerConnector.resolveColumnName("[id]", known), "方括号名");
        eq("tiny", SqlServerConnector.resolveColumnName(
                "CAST([tiny] AS SMALLINT) AS [tiny]", known), "CAST 片段取 AS 别名");
        eq("a]b", SqlServerConnector.resolveColumnName("[a]]b]", known), "内嵌 ] 还原");
        eq("zzz", SqlServerConnector.resolveColumnName("zzz", known), "未知列原样返回");
        eq(null, SqlServerConnector.resolveColumnName(null, known), "null 输入");

        eq("anything", SqlServerConnector.resolveColumnName("anything", Set.of()),
                "空元数据时原样返回");
        check("looksLikeFragment 识别 CAST",
                SqlServerConnector.looksLikeFragment("CAST([a] AS INT) AS [a]"));
        check("looksLikeFragment 识别括号表达式",
                SqlServerConnector.looksLikeFragment("coalesce([a], 0)"));
        check("looksLikeFragment 不把方括号名当片段",
                !SqlServerConnector.looksLikeFragment("[a]"));
        check("looksLikeFragment null", !SqlServerConnector.looksLikeFragment(null));
    }

    private static void s7ReadSql() {
        SqlServerConnector conn = new SqlServerConnector("srv", URL, "sa", "");

        eq("SELECT [id], [name] FROM [dbo].[users]",
                conn.buildReadSql(request(List.of("[id]", "[name]"), null, 0, 0)),
                "基础投影 + 方括号限定名");

        eq("SELECT [id] FROM [dbo].[users] WHERE [id] > 10 AND [dt] = '2026-10-22'",
                conn.buildReadSql(request(List.of("[id]"), "[id] > 10 AND [dt] = '2026-10-22'", 0, 0)),
                "WHERE 原样拼入");

        eq("SELECT TOP (100) [id] FROM [dbo].[users]",
                conn.buildReadSql(request(List.of("[id]"), null, 0, 100)),
                "只有 limit → SELECT TOP");

        eq("SELECT [id] FROM [dbo].[users] WHERE [id] > 5",
                conn.buildReadSql(request(List.of("[id]"), "[id] > 5", 0, 0)),
                "limit=0 不加 TOP");

        eq("SELECT [id] FROM [dbo].[users] ORDER BY (SELECT NULL) OFFSET 20 ROWS FETCH NEXT 10 ROWS ONLY",
                conn.buildReadSql(request(List.of("[id]"), null, 20, 10)),
                "offset + limit → OFFSET/FETCH");

        eq("SELECT [id] FROM [dbo].[users] ORDER BY (SELECT NULL) OFFSET 20 ROWS",
                conn.buildReadSql(request(List.of("[id]"), null, 20, 0)),
                "只有 offset → 不带 FETCH");

        eq("SELECT [id] FROM [dbo].[users] WHERE [x] = 1"
                        + " ORDER BY (SELECT NULL) OFFSET 7 ROWS FETCH NEXT 3 ROWS ONLY",
                conn.buildReadSql(request(List.of("[id]"), "[x] = 1", 7, 3)),
                "WHERE 在 OFFSET 之前");

        eq("SELECT [a] FROM [my]]schema].[t]]bl]",
                conn.buildReadSql(new ReadRequest("srv", "my]schema", "t]bl",
                        Optional.of(List.of("[a]")), Optional.empty(), 0, 0)),
                "schema / table 内嵌 ] 也转义");

        eq("SELECT CAST([tiny] AS SMALLINT) AS [tiny], [id] FROM [dbo].[users]",
                conn.buildReadSql(request(List.of("CAST([tiny] AS SMALLINT) AS [tiny]", "[id]"), null, 0, 0)),
                "列投影片段原样拼入");
    }

    private static void s8Pushdown() {
        SqlServerConnector conn = new SqlServerConnector("srv", URL, "sa", "");
        check("声明支持谓词下推", conn.supportsPredicatePushdown());

        ReadRequest noFilter = request(List.of("[id]"), null, 0, 0);
        eq(Optional.of("[a] = 1"), conn.pushdownPredicate(noFilter, "[a] = 1").filterExpression(),
                "无既有谓词 → 直接写入");

        ReadRequest withFilter = request(List.of("[id]"), "[a] = 1", 0, 0);
        eq(Optional.of("([a] = 1) AND ([b] = 2)"),
                conn.pushdownPredicate(withFilter, "[b] = 2").filterExpression(),
                "既有谓词 → 括号合并");

        check("空白谓词不变",
                conn.pushdownPredicate(noFilter, "  ").filterExpression().isEmpty());
        check("null 谓词不变",
                conn.pushdownPredicate(noFilter, null).filterExpression().isEmpty());
    }

    private static void s9TypeMapping() {
        eq(DojaoType.SMALLINT, SqlServerConnector.dojaoType("tinyint"),
                "tinyint（无符号 0..255）→ SMALLINT（与读取投影的 CAST 一致）");
        eq(DojaoType.SMALLINT, SqlServerConnector.dojaoType("smallint"), "smallint");
        eq(DojaoType.INT, SqlServerConnector.dojaoType("int"), "int");
        eq(DojaoType.BIGINT, SqlServerConnector.dojaoType("bigint"), "bigint");
        eq(DojaoType.BOOLEAN, SqlServerConnector.dojaoType("bit"), "bit");
        eq(DojaoType.FLOAT, SqlServerConnector.dojaoType("real"), "real");
        eq(DojaoType.DOUBLE, SqlServerConnector.dojaoType("float"), "float");
        eq(DojaoType.DECIMAL, SqlServerConnector.dojaoType("decimal"), "decimal");
        eq(DojaoType.DECIMAL, SqlServerConnector.dojaoType("numeric"), "numeric");
        eq(DojaoType.DECIMAL, SqlServerConnector.dojaoType("money"), "money");
        eq(DojaoType.DECIMAL, SqlServerConnector.dojaoType("smallmoney"), "smallmoney");
        eq(DojaoType.STRING, SqlServerConnector.dojaoType("varchar"), "varchar");
        eq(DojaoType.STRING, SqlServerConnector.dojaoType("nvarchar"), "nvarchar");
        eq(DojaoType.STRING, SqlServerConnector.dojaoType("text"), "text");
        eq(DojaoType.STRING, SqlServerConnector.dojaoType("uniqueidentifier"), "uniqueidentifier");
        eq(DojaoType.DATE, SqlServerConnector.dojaoType("date"), "date");
        eq(DojaoType.STRING, SqlServerConnector.dojaoType("time"), "time → 字符串");
        eq(DojaoType.DATETIME, SqlServerConnector.dojaoType("datetime"), "datetime");
        eq(DojaoType.DATETIME, SqlServerConnector.dojaoType("datetime2"), "datetime2");
        eq(DojaoType.DATETIME, SqlServerConnector.dojaoType("smalldatetime"), "smalldatetime");
        eq(DojaoType.TIMESTAMP_TZ, SqlServerConnector.dojaoType("datetimeoffset"), "datetimeoffset");
        eq(DojaoType.STRING, SqlServerConnector.dojaoType("timestamp"),
                "timestamp = rowversion（不是时间）→ 字符串");
        eq(DojaoType.STRING, SqlServerConnector.dojaoType("rowversion"), "rowversion");
        eq(DojaoType.STRING, SqlServerConnector.dojaoType("varbinary"), "varbinary");
        eq(DojaoType.INT, SqlServerConnector.dojaoType("INT"), "大小写不敏感");
        eq(DojaoType.STRING, SqlServerConnector.dojaoType("hierarchyid"), "hierarchyid");
        eq(DojaoType.STRING, SqlServerConnector.dojaoType("geography"), "geography");
        eq(DojaoType.STRING, SqlServerConnector.dojaoType("unknown_type"), "未知类型兜底字符串");
        eq(DojaoType.STRING, SqlServerConnector.dojaoType(null), "null 兜底字符串");
    }

    private static void s10ArrowToDdl() {
        eq("SMALLINT", SqlServerConnector.arrowTypeToSqlServer(new ArrowType.Int(8, true)),
                "Int8 绝不落 TINYINT（SQL Server tinyint 无符号）");
        eq("SMALLINT", SqlServerConnector.arrowTypeToSqlServer(new ArrowType.Int(16, true)), "Int16");
        eq("INT", SqlServerConnector.arrowTypeToSqlServer(new ArrowType.Int(32, true)), "Int32");
        eq("BIGINT", SqlServerConnector.arrowTypeToSqlServer(new ArrowType.Int(64, true)), "Int64");
        eq("NVARCHAR(MAX)", SqlServerConnector.arrowTypeToSqlServer(new ArrowType.Utf8()), "Utf8");
        eq("NVARCHAR(MAX)", SqlServerConnector.arrowTypeToSqlServer(new ArrowType.LargeUtf8()),
                "LargeUtf8");
        eq("REAL", SqlServerConnector.arrowTypeToSqlServer(
                new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), "Float32");
        eq("FLOAT", SqlServerConnector.arrowTypeToSqlServer(
                new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), "Float64");
        eq("DECIMAL(38,10)", SqlServerConnector.arrowTypeToSqlServer(
                new ArrowType.Decimal(38, 10, 128)), "Decimal 带精度刻度");
        eq("BIT", SqlServerConnector.arrowTypeToSqlServer(new ArrowType.Bool()), "Bool");
        eq("DATE", SqlServerConnector.arrowTypeToSqlServer(new ArrowType.Date(
                org.apache.arrow.vector.types.DateUnit.DAY)), "Date");
        eq("TIME", SqlServerConnector.arrowTypeToSqlServer(
                new ArrowType.Time(TimeUnit.MILLISECOND, 32)), "Time");
        eq("DATETIME2", SqlServerConnector.arrowTypeToSqlServer(
                new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)), "Timestamp");
        eq("NVARCHAR(MAX)", SqlServerConnector.arrowTypeToSqlServer(new ArrowType.List()), "List");
        eq("NVARCHAR(MAX)", SqlServerConnector.arrowTypeToSqlServer(new ArrowType.Null()),
                "Null 兜底文本");
    }

    private static void s11Literal() {
        eq("O''Brien", SqlServerConnector.literal("O'Brien"), "单引号翻倍");
        eq("", SqlServerConnector.literal(null), "null → 空串");
        eq("a''b''c", SqlServerConnector.literal("a'b'c"), "多个单引号");
        eq("plain", SqlServerConnector.literal("plain"), "普通文本不变");
    }

    private static void s12Driver() {
        try {
            Class<?> driver = Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            check("SQLServerDriver 类存在", driver != null);
            boolean registered = false;
            Iterator<Driver> it = DriverManager.getDrivers().asIterator();
            while (it.hasNext()) {
                if (driver.isInstance(it.next())) registered = true;
            }
            check("SQLServerDriver 已向 DriverManager 注册", registered);
            check("驱动接受 SQL Server URL 前缀",
                    driver.getDeclaredConstructor().newInstance() instanceof Driver d
                            && d.acceptsURL("jdbc:sqlserver://h:1433;databaseName=db"));
        } catch (Exception e) {
            check("SQL Server JDBC 驱动可加载: " + e, false);
        }
    }

    // ==================== 工具 ====================

    private static ReadRequest request(List<String> columns, String filter, long offset, long limit) {
        return new ReadRequest("srv", "dbo", "users",
                Optional.of(columns),
                filter == null ? Optional.empty() : Optional.of(filter),
                offset, limit);
    }

    private static void scenario(String name, Runnable body) {
        System.out.println();
        System.out.println("---- " + name + " ----");
        try {
            body.run();
        } catch (Throwable t) {
            fail++;
            System.out.println("  [FAIL] 场景异常: " + t);
            t.printStackTrace(System.out);
        }
    }

    private static void check(String what, boolean cond) {
        if (cond) {
            pass++;
            System.out.println("  [PASS] " + what);
        } else {
            fail++;
            System.out.println("  [FAIL] " + what);
        }
    }

    private static void eq(Object expected, Object actual, String what) {
        boolean ok = expected == null ? actual == null : expected.equals(actual);
        if (ok) {
            pass++;
            System.out.println("  [PASS] " + what);
        } else {
            fail++;
            System.out.println("  [FAIL] " + what + "\n         期望: " + expected
                    + "\n         实为: " + actual);
        }
    }

    private static <T extends Throwable> T expectThrows(Class<T> type, Runnable body, String what) {
        try {
            body.run();
            fail++;
            System.out.println("  [FAIL] " + what + "（没有抛异常）");
            return null;
        } catch (Throwable t) {
            if (type.isInstance(t)) {
                pass++;
                System.out.println("  [PASS] " + what + " → " + t.getClass().getSimpleName());
                return type.cast(t);
            }
            fail++;
            System.out.println("  [FAIL] " + what + "（抛了 " + t.getClass().getName() + "）");
            return null;
        }
    }

    private SqlServerConnectorProbeTest() {
    }
}
