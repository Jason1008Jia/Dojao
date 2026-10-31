package com.dojao.connector.tidb;

import com.dojao.spi.bean.DojaoType;
import com.dojao.spi.bean.ReadRequest;
import com.dojao.spi.service.ConnectorFactory;
import com.dojao.spi.service.DojaoConnector;
import com.dojao.spi.util.VarcharMapping;
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

/**
 * TiDB 连接器离线探针 —— <b>不连任何数据库</b>，只断言方言拼装与类型契约。
 *
 * <p>覆盖：ServiceLoader 发现 / 工厂校验与默认值 / catalog 身份 / 反引号限定名 /
 * {@code buildReadSql}（投影、WHERE、LIMIT、OFFSET）/ 谓词合并 / DATA_TYPE → DojaoType （含
 * TiDB 8.4 的 {@code VECTOR}）/ Arrow 类型 → MySQL DDL / VARCHAR 行大小映射 / JDBC 驱动存在。</p>
 *
 * <p>运行（模块目录下）：{@code java -cp ... TidbConnectorProbeTest}。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/25
 */
public final class TidbConnectorProbeTest {

    private static int pass;
    private static int fail;

    private static final String URL = "jdbc:mysql://tidb.example:4000/app?useSSL=false";

    public static void main(String[] args) {
        scenario("S1  ServiceLoader 能发现 tidb 工厂", TidbConnectorProbeTest::s1Discovery);
        scenario("S2  工厂参数校验 / 默认账号 / 创建", TidbConnectorProbeTest::s2Factory);
        scenario("S3  catalog 身份 / 副本独立", TidbConnectorProbeTest::s3Catalog);
        scenario("S4  反引号限定名", TidbConnectorProbeTest::s4Qualified);
        scenario("S5  buildReadSql（投影 / WHERE / LIMIT / OFFSET）", TidbConnectorProbeTest::s5ReadSql);
        scenario("S6  谓词下推合并", TidbConnectorProbeTest::s6Pushdown);
        scenario("S7  DATA_TYPE → DojaoType", TidbConnectorProbeTest::s7TypeMapping);
        scenario("S8  Arrow 类型 → DDL / VARCHAR 行大小映射", TidbConnectorProbeTest::s8ArrowToDdl);
        scenario("S9  JDBC 驱动随模块装配", TidbConnectorProbeTest::s9Driver);

        System.out.println();
        System.out.println("==== TiDB 连接器探针: " + pass + " PASS / " + fail + " FAIL ====");
        if (fail > 0) System.exit(1);
    }

    // ==================== 场景 ====================

    private static void s1Discovery() {
        ConnectorFactory found = null;
        Iterator<ConnectorFactory> it = ServiceLoader.load(ConnectorFactory.class).iterator();
        while (it.hasNext()) {
            ConnectorFactory f = it.next();
            if ("tidb".equals(f.type())) found = f;
        }
        check("ServiceLoader 发现 type()=tidb 的工厂", found != null);
        if (found == null) return;

        check("工厂实现类正确", found instanceof TidbConnectorFactory);
        check("displayName = TiDB", "TiDB".equals(found.displayName()));

        Map<String, String> props = found.describeProperties();
        check("声明 3 个属性", props != null && props.size() == 3);
        check("属性键带 catalog.tidb. 前缀", props != null
                && props.containsKey("catalog.tidb.jdbcUrl")
                && props.containsKey("catalog.tidb.username")
                && props.containsKey("catalog.tidb.password"));
        check("jdbcUrl 标注必填", props != null
                && props.get("catalog.tidb.jdbcUrl").contains("必填"));
        check("说明里给出 4000 端口", props != null
                && props.get("catalog.tidb.jdbcUrl").contains("4000"));
    }

    private static void s2Factory() {
        ConnectorFactory factory = new TidbConnectorFactory();

        IllegalArgumentException missing = expectThrows(IllegalArgumentException.class,
                () -> factory.validate("srv", Map.of()),
                "空属性缺少 jdbcUrl → 拒绝");
        check("缺少 jdbcUrl 的报错点名属性键", missing != null
                && missing.getMessage().contains("catalog.tidb.jdbcUrl"));

        expectThrows(IllegalArgumentException.class,
                () -> factory.validate("srv", Map.of("catalog.tidb.jdbcUrl", "   ")),
                "空白 jdbcUrl → 拒绝");

        factory.validate("srv", Map.of("catalog.tidb.jdbcUrl", URL));

        TidbConnector conn = (TidbConnector) factory.create("srv",
                Map.of("catalog.tidb.jdbcUrl", URL));
        check("create 返回 TidbConnector", conn != null);
        check("catalogName 透传", "srv".equals(conn.getCatalogName()));
        check("jdbcUrl 透传", URL.equals(conn.getJdbcUrl()));
        check("username 默认 root", "root".equals(conn.getUsername()));
        check("password 默认空串", "".equals(conn.getPassword()));

        TidbConnector explicit = (TidbConnector) factory.create("srv", Map.of(
                "catalog.tidb.jdbcUrl", URL,
                "catalog.tidb.username", "reader",
                "catalog.tidb.password", " p@ss "));
        check("显式 username 生效", "reader".equals(explicit.getUsername()));
        check("显式 password 不 trim", " p@ss ".equals(explicit.getPassword()));

        TidbConnector bare = (TidbConnector) factory.create("srv", Map.of("jdbcUrl", URL));
        check("裸键 jdbcUrl 兼容（旧写法）", URL.equals(bare.getJdbcUrl()));
    }

    private static void s3Catalog() {
        TidbConnector conn = new TidbConnector("srv", URL, "root", "");
        check("catalogExists 命中自身", conn.catalogExists("srv"));
        check("catalogExists 拒绝其他名字", !conn.catalogExists("other"));
        check("listCatalogs 只有自身", conn.listCatalogs().equals(List.of("srv")));
        check("getCatalog 返回类型 tidb",
                conn.getCatalog("srv").map(c -> "tidb".equals(c.type())).orElse(false));
        check("getCatalog 未知名字为空", conn.getCatalog("other").isEmpty());

        DojaoConnector copy = conn.copy();
        check("copy 返回新实例", copy != conn);
        check("copy 保留连接信息", copy instanceof TidbConnector c
                && URL.equals(c.getJdbcUrl()) && "srv".equals(c.getCatalogName()));
    }

    private static void s4Qualified() {
        eq("`db`.`t`", TidbConnector.q("db", "t"), "反引号限定名");
        eq("`a b`.`c`", TidbConnector.q("a b", "c"), "含空格");
    }

    private static void s5ReadSql() {
        TidbConnector conn = new TidbConnector("srv", URL, "root", "");

        eq("SELECT `id`, `name` FROM app.users",
                conn.buildReadSql(request(List.of("`id`", "`name`"), null, 0, 0)),
                "基础投影");

        eq("SELECT * FROM app.users",
                conn.buildReadSql(request(List.of(), null, 0, 0)),
                "空列 → *");

        eq("SELECT `id` FROM app.users WHERE `id` > 10 AND `dt` = '2026-10-25'",
                conn.buildReadSql(request(List.of("`id`"),
                        "`id` > 10 AND `dt` = '2026-10-25'", 0, 0)),
                "WHERE 原样拼入");

        eq("SELECT `id` FROM app.users LIMIT 100",
                conn.buildReadSql(request(List.of("`id`"), null, 0, 100)),
                "LIMIT");

        eq("SELECT `id` FROM app.users LIMIT 100 OFFSET 20",
                conn.buildReadSql(request(List.of("`id`"), null, 20, 100)),
                "LIMIT + OFFSET（MySQL 语法顺序）");

        eq("SELECT `id` FROM app.users WHERE `x` = 1 LIMIT 5",
                conn.buildReadSql(request(List.of("`id`"), "`x` = 1", 0, 5)),
                "WHERE 在 LIMIT 之前");
    }

    private static void s6Pushdown() {
        TidbConnector conn = new TidbConnector("srv", URL, "root", "");
        check("声明支持谓词下推", conn.supportsPredicatePushdown());

        ReadRequest noFilter = request(List.of("`id`"), null, 0, 0);
        eq(Optional.of("`a` = 1"), conn.pushdownPredicate(noFilter, "`a` = 1").filterExpression(),
                "无既有谓词 → 直接写入");

        ReadRequest withFilter = request(List.of("`id`"), "`a` = 1", 0, 0);
        eq(Optional.of("(`a` = 1) AND (`b` = 2)"),
                conn.pushdownPredicate(withFilter, "`b` = 2").filterExpression(),
                "既有谓词 → 括号合并");

        check("空白谓词不变",
                conn.pushdownPredicate(noFilter, "  ").filterExpression().isEmpty());
        check("null 谓词不变",
                conn.pushdownPredicate(noFilter, null).filterExpression().isEmpty());
    }

    private static void s7TypeMapping() {
        eq(DojaoType.BOOLEAN, TidbConnector.dojaoType("bit"), "bit");
        eq(DojaoType.TINYINT, TidbConnector.dojaoType("tinyint"), "tinyint");
        eq(DojaoType.SMALLINT, TidbConnector.dojaoType("smallint"), "smallint");
        eq(DojaoType.INT, TidbConnector.dojaoType("mediumint"), "mediumint → INT");
        eq(DojaoType.INT, TidbConnector.dojaoType("int"), "int");
        eq(DojaoType.BIGINT, TidbConnector.dojaoType("bigint"), "bigint");
        eq(DojaoType.DECIMAL, TidbConnector.dojaoType("decimal"), "decimal");
        eq(DojaoType.FLOAT, TidbConnector.dojaoType("float"), "float");
        eq(DojaoType.DOUBLE, TidbConnector.dojaoType("double"), "double");
        eq(DojaoType.STRING, TidbConnector.dojaoType("varchar"), "varchar");
        eq(DojaoType.DATE, TidbConnector.dojaoType("date"), "date");
        eq(DojaoType.STRING, TidbConnector.dojaoType("time"), "time → 字符串");
        eq(DojaoType.DATETIME, TidbConnector.dojaoType("datetime"), "datetime");
        eq(DojaoType.TIMESTAMP, TidbConnector.dojaoType("timestamp"), "timestamp");
        eq(DojaoType.JSON, TidbConnector.dojaoType("json"), "json");
        eq(DojaoType.STRING, TidbConnector.dojaoType("vector"),
                "VECTOR（TiDB 8.4+）按文本读取");
        eq(DojaoType.STRING, TidbConnector.dojaoType("blob"), "blob");
        eq(DojaoType.INT, TidbConnector.dojaoType("INT"), "大小写不敏感");
        eq(DojaoType.STRING, TidbConnector.dojaoType("unknown_type"), "未知类型兜底字符串");
        eq(DojaoType.STRING, TidbConnector.dojaoType(null), "null 兜底字符串");
    }

    private static void s8ArrowToDdl() {
        VarcharMapping vm = TidbVarcharMapping.INSTANCE;

        eq("TINYINT", TidbConnector.arrowTypeToMysql(new ArrowType.Int(8, true),
                "utf8mb4", false, vm), "Int8");
        eq("SMALLINT", TidbConnector.arrowTypeToMysql(new ArrowType.Int(16, true),
                "utf8mb4", false, vm), "Int16");
        eq("INT", TidbConnector.arrowTypeToMysql(new ArrowType.Int(32, true),
                "utf8mb4", false, vm), "Int32");
        eq("BIGINT", TidbConnector.arrowTypeToMysql(new ArrowType.Int(64, true),
                "utf8mb4", false, vm), "Int64");
        eq("VARCHAR(16383)", TidbConnector.arrowTypeToMysql(new ArrowType.Utf8(),
                "utf8mb4", false, vm), "Utf8 + utf8mb4");
        eq("TEXT", TidbConnector.arrowTypeToMysql(new ArrowType.Utf8(),
                "utf8mb4", true, vm), "整行超限 → TEXT");
        eq("LONGTEXT", TidbConnector.arrowTypeToMysql(new ArrowType.LargeUtf8(),
                "utf8mb4", false, vm), "LargeUtf8 → LONGTEXT");
        eq("FLOAT", TidbConnector.arrowTypeToMysql(
                new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), "utf8mb4", false, vm),
                "Float32");
        eq("DOUBLE", TidbConnector.arrowTypeToMysql(
                new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), "utf8mb4", false, vm),
                "Float64");
        eq("DECIMAL(38,10)", TidbConnector.arrowTypeToMysql(
                new ArrowType.Decimal(38, 10, 128), "utf8mb4", false, vm), "Decimal 带精度刻度");
        eq("BOOLEAN", TidbConnector.arrowTypeToMysql(new ArrowType.Bool(),
                "utf8mb4", false, vm), "Bool");
        eq("DATE", TidbConnector.arrowTypeToMysql(new ArrowType.Date(
                org.apache.arrow.vector.types.DateUnit.DAY), "utf8mb4", false, vm), "Date");
        eq("DATETIME", TidbConnector.arrowTypeToMysql(
                new ArrowType.Timestamp(TimeUnit.MILLISECOND, null), "utf8mb4", false, vm),
                "Timestamp");
        eq("JSON", TidbConnector.arrowTypeToMysql(new ArrowType.List(),
                "utf8mb4", false, vm), "List → JSON");

        eq("VARCHAR(16383)", vm.resolveVarcharType("utf8mb4"), "utf8mb4 → 16383 字符");
        eq("VARCHAR(65535)", vm.resolveVarcharType("latin1"), "latin1 → 65535 字符");
        eq("TEXT", vm.resolveVarcharType(null), "未知字符集 → TEXT");
        eq(65532, vm.varcharMaxBytes("utf8mb4"), "utf8mb4 字节上限取 4 的整数倍");
        eq(16, vm.textRowBytes(), "TEXT 行内指针 16 字节");
        check("连接器暴露 TiDB 映射实现", conn().varcharMapping() instanceof TidbVarcharMapping);
    }

    private static void s9Driver() {
        try {
            Class<?> driver = Class.forName("com.mysql.cj.jdbc.Driver");
            check("MySQL 驱动类存在", driver != null);
            boolean registered = false;
            Iterator<Driver> it = DriverManager.getDrivers().asIterator();
            while (it.hasNext()) {
                if (driver.isInstance(it.next())) registered = true;
            }
            check("驱动已向 DriverManager 注册", registered);
            check("驱动接受 TiDB 的 JDBC URL 前缀",
                    driver.getDeclaredConstructor().newInstance() instanceof Driver d
                            && d.acceptsURL("jdbc:mysql://h:4000/db"));
        } catch (Exception e) {
            check("TiDB 复用的 MySQL JDBC 驱动可加载: " + e, false);
        }
    }

    // ==================== 工具 ====================

    private static TidbConnector conn() {
        return new TidbConnector("srv", URL, "root", "");
    }

    private static ReadRequest request(List<String> columns, String filter, long offset, long limit) {
        return new ReadRequest("srv", "app", "users",
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

    private TidbConnectorProbeTest() {
    }
}
