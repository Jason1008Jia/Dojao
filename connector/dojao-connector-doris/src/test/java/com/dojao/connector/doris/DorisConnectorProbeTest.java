package com.dojao.connector.doris;

import com.dojao.spi.bean.DojaoType;
import com.dojao.spi.bean.ReadRequest;
import com.dojao.spi.service.ConnectorFactory;
import com.dojao.spi.service.DojaoConnector;
import com.dojao.spi.util.VarcharMapping;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;

/**
 * Apache Doris 连接器离线探针 —— <b>不连任何数据库</b>，只断言方言拼装与类型契约。
 *
 * <p>覆盖：ServiceLoader 发现 / 工厂校验与默认值 / catalog 身份 / 反引号限定名 /
 * {@code buildReadSql}（投影、WHERE、LIMIT、OFFSET）/ 谓词合并 / DATA_TYPE → DojaoType /
 * Arrow 类型 → Doris DDL / DUPLICATE KEY 与分桶列选取 / 无主键契约 / VARCHAR 映射 / JDBC 驱动存在。</p>
 *
 * <p>运行（模块目录下）：{@code java -cp ... DorisConnectorProbeTest}。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/25
 */
public final class DorisConnectorProbeTest {

    private static int pass;
    private static int fail;

    private static final String URL = "jdbc:mysql://doris.example:9030/app?useSSL=false";

    public static void main(String[] args) {
        scenario("S1  ServiceLoader 能发现 doris 工厂", DorisConnectorProbeTest::s1Discovery);
        scenario("S2  工厂参数校验 / 默认账号 / 创建", DorisConnectorProbeTest::s2Factory);
        scenario("S3  catalog 身份 / 副本独立", DorisConnectorProbeTest::s3Catalog);
        scenario("S4  反引号限定名", DorisConnectorProbeTest::s4Qualified);
        scenario("S5  buildReadSql（投影 / WHERE / LIMIT / OFFSET）", DorisConnectorProbeTest::s5ReadSql);
        scenario("S6  谓词下推合并", DorisConnectorProbeTest::s6Pushdown);
        scenario("S7  DATA_TYPE → DojaoType", DorisConnectorProbeTest::s7TypeMapping);
        scenario("S8  Arrow 类型 → Doris DDL / key / 分桶列", DorisConnectorProbeTest::s8ArrowToDoris);
        scenario("S9  无主键契约 / 数值列判据 / STRING 映射", DorisConnectorProbeTest::s9PartitionAndVarchar);
        scenario("S10 JDBC 驱动随模块装配", DorisConnectorProbeTest::s10Driver);

        System.out.println();
        System.out.println("==== Doris 连接器探针: " + pass + " PASS / " + fail + " FAIL ====");
        if (fail > 0) System.exit(1);
    }

    // ==================== 场景 ====================

    private static void s1Discovery() {
        ConnectorFactory found = null;
        Iterator<ConnectorFactory> it = ServiceLoader.load(ConnectorFactory.class).iterator();
        while (it.hasNext()) {
            ConnectorFactory f = it.next();
            if ("doris".equals(f.type())) found = f;
        }
        check("ServiceLoader 发现 type()=doris 的工厂", found != null);
        if (found == null) return;

        check("工厂实现类正确", found instanceof DorisConnectorFactory);
        check("displayName = Doris", "Doris".equals(found.displayName()));

        Map<String, String> props = found.describeProperties();
        check("声明 3 个属性", props != null && props.size() == 3);
        check("属性键带 catalog.doris. 前缀", props != null
                && props.containsKey("catalog.doris.jdbcUrl")
                && props.containsKey("catalog.doris.username")
                && props.containsKey("catalog.doris.password"));
        check("jdbcUrl 标注必填", props != null
                && props.get("catalog.doris.jdbcUrl").contains("必填"));
        check("说明里给出 9030 端口", props != null
                && props.get("catalog.doris.jdbcUrl").contains("9030"));
    }

    private static void s2Factory() {
        ConnectorFactory factory = new DorisConnectorFactory();

        IllegalArgumentException missing = expectThrows(IllegalArgumentException.class,
                () -> factory.validate("srv", Map.of()),
                "空属性缺少 jdbcUrl → 拒绝");
        check("缺少 jdbcUrl 的报错点名属性键", missing != null
                && missing.getMessage().contains("catalog.doris.jdbcUrl"));

        expectThrows(IllegalArgumentException.class,
                () -> factory.validate("srv", Map.of("catalog.doris.jdbcUrl", "   ")),
                "空白 jdbcUrl → 拒绝");

        factory.validate("srv", Map.of("catalog.doris.jdbcUrl", URL));

        DorisConnector conn = (DorisConnector) factory.create("srv",
                Map.of("catalog.doris.jdbcUrl", URL));
        check("create 返回 DorisConnector", conn != null);
        check("catalogName 透传", "srv".equals(conn.getCatalogName()));
        check("jdbcUrl 透传", URL.equals(conn.getJdbcUrl()));
        check("username 默认 root", "root".equals(conn.getUsername()));
        check("password 默认空串", "".equals(conn.getPassword()));

        DorisConnector explicit = (DorisConnector) factory.create("srv", Map.of(
                "catalog.doris.jdbcUrl", URL,
                "catalog.doris.username", "reader",
                "catalog.doris.password", " p@ss "));
        check("显式 username 生效", "reader".equals(explicit.getUsername()));
        check("显式 password 不 trim", " p@ss ".equals(explicit.getPassword()));

        DorisConnector bare = (DorisConnector) factory.create("srv", Map.of("jdbcUrl", URL));
        check("裸键 jdbcUrl 兼容（旧写法）", URL.equals(bare.getJdbcUrl()));
    }

    private static void s3Catalog() {
        DorisConnector conn = new DorisConnector("srv", URL, "root", "");
        check("catalogExists 命中自身", conn.catalogExists("srv"));
        check("catalogExists 拒绝其他名字", !conn.catalogExists("other"));
        check("listCatalogs 只有自身", conn.listCatalogs().equals(List.of("srv")));
        check("getCatalog 返回类型 doris",
                conn.getCatalog("srv").map(c -> "doris".equals(c.type())).orElse(false));
        check("getCatalog 未知名字为空", conn.getCatalog("other").isEmpty());

        DojaoConnector copy = conn.copy();
        check("copy 返回新实例", copy != conn);
        check("copy 保留连接信息", copy instanceof DorisConnector c
                && URL.equals(c.getJdbcUrl()) && "srv".equals(c.getCatalogName()));
    }

    private static void s4Qualified() {
        eq("`db`.`t`", DorisConnector.q("db", "t"), "反引号限定名");
        eq("`a b`.`c`", DorisConnector.q("a b", "c"), "含空格");
    }

    private static void s5ReadSql() {
        DorisConnector conn = conn();

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
        DorisConnector conn = conn();
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
        eq(DojaoType.BOOLEAN, DorisConnector.dojaoType("bit"), "bit");
        eq(DojaoType.BOOLEAN, DorisConnector.dojaoType("boolean"), "boolean");
        eq(DojaoType.TINYINT, DorisConnector.dojaoType("tinyint"), "tinyint");
        eq(DojaoType.SMALLINT, DorisConnector.dojaoType("smallint"), "smallint");
        eq(DojaoType.INT, DorisConnector.dojaoType("int"), "int");
        eq(DojaoType.INT, DorisConnector.dojaoType("integer"), "integer → INT");
        eq(DojaoType.BIGINT, DorisConnector.dojaoType("bigint"), "bigint");
        eq(DojaoType.DECIMAL, DorisConnector.dojaoType("largeint"), "LARGEINT → DECIMAL（128 位）");
        eq(DojaoType.FLOAT, DorisConnector.dojaoType("float"), "float");
        eq(DojaoType.DOUBLE, DorisConnector.dojaoType("double"), "double");
        eq(DojaoType.DECIMAL, DorisConnector.dojaoType("decimal"), "decimal");
        eq(DojaoType.DECIMAL, DorisConnector.dojaoType("decimalv2"), "decimalv2");
        eq(DojaoType.DECIMAL, DorisConnector.dojaoType("decimalv3"), "decimalv3");
        eq(DojaoType.DECIMAL, DorisConnector.dojaoType("numeric"), "numeric");
        eq(DojaoType.STRING, DorisConnector.dojaoType("char"), "char");
        eq(DojaoType.STRING, DorisConnector.dojaoType("varchar"), "varchar");
        eq(DojaoType.STRING, DorisConnector.dojaoType("string"), "STRING");
        eq(DojaoType.STRING, DorisConnector.dojaoType("text"), "text");
        eq(DojaoType.DATE, DorisConnector.dojaoType("date"), "date");
        eq(DojaoType.DATE, DorisConnector.dojaoType("datev2"), "datev2");
        eq(DojaoType.DATETIME, DorisConnector.dojaoType("datetime"), "datetime");
        eq(DojaoType.DATETIME, DorisConnector.dojaoType("datetimev2"), "datetimev2");
        eq(DojaoType.DATETIME, DorisConnector.dojaoType("timestamp"), "timestamp → DATETIME");
        eq(DojaoType.STRING, DorisConnector.dojaoType("time"), "time → 字符串");
        eq(DojaoType.JSON, DorisConnector.dojaoType("json"), "json");
        eq(DojaoType.JSON, DorisConnector.dojaoType("jsonb"), "jsonb");
        eq(DojaoType.STRING, DorisConnector.dojaoType("array"), "array → 文本");
        eq(DojaoType.STRING, DorisConnector.dojaoType("map"), "map → 文本");
        eq(DojaoType.STRING, DorisConnector.dojaoType("struct"), "struct → 文本");
        eq(DojaoType.STRING, DorisConnector.dojaoType("variant"), "variant → 文本");
        eq(DojaoType.STRING, DorisConnector.dojaoType("bitmap"), "bitmap → 文本");
        eq(DojaoType.STRING, DorisConnector.dojaoType("hll"), "hll → 文本");
        eq(DojaoType.STRING, DorisConnector.dojaoType("ipv4"), "ipv4 → 文本");
        eq(DojaoType.STRING, DorisConnector.dojaoType("ipv6"), "ipv6 → 文本");
        eq(DojaoType.STRING, DorisConnector.dojaoType("blob"), "blob");
        eq(DojaoType.INT, DorisConnector.dojaoType("INT"), "大小写不敏感");
        eq(DojaoType.STRING, DorisConnector.dojaoType("unknown_type"), "未知类型兜底字符串");
        eq(DojaoType.STRING, DorisConnector.dojaoType(null), "null 兜底字符串");
    }

    private static void s8ArrowToDoris() {
        eq("TINYINT", DorisConnector.arrowTypeToDoris(new ArrowType.Int(8, true)), "Int8");
        eq("SMALLINT", DorisConnector.arrowTypeToDoris(new ArrowType.Int(16, true)), "Int16");
        eq("INT", DorisConnector.arrowTypeToDoris(new ArrowType.Int(32, true)), "Int32");
        eq("BIGINT", DorisConnector.arrowTypeToDoris(new ArrowType.Int(64, true)), "Int64");
        eq("STRING", DorisConnector.arrowTypeToDoris(new ArrowType.Utf8()), "Utf8 → STRING");
        eq("STRING", DorisConnector.arrowTypeToDoris(new ArrowType.LargeUtf8()),
                "LargeUtf8 → STRING");
        eq("FLOAT", DorisConnector.arrowTypeToDoris(
                new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), "Float32");
        eq("DOUBLE", DorisConnector.arrowTypeToDoris(
                new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), "Float64");
        eq("DECIMAL(38,10)", DorisConnector.arrowTypeToDoris(
                new ArrowType.Decimal(38, 10, 128)), "Decimal 带精度刻度");
        eq("BOOLEAN", DorisConnector.arrowTypeToDoris(new ArrowType.Bool()), "Bool");
        eq("DATE", DorisConnector.arrowTypeToDoris(new ArrowType.Date(DateUnit.DAY)), "Date");
        eq("DATETIME", DorisConnector.arrowTypeToDoris(
                new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)), "Timestamp");
        eq("DATETIME", DorisConnector.arrowTypeToDoris(
                new ArrowType.Time(TimeUnit.MILLISECOND, 32)), "Time → DATETIME");
        eq("STRING", DorisConnector.arrowTypeToDoris(new ArrowType.List()), "List → STRING");
        eq("STRING", DorisConnector.arrowTypeToDoris(new ArrowType.FixedSizeList(3)),
                "FixedSizeList → STRING");
        eq("STRING", DorisConnector.arrowTypeToDoris(new ArrowType.Null()), "兜底 STRING");

        // DUPLICATE KEY：key 列不支持 FLOAT/DOUBLE 与嵌套
        check("Int 可做 key", DorisConnector.keyCapable(new ArrowType.Int(64, true)));
        check("Utf8 可做 key", DorisConnector.keyCapable(new ArrowType.Utf8()));
        check("Decimal 可做 key", DorisConnector.keyCapable(new ArrowType.Decimal(38, 10, 128)));
        check("Bool 可做 key", DorisConnector.keyCapable(new ArrowType.Bool()));
        check("Date 可做 key", DorisConnector.keyCapable(new ArrowType.Date(DateUnit.DAY)));
        check("Timestamp 可做 key", DorisConnector.keyCapable(
                new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)));
        check("FLOAT/DOUBLE 不可做 key", !DorisConnector.keyCapable(
                new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
        check("嵌套类型不可做 key", !DorisConnector.keyCapable(new ArrowType.List()));

        // 分桶列：第一个可哈希的列（浮点可哈希，嵌套不行）
        eq("id", DorisConnector.distributionColumn(List.of(
                field("id", new ArrowType.Int(64, true)),
                field("name", new ArrowType.Utf8()))), "首个整型列");
        eq("score", DorisConnector.distributionColumn(List.of(
                field("nest", new ArrowType.List()),
                field("score", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)))),
                "浮点列可哈希");
        check("全为嵌套列 → null", DorisConnector.distributionColumn(List.of(
                field("a", new ArrowType.List()), field("b", new ArrowType.FixedSizeList(2))))
                == null);
        check("空列表 → null", DorisConnector.distributionColumn(List.of()) == null);
    }

    private static void s9PartitionAndVarchar() {
        DorisConnector conn = conn();

        // Doris 无主键约束：partitionKey 恒空 → 并行扫描走数值列 / LIMIT-OFFSET
        check("getPartitionKey 恒为空", conn.getPartitionKey("app", "users").isEmpty());

        check("INT 可做范围分区列", DorisConnector.isNumericType("int"));
        check("BIGINT 可做范围分区列", DorisConnector.isNumericType("bigint"));
        check("FLOAT/DOUBLE 可做范围分区列", DorisConnector.isNumericType("double"));
        check("LARGEINT 排除（long[] 可能溢出）", !DorisConnector.isNumericType("largeint"));
        check("DECIMAL 排除（long[] 可能溢出）",
                !DorisConnector.isNumericType("DECIMAL(38,10)"));
        check("字符串不是数值列", !DorisConnector.isNumericType("varchar"));
        check("null 不是数值列", !DorisConnector.isNumericType(null));

        VarcharMapping vm = DorisVarcharMapping.INSTANCE;
        eq("STRING", vm.resolveVarcharType("utf8mb4"), "任意字符集 → STRING");
        eq("STRING", vm.resolveVarcharType(null), "未知字符集 → STRING");
        eq(65533, vm.varcharMaxBytes("utf8mb4"), "行大小预算参考值");
        eq(16, vm.textRowBytes(), "行内指针 16 字节");
        check("连接器暴露 Doris 映射实现", conn.varcharMapping() instanceof DorisVarcharMapping);
    }

    private static void s10Driver() {
        try {
            Class<?> driver = Class.forName("com.mysql.cj.jdbc.Driver");
            check("MySQL 驱动类存在", driver != null);
            boolean registered = false;
            Iterator<Driver> it = DriverManager.getDrivers().asIterator();
            while (it.hasNext()) {
                if (driver.isInstance(it.next())) registered = true;
            }
            check("驱动已向 DriverManager 注册", registered);
            check("驱动接受 Doris 的 JDBC URL 前缀",
                    driver.getDeclaredConstructor().newInstance() instanceof Driver d
                            && d.acceptsURL("jdbc:mysql://h:9030/db"));
        } catch (Exception e) {
            check("Doris 复用的 MySQL JDBC 驱动可加载: " + e, false);
        }
    }

    // ==================== 工具 ====================

    private static DorisConnector conn() {
        return new DorisConnector("srv", URL, "root", "");
    }

    private static Field field(String name, ArrowType type) {
        return new Field(name, new FieldType(true, type, null), List.of());
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

    private DorisConnectorProbeTest() {
    }
}
