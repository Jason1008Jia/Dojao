package com.dojao.connector.oracle;

import com.dojao.spi.bean.DojaoType;
import com.dojao.spi.bean.ReadRequest;
import com.dojao.spi.service.ConnectorFactory;
import com.dojao.spi.service.DojaoConnector;
import com.dojao.spi.util.VarcharMapping;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.util.Text;

import java.sql.Driver;
import java.sql.DriverManager;
import java.time.LocalTime;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;

/**
 * Oracle 连接器离线探针 —— <b>不连任何数据库</b>，只断言方言拼装与类型契约。
 *
 * <p>覆盖：ServiceLoader 发现 / 工厂校验与默认值 / catalog 身份与副本 / 双引号转义（含内嵌
 * {@code "} 翻倍与幂等）/ 列投影片段（无精度 NUMBER、FLOAT → BINARY_DOUBLE）/ 片段识别 /
 * 列名还原 / {@code buildReadSql}（FETCH FIRST、OFFSET-FETCH、WHERE、限定名）/
 * 谓词合并 / Oracle 类型 → DojaoType / CAST 判定 / Arrow 类型 → DDL / 字面量与临时表名 /
 * JDBC 驱动存在。</p>
 *
 * <p>★ 表名 / 列名解析都要查数据字典（{@code ALL_TABLES} / {@code ALL_TAB_COLUMNS}），
 * 因此这里只测<b>纯函数</b>版本（{@code OracleConnector.buildReadSql(owner, table, ...)}），
 * 不调实例重载。</p>
 *
 * <p>运行（模块目录下）：{@code java -cp ... OracleConnectorProbeTest}。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/24
 */
public final class OracleConnectorProbeTest {

    private static int pass;
    private static int fail;

    private static final String URL =
            "jdbc:oracle:thin:@//db.example:1521/ORCLPDB1";

    public static void main(String[] args) {
        scenario("S1  ServiceLoader 能发现 oracle 工厂", OracleConnectorProbeTest::s1Discovery);
        scenario("S2  工厂参数校验 / 默认账号 / 创建", OracleConnectorProbeTest::s2Factory);
        scenario("S3  catalog 身份 / 副本独立 / 字符串映射", OracleConnectorProbeTest::s3Catalog);
        scenario("S4  双引号转义（\" 翻倍 / 各方言引号幂等）", OracleConnectorProbeTest::s4Quote);
        scenario("S5  列投影片段（NUMBER / FLOAT CAST、片段幂等）", OracleConnectorProbeTest::s5ProjectColumn);
        scenario("S6  片段识别 / 列名还原", OracleConnectorProbeTest::s6Resolve);
        scenario("S7  buildReadSql（FETCH FIRST / OFFSET-FETCH / WHERE / 限定名）",
                OracleConnectorProbeTest::s7ReadSql);
        scenario("S8  谓词下推合并", OracleConnectorProbeTest::s8Pushdown);
        scenario("S9  Oracle 类型 → DojaoType", OracleConnectorProbeTest::s9TypeMapping);
        scenario("S10 CAST 判定 / Arrow 类型 → DDL 类型", OracleConnectorProbeTest::s10CastAndDdl);
        scenario("S11 字面量 / 临时表名 / 写入值收敛", OracleConnectorProbeTest::s11LiteralTempName);
        scenario("S12 JDBC 驱动随模块装配", OracleConnectorProbeTest::s12Driver);

        System.out.println();
        System.out.println("==== Oracle 连接器探针: " + pass + " PASS / " + fail + " FAIL ====");
        if (fail > 0) System.exit(1);
    }

    // ==================== 场景 ====================

    private static void s1Discovery() {
        ConnectorFactory found = null;
        Iterator<ConnectorFactory> it = ServiceLoader.load(ConnectorFactory.class).iterator();
        while (it.hasNext()) {
            ConnectorFactory f = it.next();
            if ("oracle".equals(f.type())) found = f;
        }
        check("ServiceLoader 发现 type()=oracle 的工厂", found != null);
        if (found == null) return;

        check("工厂实现类正确", found instanceof OracleConnectorFactory);
        check("displayName = Oracle", "Oracle".equals(found.displayName()));

        Map<String, String> props = found.describeProperties();
        check("声明 3 个属性", props != null && props.size() == 3);
        check("属性键带 catalog.oracle. 前缀", props != null
                && props.containsKey("catalog.oracle.jdbcUrl")
                && props.containsKey("catalog.oracle.username")
                && props.containsKey("catalog.oracle.password"));
        check("jdbcUrl 标注必填", props != null
                && props.get("catalog.oracle.jdbcUrl").contains("必填"));
        check("说明里给出 thin 连接串示例", props != null
                && props.get("catalog.oracle.jdbcUrl").contains("jdbc:oracle:thin"));
        check("username 默认值写明 system", props != null
                && props.get("catalog.oracle.username").contains("system"));
    }

    private static void s2Factory() {
        ConnectorFactory factory = new OracleConnectorFactory();

        IllegalArgumentException missing = expectThrows(IllegalArgumentException.class,
                () -> factory.validate("ora", Map.of()),
                "空属性缺少 jdbcUrl → 拒绝");
        check("缺少 jdbcUrl 的报错点名属性键", missing != null
                && missing.getMessage().contains("catalog.oracle.jdbcUrl"));

        expectThrows(IllegalArgumentException.class,
                () -> factory.validate("ora", Map.of("catalog.oracle.jdbcUrl", "   ")),
                "空白 jdbcUrl → 拒绝");

        factory.validate("ora", Map.of("catalog.oracle.jdbcUrl", URL));

        OracleConnector conn = (OracleConnector) factory.create("ora",
                Map.of("catalog.oracle.jdbcUrl", URL));
        check("create 返回 OracleConnector", conn != null);
        check("catalogName 透传", "ora".equals(conn.getCatalogName()));
        check("jdbcUrl 透传", URL.equals(conn.getJdbcUrl()));
        check("username 默认 system", "system".equals(conn.getUsername()));
        check("password 默认空串", "".equals(conn.getPassword()));

        OracleConnector explicit = (OracleConnector) factory.create("ora", Map.of(
                "catalog.oracle.jdbcUrl", URL,
                "catalog.oracle.username", "scott",
                "catalog.oracle.password", " tiger "));
        check("显式 username 生效", "scott".equals(explicit.getUsername()));
        check("显式 password 不 trim", " tiger ".equals(explicit.getPassword()));

        OracleConnector bare = (OracleConnector) factory.create("ora", Map.of("jdbcUrl", URL));
        check("裸键 jdbcUrl 兼容（旧写法）", URL.equals(bare.getJdbcUrl()));
    }

    private static void s3Catalog() {
        OracleConnector conn = new OracleConnector("ora", URL, "system", "");
        check("catalogExists 命中自身", conn.catalogExists("ora"));
        check("catalogExists 拒绝其他名字", !conn.catalogExists("other"));
        check("listCatalogs 只有自身", conn.listCatalogs().equals(List.of("ora")));
        check("getCatalog 返回类型 oracle",
                conn.getCatalog("ora").map(c -> "oracle".equals(c.type())).orElse(false));
        check("getCatalog 未知名字为空", conn.getCatalog("other").isEmpty());

        DojaoConnector copy = conn.copy();
        check("copy 返回新实例", copy != conn);
        check("copy 保留连接信息", copy instanceof OracleConnector o
                && URL.equals(o.getJdbcUrl()) && "ora".equals(o.getCatalogName()));

        VarcharMapping varchar = conn.varcharMapping();
        check("字符串列 DDL = CLOB", "CLOB".equals(varchar.resolveVarcharType("UTF-8")));
        check("varchar 上限 = 行外 16 字节", varchar.varcharMaxBytes("UTF-8") == 16);
        check("没有 schema 级默认字符集", conn.getDefaultCharset("HR").isEmpty());
    }

    private static void s4Quote() {
        eq("\"id\"", OracleConnector.quote("id"), "普通标识符");
        eq("\"my col\"", OracleConnector.quote("my col"), "含空格");
        eq("\"a\"\"b\"", OracleConnector.quote("a\"b"), "内嵌 \" 翻倍");
        eq("\"id\"", OracleConnector.quote("\"id\""), "\"id\" 幂等");
        eq("\"a\"\"b\"", OracleConnector.quote("\"a\"\"b\""), "已转义形态幂等");
        eq("\"id\"", OracleConnector.quote("`id`"), "反引号输入先剥引号");
        eq("\"id\"", OracleConnector.quote("[id]"), "方括号输入先剥引号");
        eq("\"列名\"", OracleConnector.quote("列名"), "支持中文标识符");
    }

    private static void s5ProjectColumn() {
        eq("\"id\"", OracleConnector.projectColumn("id", Map.of()), "普通列加双引号");
        eq("\"id\"", OracleConnector.projectColumn("\"id\"", Map.of()), "已带双引号不重复加");

        Map<String, String> casts = Map.of(
                "AMOUNT", "NUMBER(38,10)",
                "RATIO", "BINARY_DOUBLE");
        eq("CAST(\"amount\" AS NUMBER(38,10)) AS \"amount\"",
                OracleConnector.projectColumn("amount", casts),
                "无精度 NUMBER 列 CAST 成 NUMBER(38,10)");
        eq("CAST(\"AMOUNT\" AS NUMBER(38,10)) AS \"AMOUNT\"",
                OracleConnector.projectColumn("AMOUNT", casts),
                "CAST 判定大小写不敏感（标识符原样保留大小写）");
        eq("CAST(\"ratio\" AS BINARY_DOUBLE) AS \"ratio\"",
                OracleConnector.projectColumn("ratio", casts),
                "FLOAT 列 CAST 成 BINARY_DOUBLE");
        eq("\"other\"", OracleConnector.projectColumn("other", casts),
                "不在 CAST 清单 → 原样加引号");
        eq("CAST(\"amount\" AS NUMBER(38,10)) AS \"amount\"",
                OracleConnector.projectColumn(
                        "CAST(\"amount\" AS NUMBER(38,10)) AS \"amount\"", casts),
                "已是片段 → 原样返回（幂等）");
        eq("sum(x)", OracleConnector.projectColumn("sum(x)", Map.of()), "表达式片段原样返回");
        eq(null, OracleConnector.projectColumn(null, Map.of()), "null 输入");
    }

    private static void s6Resolve() {
        Set<String> known = new java.util.LinkedHashSet<>(List.of("ID", "AMOUNT", "A\"B"));
        eq("ID", OracleConnector.resolveColumnName("ID", known), "裸名精确命中");
        eq("ID", OracleConnector.resolveColumnName("id", known), "小写输入还原成字典名");
        eq("ID", OracleConnector.resolveColumnName("\"ID\"", known), "双引号名");
        eq("ID", OracleConnector.resolveColumnName("\"id\"", known), "双引号小写名");
        eq("AMOUNT", OracleConnector.resolveColumnName(
                "CAST(\"amount\" AS NUMBER(38,10)) AS \"amount\"", known), "CAST 片段取 AS 别名");
        eq("A\"B", OracleConnector.resolveColumnName("\"A\"\"B\"", known), "内嵌 \" 还原");
        eq("ID", OracleConnector.resolveColumnName("COALESCE(\"ID\", 0)", known),
                "无 AS 别名时取最后一段双引号标识符");
        eq("zzz", OracleConnector.resolveColumnName("zzz", known), "未知列原样返回");
        eq(null, OracleConnector.resolveColumnName(null, known), "null 输入");

        eq("anything", OracleConnector.resolveColumnName("anything", Set.of()),
                "空元数据时原样返回");
        check("looksLikeFragment 识别 CAST",
                OracleConnector.looksLikeFragment("CAST(\"a\" AS NUMBER(38,10)) AS \"a\""));
        check("looksLikeFragment 识别括号表达式",
                OracleConnector.looksLikeFragment("coalesce(\"a\", 0)"));
        check("looksLikeFragment 不把带引号名当片段",
                !OracleConnector.looksLikeFragment("\"a\""));
        check("looksLikeFragment null", !OracleConnector.looksLikeFragment(null));
    }

    private static void s7ReadSql() {
        eq("SELECT \"ID\", \"NAME\" FROM \"HR\".\"EMPLOYEES\"",
                OracleConnector.buildReadSql("HR", "EMPLOYEES", "\"ID\", \"NAME\"", null, 0, 0),
                "基础投影 + 双引号限定名");

        eq("SELECT \"ID\" FROM \"HR\".\"EMPLOYEES\" WHERE \"ID\" > 10 AND \"DT\" = '2026-10-24'",
                OracleConnector.buildReadSql("HR", "EMPLOYEES", "\"ID\"",
                        "\"ID\" > 10 AND \"DT\" = '2026-10-24'", 0, 0),
                "WHERE 原样拼入");

        eq("SELECT \"ID\" FROM \"HR\".\"EMPLOYEES\" FETCH FIRST 100 ROWS ONLY",
                OracleConnector.buildReadSql("HR", "EMPLOYEES", "\"ID\"", null, 0, 100),
                "只有 limit → FETCH FIRST");

        eq("SELECT \"ID\" FROM \"HR\".\"EMPLOYEES\"",
                OracleConnector.buildReadSql("HR", "EMPLOYEES", "\"ID\"", null, 0, 0),
                "limit=0 不分页");

        eq("SELECT \"ID\" FROM \"HR\".\"EMPLOYEES\" OFFSET 20 ROWS FETCH NEXT 10 ROWS ONLY",
                OracleConnector.buildReadSql("HR", "EMPLOYEES", "\"ID\"", null, 20, 10),
                "offset + limit → OFFSET/FETCH");

        eq("SELECT \"ID\" FROM \"HR\".\"EMPLOYEES\" OFFSET 20 ROWS",
                OracleConnector.buildReadSql("HR", "EMPLOYEES", "\"ID\"", null, 20, 0),
                "只有 offset → 不带 FETCH");

        eq("SELECT \"ID\" FROM \"HR\".\"EMPLOYEES\" WHERE \"X\" = 1"
                        + " OFFSET 7 ROWS FETCH NEXT 3 ROWS ONLY",
                OracleConnector.buildReadSql("HR", "EMPLOYEES", "\"ID\"", "\"X\" = 1", 7, 3),
                "WHERE 在 OFFSET 之前");

        eq("SELECT \"A\" FROM \"my\"\"schema\".\"t\"\"bl\"",
                OracleConnector.buildReadSql("my\"schema", "t\"bl", "\"A\"", null, 0, 0),
                "schema / table 内嵌 \" 也转义");

        eq("SELECT CAST(\"AMOUNT\" AS NUMBER(38,10)) AS \"AMOUNT\", \"ID\" FROM \"HR\".\"EMPLOYEES\"",
                OracleConnector.buildReadSql("HR", "EMPLOYEES",
                        "CAST(\"AMOUNT\" AS NUMBER(38,10)) AS \"AMOUNT\", \"ID\"", null, 0, 0),
                "列投影片段原样拼入");
    }

    private static void s8Pushdown() {
        OracleConnector conn = new OracleConnector("ora", URL, "system", "");
        check("声明支持谓词下推", conn.supportsPredicatePushdown());

        ReadRequest noFilter = request(List.of("\"ID\""), null, 0, 0);
        eq(Optional.of("\"A\" = 1"), conn.pushdownPredicate(noFilter, "\"A\" = 1").filterExpression(),
                "无既有谓词 → 直接写入");

        ReadRequest withFilter = request(List.of("\"ID\""), "\"A\" = 1", 0, 0);
        eq(Optional.of("(\"A\" = 1) AND (\"B\" = 2)"),
                conn.pushdownPredicate(withFilter, "\"B\" = 2").filterExpression(),
                "既有谓词 → 括号合并");

        check("空白谓词不变",
                conn.pushdownPredicate(noFilter, "  ").filterExpression().isEmpty());
        check("null 谓词不变",
                conn.pushdownPredicate(noFilter, null).filterExpression().isEmpty());
    }

    private static void s9TypeMapping() {
        eq(DojaoType.DECIMAL, OracleConnector.dojaoType("NUMBER"), "NUMBER → DECIMAL");
        eq(DojaoType.DECIMAL, OracleConnector.dojaoType("numeric"), "ANSI numeric 同义词");
        eq(DojaoType.DECIMAL, OracleConnector.dojaoType("decimal"), "ANSI decimal 同义词");
        eq(DojaoType.DECIMAL, OracleConnector.dojaoType("integer"), "ANSI integer 同义词");
        eq(DojaoType.DECIMAL, OracleConnector.dojaoType("int"), "int 同义词");
        eq(DojaoType.DOUBLE, OracleConnector.dojaoType("FLOAT"),
                "FLOAT 是二进制精度 NUMBER → DOUBLE（读取时 CAST 成 BINARY_DOUBLE）");
        eq(DojaoType.FLOAT, OracleConnector.dojaoType("binary_float"), "BINARY_FLOAT");
        eq(DojaoType.DOUBLE, OracleConnector.dojaoType("binary_double"), "BINARY_DOUBLE");
        eq(DojaoType.STRING, OracleConnector.dojaoType("char"), "char");
        eq(DojaoType.STRING, OracleConnector.dojaoType("nchar"), "nchar");
        eq(DojaoType.STRING, OracleConnector.dojaoType("varchar2"), "varchar2");
        eq(DojaoType.STRING, OracleConnector.dojaoType("nvarchar2"), "nvarchar2");
        eq(DojaoType.STRING, OracleConnector.dojaoType("varchar"), "varchar");
        eq(DojaoType.STRING, OracleConnector.dojaoType("long"), "long");
        eq(DojaoType.STRING, OracleConnector.dojaoType("clob"), "clob");
        eq(DojaoType.STRING, OracleConnector.dojaoType("nclob"), "nclob");
        eq(DojaoType.STRING, OracleConnector.dojaoType("rowid"), "rowid");
        eq(DojaoType.STRING, OracleConnector.dojaoType("urowid"), "urowid");
        eq(DojaoType.STRING, OracleConnector.dojaoType("xmltype"), "xmltype");
        eq(DojaoType.STRING, OracleConnector.dojaoType("json"), "json");
        eq(DojaoType.STRING, OracleConnector.dojaoType("blob"), "blob → 字符串（无二进制枚举）");
        eq(DojaoType.STRING, OracleConnector.dojaoType("raw"), "raw → 字符串");
        eq(DojaoType.STRING, OracleConnector.dojaoType("long raw"), "long raw → 字符串");
        eq(DojaoType.STRING, OracleConnector.dojaoType("bfile"), "bfile → 字符串");
        eq(DojaoType.DATETIME, OracleConnector.dojaoType("DATE"),
                "DATE 带时分秒 → DATETIME（不是 DATE）");
        eq(DojaoType.DATETIME, OracleConnector.dojaoType("timestamp"), "timestamp");
        eq(DojaoType.TIMESTAMP_TZ, OracleConnector.dojaoType("TIMESTAMP WITH TIME ZONE"),
                "带时区 timestamp");
        eq(DojaoType.TIMESTAMP_TZ, OracleConnector.dojaoType("TIMESTAMP WITH LOCAL TIME ZONE"),
                "带本地时区 timestamp");
        eq(DojaoType.STRING, OracleConnector.dojaoType("INTERVAL DAY TO SECOND"), "interval day");
        eq(DojaoType.STRING, OracleConnector.dojaoType("INTERVAL YEAR TO MONTH"), "interval year");
        eq(DojaoType.BOOLEAN, OracleConnector.dojaoType("boolean"), "23c 原生 BOOLEAN");
        eq(DojaoType.STRING, OracleConnector.dojaoType("unknown_type"), "未知类型兜底字符串");
        eq(DojaoType.STRING, OracleConnector.dojaoType(null), "null 兜底字符串");
    }

    private static void s10CastAndDdl() {
        check("无精度 NUMBER 需要 CAST", OracleConnector.needsNumberCast(0, 0));
        check("负 scale 需要 CAST", OracleConnector.needsNumberCast(10, -2));
        check("scale > precision 需要 CAST", OracleConnector.needsNumberCast(5, 7));
        check("合法 (p,s) 不需要 CAST", !OracleConnector.needsNumberCast(10, 2));

        eq("NUMBER(38,10)", OracleConnector.castTarget("NUMBER", 0, 0), "无精度 NUMBER");
        eq("NUMBER(38,10)", OracleConnector.castTarget("number", 0, 0), "大小写不敏感");
        eq("NUMBER(38,10)", OracleConnector.castTarget("INTEGER", 0, 0), "INTEGER 同义词");
        eq(null, OracleConnector.castTarget("NUMBER", 10, 2), "合法 NUMBER 不 CAST");
        eq("BINARY_DOUBLE", OracleConnector.castTarget("FLOAT", 126, 0), "FLOAT → BINARY_DOUBLE");
        eq(null, OracleConnector.castTarget("VARCHAR2", 0, 0), "字符串不 CAST");
        eq(null, OracleConnector.castTarget(null, 0, 0), "null 类型不 CAST");

        eq("NUMBER(5)", OracleConnector.arrowTypeToOracle(new ArrowType.Int(8, true)), "Int8");
        eq("NUMBER(5)", OracleConnector.arrowTypeToOracle(new ArrowType.Int(16, true)), "Int16");
        eq("NUMBER(10)", OracleConnector.arrowTypeToOracle(new ArrowType.Int(32, true)), "Int32");
        eq("NUMBER(19)", OracleConnector.arrowTypeToOracle(new ArrowType.Int(64, true)), "Int64");
        eq("CLOB", OracleConnector.arrowTypeToOracle(new ArrowType.Utf8()), "Utf8 → CLOB");
        eq("CLOB", OracleConnector.arrowTypeToOracle(new ArrowType.LargeUtf8()), "LargeUtf8 → CLOB");
        eq("BINARY_FLOAT", OracleConnector.arrowTypeToOracle(
                new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), "Float32");
        eq("BINARY_DOUBLE", OracleConnector.arrowTypeToOracle(
                new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), "Float64");
        eq("NUMBER(38,10)", OracleConnector.arrowTypeToOracle(
                new ArrowType.Decimal(38, 10, 128)), "Decimal 带精度刻度");
        eq("NUMBER(38,2)", OracleConnector.arrowTypeToOracle(
                new ArrowType.Decimal(40, 2, 128)), "Decimal 精度夹紧到 Oracle 上限 38");
        eq("NUMBER(1)", OracleConnector.arrowTypeToOracle(new ArrowType.Bool()), "Bool");
        eq("DATE", OracleConnector.arrowTypeToOracle(new ArrowType.Date(DateUnit.DAY)), "Date");
        eq("VARCHAR2(32)", OracleConnector.arrowTypeToOracle(
                new ArrowType.Time(TimeUnit.MILLISECOND, 32)), "Time → 字符串（Oracle 无 TIME）");
        eq("TIMESTAMP", OracleConnector.arrowTypeToOracle(
                new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)), "Timestamp");
        eq("TIMESTAMP WITH TIME ZONE", OracleConnector.arrowTypeToOracle(
                new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")), "带时区 Timestamp");
        eq("CLOB", OracleConnector.arrowTypeToOracle(new ArrowType.List()), "List → CLOB");
        eq("CLOB", OracleConnector.arrowTypeToOracle(new ArrowType.Null()), "Null 兜底文本");
    }

    private static void s11LiteralTempName() {
        eq("O''Brien", OracleConnector.literal("O'Brien"), "单引号翻倍");
        eq("", OracleConnector.literal(null), "null → 空串");
        eq("a''b''c", OracleConnector.literal("a'b'c"), "多个单引号");
        eq("plain", OracleConnector.literal("plain"), "普通文本不变");

        String staging = OracleConnector.tempName("users", "__dojao_tmp_");
        check("临时名带目标表前缀", staging.startsWith("users__dojao_tmp_"));
        check("临时名 ≤ 128（Oracle 标识符上限）",
                staging.length() <= OracleConnector.MAX_IDENTIFIER_LENGTH);
        check("临时名带 8 位随机后缀",
                staging.substring("users__dojao_tmp_".length()).matches("[0-9a-f]{8}"));
        check("两次生成不同（避免并发覆盖）",
                !staging.equals(OracleConnector.tempName("users", "__dojao_tmp_")));

        String longBase = "x".repeat(200);
        String truncated = OracleConnector.tempName(longBase, "__dojao_old_");
        check("超长表名被截断到 128 内",
                truncated.length() <= OracleConnector.MAX_IDENTIFIER_LENGTH);
        check("截断后仍带 tag 与 8 位随机后缀",
                truncated.matches("x+__dojao_old_[0-9a-f]{8}"));
        check("null 表名用 t 兜底",
                OracleConnector.tempName(null, "__dojao_pr_").startsWith("t__dojao_pr_"));

        eq("abc", OracleConnector.jdbcValue(new Text("abc")), "Arrow Text → String");
        eq(1, OracleConnector.jdbcValue(Boolean.TRUE), "Boolean true → 1");
        eq(0, OracleConnector.jdbcValue(Boolean.FALSE), "Boolean false → 0");
        eq("12:34:56", OracleConnector.jdbcValue(LocalTime.of(12, 34, 56)),
                "LocalTime → 字符串（Oracle 无 TIME）");
        eq(42L, OracleConnector.jdbcValue(42L), "其他值原样透传");
    }

    private static void s12Driver() {
        try {
            Class<?> driver = Class.forName("oracle.jdbc.OracleDriver");
            check("OracleDriver 类存在", driver != null);
            boolean registered = false;
            Iterator<Driver> it = DriverManager.getDrivers().asIterator();
            while (it.hasNext()) {
                if (driver.isInstance(it.next())) registered = true;
            }
            check("OracleDriver 已向 DriverManager 注册", registered);
            check("驱动接受 Oracle thin URL",
                    driver.getDeclaredConstructor().newInstance() instanceof Driver d
                            && d.acceptsURL("jdbc:oracle:thin:@//h:1521/ORCLPDB1"));
        } catch (Exception e) {
            check("Oracle JDBC 驱动可加载: " + e, false);
        }
    }

    // ==================== 工具 ====================

    private static ReadRequest request(List<String> columns, String filter, long offset, long limit) {
        return new ReadRequest("ora", "HR", "EMPLOYEES",
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

    private OracleConnectorProbeTest() {
    }
}
