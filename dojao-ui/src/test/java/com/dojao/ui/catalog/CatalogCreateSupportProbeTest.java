package com.dojao.ui.catalog;

import com.dojao.grammar.analyzer.SemanticAnalyzer;
import com.dojao.grammar.analyzer.plan.LogicalPlan;
import com.dojao.metastore.MetaStore;
import com.dojao.metastore.MetaStoreType;
import com.dojao.spi.service.ConnectorFactory;
import com.dojao.spi.service.ConnectorRegistry;
import com.dojao.spi.service.DojaoConnector;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 探针：{@link CatalogCreateSupport} —— 连接器卡片描述 / 表单取值 / CREATE CATALOG 生成与回读（离线）。
 *
 * <p><b>判别力</b>：去掉 {@link CatalogCreateSupport#escapeLiteral} 的单引号翻倍，
 * S4/S5 的「密码里带单引号」用例会解析失败或属性对不上；把 {@code parseDefault}
 * 的「默认空」分支删掉，密码字段默认值用例 FAIL；把 {@code effectiveValues} 的
 * required 校验去掉，S3 缺必填用例 FAIL。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/22
 */
public final class CatalogCreateSupportProbeTest {

    private static int fail = 0;

    public static void main(String[] args) {
        fieldParsing();
        connectorDescribe();
        effectiveValues();
        createSql();
        sqlRoundTrip();
        testConnection();
        registryOrdering();

        System.out.println(fail == 0 ? "[SUMMARY] PASS=all FAIL=0" : "[SUMMARY] FAIL=" + fail);
        System.exit(fail == 0 ? 0 : 1);
    }

    // ---------------------------------------------------------------- S1 字段解析

    private static void fieldParsing() {
        CatalogCreateSupport.CatalogField jdbc = CatalogCreateSupport.parseField(
                "catalog.mysql.jdbcUrl", "必填，如 jdbc:mysql://host:3306/db");
        check("jdbcUrl 完整键", "catalog.mysql.jdbcUrl".equals(jdbc.key()));
        check("jdbcUrl 裸名", "jdbcUrl".equals(jdbc.name()));
        check("jdbcUrl 标签", "JDBC 连接串".equals(jdbc.label()));
        check("jdbcUrl 必填", jdbc.required());
        check("jdbcUrl 非密码", !jdbc.secret());
        check("jdbcUrl 文本", "text".equals(jdbc.kind()));
        check("jdbcUrl 占位符", "jdbc:mysql://host:3306/db".equals(jdbc.placeholder()));
        check("jdbcUrl 帮助原文", jdbc.help().contains("必填"));

        CatalogCreateSupport.CatalogField user = CatalogCreateSupport.parseField(
                "catalog.mysql.username", "可选，默认 root");
        check("username 默认值", "root".equals(user.defaultValue()));
        check("username 标签", "用户名".equals(user.label()));
        check("username 非必填", !user.required());

        CatalogCreateSupport.CatalogField pwd = CatalogCreateSupport.parseField(
                "catalog.mysql.password", "密码，可选，默认空");
        check("password 密码框", pwd.secret());
        check("password kind", "password".equals(pwd.kind()));
        check("password 默认空", "".equals(pwd.defaultValue()));

        CatalogCreateSupport.CatalogField pruning = CatalogCreateSupport.parseField(
                "catalog.hive.bucket.pruning", "可选，默认 true");
        check("pruning 布尔", "boolean".equals(pruning.kind()));
        check("pruning 默认 true", "true".equals(pruning.defaultValue()));

        CatalogCreateSupport.CatalogField batch = CatalogCreateSupport.parseField(
                "catalog.hive.batch.size", "可选，默认 10000");
        check("batch.size 数字", "number".equals(batch.kind()));
        check("batch.size 默认", "10000".equals(batch.defaultValue()));

        CatalogCreateSupport.CatalogField sample = CatalogCreateSupport.parseField(
                "catalog.excel.sampleRows", "可选，默认 200");
        check("sampleRows 数字", "number".equals(sample.kind()));
        check("sampleRows 默认", "200".equals(sample.defaultValue()));

        CatalogCreateSupport.CatalogField unknown = CatalogCreateSupport.parseField(
                "catalog.foo.barBaz", "必填");
        check("未知键标签用属性名", "barBaz".equals(unknown.label()));
        check("未知键仍可必填", unknown.required());

        CatalogCreateSupport.CatalogField header = CatalogCreateSupport.parseField(
                "catalog.excel.header", "可选，默认 true");
        check("header 布尔", "boolean".equals(header.kind()));
        check("header 标签", "首行为列名".equals(header.label()));
    }

    // ---------------------------------------------------------------- S2 连接器描述

    private static void connectorDescribe() {
        FakeFactory mysql = new FakeFactory("mysql", "MySQL", "manual", false);
        mysql.properties.put("catalog.mysql.jdbcUrl", "必填，如 jdbc:mysql://host:3306/db");
        mysql.properties.put("catalog.mysql.username", "可选，默认 root");
        mysql.properties.put("catalog.mysql.password", "可选，默认空");

        CatalogCreateSupport.CatalogConnector card = CatalogCreateSupport.describe(mysql, null);
        check("卡片类型", "mysql".equals(card.type()));
        check("卡片展示名", "MySQL".equals(card.displayName()));
        check("卡片简介", card.description().contains("MySQL"));
        check("卡片 origin 空串", "".equals(card.origin()));
        check("卡片字段数", card.fields().size() == 3);
        check("卡片字段保序", "catalog.mysql.jdbcUrl".equals(card.fields().get(0).key()));

        FakeFactory blank = new FakeFactory("widget", "  ", "x.jar", false);
        CatalogCreateSupport.CatalogConnector fallback = CatalogCreateSupport.describe(blank, "x.jar");
        check("空展示名回落 type", "widget".equals(fallback.displayName()));
        check("未知类型简介", "widget 连接器".equals(fallback.description()));
        check("origin 透传", "x.jar".equals(fallback.origin()));
        check("无属性字段为空", fallback.fields().isEmpty());

        FakeFactory nullProps = new FakeFactory("nullish", "Nullish", "manual", false) {
            @Override
            public Map<String, String> describeProperties() {
                return null;
            }
        };
        check("describeProperties 为 null 不崩", CatalogCreateSupport.describe(nullProps, "m").fields().isEmpty());
        check("空注册表返回空列表", CatalogCreateSupport.describe((ConnectorRegistry) null).isEmpty());
    }

    // ---------------------------------------------------------------- S3 表单取值

    private static void effectiveValues() {
        FakeFactory mysql = new FakeFactory("mysql", "MySQL", "manual", false);
        mysql.properties.put("catalog.mysql.jdbcUrl", "必填");
        mysql.properties.put("catalog.mysql.username", "可选，默认 root");
        mysql.properties.put("catalog.mysql.password", "可选，默认空");
        CatalogCreateSupport.CatalogConnector card = CatalogCreateSupport.describe(mysql, "manual");

        Map<String, String> raw = new LinkedHashMap<>();
        raw.put("catalog.mysql.jdbcUrl", "  jdbc:mysql://h/db  ");
        raw.put("catalog.mysql.username", "   ");
        raw.put("catalog.mysql.password", "  p w  ");
        raw.put("catalog.mysql.bogus", "drop");
        Map<String, String> vals = CatalogCreateSupport.effectiveValues(card, raw);
        check("非密码值 trim", "jdbc:mysql://h/db".equals(vals.get("catalog.mysql.jdbcUrl")));
        check("空白可选值不提交", !vals.containsKey("catalog.mysql.username"));
        check("密码不 trim", "  p w  ".equals(vals.get("catalog.mysql.password")));
        check("未知键被忽略", !vals.containsKey("catalog.mysql.bogus"));

        Map<String, String> bare = Map.of("jdbcUrl", "jdbc:mysql://h/db");
        check("兼容裸键", "jdbc:mysql://h/db".equals(
                CatalogCreateSupport.effectiveValues(card, bare).get("catalog.mysql.jdbcUrl")));

        Map<String, String> missing = new LinkedHashMap<>();
        missing.put("catalog.mysql.username", "root");
        String err = expectThrows(() -> CatalogCreateSupport.effectiveValues(card, missing));
        check("缺必填报错", err != null && err.contains("必填项") && err.contains("JDBC 连接串"));
        check("null 入参也按缺必填报错",
                expectThrows(() -> CatalogCreateSupport.effectiveValues(card, null)) != null);
    }

    // ---------------------------------------------------------------- S4 SQL 生成

    private static void createSql() {
        check("无属性 SQL", "CREATE CATALOG c USING mysql".equals(
                CatalogCreateSupport.buildCreateSql("c", "mysql", Map.of())));

        Map<String, String> vals = new LinkedHashMap<>();
        vals.put("catalog.mysql.jdbcUrl", "jdbc:mysql://h:3306/db");
        vals.put("catalog.mysql.password", "p'w");
        String sql = CatalogCreateSupport.buildCreateSql("c", "mysql", vals);
        check("带属性 SQL 形状", sql.equals("CREATE CATALOG c USING mysql WITH ("
                + "'catalog.mysql.jdbcUrl' = 'jdbc:mysql://h:3306/db', "
                + "'catalog.mysql.password' = 'p''w')"));

        check("非法名称数字开头", expectThrows(() -> CatalogCreateSupport.requireName("1abc")) != null);
        check("非法名称连字符", expectThrows(() -> CatalogCreateSupport.requireName("my-cat")) != null);
        check("空名称", expectThrows(() -> CatalogCreateSupport.requireName("  ")) != null);
        check("名称过长", expectThrows(() -> CatalogCreateSupport.requireName("a".repeat(129))) != null);
        check("名称 128 边界可接受", "a".repeat(128).equals(
                CatalogCreateSupport.requireName("a".repeat(128))));
        check("名称两侧空白被 trim", "abc".equals(CatalogCreateSupport.requireName(" abc ")));

        check("非法类型", expectThrows(() -> CatalogCreateSupport.requireType("my type")) != null);
        check("类型空白", expectThrows(() -> CatalogCreateSupport.requireType("")) != null);
        check("合法类型", "hive3".equals(CatalogCreateSupport.requireType(" hive3 ")));

        check("值内 NUL 被拒", expectThrows(
                () -> CatalogCreateSupport.escapeLiteral("a\0b")) != null);
        check("属性键非法", expectThrows(() -> CatalogCreateSupport.buildCreateSql("c", "mysql",
                Map.of("bad key", "v"))) != null);
    }

    // ---------------------------------------------------------------- S5 SQL 回读

    private static void sqlRoundTrip() {
        Map<String, String> vals = new LinkedHashMap<>();
        vals.put("catalog.mysql.jdbcUrl", "jdbc:mysql://h:3306/db?useSSL=false");
        vals.put("catalog.mysql.username", "root");
        vals.put("catalog.mysql.password", "p'w; -- it's  ok");
        String sql = CatalogCreateSupport.buildCreateSql("mysql_prod", "mysql", vals);
        try {
            LogicalPlan plan = new SemanticAnalyzer(new MetaStore(MetaStoreType.LOCAL)).analyze(sql);
            check("回读为 CreateCatalog", plan instanceof LogicalPlan.CreateCatalog);
            LogicalPlan.CreateCatalog cc = (LogicalPlan.CreateCatalog) plan;
            check("回读名称", "mysql_prod".equals(cc.name()));
            check("回读类型", "mysql".equals(cc.connectorType()));
            check("回读 jdbcUrl", vals.get("catalog.mysql.jdbcUrl")
                    .equals(cc.properties().get("catalog.mysql.jdbcUrl")));
            check("回读密码单引号还原", vals.get("catalog.mysql.password")
                    .equals(cc.properties().get("catalog.mysql.password")));
            check("回读用户名", "root".equals(cc.properties().get("catalog.mysql.username")));

            String raw = "CREATE CATALOG x USING mysql WITH ('catalog.mysql.password' = 'a''b''''c')";
            LogicalPlan.CreateCatalog rawPlan = (LogicalPlan.CreateCatalog)
                    new SemanticAnalyzer(new MetaStore(MetaStoreType.LOCAL)).analyze(raw);
            check("手写 DDL 转义一致", "a'b''c".equals(rawPlan.properties().get("catalog.mysql.password")));
        } catch (RuntimeException e) {
            check("SQL 应可解析: " + e, false);
        }
    }

    // ---------------------------------------------------------------- S6 测试连接

    private static void testConnection() {
        List<String> events = new ArrayList<>();
        FakeFactory ok = new FakeFactory("mysql", "MySQL", "manual", false);
        ok.events = events;
        CatalogCreateSupport.testConnection(ok, "c1",
                Map.of("catalog.mysql.jdbcUrl", "jdbc:mysql://h/db"));
        check("测试连接事件顺序", events.equals(List.of(
                "validate:c1:{catalog.mysql.jdbcUrl=jdbc:mysql://h/db}",
                "create:c1", "probe:c1")));

        events.clear();
        CatalogCreateSupport.testConnection(ok, "  ", Map.of());
        check("空名称用 probe 占位", events.equals(List.of("validate:probe:{}", "create:probe", "probe:probe")));

        List<String> failEvents = new ArrayList<>();
        FakeFactory badValidate = new FakeFactory("mysql", "MySQL", "manual", false);
        badValidate.events = failEvents;
        badValidate.validateFailure = new IllegalArgumentException("缺少 jdbcUrl");
        String vErr = expectThrows(() -> CatalogCreateSupport.testConnection(badValidate, "c", Map.of()));
        check("validate 失败原样抛出", "缺少 jdbcUrl".equals(vErr));
        check("validate 失败不 create", failEvents.equals(List.of("validate:c:{}")));

        List<String> probeEvents = new ArrayList<>();
        FakeFactory badProbe = new FakeFactory("mysql", "MySQL", "manual", true);
        badProbe.events = probeEvents;
        String pErr = expectThrows(() -> CatalogCreateSupport.testConnection(badProbe, "c", Map.of()));
        check("探测失败原样抛出", "Access denied for user 'root'".equals(pErr));
        check("探测失败已 create", probeEvents.contains("create:c"));
    }

    // ---------------------------------------------------------------- S7 排序

    private static void registryOrdering() {
        ConnectorRegistry registry = ConnectorRegistry.load(null);
        registry.register(new FakeFactory("excel", "Excel", "manual", false));
        registry.register(new FakeFactory("clickhouse", "ClickHouse", "manual", false));
        registry.register(new FakeFactory("sqlserver", "SQL Server", "manual", false));
        registry.register(new FakeFactory("mysql", "MySQL", "manual", false));
        registry.register(new FakeFactory("zzz", "ZZZ", "manual", false));

        List<CatalogCreateSupport.CatalogConnector> list = CatalogCreateSupport.describe(registry);
        List<String> types = new ArrayList<>();
        list.forEach(c -> types.add(c.type()));
        int mysql = types.indexOf("mysql");
        int sqlserver = types.indexOf("sqlserver");
        int ch = types.indexOf("clickhouse");
        int excel = types.indexOf("excel");
        int zzz = types.indexOf("zzz");
        check("五个注册器都在列表", mysql >= 0 && sqlserver >= 0 && ch >= 0 && excel >= 0 && zzz >= 0);
        check("常用连接器在前（mysql < sqlserver < clickhouse < excel < 未知）",
                mysql < sqlserver && sqlserver < ch && ch < excel && excel < zzz);
        check("sqlserver 有专属副标题", list.stream()
                .filter(c -> "sqlserver".equals(c.type())).findFirst()
                .map(CatalogCreateSupport.CatalogConnector::description)
                .map(d -> d.contains("SQL Server"))
                .orElse(false));
        check("origin 来自注册表", "manual".equals(
                list.stream().filter(c -> "mysql".equals(c.type())).findFirst()
                        .map(CatalogCreateSupport.CatalogConnector::origin).orElse(null)));
    }

    // ---------------------------------------------------------------- 工具

    private static void check(String name, boolean ok) {
        if (ok) {
            System.out.println("[PASS] " + name);
        } else {
            fail++;
            System.out.println("[FAIL] " + name);
        }
    }

    private static String expectThrows(Runnable body) {
        try {
            body.run();
            return null;
        } catch (RuntimeException e) {
            return e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
        }
    }

    /** 假工厂：记录 validate/create，create 返回只认 probeConnectivity 的代理连接器。 */
    private static class FakeFactory implements ConnectorFactory {
        final String type;
        final String display;
        final String origin;
        final boolean probeFails;
        final Map<String, String> properties = new LinkedHashMap<>();
        List<String> events = new ArrayList<>();
        RuntimeException validateFailure;

        FakeFactory(String type, String display, String origin, boolean probeFails) {
            this.type = type;
            this.display = display;
            this.origin = origin;
            this.probeFails = probeFails;
        }

        @Override
        public String type() {
            return type;
        }

        @Override
        public String displayName() {
            return display;
        }

        @Override
        public Map<String, String> describeProperties() {
            return properties;
        }

        @Override
        public void validate(String catalogName, Map<String, String> props) {
            events.add("validate:" + catalogName + ":" + props);
            if (validateFailure != null) throw validateFailure;
        }

        @Override
        public DojaoConnector create(String catalogName, Map<String, String> props) {
            events.add("create:" + catalogName);
            RuntimeException failure = probeFails
                    ? new IllegalStateException("Access denied for user 'root'") : null;
            return (DojaoConnector) Proxy.newProxyInstance(
                    DojaoConnector.class.getClassLoader(),
                    new Class<?>[]{DojaoConnector.class},
                    (proxy, method, args) -> {
                        if ("probeConnectivity".equals(method.getName())) {
                            events.add("probe:" + (args == null ? null : args[0]));
                            if (failure != null) throw failure;
                            return null;
                        }
                        if ("toString".equals(method.getName())) {
                            return "FakeConnector(" + type + ")";
                        }
                        Class<?> rt = method.getReturnType();
                        if (rt == boolean.class) return false;
                        if (rt == int.class) return 0;
                        if (rt == long.class) return 0L;
                        return null;
                    });
        }
    }
}
