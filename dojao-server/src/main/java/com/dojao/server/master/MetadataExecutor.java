package com.dojao.server.master;

import com.dojao.core.rpc.QueryResponse;
import com.dojao.core.rpc.Row;
import com.dojao.grammar.analyzer.plan.LogicalPlan;
import com.dojao.metastore.CatalogDef;
import com.dojao.metastore.MetaStore;
import com.dojao.spi.bean.Catalog;
import com.dojao.spi.bean.Column;
import com.dojao.spi.bean.Table;
import com.dojao.spi.service.ConnectorRegistry;
import com.dojao.spi.service.DojaoConnector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * <b>元数据语句执行器</b> —— {@code SHOW *} / {@code DESCRIBE} / {@code USE} 的落地实现。
 *
 * <p>这些语句<b>不进算子链、不过 Planner</b>：它们没有扫描 / 连接 / 聚合，只是把 Dojao 注册表或
 * 连接器背后的数据源元数据以<b>普通结果集</b>（{@link QueryResponse} 的 columns + rows）返回。
 * 因此协议零改动 —— CLI / WebUI 拿到的与其他 SELECT 结果完全同构，直接当表格渲染。</p>
 *
 * <p>数据来源分两类（与两个元数据 SPI 的分工一致）：</p>
 * <ul>
 *   <li>{@code SHOW CATALOGS} —— {@link MetaStore#listRegisteredCatalogs()}，即 <b>Dojao 自己</b>
 *       注册过哪些 catalog（持久化在元数据库里，重启不丢）；</li>
 *   <li>其余语句 —— 走对应 catalog 的 {@link DojaoConnector}，即<b>数据源里</b>有什么
 *       （MySQL 查 information_schema、PostgreSQL 查 pg_catalog ……）。</li>
 * </ul>
 *
 * <p><b>catalog / schema 缺省规则</b>：单段 / 双段名字按当前 {@code USE} 值补齐（与
 * {@code TableResolver} 一致）；没有 {@code USE} 过又没写全 → 明确报错，不做猜测。</p>
 *
 * <p><b>不检查存在性的地方</b>：{@code SHOW SCHEMAS} / {@code SHOW TABLES} 对不存在的
 * schema 返回<b>空结果</b>（与连接器 {@code listSchemas/listTables} 语义一致，少一次连接探测）；
 * {@code SHOW COLUMNS} / {@code SHOW CREATE TABLE} 必须有表结构 → 表不存在直接报错。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/17
 */
public final class MetadataExecutor {

    private final MetaStore metaStore;
    /** 当前 catalog（{@code USE} 设置，可能为 null = 尚未 USE）。 */
    private final String currentCatalog;
    /** 当前 schema（{@code USE} 设置，可能为 null）。 */
    private final String currentSchema;
    /** 当前视角用户名（Web 控制台登录）；{@code null} = 全局（CLI/JDBC/admin）。用于 catalog 归属隔离。 */
    private final String viewer;

    public MetadataExecutor(MetaStore metaStore, String currentCatalog, String currentSchema) {
        this(metaStore, currentCatalog, currentSchema, null);
    }

    public MetadataExecutor(MetaStore metaStore, String currentCatalog, String currentSchema, String viewer) {
        this.metaStore = metaStore;
        this.currentCatalog = currentCatalog;
        this.currentSchema = currentSchema;
        this.viewer = viewer;
    }

    /**
     * 该逻辑计划是否由本执行器处理（SHOW CATALOGS / SCHEMAS / TABLES / COLUMNS / CREATE TABLE、
     * DESCRIBE、USE、CREATE / DROP CATALOG、DROP TABLE、TRUNCATE TABLE 共 11 种）。
     */
    public static boolean handles(LogicalPlan plan) {
        return plan instanceof LogicalPlan.ShowCatalogs
                || plan instanceof LogicalPlan.ShowSchemas
                || plan instanceof LogicalPlan.ShowTables
                || plan instanceof LogicalPlan.ShowColumns
                || plan instanceof LogicalPlan.ShowCreateTable
                || plan instanceof LogicalPlan.Describe
                || plan instanceof LogicalPlan.Use
                || plan instanceof LogicalPlan.CreateCatalog
                || plan instanceof LogicalPlan.DropCatalog
                || plan instanceof LogicalPlan.DropTable
                || plan instanceof LogicalPlan.TruncateTable;
    }

    /**
     * 执行一条元数据语句，返回单块结果集（列头 + 数据行）。
     *
     * @param queryId 查询标识（回填进响应，供监控 / 客户端对齐）
     * @param plan    语义分析 + 优化后的逻辑计划
     */
    public QueryResponse execute(String queryId, LogicalPlan plan) {
        return switch (plan) {
            case LogicalPlan.ShowCatalogs p -> showCatalogs(queryId);
            case LogicalPlan.ShowSchemas p -> showSchemas(queryId, p.catalog());
            case LogicalPlan.ShowTables p -> showTables(queryId, p.qualifiedName(), p.likePattern());
            case LogicalPlan.ShowColumns p -> showColumns(queryId, p.qualifiedName());
            case LogicalPlan.Describe p -> showColumns(queryId, p.qualifiedName());
            case LogicalPlan.ShowCreateTable p -> showCreateTable(queryId, p.qualifiedName());
            case LogicalPlan.Use p -> use(queryId, p);
            case LogicalPlan.CreateCatalog p -> createCatalog(queryId, p);
            case LogicalPlan.DropCatalog p -> dropCatalog(queryId, p);
            case LogicalPlan.DropTable p -> dropTable(queryId, p);
            case LogicalPlan.TruncateTable p -> truncateTable(queryId, p);
            default -> throw new UnsupportedOperationException(
                    "MetadataExecutor 不处理的语句: " + plan.getClass().getSimpleName());
        };
    }

    // ==================== CREATE / DROP CATALOG ====================

    /**
     * {@code CREATE CATALOG [IF NOT EXISTS] name USING type [WITH (k='v', ...)]}
     * —— 注册一个数据源，落进元数据库（重启不丢），本进程与后续所有进程即刻可用。
     *
     * <p>顺序刻意如此：<b>先</b>校验类型有对应的 {@link ConnectorFactory}，<b>再</b>构造连接器并让
     * 它用 {@code WITH} 里的属性<b>真的连一次数据源</b>（
     * {@link DojaoConnector#probeConnectivity(String)}，见
     * {@link MetaStore#registerConnector(CatalogDef)}），<b>最后</b>才落库。于是「建 catalog 成功」
     * 等价于「这个数据源现在真的能用」，不会留下连不上的死注册 —— 属性写错、url / 账号密码不对、
     * 数据源不可达，全在这一步原样抛出（连不上就什么都不留）。</p>
     *
     * <p>类型串大小写归一（{@code USING HIVE3} == {@code USING hive3}）：连接器工厂以
     * {@code hive2} / {@code hive3} 这类小写名注册。</p>
     */
    private QueryResponse createCatalog(String queryId, LogicalPlan.CreateCatalog p) {
        String name = p.name();
        String type = p.connectorType().toLowerCase(Locale.ROOT);

        if (metaStore.isCatalogRegistered(name)) {
            if (p.ifNotExists()) {
                return ddlOk(queryId, "OK（" + name + " 已存在，跳过）");
            }
            throw new IllegalStateException("Catalog '" + name + "' 已存在。"
                    + "如需重建，请先 DROP CATALOG " + name
                    + "；或改用 CREATE CATALOG IF NOT EXISTS " + name + " USING " + type);
        }
        if (!MetaStore.supportsType(type)) {
            throw new IllegalArgumentException("未知的连接器类型: '" + type + "'（catalog " + name
                    + "）。当前可用的类型: " + String.join(", ", MetaStore.registeredTypes())
                    + "；若类型没问题，请确认对应连接器 jar 在 classpath 或插件目录 "
                    + ConnectorRegistry.pluginDirDescription() + " 下"
                    + "（服务发现走 META-INF/services/com.dojao.spi.service.ConnectorFactory）");
        }
        // 先跑工厂的静态属性校验：缺必填项这种低级错误在这里就说清楚（连可用属性清单一起给出），
        // 不必等到真去连数据源时抛一个数据源侧的报错
        MetaStore.factoryFor(type).ifPresent(f -> f.validate(name, p.properties()));
        // 构造 + 真实连一次数据源（probeConnectivity）+ 落库：任一步失败即异常上抛，
        // 不留下半截注册 —— 连不上的 catalog 不会进元数据库
        metaStore.registerConnector(new CatalogDef(name, type, p.properties(), viewer));
        return ddlOk(queryId, "OK（已创建 " + name + "，类型 " + type
                + "，属性 " + p.properties() + "）");
    }

    /**
     * {@code DROP CATALOG [IF EXISTS] name} —— 从元数据库注销（连接器实例无需显式关闭，
     * 各进程的内存表会在下次 {@code getConnector} 未命中时自然失效）。
     *
     * <p>注意这里是<b>只注销注册信息</b>，不碰数据源里的任何数据。</p>
     */
    private QueryResponse dropCatalog(String queryId, LogicalPlan.DropCatalog p) {
        if (!metaStore.isCatalogRegistered(p.name())) {
            if (p.ifExists()) {
                return ddlOk(queryId, "OK（" + p.name() + " 不存在，跳过）");
            }
            // 语义分析层已拦过一道（SemanticAnalyzer.visitDropCatalogStatement），这里是执行期兜底
            throw new IllegalStateException("Catalog '" + p.name() + "' 不存在");
        }
        if (!metaStore.isCatalogVisibleTo(p.name(), viewer)) {
            throw new IllegalStateException("Catalog '" + p.name() + "' 属于其他用户，无权删除");
        }
        metaStore.unregisterConnector(p.name());
        return ddlOk(queryId, "OK（已注销 " + p.name() + "）");
    }

    // ==================== SHOW CATALOGS ====================

    /** 已注册 catalog 清单：{@code catalog} / {@code type}（连接器类型）。 */
    private QueryResponse showCatalogs(String queryId) {
        QueryResponse.Builder b = result(queryId, "catalog", "type");
        for (String name : sorted(metaStore.listRegisteredCatalogs(viewer))) {
            String type = metaStore.getRegisteredCatalog(name).map(Catalog::type).orElse("");
            b.addRows(row(name, type));
        }
        return b.build();
    }

    // ==================== SHOW SCHEMAS ====================

    /** 某 catalog 下的 schema 清单：单列 {@code schema}。 */
    private QueryResponse showSchemas(String queryId, Optional<String> explicitCatalog) {
        String catalog = requireCatalog(explicitCatalog);
        QueryResponse.Builder b = result(queryId, "schema");
        for (String schema : sorted(connector(catalog).listSchemas(catalog))) {
            b.addRows(row(schema));
        }
        return b.build();
    }

    // ==================== SHOW TABLES ====================

    /** 某 schema 下的表清单：单列 {@code table}；{@code LIKE} 通配 {@code %} / {@code _}。 */
    private QueryResponse showTables(String queryId, Optional<List<String>> qualifiedName,
                                     Optional<String> likePattern) {
        String[] cs = resolveCatalogSchema(qualifiedName);
        Pattern filter = likePattern.map(MetadataExecutor::likeToPattern).orElse(null);
        QueryResponse.Builder b = result(queryId, "table");
        for (String table : sorted(connector(cs[0]).listTables(cs[0], cs[1]))) {
            if (filter == null || filter.matcher(table).matches()) {
                b.addRows(row(table));
            }
        }
        return b.build();
    }

    // ==================== SHOW COLUMNS / DESCRIBE ====================

    /** 表结构：{@code field} / {@code type} / {@code nullable} / {@code default} / {@code comment}。 */
    private QueryResponse showColumns(String queryId, List<String> qualifiedName) {
        Table table = requireTable(qualifiedName);
        QueryResponse.Builder b = result(queryId, "field", "type", "nullable", "default", "comment");
        for (Column c : table.columns()) {
            b.addRows(row(c.name(), c.dataType(), c.nullable() ? "YES" : "NO",
                    nullToEmpty(c.defaultValue()), nullToEmpty(c.comment())));
        }
        return b.build();
    }

    // ==================== SHOW CREATE TABLE ====================

    /**
     * 表的建表语句（单列 {@code Create Table}）。列定义顺序按 Dojao 的 CREATE TABLE 语法排列：
     * {@code 名字 类型 [COMMENT '..'] [DEFAULT ..] [NOT NULL]}；表属性（engine / collation 等，
     * 属数据源自带、非 Dojao 建表属性）放在末尾块注释里，仅作展示。
     *
     * <p><b>不是无损的 {@code mysqldump}</b>：类型串直接用数据源原文（如 MySQL 的
     * {@code bigint unsigned}、{@code text}、{@code json}），Dojao 的 {@code dataType} 语法不认这些
     * 修饰 → 输出<b>可读、可参考</b>，但不保证能原样喂回解析器重建。</p>
     */
    private QueryResponse showCreateTable(String queryId, List<String> qualifiedName) {
        Table table = requireTable(qualifiedName);
        QueryResponse.Builder b = result(queryId, "Create Table");
        b.addRows(row(ddlOf(table)));
        return b.build();
    }

    private static String ddlOf(Table table) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ")
                .append(table.catalogName()).append('.').append(table.schemaName()).append('.').append(table.name())
                .append(" (\n");
        List<Column> columns = table.columns();
        for (int i = 0; i < columns.size(); i++) {
            Column c = columns.get(i);
            sb.append("  ").append(c.name()).append(' ').append(c.dataType());
            if (!nullToEmpty(c.comment()).isEmpty()) {
                sb.append(" COMMENT '").append(escapeQuote(c.comment())).append('\'');
            }
            if (!nullToEmpty(c.defaultValue()).isEmpty()) {
                sb.append(" DEFAULT ").append(c.defaultValue());
            }
            if (!c.nullable()) {
                sb.append(" NOT NULL");
            }
            if (i < columns.size() - 1) {
                sb.append(',');
            }
            sb.append('\n');
        }
        sb.append(')');
        if (!nullToEmpty(table.comment()).isEmpty()) {
            sb.append(" COMMENT '").append(escapeQuote(table.comment())).append('\'');
        }
        Map<String, String> props = table.properties();
        if (props != null && !props.isEmpty()) {
            StringBuilder p = new StringBuilder();
            for (Map.Entry<String, String> e : props.entrySet()) {
                if (p.length() > 0) {
                    p.append(", ");
                }
                p.append(e.getKey()).append('=').append(e.getValue());
            }
            sb.append("\n/* 数据源属性: ").append(p).append(" */");
        }
        return sb.toString();
    }

    // ==================== DROP TABLE / TRUNCATE TABLE ====================

    /**
     * {@code DROP TABLE [IF EXISTS] name} —— 在 <b>Master 端</b>用连接器直接删表。
     *
     * <p>Dojao <b>不保证</b>删除成功：目标数据源账号可能没有 DROP 权限（连接器原样抛出的异常
     * 会作为查询错误返回给客户端）；删成功则返回 {@code OK}。</p>
     *
     * <p>{@code IF EXISTS} 且表不存在 → 直接跳过返回 OK（不再空跑一次 DROP，也避免把
     * 「本就不存在」误报成连接器权限错误）；不带 {@code IF EXISTS} 时语义分析层已校验表存在。</p>
     */
    private QueryResponse dropTable(String queryId, LogicalPlan.DropTable p) {
        String[] cst = resolveTable(p.qualifiedName());
        DojaoConnector conn = connector(cst[0]);
        if (p.ifExists() && conn.getTable(cst[0], cst[1], cst[2]).isEmpty()) {
            return ddlOk(queryId, "OK（" + String.join(".", cst) + " 不存在，跳过）");
        }
        conn.dropTable(cst[0], cst[1], cst[2]);
        return ddlOk(queryId, "OK（已删除 " + String.join(".", cst) + "）");
    }

    /**
     * {@code TRUNCATE TABLE name} —— 在 <b>Master 端</b>用连接器清空表数据、保留表结构。
     *
     * <p>与 {@link #dropTable} 同样不保证成功：权限不足 / 表不存在等由连接器抛出的异常原样上抛，
     * 客户端收到错误；成功则返回 {@code OK}。</p>
     */
    private QueryResponse truncateTable(String queryId, LogicalPlan.TruncateTable p) {
        String[] cst = resolveTable(p.qualifiedName());
        connector(cst[0]).truncateTable(cst[0], cst[1], cst[2]);
        return ddlOk(queryId, "OK（已清空 " + String.join(".", cst) + "）");
    }

    // ==================== USE ====================

    /**
     * {@code USE catalog[.schema]} 的回执 —— 当前库的切换与 catalog/schema 存在性校验
     * 都在语义分析阶段完成（{@code SemanticAnalyzer.visitUseStatement}），执行层只回一行状态。
     */
    private QueryResponse use(String queryId, LogicalPlan.Use use) {
        QueryResponse.Builder b = result(queryId, "status", "catalog", "schema");
        b.addRows(row("OK", use.catalog(), use.schema().orElse("")));
        return b.build();
    }

    // ==================== 解析 / 工具 ====================

    /** catalog：显式指定优先，否则用当前 {@code USE} 值；都没有 → 报错（不猜）。 */
    private String requireCatalog(Optional<String> explicit) {
        if (explicit.isPresent() && !explicit.get().isBlank()) {
            return explicit.get();
        }
        if (currentCatalog == null) {
            throw new IllegalStateException(
                    "未指定 catalog：请先 USE <catalog>.<schema>，或在语句中写全 catalog.schema.table");
        }
        return currentCatalog;
    }

    private String requireSchema(String catalog) {
        if (currentSchema == null) {
            throw new IllegalStateException("未指定 schema：请先 USE " + catalog
                    + ".<schema>，或在语句中写全 catalog.schema.table");
        }
        return currentSchema;
    }

    /**
     * {@code SHOW TABLES [FROM x.y]} 的来源解析：0 段 = 当前库、1 段 = 当前 catalog 下的 schema、
     * 2 段 = catalog.schema；更多段视为写法错误。
     */
    private String[] resolveCatalogSchema(Optional<List<String>> qualifiedName) {
        if (qualifiedName.isEmpty()) {
            String catalog = requireCatalog(Optional.empty());
            return new String[]{catalog, requireSchema(catalog)};
        }
        List<String> parts = qualifiedName.get();
        return switch (parts.size()) {
            case 1 -> {
                String catalog = requireCatalog(Optional.empty());
                yield new String[]{catalog, parts.get(0)};
            }
            case 2 -> new String[]{parts.get(0), parts.get(1)};
            default -> throw new IllegalArgumentException(
                    "SHOW TABLES 的来源最多写到 catalog.schema，收到: " + String.join(".", parts));
        };
    }

    /** 表名解析：3 段直接用；2 段补当前 catalog；1 段补当前 catalog + schema。 */
    private String[] resolveTable(List<String> qualifiedName) {
        if (qualifiedName.size() == 3) {
            return new String[]{qualifiedName.get(0), qualifiedName.get(1), qualifiedName.get(2)};
        }
        String catalog = requireCatalog(Optional.empty());
        if (qualifiedName.size() == 2) {
            return new String[]{catalog, qualifiedName.get(0), qualifiedName.get(1)};
        }
        if (qualifiedName.size() == 1) {
            return new String[]{catalog, requireSchema(catalog), qualifiedName.get(0)};
        }
        throw new IllegalArgumentException("表名格式非法: '" + String.join(".", qualifiedName)
                + "'（应为 table / schema.table / catalog.schema.table）");
    }

    /** 取表结构；表不存在 → 明确报错（而不是静默返回空结果集）。 */
    private Table requireTable(List<String> qualifiedName) {
        String[] cst = resolveTable(qualifiedName);
        return connector(cst[0]).getTable(cst[0], cst[1], cst[2]).orElseThrow(() ->
                new IllegalArgumentException("表不存在: " + String.join(".", cst)));
    }

    private DojaoConnector connector(String catalog) {
        return metaStore.getConnector(catalog).orElseThrow(() ->
                new IllegalStateException("catalog 未注册或连接器未装载: " + catalog));
    }

    /** SQL LIKE 模式 → 正则：{@code %} 任意串、{@code _} 单字符，其余按字面量；大小写不敏感。 */
    private static Pattern likeToPattern(String like) {
        StringBuilder re = new StringBuilder();
        for (int i = 0; i < like.length(); i++) {
            char c = like.charAt(i);
            switch (c) {
                case '%' -> re.append(".*");
                case '_' -> re.append('.');
                default -> re.append(Pattern.quote(String.valueOf(c)));
            }
        }
        return Pattern.compile(re.toString(), Pattern.CASE_INSENSITIVE);
    }

    private static List<String> sorted(List<String> names) {
        List<String> out = new ArrayList<>(names);
        out.sort(Comparator.naturalOrder());
        return out;
    }

    private static String nullToEmpty(String s) {
        return s == null ? "" : s;
    }

    private static String escapeQuote(String s) {
        return s.replace("'", "''");
    }

    /** 结果集构造：全部列按 VARCHAR 声明（与 QueryServiceImpl 的既有做法一致，值以字符串传输）。 */
    private static QueryResponse.Builder result(String queryId, String... columnNames) {
        QueryResponse.Builder b = QueryResponse.newBuilder().setQueryId(queryId);
        for (String name : columnNames) {
            b.addColumns(com.dojao.core.rpc.Column.newBuilder().setName(name).setType("VARCHAR").build());
        }
        return b;
    }

    /** DDL 回执：单列 {@code status}、单行 {@code OK}（形状与 QueryServiceImpl.respondDdl 一致）。 */
    private static QueryResponse ddlOk(String queryId, String status) {
        QueryResponse.Builder b = result(queryId, "status");
        b.addRows(row(status));
        return b.build();
    }

    private static Row row(String... values) {
        return Row.newBuilder().addAllValues(List.of(values)).build();
    }
}
