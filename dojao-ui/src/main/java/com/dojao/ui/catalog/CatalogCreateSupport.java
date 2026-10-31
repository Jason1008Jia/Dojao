package com.dojao.ui.catalog;

import com.dojao.spi.service.ConnectorFactory;
import com.dojao.spi.service.ConnectorRegistry;
import com.dojao.spi.service.DojaoConnector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 「新建 Catalog」向导的后端纯逻辑 —— 连接器清单、表单字段描述、SQL 生成与取值校验。
 *
 * <p>这一层刻意不碰 HTTP / Javalin：{@link #describe(ConnectorRegistry)} 与
 * {@link #buildCreateSql} 都是纯函数，探针可以离线直接调；真正执行 DDL 的提交与轮询
 * 留在 {@code DojaoUiApp} 的 REST 处理器里。</p>
 *
 * <p><b>字段从哪来</b>：连接器工厂的 {@link ConnectorFactory#describeProperties()}
 * 返回「完整属性键 → 中文说明」。说明文本本身已经带了「必填 / 可选，默认 X」以及
 * 「如 jdbc:mysql://host:3306/db」这类示例，这里把它解析成结构化表单字段
 * （必填、密码、布尔、数字、默认值、占位符），前端据此渲染，不再维护第二份连接器配置清单。</p>
 *
 * <p><b>为什么建 SQL 而不直接调 {@code MetaStore.registerConnector}</b>：用户明确要求
 * 「创建时后台执行 CREATE CATALOG 的 SQL」—— 走同一条 SQL 执行线，权限 / 校验 / 连接探针 /
 * 元数据落库的顺序与手写 DDL 完全一致，不会出现「UI 建出来的 catalog 和 SQL 建的不一样」。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/22
 */
public final class CatalogCreateSupport {

    /** {@code catalog.<类型>.} 前缀（Hive 系写的是家族的 {@code catalog.hive.}，与 type 无关）。 */
    private static final Pattern CATALOG_PREFIX = Pattern.compile("^catalog\\.[^.]+\\.", Pattern.CASE_INSENSITIVE);

    /** 说明里的「默认 X」——只认 ASCII 字面量，避免把「默认取引擎默认批大小」当默认值。 */
    private static final Pattern DEFAULT_LITERAL =
            Pattern.compile("默认(?:值)?(?:为)?\\s*[：:]?\\s*([A-Za-z0-9._-]+)");

    /** 说明里的「如 xxx」示例。 */
    private static final Pattern EXAMPLE = Pattern.compile("如\\s*([^。；;，,（(]+)");

    private static final Pattern NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final Pattern TYPE = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final Pattern PROPERTY_KEY = Pattern.compile("[A-Za-z_][A-Za-z0-9_.]*");

    /** 卡片顺序：常用关系库在前，未知类型排最后（同组按 type 字典序）。 */
    private static final Map<String, Integer> ORDER = Map.of(
            "mysql", 0,
            "tidb", 1,
            "postgresql", 2,
            "sqlserver", 3,
            "oracle", 4,
            "hive3", 5,
            "hive2", 6,
            "clickhouse", 7,
            "doris", 8,
            "excel", 9);

    /** 已知属性 → 中文标签（未收录的用属性名本身，不猜测含义）。 */
    private static final Map<String, String> LABELS = Map.ofEntries(
            Map.entry("jdbcurl", "JDBC 连接串"),
            Map.entry("username", "用户名"),
            Map.entry("password", "密码"),
            Map.entry("path", "文件 / 目录路径"),
            Map.entry("metastore.uris", "MetaStore 地址"),
            Map.entry("fs.defaultfs", "默认文件系统"),
            Map.entry("default.format", "写入格式"),
            Map.entry("batch.size", "单批读取行数"),
            Map.entry("partition.limit", "分区枚举上限"),
            Map.entry("bucket.pruning", "分桶裁剪"),
            Map.entry("acid.read", "ACID 事务表读取"),
            Map.entry("compact.target.bytes", "小文件合并目标大小"),
            Map.entry("split.file.min.bytes", "文件内切分阈值"),
            Map.entry("metastore.kerberos.principal", "Kerberos 主体"),
            Map.entry("header", "首行为列名"),
            Map.entry("samplerows", "类型推断采样行数"));

    /** 卡片副标题（连接器自带 displayName 之外的一句话说明）。 */
    private static final Map<String, String> DESCRIPTIONS = Map.of(
            "mysql", "MySQL 关系型数据库 · 支持联邦查询与读写",
            "tidb", "TiDB 分布式关系型数据库 · MySQL 兼容，支持联邦查询与读写",
            "postgresql", "PostgreSQL 关系型数据库 · 支持联邦查询与读写",
            "sqlserver", "Microsoft SQL Server 关系型数据库 · 支持联邦查询、读写与覆盖写",
            "oracle", "Oracle 数据库 · 支持联邦查询、读写与覆盖写",
            "hive2", "Hive 2.x 数据仓库 · MetaStore 元数据 + ORC / Parquet 分区表",
            "hive3", "Hive 3.x 数据仓库 · MetaStore 元数据 + ACID 事务表",
            "clickhouse", "ClickHouse 列式分析库 · 适合大宽表聚合分析",
            "doris", "Apache Doris 实时分析库 · 支持联邦查询、读写与覆盖写",
            "excel", "本地 Excel 工作簿 · 单分区读取，适合小数据量分析");

    private CatalogCreateSupport() {
    }

    // ==================== 连接器清单 ====================

    /**
     * 列出注册表里的全部连接器（含结构化字段），按「常用程度 + type」排序。
     *
     * <p>空注册表 / null 返回空列表 —— 前端展示「没有可用连接器」而不是空白页。</p>
     */
    public static List<CatalogConnector> describe(ConnectorRegistry registry) {
        List<CatalogConnector> out = new ArrayList<>();
        if (registry == null) return out;
        Map<String, String> origins = registry.origins();
        for (String type : registry.types()) {
            registry.factory(type).ifPresent(factory ->
                    out.add(describe(factory, origins.get(type))));
        }
        out.sort(Comparator.comparingInt((CatalogConnector c) -> ORDER.getOrDefault(c.type(), 100))
                .thenComparing(CatalogConnector::type));
        return out;
    }

    /** 单个连接器 → 卡片 + 表单描述。 */
    public static CatalogConnector describe(ConnectorFactory factory, String origin) {
        String type = factory.type();
        String display = factory.displayName();
        if (display == null || display.isBlank()) display = type;

        List<CatalogField> fields = new ArrayList<>();
        Map<String, String> described = factory.describeProperties();
        if (described != null) {
            for (Map.Entry<String, String> entry : described.entrySet()) {
                if (entry.getKey() == null || entry.getKey().isBlank()) continue;
                fields.add(parseField(entry.getKey(), entry.getValue()));
            }
        }

        String description = DESCRIPTIONS.get(type.toLowerCase(Locale.ROOT));
        if (description == null) description = display + " 连接器";
        return new CatalogConnector(type, display, description,
                origin == null ? "" : origin, List.copyOf(fields));
    }

    /**
     * 解析一条 {@code describeProperties()} 条目。
     *
     * <ul>
     *   <li>{@code catalog.mysql.jdbcUrl} → 展示键 {@code catalog.mysql.jdbcUrl}、裸名 {@code jdbcUrl}；</li>
     *   <li>说明含「必填」→ {@code required}；含「密码 / secret / token」→ 密码框；</li>
     *   <li>「可选，默认 root」→ 预填 {@code root}；「默认空」→ 预填空串；</li>
     *   <li>默认是 true/false（或 {@code *.read} / {@code header} / {@code *.pruning}）→ 布尔下拉；</li>
     *   <li>默认是整数或名字含 rows/size/limit/bytes → 数字框。</li>
     * </ul>
     */
    public static CatalogField parseField(String fullKey, String helpText) {
        String key = fullKey.trim();
        String name = CATALOG_PREFIX.matcher(key).replaceFirst("");
        String help = helpText == null ? "" : helpText.trim();
        String lower = name.toLowerCase(Locale.ROOT);

        boolean required = help.contains("必填");
        boolean secret = lower.contains("password") || lower.contains("secret") || lower.contains("token");
        String defaultValue = parseDefault(help);

        String kind;
        if (secret) {
            kind = "password";
        } else if (isBoolean(lower, defaultValue)) {
            kind = "boolean";
        } else if (isNumeric(lower, defaultValue)) {
            kind = "number";
        } else {
            kind = "text";
        }

        String label = LABELS.getOrDefault(lower, name);
        String placeholder = parseExample(help);
        return new CatalogField(key, name, label, required, secret, kind,
                defaultValue == null ? "" : defaultValue,
                placeholder == null ? "" : placeholder, help);
    }

    private static String parseDefault(String help) {
        if (help == null || help.isEmpty()) return null;
        if (help.contains("默认空")) return "";
        Matcher m = DEFAULT_LITERAL.matcher(help);
        return m.find() ? m.group(1) : null;
    }

    private static String parseExample(String help) {
        if (help == null || help.isEmpty()) return "";
        Matcher m = EXAMPLE.matcher(help);
        if (!m.find()) return "";
        String v = m.group(1).trim();
        return v.isEmpty() ? "" : v;
    }

    private static boolean isBoolean(String lowerName, String defaultValue) {
        if ("true".equalsIgnoreCase(defaultValue) || "false".equalsIgnoreCase(defaultValue)) return true;
        return lowerName.equals("header")
                || lowerName.endsWith(".read")
                || lowerName.endsWith(".pruning");
    }

    private static boolean isNumeric(String lowerName, String defaultValue) {
        if (defaultValue != null && defaultValue.matches("\\d+")) return true;
        return lowerName.contains("rows") || lowerName.contains("size")
                || lowerName.contains("limit") || lowerName.contains("bytes")
                || lowerName.endsWith(".ms") || lowerName.contains("port");
    }

    // ==================== 表单取值 ====================

    /**
     * 把前端提交的「属性键 → 值」收敛成可执行的属性集合：
     * <ul>
     *   <li>只保留该连接器声明过的字段（未知键忽略，避免 UI 注入任意 catalog 属性）；</li>
     *   <li>空白值不写进 SQL，让连接器自己的默认值生效；</li>
     *   <li>密码类字段<b>不 trim</b>（首尾空格可能是密码的一部分）；</li>
     *   <li>必填项缺失 → {@link IllegalArgumentException}，文案直接指名字段。</li>
     * </ul>
     */
    public static Map<String, String> effectiveValues(CatalogConnector connector, Map<String, String> raw) {
        Map<String, String> input = raw == null ? Map.of() : raw;
        Map<String, String> out = new LinkedHashMap<>();
        List<String> missing = new ArrayList<>();
        for (CatalogField field : connector.fields()) {
            String value = input.get(field.key());
            if (value == null) value = input.get(field.name());   // 兼容只传裸键的调用方
            if (value == null) value = "";
            if (!field.secret()) value = value.trim();
            if (value.isEmpty()) {
                if (field.required()) missing.add(field.label());
                continue;
            }
            out.put(field.key(), value);
        }
        if (!missing.isEmpty()) {
            throw new IllegalArgumentException("请填写必填项: " + String.join("、", missing));
        }
        return out;
    }

    // ==================== CREATE CATALOG SQL ====================

    /**
     * 生成 {@code CREATE CATALOG <名> USING <类型> WITH ('键' = '值', ...)}。
     *
     * <p>名字 / 类型 / 属性键先做字符集校验（不拼任何未校验文本），值按 SQL 字符串字面量
     * 规则把单引号翻倍 —— 对应文法 {@code STRING: '\'' ( ~'\'' | '\'\'' )* '\''}。
     * 生成结果交给正常 SQL 执行线，不在 UI 进程里直接改元数据。</p>
     */
    public static String buildCreateSql(String catalogName, String type, Map<String, String> values) {
        String name = requireName(catalogName);
        String connectorType = requireType(type);
        StringBuilder sql = new StringBuilder("CREATE CATALOG ").append(name)
                .append(" USING ").append(connectorType);
        if (values != null && !values.isEmpty()) {
            sql.append(" WITH (");
            boolean first = true;
            for (Map.Entry<String, String> entry : values.entrySet()) {
                String key = entry.getKey() == null ? "" : entry.getKey().trim();
                if (!PROPERTY_KEY.matcher(key).matches()) {
                    throw new IllegalArgumentException("非法的 catalog 属性名: '" + key + "'");
                }
                String value = entry.getValue() == null ? "" : entry.getValue();
                if (!first) sql.append(", ");
                first = false;
                sql.append('\'').append(escapeLiteral(key)).append("' = '")
                        .append(escapeLiteral(value)).append('\'');
            }
            sql.append(')');
        }
        return sql.toString();
    }

    /** SQL 单引号字符串转义（同时挡住 NUL：JDBC / 元数据库都存不了）。 */
    public static String escapeLiteral(String value) {
        if (value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("属性值不能包含 NUL 字符");
        }
        return value.replace("'", "''");
    }

    /** 校验并返回 catalog 名：仅字母 / 数字 / 下划线，且不能以数字开头。 */
    public static String requireName(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Catalog 名称不能为空");
        }
        String n = name.trim();
        if (n.length() > 128) {
            throw new IllegalArgumentException("Catalog 名称过长（最多 128 个字符）");
        }
        if (!NAME.matcher(n).matches()) {
            throw new IllegalArgumentException("Catalog 名称 '" + n
                    + "' 不合法：只能包含字母、数字、下划线，且不能以数字开头");
        }
        return n;
    }

    /** 校验并返回连接器类型（与 {@code USING} 后面那个词同口径）。 */
    public static String requireType(String type) {
        if (type == null || type.isBlank()) {
            throw new IllegalArgumentException("请选择连接器类型");
        }
        String t = type.trim();
        if (!TYPE.matcher(t).matches()) {
            throw new IllegalArgumentException("连接器类型 '" + t + "' 不合法");
        }
        return t;
    }

    // ==================== 测试连接 ====================

    /**
     * 真的连一次数据源（{@code validate → create → probeConnectivity}），用于「测试连接」按钮。
     *
     * <p>不落库、不进内存表；失败原样抛出，由 REST 层用 {@code Errors.describe} 展开 cause 链
     * ——「Access denied」「Unknown database」这类真因必须让用户看到。密码不写日志。</p>
     */
    public static void testConnection(ConnectorFactory factory, String catalogName,
                                      Map<String, String> values) {
        String name = (catalogName == null || catalogName.isBlank()) ? "probe" : catalogName.trim();
        factory.validate(name, values);
        DojaoConnector connector = factory.create(name, values);
        connector.probeConnectivity(name);
    }

    // ==================== DTO（Jackson 直接序列化 record） ====================

    /** 一张连接器卡片 + 它的表单字段。 */
    public record CatalogConnector(String type, String displayName, String description,
                                   String origin, List<CatalogField> fields) {
    }

    /** 一个表单字段。{@code key} 是写进 SQL 的完整属性键，{@code name} 是去掉命名空间的裸名。 */
    public record CatalogField(String key, String name, String label, boolean required,
                               boolean secret, String kind, String defaultValue,
                               String placeholder, String help) {
    }
}
