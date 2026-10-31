package com.dojao.metastore;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Catalog 定义 — 注册到 MetaStore 的 catalog 描述结构。
 *
 * <pre>{@code
 *   new CatalogDef("mysql", "mysql", Map.of(
 *       "jdbcUrl",  "jdbc:mysql://localhost:3306",
 *       "username", "root",
 *       "password", "123456"
 *   ));
 * }</pre>
 *
 * <p>对应的 JSON 结构：
 * <pre>{@code
 *   {
 *     "name": "mysql",
 *     "type": "mysql",
 *     "properties": {
 *       "jdbcUrl": "jdbc:mysql://localhost:3306",
 *       "username": "root",
 *       "password": "123456"
 *     }
 *   }
 * }</pre>
 *
 * @param name       catalog 名称
 * @param type       连接器类型（mysql / hive / clickhouse ...）
 * @param properties 连接属性（jdbcUrl, username, password 等），不含 type
 * @param owner      归属创建者（Web 控制台登录用户名）；CLI/JDBC 或未登录创建 = {@code null}（全局）。
 *                   仅当 {@code WITH (is_private='true')} 时用于可见性隔离（见 {@link #isPrivate()}）。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/09
 */
public record CatalogDef(String name, String type, Map<String, String> properties, String owner) {

    public CatalogDef {
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(type, "type must not be null");
        if (properties == null) {
            properties = Map.of();
        }
    }

    /** 向后兼容：无 owner 的 3 参构造（CLI / 未登录 / 旧调用点 → 归属为空 = 全局）。 */
    public CatalogDef(String name, String type, Map<String, String> properties) {
        this(name, type, properties, null);
    }

    /** 读取 WITH 属性，缺失返回 empty。 */
    public Optional<String> property(String key) {
        return properties == null ? Optional.empty() : Optional.ofNullable(properties.get(key));
    }

    /** 是否标记为私有：{@code WITH (is_private='true')}（大小写不敏感）。 */
    public boolean isPrivate() {
        String v = property("is_private").orElse(null);
        return v != null && "true".equalsIgnoreCase(v.trim());
    }


    /** 从 Map 构建（常用于 JSON 反序列化后）。 */
    @SuppressWarnings("unchecked")
    public static CatalogDef fromMap(Map<String, Object> raw) {
        String name = (String) raw.get("name");
        String type = (String) raw.get("type");
        Object props = raw.get("properties");
        Map<String, String> properties;
        if (props instanceof Map<?, ?> m) {
            properties = new LinkedHashMap<>();
            m.forEach((k, v) -> properties.put(String.valueOf(k), v == null ? "" : String.valueOf(v)));
        } else {
            properties = Map.of();
        }
        String owner = (String) raw.get("owner");
        return new CatalogDef(name, type, properties, owner);
    }
}
