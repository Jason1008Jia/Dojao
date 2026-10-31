package com.dojao.spi.util;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Catalog 级别配置键的<b>命名规范</b>与归一化。
 *
 * <p><b>规范</b>：{@code CREATE CATALOG ... WITH (<键> = <值>)} 里的每一个键都写成
 * {@code catalog.<连接器类型>.<属性名>}，例如：</p>
 * <ul>
 *   <li>{@code catalog.mysql.jdbcUrl} / {@code catalog.mysql.username}；</li>
 *   <li>{@code catalog.postgresql.password}；</li>
 *   <li>{@code catalog.hive.metastore.uris} / {@code catalog.hive.default.format}。</li>
 * </ul>
 *
 * <p>自定义连接器类型 {@code abc}（{@link com.dojao.spi.service.ConnectorFactory#type()} 的返回值）
 * 的专属属性即 {@code catalog.abc.<属性名>}。</p>
 *
 * <p><b>为什么</b>：属性名由各连接器自己定义（MySQL 用 {@code jdbcUrl}，Hive 用
 * {@code metastore.uris}），不加命名空间时不同连接器的同名键会互相干扰，也看不出某个键属于谁。
 * 统一加 {@code catalog.<类型>.} 前缀后，键自解释、跨连接器不冲突。</p>
 *
 * <p><b>归一化</b>：{@link #normalize} 把带前缀的键「降级」成连接器内部使用的裸键，连接器按裸键读即可；
 * 原始键全部保留，因此<b>旧写法（不带前缀）继续可用</b>，存量 catalog 不受影响。</p>
 *
 * <p><b>家族前缀</b>：带数字版本、语义上属于同一族的多套类型（{@code hive2} / {@code hive3}）除各自的
 * 精确前缀外，还可共用一个家族前缀（{@code catalog.hive.}）—— 由 {@link #normalize(String, String, Map)}
 * 的 {@code family} 参数显式声明（Hive 侧见 {@code HiveVersion#family()}）。家族名<b>不做启发式推导</b>：
 * 「剥掉结尾数字」会把 {@code db2} 误当成 {@code db} 家族，宁可显式声明。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/29
 */
public final class CatalogProperties {

    /** 命名空间前缀。 */
    public static final String PREFIX = "catalog.";

    private CatalogProperties() {
    }

    /** 规范化后的完整键名：{@code key("mysql", "jdbcUrl")} → {@code catalog.mysql.jdbcUrl}。 */
    public static String key(String type, String name) {
        return PREFIX + type + "." + name;
    }

    /** 无家族前缀的归一化（家族 = 类型本身）。 */
    public static Map<String, String> normalize(String type, Map<String, String> properties) {
        return normalize(type, type, properties);
    }

    /**
     * 归一化：返回一份「原始键 + 去前缀别名」的视图。
     *
     * <p>键 {@code catalog.<type>.<名>}（或家族前缀 {@code catalog.<family>.<名>}）会额外生成两个别名 ——
     * {@code <名>} 与 {@code <family>.<名>}。已存在的键（无论裸键还是别的写法）不会被覆盖。</p>
     *
     * @param type       连接器类型（{@link com.dojao.spi.service.ConnectorFactory#type()}），可为 null
     * @param family     家族前缀（如 {@code hive2}/{@code hive3} 的 {@code hive}）；null / 空 = 无家族
     * @param properties 原始 catalog 属性，可为 null
     * @return 归一化后的属性（原始条目全保留）
     */
    public static Map<String, String> normalize(String type, String family,
                                                Map<String, String> properties) {
        Map<String, String> out = new LinkedHashMap<>();
        if (properties != null) out.putAll(properties);
        if (properties == null || properties.isEmpty()) return out;

        String lowerType = type == null ? null : type.toLowerCase(Locale.ROOT);
        String lowerFamily = (family == null || family.isBlank())
                ? lowerType : family.toLowerCase(Locale.ROOT);
        if (lowerType == null && lowerFamily == null) return out;

        for (Map.Entry<String, String> e : properties.entrySet()) {
            String rawKey = e.getKey();
            if (rawKey == null || rawKey.length() <= PREFIX.length()) continue;
            if (!rawKey.regionMatches(true, 0, PREFIX, 0, PREFIX.length())) continue;

            String rest = rawKey.substring(PREFIX.length());
            int dot = rest.indexOf('.');
            if (dot <= 0) continue;
            String scope = rest.substring(0, dot).toLowerCase(Locale.ROOT);
            String name = rest.substring(dot + 1);
            if (name.isEmpty()) continue;
            if (!matches(scope, lowerType, lowerFamily)) continue;   // 别的类型的键，与本次无关

            out.putIfAbsent(name, e.getValue());
            if (lowerFamily != null) out.putIfAbsent(lowerFamily + "." + name, e.getValue());
        }
        return out;
    }

    private static boolean matches(String scope, String lowerType, String lowerFamily) {
        if (lowerType != null && scope.equals(lowerType)) return true;
        return lowerFamily != null && scope.equals(lowerFamily);
    }
}
