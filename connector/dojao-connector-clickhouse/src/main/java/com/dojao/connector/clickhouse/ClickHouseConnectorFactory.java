package com.dojao.connector.clickhouse;

import com.dojao.spi.service.ConnectorFactory;
import com.dojao.spi.service.DojaoConnector;
import com.dojao.spi.util.CatalogProperties;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * ClickHouse Connector SPI 工厂，由 ServiceLoader 自动发现。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/1
 */
public class ClickHouseConnectorFactory implements ConnectorFactory {

    @Override
    public String type() {
        return "clickhouse";
    }

    @Override
    public String displayName() {
        return "ClickHouse";
    }

    @Override
    public Map<String, String> describeProperties() {
        Map<String, String> props = new LinkedHashMap<>();
        props.put("catalog.clickhouse.jdbcUrl",
                "必填。JDBC 连接串，如 jdbc:clickhouse://host:8123/default");
        props.put("catalog.clickhouse.username", "可选，默认 default");
        props.put("catalog.clickhouse.password", "可选，默认空");
        props.put("catalog.clickhouse.distributed",
                "可选，默认 false。是否分布式建表：true 时同时在 view 库与 local 库建表（ON CLUSTER）");
        props.put("catalog.clickhouse.cluster", "可选。ClickHouse 集群名（如 ch_cluster）；分布式建表时必填");
        props.put("catalog.clickhouse.engine", "可选，默认 MergeTree。本地表引擎（如 MergeTree / ReplacingMergeTree）");
        props.put("catalog.clickhouse.localSchemaSuffix",
                "可选，默认 _LOCAL。local 库后缀，如 ADS → ADS_LOCAL");
        props.put("catalog.clickhouse.orderBy",
                "可选。本地表 ORDER BY 表达式；默认取第一个非容器列（可空列包 assumeNotNull）");
        props.put("catalog.clickhouse.partitionBy",
                "可选。本地表 PARTITION BY 表达式；默认存在 data_insert_time 列时用 toYYYYMMDD(data_insert_time)");
        // 注意：说明里不要写「默认 <ASCII 表达式>」——UI 会把它当字段默认值预填
        // （曾把 shardingKey 预填成 "xxHash32"，建 Distributed 表时报 Missing columns: 'xxHash32'）。
        props.put("catalog.clickhouse.shardingKey",
                "可选。Distributed 分片表达式；留空则按第一个非容器列自动生成 xxHash32(toString(col))");
        return props;
    }

    @Override
    public void validate(String catalogName, Map<String, String> properties) {
        Map<String, String> props = CatalogProperties.normalize(type(), properties);
        String jdbcUrl = props.get("jdbcUrl");
        if (jdbcUrl == null || jdbcUrl.isBlank()) {
            throw new IllegalArgumentException("ClickHouse catalog '" + catalogName
                    + "' 缺少必填属性 'catalog.clickhouse.jdbcUrl'。可用属性: " + describeProperties());
        }

        boolean distributed = Boolean.parseBoolean(value(props, "distributed", "false"));
        String engine = value(props, "engine", ClickHouseConnector.ClusterLayout.DEFAULT_ENGINE);
        if (!engine.matches("[A-Za-z0-9_]+")) {
            throw new IllegalArgumentException("ClickHouse catalog '" + catalogName
                    + "' 的 catalog.clickhouse.engine 非法: '" + engine + "'");
        }
        String suffix = value(props, "localSchemaSuffix",
                ClickHouseConnector.ClusterLayout.DEFAULT_LOCAL_SUFFIX);
        if (!suffix.matches("[A-Za-z0-9_]+")) {
            throw new IllegalArgumentException("ClickHouse catalog '" + catalogName
                    + "' 的 catalog.clickhouse.localSchemaSuffix 非法: '" + suffix + "'");
        }
        if (distributed) {
            String cluster = props.get("cluster");
            if (cluster == null || cluster.isBlank()) {
                throw new IllegalArgumentException("ClickHouse catalog '" + catalogName
                        + "' 启用了分布式建表（catalog.clickhouse.distributed=true），"
                        + "必须提供 catalog.clickhouse.cluster（如 ch_cluster）");
            }
            if (!cluster.trim().matches("[A-Za-z0-9_.-]+")) {
                throw new IllegalArgumentException("ClickHouse catalog '" + catalogName
                        + "' 的 catalog.clickhouse.cluster 非法: '" + cluster.trim() + "'");
            }
        }
    }

    @Override
    public DojaoConnector create(String catalogName, Map<String, String> properties) {
        Map<String, String> props = CatalogProperties.normalize(type(), properties);
        validate(catalogName, props);
        boolean distributed = Boolean.parseBoolean(value(props, "distributed", "false"));
        ClickHouseConnector.ClusterLayout layout = new ClickHouseConnector.ClusterLayout(
                props.get("cluster"),
                distributed,
                value(props, "engine", ClickHouseConnector.ClusterLayout.DEFAULT_ENGINE),
                value(props, "localSchemaSuffix", ClickHouseConnector.ClusterLayout.DEFAULT_LOCAL_SUFFIX),
                props.get("orderBy"),
                props.get("partitionBy"),
                props.get("shardingKey"));
        return new ClickHouseConnector(catalogName, props.get("jdbcUrl"),
                props.getOrDefault("username", "default"), props.getOrDefault("password", ""),
                layout);
    }

    private static String value(Map<String, String> props, String key, String fallback) {
        String v = props.get(key);
        return v == null || v.isBlank() ? fallback : v.trim();
    }
}
