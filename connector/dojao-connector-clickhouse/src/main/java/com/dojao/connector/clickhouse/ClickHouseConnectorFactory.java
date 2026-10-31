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
    }

    @Override
    public DojaoConnector create(String catalogName, Map<String, String> properties) {
        Map<String, String> props = CatalogProperties.normalize(type(), properties);
        validate(catalogName, props);
        return new ClickHouseConnector(catalogName, props.get("jdbcUrl"),
                props.getOrDefault("username", "default"), props.getOrDefault("password", ""));
    }
}
