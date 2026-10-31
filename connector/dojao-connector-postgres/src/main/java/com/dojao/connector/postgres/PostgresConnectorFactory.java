package com.dojao.connector.postgres;

import com.dojao.spi.service.ConnectorFactory;
import com.dojao.spi.service.DojaoConnector;
import com.dojao.spi.util.CatalogProperties;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * PostgreSQL Connector SPI 工厂，由 ServiceLoader 自动发现。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/8
 */
public class PostgresConnectorFactory implements ConnectorFactory {

    @Override
    public String type() {
        return "postgresql";
    }

    @Override
    public String displayName() {
        return "PostgreSQL";
    }

    @Override
    public Map<String, String> describeProperties() {
        Map<String, String> props = new LinkedHashMap<>();
        props.put("catalog.postgresql.jdbcUrl", "必填。JDBC 连接串，如 jdbc:postgresql://host:5432/db");
        props.put("catalog.postgresql.username", "可选，默认 postgres");
        props.put("catalog.postgresql.password", "可选，默认空");
        return props;
    }

    @Override
    public void validate(String catalogName, Map<String, String> properties) {
        Map<String, String> props = CatalogProperties.normalize(type(), properties);
        String jdbcUrl = props.get("jdbcUrl");
        if (jdbcUrl == null || jdbcUrl.isBlank()) {
            throw new IllegalArgumentException("PostgreSQL catalog '" + catalogName
                    + "' 缺少必填属性 'catalog.postgresql.jdbcUrl'。可用属性: " + describeProperties());
        }
    }

    @Override
    public DojaoConnector create(String catalogName, Map<String, String> properties) {
        Map<String, String> props = CatalogProperties.normalize(type(), properties);
        validate(catalogName, props);
        return new PostgresConnector(catalogName, props.get("jdbcUrl"),
                props.getOrDefault("username", "postgres"), props.getOrDefault("password", ""));
    }
}
