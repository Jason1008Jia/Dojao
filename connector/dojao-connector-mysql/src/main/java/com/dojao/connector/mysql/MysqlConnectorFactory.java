package com.dojao.connector.mysql;

import com.dojao.spi.service.ConnectorFactory;
import com.dojao.spi.service.DojaoConnector;
import com.dojao.spi.util.CatalogProperties;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * MySQL Connector SPI 工厂，由 ServiceLoader 自动发现。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/09
 */
public class MysqlConnectorFactory implements ConnectorFactory {

    @Override
    public String type() {
        return "mysql";
    }

    @Override
    public String displayName() {
        return "MySQL";
    }

    @Override
    public Map<String, String> describeProperties() {
        Map<String, String> props = new LinkedHashMap<>();
        props.put("catalog.mysql.jdbcUrl", "必填。JDBC 连接串，如 jdbc:mysql://host:3306/db");
        props.put("catalog.mysql.username", "可选，默认 root");
        props.put("catalog.mysql.password", "可选，默认空");
        return props;
    }

    @Override
    public void validate(String catalogName, Map<String, String> properties) {
        Map<String, String> props = CatalogProperties.normalize(type(), properties);
        String jdbcUrl = props.get("jdbcUrl");
        if (jdbcUrl == null || jdbcUrl.isBlank()) {
            throw new IllegalArgumentException("MySQL catalog '" + catalogName
                    + "' 缺少必填属性 'catalog.mysql.jdbcUrl'。可用属性: " + describeProperties());
        }
    }

    @Override
    public DojaoConnector create(String catalogName, Map<String, String> properties) {
        Map<String, String> props = CatalogProperties.normalize(type(), properties);
        validate(catalogName, props);
        return new MysqlConnector(catalogName, props.get("jdbcUrl"),
                props.getOrDefault("username", "root"), props.getOrDefault("password", ""));
    }
}
