package com.dojao.connector.tidb;

import com.dojao.spi.service.ConnectorFactory;
import com.dojao.spi.service.DojaoConnector;
import com.dojao.spi.util.CatalogProperties;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * TiDB Connector SPI 工厂，由 ServiceLoader 自动发现。
 *
 * <p>TiDB 使用 MySQL 线路协议，JDBC URL 形如
 * {@code jdbc:mysql://host:4000/db?useSSL=false&allowPublicKeyRetrieval=true}。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/25
 */
public class TidbConnectorFactory implements ConnectorFactory {

    @Override
    public String type() {
        return "tidb";
    }

    @Override
    public String displayName() {
        return "TiDB";
    }

    @Override
    public Map<String, String> describeProperties() {
        Map<String, String> props = new LinkedHashMap<>();
        props.put("catalog.tidb.jdbcUrl",
                "必填。MySQL 协议连接串，如 jdbc:mysql://host:4000/db");
        props.put("catalog.tidb.username", "可选，默认 root");
        props.put("catalog.tidb.password", "可选，默认空");
        return props;
    }

    @Override
    public void validate(String catalogName, Map<String, String> properties) {
        Map<String, String> props = CatalogProperties.normalize(type(), properties);
        String jdbcUrl = props.get("jdbcUrl");
        if (jdbcUrl == null || jdbcUrl.isBlank()) {
            throw new IllegalArgumentException("TiDB catalog '" + catalogName
                    + "' 缺少必填属性 'catalog.tidb.jdbcUrl'。可用属性: " + describeProperties());
        }
    }

    @Override
    public DojaoConnector create(String catalogName, Map<String, String> properties) {
        Map<String, String> props = CatalogProperties.normalize(type(), properties);
        validate(catalogName, props);
        return new TidbConnector(catalogName, props.get("jdbcUrl"),
                props.getOrDefault("username", "root"), props.getOrDefault("password", ""));
    }
}
