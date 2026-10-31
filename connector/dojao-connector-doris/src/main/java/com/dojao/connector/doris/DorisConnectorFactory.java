package com.dojao.connector.doris;

import com.dojao.spi.service.ConnectorFactory;
import com.dojao.spi.service.DojaoConnector;
import com.dojao.spi.util.CatalogProperties;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Apache Doris Connector SPI 工厂，由 ServiceLoader 自动发现。
 *
 * <p>Doris 的 FE 提供 MySQL 兼容协议（默认 9030），JDBC URL 形如
 * {@code jdbc:mysql://host:9030/db}。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/25
 */
public class DorisConnectorFactory implements ConnectorFactory {

    @Override
    public String type() {
        return "doris";
    }

    @Override
    public String displayName() {
        return "Doris";
    }

    @Override
    public Map<String, String> describeProperties() {
        Map<String, String> props = new LinkedHashMap<>();
        props.put("catalog.doris.jdbcUrl",
                "必填。MySQL 协议连接串（FE 端口默认 9030），如 jdbc:mysql://host:9030/db");
        props.put("catalog.doris.username", "可选，默认 root");
        props.put("catalog.doris.password", "可选，默认空");
        return props;
    }

    @Override
    public void validate(String catalogName, Map<String, String> properties) {
        Map<String, String> props = CatalogProperties.normalize(type(), properties);
        String jdbcUrl = props.get("jdbcUrl");
        if (jdbcUrl == null || jdbcUrl.isBlank()) {
            throw new IllegalArgumentException("Doris catalog '" + catalogName
                    + "' 缺少必填属性 'catalog.doris.jdbcUrl'。可用属性: " + describeProperties());
        }
    }

    @Override
    public DojaoConnector create(String catalogName, Map<String, String> properties) {
        Map<String, String> props = CatalogProperties.normalize(type(), properties);
        validate(catalogName, props);
        return new DorisConnector(catalogName, props.get("jdbcUrl"),
                props.getOrDefault("username", "root"), props.getOrDefault("password", ""));
    }
}
