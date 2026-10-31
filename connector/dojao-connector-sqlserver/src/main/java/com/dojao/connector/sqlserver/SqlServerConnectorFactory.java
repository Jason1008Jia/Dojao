package com.dojao.connector.sqlserver;

import com.dojao.spi.service.ConnectorFactory;
import com.dojao.spi.service.DojaoConnector;
import com.dojao.spi.util.CatalogProperties;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Microsoft SQL Server Connector SPI 工厂，由 ServiceLoader 自动发现。
 *
 * <p>{@code USING sqlserver}；属性命名空间为 {@code catalog.sqlserver.*}。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/22
 */
public class SqlServerConnectorFactory implements ConnectorFactory {

    @Override
    public String type() {
        return "sqlserver";
    }

    @Override
    public String displayName() {
        return "SQL Server";
    }

    @Override
    public Map<String, String> describeProperties() {
        Map<String, String> props = new LinkedHashMap<>();
        props.put("catalog.sqlserver.jdbcUrl",
                "必填。JDBC 连接串，如 jdbc:sqlserver://host:1433;databaseName=db"
                        + "（驱动默认加密握手，测试环境可追加 encrypt=trustServerCertificate）");
        props.put("catalog.sqlserver.username", "可选，默认 sa");
        props.put("catalog.sqlserver.password", "可选，默认空");
        return props;
    }

    @Override
    public void validate(String catalogName, Map<String, String> properties) {
        Map<String, String> props = CatalogProperties.normalize(type(), properties);
        String jdbcUrl = props.get("jdbcUrl");
        if (jdbcUrl == null || jdbcUrl.isBlank()) {
            throw new IllegalArgumentException("SQL Server catalog '" + catalogName
                    + "' 缺少必填属性 'catalog.sqlserver.jdbcUrl'。可用属性: " + describeProperties());
        }
    }

    @Override
    public DojaoConnector create(String catalogName, Map<String, String> properties) {
        Map<String, String> props = CatalogProperties.normalize(type(), properties);
        validate(catalogName, props);
        return new SqlServerConnector(catalogName, props.get("jdbcUrl"),
                props.getOrDefault("username", "sa"), props.getOrDefault("password", ""));
    }
}
