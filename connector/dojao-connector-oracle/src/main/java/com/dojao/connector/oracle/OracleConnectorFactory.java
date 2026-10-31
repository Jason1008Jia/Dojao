package com.dojao.connector.oracle;

import com.dojao.spi.service.ConnectorFactory;
import com.dojao.spi.service.DojaoConnector;
import com.dojao.spi.util.CatalogProperties;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Oracle Connector SPI 工厂，由 ServiceLoader 自动发现。
 *
 * <p>{@code USING oracle}；属性命名空间为 {@code catalog.oracle.*}。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/24
 */
public class OracleConnectorFactory implements ConnectorFactory {

    @Override
    public String type() {
        return "oracle";
    }

    @Override
    public String displayName() {
        return "Oracle";
    }

    @Override
    public Map<String, String> describeProperties() {
        Map<String, String> props = new LinkedHashMap<>();
        props.put("catalog.oracle.jdbcUrl",
                "必填。JDBC 连接串，如 jdbc:oracle:thin:@//host:1521/ORCLPDB1（服务名）"
                        + "或 jdbc:oracle:thin:@host:1521:ORCL（SID）");
        props.put("catalog.oracle.username", "可选，默认 system");
        props.put("catalog.oracle.password", "可选，默认空");
        return props;
    }

    @Override
    public void validate(String catalogName, Map<String, String> properties) {
        Map<String, String> props = CatalogProperties.normalize(type(), properties);
        String jdbcUrl = props.get("jdbcUrl");
        if (jdbcUrl == null || jdbcUrl.isBlank()) {
            throw new IllegalArgumentException("Oracle catalog '" + catalogName
                    + "' 缺少必填属性 'catalog.oracle.jdbcUrl'。可用属性: " + describeProperties());
        }
    }

    @Override
    public DojaoConnector create(String catalogName, Map<String, String> properties) {
        Map<String, String> props = CatalogProperties.normalize(type(), properties);
        validate(catalogName, props);
        return new OracleConnector(catalogName, props.get("jdbcUrl"),
                props.getOrDefault("username", "system"), props.getOrDefault("password", ""));
    }
}
