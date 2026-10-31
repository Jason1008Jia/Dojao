package com.dojao.connector.hive;

import com.dojao.spi.service.ConnectorFactory;
import com.dojao.spi.service.DojaoConnector;

import java.util.Map;

/**
 * Hive 3 连接器工厂 —— {@code CREATE CATALOG ... USING hive3}。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/19
 */
public class Hive3ConnectorFactory implements ConnectorFactory {

    @Override
    public String type() {
        return HiveVersion.HIVE3.type();
    }

    @Override
    public String displayName() {
        return "Hive 3.x";
    }

    @Override
    public Map<String, String> describeProperties() {
        return HiveConnectorFactorySupport.DESCRIBE;
    }

    @Override
    public void validate(String catalogName, Map<String, String> properties) {
        HiveConnectorFactorySupport.validate(HiveVersion.HIVE3, catalogName, properties);
    }

    @Override
    public DojaoConnector create(String catalogName, Map<String, String> properties) {
        return new HiveConnector(HiveVersion.HIVE3, catalogName, properties);
    }
}
