package com.dojao.connector.hive;

import com.dojao.spi.service.ConnectorFactory;
import com.dojao.spi.service.DojaoConnector;

import java.util.Map;

/**
 * Hive 2 连接器工厂 —— {@code CREATE CATALOG ... USING hive2}。
 *
 * <p>与 {@link Hive3ConnectorFactory} 共用同一个 HMS Thrift 客户端（协议兼容），
 * 差异只在 catalog 类型标识与未来 ACID 表读取布局。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/19
 */
public class Hive2ConnectorFactory implements ConnectorFactory {

    @Override
    public String type() {
        return HiveVersion.HIVE2.type();
    }

    @Override
    public String displayName() {
        return "Hive 2.x";
    }

    @Override
    public Map<String, String> describeProperties() {
        return HiveConnectorFactorySupport.DESCRIBE;
    }

    @Override
    public void validate(String catalogName, Map<String, String> properties) {
        HiveConnectorFactorySupport.validate(HiveVersion.HIVE2, catalogName, properties);
    }

    @Override
    public DojaoConnector create(String catalogName, Map<String, String> properties) {
        return new HiveConnector(HiveVersion.HIVE2, catalogName, properties);
    }
}
