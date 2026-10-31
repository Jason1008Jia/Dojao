package com.dojao.jdbc;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.RootAllocator;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Dojao JDBC Driver。URL 格式: {@code jdbc:dojao://host:port}
 *
 * <p>通过 META-INF/services/java.sql.Driver 自动注册。</p>
 *
 * <p>与 Master 的传输协议 = Arrow Flight（doAction "execute_query"）。</p>
 */
public class DojaoDriver implements Driver {

    static {
        try {
            DriverManager.registerDriver(new DojaoDriver());
        } catch (SQLException e) {
            throw new RuntimeException("Failed to register DojaoDriver", e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }

        String address = url.substring(DojaoDriverInfo.URL_PREFIX.length());
        String host;
        int port;

        int colonIdx = address.indexOf(':');
        if (colonIdx >= 0) {
            host = address.substring(0, colonIdx);
            port = Integer.parseInt(address.substring(colonIdx + 1));
        } else {
            host = address;
            port = DojaoDriverInfo.DEFAULT_PORT;
        }

        RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        FlightClient client = FlightClient.builder()
                .location(Location.forGrpcInsecure(host, port))
                .allocator(allocator)
                .maxInboundMessageSize(Integer.getInteger("DojaoMaxMessageBytes", 512 << 20))
                .build();
        return new DojaoConnection(client, allocator, url, info);
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith(DojaoDriverInfo.URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() { return DojaoDriverInfo.DRIVER_MAJOR; }

    @Override
    public int getMinorVersion() { return DojaoDriverInfo.DRIVER_MINOR; }

    @Override
    public boolean jdbcCompliant() { return false; }

    @Override
    public Logger getParentLogger() {
        return Logger.getLogger("dojao.jdbc");
    }
}
