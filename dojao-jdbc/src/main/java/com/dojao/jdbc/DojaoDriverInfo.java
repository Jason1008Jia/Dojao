package com.dojao.jdbc;

/**
 * JDBC driver constants.
 */
final class DojaoDriverInfo {

    private DojaoDriverInfo() {}

    static final String DRIVER_NAME    = "Dojao JDBC Driver";
    static final String DRIVER_VERSION = "1.0";
    static final int    DRIVER_MAJOR   = 1;
    static final int    DRIVER_MINOR   = 0;

    static final String URL_PREFIX       = "jdbc:dojao://";
    static final int    DEFAULT_PORT     = 6530;

    static final String DB_PRODUCT_NAME    = "Dojao";
    static final String DB_PRODUCT_VERSION = "1.0";
}
