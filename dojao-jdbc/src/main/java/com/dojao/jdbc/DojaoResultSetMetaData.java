package com.dojao.jdbc;

import com.dojao.core.rpc.Column;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * JDBC ResultSetMetaData 实现。
 */
public class DojaoResultSetMetaData implements ResultSetMetaData {

    private final List<Column> columns;

    DojaoResultSetMetaData(List<Column> columns) {
        this.columns = columns;
    }

    @Override
    public int getColumnCount() {
        return columns.size();
    }

    @Override
    public String getColumnName(int column) {
        return columns.get(column - 1).getName();
    }

    @Override
    public String getColumnLabel(int column) {
        return getColumnName(column);
    }

    @Override
    public int getColumnType(int column) {
        return sqlType(columns.get(column - 1).getType());
    }

    @Override
    public String getColumnTypeName(int column) {
        return columns.get(column - 1).getType();
    }

    @Override public String getColumnClassName(int column) { return "java.lang.String"; }
    @Override public int getColumnDisplaySize(int column) { return 0; }
    @Override public int getPrecision(int column) { return 0; }
    @Override public int getScale(int column) { return 0; }
    @Override public int isNullable(int column) { return columnNullableUnknown; }
    @Override public boolean isAutoIncrement(int column) { return false; }
    @Override public boolean isCaseSensitive(int column) { return true; }
    @Override public boolean isSearchable(int column) { return true; }
    @Override public boolean isCurrency(int column) { return false; }
    @Override public boolean isSigned(int column) { return false; }
    @Override public boolean isReadOnly(int column) { return true; }
    @Override public boolean isWritable(int column) { return false; }
    @Override public boolean isDefinitelyWritable(int column) { return false; }
    @Override public String getSchemaName(int column) { return ""; }
    @Override public String getTableName(int column) { return ""; }
    @Override public String getCatalogName(int column) { return ""; }
    @Override public <T> T unwrap(Class<T> iface) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public boolean isWrapperFor(Class<?> iface) { return false; }

    private static int sqlType(String typeName) {
        if (typeName == null) return Types.VARCHAR;
        return switch (typeName.toUpperCase()) {
            case "VARCHAR", "CHAR", "TEXT" -> Types.VARCHAR;
            case "INTEGER", "INT" -> Types.INTEGER;
            case "BIGINT" -> Types.BIGINT;
            case "SMALLINT" -> Types.SMALLINT;
            case "TINYINT" -> Types.TINYINT;
            case "FLOAT", "REAL" -> Types.FLOAT;
            case "DOUBLE" -> Types.DOUBLE;
            case "DECIMAL", "NUMERIC" -> Types.DECIMAL;
            case "BOOLEAN" -> Types.BOOLEAN;
            case "DATE" -> Types.DATE;
            case "TIME" -> Types.TIME;
            case "TIMESTAMP" -> Types.TIMESTAMP;
            case "BINARY", "VARBINARY", "BLOB" -> Types.BLOB;
            default -> Types.VARCHAR;
        };
    }
}
