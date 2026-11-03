package com.dojao.jdbc;

import com.dojao.core.rpc.Column;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
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
        return JdbcMetadata.sqlType(columns.get(column - 1).getType());
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
}
