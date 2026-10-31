package com.dojao.jdbc;

import com.dojao.core.rpc.QueryResponse;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

/**
 * JDBC PreparedStatement 实现。
 *
 * <p>MVP 使用简单字符串替换处理 ? 占位符，生产环境应改为服务端参数化查询。</p>
 */
public class DojaoPreparedStatement extends DojaoStatement implements PreparedStatement {

    private final String originalSql;
    private final List<Object> parameters;

    DojaoPreparedStatement(DojaoConnection connection, String sql) {
        super(connection);
        this.originalSql = sql;
        this.parameters = new ArrayList<>();
    }

    // ==================== Parameter substitution ====================

    private String buildSql() {
        if (parameters.isEmpty()) return originalSql;
        StringBuilder sb = new StringBuilder();
        int paramIdx = 0;
        for (int i = 0; i < originalSql.length(); i++) {
            char c = originalSql.charAt(i);
            if (c == '?' && paramIdx < parameters.size()) {
                sb.append(formatParam(parameters.get(paramIdx++)));
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private String formatParam(Object val) {
        return switch (val) {
            case null -> "NULL";
            case String s -> "'" + s.replace("'", "''") + "'";
            case Date date -> "'" + val + "'";
            case Timestamp timestamp -> "'" + val + "'";
            default -> String.valueOf(val);
        };
    }

    // ==================== execute methods ====================

    @Override
    public ResultSet executeQuery() throws SQLException {
        checkClosed();
        return executeQuery(buildSql());
    }

    @Override
    public int executeUpdate() throws SQLException {
        checkClosed();
        return executeUpdate(buildSql());
    }

    @Override
    public boolean execute() throws SQLException {
        checkClosed();
        return execute(buildSql());
    }

    @Override public void addBatch() { /* no-op for MVP */ }

    // ==================== setXxx ====================

    private void setParam(int idx, Object val) throws SQLException {
        // expand list to fit index (1-based)
        while (parameters.size() < idx) parameters.add(null);
        parameters.set(idx - 1, val);
    }

    @Override public void setNull(int idx, int sqlType) throws SQLException { setParam(idx, null); }
    @Override public void setNull(int idx, int sqlType, String typeName) throws SQLException { setParam(idx, null); }
    @Override public void setString(int idx, String x) throws SQLException { setParam(idx, x); }
    @Override public void setInt(int idx, int x) throws SQLException { setParam(idx, x); }
    @Override public void setLong(int idx, long x) throws SQLException { setParam(idx, x); }
    @Override public void setShort(int idx, short x) throws SQLException { setParam(idx, x); }
    @Override public void setByte(int idx, byte x) throws SQLException { setParam(idx, x); }
    @Override public void setFloat(int idx, float x) throws SQLException { setParam(idx, x); }
    @Override public void setDouble(int idx, double x) throws SQLException { setParam(idx, x); }
    @Override public void setBoolean(int idx, boolean x) throws SQLException { setParam(idx, x); }
    @Override public void setBigDecimal(int idx, BigDecimal x) throws SQLException { setParam(idx, x); }
    @Override public void setDate(int idx, Date x) throws SQLException { setParam(idx, x); }
    @Override public void setDate(int idx, Date x, Calendar cal) throws SQLException { setParam(idx, x); }
    @Override public void setTime(int idx, Time x) throws SQLException { setParam(idx, x); }
    @Override public void setTime(int idx, Time x, Calendar cal) throws SQLException { setParam(idx, x); }
    @Override public void setTimestamp(int idx, Timestamp x) throws SQLException { setParam(idx, x); }
    @Override public void setTimestamp(int idx, Timestamp x, Calendar cal) throws SQLException { setParam(idx, x); }
    @Override public void setBytes(int idx, byte[] x) throws SQLException { setParam(idx, x); }
    @Override public void setObject(int idx, Object x) throws SQLException { setParam(idx, x); }
    @Override public void setObject(int idx, Object x, int targetSqlType) throws SQLException { setParam(idx, x); }
    @Override public void setObject(int idx, Object x, int targetSqlType, int scaleOrLength) throws SQLException { setParam(idx, x); }
    @Override public void setAsciiStream(int idx, InputStream x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void setAsciiStream(int idx, InputStream x, int length) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void setAsciiStream(int idx, InputStream x, long length) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void setBinaryStream(int idx, InputStream x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void setBinaryStream(int idx, InputStream x, int length) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void setBinaryStream(int idx, InputStream x, long length) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void setCharacterStream(int idx, Reader x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void setCharacterStream(int idx, Reader x, int length) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void setCharacterStream(int idx, Reader x, long length) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void setBlob(int idx, Blob x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void setBlob(int idx, InputStream x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void setBlob(int idx, InputStream x, long length) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void setClob(int idx, Clob x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void setClob(int idx, Reader x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void setClob(int idx, Reader x, long length) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void setNClob(int idx, NClob x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void setNClob(int idx, Reader x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void setNClob(int idx, Reader x, long length) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void setNString(int idx, String x) throws SQLException { setString(idx, x); }
    @Override public void setNCharacterStream(int idx, Reader x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void setNCharacterStream(int idx, Reader x, long length) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void setArray(int idx, Array x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void setRef(int idx, Ref x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void setRowId(int idx, RowId x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void setSQLXML(int idx, SQLXML x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void setURL(int idx, URL x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void setUnicodeStream(int idx, InputStream x, int length) throws SQLException { throw DojaoConnection.notSupported(); }

    // ==================== MetaData ====================

    @Override public ResultSetMetaData getMetaData() throws SQLException {
        if (currentResultSet != null) return currentResultSet.getMetaData();
        return null;
    }

    @Override public ParameterMetaData getParameterMetaData() throws SQLException { throw DojaoConnection.notSupported(); }

    // ==================== Clear ====================

    @Override public void clearParameters() { parameters.clear(); }
}
