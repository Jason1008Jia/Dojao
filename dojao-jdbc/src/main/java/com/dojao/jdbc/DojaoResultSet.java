package com.dojao.jdbc;

import com.dojao.core.rpc.QueryResponse;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

/**
 * JDBC ResultSet 实现，包装 Flight 返回的 QueryResponse。
 *
 * <p>前向只读游标，所有值均为 String 类型（来自 protobuf）。</p>
 */
public class DojaoResultSet implements ResultSet {

    private final DojaoStatement statement;
    private final QueryResponse response;
    private final int rowCount;
    private int currentRow = -1;  // -1 = beforeFirst
    private boolean closed;
    private String lastValue;

    DojaoResultSet(DojaoStatement statement, QueryResponse response) {
        this.statement = statement;
        this.response = response;
        this.rowCount = response.getRowsCount();
    }

    /** 用于元数据构造的合成结果集（无关联 Statement），{@link #getStatement()} 返回 null。 */
    DojaoResultSet(QueryResponse response) {
        this(null, response);
    }

    public QueryResponse getResponse() { return response; }

    // ==================== Cursor ====================

    @Override
    public boolean next() throws SQLException {
        checkClosed();
        if (currentRow + 1 < rowCount) {
            currentRow++;
            return true;
        }
        return false;
    }

    @Override public boolean isBeforeFirst() { return currentRow == -1; }
    @Override public boolean isFirst() { return currentRow == 0; }
    @Override public boolean isLast() { return currentRow == rowCount - 1; }
    @Override public boolean isAfterLast() { return currentRow >= rowCount; }
    @Override public int getRow() { return currentRow >= 0 && currentRow < rowCount ? currentRow + 1 : 0; }

    @Override public void beforeFirst() throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void afterLast() throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public boolean first() throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public boolean last() throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public boolean absolute(int row) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public boolean relative(int rows) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public boolean previous() throws SQLException { throw DojaoConnection.notSupported(); }

    @Override public void setFetchDirection(int direction) { /* no-op */ }
    @Override public int getFetchDirection() { return FETCH_FORWARD; }
    @Override public void setFetchSize(int rows) { /* no-op */ }
    @Override public int getFetchSize() { return 0; }
    @Override public int getType() { return TYPE_FORWARD_ONLY; }
    @Override public int getConcurrency() { return CONCUR_READ_ONLY; }

    // ==================== getString ====================

    @Override
    public String getString(int columnIndex) throws SQLException {
        checkPosition();
        String val = response.getRows(currentRow).getValues(columnIndex - 1);
        lastValue = val;
        return val;
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    // ==================== getXxx (parse from String) ====================

    @Override public boolean getBoolean(int ci) throws SQLException { return Boolean.parseBoolean(getString(ci)); }
    @Override public boolean getBoolean(String cl) throws SQLException { return getBoolean(findColumn(cl)); }
    @Override public byte getByte(int ci) throws SQLException { return Byte.parseByte(getString(ci)); }
    @Override public byte getByte(String cl) throws SQLException { return getByte(findColumn(cl)); }
    @Override public short getShort(int ci) throws SQLException { return Short.parseShort(getString(ci)); }
    @Override public short getShort(String cl) throws SQLException { return getShort(findColumn(cl)); }
    @Override public int getInt(int ci) throws SQLException { return Integer.parseInt(getString(ci)); }
    @Override public int getInt(String cl) throws SQLException { return getInt(findColumn(cl)); }
    @Override public long getLong(int ci) throws SQLException { return Long.parseLong(getString(ci)); }
    @Override public long getLong(String cl) throws SQLException { return getLong(findColumn(cl)); }
    @Override public float getFloat(int ci) throws SQLException { return Float.parseFloat(getString(ci)); }
    @Override public float getFloat(String cl) throws SQLException { return getFloat(findColumn(cl)); }
    @Override public double getDouble(int ci) throws SQLException { return Double.parseDouble(getString(ci)); }
    @Override public double getDouble(String cl) throws SQLException { return getDouble(findColumn(cl)); }
    @Override public BigDecimal getBigDecimal(int ci) throws SQLException { return new BigDecimal(getString(ci)); }
    @Override public BigDecimal getBigDecimal(String cl) throws SQLException { return getBigDecimal(findColumn(cl)); }
    @Override public BigDecimal getBigDecimal(int ci, int scale) throws SQLException { return getBigDecimal(ci).setScale(scale); }
    @Override public BigDecimal getBigDecimal(String cl, int scale) throws SQLException { return getBigDecimal(cl).setScale(scale); }
    @Override public Object getObject(int ci) throws SQLException { return getString(ci); }
    @Override public Object getObject(String cl) throws SQLException { return getString(cl); }
    @Override public Object getObject(int ci, Map<String, Class<?>> map) throws SQLException { return getObject(ci); }
    @Override public Object getObject(String cl, Map<String, Class<?>> map) throws SQLException { return getObject(cl); }

    // ==================== Date / Time ====================

    @Override public Date getDate(int ci) throws SQLException { return Date.valueOf(getString(ci)); }
    @Override public Date getDate(String cl) throws SQLException { return getDate(findColumn(cl)); }
    @Override public Date getDate(int ci, Calendar cal) throws SQLException { return getDate(ci); }
    @Override public Date getDate(String cl, Calendar cal) throws SQLException { return getDate(cl); }
    @Override public Time getTime(int ci) throws SQLException { return Time.valueOf(getString(ci)); }
    @Override public Time getTime(String cl) throws SQLException { return getTime(findColumn(cl)); }
    @Override public Time getTime(int ci, Calendar cal) throws SQLException { return getTime(ci); }
    @Override public Time getTime(String cl, Calendar cal) throws SQLException { return getTime(cl); }
    @Override public Timestamp getTimestamp(int ci) throws SQLException { return Timestamp.valueOf(getString(ci)); }
    @Override public Timestamp getTimestamp(String cl) throws SQLException { return getTimestamp(findColumn(cl)); }
    @Override public Timestamp getTimestamp(int ci, Calendar cal) throws SQLException { return getTimestamp(ci); }
    @Override public Timestamp getTimestamp(String cl, Calendar cal) throws SQLException { return getTimestamp(cl); }

    // ==================== null was ====================

    @Override
    public boolean wasNull() { return lastValue == null; }

    // ==================== Column lookup ====================

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        List<com.dojao.core.rpc.Column> cols = response.getColumnsList();
        for (int i = 0; i < cols.size(); i++) {
            if (cols.get(i).getName().equalsIgnoreCase(columnLabel)) {
                return i + 1; // JDBC is 1-based
            }
        }
        throw new SQLException("Column not found: " + columnLabel);
    }

    // ==================== Metadata ====================

    @Override
    public ResultSetMetaData getMetaData() {
        return new DojaoResultSetMetaData(response.getColumnsList());
    }

    // ==================== Close ====================

    @Override
    public void close() { closed = true; }

    @Override public boolean isClosed() { return closed; }

    @Override public Statement getStatement() { return statement; }

    // ==================== Unsupported ====================

    @Override public String getCursorName() { return ""; }
    @Override public SQLWarning getWarnings() { return null; }
    @Override public void clearWarnings() { /* no-op */ }
    @Override public int getHoldability() { return CLOSE_CURSORS_AT_COMMIT; }
    @Override public boolean rowUpdated() { return false; }
    @Override public boolean rowInserted() { return false; }
    @Override public boolean rowDeleted() { return false; }
    @Override public <T> T getObject(int ci, Class<T> type) throws SQLException { return type.cast(getString(ci)); }
    @Override public <T> T getObject(String cl, Class<T> type) throws SQLException { return type.cast(getString(cl)); }

    // ==================== Not supported ====================

    @Override public void updateNull(int ci) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateNull(String cl) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateString(int ci, String x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateString(String cl, String x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateInt(int ci, int x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateInt(String cl, int x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateBoolean(int ci, boolean x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateBoolean(String cl, boolean x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateByte(int ci, byte x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateByte(String cl, byte x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateShort(int ci, short x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateShort(String cl, short x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateLong(int ci, long x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateLong(String cl, long x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateFloat(int ci, float x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateFloat(String cl, float x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateDouble(int ci, double x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateDouble(String cl, double x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateBigDecimal(int ci, BigDecimal x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateBigDecimal(String cl, BigDecimal x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateBytes(int ci, byte[] x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateBytes(String cl, byte[] x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateDate(int ci, Date x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateDate(String cl, Date x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateTime(int ci, Time x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateTime(String cl, Time x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateTimestamp(int ci, Timestamp x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateTimestamp(String cl, Timestamp x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateObject(int ci, Object x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateObject(String cl, Object x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateObject(int ci, Object x, int scale) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateObject(String cl, Object x, int scale) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateAsciiStream(int ci, InputStream x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateAsciiStream(String cl, InputStream x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateAsciiStream(int ci, InputStream x, int length) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateAsciiStream(String cl, InputStream x, int length) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateAsciiStream(int ci, InputStream x, long length) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateAsciiStream(String cl, InputStream x, long length) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateBinaryStream(int ci, InputStream x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateBinaryStream(String cl, InputStream x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateBinaryStream(int ci, InputStream x, int length) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateBinaryStream(String cl, InputStream x, int length) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateBinaryStream(int ci, InputStream x, long length) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateBinaryStream(String cl, InputStream x, long length) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateCharacterStream(int ci, Reader x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateCharacterStream(String cl, Reader x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateCharacterStream(int ci, Reader x, int length) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateCharacterStream(String cl, Reader x, int length) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateCharacterStream(int ci, Reader x, long length) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateCharacterStream(String cl, Reader x, long length) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateRef(int ci, Ref x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateRef(String cl, Ref x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateBlob(int ci, Blob x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateBlob(String cl, Blob x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateBlob(int ci, InputStream x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateBlob(String cl, InputStream x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateBlob(int ci, InputStream x, long length) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateBlob(String cl, InputStream x, long length) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateClob(int ci, Clob x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateClob(String cl, Clob x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateClob(int ci, Reader x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateClob(String cl, Reader x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateClob(int ci, Reader x, long length) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateClob(String cl, Reader x, long length) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateArray(int ci, Array x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateArray(String cl, Array x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateRowId(int ci, RowId x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateRowId(String cl, RowId x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateNString(int ci, String x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateNString(String cl, String x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateNClob(int ci, NClob x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateNClob(String cl, NClob x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateNClob(int ci, Reader x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateNClob(String cl, Reader x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateNClob(int ci, Reader x, long length) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateNClob(String cl, Reader x, long length) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateSQLXML(int ci, SQLXML x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateSQLXML(String cl, SQLXML x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateNCharacterStream(int ci, Reader x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateNCharacterStream(String cl, Reader x) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateNCharacterStream(int ci, Reader x, long length) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateNCharacterStream(String cl, Reader x, long length) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void insertRow() throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void updateRow() throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void deleteRow() throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void refreshRow() throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void cancelRowUpdates() throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void moveToInsertRow() throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public void moveToCurrentRow() throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public RowId getRowId(int ci) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public RowId getRowId(String cl) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public Ref getRef(int ci) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public Ref getRef(String cl) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public Blob getBlob(int ci) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public Blob getBlob(String cl) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public Clob getClob(int ci) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public Clob getClob(String cl) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public Array getArray(int ci) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public Array getArray(String cl) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public URL getURL(int ci) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public URL getURL(String cl) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public NClob getNClob(int ci) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public NClob getNClob(String cl) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public SQLXML getSQLXML(int ci) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public SQLXML getSQLXML(String cl) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public String getNString(int ci) throws SQLException { return getString(ci); }
    @Override public String getNString(String cl) throws SQLException { return getString(cl); }
    @Override public Reader getNCharacterStream(int ci) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public Reader getNCharacterStream(String cl) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public Reader getCharacterStream(int ci) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public Reader getCharacterStream(String cl) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public InputStream getAsciiStream(int ci) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public InputStream getAsciiStream(String cl) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public InputStream getUnicodeStream(int ci) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public InputStream getUnicodeStream(String cl) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public InputStream getBinaryStream(int ci) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public InputStream getBinaryStream(String cl) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public byte[] getBytes(int ci) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public byte[] getBytes(String cl) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public <T> T unwrap(Class<T> iface) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public boolean isWrapperFor(Class<?> iface) { return false; }

    // ==================== Internal ====================

    private void checkPosition() throws SQLException {
        checkClosed();
        if (currentRow < 0 || currentRow >= rowCount) {
            throw new SQLException("Invalid cursor position: " + currentRow + ". Call next() first.");
        }
    }

    private void checkClosed() throws SQLException {
        if (closed) throw new SQLException("ResultSet is closed");
    }
}
