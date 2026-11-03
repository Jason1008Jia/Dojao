package com.dojao.jdbc;

import com.dojao.core.rpc.Column;
import com.dojao.core.rpc.QueryResponse;
import com.dojao.core.rpc.Row;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * JDBC 元数据实现的内部工具。
 *
 * <p>引擎的元数据语句（SHOW CATALOGS / SHOW SCHEMAS / SHOW TABLES / SHOW COLUMNS）
 * 通过普通 {@code execute_query} 返回字符串结果；这里负责执行语句、按 JDBC 标准列名
 * 重建 {@link QueryResponse}，以及 LIKE 模式匹配与类型映射。</p>
 */
final class JdbcMetadata {

    private JdbcMetadata() {
    }

    /** 执行一条元数据 SQL，按行收集为字符串数组（不做列名映射）。 */
    static List<String[]> query(DojaoConnection connection, String sql) throws SQLException {
        List<String[]> rows = new ArrayList<>();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            int columnCount = md.getColumnCount();
            while (rs.next()) {
                String[] row = new String[columnCount];
                for (int i = 1; i <= columnCount; i++) {
                    row[i - 1] = rs.getString(i);
                }
                rows.add(row);
            }
        }
        return rows;
    }

    /** 用给定的 JDBC 标准列名和行数据构造一个只读 ResultSet。 */
    static DojaoResultSet resultSet(String[] columnNames, List<String[]> rows) {
        QueryResponse.Builder builder = QueryResponse.newBuilder();
        for (String name : columnNames) {
            builder.addColumns(Column.newBuilder().setName(name).setType("VARCHAR").build());
        }
        for (String[] values : rows) {
            Row.Builder row = Row.newBuilder();
            for (String value : values) {
                row.addValues(value == null ? "" : value);
            }
            builder.addRows(row.build());
        }
        return new DojaoResultSet(builder.build());
    }

    /** JDBC LIKE 模式匹配：{@code %} 任意串、{@code _} 单字符、{@code \} 转义；pattern 为 null 表示匹配全部。 */
    static boolean matches(String pattern, String value) {
        if (pattern == null) {
            return true;
        }
        if (value == null) {
            return false;
        }
        StringBuilder regex = new StringBuilder("^");
        boolean escaped = false;
        for (char c : pattern.toCharArray()) {
            if (escaped) {
                regex.append(Pattern.quote(String.valueOf(c)));
                escaped = false;
            } else if (c == '\\') {
                escaped = true;
            } else if (c == '%') {
                regex.append(".*");
            } else if (c == '_') {
                regex.append('.');
            } else {
                regex.append(Pattern.quote(String.valueOf(c)));
            }
        }
        if (escaped) {
            regex.append(Pattern.quote("\\"));
        }
        regex.append('$');
        return value.matches(regex.toString());
    }

    /** Dojao 类型名字符串 → {@link java.sql.Types}。未识别类型回退 VARCHAR。 */
    static int sqlType(String typeName) {
        if (typeName == null) {
            return Types.VARCHAR;
        }
        return switch (typeName.toUpperCase()) {
            case "VARCHAR", "CHAR", "TEXT", "STRING" -> Types.VARCHAR;
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
