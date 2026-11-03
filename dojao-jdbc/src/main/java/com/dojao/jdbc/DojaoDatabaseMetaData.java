package com.dojao.jdbc;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * JDBC DatabaseMetaData 实现（最小集）。
 */
public class DojaoDatabaseMetaData implements DatabaseMetaData {

    private final DojaoConnection connection;

    DojaoDatabaseMetaData(DojaoConnection connection) {
        this.connection = connection;
    }

    @Override public String getDriverName() { return DojaoDriverInfo.DRIVER_NAME; }
    @Override public String getDriverVersion() { return DojaoDriverInfo.DRIVER_VERSION; }
    @Override public int getDriverMajorVersion() { return DojaoDriverInfo.DRIVER_MAJOR; }
    @Override public int getDriverMinorVersion() { return DojaoDriverInfo.DRIVER_MINOR; }
    @Override public String getDatabaseProductName() { return DojaoDriverInfo.DB_PRODUCT_NAME; }
    @Override public String getDatabaseProductVersion() { return DojaoDriverInfo.DB_PRODUCT_VERSION; }
    @Override public int getDatabaseMajorVersion() { return 1; }
    @Override public int getDatabaseMinorVersion() { return 0; }
    @Override public String getURL() { return connection.url; }
    @Override public Connection getConnection() { return connection; }

    @Override public boolean allProceduresAreCallable() { return false; }
    @Override public boolean allTablesAreSelectable() { return true; }
    @Override public boolean isReadOnly() { return true; }
    @Override public boolean nullsAreSortedHigh() { return false; }
    @Override public boolean nullsAreSortedLow() { return true; }
    @Override public boolean nullsAreSortedAtStart() { return false; }
    @Override public boolean nullsAreSortedAtEnd() { return false; }
    @Override public boolean usesLocalFiles() { return false; }
    @Override public boolean usesLocalFilePerTable() { return false; }
    @Override public boolean supportsMixedCaseIdentifiers() { return false; }
    @Override public boolean storesUpperCaseIdentifiers() { return false; }
    @Override public boolean storesLowerCaseIdentifiers() { return false; }
    @Override public boolean storesMixedCaseIdentifiers() { return true; }
    @Override public boolean supportsMixedCaseQuotedIdentifiers() { return false; }
    @Override public boolean storesUpperCaseQuotedIdentifiers() { return false; }
    @Override public boolean storesLowerCaseQuotedIdentifiers() { return false; }
    @Override public boolean storesMixedCaseQuotedIdentifiers() { return false; }
    @Override public boolean supportsMinimumSQLGrammar() { return false; }
    @Override public boolean nullPlusNonNullIsNull() { return true; }
    @Override public boolean supportsSchemasInProcedureCalls() { return false; }
    @Override public boolean supportsCatalogsInProcedureCalls() { return false; }
    @Override public boolean doesMaxRowSizeIncludeBlobs() { return false; }
    @Override public String getUserName() { return "dojao"; }
    @Override public String getIdentifierQuoteString() { return "\""; }
    @Override public String getSQLKeywords() { return ""; }
    @Override public String getNumericFunctions() { return ""; }
    @Override public String getStringFunctions() { return ""; }
    @Override public String getSystemFunctions() { return ""; }
    @Override public String getTimeDateFunctions() { return ""; }
    @Override public String getSearchStringEscape() { return "\\"; }
    @Override public String getExtraNameCharacters() { return ""; }
    @Override public String getSchemaTerm() { return "schema"; }
    @Override public String getProcedureTerm() { return "procedure"; }
    @Override public String getCatalogTerm() { return "catalog"; }
    @Override public String getCatalogSeparator() { return "."; }
    @Override public int getMaxBinaryLiteralLength() { return 0; }
    @Override public int getMaxCharLiteralLength() { return 0; }
    @Override public int getMaxColumnNameLength() { return 0; }
    @Override public int getMaxColumnsInGroupBy() { return 0; }
    @Override public int getMaxColumnsInIndex() { return 0; }
    @Override public int getMaxColumnsInOrderBy() { return 0; }
    @Override public int getMaxColumnsInSelect() { return 0; }
    @Override public int getMaxColumnsInTable() { return 0; }
    @Override public int getMaxConnections() { return 0; }
    @Override public int getMaxCursorNameLength() { return 0; }
    @Override public int getMaxIndexLength() { return 0; }
    @Override public int getMaxSchemaNameLength() { return 0; }
    @Override public int getMaxProcedureNameLength() { return 0; }
    @Override public int getMaxCatalogNameLength() { return 0; }
    @Override public int getMaxRowSize() { return 0; }
    @Override public int getMaxStatementLength() { return 0; }
    @Override public int getMaxStatements() { return 0; }
    @Override public int getMaxTableNameLength() { return 0; }
    @Override public int getMaxTablesInSelect() { return 0; }
    @Override public int getMaxUserNameLength() { return 0; }
    @Override public int getDefaultTransactionIsolation() { return Connection.TRANSACTION_NONE; }
    @Override public int getJDBCMajorVersion() { return 4; }
    @Override public int getJDBCMinorVersion() { return 3; }
    @Override public int getResultSetHoldability() { return ResultSet.CLOSE_CURSORS_AT_COMMIT; }
    @Override public int getSQLStateType() { return sqlStateSQL; }
    @Override public boolean locatorsUpdateCopy() { return false; }
    @Override public boolean supportsAlterTableWithAddColumn() { return false; }
    @Override public boolean supportsAlterTableWithDropColumn() { return false; }
    @Override public boolean supportsColumnAliasing() { return true; }
    @Override public boolean supportsConvert() { return false; }
    @Override public boolean supportsConvert(int fromType, int toType) { return false; }
    @Override public boolean supportsCoreSQLGrammar() { return false; }
    @Override public boolean supportsExtendedSQLGrammar() { return false; }
    @Override public boolean supportsANSI92EntryLevelSQL() { return false; }
    @Override public boolean supportsANSI92IntermediateSQL() { return false; }
    @Override public boolean supportsANSI92FullSQL() { return false; }
    @Override public boolean supportsCatalogsInDataManipulation() { return false; }
    @Override public boolean supportsSchemasInDataManipulation() { return false; }
    @Override public boolean supportsCatalogsInTableDefinitions() { return false; }
    @Override public boolean supportsSchemasInTableDefinitions() { return false; }
    @Override public boolean supportsCatalogsInIndexDefinitions() { return false; }
    @Override public boolean supportsSchemasInIndexDefinitions() { return false; }
    @Override public boolean supportsCatalogsInPrivilegeDefinitions() { return false; }
    @Override public boolean supportsSchemasInPrivilegeDefinitions() { return false; }
    @Override public boolean supportsCorrelatedSubqueries() { return false; }
    @Override public boolean supportsDataDefinitionAndDataManipulationTransactions() { return false; }
    @Override public boolean supportsDataManipulationTransactionsOnly() { return false; }
    @Override public boolean dataDefinitionCausesTransactionCommit() { return false; }
    @Override public boolean dataDefinitionIgnoredInTransactions() { return false; }
    @Override public boolean supportsDifferentTableCorrelationNames() { return false; }
    @Override public boolean supportsExpressionsInOrderBy() { return true; }
    @Override public boolean supportsOrderByUnrelated() { return false; }
    @Override public boolean supportsGroupBy() { return true; }
    @Override public boolean supportsGroupByUnrelated() { return false; }
    @Override public boolean supportsGroupByBeyondSelect() { return false; }
    @Override public boolean supportsLikeEscapeClause() { return true; }
    @Override public boolean supportsMultipleOpenResults() { return false; }
    @Override public boolean supportsMultipleResultSets() { return false; }
    @Override public boolean supportsMultipleTransactions() { return false; }
    @Override public boolean supportsNamedParameters() { return false; }
    @Override public boolean supportsNonNullableColumns() { return false; }
    @Override public boolean supportsOpenCursorsAcrossCommit() { return false; }
    @Override public boolean supportsOpenCursorsAcrossRollback() { return false; }
    @Override public boolean supportsOpenStatementsAcrossCommit() { return false; }
    @Override public boolean supportsOpenStatementsAcrossRollback() { return false; }
    @Override public boolean supportsPositionedDelete() { return false; }
    @Override public boolean supportsPositionedUpdate() { return false; }
    @Override public boolean supportsSelectForUpdate() { return false; }
    @Override public boolean supportsStoredProcedures() { return false; }
    @Override public boolean supportsSubqueriesInComparisons() { return false; }
    @Override public boolean supportsSubqueriesInExists() { return false; }
    @Override public boolean supportsSubqueriesInIns() { return false; }
    @Override public boolean supportsSubqueriesInQuantifieds() { return false; }
    @Override public boolean supportsTableCorrelationNames() { return false; }
    @Override public boolean supportsTransactionIsolationLevel(int level) { return false; }
    @Override public boolean supportsTransactions() { return false; }
    @Override public boolean supportsUnion() { return false; }
    @Override public boolean supportsUnionAll() { return false; }
    @Override public boolean supportsBatchUpdates() { return false; }
    @Override public boolean supportsOuterJoins() { return false; }
    @Override public boolean supportsFullOuterJoins() { return false; }
    @Override public boolean supportsLimitedOuterJoins() { return false; }
    @Override public boolean supportsIntegrityEnhancementFacility() { return false; }
    @Override public boolean isCatalogAtStart() { return true; }
    @Override public boolean supportsResultSetType(int type) { return type == ResultSet.TYPE_FORWARD_ONLY; }
    @Override public boolean supportsResultSetConcurrency(int type, int concurrency) {
        return type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY;
    }
    @Override public boolean ownUpdatesAreVisible(int type) { return false; }
    @Override public boolean ownDeletesAreVisible(int type) { return false; }
    @Override public boolean ownInsertsAreVisible(int type) { return false; }
    @Override public boolean othersUpdatesAreVisible(int type) { return false; }
    @Override public boolean othersDeletesAreVisible(int type) { return false; }
    @Override public boolean othersInsertsAreVisible(int type) { return false; }
    @Override public boolean updatesAreDetected(int type) { return false; }
    @Override public boolean deletesAreDetected(int type) { return false; }
    @Override public boolean insertsAreDetected(int type) { return false; }
    @Override public boolean supportsSavepoints() { return false; }
    @Override public boolean supportsStoredFunctionsUsingCallSyntax() { return false; }
    @Override public boolean autoCommitFailureClosesAllResultSets() { return false; }
    @Override public boolean supportsStatementPooling() { return false; }
    @Override public boolean supportsResultSetHoldability(int holdability) { return holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT; }
    @Override public boolean supportsGetGeneratedKeys() { return false; }

    // ==================== RowIdLifetime ====================
    @Override public RowIdLifetime getRowIdLifetime() { return RowIdLifetime.ROWID_UNSUPPORTED; }

    // ==================== Catalog / Schema / Table / Column ====================
    // 基于引擎已有的 SHOW 语句实现；结果按 JDBC 标准列名重建。

    @Override
    public ResultSet getCatalogs() throws SQLException {
        List<String[]> rows = new ArrayList<>();
        for (String catalog : catalogs()) {
            rows.add(new String[]{catalog});
        }
        return JdbcMetadata.resultSet(new String[]{"TABLE_CAT"}, rows);
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return getSchemas(null, null);
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        List<String[]> rows = new ArrayList<>();
        for (String cat : catalogOrAll(catalog)) {
            try {
                for (String schema : schemas(cat)) {
                    if (JdbcMetadata.matches(schemaPattern, schema)) {
                        rows.add(new String[]{schema, cat});
                    }
                }
            } catch (SQLException ignored) {
                // 单个 catalog 不可达时跳过，尽量返回其余结果
            }
        }
        return JdbcMetadata.resultSet(new String[]{"TABLE_SCHEM", "TABLE_CATALOG"}, rows);
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        List<String[]> rows = new ArrayList<>();
        rows.add(new String[]{"TABLE"});
        return JdbcMetadata.resultSet(new String[]{"TABLE_TYPE"}, rows);
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern,
                               String[] types) throws SQLException {
        List<String[]> rows = new ArrayList<>();
        if (types == null || containsIgnoreCase(types, "TABLE")) {
            for (String cat : catalogOrAll(catalog)) {
                try {
                    for (String schema : schemas(cat)) {
                        if (!JdbcMetadata.matches(schemaPattern, schema)) {
                            continue;
                        }
                        try {
                            for (String table : tables(cat, schema)) {
                                if (JdbcMetadata.matches(tableNamePattern, table)) {
                                    rows.add(new String[]{
                                            cat, schema, table, "TABLE",
                                            "", "", "", "", "", ""
                                    });
                                }
                            }
                        } catch (SQLException ignored) {
                            // schema 不可达时跳过
                        }
                    }
                } catch (SQLException ignored) {
                    // catalog 不可达时跳过
                }
            }
        }
        return JdbcMetadata.resultSet(new String[]{
                "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS",
                "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION"
        }, rows);
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern,
                                String columnNamePattern) throws SQLException {
        List<String[]> rows = new ArrayList<>();
        for (String cat : catalogOrAll(catalog)) {
            try {
                for (String schema : schemas(cat)) {
                    if (!JdbcMetadata.matches(schemaPattern, schema)) {
                        continue;
                    }
                    try {
                        for (String table : tables(cat, schema)) {
                            if (!JdbcMetadata.matches(tableNamePattern, table)) {
                                continue;
                            }
                            try {
                                List<String[]> columns = columns(cat, schema, table);
                                for (int i = 0; i < columns.size(); i++) {
                                    String[] column = columns.get(i);
                                    String name = get(column, 0);
                                    String type = get(column, 1);
                                    String nullable = get(column, 2);
                                    String defaultValue = get(column, 3);
                                    String comment = get(column, 4);
                                    if (!JdbcMetadata.matches(columnNamePattern, name)) {
                                        continue;
                                    }
                                    boolean isNullable = !"NO".equalsIgnoreCase(nullable);
                                    rows.add(new String[]{
                                            cat, schema, table, name,
                                            String.valueOf(JdbcMetadata.sqlType(type)), type,
                                            "0", "0", "0", "10",
                                            isNullable ? String.valueOf(columnNullable) : String.valueOf(columnNoNulls),
                                            comment, defaultValue,
                                            "0", "0", "0",
                                            String.valueOf(i + 1),
                                            isNullable ? "YES" : "NO",
                                            "", "", "",
                                            "0", "NO", "NO"
                                    });
                                }
                            } catch (SQLException ignored) {
                                // 单表不可达时跳过
                            }
                        }
                    } catch (SQLException ignored) {
                        // schema 不可达时跳过
                    }
                }
            } catch (SQLException ignored) {
                // catalog 不可达时跳过
            }
        }
        return JdbcMetadata.resultSet(new String[]{
                "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME",
                "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS",
                "NUM_PREC_RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF",
                "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION",
                "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE",
                "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN"
        }, rows);
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        return emptyResult("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME");
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return emptyResult("PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME",
                "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ",
                "UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME", "DEFERRABILITY");
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return getImportedKeys(catalog, schema, table);
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
                                       String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        return getImportedKeys(parentCatalog, parentSchema, parentTable);
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table,
                                  boolean unique, boolean approximate) throws SQLException {
        return emptyResult("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "NON_UNIQUE", "INDEX_QUALIFIER",
                "INDEX_NAME", "TYPE", "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC",
                "CARDINALITY", "PAGES", "FILTER_CONDITION");
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table,
                                          int scope, boolean nullable) throws SQLException {
        return emptyResult("SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE",
                "BUFFER_LENGTH", "DECIMAL_DIGITS", "PSEUDO_COLUMN");
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return emptyResult("SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE",
                "BUFFER_LENGTH", "DECIMAL_DIGITS", "PSEUDO_COLUMN");
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        String[][] types = {
                // TYPE_NAME, PRECISION, LITERAL_PREFIX, LITERAL_SUFFIX, CREATE_PARAMS, CASE_SENSITIVE
                {"VARCHAR", "65535", "'", "'", "length", "true"},
                {"INTEGER", "10", "", "", "", "false"},
                {"BIGINT", "19", "", "", "", "false"},
                {"SMALLINT", "5", "", "", "", "false"},
                {"TINYINT", "3", "", "", "", "false"},
                {"FLOAT", "7", "", "", "", "false"},
                {"DOUBLE", "15", "", "", "", "false"},
                {"DECIMAL", "38", "", "", "precision,scale", "false"},
                {"BOOLEAN", "1", "", "", "", "false"},
                {"DATE", "10", "'", "'", "", "false"},
                {"TIME", "8", "'", "'", "", "false"},
                {"TIMESTAMP", "26", "'", "'", "", "false"},
                {"BLOB", "0", "", "", "", "false"}
        };
        List<String[]> rows = new ArrayList<>();
        for (String[] t : types) {
            rows.add(new String[]{
                    t[0], String.valueOf(JdbcMetadata.sqlType(t[0])), t[1], t[2], t[3], t[4],
                    String.valueOf(typeNullable), t[5], String.valueOf(typeSearchable),
                    "false", "false", "false", t[0], "0", "0", "0", "0", "10"
            });
        }
        return JdbcMetadata.resultSet(new String[]{
                "TYPE_NAME", "DATA_TYPE", "PRECISION", "LITERAL_PREFIX", "LITERAL_SUFFIX",
                "CREATE_PARAMS", "NULLABLE", "CASE_SENSITIVE", "SEARCHABLE", "UNSIGNED_ATTRIBUTE",
                "FIXED_PREC_SCALE", "AUTO_INCREMENT", "LOCAL_TYPE_NAME", "MINIMUM_SCALE",
                "MAXIMUM_SCALE", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "NUM_PREC_RADIX"
        }, rows);
    }

    // ==================== 无对应能力的元数据：返回空结果集 ====================

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        return emptyResult("PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME",
                "RESERVED1", "RESERVED2", "RESERVED3", "REMARKS", "PROCEDURE_TYPE", "SPECIFIC_NAME");
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
                                         String columnNamePattern) throws SQLException {
        return emptyResult("PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "COLUMN_NAME",
                "COLUMN_TYPE", "DATA_TYPE", "TYPE_NAME", "PRECISION", "LENGTH", "SCALE", "RADIX",
                "NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB",
                "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE", "SPECIFIC_NAME");
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table,
                                         String columnNamePattern) throws SQLException {
        return emptyResult("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "GRANTOR",
                "GRANTEE", "PRIVILEGE", "IS_GRANTABLE");
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return emptyResult("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "GRANTOR", "GRANTEE",
                "PRIVILEGE", "IS_GRANTABLE");
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        return emptyResult("TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "CLASS_NAME", "DATA_TYPE",
                "REMARKS", "BASE_TYPE");
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return emptyResult("TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SUPERTYPE_CAT",
                "SUPERTYPE_SCHEM", "SUPERTYPE_NAME");
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return emptyResult("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "SUPERTABLE_NAME");
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
                                   String attributeNamePattern) throws SQLException {
        return emptyResult("TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "ATTR_NAME", "DATA_TYPE",
                "ATTR_TYPE_NAME", "ATTR_SIZE", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE",
                "REMARKS", "ATTR_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH",
                "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE",
                "SOURCE_DATA_TYPE");
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return emptyResult("NAME", "MAX_LEN", "DEFAULT_VALUE", "DESCRIPTION");
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        return emptyResult("FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "REMARKS",
                "FUNCTION_TYPE", "SPECIFIC_NAME");
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
                                        String columnNamePattern) throws SQLException {
        return emptyResult("FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "COLUMN_NAME",
                "COLUMN_TYPE", "DATA_TYPE", "TYPE_NAME", "PRECISION", "LENGTH", "SCALE", "RADIX",
                "NULLABLE", "REMARKS", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE",
                "SPECIFIC_NAME");
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
                                      String columnNamePattern) throws SQLException {
        return emptyResult("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE",
                "COLUMN_SIZE", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "COLUMN_USAGE", "REMARKS",
                "CHAR_OCTET_LENGTH", "IS_NULLABLE");
    }

    @Override public boolean generatedKeyAlwaysReturned() { return false; }

    @Override public <T> T unwrap(Class<T> iface) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public boolean isWrapperFor(Class<?> iface) { return false; }

    // ==================== Internal ====================

    private ResultSet emptyResult(String... columnNames) {
        return JdbcMetadata.resultSet(columnNames, List.of());
    }

    private List<String> catalogOrAll(String catalog) throws SQLException {
        return catalog == null ? catalogs() : List.of(catalog);
    }

    private List<String> catalogs() throws SQLException {
        List<String> names = new ArrayList<>();
        for (String[] row : JdbcMetadata.query(connection, "SHOW CATALOGS")) {
            String name = get(row, 0);
            if (name != null && !name.isEmpty()) {
                names.add(name);
            }
        }
        return names;
    }

    private List<String> schemas(String catalog) throws SQLException {
        List<String> names = new ArrayList<>();
        for (String[] row : JdbcMetadata.query(connection, "SHOW SCHEMAS FROM " + catalog)) {
            String name = get(row, 0);
            if (name != null && !name.isEmpty()) {
                names.add(name);
            }
        }
        return names;
    }

    private List<String> tables(String catalog, String schema) throws SQLException {
        List<String> names = new ArrayList<>();
        for (String[] row : JdbcMetadata.query(connection,
                "SHOW TABLES FROM " + catalog + "." + schema)) {
            String name = get(row, 0);
            if (name != null && !name.isEmpty()) {
                names.add(name);
            }
        }
        return names;
    }

    private List<String[]> columns(String catalog, String schema, String table) throws SQLException {
        return JdbcMetadata.query(connection,
                "SHOW COLUMNS FROM " + catalog + "." + schema + "." + table);
    }

    private static String get(String[] row, int index) {
        return index < row.length && row[index] != null ? row[index] : "";
    }

    private static boolean containsIgnoreCase(String[] values, String target) {
        for (String value : values) {
            if (target.equalsIgnoreCase(value)) {
                return true;
            }
        }
        return false;
    }
}
