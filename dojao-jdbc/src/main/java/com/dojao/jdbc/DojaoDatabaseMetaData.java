package com.dojao.jdbc;

import java.sql.*;

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

    // ==================== getXxx (return empty ResultSets) ====================
    @Override public ResultSet getSchemas() throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public ResultSet getCatalogs() throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public ResultSet getTableTypes() throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public ResultSet getProcedures(String c, String s, String p) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public ResultSet getProcedureColumns(String c, String s, String p, String col) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public ResultSet getTables(String c, String s, String t, String[] types) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public ResultSet getColumns(String c, String s, String t, String col) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public ResultSet getColumnPrivileges(String c, String s, String t, String col) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public ResultSet getTablePrivileges(String c, String s, String t) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public ResultSet getBestRowIdentifier(String c, String s, String t, int scope, boolean nullable) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public ResultSet getVersionColumns(String c, String s, String t) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public ResultSet getPrimaryKeys(String c, String s, String t) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public ResultSet getImportedKeys(String c, String s, String t) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public ResultSet getExportedKeys(String c, String s, String t) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public ResultSet getCrossReference(String pc, String ps, String pt, String fc, String fs, String ft) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public ResultSet getTypeInfo() throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public ResultSet getIndexInfo(String c, String s, String t, boolean unique, boolean approximate) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public ResultSet getUDTs(String c, String s, String t, int[] types) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public ResultSet getSuperTypes(String c, String s, String t) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public ResultSet getSuperTables(String c, String s, String t) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public ResultSet getAttributes(String c, String s, String t, String name) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public ResultSet getClientInfoProperties() throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public ResultSet getFunctionColumns(String c, String s, String f, String col) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public ResultSet getPseudoColumns(String c, String s, String t, String col) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public boolean generatedKeyAlwaysReturned() { return false; }

    @Override public <T> T unwrap(Class<T> iface) throws SQLException { throw DojaoConnection.notSupported(); }
    @Override public boolean isWrapperFor(Class<?> iface) { return false; }
}
