package org.mariadb.jdbc;

import org.mariadb.jdbc.internal.com.read.resultset.SelectResultSet;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OceanBaseOracleDatabaseMetadata implements DatabaseMetaData {

    public static final String DRIVER_NAME = "MariaDB Connector/J";
    private final UrlParser urlParser;
    private final MariaDbConnection connection;
    protected static Pattern sqlWildcardPattern = null;

    public OceanBaseOracleDatabaseMetadata(UrlParser urlParser, Connection connection) {
        this.urlParser = urlParser;
        this.connection = (MariaDbConnection) connection;
    }

    public ResultSet executeQuery(String sql) throws SQLException {
        Statement stmt = connection.createStatement();
        SelectResultSet rs = (SelectResultSet) stmt.executeQuery(sql);
        rs.setStatement(null); // bypass Hibernate statement tracking (CONJ-49)
        rs.setForceTableAlias();
        return rs;
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return false;
    }

    @Override
    public String getURL() throws SQLException {
        return null;
    }

    @Override
    public String getUserName() throws SQLException {
        return null;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return null;
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return null;
    }

    @Override
    public String getDriverName() throws SQLException {
        return null;
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return null;
    }

    @Override
    public int getDriverMajorVersion() {
        return 0;
    }

    @Override
    public int getDriverMinorVersion() {
        return 0;
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return null;
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return null;
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return null;
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return null;
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return null;
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return null;
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return null;
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return null;
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return false;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return null;
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return null;
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return null;
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return false;
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return null;
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return 0;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        String var1 = "SELECT\n  -- Standalone procedures and functions\n  NULL AS procedure_cat,\n  owner AS procedure_schem,\n  object_name AS procedure_name,\n  NULL,\n  NULL,\n  NULL,\n  'Standalone procedure or function' AS remarks,\n  DECODE(object_type, 'PROCEDURE', 1,\n                      'FUNCTION', 2,\n                      0) AS procedure_type\n,  NULL AS specific_name\nFROM all_objects\nWHERE (object_type = 'PROCEDURE' OR object_type = 'FUNCTION')\n  AND owner LIKE ? \n  AND object_name LIKE ? \n";
        String var2 = "SELECT\n  -- Packaged procedures with no arguments\n  package_name AS procedure_cat,\n  owner AS procedure_schem,\n  object_name AS procedure_name,\n  NULL,\n  NULL,\n  NULL,\n  'Packaged procedure' AS remarks,\n  1 AS procedure_type\n,  NULL AS specific_name\nFROM all_arguments\nWHERE argument_name IS NULL\n  AND data_type IS NULL\n  AND ";
        String var3 = "SELECT\n  -- Packaged procedures with arguments\n  package_name AS procedure_cat,\n  owner AS procedure_schem,\n  object_name AS procedure_name,\n  NULL,\n  NULL,\n  NULL,\n  'Packaged procedure' AS remarks,\n  1 AS procedure_type\n,  NULL AS specific_name\nFROM all_arguments\nWHERE argument_name IS NOT NULL\n  AND position = 1\n  AND position = sequence\n  AND ";
        String var4 = "SELECT\n  -- Packaged functions\n  package_name AS procedure_cat,\n  owner AS procedure_schem,\n  object_name AS procedure_name,\n  NULL,\n  NULL,\n  NULL,\n  'Packaged function' AS remarks,\n  2 AS procedure_type\n,  NULL AS specific_name\nFROM all_arguments\nWHERE argument_name IS NULL\n  AND in_out = 'OUT'\n  AND   data_level = 0\n  AND ";
        String var5 = "package_name LIKE ? \n  AND owner LIKE ? \n  AND object_name LIKE ? \n";
        String var6 = "package_name IS NOT NULL\n  AND owner LIKE ? \n  AND object_name LIKE ? \n";
        String var7 = "ORDER BY procedure_schem, procedure_name\n";
        PreparedStatement stmt = null;
        String var8 = null;
        String var9 = null;
        if (schemaPattern == null) {
            var9 = "%";
        } else if (schemaPattern.equals("")) {
            var9 = this.getUserName().toUpperCase();
        } else {
            var9 = schemaPattern.toUpperCase();
        }

        String var14 = null;
        if (procedureNamePattern == null) {
            var14 = "%";
        } else if (procedureNamePattern.equals("")) {
            throw new SQLException();
        } else {
            var14 = procedureNamePattern.toUpperCase();
        }

        if (catalog == null) {
            var8 = var1 + "UNION ALL " + var2 + var6 + "UNION ALL " + var3 + var6 + "UNION ALL " + var4 + var6 + var7;
            stmt = this.connection.prepareStatement(var8);
            stmt.setString(1, var9);
            stmt.setString(2, var14);
            stmt.setString(3, var9);
            stmt.setString(4, var14);
            stmt.setString(5, var9);
            stmt.setString(6, var14);
            stmt.setString(7, var9);
            stmt.setString(8, var14);
        } else if (catalog.equals("")) {
            stmt = this.connection.prepareStatement(var1);
            stmt.setString(1, var9);
            stmt.setString(2, var14);
        } else {
            var8 = var2 + var5 + "UNION ALL " + var3 + var5 + "UNION ALL " + var4 + var5 + var7;
            stmt = this.connection.prepareStatement(var8);
            stmt.setString(1, catalog);
            stmt.setString(2, var9);
            stmt.setString(3, var14);
            stmt.setString(4, catalog);
            stmt.setString(5, var9);
            stmt.setString(6, var14);
            stmt.setString(7, catalog);
            stmt.setString(8, var9);
            stmt.setString(9, var14);
        }

        ResultSet resultSet = stmt.executeQuery();
        return resultSet;
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        String var1 = "SELECT NULL AS table_cat,\n       o.owner AS table_schem,\n       o.object_name AS table_name,\n       o.object_type AS table_type,\n";
        String var2 = "       NULL AS remarks\n";
        String var3 = "  FROM all_objects o\n";
        String var4 = "  WHERE o.owner LIKE ? \n    AND o.object_name LIKE ? \n";
        String var5 = "";
        String var6 = "";
        if (types != null) {
            var5 = "    AND o.object_type IN ('xxx'";
            var6 = "    AND o.object_type IN ('xxx'";
            for (int i = 0; i < types.length; ++i) {
                if (types[i].equals("SYNONYM")) {
                    var5 = var5 + ", '" + types[i] + "'";
                } else {
                    var5 = var5 + ", '" + types[i] + "'";
                    var6 = var6 + ", '" + types[i] + "'";
                }
            }
            var5 = var5 + ")\n";
        } else {
            var5 = "    AND o.object_type IN ('TABLE', 'SYNONYM', 'VIEW')\n";
        }
        String var7 = "  ORDER BY table_type, table_schem, table_name\n";
        String sql = "";
        sql = sql + var1;
        sql = sql + var2 + var3;
        sql = sql + var4;
        sql = sql + var5;
        sql = sql + var7;
        PreparedStatement stmt = this.connection.prepareStatement(sql);
        stmt.setString(1, schemaPattern == null ? "%" : schemaPattern);
        stmt.setString(2, tableNamePattern == null ? "%" : tableNamePattern);
        ResultSet resultSet = stmt.executeQuery();
        return resultSet;
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        return executeQuery("select 'nothing' as table_cat from dual where 1 = 2");
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
//        String var1 = "SELECT ";
//        String var2 = " NULL AS table_cat,\n";
//        String var3 = "         DECODE (t.data_type, 'CHAR', t.char_length,                   'VARCHAR', t.char_length,                   'VARCHAR2', t.char_length,                   'NVARCHAR2', t.char_length,                   'NCHAR', t.char_length,                   'NUMBER', 0,           t.data_length)";
//        String var5 = "       t.owner AS table_schem,\n       t.table_name AS table_name,\n";
//        String var6 = "       t.column_name AS column_name,\n       DECODE (t.data_type, 'CHAR', 1, 'VARCHAR2', 12, 'NUMBER', 3,\n               'LONG', -1, 'DATE', 93 , 'RAW', -3, 'LONG RAW', -4,  \n" + "               'BLOB', 2004, 'CLOB', 2005, 'BFILE', -13, 'FLOAT', 6, \n" + "               'TIMESTAMP(6)', 93, 'TIMESTAMP(6) WITH TIME ZONE', -101, \n" + "               'TIMESTAMP(6) WITH LOCAL TIME ZONE', -102, \n" + "               'INTERVAL YEAR(2) TO MONTH', -103, \n" + "               'INTERVAL DAY(2) TO SECOND(6)', -104, \n" + "               'BINARY_FLOAT', 100, 'BINARY_DOUBLE', 101, \n" + "               'XMLTYPE', 2009, \n" + "               1111)\n" + "              AS data_type,\n" + "       t.data_type AS type_name,\n" + "       DECODE (t.data_precision, " + "               null, DECODE(t.data_type, " + "                       'NUMBER', DECODE(t.data_scale, " + " null, 0 ,38), " + var3 + "                           )," + "         t.data_precision)\n" + "              AS column_size,\n" + "       0 AS buffer_length,\n" + "       DECODE (t.data_type, " + "               'NUMBER', DECODE(t.data_precision, " + "                                null, DECODE(t.data_scale, " + "                                             null, -127                                             , t.data_scale), " + "                                 t.data_scale), " + "               t.data_scale) AS decimal_digits,\n" + "       10 AS num_prec_radix,\n" + "       DECODE (t.nullable, 'N', 0, 1) AS nullable,\n";
//        String var7 = "       NULL AS remarks,\n";
//        String var8 = "       t.data_default AS column_def,\n       0 AS sql_data_type,\n       0 AS sql_datetime_sub,\n       t.data_length AS char_octet_length,\n       t.column_id AS ordinal_position,\n       DECODE (t.nullable, 'N', 'NO', 'YES') AS is_nullable,\n";
//        String var9 = "         null as SCOPE_CATALOG,\n       null as SCOPE_SCHEMA,\n       null as SCOPE_TABLE,\n       null as SOURCE_DATA_TYPE,\n       'NO' as IS_AUTOINCREMENT\n";
//        String var10 = "FROM all_tab_columns t";
//        String var11 = "WHERE t.owner LIKE ?  AND t.table_name LIKE ?  AND t.column_name LIKE ?";
//        String var12 = "ORDER BY table_schem, table_name, ordinal_position\n";
//
//        String var13 = var1 + var2;
//        var13 = var13 + var5;
//        var13 = var13 + var6;
//        var13 = var13 + var7;
//        var13 = var13 + var8 + var9 + var10;
//        var13 = var13 + "\n" + var11;
//        var13 = var13 + "\n" + var12;

        String sql = "SELECT  NULL AS table_cat,\n" +
                "       t.owner AS table_schem,\n" +
                "       t.table_name AS table_name,\n" +
                "       t.column_name AS column_name,\n" +
                "       DECODE (t.data_type, 'CHAR', 1, 'VARCHAR2', 12, 'NUMBER', 3,\n" +
                "               'LONG', -1, 'DATE', 93 , 'RAW', -3, 'LONG RAW', -4,  \n" +
                "               'BLOB', 2004, 'CLOB', 2005, 'BFILE', -13, 'FLOAT', 6, \n" +
                "               'TIMESTAMP(6)', 93, 'TIMESTAMP(6) WITH TIME ZONE', -101, \n" +
                "               'TIMESTAMP(6) WITH LOCAL TIME ZONE', -102, \n" +
                "               'INTERVAL YEAR(2) TO MONTH', -103, \n" +
                "               'INTERVAL DAY(2) TO SECOND(6)', -104, \n" +
                "               'BINARY_FLOAT', 100, 'BINARY_DOUBLE', 101, \n" +
                "               'XMLTYPE', 2009, \n" +
                "               1111)\n" +
                "              AS data_type,\n" +
                "       t.data_type AS type_name,\n" +
                "       DECODE (t.data_precision,                null, DECODE(t.data_type,                        'NUMBER', DECODE(t.data_scale,  null, 0 ,38),          DECODE (t.data_type, 'CHAR', t.char_length,                   'VARCHAR', t.char_length,                   'VARCHAR2', t.char_length,                   'NVARCHAR2', t.char_length,                   'NCHAR', t.char_length,                   'NUMBER', 0,           t.data_length)                           ),         t.data_precision)\n" +
                "              AS column_size,\n" +
                "       0 AS buffer_length,\n" +
                "       DECODE (t.data_type,                'NUMBER', DECODE(t.data_precision,                                 null, DECODE(t.data_scale,                                              null, -127                                             , t.data_scale),                                  t.data_scale),                t.data_scale) AS decimal_digits,\n" +
                "       10 AS num_prec_radix,\n" +
                "       DECODE (t.nullable, 'N', 0, 1) AS nullable,\n" +
                "       NULL AS remarks,\n" +
                "       t.data_default AS column_def,\n" +
                "       0 AS sql_data_type,\n" +
                "       0 AS sql_datetime_sub,\n" +
                "       t.data_length AS char_octet_length,\n" +
                "       t.column_id AS ordinal_position,\n" +
                "       DECODE (t.nullable, 'N', 'NO', 'YES') AS is_nullable,\n" +
                "         null as SCOPE_CATALOG,\n" +
                "       null as SCOPE_SCHEMA,\n" +
                "       null as SCOPE_TABLE,\n" +
                "       null as SOURCE_DATA_TYPE,\n" +
                "       'NO' as IS_AUTOINCREMENT\n" +
                "FROM all_tab_columns t\n" +
                "WHERE t.owner LIKE ?  AND t.table_name LIKE ?  AND t.column_name LIKE ?\n" +
                "ORDER BY table_schem, table_name, ordinal_position\n";

        PreparedStatement stmt = this.connection.prepareStatement(sql);
        stmt.setString(1, schemaPattern == null ? "%" : schemaPattern);
        stmt.setString(2, tableNamePattern == null ? "%" : tableNamePattern);
        stmt.setString(3, columnNamePattern == null ? "%" : columnNamePattern);

        ResultSet resultSet = stmt.executeQuery();
        stmt.close();
        return resultSet;
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        return null;
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return false;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        return null;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return null;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        return null;
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return 0;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return 0;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        return null;
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        String var4 = "SELECT\n  NULL AS function_cat,\n  owner AS function_schem,\n  object_name AS function_name,\n  'Standalone function' AS remarks,\n  0 AS function_type,\n  NULL AS specific_name\nFROM all_objects\nWHERE object_type = 'FUNCTION'\n  AND owner LIKE ? \n  AND object_name LIKE ? \n";
        String var5 = "SELECT\n  -- Packaged functions\n  package_name AS function_cat,\n  owner AS function_schem,\n  object_name AS function_name,\n  'Packaged function' AS remarks,\n  decode (data_type, 'TABLE', 2, 'PL/SQL TABLE', 2, 1) AS function_type,\n  NULL AS specific_name\nFROM all_arguments\nWHERE argument_name IS NULL\n  AND in_out = 'OUT'\n  AND data_level = 0\n";
        String var6 = "  AND package_name LIKE ? \n  AND owner LIKE ? \n  AND object_name LIKE ? \n";
        String var7 = "  AND package_name IS NOT NULL\n  AND owner LIKE ? \n  AND object_name LIKE ? \n";
        String var8 = "ORDER BY function_schem, function_name\n";
        PreparedStatement var9 = null;
        String var10 = null;
        String var11 = schemaPattern;
        if (schemaPattern == null) {
            var11 = "%";
        } else if (schemaPattern.equals("")) {
            var11 = this.getUserName().toUpperCase();
        } else {
            var11 = schemaPattern.toUpperCase();
        }
        String var12 = functionNamePattern;
        if (functionNamePattern == null) {
            var12 = "%";
        } else if (functionNamePattern.equals("")) {
            throw new SQLException();
        } else {
            var12 = functionNamePattern.toUpperCase();
        }
        if (catalog == null) {
            var10 = var4 + "UNION ALL " + var5 + var7 + var8;
            var9 = this.connection.prepareStatement(var10);
            var9.setString(1, var11);
            var9.setString(2, var12);
            var9.setString(3, var11);
            var9.setString(4, var12);
        } else if (catalog.equals("")) {
            var9 = this.connection.prepareStatement(var4);
            var9.setString(1, var11);
            var9.setString(2, var12);
        } else {
            var10 = var5 + var6 + var8;
            var9 = this.connection.prepareStatement(var10);
            var9.setString(1, var11);
            var9.setString(2, var11);
            var9.setString(3, var12);
        }

        ResultSet var13 = var9.executeQuery();
        return var13;
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
