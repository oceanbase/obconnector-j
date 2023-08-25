/**
 *  OceanBase Client for Java
 *
 *  Copyright (c) 2012-2014 Monty Program Ab.
 *  Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *  Copyright (c) 2021 OceanBase.
 *
 *  This library is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU Lesser General Public License as published by the Free
 *  Software Foundation; either version 2.1 of the License, or (at your option)
 *  any later version.
 *
 *  This library is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License along
 *  with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 *  This particular MariaDB Client for Java file is work
 *  derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 *  the following copyright and notice provisions:
 *
 *  Copyright (c) 2009-2011, Marcus Eriksson
 *
 *  Redistribution and use in source and binary forms, with or without modification,
 *  are permitted provided that the following conditions are met:
 *  Redistributions of source code must retain the above copyright notice, this list
 *  of conditions and the following disclaimer.
 *
 *  Redistributions in binary form must reproduce the above copyright notice, this
 *  list of conditions and the following disclaimer in the documentation and/or
 *  other materials provided with the distribution.
 *
 *  Neither the name of the driver nor the names of its contributors may not be
 *  used to endorse or promote products derived from this software without specific
 *  prior written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 *  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 *  IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 *  INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 *  NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 *  PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 *  WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 *  OF SUCH DAMAGE.
 */
package com.oceanbase.jdbc;

import static org.junit.Assert.*;

import java.io.*;
import java.sql.*;
import java.util.HashMap;
import java.util.Locale;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class DatabaseMetadataOracleTest extends BaseOracleTest {

    private static DatabaseMetaData dbmd;

    static {
        try {
            dbmd = sharedConnection.getMetaData();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Before
    public void checkSupported() throws SQLException {
        requireMinimumVersion(5, 1);
    }

    @Test
    public void testGetCatalogs() throws SQLException {
        ResultSet rs = dbmd.getCatalogs();
        boolean hasNext = true;
        if (!rs.next()) {
            hasNext = false;
        }
        //there return null in oracle
        assertTrue(!hasNext);
    }

    @Test
    public void testGetColumns() throws SQLException {
        createTable("GETCOLUMNS_TEST", "a INT NOT NULL primary key, b VARCHAR(32), c INT , "
                                       + "d VARCHAR(5)", null);

        ResultSet rs = dbmd.getColumns(null, null, "GETCOLUMNS_TEST", null);
        {
            assertTrue(rs.next());
            assertEquals(null, rs.getString(1)); // TABLE_CAT
            assertEquals("UNITTESTS", rs.getString(2)); // OWNER
            assertEquals("GETCOLUMNS_TEST", rs.getString(3)); // TABLE_NAME
            assertEquals("A", rs.getString(4)); // COLUMN_NAME
            assertEquals(Types.DECIMAL, rs.getInt(5)); // DATA_TYPE
            assertEquals("NUMBER", rs.getString(6)); // "TYPE_NAME
            assertEquals(38, rs.getInt(7)); // "COLUMN_SIZE
            assertEquals(0, rs.getInt(8));//BUFFER_LENGTH
            assertEquals(0, rs.getInt(9)); // DECIMAL_DIGITS
            assertEquals(10, rs.getInt(10)); // NUM_PREC_RADIX
            assertEquals(0, rs.getInt(11)); // NULLABLE
            assertEquals(null, rs.getString(12)); // REMARKS
            assertEquals(null, rs.getString(13)); // DATA_DEFAULT
            assertEquals(0, rs.getInt(14));//SQL_DATA_TYPE
            assertEquals(0, rs.getInt(15));//SQL_DATETIME_SUB
            assertEquals(22, rs.getInt(16)); // DATA_LENGTH
            assertEquals(1, rs.getInt(17)); // COLUMN_ID
            assertEquals("NO", rs.getString(18)); // IS_NULLABLE
            assertEquals(null, rs.getString(19)); // SCOPE_CATALOG
            assertEquals(null, rs.getString(20)); // SCOPE_SCHEMA
            assertEquals(null, rs.getString(21)); // SCOPE_TABLE
            assertEquals(0, rs.getShort(22)); // SOURCE_DATA_TYPE
            assertEquals("NO", rs.getString(23)); // IS_AUTOINCREMENT

            assertTrue(rs.next());
            assertEquals(null, rs.getString(1)); // TABLE_CAT
            assertEquals("UNITTESTS", rs.getString(2)); // OWNER
            assertEquals("GETCOLUMNS_TEST", rs.getString(3)); // TABLE_NAME
            assertEquals("B", rs.getString(4)); // COLUMN_NAME
            assertEquals(Types.VARCHAR, rs.getInt(5)); // DATA_TYPE
            assertEquals("VARCHAR2", rs.getString(6)); // "TYPE_NAME
            assertEquals(32, rs.getInt(7)); // "COLUMN_SIZE
            assertEquals(0, rs.getInt(8));//BUFFER_LENGTH
            assertEquals(0, rs.getInt(9)); // DECIMAL_DIGITS
            assertEquals(10, rs.getInt(10)); // NUM_PREC_RADIX
            assertEquals(1, rs.getInt(11)); // NULLABLE
            assertEquals(null, rs.getString(12)); // REMARKS
            assertTrue("null".equalsIgnoreCase(rs.getString(13)) || rs.getString(13) == null); // DATA_DEFAULT
            assertEquals(0, rs.getInt(14));//SQL_DATA_TYPE
            assertEquals(0, rs.getInt(15));//SQL_DATETIME_SUB
            assertEquals(32, rs.getInt(16)); // DATA_LENGTH
            assertEquals(2, rs.getInt(17)); // COLUMN_ID
            assertEquals("YES", rs.getString(18)); // IS_NULLABLE
            assertEquals(null, rs.getString(19)); // SCOPE_CATALOG
            assertEquals(null, rs.getString(20)); // SCOPE_SCHEMA
            assertEquals(null, rs.getString(21)); // SCOPE_TABLE
            assertEquals(0, rs.getShort(22)); // SOURCE_DATA_TYPE
            assertEquals("NO", rs.getString(23)); // IS_AUTOINCREMENT

            assertTrue(rs.next());
            assertEquals(null, rs.getString(1)); // TABLE_CAT
            assertEquals("UNITTESTS", rs.getString(2)); // OWNER
            assertEquals("GETCOLUMNS_TEST", rs.getString(3)); // TABLE_NAME
            assertEquals("C", rs.getString(4)); // COLUMN_NAME
            assertEquals(Types.DECIMAL, rs.getInt(5)); // DATA_TYPE
            assertEquals("NUMBER", rs.getString(6)); // "TYPE_NAME
            assertEquals(38, rs.getInt(7)); // "COLUMN_SIZE
            assertEquals(0, rs.getInt(9)); // DECIMAL_DIGITS
            assertEquals(10, rs.getInt(10)); // NUM_PREC_RADIX
            assertEquals(1, rs.getInt(11)); // NULLABLE
            assertEquals(null, rs.getString(12)); // REMARKS
            assertTrue("CHAR_LENGTH(`b`)".equalsIgnoreCase(rs.getString(13))
                       || rs.getString(13) == null); // COLUMN_DEF
            //            assertEquals(0, rs.getInt(16)); // CHAR_OCTET_LENGTH
            assertEquals(3, rs.getInt(17)); // ORDINAL_POSITION
            assertEquals("YES", rs.getString(18)); // IS_NULLABLE
            assertEquals(null, rs.getString(19)); // SCOPE_CATALOG
            assertEquals(null, rs.getString(20)); // SCOPE_SCHEMA
            assertEquals(null, rs.getString(21)); // SCOPE_TABLE
            assertEquals(0, rs.getShort(22)); // SOURCE_DATA_TYPE
            assertEquals("NO", rs.getString(23)); // IS_AUTOINCREMENT

            assertTrue(rs.next());
            assertEquals(null, rs.getString(1)); // TABLE_CAT
            assertEquals("UNITTESTS", rs.getString(2)); // OWNER
            assertEquals("GETCOLUMNS_TEST", rs.getString(3)); // TABLE_NAME
            assertEquals("D", rs.getString(4)); // COLUMN_NAME
            assertEquals(Types.VARCHAR, rs.getInt(5)); // DATA_TYPE
            assertEquals("VARCHAR2", rs.getString(6)); // "TYPE_NAME
            assertEquals(5, rs.getInt(7)); // "COLUMN_SIZE
            assertEquals(0, rs.getInt(9)); // DECIMAL_DIGITS
            assertEquals(10, rs.getInt(10)); // NUM_PREC_RADIX
            assertEquals(1, rs.getInt(11)); // NULLABLE
            assertEquals(null, rs.getString(12)); // REMARKS
            assertTrue("CHAR_LENGTH(`b`)".equalsIgnoreCase(rs.getString(13))
                       || rs.getString(13) == null); // COLUMN_DEF
            assertEquals(5, rs.getInt(16)); // CHAR_OCTET_LENGTH
            assertEquals(4, rs.getInt(17)); // ORDINAL_POSITION
            assertEquals("YES", rs.getString(18)); // IS_NULLABLE
            assertEquals(null, rs.getString(19)); // SCOPE_CATALOG
            assertEquals(null, rs.getString(20)); // SCOPE_SCHEMA
            assertEquals(null, rs.getString(21)); // SCOPE_TABLE
            assertEquals(0, rs.getShort(22)); // SOURCE_DATA_TYPE
            assertEquals("NO", rs.getString(23)); // IS_AUTOINCREMENT

            assertTrue(!rs.next());
        }
    }

    @Test
    public void getColumnsBasic() throws SQLException {
        testResultSetColumns(
            sharedConnection.getMetaData().getColumns(null, null, null, null),
            "TABLE_CAT String,TABLE_SCHEM String,TABLE_NAME String,COLUMN_NAME String,"
                    + "DATA_TYPE decimal,TYPE_NAME String,COLUMN_SIZE decimal,BUFFER_LENGTH decimal,"
                    + "DECIMAL_DIGITS decimal,NUM_PREC_RADIX decimal,NULLABLE decimal,"
                    + "REMARKS String,COLUMN_DEF String,SQL_DATA_TYPE decimal,"
                    + "SQL_DATETIME_SUB decimal, CHAR_OCTET_LENGTH decimal,"
                    + "ORDINAL_POSITION decimal,IS_NULLABLE String,"
                    + "SCOPE_CATALOG String,SCOPE_SCHEMA String,"
                    + "SCOPE_TABLE String,SOURCE_DATA_TYPE null");
    }

    @Test
    public void getDatabaseProductNameTest() throws SQLException {
        DatabaseMetaData dmd = sharedConnection.getMetaData();
        String databaseProductName = dmd.getDatabaseProductName();
        assertEquals(databaseProductName, "Oracle");
    }

    @Ignore
    public void getFunctionsTest() throws SQLException {
        ResultSet rs = sharedConnection.getMetaData().getFunctions(null, null, null);
        assertTrue(rs.next());
        assertEquals(rs.getString("FUNCTION_CAT"), null);
        assertEquals(rs.getString("FUNCTION_SCHEM"), "ADMIN");
        assertEquals(rs.getString("FUNCTION_NAME"), "CALC_FUNC");
        assertEquals(rs.getString("REMARKS"), "Standalone function");
        assertEquals(rs.getInt("FUNCTION_TYPE"), 0);
        assertEquals(rs.getString("SPECIFIC_NAME"), null);
    }

    @Ignore
    public void getFunctionsBasic() throws SQLException {
        testResultSetColumns(
            sharedConnection.getMetaData().getFunctions(null, null, null),
            "FUNCTION_CAT String, FUNCTION_SCHEM String,FUNCTION_NAME String,REMARKS String,FUNCTION_TYPE decimal,SPECIFIC_NAME String");
    }

    @Ignore
    public void getProceduresTest() throws SQLException {
        try {
            ResultSet rs = sharedConnection.getMetaData().getProcedures(null, null, null);
            Connection oraConnection = getOracleConnection();
            ResultSet oraRs = oraConnection.getMetaData().getProcedures(null, null, null);
            assertTrue(rs.next() && oraRs.next());
            System.out.println(rs.getString("PROCEDURE_CAT") + " "
                               + oraRs.getString("PROCEDURE_CAT"));
            System.out.println(rs.getString("PROCEDURE_SCHEM") + " "
                               + oraRs.getString("PROCEDURE_SCHEM"));
            System.out.println(rs.getString("PROCEDURE_NAME") + " "
                               + oraRs.getString("PROCEDURE_NAME"));
            System.out.println(rs.getString("REMARKS") + " " + oraRs.getString("REMARKS"));
            System.out.println(rs.getInt("PROCEDURE_TYPE") + " " + oraRs.getInt("PROCEDURE_TYPE"));
            System.out.println(rs.getString("SPECIFIC_NAME") + " "
                               + oraRs.getString("SPECIFIC_NAME"));
            int i = rs.getMetaData().getColumnCount();
            for (int i1 = 1; i1 < i; i1++) {
                //                System.out.print(rs.getMetaData().getColumnName(i1) + ":");
                //                System.out.print(rs.getMetaData().getColumnTypeName(i1) + ":");
                //                System.out.println(rs.getMetaData().getColumnType(i1));
                //                System.out.println("++++++++++++++++");
                //                System.out.print(oraRs.getMetaData().getColumnName(i1) + ":");
                //                System.out.print(oraRs.getMetaData().getColumnTypeName(i1) + ":");
                //                System.out.println(oraRs.getMetaData().getColumnType(i1));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Ignore
    public void getProceduresBasic() throws SQLException {
        ResultSet rs = sharedConnection.getMetaData().getProcedures(null, null, null);
        testResultSetColumns(
            rs,
            "PROCEDURE_CAT String,PROCEDURE_SCHEM String,PROCEDURE_NAME String,NULL String,NULL String,NULL String,REMARKS String,PROCEDURE_TYPE decimal,SPECIFIC_NAME String");
    }

    @Ignore
    public void testGetSchemas() throws SQLException {
        try {
            Connection oracleConnection = getOracleConnection();
            DatabaseMetaData oracleDbmd = sharedConnection.getMetaData();
            ResultSet oraRs = oracleDbmd.getSchemas();
            ResultSetMetaData oraMetaData = oraRs.getMetaData();

            DatabaseMetaData obDbmd = sharedConnection.getMetaData();
            ResultSet rs = obDbmd.getSchemas();
            ResultSetMetaData obMetaData = rs.getMetaData();

            assertEquals(obMetaData.getColumnName(1), oraMetaData.getColumnName(1));
            assertEquals(obMetaData.getColumnName(2), oraMetaData.getColumnName(2));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getSchemasBasic() throws SQLException {
        DatabaseMetaData dbmd = sharedConnection.getMetaData();
        ResultSet rs = dbmd.getSchemas();
        testResultSetColumns(rs, "TABLE_SCHEM String , TABLE_CATALOG String");
    }

    //
    @Test
    public void testGetTables() throws Exception {
        ResultSet rs = sharedConnection.getMetaData().getTables(null, null, null, null);
        int count = rs.getMetaData().getColumnCount();
        while (rs.next()) {
            System.out.println("rs.getString(\"TABLE_CAT\") = " + rs.getString("TABLE_CAT"));
            System.out.println("rs.getString(\"TABLE_SCHEM\") = " + rs.getString("TABLE_SCHEM"));
            System.out.println("rs.getString(\"TABLE_NAME\") = " + rs.getString("TABLE_NAME"));
            System.out.println("rs.getString(\"TABLE_TYPE\") = " + rs.getString("TABLE_TYPE"));
            System.out.println("rs.getString(\"REMARKS\") = " + rs.getString("REMARKS"));
            System.out.println("+++++++++++++++++++++++");
        }
    }

    @Test
    public void getDriverNameTest() throws SQLException {
        String driverName = sharedConnection.getMetaData().getDriverName();
        assertEquals(driverName, "OceanBase Connector/J");
    }

    @Test
    public void getUserNameTest() throws SQLException {
        String userName = sharedConnection.getMetaData().getUserName();
        assertEquals(userName, "TESTER");

    }

    private void testResultSetColumns(ResultSet rs, String spec) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        String[] tokens = spec.split(",");
        int count = rsmd.getColumnCount();
        System.out.println("count = " + count);
        for (int i = 1; i <= count; i++) {
            String lable = rsmd.getColumnLabel(i);
            System.out.println("lable = " + lable);
            int type = rsmd.getColumnType(i);
            System.out.println("type = " + type);
        }
    }

    @Test
    public void testGetTablesWithParameter() throws SQLException {
        createTable("test_getTables", "a INT NOT NULL primary key, b VARCHAR(32), c INT , "
                                      + "d VARCHAR(5)", null);
        ResultSet rs = sharedConnection.getMetaData().getTables(sharedConnection.getCatalog(),
            sharedConnection.getSchema(), "TEST_GETTABLES", null);
        rs.next();

        assertEquals("UNITTESTS", rs.getString("TABLE_SCHEM"));
        assertEquals("TEST_GETTABLES", rs.getString("TABLE_NAME"));
        assertEquals("TABLE", rs.getString("TABLE_TYPE"));
    }

    @Test
    public void testGetBestRowIdentifier() throws SQLException {

        ResultSet rs = rs = sharedConnection.getMetaData().getBestRowIdentifier(null,
            sharedConnection.getSchema(), "TEST_GETTABLES", 1, false);
        assertEquals(true, rs.next());
    }

    @Test
    public void testGetTableTypes() throws SQLException {
        ResultSet rs = sharedConnection.getMetaData().getTableTypes();
        Assert.assertTrue(rs.next());

        assertEquals("TABLE", rs.getString(1));
        Assert.assertTrue(rs.next());

        assertEquals("VIEW", rs.getString(1));

        Assert.assertTrue(rs.next());
        assertEquals("SYNONYM", rs.getString(1));
    }

    @Test
    public void testReturnBooleanMethodsWithOutParameter() throws SQLException, IOException {

        DatabaseMetaData metaData = sharedConnection.getMetaData();
        HashMap<String, Boolean> map = new HashMap<>();
        HashMap<String, String> mapBy1X = new HashMap<>();

        //result of OceanBase-client1.X
        String s = "supportsMinimumSQLGrammar:true\n" +
                "supportsPositionedDelete:false\n" +
                "autoCommitFailureClosesAllResultSets:false\n" +
                "supportsSubqueriesInComparisons:true\n" +
                "supportsANSI92IntermediateSQL:false\n" +
                "nullPlusNonNullIsNull:true\n" +
                "supportsCatalogsInDataManipulation:true\n" +
                "supportsMultipleTransactions:true\n" +
                "supportsCatalogsInTableDefinitions:false\n" +
                "supportsOpenCursorsAcrossRollback:false\n" +
                "supportsStatementPooling:false\n" +
                "supportsDataDefinitionAndDataManipulationTransactions:false\n" +
                "supportsTableCorrelationNames:true\n" +
                "usesLocalFiles:false\n" +
                "supportsFullOuterJoins:false\n" +
                "supportsExpressionsInOrderBy:true\n" +
                "allProceduresAreCallable:false\n" +
                "storesMixedCaseQuotedIdentifiers:false\n" +
                "supportsSchemasInProcedureCalls:false\n" +
                "nullsAreSortedAtStart:false\n" +
                "supportsLikeEscapeClause:true\n" +
                "supportsDataManipulationTransactionsOnly:false\n" +
                "supportsSchemasInDataManipulation:false\n" +
                "supportsPositionedUpdate:false\n" +
                "supportsGetGeneratedKeys:true\n" +
                "supportsGroupBy:true\n" +
                "supportsOuterJoins:true\n" +
                "supportsBatchUpdates:true\n" +
                "supportsLimitedOuterJoins:true\n" +
                "allTablesAreSelectable:false\n" +
                "supportsSchemasInIndexDefinitions:false\n" +
                "supportsConvert1:false\n" +
                "supportsDifferentTableCorrelationNames:true\n" +
                "supportsMultipleOpenResults:true\n" +
                "supportsTransactions:true\n" +
                "supportsUnion:true\n" +
                "supportsOpenCursorsAcrossCommit:false\n" +
                "storesLowerCaseQuotedIdentifiers:true\n" +
                "supportsANSI92EntryLevelSQL:true\n" +
                "supportsStoredProcedures:true\n" +
                "supportsCatalogsInPrivilegeDefinitions:true\n" +
                "supportsAlterTableWithDropColumn:true\n" +
                "nullsAreSortedHigh:false\n" +
                "supportsCatalogsInProcedureCalls:true\n" +
                "supportsNonNullableColumns:true\n" +
                "supportsSelectForUpdate:true\n" +
                "supportsSubqueriesInExists:true\n" +
                "supportsOpenStatementsAcrossRollback:false\n" +
                "supportsOpenStatementsAcrossCommit:false\n" +
                "supportsColumnAliasing:true\n" +
                "supportsSavepoints:true\n" +
                "dataDefinitionCausesTransactionCommit:true\n" +
                "supportsMixedCaseIdentifiers:true\n" +
                "nullsAreSortedAtEnd:false\n" +
                "supportsAlterTableWithAddColumn:true\n" +
                "isCatalogAtStart:true\n" +
                "supportsUnionAll:true\n" +
                "dataDefinitionIgnoredInTransactions:false\n" +
                "supportsCoreSQLGrammar:true\n" +
                "isReadOnly:false\n" +
                "supportsGroupByUnrelated:true\n" +
                "nullsAreSortedLow:true\n" +
                "supportsCorrelatedSubqueries:true\n" +
                "locatorsUpdateCopy:true\n" +
                "supportsANSI92FullSQL:false\n" +
                "storesUpperCaseIdentifiers:true\n" +
                "supportsMultipleResultSets:true\n" +
                "supportsSchemasInPrivilegeDefinitions:false\n" +
                "supportsMixedCaseQuotedIdentifiers:false\n" +
                "supportsGroupByBeyondSelect:true\n" +
                "supportsCatalogsInIndexDefinitions:true\n" +
                "supportsOrderByUnrelated:false\n" +
                "supportsNamedParameters:false\n" +
                "doesMaxRowSizeIncludeBlobs:true\n" +
                "supportsSubqueriesInQuantifieds:true\n" +
                "supportsStoredFunctionsUsingCallSyntax:true\n" +
                "usesLocalFilePerTable:false\n" +
                "supportsIntegrityEnhancementFacility:false\n" +
                "storesLowerCaseIdentifiers:true\n" +
                "supportsSubqueriesInIns:true\n" +
                "supportsExtendedSQLGrammar:false\n" +
                "storesUpperCaseQuotedIdentifiers:true\n" +
                "supportsSchemasInTableDefinitions:true\n" +
                "storesMixedCaseIdentifiers:false\n" +
                "supportsConvert:false\n" +
                "generatedKeyAlwaysReturned:false";

        String[] split = s.split("\n");
        for (String s1 : split) {
            String[] s2 = s1.split(":");
            mapBy1X.put(s2[0],s2[1]);
        }

        boolean allProceduresAreCallable = metaData.allProceduresAreCallable();
        map.put("allProceduresAreCallable", allProceduresAreCallable);

        boolean allTablesAreSelectable = metaData.allTablesAreSelectable();
        map.put("allTablesAreSelectable", allTablesAreSelectable);

        boolean isReadOnly = metaData.isReadOnly();
        map.put("isReadOnly", isReadOnly);

        boolean nullsAreSortedHigh = metaData.nullsAreSortedHigh();
        map.put("nullsAreSortedHigh", nullsAreSortedHigh);

        boolean nullsAreSortedLow = metaData.nullsAreSortedLow();
        map.put("nullsAreSortedLow", nullsAreSortedLow);

        boolean nullsAreSortedAtStart = metaData.nullsAreSortedAtStart();
        map.put("nullsAreSortedAtStart", nullsAreSortedAtStart);

        boolean nullsAreSortedAtEnd = metaData.nullsAreSortedAtEnd();
        map.put("nullsAreSortedAtEnd", nullsAreSortedAtEnd);

        boolean usesLocalFiles = metaData.usesLocalFiles();
        map.put("usesLocalFiles", usesLocalFiles);

        boolean usesLocalFilePerTable = metaData.usesLocalFilePerTable();
        map.put("usesLocalFilePerTable", usesLocalFilePerTable);

        boolean supportsMixedCaseIdentifiers = metaData.supportsMixedCaseIdentifiers();
        map.put("supportsMixedCaseIdentifiers", supportsMixedCaseIdentifiers);

        boolean storesUpperCaseIdentifiers = metaData.storesUpperCaseIdentifiers();
        map.put("storesUpperCaseIdentifiers", storesUpperCaseIdentifiers);

        boolean storesLowerCaseIdentifiers = metaData.storesLowerCaseIdentifiers();
        map.put("storesLowerCaseIdentifiers", storesLowerCaseIdentifiers);

        boolean storesMixedCaseIdentifiers = metaData.storesMixedCaseIdentifiers();
        map.put("storesMixedCaseIdentifiers", storesMixedCaseIdentifiers);

        boolean supportsMixedCaseQuotedIdentifiers = metaData.supportsMixedCaseQuotedIdentifiers();
        map.put("supportsMixedCaseQuotedIdentifiers", supportsMixedCaseQuotedIdentifiers);

        boolean storesUpperCaseQuotedIdentifiers = metaData.storesUpperCaseQuotedIdentifiers();
        map.put("storesUpperCaseQuotedIdentifiers", storesUpperCaseQuotedIdentifiers);

        boolean storesLowerCaseQuotedIdentifiers = metaData.storesLowerCaseQuotedIdentifiers();
        map.put("storesLowerCaseQuotedIdentifiers", storesLowerCaseQuotedIdentifiers);

        boolean storesMixedCaseQuotedIdentifiers = metaData.storesMixedCaseQuotedIdentifiers();
        map.put("storesMixedCaseQuotedIdentifiers", storesMixedCaseQuotedIdentifiers);

        boolean supportsAlterTableWithAddColumn = metaData.supportsAlterTableWithAddColumn();
        map.put("supportsAlterTableWithAddColumn", supportsAlterTableWithAddColumn);

        boolean supportsAlterTableWithDropColumn = metaData.supportsAlterTableWithDropColumn();
        map.put("supportsAlterTableWithDropColumn", supportsAlterTableWithDropColumn);

        boolean supportsColumnAliasing = metaData.supportsColumnAliasing();
        map.put("supportsColumnAliasing", supportsColumnAliasing);

        boolean nullPlusNonNullIsNull = metaData.nullPlusNonNullIsNull();
        map.put("nullPlusNonNullIsNull", nullPlusNonNullIsNull);

        boolean supportsConvert = metaData.supportsConvert();
        map.put("supportsConvert", supportsConvert);

        boolean supportsConvert1 = metaData.supportsConvert(0, 0);
        map.put("supportsConvert1", supportsConvert1);

        boolean supportsTableCorrelationNames = metaData.supportsTableCorrelationNames();
        map.put("supportsTableCorrelationNames", supportsTableCorrelationNames);

        boolean supportsDifferentTableCorrelationNames = metaData.supportsDifferentTableCorrelationNames();
        map.put("supportsDifferentTableCorrelationNames", supportsDifferentTableCorrelationNames);

        boolean supportsExpressionsInOrderBy = metaData.supportsExpressionsInOrderBy();
        map.put("supportsExpressionsInOrderBy", supportsExpressionsInOrderBy);

        boolean supportsOrderByUnrelated = metaData.supportsOrderByUnrelated();
        map.put("supportsOrderByUnrelated", supportsOrderByUnrelated);

        boolean supportsGroupBy = metaData.supportsGroupBy();
        map.put("supportsGroupBy", supportsGroupBy);

        boolean supportsGroupByUnrelated = metaData.supportsGroupByUnrelated();
        map.put("supportsGroupByUnrelated", supportsGroupByUnrelated);

        boolean supportsGroupByBeyondSelect = metaData.supportsGroupByBeyondSelect();
        map.put("supportsGroupByBeyondSelect", supportsGroupByBeyondSelect);

        boolean supportsLikeEscapeClause = metaData.supportsLikeEscapeClause();
        map.put("supportsLikeEscapeClause", supportsLikeEscapeClause);

        boolean supportsMultipleResultSets = metaData.supportsMultipleResultSets();
        map.put("supportsMultipleResultSets", supportsMultipleResultSets);

        boolean supportsMultipleTransactions = metaData.supportsMultipleTransactions();
        map.put("supportsMultipleTransactions", supportsMultipleTransactions);

        boolean supportsNonNullableColumns = metaData.supportsNonNullableColumns();
        map.put("supportsNonNullableColumns", supportsNonNullableColumns);

        boolean supportsMinimumSQLGrammar = metaData.supportsMinimumSQLGrammar();
        map.put("supportsMinimumSQLGrammar", supportsMinimumSQLGrammar);

        boolean supportsCoreSQLGrammar = metaData.supportsCoreSQLGrammar();
        map.put("supportsCoreSQLGrammar", supportsCoreSQLGrammar);

        boolean supportsExtendedSQLGrammar = metaData.supportsExtendedSQLGrammar();
        map.put("supportsExtendedSQLGrammar", supportsExtendedSQLGrammar);

        boolean supportsANSI92EntryLevelSQL = metaData.supportsANSI92EntryLevelSQL();
        map.put("supportsANSI92EntryLevelSQL", supportsANSI92EntryLevelSQL);

        boolean supportsANSI92IntermediateSQL = metaData.supportsANSI92IntermediateSQL();
        map.put("supportsANSI92IntermediateSQL", supportsANSI92IntermediateSQL);

        boolean supportsANSI92FullSQL = metaData.supportsANSI92FullSQL();
        map.put("supportsANSI92FullSQL", supportsANSI92FullSQL);

        boolean supportsIntegrityEnhancementFacility = metaData.supportsIntegrityEnhancementFacility();
        map.put("supportsIntegrityEnhancementFacility", supportsIntegrityEnhancementFacility);

        boolean supportsOuterJoins = metaData.supportsOuterJoins();
        map.put("supportsOuterJoins", supportsOuterJoins);

        boolean supportsFullOuterJoins = metaData.supportsFullOuterJoins();
        map.put("supportsFullOuterJoins", supportsFullOuterJoins);

        boolean supportsLimitedOuterJoins = metaData.supportsLimitedOuterJoins();
        map.put("supportsLimitedOuterJoins", supportsLimitedOuterJoins);

        boolean isCatalogAtStart = metaData.isCatalogAtStart();
        map.put("isCatalogAtStart", isCatalogAtStart);

        boolean supportsSchemasInDataManipulation = metaData.supportsSchemasInDataManipulation();
        map.put("supportsSchemasInDataManipulation", supportsSchemasInDataManipulation);

        boolean supportsSchemasInProcedureCalls = metaData.supportsSchemasInProcedureCalls();
        map.put("supportsSchemasInProcedureCalls", supportsSchemasInProcedureCalls);

        boolean supportsSchemasInTableDefinitions = metaData.supportsSchemasInTableDefinitions();
        map.put("supportsSchemasInTableDefinitions", supportsSchemasInTableDefinitions);

        boolean supportsSchemasInIndexDefinitions = metaData.supportsSchemasInIndexDefinitions();
        map.put("supportsSchemasInIndexDefinitions", supportsSchemasInIndexDefinitions);

        boolean supportsSchemasInPrivilegeDefinitions = metaData.supportsSchemasInPrivilegeDefinitions();
        map.put("supportsSchemasInPrivilegeDefinitions", supportsSchemasInPrivilegeDefinitions);

        boolean supportsCatalogsInDataManipulation = metaData.supportsCatalogsInDataManipulation();
        map.put("supportsCatalogsInDataManipulation", supportsCatalogsInDataManipulation);

        boolean supportsCatalogsInProcedureCalls = metaData.supportsCatalogsInProcedureCalls();
        map.put("supportsCatalogsInProcedureCalls", supportsCatalogsInProcedureCalls);

        boolean supportsCatalogsInTableDefinitions = metaData.supportsCatalogsInTableDefinitions();
        map.put("supportsCatalogsInTableDefinitions", supportsCatalogsInTableDefinitions);

        boolean supportsCatalogsInIndexDefinitions = metaData.supportsCatalogsInIndexDefinitions();
        map.put("supportsCatalogsInIndexDefinitions", supportsCatalogsInIndexDefinitions);

        boolean supportsCatalogsInPrivilegeDefinitions = metaData.supportsCatalogsInPrivilegeDefinitions();
        map.put("supportsCatalogsInPrivilegeDefinitions", supportsCatalogsInPrivilegeDefinitions);

        boolean supportsPositionedDelete = metaData.supportsPositionedDelete();
        map.put("supportsPositionedDelete", supportsPositionedDelete);

        boolean supportsPositionedUpdate = metaData.supportsPositionedUpdate();
        map.put("supportsPositionedUpdate", supportsPositionedUpdate);

        boolean supportsSelectForUpdate = metaData.supportsSelectForUpdate();
        map.put("supportsSelectForUpdate", supportsSelectForUpdate);

        boolean supportsStoredProcedures = metaData.supportsStoredProcedures();
        map.put("supportsStoredProcedures", supportsStoredProcedures);

        boolean supportsSubqueriesInComparisons = metaData.supportsSubqueriesInComparisons();
        map.put("supportsSubqueriesInComparisons", supportsSubqueriesInComparisons);

        boolean supportsSubqueriesInExists = metaData.supportsSubqueriesInExists();
        map.put("supportsSubqueriesInExists", supportsSubqueriesInExists);

        boolean supportsSubqueriesInIns = metaData.supportsSubqueriesInIns();
        map.put("supportsSubqueriesInIns", supportsSubqueriesInIns);

        boolean supportsSubqueriesInQuantifieds = metaData.supportsSubqueriesInQuantifieds();
        map.put("supportsSubqueriesInQuantifieds", supportsSubqueriesInQuantifieds);

        boolean supportsCorrelatedSubqueries = metaData.supportsCorrelatedSubqueries();
        map.put("supportsCorrelatedSubqueries", supportsCorrelatedSubqueries);

        boolean supportsUnion = metaData.supportsUnion();
        map.put("supportsUnion", supportsUnion);

        boolean supportsUnionAll = metaData.supportsUnionAll();
        map.put("supportsUnionAll", supportsUnionAll);

        boolean supportsOpenCursorsAcrossCommit = metaData.supportsOpenCursorsAcrossCommit();
        map.put("supportsOpenCursorsAcrossCommit", supportsOpenCursorsAcrossCommit);

        boolean supportsOpenCursorsAcrossRollback = metaData.supportsOpenCursorsAcrossRollback();
        map.put("supportsOpenCursorsAcrossRollback", supportsOpenCursorsAcrossRollback);

        boolean supportsOpenStatementsAcrossCommit = metaData.supportsOpenStatementsAcrossCommit();
        map.put("supportsOpenStatementsAcrossCommit", supportsOpenStatementsAcrossCommit);

        boolean supportsOpenStatementsAcrossRollback = metaData.supportsOpenStatementsAcrossRollback();
        map.put("supportsOpenStatementsAcrossRollback", supportsOpenStatementsAcrossRollback);

        boolean doesMaxRowSizeIncludeBlobs = metaData.doesMaxRowSizeIncludeBlobs();
        map.put("doesMaxRowSizeIncludeBlobs", doesMaxRowSizeIncludeBlobs);

        boolean supportsTransactions = metaData.supportsTransactions();
        map.put("supportsTransactions", supportsTransactions);

        boolean supportsDataDefinitionAndDataManipulationTransactions = metaData.supportsDataDefinitionAndDataManipulationTransactions();
        map.put("supportsDataDefinitionAndDataManipulationTransactions", supportsDataDefinitionAndDataManipulationTransactions);

        boolean supportsDataManipulationTransactionsOnly = metaData.supportsDataManipulationTransactionsOnly();
        map.put("supportsDataManipulationTransactionsOnly", supportsDataManipulationTransactionsOnly);

        boolean dataDefinitionCausesTransactionCommit = metaData.dataDefinitionCausesTransactionCommit();
        map.put("dataDefinitionCausesTransactionCommit", dataDefinitionCausesTransactionCommit);

        boolean dataDefinitionIgnoredInTransactions = metaData.dataDefinitionIgnoredInTransactions();
        map.put("dataDefinitionIgnoredInTransactions", dataDefinitionIgnoredInTransactions);

/**
 * todo with out parameter

        boolean supportsTransactionIsolationLevel = metaData.supportsTransactionIsolationLevel(1);
        map.put("supportsTransactionIsolationLevel", supportsTransactionIsolationLevel);

        boolean supportsResultSetType = metaData.supportsResultSetType(1);
        map.put("supportsResultSetType", supportsResultSetType);

        boolean updatesAreDetected = metaData.updatesAreDetected(1);
        map.put("updatesAreDetected", updatesAreDetected);

        boolean othersDeletesAreVisible = metaData.othersDeletesAreVisible(1);
        map.put("othersDeletesAreVisible", othersDeletesAreVisible);

        boolean othersInsertsAreVisible = metaData.othersInsertsAreVisible(1);
        map.put("othersInsertsAreVisible", othersInsertsAreVisible);

        boolean supportsResultSetConcurrency = metaData.supportsResultSetConcurrency(1,2);
        map.put("supportsResultSetConcurrency", supportsResultSetConcurrency);

        boolean ownUpdatesAreVisible = metaData.ownUpdatesAreVisible(1);
        map.put("ownUpdatesAreVisible", ownUpdatesAreVisible);

        boolean ownDeletesAreVisible = metaData.ownDeletesAreVisible(1);
        map.put("ownDeletesAreVisible", ownDeletesAreVisible);

        boolean ownInsertsAreVisible = metaData.ownInsertsAreVisible(1);
        map.put("ownInsertsAreVisible", ownInsertsAreVisible);

        boolean othersUpdatesAreVisible = metaData.othersUpdatesAreVisible(1);
        map.put("othersUpdatesAreVisible", othersUpdatesAreVisible);

        boolean deletesAreDetected = metaData.deletesAreDetected(1);
        map.put("deletesAreDetected", deletesAreDetected);

        boolean insertsAreDetected = metaData.insertsAreDetected(1);
        map.put("insertsAreDetected", insertsAreDetected);

        boolean supportsResultSetHoldability = metaData.supportsResultSetHoldability(1);
        map.put("supportsResultSetHoldability", supportsResultSetHoldability);
*/

        boolean supportsBatchUpdates = metaData.supportsBatchUpdates();
        map.put("supportsBatchUpdates", supportsBatchUpdates);

        boolean supportsSavepoints = metaData.supportsSavepoints();
        map.put("supportsSavepoints", supportsSavepoints);

        boolean supportsNamedParameters = metaData.supportsNamedParameters();
        map.put("supportsNamedParameters", supportsNamedParameters);

        boolean supportsMultipleOpenResults = metaData.supportsMultipleOpenResults();
        map.put("supportsMultipleOpenResults", supportsMultipleOpenResults);

        boolean supportsGetGeneratedKeys = metaData.supportsGetGeneratedKeys();
        map.put("supportsGetGeneratedKeys", supportsGetGeneratedKeys);

        boolean locatorsUpdateCopy = metaData.locatorsUpdateCopy();
        map.put("locatorsUpdateCopy", locatorsUpdateCopy);

        boolean supportsStatementPooling = metaData.supportsStatementPooling();
        map.put("supportsStatementPooling", supportsStatementPooling);

        boolean supportsStoredFunctionsUsingCallSyntax = metaData.supportsStoredFunctionsUsingCallSyntax();
        map.put("supportsStoredFunctionsUsingCallSyntax", supportsStoredFunctionsUsingCallSyntax);

        boolean autoCommitFailureClosesAllResultSets = metaData.autoCommitFailureClosesAllResultSets();
        map.put("autoCommitFailureClosesAllResultSets", autoCommitFailureClosesAllResultSets);

        boolean generatedKeyAlwaysReturned = metaData.generatedKeyAlwaysReturned();
        map.put("generatedKeyAlwaysReturned", generatedKeyAlwaysReturned);

        Set<String> keys = map.keySet();
        for (String k : keys) {
            try {
                assertEquals(map.get(k).toString(),mapBy1X.get(k));
            } catch (AssertionError e) {
                if ("supportsDataDefinitionAndDataManipulationTransactions".equals(k)
                ||"supportsFullOuterJoins".equals(k)
                ||"supportsMultipleOpenResults".equals(k)
                ||"generatedKeyAlwaysReturned".equals(k)
                ||"supportsSchemasInPrivilegeDefinitions".equals(k)
                ||"supportsOrderByUnrelated".equals(k)
                ||"supportsIntegrityEnhancementFacility".equals(k)
                ){
                    // ignore , Here 1.X isn't consistent with 2.X , but 2.X is consistent with Oracle-JDBC
                }else {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void fixGetTables() {
        try {
            Statement stmt = sharedConnection.createStatement();
            try {
                stmt.execute("drop table \"Abc\"");
            } catch (SQLException e) {
                // eat exception
            }
            stmt.execute("create table  \"Abc\"(c1 int)");
            ResultSet rs = sharedConnection.getMetaData().getTables(null,
                sharedConnection.getSchema(), "Abc", null);
            Assert.assertTrue(rs.next());
            Assert.assertEquals("Abc", rs.getString("TABLE_NAME"));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void fixGetColumns() {
        try {
            Statement stmt = sharedConnection.createStatement();
            try {
                stmt.execute("drop table \"Xyz\"");
            } catch (SQLException e) {
                // eat exception
            }
            stmt.execute("create table  \"Xyz\"(c1 int)");
            ResultSet rs = sharedConnection.getMetaData().getColumns(null,
                sharedConnection.getSchema(), "Xyz", null);
            Assert.assertTrue(rs.next());
            Assert.assertEquals("Xyz", rs.getString("TABLE_NAME"));
            Assert.assertEquals("C1", rs.getString("COLUMN_NAME"));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void fixGetColumns2() {
        try {
            Statement stmt = sharedConnection.createStatement();
            try {
                stmt.execute("drop table \"Qwe\"");
            } catch (SQLException e) {
                // eat exception
            }
            stmt.execute("create table  \"Qwe\"(\"coL\" int)");
            ResultSet rs = sharedConnection.getMetaData().getColumns(null,
                sharedConnection.getSchema(), "Qwe", "COL");
            Assert.assertFalse(rs.next());
            rs = sharedConnection.getMetaData().getColumns(null, sharedConnection.getSchema(),
                "Qwe", "coL");
            Assert.assertTrue(rs.next());
            Assert.assertEquals("Qwe", rs.getString("TABLE_NAME"));
            Assert.assertEquals("coL", rs.getString("COLUMN_NAME"));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testgetImportedKeyAndExportKeys() {
        try {
            Connection conn = sharedConnection;
            DatabaseMetaData metaData = conn.getMetaData();
            Statement stmt = conn.createStatement();
            try {
                stmt.execute("drop table t2");
            } catch (Exception e) {
            }
            try {
                stmt.execute("drop table t1");
            } catch (Exception e) {
            }
            stmt.execute("create table t1(o_w_id int primary key)");
            stmt.execute("create table t2(o_c_id int not null,"
                         + "constraint fk_t2 foreign key(o_c_id) references t1(o_w_id))");
            ResultSet rs = null;
            rs = metaData.getImportedKeys(null, null, "T2");
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getString("FKCOLUMN_NAME").equals("O_C_ID"));

            rs = metaData.getExportedKeys(null, null, "T1");
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getString("FKCOLUMN_NAME").equals("O_C_ID"));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testGetFunctionColumns() {
        Connection conn = sharedConnection;
        try {
            Statement stmt = conn.createStatement();
            try {
                stmt.execute("drop function testFunction");
            } catch (SQLException e) {
                //
            }
            stmt.execute("create or replace function testFunction(id3 in int) return int is id1 int; begin id1:=1; end;");
            ResultSet rs = conn.getMetaData().getFunctionColumns(null, null, "TESTFUNCTION", "ID3");
            assertTrue(rs.next());
            Assert.assertEquals("TESTFUNCTION", rs.getString("FUNCTION_NAME"));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testGetProcedureColumns() {
        try {
            Connection conn = sharedConnection;
            Statement stmt = conn.createStatement();
            try {
                stmt.execute("drop procedure testProcedure");
            } catch (SQLException e) {
                //
            }
            stmt.execute("create or replace procedure testProcedure (id_1 in int,id_2 in nvarchar2,id_3 out int)is id_4 int;  begin id_4:=id_1; end testProcedure;");
            ResultSet rs = conn.getMetaData().getProcedureColumns(null, null, "TESTPROCEDURE",
                "ID_2");
            assertTrue(rs.next());
            Assert.assertEquals("TESTPROCEDURE", rs.getString(3));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testFunc() {
        try {
            Connection conn = sharedConnection;
            Statement stmt = conn.createStatement();
            try {
                stmt.execute("drop table TEST_GET_COLUMN;");
            } catch (SQLException e) {
            }
            stmt.execute("create table TEST_GET_COLUMN (v0 number , v1 varchar2(32) , v2 varchar2(1))");
            ResultSet rs = conn.getMetaData().getColumns("%", "%", "TEST_GET_COLUMN", "%");
            ResultSetMetaData metaData = rs.getMetaData();
            rs.next();
            int count = metaData.getColumnCount();
            for (int i = 1; i <= count; i++) {
                System.out.print(metaData.getColumnName(i) + "\t:\t");
                System.out.print(metaData.getColumnType(i) + "\t:\t");
                try {
                    System.out.println(rs.getObject(i));
                } catch (SQLException e) {
                    e.printStackTrace();
                    Assert.fail();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }


    @Test
    public void getIdentifierQuoteStringTest() throws SQLException {
        Statement stmt = null;
        ResultSet rs = null;
        ResultSetMetaData meta = null;

        createTable("quote_test", "\"id\" int primary key , \"create_TIME\" timestamp");
        stmt = sharedConnection.createStatement();
        stmt.executeUpdate("insert into quote_test values(1,sysdate)");

        rs = stmt.executeQuery("select * from quote_test");
        assertTrue(rs.next());
        assertEquals("\"", sharedConnection.getMetaData().getIdentifierQuoteString());
        meta = rs.getMetaData();
        Assert.assertEquals("create_TIME", meta.getColumnName(2));
        Assert.assertEquals("create_TIME", meta.getColumnLabel(2));
        Assert.assertEquals("", meta.getSchemaName(2));
        Assert.assertEquals("TEST", meta.getCatalogName(2));
        Assert.assertEquals("QUOTE_TEST", meta.getTableName(2));

        try {
            rs = stmt.executeQuery("select \"id\", create_TIME from quote_test");
            assertTrue(rs.next());
            meta = rs.getMetaData();
            System.out.println("ColumnName: " + meta.getColumnName(2));
            System.out.println("ColumnLabel: " + meta.getColumnLabel(2));
            System.out.println("SchemaName: " + meta.getSchemaName(2));
            System.out.println("CatalogName: " + meta.getCatalogName(2));
            System.out.println("TableName: " + meta.getTableName(2));
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("ORA-00904: invalid identifier 'CREATE_TIME' in 'field list'"));
        }

        rs = stmt.executeQuery("select \"id\", \"create_TIME\" as create_time from quote_test");
        assertTrue(rs.next());
        meta = rs.getMetaData();
        Assert.assertEquals("CREATE_TIME", meta.getColumnName(2));
        Assert.assertEquals("CREATE_TIME", meta.getColumnLabel(2));
        Assert.assertEquals("", meta.getSchemaName(2));
        Assert.assertEquals("TEST", meta.getCatalogName(2));
        Assert.assertEquals("QUOTE_TEST", meta.getTableName(2));

        createTable("quote_test_2", "id int primary key , create_TIME timestamp");
        stmt = sharedConnection.createStatement();
        stmt.executeUpdate("insert into quote_test_2 values(1,sysdate)");

        rs = stmt.executeQuery("select * from quote_test_2");
        assertTrue(rs.next());
        assertEquals("\"", sharedConnection.getMetaData().getIdentifierQuoteString());
        meta = rs.getMetaData();
        Assert.assertEquals("CREATE_TIME", meta.getColumnName(2));
        Assert.assertEquals("CREATE_TIME", meta.getColumnLabel(2));
        Assert.assertEquals("", meta.getSchemaName(2));
        Assert.assertEquals("TEST", meta.getCatalogName(2));
        Assert.assertEquals("QUOTE_TEST_2", meta.getTableName(2));

        rs = stmt.executeQuery("select id, create_TIME as create_time from quote_test_2");
        assertTrue(rs.next());
        meta = rs.getMetaData();
        Assert.assertEquals("CREATE_TIME", meta.getColumnName(2));
        Assert.assertEquals("CREATE_TIME", meta.getColumnLabel(2));
        Assert.assertEquals("", meta.getSchemaName(2));
        Assert.assertEquals("TEST", meta.getCatalogName(2));
        Assert.assertEquals("QUOTE_TEST_2", meta.getTableName(2));
    }

    @Test
    public void getSQLKeywordsTest() throws SQLException {
        assertEquals(
                "ACCESS, ADD, ALTER, AUDIT, CLUSTER, COLUMN, COMMENT, COMPRESS, CONNECT, DATE, DROP, EXCLUSIVE, FILE, IDENTIFIED, IMMEDIATE, INCREMENT, INDEX, INITIAL, INTERSECT, LEVEL, LOCK, LONG, MAXEXTENTS, MINUS, MODE, NOAUDIT, NOCOMPRESS, NOWAIT, NUMBER, OFFLINE, ONLINE, PCTFREE, PRIOR, all_PL_SQL_reserved_ words",
                sharedConnection.getMetaData().getSQLKeywords());
    }

    @Test
    public void getNumericFunctionsTest() throws SQLException {
        assertEquals(
                "ABS,ACOS,ASIN,ATAN,ATAN2,CEILING,COS,EXP,FLOOR,LOG,LOG10,MOD,PI,POWER,ROUND,SIGN,SIN,SQRT,TAN,TRUNCATE",
                sharedConnection.getMetaData().getNumericFunctions());
    }

    @Test
    public void getStringFunctionsTest() throws SQLException {
        assertEquals(
                "ASCII,CHAR,CHAR_LENGTH,CHARACTER_LENGTH,CONCAT,LCASE,LENGTH,LTRIM,OCTET_LENGTH,REPLACE,RTRIM,SOUNDEX,SUBSTRING,UCASE",
                sharedConnection.getMetaData().getStringFunctions());
    }

    @Test
    public void getSystemFunctionsTest() throws SQLException {
        assertEquals("USER", sharedConnection.getMetaData().getSystemFunctions());
    }

    @Test
    public void getTimeDateFunctionsTest() throws SQLException {
        assertEquals(
                "CURRENT_DATE,CURRENT_TIMESTAMP,CURDATE,EXTRACT,HOUR,MINUTE,MONTH,SECOND,YEAR",
                sharedConnection.getMetaData().getTimeDateFunctions());
    }

    @Test
    public void getIndexInfoTest() throws SQLException {
        String tableName2 = "getIndexInfoTest" + getRandomString(10);
        createTable(tableName2, "no INT NOT NULL ,\n" + "    product_category INT NOT NULL,\n"
                + "    product_id INT NOT NULL,\n"
                + "    customer_id INT NOT NULL,\n" + "    PRIMARY KEY(no)\n");

        try {
            Connection connection = sharedConnection;
            ResultSet rs = connection.getMetaData().getIndexInfo(null, null,
                    tableName2.toUpperCase(Locale.ROOT), false, false); // same as oracle driver ,Need to use uppercase table name
            int columnCount = rs.getMetaData().getColumnCount();
            Assert.assertEquals(13, columnCount);
            ResultSetMetaData resultSetMetaData = rs.getMetaData();
            Assert.assertTrue(rs.next());
            for (int i = 1; i <= columnCount; i++) {
                System.out.print("column " + resultSetMetaData.getColumnName(i));
                System.out.println(" value = " + rs.getString(i));
            }
            Assert.assertEquals("TABLE_SCHEM", resultSetMetaData.getColumnName(2));
            Assert.assertEquals("UNITTESTS", rs.getString(2));
            Assert.assertEquals("TABLE_NAME", resultSetMetaData.getColumnName(3));
            Assert.assertEquals(tableName2.toUpperCase(Locale.ROOT), rs.getString(3));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testGetPLColumns() throws Exception {
        Connection conn = sharedConnection;

        Statement stmt = conn.createStatement();

        stmt.execute("create or replace procedure testGetProcedureColumns (var1 out int, var2 int) is BEGIN var1 := var2; END;");
        ResultSet rs = conn.getMetaData().getProcedureColumns(null, null,
                "TESTGETPROCEDURECOLUMNS", "VAR2");
        assertTrue(rs.next());
        assertEquals("VAR2", rs.getString("COLUMN_NAME"));
        assertEquals("VAR2", rs.getObject("COLUMN_NAME"));
        assertFalse(rs.next());

        stmt.execute("create or replace function testGetFunctionColumns (var1 out int, var2 int) return int is BEGIN var1 := var2; return var1; END;");
        rs = conn.getMetaData().getFunctionColumns(null, null, "TESTGETFUNCTIONCOLUMNS", "VAR1");
        assertTrue(rs.next());
        assertEquals("VAR1", rs.getString("COLUMN_NAME"));
        assertEquals("VAR1", rs.getObject("COLUMN_NAME"));
        assertFalse(rs.next());
    }

}
