package com.alipay.oceanbase.jdbc;

import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.*;

//在oracle模式账户下执行本测试类
public class OracleModeDatabaseMetadataTest extends BaseOracleTest {

    private static DatabaseMetaData dbmd;

    static {
        try {
            dbmd = sharedConnection.getMetaData();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetCatalogs() throws SQLException {
        ResultSet rs = dbmd.getCatalogs();
        boolean hasNext = true;
        if (!rs.next()) {
            hasNext = false;
        }
        //oracle中此方法返回null
        assertTrue(!hasNext);
    }

    @Test
    public void testGetColumns() throws SQLException {
        createTable("GETCOLUMNS_TEST", "a INT NOT NULL primary key, b VARCHAR(32), c INT , "
                                       + "d VARCHAR(5)", null);

        DatabaseMetaData dbmd = sharedConnection.getMetaData();
        ResultSet rs = dbmd.getColumns(null, null, "GETCOLUMNS_TEST", null);
        assertTrue(rs.next());
        assertEquals(null, rs.getString(1)); // TABLE_CAT
        assertEquals("TEST", rs.getString(2)); // OWNER
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
        assertEquals(0, rs.getInt(16)); // DATA_LENGTH
        assertEquals(16, rs.getInt(17)); // COLUMN_ID
        assertEquals("NO", rs.getString(18)); // IS_NULLABLE
        assertEquals(null, rs.getString(19)); // SCOPE_CATALOG
        assertEquals(null, rs.getString(20)); // SCOPE_SCHEMA
        assertEquals(null, rs.getString(21)); // SCOPE_TABLE
        assertEquals(0, rs.getShort(22)); // SOURCE_DATA_TYPE
        assertEquals("NO", rs.getString(23)); // IS_AUTOINCREMENT

        assertTrue(rs.next());
        assertEquals(null, rs.getString(1)); // TABLE_CAT
        assertEquals("TEST", rs.getString(2)); // OWNER
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
        assertEquals(17, rs.getInt(17)); // COLUMN_ID
        assertEquals("YES", rs.getString(18)); // IS_NULLABLE
        assertEquals(null, rs.getString(19)); // SCOPE_CATALOG
        assertEquals(null, rs.getString(20)); // SCOPE_SCHEMA
        assertEquals(null, rs.getString(21)); // SCOPE_TABLE
        assertEquals(0, rs.getShort(22)); // SOURCE_DATA_TYPE
        assertEquals("NO", rs.getString(23)); // IS_AUTOINCREMENT

        assertTrue(rs.next());
        assertEquals(null, rs.getString(1)); // TABLE_CAT
        assertEquals("TEST", rs.getString(2)); // OWNER
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
        assertEquals(0, rs.getInt(16)); // CHAR_OCTET_LENGTH
        assertEquals(18, rs.getInt(17)); // ORDINAL_POSITION
        assertEquals("YES", rs.getString(18)); // IS_NULLABLE
        assertEquals(null, rs.getString(19)); // SCOPE_CATALOG
        assertEquals(null, rs.getString(20)); // SCOPE_SCHEMA
        assertEquals(null, rs.getString(21)); // SCOPE_TABLE
        assertEquals(0, rs.getShort(22)); // SOURCE_DATA_TYPE
        assertEquals("NO", rs.getString(23)); // IS_AUTOINCREMENT

        assertTrue(rs.next());
        assertEquals(null, rs.getString(1)); // TABLE_CAT
        assertEquals("TEST", rs.getString(2)); // OWNER
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
        assertEquals(19, rs.getInt(17)); // ORDINAL_POSITION
        assertEquals("YES", rs.getString(18)); // IS_NULLABLE
        assertEquals(null, rs.getString(19)); // SCOPE_CATALOG
        assertEquals(null, rs.getString(20)); // SCOPE_SCHEMA
        assertEquals(null, rs.getString(21)); // SCOPE_TABLE
        assertEquals(0, rs.getShort(22)); // SOURCE_DATA_TYPE
        assertEquals("NO", rs.getString(23)); // IS_AUTOINCREMENT

        assertTrue(!rs.next());
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

    @Test
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

    @Test
    public void getFunctionsBasic() throws SQLException {
        testResultSetColumns(
            sharedConnection.getMetaData().getFunctions(null, null, null),
            "FUNCTION_CAT String, FUNCTION_SCHEM String,FUNCTION_NAME String,REMARKS String,FUNCTION_TYPE decimal,SPECIFIC_NAME String");
    }

    @Test
    public void getProceduresTest() throws SQLException {
        ResultSet rs = sharedConnection.getMetaData().getProcedures(null, null, null);
        assertTrue(rs.next());
        assertEquals(rs.getString("PROCEDURE_CAT"), null);
        assertEquals(rs.getString("PROCEDURE_SCHEM"), "ADMIN");
        assertEquals(rs.getString("PROCEDURE_NAME"), "ARR_PROC");
        assertEquals(rs.getString("REMARKS"), "Standalone procedure or function");
        assertEquals(rs.getInt("PROCEDURE_TYPE"), 1);
        assertEquals(rs.getString("SPECIFIC_NAME"), null);
        int i = rs.getMetaData().getColumnCount();
        for (int i1 = 1; i1 < i; i1++) {
            System.out.print(rs.getMetaData().getColumnName(i1) + ":");
            System.out.print(rs.getMetaData().getColumnTypeName(i1) + ":");
            System.out.println(rs.getMetaData().getColumnType(i1));
        }
    }

    @Test
    public void getProceduresBasic() throws SQLException {
        ResultSet rs = sharedConnection.getMetaData().getProcedures(null, null, null);
        testResultSetColumns(
            rs,
            "PROCEDURE_CAT String,PROCEDURE_SCHEM String,PROCEDURE_NAME String,NULL String,NULL String,NULL String,REMARKS String,PROCEDURE_TYPE decimal,SPECIFIC_NAME String");
    }

    @Test
    public void testGetSchemas() throws SQLException {
        DatabaseMetaData dbmd = sharedConnection.getMetaData();
        ResultSet rs = dbmd.getSchemas();
        ResultSetMetaData metaData = rs.getMetaData();
        assertTrue(rs.next());
        assertEquals(metaData.getColumnName(1), "USERNAME");
        System.out.println(metaData.getColumnName(1));
        System.out.println(metaData.getColumnTypeName(1));
        assertEquals(metaData.getColumnName(2), "TABLE_CATALOG");
    }

    @Test
    public void getSchemasBasic() throws SQLException {
        DatabaseMetaData dbmd = sharedConnection.getMetaData();
        ResultSet rs = dbmd.getSchemas();
        testResultSetColumns(rs, "TABLE_SCHEM String , TABLE_CATALOG String");
    }

    @Test
    public void testGetTables() throws Exception {
        ResultSet rs = sharedConnection.getMetaData().getTables(null, null, null, null);
        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_CAT"), null);
        assertEquals(rs.getString("TABLE_SCHEM"), "__public");
        assertEquals(rs.getString("TABLE_NAME"), "TESTXX");
        assertEquals(rs.getString("TABLE_TYPE"), "SYNONYM");
        assertEquals(rs.getString("REMARKS"), null);

        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_CAT"), null);
        assertEquals(rs.getString("TABLE_SCHEM"), "__public");
        assertEquals(rs.getString("TABLE_NAME"), "TEST_SYNONYMS");
        assertEquals(rs.getString("TABLE_TYPE"), "SYNONYM");
        assertEquals(rs.getString("REMARKS"), null);

        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_CAT"), null);
        assertEquals(rs.getString("TABLE_SCHEM"), "ADMIN");
        assertEquals(rs.getString("TABLE_NAME"), "ADMIN");
        assertEquals(rs.getString("TABLE_TYPE"), "TABLE");
        assertEquals(rs.getString("REMARKS"), null);
    }

    @Test
    public void getDriverNameTest() throws SQLException {
        String driverName = sharedConnection.getMetaData().getDriverName();
        assertEquals(driverName, "OceanBase Connector/J");
    }

    @Test
    public void getUserNameTest() throws SQLException {
        String userName = sharedConnection.getMetaData().getUserName();
        String url = System.getProperty("dbUrl", mDefUrl);
        UrlParser parse = UrlParser.parse(url);
        assertEquals(userName, parse.getUsername());
    }

    private void testResultSetColumns(ResultSet rs, String spec) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        String[] tokens = spec.split(",");

        for (int i = 0; i < tokens.length; i++) {
            String[] splitTokens = tokens[i].trim().split(" ");
            String label = splitTokens[0];
            String type = splitTokens[1];

            int col = i + 1;
            assertEquals(label, rsmd.getColumnLabel(col));
            int columnType = rsmd.getColumnType(col);
            switch (type) {
                case "String":
                    assertTrue("invalid type  " + columnType + " for " + rsmd.getColumnLabel(col)
                               + ",expected String", columnType == Types.VARCHAR
                                                     || columnType == Types.NULL
                                                     || columnType == Types.LONGVARCHAR);
                    break;
                case "decimal":
                    assertTrue("invalid type  " + columnType + "( " + rsmd.getColumnTypeName(col)
                               + " ) for " + rsmd.getColumnLabel(col) + ",expected decimal",
                        columnType == Types.DECIMAL);
                    break;
                case "int":
                case "short":
                    assertTrue("invalid type  " + columnType + "( " + rsmd.getColumnTypeName(col)
                               + " ) for " + rsmd.getColumnLabel(col) + ",expected numeric",
                        columnType == Types.BIGINT || columnType == Types.INTEGER
                                || columnType == Types.SMALLINT || columnType == Types.TINYINT);

                    break;
                case "boolean":
                    assertTrue("invalid type  " + columnType + "( " + rsmd.getColumnTypeName(col)
                               + " ) for " + rsmd.getColumnLabel(col) + ",expected boolean",
                        columnType == Types.BOOLEAN || columnType == Types.BIT);

                    break;
                case "null":
                    assertTrue("invalid type  " + columnType + " for " + rsmd.getColumnLabel(col)
                               + ",expected null", columnType == Types.NULL);
                    break;
                default:
                    fail("invalid type '" + type + "'");
                    break;
            }
        }
    }
}
