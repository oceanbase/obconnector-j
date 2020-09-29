package org.mariadb.jdbc;

import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.*;

//请在oracle模式账户下执行本测试类
public class OceanBaseOracleDatabaseMetadataTest extends BaseTest {
    DatabaseMetaData dbmd = sharedConnection.getMetaData();

    @Test
    public void testGetCatalogs() throws SQLException {
        ResultSet rs = dbmd.getCatalogs();
        boolean hasNext = true;
        if (!rs.next()) {
            hasNext = false;
        }
        //oracle中此方法拿不到任何数据
        assertTrue(!hasNext);
    }

    @Test
    public void testGetColumns() throws SQLException {
        createTable(
                "GETCOLUMNS_TEST",
                "a INT NOT NULL primary key, b VARCHAR(32), c INT , "
                        + "d VARCHAR(5)",
                null);

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
        assertEquals(0,rs.getInt(8));//BUFFER_LENGTH
        assertEquals(0, rs.getInt(9)); // DECIMAL_DIGITS
        assertEquals(10, rs.getInt(10)); // NUM_PREC_RADIX
        assertEquals(0, rs.getInt(11)); // NULLABLE
        assertEquals(null, rs.getString(12)); // REMARKS
        assertEquals(null, rs.getString(13)); // DATA_DEFAULT
        assertEquals(0,rs.getInt(14));//SQL_DATA_TYPE
        assertEquals(0,rs.getInt(15));//SQL_DATETIME_SUB
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
        assertEquals(0,rs.getInt(8));//BUFFER_LENGTH
        assertEquals(0, rs.getInt(9)); // DECIMAL_DIGITS
        assertEquals(10, rs.getInt(10)); // NUM_PREC_RADIX
        assertEquals(1, rs.getInt(11)); // NULLABLE
        assertEquals(null, rs.getString(12)); // REMARKS
        assertTrue("null".equalsIgnoreCase(rs.getString(13)) || rs.getString(13) == null); // DATA_DEFAULT
        assertEquals(0,rs.getInt(14));//SQL_DATA_TYPE
        assertEquals(0,rs.getInt(15));//SQL_DATETIME_SUB
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
        assertTrue("CHAR_LENGTH(`b`)".equalsIgnoreCase(rs.getString(13)) || rs.getString(13) == null); // COLUMN_DEF
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
        assertTrue("CHAR_LENGTH(`b`)".equalsIgnoreCase(rs.getString(13)) || rs.getString(13) == null); // COLUMN_DEF
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
    public void getDatabaseProductNameTest() throws SQLException {
        DatabaseMetaData dmd = sharedConnection.getMetaData();
        String databaseProductName = dmd.getDatabaseProductName();
        assertEquals(databaseProductName,"OceanBase");
    }

    @Test
    public void getFunctionsTest() throws SQLException {
        ResultSet rs = sharedConnection.getMetaData().getFunctions(null, null, null);
        assertTrue(rs.next());
        assertEquals(rs.getString("FUNCTION_CAT"),null);
        assertEquals(rs.getString("FUNCTION_SCHEM"),"ADMIN");
        assertEquals(rs.getString("FUNCTION_NAME"),"CALC_FUNC");
        assertEquals(rs.getString("REMARKS"),"Standalone function");
        assertEquals(rs.getInt("FUNCTION_TYPE"),0);
        assertEquals(rs.getString("SPECIFIC_NAME"),null);
    }

    @Test
    public void getProceduresTest() throws SQLException {
        ResultSet rs = sharedConnection.getMetaData().getProcedures(null, null, null);
        assertTrue(rs.next());
        assertEquals(rs.getString("PROCEDURE_CAT"),null);
        assertEquals(rs.getString("PROCEDURE_SCHEM"),"ADMIN");
        assertEquals(rs.getString("PROCEDURE_NAME"),"ARR_PROC");
        assertEquals(rs.getString("REMARKS"),"Standalone procedure or function");
        assertEquals(rs.getInt("PROCEDURE_TYPE"),1);
        assertEquals(rs.getString("SPECIFIC_NAME"),null);
    }

    @Test
    public void testGetSchemas() throws SQLException {
        DatabaseMetaData dbmd = sharedConnection.getMetaData();
        ResultSet rs = dbmd.getSchemas();
        ResultSetMetaData metaData = rs.getMetaData();
        assertTrue(rs.next());
        assertEquals(metaData.getColumnName(1),"USERNAME");
        assertEquals(metaData.getColumnName(2),"TABLE_CATALOG");
    }

    @Test
    public void testGetTables() throws Exception {
        ResultSet rs = sharedConnection.getMetaData().getTables(null,null,null,null);
        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_CAT"),null);
        assertEquals(rs.getString("TABLE_SCHEM"),"__public");
        assertEquals(rs.getString("TABLE_NAME"),"TESTXX");
        assertEquals(rs.getString("TABLE_TYPE"),"SYNONYM");
        assertEquals(rs.getString("REMARKS"),null);

        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_CAT"),null);
        assertEquals(rs.getString("TABLE_SCHEM"),"__public");
        assertEquals(rs.getString("TABLE_NAME"),"TEST_SYNONYMS");
        assertEquals(rs.getString("TABLE_TYPE"),"SYNONYM");
        assertEquals(rs.getString("REMARKS"),null);

        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_CAT"),null);
        assertEquals(rs.getString("TABLE_SCHEM"),"ADMIN");
        assertEquals(rs.getString("TABLE_NAME"),"ADMIN");
        assertEquals(rs.getString("TABLE_TYPE"),"TABLE");
        assertEquals(rs.getString("REMARKS"),null);
    }

    @Test
    public void getDriverNameTest() throws SQLException {
        String driverName = sharedConnection.getMetaData().getDriverName();
        assertEquals(driverName,"OceanBase Connector/J");
    }

    @Test
    public void getUserNameTest() throws SQLException {
        String userName = sharedConnection.getMetaData().getUserName();
        String url = System.getProperty("dbUrl", mDefUrl);
        UrlParser parse = UrlParser.parse(url);
        assertEquals(userName,parse.getUsername());
    }

}
