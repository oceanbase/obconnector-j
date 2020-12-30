package com.alipay.oceanbase.jdbc;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;

/**
 * @Author : wb-xsy @Description @Date : 2020/9/25 16:48
 */
public class OracleDatabaseMetadataTest extends BaseOracleTest {

    /**
     * Initialisation.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("quote_test1", "id int primary key , create_time timestamp");
    }

    @Before
    public void checkSupported() throws SQLException {
        requireMinimumVersion(5, 1);
    }

    @Test
    public void getIdentifierQuoteStringTest() throws SQLException {
        Statement stmt = null;
        ResultSet rs = null;
        stmt = sharedConnection.createStatement();
        stmt.executeUpdate("insert into quote_test1 values(1,sysdate)");
        rs = stmt.executeQuery("select ID,CREATE_TIME from quote_test");
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("CREATE_TIME", rs.getMetaData().getColumnName(2));
        assertEquals(true, rs.next());
        assertEquals("\"", sharedConnection.getMetaData().getIdentifierQuoteString());
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
}
