package org.mariadb.jdbc;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;

/**
 * @Author : wb-xsy
 * @Description
 * @Date : 2020/9/25 16:48
 */
public class OracleDatabaseMetadataTest extends BaseOracleTest{

    /**
     * Initialisation.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("quote_test","id int primary key , create_time timestamp") ;
    }

    @Before
    public void checkSupported() throws SQLException {
        requireMinimumVersion(5, 1);
    }

    @Test
    public void getIdentifierQuoteStringTest(){
        Statement stmt = null;
        ResultSet rs = null ;
        try {
            stmt = sharedConnection.createStatement();
            stmt.executeUpdate("insert into quote_test values(1,sysdate)") ;
            rs = stmt.executeQuery("select ID,CREATE_TIME from quote_test");
            assertEquals("ID", rs.getMetaData().getColumnName(1));
            assertEquals("CREATE_TIME", rs.getMetaData().getColumnName(2));
            assertEquals(true, rs.next());
            assertEquals("\"",sharedConnection.getMetaData().getIdentifierQuoteString());
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    @Test
    public void getSQLKeywordsTest(){
        Statement stmt = null;
        ResultSet rs = null ;
        try {
            stmt = sharedConnection.createStatement();
            assertEquals("ACCESS, ADD, ALTER, AUDIT, CLUSTER, COLUMN, COMMENT, COMPRESS, CONNECT, DATE, DROP, EXCLUSIVE, FILE, IDENTIFIED, IMMEDIATE, INCREMENT, INDEX, INITIAL, INTERSECT, LEVEL, LOCK, LONG, MAXEXTENTS, MINUS, MODE, NOAUDIT, NOCOMPRESS, NOWAIT, NUMBER, OFFLINE, ONLINE, PCTFREE, PRIOR, all_PL_SQL_reserved_ words"
                    ,sharedConnection.getMetaData().getSQLKeywords());
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    @Test
    public void getNumericFunctionsTest(){
        Statement stmt = null;
        ResultSet rs = null ;
        try {
            stmt = sharedConnection.createStatement();
            assertEquals("ABS,ACOS,ASIN,ATAN,ATAN2,CEILING,COS,EXP,FLOOR,LOG,LOG10,MOD,PI,POWER,ROUND,SIGN,SIN,SQRT,TAN,TRUNCATE"
                    ,sharedConnection.getMetaData().getNumericFunctions());
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    @Test
    public void getStringFunctionsTest(){
        Statement stmt = null;
        ResultSet rs = null ;
        try {
            stmt = sharedConnection.createStatement();
            assertEquals("ASCII,CHAR,CHAR_LENGTH,CHARACTER_LENGTH,CONCAT,LCASE,LENGTH,LTRIM,OCTET_LENGTH,REPLACE,RTRIM,SOUNDEX,SUBSTRING,UCASE"
                    ,sharedConnection.getMetaData().getStringFunctions());
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }
    @Test
    public void getSystemFunctionsTest(){
        Statement stmt = null;
        ResultSet rs = null ;
        try {
            stmt = sharedConnection.createStatement();
            assertEquals("USER"
                    ,sharedConnection.getMetaData().getSystemFunctions());
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }
    @Test
    public void getTimeDateFunctionsTest(){
        Statement stmt = null;
        ResultSet rs = null ;
        try {
            stmt = sharedConnection.createStatement();
            assertEquals("CURRENT_DATE,CURRENT_TIMESTAMP,CURDATE,EXTRACT,HOUR,MINUTE,MONTH,SECOND,YEAR"
                    ,sharedConnection.getMetaData().getTimeDateFunctions());
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }
}
