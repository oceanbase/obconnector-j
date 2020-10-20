package com.alipay.oceanbase.jdbc;

import com.alipay.oceanbase.jdbc.extend.datatype.INTERVALDS;
import com.alipay.oceanbase.jdbc.extend.datatype.INTERVALYM;
import org.junit.*;
import java.sql.*;

import static org.junit.Assert.*;

/**
 *  The test depends on the ob version .Version 2.2.52 has no problems, but 2.2.60 has errors !
 */
public class INTERVALOracleTest extends BaseOracleTest {

    /**
     * Tables initialisation.
     *
     * @throws SQLException exception
     */
    public static String tableNameDS   = "test_intervalds";
    public static String tableNameYM   = "test_intervalym";
    public static String tableNameYMDS = "test_intervalymds";

    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable(tableNameDS, "c1 int primary key,c2 interval day(6) to second(5)");
        createTable(tableNameYM, " c1 int primary key,c2 interval year(4) to month");
        createTable(tableNameYMDS,
            "c1 int primary key,c3 interval day(6) to second(5),c2 interval year(4) to month");

    }

    /**
     * The basic test of intervalds includes query, insert, update;Due to AONE-30982102,The table contains only one interval column
     * @throws SQLException exception
     */
    @Test
    public void basicOracleIntervalds() throws SQLException {

        String sql = null;
        sql = "delete  from " + tableNameDS;
        sharedPSConnection.createStatement().execute(sql);

        sql = "insert into " + tableNameDS + "  values(?, ?)";
        PreparedStatement pstmt = sharedPSConnection.prepareStatement(sql);
        pstmt.setInt(1, 1);
        ((com.alipay.oceanbase.jdbc.BasePrepareStatement) pstmt).setINTERVALDS(2, new INTERVALDS(
            "2324 5:12:10.212"));
        pstmt.execute();
        sql = "insert into " + tableNameDS + " values(2, '-122324 5:12:10.22222222')";
        pstmt = sharedPSConnection.prepareStatement(sql);
        pstmt.execute();
        sql = "select c1, c2 from " + tableNameDS;
        pstmt = sharedPSConnection.prepareStatement(sql);
        pstmt.execute();
        ResultSet resultSet = pstmt.getResultSet();

        assertTrue(resultSet.next());
        assertEquals(1, resultSet.getInt("c1"));
        assertEquals("+002324 05:12:10.21200", resultSet.getString("c2"));
        assertTrue(resultSet.next());
        assertEquals(2, resultSet.getInt("c1"));
        assertEquals("-122324 05:12:10.22222", resultSet.getString("c2"));
        resultSet.close();
        // test for update and query

        sql = "update " + tableNameDS + " set c2 = ? where c1 = 1";
        pstmt = sharedConnection.prepareStatement(sql);
        ((com.alipay.oceanbase.jdbc.BasePrepareStatement) pstmt).setINTERVALDS(1, new INTERVALDS(
            "1234 5:12:10.212"));
        pstmt.execute();

        sql = "select c2 from " + tableNameDS + " where c1 = 1";
        pstmt = sharedPSConnection.prepareStatement(sql);
        pstmt.execute();
        resultSet = pstmt.getResultSet();
        assertTrue(resultSet.next());
        assertEquals("+001234 05:12:10.21200", resultSet.getString("c2"));
        resultSet.close();
    }

    /**
     * Constructor Summary:
     * INTERVALDS()
     * INTERVALDS(byte[] intervalDS)
     * INTERVALDS(String str)
     */
    @Test
    public void constructorOracleIntervalds() throws SQLException {
        String sql = null;
        sql = "delete  from " + tableNameDS;
        sharedPSConnection.createStatement().execute(sql);

        sql = "insert into " + tableNameDS + "  values(?,?)";
        PreparedStatement pstmt = sharedPSConnection.prepareStatement(sql);
        pstmt.setInt(1, 1);
        ((com.alipay.oceanbase.jdbc.BasePrepareStatement) pstmt).setINTERVALDS(2, new INTERVALDS()); //
        pstmt.addBatch();
        pstmt.setInt(1, 2);
        INTERVALDS val = new INTERVALDS("2324 5:12:10.212");
        ((com.alipay.oceanbase.jdbc.BasePrepareStatement) pstmt).setINTERVALDS(2, new INTERVALDS(
            val.getBytes()));
        pstmt.addBatch();
        pstmt.setInt(1, 3);
        ((com.alipay.oceanbase.jdbc.BasePrepareStatement) pstmt).setINTERVALDS(2, new INTERVALDS(
            "2324 5:12:10.212"));
        pstmt.addBatch();
        pstmt.executeBatch();

        sql = "select c1, c2 from " + tableNameDS;
        pstmt = sharedPSConnection.prepareStatement(sql);
        pstmt.execute();
        ResultSet resultSet = pstmt.getResultSet();

        assertTrue(resultSet.next());
        assertEquals(1, resultSet.getInt("c1"));
        assertEquals("+000000 00:00:00.00000", resultSet.getString("c2"));

        assertTrue(resultSet.next());
        assertEquals(2, resultSet.getInt("c1"));
        assertEquals("+002324 05:12:10.21200", resultSet.getString("c2"));
        assertTrue(resultSet.next());

        assertEquals(3, resultSet.getInt("c1"));
        assertEquals("+002324 05:12:10.21200", resultSet.getString("c2"));
        resultSet.close();

    }

    /**
     * Method Summary:
     * boolean	isConvertibleTo(Class cls)
     * Object	makeJdbcArray(int arraySize)
     * String	stringValue()
     * byte[]	toBytes()
     * static byte[] toBytes(String str)
     * Object	toJdbc()
     * String	toString()
     * static String	toString(byte[] inparray)
     */
    @Test
    public void methodOracleIntervalds() throws SQLException {
        INTERVALDS intervalds = new INTERVALDS("10 3:36:12.222");

        assertTrue(intervalds.isConvertibleTo(String.class));
        assertEquals("+10 03:36:12.222000000", intervalds.toString());
        byte[] bdata = INTERVALDS.toBytes("10 3:36:12.222");
        assertEquals("+10 03:36:12.222000000", INTERVALDS.toString(bdata));
        assertArrayEquals(INTERVALDS.toBytes("5 3:20:11.1"), INTERVALDS.toBytes("5 3:20:11.1"));

    }

    /**
     * Boundary value test of intervalds:
     * Positive number , negative number and unsigned number
     * Invalid hour minute and second number
     * Invalid day length
     * Invalid fracsecond length
     * Invalid format
     * @throws SQLException exception
     */
    @Test
    public void boundaryOracleIntervalds() throws SQLException {
        INTERVALDS unsignedIntervalds = new INTERVALDS("100 10:10:10.10");
        INTERVALDS negIntervalds = new INTERVALDS("-100 10:10:10.10");
        INTERVALDS posIntervalds = new INTERVALDS("+100 10:10:10.10");
        assertEquals("+100 10:10:10.100000000", unsignedIntervalds.toString());
        assertEquals("+100 10:10:10.100000000", posIntervalds.toString());
        assertEquals("-100 10:10:10.100000000", negIntervalds.toString());

        try {
            INTERVALDS inHour = new INTERVALDS("+100 24:10:10.10");
        } catch (Exception e) {
            assertEquals("invalid hour 24 in +100 24:10:10.10", e.getMessage());
        }
        try {
            INTERVALDS inMinute = new INTERVALDS("+100 12:65:10.11");
        } catch (Exception e) {
            assertEquals("invalid minute  65 in +100 12:65:10.11", e.getMessage());
        }
        try {
            INTERVALDS inSecond = new INTERVALDS("+100 12:11:60.11");
        } catch (Exception e) {
            assertEquals("invalid second  60 in +100 12:11:60.11", e.getMessage());
        }

        try {
            INTERVALDS inDaylen = new INTERVALDS("1234567890 12:11:11.11");
        } catch (Exception e) {
            assertEquals("invalid daylen 1234567890", e.getMessage());
        }
        try {
            INTERVALDS inFraceSecondlen = new INTERVALDS("123 12:11:11.1234567890");
        } catch (Exception e) {
            assertEquals("invalid fracsecond length 1234567890 in 123 12:11:11.1234567890",
                e.getMessage());
        }

        try {
            INTERVALDS inFormat = new INTERVALDS("100 11+23+11.20");
        } catch (Exception e) {
            assertEquals("invalid format 100 11+23+11.20", e.getMessage());
        }

    }

    /**
     * The basic test of intervalym includes query, insert, update;Due to AONE-30982102,The table contains only one interval column
     * @throws SQLException
     */
    @Test
    public void basicOracleIntervalym() throws SQLException {
        String sql = null;
        sql = "delete from " + tableNameYM;
        sharedPSConnection.createStatement().execute(sql);

        sql = "insert into " + tableNameYM + " values(?, ?)";
        PreparedStatement pstmt = sharedPSConnection.prepareStatement(sql);
        pstmt.setInt(1, 1);
        ((BasePrepareStatement) pstmt).setINTERVALYM(2, new INTERVALYM("123-2"));
        pstmt.execute();
        sql = "insert into " + tableNameYM + " values(2,'123-2')";
        pstmt = sharedPSConnection.prepareStatement(sql);
        pstmt.execute();

        sql = "select c1, c2 from " + tableNameYM;
        pstmt = sharedPSConnection.prepareStatement(sql);
        pstmt.execute();
        ResultSet rs = pstmt.getResultSet();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("+0123-02", rs.getString(2));

        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("+0123-02", rs.getString(2));

        // test for update and query
        sql = "update " + tableNameYM + " set c2 = ? where c1 = 1";
        pstmt = sharedPSConnection.prepareStatement(sql);
        ((com.alipay.oceanbase.jdbc.BasePrepareStatement) pstmt).setINTERVALYM(1, new INTERVALYM(
            "10-10"));
        pstmt.execute();

        sql = "select c2 from " + tableNameYM + " where c1 = 1";
        pstmt = sharedPSConnection.prepareStatement(sql);
        pstmt.execute();
        rs = pstmt.getResultSet();
        assertTrue(rs.next());
        assertEquals("+0010-10", rs.getString(1));
        rs.close();
    }

    /**
     * Constructor Summary:
     * INTERVALYM()
     * INTERVALYM(byte[] intervalYM)
     * INTERVALYM(String str)
     * @throws SQLException
     */
    @Test
    public void constructorOracleInetervalym() throws SQLException {
        String sql = null;
        sql = "delete from " + tableNameYM;
        sharedPSConnection.createStatement().execute(sql);

        sql = "insert into " + tableNameYM + " values(?,?)";
        PreparedStatement pstmt = sharedPSConnection.prepareStatement(sql);
        pstmt.setInt(1, 1);
        ((com.alipay.oceanbase.jdbc.BasePrepareStatement) pstmt).setINTERVALYM(2, new INTERVALYM());
        pstmt.addBatch();
        pstmt.setInt(1, 2);
        INTERVALYM val = new INTERVALYM("22-3");
        ((com.alipay.oceanbase.jdbc.BasePrepareStatement) pstmt).setINTERVALYM(2, new INTERVALYM(
            val.getBytes()));
        pstmt.addBatch();
        pstmt.setInt(1, 3);
        ((com.alipay.oceanbase.jdbc.BasePrepareStatement) pstmt).setINTERVALYM(2, new INTERVALYM(
            "21-4"));
        pstmt.addBatch();
        pstmt.executeBatch();

        sql = "select c1,c2 from " + tableNameYM;
        pstmt = sharedPSConnection.prepareStatement(sql);
        pstmt.execute();
        ResultSet resultSet = pstmt.getResultSet();

        assertTrue(resultSet.next());
        assertEquals(1, resultSet.getInt("c1"));
        assertEquals("+0000-00", resultSet.getString("c2"));

        assertTrue(resultSet.next());
        assertEquals(2, resultSet.getInt("c1"));
        assertEquals("+0022-03", resultSet.getString("c2"));
        assertTrue(resultSet.next());

        assertEquals(3, resultSet.getInt("c1"));
        assertEquals("+0021-04", resultSet.getString("c2"));
        resultSet.close();

    }

    /**
     * Method Summary:
     * boolean	isConvertibleTo(Class cls)
     * Object	makeJdbcArray(int arraySize)
     * String	stringValue()
     * byte[]	toBytes()
     * static byte[] toBytes(String str)
     * Object	toJdbc()
     * String	toString()
     * static String	toString(byte[] inparray)
     */
    @Test
    public void methodOracleIntervalym() throws SQLException {
        INTERVALYM intervalym = new INTERVALYM("10-11");

        assertTrue(intervalym.isConvertibleTo(String.class));
        assertEquals("+10-11", intervalym.toString());
        byte[] bdata = INTERVALYM.toBytes("11-11");
        assertEquals("+11-11", INTERVALYM.toString(bdata));
        assertArrayEquals(INTERVALYM.toBytes("10-10"), INTERVALYM.toBytes("10-10"));

    }

    /**
     * Boundary value test of intervalym:
     * Positive number , negative number and unsigned number
     * Invalid year length
     * Invalid month number
     * @throws SQLException exception
     */
    @Test
    public void boundaryOracleIntervalym() throws SQLException {
        INTERVALYM unsignedIntervalym = new INTERVALYM("10-1");
        INTERVALYM negIntervalym = new INTERVALYM("-10-1");
        INTERVALYM posIntervalym = new INTERVALYM("+10-1");
        assertEquals("+10-01", unsignedIntervalym.toString());
        assertEquals("-10-01", negIntervalym.toString());
        assertEquals("+10-01", posIntervalym.toString());

        try {
            INTERVALYM inYear = new INTERVALYM("1234567890-10");
        } catch (Exception e) {
            assertEquals("invalid year 1234567890 in 1234567890-10", e.getMessage());
        }
        try {
            INTERVALYM inMonth = new INTERVALYM("10-13");
        } catch (Exception e) {
            assertEquals("invalid month 13 in 10-13", e.getMessage());
        }

    }

    /**
     * Basic tests for intervalds and intervalym both , testing fails now due to AONE-30982102.Ignore this test case now.
     * @throws SQLException exception
     */
    @Ignore
    public void testOracleIntervalymds() throws SQLException {
        String sql = "insert into " + tableNameYMDS + " values(?,?,?)";
        PreparedStatement pstmt = sharedPSConnection.prepareStatement(sql);
        pstmt.setInt(1, 1);
        //        ((BasePrepareStatement)pstmt).setINTERVALDS(,new INTERVALDS("2324 5:12:10.212"));
        pstmt.setString(2, "2324 5:12:10.212");
        ((BasePrepareStatement) pstmt).setINTERVALYM(3, new INTERVALYM("123-2"));
        pstmt.execute();
        sql = "insert into " + tableNameYMDS + " values(2, '-122324 5:12:10.22222222', '123-2')";
        pstmt = sharedPSConnection.prepareStatement(sql);
        pstmt.execute();

        sql = "select * from " + tableNameYMDS;
        pstmt = sharedPSConnection.prepareStatement(sql);
        pstmt.execute();
        ResultSet rs = pstmt.getResultSet();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("+002324 05:12:10.21200", rs.getString(2));
        assertEquals("+0123-02", rs.getString((3)));

        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("-122324 05:12:10.22222", rs.getString(2));
        assertEquals("+0123-02", rs.getString((3)));

    }
}
