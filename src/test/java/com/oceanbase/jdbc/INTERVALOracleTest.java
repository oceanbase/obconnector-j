/*
 *
 * OceanBase Client for Java
 *
 * Copyright (c) 2021 OceanBase.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */
package com.oceanbase.jdbc;

import static org.junit.Assert.*;

import java.sql.*;

import org.junit.*;

import com.oceanbase.jdbc.extend.datatype.INTERVALDS;
import com.oceanbase.jdbc.extend.datatype.INTERVALYM;

import oracle.jdbc.*;

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

    @Test
    public void INTERVALYMTest() {
        {
            String str = "+01233-11";
            INTERVALYM intervalym = new INTERVALYM(str);
            oracle.sql.INTERVALYM intervalym1 = new oracle.sql.INTERVALYM(str);
            System.out.println("intervalym1 = " + intervalym1);
            System.out.println("intervalym = " + intervalym);
            Assert.assertEquals(intervalym1.toString(), intervalym.toString());
        }
        {
            String str = "-1233-13";
            try {
                INTERVALYM intervalym = new INTERVALYM(str);
            } catch (Exception ex) {
                Assert.assertEquals(ex.getMessage(), "invalid month 13 in " + str);
            }
        }
    }

    @Test
    public void INTERVALDSTest() {
        {
            String str = "-111 12:11:10.1110000";
            INTERVALDS intervalds = new INTERVALDS(str);
            oracle.sql.INTERVALDS intervalds1 = new oracle.sql.INTERVALDS(str);
            Assert.assertEquals(intervalds1.toString(), intervalds.toString());
        }
    }

    /**
     * The basic test of intervalds includes query, insert, update;Due to AONE-30982102,The table contains only one interval column
     * @throws SQLException exception
     */
    @Test
    public void basicOracleIntervalds() throws SQLException {
        try {
            Connection oracleConnection = getOracleConnection();
            String sql = null;
            sql = "delete  from " + tableNameDS;
            sharedPSConnection.createStatement().execute(sql);

            try {
                oracleConnection.createStatement().execute("drop table test_intervalds");
            } catch (SQLException throwables) {
                // ignore
            }
            oracleConnection
                .createStatement()
                .execute(
                    "create table test_intervalds (c1 int primary key,c2 interval day(6) to second(5))");

            String sql1 = "insert into " + tableNameDS + "  values(?, ?)";

            PreparedStatement pstmt = sharedPSConnection.prepareStatement(sql1);
            pstmt.setInt(1, 1);
            ((BasePrepareStatement) pstmt).setINTERVALDS(2, new INTERVALDS("2324 5:12:10.0212"));
            pstmt.execute();
            String sql2 = "insert into " + tableNameDS + " values(2, '-122324 5:12:10.22222222')";
            pstmt = sharedPSConnection.prepareStatement(sql2);
            pstmt.execute();
            String sql3 = "select c1, c2 from " + tableNameDS;
            pstmt = sharedPSConnection.prepareStatement(sql3);
            pstmt.execute();
            ResultSet resultSet = pstmt.getResultSet();

            PreparedStatement oraclePstmt = oracleConnection.prepareStatement(sql1);
            oraclePstmt.setInt(1, 1);
            ((OraclePreparedStatement) oraclePstmt).setINTERVALDS(2, new oracle.sql.INTERVALDS(
                "2324 5:12:10.0212"));
            oraclePstmt.execute();
            oraclePstmt = oracleConnection.prepareStatement(sql2);
            oraclePstmt.execute();
            oraclePstmt = oracleConnection.prepareStatement(sql3);
            oraclePstmt.execute();
            ResultSet oracleResultSet = oraclePstmt.getResultSet();

            assertTrue(resultSet.next() && oracleResultSet.next());
            assertEquals(oracleResultSet.getInt("c1"), resultSet.getInt("c1"));
            assertEquals(oracleResultSet.getString("c2"), resultSet.getString("c2"));
            assertTrue(resultSet.next() && oracleResultSet.next());
            assertEquals(oracleResultSet.getInt("c1"), resultSet.getInt("c1"));
            assertEquals(oracleResultSet.getString("c2"), resultSet.getString("c2"));
            resultSet.close();
            // test for update and query

            sql = "update " + tableNameDS + " set c2 = ? where c1 = 1";
            pstmt = sharedConnection.prepareStatement(sql);
            ((BasePrepareStatement) pstmt).setINTERVALDS(1, new INTERVALDS("1234 5:12:10.212"));
            pstmt.execute();
            oraclePstmt = oracleConnection.prepareStatement(sql);
            ((OraclePreparedStatement) oraclePstmt).setINTERVALDS(1, new oracle.sql.INTERVALDS(
                "1234 5:12:10.212"));
            oraclePstmt.execute();

            sql = "select c2 from " + tableNameDS + " where c1 = 1";
            pstmt = sharedPSConnection.prepareStatement(sql);
            pstmt.execute();
            resultSet = pstmt.getResultSet();

            oraclePstmt = oracleConnection.prepareStatement(sql);
            oraclePstmt.execute();
            oracleResultSet = oraclePstmt.getResultSet();

            assertTrue(resultSet.next() && oracleResultSet.next());
            assertEquals(oracleResultSet.getString("c2"), resultSet.getString("c2"));
            resultSet.close();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    /**
     * Constructor Summary:
     * INTERVALDS()
     * INTERVALDS(byte[] intervalDS)
     * INTERVALDS(String str)
     */

    @Ignore
    public void constructorOracleIntervalds() throws SQLException {
        try {
            Connection oracleConnection = getOracleConnection();
            System.out.println("start !!!!!!");
            String sql = null;
            sql = "delete  from " + tableNameDS;
            sharedPSConnection.createStatement().execute(sql);
            try {
                oracleConnection.createStatement().execute("drop table test_intervalds");
            } catch (SQLException throwables) {
                // ignore
            }
            oracleConnection
                .createStatement()
                .execute(
                    "create table test_intervalds (c1 int primary key,c2 interval day(6) to second(5))");

            String intervalStr = "2324 5:12:10.21211111";
            sql = "insert into " + tableNameDS + "  values(?,?)";
            PreparedStatement pstmt = sharedPSConnection.prepareStatement(sql);
            pstmt.setInt(1, 1);
            INTERVALDS tmp = new INTERVALDS();
            ((BasePrepareStatement) pstmt).setINTERVALDS(2, new INTERVALDS()); //
            pstmt.addBatch();
            pstmt.setInt(1, 2);
            INTERVALDS val = new INTERVALDS(intervalStr);
            ((BasePrepareStatement) pstmt).setINTERVALDS(2, new INTERVALDS(val.getBytes()));
            pstmt.addBatch();
            pstmt.setInt(1, 3);
            ((BasePrepareStatement) pstmt).setINTERVALDS(2, new INTERVALDS(intervalStr));
            pstmt.addBatch();
            pstmt.executeBatch();

            PreparedStatement oraclePstmt = oracleConnection.prepareStatement(sql);
            oraclePstmt.setInt(1, 1);
            ((OraclePreparedStatement) oraclePstmt).setINTERVALDS(2, new oracle.sql.INTERVALDS());
            oraclePstmt.addBatch();

            oracle.sql.INTERVALDS oracleVal = new oracle.sql.INTERVALDS(intervalStr);
            oraclePstmt.setInt(1, 2);
            ((OraclePreparedStatement) oraclePstmt).setINTERVALDS(2, new oracle.sql.INTERVALDS(
                oracleVal.toBytes()));
            oraclePstmt.addBatch();

            oraclePstmt.setInt(1, 3);
            ((OraclePreparedStatement) oraclePstmt).setINTERVALDS(2, new oracle.sql.INTERVALDS(
                intervalStr));
            oraclePstmt.addBatch();
            oraclePstmt.executeBatch();

            sql = "select c1, c2 from " + tableNameDS;
            pstmt = sharedPSConnection.prepareStatement(sql);
            pstmt.execute();
            ResultSet resultSet = pstmt.getResultSet();

            oraclePstmt = oracleConnection.prepareStatement(sql);
            oraclePstmt.execute();
            ResultSet oracleResultSet = oraclePstmt.getResultSet();

            assertTrue(resultSet.next() && oracleResultSet.next());
            assertEquals(1, resultSet.getInt("c1"));
            //            assertEquals(oracleResultSet.getString("c2"), resultSet.getString("c2"));

            assertTrue(resultSet.next() && oracleResultSet.next());
            assertEquals(2, resultSet.getInt("c1"));
            assertEquals(oracleResultSet.getString("c2"), resultSet.getString("c2"));

            assertTrue(resultSet.next() && oracleResultSet.next());
            assertEquals(3, resultSet.getInt("c1"));
            assertEquals(oracleResultSet.getString("c2"), resultSet.getString("c2"));

        } catch (Exception e) {
            e.printStackTrace();
        }

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
        String interval = "1 1:1:2.222";
        INTERVALDS intervalds = new INTERVALDS(interval);
        oracle.sql.INTERVALDS intervalds1 = new oracle.sql.INTERVALDS(interval);
        assertTrue(intervalds.isConvertibleTo(String.class));
        assertEquals(intervalds1.toString(), intervalds.toString());
        byte[] bdata = INTERVALDS.toBytes(interval);
        assertEquals(intervalds1.toString(), INTERVALDS.toString(bdata));
        assertArrayEquals(INTERVALDS.toBytes(interval), INTERVALDS.toBytes(interval));

        interval = "01 1:1:2.222";
        intervalds = new INTERVALDS(interval);
        intervalds1 = new oracle.sql.INTERVALDS(interval);
        assertTrue(intervalds.isConvertibleTo(String.class));
        System.out.println("intervalds.toString() = " + intervalds.toString());
        assertEquals(intervalds1.toString(), intervalds.toString());
        bdata = INTERVALDS.toBytes(interval);
        assertEquals(intervalds1.toString(), INTERVALDS.toString(bdata));
        assertArrayEquals(INTERVALDS.toBytes(interval), INTERVALDS.toBytes(interval));

        interval = "100 1:1:2.222";
        intervalds = new INTERVALDS(interval);
        intervalds1 = new oracle.sql.INTERVALDS(interval);
        assertTrue(intervalds.isConvertibleTo(String.class));
        assertEquals(intervalds1.toString(), intervalds.toString());
        bdata = INTERVALDS.toBytes(interval);
        assertEquals(intervalds1.toString(), INTERVALDS.toString(bdata));
        assertArrayEquals(INTERVALDS.toBytes(interval), INTERVALDS.toBytes(interval));

        interval = "100 01:1:2.222";
        intervalds = new INTERVALDS(interval);
        intervalds1 = new oracle.sql.INTERVALDS(interval);
        System.out.println("intervalds = " + intervalds.toString());
        assertTrue(intervalds.isConvertibleTo(String.class));
        assertEquals(intervalds1.toString(), intervalds.toString());
        bdata = INTERVALDS.toBytes(interval);
        assertEquals(intervalds1.toString(), INTERVALDS.toString(bdata));
        assertArrayEquals(INTERVALDS.toBytes(interval), INTERVALDS.toBytes(interval));

        interval = "100 00:1:2.222";
        intervalds = new INTERVALDS(interval);
        intervalds1 = new oracle.sql.INTERVALDS(interval);
        System.out.println("intervalds = " + intervalds.toString());
        assertTrue(intervalds.isConvertibleTo(String.class));
        assertEquals(intervalds1.toString(), intervalds.toString());
        bdata = INTERVALDS.toBytes(interval);
        assertEquals(intervalds1.toString(), INTERVALDS.toString(bdata));
        assertArrayEquals(INTERVALDS.toBytes(interval), INTERVALDS.toBytes(interval));

        interval = "+100 00:1:2.222";
        intervalds = new INTERVALDS(interval);
        intervalds1 = new oracle.sql.INTERVALDS(interval);
        System.out.println("intervalds = " + intervalds.toString());
        assertTrue(intervalds.isConvertibleTo(String.class));
        assertEquals(intervalds1.toString(), intervalds.toString());
        bdata = INTERVALDS.toBytes(interval);
        assertEquals(intervalds1.toString(), INTERVALDS.toString(bdata));
        assertArrayEquals(INTERVALDS.toBytes(interval), INTERVALDS.toBytes(interval));

        interval = "-100 00:1:2.0222";
        intervalds = new INTERVALDS(interval);
        intervalds1 = new oracle.sql.INTERVALDS(interval);
        System.out.println("intervalds = " + intervalds.toString());
        assertTrue(intervalds.isConvertibleTo(String.class));
        System.out.println("intervalds1 = " + intervalds1.toString());
        assertEquals(intervalds1.toString(), intervalds.toString());
        bdata = INTERVALDS.toBytes(interval);
        assertEquals(intervalds1.toString(), INTERVALDS.toString(bdata));
        assertArrayEquals(INTERVALDS.toBytes(interval), INTERVALDS.toBytes(interval));

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
        INTERVALDS unsignedIntervalds = new INTERVALDS("2324 5:12:10.012345678");
        oracle.sql.INTERVALDS oracleUnsignedIntervalds = new oracle.sql.INTERVALDS(
            "2324 5:12:10.012345678");
        System.out.println("unsignedIntervalds = " + unsignedIntervalds.toString());
        System.out.println("oracleUnsignedIntervalds.toString() = "
                           + oracleUnsignedIntervalds.toString());
        INTERVALDS negIntervalds = new INTERVALDS("-100 10:10:10.010");
        oracle.sql.INTERVALDS oracleNegIntervalds = new oracle.sql.INTERVALDS("-100 10:10:10.010");
        INTERVALDS posIntervalds = new INTERVALDS("+100 10:10:10.010");
        oracle.sql.INTERVALDS oraclePosIntervalds = new oracle.sql.INTERVALDS("+100 10:10:10.010");
        assertEquals(oracleUnsignedIntervalds.toString(), unsignedIntervalds.toString());
        assertEquals(oracleNegIntervalds.toString(), negIntervalds.toString());
        assertEquals(oraclePosIntervalds.toString(), posIntervalds.toString());

        try {
            INTERVALDS inHour = new INTERVALDS("+100 24:10:10.10");
        } catch (Exception e) {
            assertEquals("hour 24 is not valid，should not exceed 23", e.getMessage());
        }
        try {
            INTERVALDS inMinute = new INTERVALDS("+100 12:65:10.11");
        } catch (Exception e) {
            assertEquals("minute 65 is not valid，should not exceed 59", e.getMessage());
        }
        try {
            INTERVALDS inSecond = new INTERVALDS("+100 12:11:60.11");
        } catch (Exception e) {
            assertEquals("second 60 is not valid，should not exceed 59", e.getMessage());
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
        try {
            Connection oracleConnection = getOracleConnection();

            try {
                oracleConnection.createStatement().execute("drop table test_intervalym");
            } catch (SQLException throwables) {
                // ignore
            }
            oracleConnection.createStatement().execute(
                "create table test_intervalym (c1 int primary key,c2 interval year(4) to month)");

            String sql = null;
            sql = "delete from " + tableNameYM;
            sharedPSConnection.createStatement().execute(sql);

            String sql1 = "insert into " + tableNameYM + " values(?, ?)";
            PreparedStatement pstmt = sharedPSConnection.prepareStatement(sql1);
            pstmt.setInt(1, 1);
            ((BasePrepareStatement) pstmt).setINTERVALYM(2, new INTERVALYM("123-2"));
            pstmt.execute();
            String sql2 = "insert into " + tableNameYM + " values(2,'123-2')";
            pstmt = sharedPSConnection.prepareStatement(sql2);
            pstmt.execute();
            PreparedStatement oraclePstmt = oracleConnection.prepareStatement(sql1);
            oraclePstmt.setInt(1, 1);
            ((OraclePreparedStatement) oraclePstmt).setINTERVALYM(2, new oracle.sql.INTERVALYM(
                "123-2"));
            oraclePstmt.execute();
            oraclePstmt = oracleConnection.prepareStatement(sql2);
            oraclePstmt.execute();

            String sql3 = "select c1, c2 from " + tableNameYM;
            pstmt = sharedPSConnection.prepareStatement(sql3);
            pstmt.execute();
            ResultSet rs = pstmt.getResultSet();
            oraclePstmt = oracleConnection.prepareStatement(sql3);
            oraclePstmt.execute();
            ResultSet oracleResult = oraclePstmt.getResultSet();

            assertTrue(rs.next() && oracleResult.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(oracleResult.getString(2), rs.getString(2));

            assertTrue(rs.next() && oracleResult.next());
            assertEquals(2, rs.getInt(1));
            assertEquals(oracleResult.getString(2), rs.getString(2));

            // test for update and query
            sql = "update " + tableNameYM + " set c2 = ? where c1 = 1";
            pstmt = sharedPSConnection.prepareStatement(sql);
            ((BasePrepareStatement) pstmt).setINTERVALYM(1, new INTERVALYM("10-10"));
            pstmt.execute();
            oraclePstmt = oracleConnection.prepareStatement(sql);
            ((OraclePreparedStatement) oraclePstmt).setINTERVALYM(1, new oracle.sql.INTERVALYM(
                "10-10"));
            oraclePstmt.execute();

            sql = "select c2 from " + tableNameYM + " where c1 = 1";
            pstmt = sharedPSConnection.prepareStatement(sql);
            pstmt.execute();
            rs = pstmt.getResultSet();
            oraclePstmt = oracleConnection.prepareStatement(sql);
            oraclePstmt.execute();
            oracleResult = oraclePstmt.getResultSet();

            assertTrue(rs.next() && oracleResult.next());
            assertEquals(oracleResult.getString(1), rs.getString(1));
            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
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
        Assume.assumeFalse(sharedIsRewrite()); // error while rewriteBatchStatement
        try {
            Connection oracleConnection = getOracleConnection();

            try {
                oracleConnection.createStatement().execute("drop table test_intervalym");
            } catch (SQLException throwables) {
                // ignore
            }
            oracleConnection.createStatement().execute(
                "create table test_intervalym (c1 int primary key,c2 interval year(4) to month)");

            String sql = null;
            sql = "delete from " + tableNameYM;
            sharedPSConnection.createStatement().execute(sql);

            sql = "insert into " + tableNameYM + " values(?,?)";
            PreparedStatement pstmt = sharedPSConnection.prepareStatement(sql);
            pstmt.setInt(1, 1);
            ((BasePrepareStatement) pstmt).setINTERVALYM(2, new INTERVALYM());
            pstmt.addBatch();
            pstmt.setInt(1, 2);
            INTERVALYM val = new INTERVALYM("22-3");
            ((BasePrepareStatement) pstmt).setINTERVALYM(2, new INTERVALYM(val.getBytes()));
            pstmt.addBatch();
            pstmt.setInt(1, 3);
            ((BasePrepareStatement) pstmt).setINTERVALYM(2, new INTERVALYM("21-4"));
            pstmt.addBatch();
            pstmt.executeBatch();
            PreparedStatement oraclePstmt = oracleConnection.prepareStatement(sql);
            oraclePstmt.setInt(1, 1);
            ((OraclePreparedStatement) oraclePstmt).setINTERVALYM(2, new oracle.sql.INTERVALYM());
            oraclePstmt.addBatch();
            oraclePstmt.setInt(1, 2);
            oracle.sql.INTERVALYM val2 = new oracle.sql.INTERVALYM("22-3");
            ((OraclePreparedStatement) oraclePstmt).setINTERVALYM(2,
                new oracle.sql.INTERVALYM(val2.getBytes()));
            oraclePstmt.addBatch();
            oraclePstmt.setInt(1, 3);
            ((OraclePreparedStatement) oraclePstmt).setINTERVALYM(2, new oracle.sql.INTERVALYM(
                "21-4"));
            oraclePstmt.addBatch();
            oraclePstmt.executeBatch();

            sql = "select c1,c2 from " + tableNameYM;
            pstmt = sharedPSConnection.prepareStatement(sql);
            pstmt.execute();
            ResultSet resultSet = pstmt.getResultSet();
            oraclePstmt = oracleConnection.prepareStatement(sql);
            oraclePstmt.execute();
            ResultSet oracleResultSet = oraclePstmt.getResultSet();

            assertTrue(resultSet.next() && oracleResultSet.next());
            assertEquals(1, resultSet.getInt("c1"));
            assertEquals(oracleResultSet.getString("c2"), resultSet.getString("c2"));

            assertTrue(resultSet.next() && oracleResultSet.next());
            assertEquals(2, resultSet.getInt("c1"));
            assertEquals(oracleResultSet.getString("c2"), resultSet.getString("c2"));

            assertTrue(resultSet.next() && oracleResultSet.next());
            assertEquals(3, resultSet.getInt("c1"));
            assertEquals(oracleResultSet.getString("c2"), resultSet.getString("c2"));
            resultSet.close();
            oracleResultSet.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

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
        String intervalStr = "10-11";
        INTERVALYM intervalym = new INTERVALYM(intervalStr);
        oracle.sql.INTERVALYM intervalym1 = new oracle.sql.INTERVALYM(intervalStr);
        assertTrue(intervalym.isConvertibleTo(String.class));
        assertEquals(intervalym1.toString(), intervalym.toString());

        byte[] bdata = INTERVALYM.toBytes(intervalStr);
        assertEquals(intervalym1.toString(), INTERVALYM.toString(bdata));
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
        oracle.sql.INTERVALYM oracleUnsignedIntervalym = new oracle.sql.INTERVALYM("10-1");
        INTERVALYM negIntervalym = new INTERVALYM("-10-1");
        oracle.sql.INTERVALYM oracleNegIntervalym = new oracle.sql.INTERVALYM("-10-1");
        INTERVALYM posIntervalym = new INTERVALYM("+10-1");
        oracle.sql.INTERVALYM oraclePosIntervalym = new oracle.sql.INTERVALYM("+10-1");

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

    @Test
    public void testIntervalDsSecondByZero() throws SQLException {
        String tableNameDSSecondByZero = "test_intervalds_sec0";
        createTable(tableNameDSSecondByZero,"c1 INTERVAL DAY (2) TO SECOND (6),c2 INTERVAL DAY (2) TO SECOND (3),c3 INTERVAL DAY (2) TO SECOND (0)");
        Statement stmt = sharedConnection.createStatement();

        stmt.execute("INSERT INTO " + tableNameDSSecondByZero + " (c1,c2,c3) VALUES (" +
                " INTERVAL '0 00:01:00.000000000' DAY TO SECOND ," +
                " INTERVAL '0 00:01:00.000000000' DAY TO SECOND ," +
                " INTERVAL '0 00:01:00.000000000' DAY TO SECOND)");
        stmt.execute("INSERT INTO " + tableNameDSSecondByZero + " (c1,c2,c3) VALUES (" +
                " INTERVAL '0 00:01:00.000123456' DAY TO SECOND ," +
                " INTERVAL '0 00:01:00.0123' DAY TO SECOND," +
                " INTERVAL '0 00:01:00.111' DAY TO SECOND)");

        ResultSet rs = stmt.executeQuery("select * from  " + tableNameDSSecondByZero);
        Assert.assertTrue(rs.next());
        Assert.assertEquals("0 0:1:0.0",rs.getString(1));
        Assert.assertEquals("0 0:1:0.0",rs.getString(2));
        Assert.assertEquals("0 0:1:0.0",rs.getString(3));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("0 0:1:0.000123",rs.getString(1));
        Assert.assertEquals("0 0:1:0.012",rs.getString(2));
        Assert.assertEquals("0 0:1:0.0",rs.getString(3));
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
