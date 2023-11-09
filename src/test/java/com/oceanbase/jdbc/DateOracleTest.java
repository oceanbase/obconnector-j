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
import static org.junit.Assert.fail;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class DateOracleTest extends BaseOracleTest {

    private static final String TIMESTAMP_1         = "2015-05-13 08:15:14";
    private static final String TIMESTAMP_2         = "2015-05-13 08:15:14";
    private static final String TIMESTAMP_YEAR_ZERO = "0000-11-15 10:15:22";

    /**
     * Initialization.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {

        createTable("dtest", "d date");
        createTable(
            "date_test2",
            "id int not null primary key , d_from timestamp ,d_to timestamp ,timestamp_local TIMESTAMP WITH LOCAL TIME ZONE ,timestamp_timezone TIMESTAMP WITH TIME ZONE");
        createTable("timestampzerotest", "ts timestamp, dt timestamp, dd date");
        createTable("dtest", "d timestamp");
        createTable("dtest2", "d date");
        createTable("dtest3", "d date");
        //        createTable("dtest4", "d  time");
        createTable("date_test3", " x date");
        createTable("date_test4", "x date");
        if (doPrecisionTest) {
            createTable("timestampAsDate", "ts timestamp(6), dt timestamp(6), dd date");
        }
    }

    @Test
    public void dateTestLegacy() throws SQLException {
        dateTest(true);
    }

    @Test
    public void dateTestWithoutLegacy() throws SQLException {
        dateTest(false);
    }

    /**
     * Date testing.
     *
     * @param useLegacy use legacy client side timezone or server side timezone.
     * @throws SQLException exception
     */
    public void dateTest(boolean useLegacy) throws SQLException {
        Assume.assumeFalse(sharedIsRewrite());
        try (Connection connection =
                     setConnection(
                             "&useLegacyDatetimeCode="
                                     + useLegacy
                                     + "&serverTimezone=+5:00&maximizeMysqlCompatibility=false&useServerPrepStmts=true")) {

            setSessionTimeZone(connection, "+5:00");
            createTable(
                    "date_test",
                    "id int not null primary key, d_test date,dt_test timestamp , timestamp_local TIMESTAMP WITH LOCAL TIME ZONE ,timestamp_timezone TIMESTAMP WITH TIME ZONE " );
            Statement stmt = connection.createStatement();
            Date date = Date.valueOf("2009-01-17");
            Timestamp timestamp = Timestamp.valueOf("2009-01-17 15:41:01");
            Timestamp localTimestamp = Timestamp.valueOf(LocalDateTime.now()) ;
            ZonedDateTime zonedDateTime = ZonedDateTime.of(LocalDateTime.of(2021,3,23,18,23,23,123456), ZoneId.of("Europe/Paris")) ;
            System.out.println("zonedDateTime: "+zonedDateTime);
            String str = new StringBuilder().append(zonedDateTime.getYear()).append("-").append(zonedDateTime.getMonthValue()<10 ? "0"+zonedDateTime.getMonthValue() : zonedDateTime.getMonthValue()).append("-").append(zonedDateTime.getDayOfMonth()).append(" ")
                    .append(zonedDateTime.getHour()).append(":").append(zonedDateTime.getMinute()).append(":").append(zonedDateTime.getSecond()).append(".").append(zonedDateTime.getNano()).toString() ;
            System.out.println("datestr: "+str);
            Timestamp timestamp1 = Timestamp.valueOf(str) ;
            PreparedStatement ps =
                    connection.prepareStatement(
                            "insert into date_test (id,d_test, dt_test, timestamp_local,timestamp_timezone) " + "values (1,?,?,?,?)");
            ps.setDate(1, date);
            ps.setTimestamp(2, timestamp);
            ps.setTimestamp(3,localTimestamp);
            ps.setTimestamp(4,timestamp1);
            ps.executeUpdate();
            ResultSet rs = stmt.executeQuery("select id, d_test, dt_test,timestamp_local,timestamp_timezone from date_test");
            assertEquals(true, rs.next());
            Date date2 = rs.getDate(2);
            Date date3 = rs.getDate("d_test");
            assertEquals(date.toString(), date2.toString());
            assertEquals(date.toString(), date3.toString());
            Timestamp timestamp2 = rs.getTimestamp(3);
            assertEquals(timestamp.toString(), timestamp2.toString());
            Timestamp timestamp3 = rs.getTimestamp("dt_test");
            assertEquals(timestamp.toString(), timestamp3.toString());

            Timestamp timestamp4 = rs.getTimestamp(4) ;
            Timestamp timestamp5 = rs.getTimestamp("timestamp_local") ;
            assertEquals(localTimestamp.toString(),timestamp4.toString());
            assertEquals(localTimestamp.toString(),timestamp5.toString());

            Timestamp timestamp6 = rs.getTimestamp(5) ;
            Timestamp timestamp7 = rs.getTimestamp("timestamp_timezone") ;
            Assert.assertFalse(zonedDateTime.toString().equals(timestamp6));
            Assert.assertFalse(zonedDateTime.toString().equals(timestamp7));
        }
    }

    @Test
    public void dateRangeTest() throws SQLException {
        PreparedStatement ps = sharedConnection
            .prepareStatement("insert into date_test2 (id, d_from, d_to) values " + "(1, ?,?)");
        Timestamp timestamp1 = Timestamp.valueOf("2009-01-17 15:41:01");
        Timestamp timestamp2 = Timestamp.valueOf("2015-01-17 15:41:01");
        ps.setTimestamp(1, timestamp1);
        ps.setTimestamp(2, timestamp2);
        ps.executeUpdate();
        PreparedStatement ps1 = sharedConnection
            .prepareStatement("select d_from, d_to from date_test2 "
                              + "where d_from <= ? and d_to >= ?");
        Timestamp timestamp3 = Timestamp.valueOf("2014-01-17 15:41:01");
        ps1.setTimestamp(1, timestamp3);
        ps1.setTimestamp(2, timestamp3);
        ResultSet rs = ps1.executeQuery();
        assertEquals(true, rs.next());
        Timestamp ts1 = rs.getTimestamp(1);
        Timestamp ts2 = rs.getTimestamp(2);
        assertEquals(ts1.toString(), timestamp1.toString());
        assertEquals(ts2.toString(), timestamp2.toString());
    }

    @Test(expected = SQLException.class)
    public void dateTest2() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        ResultSet rs = stmt.executeQuery("select 1");
        assertTrue(rs.next());
        rs.getDate(1);
    }

    @Test(expected = SQLException.class)
    public void dateTest3() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        ResultSet rs = stmt.executeQuery("select 1 as a");
        assertTrue(rs.next());
        rs.getDate("a");
    }

    @Test(expected = SQLException.class)
    public void timeTest3() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        ResultSet rs = stmt.executeQuery("select 'aaa' as a");
        assertTrue(rs.next());
        System.out.println(rs.getTimestamp("a"));
    }

    @Test
    public void timestampZeroTest() {
        String timestampZero = "0000-00-00 00:00:00";
        String dateZero = "0000-00-00";
        try {
            sharedConnection.createStatement().execute(
                "insert into timestampzerotest values ('" + timestampZero + "', '" + timestampZero
                        + "', '" + dateZero + "')");
        } catch (Exception e) {
            Assert.assertTrue("", e.getMessage().contains("day of month must be between 1 and last day of month"));
        }

    }

    @Test
    public void timestampAsDate() throws SQLException {
        Assume.assumeTrue(doPrecisionTest);

        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        Calendar cal2 = Calendar.getInstance();
        cal2.set(Calendar.YEAR, 1970);
        cal2.set(Calendar.MONTH, 0);
        cal2.set(Calendar.DAY_OF_YEAR, 1);

        Calendar cal3 = Calendar.getInstance();
        cal3.set(Calendar.HOUR_OF_DAY, 0);
        cal3.set(Calendar.MINUTE, 0);
        cal3.set(Calendar.SECOND, 0);
        cal3.set(Calendar.MILLISECOND, 0);
        cal3.set(Calendar.YEAR, 1970);
        cal3.set(Calendar.MONTH, 0);
        cal3.set(Calendar.DAY_OF_YEAR, 1);

        Timestamp currentTimeStamp = new Timestamp(System.currentTimeMillis());
        PreparedStatement preparedStatement1 = sharedConnection
            .prepareStatement("/*CLIENT*/ insert into timestampAsDate values (?, ?, ?)");
        preparedStatement1.setTimestamp(1, currentTimeStamp);
        preparedStatement1.setTimestamp(2, currentTimeStamp);
        preparedStatement1.setDate(3, new Date(currentTimeStamp.getTime()));
        System.out.println("date: " + new Date(currentTimeStamp.getTime()));
        preparedStatement1.addBatch();
        preparedStatement1.execute();
        //        preparedStatement1.executeBatch();

        Date dateWithoutTime = new Date(cal.getTimeInMillis());

        ResultSet rs = sharedConnection.createStatement().executeQuery(
            "select * from timestampAsDate");
        checkResult(rs, currentTimeStamp, cal, dateWithoutTime);

        PreparedStatement pstmt = sharedConnection
            .prepareStatement("select * from timestampAsDate where 1 = ?");
        pstmt.setInt(1, 1);
        pstmt.addBatch();
        rs = pstmt.executeQuery();
        checkResult(rs, currentTimeStamp, cal, dateWithoutTime);
    }

    private void checkResult(ResultSet rs, Timestamp currentTimeStamp, Calendar cal,
                             Date dateWithoutTime) throws SQLException {
        if (rs.next()) {
            assertEquals(rs.getTimestamp(1), currentTimeStamp);
            assertEquals(rs.getTimestamp(2), currentTimeStamp);
            Assert.assertFalse("is not equals as with hours minutes and seconds", rs
                .getTimestamp(3).equals(new Timestamp(cal.getTimeInMillis())));

            Assert.assertTrue(rs.getDate(1).toString()
                .equals(new Date(currentTimeStamp.getTime()).toString()));
            Assert.assertTrue(rs.getDate(2).toString()
                .equals(new Date(currentTimeStamp.getTime()).toString()));
            Assert.assertTrue(rs.getDate(3).toString().equals(dateWithoutTime.toString()));
            Assert.assertTrue(rs.getTime(1).toString()
                .equals(new Time(currentTimeStamp.getTime()).toString()));
            Assert.assertTrue(rs.getTime(2).toString()
                .equals(new Time(currentTimeStamp.getTime()).toString()));
            Assert.assertTrue("read Time from a Types.DATE field", rs.getTime(3).toString()
                .contains(":"));
        } else {
            fail("Must have a result");
        }
        rs.close();
    }

    @Test
    public void javaUtilDateInPreparedStatementAsTimeStamp() throws Exception {
        java.util.Date currentDate = Calendar.getInstance(TimeZone.getDefault()).getTime();
        PreparedStatement ps = sharedConnection.prepareStatement("insert into dtest values(?)");
        ps.setObject(1, currentDate, Types.TIMESTAMP);
        ps.executeUpdate();
        ResultSet rs = sharedConnection.createStatement().executeQuery("select * from dtest");
        assertTrue(rs.next());
        /* Check that time is correct, up to seconds precision */
        assertTrue(Math.abs((currentDate.getTime() - rs.getTimestamp(1).getTime())) <= 1000);
    }

    @Test
    public void nullTimestampTest() throws SQLException {
        PreparedStatement ps = sharedConnection.prepareStatement("insert into dtest2 values(null)");
        ps.executeUpdate();
        ResultSet rs = sharedConnection.createStatement().executeQuery(
            "select * from dtest2 where d is null");
        assertTrue(rs.next());
        Calendar cal = new GregorianCalendar();
        assertEquals(null, rs.getTimestamp(1, cal));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void javaUtilDateInPreparedStatementAsDate() throws Exception {
        java.util.Date currentDate = Calendar.getInstance(TimeZone.getDefault()).getTime();
        PreparedStatement ps = sharedConnection.prepareStatement("insert into dtest3 values(?)");
        ps.setObject(1, currentDate, Types.DATE);
        ps.executeUpdate();
        ResultSet rs = sharedConnection.createStatement().executeQuery("select * from dtest3");
        assertTrue(rs.next());
        /* Check that time is correct, up to seconds precision */
        assertEquals(currentDate.getYear(), rs.getDate(1).getYear());
        assertEquals(currentDate.getMonth(), rs.getDate(1).getMonth());
        assertEquals(currentDate.getDay(), rs.getDate(1).getDay());
    }

    /**
     * Conj-107.
     *
     * @throws SQLException exception
     */
    @Test
    public void timestampMillisecondsTest() throws SQLException {
        Statement statement = sharedConnection.createStatement();
        createTable("tt1", "id decimal(10), create_time timestamp(6)");
        statement
            .execute("INSERT INTO tt1 (id, create_time) VALUES (1,'2013-07-18 13:44:22.123456')");
        PreparedStatement ps = sharedConnection
            .prepareStatement("insert into tt1 (id, create_time) values (?,?)");
        ps.setInt(1, 2);
        Timestamp writeTs = new Timestamp(1273017612999L);
        Timestamp writeTsWithoutMilliSec = new Timestamp(1273017612999L);
        ps.setTimestamp(2, writeTs);
        ps.execute();
        ResultSet rs = statement.executeQuery("SELECT * FROM tt1");

        assertTrue(rs.next());
        assertTrue("2013-07-18 13:44:22.123456".equals(rs.getString(2)));

        assertTrue(rs.next());
        Timestamp readTs = rs.getTimestamp(2);
        assertEquals(writeTs, readTs);
        assertEquals(writeTs, writeTsWithoutMilliSec);
    }

    @Test
    public void dateTestWhenServerDifference() throws Throwable {
        try (Connection connection = setConnection("&serverTimezone=UTC")) {
            try (PreparedStatement pst =
                         connection.prepareStatement("insert into date_test3 values (?)")) {
                Date date = Date.valueOf("2013-02-01");
                pst.setDate(1, date);
                pst.execute();

                try (PreparedStatement pst2 =
                             connection.prepareStatement("select x from date_test3 WHERE x = ?")) {
                    pst2.setDate(1, date);
                    try (ResultSet rs = pst2.executeQuery()) {
                        assertTrue(rs.next());
                        Date dd = rs.getDate(1);
                        Assert.assertEquals("dd is not equals to date dd: "+dd ,dd, date);
                    }
                }
            }
        }
    }

    @Test
    public void dateTestWhenServerDifferenceClient() throws Throwable {
        try (Connection connection = setConnection("&serverTimezone=UTC")) {
            try (PreparedStatement pst =
                         connection.prepareStatement("/*CLIENT*/insert into date_test4 values (?)")) {
                Date date = Date.valueOf("2013-02-01");
                pst.setDate(1, date);
                pst.execute();

                try (PreparedStatement pst2 =
                             connection.prepareStatement("/*CLIENT*/ select x from date_test4 WHERE x = ?")) {
                    pst2.setDate(1, date);
                    try (ResultSet rs = pst2.executeQuery()) {
                        assertTrue(rs.next());
                        Date dd = rs.getDate(1);
                        Assert.assertEquals("dd is not equals to date dd: "+dd ,dd, date);
                    }
                }
            }
        }
    }

    @Test
    public void dateMinusDateTest() throws SQLException {
        String querySql = "select to_char(( ? - to_date('1970-01-01 08:00:00 ' , 'YYYY-MM-DD HH24:MI:SS') )*24*60*60*1000) CAL_MILLISECONDS FROM DUAL";
        PreparedStatement pst = sharedConnection.prepareStatement(querySql);
        Date date = new Date(System.currentTimeMillis());
        pst.setDate(1,date);
        ResultSet rs = pst.executeQuery();
        rs.next();

        try (Connection connection = setConnection("&useServerPrepStmts=true")) {
            try (PreparedStatement pstByPs =
                         connection.prepareStatement(querySql)) {
                pstByPs.setDate(1, date);
                ResultSet rsByPs = pstByPs.executeQuery();
                rsByPs.next();
                Assert.assertEquals(rs.getString(1),rsByPs.getString(1));
            }
        }
    }

    @Test
    public void testTimeTypeToStringPs() {
        try {
            ResultSet rs = sharedConnection.prepareStatement(
                "select to_date('2022-02-02 15:30:20','yyyy-MM-DD hh24:MI:SS') from dual")
                .executeQuery();
            Assert.assertTrue(rs.next());
            Assert.assertEquals("2022-02-02 15:30:20", rs.getString(1));//2022-02-02 15:30:20.0
            rs = sharedConnection
                .prepareStatement(
                    "select to_timestamp('2022-02-02 15:30:20.000000','yyyy-MM-DD hh24:MI:SS.FF') from dual")
                .executeQuery();
            Assert.assertTrue(rs.next());
            Assert.assertEquals("2022-02-02 15:30:20", rs.getString(1));
            rs = sharedConnection
                .prepareStatement(
                    "select to_timestamp('2022-02-02 15:30:20.123000','yyyy-MM-DD hh24:MI:SS.FF') from dual")
                .executeQuery();
            Assert.assertTrue(rs.next());
            Assert.assertEquals("2022-02-02 15:30:20.123", rs.getString(1));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testTimestampParamOnDateIndex() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        createTable("test_date_index","v0 date");
        Date d = new Date(System.currentTimeMillis());
        Timestamp t = new Timestamp(d.getTime());
        t.setNanos(0);
        try {
            stmt.execute("create index test_timestamp_to_date_index on test_date_index(v0)");
        } catch (SQLException e) {
        }
        PreparedStatement pstmt = sharedConnection.prepareStatement("insert into test_date_index values (?)");
        pstmt.setDate(1,d);
        pstmt.execute();
        pstmt = sharedConnection.prepareStatement("select * from test_date_index where v0 = ?");
        pstmt.setTimestamp(1,t);
        ResultSet rs = pstmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(t,rs.getTimestamp(1));
        assertEquals(d.toString(),rs.getDate(1).toString());
        pstmt = sharedConnection.prepareStatement("explain select * from test_date_index where v0 = ?");
        pstmt.setTimestamp(1,t);
        rs = pstmt.executeQuery();
        assertTrue(rs.next());
        assertTrue(rs.getString("Query Plan").contains("TEST_DATE_INDEX(TEST_TIMESTAMP_TO_DATE_INDEX)"));
    }

    @Test
    public void testTimestampParamCaseToDateParamOnTimestampIndex() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        createTable("test_timestamp_index", "v0 timestamp");
        Date d = new Date(System.currentTimeMillis());
        Timestamp t = new Timestamp(d.getTime());
        t.setNanos(0);
        try {
            stmt.execute("create index test_timestamp_on_timestamp_index on test_timestamp_index(v0)");
        } catch (SQLException e) {
        }
        PreparedStatement pstmt = sharedConnection.prepareStatement("insert into test_timestamp_index values (?)");
        pstmt.setDate(1,d);
        pstmt.execute();
        pstmt = sharedConnection.prepareStatement("select * from test_timestamp_index where v0 = ?");
        pstmt.setTimestamp(1,t);
        ResultSet rs = pstmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(t,rs.getTimestamp(1));
        assertEquals(d.toString(),rs.getDate(1).toString());
        pstmt = sharedConnection.prepareStatement("explain select * from test_timestamp_index where v0 = ?");
        pstmt.setTimestamp(1,t);
        rs = pstmt.executeQuery();
        assertTrue(rs.next());
        assertTrue(rs.getString("Query Plan").contains("TEST_TIMESTAMP_INDEX(TEST_TIMESTAMP_ON_TIMESTAMP_INDEX)"));
    }

    @Test
    public void testIgnoreHourMinuteSecond() throws SQLException {
        Timestamp timestamp = Timestamp.valueOf("2023-10-10 11:11:11.111111111");
        Date date = new Date(timestamp.getTime());
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);

        Connection conn = sharedConnection;
        createTable("dtest5", "d date, t timestamp");
        Statement stmt = conn.createStatement();

        PreparedStatement pstmt = conn.prepareStatement("insert into dtest5 (d, t) values (?, ?)");
        pstmt.setDate(1, date);
        pstmt.setTimestamp(2, timestamp);
        pstmt.execute();
        pstmt.setTimestamp(1, timestamp);
        pstmt.setDate(2, date);
        pstmt.execute();
        ResultSet rs = null;

        rs = stmt.executeQuery("select d, t from dtest5");
        assertTrue(rs.next());
        assertEquals(new Date(Timestamp.valueOf("2023-10-10 00:00:00.0").getTime()), rs.getDate(1));
        assertEquals(new Date(Timestamp.valueOf("2023-10-10 00:00:00.0").getTime()), rs.getDate(2));
        assertEquals(Timestamp.valueOf("2023-10-10 00:00:00.0"), rs.getTimestamp(1));
        assertEquals(Timestamp.valueOf("2023-10-10 11:11:11.111111"), rs.getTimestamp(2));
        assertEquals("2023-10-10 00:00:00.0", rs.getString(1));
        assertEquals("2023-10-10 11:11:11.111111", rs.getString(2));

        assertTrue(rs.next());
        assertEquals(new Date(Timestamp.valueOf("2023-10-10 00:00:00.0").getTime()), rs.getDate(1));
        assertEquals(new Date(Timestamp.valueOf("2023-10-10 00:00:00.0").getTime()), rs.getDate(2));
        assertEquals(Timestamp.valueOf("2023-10-10 11:11:11.0"), rs.getTimestamp(1));
        assertEquals(Timestamp.valueOf("2023-10-10 00:00:00.0"), rs.getTimestamp(2));
        assertEquals("2023-10-10 11:11:11.0", rs.getString(1));
        assertEquals("2023-10-10 00:00:00", rs.getString(2));
        rs.close();
        pstmt.close();

        conn = setConnection("&compatibleOjdbcVersion=8");
        stmt = conn.createStatement();
        stmt.execute("delete from dtest5");

        pstmt = conn.prepareStatement("insert into dtest5 (d, t) values (?, ?)");
        pstmt.setDate(1, date);
        pstmt.setTimestamp(2, timestamp);
        pstmt.execute();
        pstmt.setTimestamp(1, timestamp);
        pstmt.setDate(2, date);
        pstmt.execute();

        rs = stmt.executeQuery("select d, t from dtest5");
        assertTrue(rs.next());
        assertEquals(new Date(Timestamp.valueOf("2023-10-10 11:11:11.0").getTime()), rs.getDate(1));
        assertEquals(new Date(Timestamp.valueOf("2023-10-10 11:11:11.0").getTime()), rs.getDate(2));
        assertEquals(Timestamp.valueOf("2023-10-10 11:11:11.0"), rs.getTimestamp(1));
        assertEquals(Timestamp.valueOf("2023-10-10 11:11:11.111111"), rs.getTimestamp(2));
        assertEquals("2023-10-10 11:11:11", rs.getString(1));
        assertEquals("2023-10-10 11:11:11.111111", rs.getString(2));

        assertTrue(rs.next());
        assertEquals(new Date(Timestamp.valueOf("2023-10-10 11:11:11.0").getTime()), rs.getDate(1));
        assertEquals(new Date(Timestamp.valueOf("2023-10-10 11:11:11.0").getTime()), rs.getDate(2));
        assertEquals(Timestamp.valueOf("2023-10-10 11:11:11.0"), rs.getTimestamp(1));
        assertEquals(Timestamp.valueOf("2023-10-10 11:11:11.0"), rs.getTimestamp(2));
        assertEquals("2023-10-10 11:11:11", rs.getString(1));
        assertEquals("2023-10-10 11:11:11", rs.getString(2));
        rs.close();
        pstmt.close();
    }
}
