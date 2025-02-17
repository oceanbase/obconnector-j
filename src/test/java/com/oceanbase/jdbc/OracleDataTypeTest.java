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
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 * This particular MariaDB Client for Java file is work
 * derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 * the following copyright and notice provisions:
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
 */
package com.oceanbase.jdbc;

import static org.junit.Assert.*;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.*;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.oceanbase.jdbc.extend.datatype.TIMESTAMPLTZ;
import com.oceanbase.jdbc.internal.com.read.resultset.SelectResultSet;

public class OracleDataTypeTest extends BaseOracleTest {

    private static PreparedStatement ps   = null;
    private static final Connection  conn = sharedConnection;

    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("t_number", "n number(6,2)");
        createTable("t_binary_float", "bf binary_float");
        createTable("t_binary_double", "bd binary_double");
        createTable("t_char", "c char(10)");
        createTable("t_nchar", "nc nchar(10)");
        createTable("t_varchar2", "v varchar2(20)");
        createTable("t_varchar2_charset", "v varchar2(20)");
        createTable("t_nvarchar2", "nv nvarchar2(20)");
        createTable("t_nvarchar2_charset", "nv nvarchar2(20)");
        createTable("t_date", "d date");
        createTable("t_timestamp", "t timestamp");
        createTable("t_date_timestamp", "id number, d date, t timestamp");
        createTable("t_timestamp_with_timezone", "twtz timestamp with time zone");
        createTable("t_timestamp_with_local_timezone", "twltz timestamp with local time zone");
        createTable("t_interval_year_to_month", "ym interval year(3) to month");
        createTable("t_interval_year_to_month2", "ym interval year(0) to month");
        createTable("t_interval_day_to_second", "ds interval day(3) to second");
        createTable("t_nested_array", "id1 number, id2 number");
    }

    @Test
    public void testNumber() throws SQLException {
        String sql = "insert into t_number  values (1), (-1), (1000.556), (1000.554), (1000.5554)";
        assertEquals(5, conn.createStatement().executeUpdate(sql));
        ps = conn.prepareStatement("select * from t_number");
        ResultSet rs = ps.executeQuery();
        BigDecimal[] decimals = new BigDecimal[] { BigDecimal.valueOf(1), BigDecimal.valueOf(-1),
                BigDecimal.valueOf(1000.56), BigDecimal.valueOf(1000.55),
                BigDecimal.valueOf(1000.56) };
        int i = 0;
        while (rs.next()) {
            assertEquals(decimals[i], rs.getBigDecimal(1));
            i++;
        }
        assertEquals("java.math.BigDecimal", rs.getMetaData().getColumnClassName(1));
        assertEquals("NUMBER", rs.getMetaData().getColumnTypeName(1));
        assertEquals(Types.NUMERIC, rs.getMetaData().getColumnType(1));
        rs.close();
    }

    @Test(expected = SQLException.class)
    public void testNumberPrecision() throws SQLException {
        String sql = "insert into t_number (id) values (10000.55)";
        assertEquals(1, conn.createStatement().executeUpdate(sql));
    }

    @Test
    public void testBinaryFloat() throws SQLException {
        String sql = "insert into t_binary_float (bf) values (1.9),(1.9f),(1)";
        assertEquals(3, conn.createStatement().executeUpdate(sql));
        ResultSet rs = conn.prepareStatement("select bf from t_binary_float").executeQuery();
        BigDecimal[] decimals = new BigDecimal[] { BigDecimal.valueOf(1.9f),
                BigDecimal.valueOf(1.9f), BigDecimal.valueOf(1) };
        int i = 0;
        while (rs.next()) {
            assertEquals(0, decimals[i].compareTo(rs.getBigDecimal(1)));
            i++;
        }
        assertEquals("BINARY_FLOAT", rs.getMetaData().getColumnTypeName(1));
        rs.close();
    }

    @Test
    public void testBinaryDouble() throws SQLException {
        String sql = "insert into t_binary_double (bd) values (1.9f),(1.9d),(1)";
        assertEquals(3, conn.createStatement().executeUpdate(sql));
        ResultSet rs = conn.prepareStatement("select bd from t_binary_double").executeQuery();
        BigDecimal[] decimals = new BigDecimal[] { BigDecimal.valueOf(1.9f),
                BigDecimal.valueOf(1.9d), BigDecimal.valueOf(1) };
        int i = 0;
        while (rs.next()) {
            assertEquals(0, decimals[i].compareTo(rs.getBigDecimal(1)));
            i++;
        }
        assertEquals("BINARY_DOUBLE", rs.getMetaData().getColumnTypeName(1));
        rs.close();
    }

    @Test
    public void testChar() throws SQLException {
        String sql = "insert into t_char (c) values ('oracle'), ('中文'), (null) , ('')";
        assertEquals(4, conn.createStatement().executeUpdate(sql));
        ResultSet rs = conn.prepareStatement("select c from t_char").executeQuery();
        String[] str = new String[] { "oracle", "中文", null, null };
        int i = 0;
        while (rs.next()) {
            if (rs.getString(1) != null) {
                assertTrue(rs.getString(1).contains(str[i]));
            }
            i++;
        }
        assertEquals(Types.CHAR, rs.getMetaData().getColumnType(1));
        rs = conn.createStatement().executeQuery(
            "select value from nls_database_parameters where parameter = 'NLS_CHARACTERSET'");
        Assert.assertTrue(rs.next());
        assertEquals("AL32UTF8", rs.getString(1));
        rs.close();
    }

    @Test
    public void testNchar() throws SQLException {
        String sql = "insert into t_nchar (nc) values ('oracle'), ('中文'), (null) , ('')";
        assertEquals(4, conn.createStatement().executeUpdate(sql));
        ResultSet rs = conn.prepareStatement("select nc from t_nchar").executeQuery();
        String[] str = new String[] { "oracle", "中文", null, null };
        int i = 0;
        while (rs.next()) {
            if (rs.getString(1) != null) {
                assertTrue(rs.getString(1).contains(str[i]));
            }
            i++;
        }
        assertEquals(Types.NCHAR, rs.getMetaData().getColumnType(1));
        rs = conn.createStatement().executeQuery(
            "select value from nls_database_parameters where parameter = 'NLS_NCHAR_CHARACTERSET'");
        Assert.assertTrue(rs.next());
        assertEquals("AL16UTF16", rs.getString(1));

        rs = conn.createStatement().executeQuery("select lengthb(nc) from t_nchar");
        Assert.assertTrue(rs.next());
        assertEquals(20, rs.getInt(1));
        rs.close();
    }

    @Test
    public void testVarchar2() throws SQLException {
        String sql = "insert into t_varchar2 (v) values ('oracle'), (null) , (''), ('abc中文$#%') ";
        assertEquals(4, conn.createStatement().executeUpdate(sql));
        ResultSet rs = conn.prepareStatement("select v from t_varchar2").executeQuery();
        String[] str = new String[] { "oracle", null, null, "abc中文$#%" };
        int i = 0;
        while (rs.next()) {
            assertEquals(str[i], rs.getString(1));
            i++;
        }
        assertEquals(Types.VARCHAR, rs.getMetaData().getColumnType(1));
        rs.close();
    }

    @Test
    public void testNvarchar2() throws SQLException {
        String sql = "insert into t_nvarchar2 (nv) values ('oracle'), (null) , (''), ('abc中文$#%') ";
        assertEquals(4, conn.createStatement().executeUpdate(sql));
        ResultSet rs = conn.prepareStatement("select nv from t_nvarchar2").executeQuery();
        String[] str = new String[] { "oracle", null, null, "abc中文$#%" };
        int i = 0;
        while (rs.next()) {
            assertEquals(str[i], rs.getString(1));
            i++;
        }
        assertEquals(Types.NVARCHAR, rs.getMetaData().getColumnType(1));
        rs.close();
    }

    @Test
    public void testVarchar2CharsetGB18030() throws UnsupportedEncodingException, SQLException {
        String sql = "insert into t_varchar2_charset (v) values (?) ";
        String str = "abc中文";
        String str1 = new String(str.getBytes(StandardCharsets.UTF_8), "GB18030");
        ps = conn.prepareStatement(sql);
        ps.setString(1, str1);
        assertEquals(1, ps.executeUpdate());
        ResultSet rs = conn.prepareStatement("select v from t_varchar2_charset").executeQuery();
        assertTrue(rs.next());
        assertEquals(str1, rs.getString(1));
        String str2 = new String(rs.getString(1).getBytes("GB18030"), StandardCharsets.UTF_8);
        assertEquals(str, str2);
        assertEquals(Types.VARCHAR, rs.getMetaData().getColumnType(1));
        rs.close();
    }

    @Test
    public void testVarchar2CharSetGBK() throws SQLException, UnsupportedEncodingException {
        String sql = "insert into t_varchar2_charset (v) values (?) ";
        String str = "abc中文";
        String str1 = new String(str.getBytes(StandardCharsets.UTF_8), "GBK");
        ps = conn.prepareStatement(sql);
        ps.setString(1, str1);
        assertEquals(1, ps.executeUpdate());
        ResultSet rs = conn.prepareStatement("select v from t_varchar2_charset").executeQuery();
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertEquals(str1, rs.getString(1));
        String str2 = new String(rs.getString(1).getBytes("GBK"), StandardCharsets.UTF_8);
        assertEquals(str, str2);
        assertEquals(Types.VARCHAR, rs.getMetaData().getColumnType(1));
        rs.close();
    }

    @Test
    public void testNvarchar2CharsetGB18030() throws UnsupportedEncodingException, SQLException {
        String sql = "insert into t_nvarchar2_charset (nv) values (?) ";
        String str = "abc中文";
        String str1 = new String(str.getBytes(StandardCharsets.UTF_8), "GB18030");
        ps = conn.prepareStatement(sql);
        ps.setString(1, str1);
        assertEquals(1, ps.executeUpdate());
        ResultSet rs = conn.prepareStatement("select nv from t_nvarchar2_charset").executeQuery();
        assertTrue(rs.next());
        assertEquals(str1, rs.getString(1));
        String str2 = new String(rs.getString(1).getBytes("GB18030"), StandardCharsets.UTF_8);
        assertEquals(str, str2);
        assertEquals(Types.NVARCHAR, rs.getMetaData().getColumnType(1));
        rs.close();
    }

    @Test
    public void testIntervalYear3ToMonth() throws Exception {
        Statement stmt = conn.createStatement();

        String sql1 = "insert into t_interval_year_to_month values ('0-1')";
        assertEquals(1, stmt.executeUpdate(sql1));
        String sql2 = "insert into t_interval_year_to_month values ('-0-2')";
        assertEquals(1, stmt.executeUpdate(sql2));
        String sql3 = "insert into t_interval_year_to_month values ('1-3')";
        assertEquals(1, stmt.executeUpdate(sql3));
        String sql4 = "insert into t_interval_year_to_month values ('-1-4')";
        assertEquals(1, stmt.executeUpdate(sql4));
        String sql6 = "insert into t_interval_year_to_month values (interval '123-6' year(3) to month)";
        assertEquals(1, stmt.executeUpdate(sql6));
        String sql8 = "insert into t_interval_year_to_month values (interval '8' month)";
        assertEquals(1, stmt.executeUpdate(sql8));
        try {
            String sql5 = "insert into t_interval_year_to_month values ('1234-5')";
            stmt.executeUpdate(sql5);
            fail("inserting is expected to fail");
        } catch (SQLException e) {
            // ignore, should continue this test
        }
        try {
            String sql7 = "insert into t_interval_year_to_month values (interval '321-7' year(0) to month)";
            stmt.executeUpdate(sql7);
            fail("inserting is expected to fail");
        } catch (SQLException e) {
            // ignore, should continue this test
        }

        ResultSet rs = null;
        try {
            rs = stmt.executeQuery("select ym from t_interval_year_to_month");
            String[] str = new String[] { "0-1", "-0-2", "1-3", "-1-4", "123-6", "0-8" };
            int i = 0;
            while (rs.next()) {
                assertEquals(str[i++], rs.getString(1));
            }
            assertEquals(6, i);
            assertEquals("INTERVALYM", rs.getMetaData().getColumnTypeName(1));
        } finally {
            if (rs != null) {
                rs.close();
            }
        }
    }

    @Test
    public void testIntervalYear0ToMonth() throws Exception {
        Statement stmt = conn.createStatement();

        String sql1 = "insert into t_interval_year_to_month2 values ('0-8')";
        assertEquals(1, stmt.executeUpdate(sql1));
        String sql2 = "insert into t_interval_year_to_month2 values ('-0-7')";
        assertEquals(1, stmt.executeUpdate(sql2));
        try {
            String sql3 = "insert into t_interval_year_to_month2 values ('1-6')";
            stmt.executeUpdate(sql3);
            fail("inserting is expected to fail");
        } catch (SQLException e) {
            // ignore, should continue this test
        }
        try {
            String sql4 = "insert into t_interval_year_to_month2 values ('-1-5')";
            stmt.executeUpdate(sql4);
            fail("inserting is expected to fail");
        } catch (SQLException e) {
            // ignore, should continue this test
        }
        try {
            String sql5 = "insert into t_interval_year_to_month2 values ('1234-4')";
            stmt.executeUpdate(sql5);
            fail("inserting is expected to fail");
        } catch (SQLException e) {
            // ignore, should continue this test
        }
        try {
            String sql6 = "insert into t_interval_year_to_month2 values (interval '123-3' year(3) to month)";
            stmt.executeUpdate(sql6);
            fail("inserting is expected to fail");
        } catch (SQLException e) {
            // ignore, should continue this test
        }
        try {
            String sql7 = "insert into t_interval_year_to_month2 values (interval '321-2' year(0) to month)";
            stmt.executeUpdate(sql7);
            fail("inserting is expected to fail");
        } catch (SQLException e) {
            // ignore, should continue this test
        }
        String sql8 = "insert into t_interval_year_to_month2 values (interval '1' month)";
        assertEquals(1, stmt.executeUpdate(sql8));

        ResultSet rs = null;
        try {
            rs = stmt.executeQuery("select ym from t_interval_year_to_month2");
            String[] str = new String[] { "0-8", "-0-7", "0-1" };
            int i = 0;
            while (rs.next()) {
                assertEquals(str[i++], rs.getString(1));
            }
            assertEquals(3, i);
            assertEquals("INTERVALYM", rs.getMetaData().getColumnTypeName(1));
        } finally {
            if (rs != null) {
                rs.close();
            }
        }
    }

    @Test
    public void testIntervalDayToSecond() throws SQLException {
        String sql = "insert into t_interval_day_to_second values (interval '100 15:02:23.666' day(3) to second),"
                     + "(interval '15:2:3.66' hour to second), "
                     + "(interval '15:02' minute to second)";
        assertEquals(3, conn.createStatement().executeUpdate(sql));
        ResultSet rs = conn.prepareStatement("select ds from t_interval_day_to_second")
            .executeQuery();
        String[] str = new String[] { "100 15:2:23.666", "0 15:2:3.66", "0 0:15:2.000000" };
        int i = 0;
        while (rs.next()) {
            assertEquals(str[i], rs.getString(1));
            i++;
        }
        assertEquals("INTERVALDS", rs.getMetaData().getColumnTypeName(1));
    }

    // fix it later,error with cursor resultSet oracle mode
    @Ignore
    public void testNumToInterval() throws SQLException {
        String sql = "select numtodsinterval(?,'day'), numtodsinterval(?,'hour'), numtoyminterval(?,'month') from dual";
        ps = conn.prepareStatement(sql);
        ps.setInt(1, 80);
        ps.setInt(2, 80);
        ps.setInt(3, 80);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals("INTERVALDS", rs.getMetaData().getColumnTypeName(1));
        assertEquals("INTERVALDS", rs.getMetaData().getColumnTypeName(2));
        assertEquals("INTERVALYM", rs.getMetaData().getColumnTypeName(3));
        assertEquals("80 0:0:0.000000000", rs.getString(1));
        assertEquals("3 8:0:0.000000000", rs.getString(2));
        assertEquals("6-8", rs.getString(3));
    }

    // cursorFetch error now !
    @Ignore
    public void testToInterval() throws SQLException {
        String sql = "select to_yminterval('6-8') , to_dsinterval('3 8:0:0.000000000') from dual";
        ResultSet rs = conn.createStatement().executeQuery(sql);
        assertTrue(rs.next());
        assertEquals("INTERVALYM", rs.getMetaData().getColumnTypeName(1));
        assertEquals("INTERVALDS", rs.getMetaData().getColumnTypeName(2));
        assertEquals("6-8", rs.getString(1));
        assertEquals("3 8:0:0.000000000", rs.getString(2));
    }

    @Test
    public void testDateAndTimestamp() throws SQLException {
        String sql = "insert into t_date_timestamp (id, d, t )values (1, ?,?)";
        ps = conn.prepareStatement(sql);
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        ps.setDate(1, new Date(Timestamp.valueOf("2020-10-05 21:03:15.878879").getTime()));
        ps.setTimestamp(2, timestamp);
        assertEquals(1, ps.executeUpdate());

        ResultSet rs = conn.createStatement().executeQuery("select d, t from t_date_timestamp");
        assertTrue(rs.next());
        assertEquals(Timestamp.valueOf("2020-10-05 21:03:15.0"), rs.getTimestamp(1));
        assertEquals(timestamp, rs.getTimestamp(2));
        assertEquals(Types.TIMESTAMP, rs.getMetaData().getColumnType(1));
        assertEquals(Types.TIMESTAMP, rs.getMetaData().getColumnType(2));

        sql = "update t_date_timestamp set t=? where id = 1";
        ps = conn.prepareStatement(sql);
        ps.setDate(1, new Date(Timestamp.valueOf("1997-05-01 02:56:15.0").getTime()));
        assertEquals(1, ps.executeUpdate());

        rs = conn.createStatement().executeQuery("select t from t_date_timestamp");
        assertTrue(rs.next());
        assertEquals(Timestamp.valueOf("1997-05-01 02:56:15.0"), rs.getTimestamp(1));
        rs.close();
    }

    @Test
    public void testTimestampToDate() throws SQLException {
        String sql = "select cast(? as date)  from dual";
        ps = conn.prepareStatement(sql);
        ps.setTimestamp(1, Timestamp.valueOf("1997-05-01 02:56:15.233335"));
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(Timestamp.valueOf("1997-05-01 02:56:15.0"), rs.getTimestamp(1));
        assertEquals(Types.TIMESTAMP, rs.getMetaData().getColumnType(1));

        sql = "select to_timestamp(?) from dual";
        ps = conn.prepareStatement(sql);
        ps.setTimestamp(1, Timestamp.valueOf("1997-05-01 02:56:15.233335"));
        rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(Timestamp.valueOf("1997-05-01 02:56:15.233335"), rs.getTimestamp(1));
        assertEquals(Types.TIMESTAMP, rs.getMetaData().getColumnType(1));
        rs.close();
    }

    @Test
    public void testTimestampWTZ() throws SQLException {
        String sql = "insert into t_timestamp_with_timezone (twtz) values (?)";
        Timestamp timestamp = Timestamp.valueOf("2018-12-26 16:54:19.878879");
        ps = conn.prepareStatement(sql);
        ps.setTimestamp(1, timestamp);
        assertEquals(1, ps.executeUpdate());
        ResultSet rs = conn.createStatement().executeQuery(
            "select twtz from t_timestamp_with_timezone");
        assertTrue(rs.next());
        assertEquals(timestamp, rs.getTimestamp(1));
        assertEquals("TIMESTAMP WITH TIME ZONE", rs.getMetaData().getColumnTypeName(1));
        rs.close();
    }

    @Test
    public void testTo_timestamp_tz() throws SQLException {
        String sql = "select to_timestamp_tz(?,'yyyy-mm-dd hh24:mi:ss.ff') from dual";
        ps = conn.prepareStatement(sql);
        ps.setString(1, "2020-10-05 21:03:15.233");
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        // Consistent with oracle jdbc
        assertEquals("2020-10-05 21:03:15.233 +8:00", rs.getString(1));
        assertEquals("TIMESTAMP WITH TIME ZONE", rs.getMetaData().getColumnTypeName(1));
        assertEquals("com.oceanbase.jdbc.extend.datatype.TIMESTAMPTZ", rs.getMetaData()
            .getColumnClassName(1));
        //assertEquals("oracle.sql.TIMESTAMPTZ", rs.getMetaData().getColumnClassName(1));
        rs.close();
    }

    @Test
    public void testFrom_tz() throws SQLException {
        String sql = "select from_tz ( to_timestamp ( ?),'+08:00') from dual";
        ps = conn.prepareStatement(sql);
        Timestamp timestamp = Timestamp.valueOf("2020-10-05 21:03:15.233");
        ps.setTimestamp(1, timestamp);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(timestamp, rs.getTimestamp(1));
        assertEquals("TIMESTAMP WITH TIME ZONE", rs.getMetaData().getColumnTypeName(1));
        assertEquals("com.oceanbase.jdbc.extend.datatype.TIMESTAMPTZ", rs.getMetaData()
            .getColumnClassName(1));
        rs.close();
    }

    @Test
    public void testTimestampWLTZ() throws SQLException {
        String sql = "insert into t_timestamp_with_local_timezone (twltz) values (?)";
        TIMESTAMPLTZ timestampltz = new TIMESTAMPLTZ(conn,
            Timestamp.valueOf("2018-12-26 16:54:19.878879"));
        BasePrepareStatement bps = (BasePrepareStatement) conn.prepareStatement(sql);
        bps.setTIMESTAMPLTZ(1, timestampltz);
        assertEquals(1, bps.executeUpdate());
        SelectResultSet srs = (SelectResultSet) conn.prepareStatement(
            "select twltz from t_timestamp_with_local_timezone").executeQuery();
        assertTrue(srs.next());
        assertEquals(timestampltz, srs.getTIMESTAMPLTZ(1));
        assertEquals("TIMESTAMP WITH LOCAL TIME ZONE", srs.getMetaData().getColumnTypeName(1));

        sql = "select cast(? as timestamp with local time zone)from dual";
        ps = conn.prepareStatement(sql);
        Timestamp timestamp = Timestamp.valueOf("2020-10-05 21:03:15.566");
        ps.setTimestamp(1, timestamp);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(timestamp, rs.getTimestamp(1));
        assertEquals("TIMESTAMP WITH LOCAL TIME ZONE", rs.getMetaData().getColumnTypeName(1));
        assertEquals("com.oceanbase.jdbc.extend.datatype.TIMESTAMPLTZ", rs.getMetaData()
            .getColumnClassName(1));
        //assertEquals("oracle.sql.TIMESTAMPLTZ", rs.getMetaData().getColumnClassName(1));
        rs.close();
    }

    @Test
    public void testNestedArray() throws SQLException {
        String sql = "create or replace type  t_array is table of number";
        ps = conn.prepareStatement(sql);
        assertEquals(0, ps.executeUpdate());

        sql = "create or replace procedure test_insert (a in number,b in t_array) is begin "
              + "for i in 1..b.count loop "
              + "insert into t_nested_array values (a,b(i)); end loop; end";
        assertEquals(0, conn.prepareStatement(sql).executeUpdate());
        CallableStatement cs = conn.prepareCall("{call test_insert(?, ?)}");
        cs.setInt(1, 1);
        cs.setArray(2, conn.createArrayOf("number", new Integer[] { 5, 6 }));
        cs.execute();

        sql = "create or replace procedure test_select (a in integer,b out t_array) is begin "
              + "for i in 1..b.count loop "
              + "select id2 into b(i) from t_nested_array where id1 = a; end loop; end";
        assertEquals(0, conn.prepareStatement(sql).executeUpdate());
        cs = conn.prepareCall("{call test_select(?, ?)}");
        cs.setInt(1, 1);
        cs.registerOutParameter(2, Types.ARRAY);
        cs.execute();
        // fix next version
        //        assertNotNull(cs.getArray(2).getArray());
    }

    @Test
    public void testIntNumberFloatGetString() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        try {
            stmt.execute("drop table t_20221123");
        } catch (SQLException e) {
        }
        stmt.execute("create table t_20221123 ("
                    + "    c1 int,\n"
                    + "    c2 number,\n"
                    + "    c3 FLOAT )");
        stmt.execute("insert into t_20221123 values(1.1, 2.2, 3.3)");
        stmt.execute("insert into t_20221123 values(0.1, 0.2, 0.3)");

        ResultSet rs = stmt.executeQuery("select * from t_20221123");
        assertTrue(rs.next());
        assertEquals("1", rs.getString(1));
        assertEquals("2.2", rs.getString(2));
        assertEquals("3.3", rs.getString(3));
        assertTrue(rs.next());
        assertEquals("0", rs.getString(1));
        assertEquals("0.2", rs.getString(2));
        assertEquals("0.3", rs.getString(3));
    }

    @Test
    public void testFetTimeFromCharacter() throws SQLException {
        createTable("test_get_time_from_character","v0 char(25), v1 nchar(25), v2 varchar2(50), v3 nvarchar2(50)");
        Connection conn = setConnection("&compatibleOjdbcVersion=6");
        Statement stmt = conn.createStatement();
        stmt.execute("insert into test_get_time_from_character values ('2022-12-12 10:50:30.6','2022-12-12 10:50:30.6','2022-12-12 10:50:30.6','2022-12-12 10:50:30.6')");
        stmt.execute("insert into test_get_time_from_character values ('2022-12-12','2022-12-12','2022-12-12','2022-12-12')");
        stmt.execute("insert into test_get_time_from_character values ('10:50:30','10:50:30','10:50:30','10:50:30')");

        ResultSet rs = stmt.executeQuery("select * from test_get_time_from_character");
        Timestamp timestamp = Timestamp.valueOf("2022-12-12 10:50:30.6");
        String timestampStr = "2022-12-12 10:50:30.6    ";
        String timeStr = "2022-12-12               ";
        assertTrue(rs.next());
        for (int i = 1; i <= 4; i++) {
            //char or varchar2, nchar or nvarchar2
            if (i < 3) {
                //char or nchar
                assertEquals(timestampStr, rs.getString(i));
            } else {
                //varchar2 or nvarchar2
                assertEquals(timestampStr.trim(), rs.getString(i));
            }
            assertEquals(timestamp,rs.getTimestamp(i));
            try {
                rs.getDate(i);
                fail();
            } catch (Exception e) {
                assertTrue(e.getMessage().startsWith("bad format"));
            }
            try {
                rs.getTime(i);
                fail();
            } catch (Exception e) {
//                assertEquals("Time format incorrect, must be HH:mm:ss", e.getMessage());
                assertTrue(e.getMessage().contains("incorrect, must be HH:mm:ss"));
            }
        }
        assertTrue(rs.next());
        for (int i = 1; i <= 4; i++) {
            //char or varchar2, nchar or nvarchar2
            if (i < 3) {
                //char or nchar
                assertEquals(timeStr, rs.getString(i));
            } else {
                //varchar2 or nvarchar2
                assertEquals(timeStr.trim(), rs.getString(i));
            }
            try {
                rs.getTimestamp(i);
                fail();
            } catch (Exception e) {
                assertTrue(e.getMessage().startsWith("bad format"));
            }
            rs.getDate(i);
            try {
                rs.getTime(i);
                fail();
            } catch (Exception e) {
                assertTrue(e.getMessage().startsWith("Time format"));
            }
        }
        assertTrue(rs.next());
        assertEquals(Time.valueOf("10:50:30"),rs.getTime(1));
        assertEquals(Time.valueOf("10:50:30"),rs.getTime(2));

        conn = setConnection("&compatibleOjdbcVersion=8");
        stmt = conn.createStatement();
        rs = stmt.executeQuery("select * from test_get_time_from_character");
        assertTrue(rs.next());
        for (int i = 1; i <= 4; i++) {
            //char or varchar2, nchar or nvarchar2
            if (i < 3) {
                //char or nchar
                assertEquals(timestampStr, rs.getString(i));
            } else {
                //varchar2 or nvarchar2
                assertEquals(timestampStr.trim(), rs.getString(i));
            }
            assertEquals(timestamp,rs.getTimestamp(i));
            try {
                rs.getDate(i);
                fail();
            } catch (Exception e) {
                assertTrue(e instanceof SQLException);
            }
            try {
                rs.getTime(i);
                fail();
            } catch (Exception e) {
                assertTrue(e instanceof SQLException);
            }
        }
        assertTrue(rs.next());
        for (int i = 1; i <= 4; i++) {
            //char or varchar2, nchar or nvarchar2
            if (i < 3) {
                //char or nchar
                assertEquals(timeStr, rs.getString(i));
            } else {
                //varchar2 or nvarchar2
                assertEquals(timeStr.trim(), rs.getString(i));
            }
            try {
                rs.getTimestamp(i);
                fail();
            } catch (Exception e) {
                assertTrue(e instanceof SQLException);
            }
            rs.getDate(i);
            try {
                rs.getTime(i);
                fail();
            } catch (Exception e) {
                assertTrue(e instanceof SQLException);
            }
        }
        assertTrue(rs.next());
        assertEquals(Time.valueOf("10:50:30"),rs.getTime(1));
        assertEquals(Time.valueOf("10:50:30"),rs.getTime(2));
    }

    @Test
    public void testForDima2024062200102855704() throws SQLException {
        createTable("t1", "c1 float, c2 binary_float, c3 binary_double");
        Connection conn = setConnection("&useServerPrepStmts=false");
        Statement stmt = conn.createStatement();
        stmt.execute("insert into t1 values (1.2, 1.2, 1.2)");
        ResultSet rs = stmt.executeQuery("select * from t1");

        for (int i = 0; i < 2; i++) {
            if (i == 1) {
                conn = setConnection("&useServerPrepStmts=true");
                stmt = conn.createStatement();
                rs = stmt.executeQuery("select * from t1");
            }
            rs.next();
            Assert.assertEquals("1.2" , String.valueOf(rs.getFloat(1)));
            Assert.assertEquals("1.2" , String.valueOf(rs.getDouble(1)));
            Assert.assertEquals(new BigDecimal("1.2") , rs.getBigDecimal(1) );

            Assert.assertEquals("1.2" , String.valueOf(rs.getFloat(2)));
            Assert.assertEquals("1.2000000476837158" , String.valueOf(rs.getDouble(2)));
            Assert.assertEquals(new BigDecimal("1.2") , rs.getBigDecimal(2) );

            Assert.assertEquals("1.2" , String.valueOf(rs.getFloat(3)));
            Assert.assertEquals("1.2" , String.valueOf(rs.getDouble(3)));
            Assert.assertEquals(new BigDecimal("1.2") , rs.getBigDecimal(3) );
        }
    }

    @Test
    public void testForDima2024062200102855907() throws SQLException {
        createTable("t1", "c1 float, c2 binary_float, c3 binary_double");
        Connection conn = setConnection("&useServerPrepStmts=true");
        Statement stmt = conn.createStatement();
        stmt.execute("insert into t1 values (0.2, 0.2, 0.2)");

        ResultSet rs = stmt.executeQuery("select * from t1");
        rs.next();
        for (int i = 1; i <= 3; i++) {
            Assert.assertEquals("0", String.valueOf(rs.getShort(i)));
            Assert.assertEquals("0", String.valueOf(rs.getInt(i)));
            Assert.assertEquals("0", String.valueOf(rs.getLong(i)));
            Assert.assertEquals("0.2", String.valueOf(rs.getFloat(i)));

            if (i == 2) {
                Assert.assertEquals("0.20000000298023224", String.valueOf(rs.getDouble(i)));
            } else {
                Assert.assertEquals("0.2", String.valueOf(rs.getDouble(i)));
            }
            Assert.assertEquals("0.2", String.valueOf(rs.getBigDecimal(i)));
            Assert.assertEquals("0", String.valueOf(rs.getByte(i)));
        }
    }

    @Test
    public void testForDima2024120600106020947() throws SQLException {
        createTable("t_demo", "num float");
        Statement stmt = sharedConnection.createStatement();
        stmt.execute("insert into t_demo values(0.1733206919376)");
        stmt.execute("insert into t_demo values(1733206919110.1733206919133)");
        stmt.execute("insert into t_demo values(1733206919000)");
        stmt.execute("insert into t_demo values(1733206919133)");

        for (int i = 0; i < 2; i++) {
            Connection conn = setConnection("&useServerPrepStmts=false");
            if (i == 1) {
                conn = setConnection("&useServerPrepStmts=true");
            }
            PreparedStatement ps = conn.prepareStatement("select * from t_demo");
            ResultSet rs = ps.executeQuery();
            rs.next();
            Assert.assertEquals("0.1733206919376", rs.getString(1));
            Assert.assertEquals(0L, rs.getLong(1));

            rs.next();
            Assert.assertEquals("1733206919110.1733206919133", rs.getString(1));
            Assert.assertEquals(1733206919110L, rs.getLong(1));

            rs.next();
            Assert.assertEquals("1733206919000", rs.getString(1));
            Assert.assertEquals(1733206919000L, rs.getLong(1));

            rs.next();
            Assert.assertEquals("1733206919133", rs.getString(1));
            Assert.assertEquals(1733206919133L, rs.getLong(1));

            rs.close();
            ps.close();
            conn.close();
        }
    }

    @Test
    public void TestForFloatExceedsRange() throws SQLException {
        createTable("t_demo", "c1 float,  c2 binary_float, c3 binary_double");
        Statement stmt = sharedConnection.createStatement();
        stmt.execute("insert into t_demo values(9423372036854775807.1733206919376, 9423372036854775807.1733206919376, 9423372036854775807.1733206919376)");
        for (int i = 0; i < 2; i++) {
            Connection conn = setConnection("&useServerPrepStmts=false");
            if (i == 1) {
                conn = setConnection("&useServerPrepStmts=true");
            }

            PreparedStatement ps = conn.prepareStatement("select * from t_demo");
            ResultSet rs = ps.executeQuery();
            rs.next();

            // float type
            try {
                Assert.assertEquals(-1, rs.getByte(1));
                fail();
            } catch (SQLException e) {
                Assert.assertTrue(e.getMessage().contains("Out of range value"));
            }

            try {
                Assert.assertEquals(-1, rs.getShort(1));
                fail();
            } catch (SQLException e) {
                Assert.assertTrue(e.getMessage().contains("Out of range value"));
            }

            try {
                Assert.assertEquals(2147483647, rs.getInt(1));
                fail();
            } catch (SQLException e) {
                Assert.assertTrue(e.getMessage().contains("Out of range value"));
            }

            try {
                Assert.assertEquals(9223372036854775807L, rs.getLong(1));
                fail();
            } catch (SQLException e) {
                Assert.assertTrue(e.getMessage().contains("Out of range value"));
            }
            Assert.assertEquals("9.423372E18", String.valueOf(rs.getFloat(1)));
            Assert.assertEquals("9.423372036854776E18", String.valueOf(rs.getDouble(1)));
            Assert.assertEquals("9423372036854775807.1733206919376", String.valueOf(rs.getBigDecimal(1)));
            Assert.assertEquals("9423372036854775807.1733206919376", String.valueOf(rs.getString(1)));
            Assert.assertEquals("9423372036854775807.1733206919376", String.valueOf(rs.getObject(1)));

            // binary_float type
            Assert.assertEquals(-1, rs.getByte(2));
            Assert.assertEquals(-1, rs.getShort(2));
            Assert.assertEquals(2147483647, rs.getInt(2));
            Assert.assertEquals(9223372036854775807L, rs.getLong(2));
            Assert.assertEquals("9.423372E18", String.valueOf(rs.getFloat(2)));
            Assert.assertEquals("9.423372102435602E18", String.valueOf(rs.getDouble(2)));
            Assert.assertEquals("9.423372E+18", String.valueOf(rs.getBigDecimal(2)));
            Assert.assertEquals("9.423372E18", String.valueOf(rs.getString(2)));
            Assert.assertEquals("9.423372E18", String.valueOf(rs.getObject(2)));

            // binary_double type
            Assert.assertEquals(-1, rs.getByte(3));
            Assert.assertEquals(-1, rs.getShort(3));
            Assert.assertEquals(2147483647, rs.getInt(3));
            Assert.assertEquals(9223372036854775807L, rs.getLong(3));
            Assert.assertEquals("9.423372E18", String.valueOf(rs.getFloat(3)));
            Assert.assertEquals("9.423372036854776E18", String.valueOf(rs.getDouble(3)));
            Assert.assertEquals("9.423372036854776E+18", String.valueOf(rs.getBigDecimal(3)));
            Assert.assertEquals("9.423372036854776E18", String.valueOf(rs.getString(3)));
            Assert.assertEquals("9.423372036854776E18", String.valueOf(rs.getObject(3)));
        }
    }

}
