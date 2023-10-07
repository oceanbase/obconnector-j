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

}
