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

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class MysqlDataTypeTest extends BaseTest {

    private static final Connection conn = sharedConnection;
    private PreparedStatement       ps   = null;

    @BeforeClass
    public static void initClass() throws SQLException {
        createTable("t_var", "id int not null primary key auto_increment, str varchar(20)");
        createTable("t_var_charset", "v varchar(20)");
        createTable("t_year", "id int not null primary key auto_increment, y year(2)");
        createTable("t_date", "d date");
        createTable("t_timestamp", "ts timestamp");
        createTable("t_time", "t time");
        createTable("t_datetime", "dt datetime");
        createTable("T_Case", "id int");
    }

    @Test
    public void testYear() throws SQLException {
        int i = conn.createStatement().executeUpdate(
            "insert into t_year (y) values (null), (1901), (70), (69)");
        assertEquals(4, i);
        ps = conn.prepareStatement("select y from t_year");
        ResultSet rs = ps.executeQuery();
        Date[] data = new Date[] { null, Date.valueOf("1901-01-01"), Date.valueOf("1970-01-01"),
                Date.valueOf("2069-01-01") };
        i = 0;
        while (rs.next()) {
            assertEquals(data[i], rs.getDate(1));
            assertEquals(data[i], rs.getObject(1));
            i++;
        }
        assertEquals("java.sql.Date", rs.getMetaData().getColumnClassName(1));
        assertEquals("YEAR", rs.getMetaData().getColumnTypeName(1));
        assertEquals(Types.DATE, rs.getMetaData().getColumnType(1));
    }

    @Test
    public void testDate() throws SQLException {
        ps = conn.prepareStatement("insert into t_date values (null), (?)");
        Date date = new Date(Timestamp.valueOf("2020-10-05 21:03:15.878879").getTime());
        ps.setDate(1, date);
        assertEquals(2, ps.executeUpdate());
        ResultSet rs = conn.prepareStatement("select * from t_date").executeQuery();
        assertTrue(rs.next());
        assertNull(rs.getDate(1));
        assertTrue(rs.next());
        assertEquals(date.toString(), rs.getDate(1).toString());
        rs.close();
    }

    @Test
    public void testTime() throws SQLException {
        ps = conn.prepareStatement("insert into t_time values (?), (null)");
        Time time = new Time(Timestamp.valueOf("2020-10-05 21:03:15.1").getTime());
        ps.setTime(1, time);
        assertEquals(2, ps.executeUpdate());
        ResultSet rs = conn.prepareStatement("select * from t_time").executeQuery();
        assertTrue(rs.next());
        assertEquals(time.toString(), rs.getTime(1).toString());
        assertTrue(rs.next());
        assertNull(rs.getDate(1));
        rs.close();
    }

    @Test
    public void testDatetime() throws SQLException {
        ps = conn.prepareStatement("insert into t_datetime values (?), (?), (null)");
        Timestamp timestamp1 = Timestamp.valueOf("2020-10-05 21:03:15.0");
        Timestamp timestamp2 = Timestamp.valueOf("1969-01-01 22:00:01.0");
        ps.setTimestamp(1, timestamp1);
        ps.setTimestamp(2, timestamp2);
        assertEquals(3, ps.executeUpdate());
        ResultSet rs = conn.prepareStatement("select * from t_datetime").executeQuery();
        Timestamp[] t = new Timestamp[] { timestamp1, timestamp2, null };
        int i = 0;
        while (rs.next()) {
            assertEquals(t[i], rs.getTimestamp(1));
            i++;
        }
        rs.close();
    }

    @Test
    public void testTimeStamp() throws SQLException {
        ps = conn.prepareStatement("insert into t_timestamp values (?), (?), (null)");
        Timestamp timestamp1 = Timestamp.valueOf("2020-10-05 21:03:15.0");
        Timestamp timestamp2 = Timestamp.valueOf("1970-01-01 22:00:01.0");
        ps.setTimestamp(1, timestamp1);
        ps.setTimestamp(2, timestamp2);
        assertEquals(3, ps.executeUpdate());
        ResultSet rs = conn.prepareStatement("select * from t_timestamp").executeQuery();
        Timestamp[] t = new Timestamp[] { timestamp1, timestamp2, null };
        int i = 0;
        while (rs.next()) {
            assertEquals(t[i], rs.getTimestamp(1));
            i++;
        }
        rs.close();
    }

    @Test
    public void testUnix_timestampToBigint() throws SQLException {
        String sql = "select unix_timestamp(?)";
        ps = conn.prepareStatement(sql);
        Timestamp timestamp = Timestamp.valueOf("2009-01-17 15:41:01");
        ps.setTimestamp(1, timestamp);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(1232178061L, rs.getLong(1));
        assertEquals(Types.BIGINT, rs.getMetaData().getColumnType(1));
        assertEquals("java.lang.Long", rs.getMetaData().getColumnClassName(1));
        rs.close();
    }

    @Test
    public void testUnix_timestampToDecimal() throws SQLException {
        String sql = "select unix_timestamp('15:14:07')";
        ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(new BigDecimal("0.000000"), rs.getBigDecimal(1));
        assertEquals(Types.DECIMAL, rs.getMetaData().getColumnType(1));
        assertEquals("java.math.BigDecimal", rs.getMetaData().getColumnClassName(1));
        rs.close();
    }

    @Test
    public void testFrom_UnixTime() throws SQLException {
        String sql = "select from_unixtime(?)";
        ps = conn.prepareStatement(sql);
        ps.setLong(1, 1218169800L);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(Timestamp.valueOf("2008-08-08 12:30:00.0"), rs.getTimestamp(1));
        assertEquals(Types.TIMESTAMP, rs.getMetaData().getColumnType(1));
        rs.close();
    }

    @Test
    public void testFrom_UnixTimeWithFormat() throws SQLException {
        String sql = "select from_unixtime(?, '%Y %D %M %h:%i:%s')";
        ps = conn.prepareStatement(sql);
        ps.setLong(1, 1218169800L);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals("2008 8th August 12:30:00", rs.getObject(1));
        assertEquals(Types.VARCHAR, rs.getMetaData().getColumnType(1));
        rs.close();
    }

    // OceanBase not support select timestamp('2020-01-01');
    @Ignore
    public void testTimestampFun() throws SQLException {
        String sql = "select timestamp(?)";
        ps = conn.prepareStatement(sql);
        Timestamp timestamp = Timestamp.valueOf("2020-01-01 22:00:01");
        ps.setDate(1, new Date(timestamp.getTime()));
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals("2020-01-01", rs.getDate(1).toString());
        assertEquals(Types.TIMESTAMP, rs.getMetaData().getColumnType(1));

        sql = "select timestamp(?,'01:00:00')";
        ps = conn.prepareStatement(sql);
        ps.setTimestamp(1, timestamp);
        rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(Timestamp.valueOf("2020-01-01 23:00:01"), rs.getTimestamp(1));
        assertEquals(Types.TIMESTAMP, rs.getMetaData().getColumnType(1));
        rs.close();
    }

    @Test
    public void testStrToDate() throws SQLException {
        String sql = "select str_to_date(?, '%Y-%m-%d')";
        ps = conn.prepareStatement(sql);
        ps.setString(1, "2020-10-20");
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals("2020-10-20", rs.getDate(1).toString());
        assertEquals(Types.DATE, rs.getMetaData().getColumnType(1));

        sql = "select str_to_date(?, '%Y-%m-%d %h:%i:%s')";
        ps = conn.prepareStatement(sql);
        Timestamp timestamp = Timestamp.valueOf("1970-01-01 22:00:01");
        ps.setTimestamp(1, timestamp);
        rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(timestamp, rs.getTimestamp(1));
        assertEquals(Types.TIMESTAMP, rs.getMetaData().getColumnType(1));

        sql = "select str_to_date(?, '%h:%i:%s')";
        ps = conn.prepareStatement(sql);
        Time time = new Time(Timestamp.valueOf("2020-10-05 08:09:30.1").getTime());
        ps.setTime(1, time);
        rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(time.toString(), rs.getTime(1).toString());
        assertEquals(Types.TIME, rs.getMetaData().getColumnType(1));
        rs.close();
    }

    @Test
    public void testVarchar() throws SQLException {
        String sql = "insert into t_var (str) values (?)";
        ps = conn.prepareStatement(sql);
        String s = "aiWO中国%$#*[]圐圙曌";
        ps.setString(1, "aiWO中国%$#*[]圐圙曌");
        assertEquals(1, ps.executeUpdate());
        sql = "select str from t_var";
        ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(s, rs.getString(1));
        assertEquals(Types.VARCHAR, rs.getMetaData().getColumnType(1));
        String s1 = new String(s.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
        assertEquals(s, s1);
        rs.close();
    }

    @Test
    public void testVarcharCharSetGB18030() throws SQLException, UnsupportedEncodingException {
        String sql = "insert into t_var_charset (v) values (?) ";
        String str = "abc中文";
        String str1 = new String(str.getBytes(StandardCharsets.UTF_8), "GB18030");
        ps = conn.prepareStatement(sql);
        ps.setString(1, str1);
        assertEquals(1, ps.executeUpdate());
        ResultSet rs = conn.prepareStatement("select v from t_var_charset").executeQuery();
        assertTrue(rs.next());
        assertEquals(str1, rs.getString(1));
        String str2 = new String(rs.getString(1).getBytes("GB18030"), StandardCharsets.UTF_8);
        assertEquals(str, str2);
        assertEquals(Types.VARCHAR, rs.getMetaData().getColumnType(1));
        rs.close();
    }

    @Test
    public void testVarcharCharSetGBK() throws SQLException, UnsupportedEncodingException {
        String sql = "insert into t_var_charset (v) values (?) ";
        String str = "abc中文";
        String str1 = new String(str.getBytes(StandardCharsets.UTF_8), "GBK");
        ps = conn.prepareStatement(sql);
        ps.setString(1, str1);
        assertEquals(1, ps.executeUpdate());
        ResultSet rs = conn.prepareStatement("select v from t_var_charset").executeQuery();
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertEquals(str1, rs.getString(1));
        String str2 = new String(rs.getString(1).getBytes("GBK"), StandardCharsets.UTF_8);
        assertEquals(str, str2);
        assertEquals(Types.VARCHAR, rs.getMetaData().getColumnType(1));
        rs.close();
    }

    // 'lower_case_table_names' is a read only variable Can not be dynamically set
    @Ignore
    public void testLowerCaseTableName() throws SQLException {
        String sql = "show variables like 'lower_case_table_names'";
        ResultSet rs = conn.prepareStatement(sql).executeQuery();
        assertTrue(rs.next());
        assertEquals("lower_case_table_names", rs.getString(1));
        assertEquals(0, rs.getInt(2));

        sql = "show tables like 'T_Case'";
        rs = conn.prepareStatement(sql).executeQuery();
        assertTrue(rs.next());
        assertEquals("T_Case", rs.getString(1));

        sql = "show tables like 't_Case'";
        rs = conn.prepareStatement(sql).executeQuery();
        assertFalse(rs.next());

        sql = "show tables like 'T_case'";
        rs = conn.prepareStatement(sql).executeQuery();
        assertFalse(rs.next());
    }
}
