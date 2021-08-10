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

import java.sql.*;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.oceanbase.jdbc.extend.datatype.TIMESTAMPLTZ;
import com.oceanbase.jdbc.extend.datatype.TIMESTAMPTZ;

public class OracleTimeZoneTablesImported extends BaseOracleTest {
    static String tztable1 = "tztest1";
    static String tztable2 = "tztest2";
    static String tztable3 = "tztest3";

    @BeforeClass
    public static void initClass() throws SQLException {
        createTable(tztable1, "c1 TIMESTAMP(8) WITH TIME ZONE");
        createTable(tztable2, "c1 TIMESTAMP(8) WITH TIME ZONE");
        createTable(tztable3, "c1 TIMESTAMP(8) WITH LOCAL TIME ZONE");
    }

    @Test
    public void testGetStringTSTZ() {
        try {
            Statement stmt = sharedConnection.createStatement();
            stmt.execute("insert into " + tztable1
                         + " values('1941-03-16 16:24:11.111101Asia/Shanghai')");
            stmt.execute("insert into " + tztable1 + " values('1941-03-16 16:24:11Asia/Calcutta')");
            stmt.execute("insert into " + tztable1 + " values('1941-03-16 16:24:11')");
            PreparedStatement preparedStatement = sharedConnection.prepareStatement("insert into "
                                                                                    + tztable1
                                                                                    + " values(?)");
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss a Z")
                .withLocale(Locale.ENGLISH);
            ZonedDateTime tt1 = ZonedDateTime.parse("2019-03-27 10:15:30 AM +0530", formatter);
            ZonedDateTime tt2 = ZonedDateTime
                .parse("2019-04-01T16:24:11.125201+01:00[Africa/Ndjamena]");
            preparedStatement.setObject(1, tt1);
            preparedStatement.execute();
            ResultSet rs = stmt.executeQuery("select c1 from " + tztable1);
            Assert.assertTrue(rs.next());
            Assert.assertEquals("1941-03-16 16:24:11.111101 Asia/Shanghai", rs.getString(1));
            Assert.assertTrue(rs.next());
            Assert.assertEquals("1941-03-16 16:24:11.0 Asia/Calcutta", rs.getString(1));
            Assert.assertTrue(rs.next());
            Assert.assertEquals("1941-03-16 16:24:11.0 Asia/Shanghai", rs.getString(1));
            Assert.assertTrue(rs.next());
            Assert.assertEquals("2019-03-27 10:15:30.0 +5:30", rs.getString(1));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void insertTSTZ() {
        try {
            Statement stmt = sharedConnection.createStatement();
            PreparedStatement ps = sharedConnection.prepareStatement("insert into " + tztable2
                                                                     + " values(?)");
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss a Z")
                .withLocale(Locale.ENGLISH);
            ZonedDateTime tt1 = ZonedDateTime.parse("2019-03-27 10:15:30 AM +0530", formatter);
            ZonedDateTime tt2 = ZonedDateTime
                .parse("2019-04-01T16:24:11.125201+01:00[Africa/Ndjamena]");
            ps.setObject(1, tt1);
            ps.execute();
            ps.setObject(1, tt2);
            ps.execute();
            ResultSet rs = sharedConnection.createStatement().executeQuery(
                "select c1 from " + tztable2);
            Assert.assertTrue(rs.next());
            System.out.println("rs.getString(1) = " + rs.getString(1));
            Assert.assertEquals("2019-03-27 10:15:30.0 +5:30", rs.getString(1));
            Assert.assertTrue(rs.next());
            System.out.println("rs.getString(1) = " + rs.getString(1));
            Assert.assertEquals("2019-04-01 16:24:11.125201 Africa/Ndjamena", rs.getString(1));

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void insertTSLTZ() {
        try {
            Connection connection = sharedConnection;
            String val = ((ConnectionImpl) sharedConnection).getSessionTimeZone();
            ((ConnectionImpl) sharedConnection).setSessionTimeZone("Asia/Shanghai");
            val = ((ConnectionImpl) sharedConnection).getSessionTimeZone();
            System.out.println("set time zone to " + val);
            PreparedStatement ps = connection.prepareStatement("insert into " + tztable3
                                                               + " values(?)");
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss a Z")
                .withLocale(Locale.ENGLISH);
            ZonedDateTime tt1 = ZonedDateTime.parse("2019-03-27 10:15:30 AM +0530", formatter);
            ZonedDateTime tt2 = ZonedDateTime
                .parse("2019-04-01T16:24:11.125201+01:00[Africa/Ndjamena]");
            ps.setObject(1, tt1);
            ps.execute();
            ps.setObject(1, tt2);
            ps.execute();
            ResultSet rs = connection.createStatement().executeQuery("select c1 from " + tztable3);
            Assert.assertTrue(rs.next());
            System.out.println("rs.getString(1) = " + rs.getString(1));
            Assert.assertEquals("2019-03-27 12:45:30.0 Asia/Shanghai", rs.getString(1));
            Assert.assertTrue(rs.next());
            System.out.println("rs.getString(1) = " + rs.getString(1));
            Assert.assertEquals("2019-04-01 23:24:11.125201 Asia/Shanghai", rs.getString(1));
            ((ConnectionImpl) sharedConnection).setSessionTimeZone(val);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void comparisonTest() {
        Connection conn = null;
        try {
            Timestamp tResTZ[][] = new Timestamp[2][50];
            Timestamp tResLTZ[][] = new Timestamp[2][50];
            String sResTZ[][] = new String[2][50];
            String sResLTZ[][] = new String[2][50];

            Time tmResTZ[][] = new Time[2][50];
            Time tmResLTZ[][] = new Time[2][50];
            Date dateResTZ[][] = new Date[2][50];
            Date dateResLTZ[][] = new Date[2][50];

            String stringValTZ[][] = new String[2][50];
            String stringValLTZ[][] = new String[2][50];
            Time timeValTZ[][] = new Time[2][50];
            Time timeValLTZ[][] = new Time[2][50];
            Timestamp timestampValTZ[][] = new Timestamp[2][50];
            Timestamp timestampValLTZ[][] = new Timestamp[2][50];
            Date dateValTZ[][] = new Date[2][50];
            Date dateValLTZ[][] = new Date[2][50];
            ZonedDateTime now = ZonedDateTime.now();
            String table = "ZonedDateTime_test1";
            int len = 0;
            for (int i = 0; i < 2; i++) {
                if (i == 0) {
                    conn = sharedConnection;
                    System.out.println("OceanBase Test");
                } else {
                    conn = getOracleConnection();
                    System.out.println("Oracle Test");
                }
                try {
                    PreparedStatement ps = conn.prepareStatement("drop table " + table);
                    ps.execute();
                } catch (SQLException e) {
                    //
                }
                conn.createStatement().execute("alter session  set time_zone = 'Asia/Shanghai'"); // make oracle oceanbase session time zone same
                PreparedStatement ps1 = conn
                    .prepareStatement("create table "
                                      + table
                                      + "(TIMESTAMP_WTZ_TEST TIMESTAMP(8) WITH TIME ZONE, TIMESTAMP_WLTZ_TEST TIMESTAMP(8) WITH  LOCAL TIME ZONE)");
                ps1.execute();

                ps1 = conn
                    .prepareStatement("insert into " + table
                                      + "(TIMESTAMP_WTZ_TEST, TIMESTAMP_WLTZ_TEST) values (?, ?)");

                String timeStr[] = {
                        "1941-03-16T16:24:11.00034200+08:00[Asia/Shanghai]",
                        "2019-04-01T16:24:11.125200+01:00[Africa/Ndjamena]",
                        now.toString(),
                        "1941-03-16T16:24:11.252+08:00[Asia/Shanghai]",
                        "1941-03-16T16:24:11+08:00[Asia/Shanghai]",
                        "1941-03-16T16:24:11.000342+08:00[Asia/Shanghai]",
                        "1941-03-16T16:24:11.34200+08:00[Asia/Shanghai]",
                        "2019-04-01T16:24:11.0025200+05:30[Asia/Shanghai]",
                        "2019-04-01T16:24:11.25200+05:30[Asia/Calcutta]", //not any use for Zone be
                        "2019-04-01T16:24:11.125200-03:00[America/Maceio]",
                        "2019-04-01T16:24:11.125200-03:00[Antarctica/Rothera]",
                        "2019-04-01T16:24:11.125200-03:00[Indian/Mayotte]",

                        // ZonedTime as belows not equal with Oracle-JDBC for Summer/Winter Time
                        // Atlantic/Madeira: diff in 03-28 ~ 10-30 and same in 10-31 ~ 03-27(Next Year)
                        // OB-Server Oracle mode not handle it.
                        "2019-04-01T16:24:11.125200+00:00[Atlantic/Madeira]",
                        "2021-03-27T16:24:11.125200+00:00[Atlantic/Madeira]",
                        "2021-03-28T16:24:11.125200+00:00[Atlantic/Madeira]",

                        "2021-10-30T16:24:11.125200+00:00[Atlantic/Madeira]",
                        "2021-10-31T16:24:11.125200+00:00[Atlantic/Madeira]",
                        "2021-12-28T16:24:11.125200+00:00[Atlantic/Madeira]",

                        "2019-04-01T16:24:11.125200-03:00[Australia/Hobart]",
                        "2019-04-01T16:24:11.125200-03:00[Europe/Monaco]" };
                for (int k = 0; k < timeStr.length; k++) {
                    ZonedDateTime tt = ZonedDateTime.parse(timeStr[k]);
                    ps1.setObject(1, tt);
                    ps1.setObject(2, tt);
                    ps1.execute();
                }

                String timeStr2[] = { "2019-03-27 10:15:30 AM -0800",
                        "2019-03-27 10:15:30 AM +0530", "2019-03-27 10:15:30 AM +0000",
                        "2019-04-01 16:24:11 PM -0500", "2021-03-16 10:15:30 AM +0000",
                        "2021-03-16 10:15:30 AM +0800", "1941-03-16 10:15:30 AM +0000",
                        "1941-03-16 10:15:30 AM +0800" };
                DateTimeFormatter formatter = DateTimeFormatter
                    .ofPattern("yyyy-MM-dd HH:mm:ss a Z").withLocale(Locale.ENGLISH);
                for (int k = 0; k < timeStr2.length; k++) {
                    ZonedDateTime tt = ZonedDateTime.parse(timeStr2[k], formatter);
                    ps1.setObject(1, tt);
                    ps1.setObject(2, tt);
                    ps1.execute();
                }

                String timeStr3[] = { "Mon, 1 Apr 2019 11:05:30 +0800",
                        "Sun, 16 Mar 1941 23:59:59 +0800", "Sun, 16 Mar 1941 23:59:59 +0000" };
                formatter = DateTimeFormatter.ofPattern("E, d MMM yyyy HH:mm:ss Z").withLocale(
                    Locale.ENGLISH);
                for (int k = 0; k < timeStr3.length; k++) {
                    ZonedDateTime tt = ZonedDateTime.parse(timeStr3[k], formatter);
                    ps1.setObject(1, tt);
                    ps1.setObject(2, tt);
                    ps1.execute();
                }

                String timeStr4[] = {

                "2019-03-27 10:15:30.001230000 AM +0530", "2019-03-27 10:15:30.001230000 AM -0530",
                        "2019-03-27 10:15:30.001230001 AM -0530" };

                formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS a Z")
                    .withLocale(Locale.ENGLISH);
                for (int k = 0; k < timeStr4.length; k++) {
                    ZonedDateTime tt = ZonedDateTime.parse(timeStr4[k], formatter);
                    ps1.setObject(1, tt);
                    ps1.setObject(2, tt);
                    ps1.execute();
                }

                ResultSet rs = conn.prepareStatement("select * from " + table + "").executeQuery();
                Timestamp timestamp = Timestamp.from(now.toInstant());
                int k = 0;
                while (rs.next()) {
                    Timestamp t = rs.getTimestamp(1);
                    String s = rs.getString(1);
                    Time time1 = rs.getTime(1);
                    Date date1 = rs.getDate(1);
                    Timestamp t2 = rs.getTimestamp(2);
                    String s2 = rs.getString(2);
                    Time time2 = rs.getTime(2);
                    Date date2 = rs.getDate(2);
                    sResTZ[i][k] = s;
                    sResLTZ[i][k] = s2;
                    tResTZ[i][k] = t;
                    tResLTZ[i][k] = t2;
                    tmResTZ[i][k] = time1;
                    tmResLTZ[i][k] = time2;
                    dateResTZ[i][k] = date1;
                    dateResLTZ[i][k] = date2;

                    if (i == 0) {
                        TIMESTAMPTZ timestamptz = null;
                        TIMESTAMPLTZ timestampltz = null;
                        timestamptz = ((ObResultSet) rs).getTIMESTAMPTZ(1);
                        timestampltz = ((ObResultSet) rs).getTIMESTAMPLTZ(2);
                        stringValTZ[i][k] = timestamptz.stringValue(conn);
                        stringValLTZ[i][k] = timestampltz.stringValue(conn);
                        timeValTZ[i][k] = timestamptz.timeValue(conn);
                        timeValLTZ[i][k] = timestampltz.timeValue(conn);
                        timestampValTZ[i][k] = timestamptz.timestampValue(conn);
                        timestampValLTZ[i][k] = timestampltz.timestampValue(conn);
                        dateValTZ[i][k] = timestamptz.dateValue(conn);
                        dateValLTZ[i][k] = timestampltz.dateValue(conn);
                    } else {
                        oracle.sql.TIMESTAMPTZ timestamptzora = null;
                        oracle.sql.TIMESTAMPLTZ timestampltzora = null;
                        timestamptzora = ((oracle.jdbc.OracleResultSet) rs).getTIMESTAMPTZ(1);
                        timestampltzora = ((oracle.jdbc.OracleResultSet) rs).getTIMESTAMPLTZ(2);
                        stringValTZ[i][k] = timestamptzora.stringValue(conn);
                        stringValLTZ[i][k] = timestampltzora.stringValue(conn);
                        timeValTZ[i][k] = timestamptzora.timeValue(conn);
                        timeValLTZ[i][k] = timestampltzora.timeValue(conn);
                        timestampValTZ[i][k] = timestamptzora.timestampValue(conn);
                        timestampValLTZ[i][k] = timestampltzora.timestampValue(conn);
                        dateValTZ[i][k] = timestamptzora.dateValue(conn);
                        dateValLTZ[i][k] = timestampltzora.dateValue(conn);
                    }
                    k++;
                }
                len = k;
                ps1.execute();
                ps1.close();
            }
            System.out.println("Comparison for string");
            for (int j = 0; j < len; j++) {
                System.out.println("OB-Oracle" + "\t[" + j + "]\t" + sResTZ[0][j] + "\tAND "
                                   + sResLTZ[0][j]);
                System.out.println("Oracle-8.0:" + "\t[" + j + "]\t" + sResTZ[1][j] + "\tAND "
                                   + sResLTZ[1][j]);
                Assert.assertEquals(sResTZ[0][j], sResTZ[1][j]);
                Assert.assertEquals(sResLTZ[0][j], sResLTZ[1][j]);
            }
            System.out.println("Comparison for timestamp");
            for (int j = 0; j < len; j++) {
                System.out.println("OB-Oracle" + "\t[" + j + "]\t" + tResTZ[0][j] + "\tAND "
                                   + tResLTZ[0][j]);
                System.out.println("Oracle-8.0:" + "\t[" + j + "]\t" + tResLTZ[1][j] + "\tAND "
                                   + tResLTZ[1][j]);
                Assert.assertEquals(tResTZ[0][j], tResTZ[1][j]);
                Assert.assertEquals(tResLTZ[0][j], tResLTZ[1][j]);
            }
            System.out.println("Comparison for time");
            for (int j = 0; j < len; j++) {
                System.out.println("OB-Oracle" + "\t[" + j + "]\t" + tmResTZ[0][j] + "\tAND "
                                   + tmResLTZ[0][j]);
                System.out.println("Oracle-8.0:" + "\t[" + j + "]\t" + tmResTZ[1][j] + "\tAND "
                                   + tmResLTZ[1][j]);
                Assert.assertEquals(tmResTZ[0][j].toString(), tmResTZ[1][j].toString());
                Assert.assertEquals(tmResLTZ[0][j].toString(), tmResLTZ[1][j].toString());
            }

            System.out.println("Comparison for date");
            for (int j = 0; j < len; j++) {
                System.out.println("OB-Oracle" + "\t[" + j + "]\t" + dateResTZ[0][j].toString()
                                   + "\tAND " + dateResTZ[0][j].toString());
                System.out.println("Oracle-8.0:" + "\t[" + j + "]\t" + dateResLTZ[1][j].toString()
                                   + "\tAND " + dateResLTZ[1][j].toString());
                Assert.assertEquals(dateResTZ[0][j].toString(), dateResTZ[1][j].toString());
                Assert.assertEquals(dateResLTZ[0][j].toString(), dateResLTZ[1][j].toString());
            }
            System.out.println("Comparison for string value");
            for (int j = 0; j < len; j++) {
                System.out.println("OB-Oracle" + "\t[" + j + "]\t" + stringValTZ[0][j] + "\tAND "
                                   + stringValLTZ[0][j]);
                System.out.println("Oracle-8.0:" + "\t[" + j + "]\t" + stringValTZ[1][j] + "\tAND "
                                   + stringValLTZ[1][j]);
                Assert.assertEquals(stringValTZ[0][j], stringValTZ[1][j]);
                Assert.assertEquals(stringValLTZ[0][j], stringValLTZ[1][j]);
            }
            System.out.println("Comparison for time value");
            for (int j = 0; j < len; j++) {
                System.out.println("OB-Oracle" + "\t[" + j + "]\t" + timeValTZ[0][j] + "\tAND "
                                   + timeValLTZ[0][j]);
                System.out.println("Oracle-8.0:" + "\t[" + j + "]\t" + timeValTZ[1][j] + "\tAND "
                                   + timeValLTZ[1][j]);
                Assert.assertEquals(timeValTZ[0][j].toString(), timeValTZ[1][j].toString());
                Assert.assertEquals(timeValLTZ[0][j].toString(), timeValLTZ[1][j].toString());
            }

            System.out.println("Comparison for timestamp value");
            for (int j = 0; j < len; j++) {
                System.out.println("OB-Oracle" + "\t[" + j + "]\t" + timestampValTZ[0][j]
                                   + "\tAND " + timestampValLTZ[0][j]);
                System.out.println("Oracle-8.0:" + "\t[" + j + "]\t" + timestampValTZ[1][j]
                                   + "\tAND " + timestampValLTZ[1][j]);
                Assert.assertEquals(timestampValTZ[0][j].toString(),
                    timestampValTZ[1][j].toString());
                Assert.assertEquals(timestampValLTZ[0][j].toString(),
                    timestampValLTZ[1][j].toString());
            }

            System.out.println("Comparison for date value");
            for (int j = 0; j < len; j++) {
                System.out.println("OB-Oracle" + "\t[" + j + "]\t" + dateValTZ[0][j] + "\tAND "
                                   + dateValLTZ[0][j]);
                System.out.println("Oracle-8.0:" + "\t[" + j + "]\t" + dateValTZ[1][j] + "\tAND "
                                   + dateValLTZ[1][j]);
                Assert.assertEquals(dateValTZ[0][j].toString(), dateValTZ[1][j].toString());
                Assert.assertEquals(dateValLTZ[0][j].toString(), dateValLTZ[1][j].toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

    }
}
