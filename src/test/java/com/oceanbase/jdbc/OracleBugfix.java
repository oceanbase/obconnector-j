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

import java.io.*;
import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.TimeZone;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.junit.*;

import com.oceanbase.jdbc.jdbc2.optional.JDBC4MysqlXAConnection;
import com.oceanbase.jdbc.jdbc2.optional.MysqlXid;

public class OracleBugfix extends BaseOracleTest {
    static String testTMTable               = "test_timestamp" + getRandomString(5);
    static String testColonIdentifier       = "testcolon" + getRandomString(5);
    static String testColonIdentifierBatch  = "testcolonbatch" + getRandomString(5);
    static String testColonIdentifierBatch2 = "testcolonbatch" + getRandomString(5);
    static String testColonIdentifierProc   = "testcolonproc" + getRandomString(5);
    static String testJDBIfunction          = "JDBIfunction1" + getRandomString(5);
    static String testJDBIfunction2         = "JDBIfunction1" + getRandomString(5);
    static String testNumberCol1            = "testNumberCol1" + getRandomString(5);
    static String testNumberCol2            = "testNumberCol2" + getRandomString(5);
    static String testNumberCol3            = "testNumberCol3" + getRandomString(5);
    static String testNumberCol4            = "testNumberCol4" + getRandomString(5);
    static String testNumberCol5            = "testNumberCol5" + getRandomString(5);
    static String testNumberCol6            = "testNumberCol6" + getRandomString(5);
    static String longNameFix               = "longNameFix" + getRandomString(5);
    static String testTimeStamp             = "testTimeStamp" + getRandomString(5);
    static String blobFix                   = "blobFix" + getRandomString(5);
    static String clobFix                   = "clobFix" + getRandomString(5);
    static String binaryNumberProc1         = "binaryNumberProc1" + getRandomString(5);
    static String binaryNumberProc2         = "binaryNumberProc2" + getRandomString(5);
    static String testRaw                   = "testRaw" + getRandomString(5);
    static String testProceSetdefaultVal    = "testProceSetdefaultVal" + getRandomString(5);
    static String testFunction              = "testfunction" + getRandomString(5);
    static String testDate                  = "testDate" + getRandomString(5);
    static String testDate2                 = "testDate2" + getRandomString(5);
    static String funcWithoutParam          = "funcWithoutParam" + getRandomString(5);
    static String procWithoutParam          = "procWithoutParam" + getRandomString(5);
    static String test                      = "test" + getRandomString(5);
    static String zonedDateTimeTable        = "zonedDateTime" + getRandomString(5);
    static String varchar2Number            = "varchar2Number" + getRandomString(5);
    static String nvarchar2Number           = "nvarchar2Number" + getRandomString(5);
    static String clobAndBlob               = "clobAndBlob" + getRandomString(5);
    static String convertCharToBoolean      = "convertCharToBoolean" + getRandomString(5);
    static String convertBlobToBytes        = "convertBlobToBytes" + getRandomString(5);
    static String testDateAndTS             = "testDateAndTS" + getRandomString(5);
    static String testReaderToCLob          = "testReaderToCLob" + getRandomString(5);
    static String testReaderToCLob2         = "testReaderToCLob2" + getRandomString(5);
    static String testSetDate               = "testSetDate" + getRandomString(5);
    static String testSetReaderToClob       = "testSetReaderToClob" + getRandomString(5);
    static String testEmpty                 = "testEmpty" + getRandomString(5);
    static String nvarchar2Test             = "nvarchar2_test" + getRandomString(5);

    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable(testTMTable, "ci timestamp(6)");
        createTable(testColonIdentifier, "c1 varchar2(200),c2 int ,c3 int");
        createTable(testColonIdentifierBatch, "c1 varchar2(200),c2 int ,c3 int");
        createTable(testColonIdentifierBatch2, "c1 varchar2(200),c2 int ,c3 int");
        createProcedure(testColonIdentifierProc,
            "(p1 IN int ,p2 OUT int) is begin  p2:=p1 + 250; end");
        createFunction(
            testJDBIfunction,
            "(\n"
                    + "  I_BIRTHDAY               IN DATE,        "
                    + "  I_HOLD_DATE              IN DATE,        "
                    + "  I_VALIDATE_DATE          IN DATE       "
                    + ") RETURN NUMBER\n"
                    + "as\n"
                    + "  V_AGE_MONTH  NUMBER;\n"
                    + "begin\n"
                    + "  if true then\n"
                    + "    V_AGE_MONTH := mod(floor(months_between(I_HOLD_DATE,I_BIRTHDAY)), 12);\n"
                    + "  else\n"
                    + "    V_AGE_MONTH := mod(floor(months_between(I_VALIDATE_DATE,I_BIRTHDAY)), 12);\n"
                    + "  end if;\n" + "  return V_AGE_MONTH;\n" + "end;\n");
        createFunction(testJDBIfunction2, "(\n" + "  i1               IN INt,       "
                                          + "  i2              IN INT,       "
                                          + "  i3          IN INT        " + ") RETURN NUMBER\n"
                                          + "as\n" + "  ototoal  NUMBER;\n" + "begin\n"
                                          + "ototoal := i1 + i2 + i3;\n" + "return ototoal;\n"
                                          + "end");
        createTable(longNameFix, "MONEY  number(38)");
        createTable(testTimeStamp, "id number not null , c01 timestamp, c02 date");
        createTable(blobFix, "c1 blob");
        createTable(clobFix, "c1 clob");
        createProcedure(binaryNumberProc1, "(var1 in int,var2 out binary_integer) is\n" + "begin\n"
                                           + "var2 := var1;\n" + "end;");
        createProcedure(binaryNumberProc2, "(var1 in int,var2 out binary_integer) is\n" + "begin\n"
                                           + "var2 := var1;\n" + "end;");
        createTable(testRaw, "c1 raw(20),c2 int");

        createProcedure(
            testProceSetdefaultVal,
            "(cycle# in number,c_num in number,v_cycle out number ,v_mulbillcycle out number,v_mulbillcycle_end out number) is \n"
                    + "i_date  date; \n"
                    + " i_day varchar2(4);\n"
                    + "v_date date; \n"
                    + " v_end_day varchar2(4)\n;"
                    + "begin"
                    + " if cycle# is null then \n"
                    + "RAISE_APPLICATION_ERROR(-20001,'输入日期不能为空');\n"
                    + "end if ;\n"
                    + " i_date := to_date(to_char(cycle#), 'yyyymmdd'); \n"
                    + " i_day := to_char(i_date,'dd') ; \n"
                    + "v_date := add_months(i_date,c_num) ; \n"
                    + "v_cycle := to_number(to_char(trunc(v_date, 'MM'), 'yyyymm'));\n"
                    + " if i_date = last_day(i_date) then \n"
                    + "v_end_day := '31' ; \n"
                    + " else \n"
                    + "v_end_day := i_day ; \n"
                    + " end if ;\n"
                    + "v_mulbillcycle := to_number(to_char(v_cycle)||i_day); \n"
                    + " v_mulbillcycle_end := to_number(to_char(v_cycle)||v_end_day);\n"
                    + "end "
                    + testProceSetdefaultVal + ";\n");
        createFunction(testFunction,
            "(c1 varchar2,c2 int,c3 out  int,c4 in int )   return number is \n" + "begin \n"
                    + "   c3 := 3; \n" + "   return 1;\n" + "end ");
        createFunction(funcWithoutParam, "    return  number is\n" + "begin\n" + "    null;\n"
                                         + "    return 2;\n" + "end ;");
        createTable(test, "c1 int");
        createProcedure(procWithoutParam, " as begin insert into " + test + " values(1); end;");
        createTable(testDate, "c1 date");
        createTable(testDate2, "a int, b date, c date");

        createTable(zonedDateTimeTable,
            "a NUMBER(6), b TIMESTAMP(6) WITH TIME ZONE,c int,d  TIMESTAMP(6) WITH TIME ZONE");
        createTable(varchar2Number, "c1 varchar2(200)");
        createTable(nvarchar2Number, "c1 nvarchar2(200)");
        createTable(clobAndBlob, "c1 clob,c2 blob");
        createTable(convertCharToBoolean, "c1 nvarchar2(200),c2 varchar2(200)");
        createTable(convertBlobToBytes, "c1 int ,c2 blob");
        createTable(testDateAndTS, "c1 date ,c2 timestamp");
        createTable(testReaderToCLob, "c1 clob, c2 number, c3 clob, c4 int");
        createTable(testReaderToCLob2, "c1 clob, c2 number, c3 clob, c4 int");
        createTable(testSetDate, "c1 date");
        createTable(testSetReaderToClob, "c1 clob");
        createTable(testEmpty, "c1 blob,c2 clob");
        createTable(nvarchar2Test, "c1 nvarchar2(200)");
    }

    public static ZonedDateTime getZonedDateTime(String value) {
        DateTimeFormatter dateTimeFormatter = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ISO_LOCAL_DATE).appendLiteral(' ')
            .append(DateTimeFormatter.ISO_LOCAL_TIME).appendLiteral(' ').appendZoneOrOffsetId()
            .toFormatter();
        return ZonedDateTime.parse(value.trim(), dateTimeFormatter);
    }

    @Test
    public void testForTimestampillisecond() {
        try {
            Statement stmt = sharedConnection.createStatement();
            stmt.execute("insert into " + testTMTable + " values('2020-10-10 20:00:00.001111');");
            stmt.execute("insert into " + testTMTable + " values('2020-10-10 20:00:01.001234');");
            ResultSet rs = stmt.executeQuery("select * from " + testTMTable);
            Assert.assertTrue(rs.next());
            Assert.assertEquals("2020-10-10 20:00:00.001111", rs.getString(1));
            Assert.assertTrue(rs.next());
            Assert.assertEquals("2020-10-10 20:00:01.001234", rs.getString(1));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    // need batch options
    @Test
    public void supportColonIdentifier() {
        try {
            Connection conn = setConnection("&supportNameBinding=true");
            Statement stmt = conn.createStatement();
            stmt.execute("insert /* commment */  into " + testColonIdentifier
                         + " values('varcharvalue111',1,2)");
            stmt.execute("insert into " + testColonIdentifier + " values('varcharvalue222',1,3)");
            PreparedStatement pstmt = conn.prepareStatement("select * /* comment */ from "
                                                            + testColonIdentifier
                                                            + " where  c3 =:n and c2 = :name ");
            pstmt.setInt(1, 3);
            pstmt.setInt(2, 1);
            ResultSet rs = pstmt.executeQuery();
            Assert.assertTrue(rs.next());
            Assert.assertEquals("varcharvalue222", rs.getString(1));
            Assert.assertEquals("3", rs.getString("c3"));

        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void supportColonIdentifierBatch() {
        try {
            Connection conn = setConnection("&supportNameBinding=true");
            Statement stmt = conn.createStatement();
            PreparedStatement pstmt = conn.prepareStatement("insert into "
                                                            + testColonIdentifierBatch
                                                            + " values(:var1,:var2,:var3);");
            pstmt.setString(1, "values");
            pstmt.setInt(2, 1);
            pstmt.setInt(3, 2);
            pstmt.addBatch();
            pstmt.setString(1, "values");
            pstmt.setInt(2, 1);
            pstmt.setInt(3, 2);
            pstmt.addBatch();
            pstmt.executeBatch();

        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void supportColonIdentifierBatch2() {
        try {
            Connection conn = setConnection("&rewriteBatchedStatements=true");
            Statement stmt = conn.createStatement();
            PreparedStatement pstmt = conn.prepareStatement("insert into "
                                                            + testColonIdentifierBatch2
                                                            + " values(:var1,:var2,:var3);");
            pstmt.setString(1, "values");
            pstmt.setInt(2, 1);
            pstmt.setInt(3, 2);
            pstmt.addBatch();
            pstmt.setString(1, "values");
            pstmt.setInt(2, 1);
            pstmt.setInt(3, 2);
            pstmt.addBatch();
            pstmt.executeBatch();

        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void supportColonIdentifierProcedure() {
        try {
            Connection conn = null;
            conn = setConnection("&supportNameBinding=true");
            CallableStatement cstmt = conn.prepareCall("{ call " + testColonIdentifierProc
                                                       + "\n(:inparam,:outparam) }");
            cstmt.setInt(1, 100);
            cstmt.registerOutParameter(2, Types.INTEGER);
            cstmt.executeUpdate();
            System.out.println("getValue with index = " + cstmt.getInt(2));
            Assert.assertEquals(350, cstmt.getInt(2));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    // bugfix  for JDBI name binding setDate
    @Test
    public void functionTest() {
        try {
            Connection conn = sharedConnection;
            CallableStatement cstmt = conn.prepareCall("{? =  call " + testJDBIfunction
                                                       + "(:name1,:name2,:name3)}");
            cstmt.registerOutParameter(1, Types.INTEGER);
            Date date = new Date(System.currentTimeMillis());
            cstmt.setDate(2, date);
            cstmt.setDate(3, date);
            cstmt.setDate(4, date);
            cstmt.execute();
            Assert.assertEquals(0, cstmt.getInt(1));
            System.out.println("cstmt = " + cstmt.getInt(1));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void functionTest2() {

        try {
            Connection conn = sharedConnection;
            CallableStatement cstmt = conn.prepareCall("{? =  call " + testJDBIfunction2
                                                       + "(:name1,:name2,:name3)}");
            cstmt.registerOutParameter(1, Types.INTEGER);
            cstmt.setInt(2, 1);
            cstmt.setInt(3, 1);
            cstmt.setInt(4, 1);
            cstmt.execute();
            System.out.println("cstmt = " + cstmt.getInt(1));
            Assert.assertEquals(3, cstmt.getInt(1));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testNameBindingChinese() throws SQLException {
        try {
            Connection conn = sharedConnection;
            try {
                conn.prepareStatement("drop table ttt_name1").executeUpdate();
            } catch (SQLException e) {
                //                e.printStackTrace();
            }
            conn.prepareStatement("create table ttt_name1 (c1 number, c2 varchar2(50))")
                .executeUpdate();
            String sql = "insert into ttt_name1 values(:b, :张\uE863)";
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setInt(1, 1);
            ps.setString(2, "生僻字占位符");
            ps.executeUpdate();
            ResultSet rs = conn.prepareStatement("select c1 from ttt_name1").executeQuery();
            rs.next();
        } catch (Exception e) {
            Assert.fail();
            e.printStackTrace();
        }
    }

    @Test
    public void testNameBindingChinese2() throws SQLException {
        try {
            Connection conn = setConnection("&rewriteBatchedStatements");
            try {
                conn.prepareStatement("drop table ttt_name2").executeUpdate();
            } catch (SQLException e) {
                //                e.printStackTrace();
            }
            conn.prepareStatement("create table ttt_name2 (c1 number, c2 varchar2(50))")
                .executeUpdate();
            String sql = "insert into ttt_name2 values(:b, :张\uE863)";
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setInt(1, 1);
            ps.setString(2, "生僻字占位符");
            ps.executeUpdate();
            ResultSet rs = conn.prepareStatement("select c1 from ttt_name2").executeQuery();
            rs.next();
        } catch (Exception e) {
            Assert.fail();
            e.printStackTrace();
        }
    }

    @Test
    public void testNumberCol() {
        try {
            createTable(testNumberCol1, "c1 int");
            createTable(testNumberCol2, "c1 float");
            createTable(testNumberCol3, "c1 decimal");
            createTable(testNumberCol4, "c1 number");
            createTable(testNumberCol5, "c1 binary_float");
            createTable(testNumberCol6, "c1 binary_double");

            String nameList[] = { testNumberCol1, testNumberCol2, testNumberCol3, testNumberCol4, testNumberCol5, testNumberCol6 };
            Statement statement = sharedConnection.createStatement();
            ResultSet rs = null;
            for (int i = 0; i < nameList.length; i++) {
                String name = nameList[i];
                statement.execute("insert into " + name + " values(1) ");
                rs = statement.executeQuery("select c1 from " + name);
                Assert.assertTrue(rs.next());
                Object obj = rs.getObject(1);
                if (i == 4) {
                    Assert.assertEquals("i = " + i, Float.class, obj.getClass());
                } else if (i == 5) {
                    Assert.assertEquals("i = " + i, Double.class, obj.getClass());
                } else {
                    Assert.assertEquals("i = " + i, BigDecimal.class, obj.getClass());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testBug35276523() throws Exception {
        Connection conn = sharedConnection;
        Statement statement = conn.createStatement();
        try {
            statement.execute("drop table test_decimal");
        } catch (SQLException e) {
            // e.printStackTrace();
        }
        statement
            .executeUpdate("create table test_decimal (c1 int, c2 float, c3 decimal, c4 number)");
        String sql = "insert into test_decimal values (1, 1, 1, 1)";
        Assert.assertEquals(1, conn.prepareStatement(sql).executeUpdate());

        ResultSet rs = conn.prepareStatement("select * from test_decimal").executeQuery();
        Assert.assertTrue(rs.next());
        Assert.assertEquals(BigDecimal.class, rs.getObject(1).getClass());
        Assert.assertEquals(BigDecimal.class, rs.getObject(2).getClass());
        Assert.assertEquals(BigDecimal.class, rs.getObject(3).getClass());
        Assert.assertEquals(BigDecimal.class, rs.getObject(4).getClass());
    }

    @Test
    public void testStatementWarnings() {
        try {
            Connection conn = sharedConnection;
            try {
                conn.createStatement().execute("drop table t");
            } catch (SQLException e) {
                //                e.printStackTrace();
            }
            Statement stmt = conn.createStatement();
            stmt.execute("create table t(a int)");
            stmt.execute("create or replace procedure pp\n" + "is\n" + "begin\n"
                         + "insert into t(a1) values(1);\n" + "end;\n");
            SQLWarning sqlWarning = stmt.getWarnings();
            System.out.println("sqlWarning = " + sqlWarning.getMessage());
            sqlWarning = sqlWarning.getNextWarning();
            Assert.assertEquals(null, sqlWarning);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void fixGetColumnsNullable() {
        try {
            Connection conn = sharedConnection;
            Statement stmt = conn.createStatement();
            try {
                stmt.execute("drop table TESTNULLABLE");
            } catch (SQLException e) {
                //                e.printStackTrace();
            }
            stmt.execute("create table TESTNULLABLE (c1 varchar2(36))");
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            ResultSet resultSet = databaseMetaData.getColumns(null, null, "TESTNULLABLE", null);
            while (resultSet.next()) {
                String name = resultSet.getString("COLUMN_NAME");
                System.out.println("name = " + name);
                int length = resultSet.getInt("COLUMN_SIZE");
                System.out.println("length = " + length);
                int nullable = resultSet.getInt("NULLABLE");
                System.out.println("nullable = " + nullable);
                Assert.assertEquals(1, nullable);
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void fixGetClientInfo() {
        try {
            Properties properties = sharedConnection.getClientInfo();
            String appName = sharedConnection.getClientInfo("ApplicationName");
            String clientUser = sharedConnection.getClientInfo("ClientUser");
            String ClientHostname = sharedConnection.getClientInfo("ClientHostname");
            Assert.assertEquals(properties.get("ApplicationName"), appName);
            Assert.assertEquals(properties.get("ClientUser"), clientUser);
            Assert.assertEquals(properties.get("ClientHostname"), ClientHostname);
            // These attributes of OceanBase  are all empty now , there is no need to compare values temporarily
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testLongName() {
        try {
            Statement statement = sharedConnection.createStatement();
            ResultSet rs = statement
                .executeQuery("select MONEY+MONEY+MONEY+MONEY+MONEY+MONEY+MONEY+MONEY+MONEY+MONEY+MONEY+MONEY+MONEY+MONEY+MONEY+MONEY+MONEY+MONEY+MONEY+MONEY-MONEY+MONEY-MONEY+MONEY-MONEY+MONEY-MONEY+MONEY-MONEY+MONEY-MONEY+MONEY-MONEY+MONEY-MONEY+MONEY-MONEY+MONEY-MONEY+MONEY-MONEY+MONEY-MONEY+MONEY-MONEY+MONEY+MONEY-MONEY+MONEY-MONEY+MONEY-MONEY+MONEY-MONEY+MONEY-MONEY+MONEY-MONEY+MONEY-MONEY+MONEY-MONEY+MONEY-MONEY+MONEY-MONEY+MONEY-MONEY+MONEY-MONEY+MONEY-MONEY+MONEY from "
                              + longNameFix + " ");
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    public LocalDateTime getLocalDateTimeNano(String value) {
        DateTimeFormatter localDateFormatterNano = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ISO_LOCAL_DATE).appendLiteral(' ')
            .append(DateTimeFormatter.ISO_LOCAL_TIME).toFormatter();
        return LocalDateTime.parse(value, localDateFormatterNano);
    }

    @org.junit.Test
    public void testTimeZone() {
        TimeZone tzDefault = TimeZone.getDefault();
        try {
            Connection conn = sharedConnection;
            Statement stmt = conn.createStatement();
            String insertTableSql = "insert into " + testTimeStamp + " values(?,?,?)";
            TimeZone timeZone = TimeZone.getTimeZone("GMT" + "+08:00");
            timeZone.setID("+08:00");
            TimeZone.setDefault(timeZone);

            PreparedStatement ps2 = conn.prepareStatement(insertTableSql);
            ps2.setObject(1, 2);
            ps2.setObject(2, getLocalDateTimeNano("1988-09-09 15:43:01"));
            ps2.setObject(3, getLocalDateTimeNano("1988-09-09 15:43:01"));
            ps2.executeUpdate();
            ps2.close();
            ResultSet resultSet = stmt.executeQuery("select * from " + testTimeStamp);
            String value = "1988-09-09 15:43:01";
            String dateValue = "1988-09-09";
            Assert.assertTrue(resultSet.next());
            String timeStampStr = resultSet.getString(2);
            Assert.assertEquals(value, timeStampStr);
            String timeStampStr2 = resultSet.getString(3);
            Assert.assertEquals(value, timeStampStr2);
            Timestamp ts = resultSet.getTimestamp(2);
            Assert.assertEquals(value + ".0", ts.toString());
            Date date = resultSet.getDate(3);
            Assert.assertEquals(dateValue, date.toString());
            Timestamp ts2 = resultSet.getTimestamp(3);
            Assert.assertEquals(value + ".0", ts2.toString());
            Object obj1 = resultSet.getObject(2);
            Assert.assertEquals(value + ".0", ((Timestamp) obj1).toString());
            Object obj2 = resultSet.getObject(3);
            Assert.assertEquals(value + ".0", ((Timestamp) obj2).toString());

            PreparedStatement preparedStatement = conn.prepareStatement("select * from "
                                                                        + testTimeStamp);
            resultSet = preparedStatement.executeQuery();

            Assert.assertTrue(resultSet.next());

            timeStampStr = resultSet.getString(2);
            Assert.assertEquals(value, timeStampStr);
            timeStampStr2 = resultSet.getString(3);
            Assert.assertEquals(value, timeStampStr2);
            ts = resultSet.getTimestamp(2);
            Assert.assertEquals(value + ".0", ts.toString());
            date = resultSet.getDate(3);
            Assert.assertEquals(dateValue, date.toString());
            ts2 = resultSet.getTimestamp(3);
            Assert.assertEquals(value + ".0", ts2.toString());
            obj1 = resultSet.getObject(2);
            Assert.assertEquals(value + ".0", ((Timestamp) obj1).toString());
            obj2 = resultSet.getObject(3);
            Assert.assertEquals(value + ".0", ((Timestamp) obj2).toString());
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            TimeZone.setDefault(tzDefault);
        }
    }

    @org.junit.Test
    public void test36103306() {
        try {
            Connection conn = sharedConnection;
            PreparedStatement ps = conn.prepareStatement("insert into " + blobFix
                                                         + "(c1) values(?)");
            ps.setBlob(1, new SerialBlob(new byte[] { (byte) 0x00, 0x32, 0x32 }));
            fail();
            ps.execute();
        } catch (Throwable e) {
            Assert.assertTrue(e.getMessage().contains(
                "javax.sql.rowset.serial.SerialBlob is not supported"));
        }
    }

    @Test
    public void testProcBinaryInterger() {
        try {
            Connection conn = sharedConnection;
            CallableStatement callableStatement = conn.prepareCall("{call " + binaryNumberProc1
                                                                   + "(1,?)}");
            callableStatement.registerOutParameter(1, Types.VARCHAR);
            callableStatement.execute();
            Assert.assertEquals("1", callableStatement.getString(1));
            Assert.assertEquals(1, callableStatement.getInt(1));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @org.junit.Test
    public void test36103306_1() {
        try {
            Connection conn = sharedConnection;
            PreparedStatement ps = conn.prepareStatement("insert into " + clobFix
                                                         + "(c1) values(?)");
            ps.setClob(1, new SerialClob(new char[] { 'a', 'b', 'c' }));
            ps.execute();
        } catch (Throwable e) {
            Assert.assertTrue(e.getMessage().contains(
                "javax.sql.rowset.serial.SerialClob is not supported"));
        }
    }

    public void testProcBinaryInterger2() {
        try {
            Connection conn = sharedConnection;
            CallableStatement callableStatement = conn.prepareCall("{call " + binaryNumberProc2
                                                                   + "(?,?)}");
            callableStatement.registerOutParameter(2, Types.VARCHAR);
            callableStatement.setInt(1, 1);
            callableStatement.execute();
            Assert.assertEquals("1", callableStatement.getString(2));
            Assert.assertEquals(1, callableStatement.getInt(2));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    // fix array.delete return value
    @org.junit.Test
  public void test34971515() throws Exception {
    Connection conn = sharedConnection;
    ArrayList<Object> res_list = new ArrayList<>();
    String sql3 = "create or replace type var_t1 is varray(3) of number";
    String sql4 = "create or replace procedure pro1(var out var_t1) is\n" +
        "begin\n" +
        "var :=var_t1(1,2,3);\n" +
        "var.delete();\n" +
        "end;";


    String paraCall = "call pro1(?)";
    Statement stmt = conn.createStatement();
    stmt.execute(sql3);
    stmt.execute(sql4);
    CallableStatement cst = null;

    try {
      cst = conn.prepareCall(paraCall);
      cst.registerOutParameter(1, Types.ARRAY, "VAR_T1");
      cst.execute();

      Array array = cst.getArray(1);
      Object[] arrobj = (Object[]) array.getArray();
      Assert.assertNotNull(arrobj);

    } catch (Exception e) {
      res_list.add(e);
      e.printStackTrace();
    }
  }

    // array support char elements
    @org.junit.Test
    public void test34946578() throws Exception {
        try {
            Connection conn = sharedConnection;
            String sql3 = "create or replace type var_t2 is varray(3) of char";
            String sql4 = "create or replace procedure pro2(var out var_t2) is\n" + "begin\n"
                          + "var :=var_t2('a','b','c');\n" + "end;";

            String paraCall = "call pro2(?)";
            Statement stmt;
            stmt = conn.createStatement();
            stmt.execute(sql3);
            stmt.execute(sql4);
            CallableStatement cst = null;
            cst = conn.prepareCall(paraCall);
            cst.registerOutParameter(1, Types.ARRAY, "VAR_T2");
            cst.execute();

            Array array = cst.getArray(1);

            Object[] arrobj = (Object[]) array.getArray();
            Assert.assertEquals('a', arrobj[0]);
            Assert.assertEquals('b', arrobj[1]);
            Assert.assertEquals('c', arrobj[2]);
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @org.junit.Test
    public void chineseColumnName() {
        try {
            Connection conn = sharedConnection;
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select '中文' as \"列1\" from dual");
            ResultSetMetaData resultSetMetaData = rs.getMetaData();
            Assert.assertEquals("列1", resultSetMetaData.getColumnName(1));
            Assert.assertTrue(rs.next());
            Assert.assertEquals("中文", rs.getString(1));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    // fix the type of raw element
    @org.junit.Test
    public void test34971454() {
        try {
            Connection conn = sharedConnection;
            String sql3 = "create or replace type var_t3 is varray(3) of raw(10)";
            String sql4 = "create or replace procedure pro3(var out var_t3) is\n" + "begin\n"
                          + "var :=var_t3(hextoraw('ff'),hextoraw('fa'),hextoraw('fc'));\n"
                          + "end;";
            String paraCall = "call pro3(?)";
            Statement stmt = conn.createStatement();
            stmt.execute(sql3);
            stmt.execute(sql4);
            CallableStatement cst;
            cst = conn.prepareCall(paraCall);
            cst.registerOutParameter(1, Types.ARRAY, "VAR_T3");
            cst.execute();
            Array array = cst.getArray(1);
            Object[] arrobj = (Object[]) array.getArray();
            Assert.assertNotNull(array);
            for (int i = 0; i < arrobj.length; i++) {
                Assert.assertEquals(byte[].class, arrobj[i].getClass());
            }
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testRawFix() {
        try {
            Connection conn = sharedConnection;
            Statement statement = conn.createStatement();
            statement.execute("insert into " + testRaw + " values(utl_raw.cast_to_raw('阿里'),1)");
            PreparedStatement preparedStatement = conn.prepareStatement("select * from " + testRaw
                                                                        + " where c1 = ?");
            preparedStatement.setBytes(1, "阿里".getBytes());
            ResultSet resultSet = preparedStatement.executeQuery();
            Assert.assertNotNull(resultSet.next());
            Assert.assertEquals("阿里", new String(resultSet.getBytes(1)));
            Assert.assertEquals(1, resultSet.getInt(2));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }

    }

    @org.junit.Test
    public void tesOracleModeProc() {
        try {
            Connection conn = sharedConnection;
            CallableStatement callableStatement = conn.prepareCall("call " + testProceSetdefaultVal
                                                                   + "(?,0,?,?,?)");
            callableStatement.setInt(1, 20210709);
            callableStatement.setInt(2, -2);
            callableStatement.registerOutParameter(3, Types.NUMERIC);
            callableStatement.registerOutParameter(4, Types.NUMERIC);
            callableStatement.execute();
            Assert.assertEquals(202107, callableStatement.getInt(3));
            Assert.assertEquals(20210709, callableStatement.getInt(4));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @org.junit.Test
    public void tesOracleModeFunction() {
        try {

            Connection conn = sharedConnection;
            CallableStatement callableStatement = conn.prepareCall("? = call " + testFunction
                                                                   + "('fsfsfs',100,?,?)");
            callableStatement.registerOutParameter(1, Types.NUMERIC);
            callableStatement.registerOutParameter(2, Types.NUMERIC);
            //            callableStatement.setInt(3, 20);
            callableStatement.setInt("c3", 20);
            try {
                callableStatement.execute();
                Assert.fail();
            } catch (SQLException e) {
                assertEquals(
                    "The number of parameter names '1' does not match the number of registered parameters in sql '3'.",
                    e.getMessage());
            }

            callableStatement = conn.prepareCall("? = call " + testFunction + "('fsfsfs',?,?,?)");
            callableStatement.registerOutParameter(1, Types.NUMERIC);
            callableStatement.registerOutParameter(3, Types.NUMERIC);
            callableStatement.setInt("c2", 100);
            callableStatement.setInt(4, 20);
            callableStatement.execute();
            Assert.assertEquals(1, callableStatement.getInt(1));
            Assert.assertEquals(3, callableStatement.getInt(3));

        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @org.junit.Test
    public void testSetTimeForDate() {
        try {
            Connection conn = sharedConnection;
            conn.createStatement().execute("delete from " + testDate);
            PreparedStatement ps = sharedConnection.prepareStatement("insert into " + testDate
                                                                     + " values(?)");
            Time testTime = new Time(12, 0, 0);
            ps.setTime(1, testTime);
            ps.execute();
            ResultSet rs = conn.createStatement().executeQuery("select c1 from " + testDate);
            Assert.assertTrue(rs.next());
            Assert.assertEquals("1970-01-01", rs.getDate(1).toString());
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @org.junit.Test
    public void testSetTimeForDate2() throws SQLException {
        Connection conn = setConnection("&emulateUnsupportedPstmts=true");
        PreparedStatement ps = conn.prepareStatement("select a from " + testDate2
                                                     + " where nvl(b, ? + 1) > c");
        Time testTime = new Time(12, 0, 0);
        ps.setTime(1, testTime);
        ps.executeQuery();

        try {
            ps = sharedConnection.prepareStatement("select a from " + testDate2
                                                   + " where nvl(b, ? + 1) > c");
            ps.setTime(1, testTime);
            ps.executeQuery();
        } catch (SQLSyntaxErrorException e) {
            if (!e.getMessage().contains("ORA-00932: inconsistent datatypes")) {
                e.printStackTrace();
                Assert.fail();
            }
        }
    }

    @org.junit.Test
    public void testFuntionWithoutParam() {
        try {
            Connection conn = sharedConnection;
            CallableStatement callableStatement = conn.prepareCall("?= call " + funcWithoutParam);
            callableStatement.registerOutParameter(1, Types.INTEGER);
            callableStatement.execute();
            Assert.assertEquals(2, callableStatement.getInt(1));
            callableStatement = conn.prepareCall("?= call " + funcWithoutParam + "()");
            callableStatement.registerOutParameter(1, Types.INTEGER);
            callableStatement.execute();
            Assert.assertEquals(2, callableStatement.getInt(1));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @org.junit.Test
    public void testProcWithoutParam() {
        try {
            Connection conn = sharedConnection;
            CallableStatement callableStatement = conn.prepareCall("call " + procWithoutParam);
            callableStatement.execute();
            ResultSet rs = conn.createStatement().executeQuery("select count(*) from " + test);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getInt(1));
            callableStatement = conn.prepareCall("call " + procWithoutParam + "()");
            callableStatement.execute();
            rs = conn.createStatement().executeQuery("select count(*) from " + test);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(2, rs.getInt(1));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testZoneDate() {
        try {
            Statement stmt = sharedConnection.createStatement();
            String sql = "insert into " + zonedDateTimeTable + " values(?,?, ?,?)";
            LocalDateTime ldt = LocalDateTime.of(2019, 9, 13, 17, 16, 17, 920);
            ZonedDateTime zny = ldt.atZone(ZoneId.of("Asia/Shanghai"));
            PreparedStatement ps1 = sharedConnection.prepareStatement(sql);
            ps1.setObject(1, 1);
            ps1.setObject(2, zny);
            ps1.setObject(3, 2);
            ps1.setObject(4, zny);
            ps1.executeUpdate();
            ResultSet rs = stmt.executeQuery("select * from " + zonedDateTimeTable);
            Assert.assertTrue(rs.next());
        } catch (Throwable e) {
            Assert.fail();
            e.printStackTrace();
        }
    }

    @Test
    public void testGBK() {
        try {
            Connection conn = setConnection("&characterEncoding=gbk");
            Statement stmt = conn.createStatement();
            ResultSet resultSet = stmt.executeQuery("select '中文ab' from dual");
            resultSet.next();
            Assert.assertEquals("中文ab", resultSet.getString(1));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testVarchar2Number() {
        try {
            Statement stmt = sharedConnection.createStatement();
            stmt.execute("insert into " + varchar2Number + " values('100')");
            ResultSet rs1 = stmt.executeQuery("select c1 from " + varchar2Number);
            Assert.assertTrue(rs1.next());
            Assert.assertEquals(100, rs1.getInt(1));
            Assert.assertEquals((byte) 100, rs1.getByte(1));
            Assert.assertEquals((short) 100, rs1.getShort(1));
            Assert.assertEquals((long) 100, rs1.getLong(1));
            Assert.assertEquals("100.0", Float.toString(rs1.getFloat(1)));
            Assert.assertEquals("100.0", Double.toString(rs1.getFloat(1)));
            PreparedStatement ps = sharedConnection.prepareStatement("select c1 from "
                                                                     + varchar2Number);
            rs1 = ps.executeQuery();
            Assert.assertTrue(rs1.next());
            Assert.assertEquals(100, rs1.getInt(1));
            Assert.assertEquals((byte) 100, rs1.getByte(1));
            Assert.assertEquals((short) 100, rs1.getShort(1));
            Assert.assertEquals((long) 100, rs1.getLong(1));
            Assert.assertEquals("100.0", Float.toString(rs1.getFloat(1)));
            Assert.assertEquals("100.0", Double.toString(rs1.getFloat(1)));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testNvarchar2Number() {
        try {
            Statement stmt = sharedConnection.createStatement();
            stmt.execute("insert into " + nvarchar2Number + " values('100')");
            ResultSet rs1 = stmt.executeQuery("select c1 from " + nvarchar2Number);
            Assert.assertTrue(rs1.next());
            Assert.assertEquals(100, rs1.getInt(1));
            Assert.assertEquals((byte) 100, rs1.getByte(1));
            Assert.assertEquals((short) 100, rs1.getShort(1));
            Assert.assertEquals((long) 100, rs1.getLong(1));
            Assert.assertEquals("100.0", Float.toString(rs1.getFloat(1)));
            Assert.assertEquals("100.0", Double.toString(rs1.getFloat(1)));
            PreparedStatement ps = sharedConnection.prepareStatement("select c1 from "
                                                                     + nvarchar2Number);
            rs1 = ps.executeQuery();
            Assert.assertTrue(rs1.next());
            Assert.assertEquals(100, rs1.getInt(1));
            Assert.assertEquals((byte) 100, rs1.getByte(1));
            Assert.assertEquals((short) 100, rs1.getShort(1));
            Assert.assertEquals((long) 100, rs1.getLong(1));
            Assert.assertEquals("100.0", Float.toString(rs1.getFloat(1)));
            Assert.assertEquals("100.0", Double.toString(rs1.getFloat(1)));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testConnectionVariables() {
        try {
            Connection conn1 = setConnection("&usePipelineAuth=true");
            Connection conn2 = setConnection("&usePipelineAuth=false");
            Connection conn3 = setConnection("&usePipelineAuth=false&characterEncoding=utf-8");
            Connection conn4 = setConnection("&usePipelineAuth=false&characterEncoding=utf8");
            Connection conn5 = setConnection("&usePipelineAuth=true&characterEncoding=utf-8");
            Connection conn6 = setConnection("&usePipelineAuth=true&characterEncoding=utf8");
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testClobAndBlob() {
        try {
            Statement stmt = sharedConnection.createStatement();
            stmt.execute("insert into " + clobAndBlob + " values('100',rawtohex('xxxxx'))");
            ResultSet rs1 = stmt.executeQuery("select * from " + clobAndBlob);
            Assert.assertTrue(rs1.next());
            Object clobObj = rs1.getObject(1);
            Object blobObj = rs1.getObject(2);
            Assert.assertTrue(clobObj instanceof java.sql.Clob);
            Assert.assertFalse(clobObj instanceof java.sql.Blob);
            Assert.assertTrue(blobObj instanceof java.sql.Blob);
            Assert.assertFalse(blobObj instanceof java.sql.Clob);
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testGetBoolean() {
        try {
            Statement stmt = sharedConnection.createStatement();
            stmt.execute("insert into "
                         + convertCharToBoolean
                         + " values('n','n'),('y','y'),('t','t'),('f','f'),('1','1'),('-1','-1'),('0','0'),('x','z'),('string value','string values2'),(null,null)");
            ResultSet rs = stmt.executeQuery("select * from " + convertCharToBoolean);
            Assert.assertTrue(rs.next());
            Assert.assertFalse(rs.getBoolean(1));
            Assert.assertFalse(rs.getBoolean(2));
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getBoolean(1));
            Assert.assertTrue(rs.getBoolean(2));
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getBoolean(1));
            Assert.assertTrue(rs.getBoolean(2));
            Assert.assertTrue(rs.next());
            Assert.assertFalse(rs.getBoolean(1));
            Assert.assertFalse(rs.getBoolean(2));
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getBoolean(1));
            Assert.assertTrue(rs.getBoolean(2));
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getBoolean(1));
            Assert.assertTrue(rs.getBoolean(2));
            Assert.assertTrue(rs.next());
            Assert.assertFalse(rs.getBoolean(1));
            Assert.assertFalse(rs.getBoolean(2));
            // other string values are all false
            Assert.assertTrue(rs.next());
            Assert.assertFalse(rs.getBoolean(1));
            Assert.assertFalse(rs.getBoolean(2));
            Assert.assertTrue(rs.next());
            Assert.assertFalse(rs.getBoolean(1));
            Assert.assertFalse(rs.getBoolean(2));
            Assert.assertTrue(rs.next());
            Assert.assertFalse(rs.getBoolean(1));
            Assert.assertFalse(rs.getBoolean(2));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testSendBatchBug37854707() {
        try {
            Connection conn = setConnection("&usePieceData=true&rewriteBatchedStatements=true");
            String clobTest1 = "t_clob";
            try {
                conn.createStatement().execute("drop table t_clob");
            } catch (SQLException e) {
                //                e.printStackTrace();
            }
            conn.createStatement().execute(
                "create table t_clob(c1 clob, c2 number, c3 clob, c4 int)");
            PreparedStatement ps = conn.prepareStatement("insert into " + clobTest1
                                                         + " values(?, ?, ?, ?)");
            String str = "\u571e\u58dc";
            ps.setCharacterStream(1, new StringReader(str));
            ps.setObject(3, str);
            ps.setInt(2, 100);
            ps.setInt(4, 100);
            ps.addBatch();

            ps.setCharacterStream(1, new StringReader(str));
            ps.setObject(3, str);
            ps.setInt(2, 100);
            ps.setInt(4, 100);
            ps.addBatch();
            ps.executeBatch();
            ps = conn.prepareStatement("select * from " + clobTest1);
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals(str, rs.getString(1));
            assertEquals(str, rs.getString(3));
            assertEquals(100, rs.getInt(2));
            assertEquals(100, rs.getInt(4));

            assertTrue(rs.next());
            assertEquals(str, rs.getString(1));
            assertEquals(str, rs.getString(3));
            assertEquals(100, rs.getInt(2));
            assertEquals(100, rs.getInt(4));

        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void getObjectFromBlob() {
        try {
            Connection conn = sharedConnection;
            Statement stmt = conn.createStatement();
            byte[] value = "中文".getBytes();
            PreparedStatement ps = conn.prepareStatement("insert into " + convertBlobToBytes
                                                         + " values(?,?)");
            ps.setInt(1, 1);
            ps.setBytes(2, value);
            ps.execute();
            ResultSet rs = stmt.executeQuery("select * from " + convertBlobToBytes);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getInt(1));
            Assert.assertEquals("中文", new String(rs.getObject(2, byte[].class)));
            ps = conn.prepareStatement("select * from " + convertBlobToBytes);
            rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getInt(1));
            Assert.assertEquals("中文", new String(rs.getObject(2, byte[].class)));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testTimestampToLocalDate() {
        try {
            Connection conn = sharedConnection;
            Statement stmt = conn.createStatement();
            PreparedStatement preparedStatement = conn.prepareStatement("insert into "
                                                                        + testDateAndTS
                                                                        + " values(?,?)");
            Date date = new Date(System.currentTimeMillis());
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            preparedStatement.setDate(1, date);
            preparedStatement.setTimestamp(2, timestamp);
            preparedStatement.execute();

            ResultSet rs = stmt.executeQuery("select * from " + testDateAndTS);
            while (rs.next()) {
                Object c1 = rs.getObject(1, LocalDateTime.class);
                Object c2 = rs.getObject(2, LocalDateTime.class);
            }
            preparedStatement = conn.prepareStatement("select * from " + testDateAndTS);
            rs = preparedStatement.executeQuery();
            while (rs.next()) {
                Object c1 = rs.getObject(1, LocalDateTime.class);
                Object c2 = rs.getObject(2, LocalDateTime.class);
            }
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void executeUpdateSendPiece() {
        Assume.assumeTrue(sharedUsePrepare());
        try {
            Connection conn = setConnection("&usePieceData=true");
            PreparedStatement ps = conn.prepareStatement("insert into " + testReaderToCLob
                                                         + " values(?, ?, ?, ?)");
            String str = "\u571e\u58dc";
            ps.setCharacterStream(1, new StringReader(str));
            ps.setObject(3, str);
            ps.setInt(2, 100);
            ps.setInt(4, 100);
            assertEquals(1, ps.executeUpdate());
            assertEquals(1, ps.getUpdateCount());
            ps = conn.prepareStatement("select * from " + testReaderToCLob);
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals(str, rs.getString(1));
            assertEquals(str, rs.getString(3));
            assertEquals(100, rs.getInt(2));
            assertEquals(100, rs.getInt(4));
        } catch (Throwable e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testSendPieceBatchNums() {
        Assume.assumeTrue(sharedUsePrepare());
        try {
            Connection conn = setConnection("&usePieceData=true");
            Statement stmt = conn.createStatement();
            PreparedStatement ps = conn.prepareStatement("insert into " + testReaderToCLob2
                                                         + " values(?, ?, ?, ?)");
            String str = "\u571e\u58dc";
            int num = 3;
            for (int i = 0; i < 3; i++) {
                ps.setCharacterStream(1, new StringReader(str));
                ps.setObject(3, str);
                ps.setInt(2, 100);
                ps.setInt(4, 100);
                ps.addBatch();
            }
            int[] ints = ps.executeBatch();
            System.out.println(Arrays.toString(ints));
            assertEquals(num, ints.length);
            ResultSet rs = stmt.executeQuery("select * from " + testReaderToCLob2);
            while (rs.next()) {
                assertEquals(str, rs.getString(1));
                assertEquals(str, rs.getString(3));
                assertEquals(100, rs.getInt(2));
                assertEquals(100, rs.getInt(4));
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testSetDate() {
        try {
            Statement stmt = sharedConnection.createStatement();
            sharedConnection.createStatement().execute("delete from " + testDate);
            PreparedStatement ps = sharedConnection.prepareStatement("insert into " + testDate
                                                                     + " values(?)");
            Date date = new Date(18, 10, 11);
            ps.setDate(1, date);
            ps.execute();
            ResultSet rs = stmt.executeQuery("select c1 from " + testDate);
            Assert.assertTrue(rs.next());
            Date date1 = rs.getDate(1);
            Assert.assertEquals(date1, date);
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testSetReaderToClob() {
        Assume.assumeFalse(sharedUsePrepare());
        try {
            PreparedStatement statement = sharedConnection.prepareStatement("insert into "
                                                                            + testSetReaderToClob
                                                                            + " values(?)");
            String value = "test ''\0\n\\";
            StringReader stringReader = new StringReader(value);
            statement.setClob(1, stringReader);
            statement.executeUpdate();
            ResultSet rs = statement.executeQuery("select c1 from " + testSetReaderToClob);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(value, rs.getString(1));
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testEmpty() {
        try {
            Connection conn = setConnection("&usePieceData");
            Statement stmt = conn.createStatement();
            PreparedStatement ps = conn.prepareStatement("insert into " + testEmpty
                                                         + " values(?,?)");
            ps.setBlob(1, Blob.getEmptyBLOB());
            ps.setClob(2, Clob.getEmptyCLOB());
            ps.execute();
            ps.clearParameters();
            ps.setBytes(1, new byte[] { '1', '@', 'a', 'A' });
            ps.setString(2, "中文\nabc");
            ps.execute();
            ResultSet rs = stmt.executeQuery("select count(*) from " + testEmpty);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(2, rs.getInt(1));
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testHint() {
        try {
            try {
                sharedConnection.createStatement().execute(
                    "drop outline \"CONCURRENT_LIMIT_2d312698bdea452387c7a523e9631157\"");
            } catch (SQLException e) {
            }
            String sql = "CREATE OUTLINE \"CONCURRENT_LIMIT_2d312698bdea452387c7a523e9631157\" ON '3D8DD579717E266F5DD3DA99A6E8E2EA' USING HINT /*+ max_concurrent(10) */";
            sharedConnection.prepareStatement(sql).execute();

        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void prepareCalllAnonymousBlockWithNameAndHint() {
        try {
            Connection conn = sharedConnection;
            CallableStatement callableStatement = null;
            try {
                callableStatement = conn.prepareCall("drop table result_table");
                callableStatement.execute();
            } catch (SQLException e) {
                //                e.printStackTrace();
            }
            callableStatement = conn
                .prepareCall("create table result_table(c1 varchar2(200),c2 number)");
            callableStatement.execute();
            String sql = "DECLARE\n"
                         + "  V_TABLENAME      VARCHAR2(20);\n"
                         + "  V_NUMBER         NUMBER;\n"
                         + "BEGIN\n"
                         + "  V_TABLENAME     := :V_TABLENAME ;\n"
                         + "  V_NUMBER     := to_number(:NUMBER1) ;\n"
                         + "  insert  /*+ ORDERED */ into result_table values(V_TABLENAME,V_NUMBER);\n"
                         + "\n" + "end ; ";
            callableStatement = conn.prepareCall(sql);
            callableStatement.setString(1, "test_val");
            callableStatement.setInt(2, 100);
            callableStatement.execute();
            callableStatement = conn.prepareCall("select * from result_table");
            ResultSet rs = callableStatement.executeQuery();
            Assert.assertTrue(rs.next());
            Assert.assertEquals(100, rs.getInt(2));
            Assert.assertEquals("test_val", rs.getString(1));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testHintBatch() {
        try {
            Statement statement = sharedConnection.createStatement();
            try {
                sharedConnection.createStatement().execute("drop table testBatchHint");
            } catch (SQLException e) {
                //                e.printStackTrace();
            }
            sharedConnection.createStatement().execute(
                "create table testBatchHint(c1 varchar2(200),c2 int)");
            PreparedStatement ps = sharedConnection
                .prepareStatement("insert /*+ ORDERED */  into testBatchHint values(?,?)");
            ps.setString(1, "teststr1");
            ps.setInt(2, 1);
            ps.addBatch();
            ps.setString(1, "teststr2");
            ps.setInt(2, 2);
            ps.addBatch();
            ps.setString(1, "teststr3");
            ps.setInt(2, 3);
            ps.addBatch();
            ps.executeBatch();
            ResultSet rs = statement.executeQuery("select * from testBatchHint");
            Assert.assertTrue(rs.next());
            Assert.assertEquals("teststr1", rs.getString(1));
            Assert.assertEquals(1, rs.getInt(2));
            Assert.assertTrue(rs.next());
            Assert.assertEquals("teststr2", rs.getString(1));
            Assert.assertEquals(2, rs.getInt(2));
            Assert.assertTrue(rs.next());
            Assert.assertEquals("teststr3", rs.getString(1));
            Assert.assertEquals(3, rs.getInt(2));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testHintRewriteBatch() {
        try {
            Connection conn = setConnection("&rewriteBatchedStatements=true");
            Statement statement = sharedConnection.createStatement();
            try {
                sharedConnection.createStatement().execute("drop table testBatchRwHint");
            } catch (SQLException e) {
                //                e.printStackTrace();
            }
            sharedConnection.createStatement().execute(
                "create table testBatchRwHint(c1 varchar2(200),c2 int)");
            PreparedStatement ps = sharedConnection
                .prepareStatement("insert /*+ ORDERED */ into testBatchRwHint values(?,?)");
            ps.setString(1, "teststr1");
            ps.setInt(2, 1);
            ps.addBatch();
            ps.setString(1, "teststr2");
            ps.setInt(2, 2);
            ps.addBatch();
            ps.setString(1, "teststr3");
            ps.setInt(2, 3);
            ps.addBatch();
            ps.executeBatch();
            ResultSet rs = statement.executeQuery("select * from testBatchRwHint");
            Assert.assertTrue(rs.next());
            Assert.assertEquals("teststr1", rs.getString(1));
            Assert.assertEquals(1, rs.getInt(2));
            Assert.assertTrue(rs.next());
            Assert.assertEquals("teststr2", rs.getString(1));
            Assert.assertEquals(2, rs.getInt(2));
            Assert.assertTrue(rs.next());
            Assert.assertEquals("teststr3", rs.getString(1));
            Assert.assertEquals(3, rs.getInt(2));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testTimestampNanoBug() {
        try {
            Connection conn = sharedConnection;
            Statement stmt = conn.createStatement();
            String testTimestampNanos = "test_timestamp_nanos";
            try {
                conn.createStatement().execute("drop table " + testTimestampNanos);
            } catch (SQLException e) {
                //                e.printStackTrace();
            }
            conn.createStatement()
                .execute(
                    "create table " + testTimestampNanos
                            + "(c1 timestamp, c2 timestamp, c3 timestamp)");
            PreparedStatement ps = conn.prepareStatement("insert into " + testTimestampNanos
                                                         + " values(?, ?, ?)");
            Timestamp timestamp1 = Timestamp.valueOf("2012-12-12 12:12:12.987654");
            Timestamp timestamp2 = Timestamp.valueOf("2012-12-12 12:12:12.987654321");
            Timestamp timestamp3 = new Timestamp(18, 10, 11, 11, 11, 11, 123456789);
            ps.setTimestamp(1, timestamp1);
            ps.setTimestamp(2, timestamp2);
            ps.setTimestamp(3, timestamp3);
            ps.execute();
            ResultSet rs = stmt.executeQuery("select *  from " + testTimestampNanos);
            Assert.assertTrue(rs.next());
            assertEquals(987654000, rs.getTimestamp(1).getNanos());
            assertEquals(987654000, rs.getTimestamp(2).getNanos());
            assertEquals(123457000, rs.getTimestamp(3).getNanos());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    public static void createTable(String tableName, String tableColumns, Statement stmt)
                                                                                         throws SQLException {
        try {
            stmt.execute("drop table " + tableName + "");
        } catch (SQLException e) {
            //            e.printStackTrace();
        }
        stmt.execute("create table " + tableName + " (" + tableColumns + ") ");
    }

    @Test
    public void testPsCursorBug38105626Fix() throws SQLException {
        Connection conn = sharedConnection;
        Statement stmt = conn.createStatement();
        createTable("GROUP_SUBS_MEMBER",
            " \"OID\" NUMBER(14) NOT NULL ENABLE,\n" + "  \"REGION\" NUMBER(5) NOT NULL ENABLE,\n"
                    + "  \"GROUPOID\" NUMBER(14),\n" + "  \"MEMTYPE\" NUMBER(2),\n"
                    + "  \"SUBSID\" NUMBER(14),\n" + "  \"MEMREGION\" NUMBER(5),\n"
                    + "  \"MEMSERVNUMBER\" VARCHAR2(32),\n" + "  \"PRODID\" VARCHAR2(32),\n"
                    + "  \"SHORTNUMBER\" VARCHAR2(10),\n" + "  \"STARTDATE\" DATE,\n"
                    + "  \"APPLYOID\" NUMBER(14),\n" + "  \"CANCELOID\" NUMBER(14),\n"
                    + "  \"STATUS\" VARCHAR2(16),\n" + "  \"STATUSDATE\" DATE,\n"
                    + "  \"GRPPKGSUBSID\" NUMBER(14),\n" + "  \"ENDDATE\" DATE,\n"
                    + "  \"ORDERSTATUS\" NUMBER(1),\n" + "  \"ORDERTIME\" DATE,\n"
                    + "  \"ORDERRESULT\" VARCHAR2(512),\n" + "  \"OPERTYPE\" VARCHAR2(16),\n"
                    + "  \"ISPRIMA\" NUMBER(1),\n" + "  \"FAMILYSUBSID\" NUMBER(18),\n"
                    + "  \"CREATEDATE\" DATE,\n" + "  \"APPLYOPERID\" VARCHAR2(20),\n"
                    + "  \"STAMPTIME\" DATE,\n" + "  \"SUBSPRODOID\" NUMBER(14),\n"
                    + "  \"PKGPRODID\" VARCHAR2(32),\n"
                    + "  CONSTRAINT \"PK_CM_GROUP_MEMBER\" UNIQUE (\"OID\", \"REGION\")", stmt);
        try {

            String sql = "select /*+ no_rewrite */  1 from group_subs_member a where a.region= :region and a.groupoid= :groupoid and a.memservnumber= :memservnumber  and a.status not like 'stcmR%'  and (a.status !=:STATUSINV or (a.status =:STATUSINV and a.orderstatus =1) )";

            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, "633");
            pstmt.setString(2, "6331234567890");
            pstmt.setString(3, "13500000000");
            pstmt.setString(4, "stcmInv");
            pstmt.setString(5, "stcmInv");
            pstmt.execute();
            System.out.println(0);

        } catch (SQLException e) {
            e.printStackTrace();
            System.out.printf(e.toString());
            Assert.assertTrue("server bug 38105626", false);
            Assert.fail();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.toString());
            Assert.fail();
        }
    }

    @Test
    public void testTimestampToLocalDate_2() {
        try {
            Connection conn = sharedConnection;
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt
                .executeQuery("select sysdate, systimestamp, localtimestamp, current_timestamp from dual");
            assertTrue(rs.next());
            rs.getObject(1, LocalDateTime.class);
            rs.getObject(2, LocalDateTime.class);
            rs.getObject(3, LocalDateTime.class);
            rs.getObject(4, LocalDateTime.class);
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void oracleModeGetPrecisionTest() {
        try {
            Connection conn = sharedConnection;
            try {
                conn.createStatement().execute("drop table varchartest");
            } catch (SQLException e) {
                //                e.printStackTrace();
            }
            conn.createStatement()
                .execute(
                    "create table varchartest(c1 varchar2(1),c2 varchar2(2),c3 varchar2(10),c4  varchar2(100))");
            ResultSet rs = conn.createStatement().executeQuery("select * from varchartest");
            ResultSetMetaData resultSetMetaData = rs.getMetaData();
            Assert.assertEquals(1, resultSetMetaData.getPrecision(1));
            Assert.assertEquals(2, resultSetMetaData.getPrecision(2));
            Assert.assertEquals(10, resultSetMetaData.getPrecision(3));
            Assert.assertEquals(100, resultSetMetaData.getPrecision(4));
            try {
                conn.createStatement().execute("drop table varchartest");
            } catch (SQLException e) {
                //                e.printStackTrace();
            }
            conn.createStatement()
                .execute(
                    "create table varchartest(c1 varchar2(1 byte),c2 varchar2(2 byte),c3 varchar2(10 byte),c4  varchar2(100 byte))");
            rs = conn.createStatement().executeQuery("select * from varchartest");
            resultSetMetaData = rs.getMetaData();
            System.out.println("varchar2 with byte");
            Assert.assertEquals(1, resultSetMetaData.getPrecision(1));
            Assert.assertEquals(2, resultSetMetaData.getPrecision(2));
            Assert.assertEquals(10, resultSetMetaData.getPrecision(3));
            Assert.assertEquals(100, resultSetMetaData.getPrecision(4));

            try {
                conn.createStatement().execute("drop table nvarchartest");
            } catch (SQLException e) {
                //                e.printStackTrace();
            }
            conn.createStatement()
                .execute(
                    "create table nvarchartest(c1 nvarchar2(1),c2 nvarchar2(2),c3 nvarchar2(10),c4 nvarchar2(100))");
            rs = conn.createStatement().executeQuery("select * from nvarchartest");
            resultSetMetaData = rs.getMetaData();
            System.out.println("nvarchar2");
            Assert.assertEquals(1, resultSetMetaData.getPrecision(1));
            Assert.assertEquals(2, resultSetMetaData.getPrecision(2));
            Assert.assertEquals(10, resultSetMetaData.getPrecision(3));
            Assert.assertEquals(100, resultSetMetaData.getPrecision(4));

            try {
                conn.createStatement().execute("drop table chartest");
            } catch (SQLException e) {
                //                e.printStackTrace();
            }
            conn.createStatement().execute(
                "create table chartest(c1 char(1),c2 char(2),c3 char(10),c4 char(100))");
            rs = conn.createStatement().executeQuery("select * from chartest");
            resultSetMetaData = rs.getMetaData();
            System.out.println("char");
            Assert.assertEquals(1, resultSetMetaData.getPrecision(1));
            Assert.assertEquals(2, resultSetMetaData.getPrecision(2));
            Assert.assertEquals(10, resultSetMetaData.getPrecision(3));
            Assert.assertEquals(100, resultSetMetaData.getPrecision(4));

            try {
                conn.createStatement().execute("drop table nchartest");
            } catch (SQLException e) {
                //                e.printStackTrace();
            }
            conn.createStatement().execute(
                "create table nchartest(c1 nchar(1),c2 nchar(2),c3 nchar(10),c4 nchar(100))");
            rs = conn.createStatement().executeQuery("select * from nchartest");
            resultSetMetaData = rs.getMetaData();
            System.out.println("nchar");
            Assert.assertEquals(1, resultSetMetaData.getPrecision(1));
            Assert.assertEquals(2, resultSetMetaData.getPrecision(2));
            Assert.assertEquals(10, resultSetMetaData.getPrecision(3));
            Assert.assertEquals(100, resultSetMetaData.getPrecision(4));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testXAReconnect1() {
        Assume.assumeTrue(sharedUsePrepare());
        try {
            Connection conn = setConnection("&cachePrepStmts=true&autoReconnect=true");

            conn.createStatement().execute("set wait_timeout=5");

            JDBC4MysqlXAConnection mysqlXAConnection = new JDBC4MysqlXAConnection(
                (OceanBaseConnection) conn);
            String gtridStr = "gtrid_test_wgs_ob_oracle_xa_one_phase_reconn";
            String bqualStr = "bqual_test_wgs_ob_oracle_xa_one_phase_reconn";

            Xid xid = new MysqlXid(gtridStr.getBytes(), bqualStr.getBytes(), 123);
            try {
                mysqlXAConnection.start(xid, XAResource.TMNOFLAGS);
                // ps test
                PreparedStatement pstmt = null;
                ResultSet rs = null;
                pstmt = conn.prepareStatement("select 1 from dual");
                rs = pstmt.executeQuery();

                while (rs.next()) {
                    System.out.println(rs.getInt(1));
                }

                pstmt.close();

                mysqlXAConnection.end(xid, XAResource.TMSUCCESS);
                mysqlXAConnection.prepare(xid);
                mysqlXAConnection.commit(xid, false);
                Thread.sleep(7000);
                try {
                    conn.createStatement().execute("select 1 from dual");
                } catch (Exception e) {
                    //throw
                }
                mysqlXAConnection.start(xid, XAResource.TMNOFLAGS);
            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            }
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testXAReconnect2() throws SQLException, InterruptedException {
        Assume.assumeTrue(sharedUsePrepare());
        Connection conn = setConnection("&cachePrepStmts=true&autoReconnect=true");
        Statement stmt = conn.createStatement();
        PreparedStatement ps = conn.prepareStatement("select 1 from dual");
        ResultSet rs = ps.executeQuery();
        stmt.execute("SET SESSION wait_timeout = 4");
        Thread.sleep(5000);
        try {
            stmt.execute("select 3 from dual");// reconnect
        } catch (SQLNonTransientConnectionException e) {
            e.printStackTrace();
            Assert.fail("Must have send a SQLTransientConnectionException !");
        } catch (SQLTransientConnectionException e) {
            try {
                stmt.execute("select 4 from dual");
                ps = conn.prepareStatement("select 1 from dual");
                rs = ps.executeQuery();
            } catch (SQLException ee) {
                ee.printStackTrace();
                Assert.fail("Must have reconnect automatically !");
            }
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail("Must have reconnect automatically !");
        }
    }

    //When rewriteBatchedStatements=true, the length of updateCounts is always equal to the number of addBatch
    @Test
    public void testContinueBatchOnErrorFalse() {
        Assume.assumeTrue(sharedUsePrepare());
        try {
            int count = 2;
            while (count > 0) {
                Connection conn = setConnectionOrigin("?continueBatchOnError=false&rewriteBatchedStatements=false&useServerPrepStmts="
                                                      + (count == 2 ? "false" : "true"));
                Statement stmt = conn.createStatement();
                try {
                    stmt.execute("drop table rewriteErrors");
                } catch (SQLException e) {
                    //            e.printStackTrace();
                }
                stmt.execute("create table rewriteErrors (field1 int not null primary key)");
                PreparedStatement ps = conn
                    .prepareStatement("INSERT INTO rewriteErrors VALUES (?)");
                int a = 5;
                ps.setInt(1, a);
                ps.addBatch();
                int num = 10;
                for (int i = 1; i <= num; i++) {
                    ps.setInt(1, i);
                    ps.addBatch();
                }
                try {
                    int[] counts1 = ps.executeBatch();
                } catch (BatchUpdateException e) {
                    int[] counts = e.getUpdateCounts();
                    Assert.assertEquals(a, counts.length);
                }
                ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM rewriteErrors");
                rs.next();
                Assert.assertEquals(a, rs.getInt(1));
                conn.close();
                count--;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testBug38422522() {
        Assume.assumeTrue(sharedUsePrepare());
        try {
            Connection conn = setConnection();
            try {
                conn.createStatement().execute("drop table rewriteBatchLoop");
            } catch (SQLException e) {
                //                e.printStackTrace();
            }
            conn.createStatement().execute(
                "create table rewriteBatchLoop(c1 int,c2 int,c3 varchar2(20))");
            PreparedStatement ps = conn
                .prepareStatement("insert into rewriteBatchLoop values(?,?,?)");

            for (int j = 0; j < 2; j++) {
                for (int i = 0; i < 2; i++) {
                    ps.setInt(1, 1);
                    ps.setInt(2, 1);
                    ps.setString(3, "fsfsfsfs");
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testRefCursorResultSet() {
        try {
            Assume.assumeTrue(sharedUsePrepare());
            Connection conn = sharedConnection;
            Statement stmt = conn.createStatement();
            try {
                stmt.execute("drop table test_timestamp_cursor");
            } catch (SQLException e) {
            }
            stmt.execute("create table test_timestamp_cursor (test_timestamp DATE)");
            stmt.execute("insert into test_timestamp_cursor values (to_date('2021-12-24 14:45:56','yyyy-mm-dd hh24:mi:ss'))");
            stmt.execute("create or replace procedure p_sms_send( ref_cursor  out sys_refcursor) as\n"
                         + "v_sql       varchar2(2000);\n"
                         + "begin\n"
                         + "  v_sql := 'SELECT * FROM test_timestamp_cursor WHERE rownum<=1';\n"
                         + "  open ref_cursor for v_sql;\n" + "\n" + "end;");

            CallableStatement cs = conn.prepareCall("call P_SMS_SEND(?)");
            cs.registerOutParameter(1, Types.REF_CURSOR);
            cs.execute();
            ResultSet rs = (ResultSet) cs.getObject(1);
            Assert.assertTrue(rs.next());
            Timestamp t = rs.getTimestamp(1);

            ResultSet rs1 = stmt.executeQuery("select * from test_timestamp_cursor");
            Assert.assertTrue(rs1.next());
            Assert.assertEquals(rs1.getTimestamp(1).toString(), "2021-12-24 14:45:56.0");
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testBug38422522ForPrepExecute() {
        Assume.assumeTrue(sharedUsePrepare());
        try {
            Connection conn = setConnection("&usePieceData=true&rewriteBatchedStatements=true");
            try {
                conn.createStatement().execute("drop table rewriteBatchLoop");
            } catch (SQLException e) {
                //                e.printStackTrace();
            }
            conn.createStatement().execute(
                "create table rewriteBatchLoop(c1 int,c2 int,c3 varchar2(20))");
            PreparedStatement ps = conn
                .prepareStatement("insert into rewriteBatchLoop values(?,?,?)");

            for (int j = 0; j < 2; j++) {
                for (int i = 0; i < 2; i++) {
                    ps.setInt(1, 1);
                    ps.setInt(2, 1);
                    ps.setString(3, "fsfsfsfs");
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testContinueBatchOnErrorFalseWithPrepExecute() {
        try {
            Connection conn = setConnectionOrigin("?continueBatchOnError=false&rewriteBatchedStatements=false&useServerPrepStmts=true&useOraclePrepareExecute=true");
            Statement stmt = conn.createStatement();
            try {
                stmt.execute("drop table rewriteErrors");
            } catch (SQLException e) {
                //            e.printStackTrace();
            }
            stmt.execute("create table rewriteErrors (field1 int not null primary key)");
            PreparedStatement ps = conn.prepareStatement("INSERT INTO rewriteErrors VALUES (?)");
            int a = 5;
            ps.setInt(1, a);
            ps.addBatch();
            int num = 10;
            for (int i = 1; i <= num; i++) {
                ps.setInt(1, i);
                ps.addBatch();
            }
            try {
                int[] counts1 = ps.executeBatch();
            } catch (BatchUpdateException e) {
                int[] counts = e.getUpdateCounts();
                Assert.assertEquals(a, counts.length);
            }
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM rewriteErrors");
            rs.next();
            Assert.assertEquals(a, rs.getInt(1));
            conn.close();
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testGetUsernameNpe() {
        try {
            String userName = sharedConnection.getMetaData().getUserName();
            Assert.assertEquals("TEST", userName);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    // prepareExecute protocol ref_cursor error
    @Test
    public void testNewProcedureCursor() {
        Assume.assumeTrue(sharedUsePrepare());
        try {
            Connection conn = setConnection("&useOraclePrepareExecute=true");
            String sql = "create or replace procedure pro1( b out sys_refcursor) is begin open b for select 1 from dual; end;";
            conn.createStatement().execute(sql);
            CallableStatement cstmt = conn.prepareCall("{call pro1(?)}");
            cstmt.registerOutParameter(1, Types.REF_CURSOR);
            cstmt.execute();
            ResultSet resultSet = (ResultSet) cstmt.getObject(1);
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(1, resultSet.getInt(1));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testNVARCHAR2() {
        try {
            Statement stmt = sharedConnection.createStatement();
            stmt.execute("insert into " + nvarchar2Test + " values('stringval')");
            ResultSet rs = stmt.executeQuery("select * from " + nvarchar2Test);
            Assert.assertTrue(rs.next());
            Object obj = rs.getObject(1);
            Assert.assertEquals("stringval", (String) obj);

        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testCursorPositionError1() {
        Assume.assumeTrue(sharedUsePrepare());
        try {
            Connection conn = sharedConnection;
            String sql = "create or replace procedure pro1( ref_param  out sys_refcursor,param2 out int) is begin open ref_param  for select 1 ,2 from dual; param2 := 100; end;";
            conn.createStatement().execute(sql);
            CallableStatement cstmt = conn.prepareCall("{call pro1(?,?)}");
            cstmt.registerOutParameter(1, Types.REF_CURSOR);
            cstmt.registerOutParameter(2, Types.INTEGER);
            cstmt.execute();
            ResultSet resultSet = (ResultSet) cstmt.getObject(1);
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(1, resultSet.getInt(1));
            int param2Value = cstmt.getInt(2);
            Assert.assertEquals(100, param2Value);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // refcursor not supports OCI_FETCH_ABSOLUTE now
    @Ignore
    public void testCursorPositionError2() {
        Assume.assumeTrue(sharedUsePrepare());
        try {
            Connection conn = sharedConnection;
            Statement stmt = conn.createStatement();
            try {
                stmt.execute("drop table cursror_table ");
            } catch (SQLException e) {
            }
            stmt.execute("create table cursror_table(c1 int)");
            for (int i = 1; i < 100; i++) {
                conn.createStatement().execute("insert into cursror_table values(" + i + ")");
            }
            String sql = "create or replace procedure pro1( ref_param  out sys_refcursor,param2 out int) is begin open ref_param  for select *  from cursror_table; param2 := 100; end;";
            conn.createStatement().execute(sql);
            CallableStatement cstmt = conn.prepareCall("{call pro1(?,?)}");
            cstmt.registerOutParameter(1, Types.REF_CURSOR);
            cstmt.registerOutParameter(2, Types.INTEGER);
            cstmt.execute();
            ResultSet resultSet = (ResultSet) cstmt.getObject(1);
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(1, resultSet.getInt(1));
            resultSet = (ResultSet) cstmt.getObject(1);
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(2, resultSet.getInt(1));
            int param2Value = cstmt.getInt(2);
            Assert.assertEquals(100, param2Value);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testSameProcedureRefCursor() {
        Assume.assumeTrue(sharedUsePrepare());
        try {
            Connection conn = sharedConnection;
            Statement stmt = conn.createStatement();
            String sql = "create or replace procedure pro22(b out sys_refcursor) is begin open b for select 1 from dual; end;";
            stmt.execute(sql);
            CallableStatement cstmt = conn.prepareCall("{call pro22(?)}");
            cstmt.registerOutParameter(1, Types.REF_CURSOR);
            cstmt.execute();
            ResultSet resultSet = (ResultSet) cstmt.getObject(1);
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(1, resultSet.getInt(1));

            sql = "create or replace procedure pro22(b out sys_refcursor) is begin open b for select 1 from dual; end;";
            stmt.execute(sql);
            cstmt = conn.prepareCall("{call pro22(?)}");
            cstmt.registerOutParameter(1, Types.REF_CURSOR);
            cstmt.execute();
            ResultSet rs = (ResultSet) cstmt.getObject(1);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));

        } catch (SQLException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testArray() {
        Assume.assumeTrue(sharedUsePrepare());
        try {
            Connection conn = setConnectionOrigin("?useServerPrepStmts=true&useOraclePrepareExecute=true");
            Statement stmt = conn.createStatement();
            stmt.execute("create or replace type array01 is varray(2) of number");
            stmt.execute("create or replace procedure t_array123 (arr out array01) is begin "
                         + "arr:= array01(1, 2); end;");
            CallableStatement cs = conn.prepareCall("{call t_array123(?)}");
            cs.registerOutParameter(1, Types.ARRAY, "ARRAY01");
            cs.execute();
            Array array = cs.getArray(1);
            Object[] object = (Object[]) array.getArray();
            Assert.assertEquals(BigDecimal.valueOf(1), object[0]);
            Assert.assertEquals(BigDecimal.valueOf(2), object[1]);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testTrigger() {
        try {
            Connection conn = sharedConnection;
            try {
                conn.createStatement().execute("drop table AUTHORITIES");
            } catch (SQLException e) {
                //                e.printStackTrace();
            }
            conn.createStatement().execute("create table AUTHORITIES(id int)");
            String sql = "create or replace TRIGGER AUTHORITIES_ID_TRI BEFORE\n"
                         + "INSERT ON \"AUTHORITIES\"\n"
                         + "REFERENCING NEW AS \"new\" OLD AS \"old\"\n" + "FOR EACH ROW\n"
                         + "ENABLE\n" + "WHEN (new.id is null)\n" + "BEGIN\n"
                         + "select  AUTHORITIES_ID_SEQ.nextval into :new.id from dual;\n"
                         + "END;\n";
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.execute();
            Statement stmt = conn.createStatement();
            stmt.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testTrigger2() {
        Assume.assumeTrue(sharedUsePrepare());
        try {
            Connection conn = setConnectionOrigin("?useServerPrepStmts=true&useOraclePrepareExecute=true");
            try {
                conn.createStatement().execute("drop table AUTHORITIES");
            } catch (SQLException e) {
                //                e.printStackTrace();
            }
            conn.createStatement().execute("create table AUTHORITIES(id int)");
            String sql = "create or replace TRIGGER AUTHORITIES_ID_TRI  BEFORE\n"
                         + "INSERT ON \"AUTHORITIES\"\n"
                         + "REFERENCING NEW AS \"new\" OLD AS \"old\"\n" + "FOR EACH ROW\n"
                         + "ENABLE\n" + "WHEN (new.id is null)\n" + "BEGIN\n"
                         + "select  AUTHORITIES_ID_SEQ.nextval into :new.id from dual;\n"
                         + "END;\n";
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.execute();
            Statement stmt = conn.createStatement();
            stmt.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testPrepExecuteLargeUpdate() {
        Assume.assumeTrue(sharedUsePrepare());
        try {
            Connection conn = setConnectionOrigin("?useServerPrepStmts=true&useOraclePrepareExecute=true");
            Statement stmt = conn.createStatement();
            createTable("testPrepExecuteLargeUpdate", "c1 int");

            PreparedStatement ps = conn
                .prepareStatement("INSERT INTO testPrepExecuteLargeUpdate VALUES (?),(?)");
            ps.setInt(1, 1);
            ps.setInt(2, 2);

            long count = ps.executeLargeUpdate();
            assertEquals(2, count);

            ResultSet rs = stmt.executeQuery("select * from testPrepExecuteLargeUpdate");
            int j = 1;
            while (rs.next()) {
                assertEquals(j++, rs.getInt(1));
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testCallStmtExecuteLargeUpdate() {
        Assume.assumeTrue(sharedUsePrepare());
        try {
            Connection conn = setConnectionOrigin("?useServerPrepStmts=true&useOraclePrepareExecute=true");
            Statement stmt = conn.createStatement();
            createTable("testCallStmtLargeUpdate", "c1 int");
            createProcedure(
                "testCallStmtLargeUpdateProc",
                "(n1 int, n2 int, n3 int, n4 int) is BEGIN "
                        + " INSERT INTO testCallStmtLargeUpdate VALUES (n1), (n2), (n3), (n4); end ");

            CallableStatement cstmt = conn
                .prepareCall("{CALL testCallStmtLargeUpdateProc(?, ?, ?, ?)}");
            cstmt.setInt(1, 1);
            cstmt.setInt(2, 2);
            cstmt.setInt(3, 3);
            cstmt.setInt(4, 4);

            long count = cstmt.executeLargeUpdate();
            assertEquals(0, count);

            ResultSet rs = stmt.executeQuery("select * from testCallStmtLargeUpdate");
            int j = 1;
            while (rs.next()) {
                assertEquals(j++, rs.getInt(1));
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testCursorSPD() {
        Assume.assumeTrue(sharedUsePrepare());
        try {
            Connection conn = sharedConnection;
            Statement stmt = conn.createStatement();
            try {
                stmt.execute("drop table testcur1");
            } catch (SQLException e) {
                //                e.printStackTrace();
            }
            stmt.execute("create  table testcur1(c1 int,c2 int)");
            PreparedStatement ps1 = conn.prepareStatement("insert into testcur1 values(?,?)");
            for (int i = 0; i < 30; i++) {
                ps1.setInt(1, i);
                ps1.setInt(2, i);
                ps1.addBatch();
            }
            ps1.executeBatch();
            PreparedStatement ps = conn.prepareStatement("select * from testcur1");
            ResultSet rs = ps.executeQuery();
            int cur = 0;
            int row = rs.getRow();
            Assert.assertEquals(cur, row);
            boolean ret = rs.next();
            int i = 0;
            while (ret) {
                cur++;
                row = rs.getRow();
                Assert.assertEquals(cur, row);
                Assert.assertEquals(rs.getInt(1), i++);
                ret = rs.next();
            }
            Assert.assertEquals(30, i);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testCursorSPD2() {
        Assume.assumeTrue(sharedUsePrepare());
        try {
            Connection conn = sharedConnection;
            Statement stmt = conn.createStatement();
            try {
                stmt.execute("drop table testcur1");
            } catch (SQLException e) {
                //                e.printStackTrace();
            }

            stmt.execute("create  table testcur1(c1 int,c2 int)");
            PreparedStatement ps1 = conn.prepareStatement("insert into testcur1 values(?,?)");
            for (int i = 0; i < 30; i++) {
                ps1.setInt(1, i);
                ps1.setInt(2, i);
                ps1.addBatch();
            }
            ps1.executeBatch();
            String sql = "create or replace procedure pro1( b out sys_refcursor) is begin open b for select * from testcur1; end;";
            conn.createStatement().execute(sql);
            CallableStatement cstmt = conn.prepareCall("{call pro1(?)}");
            cstmt.registerOutParameter(1, Types.REF_CURSOR);
            cstmt.executeQuery();
            ResultSet rs = (ResultSet) cstmt.getObject(1);
            int cur = 0;
            int row = rs.getRow();
            Assert.assertEquals(cur, row);
            boolean ret = rs.next();
            int i = 0;
            while (ret) {
                cur++;
                row = rs.getRow();
                Assert.assertEquals(cur, row);
                Assert.assertEquals(rs.getInt(1), i++);
                ret = rs.next();
            }
            Assert.assertEquals(30, i);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testAone39114534() {
        try {
            Connection conn = setConnection("&useOraclePrepareExecute=false");
            createFunction("testFunctionCall",
                "(a float, b NUMBER, c int) RETURN INT IS\nBEGIN\n RETURN a + b + c;\nEND;");
            CallableStatement cs = conn.prepareCall("? = CALL testFunctionCall(?,?,?)");

            cs.registerOutParameter(1, Types.INTEGER);
            cs.setFloat(2, 1);
            cs.setInt(3, 2);
            cs.setInt(4, 3);
            assertEquals(0, cs.executeUpdate()); // &useOraclePrepareExecute=false
            assertNull(cs.getResultSet());
            assertEquals(6f, cs.getInt(1), .001);

            cs.registerOutParameter(1, Types.INTEGER);
            cs.setFloat(2, 2);
            cs.setInt(3, 3);
            cs.setInt(4, 4);
            assertEquals(0, cs.executeUpdate()); // &useOraclePrepareExecute=false
            assertEquals(9f, cs.getInt(1), .001);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testCallStmtBatchSqlBug36022265_Oracle() throws Exception {
        try {
            Connection conn = sharedConnection;
            Statement stmt = conn.createStatement();
            try {
                stmt.execute("drop table testCallaStmtBatch");
            } catch (SQLException throwables) {
                //                throwables.printStackTrace();
            }
            stmt.execute("create table testCallaStmtBatch (id int , c1 INT)");
            stmt.execute("drop procedure if exists testCallaStmtBatchPro");
            stmt.execute("create procedure testCallaStmtBatchPro(n IN  INT) IS BEGIN INSERT INTO testCallaStmtBatch (id,c1) VALUES (n,n); END");

            CallableStatement cs = conn.prepareCall("{CALL testCallaStmtBatchPro(?)}");
            cs.setInt(1, 1);
            cs.addBatch();
            cs.setInt(1, 2);
            cs.addBatch();
            cs.addBatch("{CALL testCallaStmtBatchPro(3)}");
            cs.addBatch("{CALL testCallaStmtBatchPro(4)}");
            cs.addBatch("{CALL testCallaStmtBatchPro(5)}");

            long[] counts = cs.executeLargeBatch();
            System.out.println(Arrays.toString(counts));
            assertEquals(5, counts.length);

            ResultSet rs = cs.getGeneratedKeys();
            ResultSetMetaData rsmd = rs.getMetaData();
            assertEquals(1, rsmd.getColumnCount());
            assertEquals(Types.BIGINT, rsmd.getColumnType(1));
            rs = conn.createStatement().executeQuery("select count(*) from testCallaStmtBatch");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(5, rs.getInt(1));
        } catch (SQLException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testGetObjectTimestampBug39354335() {
        try {
            PreparedStatement ps = sharedConnection.prepareStatement("select ? from dual");
            Timestamp timestamp = Timestamp.valueOf("2020-12-12 12:12:12.333");
            ps.setTimestamp(1, timestamp);
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals(timestamp, rs.getTimestamp(1));
            assertEquals(String.valueOf(timestamp), rs.getObject(1, String.class));
            assertEquals(timestamp, rs.getObject(1, Timestamp.class));

            try {
                rs = sharedConnection.createStatement().executeQuery(
                    "select '2020-12-12 12:12:12.333' from dual");
                assertTrue(rs.next());
                assertEquals(String.valueOf(timestamp), rs.getObject(1, String.class));
                assertEquals(timestamp, rs.getTimestamp(1));
                assertEquals(timestamp, rs.getObject(1, Timestamp.class));
            } catch (AssertionError e) {
                e.printStackTrace();
            }

            try {
                rs = sharedConnection
                    .prepareStatement("select '2020-12-12 12:12:12.333' from dual").executeQuery();
                assertTrue(rs.next());
                assertEquals(String.valueOf(timestamp), rs.getObject(1, String.class));
                assertEquals(timestamp, rs.getTimestamp(1));
                assertEquals(timestamp, rs.getObject(1, Timestamp.class));
            } catch (AssertionError e) {
                e.printStackTrace();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            fail();
        }
    }

    // test for aone 40467149
    @Test
    public void testLobOracle() {
        //        Assume.assumeTrue(sharedUsePrepare());
        try {
            Connection conn = setConnection("&usePieceData");
            createTable("testLob", "c1 clob, c2 blob");
            String sql = "insert into testLob values (?, ?)";
            PreparedStatement ps = conn.prepareStatement(sql);
            String str = "abcABC中文";
            java.sql.Clob clob = new com.oceanbase.jdbc.Clob();
            Writer writer = clob.setCharacterStream(1);
            writer.write(str, 0, 8);
            writer.flush();
            byte[] bytes = str.getBytes();
            Blob blob = new Blob(bytes);

            ps.setClob(1, clob);
            ps.setBlob(2, blob);
            assertEquals(1, ps.executeUpdate());
            ps.setClob(1, clob);
            ps.setBlob(2, blob);
            assertEquals(1, ps.executeUpdate());

            ResultSet rs = conn.prepareStatement("select * from testLob").executeQuery();
            assertTrue(rs.next());
            assertEquals(str, rs.getString(1));
            assertEquals(new String(bytes), new String(rs.getBytes(2)));
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testLobOracle2() {
        try {
            Connection conn = setConnection("&usePieceData");
            createTable("testLobOracle_2", "c1 clob, c2 blob");
            String sql = "insert into testLobOracle_2 values (?, ?)";
            PreparedStatement ps = conn.prepareStatement(sql);
            String str = "abcABC中文";
            java.sql.Clob clob = new com.oceanbase.jdbc.Clob();
            Writer writer = clob.setCharacterStream(1);
            writer.write(str, 0, 8);
            writer.flush();
            byte[] bytes = str.getBytes();
            Blob blob = new Blob(bytes);

            ps.setClob(1, clob);
            ps.setBlob(2, blob);
            assertEquals(1, ps.executeUpdate());

            ResultSet rs = conn.prepareStatement("select * from testLobOracle_2").executeQuery();
            assertTrue(rs.next());
            java.sql.Clob clob1 = rs.getClob(1);
            java.sql.Blob blob1 = rs.getBlob(2);
            conn.createStatement().execute("delete from testLobOracle_2");
            ps = conn.prepareStatement(sql);
            ps.setClob(1, clob1);
            ps.setBlob(2, blob1);
            assertEquals(1, ps.executeUpdate());
            ps.setClob(1, clob1);
            ps.setBlob(2, blob1);
            assertEquals(1, ps.executeUpdate());

            rs = conn.prepareStatement("select * from testLobOracle_2").executeQuery();
            assertTrue(rs.next());
            assertEquals(str, rs.getString(1));
            assertEquals(new String(bytes), new String(rs.getBytes(2)));
            assertTrue(rs.next());
            assertEquals(str, rs.getString(1));
            assertEquals(new String(bytes), new String(rs.getBytes(2)));
            System.out.println("test finished ");
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testClob() {
        try {
            createTable("t_clob1", "c1 clob");
            Connection conn = setConnectionOrigin("?useServerPrepStmts=true&usePieceData=true");
            PreparedStatement ps = conn.prepareStatement("insert into t_clob1 values(?)");
            String str = "123abc";
            //            Clob clob = (Clob) conn.createClob();
            com.oceanbase.jdbc.Clob clob = new Clob();
            Writer writer = clob.setCharacterStream(1);
            writer.write(str);
            writer.flush();
            ps.setClob(1, clob);
            ps.execute();

            ps = conn.prepareStatement("select * from t_clob1");
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            Reader reader = rs.getCharacterStream(1);
            StringBuilder sb = new StringBuilder();
            int ch;
            while ((ch = reader.read()) != -1) {
                sb.append((char) ch);
            }
            assertEquals(str, sb.toString());

        } catch (Throwable e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testClob2() {
        try {
            createTable("t_clob1", "c1 clob, c2 clob");
            Connection conn = setConnectionOrigin("?useServerPrepStmts=true&usePieceData=true");
            PreparedStatement ps = conn.prepareStatement("insert into t_clob1 values(?, ?)");
            String str = "中文abc";
            com.oceanbase.jdbc.Clob clob = new Clob();
            Writer writer = clob.setCharacterStream(1);
            writer.write(str);
            writer.flush();
            ps.setClob(1, clob);
            ps.setClob(2, clob);
            ps.execute();

            ps = conn.prepareStatement("select * from t_clob1");
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            Reader reader = rs.getCharacterStream(1);
            StringBuilder sb = new StringBuilder();
            int ch;
            while ((ch = reader.read()) != -1) {
                sb.append((char) ch);
            }
            assertEquals(str, sb.toString());
            assertEquals(str, rs.getString(2));
        } catch (Throwable e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testBlob() {
        try {
            createTable("t_blob1", "c1 blob, c2 blob");
            Connection conn = setConnectionOrigin("?useServerPrepStmts=true&usePieceData=true");
            PreparedStatement ps = conn.prepareStatement("insert into t_blob1 values(?, ?)");
            String str = "中文abc";
            byte[] bytes = str.getBytes();
            com.oceanbase.jdbc.Blob blob = new Blob(bytes);

            ps.setBlob(1, blob);
            InputStream stream = new ByteArrayInputStream(bytes);
            ps.setBinaryStream(2, stream);
            ps.execute();

            ps = conn.prepareStatement("select * from t_blob1");
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());

            assertEquals(new String(bytes), new String(rs.getBytes(1)));
            assertEquals(new String(bytes), new String(rs.getBytes(2)));
        } catch (Throwable e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testRefCursorAnonymousServerBug39388375() throws ClassNotFoundException,
                                                         SQLException {

        Connection conn = sharedConnection;
        String sql = "create or replace procedure proVarCursor(b out sys_refcursor, a out varchar2) is begin open b for select 1 from dual; a:= 'bbb'; end;";
        conn.createStatement().execute(sql);
        //CallableStatement cstmt = conn.prepareCall("call proVarCursor(?,?)");
        //CallableStatement cstmt = conn.prepareCall("{call proVarCursor(?,?)}");
        CallableStatement cstmt = conn.prepareCall("begin proVarCursor(?,?); end");
        cstmt.registerOutParameter(1, Types.REF_CURSOR);
        cstmt.registerOutParameter(2, Types.VARCHAR);
        cstmt.execute();
        assertEquals("bbb", cstmt.getString(2));

        ResultSet rs = (ResultSet) cstmt.getObject(1);
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertFalse(rs.next());
        assertFalse(rs.next());
        try {
            int i = rs.getInt(1);
            fail();
        } catch (SQLException e) {
            Assert.assertEquals("Current position is after the last row", e.getMessage());
        }
    }

    @Test
    public void oracleTimeTypeToString() {
        try {
            Connection conn = sharedConnection;
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt
                .executeQuery("select to_date('2022-02-02 15:30:20','yyyy-MM-DD hh24:MI:SS') from dual");
            Assert.assertTrue(rs.next());
            Assert.assertEquals("2022-02-02 15:30:20", rs.getString(1));
            rs = stmt
                .executeQuery("select to_timestamp('2022-02-02 15:30:20.000000','yyyy-MM-DD hh24:MI:SS.FF') from dual");
            Assert.assertTrue(rs.next());
            Assert.assertEquals("2022-02-02 15:30:20", rs.getString(1));
            rs = stmt
                .executeQuery("select to_timestamp('2022-02-02 15:30:20.123000','yyyy-MM-DD hh24:MI:SS.FF') from dual");
            Assert.assertTrue(rs.next());
            Assert.assertEquals("2022-02-02 15:30:20.123", rs.getString(1));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testProcErrorAone40384433() {
        try {
            Connection conn = sharedConnection;
            Statement stmt = conn.createStatement();
            try {
                stmt.execute("drop table  t");
            } catch (SQLException throwables) {
            }
            stmt.execute("create table t(col int primary key)");
            stmt.execute("create or replace procedure proc is " + "begin"
                         + "  insert into t values(1);" + "  insert into t values(1);" + "end;");
            PreparedStatement ps = conn.prepareStatement("call proc()");
            ps.execute();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            Assert.assertTrue(throwables.getMessage().contains("line : 1, col : 5"));
        }
    }

    @Test
    public void testRefCursorResultSetClose() {
        try {
            Connection conn = sharedConnection;
            Statement stmt = conn.createStatement();
            try {
                stmt.execute("drop table test_ref_cursor");
            } catch (SQLException e) {
            }
            try {
                stmt.execute("drop procedure p_sms_send");
            } catch (SQLException e) {
            }

            stmt.execute("create table test_ref_cursor (c1  int ,c2 varchar2(200))");
            for (int i = 0; i < 100; i++) {
                stmt.execute("insert into test_ref_cursor values (1,'sfsfsfs')");

            }
            stmt.execute("create or replace procedure p_sms_send( ref_cursor  out sys_refcursor) as\n"
                         + "v_sql       varchar2(2000);\n"
                         + "begin\n"
                         + "  v_sql := 'SELECT * FROM test_ref_cursor ';\n"
                         + "  open ref_cursor for v_sql;\n" + "\n" + "end;");

            CallableStatement cs = conn.prepareCall("call P_SMS_SEND(?)");
            cs.registerOutParameter(1, Types.REF_CURSOR);
            cs.execute();
            ResultSet rs = (ResultSet) cs.getObject(1);
            rs.close();
            int count = 1;
            try {
                while (rs.next()) {
                    count++;
                }
            } catch (SQLException throwables) {
                Assert.assertTrue(throwables.getMessage().contains(
                    "Operation not permit on a closed resultSet"));
            }
            Assert.assertEquals(1, count);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testUpdateCounts() {
        try {
            Connection conn = setConnection("&continueBatchOnError=false");
            Statement stmt = conn.createStatement();
            try {
                stmt.execute("drop table blobtest1");
            } catch (SQLException e) {
                //                throw new RuntimeException(e);
            }
            int paramCount = 100;
            int batchTimes = 1000;

            try {
                StringBuilder sb = new StringBuilder("create table blobtest1(c1 int");
                for (int i = 2; i <= paramCount; i++) {
                    sb.append(",c" + i + " int");
                }
                sb.append(")");
                System.out.println("sb.toString() = " + sb.toString());
                stmt.execute(sb.toString());
            } catch (Exception e) {
                // e.printStackTrace();
            }
            StringBuilder sb = new StringBuilder("insert into blobtest1 values(?");
            for (int i = 2; i <= paramCount; i++) {
                sb.append(",?");
            }
            sb.append(")");
            System.out.println("sb.toString() = " + sb.toString());
            PreparedStatement ps = conn.prepareStatement(sb.toString());
            for (int i = 0; i < batchTimes; i++) {
                for (int j = 1; j <= paramCount; j++) {
                    if (i == 0 && j == 5) {
                        ps.setString(j, "s");
                    } else {
                        ps.setInt(j, j + 1);
                    }
                }

                ps.addBatch();
            }
            try {
                int ret[] = ps.executeBatch();
            } catch (BatchUpdateException e) {
                int counts[] = e.getUpdateCounts();
                Assert.assertEquals(0, counts.length);
            }
            ResultSet rs = stmt.executeQuery("select count(*) from blobtest1");
            rs.next();
            Assert.assertEquals(0, rs.getInt(1));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testUpdateCounts2() {
        Assume.assumeFalse(sharedUsePrepare());
        try {
            for (int t = 0; t < 2; t++) {
                Connection conn = setConnection("&continueBatchOnError=false&rewriteBatchedStatements=true&rewriteInsertByMultiQueries="
                                                + (t == 0 ? "true" : "false"));
                Statement stmt = conn.createStatement();
                try {
                    stmt.execute("drop table blobtest1");
                } catch (SQLException e) {
                    //                throw new RuntimeException(e);
                }
                int paramCount = 100;
                int batchTimes = 1000;

                try {
                    StringBuilder sb = new StringBuilder("create table blobtest1(c1 int");
                    for (int i = 2; i <= paramCount; i++) {
                        sb.append(",c" + i + " int");
                    }
                    sb.append(")");
                    System.out.println("sb.toString() = " + sb.toString());
                    stmt.execute(sb.toString());
                } catch (Exception e) {
                    // e.printStackTrace();
                }
                StringBuilder sb = new StringBuilder("insert into blobtest1 values(?");
                for (int i = 2; i <= paramCount; i++) {
                    sb.append(",?");
                }
                sb.append(")");
                System.out.println("sb.toString() = " + sb.toString());
                PreparedStatement ps = conn.prepareStatement(sb.toString());
                for (int i = 0; i < batchTimes; i++) {
                    for (int j = 1; j <= paramCount; j++) {
                        if (i == 0 && j == 5) {
                            ps.setString(j, "s");
                        } else {
                            ps.setInt(j, j + 1);
                        }
                    }

                    ps.addBatch();
                }
                try {
                    int ret[] = ps.executeBatch();
                } catch (BatchUpdateException e) {
                    int counts[] = e.getUpdateCounts();
                    if (t == 0) {
                        Assert.assertEquals(0, counts.length);
                    } else {
                        Assert.assertEquals(999, counts.length);
                    }
                }
                ResultSet rs = stmt.executeQuery("select count(*) from blobtest1");
                rs.next();
                Assert.assertEquals(0, rs.getInt(1));

                stmt.execute("delete from blobtest1");

                ps = conn.prepareStatement(sb.toString());
                for (int i = 0; i < batchTimes; i++) {
                    for (int j = 1; j <= paramCount; j++) {
                        if (i == 5 && j == 5) {
                            ps.setString(j, "s");
                        } else {
                            ps.setInt(j, j + 1);
                        }
                    }

                    ps.addBatch();
                }
                try {
                    int ret[] = ps.executeBatch();
                } catch (BatchUpdateException e) {
                    int counts[] = e.getUpdateCounts();
                    if (t == 0) {
                        Assert.assertEquals(5, counts.length);
                    } else {
                        Assert.assertEquals(999, counts.length);
                    }
                }
                rs = stmt.executeQuery("select count(*) from blobtest1");
                rs.next();
                if (t == 0) {
                    Assert.assertEquals(5, rs.getInt(1));
                } else {
                    Assert.assertEquals(0, rs.getInt(1));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testRemarksAndCaseIdentifiers() {
        try {
            Connection conn = sharedConnection;
            DatabaseMetaData metaData = conn.getMetaData();
            Assert.assertEquals(true, metaData.storesUpperCaseIdentifiers());
            Assert.assertEquals(false, metaData.storesLowerCaseIdentifiers());
            Assert.assertEquals(false, metaData.storesMixedCaseIdentifiers());
            try {
                conn.createStatement().execute("drop table testcolumns");
            } catch (SQLException e) {
            }
            conn.createStatement().execute("create table testcolumns (c1 int)");
            conn.createStatement().execute("COMMENT on  column TESTCOLUMNS.C1 is 'column comment'");
            ((OceanBaseConnection) conn).setRemarksReporting(true);
            ResultSet rs = metaData.getColumns(null, null, "testcolumns".toUpperCase(), "%");
            Assert.assertTrue(rs.next());
            Assert.assertEquals("column comment", rs.getString("REMARKS"));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void fix44512883() throws SQLException {
        Assume.assumeTrue(sharedUsePrepare());
        try {
            Connection conn = sharedConnection;
            try {
                conn.createStatement().execute("drop table t0");
            } catch (Exception e) {
            }
            conn.createStatement().execute("create table t0 (c0 varchar2(10))");
            conn.createStatement().execute("insert into t0 values('+-*-')");
            for (int i = 0; i < 60; i++) {
                System.out.println("\nindex:" + i);
                Statement s = conn.createStatement();
                try {
                    s.executeQuery("SELECT sum(T0.C0) from t0");
                } catch (SQLException e) {
                    System.out.println(e.getMessage());
                } finally {
                    System.out.println("s.close()");
                    s.close();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testArrayBindingSetNull() {
        Assume.assumeTrue(sharedUsePrepare());
        try {
            Connection conn = setConnectionOrigin("?useServerPrepStmts=true&useArrayBinding=true&usePieceData=true");
            Statement stmt = conn.createStatement();
            try {
                stmt.execute("drop table t1");
            } catch (SQLException e) {
            }

            //execute the test sql.
            stmt.execute("create table t1(c timestamp)");
            conn.setAutoCommit(false);
            PreparedStatement ps1 = conn.prepareStatement("insert into t1 values(?)");

            ps1.setNull(1, java.sql.Types.TIMESTAMP);
            ps1.addBatch();
            ps1.setNull(1, java.sql.Types.TIMESTAMP);
            ps1.addBatch();
            ps1.executeBatch();
            conn.commit();

        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void createDatabaseIfNotExistTest() {
        try {
            Connection conn = setConnection("&createDatabaseIfNotExist=true");
            ResultSet rs = conn.createStatement().executeQuery(
                "select sys_context('userenv','current_schema') from dual");
            rs.next();
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testGetColumnsNullable() {
        try {
            createTable("test_nullable", "c1 varchar2(36), c2 number not null");
            DatabaseMetaData dMetaData = sharedConnection.getMetaData();
            ResultSet rs = dMetaData.getColumns(null, null, "test_nullable".toUpperCase(), null);
            assertTrue(rs.next());
            assertTrue("c1".equalsIgnoreCase(rs.getString("COLUMN_NAME")));
            assertEquals(1, rs.getInt("NULLABLE"));
            assertTrue(rs.next());
            assertTrue("c2".equalsIgnoreCase(rs.getString("COLUMN_NAME")));
            assertEquals(0, rs.getInt("NULLABLE"));
            assertFalse(rs.next());
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testOceanBaseFloat() {
        try {
            Connection conn = sharedConnection;
            Statement statement = conn.createStatement();
            try {
                statement.execute("drop table TEST_FLOAT_ORACLE");
            } catch (Exception e) {
                // eat exception
            } finally {
                statement.execute("CREATE TABLE TEST_FLOAT_ORACLE (c1 int,c2 float);");
            }
            String sql = "INSERT INTO TEST_FLOAT_ORACLE(C1,C2) VALUES (?,?)";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, "111");
            pstmt.setFloat(2, 3.1f);
            pstmt.executeUpdate();

            String querySql = "SELECT C2 FROM TEST_FLOAT_ORACLE WHERE C1 = ?";
            pstmt = conn.prepareStatement(querySql);
            pstmt.setInt(1, 111);
            ResultSet rs = pstmt.executeQuery();
            Assert.assertTrue(rs.next());
            final double THRESHOLD = .0001;
            Assert.assertTrue(Math.abs(rs.getDouble(1) - 3.1d) < THRESHOLD);
            Assert.assertTrue(Math.abs(rs.getFloat(1) - 3.1f) < THRESHOLD);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testNumericFloat() {
        try {
            Connection conn = sharedConnection;
            createTable("binaryfloattest", "c1 binary_float");
            Statement stmt = conn.createStatement();
            stmt.execute("insert into binaryfloattest values(1.5)");
            PreparedStatement ps = conn.prepareStatement("select c1 from binaryfloattest");
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            System.out.println("Type Name " + rs.getMetaData().getColumnTypeName(1));
            System.out.println("Type " + rs.getMetaData().getColumnType(1));
            assertEquals(1.5f, rs.getFloat(1), 0.01);
            assertEquals(1.5f, rs.getDouble(1), 0.01);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testClobReaderTwice() {
        try {
            Connection conn = setConnectionOrigin("?useServerPrepStmts=true&usePieceData=true");
            String clobTest3 = "t_clob" + getRandomString(5);

            createTable(clobTest3, "c1 clob, c2 clob, c3 clob, c4 clob, c5 clob, c6 clob");
            PreparedStatement ps = conn.prepareStatement("insert into " + clobTest3
                    + " values(?, ?, ?, ?, ?, ?)");

            String str = "1231abcd奥星贝斯";
            ps.setCharacterStream(1, new StringReader(str));
            ps.setCharacterStream(2, new StringReader("")); //(CLOB)null
            ps.setCharacterStream(3, null);
            ps.setClob(4, (Clob) null);
            ps.setClob(5, Clob.getEmptyCLOB()); // server hopes to receive empty string

            java.sql.Clob clob = conn.createClob();
            Writer writer = clob.setCharacterStream(1);
            writer.write(str);
            writer.flush();
            ps.setClob(6, clob); // server hopes to receive empty string
            ps.execute();

            ps = conn.prepareStatement("select * from " + clobTest3);
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals(rs.getString(1), "1231abcd奥星贝斯");
            assertEquals(rs.getString(2), null);
            assertEquals(rs.getString(3), null);
            assertEquals(rs.getString(4), null);
            assertEquals(rs.getString(5), "");
            assertEquals(rs.getString(6), "1231abcd奥星贝斯");
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testNumber() throws Exception {
        Connection conn = sharedConnection;
        Statement stmt = conn.createStatement();
        try {
            stmt.execute("drop table test");
        } catch (SQLException ignore) {} finally {
            stmt.execute("create table test(no_ps number, p_star number(*), p number(30),p_s number(10,5), no_p number(*,3))");
        }

        ResultSet rs = stmt.executeQuery("select no_ps, p_star, p, p_s, no_p, (no_p+1), (no_ps+1), (p_s+1) from test");
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();
        int i = 1;
        Assert.assertEquals("NO_PS", rsmd.getColumnName(i));
        Assert.assertEquals(0, rsmd.getScale(i));// oracle: -127
        Assert.assertEquals(0, rsmd.getPrecision(i++));
        Assert.assertEquals("P_STAR", rsmd.getColumnName(i));
        Assert.assertEquals(0, rsmd.getScale(i));// oracle: -127
        Assert.assertEquals(0, rsmd.getPrecision(i++));
        Assert.assertEquals("P", rsmd.getColumnName(i));
        Assert.assertEquals(0, rsmd.getScale(i));
        Assert.assertEquals(30, rsmd.getPrecision(i++));
        Assert.assertEquals("P_S", rsmd.getColumnName(i));
        Assert.assertEquals(5, rsmd.getScale(i));
        Assert.assertEquals(10, rsmd.getPrecision(i++));
        Assert.assertEquals("NO_P", rsmd.getColumnName(i));
        Assert.assertEquals(3, rsmd.getScale(i));
        Assert.assertEquals(38, rsmd.getPrecision(i++));
        Assert.assertEquals("(NO_P+1)", rsmd.getColumnName(i));
        Assert.assertEquals(0, rsmd.getScale(i));
        Assert.assertEquals(0, rsmd.getPrecision(i++));
        Assert.assertEquals("(NO_PS+1)", rsmd.getColumnName(i));
        Assert.assertEquals(0, rsmd.getScale(i));
        Assert.assertEquals(0, rsmd.getPrecision(i++));
        Assert.assertEquals("(P_S+1)", rsmd.getColumnName(i));
        Assert.assertEquals(0, rsmd.getScale(i));
        Assert.assertEquals(0, rsmd.getPrecision(i));
        Assert.assertEquals(i, columnCount);
        conn.close();
    }

    @Ignore
    public void testBinaryFloatBinaryDoubleFloatVarchar() throws Exception {
        Connection conn = sharedConnection;
        Statement stmt = conn.createStatement();
        try {
            stmt.execute("drop table t1");
        } catch (SQLException ignore) {} finally {
            stmt.execute("create table t1(bf_c binary_float, bd_c binary_double, float_c float, var_c varchar(10));");
        }

        ResultSet rs = stmt.executeQuery("select bf_c, bd_c, float_c, var_c from t1");
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();
        int i = 1;
        Assert.assertEquals("BF_C", rsmd.getColumnName(i));
        Assert.assertEquals(0, rsmd.getScale(i));
        Assert.assertEquals(0, rsmd.getPrecision(i++));
        Assert.assertEquals("BD_C", rsmd.getColumnName(i));
        Assert.assertEquals(0, rsmd.getScale(i));
        Assert.assertEquals(0, rsmd.getPrecision(i++));
        Assert.assertEquals("FLOAT_C", rsmd.getColumnName(i));
        Assert.assertEquals(-127, rsmd.getScale(i));
        Assert.assertEquals(126, rsmd.getPrecision(i++));
        Assert.assertEquals("VAR_C", rsmd.getColumnName(i));
        Assert.assertEquals(0, rsmd.getScale(i));
        Assert.assertEquals(10, rsmd.getPrecision(i));
        Assert.assertEquals(i, columnCount);
        conn.close();
    }

    @Test
    public void test2() throws SQLException {
        Connection conn = sharedConnection;
        Statement stmt = conn.createStatement();
        try {
            stmt.execute("drop table t0");
        } catch (SQLException ignore) {
        }
//        stmt.execute("create table t0(v0 number)");
//        stmt.execute("create table t0(v0 number(*))");
//        stmt.execute("create table t0(v0 number(5))");
        stmt.execute("create table t0(v0 number(*, 0))");

        ResultSet rs = stmt.executeQuery("select v0, v0+1, max(v0), sum(v0), cast(v0+to_number(5) as number) from t0 group by v0");
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();
        int i = 1;
        Assert.assertEquals("V0", rsmd.getColumnName(i));
        Assert.assertEquals(0, rsmd.getScale(i));
        Assert.assertEquals(0, rsmd.getPrecision(i++));
        Assert.assertEquals("V0+1", rsmd.getColumnName(i));
        Assert.assertEquals(0, rsmd.getScale(i));
        Assert.assertEquals(0, rsmd.getPrecision(i++));
        Assert.assertEquals("MAX(V0)", rsmd.getColumnName(i));
        Assert.assertEquals(0, rsmd.getScale(i));
        Assert.assertEquals(0, rsmd.getPrecision(i++));
        Assert.assertEquals("SUM(V0)", rsmd.getColumnName(i));
        Assert.assertEquals(0, rsmd.getScale(i));
        Assert.assertEquals(0, rsmd.getPrecision(i++));
        Assert.assertEquals("CAST(V0+TO_NUMBER(5)ASNUMBER)", rsmd.getColumnName(i));
        Assert.assertEquals(0, rsmd.getScale(i));
        Assert.assertEquals(0, rsmd.getPrecision(i));
        Assert.assertEquals(i, columnCount);
    }

}
