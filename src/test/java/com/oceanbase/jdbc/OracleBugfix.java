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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringReader;
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

import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

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
    static String testLargeStream           = "testLargeStream" + getRandomString(5);
    static String testLargeReader           = "testLargeReader" + getRandomString(5);
    static String pieceReuse                = "pieceReuse" + getRandomString(5);

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
        createTable(testNumberCol1, "c1 int");
        createTable(testNumberCol2, "c1 float");
        createTable(testNumberCol3, "c1 decimal");
        createTable(testNumberCol4, "c1 number");
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
        createTable(testLargeStream, "c1 blob,c2 varchar2(20)");
        createTable(testLargeReader, "c1 clob,c2 varchar2(20)");
        createTable(pieceReuse, "c1 clob, c2 number, c3 clob, c4 int");
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
            String nameList[] = { testNumberCol1, testNumberCol2, testNumberCol3, testNumberCol4 };
            Statement statement = sharedConnection.createStatement();
            ResultSet rs = null;
            for (int i = 0; i < nameList.length; i++) {
                String name = nameList[i];
                statement.execute("insert into " + name + " values(1) ");
                rs = statement.executeQuery("select c1 from " + name);
                Assert.assertTrue(rs.next());
                Object obj = rs.getObject(1);
                Assert.assertEquals(obj.getClass(), BigDecimal.class);
            }
        } catch (SQLException e) {
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
            Assert.fail();
            e.printStackTrace();
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

    @Test
    public void testDbWithQuotes() throws SQLException {
        Connection conn = null;
        try {
            String url = System.getProperty("dbWithQuotes");
            conn = DriverManager.getConnection(url);
            Statement statement = conn.createStatement();
            try {
                statement.execute("drop procedure  pl_t2");
            } catch (SQLException e) {
            }
            statement
                .execute("create procedure pl_t2 (listing in out varchar2) is begin listing := 'abc';  end pl_t2; ");
            CallableStatement callableStatement = conn.prepareCall("call pl_t2(?)");
            callableStatement.registerOutParameter(1, Types.VARCHAR);
            callableStatement.setString(1, "1212121");
            callableStatement.execute();
            Assert.assertEquals("abc", callableStatement.getString(1));
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
            String value = "1988-09-09 15:43:01.0";
            String dateValue = "1988-09-09";
            Assert.assertTrue(resultSet.next());
            String timeStampStr = resultSet.getString(2);
            Assert.assertEquals(value, timeStampStr);
            String timeStampStr2 = resultSet.getString(3);
            Assert.assertEquals(value, timeStampStr2);
            Timestamp ts = resultSet.getTimestamp(2);
            Assert.assertEquals(value, ts.toString());
            Date date = resultSet.getDate(3);
            Assert.assertEquals(dateValue, date.toString());
            Timestamp ts2 = resultSet.getTimestamp(3);
            Assert.assertEquals(value, ts2.toString());
            Object obj1 = resultSet.getObject(2);
            Assert.assertEquals(value, ((Timestamp) obj1).toString());
            Object obj2 = resultSet.getObject(3);
            Assert.assertEquals(value, ((Timestamp) obj2).toString());

            PreparedStatement preparedStatement = conn.prepareStatement("select * from "
                                                                        + testTimeStamp);
            resultSet = preparedStatement.executeQuery();

            Assert.assertTrue(resultSet.next());

            timeStampStr = resultSet.getString(2);
            Assert.assertEquals(value, timeStampStr);
            timeStampStr2 = resultSet.getString(3);
            Assert.assertEquals(value, timeStampStr2);
            ts = resultSet.getTimestamp(2);
            Assert.assertEquals(value, ts.toString());
            date = resultSet.getDate(3);
            Assert.assertEquals(dateValue, date.toString());
            ts2 = resultSet.getTimestamp(3);
            Assert.assertEquals(value, ts2.toString());
            obj1 = resultSet.getObject(2);
            Assert.assertEquals(value, ((Timestamp) obj1).toString());
            obj2 = resultSet.getObject(3);
            Assert.assertEquals(value, ((Timestamp) obj2).toString());
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
            ps.execute();
        } catch (Throwable e) {
            Assert.assertNotNull(e);
            e.printStackTrace();
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
            Assert.assertNotNull(e);
            e.printStackTrace();
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
            callableStatement.execute();
            Assert.assertEquals(1, callableStatement.getInt(1));
            Assert.assertEquals(3, callableStatement.getInt(2));
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
    public void testSetTimeForDate2() {
        try {
            Connection conn = sharedConnection;
            PreparedStatement ps = sharedConnection.prepareStatement("select a from " + testDate2
                                                                     + " where nvl(b, ? + 1) > c");
            Time testTime = new Time(12, 0, 0);
            ps.setTime(1, testTime);
            ps.executeQuery();
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
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
    public void testSendBatchBugForRwrite() {
        Assume.assumeTrue(sharedUsePrepare());
        try {
            Connection conn = setConnection("&usePieceData=true&rewriteBatchedStatements=true&maxBatchTotalParamsNum=200");
            Statement stmt = conn.createStatement();
            try {
                stmt.execute("drop table rewriteclob");
            } catch (SQLException e) {
                //                e.printStackTrace();
            }
            stmt.execute("create table rewriteclob(c1 clob,c2 varchar2(200),c3 varchar2(200),c4 clob,c5 int,c6 number(12,6),c7 date,c8 varchar2(200),c9 number(12,6),c10 varchar2(200),c11 varchar2(200))");
            PreparedStatement preparedStatement = conn
                .prepareStatement("insert into rewriteclob values(?,?,?,?,?,?,?,?,?,null,null)");
            int val = 0;
            preparedStatement.setCharacterStream(1, new StringReader("string value"));
            preparedStatement.setString(2, "string value");
            preparedStatement.setString(3, "string value");
            preparedStatement.setString(4, "string value");
            preparedStatement.setLong(5, val++);

            preparedStatement.setBigDecimal(6, new BigDecimal(134254.3342));
            preparedStatement.setDate(7, null);

            preparedStatement.setString(8, "string value");
            preparedStatement.setBigDecimal(9, new BigDecimal(134254.3342));
            preparedStatement.execute();
            ResultSet rs = stmt.executeQuery("select * from rewriteclob");
            val = 0;
            while (rs.next()) {
                Assert.assertEquals("string value", rs.getString(1));
                Assert.assertEquals(val++, rs.getLong(5));
            }
            stmt.execute("delete from rewriteclob");
            int batchTimes = 99;
            val = 0;
            System.out.println("test piece");
            preparedStatement = conn
                .prepareStatement("insert into rewriteclob values(?,?,?,?,?,?,?,?,?,null,null)");
            for (int i = 0; i < batchTimes; i++) {
                preparedStatement.setCharacterStream(1, new StringReader("string value")); //  test for long data
                preparedStatement.setString(2, "string value");
                preparedStatement.setString(3, "string value");
                preparedStatement.setCharacterStream(4, new StringReader("string value")); // test for long data
                preparedStatement.setLong(5, val++);

                preparedStatement.setBigDecimal(6, new BigDecimal(134254.3342));
                preparedStatement.setDate(7, null);

                preparedStatement.setString(8, "string value");
                preparedStatement.setBigDecimal(9, new BigDecimal(134254.3342));
                preparedStatement.addBatch();
            }
            val = 0;
            preparedStatement.executeBatch();
            rs = stmt.executeQuery("select * from rewriteclob");
            long count = 0;
            while (rs.next()) {
                count++;
                if (count < 10) {
                    Assert.assertEquals("string value", rs.getString(1));
                    Assert.assertEquals("string value", rs.getString(4));
                }
                Assert.assertEquals(val++, rs.getLong(5));
            }
            Assert.assertEquals(batchTimes, count);

            stmt.execute("delete from rewriteclob");

            batchTimes = 30001;
            val = 0;
            preparedStatement = conn
                .prepareStatement("insert into rewriteclob values(?,?,?,?,?,?,?,?,?,null,null)");
            for (int i = 0; i < batchTimes; i++) {
                preparedStatement.setString(1, "string value");
                preparedStatement.setString(2, "string value");
                preparedStatement.setString(3, "string value");
                preparedStatement.setString(4, "string value");
                preparedStatement.setLong(5, val++);
                preparedStatement.setBigDecimal(6, new BigDecimal(134254.3342));
                preparedStatement.setDate(7, null);
                preparedStatement.setString(8, "string value");
                preparedStatement.setBigDecimal(9, new BigDecimal(134254.3342));
                preparedStatement.addBatch();
            }
            val = 0;
            preparedStatement.executeBatch();
            rs = stmt.executeQuery("select * from rewriteclob");
            count = 0;
            while (rs.next()) {
                count++;
                if (count < 10) {
                    Assert.assertEquals("string value", rs.getString(1));
                    Assert.assertEquals("string value", rs.getString(4));
                }
                Assert.assertEquals(val++, rs.getLong(5));
            }
            Assert.assertEquals(batchTimes, count);
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
    public void testLargeStreamValue() {
        Assume.assumeTrue(sharedUsePrepare());
        try {
            Connection conn = setConnection("&usePieceData=true");
            int size = 1024 * 1024 * 40; // 40mb
            byte[] theBlob = new byte[size];
            InputStream stream = new ByteArrayInputStream(theBlob);
            int pos = 0;
            int ch = 0;
            ResultSet rs;
            PreparedStatement ps = conn.prepareStatement("insert into " + testLargeStream
                                                         + " values(?, ?)");
            ps.setAsciiStream(1, stream);
            ps.setString(2, "aaa");
            ps.execute();
            ps = conn.prepareStatement("select * from " + testLargeStream);
            rs = ps.executeQuery();
            assertTrue(rs.next());
            InputStream is = rs.getAsciiStream(1);
            while ((ch = is.read()) != -1) {
                assertEquals(theBlob[pos++], ch);
            }
            Assert.assertEquals(size, pos);
        } catch (Throwable e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testLargeReaderValue() {
        Assume.assumeTrue(sharedUsePrepare());
        try {
            Connection conn = setConnection("&usePieceData=true");
            String str = getRandomString(1024 * 1024 * 40); // 40MB
            StringReader stringReader = new StringReader(str);
            ResultSet rs;
            PreparedStatement ps = conn.prepareStatement("insert into " + testLargeReader
                                                         + " values(?, ?)");
            ps.setClob(1, stringReader);
            ps.setString(2, "aaa");
            ps.execute();
            ps = conn.prepareStatement("select * from " + testLargeReader);
            rs = ps.executeQuery();
            assertTrue(rs.next());
            String str1 = rs.getString(1);
            Assert.assertEquals(str1, str);
        } catch (Throwable e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testSendPieceReuseConn() {
        Assume.assumeTrue(sharedUsePrepare());
        try {
            Connection conn = setConnection("&usePieceData=true");
            Statement stmt = conn.createStatement();
            for (int j = 0; j < 5; j++) {
                System.out.println("test turn  = " + j);
                for (int i = 0; i < 20; i++) {
                    PreparedStatement ps = conn.prepareStatement("insert into " + pieceReuse
                                                                 + " values(?,?,?,?)");
                    String str = "abcd";
                    if (i >= 10) {
                        ps.setObject(1, str);
                    } else {
                        ps.setCharacterStream(1, new StringReader(str));
                    }
                    ps.setObject(3, str);
                    ps.setInt(2, 100);
                    ps.setInt(4, 100);
                    ps.execute();
                    if (i <= 10) {
                        ps.setObject(1, str);
                    } else {
                        ps.setCharacterStream(1, new StringReader(str));
                    }
                    ps.setObject(3, str);
                    ps.setInt(2, 100);
                    ps.setInt(4, 100);
                    ps.execute();

                    ps = conn.prepareStatement("insert into " + pieceReuse + " values(?,?,?,?)");
                    if (i > 10) {
                        ps.setObject(1, str);
                    } else {
                        ps.setCharacterStream(1, new StringReader(str));
                    }
                    ps.setObject(3, str);
                    ps.setInt(2, 100);
                    ps.setInt(4, 100);
                    ps.addBatch();
                    if (i > 10) {
                        ps.setObject(1, str);
                    } else {
                        ps.setCharacterStream(1, new StringReader(str));
                    }
                    ps.setObject(3, str);
                    ps.setInt(2, 100);
                    ps.setInt(4, 100);
                    ps.addBatch();
                    ps.executeBatch();
                }
            }
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

}
