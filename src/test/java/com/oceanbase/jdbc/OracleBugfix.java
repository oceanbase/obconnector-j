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

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.Properties;
import java.util.TimeZone;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;

import org.junit.Assert;
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
}
