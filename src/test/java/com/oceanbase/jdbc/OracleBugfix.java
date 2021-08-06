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

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class OracleBugfix extends BaseOracleTest {
    static String testTMTable = "test_timestamp" + getRandomString(5);
    static String testColonIdentifier = "testcolon" + getRandomString(5);
    static String testColonIdentifierBatch = "testcolonbatch" + getRandomString(5);
    static String testColonIdentifierBatch2 = "testcolonbatch" + getRandomString(5);
    static String testColonIdentifierProc = "testcolonproc" + getRandomString(5);
    static String testJDBIfunction = "JDBIfunction1" + getRandomString(5);
    static String testJDBIfunction2 = "JDBIfunction1" + getRandomString(5);
    static String testNumberCol1 = "testNumberCol1" + getRandomString(5);
    static String testNumberCol2 = "testNumberCol2" + getRandomString(5);
    static String testNumberCol3 = "testNumberCol3" + getRandomString(5);
    static String testNumberCol4 = "testNumberCol4" + getRandomString(5);

    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable(testTMTable, "ci timestamp(6)");
        createTable(testColonIdentifier, "c1 varchar2(200),c2 int ,c3 int");
        createTable(testColonIdentifierBatch, "c1 varchar2(200),c2 int ,c3 int");
        createTable(testColonIdentifierBatch2, "c1 varchar2(200),c2 int ,c3 int");
        createProcedure(testColonIdentifierProc, "(p1 IN int ,p2 OUT int) is begin  p2:=p1 + 250; end");
        createFunction(testJDBIfunction,
                "(\n" + "  I_BIRTHDAY               IN DATE,        " + "  I_HOLD_DATE              IN DATE,        "
                        + "  I_VALIDATE_DATE          IN DATE       " + ") RETURN NUMBER\n" + "as\n"
                        + "  V_AGE_MONTH  NUMBER;\n" + "begin\n" + "  if true then\n"
                        + "    V_AGE_MONTH := mod(floor(months_between(I_HOLD_DATE,I_BIRTHDAY)), 12);\n" + "  else\n"
                        + "    V_AGE_MONTH := mod(floor(months_between(I_VALIDATE_DATE,I_BIRTHDAY)), 12);\n"
                        + "  end if;\n" + "  return V_AGE_MONTH;\n" + "end;\n");
        createFunction(testJDBIfunction2,
                "(\n" + "  i1               IN INt,       " + "  i2              IN INT,       "
                        + "  i3          IN INT        " + ") RETURN NUMBER\n" + "as\n" + "  ototoal  NUMBER;\n"
                        + "begin\n" + "ototoal := i1 + i2 + i3;\n" + "return ototoal;\n" + "end");
        createTable(testNumberCol1, "c1 int");
        createTable(testNumberCol2, "c1 float");
        createTable(testNumberCol3, "c1 decimal");
        createTable(testNumberCol4, "c1 number");
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
            stmt.execute("insert /* commment */  into " + testColonIdentifier + " values('varcharvalue111',1,2)");
            stmt.execute("insert into " + testColonIdentifier + " values('varcharvalue222',1,3)");
            PreparedStatement pstmt = conn.prepareStatement(
                    "select * /* comment */ from " + testColonIdentifier + " where  c3 =:n and c2 = :name ");
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
            PreparedStatement pstmt = conn
                    .prepareStatement("insert into " + testColonIdentifierBatch + " values(:var1,:var2,:var3);");
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
            PreparedStatement pstmt = conn
                    .prepareStatement("insert into " + testColonIdentifierBatch2 + " values(:var1,:var2,:var3);");
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
            CallableStatement cstmt = conn
                    .prepareCall("{ call " + testColonIdentifierProc + "\n(:inparam,:outparam) }");
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

    // bugfix for JDBI name binding setDate
    @Test
    public void functionTest() {
        try {
            Connection conn = sharedConnection;
            CallableStatement cstmt = conn.prepareCall("{? =  call " + testJDBIfunction + "(:name1,:name2,:name3)}");
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
            CallableStatement cstmt = conn.prepareCall("{? =  call " + testJDBIfunction2 + "(:name1,:name2,:name3)}");
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
                // e.printStackTrace();
            }
            conn.prepareStatement("create table ttt_name1 (c1 number, c2 varchar2(50))").executeUpdate();
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
                // e.printStackTrace();
            }
            conn.prepareStatement("create table ttt_name2 (c1 number, c2 varchar2(50))").executeUpdate();
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
        statement.executeUpdate("create table test_decimal (c1 int, c2 float, c3 decimal, c4 number)");
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
                // e.printStackTrace();
            }
            Statement stmt = conn.createStatement();
            stmt.execute("create table t(a int)");
            stmt.execute("create or replace procedure pp\n" + "is\n" + "begin\n" + "insert into t(a1) values(1);\n"
                    + "end;\n");
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
                // e.printStackTrace();
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

}
