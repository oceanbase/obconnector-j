/**
 *  OceanBase Client for Java
 *
 *  Copyright (c) 2012-2014 Monty Program Ab.
 *  Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *  Copyright (c) 2021 OceanBase.
 *
 *  This library is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU Lesser General Public License as published by the Free
 *  Software Foundation; either version 2.1 of the License, or (at your option)
 *  any later version.
 *
 *  This library is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License along
 *  with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 *  This particular MariaDB Client for Java file is work
 *  derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 *  the following copyright and notice provisions:
 *
 *  Copyright (c) 2009-2011, Marcus Eriksson
 *
 *  Redistribution and use in source and binary forms, with or without modification,
 *  are permitted provided that the following conditions are met:
 *  Redistributions of source code must retain the above copyright notice, this list
 *  of conditions and the following disclaimer.
 *
 *  Redistributions in binary form must reproduce the above copyright notice, this
 *  list of conditions and the following disclaimer in the documentation and/or
 *  other materials provided with the distribution.
 *
 *  Neither the name of the driver nor the names of its contributors may not be
 *  used to endorse or promote products derived from this software without specific
 *  prior written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 *  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 *  IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 *  INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 *  NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 *  PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 *  WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 *  OF SUCH DAMAGE.
 */
package com.oceanbase.jdbc;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import java.sql.*;

import org.junit.*;

/**
 * @Description
 * @Date : 2021/2/5 15:27
 */
public class CallStatementOracleTest extends BaseOracleTest {

    /**
     * Initialisation.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createProcedure("useParameterName", "(a int) is begin select a from dual ; end");
        createProcedure("useWrongParameterName", "(a int) is begin select a from dual ; end");
        createProcedure("multiResultSets",
            "(a int) is begin  SELECT 1 from dual ; SELECT 2 from dual ; end");
        createProcedure("inoutParam", "(p1 in out int ) is begin set p1 = p1 + 1; end\n");
        createProcedure("testGetProcedures",
            "(p1 IN OUT number(12) ) is begin set p1 = p1 + 1; end\n");
        createProcedure("withStrangeParameter",
            "(a IN number(10,2)) is begin select a from dual ; end");

    }

    @Before
    public void checkSp() throws SQLException {
        requireMinimumVersion(5, 0);
    }

    //Oceanbase stored procedures do not support returning result sets
    @Ignore
    public void stmtSimple() throws SQLException {
        createProcedure("stmtSimple", "(IN p1 INT, IN p2 INT) begin SELECT p1 + p2; end\n");
        ResultSet rs = sharedConnection.createStatement().executeQuery("{call stmtSimple(2,2)}");
        assertTrue(rs.next());
        int result = rs.getInt(1);
        assertEquals(result, 4);
    }

    //Oceanbase stored procedures do not support returning result sets
    @Ignore
    public void prepareStmtSimple() throws SQLException {
        createProcedure("prepareStmtSimple", "(IN p1 INT, IN p2 INT) begin SELECT p1 + p2; end\n");
        PreparedStatement preparedStatement = sharedConnection
            .prepareStatement("{call prepareStmtSimple(?,?)}");
        preparedStatement.setInt(1, 2);
        preparedStatement.setInt(2, 2);
        ResultSet rs = preparedStatement.executeQuery();
        assertTrue(rs.next());
        int result = rs.getInt(1);
        assertEquals(result, 4);
    }

    @Test
    public void stmtSimpleFunction() throws SQLException {
        try {
            createFunction(
                "stmtSimpleFunction",
                "(a float, b int , c int) RETURN number is cc number(12,2) ; \nBEGIN\n select a into cc from dual ; return cc ;\nEND");
            //            sharedConnection.createStatement().execute("{call stmtSimpleFunction(2.0,2,2) }");
            ResultSet result = sharedConnection.createStatement().executeQuery(
                "select stmtSimpleFunction(5,2,2) from dual ");
            result.next();
            System.out.println("result:--> " + result.getInt(1));
            //            fail("call mustn't work for function, use SELECT <function>");
        } catch (SQLSyntaxErrorException sqle) {
            assertTrue("error : " + sqle.getMessage(),
                sqle.getMessage().contains("stmtSimpleFunction does not exist"));
        }
    }

    @Test
    public void prepareStmtSimpleFunction() throws SQLException {
        try {
            createFunction(
                "stmtSimpleFunction",
                "(a float, b int , c int) RETURN INT is num int ; \n  begin select a into num from dual ; \nRETURN num ; \nEND");

            PreparedStatement preparedStatement = sharedConnection
                .prepareStatement("select stmtSimpleFunction(5,2,2) from dual ");
            ResultSet result = preparedStatement.executeQuery();
            result.next();
            System.out.println("result:--> " + result.getInt(1));
            //            fail("call mustn't work for function, use SELECT <function>");
        } catch (SQLSyntaxErrorException sqle) {
            assertTrue("error : " + sqle.getMessage(),
                sqle.getMessage().contains("stmtSimpleFunction does not exist"));
        }
    }

    // Consistent with jdbc 1.x, the test failed
    @Ignore
    public void prepareStmtWithOutParameter() throws SQLException {
        createProcedure(
            "prepareStmtWithOutParameter",
            "(x IN int, y OUT int)\n"
                    + " is BEGIN\n"
                    + "SELECT 1 into y from dual ;select 10000+y into y from dual ;select y into y from dual ; end\n");
        CallableStatement callableStatement = sharedConnection
            .prepareCall("{call prepareStmtWithOutParameter(?,?)}");
        callableStatement.setInt(1, 2);
        callableStatement.registerOutParameter(2, Types.INTEGER);
        assertFalse(callableStatement.execute());
        System.out.println("callableStatement.getInt(2) = " + callableStatement.getInt(2)); ///return out parameter
    }

    //Consistent with jdbc 1.x, the test failed
    @Ignore
    public void prepareBatchMultiResultSets() throws Exception {
        PreparedStatement stmt = sharedConnection.prepareStatement("{call multiResultSets(?)}");
        stmt.setInt(1, 1);
        stmt.addBatch();
        //        stmt.addBatch();
        try {
            stmt.executeBatch();
            // java.sql.BatchUpdateException: (conn=-1073309480) ORA-00600: internal error code, arguments: -4007, Not supported feature or function
        } catch (SQLException e) {
            e.printStackTrace();

        }
    }

    @Test
    public void prepareCallBatchMultiResultSets() throws Exception {

        try {
            Statement stmt = sharedConnection.prepareStatement("select multiResultSets(2)");
            stmt.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
            assertTrue(e.getMessage().contains(
            //                "Select command are not permitted via executeBatch() command"));
                "ORA-00900: You have an error in your SQL syntax;"));
        }
        if (!sharedOptions().emulateUnsupportedPstmts) {
            Connection newConn = setConnection("&emulateUnsupportedPstmts=true");
            Statement stmt = newConn.prepareStatement("select multiResultSets(2)");
            stmt.executeBatch();
        }
    }

    //Oceanbase stored procedures do not support returning result sets
    @Ignore
    public void stmtMultiResultSets() throws Exception {
        Statement stmt = sharedConnection.createStatement();
        stmt.execute("{call multiResultSets()}");
        ResultSet rs = stmt.getResultSet();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertFalse(rs.next());
        assertTrue(stmt.getMoreResults());
        rs = stmt.getResultSet();
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertFalse(rs.next());
        assertFalse(stmt.getMoreResults());
    }

    //Oceanbase stored procedures do not support returning result sets
    @Ignore
    public void prepareStmtMultiResultSets() throws Exception {
        PreparedStatement stmt = sharedConnection.prepareStatement("call multiResultSets()");
        stmt.execute();
        ResultSet rs = stmt.getResultSet();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertFalse(rs.next());
        assertTrue(stmt.getMoreResults());
        rs = stmt.getResultSet();
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertFalse(rs.next());
        assertFalse(stmt.getMoreResults());
    }

    @Test
    public void getProcedures() throws SQLException {
        ResultSet rs = sharedConnection.getMetaData().getProcedures(null, null, "testGetProc%");
        ResultSetMetaData md = rs.getMetaData();
        for (int i = 1; i <= md.getColumnCount(); i++) {
            md.getColumnLabel(i);
        }

        while (rs.next()) {
            for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                rs.getObject(i);
            }
        }
    }

    @Test
    public void meta() throws Exception {
        createProcedure("callabletest1", "(x in int)\n is BEGIN\n SELECT 1 from dual ;end\n");
        ResultSet rs = sharedConnection.getMetaData().getProcedures(null, null, "callabletest1");
        if (rs.next()) {
            System.out.println("result:" + rs.getString(3));
            assertTrue("callabletest1".equalsIgnoreCase(rs.getString(3)));
        } else {
            fail();
        }
    }

    @Test
    public void testMetaWildcard() throws Exception {
        createProcedure("testMetaWildcard", "(x int, y out varchar2(200))\n" + "is BEGIN\n"
                                            + "SELECT 1 from dual ;end\n");
        ResultSet rs = sharedConnection.getMetaData().getProcedureColumns(null, null,
            "TESTMETAWILDCARD%", "%");

        if (rs.next()) {
            assertEquals("TESTMETAWILDCARD", rs.getString(3));
            assertEquals("X", rs.getString(4));

            assertTrue(rs.next());
            assertEquals("TESTMETAWILDCARD", rs.getString(3));
            assertEquals("Y", rs.getString(4));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testMetaWildcard2() throws Exception {
        createProcedure("\"testMetaWildcard\"", "(x int, y out int)\n" + "is BEGIN\n"
                                                + "SELECT 1 from dual ;end\n");
        ResultSet rs = sharedConnection.getMetaData().getProcedureColumns(null, null,
            "testMetaWildcard%", "%");

        if (rs.next()) {
            assertEquals("testMetaWildcard", rs.getString(3));
            assertEquals("X", rs.getString(4));

            assertTrue(rs.next());
            assertEquals("testMetaWildcard", rs.getString(3));
            assertEquals("Y", rs.getString(4));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testMetaCatalog() throws Exception {
        createProcedure("testMetaCatalog",
            "(x int, y out int) \n is BEGIN\nSELECT 1 from dual ;end\n");
        ResultSet rs = sharedConnection.getMetaData().getProcedures(sharedConnection.getCatalog(),
            null, "testMetaCatalog");
        assertTrue(rs.next());
        System.out.println("rs.getString(3)： " + rs.getString(3));
        assertTrue("testMetaCatalog".equalsIgnoreCase(rs.getString(3)));
        assertFalse(rs.next());

        // test with bad catalog
        rs = sharedConnection.getMetaData().getProcedures("yahoooo", null, "testMetaCatalog");
        assertFalse(rs.next());

        // test without catalog
        rs = sharedConnection.getMetaData().getProcedures(null, null, "testMetaCatalog");
        assertTrue(rs.next());
        assertTrue("testMetaCatalog".equalsIgnoreCase(rs.getString(3)));
        assertFalse(rs.next());
    }

    //Oceanbase stored procedures do not support returning result sets
    @Ignore
    public void prepareWithNoParameters() throws SQLException {
        createProcedure("prepareWithNoParameters", "()\n" + "begin\n"
                                                   + "    SELECT 'mike' from dual ;" + "end\n");

        PreparedStatement preparedStatement = sharedConnection
            .prepareStatement("{call prepareWithNoParameters()}");
        ResultSet rs = preparedStatement.executeQuery();
        assertTrue(rs.next());
        assertEquals("mike", rs.getString(1));
    }

    //Oceanbase stored procedures do not support returning result sets
    @Ignore
    public void testCallWithFetchSize() throws SQLException {
        createProcedure("testCallWithFetchSize", "()\nBEGIN\nSELECT 1 from dual ;SELECT 2 from dual ;\nEND");
        try (Statement statement = sharedConnection.createStatement()) {
            statement.setFetchSize(1);
            try (ResultSet resultSet = statement.executeQuery("CALL testCallWithFetchSize()")) {
                int rowCount = 0;
                while (resultSet.next()) {
                    rowCount++;
                }
                assertEquals(1, rowCount);
            }
            statement.execute("SELECT 1 from dual ");
        }
    }

    // obproxy error
    @Ignore
    public void testAnonymousBlock() {
        try {
            Statement stmt = sharedConnection.createStatement();
            createProcedure("outstring_pl1",
                "(P1 OUT VARCHAR2,P2 IN VARCHAR,P3 OUT VARCHAR) is begin \n p1:='wfsfsfs'; p3:=p2;\nend;");
            CallableStatement callableStatement = sharedConnection
                .prepareCall("begin \n outstring_pl1(?,?,?); end;");
            callableStatement.registerOutParameter(1, Types.VARCHAR);
            callableStatement.registerOutParameter(3, Types.VARCHAR);
            callableStatement.setString(2, "1212121");
            callableStatement.executeUpdate();
            String str1 = callableStatement.getString(1);
            String str2 = callableStatement.getString(3);
            System.out.println("str1 = " + str1);
            System.out.println("str2 = " + str2);
            Assert.assertEquals("wfsfsfs", str1);
            Assert.assertEquals("1212121", str2);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testAnonymousBlock2() {
        try {
            Statement stmt = sharedConnection.createStatement();
            createProcedure(
                "outstring_pl2",
                "/* comment 1*/ -- comment 2 \n(P1 OUT VARCHAR2,P2 IN VARCHAR,P3 OUT VARCHAR) is begin \n p1:='wfsfsfs'; p3:=p2;\nend");
            CallableStatement callableStatement = sharedConnection
                .prepareCall("begin \n outstring_pl2(?,'121',?); end;");
            callableStatement.registerOutParameter(1, Types.VARCHAR);
            callableStatement.registerOutParameter(2, Types.VARCHAR);
            callableStatement.executeUpdate();
            String str1 = callableStatement.getString(1);
            String str2 = callableStatement.getString(2);
            System.out.println("str1 = " + str1);
            System.out.println("str2 = " + str2);
            Assert.assertEquals("wfsfsfs", str1);
            Assert.assertEquals("121", str2);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testAnonymousBlock3() {
        try {
            Statement stmt = sharedConnection.createStatement();
            createProcedure("outstring_pl3",
                "(P1 OUT VARCHAR2,P2 IN VARCHAR,P3 OUT VARCHAR,P4 IN VARCHAR ) is begin \n p1:= p4; p3:=p2;\nend;");

            CallableStatement callableStatement = sharedConnection
                .prepareCall("begin \n outstring_pl3(?,?,?,?); end;");
            callableStatement.registerOutParameter(1, Types.VARCHAR);
            callableStatement.registerOutParameter(3, Types.VARCHAR);
            callableStatement.setString(2, "1212121");
            callableStatement.setString(4, "p4string");
            callableStatement.executeUpdate();
            String str1 = callableStatement.getString(1);
            String str2 = callableStatement.getString(3);
            System.out.println("str1 = " + str1);
            System.out.println("str2 = " + str2);
            Assert.assertEquals("p4string", str1);
            Assert.assertEquals("1212121", str2);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testAnonymousBlock4() {
        try {
            Statement stmt = sharedConnection.createStatement();
            createProcedure(
                "outstring_pl4",
                "(P1 OUT VARCHAR,P2 IN OUT VARCHAR,P3 OUT VARCHAR) is begin \n p1:='p1string';p2:='p2string';p3:=p2;\nend;");

            CallableStatement callableStatement = sharedConnection
                .prepareCall("begin \n outstring_pl4(?,?,?); end;");
            callableStatement.registerOutParameter(1, Types.VARCHAR);
            callableStatement.registerOutParameter(2, Types.VARCHAR);
            callableStatement.registerOutParameter(3, Types.VARCHAR);
            callableStatement.setString(2, "p2string");
            callableStatement.executeUpdate();
            String str1 = callableStatement.getString(1);
            String str2 = callableStatement.getString(2);
            String str3 = callableStatement.getString(3);
            System.out.println("str1 = " + str1);
            System.out.println("str2 = " + str2);
            System.out.println("str3 = " + str3);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testBeginInsertRepeatParameterName() throws Exception {
        Connection connection = sharedPSConnection;
        DatabaseMetaData dbmd = connection.getMetaData();
        Assert.assertTrue(dbmd.supportsNamedParameters());

        String tableName = "T_DS_TASK_INSTANCE";
        createTable(tableName, "c1 varchar(30), c2 int, c3 varchar(30)");

        CallableStatement cs = connection.prepareCall("begin\n" + "  insert into " + tableName
                                                      + " values(:col1, :col2, :col3);\n" + "end;");
        cs.setString("col1", "aa1");
        cs.setInt("col2", 111);
        cs.setString("col3", "bb1");
        cs.execute();
        cs.setString("col1", "aa2");
        cs.setInt("col2", 222);
        cs.setString("col3", "bb2");
        cs.execute();

        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("select * from " + tableName);
        boolean first = true;
        int i = 0;
        while (rs.next()) {
            i++;
            if (first) {
                first = false;
                try {
                    rs.getString("col3");// oracle: "列名无效"
                    fail();
                } catch (Exception e) {
                    assertTrue(e.getMessage().contains("No such column: 'col3'"));
                }
            }
            assertEquals(rs.getString(1), "aa" + i);
            assertEquals(rs.getInt("c2"), 111 * i);
            assertEquals(rs.getString("c3"), "bb" + i);
        }
        assertEquals(i, 2);
        // oracle:
        // val is aa1, 111, bb1
        // val is aa2, 222, bb2
    }

    @Test
    public void testBeginInsertDifferentParameterName() throws Exception {
        Connection connection = sharedPSConnection;

        String tableName = "T_DS_TASK_INSTANCE";
        createTable(tableName, "c1 varchar(30), c2 int, c3 varchar(30)");

        CallableStatement cs = connection.prepareCall("begin\n" + "  insert into " + tableName
                                                      + " values(:col1, :col2, :col3);\n" + "end;");
        cs.setString("col1", "aa1");
        cs.setInt("col2", 111);
        cs.setString("col3", "bb1");
        cs.execute();
        try {
            cs.setString("c1", "aa2");// oracle: "无效的列索引"
            fail();
            cs.setString("c2", "222");
            cs.setString("c3", "bb2");
            cs.execute();
        } catch (Exception e) {
            assertEquals(e.getMessage(), "invalid parameter index 4");
        }
        try {
            cs.setString("col1", "aa3");
            cs.setInt("col2", 333);
            cs.setString("col3", "bb3");
            cs.execute();// oracle: "参数名的数目与已注册参数的数目不匹配"
            fail();
        } catch (Exception e) {
            assertEquals(e.getMessage(),
                "The number of parameter names '4' does not match the number of registered parameters in sql '3'.");
        }

        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("select * from " + tableName);
        int i = 0;
        while (rs.next()) {
            i++;
            assertEquals(rs.getString(1), "aa" + i);
            assertEquals(rs.getInt("c2"), 111 * i);
            assertEquals(rs.getString("c3"), "bb" + i);
        }
        assertEquals(i, 1);
        // oracle:
        // val is aa1, 111, bb1
    }

    @Test
    public void testBeginInsertRepeatParameterNameOutOfOrder() throws Exception {
        Connection connection = sharedPSConnection;

        String tableName = "T_DS_TASK_INSTANCE";
        createTable(tableName, "c1 varchar(30), c2 int, c3 varchar(30)");

        CallableStatement cs = connection.prepareCall("begin\n" + "  insert into " + tableName
                                                      + " values(:col1, :col2, :col3);\n" + "end;");
        cs.setString("col1", "aa1");
        cs.setInt("col2", 111);
        cs.setString("col3", "bb1");
        cs.execute();
        cs.setInt("col2", 222);
        cs.setString("col1", "aa2");
        cs.setString("col3", "bb2");
        cs.execute();

        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("select * from " + tableName);
        int i = 0;
        while (rs.next()) {
            i++;
            assertEquals(rs.getString(1), "aa" + i);
            assertEquals(rs.getInt("c2"), 111 * i);
            assertEquals(rs.getString("c3"), "bb" + i);
        }
        assertEquals(i, 2);
        // oracle:
        // val is aa1, 111, bb1
        // val is aa2, 222, bb2
    }

    @Test
    public void testBeginInsertSameParameterName() throws Exception {
        Connection connection = sharedPSConnection;

        String tableName = "T_DS_TASK_INSTANCE";
        createTable(tableName, "c1 varchar(30), c2 int, c3 varchar(30)");

        CallableStatement cs = connection.prepareCall("begin\n" + "  insert into " + tableName
                                                      + " values(:col1, :col2, :col1);\n" + "end;");
        try {
            cs.setString("col1", "aa1");
            cs.setInt("col2", 111);
            cs.execute();// oracle: "参数名的数目与已注册参数的数目不匹配"
            fail();
        } catch (Exception e) {
            assertEquals(e.getMessage(),
                "The number of parameter names '2' does not match the number of registered parameters in sql '3'.");
        }

        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("select * from " + tableName);
        int i = 0;
        while (rs.next()) {
            i++;
            assertEquals(rs.getString(1), "aa" + i);
            assertEquals(rs.getInt("c2"), 111 * i);
            assertEquals(rs.getString("c3"), "bb" + i);
        }
        assertEquals(i, 0);
        // oracle:
        // none
    }

    @Test
    public void testBeginInsertAllQuestionMark() throws Exception {
        Connection connection = sharedPSConnection;

        String tableName = "T_DS_TASK_INSTANCE";
        createTable(tableName, "c1 varchar(30), c2 int, c3 varchar(30)");

        CallableStatement cs = connection.prepareCall("begin\n" + "  insert into " + tableName
                                                      + " values(?, ?, ?);\n" + "end;");
        cs.setString("col1", "aa1");
        cs.setInt("col2", 111);
        cs.setString("col3", "bb1");
        cs.execute();// oracle: "ORA-06550: 第 2 行, 第 45 列: PL/SQL: ORA-00917: 缺失逗号 ORA-06550: 第 2 行, 第 3 列: PL/SQL: SQL Statement ignored"

        cs = connection.prepareCall("begin\n" + "  insert into " + tableName
                                    + " values(?, ?, ?);\n" + "end;");
        cs.setString(1, "aa2");
        cs.setInt(2, 222);
        cs.setString(3, "bb2");
        cs.execute();

        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("select * from " + tableName);
        int i = 0;
        while (rs.next()) {
            i++;
            assertEquals(rs.getString(1), "aa" + i);
            assertEquals(rs.getInt("c2"), 111 * i);
            assertEquals(rs.getString("c3"), "bb" + i);
        }
        assertEquals(i, 2);
        // oracle:
        // val is aa2, 222, bb2
    }

    @Test
    public void testBeginInsertSomeQuestionMark() throws Exception {
        Connection connection = sharedPSConnection;

        String tableName = "T_DS_TASK_INSTANCE";
        createTable(tableName, "c1 varchar(30), c2 int, c3 varchar(30)");

        CallableStatement cs = connection.prepareCall("begin\n" + "  insert into " + tableName
                                                      + " values(?, :col2, ?);\n" + "end;");
        cs.setString("col1", "aa1");
        cs.setInt("col2", 111);
        cs.setString("col3", "bb1");
        cs.execute();// oracle: "ORA-06550: 第 2 行, 第 45 列: PL/SQL: ORA-00917: 缺失逗号 ORA-06550: 第 2 行, 第 3 列: PL/SQL: SQL Statement ignored"

        cs.setString(1, "aa2");
        cs.setInt(2, 222);
        cs.setString(3, "bb2");
        cs.execute();// oracle: "不允许的操作: Ordinal binding and Named binding cannot be combined!"

        cs = connection.prepareCall("begin\n" + "  insert into " + tableName
                                    + " values(?, :col2, ?);\n" + "end;");
        cs.setString(1, "aa3");
        cs.setInt(2, 333);
        cs.setString(3, "bb3");
        cs.execute();

        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("select * from " + tableName);
        int i = 0;
        while (rs.next()) {
            i++;
            assertEquals(rs.getString(1), "aa" + i);
            assertEquals(rs.getInt("c2"), 111 * i);
            assertEquals(rs.getString("c3"), "bb" + i);
        }
        assertEquals(i, 3);
        // oracle:
        // val is aa3, 333, bb3
    }

    @Test
    public void testInsertAllQuestionMark() throws Exception {
        Connection connection = sharedPSConnection;

        String tableName = "T_DS_TASK_INSTANCE";
        createTable(tableName, "c1 varchar(30), c2 int, c3 varchar(30)");

        CallableStatement cs = connection.prepareCall("insert into " + tableName
                                                      + " values(?, ?, ?)");
        cs.setString("col1", "aa1");
        cs.setInt("col2", 111);
        cs.setString("col3", "bb1");
        cs.execute();// oracle: "ORA-00917: 缺失逗号"

        cs = connection.prepareCall("insert into " + tableName + " values(?, ?, ?)");
        cs.setString(1, "aa2");
        cs.setInt(2, 222);
        cs.setString(3, "bb2");
        cs.execute();

        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("select * from " + tableName);
        int i = 0;
        while (rs.next()) {
            i++;
            assertEquals(rs.getString(1), "aa" + i);
            assertEquals(rs.getInt("c2"), 111 * i);
            assertEquals(rs.getString("c3"), "bb" + i);
        }
        assertEquals(i, 2);
        // oracle:
        // val is aa2, 222, bb2
    }

    @Test
    public void testInsertRepeatParameterName() throws Exception {
        Connection connection = sharedPSConnection;
        DatabaseMetaData dbmd = connection.getMetaData();
        Assert.assertTrue(dbmd.supportsNamedParameters());

        String tableName = "T_DS_TASK_INSTANCE";
        createTable(tableName, "c1 varchar(30), c2 int, c3 varchar(30)");

        CallableStatement cs = connection.prepareCall("insert into " + tableName
                                                      + " values(:col1, :col2, :col3);");
        cs.setString("col1", "aa1");
        cs.setInt("col2", 111);
        cs.setString("col3", "bb1");
        cs.execute();
        cs.setString("col1", "aa2");
        cs.setInt("col2", 222);
        cs.setString("col3", "bb2");
        cs.execute();

        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("select * from " + tableName);
        boolean first = true;
        int i = 0;
        while (rs.next()) {
            i++;
            if (first) {
                first = false;
                try {
                    rs.getString("col3");// oracle: "列名无效"
                    fail();
                } catch (Exception e) {
                    assertTrue(e.getMessage().contains("No such column: 'col3'"));
                }
            }
            assertEquals(rs.getString(1), "aa" + i);
            assertEquals(rs.getInt("c2"), 111 * i);
            assertEquals(rs.getString("c3"), "bb" + i);
        }
        assertEquals(i, 2);
        // oracle:
        // val is aa1, 111, bb1
        // val is aa2, 222, bb2
    }

    @Test
    public void testInsertSomeQuestionMark() throws Exception {
        Connection connection = sharedPSConnection;

        String tableName = "T_DS_TASK_INSTANCE";
        createTable(tableName, "c1 varchar(30), c2 int, c3 varchar(30)");

        CallableStatement cs = connection.prepareCall("insert into " + tableName
                                                      + " values(?, :col2, ?)");
        cs.setString("col1", "aa1");
        cs.setInt("col2", 111);
        cs.setString("col3", "bb1");
        cs.execute();// oracle: "ORA-00917: 缺失逗号"

        cs = connection.prepareCall("insert into " + tableName + " values(?, :col2, ?)");
        cs.setString(1, "aa2");
        cs.setInt(2, 222);
        cs.setString(3, "bb2");
        cs.execute();

        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("select * from " + tableName);
        int i = 0;
        while (rs.next()) {
            i++;
            assertEquals(rs.getString(1), "aa" + i);
            assertEquals(rs.getInt("c2"), 111 * i);
            assertEquals(rs.getString("c3"), "bb" + i);
        }
        assertEquals(i, 2);
        // oracle:
        // val is aa2, 222, bb2
    }

    @Test
    public void testUpdateCorrectOrder() throws Exception {
        Connection connection = sharedPSConnection;

        String tableName = "T_DS_TASK_INSTANCE";
        createTable(tableName, "c1 varchar(30), c2 int, c3 varchar(30)");

        Statement stmt = connection.createStatement();
        stmt.execute("insert into " + tableName + " values('aa1', 111, 'bb1')");
        stmt.execute("insert into " + tableName + " values('aa2', 222, 'bb2')");

        CallableStatement cs = connection.prepareCall("update " + tableName
                                                      + " set c1 = :col1  where c2 = :col2");
        cs.setString("col1", "cc1");
        cs.setInt("col2", 111);
        cs.execute();
        cs.setInt("col2", 222);
        cs.setString("col1", "cc2");
        cs.execute();

        ResultSet rs = stmt.executeQuery("select * from " + tableName);
        int i = 0;
        while (rs.next()) {
            i++;
            assertEquals(rs.getString(1), "cc" + i);
            assertEquals(rs.getInt("c2"), 111 * i);
            assertEquals(rs.getString("c3"), "bb" + i);
        }
        assertEquals(i, 2);
        // oracle:
        // val is cc1, 111, bb1
        // val is cc2, 222, bb2
    }

    @Test
    public void testUpdateWrongOrder() throws Exception {
        Connection connection = sharedPSConnection;

        String tableName = "T_DS_TASK_INSTANCE";
        createTable(tableName, "c1 varchar(30), c2 int, c3 varchar(30)");

        Statement stmt = connection.createStatement();
        stmt.execute("insert into " + tableName + " values('aa1', 111, 'bb1')");
        stmt.execute("insert into " + tableName + " values('aa2', 222, 'bb2')");

        CallableStatement cs = connection.prepareCall("update " + tableName
                                                      + " set c1 = :col1  where c2 = :col2");
        try {
            cs.setInt("col2", 222);
            cs.setString("col1", "cc2");
            cs.execute();//oracle: ORA-01722: 无效数字
            fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("ORA-01722: invalid number"));
        }
        try {
            cs.setString("col1", "cc1");
            cs.setInt("col2", 111);
            cs.execute();//oracle: ORA-01722: 无效数字
            fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("ORA-01722: invalid number"));
        }

        ResultSet rs = stmt.executeQuery("select * from " + tableName);
        int i = 0;
        while (rs.next()) {
            i++;
            assertEquals(rs.getString(1), "aa" + i);
            assertEquals(rs.getInt("c2"), 111 * i);
            assertEquals(rs.getString("c3"), "bb" + i);
        }
        assertEquals(i, 2);
        // oracle:
        // val is aa1, 111, bb1
        // val is aa2, 222, bb2
    }

    @Ignore
    public void functionCall() throws SQLException {
        Connection conn = sharedPSConnection;
        createFunction("testFunctionCall",
            "(a FLOAT, b NUMBER, c in out INT) RETURN INT AS d INT;\nBEGIN\nd:=a+b+c;\nc:=10;\nRETURN d;\nEND;");
        CallableStatement cs = conn.prepareCall("? = call testFunctionCall(?,?,?)");

        cs.registerOutParameter(1, Types.INTEGER);
        cs.setFloat(2, 1);
        cs.setInt(3, 2);
        cs.setInt(4, 3);
        cs.registerOutParameter(4, Types.INTEGER);
        //无效SQL，:1 = call testFunctionCall(:2,:3,:4)
        assertFalse(cs.execute());
        assertEquals(6f, cs.getInt(1), .001);

        cs.registerOutParameter(1, Types.INTEGER);
        cs.setFloat(2, 2);
        cs.setInt(3, 3);
        cs.setInt(4, 4);
        assertEquals(1, cs.executeUpdate());
        assertEquals(9f, cs.getInt(1), .001);

        // reuse a statement, and set by name
        cs.registerOutParameter("d", Types.INTEGER);
        cs.setFloat("a", 4);
        cs.setInt("b", 2);
        cs.setInt("c", 1);
        assertEquals(0, cs.executeUpdate());
        assertEquals(7f, cs.getInt(1), .001);
    }

    @Ignore
    public void functionCallByName() throws SQLException {
        Connection conn = sharedPSConnection;
        createFunction("testFunctionCall",
            "(a FLOAT, b NUMBER, c in out INT) RETURN INT AS d INT;\nBEGIN\nd:=a+b+c;\nRETURN d;\nEND;");
        CallableStatement cs = conn.prepareCall("? = call testFunctionCall(?,?,?)");

        cs.registerOutParameter("d", Types.INTEGER);
        cs.setFloat("a", 4);
        cs.setInt("b", 2);
        cs.setInt("c", 1);
        cs.registerOutParameter("c", Types.INTEGER);
        //无效SQL，D=>:0 = call testFunctionCall(A=>:1,B=>:2,C=>:3)
        assertEquals(0, cs.executeUpdate());
        assertEquals(7f, cs.getInt(1), .001);
    }

    @Test
    public void functionAnonymousBlock() throws SQLException {
        Connection conn = sharedPSConnection;
        createFunction("testFunctionCall",
            "(a FLOAT, b NUMBER, c in out INT) RETURN INT AS d INT;\nBEGIN\nd:=a+b+c;\nc:=10;\nRETURN d;\nEND;");
        CallableStatement cs = conn.prepareCall("begin ? := testFunctionCall (?,?,?); end;");

        cs.registerOutParameter(1, Types.INTEGER);
        cs.setFloat(2, 1);
        cs.setInt(3, 2);
        cs.setInt(4, 3);
        cs.registerOutParameter(4, Types.INTEGER);
        cs.execute();
        assertEquals(6f, cs.getInt(1), .001);
        assertEquals(10f, cs.getInt(4), .001);

        cs.registerOutParameter(1, Types.INTEGER);
        cs.setFloat(2, 2);
        cs.setInt(3, 3);
        cs.setInt(4, 4);
        cs.execute();
        assertEquals(9f, cs.getInt(1), .001);
    }

    @Test
    public void functionAnonymousBlockByName() throws SQLException {
        Connection conn = sharedPSConnection;
        createFunction("testFunctionCall",
            "(a FLOAT, b NUMBER, c in out INT) RETURN INT AS d INT;\nBEGIN\nd:=a+b+c;\nc:=10;\nRETURN d;\nEND;");
        CallableStatement cs = conn.prepareCall("begin ? := testFunctionCall (?,?,?); end;");

        cs.registerOutParameter("d", Types.INTEGER);
        cs.setFloat("a", 4);
        cs.setInt("b", 2);
        cs.setInt("c", 1);
        cs.registerOutParameter("c", Types.INTEGER);
        cs.execute();
        assertEquals(7f, cs.getInt(1), .001);
        assertEquals(10f, cs.getInt(4), .001);

        cs.registerOutParameter("d", Types.INTEGER);
        cs.setFloat("a", 1);
        cs.setInt("b", 2);
        cs.setInt("c", 3);
        cs.registerOutParameter("c", Types.INTEGER);
        cs.execute();
        assertEquals(6f, cs.getInt(1), .001);
        assertEquals(10f, cs.getInt(4), .001);
    }

}
