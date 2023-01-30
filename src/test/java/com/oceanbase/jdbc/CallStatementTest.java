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

import java.sql.*;

import org.junit.*;

public class CallStatementTest extends BaseTest {

    /**
     * Initialisation.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createProcedure("useParameterName", "(a int) begin select a; end");
        createProcedure("useWrongParameterName", "(a int) begin select a; end");
        createProcedure("multiResultSets", "() BEGIN  SELECT 1; SELECT 2; END");
        createProcedure("inoutParam", "(INOUT p1 INT) begin set p1 = p1 + 1; end\n");
        createProcedure("testGetProcedures", "(INOUT p1 INT) begin set p1 = p1 + 1; end\n");
        createProcedure("withStrangeParameter", "(IN a DECIMAL(10,2)) begin select a; end");
        createProcedure("TEST_SP1", "() BEGIN\n" + "SELECT @Something := 'Something';\n"
        //                                    + "SIGNAL SQLSTATE '70100'\n"
                                    + "SIGNAL SQLSTATE '70100';\n"
                                    //                                    + "SET MESSAGE_TEXT = 'Test error from SP'; \n" + "END");
                                    + "END");
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
            createFunction("stmtSimpleFunction",
                "(a float, b bigint, c int) RETURNS INT NO SQL\nBEGIN\nRETURN a;\nEND");
            sharedConnection.createStatement().execute("{call stmtSimpleFunction(2,2,2)}");
            fail("call mustn't work for function, use SELECT <function>");
        } catch (SQLSyntaxErrorException sqle) {
            assertTrue("error : " + sqle.getMessage(),
                sqle.getMessage().contains("stmtSimpleFunction does not exist"));
        }
    }

    @Test
    public void prepareStmtSimpleFunction() throws SQLException {
        try {
            createFunction("stmtSimpleFunction",
                "(a float, b bigint, c int) RETURNS INT NO SQL\nBEGIN\nRETURN a;\nEND");
            PreparedStatement preparedStatement = sharedConnection
                .prepareStatement("select stmtSimpleFunction(2,3,4) ");
            ResultSet result1 = preparedStatement.executeQuery();
            result1.next();
            Assert.assertTrue("not equals a ", result1.getInt(1) == 2);

            //            fail("call mustn't work for function, use SELECT <function>");
        } catch (SQLSyntaxErrorException sqle) {
            assertTrue("error : " + sqle.getMessage(),
                sqle.getMessage().contains("stmtSimpleFunction does not exist"));
        }
    }

    @Test
    public void prepareStmtWithOutParameter() throws SQLException {
        //        Assume.assumeTrue(sharedUsePrepare());
        createProcedure("prepareStmtWithOutParameter", "(IN x int, INOUT y int)\n" + "BEGIN\n"
                                                       + "SELECT 1;select 10000 into y;end\n");
        CallableStatement callableStatement = sharedConnection
            .prepareCall("{call prepareStmtWithOutParameter(?,?)}");
        callableStatement.setInt(1, 2);
        callableStatement.registerOutParameter(2, Types.INTEGER);
        callableStatement.setInt(2, 3);
        assertTrue(callableStatement.execute());
        System.out.println("callableStatement.getInt(2) = " + callableStatement.getInt(2));
    }

    @Test
    public void prepareBatchMultiResultSets() throws Exception {
        Assume.assumeTrue(!sharedUsePrepare());
        PreparedStatement stmt = sharedConnection.prepareStatement("{call multiResultSets()}");
        stmt.addBatch();
        stmt.addBatch();
        try {
            stmt.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
            assertTrue(e.getMessage().contains(
                "Select command are not permitted via executeBatch() command"));
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
  public void stmtInoutParam() throws SQLException {
    try (Connection connection = setConnection("&dumpQueriesOnException")) {
      Statement stmt = connection.createStatement();
      stmt.execute("{call inOutParam(1)}");
      fail("must fail : statement cannot be use when there is out parameter");
    } catch (SQLSyntaxErrorException e) {
      assertTrue(
          e.getMessage().contains("OUT or INOUT argument 0 for routine")
              && e.getMessage().contains("Query is: call inOutParam(1)"));
    }
  }

    @Test
    public void getProcedures() throws SQLException {
        ResultSet rs = sharedConnection.getMetaData().getProcedures(null, null, "testGetProc%");
        ResultSetMetaData md = rs.getMetaData();
        for (int i = 1; i <= md.getColumnCount(); i++) {
            System.out.println("i = " + md.getColumnLabel(i));
        }
        while (rs.next()) {
            for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                rs.getObject(i);
            }
        }
    }

    @Test
    public void meta() throws Exception {
        createProcedure("callabletest1", "()\nBEGIN\nSELECT 1;end\n");
        ResultSet rs = sharedConnection.getMetaData().getProcedures(null, null, "callabletest1");
        if (rs.next()) {
            assertTrue("callabletest1".equalsIgnoreCase(rs.getString(3)));
        } else {
            fail();
        }
    }

    @Test
    public void testMetaWildcard() throws Exception {
        createProcedure("testMetaWildcard", "(x int, out y varchar(200))\n" + "BEGIN\n"
                                            + "SELECT 1;end\n");
        ResultSet rs = sharedConnection.getMetaData().getProcedureColumns(null, null,
            "testMetaWildcard%", "%");
        Assert.assertTrue(rs.next());
        System.out.println("1");
        assertEquals("testMetaWildcard", rs.getString(3));
        assertEquals("x", rs.getString(4));

        assertTrue(rs.next());
        assertEquals("testMetaWildcard", rs.getString(3));
        assertEquals("y", rs.getString(4));
        assertFalse(rs.next());

    }

    @Test
    public void testMetaCatalog() throws Exception {
        createProcedure("testMetaCatalog", "(x int, out y int)\nBEGIN\nSELECT 1;end\n");
        ResultSet rs = sharedConnection.getMetaData().getProcedures(sharedConnection.getCatalog(),
            null, "testMetaCatalog");
        assertTrue(rs.next());
        assertTrue("testMetaCatalog".equals(rs.getString(3)));
        assertFalse(rs.next());

        // test with bad catalog
        rs = sharedConnection.getMetaData().getProcedures("yahoooo", null, "testMetaCatalog");
        assertFalse(rs.next());

        // test without catalog
        rs = sharedConnection.getMetaData().getProcedures(null, null, "testMetaCatalog");
        assertTrue(rs.next());
        assertTrue("testMetaCatalog".equals(rs.getString(3)));
        assertFalse(rs.next());
    }

    //Oceanbase stored procedures do not support returning result sets
    @Ignore
    public void prepareWithNoParameters() throws SQLException {
        createProcedure("prepareWithNoParameters", "()\n" + "begin\n" + "    SELECT 'mike';"
                                                   + "end\n");

        PreparedStatement preparedStatement = sharedConnection
            .prepareStatement("{call prepareWithNoParameters()}");
        ResultSet rs = preparedStatement.executeQuery();
        assertTrue(rs.next());
        assertEquals("mike", rs.getString(1));
    }

    //Oceanbase stored procedures do not support returning result sets
    @Ignore
    public void testCallWithFetchSize() throws SQLException {
        createProcedure("testCallWithFetchSize", "()\nBEGIN\nSELECT 1;SELECT 2;\nEND");
        try (Statement statement = sharedConnection.createStatement()) {
            statement.setFetchSize(1);
            try (ResultSet resultSet = statement.executeQuery("CALL testCallWithFetchSize()")) {
                int rowCount = 0;
                while (resultSet.next()) {
                    rowCount++;
                }
                assertEquals(1, rowCount);
            }
            statement.execute("SELECT 1");
        }
    }

    @Test
    public void testBeginInsertAllQuestionMark() throws Exception {
        Connection connection = setConnection("&useServerPrepStmts=true");

        String tableName = "T_DS_TASK_INSTANCE";
        createTable(tableName, "c1 varchar(30), c2 int, c3 varchar(30)");

        CallableStatement cs;
        try {
            cs = connection.prepareCall("begin\n" + "  insert into " + tableName
                                        + " values(?, ?, ?);\n" + "end;");
            fail();
            cs.setString("col1", "aa1");// mysql: "No parameter named 'col1'"
            cs.setInt("col2", 111);
            cs.setString("col3", "bb1");
            cs.execute();
        } catch (Exception e) {
            assertTrue(e
                .getMessage()
                .contains(
                    "invalid callable syntax. must be like {[?=]call <procedure/function name>[(?,?, ...)]}"));
        }

        try {
            cs = connection.prepareCall("begin\n" + "  insert into " + tableName
                                        + " values(?, ?, ?);\n" + "end;");
            fail();
            cs.setString(1, "aa2");
            cs.setInt(2, 222);
            cs.setString(3, "bb2");
            cs.execute(); // mysql: "#42000"
        } catch (Exception e) {
            assertTrue(e
                .getMessage()
                .contains(
                    "invalid callable syntax. must be like {[?=]call <procedure/function name>[(?,?, ...)]}"));
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
        // mysql:
        // none
    }

    @Test
    public void testBeginInsertRepeatParameterName() throws Exception {
        Connection connection = setConnection("&useServerPrepStmts=true");
        DatabaseMetaData dbmd = connection.getMetaData();
        Assert.assertFalse(dbmd.supportsNamedParameters());

        String tableName = "T_DS_TASK_INSTANCE";
        createTable(tableName, "c1 varchar(30), c2 int, c3 varchar(30)");

        CallableStatement cs;
        try {
            cs = connection.prepareCall("begin\n" + "  insert into " + tableName
                                        + " values(:col1, :col2, :col3);\n" + "end;");
            fail();
            cs.setString("col1", "aa1");// mysql: NullPointerException
            cs.setInt("col2", 111);
            cs.setString("col3", "bb1");
            cs.execute();
        } catch (Exception e) {
            assertTrue(e
                .getMessage()
                .contains(
                    "invalid callable syntax. must be like {[?=]call <procedure/function name>[(?,?, ...)]}"));
        }

        try {
            cs = connection.prepareCall("begin\n" + "  insert into " + tableName
                                        + " values(:col1, :col2, :col3);\n" + "end;");
            fail();
            cs.setString(1, "aa2");// mysql: "Parameter index out of range (1 > number of parameters, which is 0)."
            cs.setInt(2, 222);
            cs.setString(3, "bb2");
            cs.execute();
        } catch (Exception e) {
            assertTrue(e
                .getMessage()
                .contains(
                    "invalid callable syntax. must be like {[?=]call <procedure/function name>[(?,?, ...)]}"));
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
        // mysql:
        // none
    }

    @Test
    public void testBeginInsertSomeQuestionMark() throws Exception {
        Connection connection = setConnection("&useServerPrepStmts=true");

        String tableName = "T_DS_TASK_INSTANCE";
        createTable(tableName, "c1 varchar(30), c2 int, c3 varchar(30)");

        CallableStatement cs;
        try {
            cs = connection.prepareCall("begin\n" + "  insert into " + tableName
                                        + " values(?, :col2, ?);\n" + "end;");
            fail();
            cs.setString("col1", "aa1");// mysql: "No parameter named 'col1'"
            cs.setInt("col2", 111);
            cs.setString("col3", "bb1");
            cs.execute();
        } catch (Exception e) {
            assertTrue(e
                .getMessage()
                .contains(
                    "invalid callable syntax. must be like {[?=]call <procedure/function name>[(?,?, ...)]}"));
        }

        try {
            cs = connection.prepareCall("begin\n" + "  insert into " + tableName
                                        + " values(?, :col2, ?);\n" + "end;");
            fail();
            cs.setString(1, "aa2");
            cs.setInt(2, 222);
            cs.setString(3, "bb2");// mysql: "Parameter index out of range (3 > number of parameters, which is 2)."
            cs.execute();
        } catch (Exception e) {
            assertTrue(e
                .getMessage()
                .contains(
                    "invalid callable syntax. must be like {[?=]call <procedure/function name>[(?,?, ...)]}"));
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
        // mysql:
        // none
    }

    @Test
    public void testInsertAllQuestionMark() throws Exception {
        Connection connection = setConnection("&useServerPrepStmts=true");

        String tableName = "T_DS_TASK_INSTANCE";
        createTable(tableName, "c1 varchar(30), c2 int, c3 varchar(30)");

        CallableStatement cs;
        try {
            cs = connection.prepareCall("insert into " + tableName + " values(?, ?, ?)");
            fail();
            cs.setString("col1", "aa1"); // mysql: "No parameter named 'col1'"
            cs.setInt("col2", 111);
            cs.setString("col3", "bb1");
            cs.execute();
        } catch (Exception e) {
            assertTrue(e
                .getMessage()
                .contains(
                    "invalid callable syntax. must be like {[?=]call <procedure/function name>[(?,?, ...)]}"));
        }

        try {
            cs = connection.prepareCall("insert into " + tableName + " values(?, ?, ?)");
            fail();
            cs.setString(1, "aa2");
            cs.setInt(2, 222);
            cs.setString(3, "bb2");
            cs.execute();
        } catch (Exception e) {
            assertTrue(e
                .getMessage()
                .contains(
                    "invalid callable syntax. must be like {[?=]call <procedure/function name>[(?,?, ...)]}"));
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
        // mysql:
        // val is aa2, 222, bb2
    }

    @Test
    public void testInsertRepeatParameterName() throws Exception {
        Connection connection = setConnection("&useServerPrepStmts=true");

        String tableName = "T_DS_TASK_INSTANCE";
        createTable(tableName, "c1 varchar(30), c2 int, c3 varchar(30)");

        CallableStatement cs;
        try {
            cs = connection
                .prepareCall("insert into " + tableName + " values(:col1, :col2, :col3)");
            fail();
            cs.setString("col1", "aa1");// mysql: NullPointerException
            cs.setInt("col2", 111);
            cs.setString("col3", "bb1");
            cs.execute();
        } catch (Exception e) {
            assertTrue(e
                .getMessage()
                .contains(
                    "invalid callable syntax. must be like {[?=]call <procedure/function name>[(?,?, ...)]}"));
        }

        try {
            cs = connection
                .prepareCall("insert into " + tableName + " values(:col1, :col2, :col3)");
            fail();
            cs.setString(1, "aa2");// mysql: "Parameter index out of range (1 > number of parameters, which is 0)."
            cs.setInt(2, 222);
            cs.setString(3, "bb2");
            cs.execute();
        } catch (Exception e) {
            assertTrue(e
                .getMessage()
                .contains(
                    "invalid callable syntax. must be like {[?=]call <procedure/function name>[(?,?, ...)]}"));
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
        // mysql:
        // none
    }

    @Test
    public void testInsertSomeQuestionMark() throws Exception {
        Connection connection = setConnection("&useServerPrepStmts=true");

        String tableName = "T_DS_TASK_INSTANCE";
        createTable(tableName, "c1 varchar(30), c2 int, c3 varchar(30)");

        CallableStatement cs;
        try {
            cs = connection.prepareCall("insert into " + tableName + " values(?, :col2, ?)");
            fail();
            cs.setString("col1", "aa1");// "No parameter named 'col1'"
            cs.setInt("col2", 111);
            cs.setString("col3", "bb1");
            cs.execute();
        } catch (Exception e) {
            assertTrue(e
                .getMessage()
                .contains(
                    "invalid callable syntax. must be like {[?=]call <procedure/function name>[(?,?, ...)]}"));
        }

        try {
            cs = connection.prepareCall("insert into " + tableName + " values(?, :col2, ?)");
            fail();
            cs.setString(1, "aa2");
            cs.setInt(2, 222);
            cs.setString(3, "bb2");
            cs.execute();
        } catch (Exception e) {
            assertTrue(e
                .getMessage()
                .contains(
                    "invalid callable syntax. must be like {[?=]call <procedure/function name>[(?,?, ...)]}"));
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
        // mysql:
        // none
    }

    @Test
    public void fixTwoSingleQuotations() throws SQLException {
        Connection conn = sharedConnection;
        createTable(
            "test_rsmd",
            "id_col int not null primary key auto_increment, "
                    + "nullable_col varchar(20),unikey_col int unique, char_col char(10), us  smallint unsigned");
        createTable("t1", "id int, name varchar(20)");
        createTable("t2", "id int, name varchar(20)");
        createTable("t3", "id int, name varchar(20)");
        try {
            Statement s = conn.createStatement();
            s.execute("create procedure pl_sql2(test_str varchar(4000), inner_sql2 varchar(4000)) begin set @a=inner_sql2; prepare stmt from @a; execute stmt; deallocate prepare stmt; end;");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        CallableStatement callStmt = conn.prepareCall("CALL pl_sql2('what is your name', ?)");
        callStmt
            .setString(
                1,
                "UPDATE /*+ use_px parallel(2) */ t3 SET t3.c1 = timestamp'2001-1-12 2:49:53', t3.c2 = t3.c2, t3.c3 = t3.c3, t3.c4 = (SELECT MIN(t3.c4) FROM t3), t3.c5 = t3.c5, t3.c6 = t3.c6, t3.c7 = t3.c7, t3.c8 = t3.c8, t3.c9 = t3.c9, t3.c10 = (SELECT MAX(t3.c10) FROM t3), t3.c11 = t3.c11, t3.c12 = t3.c12, t3.c13 = t3.c13, t3.c14 = t3.c14, t3.c15 = t3.c15, t3.c16 = timestamp'2029-3-18 0:5:13', t3.c17 = t3.c17 WHERE t3.c1 >= t3.c16 AND t3.c5 = 221");
        try {
            callStmt.execute();
        } catch (SQLSyntaxErrorException e) {
            assertTrue(e.getMessage().contains("Unknown column 't3.c1' in 'field list'"));
        }
    }

    @Test
    public void testProcedureWithSpecialChar() {
        try {
            try {
                sharedConnection.createStatement().execute("drop procedure if exists p2");
            } catch (SQLException e) {

            }
            sharedConnection.createStatement().execute(
                "CREATE PROCEDURE p2(A$TABLE_NAME VARCHAR(100)) begin set @a=10;end;");
            CallableStatement call = sharedConnection.prepareCall("call p2(?)");
            call.setString(1, "0010");
            call.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

}
