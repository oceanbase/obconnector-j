/**
 *  OceanBase Client for Java
 *
 *  Copyright (c) 2012-2014 Monty Program Ab.
 *  Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *  Copyright (c) 2021 OceanBase Technology Co.,Ltd.
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
package com.alipay.oceanbase.jdbc;

import org.junit.*;

import java.sql.*;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

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
        Statement stmt = sharedConnection.prepareStatement("select multiResultSets(2)");
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

    // do not support fixme
    @Ignore
    public void testMetaWildcard() throws Exception {
        createProcedure("testMetaWildcard", "(x int, y out int)\n" + "is BEGIN\n"
                                            + "SELECT 1 from dual ;end\n");
        ResultSet rs = sharedConnection.getMetaData().getProcedureColumns("test", null,
            "testMetaWildcard%", "%");
        if (rs.next()) {
            assertEquals("testMetaWildcard", rs.getString(3));
            assertEquals("x", rs.getString(4));

            assertTrue(rs.next());
            assertEquals("testMetaWildcard", rs.getString(3));
            assertEquals("y", rs.getString(4));
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
}
