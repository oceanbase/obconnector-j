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
import static org.junit.Assert.assertEquals;

import java.sql.*;
import java.sql.Clob;
import java.time.LocalDateTime;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class Bugfix extends BaseTest {
    static String testProceSetdefaultVal = "testProceSetdefaultVal" + getRandomString(5);
    static String testFunction           = "testfunction" + getRandomString(5);
    static String testSwap               = "testSwap" + getRandomString(5);
    static String testDateAndTS          = "testDateAndTS" + getRandomString(5);
    static String refreshRow             = "refreshRow" + getRandomString(5);
    static String testSetObjectBlob      = "testSetObjectBlob" + getRandomString(5);

    @BeforeClass()
    public static void initClass() throws SQLException {
        createFunction(testFunction, "(c1 varchar(200),c2 int, c3  int,c4  int)   returns int  \n"
                                     + "begin \n" + "   return c2 + c3 + c4 ;\n" + "end ");

        createProcedure(testProceSetdefaultVal,
            "(inout c1 varchar(200),c2 int,out  c3  int,c4  int)    \n" + "begin \n"
                    + "  set c3 = 100  ;\n set c1 = c1;\n" + "end ");

        createProcedure(
            testSwap,
            "(inout c1 int,inout c2 int)    \n"
                    + "begin \n"
                    + "DECLARE tmp  INT DEFAULT 0;\n set tmp = c1 ;\n set c1 := c2 ;\nset c2 := tmp  ;\n"
                    + "end ");
        createTable(testDateAndTS, "c1 date ,c2 timestamp");
        createTable(refreshRow, "c1 int not null primary key,c2 varchar(200)");
        createTable(testSetObjectBlob, "c1 blob,c2 blob,c3 blob,c4 blob");
    }

    @Test
    public void tesMysqlModeFunction() {
        try {
            Connection conn = sharedConnection;
            CallableStatement callableStatement = conn.prepareCall("? = call " + testFunction
                                                                   + "('fsfsfs',100,?,?)");
            callableStatement.registerOutParameter(1, Types.NUMERIC);
            callableStatement.setInt(2, 20);
            callableStatement.setInt(3, 20);
            callableStatement.execute();
            Assert.assertEquals(140, callableStatement.getInt(1));
            callableStatement = conn.prepareCall("? = call " + testFunction + "('fsfsfs',?,10,10)");
            callableStatement.registerOutParameter(1, Types.NUMERIC);
            callableStatement.setInt(2, 20);
            callableStatement.execute();
            Assert.assertEquals(40, callableStatement.getInt(1));

            callableStatement = conn.prepareCall("? = call " + testFunction + "(?,?,10,?)");
            callableStatement.registerOutParameter(1, Types.NUMERIC);
            callableStatement.setString(2, "sssss");
            callableStatement.setInt(3, 20);
            callableStatement.setInt(4, 20);
            callableStatement.execute();
            Assert.assertEquals(50, callableStatement.getInt(1));
            callableStatement = conn
                .prepareCall("? = call " + testFunction + "('fsfsfs',10,10,10)");
            callableStatement.registerOutParameter(1, Types.NUMERIC);
            callableStatement.execute();
            Assert.assertEquals(30, callableStatement.getInt(1));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void tesMysqlModeProc() {
        try {
            Connection conn = sharedConnection;
            CallableStatement callableStatement = conn.prepareCall(" call "
                                                                   + testProceSetdefaultVal
                                                                   + "(?,100,?,10)");
            callableStatement.registerOutParameter(1, Types.VARCHAR);
            callableStatement.registerOutParameter(2, Types.INTEGER);
            callableStatement.setString(1, "c1value");
            callableStatement.setInt(2, 20);
            callableStatement.execute();
            Assert.assertEquals("c1value", callableStatement.getString(1));
            Assert.assertEquals(100, callableStatement.getInt(2));
            callableStatement = conn.prepareCall(" call " + testProceSetdefaultVal + "(?,?,?,10)");
            callableStatement.registerOutParameter(1, Types.VARCHAR);
            callableStatement.registerOutParameter(3, Types.INTEGER);
            callableStatement.setString(1, "c1value2");
            callableStatement.setInt("c2", 20);
            callableStatement.setInt(3, 20);
            callableStatement.execute();
            Assert.assertEquals("c1value2", callableStatement.getString(1));
            Assert.assertEquals(100, callableStatement.getInt(3));

        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void tesMysqlModeProc2() {
        try {
            Connection conn = sharedConnection;
            CallableStatement callableStatement = conn.prepareCall(" call " + testSwap + "(?,?)");
            callableStatement.registerOutParameter(1, Types.INTEGER);
            callableStatement.registerOutParameter(2, Types.INTEGER);
            callableStatement.setInt(1, 10);
            callableStatement.setInt(2, 20);
            callableStatement.execute();
            Assert.assertEquals(20, callableStatement.getInt(1));
            Assert.assertEquals(10, callableStatement.getInt(2));
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
    public void testRefreshRow() {
        try {
            Connection conn = setConnectionOrigin("?useServerPrepStmts=false");
            Statement stmt = conn.createStatement();
            stmt.execute("insert into " + refreshRow + " values(100,'100+string')");
            PreparedStatement ps = conn.prepareStatement("select  c1, c2 from " + refreshRow + "\n"
                                                         + "where 2 = ?",
                ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ps.setInt(1, 2);

            ResultSet rs = ps.executeQuery();
            Assert.assertEquals(0, ps.getFetchSize());
            Assert.assertEquals(ResultSet.TYPE_SCROLL_SENSITIVE, rs.getType());
            Assert.assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
            Assert.assertTrue(rs.last());
            Assert.assertEquals("100+string", rs.getString(2));

            Connection another = setConnectionOrigin("?useServerPrepStmts=true");
            stmt = another.createStatement();

            stmt.execute("update " + refreshRow + "  set c2='-1000000+changed' where c1=100");
            Assert.assertTrue(rs.last());
            Assert.assertEquals("100+string", rs.getString(2));
            rs.refreshRow();
            Assert.assertEquals("-1000000+changed", rs.getString(2));

            stmt.execute("delete from " + refreshRow + " where c1=100");
            Assert.assertTrue(rs.last());
            Assert.assertEquals("-1000000+changed", rs.getString(2));
            rs.refreshRow();// Origin  MySQL JDBC throws  java.sql.SQLException: refreshRow() called on row that has been deleted or had primary key changed
            Assert.fail();
        } catch (Throwable e) {
            Assert.assertEquals(
                "refreshRow() called on row that has been deleted or had primary key changed.",
                e.getMessage());
        }
    }

    @Test
    public void testSetObjectBlobBug() {
        try {
            PreparedStatement ps = sharedConnection.prepareStatement("INSERT INTO "
                                                                     + testSetObjectBlob
                                                                     + " VALUES (?, ?, ?, ?)");
            Statement stmt = sharedConnection.createStatement();
            String str = "中文abc";
            byte[] bytes = str.getBytes();
            ps.setObject(1, bytes, Types.BINARY);
            ps.setObject(2, bytes, Types.VARBINARY);
            ps.setObject(3, bytes, Types.LONGVARBINARY);
            ps.setObject(4, bytes, Types.BLOB);
            ps.execute();
            Assert.assertEquals(1, ps.getUpdateCount());
            ResultSet rs = stmt.executeQuery("SELECT * FROM " + testSetObjectBlob);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(str, new String(rs.getBytes(2)));
            Assert.assertEquals(str, new String(rs.getBytes(2)));
            Assert.assertEquals(str, new String(rs.getBytes(2)));
            Assert.assertEquals(str, new String(rs.getBytes(2)));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testClose() throws SQLException {
        Statement closeStmt = null;
        boolean exceptionAfterClosed = false;
        Connection conn = setConnection("&useServerPrepStmts=true");
        try {
            closeStmt = conn.createStatement();
            closeStmt.close();

            try {
                closeStmt.executeQuery("SELECT 1");
            } catch (SQLException sqlEx) {
                exceptionAfterClosed = true;
            }
        } finally {
            if (closeStmt != null) {
                try {
                    closeStmt.close();
                } catch (SQLException sqlEx) {
                    //
                }
            }
        }
        assertTrue("Operations not allowed on Statement after .close() is called!",
            exceptionAfterClosed);
    }

    @Test
    public void testGB18030() {
        try {
            byte[] value = new byte[] { (byte) 0x81, (byte) 0x39, (byte) 0xFB, (byte) 0x30 };
            String strValue = new String(value, "GB18030");
            Connection dbConn;
            dbConn = setConnection("&characterEncoding=gb18030&character_set_client=gb18030&character_set_connection=gb18030&character_set_results=gb1803");
            try {
                dbConn.createStatement().execute("drop table testCharsetGB18030");
            } catch (Exception e) {
                //                e.printStackTrace();
            }
            dbConn
                .createStatement()
                .execute(
                    "create table testCharsetGB18030 (c1 integer, c2 varchar(128)) character set = gb18030");

            dbConn.setAutoCommit(false);
            PreparedStatement preparedStatement = dbConn
                .prepareStatement("INSERT INTO testCharsetGB18030 (c1, c2) VALUES (?, ?)");
            preparedStatement.setInt(1, 1);
            preparedStatement.setString(2, strValue);
            int retValue = preparedStatement.executeUpdate();
            dbConn.commit();
            Assert.assertEquals(1, retValue);
            preparedStatement.close();
            dbConn.close();
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }

    }

    @Test
    public void testClobSetString() throws Exception {
        Statement stmt = sharedConnection.createStatement();
        String s1 = "NewClobData";
        ResultSet rs = stmt.executeQuery("select 'a'");
        rs.next();
        Clob c1 = rs.getClob(1);

        try {
            c1.setString(0, s1, 7, 4);
            fail("Starting position can not be < 1");
        } catch (SQLException e) {
            assertEquals("Starting position can not be < 1", e.getMessage());
        }

        try {
            c1.setString(1, s1, 8, 4);
            fail("String index out of range: 12");
        } catch (SQLException e) {
            assertEquals("String index out of range: 12", e.getMessage());
        }

        // full replace
        c1.setString(1, s1, 3, 4);
        assertEquals("Clob", c1.getSubString(1L, (int) c1.length()));

        // add
        c1.setString(5, s1, 7, 4);
        assertEquals("ClobData", c1.getSubString(1L, (int) c1.length()));
        // current clob string ClobData
        // replace middle chars
        c1.setString(2, s1, 7, 4);
        assertEquals("CDataata", c1.getSubString(1L, (int) c1.length()));
    }

    @Test
    public void testAone39674404() {
        try {
            Connection conn = setConnectionOrigin("?useServerPrepStmts=true&useCursorFetch=true&defaultFetchSize=10");

            try {
                conn.createStatement().execute("drop procedure testPro");
            } catch (SQLException e) {
                e.printStackTrace();
            }
            conn.createStatement().execute("create  procedure testPro() begin select 1;end");

            CallableStatement cs = conn.prepareCall("call testPro()");
            cs.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testAone39135621() {
        try {
            Connection conn = sharedConnection;
            try {
                conn.createStatement().execute("drop procedure pro_ps");
            } catch (SQLException e) {
                //            e.printStackTrace();
            }
            conn.createStatement().execute("create  procedure pro_ps(a int) begin select 2; end");
            PreparedStatement ps = conn.prepareStatement("{call pro_ps(?)}");
            ps.setInt(1, 1);
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testAone38614218() {
        try {
            Connection conn = sharedConnection;
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT 1");
            ResultSetMetaData rsmd = rs.getMetaData();
            assertTrue(rsmd.isReadOnly(1));
            stmt.execute("drop table if exists testRSMDIsReadOnly");
            stmt.execute("create table testRSMDIsReadOnly (field1 INT)");
            stmt.executeUpdate("INSERT INTO testRSMDIsReadOnly VALUES (1)");
            rs = stmt.executeQuery("SELECT 1, field1 + 1, field1 FROM testRSMDIsReadOnly");
            rsmd = rs.getMetaData();
            assertTrue(rsmd.isReadOnly(1));
            assertTrue(rsmd.isReadOnly(2));
            assertFalse(rsmd.isReadOnly(3));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }
}
