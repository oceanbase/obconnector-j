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
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.sql.*;
import java.sql.Clob;
import java.sql.Date;
import java.time.*;
import java.util.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.*;

public class Bugfix extends BaseTest {
    static String testProceSetdefaultVal = "testProceSetdefaultVal" + getRandomString(5);
    static String testFunction           = "testfunction" + getRandomString(5);
    static String testSwap               = "testSwap" + getRandomString(5);
    static String testDateAndTS          = "testDateAndTS" + getRandomString(5);
    static String refreshRow             = "refreshRow" + getRandomString(5);
    static String testSetObjectBlob      = "testSetObjectBlob" + getRandomString(5);
    static String testBatchValue         = "testBatchValue" + getRandomString(5);
    static String testBestRow            = "testBestRow" + getRandomString(5);

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
        createTable(testBatchValue, "c1 INT, c2 INT");
        createTable(testBestRow, "c1 INT NOT NULL primary key, b VARCHAR(32), c INT, d VARCHAR(5)");
        createTable("testExecuteLargeBatch", "id BIGINT AUTO_INCREMENT PRIMARY KEY, n INT");

    }

    @Test
    public void fixExceptionType() throws Exception {
        try {
            ResultSet rs = sharedConnection.prepareStatement("SELECT ' '").executeQuery();
            assertTrue(rs.next());

            try {
                rs.getShort(1);
            } catch (Exception e) {
                Assert.assertTrue(e instanceof SQLException);
            }
            try {
                rs.getByte(1);
            } catch (Exception e) {
                Assert.assertTrue(e instanceof SQLException);
            }
            try {
                rs.getInt(1);
            } catch (Exception e) {
                Assert.assertTrue(e instanceof SQLException);
            }
            try {
                rs.getLong(1);
            } catch (Exception e) {
                Assert.assertTrue(e instanceof SQLException);
            }
        } catch (SQLException e) {
            Assert.fail();
        }
    }

    // fix for aone 39996582
    @Test
    public void testResetStreamAndRead() {
        Assume.assumeFalse(sharedUsePrepare()); // some version send long data is not supported
        Connection conn;
        Statement stmt;

        try {
            conn = sharedConnection;
            stmt = conn.createStatement();
            stmt.executeUpdate("drop table if exists test;");
            stmt.executeUpdate("create table test(id int, b1 TINYBLOB, b2 BLOB, b3 MEDIUMBLOB, b4 LONGBLOB, b5 VARBINARY(200), c1 TINYTEXT, c2 TEXT, c3 MEDIUMTEXT, c4 LONGTEXT, c5 VARCHAR(20));");

            String s = "1231abcd奥星贝斯";
            java.sql.Clob c = new com.oceanbase.jdbc.Clob();

            c.setString(1, "1231abcd奥星贝斯");
            byte[] blobContent = s.getBytes("utf-8");
            java.sql.Blob b = new com.oceanbase.jdbc.Blob(blobContent);

            PreparedStatement insert_stmt = conn
                .prepareStatement("Insert into test values(1,?,?,?,?,?,?,?,?,?,?);");
            insert_stmt.setBlob(1, b);
            insert_stmt.setBlob(2, b);
            insert_stmt.setBlob(3, b);
            insert_stmt.setBlob(4, b);
            insert_stmt.setBlob(5, b);
            insert_stmt.setClob(6, c);
            insert_stmt.setClob(7, c);
            insert_stmt.setClob(8, c);
            insert_stmt.setClob(9, c);
            insert_stmt.setClob(10, c);

            for (int k = 1; k <= 2; k++) {
                insert_stmt.execute();
            }

            ResultSet rs = conn.createStatement().executeQuery("select * from test;");
            int count = 0;
            while (rs.next()) {
                System.out.println("count = " + count);
                for (int i = 2; i <= 6; i++) {
                    Assert.assertEquals(s,
                        new String(rs.getBlob(i).getBytes(1, (int) rs.getBlob(i).length()),
                            StandardCharsets.UTF_8));
                }
                for (int i = 7; i <= 11; i++) {
                    Assert.assertEquals(
                        s,
                        rs.getClob(i).getSubString((long) 1,
                            Math.toIntExact(rs.getClob(i).length())));
                }
                count++;
            }

            stmt.executeUpdate("drop table test;");
            stmt.close();
            insert_stmt.close();
        } catch (Exception se) {
            se.printStackTrace();
            Assert.fail();
        }
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

    @Test
    public void testAone37811114() {
        try {
            Connection conn = sharedConnection;
            Statement stmt = conn.createStatement();
            stmt.execute("drop table if exists testCallStmtLargeBatch");
            stmt.execute("create table testCallStmtLargeBatch (n INT)");
            stmt.execute("drop procedure if exists testCallStmtLargeBatchProc");
            stmt.execute("create procedure testCallStmtLargeBatchProc (a int) BEGIN \n"
                         + " INSERT INTO testCallStmtLargeBatch VALUES (a); end ");
            CallableStatement cstmt = conn.prepareCall("{CALL testCallStmtLargeBatchProc(?)}");
            int num = 5;
            for (int i = 1; i <= 5; i++) {
                cstmt.setInt(1, i);
                cstmt.addBatch();
            }
            int[] count = cstmt.executeBatch();
            assertEquals(num, count.length);
            System.out.println(Arrays.toString(count)); // Mysql = [1,1,1]
            ResultSet rs = stmt.executeQuery("select * from testCallStmtLargeBatch");
            int j = 1;
            while (rs.next()) {
                assertEquals(j, rs.getInt(1));
                j++;
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testAone38913464() {
        try {
            LocalDateTime localDateTime = LocalDateTime.now();
            Connection conn = sharedConnection;
            LocalTime localTime = LocalTime.now();
            createTable("testUpdateObject2",
                "id INT PRIMARY KEY, ot1 VARCHAR(100), ot2 BLOB, odt1 VARCHAR(100), odt2 BLOB");
            PreparedStatement ps = conn
                .prepareStatement("insert into testUpdateObject2 values (?,?,?,?,?)");
            ps.setInt(1, 1);
            ps.setObject(2, localTime);
            ps.setObject(3, localTime);
            ps.setObject(4, localDateTime);
            ps.setObject(5, localDateTime);
            ps.execute();
            ResultSet rs = conn.createStatement().executeQuery(
                "SELECT count(*) FROM testUpdateObject2");
            rs.next();
            assertEquals(1, rs.getInt(1));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    // test for clob position
    @Test
    public void testAone39073141() {
        try {
            Statement stmt = sharedConnection.createStatement();
            ResultSet rs = stmt.executeQuery("select 'abcd', 'a', 'b', 'c', 'd', 'e' from dual");
            rs.next();
            Clob clob = rs.getClob(1);
            try {
                clob.position(rs.getClob(2), 0);
                fail("Illegal starting position for search, '0'");
            } catch (SQLException e) {
                //e.printStackTrace();
            }
            try {
                clob.position(rs.getClob(2), 10);
                fail("Starting position for search is past end of CLOB");
            } catch (SQLException e) {
                //
            }
            assertEquals(1, clob.position(rs.getClob(2), 1));
            assertEquals(2, clob.position(rs.getClob(3), 1));
            assertEquals(3, clob.position(rs.getClob(4), 1));
            assertEquals(4, clob.position(rs.getClob(5), 1));
            assertEquals(-1, clob.position(rs.getClob(6), 1));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testAone36022236() {
        try {
            Connection conn = sharedConnection;
            Statement stmt = conn.createStatement();
            stmt.execute("drop table if exists testPreStmtBatchSql");
            stmt.execute("create table testPreStmtBatchSql (id BIGINT AUTO_INCREMENT PRIMARY KEY, n INT)");
            PreparedStatement ps = conn
                .prepareStatement("INSERT INTO testPreStmtBatchSql (n) VALUES (?)");
            ps.setInt(1, -1);
            ps.addBatch();
            ps.setInt(1, 0);
            ps.addBatch();
            ps.addBatch("INSERT INTO testPreStmtBatchSql (n) VALUES (1)");
            ps.addBatch("INSERT INTO testPreStmtBatchSql (n) VALUES (2)");
            ps.addBatch("INSERT INTO testPreStmtBatchSql (n) VALUES (3)");
            ps.executeBatch();
            ResultSet rs = stmt.executeQuery("select count(*) from testPreStmtBatchSql");
            assertTrue(rs.next());
            assertEquals(5, rs.getInt(1));
            ps = conn.prepareStatement("INSERT INTO testPreStmtBatchSql (n) VALUES (?)");
            ps.setInt(1, -1);
            ps.addBatch();
            ps.setInt(1, 0);
            ps.addBatch();
            ps.addBatch("INSERT INTO testPreStmtBatchSql (n) VALUES (1)");
            ps.addBatch("INSERT INTO testPreStmtBatchSql (n) VALUES (2)");
            ps.addBatch("INSERT INTO testPreStmtBatchSql (n) VALUES (3)");
            ps.executeLargeBatch();
            rs = stmt.executeQuery("select count(*) from testPreStmtBatchSql");
            assertTrue(rs.next());
            assertEquals(10, rs.getInt(1));
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testAone36022265() {
        try {
            Connection conn = sharedConnection;
            Statement stmt = conn.createStatement();
            stmt.execute("drop table if exists testCallaStmtBatch");
            stmt.execute("create table testCallaStmtBatch (id BIGINT AUTO_INCREMENT PRIMARY KEY, c1 INT)");
            stmt.execute("drop procedure if exists testCallaStmtBatchPro");
            stmt.execute("create procedure testCallaStmtBatchPro(IN n INT) BEGIN INSERT INTO testCallaStmtBatch (c1) VALUES (n); END");
            CallableStatement cs = conn.prepareCall("{CALL testCallaStmtBatchPro(?)}");
            cs.setInt(1, 1);
            cs.addBatch();
            cs.setInt(1, 2);
            cs.addBatch();
            cs.addBatch("CALL testCallaStmtBatchPro(3)");
            cs.addBatch("CALL testCallaStmtBatchPro(4)");
            cs.addBatch("CALL testCallaStmtBatchPro(5)");
            long[] counts = cs.executeLargeBatch();
            assertEquals(5, counts.length);

            ResultSet rs = cs.getGeneratedKeys();
            ResultSetMetaData rsmd = rs.getMetaData();
            assertEquals(1, rsmd.getColumnCount());
            assertEquals(Types.BIGINT, rsmd.getColumnType(1));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testAone37760229() {
        try {
            Connection conn = sharedConnection;
            Statement stmt = conn.createStatement();
            stmt.execute("DROP TABLE IF EXISTS testNanosParsing");
            stmt.execute("CREATE TABLE testNanosParsing (dateIndex int, field1 VARCHAR(32))");
            stmt.execute("INSERT INTO testNanosParsing VALUES (1, '1969-12-31 18:00:00.0'), (2, '1969-12-31 18:00:00.000000090'), "
                         + "(3, '1969-12-31 18:00:00.000000900'), (4, '1969-12-31 18:00:00.000009000'), (5, '1969-12-31 18:00:00.000090000'), "
                         + "(6, '1969-12-31 18:00:00.000900000'), (7, '1969-12-31 18:00:00.')");
            PreparedStatement ps = conn
                .prepareStatement("SELECT field1 FROM testNanosParsing ORDER BY dateIndex ASC");
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            Timestamp ts = rs.getTimestamp(1);
            assertEquals(0, rs.getTimestamp(1).getNanos());
            assertTrue(rs.next());
            assertEquals(90, rs.getTimestamp(1).getNanos());
            assertTrue(rs.next());
            assertEquals(900, rs.getTimestamp(1).getNanos());
            assertTrue(rs.next());
            assertEquals(9000, rs.getTimestamp(1).getNanos());
            assertTrue(rs.next());
            assertEquals(90000, rs.getTimestamp(1).getNanos());
            assertTrue(rs.next());
            assertEquals(900000, rs.getTimestamp(1).getNanos());
            assertTrue(rs.next());
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testAone37757559() {
        try {
            Timestamp ts = new Timestamp(System.currentTimeMillis());
            Date dt = new Date(ts.getTime());
            Time tm = new Time(ts.getTime());
            Connection conn = sharedConnection;
            createTable("testNativeConversions",
                "time_field TIME, date_field DATE, datetime_field DATETIME, timestamp_field TIMESTAMP");
            PreparedStatement ps = conn
                .prepareStatement("INSERT INTO testNativeConversions VALUES (?,?,?,?)");
            ps.setTime(1, tm);
            ps.setDate(2, dt);
            ps.setTimestamp(3, ts);
            ps.setTimestamp(4, ts);
            ps.execute();
            ps.close();
            ps = conn
                .prepareStatement("SELECT time_field, date_field, datetime_field, timestamp_field FROM testNativeConversions");
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            System.out.println(rs.getTime(1));
            System.out.println(rs.getTime(2));
            System.out.println(rs.getTime(3));
            System.out.println(rs.getTime(4));
            System.out.println();
            System.out.println(rs.getDate(1));
            System.out.println(rs.getDate(2));
            System.out.println(rs.getDate(3));
            System.out.println(rs.getDate(4));
            System.out.println();
            System.out.println(rs.getTimestamp(1));
            System.out.println(rs.getTimestamp(2));
            System.out.println(rs.getTimestamp(3));
            System.out.println(rs.getTimestamp(4));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testAone37810981() {
        try {
            Connection conn = sharedConnection;
            Statement stmt = conn.createStatement();
            stmt.execute("drop table if exists testCallStmtUpdate");
            stmt.execute("create table testCallStmtUpdate (n INT)");
            stmt.execute("drop procedure if exists testCallStmtUpdateProc");
            stmt.execute("create procedure testCallStmtUpdateProc (n1 int, n2 int, n3 int, n4 int, n5 int) BEGIN \n"
                         + " INSERT INTO testCallStmtUpdate VALUES (n1), (n2), (n3), (n4); "
                         + " INSERT INTO testCallStmtUpdate VALUES (5); END");
            CallableStatement cstmt = conn
                .prepareCall("{CALL testCallStmtUpdateProc(?, ?, ?, ?, ?)}");
            cstmt.setInt(1, 1);
            cstmt.setInt(2, 2);
            cstmt.setInt(3, 3);
            cstmt.setInt(4, 4);
            cstmt.setInt(5, 5);
            cstmt.execute();
            assertEquals(1, cstmt.getUpdateCount()); // mysql jdbc 1
            ResultSet rs = stmt.executeQuery("select * from testCallStmtUpdate");
            int j = 1;
            while (rs.next()) {
                assertEquals(j, rs.getInt(1));
                j++;
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testAone38908835() {
        try {
            LocalDateTime localDateTime = LocalDateTime.now();
            LocalTime localTime = LocalTime.now();
            LocalDate localDate = LocalDate.now();
            createTable("testUpdateObject1",
                "id INT PRIMARY KEY, d DATE, t TIME, dt DATETIME, ts TIMESTAMP");
            Connection testConn = sharedConnection;
            Statement stmt = testConn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = stmt.executeQuery("SELECT * FROM testUpdateObject1");
            rs.moveToInsertRow();
            rs.updateInt(1, 1);
            rs.updateObject(2, localDate, JDBCType.DATE);
            rs.updateObject(3, localTime, JDBCType.TIME);
            rs.updateObject(4, localDateTime, JDBCType.TIMESTAMP);
            rs.updateObject(5, localDateTime, JDBCType.TIMESTAMP);
            rs.insertRow();
            rs = stmt.executeQuery("SELECT count(*) FROM testUpdateObject1");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testAone37805924() {
        try {
            Connection conn = null;
            Statement stmt = null;

            for (int i = 0; i < 4; i++) {
                conn = setConnectionOrigin("?useServerPrepStmts=false&rewriteBatchedStatements=false");
                if (i == 1) {
                    conn = setConnectionOrigin("?useServerPrepStmts=true&rewriteBatchedStatements=false");
                }
                if (i == 2) {
                    conn = setConnectionOrigin("?useServerPrepStmts=false&rewriteBatchedStatements=true");
                }
                if (i == 3) {
                    conn = setConnectionOrigin("?useServerPrepStmts=true&rewriteBatchedStatements=true");
                }
                stmt = conn.createStatement();
                stmt.execute("DROP table if exists testExecuteLargeBatch");
                stmt.execute("create table testExecuteLargeBatch(n INT)");
                PreparedStatement ps = conn
                    .prepareStatement("INSERT INTO testExecuteLargeBatch VALUES (?)");
                int num = 5;
                for (int j = 1; j <= num; j++) {
                    ps.setInt(1, j);
                    ps.addBatch();
                }
                long[] counts = ps.executeLargeBatch();
                assertEquals(num, counts.length);
                System.out.println(Arrays.toString(counts));
                if (i == 1 || i == 0) {
                    Assert.assertEquals("[1, 1, 1, 1, 1]", Arrays.toString(counts));
                } else {
                    Assert.assertEquals("[-2, -2, -2, -2, -2]", Arrays.toString(counts));
                }
                ResultSet rs = stmt.executeQuery("select * from testExecuteLargeBatch");
                int j = 1;
                while (rs.next()) {
                    assertEquals(j, rs.getInt(1));
                    j++;
                }
                stmt.execute("DROP table if exists testExecuteLargeBatch");
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testAone38674100() {
        try {
            Connection conn = sharedConnection;
            Statement stmt = conn.createStatement();
            stmt.execute("DROP TABLE IF EXISTS foreign_yyy");
            stmt.execute("DROP TABLE IF EXISTS foreign_xxx");
            stmt.execute("create table foreign_xxx (id1 int(8) not null auto_increment primary key)");
            stmt.execute("create table foreign_yyy (foreign_1_id int(8) not null, primary key (foreign_1_id),"
                         + " CONSTRAINT foreign_fk_abcdefg foreign key (foreign_1_id) references foreign_xxx(id1))");
            DatabaseMetaData dbmd = conn.getMetaData();
            String catalog = conn.getCatalog();
            ResultSet rs = dbmd.getImportedKeys(catalog, catalog, "foreign_yyy");
            assertTrue(rs.next());
            assertEquals("foreign_fk_abcdefg", rs.getString("FK_NAME"));
            assertEquals("id1", rs.getString("PKCOLUMN_NAME"));
            assertEquals("foreign_1_id", rs.getString("FKCOLUMN_NAME"));
            assertFalse(rs.next());
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testExportAndForeignKeys() {
        try {
            Connection conn = sharedConnection;
            DatabaseMetaData metaData = conn.getMetaData();
            Statement stmt = conn.createStatement();
            stmt.execute("drop table if exists t2");
            stmt.execute("drop table if exists t1");
            stmt.execute("create table t1(o_1 int, o_2 int , o_3 int, primary key(o_1,o_2,o_3))");
            stmt.execute("create table t2(p_1 int not null,p_2 int not null,p_3 int not null,"
                         + "foreign key(p_1,p_2,p_3) references t1(o_1,o_2,o_3) on delete cascade)");
            ResultSet rs = metaData.getImportedKeys(null, null, "t2");
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getString("FKCOLUMN_NAME").equals("p_1"));
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getString("FKCOLUMN_NAME").equals("p_2"));
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getString("FKCOLUMN_NAME").equals("p_3"));

            rs = metaData.getExportedKeys(null, null, "t1");
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getString("FKCOLUMN_NAME").equals("p_1"));
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getString("FKCOLUMN_NAME").equals("p_2"));
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getString("FKCOLUMN_NAME").equals("p_3"));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    //send long data is only supported in high-level observers, temporarily ignore test case
    @Ignore
    public void testSendLongData() {
        try {
            Connection conn1;
            Connection conn2;
            Statement stmt1;
            Statement stmt2;
            conn1 = setConnection();
            stmt1 = conn1.createStatement();
            System.out.println("change max_allowed_packet");
            stmt1.executeUpdate("set global max_allowed_packet=20000000;");
            conn2 = setConnection();
            stmt2 = conn2.createStatement();

            stmt2.executeUpdate("drop table if exists test;");
            System.out.println("create test tables ");
            stmt2.executeUpdate("create table test(b LONGBLOB);");

            System.out.println("check default max_allowed_packet");
            ResultSet rs = stmt2.executeQuery("show variables like'max_allowed_packet';");
            ByteArrayInputStream fis = new ByteArrayInputStream(new byte[50000000]);
            String insert_sql = "Insert into test values(?);";
            PreparedStatement insert_stmt = conn2.prepareStatement(insert_sql);
            insert_stmt.setBlob(1, fis);
            insert_stmt.executeUpdate();
            ResultSet rs1 = stmt2.executeQuery("select length(b) from test;");
            Assert.assertTrue(rs1.next());
            Assert.assertEquals(50000000, rs1.getInt(1));
            stmt2.executeUpdate("drop table test;");
            System.out.println("reset max_allowed_packet");
            stmt1.executeUpdate("set global max_allowed_packet=DEFAULT;");
            insert_stmt.close();
            stmt1.close();
            stmt2.close();
            conn1.close();
            conn2.close();
            fis.close();
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testAone38636371() {
        try {
            Connection conn = sharedConnection;
            conn.createStatement().execute("drop procedure if exists testGetProcedureColumnsP");
            conn.createStatement().execute(
                "create procedure testGetProcedureColumnsP(c1 INT, Out c2 varchar(255), INOUT c3 decimal(9,2))"
                        + " COMMENT 'testGetProcedureColumns comment1' \n BEGIN\nSELECT 1;end");
            ResultSet rs = conn.getMetaData().getProcedureColumns(null, null,
                "testGetProcedureColumnsP", "%");

            assertTrue(rs.next());
            assertNull(rs.getString("PROCEDURE_SCHEM"));
            assertEquals("testGetProcedureColumnsP", rs.getString("PROCEDURE_NAME"));
            assertEquals("c1", rs.getString("COLUMN_NAME"));
            assertEquals(DatabaseMetaData.procedureColumnIn, rs.getShort("COLUMN_TYPE"));
            assertEquals(Types.INTEGER, rs.getInt("DATA_TYPE"));
            assertEquals("INT", rs.getString("TYPE_NAME"));
            assertEquals(10, rs.getInt("PRECISION"));
            assertEquals(10, rs.getInt("LENGTH"));
            assertEquals(0, rs.getInt("SCALE"));
            assertEquals(10, rs.getInt("RADIX"));
            assertEquals(1, rs.getShort("NULLABLE"));
            assertNull(rs.getString("REMARKS"));
            assertNull(rs.getString("COLUMN_DEF"));
            assertNull(rs.getString("SQL_DATA_TYPE"));
            assertNull(rs.getString("SQL_DATETIME_SUB"));
            assertNull(rs.getString("CHAR_OCTET_LENGTH"));
            assertEquals(1, rs.getInt("ORDINAL_POSITION"));
            assertEquals("YES", rs.getString("IS_NULLABLE"));
            assertEquals("testGetProcedureColumnsP", rs.getString("SPECIFIC_NAME"));

            assertTrue(rs.next());
            assertEquals("c2", rs.getString("COLUMN_NAME"));
            assertEquals(DatabaseMetaData.procedureColumnOut, rs.getShort("COLUMN_TYPE"));
            assertEquals(Types.VARCHAR, rs.getInt("DATA_TYPE"));
            assertEquals("VARCHAR", rs.getString("TYPE_NAME"));
            assertEquals(2, rs.getInt("ORDINAL_POSITION"));
            assertEquals(255, rs.getInt("PRECISION"));
            assertEquals(255, rs.getInt("LENGTH"));

            assertTrue(rs.next());
            assertEquals("c3", rs.getString("COLUMN_NAME"));
            assertEquals(DatabaseMetaData.procedureColumnInOut, rs.getShort("COLUMN_TYPE"));
            assertEquals(Types.DECIMAL, rs.getInt("DATA_TYPE"));
            assertEquals("DECIMAL", rs.getString("TYPE_NAME"));
            assertEquals(3, rs.getInt("ORDINAL_POSITION"));
            assertEquals(9, rs.getInt("PRECISION"));
            assertEquals(9, rs.getInt("LENGTH"));
            assertFalse(rs.next());
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testAone37601384() {
        try {
            Connection conn = sharedConnection;
            Statement stmt = conn.createStatement();
            stmt.execute("DROP TABLE IF EXISTS foreign_4");
            stmt.execute("DROP TABLE IF EXISTS foreign_3");
            stmt.execute("DROP TABLE IF EXISTS foreign_2");
            stmt.execute("DROP TABLE IF EXISTS foreign_1");
            stmt.execute("create table foreign_1 (id1 int(8) not null auto_increment primary key)");
            stmt.execute("create table foreign_2 (id2 int(8) not null auto_increment primary key)");
            stmt.execute("create table foreign_3 (foreign_13_id int(8) not null,foreign_23_id int(8) not null, primary key (foreign_13_id, foreign_23_id),"
                         + " foreign key (foreign_13_id) references foreign_1(id1),foreign key (foreign_23_id) references foreign_2(id2))");
            stmt.execute("create table foreign_4 (foreign_14_id int(8) not null,foreign_24_id int(8) not null,"
                         + "foreign key (foreign_14_id, foreign_24_id) references foreign_3(foreign_13_id, foreign_23_id))");
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getCrossReference(null, null, "foreign_3", null, null, "foreign_4");
            assertTrue(rs.next());
            assertEquals("foreign_13_id", rs.getString("PKCOLUMN_NAME")); // `foreign_23_id`
            assertEquals("foreign_14_id", rs.getString("FKCOLUMN_NAME")); // `foreign_24_id
            assertEquals("foreign_3", rs.getString("PKTABLE_NAME"));
            assertEquals("foreign_4", rs.getString("FKTABLE_NAME"));
            assertEquals(0, rs.getInt("KEY_SEQ")); // 1
            assertTrue(rs.next());
            assertEquals("foreign_23_id", rs.getString("PKCOLUMN_NAME")); // `foreign_23_id`
            assertEquals("foreign_24_id", rs.getString("FKCOLUMN_NAME")); // `foreign_24_id
            assertFalse(rs.next());
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testAone40086972() throws Exception {
        try {
            Connection conn = sharedConnection;
            String dbName = "testcreate";
            String dbName2 = "testcreate2";
            Statement stmt = conn.createStatement();
            stmt.execute("DROP DATABASE IF EXISTS " + dbName);
            stmt.execute("DROP DATABASE IF EXISTS " + dbName2);
            stmt.close();
            Properties props = new Properties();
            props.setProperty("createDatabaseIfNotExist", "true");
            props.setProperty("DBNAME", dbName2);

            Connection conn1 = DriverManager.getConnection("jdbc:oceanbase://" + hostname + ":"
                                                           + port + "/" + dbName + "?user="
                                                           + username + "&password=" + password,
                props);
            System.out.println("conn1 = " + conn1);
            Statement stmt1 = conn1.createStatement();
            ResultSet rs = stmt1.executeQuery("show databases like '" + dbName + "%'");
            assertTrue(rs.next());
            assertEquals(dbName2, rs.getString(1));
            stmt1.execute("DROP DATABASE IF EXISTS " + dbName);
            stmt1.execute("DROP DATABASE IF EXISTS " + dbName2);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // test for aone 40082115
    @Test
    public void fixClobLengthError() {
        Assume.assumeFalse(sharedUsePrepare()); // send long data not supported for low version
        try {
            Connection connt = setConnection("&characterEncoding=gbk");
            Statement stmt = connt.createStatement();

            stmt.executeUpdate("drop table if exists test;");
            stmt.executeUpdate("create table test(id INT, b1 TINYBLOB, b2 BLOB, b3 MEDIUMBLOB, b4 LONGBLOB, b5 VARBINARY(20), c1 TINYTEXT, c2 TEXT, c3 MEDIUMTEXT, c4 LONGTEXT, c5 VARCHAR(20)) CHARACTER SET gbk;");

            String s = "1231abcd奥星贝斯";
            Blob b = null;
            try {
                b = new Blob(s.getBytes("gbk"));
            } catch (Exception e) {
                e.printStackTrace();
            }
            Clob c = connt.createClob();
            Writer writer = c.setCharacterStream(1);
            writer.write(s);
            writer.flush();
            System.out.println(c.getSubString((long) 1, Math.toIntExact(c.length())));

            //查询
            ResultSet rs1 = stmt.executeQuery("show variables like 'character_set_client';");
            while (rs1.next()) {
                Assert.assertEquals("gbk", rs1.getString(2));
            }
            ResultSet rs2 = stmt.executeQuery("show variables like 'character_set_connection';");
            while (rs2.next()) {
                Assert.assertEquals("gbk", rs2.getString(2));
            }

            PreparedStatement insert_stmt = connt
                .prepareStatement("Insert into test values(?,?,?,?,?,?,?,?,?,?,?);");
            insert_stmt.setInt(1, 1);
            insert_stmt.setBlob(2, b);
            insert_stmt.setBlob(3, b);
            insert_stmt.setBlob(4, b);
            insert_stmt.setBlob(5, b);
            insert_stmt.setBlob(6, b);
            insert_stmt.setClob(7, c);
            insert_stmt.setClob(8, c);
            insert_stmt.setClob(9, c);
            insert_stmt.setClob(10, c);
            insert_stmt.setClob(11, c);
            insert_stmt.executeUpdate();

            ResultSet rs = stmt.executeQuery("select * from test;");
            while (rs.next()) {
                System.out.println(rs.getInt(1));
                for (int i = 2; i <= 6; i++) {
                    Assert.assertEquals(s,
                        new String(rs.getBlob(i).getBytes(1, (int) rs.getBlob(i).length()), "gbk"));
                }
                ;
                for (int i = 7; i <= 11; i++) {
                    Assert.assertEquals(
                        s,
                        rs.getClob(i).getSubString((long) 1,
                            Math.toIntExact(rs.getClob(i).length())));
                }
            }
            stmt.close();
            insert_stmt.close();
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testAone37636242() {
        try {
            String tableName = "testTinyint1IsBit";
            Properties props = new Properties();
            props.setProperty("tinyint1IsBit", "true");
            props.setProperty("transformedBitIsBoolean", "false");
            Connection conn = setConnection(props);
            Statement stmt = conn.createStatement();
            stmt.execute("drop table if exists " + tableName);
            stmt.execute("create table " + tableName + " (field1 TINYINT(1))");
            stmt.execute("INSERT INTO " + tableName + " VALUES (1)");
            ResultSet rs = conn.createStatement().executeQuery("SELECT field1 FROM " + tableName);
            rs.next();
            assertEquals("java.lang.Boolean", rs.getObject(1).getClass().getName());
            assertEquals(Types.BIT, rs.getMetaData().getColumnType(1));
            rs = conn.prepareStatement("SELECT field1 FROM " + tableName).executeQuery();
            rs.next();
            assertEquals(Types.BIT, rs.getMetaData().getColumnType(1));
            rs = conn.getMetaData().getColumns(conn.getCatalog(), null, tableName, "field1");
            assertTrue(rs.next());
            assertEquals(Types.BIT, rs.getInt("DATA_TYPE"));
            assertEquals("BIT", rs.getString("TYPE_NAME"));
            props.clear();
            props.setProperty("tinyint1IsBit", "true");
            props.setProperty("transformedBitIsBoolean", "true");
            Connection conn1 = setConnection(props);
            rs = conn1.createStatement().executeQuery("SELECT field1 FROM " + tableName);
            rs.next();
            assertEquals("TINYINT", rs.getMetaData().getColumnTypeName(1));
            assertEquals("java.lang.Boolean", rs.getMetaData().getColumnClassName(1));
            assertEquals(Types.BOOLEAN, rs.getMetaData().getColumnType(1)); // BIT
            rs = conn1.prepareStatement("SELECT field1 FROM " + tableName).executeQuery();
            rs.next();
            assertEquals(Types.BOOLEAN, rs.getMetaData().getColumnType(1));
            rs = conn1.getMetaData().getColumns(conn.getCatalog(), null, tableName, "field1");
            assertTrue(rs.next());
            assertEquals(Types.BOOLEAN, rs.getInt("DATA_TYPE"));
            assertEquals("BOOLEAN", rs.getString("TYPE_NAME")); //BIT
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testAone36022715() {
        try {
            Connection conn = setConnection("&characterEncoding=utf8&connectionCollation=gbk_bin");
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW VARIABLES LIKE 'collation_connection'");
            assertTrue(rs.next());
            assertEquals("gbk_bin", rs.getString(2));
        } catch (SQLException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testCallStmtBatchSqlBug36022265_Mysql() throws Exception {
        try {
            Connection conn = sharedConnection;
            Statement stmt = conn.createStatement();
            stmt.execute("drop table if exists testCallaStmtBatch");
            stmt.execute("create table testCallaStmtBatch (id BIGINT AUTO_INCREMENT PRIMARY KEY, c1 INT)");
            stmt.execute("drop procedure if exists testCallaStmtBatchPro");
            stmt.execute("create procedure testCallaStmtBatchPro(IN n INT) BEGIN INSERT INTO testCallaStmtBatch (c1) VALUES (n); END");

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
    public void testAone40296580() throws IOException {
        Assume.assumeFalse(sharedUsePrepare()); // some version send long data is not supported
        Connection conn = null;
        Statement stmt = null;
        String s1 = "先帝创业未半而中道崩殂，今天下三分，益州疲弊，此诚危急存亡之秋也。然侍卫之臣不懈于内，忠志之士忘身于外者，盖追先帝之殊遇，欲报之于陛下也。诚宜开张圣听，以光先帝遗德，恢弘志士之气，不宜妄自菲薄，引喻失义，以塞忠谏之路也。\n宫中府中，俱为一体，陟罚臧否，不宜异同。若有作奸犯科及为忠善者，宜付有司论其刑赏，以昭陛下平明之理，不宜偏私，使内外异法也。";
        String s2 = "52,44,112,97,115,115,119,111,114,100,58,52,52,52,52,125";
        File txt = File.createTempFile("temp1", ".txt");
        File txt_s = File.createTempFile("temp2", ".txt");
        txt.deleteOnExit();
        txt_s.deleteOnExit();

        FileWriter writer1 = new FileWriter(txt);
        writer1.write(s1);
        writer1.flush();
        writer1.close();

        FileWriter writer2 = new FileWriter(txt_s);
        writer2.write(s2);
        writer2.flush();
        writer2.close();
        try {
            conn = sharedConnection;
            stmt = conn.createStatement();
            stmt.executeUpdate("drop table if exists test");
            stmt.executeUpdate("create table test(id INT, c1 TINYTEXT, c2 TEXT, c3 MEDIUMTEXT, c4 LONGTEXT, c5 VARCHAR(100));");
            FileReader fis1 = new FileReader(txt_s);
            FileReader fis2 = new FileReader(txt);
            FileReader fis3 = new FileReader(txt);
            FileReader fis4 = new FileReader(txt);
            FileReader fis5 = new FileReader(txt_s);

            String insert_sql = "Insert into test values(?,?,?,?,?,?);";
            PreparedStatement insert_stmt = conn.prepareStatement(insert_sql);
            insert_stmt.setInt(1, 1);
            insert_stmt.setCharacterStream(2, fis1);
            insert_stmt.setCharacterStream(3, fis2);
            insert_stmt.setCharacterStream(4, fis3);
            insert_stmt.setCharacterStream(5, fis4);
            insert_stmt.setCharacterStream(6, fis5);
            insert_stmt.executeUpdate();

            String insert_null_sql = "Insert into test values(?,?,?,?,?,?);";
            PreparedStatement insert_null_stmt = conn.prepareStatement(insert_null_sql);
            insert_null_stmt.setInt(1, 2);
            insert_null_stmt.setCharacterStream(2, (Reader) null);
            insert_null_stmt.setCharacterStream(3, (Reader) null);
            insert_null_stmt.setCharacterStream(4, (Reader) null);
            insert_null_stmt.setCharacterStream(5, (Reader) null);
            insert_null_stmt.setCharacterStream(6, (Reader) null);
            insert_null_stmt.executeUpdate();

            ResultSet rs = stmt.executeQuery("select * from test where id=1");
            while (rs.next()) {
                for (int i = 2; i <= 6; i++) {
                    System.out.println(rs.getClob(i).getSubString((long) 1,
                        Math.toIntExact(rs.getClob(i).length())));
                }
                ;
            }
            stmt.executeUpdate("drop table test;");

            insert_stmt.close();
            insert_null_stmt.close();
            stmt.close();
            fis1.close();
            fis2.close();
            fis3.close();
            fis4.close();
            fis5.close();
        } catch (SQLException se) {
            se.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null)
                    stmt.close();
            } catch (SQLException se2) {
            }
        }
    }

    @Test
    public void testAone40298448() {
        Connection conn;
        Statement stmt;
        try {
            conn = sharedConnection;
            stmt = conn.createStatement();
            stmt.executeUpdate("drop table if exists test;");
            stmt.executeUpdate("create table test(b1 TINYBLOB, b2 BLOB, b3 MEDIUMBLOB, b4 LONGBLOB, b5 VARBINARY(200), c1 TINYTEXT, c2 TEXT, c3 MEDIUMTEXT, c4 LONGTEXT, c5 VARCHAR(20));");

            String s = "1231abcd奥星贝斯";
            Blob b = new com.oceanbase.jdbc.Blob(s.getBytes(StandardCharsets.UTF_8));
            java.sql.Clob c = conn.createClob();
            Writer writer = c.setCharacterStream(1);
            writer.write(s);
            writer.flush();

            PreparedStatement insert_stmt = conn
                .prepareStatement("Insert into test values(?,?,?,?,?,?,?,?,?,?);");
            insert_stmt.setBlob(1, b);
            insert_stmt.setBlob(2, b);
            insert_stmt.setBlob(3, b);
            insert_stmt.setBlob(4, b);
            insert_stmt.setBlob(5, b);
            insert_stmt.setClob(6, c);
            insert_stmt.setClob(7, c);
            insert_stmt.setClob(8, c);
            insert_stmt.setClob(9, c);
            insert_stmt.setClob(10, c);
            insert_stmt.addBatch();
            insert_stmt.executeBatch();

            ResultSet rs = conn.createStatement().executeQuery("select * from test limit 1;");
            while (rs.next()) {
                for (int i = 1; i <= 5; i++) {
                    Assert.assertEquals(s,
                        new String(rs.getBlob(i).getBytes(1, (int) rs.getBlob(i).length()),
                            StandardCharsets.UTF_8));
                }
                for (int i = 6; i <= 10; i++) {
                    Assert.assertEquals(
                        s,
                        rs.getClob(i).getSubString((long) 1,
                            Math.toIntExact(rs.getClob(i).length())));
                }
                ;
            }

            String delete_sql = "delete from test where b1=? and b2=? and b3=? and b4=? and b5=? and c1=? and c2=? and c3=? and c4=? and c5=?;";
            PreparedStatement delete_stmt = conn.prepareStatement(delete_sql);
            delete_stmt.setBlob(1, b);
            delete_stmt.setBlob(2, b);
            delete_stmt.setBlob(3, b);
            delete_stmt.setBlob(4, b);
            delete_stmt.setBlob(5, b);
            delete_stmt.setClob(6, c);
            delete_stmt.setClob(7, c);
            delete_stmt.setClob(8, c);
            delete_stmt.setClob(9, c);
            delete_stmt.setClob(10, c);
            delete_stmt.executeUpdate();

            ResultSet rs1 = stmt.executeQuery("select * from test;");
            while (rs1.next()) {
                Assert.assertNull(rs1);
            }
            stmt.executeUpdate("drop table test;");
            stmt.close();
            insert_stmt.close();
            delete_stmt.close();
        } catch (Exception se) {
            se.printStackTrace();
        }
    }

    @Test
    public void testGetObjectTimestamp40336255() {

        Timestamp timestamp = Timestamp.valueOf("2020-12-12 12:12:12.1");
        ResultSet rs = null;
        try {
            PreparedStatement ps = sharedConnection.prepareStatement("select ?");
            ps.setTimestamp(1, timestamp);
            rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals(timestamp, rs.getTimestamp(1));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testBug84189() {
        Assume.assumeTrue(sharedUsePrepare());
        try {
            Statement stmt = sharedConnection.createStatement();
            createTable("testBug84189",
                "d DATE NULL, t TIME NULL, dt DATETIME NULL, ts TIMESTAMP NULL, ot VARCHAR(100), odt VARCHAR(100)");
            stmt.execute("INSERT INTO testBug84189 VALUES ('2017-01-01', '10:20:30', '2017-01-01 10:20:30', '2017-01-01 10:20:30', '10:20:30+04:00', '2017-01-01T10:20:30+04:00')");
            stmt.execute("INSERT INTO testBug84189 VALUES (NULL, NULL, NULL, NULL, NULL, NULL)");

            ResultSet rs = stmt.executeQuery("SELECT * FROM testBug84189");
            assertTrue(rs.next());
            assertEquals(LocalDate.of(2017, 1, 1), rs.getObject(1, LocalDate.class));
            assertEquals(LocalTime.of(10, 20, 30), rs.getObject(2, LocalTime.class));
            assertEquals(LocalDateTime.of(2017, 1, 1, 10, 20, 30),
                rs.getObject(3, LocalDateTime.class));
            assertEquals(LocalDateTime.of(2017, 1, 1, 10, 20, 30),
                rs.getObject(4, LocalDateTime.class));
            assertEquals(OffsetTime.of(10, 20, 30, 0, ZoneOffset.ofHours(4)),
                rs.getObject(5, OffsetTime.class));
            assertEquals(OffsetDateTime.of(2017, 01, 01, 10, 20, 30, 0, ZoneOffset.ofHours(4)),
                rs.getObject(6, OffsetDateTime.class));

            assertEquals(LocalDate.class, rs.getObject(1, LocalDate.class).getClass());
            assertEquals(LocalTime.class, rs.getObject(2, LocalTime.class).getClass());
            assertEquals(LocalDateTime.class, rs.getObject(3, LocalDateTime.class).getClass());
            assertEquals(LocalDateTime.class, rs.getObject(4, LocalDateTime.class).getClass());
            assertEquals(OffsetTime.class, rs.getObject(5, OffsetTime.class).getClass());
            assertEquals(OffsetDateTime.class, rs.getObject(6, OffsetDateTime.class).getClass());

            assertTrue(rs.next());
            assertNull(rs.getObject(1, LocalDate.class));
            assertNull(rs.getObject(2, LocalTime.class));
            assertNull(rs.getObject(3, LocalDateTime.class));
            assertNull(rs.getObject(4, LocalDateTime.class));
            assertNull(rs.getObject(5, OffsetTime.class));
            assertNull(rs.getObject(6, OffsetDateTime.class));
            assertFalse(rs.next());

            PreparedStatement ps = sharedConnection.prepareStatement("SELECT * FROM testBug84189");
            rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals(LocalDate.of(2017, 1, 1), rs.getObject(1, LocalDate.class));
            assertEquals(LocalTime.of(10, 20, 30), rs.getObject(2, LocalTime.class));
            assertEquals(LocalDateTime.of(2017, 1, 1, 10, 20, 30),
                rs.getObject(3, LocalDateTime.class));
            assertEquals(LocalDateTime.of(2017, 1, 1, 10, 20, 30),
                rs.getObject(4, LocalDateTime.class));
            assertEquals(OffsetTime.of(10, 20, 30, 0, ZoneOffset.ofHours(4)),
                rs.getObject(5, OffsetTime.class));
            assertEquals(OffsetDateTime.of(2017, 01, 01, 10, 20, 30, 0, ZoneOffset.ofHours(4)),
                rs.getObject(6, OffsetDateTime.class));

            assertEquals(LocalDate.class, rs.getObject(1, LocalDate.class).getClass());
            assertEquals(LocalTime.class, rs.getObject(2, LocalTime.class).getClass());
            assertEquals(LocalDateTime.class, rs.getObject(3, LocalDateTime.class).getClass());
            assertEquals(LocalDateTime.class, rs.getObject(4, LocalDateTime.class).getClass());
            assertEquals(OffsetTime.class, rs.getObject(5, OffsetTime.class).getClass());
            assertEquals(OffsetDateTime.class, rs.getObject(6, OffsetDateTime.class).getClass());

            assertTrue(rs.next());
            assertNull(rs.getObject(1, LocalDate.class));
            assertNull(rs.getObject(2, LocalTime.class));
            assertNull(rs.getObject(3, LocalDateTime.class));
            assertNull(rs.getObject(4, LocalDateTime.class));
            assertNull(rs.getObject(5, OffsetTime.class));
            assertNull(rs.getObject(6, OffsetDateTime.class));
            assertFalse(rs.next());
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testCurrentTimestmap() {
        Assume.assumeTrue(sharedUsePrepare());
        try {
            Statement stmt = sharedConnection.createStatement();
            ResultSet rs = stmt.executeQuery("select CURRENT_TIMESTAMP");
            Assert.assertTrue(rs.next());
            rs.getObject(1, OffsetDateTime.class);
            PreparedStatement ps = sharedConnection.prepareStatement("select CURRENT_TIMESTAMP");
            rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            rs.getObject(1, OffsetDateTime.class);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testLobMysql() {
        try {
            createTable("testLobMysql", "c1 varchar(100), c2 blob");
            String sql = "insert into testLobMysql values (?, ?)";
            PreparedStatement ps = sharedConnection.prepareStatement(sql);
            String str = "abcABC中文";
            Clob clob = new com.oceanbase.jdbc.Clob();
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

            ResultSet rs = sharedConnection.prepareStatement("select * from testLobMysql")
                .executeQuery();
            assertTrue(rs.next());
            assertEquals(str, rs.getString(1));
            assertEquals(new String(bytes), new String(rs.getBytes(2)));
            assertTrue(rs.next());
            assertEquals(str, rs.getString(1));
            assertEquals(new String(bytes), new String(rs.getBytes(2)));
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testDataSource() {
        try {
            OceanBaseDataSource ds = new OceanBaseDataSource("localhost", 1130, "db");
            Connection connection = ds.getConnection("username", "password");
        } catch (Exception throwables) {
            Assert.assertTrue(throwables instanceof SQLNonTransientConnectionException);
            Assert
                .assertTrue(throwables
                    .getMessage()
                    .contains(
                        "Could not connect to HostAddress{host='localhost', port=1130, type='master'}. Connection refused (Connection refused)"));
        }
    }

    @Test
    public void testAone40082722() {
        Connection conn;
        Statement stmt;

        try {
            conn = sharedConnection;
            stmt = conn.createStatement();

            stmt.executeUpdate("drop table if exists test;");
            stmt.executeUpdate("create table test(id int, b1 TINYBLOB, b2 BLOB, b3 MEDIUMBLOB, b4 LONGBLOB, b5 VARBINARY(200), c1 TINYTEXT, c2 TEXT, c3 MEDIUMTEXT, c4 LONGTEXT, c5 VARCHAR(20));");

            String str = "abcABC中文";
            Clob c = new com.oceanbase.jdbc.Clob();
            Writer writer = c.setCharacterStream(1);
            writer.write(str, 0, 8);
            writer.flush();
            byte[] bytes = str.getBytes();
            Blob b = new com.oceanbase.jdbc.Blob(bytes);
            PreparedStatement insert_stmt = conn
                .prepareStatement("Insert into test values(1,?,?,?,?,?,?,?,?,?,?);");
            insert_stmt.setBlob(1, b);
            insert_stmt.setBlob(2, b);
            insert_stmt.setBlob(3, b);
            insert_stmt.setBlob(4, b);
            insert_stmt.setBlob(5, b);
            insert_stmt.setClob(6, c);
            insert_stmt.setClob(7, c);
            insert_stmt.setClob(8, c);
            insert_stmt.setClob(9, c);
            insert_stmt.setClob(10, c);
            for (int k = 1; k <= 8; k++) {
                insert_stmt.execute();
            }

            ResultSet rs = conn.createStatement().executeQuery("select * from test;");
            while (rs.next()) {
                for (int i = 2; i <= 6; i++) {
                    Assert.assertEquals(str,
                        new String(rs.getBlob(i).getBytes(1, (int) rs.getBlob(i).length()),
                            StandardCharsets.UTF_8));
                }
                for (int i = 7; i <= 11; i++) {
                    Assert.assertEquals(
                        str,
                        rs.getClob(i).getSubString((long) 1,
                            Math.toIntExact(rs.getClob(i).length())));
                }
                ;
            }

            stmt.close();
            insert_stmt.close();
        } catch (Exception se) {
            se.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testCallStmtBatchSqlBugAone40302848() {
        try {
            Connection conn = sharedConnection;
            Statement stmt = conn.createStatement();
            stmt.execute("drop table if exists testCallaStmtBatch");
            stmt.execute("create table testCallaStmtBatch (id BIGINT AUTO_INCREMENT PRIMARY KEY, c1 INT)");
            stmt.execute("drop procedure if exists testCallaStmtBatchPro");
            stmt.execute("create procedure testCallaStmtBatchPro(IN n INT) BEGIN INSERT INTO testCallaStmtBatch (c1) VALUES (n); END");

            CallableStatement cs = conn.prepareCall("{CALL testCallaStmtBatchPro(?)}");
            cs.setInt(1, 1);
            cs.addBatch();
            cs.setInt(1, 2);
            cs.addBatch();
            cs.addBatch("{CALL testCallaStmtBatchPro(3)}");
            cs.addBatch("{CALL testCallaStmtBatchPro(4)}");
            cs.addBatch("{CALL testCallaStmtBatchPro(5)}");

            long[] counts = cs.executeLargeBatch();
            assertEquals(5, counts.length);

            ResultSet rs = cs.getGeneratedKeys();
            ResultSetMetaData rsmd = rs.getMetaData();
            assertEquals(1, rsmd.getColumnCount());
            assertEquals(Types.BIGINT, rsmd.getColumnType(1));
        } catch (SQLException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testGetWarnings() {
        try {
            Connection conn = sharedConnection;
            Statement stmt = conn.createStatement();
            try {
                stmt.execute("select * from dual");
            } catch (SQLException e) {
                SQLWarning warning = stmt.getWarnings();
                Assert.assertNotNull(warning);
                Assert.assertTrue(warning.getMessage().contains("No tables used"));
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            Assert.fail();
        }
    }

    // The affected rows will fixed by OB 4.0,We will run this case after OB4.0 Released.
    @Ignore
    public void testCallStmtExecuteLargeUpdateBug40830770() {
        try {
            Connection conn = sharedConnection;
            Statement stmt = conn.createStatement();
            stmt.execute("drop table if exists testCallStmtLargeUpdate");
            stmt.execute("create table testCallStmtLargeUpdate (n INT)");
            stmt.execute("drop procedure if exists testCallStmtLargeUpdateProc");
            stmt.execute("create procedure testCallStmtLargeUpdateProc (n1 int, n2 int, n3 int, n4 int, n5 int) BEGIN \n"
                         + " INSERT INTO testCallStmtLargeUpdate VALUES (n1), (n2), (n3), (n4); "
                         + " INSERT INTO testCallStmtLargeUpdate VALUES (5),(6); END");

            CallableStatement cstmt = conn
                .prepareCall("{CALL testCallStmtLargeUpdateProc(?, ?, ?, ?, ?)}");
            cstmt.setInt(1, 1);
            cstmt.setInt(2, 2);
            cstmt.setInt(3, 3);
            cstmt.setInt(4, 4);
            cstmt.setInt(5, 5);

            long count = cstmt.executeLargeUpdate(); //ob 空指
            assertEquals(2, count);
            long t = cstmt.getLargeUpdateCount();
            assertEquals(2, t);

            ResultSet rs = stmt.executeQuery("select * from testCallStmtLargeUpdate");
            int j = 1;
            while (rs.next()) {
                assertEquals(j, rs.getInt(1));
                j++;
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testPrepStmtExecuteLargeUpdate() {
        try {
            createTable("test1", "id int");

            PreparedStatement ps = sharedConnection
                .prepareStatement("INSERT INTO test1 VALUES (?), (?), (?), (?), (?)");
            ps.setInt(1, 1);
            ps.setInt(2, 2);
            ps.setInt(3, 3);
            ps.setInt(4, 4);
            ps.setInt(5, 5);

            long count = ps.executeLargeUpdate();
            assertEquals(5, count);
            assertEquals(5, ps.getLargeUpdateCount());
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testConnClosedStmt() {
        try {
            Connection conn = setConnection();
            Statement stmt = conn.createStatement();
            conn.close();
            stmt.getResultSet();
            fail("Should've caught an exception here");
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains(
                "Cannot do an operation on a closed statement"));
        }
    }

    @Test
    public void testAllDataTypeMysql() {
        try {
            Connection conn = sharedConnection;
            Map<String, Object> map = new HashMap<>();
            map.put("TypeNameCol1","BLOB");
            map.put("TypeNameCol2","MEDIUMBLOB");
            map.put("TypeNameCol3","VARCHAR");
            map.put("TypeNameCol4","VARCHAR");
            map.put("TypeValueCol1",-4);
            map.put("TypeValueCol2",-4);
            map.put("TypeValueCol3",-1);
            map.put("TypeValueCol4",-1);
            Statement statement = conn.createStatement();
            try {
                statement.execute("drop table ALL_DATATYPE_TEST");
            } catch (SQLException e) {
                // e.printStackTrace();
            }
            statement.execute("create table ALL_DATATYPE_TEST(c1 int, c2 tinyInt, c3 smallInt, c4 mediumInt, c5 bigInt," +
                " c6 float, c7 double, c8 decimal(5,2), c9 date, c10 time, c11 year, c12 datetime, c13 timestamp," +
                " c14 char(20), c15 varchar(20), c16 tinyBLob, c17 Blob, c18 mediumBlob, c19 longBlob, c20 tinyText," +
                " c21 Text, c22 mediumText, c23 longText, c24 binary(20), c25 varBinary(20))");
            ResultSet rs = statement.executeQuery("select c17,c18,c21,c22 FROM ALL_DATATYPE_TEST");
            for(int i = 1; i <= 4;i++) {
                String columnTypeName = rs.getMetaData().getColumnTypeName(i);
                int columnType = rs.getMetaData().getColumnType(i);
                Assert.assertEquals(map.get("TypeNameCol" + i),columnTypeName);
                Assert.assertEquals(map.get("TypeValueCol" + i),columnType);
            }

        } catch (SQLException throwables) {
            throwables.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testUpdateCounts1() {
        try {
            Connection conn = setConnection("&continueBatchOnError=true&rewriteBatchedStatements=true");
            Statement stmt = conn.createStatement();

            stmt.execute("drop table if exists test1");
            stmt.execute("create table test1(c1 int)");
            int paramCount = 10;

            PreparedStatement ps = conn.prepareStatement("insert into test1 values(?)");

            for (int j = 1; j <= paramCount; j++) {
                if (j == 5) {
                    ps.setString(1, "s");
                } else {
                    ps.setInt(1, j);
                }
                ps.addBatch();
            }
            try {
                ps.executeBatch();
            } catch (BatchUpdateException e) {
                int counts[] = e.getUpdateCounts();
                System.out.println(Arrays.toString(counts));
                Assert.assertEquals(10, counts.length);
            }
            ResultSet rs = stmt.executeQuery("select count(*) from test1");
            rs.next();
            Assert.assertEquals(0, rs.getInt(1));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testProcedureWithQuotes() throws Exception {
        createTable("test1", "col varchar(40)");

        createProcedure("testPro1",
            "(IN c1 varchar(255)) BEGIN insert into test1(col) values(c1); END");

        CallableStatement cstmt = sharedConnection.prepareCall("{call testPro1(?)}");
        cstmt.setString(1, "'john'");
        cstmt.execute();

        ResultSet rs = sharedConnection.createStatement().executeQuery("select col from test1");
        assertTrue(rs.next());
        assertEquals("'john'", rs.getString(1));
    }

    @Test
    public void testParameterWithBackQuotes() throws Exception {
        createProcedure("testPro", "(c1 int, `c2-c3` int, c4 int) begin select 1; end");
        ResultSet rs = sharedConnection.getMetaData().getProcedureColumns(null, null, "testPro",
            "%");
        assertTrue(rs.next());
        assertEquals("c1", rs.getString("COLUMN_NAME"));
        assertTrue(rs.next());
        assertEquals("c2-c3", rs.getString("COLUMN_NAME"));
        assertTrue(rs.next());
        assertEquals("c4", rs.getString("COLUMN_NAME"));

    }

    @Test
    public void testProcedureWithBackQuotes() {
        try {
            Statement stmt = sharedConnection.createStatement();
            stmt.execute("drop procedure if exists `test-pro`");
            stmt.execute("create procedure `test-Pro`(a int) begin select a; end");
            CallableStatement cs = sharedConnection.prepareCall("call `test-pro`(?)");
            cs.setInt(1, 1);
            cs.execute();
            Assert.assertEquals("java.lang.Integer", cs.getParameterMetaData().getParameterClassName(1));;
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testBigDecimalToBoolean() {
        try {
            PreparedStatement pstmt = sharedConnection.prepareStatement("SELECT ?");
            pstmt.setObject(1, "Y", Types.BOOLEAN);
            pstmt.setObject(1, "true", Types.BOOLEAN);
            pstmt.setObject(1, "3", Types.BOOLEAN);
            pstmt.setObject(1, new BigDecimal("3"), Types.BOOLEAN);
            pstmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testSetObjectTypesBOOLEAN() throws Exception {
        String[] falses = new String[] { "False", "0", "n", "-0", "0.00", "-0.0" };
        PreparedStatement ps = sharedConnection.prepareStatement("select ?");
        int[] ret = new int[] { 0, 0, 0, 0, 0, 0 };
        int index = 0;
        for (String val : falses) {
            ps.clearParameters();
            ps.setObject(1, val, Types.BOOLEAN);
            ResultSet rs = ps.executeQuery();
            rs.next();
            Assert.assertEquals(ret[index++], rs.getInt(1));
        }
    }

    @Test
    public void testClosePS() throws SQLException {
        PreparedStatement ps = sharedConnection.prepareStatement("select 1");
        ps.close();
        try {
            ps.clearParameters();
            Assert.fail();
        } catch (SQLException e) {
            //
        }
    }

    @Test
    public void testProcedureBlob() throws SQLException {
        createProcedure("Pro_blob", "(c1 BLOB) BEGIN SELECT c1 ;\nEND");
        CallableStatement cstmt = sharedConnection.prepareCall("{call Pro_blob(?)}");
        byte[] bytes = { 1, 2, 3 };
        cstmt.setBytes(1, bytes);
        cstmt.execute();
        ResultSet rs = cstmt.getResultSet();
        assertTrue(rs.next());
        assertEquals(Arrays.toString(bytes), Arrays.toString(rs.getBytes(1)));
    }

    @Test
    public void testSetDateWithLocale() throws Exception {
        Locale.setDefault(new Locale("th", "TH"));
        System.out.println(Locale.getDefault());
        Connection conn = sharedConnection;
        Statement stmt = conn.createStatement();
        PreparedStatement prepStmt = null;
        try {
            try {
                stmt.execute("drop table testLocale");
            } catch (SQLException e) {
                //                throw new RuntimeException(e);
            }
            stmt.execute("create table testLocale(field1 DATE, field2 TIMESTAMP)");
            stmt.execute("INSERT INTO testLocale VALUES (NOW(), NOW())");

            ResultSet rs = stmt.executeQuery("SELECT field1, field2 FROM testLocale");
            rs.next();

            Date origDate = rs.getDate(1);
            Timestamp origTimestamp = rs.getTimestamp(1);
            rs.close();

            stmt.executeUpdate("TRUNCATE TABLE testLocale");

            prepStmt = conn.prepareStatement("INSERT INTO testLocale VALUES (?,?)");
            prepStmt.setDate(1, origDate);
            prepStmt.setTimestamp(2, origTimestamp);
            prepStmt.executeUpdate();

            rs = stmt.executeQuery("SELECT field1, field2 FROM testLocale");
            rs.next();

            Date testDate = rs.getDate(1);
            Timestamp testTimestamp = rs.getTimestamp(1);
            rs.close();

            assertEquals(origDate, testDate);
            assertEquals(origTimestamp, testTimestamp);

        } finally {
            Locale.setDefault(new Locale("zh", "CN"));
        }
    }

    @Test
    public void testSetObjectTypesBIT() throws Exception {
        PreparedStatement ps = sharedConnection.prepareStatement("select ?");
        ps.setObject(1, "false", Types.BIT);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertFalse(rs.getBoolean(1));
    }

    @Test
    public void testBatchValue() {
        try {
            Connection conn = setConnection("&rewriteBatchedStatements=true");
            PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + testBatchValue
                                                            + " VALUE (?, ?)");
            for (int i = 1; i <= 3; i++) {
                pstmt.setInt(1, i);
                pstmt.setInt(2, i);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        } catch (SQLException e) {
            Assert.fail();
        }
    }

    @Test
    public void testCharacterEncoding() {
        try {
            setConnection("&characterEncoding=latin1");
            fail("Exception should've been thrown");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Unknown character set: 'latin1'"));
            e.printStackTrace();
        }
        System.out.println("================");
        try {
            setConnection("&characterEncoding=NonexistentEncoding");
            fail("Exception should've been thrown");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof UnsupportedCharsetException);
            e.printStackTrace();
        }
    }

    @Test
    public void fixTwoSingleQuotations1() {
        try {
            createTable("tabletodrop", "c1 int");
            try {
                Statement s = sharedConnection.createStatement();
                s.execute("create procedure pl_sql2(test_str varchar(4000), inner_sql2 varchar(4000)) begin set @a=inner_sql2; prepare stmt from @a; execute stmt; deallocate prepare stmt; end;");
            } catch (Exception e) {
            }

            CallableStatement callStmt = sharedConnection
                .prepareCall("CALL pl_sql2('what is your name?', ?)");
            callStmt.setString(1, "insert into tabletodrop values(1)  /*+ use_px parallel(2) */ ");
            callStmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testGetBestRowIdentifier23() throws SQLException {
        DatabaseMetaData metaData = sharedConnection.getMetaData();
        ResultSet rs = metaData.getBestRowIdentifier(null, null, testBestRow, 1, false);
        assertTrue(rs.next());
        assertEquals(metaData.bestRowSession, rs.getInt("SCOPE")); //bestRowTemporary
        assertEquals("c1", rs.getString("COLUMN_NAME"));
        assertEquals(Types.INTEGER, rs.getInt("DATA_TYPE"));
        assertEquals("int", rs.getString("TYPE_NAME"));
        assertEquals(11, rs.getInt("COLUMN_SIZE"));
        assertEquals(11, rs.getInt("BUFFER_LENGTH")); //0
        assertEquals(0, rs.getInt("DECIMAL_DIGITS"));
        assertEquals(metaData.bestRowNotPseudo, rs.getInt("PSEUDO_COLUMN"));
        assertFalse(rs.next());
    }

    @Test
    public void testBug10310_1() throws Exception {
        Connection conn = sharedConnection;
        Statement stmt = conn.createStatement();
        CallableStatement cStmt = null;
        ResultSet rs = null;

        stmt.executeUpdate("DROP FUNCTION IF EXISTS testBug10310");
        stmt.executeUpdate("CREATE FUNCTION testBug10310(a float, b bigint, c int) RETURNS INT NO SQL\nBEGIN\nRETURN a;\nEND");
        cStmt = conn.prepareCall("{? = CALL testBug10310(?,?,?)}");
        cStmt.registerOutParameter(1, Types.INTEGER);
        cStmt.setFloat(2, 2);
        cStmt.setInt(3, 1);
        cStmt.setInt(4, 1);
        assertEquals(4, cStmt.getParameterMetaData().getParameterCount());
        assertEquals(Types.INTEGER, cStmt.getParameterMetaData().getParameterType(1));

        DatabaseMetaData dbmd = conn.getMetaData();

        rs = dbmd.getFunctionColumns(conn.getCatalog(), null, "testBug10310", "%");
        ResultSetMetaData rsmd = rs.getMetaData();

        assertEquals(17, rsmd.getColumnCount());
        assertEquals("FUNCTION_CAT", rsmd.getColumnName(1));
        assertEquals("FUNCTION_SCHEM", rsmd.getColumnName(2));
        assertEquals("FUNCTION_NAME", rsmd.getColumnName(3));
        assertEquals("COLUMN_NAME", rsmd.getColumnName(4));
        assertEquals("COLUMN_TYPE", rsmd.getColumnName(5));
        assertEquals("DATA_TYPE", rsmd.getColumnName(6));
        assertEquals("TYPE_NAME", rsmd.getColumnName(7));
        assertEquals("PRECISION", rsmd.getColumnName(8));
        assertEquals("LENGTH", rsmd.getColumnName(9));
        assertEquals("SCALE", rsmd.getColumnName(10));
        assertEquals("RADIX", rsmd.getColumnName(11));
        assertEquals("NULLABLE", rsmd.getColumnName(12));
        assertEquals("REMARKS", rsmd.getColumnName(13));
        assertEquals("CHAR_OCTET_LENGTH", rsmd.getColumnName(14));
        assertEquals("ORDINAL_POSITION", rsmd.getColumnName(15));
        assertEquals("IS_NULLABLE", rsmd.getColumnName(16));
        assertEquals("SPECIFIC_NAME", rsmd.getColumnName(17));

        rs.close();

        assertFalse(cStmt.execute());
        assertEquals(2f, cStmt.getInt(1), .001);
        assertEquals("java.lang.Integer", cStmt.getObject(1).getClass().getName());

        assertEquals(-1, cStmt.executeUpdate());
        assertEquals(2f, cStmt.getInt(1), .001);
        assertEquals("java.lang.Integer", cStmt.getObject(1).getClass().getName());

        cStmt.setFloat("a", 4);
        cStmt.setInt("b", 1);
        cStmt.setInt("c", 1);

        assertFalse(cStmt.execute());
        assertEquals(4f, cStmt.getInt(1), .001);
        assertEquals("java.lang.Integer", cStmt.getObject(1).getClass().getName());

        assertEquals(-1, cStmt.executeUpdate());
        assertEquals(4f, cStmt.getInt(1), .001);
        assertEquals("java.lang.Integer", cStmt.getObject(1).getClass().getName());

        // Check metadata while we're at it

        rs = dbmd.getProcedures(conn.getCatalog(), null, "testBug10310");
        rs.next();
        assertEquals("testBug10310", rs.getString("PROCEDURE_NAME"));
        assertEquals(DatabaseMetaData.procedureReturnsResult, rs.getShort("PROCEDURE_TYPE"));
        cStmt.setNull(2, Types.FLOAT);
        cStmt.setInt(3, 1);
        cStmt.setInt(4, 1);

        assertFalse(cStmt.execute());
        assertEquals(0f, cStmt.getInt(1), .001);
        assertTrue(cStmt.wasNull());
        assertNull(cStmt.getObject(1));
        assertTrue(cStmt.wasNull());

        assertEquals(-1, cStmt.executeUpdate());
        assertEquals(0f, cStmt.getInt(1), .001);
        assertTrue(cStmt.wasNull());
        assertNull(cStmt.getObject(1));
        assertTrue(cStmt.wasNull());

        // Check with literals, not all parameters filled!
        cStmt = conn.prepareCall("{? = CALL testBug10310(4,5,?)}");
        cStmt.registerOutParameter(1, Types.INTEGER);
        cStmt.setInt(2, 1);

        assertFalse(cStmt.execute());
        assertEquals(4f, cStmt.getInt(1), .001);
        assertEquals("java.lang.Integer", cStmt.getObject(1).getClass().getName());

        assertEquals(-1, cStmt.executeUpdate());
        assertEquals(4f, cStmt.getInt(1), .001);
        assertEquals("java.lang.Integer", cStmt.getObject(1).getClass().getName());

        assertEquals(2, cStmt.getParameterMetaData().getParameterCount());
        assertEquals(Types.INTEGER, cStmt.getParameterMetaData().getParameterType(1));
        assertEquals(Types.INTEGER, cStmt.getParameterMetaData().getParameterType(2));

    }

    @Test
    public void testUseAffectedRowError() throws Exception {
        Connection connection = setConnection("&useAffectedRows=true");
        PreparedStatement preparedStatement;
        Statement statement = connection.createStatement();
        int ids[] = { 13, 1, 8 };
        String vals[] = { "c", "a", "b" };
        statement.executeUpdate("drop table if exists testBug37458");
        statement
            .executeUpdate("create table testBug37458 (id int not null auto_increment, val varchar(100), primary key (id), unique (val))");
        statement.executeUpdate("insert into testBug37458 values (1, 'a'), (8, 'b'), (13, 'c')");
        preparedStatement = connection
            .prepareStatement(
                "insert into testBug37458 (val) values (?) on duplicate key update id = last_insert_id(id)",
                Statement.RETURN_GENERATED_KEYS);
        for (int i = 0; i < ids.length; ++i) {
            preparedStatement.setString(1, vals[i]);
            preparedStatement.addBatch();
        }
        preparedStatement.executeBatch();
        ResultSet keys = preparedStatement.getGeneratedKeys();
        for (int i = 0; i < ids.length; ++i) {
            assertTrue(keys.next());
            assertEquals(ids[i], keys.getInt(1));
        }
    }

    @Test
    public void testGetDate() {
        try {
            Date date = Date.valueOf("2020-12-12");
            PreparedStatement ps = sharedConnection.prepareStatement("select ?");
            ps.setString(1, "2020-12-12");
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            System.out.println(rs.getDate(1));
            ps = sharedConnection.prepareStatement("select ?");
            ps.setDate(1, date);
            rs = ps.executeQuery();
            assertTrue(rs.next());
            System.out.println(rs.getDate(1));
        } catch (SQLException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testGetDate2() {
        try {
            Date date = Date.valueOf("2020-12-12");
            PreparedStatement ps = sharedConnection.prepareStatement("select ?, ?");
            ps.setString(1, "2020-12-12");
            ps.setDate(2, date);
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals(date, rs.getDate(1));
            assertEquals(date, rs.getDate(2));
        } catch (SQLException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testGetTime() {
        try {
            Time time = Time.valueOf("11:22:33");
            PreparedStatement ps = sharedConnection.prepareStatement("select ?");
            ps.setString(1, "11:22:33");
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals(time, rs.getTime(1));
            ps = sharedConnection.prepareStatement("select ?");
            ps.setTime(1, time);
            rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals(time, rs.getTime(1));
        } catch (SQLException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void fixExecptionType() {
        try {
            ResultSet rs = sharedConnection.prepareStatement("SELECT '00/00/0000 00:00:00'")
                .executeQuery();
            assertTrue(rs.next());
            try {
                rs.getTime(1);
            } catch (Exception e) {
                Assert.assertTrue(e instanceof SQLException);
                Assert.assertTrue(e.getMessage().contains(
                    "Time format \"00/00/0000 00:00:00\" incorrect, must be HH:mm:ss"));
            }
            try {
                rs.getDate(1);
            } catch (Exception e) {
                Assert.assertTrue(e instanceof SQLException);
                Assert.assertTrue(e.getMessage()
                    .contains("Bad format for DATE 00/00/0000 00:00:00"));
            }
            try {
                rs.getTimestamp(1);
            } catch (Exception e) {
                Assert.assertTrue(e instanceof SQLException);
                Assert.assertTrue(e.getMessage().contains(
                    "Cannot convert value 00/00/0000 00:00:00 to TIMESTAMP."));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testPrepStmtExecuteLargeBatch() {
        Assume.assumeTrue(sharedUsePrepare());
        try {
            Connection conn = setConnection("&useAffectedRows=true&rewriteBatchedStatements=true");
            Statement stmt = conn.createStatement();
            stmt.execute("DROP table if exists testExecuteLargeBatch_1");
            stmt.execute("create table testExecuteLargeBatch_1(id BIGINT AUTO_INCREMENT PRIMARY KEY, n INT)");
            PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO testExecuteLargeBatch_1 (n) VALUES (?)",
                Statement.RETURN_GENERATED_KEYS);
            ps.setInt(1, 1);
            ps.addBatch();
            ps.setInt(1, 2);
            ps.addBatch();
            ps.setInt(1, 3);
            ps.addBatch();
            ps.setInt(1, 4);
            ps.addBatch();
            ps.setInt(1, 5);
            ps.addBatch();
            long[] counts = ps.executeLargeBatch();
            assertEquals(5, counts.length);
            System.out.println(Arrays.toString(counts));
            ResultSet rs = ps.getGeneratedKeys();
            long generatedKey = 0;
            while (rs.next()) {
                assertEquals(++generatedKey, rs.getLong(1));
                System.out.println(rs.getInt(1));
            }
            assertEquals(5, generatedKey);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testTextRewriteBatch() throws Exception {
        Connection conn = setConnection("&rewriteBatchedStatements=true");
        createTable("testRewriteBatch", "id int");
        Statement stmt = conn.createStatement();
        stmt.addBatch("INSERT INTO testRewriteBatch(id) VALUES (1)");
        stmt.addBatch("INSERT INTO testRewriteBatch(id) VALUES (2)");
        stmt.addBatch("INSERT INTO testRewriteBatch(id) VALUES (3)");
        stmt.addBatch("INSERT INTO testRewriteBatch(id) VALUES (4)");
        stmt.addBatch("UPDATE testRewriteBatch SET id=10 WHERE id=1 OR id=2");

        int[] counts = stmt.executeBatch();

        assertEquals(counts.length, 5);
        System.out.println(Arrays.toString(counts));
        assertArrayEquals(new int[] { 1, 1, 1, 1, 2 }, counts);
    }

    @Test
    public void testDateTimeToTimeStamp() {
        try {
            createTable("datetimetest", "c1 datetime");
            Connection connection = setConnection("&zeroDateTimeBehavior=convertToNull");
            Statement stmt = connection.createStatement();
            stmt.execute("insert into datetimetest values('0000-00-00 00:00:00')");
            ResultSet rs = stmt.executeQuery("select * from datetimetest");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(null, rs.getString(1));
            Assert.assertEquals(null, rs.getTimestamp(1));
            PreparedStatement ps = connection.prepareStatement("select * from datetimetest");
            rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            Assert.assertEquals(null, rs.getString(1));
            Assert.assertEquals(null, rs.getTimestamp(1));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }
}
