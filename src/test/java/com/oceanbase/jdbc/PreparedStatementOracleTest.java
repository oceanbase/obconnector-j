/*
 *
 * OceanBase Client for Java
 *
 * Copyright (c) 2022 OceanBase.
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

import java.sql.*;
import java.util.concurrent.CountDownLatch;

import com.oceanbase.jdbc.internal.util.Utils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class PreparedStatementOracleTest extends BaseOracleTest {

    @Test
    public void fix46694422() throws SQLException {
        Assert.assertTrue(sharedUsePrepare());
        try {
            sharedConnection.createStatement().execute("drop table t0");
        } catch (SQLException e) {
        }
        sharedConnection.createStatement().execute("create table t0(c0 int, c1 int)");
        for (int i = 0; i < 51; i++) {
            Statement s = sharedConnection.createStatement();
            ResultSet rs = s.executeQuery("select * from t0");
            while (rs.next()) {
            }
            System.out.println(i + ":close");
            s.close();
        }
    }

    @Test
    public void testSetFixedCHAR() throws SQLException {
        Assert.assertFalse(sharedUsePrepare());
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        ObPrepareStatement pstmt = null;

        stmt = sharedConnection.createStatement();
        try {
            stmt.execute("DROP TABLE TEST_SETFIXEDCHAR");
        } catch (SQLException ignored) {
        }
        stmt.execute("CREATE TABLE TEST_SETFIXEDCHAR(C1 CHAR(10))");
        stmt.execute("INSERT INTO TEST_SETFIXEDCHAR (C1) VALUES ('ob')");
        String sql = "SELECT C1 FROM TEST_SETFIXEDCHAR WHERE C1 = ?";

        //TextRowProtocol
        pstmt = (ObPrepareStatement) sharedConnection.prepareStatement(sql);
        pstmt.setFixedCHAR(1, "ob");
        rs = pstmt.executeQuery();
        Assert.assertTrue(rs.next());
        Assert.assertEquals("ob        ", rs.getString(1));

        pstmt.setString(1, "ob");
        rs = pstmt.executeQuery();
        Assert.assertTrue(rs.next()); // oracle: Assert.assertFalse(rs.next());
        Assert.assertEquals("ob        ", rs.getString(1));

        //BinaryRowProtocol
        conn = setConnection("&useServerPrepStmts=true");
        pstmt = (ObPrepareStatement) conn.prepareStatement(sql);
        pstmt.setFixedCHAR(1, "ob");
        rs = pstmt.executeQuery();
        Assert.assertTrue(rs.next());
        Assert.assertEquals("ob        ", rs.getString(1));

        pstmt.setString(1, "ob");
        rs = pstmt.executeQuery();
        Assert.assertTrue(rs.next()); // oracle: Assert.assertFalse(rs.next());
        Assert.assertEquals("ob        ", rs.getString(1));

        rs.close();
        pstmt.close();
        stmt.close();
    }

    @Test
    public void testGeneratedKeysByColumnIndexes() throws Exception {
        Connection conn = setConnection("&useOraclePrepareExecute=true");

        Statement stmt = conn.createStatement();
        // create sequence
        try {
            stmt.execute("DROP SEQUENCE S1;");
        } catch (Exception ignored) {

        } finally {
            stmt.execute("CREATE SEQUENCE S1 START WITH 1 INCREMENT BY 1");
        }
        // create table
        try {
            stmt.execute("DROP TABLE T1;");
        } catch (Exception ignored) {

        } finally {
            stmt.execute("CREATE TABLE T1 (C1 NUMBER(10) PRIMARY KEY, C2 VARCHAR(20));");
        }
        // create trigger
        stmt.execute("CREATE OR REPLACE TRIGGER TRI1\n" + "BEFORE INSERT ON T1\n"
                     + "FOR EACH ROW\n" + "BEGIN\n" + "  SELECT S1.NEXTVAL\n" + "  INTO :new.C1\n"
                     + "  FROM dual;\n" + "END;");

        PreparedStatement pstmt = conn.prepareStatement("insert into t1(c2) values ('test1')",
            new int[] { 1, 2 });
        pstmt.executeUpdate();
        ResultSet rs = pstmt.getGeneratedKeys();
        Assert.assertTrue(rs.next());
        Assert.assertEquals("\"TEST\".\"T1\".\"C1\"", rs.getMetaData().getColumnName(1));
        Assert.assertEquals(1, rs.getInt(1));
        Assert.assertEquals("\"TEST\".\"T1\".\"C2\"", rs.getMetaData().getColumnName(2));
        Assert.assertEquals("test1", rs.getString(2));
        Assert.assertFalse(rs.next());

        try {
            pstmt = conn.prepareStatement("insert into t1(c2) values ('test2')",
                new int[] { 1, 3 });
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("index 2 must be in [0, 1]"));
        }

        try {
            pstmt = conn.prepareStatement("insert into t1(c2) values ('test3')",
                new int[] { 1, 2, 3 });
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("index 2 must be in [0, 1]"));
        }
    }

    @Test
    public void testGeneratedKeysByColumnNames() throws Exception {
        Connection conn = setConnection("&useOraclePrepareExecute=true");

        Statement stmt = conn.createStatement();
        // create sequence
        try {
            stmt.execute("DROP SEQUENCE S1;");
        } catch (Exception ignored) {

        } finally {
            stmt.execute("CREATE SEQUENCE S1 START WITH 1 INCREMENT BY 1");
        }
        // create table
        try {
            stmt.execute("DROP TABLE T1;");
        } catch (Exception ignored) {

        } finally {
            stmt.execute("CREATE TABLE T1 (C1 NUMBER(10) PRIMARY KEY, C2 VARCHAR(20));");
        }
        // create trigger
        stmt.execute("CREATE OR REPLACE TRIGGER TRI1\n" + "BEFORE INSERT ON T1\n"
                     + "FOR EACH ROW\n" + "BEGIN\n" + "  SELECT S1.NEXTVAL\n" + "  INTO :new.C1\n"
                     + "  FROM dual;\n" + "END;");

        PreparedStatement pstmt = conn.prepareStatement("insert into t1(c2) values ('test1')",
            new String[] { "C1", "C2" });
        pstmt.executeUpdate();
        ResultSet rs = pstmt.getGeneratedKeys();
        Assert.assertTrue(rs.next());
        Assert.assertEquals("\"TEST\".\"T1\".\"C1\"", rs.getMetaData().getColumnName(1));
        Assert.assertEquals(1, rs.getInt(1));
        Assert.assertEquals("\"TEST\".\"T1\".\"C2\"", rs.getMetaData().getColumnName(2));
        Assert.assertEquals("test1", rs.getString(2));
        Assert.assertFalse(rs.next());

        try {
            pstmt = conn.prepareStatement("insert into t1(c2) values ('test2')",
                new String[] {"C1", "C3" });
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Invalid parameter \"C3\""));
        }

        try {
            pstmt = conn.prepareStatement("insert into t1(c2) values ('test3')",
                new String[] {"C1", "C2", "C3" });
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Invalid parameter \"C3\""));
        }
    }

    @Test
    public void fix51317355() throws Exception {
        Connection conn = setConnection("&useOraclePrepareExecute=true");
        createTable("t0", "id number primary key, c1 INT");
        PreparedStatement ps = conn.prepareStatement("INSERT INTO t0 VALUES (1,1)",
            Statement.RETURN_GENERATED_KEYS);
        ps.execute();

        // ob 2.4.4
        ResultSet rs = ps.getGeneratedKeys();
        assertFalse(rs.next());
        // oracle 返回rowSpaceByte: AAKKeQAAVAAAGRVAAA
        //        System.out.println(rs.getString(1));
    }

    @Test
    public void fix51565869() throws Exception {
        Connection conn = setConnectionOrigin("?&useOraclePrepareExecute=true");
        createTable("t5", "c1 number primary key, c2 varchar2(20)");

        PreparedStatement ps = conn
            .prepareStatement("insert into t5 values (1, 'aaa') returning c1,c2 into ?,?");
        ((JDBC4ServerPreparedStatement) ps).registerReturnParameter(1, Types.INTEGER);
        ((JDBC4ServerPreparedStatement) ps).registerReturnParameter(2, Types.VARCHAR);
        ps.execute();
        // ob 2.4.4
        try {
            ResultSet rs = ps.getGeneratedKeys();
            fail();
        } catch (SQLException e) {
            Assert
                .assertEquals(
                    "Cannot return generated keys : query was not set with Statement.RETURN_GENERATED_KEYS",
                    e.getMessage());
        }
        // oracle
        //        ResultSet rs = ps.getGeneratedKeys();
        //        try {
        //            ResultSetMetaData metaData = rs.getMetaData();
        //            fail();
        //        } catch (SQLException e) {
        //            Assert.assertEquals("不支持的特性: getMetaData", e.getMessage());
        //        }
        //        assertFalse(rs.next());

        ps = conn.prepareStatement("insert into t5 values (2, 'bbb') returning c1,c2 into ?,?",
            Statement.RETURN_GENERATED_KEYS);
        ((JDBC4ServerPreparedStatement) ps).registerReturnParameter(1, Types.INTEGER);
        ((JDBC4ServerPreparedStatement) ps).registerReturnParameter(2, Types.VARCHAR);
        // ob 2.4.4
        ps.execute();
        ResultSet rs = ps.getGeneratedKeys();
        ResultSetMetaData metaData = rs.getMetaData();
        assertEquals(1, metaData.getColumnCount());
        assertFalse(rs.next());
        // oracle
        //        try {
        //            ps.execute();
        //            fail();
        //        } catch (SQLException e) {
        //            Assert.assertEquals("ORA-00933: SQL 命令未正确结束\n", e.getMessage());
        //        }
    }

    @Test
    public void fix51585473() throws Exception {
        Connection conn = setConnectionOrigin("?useOraclePrepareExecute=true");
        createTable("test_returning_into", "id number, c1 varchar2(20)");
        PreparedStatement ps = conn.prepareStatement(
            "INSERT INTO test_returning_into VALUES (?,'aaa')", new String[] { "id", "c1" });
        ps.setInt(1, 1);
        ps.execute();
        ResultSet rs = null;
        rs = ps.getGeneratedKeys();
        assertTrue(rs.next());
        assertEquals("1", rs.getString(1));
        assertEquals("aaa", rs.getString(2));

        ps.clearParameters();
        ps.setInt(1, 2);
        ps.execute();

        createTable("test_1", "id number, c1 varchar2(200)");
        ps = conn.prepareStatement("INSERT INTO test_1 VALUES (1,'returning ... into ?,?')",
            new String[] { "id", "c1" });
        ps.execute();
        rs = ps.getGeneratedKeys();
        assertTrue(rs.next());
        assertEquals("1", rs.getString(1));
        assertEquals("returning ... into ?,?", rs.getString(2));

        ps = conn.prepareStatement("INSERT INTO test_1 VALUES (2,' returning ... into ?,?')",
                new String[] { "id", "c1" });
        ps.execute();
        rs = ps.getGeneratedKeys();
        assertTrue(rs.next());
        assertEquals("2", rs.getString(1));
        assertEquals(" returning ... into ?,?", rs.getString(2));
    }

    @Test
    public void testReturnOfExecuteUpdateOnSelect() throws SQLException {
        Connection conn = setConnection("&useOraclePrepareExecute=true");

        String tableName = "yxy_20211108";
        createTable(tableName, "c1 int, c2 varchar(30), constraint pk primary key(c1)");
        String query1 = "insert all";
        for (int i = 1; i <= 21; i++) {
            query1 += " into " + tableName + " values(" + i + ",'" + i + "+string')";
        }
        query1 += "select * from dual";
        Statement s = conn.createStatement();
        s.execute(query1);

        // use PreparedStatement
        PreparedStatement ps = conn.prepareStatement("select * from " + tableName);
        // default fetch size 10 < total rows 21
        int i = ps.executeUpdate();
        Assert.assertEquals(10, i);
        // fetch size 8 < total rows 21
        ps.setFetchSize(8);
        i = ps.executeUpdate();
        Assert.assertEquals(8, i);
        // fetch size 30 > total rows 21
        ps.setFetchSize(30);
        i = ps.executeUpdate();
        Assert.assertEquals(21, i);

        // use Statement
        Statement stmt = conn.createStatement();
        // default fetch size 10 < total rows 21
        i = stmt.executeUpdate("select * from " + tableName);
        try {
            Assert.assertEquals(10, i);
        } catch (AssertionError ae) {
            Assert.assertEquals(21, i);
        }
        // fetch size 8 < total rows 21
        stmt.setFetchSize(8);
        i = stmt.executeUpdate("select * from " + tableName);
        try {
            Assert.assertEquals(8, i);
        } catch (AssertionError ae) {
            Assert.assertEquals(21, i);
        }
        // fetch size 30 > total rows 21
        stmt.setFetchSize(30);
        i = stmt.executeUpdate("select * from " + tableName);
        Assert.assertEquals(21, i);
    }

    @Test
    public void aone53311351() throws SQLException, InterruptedException {
        Assume.assumeFalse(sharedUsePrepare());
        createTable("test", "n1 INT, d1 DATE DEFAULT sysdate");
        Statement stmt = sharedConnection.createStatement();
        stmt.executeUpdate("INSERT INTO test(n1) VALUES(1)");
        stmt.executeUpdate("INSERT INTO test(n1) VALUES(2)");
        stmt.executeUpdate("INSERT INTO test(n1) VALUES(3)");

        PreparedStatement s = sharedConnection.prepareStatement("UPDATE test set n1 = n1 -? WHERE n1 = 1");
        s.setDouble(1,-1);
        int retRows = s.executeUpdate();
        System.out.println(retRows);
        Assert.assertEquals(1, retRows);

//        ResultSet rs = stmt.executeQuery("select * from test");
//        while (rs.next()) {
//            System.out.println(rs.getString(1) + " " + rs.getString(2));
//        }
    }

    @Test
    public void  testCloseStatment() throws Exception {
        Connection conn = setConnection("&useServerPrepStmts=true");
        createTable("t_close", "id int, st varchar(20)");
        CountDownLatch countDownLatch = new CountDownLatch(20 * 1000);
        for (int i = 0; i < 20; i++) {
            new Thread(() -> {
                for (int j = 0; j < 1000; j++) {
                    try {
                        //每次创建新的ps对象
                        PreparedStatement preparedStatement = conn.prepareStatement("INSERT INTO test_nh4 values (" + j + ",'wffffdfd');");
                        preparedStatement.execute();
                        //运行完成后进行关闭ps对象
                        preparedStatement.close();
                        System.out.println(Thread.currentThread().getName() +" 线程运行了"+ j +"次");
                        countDownLatch.countDown();
                    } catch (Exception e) {
                        System.err.println(Thread.currentThread().getName() + "报错。。。");
                        e.printStackTrace();
                    }
                }
            }).start();
        }
        countDownLatch.await();
    }

    @Test
    public void testParamentCountOfSqlForAone54180535() {
        try {
            Connection conn = setConnection("&useServerPrepStmts=true&useOraclePrepareExecute=true");
            String sql = "SELECT /*/*111 */*/* */ from (select ? from dual)";
            PreparedStatement stmt = conn.prepareStatement(sql);
            stmt.setLong(1, 2);
            ResultSet rs = stmt.executeQuery();
            rs.next();
            assertEquals("2", rs.getString(1));
        } catch (SQLException e) {
            e.printStackTrace();
        }

        String sql = "/*t1,t2,t3*/ -- t1\n-- t2\n-- t3\n/*t1, t2, t3*/ select 1 from dual";
        Assert.assertEquals("  select 1 from dual", Utils.trimSQLString(sql, false, false));
    }
    
    @Test
    public void testcachePrepStmts() throws Exception {
        Connection conn = setConnection("&useServerPrepStmts=true&useOraclePrepareExecute=true&cachePrepStmts=true");
        createTable("test_nh4", "id int, st varchar(20)");
        CountDownLatch countDownLatch = new CountDownLatch(20 * 1000);
        for (int i = 0; i < 20; i++) {
            new Thread(() -> {
                for (int j = 0; j < 1000; j++) {
                    try {
                        //每次创建新的ps对象
                        PreparedStatement preparedStatement = conn.prepareStatement("insert into test_nh4 values(?,?)");
                        preparedStatement.setInt(1, j);
                        preparedStatement.setString(2, "wjjj");
                        preparedStatement.execute();
                        //运行完成后进行关闭ps对象
                        preparedStatement.close();
                        System.out.println(Thread.currentThread().getName() +" 线程运行了"+ j +"次");
                        countDownLatch.countDown();
                    } catch (Exception e) {
                        System.err.println(Thread.currentThread().getName() + "报错。。。");
                        e.printStackTrace();
                    }
                }
            }).start();
        }
        countDownLatch.await();
    }

    @Test
    public void testForAone56598230() throws Exception {
        createTable("testAone", "a varchar(10), b varchar(10), c varchar(10)");
        Statement stmt = sharedConnection.createStatement();
        stmt.execute("insert into testAone values('a', 'b', 'c')");

        // testForAone56598230 for parameterParts
        Connection conn = setConnection("&useServerPrepStmts=false&rewriteBatchedStatements=false");;
        String sql = "/*comment*/select /*+index(a, b)*/* from testAone where a = ? and b = ? and c = ? ";
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setString(1, "a");
        ps.setString(2, "b");
        ps.setString(3, "c");
        ResultSet rs = ps.executeQuery();
        Assert.assertTrue(rs.next());

        // testForAone56598230 for rewritableParts
        conn = setConnection("&useServerPrepStmts=false&rewriteBatchedStatements=true");
        ps = conn.prepareStatement(sql);
        ps.setString(1, "a");
        ps.setString(2, "b");
        ps.setString(3, "c");
        rs = ps.executeQuery();
        Assert.assertTrue(rs.next());

        // testForAone56598230 for rewritablePartsInsertSql
        conn = setConnection("&useServerPrepStmts=true&useOraclePrepareExecute=true&rewriteBatchedStatements=true");
        ps = conn.prepareStatement(sql);
        ps.setString(1, "a");
        ps.setString(2, "b");
        ps.setString(3, "c");
        ps.addBatch();
        ps.executeBatch();

        // testForAone56598230 for canAggregateSemiColon
        conn = setConnection("&useServerPrepStmts=false&useOraclePrepareExecute=false&rewriteBatchedStatements=true");
        Statement statement = conn.createStatement();
        statement.addBatch("select /*+index(kdpl_zhminx, KDPL_ZHMINX_IDX1)*/* from testAone where a = 'a';");
        statement.addBatch("select /*+index(kdpl_zhminx, KDPL_ZHMINX_IDX1)*/* from testAone where b = 'b';");
        statement.executeBatch();
    }
}
