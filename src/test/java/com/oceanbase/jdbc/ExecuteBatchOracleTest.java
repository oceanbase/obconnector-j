/*
 *
 * OceanBase Client for Java
 *
 * Copyright (c) 2023 OceanBase.
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

import org.junit.Assert;
import org.junit.Test;

import java.sql.*;
import java.sql.Clob;
import java.util.Arrays;

import static org.junit.Assert.*;

public class ExecuteBatchOracleTest extends BaseOracleTest {

    @Test
    public void reuseStatementId() {
        int count = 0;
        try {
            Connection connection = setConnection("&maxBatchTotalParamsNum=300&cachePrepStmts=true&callableStmtCacheSize=1024&useOraclePrepareExecute=true&useServerPrepStmts=true&useCursorFetch=TRUE&defaultFetchSize=25000&rewriteBatchedStatements=TRUE&allowMultiQueries=TRUE&useLocalSessionState=TRUE&useUnicode=TRUE&characterEncoding=utf-8&socketTimeout=3000000&connectTimeout=60000");
            createTable("test_yw", "name varchar(20)");
            PreparedStatement ps = connection.prepareStatement("INSERT into test_yw(name) VALUES (?)");
            for (int i=1; i<=1000; i++) {
                count ++;
                ps.setString(1,"流水" + i);
                ps.addBatch();
                ps.executeBatch();
            }

            for (int i=1; i<=1000; i++) {
                count ++;
                ps.setString(1,"流水" + i);
                ps.addBatch();
            }
            ps.executeBatch();
            //300 300 300 100

            for (int i=1; i<=91; i++) {
                count ++;
                ps.setString(1,"流水" + i);
                ps.addBatch();
            }
            ps.executeBatch();

            ps.close();
            connection.close();
        } catch (Exception ex) {
            System.out.println("for循环个数："+count);
            ex.printStackTrace();
        }
    }

    @Test
    public void aone53890549() throws SQLException {
        createTable("t0", "c0 int,c1 int,c2 varchar2(10),c3 varchar2(10)");

        for (int k = 1; k <= 2; k++) {
            Connection conn;
            if (k == 1) {
                conn = setConnection("&maxBatchTotalParamsNum=10000&rewriteBatchedStatements=true&useServerPrepStmts=true&useOraclePrepareExecute=false");
            } else {
                conn = setConnection("&maxBatchTotalParamsNum=10000&rewriteBatchedStatements=true&useServerPrepStmts=true&useOraclePrepareExecute=true");
            }
            // conn.setAutoCommit(false);

            PreparedStatement ps = conn.prepareStatement("INSERT INTO t0 values (?,?,?,?)");
            for(int j=1;j<=10;j++){
                //System.out.println(j);
                for(int i=1;i<10;i++) {
                    ps.setInt(1, i);
                    ps.setInt(2, i);
                    ps.setString(3, "aa");
                    ps.setString(4, "aa");
                    ps.addBatch();
                }
                ps.executeBatch();
                ps.clearBatch();
            }
        }

    }

    @Test
    public void testClearParamAone53840812() throws SQLException {
        Connection conn = setConnection("&useServerPrepStmts=true&useOraclePrepareExecute=true&rewriteBatchedStatements=true");
        createTable("test_clear_param", "v0 int");
        PreparedStatement pstmt = conn.prepareStatement("insert into test_clear_param values (?)");
        pstmt.setInt(1, 1);
        pstmt.addBatch();
        pstmt.setInt(1, 3);
        pstmt.addBatch();
        pstmt.addBatch();
        pstmt.executeBatch();//3 params

        pstmt.addBatch();
        pstmt.executeBatch();//1 param
    }


    @Test
    public void aone53826258() {
        try {
            Connection conn = setConnection("&rewriteBatchedStatements=true&useServerPrepStmts=true");//&cachePrepStmts=true&callableStmtCacheSize=1024
            Statement stmt=conn.createStatement();
            try {
                stmt.execute("drop table t0");
            }catch (SQLException e) {
            }
            stmt.execute("create table t0(c0 int)");
            stmt.execute("alter system set open_cursors=10");

            PreparedStatement ps;
            for (int j=1;j<20;j++) {
                ps = conn.prepareStatement("insert into t0 values(?)");
                for (int i = 0; i <= j; i++) {
                    ps.setInt(1, i);
                    ps.addBatch();
                }
                ps.executeBatch();
                ps.clearBatch();
                ps.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void aone54176188() {
        int count = 0;
        try {
            Connection connection = setConnection("&maxBatchTotalParamsNum=3&cachePrepStmts=true&rewriteBatchedStatements=TRUE&useServerPrepStmts=true&usePieceData=true");//
            createTable("test_yw", "name clob");
            PreparedStatement ps = connection.prepareStatement("INSERT into test_yw(name) VALUES (?)");
            Clob clob = connection.createClob();
            clob.setString(1, "abc");

            for (int i=1; i<=10; i++) {
                count ++;
                ps.setClob(1,clob);
                ps.addBatch();
            }
            ps.executeBatch();
            //300 300 300 100

            for (int i=1; i<=4; i++) {
                count ++;
                ps.setClob(1,clob);
                ps.addBatch();
            }
            ps.executeBatch();

            ps.close();
            connection.close();
        } catch (Exception ex) {
            System.out.println("for循环个数："+count);
            ex.printStackTrace();
        }
    }

    @Test
    public void testEncloseParamInParentheses() throws SQLException {
        Connection conn = setConnection("&encloseParamInParentheses=true&rewriteBatchedStatements=true");
        Statement stmt = conn.createStatement();
        try {
            stmt.execute("drop table t1");
        } catch (SQLException e) {
        }
        stmt.execute("create table t1 (c1 int, c2 varchar(20))");
        stmt.execute("insert into t1 values (10, 'aaa')");
        stmt.execute("insert into t1 values (20, 'bbb')");
        stmt.execute("insert into t1 values (30, 'ccc')");


        PreparedStatement ps = conn.prepareStatement("update t1 set c1 = c1-?");
        ps.setInt(1, -1);
        ps.addBatch();
        ps.setInt(1, -10);
        ps.addBatch();
        ps.executeBatch();
        assertEquals(-1, ps.getUpdateCount());

        ResultSet rs = stmt.executeQuery("select * from t1");
        Assert.assertTrue(rs.next());
        assertEquals(21, rs.getInt(1));
        Assert.assertTrue(rs.next());
        assertEquals(31, rs.getInt(1));
        Assert.assertTrue(rs.next());
        assertEquals(41, rs.getInt(1));
    }

    @Test
    public void testEncloseParamInParenthesesForAone101574594() throws SQLException {
        Connection conn = setConnection("&useServerPrepStmts=false");
        try {
            conn.createStatement().execute("drop table t2");
        } catch (SQLException ignored) {

        }
        PreparedStatement ps = conn.prepareStatement("CREATE TABLE t2(c1 INT ,c2 INT) PARTITION BY HASH(c1) PARTITIONS ?");
        ps.setInt(1, 2);
        try {
            ps.execute();
        } catch (SQLException e) {
            fail();
        }
    }

    @Test
    public void testExecuteBatchReForAone57343322() throws SQLException {
        Connection conn = sharedConnection;
        Statement stmt = conn.createStatement();
        createTable("ob_tmp_t_uw_sy_amount", "product_id number(10), decision_id number(10), amount number(12,2)," +
                "insert_time date, last_updated_observe timestamp(6), last_updated timestamp(6)");
        stmt.execute("insert into ob_tmp_t_uw_sy_amount (product_id, decision_id, amount, insert_time, last_updated, " +
                "last_updated_observe) values (64806, 2, 40000.00, date '2016-08-30', timestamp '2024-05-31 11:38:57.248341', null)");

        String sql = "merge into OB_TMP_T_UW_SY_AMOUNT a using (\n" +
                "  select\n" +
                "    *\n" +
                "  from\n" +
                "    OB_TMP_T_UW_SY_AMOUNT\n" +
                "  where\n" +
                "    PRODUCT_ID = 64806\n" +
                "    and DECISION_ID = 2\n" +
                ") b on (\n" +
                "  a.DECISION_ID = b.DECISION_ID\n" +
                "  and a.PRODUCT_ID = b.PRODUCT_ID\n" +
                ")\n" +
                "when matched then\n" +
                "update\n" +
                "set\n" +
                "  a.AMOUNT = b.AMOUNT,\n" +
                "  a.INSERT_TIME = b.INSERT_TIME,\n" +
                "  a.LAST_UPDATED_OBSERVE = b.LAST_UPDATED_OBSERVE,\n" +
                "  a.LAST_UPDATED = b.LAST_UPDATED\n" +
                "where\n" +
                "  a.LAST_UPDATED < b.LAST_UPDATED\n" +
                "  when not matched then\n" +
                "insert\n" +
                "  (\n" +
                "    a.AMOUNT,\n" +
                "    a.INSERT_TIME,\n" +
                "    a.LAST_UPDATED_OBSERVE,\n" +
                "    a.LAST_UPDATED,\n" +
                "    a.DECISION_ID,\n" +
                "    a.PRODUCT_ID\n" +
                "  )\n" +
                "values\n" +
                "  (\n" +
                "    b.AMOUNT,\n" +
                "    b.INSERT_TIME,\n" +
                "    b.LAST_UPDATED_OBSERVE,\n" +
                "    b.LAST_UPDATED,\n" +
                "    b.DECISION_ID,\n" +
                "    b.PRODUCT_ID\n" +
                "  )";

        PreparedStatement preparedStatement = conn.prepareStatement(sql);
        preparedStatement.addBatch();
        int[] updateCounts = preparedStatement.executeBatch();

        assertEquals(0, updateCounts[0]);
    }

    @Test
    public void testBatchForDima2024081600104156188() throws SQLException {
        Connection conn = setConnection("&rewriteBatchedStatements=true&maxBatchTotalParamsNum=5&allowMultiQueries=false&useServerPrepStmts=true");
        Statement stmt = conn.createStatement();
        createTable("t_batch", "id NUMBER(10), name VARCHAR2(50),age NUMBER(3)");

        conn.setAutoCommit(false);
        String sql = "UPDATE t_batch SET age = age+? WHERE id = ?";
        PreparedStatement pstmt = conn.prepareStatement(sql);
        for (int i = 0; i < 10; i++) {
            pstmt.setInt(1, 1 + i);
            pstmt.setInt(2, 1 + i);
            pstmt.addBatch();
        }
        int[] result = pstmt.executeBatch();
        Assert.assertEquals("[0, 0, 0, 0, 0, 0, 0, 0, 0, 0]", Arrays.toString(result));
        conn.commit();
        pstmt.close();

        ResultSet rs = stmt.executeQuery("select count(1) from t_batch");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(0, rs.getInt(1));
    }
 
    @Test
    public void testReturninIntoForDima2024082600104255128() throws SQLException {
        Connection conn = setConnection("&useServerPrepStmts=true&useOraclePrepareExecute=true&useArrayBinding=true&compatibleOjdbcVersion=8");
        Statement stmt = conn.createStatement();
        try {
            stmt.execute("drop sequence s1;");
        } catch (Exception ignored) {
            //
        } finally {
            stmt.execute("create sequence s1 start with 1 increment by 1");
        }
        createTable("t_batch", "id  number(10) , c1 varchar(15), c2 int, c3 varchar(15) ");
        stmt.execute("create or replace trigger tri1\n" + "before insert on t_batch\n"
                + "for each row\n" + "begin\n" + "  select  s1.nextval\n" + "  into :new.id\n"
                + "  from dual;\n" + "end;");

        String sql = "insert into t_batch(c1, c2, c3) values(?, ?, ?) ";

        conn.setAutoCommit(false);
        PreparedStatement ps = conn.prepareStatement(sql, new int[]{1, 2, 3, 4});

        for (int i = 1; i <= 3; i++) {
            ps.setString(1, "aaa" + i);
            ps.setInt(2, i);
            ps.setString(3, "bbb" + i);
            ps.addBatch();
        }
        ps.executeBatch();

        conn.setAutoCommit(true);
        ResultSet returnRs = ps.getGeneratedKeys();
        ResultSetMetaData returnRsMetaData = returnRs.getMetaData();

        Assert.assertEquals(4, returnRsMetaData.getColumnCount());
        for (int i = 1; i <= 3; i++) {
            Assert.assertTrue(returnRs.next());
            Assert.assertEquals(i, returnRs.getInt(1));
            Assert.assertEquals("aaa" + i, returnRs.getString(2));
            Assert.assertEquals(i, returnRs.getInt(3));
            Assert.assertEquals("bbb" + i, returnRs.getString(4));
            try {
                returnRs.getString(5);
                fail();
            } catch (SQLException e) {
                Assert.assertTrue(e.getMessage().contains("No such column"));
            }
        }
        returnRs.close();
        ps.close();
        stmt.close(); 
    }

    @Test
    public void testReturningBatchForDima2024101400104695934() throws SQLException {
        createTable("t_rb", "c1 int");
        Connection conn = setConnection("&useOraclePrepareExecute=true&useArrayBinding=true&compatibleOjdbcVersion=8");
        conn.setAutoCommit(false);
        JDBC4ServerPreparedStatement pstmt = (JDBC4ServerPreparedStatement) conn.prepareStatement("insert into t_rb values(?) returning c1 into ?");
        try {
            for (int i = 1; i <= 35; i++) {
                pstmt.setInt(1, i);
                pstmt.registerReturnParameter(2, Types.INTEGER);
                if (i == 19) {
                    pstmt.registerReturnParameter(2, Types.VARCHAR);
                }
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            ResultSet rs = pstmt.getReturnResultSet();
            for (int i = 1; i <= 35; i++) {
                rs.next();
                Assert.assertEquals(i, rs.getInt(1));
            }
        } catch (SQLException e) {
            fail();
        } finally {
            conn.setAutoCommit(true);
        }
    }

}
