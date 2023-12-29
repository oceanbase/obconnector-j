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

import org.junit.Test;

import java.sql.*;
import java.sql.Clob;

import static org.junit.Assert.fail;

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

}
