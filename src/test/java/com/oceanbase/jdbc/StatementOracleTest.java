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

import java.sql.*;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.fail;

public class StatementOracleTest extends BaseOracleTest {

    @Test
    public void testMaxRowsAndLimit() throws SQLException {
        createTable("testMaxRows", "c1 INT, c2 VARCHAR(100)");
        String query = "insert all";
        for (int i = 1; i <= 20; i++) {
            query += " into testMaxRows values(" + i + ",'" + i + "+string')";
        }
        query += "select * from dual";
        Statement st = sharedConnection.createStatement();
        st.execute(query);
        st.close();

        Statement stmt = sharedConnection.createStatement();
        stmt.setMaxRows(5);
        stmt.setFetchSize(3);
        ResultSet rs = stmt.executeQuery("select * from testMaxRows");
        for (int i = 1; i <= 5; i++) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), i);
        }
        try {
            Assert.assertTrue(rs.isLast());
            fail();
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Invalid operation on TYPE_FORWARD_ONLY ResultSet");
        }
        Assert.assertFalse(rs.next());
        Assert.assertTrue(rs.isAfterLast());

        Connection conn = setConnectionOrigin("?useServerPrepStmts=true");
        PreparedStatement pstmt1 = conn.prepareStatement("select * from testMaxRows");
        pstmt1.setMaxRows(5);
        pstmt1.setFetchSize(3);
        ResultSet rs1 = pstmt1.executeQuery();
        for (int i = 1; i <= 5; i++) {
            Assert.assertTrue(rs1.next());
            Assert.assertEquals(rs1.getInt(1), i);
        }
        try {
            Assert.assertTrue(rs1.isLast());
            fail();
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Invalid operation on TYPE_FORWARD_ONLY ResultSet");
        }
        Assert.assertFalse(rs1.next());
        Assert.assertTrue(rs1.isAfterLast());

        conn = setConnectionOrigin("?useCursorFetch=true");
        PreparedStatement pstmt2 = conn.prepareStatement("select * from testMaxRows");
        pstmt2.setMaxRows(5);
        pstmt2.setFetchSize(3);
        ResultSet rs2 = pstmt2.executeQuery();
        for (int i = 1; i <= 5; i++) {
            Assert.assertTrue(rs2.next());
            Assert.assertEquals(rs2.getInt(1), i);
        }
        try {
            Assert.assertTrue(rs2.isLast());
            fail();
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Invalid operation on TYPE_FORWARD_ONLY ResultSet");
        }
        Assert.assertFalse(rs2.next());
        Assert.assertTrue(rs2.isAfterLast());

        conn = setConnectionOrigin("?useCursorFetch=true");
        PreparedStatement pstmt3 = conn.prepareStatement("select * from testMaxRows",
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        pstmt3.setMaxRows(5);
        pstmt3.setFetchSize(3);
        ResultSet rs3 = pstmt3.executeQuery();
        Assert.assertTrue(rs3.absolute(5));
        Assert.assertTrue(rs3.isLast());
        Assert.assertFalse(rs3.next());
        Assert.assertTrue(rs3.isAfterLast());
    }

    @Test
    public void testMaxRowsForAone55087008AndAone57643521() throws SQLException {
        createTable("t_max", "c1 int");
        PreparedStatement ps = sharedConnection.prepareStatement("insert into t_max values(?)");
        for (int i = 0; i < 20; i++) {
            ps.setInt(1, i);
            ps.addBatch();
        }
        ps.executeBatch();

        Connection conn = setConnection("&useServerPrepStmts=true&useOraclePrepareExecute=false&maxRows=14");
        PreparedStatement ps2 = conn.prepareStatement("select * from t_max where rownum <= 10", ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
        ResultSet rs2 = ps2.executeQuery();
        rs2.last();
        Assert.assertEquals(10, rs2.getRow());


        PreparedStatement ps3 = conn.prepareStatement("select c1 from t_max", ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
        ps3.setMaxRows(4);
        ResultSet rs3 = ps3.executeQuery();
        rs3.last();
        Assert.assertEquals(4, rs3.getRow());

        PreparedStatement ps4 = conn.prepareStatement("select c1 from t_max", ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
        ResultSet rs4 = ps4.executeQuery();
        rs4.last();
        Assert.assertEquals(14, rs4.getRow());

        Connection conn2 = setConnection("&useServerPrepStmts=true&useOraclePrepareExecute=false");
        PreparedStatement ps2_1 = conn2.prepareStatement("select c1 from t_max", ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
        ps2_1.setMaxRows(4);
        ResultSet rs2_1 = ps2_1.executeQuery();
        rs2_1.last();
        Assert.assertEquals(4, rs2_1.getRow());

        PreparedStatement ps2_2 = conn2.prepareStatement("select c1 from t_max", ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
        ResultSet rs2_2 = ps2_2.executeQuery();
        rs2_2.last();
        Assert.assertEquals(20, rs2_2.getRow());
    }

    @Test
    public void testBatchUpdateForAone57969289() throws SQLException {
        createTable("t_ar", "id int , c1 int, c2 varchar(20)");
        Connection conn = setConnection("&useArrayBinding=true&useServerPrepStmts=true&useOraclePrepareExecute=true");
        PreparedStatement ps = conn.prepareStatement("insert into t_ar values(?, ?, ?)");
        for (int i = 1; i <= 4; i++) {
            for (int j = 0; j < i; j++) {
                ps.setInt(1, i);
                ps.setInt(2, i);
                ps.setString(3, i + "_char");
                ps.addBatch();
            }
        }
        ps.executeBatch();

        conn.setAutoCommit(false);
        PreparedStatement ps1 = conn
                .prepareStatement("update t_ar set c2 = 'up' where id = ? and c1 = ?");

        ps1.setInt(1, 0);
        ps1.setString(2, "0");
        ps1.addBatch();

        for (int i = 1; i <= 3; i++) {
            ps1.setString(1, i + "");
            ps1.setInt(2, i);
            ps1.addBatch();
        }

        ps1.setInt(1, 8);
        ps1.setString(2, "8");
        ps1.addBatch();

        int[] ints = ps1.executeBatch();
        Assert.assertEquals("[-2, -2, -2, -2, -2]", Arrays.toString(ints));
        conn.setAutoCommit(true);
        conn.close();
    }

    @Test
    public void testForOrderBy() throws SQLException {
        createTable("t_test", "c1 int , c2 varchar2(100), c3 varchar2(100), one int");
        Connection conn = setConnection("&useOraclePrepareExecute=false&useCursorFetch=true&defaultFetchSize=25000");
        Statement stmt = conn.createStatement();
        stmt.execute("insert into t_test values(1, 'a', 'b', 1)");
        stmt.execute("insert into t_test values(2, 'a', 'b', 2)");
        stmt.execute("insert into t_test values(3, 'a', 'b', 3)");

        String sql = "select c1, c2 , c3 from t_test /*123\n*/order/*\n123*/by/*123*/ one";
        Statement stmt1 = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
            ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = stmt1.executeQuery(sql);

        if (rs.last()) {
            Assert.assertEquals("3", rs.getString(1));
        }
        stmt1.close();
        stmt.close();
    }

    @Test
    public void testForSqlAppend() throws SQLException {
        createTable("t_test", "c1 int , c2 varchar2(100), c3 varchar2(100)");
        Connection conn = setConnection("&useServerPrepStmts=true");
        Statement stmt = conn.createStatement();
        stmt.execute("insert into t_test values(1, 'a', 'b')");
        stmt.execute("insert into t_test values(2, 'a', 'b')");
        stmt.execute("insert into t_test values(3, 'a', 'b')");
        stmt.execute("insert into t_test values(4, 'a', 'b')");
        stmt.execute("insert into t_test values(5, 'a', 'b')");

        // having for dima2024111500105136558
        String sql = "select /*1234*/ c1, c2 from t_test having 1=1";
        PreparedStatement ps = null;
        ResultSet rs = null;

        ps = conn
            .prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
        rs = ps.executeQuery();
        rs.last();
        Assert.assertEquals(5, rs.getRow());
        rs.close();

        // limit for dima2024112500105273269
        sql = "select c1 from t_test where c2 = 'a' /*1234*/\nOFFSET 2 ROWS FETCH FIRST 3 ROWS ONLY";
        ps = conn
            .prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
        rs = ps.executeQuery();
        Assert.assertTrue(rs.next());
        Assert.assertEquals(3, rs.getInt(1));
        rs.close();

        // for update for dima2024112500105272024
        sql = "select c1  from t_test where c2 = 'a' /*1234*/\nfor/*1234*/update";
        ps = conn
            .prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
        rs = ps.executeQuery();
        rs.last();
        Assert.assertEquals(5, rs.getRow());

        // subQuery
        sql = "select c1, c2 from (select c1, c2 from t_test  where c2 = 'a' order by c2 OFFSET 2 ROWS FETCH FIRST 3 ROWS ONLY) having 1 = 1 order by c1";
        ps = conn
            .prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
        rs = ps.executeQuery();
        Assert.assertTrue(rs.next());
        Assert.assertEquals(3, rs.getInt(1));
        rs.last();
        Assert.assertEquals(3, rs.getRow());

        sql = "select c1, c2 from (select c1, c2 from t_test  where c2 = 'a' order by c2 OFFSET 2 ROWS FETCH FIRST 3 ROWS ONLY) where c1 = '4' having 1 = 1 order by c1";
        ps = conn
            .prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
        rs = ps.executeQuery();
        Assert.assertTrue(rs.next());
        Assert.assertEquals(4, rs.getInt(1));
        rs.last();
        Assert.assertEquals(1, rs.getRow());

        sql = "select c1, c2 from t_test where c1 in (select c1 from t_test  where c2 = 'a' order by c1 desc OFFSET 2 ROWS FETCH FIRST 3 ROWS ONLY)  having 1 = 1 order by c1";
        ps = conn
            .prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
        rs = ps.executeQuery();
        rs.last();
        Assert.assertEquals(3, rs.getInt(1));
        Assert.assertEquals(3, rs.getRow());
    }

    @Test
    public void testForDima2025022600107339506() throws SQLException {
        Connection conn = setConnection("&useServerPrepStmts=true");
        Statement stmt = conn.createStatement();
        createFunction("p_sleep", "(seconds int) return number as begin dbms_lock.sleep(seconds); return 1; end; ");

        try {
            stmt.setQueryTimeout(1);
            ResultSet rs = stmt.executeQuery("select p_sleep(1) from dual connect by level <= 10");
            int count = 0;
            while (rs.next())
            {
                count ++;
            }
            Assert.assertEquals(10, count);
            fail("queryTimeout not valid");
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("Query timed out"));
        }
    }

    @Test
    public void testForDima2025040700108069099() throws SQLException {
        Connection conn = setConnection("&useServerPrepStmts=true&cachePrepStmts=true");
        createTable("t_cache", "c1 varchar2(20)");

        PreparedStatement ps = conn.prepareStatement("insert into t_cache values(?)");
        for (int i = 0; i < 13; i++) {
            ps.setString(1, "aaa");
            ps.addBatch();
        }

        for (int i = 0; i < 5; i++) {
            ps.setString(1, "bbb");
            ps.addBatch();
        }
        ps.executeBatch();

        String sql = "select * from t_cache where c1 = ?";
        // add ps to cache
        PreparedStatement psTemp = conn.prepareStatement(sql);
        psTemp.close();

        PreparedStatement pstmt1 = conn.prepareStatement(sql);
        PreparedStatement pstmt2 = conn.prepareStatement(sql);
        pstmt1.setString(1, "aaa");
        pstmt2.setString(1, "bbb");

        int count = 0;
        ResultSet rs = pstmt1.executeQuery();
        // fetch 10 records
        while (rs.next()) {
            Assert.assertEquals("aaa", rs.getString(1));
            count ++;
            if (count == 10) {
                break;
            }
        }

        // start new query
        ResultSet rs2 = pstmt2.executeQuery();
        int count2 = 0;
        while (rs2.next()) {
            Assert.assertEquals("bbb", rs2.getString(1));
            count2 ++;
        }
        Assert.assertEquals(5, count2);

        // Get the remaining records of rs
        try {
            while (rs.next()) {
                Assert.assertEquals("aaa", rs.getString(1));
                count ++;
            }
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertEquals(13, count);

        pstmt1.close();
        pstmt2.close();
    }

    @Test
    public void testForDima2025071500109953119() throws SQLException, InterruptedException {
        Connection conn = setConnection("&autoReconnect=false&useServerPrepStmts=true&useOceanBaseProtocolV20=false&cachePrepStmts=false");
        Statement stmt = conn.createStatement();

        String c1_sql1 = "select 1 from dual";
        PreparedStatement c1_ps1 = conn.prepareStatement(c1_sql1);
        c1_ps1.executeQuery();

        stmt.execute("set wait_timeout = 1");
        Thread.sleep(2000);
        try {
            stmt.execute("set wait_timeout = 2");
            fail("should throw exception");
        } catch (SQLException e) {
            Assert.assertEquals("08S01", e.getSQLState());
            Assert.assertEquals(0, e.getErrorCode());
            Assert.assertEquals("java.sql.SQLNonTransientConnectionException", e.getClass().getName());
        }

        try {
            conn = setConnection("&autoReconnect=true");
            fail();
        } catch (SQLException e) {
            Assert.assertEquals("error parsing url : The autoReconnect parameter is no longer supported since version 2.4.16. Please remove it from the URL.", e.getMessage());
        }
    }

    @Test
    public void testForDima2025121500112758033() throws SQLException {
        Connection conn = setConnection("&useServerPrepStmts=true&cachePrepStmts=true&prepStmtCacheSqlLimit=20");
        createTable("zt1", "c1 int, c2 varchar(10)");

        Statement statement = conn.createStatement();
        statement.execute("alter system set open_cursors=10");
        int paramSize = 100;
        StringBuffer sb = new StringBuffer("select * from zt1 where c1 in (");
        for (int j = 0; j < paramSize; j++) {
            sb.append(" ?,");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(")");

        int count = 0;
        try {
            for (int i = 0; i < 15; i++) {
                PreparedStatement ps = conn.prepareStatement(sb.toString());
                for (int j = 0; j < paramSize; j++) {
                    ps.setInt(j + 1, i);
                }
                ps.executeQuery();
                ps.close();
                count++;
            }
        } catch (Exception e) {
            Assert.fail();
        } finally {
            statement.execute("alter system set open_cursors=100");
        }
        Assert.assertEquals(15, count);
    }

    @Test
    public void testForDima2025041700108314945() throws SQLException {
        Connection conn = setConnection("&useServerPrepStmts=false");
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select q'[select a='#{1}']' from dual");
        Assert.assertTrue(rs.next());
        Assert.assertEquals("select a='#{1}'", rs.getString(1));

        PreparedStatement ps = conn.prepareStatement("select q'2select // hello, --hello，，'123321'， #奥星贝斯  a='#{1}'2' from dual;");
        rs = ps.executeQuery();
        Assert.assertTrue(rs.next());
        Assert.assertEquals("select // hello, --hello，，'123321'， #奥星贝斯  a='#{1}'", rs.getString(1));
    }

}
