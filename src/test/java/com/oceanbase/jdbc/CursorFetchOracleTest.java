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

import java.sql.*;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class CursorFetchOracleTest extends BaseOracleTest {
    static String      tableName1 = "test_cursor_oracle";
    static String      tableName2 = "test_batch_oracle";
    static String      tableName3 = "fix_cursor_without_resultset";
    private Connection connection = sharedConnection;

    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable(tableName1, "c1 int, c2 varchar(30), constraint pk primary key(c1)");
        createTable(tableName2, "c1 varchar(30), c2 varchar(30), c3 varchar(30)");
        createTable(tableName3, "c1 int, c2 varchar(30)");

        String query = "insert all";
        for (int i = 1; i <= 100; i++) {
            query += " into " + tableName1 + " values(" + i + ",'" + i + "+string')";
        }
        query += "select * from dual";
        Statement stmt = sharedConnection.createStatement();
        stmt.execute(query);

    }

    @Test
    public void fixCursorFetch() {
        try {
            Connection conn = setConnection("&useCursorFetch=true");
            Statement statement = conn.createStatement();
            PreparedStatement preparedStatement = conn.prepareStatement(
                "insert into " + tableName3 + " values(1,'varcharvalue')",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            preparedStatement.setFetchSize(10);
            preparedStatement.setQueryTimeout(0);
            preparedStatement.execute();
            ResultSet resultSet = statement.executeQuery("select count(*) from " + tableName3);
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(1, resultSet.getInt(1));
            preparedStatement = conn.prepareStatement("update   " + tableName3
                                                      + " set c2 = 'varcharval2' where c1 = ?",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            preparedStatement.setFetchSize(10);
            preparedStatement.setQueryTimeout(0);
            preparedStatement.setInt(1, 1);
            preparedStatement.execute();
            preparedStatement = conn.prepareStatement("select * from    " + tableName3
                                                      + "  where c1 = 1",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            preparedStatement.setFetchSize(10);
            preparedStatement.setQueryTimeout(0);
            ResultSet resultSet1 = preparedStatement.executeQuery();
            Assert.assertTrue(resultSet1.next());
            Assert.assertEquals(1, resultSet1.getInt(1));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testExecuteBatch() {
        try {
            //Connection connection = setConnection("&autocommit=false&rewriteBatchedStatements=false&useBatchMultiSend=true&useBulkStmts=true");
            PreparedStatement stmt = connection.prepareStatement("insert into " + tableName2
                                                                 + " values(?,?,?)");

            stmt.setString(1, "str1");
            stmt.setString(2, "str2");
            stmt.setString(3, "str3");
            stmt.addBatch();

            stmt.setString(1, "var1");
            stmt.setString(2, "var2");
            stmt.setString(3, "var3");
            stmt.addBatch();

            //            stmt.setString(1, "chr1");
            //            stmt.setString(2, "chr2");
            //            stmt.setString(3, "chr3");
            //            stmt.addBatch();

            stmt.executeBatch();

            ResultSet rs = connection.createStatement().executeQuery("select * from " + tableName2);
            Assert.assertTrue(rs.next());
            Assert.assertEquals("str1", rs.getString(1));
            Assert.assertEquals("str2", rs.getString(2));
            Assert.assertEquals("str3", rs.getString(3));
            Assert.assertTrue(rs.next());
            Assert.assertEquals("var1", rs.getString(1));
            Assert.assertEquals("var2", rs.getString(2));
            Assert.assertEquals("var3", rs.getString(3));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testForwardCursor() throws SQLException {
        Connection connection = setConnection("&useServerPrepStmts=true&useCursorFetch=true");
        PreparedStatement ps = connection.prepareStatement("select * from " + tableName1,
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        ps.setFetchSize(40);
        ResultSet rs = ps.executeQuery();

        int i = 0;
        while (rs.next()) {
            Assert.assertEquals(++i, rs.getInt(1));
        }

        ps.close();
    }

    @Test
    public void testScrollableCursor() throws Exception {
        //Assume.assumeTrue(sharedOptions().useServerPrepStmts);
        try {
            Connection connection = setConnection("&useServerPrepStmts=true&useCursorFetch=true");
            PreparedStatement ps = connection.prepareStatement("select * from " + tableName1,
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            ps.setFetchSize(40);
            ResultSet rs = ps.executeQuery();

            Assert.assertTrue(rs.next());
            Assert.assertFalse(rs.previous());
            try {
                rs.getInt(1);
                Assert.fail("Current position is before the first row");
            } catch (SQLException e) {
                Assert.assertEquals(0, e.getErrorCode());
            }

            Assert.assertTrue(rs.absolute(39));
            Assert.assertEquals(39, rs.getInt(1));
            Assert.assertTrue(rs.relative(2));
            Assert.assertEquals(41, rs.getInt(1));

            Assert.assertFalse(rs.isLast());
            Assert.assertTrue(rs.last());
            Assert.assertTrue(rs.isLast());
            Assert.assertEquals(100, rs.getInt(1));

            Assert.assertTrue(rs.previous());
            Assert.assertEquals(99, rs.getInt(1));

            Assert.assertFalse(rs.isFirst());
            Assert.assertTrue(rs.first());
            Assert.assertTrue(rs.isFirst());
            Assert.assertEquals(1, rs.getInt(1));

            Assert.assertFalse(rs.relative(-3));

            Assert.assertTrue(rs.isBeforeFirst());
            rs.afterLast();
            Assert.assertTrue(rs.isAfterLast());
            rs.beforeFirst();
            Assert.assertTrue(rs.isBeforeFirst());

            ps.close();
            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    public void testSensitiveResultSet() {
        try {
            //Connection connection = setConnection("&useServerPrepStmts=false&useCursorFetch=false");
            PreparedStatement ps = connection.prepareStatement("select c1, c2 from " + tableName1,
                ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ps.setFetchSize(10);
            ResultSet rs = ps.executeQuery();

            Assert.assertEquals(10, ps.getFetchSize());
            Assert.assertEquals(ResultSet.TYPE_SCROLL_SENSITIVE, rs.getType());
            Assert.assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());

            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getInt(1));
            /** 中断，省略更新记录
             * update test_cursor_mysql set c2='-1+changed' where c1=1;
             * update test_cursor_mysql set c2='-3+changed' where c1=3;
             * commit;
             */
            rs.refreshRow();
            Assert.assertEquals("-1+changed", rs.getString(2));
            Assert.assertTrue(rs.absolute(3));
            Assert.assertEquals("-3+changed", rs.getString(2));

            ps.close();
            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
