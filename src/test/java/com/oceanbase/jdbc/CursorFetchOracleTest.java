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

import org.junit.*;

public class CursorFetchOracleTest extends BaseOracleTest {
    static String      tableName  = "test_cursor_oracle";
    static String      tableName2 = "test_batch_oracle";
    static String      tableName3 = "fix_cursor_without_resultset";
    private Connection connection = sharedConnection;

    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable(tableName, "c1 int, c2 varchar(30), constraint pk_yxy primary key(c1)");
        createTable(tableName2, "c1 varchar(30), c2 varchar(30), c3 varchar(30)");
        createTable(tableName3, "c1 int, c2 varchar(30)");
        initStrictTable();
    }

    private static void initStrictTable() throws SQLException {
        Statement stmt = sharedConnection.createStatement();

        stmt.execute("TRUNCATE TABLE " + tableName);

        String query = "insert all";
        for (int i = 1; i <= 100; i++) {
            query += " into " + tableName + " values(" + i + ",'" + i + "+string')";
        }
        query += "select * from dual";
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
        PreparedStatement ps = connection.prepareStatement(" \t\n /*comment*/select * from "
                                                           + tableName,
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
        Assume.assumeTrue(sharedUsePrepare());
        try {
            Connection connection = setConnection("&useServerPrepStmts=true&useCursorFetch=false");
            PreparedStatement ps = connection.prepareStatement("select * from " + tableName,
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            ps.setFetchSize(30);
            ResultSet rs = ps.executeQuery();
            Assert.assertFalse(rs.isBeforeFirst());
            Assert.assertFalse(rs.isAfterLast());

            // fetch
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getInt(1));
            Assert.assertTrue(rs.absolute(31));
            Assert.assertEquals(31, rs.getInt(1));
            Assert.assertTrue(rs.relative(30));
            Assert.assertEquals(61, rs.getInt(1));
            Assert.assertTrue(rs.absolute(-10));
            Assert.assertEquals(91, rs.getInt(1));
            Assert.assertFalse(rs.isFirst());
            Assert.assertFalse(rs.isLast());
            Assert.assertFalse(rs.isBeforeFirst());
            Assert.assertFalse(rs.isAfterLast());

            // no fetch
            Assert.assertTrue(rs.absolute(-12));
            Assert.assertEquals(89, rs.getInt(1));
            Assert.assertTrue(rs.absolute(70));
            Assert.assertEquals(70, rs.getInt(1));
            Assert.assertTrue(rs.relative(-5));
            Assert.assertEquals(65, rs.getInt(1));
            Assert.assertTrue(rs.previous());
            Assert.assertEquals(64, rs.getInt(1));
            Assert.assertTrue(rs.relative(15));
            Assert.assertEquals(79, rs.getInt(1));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(80, rs.getInt(1));
            Assert.assertFalse(rs.isFirst());
            Assert.assertFalse(rs.isLast());
            Assert.assertFalse(rs.isBeforeFirst());
            Assert.assertFalse(rs.isAfterLast());

            // first
            scrollOnFirst(rs);

            // last
            scrollOnLast(rs);

            // beforeFirst
            scrollOnAfterLast(rs);

            // afterLast
            scrollOnBeforeFirst(rs);

            scrollOnEmptySet(connection);

            ps.close();
            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testOracleForwardCursor() throws Exception {
        try {
            Connection connection = setConnection("&useServerPrepStmts=true&useCursorFetch=false&useCursorOffset=true&useOraclePrepareExecute=true");
            PreparedStatement ps = connection.prepareStatement("select * from " + tableName,
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            ps.setFetchSize(40);
            ResultSet rs = ps.executeQuery();
            Assert.assertTrue(rs.isBeforeFirst());
            Assert.assertFalse(rs.isAfterLast());

            int i = 0;
            while (rs.next()) {
                i++;
                if (i == 80) {
                    i = i;
                }
            }

            ps.close();
            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testOracleScrollableCursor() throws Exception {
        try {
            Connection connection = setConnection("&useServerPrepStmts=true&useCursorFetch=false&useCursorOffset=true&useOraclePrepareExecute=true");
            PreparedStatement ps = connection.prepareStatement("select * from " + tableName,
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            ps.setFetchSize(40);
            ResultSet rs = ps.executeQuery();
            Assert.assertTrue(rs.isBeforeFirst());
            Assert.assertFalse(rs.isAfterLast());
            try {
                Assert.assertTrue(rs.absolute(0));
            } catch (SQLException e) {
                Assert.assertEquals(0, e.getErrorCode());
                Assert.assertEquals("Invalid parameter: absolute(0)", e.getMessage());
            }

            Assert.assertTrue(rs.absolute(40));
            Assert.assertEquals(40, rs.getInt(1));
            // fetch
            Assert.assertTrue(rs.next());
            Assert.assertEquals(41, rs.getInt(1));
            Assert.assertTrue(rs.absolute(-10));
            Assert.assertEquals(91, rs.getInt(1));
            Assert.assertTrue(rs.absolute(81));
            Assert.assertEquals(81, rs.getInt(1));
            Assert.assertTrue(rs.relative(-70));
            Assert.assertEquals(11, rs.getInt(1));
            Assert.assertTrue(rs.relative(40));
            Assert.assertEquals(51, rs.getInt(1));
            Assert.assertTrue(rs.previous());
            Assert.assertEquals(50, rs.getInt(1));
            Assert.assertFalse(rs.isFirst());
            Assert.assertFalse(rs.isLast());
            Assert.assertFalse(rs.isBeforeFirst());
            Assert.assertFalse(rs.isAfterLast());

            // no fetch
            Assert.assertTrue(rs.absolute(-12));
            Assert.assertEquals(89, rs.getInt(1));
            Assert.assertTrue(rs.absolute(70));
            Assert.assertEquals(70, rs.getInt(1));
            Assert.assertTrue(rs.relative(-5));
            Assert.assertEquals(65, rs.getInt(1));
            Assert.assertTrue(rs.previous());
            Assert.assertEquals(64, rs.getInt(1));
            Assert.assertTrue(rs.relative(15));
            Assert.assertEquals(79, rs.getInt(1));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(80, rs.getInt(1));
            Assert.assertFalse(rs.isFirst());
            Assert.assertFalse(rs.isLast());
            Assert.assertFalse(rs.isBeforeFirst());
            Assert.assertFalse(rs.isAfterLast());

            // first
            scrollOnFirst(rs);

            // last
            scrollOnLast(rs);

            // beforeFirst
            scrollOnAfterLast(rs);

            // afterLast
            scrollOnBeforeFirst(rs);

            scrollOnEmptySet(connection);

            ps.close();
            rs.close();

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    private void scrollOnFirst(ResultSet rs) throws SQLException {
        Assert.assertTrue(rs.first());
        Assert.assertEquals(1, rs.getInt(1));

        Assert.assertTrue(rs.isFirst());
        Assert.assertFalse(rs.isLast());
        Assert.assertFalse(rs.isBeforeFirst());
        Assert.assertFalse(rs.isAfterLast());

        // first call next
        Assert.assertTrue(rs.next());
        Assert.assertEquals(2, rs.getInt(1));
        // first call previous
        Assert.assertTrue(rs.first());
        Assert.assertFalse(rs.previous());
        Assert.assertTrue(rs.isBeforeFirst());

        // first call relative to beforeFirst        
        Assert.assertTrue(rs.first());
        Assert.assertFalse(rs.relative(-1));
        Assert.assertTrue(rs.isBeforeFirst());
        // first call relative to before beforeFirst
        Assert.assertTrue(rs.first());
        Assert.assertFalse(rs.relative(-2));
        Assert.assertTrue(rs.isBeforeFirst());

        // first call absolute to retrieve from first
        Assert.assertTrue(rs.first());
        Assert.assertTrue(rs.absolute(2));
        Assert.assertEquals(2, rs.getInt(1));
        // first call absolute to retrieve from last
        Assert.assertTrue(rs.first());
        Assert.assertTrue(rs.absolute(-2));
        Assert.assertEquals(99, rs.getInt(1));

        // first call first
        Assert.assertTrue(rs.first());
        Assert.assertTrue(rs.first());
        Assert.assertEquals(1, rs.getInt(1));
        // first call last
        Assert.assertTrue(rs.first());
        Assert.assertTrue(rs.last());
        Assert.assertEquals(100, rs.getInt(1));

        // first call beforeFirst
        Assert.assertTrue(rs.first());
        rs.beforeFirst();
        Assert.assertTrue(rs.isBeforeFirst());
        // first call afterLast
        Assert.assertTrue(rs.first());
        rs.afterLast();
        Assert.assertTrue(rs.isAfterLast());
    }

    private void scrollOnLast(ResultSet rs) throws SQLException {
        Assert.assertTrue(rs.last());
        Assert.assertEquals(100, rs.getInt(1));

        Assert.assertFalse(rs.isFirst());
        Assert.assertTrue(rs.isLast());
        Assert.assertFalse(rs.isBeforeFirst());
        Assert.assertFalse(rs.isAfterLast());

        // last call next
        Assert.assertFalse(rs.next());
        Assert.assertTrue(rs.isAfterLast());
        // last call previous
        Assert.assertTrue(rs.last());
        Assert.assertTrue(rs.previous());
        Assert.assertEquals(99, rs.getInt(1));

        // last call relative to afterLast
        Assert.assertTrue(rs.last());
        Assert.assertFalse(rs.relative(1));
        Assert.assertTrue(rs.isAfterLast());
        // last call relative to after afterLast
        Assert.assertTrue(rs.last());
        Assert.assertFalse(rs.relative(2));
        Assert.assertTrue(rs.isAfterLast());

        // last call absolute to retrieve from first
        Assert.assertTrue(rs.last());
        Assert.assertTrue(rs.absolute(2));
        Assert.assertEquals(2, rs.getInt(1));
        // last call absolute to retrieve from last
        Assert.assertTrue(rs.last());
        Assert.assertTrue(rs.absolute(-2));
        Assert.assertEquals(99, rs.getInt(1));

        // last call first
        Assert.assertTrue(rs.last());
        Assert.assertTrue(rs.first());
        Assert.assertEquals(1, rs.getInt(1));
        // last call last
        Assert.assertTrue(rs.last());
        Assert.assertTrue(rs.last());
        Assert.assertEquals(100, rs.getInt(1));

        // last call beforeFirst
        Assert.assertTrue(rs.last());
        rs.beforeFirst();
        Assert.assertTrue(rs.isBeforeFirst());
        // last call afterLast
        Assert.assertTrue(rs.last());
        rs.afterLast();
        Assert.assertTrue(rs.isAfterLast());
    }

    private void scrollOnBeforeFirst(ResultSet rs) throws SQLException {
        rs.beforeFirst();

        Assert.assertFalse(rs.isFirst());
        Assert.assertFalse(rs.isLast());
        Assert.assertTrue(rs.isBeforeFirst());
        Assert.assertFalse(rs.isAfterLast());

        // beforeFirst call next
        Assert.assertTrue(rs.next());
        Assert.assertEquals(1, rs.getInt(1));
        // beforeFirst call previous
        rs.beforeFirst();
        Assert.assertFalse(rs.previous());
        Assert.assertTrue(rs.isBeforeFirst());

        // beforeFirst call relative to before beforeFirst
        rs.beforeFirst();
        Assert.assertFalse(rs.relative(-1));
        Assert.assertTrue(rs.isBeforeFirst());
        // beforeFirst call relative to first
        rs.beforeFirst();
        Assert.assertTrue(rs.relative(1));
        Assert.assertEquals(1, rs.getInt(1));

        // beforeFirst call absolute to retrieve from first
        rs.beforeFirst();
        Assert.assertTrue(rs.absolute(2));
        Assert.assertEquals(2, rs.getInt(1));
        // beforeFirst call absolute to retrieve from last
        rs.beforeFirst();
        Assert.assertTrue(rs.absolute(-2));
        Assert.assertEquals(99, rs.getInt(1));

        // beforeFirst call first
        rs.beforeFirst();
        Assert.assertTrue(rs.first());
        Assert.assertEquals(1, rs.getInt(1));
        // beforeFirst call last
        rs.beforeFirst();
        Assert.assertTrue(rs.last());
        Assert.assertEquals(100, rs.getInt(1));

        // beforeFirst call beforeFirst
        rs.beforeFirst();
        rs.beforeFirst();
        Assert.assertTrue(rs.isBeforeFirst());
        // beforeFirst call afterLast
        rs.beforeFirst();
        rs.afterLast();
        Assert.assertTrue(rs.isAfterLast());
    }

    private void scrollOnAfterLast(ResultSet rs) throws SQLException {
        rs.afterLast();

        Assert.assertFalse(rs.isFirst());
        Assert.assertFalse(rs.isLast());
        Assert.assertFalse(rs.isBeforeFirst());
        Assert.assertTrue(rs.isAfterLast());

        // afterLast call next
        Assert.assertFalse(rs.next());
        Assert.assertTrue(rs.isAfterLast());
        // afterLast call previous
        rs.afterLast();
        Assert.assertTrue(rs.previous());
        Assert.assertEquals(100, rs.getInt(1));

        // afterLast call relative to last
        rs.afterLast();
        Assert.assertTrue(rs.relative(-1));
        Assert.assertEquals(100, rs.getInt(1));
        // afterLast call relative to after afterLast
        rs.afterLast();
        Assert.assertFalse(rs.relative(1));
        Assert.assertTrue(rs.isAfterLast());

        // afterLast call absolute to retrieve from first
        rs.afterLast();
        Assert.assertTrue(rs.absolute(2));
        Assert.assertEquals(2, rs.getInt(1));
        // afterLast call absolute to retrieve from last
        rs.afterLast();
        Assert.assertTrue(rs.absolute(-2));
        Assert.assertEquals(99, rs.getInt(1));

        // afterLast call first
        rs.afterLast();
        Assert.assertTrue(rs.first());
        Assert.assertEquals(1, rs.getInt(1));
        // afterLast call last
        rs.afterLast();
        Assert.assertTrue(rs.last());
        Assert.assertEquals(100, rs.getInt(1));

        // afterLast call beforeFirst
        rs.afterLast();
        rs.beforeFirst();
        Assert.assertTrue(rs.isBeforeFirst());
        // afterLast call afterLast
        rs.afterLast();
        rs.afterLast();
        Assert.assertTrue(rs.isAfterLast());
    }

    private void scrollOnEmptySet(Connection conn) throws SQLException {
        PreparedStatement ps = conn.prepareStatement("select * from " + tableName + " where 1=2",
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        ps.setFetchSize(30);
        ResultSet rs = ps.executeQuery();
        Assert.assertFalse(rs.isBeforeFirst());
        Assert.assertFalse(rs.isAfterLast());

        // call first
        Assert.assertFalse(rs.first());
        Assert.assertFalse(rs.isBeforeFirst());
        Assert.assertFalse(rs.isAfterLast());

        // call last
        Assert.assertFalse(rs.last());
        Assert.assertFalse(rs.isBeforeFirst());
        Assert.assertFalse(rs.isAfterLast());

        // call beforeFirst
        rs.beforeFirst();
        Assert.assertFalse(rs.isBeforeFirst());
        Assert.assertFalse(rs.isAfterLast());

        // call afterLast
        rs.afterLast();
        Assert.assertFalse(rs.isBeforeFirst());
        Assert.assertFalse(rs.isAfterLast());

        // call previous
        Assert.assertFalse(rs.previous());
        Assert.assertFalse(rs.isBeforeFirst());
        Assert.assertFalse(rs.isAfterLast());

        // call next
        Assert.assertFalse(rs.next());
        Assert.assertFalse(rs.isBeforeFirst());
        Assert.assertFalse(rs.isAfterLast());

        // call absolute beyond the upper bound
        Assert.assertFalse(rs.absolute(140));
        Assert.assertFalse(rs.isBeforeFirst());
        Assert.assertFalse(rs.isAfterLast());

        // call absolute beyond the lower bound
        Assert.assertFalse(rs.absolute(-140));
        Assert.assertFalse(rs.isBeforeFirst());
        Assert.assertFalse(rs.isAfterLast());

        Assert.assertFalse(rs.relative(140));
        Assert.assertFalse(rs.isBeforeFirst());
        Assert.assertFalse(rs.isAfterLast());

        Assert.assertFalse(rs.relative(-140));
        Assert.assertFalse(rs.isBeforeFirst());
        Assert.assertFalse(rs.isAfterLast());

    }

    @Test
    public void testSensitiveResultSet() throws SQLException {
        Assume.assumeFalse(sharedOptions().useCursorOffset);
        try {
            Connection connection = setConnection("&useServerPrepStmts=true&useCursorFetch=true&useOraclePrepareExecute=false");
            PreparedStatement ps = connection.prepareStatement("-- select注释\n"
                                                               + "select /*插入的注释*/ c1, c2 /*\n"
                                                               + "换行的注释*/from " + tableName + "\n"
                                                               + "where 2 = ?",
            //"where 2 = :name1", // ORA-00600: internal error code, arguments: -4013, No memory or reach tenant memory limit
            //"where 2 = :1", // ORA-00600: internal error code, arguments: -4016, Internal error
                ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            ps.setInt(1, 2);

            ResultSet rs = ps.executeQuery();
            Assert.assertEquals(10, ps.getFetchSize());
            Assert.assertEquals(ResultSet.TYPE_SCROLL_SENSITIVE, rs.getType());
            Assert.assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());

            Assert.assertTrue(rs.absolute(3));
            Assert.assertEquals("3+string", rs.getString(2));
            Assert.assertEquals(2, rs.getMetaData().getColumnCount());
            Assert.assertEquals("C1", rs.getMetaData().getColumnName(1));
            Assert.assertEquals("C2", rs.getMetaData().getColumnName(2));
            Assert.assertEquals("ROWID", rs.getMetaData().getColumnName(0));

            Connection another = setConnection("&useServerPrepStmts=true&useCursorFetch=true&useOraclePrepareExecute=false");
            Statement stmt = another.createStatement();
            stmt.execute("update " + tableName + " set c2='-1+changed' where c1=1");
            stmt.execute("update " + tableName + "  set c2='-30+changed' where c1=3");
            stmt.execute("update " + tableName + "  set c2='-500+changed' where c1=5");
            stmt.execute("update " + tableName + "  set c2='-2000+changed' where c1=20");
            stmt.execute("update " + tableName + "  set c2='-45000+changed' where c1=45");
            stmt.execute("update " + tableName + "  set c2='-990000+changed' where c1=99");

            rs.refreshRow();
            Assert.assertEquals("-30+changed", rs.getString(2));

            Assert.assertTrue(rs.first());
            Assert.assertEquals("-1+changed", rs.getString(2));

            Assert.assertTrue(rs.relative(4));
            Assert.assertEquals("-500+changed", rs.getString(2));

            Assert.assertTrue(rs.absolute(45));
            Assert.assertEquals("-45000+changed", rs.getString(2));

            Assert.assertTrue(rs.relative(-25));
            Assert.assertEquals("-2000+changed", rs.getString(2));

            Assert.assertTrue(rs.last());
            Assert.assertEquals("100+string", rs.getString(2));

            stmt.execute("update " + tableName + "  set c2='-1000000+changed' where c1=100");
            Assert.assertTrue(rs.last());
            Assert.assertEquals("100+string", rs.getString(2));
            rs.refreshRow();
            Assert.assertEquals("-1000000+changed", rs.getString(2));

            stmt.execute("delete from " + tableName + " where c1=100");
            Assert.assertTrue(rs.last());
            Assert.assertEquals("-1000000+changed", rs.getString(2));

            Assert.assertTrue(rs.previous());
            Assert.assertEquals("-990000+changed", rs.getString(2));

            ps.close();
            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            initStrictTable();
        }
    }

    @Test
    public void testSensitiveResultSetInvalidSqlSyntax() {
        Assume.assumeFalse(sharedOptions().useCursorOffset);
        try {
            Connection connection = setConnection("&useServerPrepStmts=true&useCursorFetch=true&useOraclePrepareExecute=false");
            PreparedStatement ps = connection.prepareStatement("-- select注释\n"
                                                               + "select /*插入的注释*/ * /*\n"
                                                               + "换行的注释*/from " + tableName + "\n"
                                                               + "where 2 = ?",
                ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            ps.setInt(1, 2);

            ResultSet rs = ps.executeQuery();
            Assert.assertEquals(10, ps.getFetchSize());
            Assert.assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE, rs.getType());
            Assert.assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());

            Assert.assertTrue(rs.absolute(3));
            Assert.assertEquals("3+string", rs.getString(2));

            try {
                rs.refreshRow();
            } catch (SQLFeatureNotSupportedException sqlEx) {
                Assert.assertTrue(sqlEx.getMessage().contains(
                    "refreshRow are not supported when using 1004 and 1007"));
            }

            ps.close();
            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSensitiveResultSetTableNotExist() throws SQLException {
        Assume.assumeFalse(sharedOptions().useCursorOffset);
        try {
            Connection connection = setConnection("&useServerPrepStmts=true&useCursorFetch=true&useOraclePrepareExecute=false");
            PreparedStatement ps = connection.prepareStatement("-- select注释\n"
                                                               + "select /*插入的注释*/ c1, c2 /*\n"
                                                               + "换行的注释*/from " + tableName + "\n"
                                                               + "where 2 = ?",
                ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            ps.setInt(1, 2);

            ResultSet rs = ps.executeQuery();
            Assert.assertEquals(10, ps.getFetchSize());
            Assert.assertEquals(ResultSet.TYPE_SCROLL_SENSITIVE, rs.getType());
            Assert.assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());

            Assert.assertTrue(rs.absolute(3));
            Assert.assertEquals("3+string", rs.getString(2));

            Connection another = setConnection("&useServerPrepStmts=true&useCursorFetch=true&useOraclePrepareExecute=false");
            Statement stmt = another.createStatement();
            stmt.execute("drop table " + tableName);//表或视图不存在，抛异常

            try {
                rs.refreshRow();
            } catch (SQLException sqlEx) {
                Assert.assertEquals(942, sqlEx.getErrorCode());
            }

            ps.close();
            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            createTable(tableName, "c1 int, c2 varchar(30), constraint pk primary key(c1)");
            initStrictTable();
        }
    }

    @Test
    public void testSensitiveResultSetFromTwoTables() throws SQLException {
        Assume.assumeFalse(sharedOptions().useCursorOffset);
        try {
            createTable("joinTable",
                "col1 int, col2 varchar(30), col3 varchar(100), constraint pk_yxy2 primary key(col1)");
            String query = "insert all";
            for (int i = 1; i <= 10; i++) {
                query += " into joinTable values(" + i + ", '" + i + "_join', 'aaa')";
            }
            query += "select * from dual";
            Statement stmt = sharedConnection.createStatement();
            stmt.execute(query);

            Connection connection = setConnection("&useServerPrepStmts=true&useCursorFetch=true&useOraclePrepareExecute=true");
            PreparedStatement ps = connection.prepareStatement("-- select注释\n"
                                                               + "select /*插入的注释*/ c2, col2 /*\n"
                                                               + "换行的注释*/from " + tableName + "\n"
                                                               + "left join joinTable on "
                                                               + tableName
                                                               + ".c1 = joinTable.col1\n"
                                                               + "where 2 = ?",
                ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY); // OB JDBC catch ORA-00918: column 'ROWID' in field list ambiguously defined
            ps.setInt(1, 2);

            ResultSet rs = ps.executeQuery(); // Oracle JDBC catch ORA-00918: column ambiguously defined
            Assert.assertEquals(10, ps.getFetchSize());
            Assert.assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE, rs.getType());
            Assert.assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());

            Assert.assertTrue(rs.absolute(3));
            Assert.assertEquals("3+string", rs.getString(1));
            Assert.assertEquals("3_join", rs.getString(2));
            try {
                rs.updateString(1, "-3+changed");
                rs.updateRow();
            } catch (SQLFeatureNotSupportedException ex) {
                Assert.assertEquals(
                    "Updates are not supported when using ResultSet.CONCUR_READ_ONLY",
                    ex.getMessage());
            }

            ps.close();
            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            createTable(tableName, "c1 int, c2 varchar(30), constraint pk primary key(c1)");
            initStrictTable();
        }
    }

    @Test
    public void testSensitiveResultSetWithRowid() throws SQLException {
        Assume.assumeFalse(sharedOptions().useCursorOffset);
        try {
            Connection connection = setConnection("&useServerPrepStmts=true&useCursorFetch=true&useOraclePrepareExecute=false");
            PreparedStatement ps = connection.prepareStatement(
                "-- select注释\n" + "select /*插入的注释*/ rowid, c1, c2 /*\n" + "换行的注释*/from "
                        + tableName + "\n" + "where 2 = ?", ResultSet.TYPE_SCROLL_SENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
            ps.setInt(1, 2);

            ResultSet rs = ps.executeQuery();
            Assert.assertEquals(10, ps.getFetchSize());
            Assert.assertEquals(ResultSet.TYPE_SCROLL_SENSITIVE, rs.getType());
            Assert.assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());

            Assert.assertTrue(rs.absolute(3));
            Assert.assertEquals("3+string", rs.getString(3));

            Connection another = setConnection("&useServerPrepStmts=true&useCursorFetch=true&useOraclePrepareExecute=false");
            Statement stmt = another.createStatement();
            stmt.execute("update " + tableName + " set c2='-1+changed' where c1=1");
            stmt.execute("update " + tableName + "  set c2='-30+changed' where c1=3");
            stmt.execute("update " + tableName + "  set c2='-500+changed' where c1=5");
            stmt.execute("update " + tableName + "  set c2='-1500+changed' where c1=15");
            stmt.execute("update " + tableName + "  set c2='-20000+changed' where c1=20");
            stmt.execute("update " + tableName + "  set c2='-450000+changed' where c1=45");
            stmt.execute("delete from " + tableName + " where c1=45");

            rs.refreshRow();
            Assert.assertEquals("-30+changed", rs.getString(3));
            Assert.assertTrue(rs.relative(-2));
            Assert.assertEquals("-1+changed", rs.getString(3));
            Assert.assertTrue(rs.absolute(5));
            Assert.assertEquals("-500+changed", rs.getString(3));
            Assert.assertTrue(rs.absolute(15));
            Assert.assertEquals("-1500+changed", rs.getString(3));
            Assert.assertTrue(rs.absolute(20));
            Assert.assertEquals("-20000+changed", rs.getString(3));
            Assert.assertTrue(rs.absolute(45));
            Assert.assertEquals("45+string", rs.getString(3));

            ps.close();
            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            initStrictTable();
        }
    }

    @Test
    public void testForwardUpdatableCursorResultSet() throws SQLException {
        Assume.assumeFalse(sharedOptions().useCursorOffset);
        try {
            Connection connection = setConnection("&useServerPrepStmts=true&useCursorFetch=true&useOraclePrepareExecute=false");
            PreparedStatement ps = connection.prepareStatement("-- select注释\n"
                                                               + "select /*插入的注释*/ c1, c2 /*\n"
                                                               + "换行的注释*/from " + tableName + "\n"
                                                               + "where 2 = ?",
            //"where 2 = :name1", // ORA-00600: internal error code, arguments: -4013, No memory or reach tenant memory limit
            //"where 2 = :1", // ORA-00600: internal error code, arguments: -4016, Internal error
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
            ps.setInt(1, 2);

            ResultSet rs = ps.executeQuery();
            Assert.assertEquals(10, ps.getFetchSize());
            Assert.assertEquals(ResultSet.TYPE_FORWARD_ONLY, rs.getType());
            Assert.assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
            String str;

            Assert.assertTrue(rs.next());
            str = rs.getString(2);
            Assert.assertEquals("1+string", str);
            rs.updateString(2, "-1+changed");
            rs.updateRow();
            str = rs.getString(2);
            Assert.assertEquals("-1+changed", str);

            Assert.assertTrue(rs.next());
            str = rs.getString(2);
            Assert.assertEquals("2+string", str);
            rs.deleteRow();
            str = rs.getString(2);
            Assert.assertEquals("2+string", str);

            Assert.assertTrue(rs.next());
            str = rs.getString(2);
            Assert.assertEquals("3+string", str);

            rs.moveToInsertRow();
            rs.updateInt(1, 101);
            rs.updateString(2, "+101+inserted");
            rs.insertRow();

            rs.moveToCurrentRow();
            str = rs.getString(2);
            Assert.assertEquals("3+string", str);

            for (int i = 4; i < 101; i++) {
                Assert.assertTrue(rs.next());
                str = rs.getString(2);
                Assert.assertEquals(i + "+string", str);
            }

            try {
                rs.refreshRow();
            } catch (Exception ex) {
                Assert.assertEquals("refreshRow are not supported when using 1003 and 1008",
                    ex.getMessage());
            }

            Assert.assertFalse(rs.next());

            Assert.assertEquals(2, rs.getMetaData().getColumnCount());
            Assert.assertEquals("C1", rs.getMetaData().getColumnName(1));
            Assert.assertEquals("C2", rs.getMetaData().getColumnName(2));
            Assert.assertEquals("ROWID", rs.getMetaData().getColumnName(0));

            ps.close();
            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            initStrictTable();
        }
    }

    @Ignore
    public void testForwardUpdatableResultSet() throws SQLException {
        try {
            Connection connection = setConnection("&useServerPrepStmts=false&useOraclePrepareExecute=false");
            PreparedStatement ps = connection.prepareStatement("-- select注释\n"
                                                               + "select /*插入的注释*/ c1, c2 /*\n"
                                                               + "换行的注释*/from " + tableName + "\n"
                                                               + "where 2 = ?",
            //"where 2 = :name1", // ORA-00600: internal error code, arguments: -4013, No memory or reach tenant memory limit
            //"where 2 = :1", // ORA-00600: internal error code, arguments: -4016, Internal error
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
            ps.setInt(1, 2);

            ResultSet rs = ps.executeQuery();
            Assert.assertEquals(10, ps.getFetchSize());
            Assert.assertEquals(ResultSet.TYPE_FORWARD_ONLY, rs.getType());
            Assert.assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
            String str;

            Assert.assertTrue(rs.next());
            str = rs.getString(2);
            Assert.assertEquals("1+string", str);
            rs.updateString(2, "-1+changed");
            rs.updateRow();
            str = rs.getString(2);
            Assert.assertEquals("-1+changed", str);

            Assert.assertTrue(rs.next());
            str = rs.getString(2);
            Assert.assertEquals("2+string", str);
            rs.deleteRow();
            //rs.moveToCurrentRow();
            str = rs.getString(2);
            Assert.assertEquals("2+string", str);

            Assert.assertTrue(rs.next());
            str = rs.getString(2);
            Assert.assertEquals("3+string", str);

            rs.moveToInsertRow();
            rs.updateInt(1, 101);
            rs.updateString(2, "+101+inserted");
            rs.insertRow();

            rs.moveToCurrentRow();
            str = rs.getString(2);
            Assert.assertEquals("3+string", str);
            Assert.assertTrue(rs.next());
            str = rs.getString(2);
            Assert.assertEquals("4+string", str);

            Assert.assertEquals(2, rs.getMetaData().getColumnCount());
            Assert.assertEquals("C1", rs.getMetaData().getColumnName(1));
            Assert.assertEquals("C2", rs.getMetaData().getColumnName(2));
            Assert.assertEquals("ROWID", rs.getMetaData().getColumnName(0));

            ps.close();
            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            initStrictTable();
        }
    }

    @Test
    public void testInsensitiveUpdatableCursorResultSet() throws SQLException {
        Assume.assumeFalse(sharedOptions().useCursorOffset);
        try {
            Connection connection = setConnection("&useServerPrepStmts=true&useCursorFetch=true&useOraclePrepareExecute=false");
            PreparedStatement ps = connection.prepareStatement("-- select注释\n"
                                                               + "select /*插入的注释*/ c1, c2 /*\n"
                                                               + "换行的注释*/from " + tableName + "\n"
                                                               + "where 2 = ?",
            //"where 2 = :name1", // ORA-00600: internal error code, arguments: -4013, No memory or reach tenant memory limit
            //"where 2 = :1", // ORA-00600: internal error code, arguments: -4016, Internal error
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ps.setInt(1, 2);

            ResultSet rs = ps.executeQuery();
            Assert.assertEquals(10, ps.getFetchSize());
            Assert.assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE, rs.getType());
            Assert.assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
            String str;

            Assert.assertTrue(rs.next());
            str = rs.getString(2);
            Assert.assertEquals("1+string", str);
            rs.updateString(2, "-1+changed");
            rs.updateRow();
            str = rs.getString(2);
            Assert.assertEquals("-1+changed", str);

            Assert.assertTrue(rs.next());
            str = rs.getString(2);
            Assert.assertEquals("2+string", str);
            rs.deleteRow();
            str = rs.getString(2);
            Assert.assertEquals("-1+changed", str);

            Assert.assertTrue(rs.next());
            str = rs.getString(2);
            Assert.assertEquals("3+string", str);
            Assert.assertTrue(rs.absolute(2));
            str = rs.getString(2);
            Assert.assertEquals("3+string", str);

            rs.moveToInsertRow();
            rs.updateInt(1, 101);
            rs.updateString(2, "+101+inserted");
            rs.insertRow();

            rs.moveToCurrentRow();
            str = rs.getString(2);
            Assert.assertEquals("3+string", str);

            Assert.assertTrue(rs.last());
            str = rs.getString(2);
            Assert.assertEquals("100+string", str);

            rs.refreshRow();

            Assert.assertFalse(rs.next());

            Assert.assertEquals(2, rs.getMetaData().getColumnCount());
            Assert.assertEquals("C1", rs.getMetaData().getColumnName(1));
            Assert.assertEquals("C2", rs.getMetaData().getColumnName(2));
            Assert.assertEquals("ROWID", rs.getMetaData().getColumnName(0));

            ps.close();
            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            initStrictTable();
        }
    }

    @Ignore
    public void testInsensitiveUpdatableResultSet() throws SQLException {
        Assume.assumeFalse(sharedOptions().useCursorOffset);
        try {
            Connection connection = setConnection("&useServerPrepStmts=false&useOraclePrepareExecute=false");
            PreparedStatement ps = connection.prepareStatement("-- select注释\n"
                                                               + "select /*插入的注释*/ c1, c2 /*\n"
                                                               + "换行的注释*/from " + tableName + "\n"
                                                               + "where 2 = ?",
            //"where 2 = :name1", // ORA-00600: internal error code, arguments: -4013, No memory or reach tenant memory limit
            //"where 2 = :1", // ORA-00600: internal error code, arguments: -4016, Internal error
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ps.setInt(1, 2);

            ResultSet rs = ps.executeQuery();
            Assert.assertEquals(10, ps.getFetchSize());
            Assert.assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE, rs.getType());
            Assert.assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
            String str;

            Assert.assertTrue(rs.next());
            str = rs.getString(2);
            Assert.assertEquals("1+string", str);
            rs.updateString(2, "-1+changed");
            rs.updateRow();
            str = rs.getString(2);
            Assert.assertEquals("-1+changed", str);

            Assert.assertTrue(rs.next());
            str = rs.getString(2);
            Assert.assertEquals("2+string", str);
            rs.deleteRow();
            //rs.moveToCurrentRow();
            str = rs.getString(2);
            Assert.assertEquals("-1+changed", str);

            Assert.assertTrue(rs.next());
            str = rs.getString(2);
            Assert.assertEquals("3+string", str);
            Assert.assertTrue(rs.absolute(2));
            str = rs.getString(2);
            Assert.assertEquals("3+string", str);

            rs.moveToInsertRow();
            rs.updateInt(1, 101);
            rs.updateString(2, "+101+inserted");
            rs.insertRow();

            rs.moveToCurrentRow();
            str = rs.getString(2);
            Assert.assertEquals("3+string", str);
            Assert.assertTrue(rs.next());
            str = rs.getString(2);
            Assert.assertEquals("4+string", str);

            Assert.assertTrue(rs.last());
            str = rs.getString(2);
            Assert.assertEquals("100+string", str);

            Assert.assertFalse(rs.next());

            Assert.assertEquals(2, rs.getMetaData().getColumnCount());
            Assert.assertEquals("C1", rs.getMetaData().getColumnName(1));
            Assert.assertEquals("C2", rs.getMetaData().getColumnName(2));
            Assert.assertEquals("ROWID", rs.getMetaData().getColumnName(0));

            ps.close();
            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            initStrictTable();
        }
    }

    @Test
    public void testSensitiveUpdatableCursorResultSet() throws SQLException {
        Assume.assumeFalse(sharedOptions().useCursorOffset);
        try {
            Connection connection = setConnection("&useServerPrepStmts=true&useCursorFetch=true&useOraclePrepareExecute=false");
            PreparedStatement ps = connection.prepareStatement("-- select注释\n"
                                                               + "select /*插入的注释*/ c1, c2 /*\n"
                                                               + "换行的注释*/from " + tableName + "\n"
                                                               + "where 2 = ?",
            //"where 2 = :name1", // ORA-00600: internal error code, arguments: -4013, No memory or reach tenant memory limit
            //"where 2 = :1", // ORA-00600: internal error code, arguments: -4016, Internal error
                ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ps.setInt(1, 2);

            ResultSet rs = ps.executeQuery();
            Assert.assertEquals(10, ps.getFetchSize());
            Assert.assertEquals(ResultSet.TYPE_SCROLL_SENSITIVE, rs.getType());
            Assert.assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
            String str;

            Assert.assertTrue(rs.next());
            str = rs.getString(2);
            Assert.assertEquals("1+string", str);
            rs.updateString(2, "-1+changed");
            rs.updateRow();
            str = rs.getString(2);
            Assert.assertEquals("-1+changed", str);

            Assert.assertTrue(rs.next());
            str = rs.getString(2);
            Assert.assertEquals("2+string", str);
            rs.deleteRow();
            str = rs.getString(2);
            Assert.assertEquals("-1+changed", str);

            Assert.assertTrue(rs.next());
            str = rs.getString(2);
            Assert.assertEquals("3+string", str);
            Assert.assertTrue(rs.absolute(2));
            str = rs.getString(2);
            Assert.assertEquals("3+string", str);

            rs.moveToInsertRow();
            rs.updateInt(1, 101);
            rs.updateString(2, "+101+inserted");
            rs.insertRow();

            rs.moveToCurrentRow();
            str = rs.getString(2);
            Assert.assertEquals("3+string", str);

            Assert.assertTrue(rs.last());
            str = rs.getString(2);
            Assert.assertEquals("100+string", str);

            rs.refreshRow();

            Assert.assertFalse(rs.next());

            Assert.assertEquals(2, rs.getMetaData().getColumnCount());
            Assert.assertEquals("C1", rs.getMetaData().getColumnName(1));
            Assert.assertEquals("C2", rs.getMetaData().getColumnName(2));
            Assert.assertEquals("ROWID", rs.getMetaData().getColumnName(0));

            ps.close();
            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            initStrictTable();
        }
    }

    @Ignore
    public void testSensitiveUpdatableResultSet() throws SQLException {
        Assume.assumeFalse(sharedOptions().useCursorOffset);
        try {
            Connection connection = setConnection("&useServerPrepStmts=false&useOraclePrepareExecute=false");
            PreparedStatement ps = connection.prepareStatement("-- select注释\n"
                                                               + "select /*插入的注释*/ c1, c2 /*\n"
                                                               + "换行的注释*/from " + tableName + "\n"
                                                               + "where 2 = ?",
            //"where 2 = :name1", // ORA-00600: internal error code, arguments: -4013, No memory or reach tenant memory limit
            //"where 2 = :1", // ORA-00600: internal error code, arguments: -4016, Internal error
                ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ps.setInt(1, 2);

            ResultSet rs = ps.executeQuery();
            Assert.assertEquals(10, ps.getFetchSize());
            Assert.assertEquals(ResultSet.TYPE_SCROLL_SENSITIVE, rs.getType());
            Assert.assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
            String str;

            Assert.assertTrue(rs.next());
            str = rs.getString(2);
            Assert.assertEquals("1+string", str);
            rs.updateString(2, "-1+changed");
            rs.updateRow();
            str = rs.getString(2);
            Assert.assertEquals("-1+changed", str);

            Assert.assertTrue(rs.next());
            str = rs.getString(2);
            Assert.assertEquals("2+string", str);
            rs.deleteRow();
            //rs.moveToCurrentRow();
            str = rs.getString(2);
            Assert.assertEquals("-1+changed", str);

            Assert.assertTrue(rs.next());
            str = rs.getString(2);
            Assert.assertEquals("3+string", str);
            Assert.assertTrue(rs.absolute(2));
            str = rs.getString(2);
            Assert.assertEquals("3+string", str);

            rs.moveToInsertRow();
            rs.updateInt(1, 101);
            rs.updateString(2, "+101+inserted");
            rs.insertRow();

            rs.moveToCurrentRow();
            str = rs.getString(2);
            Assert.assertEquals("3+string", str);
            Assert.assertTrue(rs.next());
            str = rs.getString(2);
            Assert.assertEquals("4+string", str);

            Assert.assertTrue(rs.last());
            str = rs.getString(2);
            Assert.assertEquals("100+string", str);

            Assert.assertFalse(rs.next());

            Assert.assertEquals(2, rs.getMetaData().getColumnCount());
            Assert.assertEquals("C1", rs.getMetaData().getColumnName(1));
            Assert.assertEquals("C2", rs.getMetaData().getColumnName(2));
            Assert.assertEquals("ROWID", rs.getMetaData().getColumnName(0));

            ps.close();
            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            initStrictTable();
        }
    }

}
