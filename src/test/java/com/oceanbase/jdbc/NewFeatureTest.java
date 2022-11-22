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

import static org.junit.Assert.fail;

import java.sql.*;

import org.junit.*;

import com.oceanbase.jdbc.internal.util.StringCacheUtil;

public class NewFeatureTest extends BaseTest {
    public static String batchTable  = "batch" + getRandomString(10);
    public static String batchTable2 = "batch2" + getRandomString(10);
    public static String testDouble  = "testDouble" + getRandomString(10);

    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable(batchTable, "c1  int, c2 int , c3 int  ,test varchar(10)");
        createTable(batchTable2, "c1  int, c2 int , c3 int  ,test varchar(10)");
        createTable(testDouble, "field1 DOUBLE");

    }

    // SET SESSION TRANSACTION xxx twice
    @Test
    public void testReadOnly() {
        try {
            boolean isReadOnly = sharedConnection.isReadOnly();
            Assert.assertEquals(isReadOnly, false);
            sharedConnection.setReadOnly(isReadOnly);
            isReadOnly = sharedConnection.isReadOnly();
            Assert.assertEquals(isReadOnly, false);
            sharedConnection.setReadOnly(!isReadOnly);
            isReadOnly = sharedConnection.isReadOnly();
            Assert.assertEquals(isReadOnly, true);
            sharedConnection.setReadOnly(false);
            isReadOnly = sharedConnection.isReadOnly();
            Assert.assertEquals(isReadOnly, false);

        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testAutoCommit() {
        try {
            boolean autoCommit = sharedConnection.getAutoCommit();
            Assert.assertEquals(autoCommit, true);
            sharedConnection.setAutoCommit(false);
            autoCommit = sharedConnection.getAutoCommit();
            Assert.assertEquals(autoCommit, false);
            sharedConnection.setAutoCommit(false);
            autoCommit = sharedConnection.getAutoCommit();
            Assert.assertEquals(autoCommit, false);
            sharedConnection.setAutoCommit(true);
            autoCommit = sharedConnection.getAutoCommit();
            Assert.assertEquals(autoCommit, true);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testTransactionIsolation() {
        try {
            int level = sharedConnection.getTransactionIsolation();
            Assert.assertEquals(level, 2);
            sharedConnection.setTransactionIsolation(level);
            level = sharedConnection.getTransactionIsolation();
            Assert.assertEquals(level, 2);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testCatalog() {
        try {
            String catalog = sharedConnection.getCatalog();
            System.out.println("catalog = " + catalog);
            sharedConnection.setCatalog("sys");
            sharedConnection.setCatalog("sys");
            catalog = sharedConnection.getCatalog();
            System.out.println("catalog = " + catalog);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testUrlCharsetError() {
        try {
            Connection conn = setConnection("&characterEncoding=UTF-8");
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testNewRewrite() throws SQLException {
        try {
            Connection conn = setConnection();
            Statement stmt = conn.createStatement();
            PreparedStatement ps = conn.prepareStatement("insert into " + batchTable
                                                         + " values(?,100,?,?) ");
            ps.setInt(1, 1);
            ps.setInt(2, 30);
            ps.setString(3, "str1");
            ps.addBatch();
            ps.setInt(1, 2);
            ps.setInt(2, 30);
            ps.setString(3, "str1");
            ps.addBatch();
            ps.setInt(1, 3);
            ps.setInt(2, 30);
            ps.setString(3, "str1");
            ps.addBatch();
            ps.setInt(1, 4);
            ps.setInt(2, 30);
            ps.setString(3, "str1");
            ps.addBatch();
            ps.executeBatch();
            ResultSet rs = stmt.executeQuery("select count(*) from " + batchTable);
            Assert.assertNotNull(rs.next());
            Assert.assertEquals(rs.getInt(1), 4);
            rs = stmt.executeQuery("select * from " + batchTable);
            Assert.assertNotNull(rs.next());
            Assert.assertEquals(rs.getInt(1), 1);
            Assert.assertEquals(rs.getInt(2), 100);
            Assert.assertEquals(rs.getInt(3), 30);
            Assert.assertEquals(rs.getString(4), "str1");
            ps = conn.prepareStatement("update " + batchTable + " set c2 = ? where  c1 = ? ");
            ps.setInt(1, 1000);
            ps.setInt(2, 1);
            ps.addBatch();
            ps.setInt(1, 2000);
            ps.setInt(2, 2);
            ps.addBatch();
            ps.setInt(1, 3000);
            ps.setInt(2, 3);
            ps.addBatch();
            ps.executeBatch();

        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testNewRewrite2() throws SQLException {
        try {
            Connection conn = setConnection();
            Statement stmt = conn.createStatement();
            PreparedStatement ps = conn.prepareStatement("insert into " + batchTable2
                                                         + " values(?,100,?,'str1') /*comment*/");
            ps.setInt(1, 1);
            ps.setInt(2, 30);
            ps.addBatch();
            ps.setInt(1, 2);
            ps.setInt(2, 30);
            ps.addBatch();
            ps.setInt(1, 3);
            ps.setInt(2, 30);
            ps.addBatch();
            ps.setInt(1, 4);
            ps.setInt(2, 30);
            ps.addBatch();
            ps.executeBatch();
            ResultSet rs = stmt.executeQuery("select count(*) from " + batchTable2);
            Assert.assertNotNull(rs.next());
            Assert.assertEquals(rs.getInt(1), 4);
            rs = stmt.executeQuery("select * from " + batchTable2);
            Assert.assertNotNull(rs.next());
            Assert.assertEquals(rs.getInt(1), 1);
            Assert.assertEquals(rs.getInt(2), 100);
            Assert.assertEquals(rs.getInt(3), 30);
            Assert.assertEquals(rs.getString(4), "str1");
            ps = conn.prepareStatement("update " + batchTable2 + " set c2 = ? where  c1 = ? ");
            ps.setInt(1, 1000);
            ps.setInt(2, 1);
            ps.addBatch();
            ps.setInt(1, 2000);
            ps.setInt(2, 2);
            ps.addBatch();
            ps.setInt(1, 3000);
            ps.setInt(2, 3);
            ps.addBatch();
            ps.executeBatch();

        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testSleep() {
        try {
            Connection conn = setConnection("&enableQueryTimeouts=false");
            Statement stmt = conn.createStatement();
            stmt.setQueryTimeout(4);
            stmt.executeQuery("select sleep(5)");
            System.out.println("query finished");
            conn.close();
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
        try {
            Connection conn = setConnection("&enableQueryTimeouts=true");
            Statement stmt = conn.createStatement();
            stmt.setQueryTimeout(4);
            stmt.executeQuery("select sleep(5)");
            System.out.println("query finished");
        } catch (Throwable e) {
            Assert.assertTrue(e.getMessage().contains("Query timed out"));
        }
    }

    @Test
    public void testBug18041() throws Exception {
        Connection truncConn = sharedConnection;
        PreparedStatement stm = null;
        truncConn.createStatement().executeUpdate("drop table if exists testBug18041");
        truncConn.createStatement().executeUpdate(
            "create table testBug18041(`a` tinyint(4) NOT NULL, `b` char(4) default NULL)");
        try {
            stm = truncConn.prepareStatement("insert into testBug18041 values (?,?)");
            stm.setInt(1, 1000);
            stm.setString(2, "nnnnnnnnnnnnnnnnnnnnnnnnnnnnnn");
            stm.executeUpdate();
            fail("Truncation exception should have been thrown");
        } catch (Exception truncEx) {
            Assert.assertTrue(truncEx instanceof DataTruncation);
            System.out.println(truncEx.getMessage());
        }
    }

    @Test
    public void testSetDouble() {
        try {
            PreparedStatement pstmt = sharedConnection.prepareStatement("INSERT INTO " + testDouble
                                                                        + " VALUES (?)");

            try {
                pstmt.setDouble(1, Double.NEGATIVE_INFINITY);
                fail("Exception should've been thrown");
            } catch (Exception ex) {
                // expected
            }

            try {
                pstmt.setDouble(1, Double.POSITIVE_INFINITY);
                fail("Exception should've been thrown");
            } catch (Exception ex) {
                // expected
            }
            try {
                pstmt.setDouble(1, Double.NaN);
                fail("Exception should've been thrown");
            } catch (Exception ex) {
                // expected
            }
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testgetRowIdLifetime() {
        try {
            DatabaseMetaData dmd = sharedConnection.getMetaData();
            RowIdLifetime out = dmd.getRowIdLifetime();
            Assert.assertTrue(out.equals(RowIdLifetime.ROWID_UNSUPPORTED));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testFixVulnerabilities() throws SQLException {
        try {
            String str1 = new String("select 1 from dual");
            String str2 = new String("select 1 from dual");
            Assert.assertFalse(str1 == str2);
            String sqlCache = (String) StringCacheUtil.sqlStringCache.get(str1);
            if (sqlCache == null) {
                StringCacheUtil.sqlStringCache.put(str1, str1);
            }
            String sqlCache2 = (String) StringCacheUtil.sqlStringCache.get(str2);
            Assert.assertTrue(str1 == sqlCache2);
        } catch (Exception e) {
            Assert.fail();
            e.printStackTrace();
        }
    }
}
