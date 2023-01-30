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

import static org.junit.Assert.assertEquals;

import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Arrays;

import org.junit.*;

public class NewFeatureOracleTest extends BaseOracleTest {
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("testcommit", "c1 int");
        createTable(
            "stringTest",
            "c1 varchar2(200),c2 varchar2(200),c3 varchar2(200),c4 varchar2(200),c5 int,c6 number(12,6),c7 date,c8 varchar2(200),c9 number(12,6),c10 varchar2(200),c11 varchar2(200)");

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
            Assert.assertEquals(2, level);
            sharedConnection.setTransactionIsolation(level);
            level = sharedConnection.getTransactionIsolation();
            Assert.assertEquals(2, level);
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
    public void supportGetObjectCharacter() {
        try {
            Connection conn = setConnection("&autoDeserialize=false");
            Statement stmt = conn.createStatement();
            try {
                stmt.execute("drop table ctest1");
            } catch (SQLException e) {
            }
            stmt.execute("create table ctest1 (c1 char(1));");
            stmt.execute("insert into ctest1 values('c')");
            ResultSet rs = stmt.executeQuery("select c1 from ctest1");
            Assert.assertTrue(rs.next());
            try {
                Object obj = rs.getObject(1, Character.class);
            } catch (Throwable e1) {
                Assert.assertTrue(e1 instanceof SQLException);
            }
            conn = setConnection("&autoDeserialize=true");
            rs = stmt.executeQuery("select c1 from ctest1");
            Assert.assertTrue(rs.next());
            try {
                Object obj = rs.getObject(1, Character.class);
            } catch (Throwable e1) {
                Assert.assertTrue(e1 instanceof SQLException);
            }
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    // should fix the batch
    @org.junit.Test
    public void testSendPieceBatch() {
        Assume.assumeTrue(sharedOptions().useServerPrepStmts);
        try {
            Connection conn = setConnection("&usePieceData=true");
            Statement stmt = conn.createStatement();
            PreparedStatement ps;
            try {
                stmt.execute("drop table clobtest");
            } catch (SQLException e) {
                // e.printStackTrace();
            }
            stmt.execute("create table clobtest(c1 clob, c2 number, c3 clob, c4 int)");
            ps = conn.prepareStatement("select * from clobtest"); // prepare not useless sql
            ps = conn.prepareStatement("insert into clobtest values(?,?,?,?)");
            String str = "abcd";
            String str2 = "中文abcd";
            ps.setCharacterStream(1, new StringReader(str));
            ps.setObject(3, str);
            ps.setInt(2, 100);
            ps.setInt(4, 100);
            ps.addBatch();
            ps.setCharacterStream(1, new StringReader(str2));
            ps.setObject(3, str2);
            ps.setInt(2, 100);
            ps.setInt(4, 100);
            ps.addBatch();
            ps.executeBatch();
            ps = conn.prepareStatement("select * from clobtest");
            ResultSet rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            Assert.assertEquals(str, rs.getString(1));
            Assert.assertEquals(str, rs.getString(3));
            Assert.assertEquals(100, rs.getInt(2));
            Assert.assertEquals(100, rs.getInt(4));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(str2, rs.getString(1));
            Assert.assertEquals(str2, rs.getString(3));
            Assert.assertEquals(100, rs.getInt(2));
            Assert.assertEquals(100, rs.getInt(4));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Ignore
    public void testStringBatch() {
        try {
            Connection conn = setConnection("&rewriteBatchedStatements=true&maxBatchTotalParamsNum=5000");
            Statement stmt = conn.createStatement();
            PreparedStatement preparedStatement = conn
                .prepareStatement("insert into stringtest values(?,?,?,?,?,?,?,?,?,null,null)");
            int val = 0;
            // 30000 * 9 > 65535 （the prepare parameters limit）
            for (int i = 0; i < 30000; i++) {
                preparedStatement.setString(1, "string value");
                preparedStatement.setString(2, "string value");
                preparedStatement.setString(3, "string value");
                preparedStatement.setString(4, "string value");
                preparedStatement.setLong(5, val++);

                preparedStatement.setBigDecimal(6, new BigDecimal(134254.3342));
                preparedStatement.setDate(7, null);

                preparedStatement.setString(8, "string value");
                preparedStatement.setBigDecimal(9, new BigDecimal(134254.3342));
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            long result = val;
            ResultSet rs = stmt.executeQuery("select * from stringtest");
            val = 0;
            long count = 0;
            while (rs.next()) {
                count++;
                Assert.assertEquals(val++, rs.getLong(5));
            }
            Assert.assertEquals(count, 30000);
            Assert.assertEquals(val, result);
            preparedStatement = conn.prepareStatement("update stringtest set c1 = ? where c5 = ?");
            val = 0;
            for (int i = 0; i < 300; i++) {
                preparedStatement.setString(1, "string value updated");
                preparedStatement.setInt(2, val++);
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            rs = stmt.executeQuery("select * from stringtest where  rownum <= 300 order by c5");
            val = 0;
            while (rs.next()) {
                Assert.assertEquals(val++, rs.getLong(5));
                Assert.assertEquals("string value updated", rs.getString(1));
                count++;
            }
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }

    }

    /**
     * Test case for arrayBinding with the stream parameter and  usePieceData=true.
     */
    @Test
    public void testArrayBindingWithPieceData() {
        Assume.assumeTrue(sharedOptions().useServerPrepStmts);
        try {
            Connection conn = setConnectionOrigin("?useArrayBinding=true&usePieceData=true");
            Statement stmt = conn.createStatement();
            try {
                stmt.execute("drop table testArrayBinding");
            } catch (SQLException throwables) {
            }
            stmt.execute("create table testArrayBinding(c1 int,c2 int,c3 varchar2(200))");
            conn.setAutoCommit(false);
            PreparedStatement ps = conn
                .prepareStatement("insert into testArrayBinding values(?,?,?);");
            StringReader stringReader = new StringReader("stringvalue1");
            ps.setInt(1, 1);
            ps.setInt(2, 2);
            ps.setCharacterStream(3, stringReader);
            ps.addBatch();
            stringReader = new StringReader("stringvalue2");
            ps.setInt(1, 1);
            ps.setInt(2, 2);
            ps.setCharacterStream(3, stringReader);
            ps.addBatch();
            ps.executeBatch();
            conn.commit();
            conn.setAutoCommit(true);
            ResultSet rs = stmt.executeQuery("select count(*) from testArrayBinding");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(2, rs.getInt(1));
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            Assert.fail();
        }
    }

    /**
     * Test case for arrayBinding with the stream parameter and  usePieceData=false;
     * This action causes an exception to be thrown "Not supported send long data on ob oracle".
     */
    @Test
    public void testArrayBindingWithLongData() {
        Assume.assumeTrue(sharedOptions().useServerPrepStmts);
        try {
            Connection conn = setConnectionOrigin("?useArrayBinding=true&usePieceData=false");
            System.out.println("conn = " + conn);
            Statement stmt = conn.createStatement();
            try {
                stmt.execute("drop table testArrayBinding");
            } catch (SQLException throwables) {
            }
            stmt.execute("create table testArrayBinding(c1 int,c2 int,c3 varchar2(200))");
            conn.setAutoCommit(false);
            PreparedStatement ps = conn
                .prepareStatement("insert into testArrayBinding values(?,?,?);");
            StringReader stringReader = new StringReader("stringvalue");
            ps.setInt(1, 1);
            ps.setInt(2, 2);
            ps.setCharacterStream(3, stringReader);
            ps.addBatch();
            ps.executeBatch();
            conn.commit();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            Assert.assertTrue(throwables.getMessage().contains(
                "Not supported send long data on ob oracle"));
        }
    }

    /**
     * Test case for arrayBinding without the stream parameters.
     */
    @Test
    public void testArrayBindingNormal() {
        Assume.assumeTrue(sharedOptions().useServerPrepStmts);
        try {
            Connection conn = setConnection("&useArrayBinding=true");
            Statement stmt = conn.createStatement();
            try {
                stmt.execute("drop table test_full_link_trace");
            } catch (SQLException throwables) {
            }
            stmt.execute("create table test_full_link_trace (c1 int,c2 int)");
            conn.setAutoCommit(false);
            PreparedStatement ps = conn
                .prepareStatement("insert into test_full_link_trace values(?,?);");
            int num1 = 100;
            int num2 = 3;
            for (int i = 0; i < num1; i++) {
                ps.setInt(1, 1);
                ps.setInt(2, 2);
                ps.addBatch();
            }

            for (int i = 0; i < num2; i++) {
                ps.setInt(1, 10000);
                ps.setString(2, "3000");
                ps.addBatch();
            }

            ps.executeBatch();
            conn.commit();
            conn.setAutoCommit(true);

            ps = conn.prepareStatement("select count(*)   from test_full_link_trace");
            ResultSet rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            Assert.assertEquals(num1 + num2, rs.getInt(1));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testArrayBindingNormalPrepareExecute() {
        Assume.assumeTrue(sharedOptions().useServerPrepStmts);
        try {
            Connection conn = setConnectionOrigin("?useArrayBinding=true&usePieceData=true");
            Statement stmt = conn.createStatement();
            try {
                stmt.execute("drop table test_full_link_trace");
            } catch (SQLException throwables) {
            }
            stmt.execute("create table test_full_link_trace (c1 int,c2 int)");
            conn.setAutoCommit(false);
            PreparedStatement ps = conn
                .prepareStatement("insert into test_full_link_trace values(?,?);");
            int num1 = 1;
            int num2 = 0;
            for (int i = 0; i < num1; i++) {
                ps.setInt(1, 1);
                ps.setInt(2, 2);
                ps.addBatch();
            }

            for (int i = 0; i < num2; i++) {
                ps.setInt(1, 10000);
                ps.setString(2, "3000");
                ps.addBatch();
            }

            ps.executeBatch();
            conn.commit();
            conn.setAutoCommit(true);

            ps = conn.prepareStatement("select count(*)   from test_full_link_trace");
            ResultSet rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            Assert.assertEquals(num1 + num2, rs.getInt(1));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testArrayBindingVarchar41574930() {
        Assume.assumeTrue(sharedOptions().useServerPrepStmts);
        try {
            Connection conn = setConnectionOrigin("?useArrayBinding=true");
            createTable("testInt", "c1 int, c2 varchar2(20)");
            conn.setAutoCommit(false);
            PreparedStatement ps = conn.prepareStatement("insert into testInt values(?,?);");
            ps.setObject(1, "error");
            ps.setObject(2, "1");
            ps.addBatch();

            for (int i = 0; i < 3; i++) {
                ps.setInt(1, 10000);
                ps.setString(2, i + "3000");
                ps.addBatch();
            }
            try {
                ps.executeBatch();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            conn.commit();
            conn.setAutoCommit(true);

            ps = conn.prepareStatement("select count(*) from testInt");
            ResultSet rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            Assert.assertEquals(3, rs.getInt(1));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    /*
      Fix arrayBinding for batch exception.
     */
    @Test
    public void testArrayBindingUpdateCounts() {
        Assume.assumeTrue(sharedOptions().useServerPrepStmts);
        try {
            Connection conn = setConnectionOrigin("?useArrayBinding=true&continueBatchOnError=false");
            createTable("arrayBindingTest1", "c1 int, c2 varchar2(20)");

            int insertRow = 5;
            conn.setAutoCommit(false);
            PreparedStatement ps = conn
                .prepareStatement("insert into arrayBindingTest1 values(?,?);");
            // arrayBinding all type are string
            for (int i = 0; i < 20; i++) {
                if (i == insertRow) {
                    ps.setString(1, "errorValue");
                } else {
                    ps.setString(1, Integer.toString(i));
                }
                ps.setString(2, i + "3000");
                ps.addBatch();
            }
            try {
                int ret[] = ps.executeBatch();
            } catch (BatchUpdateException e) {
                int counts[] = e.getUpdateCounts();
                Assert.assertEquals(insertRow, counts.length);
                System.out.println(Arrays.toString(counts));
                System.out.println("counts.length = " + counts.length);
            }

            ps = conn.prepareStatement("select count(*) from arrayBindingTest1");
            ResultSet rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            Assert.assertEquals(insertRow, rs.getInt(1));
            conn.createStatement().execute("delete from arrayBindingTest1");
            insertRow = 0;
            ps = conn.prepareStatement("insert into arrayBindingTest1 values(?,?);");
            // arrayBinding all type are string
            for (int i = 0; i < 20; i++) {
                if (i == insertRow) {
                    ps.setString(1, "errorValue");
                } else {
                    ps.setString(1, Integer.toString(i));
                }
                ps.setString(2, i + "3000");
                ps.addBatch();
            }
            try {
                int ret[] = ps.executeBatch();
            } catch (BatchUpdateException e) {
                int counts[] = e.getUpdateCounts();
                Assert.assertEquals(insertRow, counts.length);
                System.out.println(Arrays.toString(counts));
                System.out.println("counts.length = " + counts.length);
            }

            ps = conn.prepareStatement("select count(*) from arrayBindingTest1");
            rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            Assert.assertEquals(insertRow, rs.getInt(1));
            conn.createStatement().execute("delete  from arrayBindingTest1");

            insertRow = 10;
            int differentTypeIndex = 5;
            ps = conn.prepareStatement("insert into arrayBindingTest1 values(?,?);");
            // |arrayBiding 1 int | arrayBinding 2 string  |
            //                    ^               ^
            // |                diffIndex      errorIndex
            for (int i = 0; i < 20; i++) {
                if (i == insertRow) {
                    ps.setString(1, "errorValue");
                } else {
                    if (i <= differentTypeIndex) {
                        ps.setString(1, Integer.toString(i));
                    } else {
                        ps.setInt(1, i);
                    }
                }
                ps.setString(2, i + "3000");
                ps.addBatch();
            }
            try {
                int ret[] = ps.executeBatch();
            } catch (BatchUpdateException e) {
                int counts[] = e.getUpdateCounts();
                Assert.assertEquals(insertRow, counts.length);
                System.out.println(Arrays.toString(counts));
                System.out.println("counts.length = " + counts.length);
            }

            ps = conn.prepareStatement("select count(*) from arrayBindingTest1");
            rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            Assert.assertEquals(insertRow, rs.getInt(1));
            conn.createStatement().execute("delete  from arrayBindingTest1");

            insertRow = 3;
            differentTypeIndex = 5;
            ps = conn.prepareStatement("insert into arrayBindingTest1 values(?,?);");
            // |arrayBiding 1 int | arrayBinding 2 string  |
            //        ^           ^
            // |  errorIndex    diffIndex
            for (int i = 0; i < 20; i++) {
                if (i == insertRow) {
                    ps.setString(1, "errorValue");
                } else {
                    if (i <= differentTypeIndex) {
                        ps.setString(1, Integer.toString(i));
                    } else {
                        ps.setInt(1, i);
                    }
                }
                ps.setString(2, i + "3000");
                ps.addBatch();
            }
            try {
                int ret[] = ps.executeBatch();
            } catch (BatchUpdateException e) {
                int counts[] = e.getUpdateCounts();
                Assert.assertEquals(insertRow, counts.length);
                System.out.println(Arrays.toString(counts));
                System.out.println("counts.length = " + counts.length);
            }

            ps = conn.prepareStatement("select count(*) from arrayBindingTest1");
            rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            Assert.assertEquals(insertRow, rs.getInt(1));
            conn.createStatement().execute("delete  from arrayBindingTest1");

            insertRow = 10;
            ps = conn.prepareStatement("insert into arrayBindingTest1 values(?,?);");
            for (int i = 0; i < 20; i++) {
                if (i == insertRow) {
                    ps.setString(1, "errorValue");
                } else {
                    ps.setInt(1, i);
                }
                ps.setString(2, i + "3000");
                ps.addBatch();
            }
            try {
                int ret[] = ps.executeBatch();
            } catch (BatchUpdateException e) {
                int counts[] = e.getUpdateCounts();
                Assert.assertEquals(insertRow, counts.length);
                System.out.println(Arrays.toString(counts));
                System.out.println("counts.length = " + counts.length);
            }

            ps = conn.prepareStatement("select count(*) from arrayBindingTest1");
            rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            Assert.assertEquals(insertRow, rs.getInt(1));
            conn.commit();
            conn.setAutoCommit(true);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testgetRowIdLifetime() {
        try {
            DatabaseMetaData dmd = sharedConnection.getMetaData();
            RowIdLifetime out = dmd.getRowIdLifetime();
            Assert.assertTrue(out.equals(RowIdLifetime.ROWID_VALID_FOREVER));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }
}
