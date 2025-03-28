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

import java.io.*;
import java.sql.*;
import java.util.Arrays;

import org.junit.*;

import com.oceanbase.jdbc.internal.util.StringCacheUtil;

import static org.junit.Assert.*;

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
            Assert.assertEquals("sys", catalog);
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

    @Test
    public void aone50629254() throws Exception {
        Connection conn = setConnection("&rewriteBatchedStatements=true");
        createTable("t_json", "c JSON, g INT GENERATED ALWAYS AS (c->\"$.id\"), INDEX i (g)");
        PreparedStatement ps = conn.prepareStatement("insert into t_json (c) values (?)");
        ps.setString(1, "{\"id\": \"1\", \"name\": \"AAA\"}");
        ps.addBatch();
        ps.setString(1, "{\"id\": \"2\", \"name\": \"BBB\"}");
        ps.addBatch();
        ps.setString(1, "{\"id\": \"3\", \"name\": \"CCC\"}");
        ps.addBatch();
        ps.executeBatch();

        ps = conn.prepareStatement("select c->>\"$.name\" from t_json where g>1");
        ResultSet rs = ps.executeQuery();
        Assert.assertTrue(rs.next());
        Assert.assertEquals("BBB", rs.getString(1));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("CCC", rs.getString(1));
    }

    @Test
    public void testLoadDataLocalFile() throws SQLException, IOException {
        File file = null;
        try {
            file = new File("./validateInfile_data.txt");
            file.deleteOnExit();
            if (!file.exists()){
                assertTrue(file.createNewFile());
            }
            try (BufferedWriter bw = new BufferedWriter(new FileWriter(file))){
                bw.write("1,John,25,123 Main St");
                bw.newLine();
                bw.write("2,Jane,30,456 Elm St");
                bw.newLine();
                bw.write("3,Michael,35,789 Oak St");
                bw.newLine();
            }
            createTable("my_table", "id INT, name VARCHAR(50), age INT, salary DECIMAL(10,2)");
            doTestLoadDataLocalFile(setConnection("&allowLoadLocalInfile=true&useOceanBaseProtocolV20=false"));
            doTestLoadDataLocalFile(setConnection("&allowLoadLocalInfile=true&useOceanBaseProtocolV20=true"));
        } finally {
            if (file != null && file.exists()) {
                file.delete();
            }
        }

    }

    public void doTestLoadDataLocalFile(Connection conn) throws SQLException {
        String tableName = "my_table";
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("delete from " + tableName);
        stmt.execute("LOAD DATA /*+parallel(8)*/ LOCAL INFILE './validateInfile_data.txt'/* This is a comment */ INTO TABLE /* This is a comment */" + tableName
                + " FIELDS /* This is a comment */ TERMINATED BY ','" + " LINES TERMINATED BY '\\n'");
        Assert.assertEquals("Records: 3  Deleted: 0  Skipped: 0  Warnings: 0",
                ((OceanBaseStatement) stmt).getServerInfo());
        ResultSet rs = stmt.executeQuery("select * from " + tableName);
        assertTrue(rs.next());
        Assert.assertEquals("1 John 25",rs.getString("id") + " " + rs.getString("name") + " " + rs.getInt("age"));
        assertTrue(rs.next());
        Assert.assertEquals("2 Jane 30",rs.getString("id") + " " + rs.getString("name") + " " + rs.getInt("age"));
        assertTrue(rs.next());
        Assert.assertEquals("3 Michael 35",rs.getString("id") + " " + rs.getString("name") + " " + rs.getInt("age"));

        stmt.executeUpdate("delete from " + tableName);
        PreparedStatement pstmt = conn.prepareStatement("LOAD DATA /*+parallel(8)*/ LOCAL INFILE ?/* This is a comment */ INTO TABLE /* This is a comment */" + tableName
                + " FIELDS /* This is a comment */ TERMINATED BY ','" + " LINES TERMINATED BY '\\n'");
        pstmt.setString(1,"./validateInfile_data.txt");
        pstmt.execute();
        rs = stmt.executeQuery("select * from " + tableName);
        assertTrue(rs.next());
        Assert.assertEquals("1 John 25",rs.getString("id") + " " + rs.getString("name") + " " + rs.getInt("age"));
        assertTrue(rs.next());
        Assert.assertEquals("2 Jane 30",rs.getString("id") + " " + rs.getString("name") + " " + rs.getInt("age"));
        assertTrue(rs.next());
        Assert.assertEquals("3 Michael 35",rs.getString("id") + " " + rs.getString("name") + " " + rs.getInt("age"));

        stmt.close();
        conn.close();
    }

    @Test
    public void testBlobDataRetrievalForDima2024062200102856563() throws Exception {
        Connection conn = setConnection("&emulateLocators=true");
        Statement stmt = conn.createStatement();
        createTable("test_blob", "id varchar(10), id_card varchar(20), name longblob, primary key(id, id_card)");
        stmt.executeUpdate("insert into test_blob (id, id_card, name) values (1, '123456', 'abcdefg')");

        ResultSet rs = stmt.executeQuery("SELECT ID, id_card, 'NAME' AS BLOB_DATA from test_blob");
        Assert.assertTrue(rs.next());
        java.sql.Blob blob = rs.getBlob("BLOB_DATA");

        Assert.assertEquals("abcdefg", new String(blob.getBytes(1, 10)));
        Assert.assertEquals(7, blob.length());
        Assert.assertEquals(3, blob.position("cde".getBytes(), 1));

        java.sql.Blob blob1 = conn.createBlob();
        blob1.setBytes(1, "efg".getBytes());
        try {
            blob.position(blob1, 1);
            fail();
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("position"));
        }

        try {
            blob.setBinaryStream(1);
            fail();
        } catch (SQLException e) {
            Assert.assertTrue(e instanceof SQLFeatureNotSupportedException);
        }

        blob.setBytes(4, "xyz".getBytes());
        Assert.assertEquals("abcxyzg", new String(blob.getBytes(1, 10)));

        blob.setBytes(4, "def".getBytes(), 1, 2);
        Assert.assertEquals("abcefzg", new String(blob.getBytes(1, 10)));

        blob.truncate(3);
        Assert.assertEquals("abc", new String(blob.getBytes(1, 10)));

        blob.free();
        try {
            blob.getBytes(1, 10);
            fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof NullPointerException);
        }
        rs.close();

        // test for locatorFetchBufferSize
        conn = setConnection("&emulateLocators=true&locatorFetchBufferSize=3");
        Statement statement = conn.createStatement();
        ResultSet resultSet = statement.executeQuery("select id, id_card, 'NAME' AS BLOB_DATA from test_blob");
        Assert.assertTrue(resultSet.next());

        java.sql.Blob blob2 = resultSet.getBlob("BLOB_DATA");
        blob2.setBytes(1, "abcdefg".getBytes());
        Assert.assertEquals("abcdefg", new String(blob2.getBytes(1, 10)));

        try {
            blob2.getBinaryStream(1, 3);
            fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof NullPointerException);
        }

        InputStream stream = blob2.getBinaryStream();
        Assert.assertEquals('a', stream.read());
        Assert.assertEquals('b', stream.read());

        byte[] bytes = new byte[4];
        stream.read(bytes);

        byte[] bytes2 = new byte[4];
        bytes2[0] = 'c';
        Assert.assertTrue(Arrays.equals(bytes2, bytes));

        Assert.assertEquals('d', stream.read());

        stream.read(bytes, 3, 1);
        bytes2[3] = 'e';
        Assert.assertTrue(Arrays.equals(bytes2, bytes));
        resultSet.close();
    }
 
    @Test
    public void testInsertSetForDima2024072400103903857() throws SQLException {
        Connection conn = setConnection("&rewriteBatchedStatements=true");
        for (int i = 0; i < 2; i++) {
            if (i == 1) {
                conn = setConnection("&rewriteBatchedStatements=true&useServerPrepStmts=true");
            }
            Statement stmt = conn.createStatement();
            stmt.execute("alter system flush plan cache global");
            createTable("tt9", "c1 int primary key auto_increment, c2 varchar(20) ");
            PreparedStatement pstmt = conn.prepareStatement("insert into tt9 set c2 = ? ");
            pstmt.setString(1, "aaa");
            pstmt.addBatch();
            pstmt.setString(1, "bbb");
            pstmt.addBatch();
            pstmt.setString(1, "ccc");
            pstmt.addBatch();
            pstmt.executeBatch();
            ResultSet rs = stmt.executeQuery("select c2 from tt9");
            rs.next();
            assertEquals("aaa", rs.getString(1));
            rs.next();
            assertEquals("bbb", rs.getString(1));
            rs.next();
            assertEquals("ccc", rs.getString(1));
        }
    }

}
