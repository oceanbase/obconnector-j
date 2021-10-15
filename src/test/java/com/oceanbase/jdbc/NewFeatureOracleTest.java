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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.sql.*;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class NewFeatureOracleTest extends BaseOracleTest {
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("testcommit", "c1 int");
    }

    @Test
    public void testSocksProxy() {
        try {
            String url = System.getProperty("socksProxyOptions");
            Connection connection = setConnection(url);
            ResultSet resultSet = connection.createStatement().executeQuery("select 1 from dual");
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(1, resultSet.getInt(1));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
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

    // test for setClob
    @org.junit.Test
    public void testPieceClob() {
        Assume.assumeTrue(sharedOptions().useServerPrepStmts);
        try {
            String url = System.getProperty("db3");
            Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            PreparedStatement ps;
            conn.setAutoCommit(true);
            try {
                stmt.execute("drop table clobtest");
            } catch (SQLException e) {
                //                e.printStackTrace();
            }
            stmt.execute("create table clobtest(c1 clob,c2 int,c3 clob,c4 int)");
            ps = conn.prepareStatement("insert into clobtest values(?,?,?,?)");
            String str1 = getRandomString(100);
            String str2 = getRandomString(21);
            Reader reader1 = new StringReader(str1);
            Reader reader2 = new StringReader(str2);
            ps.setClob(1, reader1);
            ps.setClob(3, reader2);
            ps.setInt(2, 100);
            ps.setInt(4, 100);
            ps.execute();

            ps = conn.prepareStatement("select * from clobtest");
            ResultSet rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            assertEquals(str1, rs.getString(1));
            assertEquals(100, rs.getInt(2));
            assertEquals(str2, rs.getString(3));
            assertEquals(100, rs.getInt(4));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    // test for setObject
    @org.junit.Test
    public void testPieceClob2() {
        Assume.assumeTrue(sharedOptions().useServerPrepStmts);
        try {
            String url = System.getProperty("db3");
            Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            PreparedStatement ps;
            conn.setAutoCommit(true);
            try {
                stmt.execute("drop table clobtest");
            } catch (SQLException e) {
                //                e.printStackTrace();c
            }
            stmt.execute("create table clobtest(c1 clob,c2 int,c3 clob,c4 int)");
            ps = conn.prepareStatement("insert into clobtest values(?,?,?,?)");
            String str1 = getRandomString(100);
            String str2 = getRandomString(21);
            Reader reader1 = new StringReader(str1);
            Reader reader2 = new StringReader(str2);
            ps.setObject(1, reader1);
            ps.setObject(3, reader2);
            ps.setInt(2, 100);
            ps.setInt(4, 100);
            ps.execute();

            ps = conn.prepareStatement("select * from clobtest");
            ResultSet rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            assertEquals(str1, rs.getString(1));
            assertEquals(100, rs.getInt(2));
            assertEquals(str2, rs.getString(3));
            assertEquals(100, rs.getInt(4));

        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    // test for setCharacterStream
    @org.junit.Test
    public void testPieceClob3() {
        Assume.assumeTrue(sharedOptions().useServerPrepStmts);
        try {
            String url = System.getProperty("db3");
            Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            PreparedStatement ps;
            conn.setAutoCommit(true);
            try {
                stmt.execute("drop table clobtest");
            } catch (SQLException e) {
                //                e.printStackTrace();c
            }
            stmt.execute("create table clobtest(c1 clob,c2 int,c3 clob,c4 int)");

            ps = conn.prepareStatement("insert into clobtest values(?,?,?,?)");
            String str1 = getRandomString(100);
            String str2 = getRandomString(21);
            Reader reader1 = new StringReader(str1);
            Reader reader2 = new StringReader(str2);
            ps.setCharacterStream(1, reader1);
            ps.setCharacterStream(3, reader2);
            ps.setInt(2, 100);
            ps.setInt(4, 100);
            ps.execute();

            ps = conn.prepareStatement("select * from clobtest");
            ResultSet rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            assertEquals(str1, rs.getString(1));
            assertEquals(100, rs.getInt(2));
            assertEquals(str2, rs.getString(3));
            assertEquals(100, rs.getInt(4));

        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    // test the setBlob(int parameterIndex, InputStream value)
    @org.junit.Test
    public void testPieceBlob() {
        Assume.assumeTrue(sharedOptions().useServerPrepStmts);
        try {
            String url = System.getProperty("db3");
            Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            PreparedStatement ps;
            conn.setAutoCommit(true);
            try {
                stmt.execute("drop table blobtest");
            } catch (SQLException e) {
            }
            stmt.execute("create table blobtest(c1 blob,c2 int,c3 blob,c4 int)");
            ps = conn.prepareStatement("insert into blobtest values(?,?,?,?)");
            byte[] theBlob = { 1, 2, 3, 4, 5, 6 };
            InputStream stream = new ByteArrayInputStream(theBlob);
            InputStream stream2 = new ByteArrayInputStream(theBlob);
            ps.setBlob(1, stream);
            ps.setBlob(3, stream2);
            ps.setInt(2, 100);
            ps.setInt(4, 100);
            ps.execute();
            ps = conn.prepareStatement("select * from blobtest");
            ResultSet rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            InputStream is = rs.getBinaryStream(1);
            int pos = 0;
            int ch;
            while ((ch = is.read()) != -1) {
                assertEquals(theBlob[pos++], ch);
            }
            Assert.assertNotEquals(0, pos);
            Assert.assertEquals(100, rs.getInt(4));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    // test for  setObject(int parameterIndex, java.io.InputStream x) ; x = null
    @org.junit.Test
    public void testPieceBlobNullValue() {
        Assume.assumeTrue(sharedOptions().useServerPrepStmts);
        try {
            String url = System.getProperty("db3");
            Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            PreparedStatement ps;
            conn.setAutoCommit(true);
            try {
                stmt.execute("drop table blobtest");
            } catch (SQLException e) {
            }
            stmt.execute("create table blobtest(c1 blob,c2 int,c3 blob,c4 int)");
            byte[] theBlob = { 1, 2, 3, 4, 5, 6 };
            InputStream stream = null;
            InputStream stream2 = null;
            int pos = 0;
            int ch = 0;
            ResultSet rs;
            InputStream is;
            ps = conn.prepareStatement("insert into blobtest values(?,?,?,?)");
            ps.setObject(1, stream);
            ps.setObject(3, stream2);
            ps.setInt(2, 100);
            ps.setInt(4, 100);
            ps.execute();
            ps = conn.prepareStatement("select * from blobtest");
            rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            is = rs.getAsciiStream(1);
            Assert.assertNull(is);
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    // test for  void setBinaryStream(int parameterIndex, java.io.InputStream x)
    @org.junit.Test
    public void testPieceBlob1() {
        Assume.assumeTrue(sharedOptions().useServerPrepStmts);
        try {
            String url = System.getProperty("db3");
            Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            PreparedStatement ps;
            conn.setAutoCommit(true);
            try {
                stmt.execute("drop table blobtest");
            } catch (SQLException e) {
            }
            stmt.execute("create table blobtest(c1 blob,c2 int,c3 blob,c4 int)");
            byte[] theBlob = { 1, 2, 3, 4, 5, 6 };
            InputStream stream = new ByteArrayInputStream(theBlob);
            InputStream stream2 = new ByteArrayInputStream(theBlob);
            ResultSet rs;
            InputStream is;
            int pos = 0;
            int ch;
            ps = conn.prepareStatement("insert into blobtest values(?,?,?,?)");
            ps.setBinaryStream(1, stream);
            ps.setBinaryStream(3, stream2);
            ps.setInt(2, 100);
            ps.setInt(4, 100);
            ps.execute();
            ps = conn.prepareStatement("select * from blobtest");
            rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            is = rs.getBinaryStream(1);
            pos = 0;
            while ((ch = is.read()) != -1) {
                assertEquals(theBlob[pos++], ch);
            }
            Assert.assertNotEquals(0, pos);
            Assert.assertEquals(100, rs.getInt(4));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    // test for  void setBinaryStream(int parameterIndex, java.io.InputStream x) x = null
    @org.junit.Test
    public void testPieceBlob1NullValue() {
        Assume.assumeTrue(sharedOptions().useServerPrepStmts);
        try {
            String url = System.getProperty("db3");
            Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            PreparedStatement ps;
            conn.setAutoCommit(true);
            try {
                stmt.execute("drop table blobtest");
            } catch (SQLException e) {
            }
            stmt.execute("create table blobtest(c1 blob,c2 int,c3 blob,c4 int)");
            byte[] theBlob = { 1, 2, 3, 4, 5, 6 };
            InputStream stream = null;
            InputStream stream2 = null;
            ResultSet rs;
            InputStream is;
            int pos = 0;
            int ch;
            ps = conn.prepareStatement("insert into blobtest values(?,?,?,?)");
            ps.setBinaryStream(1, stream);
            ps.setBinaryStream(3, stream2);
            ps.setInt(2, 100);
            ps.setInt(4, 100);
            ps.execute();
            ps = conn.prepareStatement("select * from blobtest");
            rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            is = rs.getBinaryStream(1);
            Assert.assertNull(is);
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    // test for  setAsciiStream(int parameterIndex, java.io.InputStream x)
    @org.junit.Test
    public void testPieceBlob3() {
        Assume.assumeTrue(sharedOptions().useServerPrepStmts);
        try {
            String url = System.getProperty("db3");
            Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            PreparedStatement ps;
            conn.setAutoCommit(true);
            try {
                stmt.execute("drop table blobtest");
            } catch (SQLException e) {
            }
            stmt.execute("create table blobtest(c1 blob,c2 int,c3 blob,c4 int)");
            byte[] theBlob = { 1, 2, 3, 4, 5, 6 };
            InputStream stream = new ByteArrayInputStream(theBlob);
            InputStream stream2 = new ByteArrayInputStream(theBlob);
            stream = new ByteArrayInputStream(theBlob);
            stream2 = new ByteArrayInputStream(theBlob);
            int pos = 0;
            int ch = 0;
            ResultSet rs;
            ps = conn.prepareStatement("insert into blobtest values(?,?,?,?)");
            ps.setAsciiStream(1, stream);
            ps.setAsciiStream(3, stream2);
            ps.setInt(2, 100);
            ps.setInt(4, 100);
            ps.execute();
            ps = conn.prepareStatement("select * from blobtest");
            rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            InputStream is = rs.getAsciiStream(1);
            while ((ch = is.read()) != -1) {
                assertEquals(theBlob[pos++], ch);
            }
            Assert.assertNotEquals(0, pos);
            Assert.assertEquals(100, rs.getInt(4));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    // test for  setAsciiStream(int parameterIndex, java.io.InputStream x) x = null
    @org.junit.Test
    public void testPieceBlob3NullValue() {
        Assume.assumeTrue(sharedOptions().useServerPrepStmts);
        try {
            String url = System.getProperty("db3");
            Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            PreparedStatement ps;
            conn.setAutoCommit(true);
            try {
                stmt.execute("drop table blobtest");
            } catch (SQLException e) {
            }
            stmt.execute("create table blobtest(c1 blob,c2 int,c3 blob,c4 int)");
            byte[] theBlob = { 1, 2, 3, 4, 5, 6 };
            InputStream stream = null;
            InputStream stream2 = null;
            int pos = 0;
            int ch = 0;
            ResultSet rs;
            ps = conn.prepareStatement("insert into blobtest values(?,?,?,?)");
            ps.setAsciiStream(1, stream);
            ps.setAsciiStream(3, stream2);
            ps.setInt(2, 100);
            ps.setInt(4, 100);
            ps.execute();
            ps = conn.prepareStatement("select * from blobtest");
            rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            InputStream is = rs.getAsciiStream(1);
            Assert.assertNull(is);
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    // test for  setAsciiStream(int parameterIndex, java.io.InputStream x)
    @org.junit.Test
    public void testPieceBlob4() {
        Assume.assumeTrue(sharedOptions().useServerPrepStmts);
        try {
            String url = System.getProperty("db3");
            Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            PreparedStatement ps;
            conn.setAutoCommit(true);
            try {
                stmt.execute("drop table blobtest");
            } catch (SQLException e) {
            }
            stmt.execute("create table blobtest(c1 blob,c2 int,c3 blob,c4 int)");
            byte[] theBlob = { 1, 2, 3, 4, 5, 6 };
            InputStream stream = new ByteArrayInputStream(theBlob);
            InputStream stream2 = new ByteArrayInputStream(theBlob);
            int pos = 0;
            int ch = 0;
            ResultSet rs;
            InputStream is;
            ps = conn.prepareStatement("insert into blobtest values(?,?,?,?)");
            ps.setAsciiStream(1, stream);
            ps.setAsciiStream(3, stream2);
            ps.setInt(2, 100);
            ps.setInt(4, 100);
            ps.execute();
            ps = conn.prepareStatement("select * from blobtest");
            rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            is = rs.getAsciiStream(1);
            pos = 0;
            while ((ch = is.read()) != -1) {
                assertEquals(theBlob[pos++], ch);
            }
            Assert.assertNotEquals(0, pos);
            Assert.assertEquals(100, rs.getInt(4));
            Assert.assertEquals(100, rs.getInt(4));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    // test for  setAsciiStream(int parameterIndex, java.io.InputStream x)
    @org.junit.Test
    public void testPieceBlob4NullValue() {
        Assume.assumeTrue(sharedOptions().useServerPrepStmts);
        try {
            String url = System.getProperty("db3");
            System.out.println("url = " + url);
            Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            PreparedStatement ps;
            conn.setAutoCommit(true);
            try {
                stmt.execute("drop table blobtest");
            } catch (SQLException e) {
            }
            stmt.execute("create table blobtest(c1 blob,c2 int,c3 blob,c4 int)");
            byte[] theBlob = { 1, 2, 3, 4, 5, 6 };
            InputStream stream = null;
            InputStream stream2 = null;
            int pos = 0;
            int ch = 0;
            ResultSet rs;
            InputStream is;
            ps = conn.prepareStatement("insert into blobtest values(?,?,?,?)");
            ps.setAsciiStream(1, stream);
            ps.setAsciiStream(3, stream2);
            ps.setInt(2, 100);
            ps.setInt(4, 100);
            ps.execute();
            ps = conn.prepareStatement("select * from blobtest");
            rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            is = rs.getAsciiStream(1);
            Assert.assertNull(is);
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    // test for  setObject(int parameterIndex, java.io.InputStream x)
    @org.junit.Test
    public void testPieceBlob5() {
        Assume.assumeTrue(sharedOptions().useServerPrepStmts);
        try {
            String url = System.getProperty("db3");
            Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            PreparedStatement ps;
            conn.setAutoCommit(true);
            try {
                stmt.execute("drop table blobtest");
            } catch (SQLException e) {
            }
            stmt.execute("create table blobtest(c1 blob,c2 int,c3 blob,c4 int)");
            byte[] theBlob = { 1, 2, 3, 4, 5, 6 };
            InputStream stream = new ByteArrayInputStream(theBlob);
            InputStream stream2 = new ByteArrayInputStream(theBlob);
            int pos = 0;
            int ch = 0;
            ResultSet rs;
            InputStream is;
            ps = conn.prepareStatement("insert into blobtest values(?,?,?,?)");
            ps.setObject(1, stream);
            ps.setObject(3, stream2);
            ps.setInt(2, 100);
            ps.setInt(4, 100);
            ps.execute();
            ps = conn.prepareStatement("select * from blobtest");
            rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            is = rs.getAsciiStream(1);
            pos = 0;
            while ((ch = is.read()) != -1) {
                assertEquals(theBlob[pos++], ch);
            }
            Assert.assertNotEquals(0, pos);
            Assert.assertEquals(100, rs.getInt(4));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    // test for  setObject(int parameterIndex, java.io.InputStream x) x = null
    @org.junit.Test
    public void testPieceBlob5Nullalue() {
        Assume.assumeTrue(sharedOptions().useServerPrepStmts);
        try {
            String url = System.getProperty("db3");
            Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            PreparedStatement ps;
            conn.setAutoCommit(true);
            try {
                stmt.execute("drop table blobtest");
            } catch (SQLException e) {
            }
            stmt.execute("create table blobtest(c1 blob,c2 int,c3 blob,c4 int)");
            byte[] theBlob = { 1, 2, 3, 4, 5, 6 };
            InputStream stream = null;
            InputStream stream2 = null;
            int pos = 0;
            int ch = 0;
            ResultSet rs;
            InputStream is;
            ps = conn.prepareStatement("insert into blobtest values(?,?,?,?)");
            ps.setObject(1, stream);
            ps.setObject(3, stream2);
            ps.setInt(2, 100);
            ps.setInt(4, 100);
            ps.execute();
            ps = conn.prepareStatement("select * from blobtest");
            rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            is = rs.getAsciiStream(1);
            Assert.assertNull(is);
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

}
