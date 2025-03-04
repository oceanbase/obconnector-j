/**
 *  OceanBase Client for Java
 *
 *  Copyright (c) 2012-2014 Monty Program Ab.
 *  Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *  Copyright (c) 2021 OceanBase.
 *
 *  This library is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU Lesser General Public License as published by the Free
 *  Software Foundation; either version 2.1 of the License, or (at your option)
 *  any later version.
 *
 *  This library is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License along
 *  with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 *  This particular MariaDB Client for Java file is work
 *  derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 *  the following copyright and notice provisions:
 *
 *  Copyright (c) 2009-2011, Marcus Eriksson
 *
 *  Redistribution and use in source and binary forms, with or without modification,
 *  are permitted provided that the following conditions are met:
 *  Redistributions of source code must retain the above copyright notice, this list
 *  of conditions and the following disclaimer.
 *
 *  Redistributions in binary form must reproduce the above copyright notice, this
 *  list of conditions and the following disclaimer in the documentation and/or
 *  other materials provided with the distribution.
 *
 *  Neither the name of the driver nor the names of its contributors may not be
 *  used to endorse or promote products derived from this software without specific
 *  prior written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 *  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 *  IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 *  INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 *  NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 *  PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 *  WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 *  OF SUCH DAMAGE.
 */
package com.oceanbase.jdbc;

import static org.junit.Assert.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ClobOracleTest extends BaseOracleTest {

    private final byte[] bytes = "abcde🙏fgh".getBytes(StandardCharsets.UTF_8);
    private final String str1  = "以后怎么教育对于然后到了.网站阅读系列那个.这些时候这里运行.";
    private final String str2  = "jdbc characterEncoding test.";
    private final String str3  = "手机您的全部.";

    @BeforeClass
    public static void initClass() throws SQLException {
        createTable("testGBK", "c1 clob,c2 clob,c3 varchar(200)");
        createTable("testGB18030", "c1 clob,c2 clob,c3 varchar(200)");
        createTable("testUTF8", "c1 clob,c2 clob,c3 varchar(200)");
    }

    @Test
    public void length() throws SQLException {
        com.oceanbase.jdbc.Clob clob = new com.oceanbase.jdbc.Clob(bytes);
        assertEquals(10, clob.length());

        com.oceanbase.jdbc.Clob clob2 = new com.oceanbase.jdbc.Clob(bytes, 2, 3);
        assertEquals(3, clob2.length());
    }

    @Test
    public void getSubString() throws SQLException {
        com.oceanbase.jdbc.Clob clob = new com.oceanbase.jdbc.Clob(bytes);
        assertEquals("abcde🙏", clob.getSubString(1, 7));
        assertEquals("abcde🙏fgh", clob.getSubString(1, 20));
        assertEquals("abcde🙏fgh", clob.getSubString(1, (int) clob.length()));
        assertEquals("ab", clob.getSubString(1, 2));
        assertEquals("🙏", clob.getSubString(6, 2));

        com.oceanbase.jdbc.Clob clob2 = new com.oceanbase.jdbc.Clob(bytes, 4, 6);

        assertEquals("e🙏f", clob2.getSubString(1, 20));
        assertEquals("🙏f", clob2.getSubString(2, 3));

        try {
            clob2.getSubString(0, 3);
            fail("must have thrown exception, min pos is 1");
        } catch (SQLException sqle) {
            // normal exception
        }
    }

    @Test
    public void getCharacterStream() throws SQLException {
        com.oceanbase.jdbc.Clob clob = new com.oceanbase.jdbc.Clob(bytes);
        assureReaderEqual("abcde🙏", clob.getCharacterStream(1, 7));
        assureReaderEqual("abcde🙏fgh", clob.getCharacterStream(1, 10));
        try {
            assureReaderEqual("abcde🙏fgh", clob.getCharacterStream(1, 20));
            fail("must have throw exception, length > to number of characters");
        } catch (SQLException sqle) {
            // normal error
        }
        assureReaderEqual("bcde🙏", clob.getCharacterStream(2, 7));

        com.oceanbase.jdbc.Clob clob2 = new com.oceanbase.jdbc.Clob(bytes, 2, 9);
        assureReaderEqual("cde🙏fg", clob2.getCharacterStream(1, 7));
        try {
            assureReaderEqual("cde🙏fg", clob2.getCharacterStream(1, 20));
            fail("must have throw exception, length > to number of characters");
        } catch (SQLException sqle) {
            // normal error
        }

        assureReaderEqual("e🙏f", clob2.getCharacterStream(3, 5));
    }

    private void assureReaderEqual(String expectedStr, Reader reader) {
        try {
            char[] expected = expectedStr.toCharArray();
            char[] readArr = new char[expected.length];
            assertEquals(expected.length, reader.read(readArr));
            assertArrayEquals(expected, readArr);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            fail();
        }
    }

    @Test
    public void setCharacterStream() throws SQLException, IOException {
        final byte[] bytes = "abcde🙏fgh".getBytes(StandardCharsets.UTF_8);
        com.oceanbase.jdbc.Clob clob = new com.oceanbase.jdbc.Clob(bytes);
        assureReaderEqual("abcde🙏", clob.getCharacterStream(1, 7));

        Writer writer = clob.setCharacterStream(2);
        writer.write("tuvxyz", 2, 3);
        writer.flush();
        assertEquals("avxye🙏", clob.getSubString(1, 7));

        clob = new com.oceanbase.jdbc.Clob(bytes);

        writer = clob.setCharacterStream(2);
        writer.write("1234567890lmnopqrstu", 1, 19);
        writer.flush();
        assertEquals("a234567890lmnopqrstu", clob.getSubString(1, 100));
    }

    @Test
    public void position() {
        try {
            Clob clob = new Clob(bytes);
            assertEquals(4, clob.position("de", 2));

            clob = new Clob(bytes, 2, 10);
            assertEquals(4, clob.position("🙏", 2));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void setString() throws SQLException {
        final byte[] bytes = "abcde🙏fgh".getBytes(StandardCharsets.UTF_8);
        com.oceanbase.jdbc.Clob clob = new com.oceanbase.jdbc.Clob(bytes);
        assureReaderEqual("abcde🙏", clob.getCharacterStream(1, 7));
        clob.setString(2, "zuv");
        assertEquals("azuve🙏", clob.getSubString(1, 7));
        clob.setString(9, "zzz");
        assertEquals("azuve🙏fgzzz", clob.getSubString(1, 12));

        clob = new com.oceanbase.jdbc.Clob("abcde🙏fgh".getBytes(StandardCharsets.UTF_8), 2, 9);
        assureReaderEqual("cde🙏fg", clob.getCharacterStream(1, 7));
        assertEquals("cde🙏fg", clob.getSubString(1, 7));

        clob.setString(2, "zg");
        assertEquals("czg🙏f", clob.getSubString(1, 6));
        clob.setString(7, "zzz");
        String ss = clob.getSubString(1, 12);
        assertEquals("czg🙏fgzzz", clob.getSubString(1, 12));
    }

    @Test
    public void setAsciiStream() throws SQLException, IOException {
        final byte[] bytes = "abcde🙏fgh".getBytes(StandardCharsets.UTF_8);
        com.oceanbase.jdbc.Clob clob = new com.oceanbase.jdbc.Clob(bytes);
        assureReaderEqual("abcde🙏", clob.getCharacterStream(1, 7));

        OutputStream stream = clob.setAsciiStream(2);
        stream.write("tuvxyz".getBytes(), 2, 3);
        stream.flush();

        assertEquals("avxye🙏", clob.getSubString(1, 7));

        clob = new com.oceanbase.jdbc.Clob(bytes);

        stream = clob.setAsciiStream(2);
        stream.write("1234567890lmnopqrstu".getBytes(), 1, 19);
        stream.flush();
        assertEquals("a234567890lmnopqrstu", clob.getSubString(1, 100));
    }

    @Test
    public void setBinaryStream() throws SQLException, IOException {
        final byte[] bytes = "abcde🙏fgh".getBytes(StandardCharsets.UTF_8);
        final byte[] otherBytes = new byte[] { 10, 11, 12, 13 };

        com.oceanbase.jdbc.Clob blob = new com.oceanbase.jdbc.Clob(new byte[] { 0, 1, 2, 3, 4, 5 });
        OutputStream out = blob.setBinaryStream(2);
        out.write(otherBytes);
        assertArrayEquals(new byte[] { 0, 10, 11, 12, 13, 5 }, blob.getBytes(1, 6));

        com.oceanbase.jdbc.Clob blob2 = new com.oceanbase.jdbc.Clob(new byte[] { 0, 1, 2, 3, 4, 5 });
        OutputStream out2 = blob2.setBinaryStream(4);
        out2.write(otherBytes);
        assertArrayEquals(new byte[] { 0, 1, 2, 10, 11, 12, 13 }, blob2.getBytes(1, 7));

        com.oceanbase.jdbc.Clob blob3 = new com.oceanbase.jdbc.Clob(
            new byte[] { 0, 1, 2, 3, 4, 5 }, 2, 3);
        OutputStream out3 = blob3.setBinaryStream(2);
        out3.write(otherBytes);
        assertArrayEquals(new byte[] { 2, 10, 11, 12, 13 }, blob3.getBytes(1, 7));

        com.oceanbase.jdbc.Clob blob4 = new com.oceanbase.jdbc.Clob(
            new byte[] { 0, 1, 2, 3, 4, 5 }, 2, 3);
        OutputStream out4 = blob4.setBinaryStream(4);
        out4.write(otherBytes);
        assertArrayEquals(new byte[] { 2, 3, 4, 10, 11, 12 }, blob4.getBytes(1, 6));

        try {
            com.oceanbase.jdbc.Clob blob5 = new com.oceanbase.jdbc.Clob(new byte[] { 0, 1, 2, 3, 4,
                    5 }, 2, 3);
            blob5.setBinaryStream(0);
        } catch (SQLException sqle) {
            // normal exception
        }
    }

    @Test
    public void setBinaryStreamOffset() throws SQLException, IOException {
        final byte[] bytes = "abcde🙏fgh".getBytes(StandardCharsets.UTF_8);
        final byte[] otherBytes = new byte[] { 10, 11, 12, 13 };

        com.oceanbase.jdbc.Clob blob = new com.oceanbase.jdbc.Clob(new byte[] { 0, 1, 2, 3, 4, 5 });
        OutputStream out = blob.setBinaryStream(2);
        out.write(otherBytes, 2, 3);
        assertArrayEquals(new byte[] { 0, 12, 13, 3, 4, 5 }, blob.getBytes(1, 6));

        com.oceanbase.jdbc.Clob blob2 = new com.oceanbase.jdbc.Clob(new byte[] { 0, 1, 2, 3, 4, 5 });
        OutputStream out2 = blob2.setBinaryStream(4);
        out2.write(otherBytes, 3, 2);
        assertArrayEquals(new byte[] { 0, 1, 2, 13, 4, 5 }, blob2.getBytes(1, 7));

        com.oceanbase.jdbc.Clob blob3 = new com.oceanbase.jdbc.Clob(
            new byte[] { 0, 1, 2, 3, 4, 5 }, 2, 4);
        OutputStream out3 = blob3.setBinaryStream(2);
        out3.write(otherBytes, 2, 3);
        assertArrayEquals(new byte[] { 2, 12, 13, 5 }, blob3.getBytes(1, 7));

        com.oceanbase.jdbc.Clob blob4 = new com.oceanbase.jdbc.Clob(
            new byte[] { 0, 1, 2, 3, 4, 5 }, 2, 3);
        OutputStream out4 = blob4.setBinaryStream(4);
        out4.write(otherBytes, 2, 2);
        assertArrayEquals(new byte[] { 2, 3, 4, 12, 13 }, blob4.getBytes(1, 6));

        com.oceanbase.jdbc.Clob blob5 = new com.oceanbase.jdbc.Clob(
            new byte[] { 0, 1, 2, 3, 4, 5 }, 2, 3);
        OutputStream out5 = blob5.setBinaryStream(4);
        out5.write(otherBytes, 2, 20);
        assertArrayEquals(new byte[] { 2, 3, 4, 12, 13 }, blob5.getBytes(1, 6));
    }

    @Test
    public void truncate() throws SQLException {
        com.oceanbase.jdbc.Clob clob = new com.oceanbase.jdbc.Clob(bytes);
        clob.truncate(20);
        assertEquals("abcde🙏f", clob.getSubString(1, 8));
        clob.truncate(8);
        assertEquals("abcde🙏f", clob.getSubString(1, 8));
        assertEquals("abcde🙏", clob.getSubString(1, 7));
        clob.truncate(7);
        assertEquals("abcde🙏", clob.getSubString(1, 8));
        clob.truncate(6);
        assertEquals("abcde�", clob.getSubString(1, 8));
        clob.truncate(4);
        assertEquals("abcd", clob.getSubString(1, 7));
        clob.truncate(0);
        assertEquals("", clob.getSubString(1, 7));

        com.oceanbase.jdbc.Clob clob2 = new com.oceanbase.jdbc.Clob(
            "abcde🙏fgh".getBytes(StandardCharsets.UTF_8), 2, 8);
        clob2.truncate(20);
        assertEquals("cde🙏f", clob2.getSubString(1, 8));
        clob2.truncate(6);
        assertEquals("cde🙏f", clob2.getSubString(1, 8));
        clob2.truncate(5);
        assertEquals("cde🙏", clob2.getSubString(1, 8));
        clob2.truncate(4);
        assertEquals("cde�", clob2.getSubString(1, 8));
        clob2.truncate(0);
        assertEquals("", clob2.getSubString(1, 7));
    }

    @Test
    public void free() {
        com.oceanbase.jdbc.Clob blob = new com.oceanbase.jdbc.Clob(bytes);
        blob.free();
        assertEquals(0, blob.length);
    }

    @Test
  @SuppressWarnings("checkstyle:AvoidEscapedUnicodeCharacters")
  public void clobLength() throws Exception {
    Statement stmt = sharedConnection.createStatement();
    try (ResultSet rs =
        stmt.executeQuery("SELECT 'ab$c', 'ab¢c', 'abहc', 'ab\uD801\uDC37c', 'ab𐍈c' from dual")) {
      while (rs.next()) {

        java.sql.Clob clob1 = rs.getClob(1);
        assertEquals(4, clob1.length());

        java.sql.Clob clob2 = rs.getClob(2);
        assertEquals(4, clob2.length());

        java.sql.Clob clob3 = rs.getClob(3);
        assertEquals(4, clob3.length());

        java.sql.Clob clob4 = rs.getClob(4);
        assertEquals(5, clob4.length());

        java.sql.Clob clob5 = rs.getClob(5);
        assertEquals(5, clob5.length());

        clob1.truncate(3);
        clob2.truncate(3);
        clob3.truncate(3);
        clob4.truncate(3);
        clob5.truncate(3);

        assertEquals(3, clob1.length());
        assertEquals(3, clob2.length());
        assertEquals(3, clob3.length());
        assertEquals(3, clob4.length());
        assertEquals(3, clob5.length());

        assertEquals("ab$", clob1.getSubString(1, 3));
        assertEquals("ab¢", clob2.getSubString(1, 3));
        assertEquals("abह", clob3.getSubString(1, 3));
        assertEquals("ab�", clob4.getSubString(1, 3));
        assertEquals("ab�", clob5.getSubString(1, 3));
      }
    }
  }

    @Test
    public void clobSetStringGBK() {
        try {
            Connection connection = setConnection("&characterEncoding=GBK");
            PreparedStatement stmt = connection
                .prepareStatement("insert into testGBK values(?,?,?)");
            stmt.setString(1, str1);
            stmt.setString(2, str2);
            stmt.setString(3, str3);
            stmt.addBatch();
            stmt.executeBatch();
            ResultSet rs = connection.createStatement().executeQuery("select * from testGBK");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(str1, rs.getString(1));
            Assert.assertEquals(str2, rs.getString(2));
            Assert.assertEquals(str3, rs.getString(3));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }

    }

    @Test
    public void clobSetStringGB18030() {
        try {
            Connection connection = setConnection("&characterEncoding=GB18030");
            PreparedStatement stmt = connection
                .prepareStatement("insert into testGB18030 values(?,?,?)");
            stmt.setString(1, str1);
            stmt.setString(2, str2);
            stmt.setString(3, str3);
            stmt.addBatch();
            stmt.executeBatch();
            ResultSet rs = connection.createStatement().executeQuery("select * from testGB18030");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(str1, rs.getString(1));
            Assert.assertEquals(str2, rs.getString(2));
            Assert.assertEquals(str3, rs.getString(3));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void clobSetStringUTF8() {
        try {
            Connection connection = setConnection("&characterEncoding=UTF8");
            PreparedStatement stmt = connection
                .prepareStatement("insert into testUTF8 values(?,?,?)");
            stmt.setString(1, str1);
            stmt.setString(2, str2);
            stmt.setString(3, str3);
            stmt.addBatch();
            stmt.executeBatch();
            ResultSet rs = connection.createStatement().executeQuery("select * from testUTF8");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(str1, rs.getString(1));
            Assert.assertEquals(str2, rs.getString(2));
            Assert.assertEquals(str3, rs.getString(3));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();

        }
    }

    @Test
    public void testLobLocatorV2() {
        try {
            Connection conn = setConnectionOrigin("?useServerPrepStmts=true&usePieceData=true");
            String clobTest3 = "t_clob" + getRandomString(5);

            createTable(clobTest3, "id int, c1 clob, c2 clob, c3 clob, c4 clob, c5 clob, c6 clob");
            PreparedStatement ps = conn
                .prepareStatement("insert into " + clobTest3
                                  + " values(1, ?, ?, ?, ?, ?, ?) returning c3, c6 into ?, ?");

            ps.setCharacterStream(1, null);
            ps.setClob(2, (Clob) null);
            ps.setCharacterStream(3, new StringReader("")); //(CLOB)null
            ps.setClob(4, Clob.getEmptyCLOB());
            ps.setCharacterStream(5, new StringReader("1234abcd奥星贝斯")); // oracle sends request
            java.sql.Clob clob = conn.createClob(); // oracle sends request
            Writer writer = clob.setCharacterStream(1);
            for (int i = 0; i < 1024; i++) {
                writer.write("abcde");
            }
            writer.flush(); // oracle sends request
            ps.setClob(6, clob);
            ((JDBC4ServerPreparedStatement) ps).registerReturnParameter(7, Types.CLOB);
            ((JDBC4ServerPreparedStatement) ps).registerReturnParameter(8, Types.CLOB);
            ps.execute();

            ps = conn.prepareStatement("select c1, c2, c3, c4, c5, c6 from " + clobTest3);
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals(null, rs.getString(1));
            assertEquals(null, rs.getString(2));
            assertEquals(null, rs.getString(3));
            assertEquals("", rs.getString(4));
            assertEquals("1234abcd奥星贝斯", rs.getString(5));
            String str = rs.getString(6);
            assertEquals(5120, str.length());
            assertTrue(str.startsWith("abcde"));

            java.sql.Clob c5 = rs.getClob(5);
            java.sql.Clob c6 = rs.getClob(6);
            ps = conn.prepareStatement("update " + clobTest3 + " set c1 = ?, c2 = ? where id = 1");
            ps.setClob(1, c5);
            ps.setClob(2, c6);
            ps.execute();

            conn.setAutoCommit(false);
            ps = conn.prepareStatement("select c1, c2, c3, c4, c5, c6 from " + clobTest3
                                       + " where id = 1 for update");
            rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals("1234abcd奥星贝斯", rs.getString(1));
            str = rs.getString(2);
            assertEquals(5120, str.length());
            assertTrue(str.startsWith("abcde"));
            assertEquals(null, rs.getString(3));
            assertEquals("", rs.getString(4));
            assertEquals("1234abcd奥星贝斯", rs.getString(5));
            str = rs.getString(6);
            assertEquals(5120, str.length());
            assertTrue(str.startsWith("abcde"));

            java.sql.Clob clob5 = rs.getClob(5);
            java.sql.Clob clob6 = rs.getClob(6);
            clob5.setString(1, "认真工作，live happily!");
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 10240; i++) {
                sb.append("1234abcd");
            }
            clob6.setString(1, sb.toString());

            conn.commit();
            conn.setAutoCommit(true);
            ps = conn.prepareStatement("select c1, c2, c3, c4, c5, c6 from " + clobTest3);
            rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals("1234abcd奥星贝斯", rs.getString(1));
            str = rs.getString(2);
            assertEquals(5120, str.length());
            assertTrue(str.startsWith("abcde"));
            assertEquals(null, rs.getString(3));
            assertEquals("", rs.getString(4));
            assertEquals("认真工作，live happily!", rs.getString(5));
            str = rs.getString(6);
            assertEquals(sb.toString(), str);
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void fix48575083() {
        try {
            Connection conn = sharedConnection;
            //            Connection conn = setConnectionOrigin("?useServerPrepStmts=false");
            String clobTest3 = "t_clob" + getRandomString(5);

            createTable(clobTest3, "c1 clob");
            PreparedStatement ps = conn.prepareStatement("insert into " + clobTest3 + " values(?)");
            int size = 1024 * 65;
            java.sql.Clob clob = conn.createClob();
            Writer writer = clob.setCharacterStream(1);
            for (int i = 0; i < size; i++) {
                writer.write("a");
            }
            writer.flush();
            ps.setClob(1, clob);
            ps.execute();

            ps = conn.prepareStatement("select c1 from " + clobTest3);
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());

            java.sql.Clob var1 = rs.getClob(1);
            long len1 = var1.length();
            String str1 = var1.toString();
            //                System.out.println("var1.length: " + len1);
            //                System.out.println("var1.toString: " + str1);
            Assert.assertEquals(size, len1);

            String var2 = rs.getString(1);
            long len2 = var2.length();
            String str2 = var2.toString();
            //                System.out.println("var2.length: " + len2);
            //                System.out.println("var2.toString: " + str2);
            Assert.assertEquals(size, len2);

            java.sql.Clob var3 = rs.getObject(1, java.sql.Clob.class);
            long len3 = var3.length();
            String str3 = var3.toString();
            //                System.out.println("var3.length: " + len3);
            //                System.out.println("var3.toString: " + str3);
            Assert.assertEquals(size, len3);

            String var4 = rs.getObject(1, String.class);
            long len4 = var4.length();
            String str4 = var4.toString();
            //                System.out.println("var4.length: " + len4);
            //                System.out.println("var4.toString: " + str4);
            Assert.assertEquals(size, len4);

            java.sql.Clob var5 = (java.sql.Clob) rs.getObject(1);
            long len5 = var5.length();
            String str5 = var5.toString();
            //                System.out.println("var5.length: " + len5);
            //                System.out.println("var5.toString: " + str5);
            Assert.assertEquals(size, len5);

            try {
                String var6 = (String) rs.getObject(1);
                long len6 = var6.length();
                String str6 = var6.toString();
                //                System.out.println("var6.length: " + len6);
                //                System.out.println("var6.toString: " + str6);
                Assert.assertEquals(size, len6);
            } catch (Exception e) {
                e.printStackTrace();
                Assert.assertTrue(e.getMessage()
                    .contains("Clob cannot be cast to java.lang.String"));
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void fix48650339() {
        try {
            Connection conn = sharedConnection;
            String clobTest = "t_clob";

            createTable(clobTest, "c1 clob, c2 clob");
            PreparedStatement ps = conn
                .prepareStatement("insert into " + clobTest + " values(?,?)");
            int size1 = 1024 * 64;
            int size2 = 1024 * 65;
            java.sql.Clob clob = conn.createClob();
            java.sql.Clob clob1 = conn.createClob();
            Writer writer = clob.setCharacterStream(1);
            for (int i = 0; i < size1; i++) {
                writer.write("a");
            }
            writer.flush();
            ps.setClob(1, clob);
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < size2; i++) {
                sb.append("b");
            }
            clob1.setString(1, sb.toString());
            ps.setClob(2, clob1);
            ps.execute();

            conn.setAutoCommit(false);
            ps = conn.prepareStatement("select c1,c2 from " + clobTest + " for update");
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());

            java.sql.Clob clob2 = rs.getClob(1);
            java.sql.Clob clob3 = rs.getClob(2);
            clob2.setString(1, "abc中文ABC");
            sb = new StringBuilder();
            for (int i = 0; i < 1024 * 10; i++) {
                sb.append("abc中文ABC");
            }
            clob3.setString(1, sb.toString());
            conn.commit();
            conn.setAutoCommit(true);

            ps = conn.prepareStatement("select c1,c2 from " + clobTest);
            rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals(size1, rs.getString(1).length());
            assertTrue(rs.getString(1).startsWith("abc中文ABCaaa"));
            Assert.assertEquals(65536, rs.getClob(1).length());

            String str = rs.getString(2);
            assertEquals(81920, str.length());
            assertTrue(str.startsWith("abc中文ABC"));
            Assert.assertEquals(81920, rs.getClob(2).length());
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void aone49521917() throws SQLException {
        Connection conn = setConnectionOrigin("?useUnicode=true&characterEncoding=utf8&socketTimeout=1800000&connectTimeout=180000"
                                              + "&usePipelineAuth=false&verifyServerCertificate=false&useSSL=false&allowMultiQueries=true&rewriteBatchedStatements=true"
                                              + "&useLocalSessionState=true&useCompression=true&noDatetimeStringSync=true&zeroDateTimeBehavior=convertToNull"
                                              + "&emulateUnsupportedPstmts=true&useServerPrepStmts=true");
        Statement stmt = conn.createStatement();
        createProcedure(
            "PRO_ZEYANG_220223",
            "(t in varchar2, v_result out number) is\n"
                    + "\n"
                    + "count_ number := 0; --用于判断是否有模型\n"
                    + "subjectName varchar2(100) := '财务总账余额等于支付未清算报文明细汇总';\n"
                    + "bb_time date;\n"
                    + "subjectNumber varchar2(100); --BEPS对应 40700030025 CBPS对应 40700030032 IBPS对应 40700030037\n"
                    + "branchId varchar2(100);\n" + "current_id varchar2(100);\n"
                    + "orgrank int;\n" + "bbsId varchar2(100);\n" + "err_code varchar2(2000);\n"
                    + "err_msg varchar2(2000);\n" + "brno varchar2(10);\n" + "tyes varchar2(10);\n"
                    + "--r number :=0;\n" + "cursor c_admission is\n"
                    + "select t3.bbs_subjectnumber bbs_subjectnumber,\n"
                    + "t3.bbs_frequency bbs_frequency,\n" + "t4.bbs_orgrank,\n"
                    + "t4.bbs_orgrank_name,\n" + "t4.bbs_currid,\n" + "t4.bbs_currid_name,\n"
                    + "t4.bbs_orgid,\n" + "t4.bbs_id\n" + "from biz_accountcheck_model t1\n"
                    + "left join biz_pattern_glacc t2\n"
                    + "on t1.bacm_model_id = t2.brpg_pattern_id\n"
                    + "inner join biz_base_subject t3\n"
                    + "on t3.bbs_subjectnumber = t2.brpg_glacc_id\n"
                    + "inner join biz_base_subject_item t4\n" + "on t4.bbs_pid = t3.bbs_id\n"
                    + "where t1.bacm_model_name = subjectName\n"
                    + "and t1.bacm_model_isvalid = 1\n" + "and t4.bbs_id in\n"
                    + "(select t.bbs_id\n" + "from biz_base_subject_branch t\n"
                    + "where t.bbs_pid = t3.bbs_id\n"
                    + "and t.bbs_patternid = t1.bacm_model_id);\n"
                    + "r_c_admission c_admission%rowtype;\n" + "begin\n" + "brno := '80002';\n"
                    + "--删除当前日期数据，防止重复\n" + "delete from biz_model_business\n"
                    + "where BIZ_NAME = subjectName\n" + "and TX_DT = t;\n" + "commit;\n"
                    + "open c_admission;\n" + "loop\n" + "fetch c_admission\n"
                    + "into r_c_admission; --循环游标中的数据\n" + "exit when c_admission%notfound;\n"
                    + "subjectNumber := r_c_admission.bbs_subjectnumber;\n"
                    + "branchId := r_c_admission.bbs_orgid;\n"
                    + "current_id := r_c_admission.bbs_currid;\n"
                    + "orgrank := r_c_admission.bbs_orgrank;\n"
                    + "bbsId := r_c_admission.bbs_id;\n" + "if orgrank = 1 then\n"
                    + "--dbms_output.put_line('总行');\n" + "insert into biz_model_business\n"
                    + "(tx_dt, --账务日期\n" + "glacc_id, --账号\n" + "curr_id, --货币代码\n"
                    + "branch_id, --机构\n" + "amount, --交易金额\n" + "biz_name, --账号名称\n"
                    + "D_AMT, --今日额变动-CNY\n" + "D_AMT1, --今日额变动（原币）\n" + "D_AMT2, --今日额变动-USD\n"
                    + "D_AMTDR, --今日借方额变动-CNY\n" + "D_AMTDR1, --今日借方额变动（原币）\n"
                    + "D_AMTDR2, --今日借方额变动-USD\n" + "D_AMTCR, --今日贷方额变动-CNY\n"
                    + "D_AMTCR1, --今日贷方额变动（原币）\n" + "D_AMTCR2, --今日贷方额变动-USD\n"
                    + "DAY1, --今日余额-CNY\n" + "DAY2, --昨天余额-CNY\n" + "DAY1_1, --今日余额（原币）\n"
                    + "DAY2_1 --昨天余额（原币）\n" + ")\n" + "select s.tx_dt as tx_dt,\n"
                    + "subjectNumber, --as 账号\n" + "s.curr_id, --as 币种\n" + "s.branch_id, \n"
                    + "case\n" + "when (b.amount is null or s.DAY1_1 = 0) then\n" + "0\n"
                    + "else\n" + "b.amount\n" + "end amt, --金额\n" + "subjectName as biz_name,\n"
                    + "s.D_AMT, --今日额变动-CNY\n" + "s.D_AMT1, --今日额变动(原币)\n"
                    + "s.D_AMT2, --今日额变动-USD\n" + "s.D_AMTDR, --今日借方额变动-CNY\n"
                    + "s.D_AMTDR1, --今日借方额变动(原币)\n" + "s.D_AMTDR2, --今日借方额变动-USD\n"
                    + "s.D_AMTCR, --今日贷方额变动-CNY\n" + "s.D_AMTCR1, --今日贷方额变动(原币)\n"
                    + "s.D_AMTCR2, --今日贷方额变动-USD\n" + "s.DAY1, --今日余额-CNY\n"
                    + "s.DAY2, --昨天余额-CNY\n" + "s.DAY1_1, --今日余额(原币)\n" + "s.DAY2_1 --昨天余额(原币)\n"
                    + "from (select u.tx_dt tx_dt,\n" + "u.glacc_id,\n" + "u.curr_id,\n"
                    + "u.branch_id,\n" + "sum(d_amt) d_amt,\n" + "sum(D_AMT1) D_AMT1,\n"
                    + "sum(D_AMT2) D_AMT2,\n" + "sum(D_AMTDR) D_AMTDR,\n"
                    + "sum(D_AMTDR1) D_AMTDR1,\n" + "sum(D_AMTDR2) D_AMTDR2,\n"
                    + "sum(D_AMTCR) D_AMTCR,\n" + "sum(D_AMTCR1) D_AMTCR1,\n"
                    + "sum(D_AMTCR2) D_AMTCR2,\n" + "sum(DAY1) DAY1,\n" + "sum(DAY2) DAY2,\n"
                    + "sum(DAY1_1) DAY1_1,\n" + "sum(DAY2_1) DAY2_1\n" + "from ods_ebs_s1gl u\n"
                    + "where u.glacc_id = subjectNumber\n" + "and u.tx_dt = t\n"
                    + "and u.branch_id = brno\n" + "and u.curr_id = current_id\n"
                    + "group by u.tx_dt, u.glacc_id, u.curr_id, u.branch_id) s\n"
                    + "left join (select tb.tx_dt,\n" + "case\n" + "when tb.sys_id = 'BEPS' then\n"
                    + "'40700030025'\n" + "when tb.sys_id = 'IBPS' then\n" + "'40700030037'\n"
                    + "when tb.sys_id = 'CBPS' then\n" + "'40700030032'\n" + "end glacc_id,\n"
                    + "sum(tb.amount) amount\n" + "from (SELECT r.tx_dt,\n" + "r.sys_id,\n"
                    + "r.srcacc_fg,\n" + "case\n" + "when r.srcacc_fg = 'D' then\n"
                    + "sum(-r.actsetl_amt)\n" + "else\n" + "sum(r.actsetl_amt)\n" + "end amount\n"
                    + "FROM ods_rs_tinf r\n" + "WHERE r.clea2_dt = t\n" + "--and r.tx_dt = t\n"
                    + "AND r.pretranssts_id = 'STLL'\n"
                    + "AND r.sys_id IN ('BEPS', 'IBPS', 'CBPS')\n"
                    + "GROUP BY r.tx_dt, r.sys_id, r.srcacc_fg) tb\n"
                    + "group by tb.tx_dt, tb.sys_id) b\n" + "on s.tx_dt = b.tx_dt\n"
                    + "and s.glacc_id = b.glacc_id;\n" + "commit;\n" + "end if;\n" + "\n"
                    + "end loop;\n" + "close c_admission;\n" + "select count(*)\n"
                    + "into count_\n" + "from biz_bridge\n"
                    + "where bb_business_name = subjectName;\n"
                    + "select sysdate into bb_time from dual;\n" + "if (count_ > 0) then\n"
                    + "update biz_bridge\n"
                    + "set bb_flag = 1, bb_modify_time = bb_time, bb_tx_dt = t\n"
                    + "where bb_business_name = subjectName;\n" + "else\n"
                    + "insert into biz_bridge\n" + "(bb_model_name,\n" + "bb_business_name,\n"
                    + "bb_flag,\n" + "bb_create_time,\n" + "bb_modify_time,\n" + "bb_tx_dt)\n"
                    + "values\n" + "(subjectName, subjectName, '1', bb_time, bb_time, t);\n"
                    + "end if;\n" + "commit;\n" + "V_RESULT := 1;\n"
                    + "dbms_output.put_line('proc PRO_ZFWQS exe status:' || V_RESULT);\n" + "\n"
                    + "EXCEPTION\n" + "WHEN OTHERS THEN\n" + "err_code := Sqlcode;\n"
                    + "err_msg := Sqlerrm;\n" + "insert into BASE_PROCEDURE_ERRORLOG\n"
                    + "(procedurename, errmsg, syserrorcode, syserrormsg, createtime)\n"
                    + "values\n"
                    + "('PRO_ZFWQS', '', err_code, err_msg, (select sysdate from dual));\n"
                    + "commit;\n" + "V_RESULT := 0;\n"
                    + "dbms_output.put_line('proc PRO_ZFWQS exe status: ' || V_RESULT);\n"
                    + "dbms_output.put_line('code:' || Sqlcode || ' error:' || Sqlerrm);\n"
                    + "end PRO_ZEYANG_220223");

        ResultSet rs = stmt
            .executeQuery("SELECT NAME, TEXT\n"
                          + "        FROM ALL_SOURCE S, ALL_OBJECTS O\n"
                          + "        WHERE S.\"OWNER\"=O.\"OWNER\" AND S.\"NAME\"=O.\"OBJECT_NAME\" AND S.\"TYPE\"=O.\"OBJECT_TYPE\"\n"
                          + "        AND O.\"OBJECT_TYPE\"='PROCEDURE' AND S.\"NAME\" = 'PRO_ZEYANG_220223'\n"
                          + "        ORDER BY \"NAME\",\"TYPE\",\"LINE\" ASC");
        Assert.assertTrue(rs.next());
        String text = rs.getString("TEXT");
        Assert.assertTrue(text
            .startsWith("procedure PRO_ZEYANG_220223(t in varchar2, v_result out number) is"));
        java.sql.Clob clob = rs.getClob(2);
        Assert.assertEquals(text.length(), clob.length());
    }

    @Test
    public void aone50824294() {
        try {
            Connection conn = setConnection("&useServerPsStmtChecksum=false");
            createTable("clobTest1", "c1 varchar2(20), c2 clob");
            String sql = "insert into  clobTest1 values ('abcString123', ?)";
            PreparedStatement ps = conn.prepareStatement(sql);

            int clobSize = 1024 * 5;
            //            int clobSize = 1024 * 4;
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0; i < clobSize; i++) {
                stringBuilder.append('a');
            }
            String str = stringBuilder.toString();

            ps.setClob(1, new StringReader(str));
            ps.execute();
            ResultSet rs = conn.prepareStatement("select c2 from clobTest1").executeQuery();
            assertTrue(rs.next());
            assertEquals(clobSize, rs.getString(1).length());
        } catch (SQLException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testInvalidHexNumber() {
        try {
            Connection conn = setConnection("&rewriteBatchedStatements=TRUE&allowMultiQueries=TRUE&useLocalSessionState=TRUE"
                                            + "&useUnicode=TRUE&socketTimeout=3000000&connectTimeout=60000");
            createTable("CLOB_TEST", "ID VARCHAR2(10), MESSAGE CLOB");

            ResultSet rs = null;
            String insertSql = "insert into CLOB_TEST(ID, MESSAGE) values ('TEST', empty_clob())";
            String querySql = "select ID, MESSAGE from CLOB_TEST s where s.ID=? for update";
            PreparedStatement insert = conn.prepareStatement(insertSql);
            PreparedStatement query = conn.prepareStatement(querySql);
            insert.executeUpdate();
            query.setString(1, "TEST");
            conn.setAutoCommit(false);
            rs = query.executeQuery();
            if (rs.next()) {
                java.sql.Clob clob = rs.getClob("MESSAGE");
                clob.setString(1L, "aaaaaa"); // aone 50972954: -9747, malformed ps packet
                assertEquals("aaaaaa", clob.getSubString(1L, (int) clob.length()));
            }
            conn.commit();
            conn.setAutoCommit(true);
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void dadibaoxian() throws SQLException, IOException {
        Connection conn = sharedConnection;
        createTable("clobTest1", "c1 int, c2 clob");
        String sql = "insert into  clobTest1 values (1, ?)";
        PreparedStatement ps = conn.prepareStatement(sql);
        int clobSize = 9 * 1024;
        char[] data = new char[clobSize];
        for (int i = 0; i < clobSize; i++) {
            data[i] = 'a';
        }
        String str = String.valueOf(data);
        ps.setString(1, str);
        ps.execute();
        ResultSet rs = conn.prepareStatement("select c2 from clobTest1").executeQuery();
        assertTrue(rs.next());
        java.sql.Clob clob2 = rs.getClob(1);
        char[] res_data = new char[clobSize];
        clob2.getCharacterStream().read(res_data);
        for (int i = 0; i < clobSize; i++) {
            assertEquals(data[i], res_data[i]);
        }
        assertEquals(clobSize, rs.getString(1).length());
    }

    @Test
    public void aone51963233() throws Exception {
        createTable("t1", "c1 clob");
        Connection conn = sharedConnection;
        Statement stmt = conn.createStatement();
        stmt.execute("insert into t1 values ('aaa')");

        ResultSet rs = conn.prepareStatement("select c1 from t1").executeQuery();
        assertTrue(rs.next());
        try {
            java.sql.Clob clob = rs.getClob(1);
            clob.setString(1, "bbbb");// server 4.3，ps 协议，ORA-00600: internal error code, arguments: -6256, Row has not been locked
            fail();
        } catch (SQLException e) {
            //oracle-jdbc  ORA-22920: 未锁定含有 LOB 值的行
        }

        conn.setAutoCommit(false);
        rs = conn.prepareStatement("select c1 from t1 for update").executeQuery();
        assertTrue(rs.next());
        assertEquals("aaa", rs.getString(1));
        java.sql.Clob clob = rs.getClob(1);
        clob.setString(1, "bbbb");
        assertEquals("bbbb", clob.getSubString(1, (int) clob.length()));
        conn.commit();
        conn.setAutoCommit(true);

        //        rs = conn.prepareStatement("select c1 from t1 for update").executeQuery(); // server 4.3，ps 二合一协议，ORA-01002: fetch out of sequence
        //        assertTrue(rs.next()); // server 4.3，ps 协议，ORA-01002: fetch out of sequence
        //        assertEquals("bbbb", rs.getString(1));
        //        clob = rs.getClob(1);
        //        clob.setString(1, "bbbb"); // server 4.3，文本协议，ORA-22990: LOB locators cannot span transactions；
        //                                            // server 3.2.3，所有协议，ORA-00600: internal error code, arguments: -6256, Row has not been locked
        //        assertEquals("bbbb", clob.getSubString(1, (int) clob.length()));

        rs = conn.prepareStatement("select c1 from t1").executeQuery();
        assertTrue(rs.next());
        assertEquals("bbbb", rs.getString(1));
    }

    @Test
    public void aone52919826TestEmpty() throws SQLException {
        Connection conn = sharedConnection;
        Statement stmt = conn.createStatement();
        try {
            stmt.execute("drop table emptylob ");
        } catch (SQLException e) {
            //                e.printStackTrace();
        }
        stmt.execute("create table emptylob(b1 blob,c2 clob)");

        PreparedStatement ps = null;
        ps = conn.prepareStatement("insert into emptylob values(?,?)");
        ps.setBlob(1, Blob.getEmptyBLOB());
        ps.setClob(2, Clob.getEmptyCLOB());
        ps.execute();

        //        stmt.execute("insert into emptylob values('','')");
        ResultSet rs = stmt.executeQuery("select * from emptylob");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(0, rs.getBytes(1).length);
        Assert.assertEquals("", rs.getString(2));
    }

    @Test
    public void aone52959303() {
        try {
            Connection conn = sharedConnection;
            createTable("test1", "c1 int, c2 clob");

            Statement stmt = conn.createStatement();
            stmt.execute("insert into test1 values (1, 'aaa')");
            CallableStatement cs = conn.prepareCall("create or replace procedure Pro_1(var clob)"
                                                    + " is begin\n"
                                                    + " update test1 set c2 = var where c1= 1;\n"
                                                    + " end;");
            cs.execute();
            cs = conn.prepareCall("{call Pro_1(?)}");
            cs.setClob(1, com.oceanbase.jdbc.Clob.getEmptyCLOB());
            cs.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void aone52985330testLobWriteNull() {
        try {
            Connection conn = sharedConnection;
            createTable("t_lob", "c1 clob, c2 blob");
            Statement stmt = conn.createStatement();
            stmt.execute("insert into t_lob values ('aaa', '616161')");

            conn.setAutoCommit(false);
            ResultSet rs = conn.prepareStatement("select * from t_lob for update").executeQuery();
            assertTrue(rs.next());
            com.oceanbase.jdbc.Clob clob = (com.oceanbase.jdbc.Clob) rs.getClob(1);
//            clob.setString(1, "");
            clob.setString(1, null);
            com.oceanbase.jdbc.Blob blob = (com.oceanbase.jdbc.Blob) rs.getBlob(2);
//            blob.setBytes(1, "".getBytes());
            blob.setBytes(1, null);
            conn.setAutoCommit(true);

            rs = conn.prepareStatement("select * from t_lob").executeQuery();
            assertTrue(rs.next());
            assertEquals("aaa", rs.getString(1));
            assertArrayEquals("".getBytes(), rs.getBytes(2));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testOracleClobTruncate() throws SQLException {
        Connection conn = sharedConnection;
        createTable("t_clob", "c1 clob");
        Statement stmt = conn.createStatement();
        stmt.execute("insert into t_clob values ('aaa')");
        conn.setAutoCommit(false);
        ResultSet rs = conn.prepareStatement("select * from t_clob for update").executeQuery();
        assertTrue(rs.next());
        java.sql.Clob clob = rs.getClob(1);
        clob.truncate(3);
        assertEquals("aaa",clob.getSubString(1, 3));
        try {
            clob.truncate(-1);
            fail();
        } catch (SQLException e) {
            assertEquals("Invalid argument: 'len' should not be < 0",e.getMessage());
        }
        try {
            clob.truncate(4);
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("Cannot truncate CLOB of length"));
        }
        conn.setAutoCommit(true);
    }

    @Test
    public void testClobLength() throws SQLException, IOException {
        Connection conn = setConnectionOrigin("?useServerPrepStmts=true&usePieceData=true");
        createTable("clobTest1", "c1 clob");
        String sql = "insert into  clobTest1 values (?)";
        PreparedStatement ps = conn.prepareStatement(sql);

        int clobSize = 1024 * 5;
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < clobSize; i++) {
            stringBuilder.append('a');
        }
        String str = stringBuilder.toString();
        ps.setClob(1, new StringReader(str));
        ps.execute();

        ResultSet rs = conn.prepareStatement("select c1 from clobTest1").executeQuery();
        assertTrue(rs.next());
        java.sql.Clob clob = rs.getClob(1);
        Assert.assertEquals(clobSize, clob.length());
        Assert.assertEquals(clobSize, clob.length());

        java.sql.Clob clob1 = conn.createClob();
        Writer writer = clob1.setCharacterStream(1);
        for (int i = 0; i < 2000; i++) {
            writer.write("a");
        }
        writer.flush();
        Assert.assertEquals(2000, clob1.length());
        writer.close();
        ps.close();
    }

    @Test
    public void testStreamResultForAone54492994() throws Exception, IOException {
        Connection conn = sharedConnection;
        createTable("clobTest1", "c1 clob");

        String sql = "insert into  clobTest1 values (?)";
        PreparedStatement ps = conn.prepareStatement(sql);
        int clobSize = 1024 * 5;
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < clobSize; i++) {
            stringBuilder.append('a');
        }
        String str = stringBuilder.toString();
        for (int i = 0; i < 5; i++) {
            ps.setClob(1, new StringReader(str));
            ps.addBatch();
        }
        ps.executeBatch();

        conn = setConnection("&extendOracleResultSetClass=true");
        PreparedStatement preparedStatement = conn.prepareStatement("select c1 from clobTest1 ");
        preparedStatement.setFetchSize(Integer.MIN_VALUE);
        ResultSet rs = preparedStatement.executeQuery();
        while (rs.next()) {
            java.sql.Clob clob = rs.getClob(1);
            Reader reader = clob.getCharacterStream();
            StringBuffer sb = new StringBuffer();
            char[] chars = new char[1024];
            while (reader.read(chars) != -1) {
                sb.append(chars);
            }
            assertEquals(clobSize, sb.length());
        }
    }

    @Test
    public void testLobStreamResultForAone55787988() throws Exception {
        Connection conn = setConnection("&usePieceData=true&extendOracleResultSetClass=true");
        createTable("t_lob", "c1 clob, c2 blob");
        PreparedStatement ps = conn.prepareStatement("insert into  t_lob values (?,?)");
        StringBuilder stringBuilder = new StringBuilder();
        int size = 1024 * 5;
        for (int i = 0; i < size; i++) {
            stringBuilder.append('a');
        }
        String str = stringBuilder.toString();
        byte[] bytes = str.getBytes();
        ps.setClob(1, new StringReader(str));
        ps.setBlob(2, new ByteArrayInputStream(bytes));
        ps.execute();

        ps = conn.prepareStatement("select * from t_lob");
        ps.setFetchSize(Integer.MIN_VALUE);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(size, rs.getClob(1).length());
        assertEquals(size, rs.getBlob(2).length());
    }

    @Test
    public void testClobLobLocatorV2ForAone55819451() {
        try {
            Connection conn = setConnectionOrigin("?useServerPrepStmts=true&usePieceData=true");
            String clobTest = "t_clob" ;

            createTable(clobTest, "c1 clob");
            PreparedStatement ps = conn.prepareStatement("insert into " + clobTest + " values(?)");
            int size1 = 1024 * 3;
            java.sql.Clob clob = conn.createClob();
            Writer writer = clob.setCharacterStream(1);
            for (int i = 0; i < size1; i++) {
                writer.write("a");
            }
            writer.flush();
            ps.setClob(1, clob);
            ps.execute();

            conn.setAutoCommit(false);
            ps = conn.prepareStatement("select c1 from " + clobTest + " for update");
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());

            java.sql.Clob clob3 = rs.getClob(1);
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 1024 * 10; i++) {
                sb.append("b");
            }
            clob3.setString(1, sb.toString());
            int updateSize = (int) clob3.length();
            assertEquals(1024 * 10, updateSize);
            String subString = clob3.getSubString(1, updateSize);
            conn.commit();
            conn.setAutoCommit(true);

            ps = conn.prepareStatement("select c1 from " + clobTest);
            rs = ps.executeQuery();
            assertTrue(rs.next());
            String string = rs.getString(1);
            assertEquals(updateSize, string.length());
            assertEquals(subString, string);
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }


    @Test
    public void testTruncate() throws SQLException, IOException {
        String str = "string";
        java.sql.Clob clob = sharedConnection.createClob();
        clob.setString(1, str);
        Exception exception = assertThrows(Exception.class, () -> clob.truncate(-1));
        Assert.assertEquals("Invalid argument: 'len' should not be < 0", exception.getMessage());
        Exception e2 = assertThrows(Exception.class, () -> clob.truncate(str.length() + 1));
        Assert.assertEquals("Cannot truncate CLOB of length 6 to length of 7", e2.getMessage());
        java.sql.Blob blob = sharedConnection.createBlob();
        Exception e3 = assertThrows(Exception.class, () -> blob.truncate(-1));
        Assert.assertEquals("'len' should be >= 0", e3.getMessage());
    }

    @Test
    public void testClobSetMethodsForAone53671316() throws Exception {
        // setString -- begin
        java.sql.Clob clob = sharedConnection.createClob();
        clob.setString(1, "clob");
        String str = "string";
        java.sql.Clob finalClob = clob;
        SQLException exception = assertThrows(SQLException.class, () -> finalClob.setString(0, str));
        Assert.assertEquals("Starting position can not be < 1", exception.getMessage());

        clob.setString(2, str);
        assertEquals("cstring", clob.getSubString(1, (int) clob.length()));

        clob = sharedConnection.createClob();
        clob.setString(1, "cl\0b");
        clob.setString(8, "\0string");
        assertEquals("cl\0b   \0string", clob.getSubString(1, (int) clob.length()));

        clob = sharedConnection.createClob();
        clob.setString(1, "clob");
        clob.setString(6, "");
        assertEquals("clob", clob.getSubString(1, (int) clob.length()));

        clob = sharedConnection.createClob();
        clob.setString(3, "string");
        assertEquals("  string", clob.getSubString(1, (int) clob.length()));

        clob = sharedConnection.createClob();
        java.sql.Clob finalClob1 = clob;
        SQLException e1 = assertThrows(SQLException.class, () -> finalClob1.setString(0, "string"));
        Assert.assertEquals("Starting position can not be < 1", e1.getMessage());

        clob = sharedConnection.createClob();
        clob.setString(2, "");
        assertEquals("", clob.getSubString(1, (int) clob.length()));
        // setString -- end

        // setCharacterStream -- begin
        java.sql.Clob clob1 = sharedConnection.createClob();
        clob1.setString(1, "clob");
        java.sql.Clob finalClob2 = clob1;
        IllegalArgumentException e2 = assertThrows(IllegalArgumentException.class, () -> finalClob2.setCharacterStream(-1));

        Writer writer = clob1.setCharacterStream(0);
        writer.write("string");
        writer.flush();
        writer.close();
        assertEquals("string", clob1.getSubString(1, (int) clob1.length()));

        clob1 = sharedConnection.createClob();
        clob1.setString(1, "cl\0b");
        writer = clob1.setCharacterStream(6);
        writer.write("string");
        writer.flush();
        writer.close();
        assertEquals("cl\0b string", clob1.getSubString(1, (int) clob1.length()));

        clob1 = sharedConnection.createClob();
        clob1.setString(1, "clob");
        writer = clob1.setCharacterStream(6);
        writer.write("");
        writer.flush();
        writer.close();
        assertEquals("clob", clob1.getSubString(1, (int) clob1.length()));

        clob1 = sharedConnection.createClob();
        writer = clob1.setCharacterStream(4);
        writer.write("string");
        writer.flush();
        writer.close();
        assertEquals("   string", clob1.getSubString(1, (int) clob1.length()));

        clob1 = sharedConnection.createClob();
        writer = clob1.setCharacterStream(4);
        writer.write("");
        writer.flush();
        writer.close();
        assertEquals("", clob1.getSubString(1, (int) clob1.length()));
        // setCharacterStream -- end
    }

    @Test
    public void testSetCharacterStremaForAone52877708() throws SQLException, IOException {
        Connection conn = sharedConnection;
        java.sql.Clob clob = conn.createClob();
        Writer writer = clob.setCharacterStream(2);
        writer.write("abc");
        writer.flush();
        Assert.assertEquals(" abc", clob.getSubString(1,4));
    }

    @Test
    public void doTestFreeForDima2024062200102856198() throws SQLException {
        // this constructor cannot determine is mysql or oracle mode
        // free(new com.oceanbase.jdbc.Clob(bytes));
        free(sharedConnection.createClob());
        Statement stmt = sharedConnection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT TO_CLOB('这是一段CLOB文本') AS clob_value FROM DUAL");
        rs.next();
        free(rs.getClob(1));
    }

    public void free(java.sql.Clob clob) throws SQLException {
        clob.free();
        assertEquals(0, ((com.oceanbase.jdbc.Clob)clob).length);
        try {
            clob.setString(3,"c");
            fail();
        } catch (SQLException e) {
            assertEquals("Invalid operation on closed LOB" , e.getMessage());
        }
        try {
            clob.getCharacterStream();
            fail();
        } catch (SQLException e) {
            assertEquals("Invalid operation on closed LOB" , e.getMessage());
        }
        try {
            clob.length();
            fail();
        } catch (SQLException e) {
            assertEquals("Invalid operation on closed LOB" , e.getMessage());
        }
        try {
            clob.setAsciiStream(3);
            fail();
        } catch (SQLException e) {
            assertEquals("Invalid operation on closed LOB" , e.getMessage());
        }
        try {
            clob.getAsciiStream();
            fail();
        } catch (SQLException e) {
            assertEquals("Invalid operation on closed LOB" , e.getMessage());
        }
        try {
            clob.getSubString(1,8);
            fail();
        } catch (SQLException e) {
            assertEquals("Invalid operation on closed LOB" , e.getMessage());
        }
        try {
            clob.position("b",1);
            fail();
        } catch (SQLException e) {
            assertEquals("Invalid operation on closed LOB" , e.getMessage());
        }
        try {
            clob.truncate(3);
            fail();
        } catch (SQLException e) {
            assertEquals("Invalid operation on closed LOB" , e.getMessage());
        }
        try {
            clob.setCharacterStream(1);
            fail();
        } catch (SQLException e) {
            assertEquals("Invalid operation on closed LOB" , e.getMessage());
        }
    }

    @Test
    public void testForDima2024112000105211953() throws SQLException {
        Connection conn = sharedConnection;
        java.sql.Blob blob = conn.createBlob();
        java.sql.Clob clob = conn.createClob();

        Assert.assertTrue(blob.toString().contains("com.oceanbase.jdbc.Blob@"));
        Assert.assertTrue(clob.toString().contains("com.oceanbase.jdbc.Clob@"));
    }

    @Test
    public void testPLSetLobNullForDima2024080100104015501() throws SQLException {
        Connection conn = sharedConnection;
        Statement stmt = conn.createStatement();
        stmt.execute("create or replace procedure test_blob(a in out blob) as begin null; end;");
        stmt.execute("create or replace procedure test_clob(a in out clob) as begin null; end;");
        stmt.close();

        for (int i = 0; i < 2; i++) {
            if (i == 0) {
                conn = setConnection("&useServerPrepStmts=true&usePieceData=true");
            } else {
                conn = setConnection("&useServerPrepStmts=true");
            }

            // Blob
            CallableStatement csBLob = conn.prepareCall("{call test_blob(?)}");
            // case 1 & 11
            String s = "ABCDEF";
            byte[] blobContent = s.getBytes(StandardCharsets.UTF_8);
            java.sql.Blob inputOutputBlob = conn.createBlob();
            inputOutputBlob.setBytes(1, blobContent);
            csBLob.setBlob(1, inputOutputBlob);
            csBLob.registerOutParameter(1, Types.BLOB);
            try {
                csBLob.execute();
                if (i == 1) {
                    fail();
                }
            } catch (SQLException e) {
                if (i == 1) {
                    Assert.assertTrue(e.getMessage().contains(" Not supported send long data on ob oracle"));
                }
            }

            // case 2 & 12
            csBLob.setBlob(1, (java.sql.Blob) null);
            csBLob.registerOutParameter(1, Types.BLOB);
            try {
                csBLob.execute();
            } catch (SQLException e) {
                fail();
            }

            // case 3 & 13
            csBLob.setBlob(1, Blob.getEmptyBLOB());
            csBLob.registerOutParameter(1, Types.BLOB);
            try {
                csBLob.execute();
            } catch (SQLException e) {
                fail();
            }

            // case 4 & 14
            csBLob.setBlob(1, new Blob());
            csBLob.registerOutParameter(1, Types.BLOB);
            try {
                csBLob.execute();
                fail();
            } catch (SQLException e) {
               Assert.assertTrue(e.getMessage().contains("-5555, Incorrect number of arguments"));
            }

            // case 5 & 15
            csBLob.setBlob(1, conn.createBlob());
            csBLob.registerOutParameter(1, Types.BLOB);
            try {
                csBLob.execute();
                fail();
            } catch (SQLException e) {
                Assert.assertTrue(e.getMessage().contains("-5555, Incorrect number of arguments"));
            }

            // Clob
            CallableStatement csClob = conn.prepareCall("{call test_clob(?)}");
            // case 6 & 16
            java.sql.Clob inputOutputClob = conn.createClob();
            inputOutputClob.setString(1, "ABCDEF");
            csClob.setClob(1, inputOutputClob);
            csClob.registerOutParameter(1, Types.CLOB);
            try {
                csClob.execute();
                if (i == 1) {
                    fail();
                }
            } catch (SQLException e) {
                if (i == 1) {
                    Assert.assertTrue(e.getMessage().contains(" Not supported send long data on ob oracle"));
                }
            }

            // case 7 & 17
            csClob.setClob(1, (java.sql.Clob) null);
            csClob.registerOutParameter(1, Types.CLOB);
            try {
                csClob.execute();
            } catch (SQLException e) {
                fail();
            }

            // case 8 & 18
            csClob.setClob(1, Clob.getEmptyCLOB());
            csClob.registerOutParameter(1, Types.CLOB);
            try {
                csClob.execute();
            } catch (SQLException e) {
                fail();
            }

            // case 9 & 19
            csClob.setClob(1, new Clob());
            csClob.registerOutParameter(1, Types.CLOB);
            try {
                csClob.execute();
            } catch (SQLException e) {
                fail();
            }

            // case 10 & 20
            csClob.setClob(1, conn.createClob());
            csClob.registerOutParameter(1, Types.CLOB);
            try {
                csClob.execute();
            } catch (SQLException e) {
                fail();
            }
        }
    }

    @Test
    public void testForDima2025022400107307089() throws SQLException {
        Connection conn = setConnection("&useServerPrepStmts=true");
        Statement stmt = conn.createStatement();
        createTable("t_clob", "c1 clob");
        stmt.execute("insert into t_clob values('aaa')");
        stmt.execute("create or replace type clob_array as table of clob");

        createProcedure("p_clob", "(var out clob_array) is begin select c1 bulk collect into var FROM t_clob;end;");
        CallableStatement cs = conn.prepareCall("{call p_clob(?)}");
        cs.registerOutParameter(1, Types.ARRAY, "CLOB_ARRAY");
        cs.execute();
        Array array = cs.getArray(1);
        ResultSet rs = array.getResultSet();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("aaa", rs.getString(2));
        assertFalse(rs.next());
    }

}
