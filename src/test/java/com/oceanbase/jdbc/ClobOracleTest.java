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
            PreparedStatement ps = conn.prepareStatement("insert into " + clobTest3
                                                         + " values(1, ?, ?, ?, ?, ?, ?)");

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

}
