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

import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.sql.*;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class OceanBaseClobTest extends BaseTest {

    private final byte[] bytes = "abcde🙏fgh".getBytes(StandardCharsets.UTF_8);

    @Test
    public void length() {
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
        assertArrayEquals(new byte[] { 0, 1, 2, 13, 4, 5, }, blob2.getBytes(1, 7));

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

    @Ignore
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
}
