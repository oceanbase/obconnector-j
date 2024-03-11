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

import org.junit.*;

import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.*;

public class BlobOracleTest extends BaseOracleTest {

    /**
     * Initialisation.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("bug716378", "id int not null primary key, test blob, test2 blob, test3 clob");
        createTable("BlobTeststreamtest2", "id int primary key not null, st varchar(20), strm clob"
                                           + ", strm2 clob, strm3 clob");
        createTable("BlobTeststreamtest3", "id int primary key not null, strm clob");
        createTable("BlobTestclobtest", "id int not null primary key, strm clob");
        createTable("BlobTestclobtest2", "strm clob");
        createTable("BlobTestclobtest3", "id int not null primary key, strm clob");
        createTable("BlobTestclobtest4", "id int not null primary key, strm clob");
        createTable("BlobTestclobtest5", "id int not null primary key, strm clob");
        createTable("BlobTestblobtest", "id int not null primary key, strm blob");
        createTable("BlobTestblobtest2", "id int not null primary key, strm blob");
        createTable("conj77_test", "Name VARCHAR(100) NOT NULL,Archive BLOB, PRIMARY KEY (Name)");
        createTable("bug716378_ps",
            "id int not null primary key, test blob, test2 blob, test3 clob");
        createTable("BlobTeststreamtest2_ps",
            "id int primary key not null, st varchar(20), strm clob" + ", strm2 clob, strm3 clob");
        createTable("BlobTeststreamtest3_ps", "id int primary key not null, strm clob");
        createTable("BlobTestclobtest_ps", "id int not null primary key, strm clob");
        createTable("BlobTestclobtest2_ps", "strm clob");
        createTable("BlobTestclobtest3_ps", "id int not null primary key, strm clob");
        createTable("BlobTestclobtest4_ps", "id int not null primary key, strm clob");
        createTable("BlobTestclobtest5_ps", "id int not null primary key, strm clob");
        createTable("BlobTestblobtest_ps", "id int not null primary key, strm blob");
        createTable("BlobTestblobtest2_ps", "id int not null primary key, strm blob");
        createTable("conj77_test_ps", "Name VARCHAR(100) NOT NULL,Archive BLOB, PRIMARY KEY (Name)");
        createTable("oracleBinary", "c1 int ,blo2 blob,blo3 blob,ca char(20),primary key (c1)");

    }

    @Test
    public void testPosition() throws SQLException {
        byte[] blobContent = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
        byte[] pattern = new byte[] { 3, 4 };
        java.sql.Blob blob = new com.oceanbase.jdbc.Blob(blobContent);
        assertEquals(3, blob.position(pattern, 1));
        pattern = new byte[] { 12, 13 };
        assertEquals(-1, blob.position(pattern, 1));
        pattern = new byte[] { 11, 12 };
        assertEquals(11, blob.position(pattern, 1));
        pattern = new byte[] { 1, 2 };
        assertEquals(1, blob.position(pattern, 1));
    }

    @Test(expected = SQLException.class)
    public void testBadStart() throws SQLException {
        byte[] blobContent = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
        byte[] pattern = new byte[] { 3, 4 };
        java.sql.Blob blob = new com.oceanbase.jdbc.Blob(blobContent);
        blob.position(pattern, 0);
    }

    @Test(expected = SQLException.class)
    public void testBadStart2() throws SQLException {
        byte[] blobContent = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
        byte[] pattern = new byte[] { 3, 4 };
        java.sql.Blob blob = new com.oceanbase.jdbc.Blob(blobContent);
        blob.position(pattern, 44);
    }

    //  For lob and other types of columns, getObject returns the corresponding type instance instead of byte[] Consistent with 1.x, it will be modified later
    @Ignore
    public void testBug716378() throws SQLException {
        Statement stmt = sharedConnection.createStatement();

        stmt.executeUpdate("insert into bug716378 values(1, 'a','b','c')");
        ResultSet rs = stmt.executeQuery("select * from bug716378");
        assertTrue(rs.next());
        byte[] arr = new byte[0];
        assertEquals(arr.getClass(), rs.getObject(2).getClass());
        assertEquals(arr.getClass(), rs.getObject(3).getClass());
        assertEquals(String.class, rs.getObject(4).getClass());
    }

    @Test
    public void testCharacterStreamWithMultibyteCharacterAndLength() throws Throwable {
        Assume.assumeFalse(sharedUsePrepare()); //COM_STMT_SEND_LONG_DATA  is temporarily not supported, ps cannot be used
        String toInsert1 = "Øbbcdefgh\njklmn\"";
        String toInsert2 = "Øabcdefgh\njklmn\"";
        PreparedStatement stmt = sharedConnection
            .prepareStatement("insert into BlobTeststreamtest2 (id, st, strm, strm2, strm3) values (?,?,?,?,?)");
        stmt.setInt(1, 2);
        stmt.setString(2, toInsert1);
        Reader reader = new StringReader(toInsert2);
        stmt.setCharacterStream(3, reader, 5);
        stmt.setCharacterStream(4, null);
        stmt.setCharacterStream(5, null, 5);
        stmt.execute();

        ResultSet rs = sharedConnection.createStatement().executeQuery(
            "select * from BlobTeststreamtest2");
        assertTrue(rs.next());
        Reader rdr = rs.getCharacterStream("strm");
        StringBuilder sb = new StringBuilder();
        int ch;
        while ((ch = rdr.read()) != -1) {
            sb.append((char) ch);
        }

        assertEquals(toInsert1, rs.getString(2));
        assertEquals(toInsert2.substring(0, 5), sb.toString());
        assertNull(rs.getString(4));
        assertNull(rs.getString(5));
    }

    //@Ignore //TODO FIXME
    @Test
    public void testPSCharacterStreamWithMultibyteCharacterAndLength() throws Throwable {
        Assume.assumeFalse(sharedUsePrepare()); //COM_STMT_SEND_LONG_DATA  is temporarily not supported, ps cannot be used
        String toInsert1 = "Øbbcdefgh\njklmn\"";
        String toInsert2 = "Øabcdefgh\njklmn\"";
        PreparedStatement stmt = sharedPSConnection
            .prepareStatement("insert into BlobTeststreamtest2_ps (id, st, strm, strm2, strm3) values (?,?,?,?,?)");
        stmt.setInt(1, 2);
        stmt.setString(2, toInsert1);
        Reader reader = new StringReader(toInsert2);
        stmt.setCharacterStream(3, reader, 5);
        stmt.setCharacterStream(4, null);
        stmt.setCharacterStream(5, null, 5);
        stmt.execute();

        ResultSet rs = sharedPSConnection.createStatement().executeQuery(
            "select * from BlobTeststreamtest2_ps");
        assertTrue(rs.next());
        Reader rdr = rs.getCharacterStream("strm");
        StringBuilder sb = new StringBuilder();
        int ch;
        while ((ch = rdr.read()) != -1) {
            sb.append((char) ch);
        }

        assertEquals(toInsert1, rs.getString(2));
        assertEquals(toInsert2.substring(0, 5), sb.toString());
        assertNull(rs.getString(4));
        assertNull(rs.getString(5));
    }

    @Test
    public void testCharacterStreamWithMultibyteCharacter() throws Throwable {
        Assume.assumeFalse(sharedUsePrepare()); //COM_STMT_SEND_LONG_DATA  is temporarily not supported, ps cannot be used
        PreparedStatement stmt = sharedConnection
            .prepareStatement("insert into BlobTeststreamtest3 (id, strm) values (?,?)");
        stmt.setInt(1, 2);
        String toInsert = "Øabcdefgh\njklmn\"";
        Reader reader = new StringReader(toInsert);
        stmt.setCharacterStream(2, reader);
        stmt.execute();
        ResultSet rs = sharedConnection.createStatement().executeQuery(
            "select * from BlobTeststreamtest3");
        assertTrue(rs.next());
        Reader rdr = rs.getCharacterStream("strm");
        StringBuilder sb = new StringBuilder();
        int ch;
        while ((ch = rdr.read()) != -1) {
            sb.append((char) ch);
        }
        assertEquals(toInsert, sb.toString());
    }

    @Test
    public void testPSCharacterStreamWithMultibyteCharacter() throws Throwable {
        Assume.assumeFalse(sharedUsePrepare()); //COM_STMT_SEND_LONG_DATA  is temporarily not supported, ps cannot be used
        PreparedStatement stmt = sharedPSConnection
            .prepareStatement("insert into BlobTeststreamtest3_ps (id, strm) values (?,?)");
        stmt.setInt(1, 2);
        String toInsert = "Øabcdefgh\njklmn\"";
        Reader reader = new StringReader(toInsert);
        stmt.setCharacterStream(2, reader);
        stmt.execute();
        ResultSet rs = sharedPSConnection.createStatement().executeQuery(
            "select * from BlobTeststreamtest3_ps");
        assertTrue(rs.next());
        Reader rdr = rs.getCharacterStream("strm");
        StringBuilder sb = new StringBuilder();
        int ch;
        while ((ch = rdr.read()) != -1) {
            sb.append((char) ch);
        }
        assertEquals(toInsert, sb.toString());
    }

    @Test
    public void testReaderWithLength() throws SQLException, IOException {
        Assume.assumeFalse(sharedUsePrepare()); //COM_STMT_SEND_LONG_DATA  is temporarily not supported, ps cannot be used
        PreparedStatement stmt = sharedConnection
            .prepareStatement("insert into BlobTestclobtest5 (id, strm) values (?,?)");
        byte[] arr = new byte[32000];
        Arrays.fill(arr, (byte) 'b');

        stmt.setInt(1, 1);
        String clob = new String(arr);
        stmt.setCharacterStream(2, new StringReader(clob), 20000);
        stmt.execute();
        ResultSet rs = sharedConnection.createStatement().executeQuery(
            "select * from BlobTestclobtest5");
        assertTrue(rs.next());
        Reader readStuff = rs.getCharacterStream("strm");

        char[] chars = new char[50000];
        //noinspection ResultOfMethodCallIgnored
        readStuff.read(chars);

        byte[] arrResult = new byte[20000];
        Arrays.fill(arrResult, (byte) 'b');

        for (int i = 0; i < chars.length; i++) {
            if (i < 20000) {
                assertEquals(arrResult[i], chars[i]);
            } else {
                assertEquals(chars[i], '\u0000');
            }
        }
    }

    @Ignore
    //TODO FIXME
    @Test
    public void testPSReaderWithLength() throws SQLException, IOException {
        PreparedStatement stmt = sharedPSConnection
            .prepareStatement("insert into BlobTestclobtest5_ps (id, strm) values (?,?)");
        byte[] arr = new byte[32000];
        Arrays.fill(arr, (byte) 'b');

        stmt.setInt(1, 1);
        String clob = new String(arr);
        stmt.setCharacterStream(2, new StringReader(clob), 20000);
        stmt.execute();
        ResultSet rs = sharedPSConnection.createStatement().executeQuery(
            "select * from BlobTestclobtest5_ps");
        assertTrue(rs.next());
        Reader readStuff = rs.getCharacterStream("strm");

        char[] chars = new char[50000];
        //noinspection ResultOfMethodCallIgnored
        readStuff.read(chars);

        byte[] arrResult = new byte[20000];
        Arrays.fill(arrResult, (byte) 'b');

        for (int i = 0; i < chars.length; i++) {
            if (i < 20000) {
                assertEquals(arrResult[i], chars[i]);
            } else {
                assertEquals(chars[i], '\u0000');
            }
        }
    }

    @Test
    public void testBlobWithLength() throws SQLException, IOException {
        Assume.assumeFalse(sharedUsePrepare()); //COM_STMT_SEND_LONG_DATA  is temporarily not supported, ps cannot be used
        PreparedStatement stmt = sharedConnection
            .prepareStatement("insert into BlobTestblobtest2 (id, strm) values (?,?)");
        byte[] arr = new byte[32000];
        Random rand = new Random();
        rand.nextBytes(arr);
        InputStream stream = new ByteArrayInputStream(arr);
        stmt.setInt(1, 1);
        stmt.setBlob(2, stream, 20000);
        stmt.execute();

        // check what stream not read after length:
        int remainRead = 0;
        while (stream.read() >= 0) {
            remainRead++;
        }
        assertEquals(12000, remainRead);

        ResultSet rs = sharedConnection.createStatement().executeQuery(
            "select * from BlobTestblobtest2");
        assertTrue(rs.next());
        InputStream readStuff = rs.getBlob("strm").getBinaryStream();
        int pos = 0;
        int ch;
        while ((ch = readStuff.read()) != -1) {
            assertEquals(arr[pos++] & 0xff, ch);
        }
        assertEquals(20000, pos);
    }

    @Test
    public void testPSBlobWithLength() throws SQLException, IOException {
        Assume.assumeFalse(sharedUsePrepare()); //COM_STMT_SEND_LONG_DATA  is temporarily not supported, ps cannot be used
        PreparedStatement stmt = sharedPSConnection
            .prepareStatement("insert into BlobTestblobtest2_ps (id, strm) values (?,?)");
        byte[] arr = new byte[32000];
        Random rand = new Random();
        rand.nextBytes(arr);
        InputStream stream = new ByteArrayInputStream(arr);
        stmt.setInt(1, 1);
        stmt.setBlob(2, stream, 20000);
        stmt.execute();

        // check what stream not read after length:
        int remainRead = 0;
        while (stream.read() >= 0) {
            remainRead++;
        }
        assertEquals(12000, remainRead);

        ResultSet rs = sharedPSConnection.createStatement().executeQuery(
            "select * from BlobTestblobtest2_ps");
        assertTrue(rs.next());
        InputStream readStuff = rs.getBlob("strm").getBinaryStream();
        int pos = 0;
        int ch;
        while ((ch = readStuff.read()) != -1) {
            assertEquals(arr[pos++] & 0xff, ch);
        }
        assertEquals(20000, pos);
    }

    @Test
    public void testClobWithLengthAndMultibyteCharacter() throws SQLException, IOException {
        Assume.assumeFalse(sharedUsePrepare()); //COM_STMT_SEND_LONG_DATA  is temporarily not supported, ps cannot be used
        PreparedStatement stmt = sharedConnection
            .prepareStatement("insert into BlobTestclobtest (id, strm) values (?,?)");
        String clob = "Øclob";
        stmt.setInt(1, 1);
        stmt.setClob(2, new StringReader(clob));
        stmt.execute();
        ResultSet rs = sharedConnection.createStatement().executeQuery(
            "select * from BlobTestclobtest");
        if (rs.next()) {
            Reader readStuff = rs.getClob("strm").getCharacterStream();
            char[] chars = new char[5];
            //noinspection ResultOfMethodCallIgnored
            readStuff.read(chars);
            assertEquals(new String(chars), clob);
        } else {
            fail();
        }
    }

    @Test
    public void testPSClobWithLengthAndMultibyteCharacter() throws SQLException, IOException {
        Assume.assumeFalse(sharedUsePrepare()); //COM_STMT_SEND_LONG_DATA  is temporarily not supported, ps cannot be used
        PreparedStatement stmt = sharedPSConnection
            .prepareStatement("insert into BlobTestclobtest_ps (id, strm) values (?,?)");
        String clob = "Øclob";
        stmt.setInt(1, 1);
        stmt.setClob(2, new StringReader(clob));
        stmt.execute();
        ResultSet rs = sharedPSConnection.createStatement().executeQuery(
            "select * from BlobTestclobtest_ps");
        if (rs.next()) {
            Reader readStuff = rs.getClob("strm").getCharacterStream();
            char[] chars = new char[5];
            //noinspection ResultOfMethodCallIgnored
            readStuff.read(chars);
            assertEquals(new String(chars), clob);
        } else {
            fail();
        }
    }

    @Test
    public void testClob3() throws Exception {
        Assume.assumeFalse(sharedUsePrepare()); //COM_STMT_SEND_LONG_DATA  is temporarily not supported, ps cannot be used
        PreparedStatement stmt = sharedConnection
            .prepareStatement("insert into BlobTestclobtest2 (strm) values (?)");
        java.sql.Clob clob = sharedConnection.createClob();
        Writer writer = clob.setCharacterStream(1);
        writer.write("Øhello", 0, 6);
        writer.flush();
        stmt.setClob(1, clob);
        stmt.execute();
        ResultSet rs = sharedConnection.createStatement().executeQuery(
            "select * from BlobTestclobtest2");
        assertTrue(rs.next());
        //        assertTrue(rs.getObject(1) instanceof Clob);
        String result = rs.getString(1);
        assertEquals("Øhello", result);
    }

    @Ignore
    //TODO FIXME
    @Test
    public void testPSClob3() throws Exception {
        PreparedStatement stmt = sharedPSConnection
            .prepareStatement("insert into BlobTestclobtest2_ps (strm) values (?)");
        java.sql.Clob clob = sharedPSConnection.createClob();
        Writer writer = clob.setCharacterStream(1);
        writer.write("Øhello", 0, 6);
        writer.flush();
        stmt.setClob(1, clob);
        stmt.execute();
        ResultSet rs = sharedPSConnection.createStatement().executeQuery(
            "select * from BlobTestclobtest2_ps");
        assertTrue(rs.next());
        assertTrue(rs.getObject(1) instanceof String);
        String result = rs.getString(1);
        assertEquals("Øhello", result);
    }

    @Test
    public void testBlob() throws SQLException, IOException {
        Assume.assumeFalse(sharedUsePrepare()); //COM_STMT_SEND_LONG_DATA  is temporarily not supported, ps cannot be used
        PreparedStatement stmt = sharedConnection
            .prepareStatement("insert into BlobTestblobtest (id, strm) values (?,?)");
        byte[] theBlob = { 1, 2, 3, 4, 5, 6 };
        InputStream stream = new ByteArrayInputStream(theBlob);
        stmt.setInt(1, 1);
        stmt.setBlob(2, stream);
        stmt.execute();
        ResultSet rs = sharedConnection.createStatement().executeQuery(
            "select * from BlobTestblobtest");
        assertTrue(rs.next());
        InputStream readStuff = rs.getBlob("strm").getBinaryStream();
        int ch;
        int pos = 0;
        while ((ch = readStuff.read()) != -1) {
            System.out.println((pos) + " --> " + ch + " and ");
            assertEquals(theBlob[pos++], ch);
        }

        readStuff = rs.getBinaryStream("strm");

        pos = 0;
        while ((ch = readStuff.read()) != -1) {
            assertEquals(theBlob[pos++], ch);
        }
    }

    @Ignore
    //TODO FIXME
    @Test
    public void testPSBlob() throws SQLException, IOException {
        PreparedStatement stmt = sharedPSConnection
            .prepareStatement("insert into BlobTestblobtest_ps (id, strm) values (?,?)");
        byte[] theBlob = { 1, 2, 3, 4, 5, 6 };
        InputStream stream = new ByteArrayInputStream(theBlob);
        stmt.setInt(1, 1);
        stmt.setBlob(2, stream);
        stmt.execute();
        ResultSet rs = sharedPSConnection.createStatement().executeQuery(
            "select * from BlobTestblobtest_ps");
        assertTrue(rs.next());
        InputStream readStuff = rs.getBlob("strm").getBinaryStream();
        int ch;
        int pos = 0;
        while ((ch = readStuff.read()) != -1) {
            System.out.println((pos) + " --> " + ch + " and ");
            assertEquals(theBlob[pos++], ch);
        }

        readStuff = rs.getBinaryStream("strm");

        pos = 0;
        while ((ch = readStuff.read()) != -1) {
            assertEquals(theBlob[pos++], ch);
        }
    }

    @Test
    public void testClobWithLength() throws SQLException, IOException {
        Assume.assumeFalse(sharedUsePrepare()); //COM_STMT_SEND_LONG_DATA  is temporarily not supported, ps cannot be used
        PreparedStatement stmt = sharedConnection
            .prepareStatement("insert into BlobTestclobtest3 (id, strm) values (?,?)");
        String clob = "clob";
        stmt.setInt(1, 1);
        stmt.setClob(2, new StringReader(clob));
        stmt.execute();
        ResultSet rs = sharedConnection.createStatement().executeQuery(
            "select * from BlobTestclobtest3");
        assertTrue(rs.next());
        Reader readStuff = rs.getClob("strm").getCharacterStream();
        char[] chars = new char[4];
        //noinspection ResultOfMethodCallIgnored
        readStuff.read(chars);
        assertEquals(new String(chars), clob);
    }

    @Ignore
    //TODO FIXME
    @Test
    public void testPSClobWithLength() throws SQLException, IOException {
        PreparedStatement stmt = sharedPSConnection
            .prepareStatement("insert into BlobTestclobtest3_ps (id, strm) values (?,?)");
        String clob = "clob";
        stmt.setInt(1, 1);
        stmt.setClob(2, new StringReader(clob));
        stmt.execute();
        ResultSet rs = sharedPSConnection.createStatement().executeQuery(
            "select * from BlobTestclobtest3_ps");
        assertTrue(rs.next());
        Reader readStuff = rs.getClob("strm").getCharacterStream();
        char[] chars = new char[4];
        //noinspection ResultOfMethodCallIgnored
        readStuff.read(chars);
        assertEquals(new String(chars), clob);
    }

    @Test
    public void testClob2() throws SQLException, IOException {
        Assume.assumeFalse(sharedUsePrepare()); //COM_STMT_SEND_LONG_DATA  is temporarily not supported, ps cannot be used
        PreparedStatement stmt = sharedConnection
            .prepareStatement("insert into BlobTestclobtest4 (id, strm) values (?,?)");
        java.sql.Clob clob = sharedConnection.createClob();
        OutputStream ostream = clob.setAsciiStream(1);
        byte[] bytes = "hello".getBytes();
        ostream.write(bytes);
        stmt.setInt(1, 1);
        stmt.setClob(2, clob);
        stmt.execute();
        ResultSet rs = sharedConnection.createStatement().executeQuery(
            "select * from BlobTestclobtest4");
        assertTrue(rs.next());
        Object obj = rs.getObject(2);
        //        assertTrue(rs.getObject(2) instanceof Clob);
        assertTrue(rs.getString(2).equals("hello"));
    }

    @Ignore
    //TODO FIXME
    @Test
    public void testPSClob2() throws SQLException, IOException {
        PreparedStatement stmt = sharedPSConnection
            .prepareStatement("insert into BlobTestclobtest4_ps (id, strm) values (?,?)");
        java.sql.Clob clob = sharedPSConnection.createClob();
        OutputStream ostream = clob.setAsciiStream(1);
        byte[] bytes = "hello".getBytes();
        ostream.write(bytes);
        stmt.setInt(1, 1);
        stmt.setClob(2, clob);
        stmt.execute();
        ResultSet rs = sharedPSConnection.createStatement().executeQuery(
            "select * from BlobTestclobtest4_ps");
        assertTrue(rs.next());
        assertTrue(rs.getObject(2) instanceof String);
        assertTrue(rs.getString(2).equals("hello"));
    }

    @Test
    public void blobSerialization() throws Exception {
        java.sql.Blob blob = new com.oceanbase.jdbc.Blob(new byte[] { 1, 2, 3 });
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(blob);

        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
        com.oceanbase.jdbc.Blob blob2 = (com.oceanbase.jdbc.Blob) ois.readObject();
        byte[] blobBytes = blob2.getBytes(1, (int) blob2.length());
        assertEquals(3, blobBytes.length);
        assertEquals(1, blobBytes[0]);
        assertEquals(2, blobBytes[1]);
        assertEquals(3, blobBytes[2]);

        java.sql.Clob clob = new com.oceanbase.jdbc.Clob(new byte[] { 1, 2, 3 });
        baos = new ByteArrayOutputStream();
        oos = new ObjectOutputStream(baos);
        oos.writeObject(clob);

        ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
        com.oceanbase.jdbc.Clob c2 = (com.oceanbase.jdbc.Clob) ois.readObject();
        blobBytes = c2.getBytes(1, (int) c2.length());
        assertEquals(3, blobBytes.length);
        assertEquals(1, blobBytes[0]);
        assertEquals(2, blobBytes[1]);
        assertEquals(3, blobBytes[2]);
    }

    @Test
    public void conj73() throws Exception {
        /* CONJ-73: Assertion error: UTF8 length calculation reports invalid ut8 characters */
        java.sql.Clob clob = new com.oceanbase.jdbc.Clob(new byte[] { (byte) 0x10, (byte) 0xD0,
                (byte) 0xA0, (byte) 0xe0, (byte) 0xa1, (byte) 0x8e });
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(clob);

        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
        com.oceanbase.jdbc.Clob c2 = (com.oceanbase.jdbc.Clob) ois.readObject();

        assertEquals(3, c2.length());
    }

    @Test
  public void conj77() throws Exception {

    byte[][] values = new byte[3][];
    values[0] = "".getBytes();
    values[1] = "hello".getBytes();
    values[2] = null;

    try (Statement sta1 = sharedConnection.createStatement()) {
      try {
        PreparedStatement pre =
                sharedConnection.prepareStatement(
                        "INSERT INTO conj77_test (Name,Archive) VALUES (?,?)");
        pre.setString(1, "1-Empty String");
        pre.setBytes(2, values[0]);
        //TODO to support batch()
        //pre.addBatch();
        pre.executeUpdate();

        pre.setString(1, "2-Data Hello");
        pre.setBytes(2, values[1]);
        //TODO to support batch()
        //pre.addBatch();
        pre.executeUpdate();

        pre.setString(1, "3-Empty Data null");
        pre.setBytes(2, values[2]);
        //TODO to support batch()
        //pre.addBatch();
        pre.executeUpdate();

        //TODO support batch()
        //pre.executeBatch();

      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    try (Statement sta2 = sharedConnection.createStatement()) {
//    try (Statement sta2 = conn.createStatement()) {
      try (ResultSet set = sta2.executeQuery("Select name,archive as text FROM conj77_test")) {
        int pos = 0;
        while (set.next()) {
          final java.sql.Blob blob = set.getBlob("text");
          if (blob != null) {

            try (ByteArrayOutputStream bout = new ByteArrayOutputStream((int) blob.length())) {
              try (InputStream bin = blob.getBinaryStream()) {
                final byte[] buffer = new byte[1024 * 4];
                for (int read = bin.read(buffer); read != -1; read = bin.read(buffer)) {
                  bout.write(buffer, 0, read);
                }
              }
              byte[] b = bout.toByteArray();
              assertArrayEquals(bout.toByteArray(), values[pos++]);
            }
          } else {
            if (values[pos] != null) {
              Assert.assertTrue(values[pos++].length == 0);
            } else {
              assertNull(values[pos++]);
            }
          }
        }
        assertEquals(pos, 3);
      }
    }
  }

    @Test
  public void conj77PS() throws Exception {

    byte[][] values = new byte[3][];
    values[0] = "".getBytes();
    values[1] = "hello".getBytes();
    values[2] = null;

    try (Statement sta1 = sharedPSConnection.createStatement()) {
      try {
        PreparedStatement pre =
                sharedPSConnection.prepareStatement(
                        "INSERT INTO conj77_test_ps (Name,Archive) VALUES (?,?)");
        pre.setString(1, "1-Empty String");
        pre.setBytes(2, values[0]);
        //TODO to support batch()
        //pre.addBatch();
        pre.executeUpdate();

        pre.setString(1, "2-Data Hello");
        pre.setBytes(2, values[1]);
        //TODO to support batch()
        //pre.addBatch();
        pre.executeUpdate();

        pre.setString(1, "3-Empty Data null");
        pre.setBytes(2, values[2]);
        //TODO to support batch()
        //pre.addBatch();
        pre.executeUpdate();

        //TODO support batch()
        //pre.executeBatch();

      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    try (Statement sta2 = sharedPSConnection.createStatement()) {
      try (ResultSet set = sta2.executeQuery("Select name,archive as text FROM conj77_test_ps")) {
        int pos = 0;
        while (set.next()) {
          final java.sql.Blob blob = set.getBlob("text");
          if (blob != null) {

            try (ByteArrayOutputStream bout = new ByteArrayOutputStream((int) blob.length())) {
              try (InputStream bin = blob.getBinaryStream()) {
                final byte[] buffer = new byte[1024 * 4];
                for (int read = bin.read(buffer); read != -1; read = bin.read(buffer)) {
                  bout.write(buffer, 0, read);
                }
              }
              assertArrayEquals(bout.toByteArray(), values[pos++]);
            }
          } else {
            if (values[pos] != null) {
              Assert.assertTrue(values[pos++].length == 0);
            } else {
              assertNull(values[pos++]);
            }
          }
        }
        assertEquals(pos, 3);
      }
    }
  }

    @Test
  public void sendEmptyBlobPreparedQuery() throws SQLException {
        Assume.assumeFalse(sharedUsePrepare()); //COM_STMT_SEND_LONG_DATA  is temporarily not supported, ps cannot be used
    createTable("emptyBlob", "test blob, test2 clob, test3 clob");
    //try (Connection conn = setConnection()) {
    //  PreparedStatement ps = conn.prepareStatement("insert into emptyBlob values(?,?,?)");
    try (PreparedStatement ps = sharedConnection.prepareStatement("insert into emptyBlob values(?,?,?)")) {
      ps.setBlob(1, new com.oceanbase.jdbc.Blob(new byte[0]));
      ps.setString(2, "a 'a ");
      ps.setNull(3, Types.VARCHAR);
      ps.executeUpdate();

      Statement stmt = sharedConnection.createStatement();
      ResultSet rs = stmt.executeQuery("select * from emptyBlob");
      assertTrue(rs.next());
      //assertEquals(0, rs.getBytes(1).length);
      assertEquals("a 'a ", rs.getString(2));
      assertNull(rs.getBytes(3));
    }
  }

    @Test
  public void sendEmptyBlobPreparedQueryPS() throws SQLException {
        Assume.assumeFalse(sharedUsePrepare()); //COM_STMT_SEND_LONG_DATA  is temporarily not supported, ps cannot be used
    createTable("emptyBlob_ps", "test blob, test2 clob, test3 clob");
    //try (Connection conn = setConnection()) {
    //  PreparedStatement ps = conn.prepareStatement("insert into emptyBlob values(?,?,?)");
    try (PreparedStatement ps = sharedPSConnection.prepareStatement("insert into emptyBlob_ps values(?,?,?)")) {
      ps.setBlob(1, new com.oceanbase.jdbc.Blob(new byte[0]));
      ps.setString(2, "a 'a ");
      ps.setNull(3, Types.VARCHAR);
      ps.executeUpdate();

      Statement stmt = sharedPSConnection.createStatement();
      ResultSet rs = stmt.executeQuery("select * from emptyBlob_ps");
      assertTrue(rs.next());
      //assertEquals(0, rs.getBytes(1).length);
      assertEquals("a 'a ", rs.getString(2));
      assertNull(rs.getBytes(3));
    }
  }

    @Test
    public void blobSerializationWithOffset() throws Exception {
        java.sql.Blob blob = new com.oceanbase.jdbc.Blob(new byte[] { 1, 2, 3, 4, 5 }, 1, 2);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(blob);

        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
        com.oceanbase.jdbc.Blob blob2 = (com.oceanbase.jdbc.Blob) ois.readObject();
        byte[] blobBytes = blob2.getBytes(1, (int) blob2.length());
        assertEquals(2, blobBytes.length);
        assertEquals(2, blobBytes[0]);
        assertEquals(3, blobBytes[1]);

        java.sql.Clob clob = new com.oceanbase.jdbc.Clob(new byte[] { 1, 2, 3, 4, 5 }, 1, 2);
        baos = new ByteArrayOutputStream();
        oos = new ObjectOutputStream(baos);
        oos.writeObject(clob);

        ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
        com.oceanbase.jdbc.Clob c2 = (com.oceanbase.jdbc.Clob) ois.readObject();
        blobBytes = c2.getBytes(1, (int) c2.length());
        assertEquals(2, blobBytes.length);
        assertEquals(2, blobBytes[0]);
        assertEquals(3, blobBytes[1]);
    }

    @Test
    public void blobDeserializationFilter() throws Exception {
        blobDeserializationFilterInternal(false, 0);
        if (System.getProperty("java.version").startsWith("9.")) {
            blobDeserializationFilterInternal(true, 2000);
            try {
                blobDeserializationFilterInternal(true, 500);
                fail("must have thrown exception, since filter limit is set lower than blob size");
            } catch (InvalidClassException e) {
                assertTrue(e.getMessage().contains("REJECTED"));
            }
        }
    }

    private void blobDeserializationFilterInternal(boolean addFilter, int filterSize)
      throws Exception {
    Assume.assumeTrue(System.getProperty("java.version").startsWith("9."));
    byte[] bb = new byte[1000];
    for (int i = 0; i < 1000; i++) {
      bb[i] = (byte) i;
    }
    com.oceanbase.jdbc.Blob blob = new com.oceanbase.jdbc.Blob(bb, 50, 750);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(bos);
    oos.writeObject(blob);
    oos.flush();
    oos.close();
    bos.close();

    byte[] data = bos.toByteArray();

    ByteArrayInputStream bis = new ByteArrayInputStream(data);
    ObjectInputStream ois = new ObjectInputStream(bis);

    if (addFilter) {
      // equivalent of :  (but permit compilation if java < 9)
      // ois.setObjectInputFilter(new ObjectInputFilter() {
      //  @Override
      //  public Status checkInput(FilterInfo filterInfo) {
      //      if (filterInfo.arrayLength() > 500) return Status.REJECTED;
      //      return Status.ALLOWED;
      //  }
      // });

      ClassLoader cl = BlobOracleTest.class.getClassLoader();
      Class<?> objectInputFilterClass = Class.forName("java.io.ObjectInputFilter");

      Object objectInputFilterImpl =
          Proxy.newProxyInstance(
              cl,
              new Class[] {objectInputFilterClass},
              (proxy, method, args) -> {
                Class<?> filterInfoClass = Class.forName("java.io.ObjectInputFilter$FilterInfo");
                Method arrayLengthMethod = filterInfoClass.getDeclaredMethod("arrayLength");
                Long arrayLength = (Long) arrayLengthMethod.invoke(args[0]);
                Class<?> statusClass = Class.forName("java.io.ObjectInputFilter$Status");
                Field rejected = statusClass.getField("REJECTED");
                Field allowed = statusClass.getField("ALLOWED");
                if (arrayLength > filterSize) {
                  return rejected.get(null);
                }
                return allowed.get(null);
              });

      Method setObjectInputFilterMethod =
          ObjectInputStream.class.getDeclaredMethod("setObjectInputFilter", objectInputFilterClass);
      setObjectInputFilterMethod.invoke(ois, objectInputFilterImpl);
    }

    com.oceanbase.jdbc.Blob resultBlob = (com.oceanbase.jdbc.Blob) ois.readObject();
    assertEquals(750, resultBlob.data.length);
    assertEquals(0, resultBlob.offset);
    assertEquals(750, resultBlob.length);

    byte[] blobBytes = resultBlob.getBytes(1, 1000);
    for (int i = 0; i < 750; i++) {
      assertEquals(bb[i + 50], blobBytes[i]);
    }
  }

    public java.sql.Blob writeDataToBlob(ResultSet rs, int column, byte[] data) throws SQLException {
        java.sql.Blob blob = rs.getBlob(column);
        if (blob == null) {
            throw new SQLException("Driver's Blob representation is null!");
        }
        if ((blob instanceof com.oceanbase.jdbc.Blob)) {
            ((com.oceanbase.jdbc.Blob) blob).setBytes(1L, data);
            return blob;
        }
        throw new SQLException("Driver's Blob representation is of an unsupported type: "
                               + blob.getClass().getName());
    }

    @Test
    public void insertBinaryInsert() throws SQLException {
        try {
            if (sharedUsePrepare()) {
                return;
            }
            String sql = "INSERT INTO oracleBinary (C1,BLO2,BLO3,CA) VALUES (?,?,?,?);";
            PreparedStatement preparedStatement = sharedConnection.prepareStatement(sql);
            String val = "绪平的测试";
            String val2 = "xupingtest";
            preparedStatement.setInt(1, 200318);
            preparedStatement.setBinaryStream(2, new ByteArrayInputStream(val2.getBytes()));
            preparedStatement.setBinaryStream(3, new ByteArrayInputStream(val2.getBytes()));
            preparedStatement.setString(4, "11111");
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            Assert.fail();
        }
    }

    @Test
    public void testEmpty() {
        try {
            Connection conn = setConnection("&usePieceData=true");
            Statement stmt = conn.createStatement();
            try {
                stmt.execute("drop table emptylob ");
            } catch (SQLException e) {
                //                e.printStackTrace();
            }
            stmt.execute("create  table emptylob(c1 blob,c2 clob)");
            PreparedStatement preparedStatement = null;
            preparedStatement = conn.prepareStatement("insert into emptylob values(?,?)");
            preparedStatement.setBlob(1, com.oceanbase.jdbc.Blob.getEmptyBLOB());
            preparedStatement.setClob(2, com.oceanbase.jdbc.Clob.getEmptyCLOB());
            preparedStatement.execute();
            preparedStatement = conn.prepareStatement("insert into emptylob values(?,?)");
            Blob blob = null;
            Clob clob = null;
            preparedStatement.setBlob(1, blob);
            preparedStatement.setClob(2, clob);
            preparedStatement.execute();
            ResultSet resultSet = stmt.executeQuery("select count(*) from emptylob ");
            Assert.assertTrue(resultSet.next());
            System.out.println("row count  = " + resultSet.getInt(1));
            Assert.assertEquals(2, resultSet.getInt(1));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testLobLocatorV2() {
        try {
            Connection conn = setConnectionOrigin("?useServerPrepStmts=true&usePieceData=true");
            String blobTest3 = "t_blob" + getRandomString(5);

            createTable(blobTest3, "id int, c1 blob, c2 blob, c3 blob, c4 blob, c5 blob, c6 blob");
            PreparedStatement ps = conn.prepareStatement("insert into " + blobTest3
                                                         + " values(1, ?, ?, ?, ?, ?, ?)");

            ps.setBinaryStream(1, null);
            ps.setBlob(2, (Blob) null);
            ps.setBinaryStream(3, new ByteArrayInputStream("".getBytes()));
            ps.setBlob(4, Blob.getEmptyBLOB());
            ps.setBinaryStream(5, new ByteArrayInputStream("1234abcd奥星贝斯".getBytes()));
            java.sql.Blob blob = conn.createBlob(); // oracle sends request
            OutputStream os = blob.setBinaryStream(1);
            for (int i = 0; i < 1024; i++) {
                os.write("abcde".getBytes(StandardCharsets.UTF_8));
            }
            os.flush();
            ps.setBlob(6, blob);
            ps.execute();

            ps = conn.prepareStatement("select c1, c2, c3, c4, c5, c6 from " + blobTest3);
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals(null, rs.getBytes(1));
            assertEquals(null, rs.getBytes(2));
            assertEquals(null, rs.getBytes(3));
            assertEquals(0, rs.getBytes(4).length);
            assertEquals("1234abcd奥星贝斯", new String(rs.getBytes(5)));
            String str = new String(rs.getBytes(6));
            assertEquals(str.length(), 5120);
            assertTrue(str.startsWith("abcde"));

            java.sql.Blob c5 = rs.getBlob(5);
            java.sql.Blob c6 = rs.getBlob(6);
            ps = conn.prepareStatement("update " + blobTest3 + " set c1 = ?, c2 = ? where id = 1");
            ps.setBlob(1, c5);
            ps.setBlob(2, c6);
            ps.execute();

            conn.setAutoCommit(false);
            ps = conn.prepareStatement("select c1, c2, c3, c4, c5, c6 from " + blobTest3
                                       + " where id = 1 for update");
            rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals("1234abcd奥星贝斯", new String(rs.getBytes(1)));
            str = new String(rs.getBytes(2));
            assertEquals(5120, str.length());
            assertTrue(str.startsWith("abcde"));
            assertEquals(null, rs.getBytes(3));
            assertEquals(0, rs.getBytes(4).length);
            assertEquals("1234abcd奥星贝斯", new String(rs.getBytes(5)));
            str = new String(rs.getBytes(6));
            assertEquals(str.length(), 5120);
            assertTrue(str.startsWith("abcde"));

            java.sql.Blob blob5 = rs.getBlob(5);
            java.sql.Blob blob6 = rs.getBlob(6);
            blob5.setBytes(1, "认真工作，live happily!".getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 10240; i++) {
                sb.append("1234abcd");
            }
            blob6.setBytes(1, sb.toString().getBytes(StandardCharsets.UTF_8));

            conn.commit();
            conn.setAutoCommit(true);
            ps = conn.prepareStatement("select c1, c2, c3, c4, c5, c6 from " + blobTest3);
            rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals("1234abcd奥星贝斯", new String(rs.getBytes(1)));
            str = new String(rs.getBytes(2));
            assertEquals(str.length(), 5120);
            assertTrue(str.startsWith("abcde"));
            assertEquals(null, rs.getBytes(3));
            assertEquals(0, rs.getBytes(4).length);
            assertEquals("认真工作，live happily!", new String(rs.getBytes(5)));
            str = new String(rs.getBytes(6));
            assertEquals(sb.toString(), str);
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testBlobINOutRow() throws SQLException, IOException {

        Connection conn = setConnectionOrigin("?useServerPrepStmts=true&usePieceData=true");
        String blobTest = "t_blob";
        createTable(blobTest, "c1 blob, c2 blob");
        PreparedStatement ps = conn.prepareStatement("insert into " + blobTest + " values(?,?)");

        int size1 = 1024 * 64;
        int size2 = 1024 * 65;
        java.sql.Blob blob = conn.createBlob();
        OutputStream os = blob.setBinaryStream(1);
        for (int i = 0; i < size1; i++) {
            os.write("a".getBytes());
        }
        os.flush();
        ps.setBlob(1, blob);

        java.sql.Blob blob1 = conn.createBlob();
        byte[] bytes = new byte[size2];
        for (int i = 0; i < size2; i++) {
            bytes[i] = 'b';
        }
        blob1.setBytes(1, bytes);
        ps.setBlob(2, blob1);
        ps.execute();

        ps = conn.prepareStatement("select * from " + blobTest);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(size1, rs.getBytes(1).length); // int-row

        byte[] bytes1 = rs.getBytes(2);
        assertEquals(size2, bytes1.length);
        assertArrayEquals(bytes, bytes1);
        assertEquals(size2, new String(bytes1).length());
        assertEquals(size2, rs.getBlob(2).length());
        assertEquals(size2, ((java.sql.Blob) rs.getObject(2)).length());
        //        assertEquals(size2, rs.getObject(2, Blob.class).length()); //oracle not supported
    }

    @Test
    public void fix48575083() {
        try {
            Connection conn = setConnectionOrigin("?useServerPrepStmts=true&usePieceData=true");
            //            Connection conn = setConnectionOrigin("?useServerPrepStmts=false");
            String blobTest3 = "t_blob" + getRandomString(5);

            createTable(blobTest3, "b1 blob");
            PreparedStatement ps = conn.prepareStatement("insert into " + blobTest3 + " values(?)");
            int size = 1024 * 65;
            java.sql.Blob blob = conn.createBlob();
            OutputStream os = blob.setBinaryStream(1);
            for (int i = 0; i < size; i++) {
                os.write("a".getBytes(StandardCharsets.UTF_8));
            }
            os.flush();
            ps.setBlob(1, blob);
            ps.execute();

            ps = conn.prepareStatement("select b1 from " + blobTest3);
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());

            java.sql.Blob var1 = rs.getBlob(1);
            long len1 = var1.length();
            String str1 = var1.toString();
            //                System.out.println("var1.length: " + len1);
            //                System.out.println("var1.toString: " + str1);
            Assert.assertEquals(size, len1);

            byte[] var2 = rs.getBytes(1);
            long len2 = var2.length;
            String str2 = var2.toString();
            //                System.out.println("var2.length: " + len2);
            //                System.out.println("var2.toString: " + str2);
            Assert.assertEquals(size, len2);

            try {
                java.sql.Blob var3 = rs.getObject(1, java.sql.Blob.class);
                long len3 = var3.length();
                String str3 = var3.toString();
                //                System.out.println("var3.length: " + len3);
                //                System.out.println("var3.toString: " + str3);
                Assert.assertEquals(size, len3);
            } catch (Exception e) {
                e.printStackTrace();
                Assert.assertTrue(e.getMessage().contains(
                    "Type class 'java.sql.Blob' is not supported"));
            }

            byte[] var4 = rs.getObject(1, byte[].class);
            long len4 = var4.length;
            String str4 = var4.toString();
            //                System.out.println("var4.length: " + len4);
            //                System.out.println("var4.toString: " + str4);
            Assert.assertEquals(size, len4);

            java.sql.Blob var5 = (java.sql.Blob) rs.getObject(1);
            long len5 = var5.length();
            String str5 = var5.toString();
            //                System.out.println("var5.length: " + len5);
            //                System.out.println("var5.toString: " + str5);
            Assert.assertEquals(size, len5);

            try {
                byte[] var6 = (byte[]) rs.getObject(1);
                long len6 = var6.length;
                String str6 = var6.toString();
                //                System.out.println("var6.length: " + len6);
                //                System.out.println("var6.toString: " + str6);
                Assert.assertEquals(size, len6);
            } catch (Exception e) {
                e.printStackTrace();
                Assert.assertTrue(e.getMessage().contains("Blob cannot be cast to "));
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void fix48638740() {
        try {
            Connection conn = setConnectionOrigin("?useServerPrepStmts=true&usePieceData=true");
            String blobTest = "t_blob";

            createTable(blobTest, "c1 blob, c2 blob");
            PreparedStatement ps = conn
                .prepareStatement("insert into " + blobTest + " values(?,?)");
            int size1 = 1024 * 64;
            int size2 = 1024 * 65;
            java.sql.Blob blob = conn.createBlob();
            OutputStream os = blob.setBinaryStream(1);
            for (int i = 0; i < size1; i++) {
                os.write("a".getBytes());
            }
            os.flush();
            ps.setBlob(1, blob);

            java.sql.Blob blob1 = conn.createBlob();
            byte[] bytes = new byte[size2];
            for (int i = 0; i < size2; i++) {
                bytes[i] = 'b';
            }
            blob1.setBytes(1, bytes);
            ps.setBlob(2, blob1);
            ps.execute();

            ps = conn.prepareStatement("select * from " + blobTest);
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals(size1, rs.getBytes(1).length); // in-row

            byte[] bytes1 = rs.getBytes(2);
            assertEquals(size2, bytes1.length);
            assertArrayEquals(bytes, bytes1);
            assertEquals(size2, new String(bytes1).length());
            assertEquals(size2, rs.getBlob(2).length());
            assertEquals(size2, ((java.sql.Blob) rs.getObject(2)).length());
            //        assertEquals(size2, rs.getObject(2, Blob.class).length()); //oracle not supported
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void fix48734085() {
        try {
            Connection conn = setConnectionOrigin("?useServerPrepStmts=true&usePieceData=true");
            String blobTest = "t_blob" + getRandomString(5);
            createTable(blobTest, "c1 blob, c2 blob");

            PreparedStatement ps = conn
                .prepareStatement("insert into " + blobTest + " values(?,?)");
            int size1 = 1024 * 64;
            java.sql.Blob blob1 = conn.createBlob();
            OutputStream os = blob1.setBinaryStream(1);
            for (int i = 0; i < size1; i++) {
                os.write("a".getBytes());
            }
            os.flush();
            ps.setBlob(1, blob1);

            int size2 = 1024 * 65;
            java.sql.Blob blob2 = conn.createBlob();
            byte[] bytes = new byte[size2];
            for (int i = 0; i < size2; i++) {
                bytes[i] = 'b';
            }
            blob2.setBytes(1, bytes);
            ps.setBlob(2, blob2);
            ps.execute();

            ps = conn.prepareStatement("select * from " + blobTest);
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());

            assertEquals(size1, rs.getBytes(1).length); // int-row
            assertArrayEquals(blob1.getBytes(1, size1), rs.getBlob(1).getBytes(1, size1));
            try {
                java.sql.Blob blob3 = rs.getBlob(1);
                blob3.setBytes(1, "abc".getBytes());
            } catch (SQLTransientConnectionException e) {
                assertTrue(e.getMessage().contains("-6256, Row has not been locked"));
            }

            byte[] bytes2 = rs.getBytes(2);
            assertEquals(size2, bytes2.length);
            assertArrayEquals(bytes, bytes2);
            assertArrayEquals(blob2.getBytes(1, size2), bytes2);
            assertEquals(size2, new String(bytes2).length());
            assertArrayEquals(bytes, rs.getBlob(2).getBytes(1, size2)); //read
            assertEquals(size2, rs.getBlob(2).length());
            assertEquals(size2, ((java.sql.Blob) rs.getObject(2)).length());
            assertEquals(size2, rs.getObject(2, Blob.class).length()); //oracle not supported

            conn.setAutoCommit(false);
            ps = conn.prepareStatement("select c1, c2 from " + blobTest + " for update");
            rs = ps.executeQuery();
            assertTrue(rs.next());
            java.sql.Blob blob3 = rs.getBlob(1);
            java.sql.Blob blob4 = rs.getBlob(2);

            blob3.setBytes(1, "abc中文ABC".getBytes(), 0, 10);
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 1024 * 10; i++) {
                sb.append("abc中文A");
            }
            blob4.setBytes(1, sb.toString().getBytes());
            conn.commit();
            conn.setAutoCommit(true);

            ps = conn.prepareStatement("select * from " + blobTest);
            rs = ps.executeQuery();
            assertTrue(rs.next());

            assertEquals(size1, rs.getBytes(1).length);
            assertTrue(new String(rs.getBytes(1)).startsWith("abc中文Aaaa"));

            byte[] bytes3 = rs.getBytes(2);
            assertEquals(102400, bytes3.length);
            assertEquals(102400, rs.getBlob(2).length());
            assertArrayEquals(sb.toString().getBytes(), bytes3);

            byte[] b4 = blob4.getBytes(1, 102400);
            assertArrayEquals(b4, bytes3);
            assertEquals(102400, b4.length);
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testBlob2() {
        try {
            Connection conn = setConnectionOrigin("?useServerPrepStmts=true&usePieceData=true");
            String blobTest = "t_blob";

            createTable(blobTest, "c1 blob, c2 blob, c3 blob");
            PreparedStatement ps = conn.prepareStatement("insert into " + blobTest
                                                         + " values(?,?,?)");

            java.sql.Blob blob = conn.createBlob();
            OutputStream outputStream = blob.setBinaryStream(1);
            outputStream.write("aaaaa".getBytes());
            outputStream.flush();
            outputStream.close();
            ps.setBlob(1, blob);
            blob.setBytes(1, "bbb".getBytes());
            ps.setBlob(2, blob);
            blob.setBytes(1, "ccccccc".getBytes());
            ps.setBlob(3, blob);
            ps.execute();

            ps = conn.prepareStatement("select * from " + blobTest);
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            byte[] bytes = rs.getBytes(1);
            assertEquals(7, bytes.length);
            for (int i = 0; i < bytes.length; i++) {
                assertEquals(99, bytes[i]);
            }
            bytes = rs.getBytes(2);
            assertEquals(7, bytes.length);
            for (int i = 0; i < bytes.length; i++) {
                assertEquals(99, bytes[i]);
            }
            bytes = rs.getBytes(3);
            assertEquals(7, bytes.length);
            for (int i = 0; i < bytes.length; i++) {
                assertEquals(99, bytes[i]);
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void aone52589180() {
        Assume.assumeFalse(sharedUsePrepare());
        try {
            Connection conn = sharedConnection;//setConnection("&useServerPrepStmts=false");
            createTable("t_blob", "c1 blob");
            String sql = "insert into t_blob values (?)";

            PreparedStatement ps = conn.prepareStatement(sql);
            int size = 1024 * 32;
            byte[] bytes = new byte[size];
            ps.setBytes(1, bytes);

            assertEquals(1, ps.executeUpdate());

            ResultSet rs = conn.prepareStatement("select * from t_blob").executeQuery();
            assertTrue(rs.next());
            assertEquals(size, rs.getBytes(1).length);
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testFunction() throws SQLException, FileNotFoundException {
        Assume.assumeFalse(sharedUsePrepare());
        Connection conn = sharedConnection;
        Statement stmt = conn.createStatement();
        try {
            stmt.execute("drop table test_version");
        } catch (SQLException e) {
        }
        stmt.execute("create table test_version (v0 number , v1 blob)");
        PreparedStatement pstmt = conn.prepareStatement("insert into test_version values(0,?)");
        pstmt.setBlob(1, new FileInputStream(new File(
            "src/test/resources/VacationRequest.bpmn20.xml")));
        pstmt.execute();
        ResultSet rs = stmt.executeQuery("select * from test_version where v0 = 0");
        rs.next();
        java.sql.Blob b1 = rs.getBlob("v1");
        System.out.println(b1.length());
        System.out.println(b1.getBytes(1, 5).length);
    }

    @Test
    public void testStreamResultForAone54492994() throws SQLException, IOException {
        Connection conn = sharedConnection;
        createTable("blobTest1", "c1 blob");
        String sql = "insert into  blobTest1 values (?)";
        PreparedStatement ps = conn.prepareStatement(sql);
        int size = 1024 * 5;
        byte[] bytes = new byte[size];
        ps.setBytes(1, bytes);
        assertEquals(1, ps.executeUpdate());

        conn = setConnection("&extendOracleResultSetClass=true");
        PreparedStatement preparedStatement = conn.prepareStatement("select c1 from blobTest1 ");
        preparedStatement.setFetchSize(Integer.MIN_VALUE);
        ResultSet rs = preparedStatement.executeQuery();
        assertTrue(rs.next());
        java.sql.Blob blob = rs.getBlob(1);
        InputStream stream = blob.getBinaryStream();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int n ;
        while (-1 != (n = stream.read(buffer))) {
            output.write(buffer, 0, n);
        }
        byte[] res = output.toByteArray();
        assertEquals(size, res.length);
    }

}
