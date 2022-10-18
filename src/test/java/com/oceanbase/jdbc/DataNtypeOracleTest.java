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

import static org.junit.Assert.*;

import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class DataNtypeOracleTest extends BaseOracleTest {

    @Test
    public void testGetNClob() throws Exception {
        Assume.assumeFalse(sharedUsePrepare()); //COM_STMT_SEND_LONG_DATA  is temporarily not supported, ps cannot be used
        createTable("testGetNClob", "id int not null primary key, str1 VARCHAR2(10)"); // , "CHARSET utf8"
        PreparedStatement stmt = sharedConnection
            .prepareStatement("insert into testGetNClob (id, str1) values (?,?)");
        NClob nclob = sharedConnection.createNClob();
        OutputStream stream = nclob.setAsciiStream(1);
        byte[] bytes = "hello".getBytes();
        stream.write(bytes);

        stmt.setInt(1, 1);
        stmt.setString(2, "hello");
        stmt.execute();

        ResultSet rs = sharedConnection.createStatement()
            .executeQuery("select * from testGetNClob");
        assertTrue(rs.next());
        assertTrue(rs.getObject(2) instanceof String);
        assertTrue(rs.getString(2).equals("hello"));
        NClob resultNClob = rs.getNClob(2);
        assertNotNull(resultNClob);
        assertEquals(5, resultNClob.getAsciiStream().available());
    }

    @Test
    public void testSetNClob() throws Exception {
        String tableName = "testSetNClob";
        Assume.assumeFalse(sharedUsePrepare()); //COM_STMT_SEND_LONG_DATA  is temporarily not supported, ps cannot be used
        createTable(tableName, "id int not null primary key, str1 CLOB "); // , "CHARSET utf8"
        PreparedStatement stmt = sharedConnection
            .prepareStatement("insert into testSetNClob (id, str1) values (?,?)");
        NClob nclob = sharedConnection.createNClob();
        String str = "hellohello";
        Assert.assertTrue(nclob.length() == 0);
        nclob.setString(1, str);
        Assert.assertTrue("nclob length : " + nclob.length(), nclob.length() == str.length());
        Assert.assertTrue("clob：" + nclob.getSubString(1, str.length()),
            str.equals(nclob.getSubString(1, str.length())));
        stmt.setInt(1, 1);
        stmt.setNClob(2, nclob);
        stmt.execute();

        ResultSet rs = sharedConnection.createStatement()
            .executeQuery("select * from testSetNClob");
        assertTrue(rs.next());
        assertFalse(rs.getObject(2) instanceof String);
        assertTrue(rs.getObject(2) instanceof Clob);
        assertTrue(rs.getString(2).equals(str));
        NClob resultNClob = rs.getNClob(2);
        assertNotNull(resultNClob);
        assertEquals(str.length(), resultNClob.getAsciiStream().available());
    }

    @Test
    public void testSetNString() throws Exception {

        createTable("testSetNString", "id int not null primary key, strm varchar(10)");
        PreparedStatement stmt = sharedConnection
            .prepareStatement("insert into testSetNString (id, strm) values (?,?)");
        stmt.setInt(1, 1);
        stmt.setNString(2, "hello");
        stmt.execute();

        ResultSet rs = sharedConnection.createStatement().executeQuery(
            "select * from testSetNString");
        assertTrue(rs.next());
        assertTrue(rs.getObject(2) instanceof String);
        assertTrue(rs.getNString(2).equals("hello"));
    }

    @Test
    public void testSetObjectNString() throws Exception {

        createTable("testSetObjectNString",
            "id int not null primary key, strm varchar(10), strm2 varchar(10)");
        PreparedStatement stmt = sharedConnection
            .prepareStatement("insert into testSetObjectNString (id, strm, strm2) values (?, ?, ?)");
        stmt.setInt(1, 2);
        stmt.setObject(2, "hello");
        stmt.setObject(3, "hello", Types.NCLOB);
        stmt.execute();

        ResultSet rs = sharedConnection.createStatement().executeQuery(
            "select * from testSetObjectNString");
        assertTrue(rs.next());
        assertTrue(rs.getObject(2) instanceof String);
        assertTrue(rs.getString(2).equals("hello"));
        assertTrue(rs.getObject(3) instanceof String);
        assertTrue(rs.getString(3).equals("hello"));
        assertEquals(5, rs.getNClob(2).getAsciiStream().available());
        assertEquals(5, rs.getNClob(3).getAsciiStream().available());
    }

    @Test
    public void testSetNCharacter() throws Exception {
        Assume.assumeFalse(sharedUsePrepare()); //COM_STMT_SEND_LONG_DATA  is temporarily not supported, ps cannot be used
        createTable("testSetNCharacter", "id int not null primary key, strm varchar2(50)");
        PreparedStatement stmt = sharedConnection
            .prepareStatement("insert into testSetNCharacter (id, strm) values (?,?)");
        String toInsert = "Øabcdefgh\njklmn\"";

        stmt.setInt(1, 1);
        stmt.setNCharacterStream(2, new StringReader(toInsert));
        stmt.execute();

        stmt.setInt(1, 2);
        stmt.setNCharacterStream(2, new StringReader(toInsert), 3);
        stmt.execute();

        ResultSet rs = sharedConnection.createStatement().executeQuery(
            "select * from testSetNCharacter");
        assertTrue(rs.next());
        assertTrue(rs.getObject(2) instanceof String);
        assertTrue(rs.getCharacterStream(2) instanceof Reader);
        checkCharStream(rs.getCharacterStream(2), toInsert);

        assertTrue(rs.next());
        checkCharStream(rs.getCharacterStream(2), toInsert.substring(0, 3));
    }

    private void checkCharStream(Reader reader, String comparedValue) throws Exception {

        StringBuilder sb = new StringBuilder();
        int ch;
        while ((ch = reader.read()) != -1) {
            sb.append((char) ch);
        }
        assertEquals(comparedValue, sb.toString());
    }

    @Test
    public void testSetObjectNCharacter() throws Exception {
        Assume.assumeFalse(sharedUsePrepare()); //COM_STMT_SEND_LONG_DATA  is temporarily not supported, ps cannot be used
        createTable("testSetObjectNCharacter", "id int not null primary key, strm varchar2(50)");
        createTable("testSetObjectNCharacter", "id int not null primary key, strm varchar2(50)");
        PreparedStatement stmt = sharedConnection
            .prepareStatement("insert into testSetObjectNCharacter (id, strm) values (?,?)");
        String toInsert = "Øabcdefgh\njklmn\"";

        stmt.setInt(1, 1);
        stmt.setObject(2, new StringReader(toInsert));
        stmt.execute();

        stmt.setInt(1, 2);
        stmt.setObject(2, new StringReader(toInsert), Types.LONGNVARCHAR);
        stmt.execute();

        stmt.setInt(1, 3);
        stmt.setObject(2, new StringReader(toInsert), Types.LONGNVARCHAR, 3);
        stmt.execute();

        ResultSet rs = sharedConnection.createStatement().executeQuery(
            "select * from testSetObjectNCharacter");
        assertTrue(rs.next());
        Reader reader1 = rs.getObject(2, Reader.class);
        assertNotNull(reader1);
        assertTrue(rs.getObject(2) instanceof String);
        assertTrue(rs.getCharacterStream(2) instanceof Reader);
        checkCharStream(rs.getCharacterStream(2), toInsert);

        assertTrue(rs.next());
        assertTrue(rs.getObject(2) instanceof String);
        checkCharStream(rs.getCharacterStream(2), toInsert);

        assertTrue(rs.next());
        checkCharStream(rs.getCharacterStream(2), toInsert.substring(0, 3));
    }

}
