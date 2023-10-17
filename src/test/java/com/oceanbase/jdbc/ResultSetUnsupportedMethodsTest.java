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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import org.junit.Before;
import org.junit.Test;

public class ResultSetUnsupportedMethodsTest extends BaseTest {

    private ResultSet rs;

    @Before
    public void before() throws SQLException {
        rs = sharedConnection.createStatement().executeQuery("select 1");
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testGetRef() throws SQLException {
        rs.getRef(1);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testGetRef2() throws SQLException {
        rs.getRef("");
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testGetArray() throws SQLException {
        rs.getArray(1);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testInsertRow() throws SQLException {
        rs.insertRow();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testDeleteRow() throws SQLException {
        rs.deleteRow();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testUpdateRow() throws SQLException {
        rs.updateRow();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testRefreshRow() throws SQLException {
        rs.refreshRow();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testCancelRowUpdates() throws SQLException {
        rs.cancelRowUpdates();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testMoveToInsertRow() throws SQLException {
        rs.moveToInsertRow();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testMoveToCurrentRow() throws SQLException {
        rs.moveToCurrentRow();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateBinaryStream() throws SQLException {
        rs.updateBinaryStream(1, null, 0);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateBinaryStream2() throws SQLException {
        rs.updateBinaryStream("", null, 0);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateObject() throws SQLException {
        rs.updateObject(1, null, 0);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateObject2() throws SQLException {
        rs.updateObject("", null, 0);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateCharStream() throws SQLException {
        rs.updateCharacterStream(1, null, 0);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateCharStream2() throws SQLException {
        rs.updateCharacterStream("", null, 0);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateAsciiStream() throws SQLException {
        rs.updateAsciiStream(1, null, 0);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateAsciiStream2() throws SQLException {
        rs.updateAsciiStream("a", null, 0);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getRowUpdated() throws SQLException {
        rs.rowUpdated();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getRowDeleted() throws SQLException {
        rs.rowDeleted();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getRowInserted() throws SQLException {
        rs.rowInserted();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getCursorName() throws SQLException {
        rs.getCursorName();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateNull0() throws SQLException {
        rs.updateNull(1);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateNull1() throws SQLException {
        rs.updateNull("a");
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateBoolean2() throws SQLException {
        rs.updateBoolean(1, false);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateBoolean3() throws SQLException {
        rs.updateBoolean("a", false);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateByte4() throws SQLException {
        rs.updateByte(1, (byte) 1);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateByte5() throws SQLException {
        rs.updateByte("a", (byte) 1);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateShort6() throws SQLException {
        rs.updateShort(1, (short) 1);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateShort7() throws SQLException {
        rs.updateShort("a", (short) 1);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateInt8() throws SQLException {
        rs.updateInt(1, 1);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateInt9() throws SQLException {
        rs.updateInt("a", 1);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateLong10() throws SQLException {
        rs.updateLong(1, 1);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateLong11() throws SQLException {
        rs.updateLong("a", 1);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateFloat12() throws SQLException {
        rs.updateFloat(1, (float) 1);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateFloat13() throws SQLException {
        rs.updateFloat("a", (float) 1);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateDouble14() throws SQLException {
        rs.updateDouble(1, 1);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateDouble15() throws SQLException {
        rs.updateDouble("a", 1);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateBigDecimal16() throws SQLException {
        rs.updateBigDecimal(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateBigDecimal17() throws SQLException {
        rs.updateBigDecimal("a", null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateString18() throws SQLException {
        rs.updateString(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateString19() throws SQLException {
        rs.updateString("a", null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateBytes20() throws SQLException {
        rs.updateBytes(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateBytes21() throws SQLException {
        rs.updateBytes("a", null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateDate22() throws SQLException {
        rs.updateDate(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateDate23() throws SQLException {
        rs.updateDate("a", null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateTime24() throws SQLException {
        rs.updateTime(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateTime25() throws SQLException {
        rs.updateTime("a", null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateTimestamp26() throws SQLException {
        rs.updateTimestamp(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateTimestamp27() throws SQLException {
        rs.updateTimestamp("a", null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateAsciiStream28() throws SQLException {
        rs.updateAsciiStream(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateAsciiStream29() throws SQLException {
        rs.updateAsciiStream("a", null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateAsciiStream30() throws SQLException {
        rs.updateAsciiStream(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateAsciiStream31() throws SQLException {
        rs.updateAsciiStream("a", null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateAsciiStream32() throws SQLException {
        rs.updateAsciiStream(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateAsciiStream33() throws SQLException {
        rs.updateAsciiStream("a", null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateBinaryStream34() throws SQLException {
        rs.updateBinaryStream(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateBinaryStream35() throws SQLException {
        rs.updateBinaryStream("a", null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateBinaryStream36() throws SQLException {
        rs.updateBinaryStream(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateBinaryStream37() throws SQLException {
        rs.updateBinaryStream("a", null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateBinaryStream38() throws SQLException {
        rs.updateBinaryStream(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateBinaryStream39() throws SQLException {
        rs.updateBinaryStream("a", null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateCharacterStream40() throws SQLException {
        rs.updateCharacterStream(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateCharacterStream41() throws SQLException {
        rs.updateCharacterStream("a", null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateCharacterStream42() throws SQLException {
        rs.updateCharacterStream(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateCharacterStream43() throws SQLException {
        rs.updateCharacterStream("a", null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateCharacterStream44() throws SQLException {
        rs.updateCharacterStream(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateCharacterStream45() throws SQLException {
        rs.updateCharacterStream("a", null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateObject46() throws SQLException {
        rs.updateObject(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateObject47() throws SQLException {
        rs.updateObject(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateObject48() throws SQLException {
        rs.updateObject("a", null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateObject49() throws SQLException {
        rs.updateObject("a", null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateRef50() throws SQLException {
        rs.updateRef(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateRef51() throws SQLException {
        rs.updateRef("a", null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateBlob52() throws SQLException {
        rs.updateBlob(1, (java.sql.Blob) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateBlob53() throws SQLException {
        rs.updateBlob("a", (java.sql.Blob) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateBlob54() throws SQLException {
        rs.updateBlob(1, (java.io.InputStream) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateBlob55() throws SQLException {
        rs.updateBlob("a", (java.io.InputStream) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateBlob56() throws SQLException {
        rs.updateBlob(1, (java.io.InputStream) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateBlob57() throws SQLException {
        rs.updateBlob("a", (java.io.InputStream) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateClob58() throws SQLException {
        rs.updateClob(1, (java.sql.Clob) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateClob59() throws SQLException {
        rs.updateClob("a", (java.sql.Clob) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateClob60() throws SQLException {
        rs.updateClob(1, (java.io.Reader) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateClob61() throws SQLException {
        rs.updateClob("a", (java.io.Reader) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateClob62() throws SQLException {
        rs.updateClob(1, (java.io.Reader) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateClob63() throws SQLException {
        rs.updateClob("a", (java.io.Reader) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateArray64() throws SQLException {
        rs.updateArray(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateArray65() throws SQLException {
        rs.updateArray("a", null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateRowId66() throws SQLException {
        rs.updateRowId(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateRowId67() throws SQLException {
        rs.updateRowId("a", null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateNString68() throws SQLException {
        rs.updateNString(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateNString69() throws SQLException {
        rs.updateNString("a", null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateNClob70() throws SQLException {
        rs.updateNClob(1, (java.sql.NClob) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateNClob71() throws SQLException {
        rs.updateNClob("a", (java.sql.NClob) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateNClob72() throws SQLException {
        rs.updateNClob(1, (java.io.Reader) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateNClob73() throws SQLException {
        rs.updateNClob("a", (java.io.Reader) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateNClob74() throws SQLException {
        rs.updateNClob(1, (java.io.Reader) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateNClob75() throws SQLException {
        rs.updateNClob("a", (java.io.Reader) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateSqlXml76() throws SQLException {
        rs.updateSQLXML(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateSsqlXml77() throws SQLException {
        rs.updateSQLXML("a", null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateNCharacterStream78() throws SQLException {
        rs.updateNCharacterStream(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateNCharacterStream79() throws SQLException {
        rs.updateNCharacterStream("a", null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateNCharacterStream80() throws SQLException {
        rs.updateNCharacterStream(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testupdateNCharacterStream81() throws SQLException {
        rs.updateNCharacterStream("a", null);
    }
}
