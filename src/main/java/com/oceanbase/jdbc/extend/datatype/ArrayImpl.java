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
package com.oceanbase.jdbc.extend.datatype;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;

import com.oceanbase.jdbc.ObArray;

public class ArrayImpl extends ComplexData implements ObArray {

    private int      cur_pos            = -1;
    public final int COLUMN_INDEX_INDEX = 1;
    public final int COLUMN_INDEX_VALUE = 2;

    public ArrayImpl(ComplexDataType type) {
        super(type);
    }

    @Override
    public String getBaseTypeName() throws SQLException {
        return this.getComplexType().getAttrType(0).getTypeName();
    }

    @Override
    public int getBaseType() throws SQLException {
        return this.getComplexType().getAttrType(0).getType();
    }

    @Override
    public Object getArray() throws SQLException {
        return this.getAttrData();
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
        return null;
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return this;
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map)
                                                                                   throws SQLException {
        return null;
    }

    @Override
    public void free() throws SQLException {
        this.setAttrData(null);
    }

    @Override
    public boolean next() throws SQLException {
        if (cur_pos + 1 < getAttrCount()) {
            cur_pos++;
            return true;
        }
        return false;
    }

    @Override
    public void close() throws SQLException {

    }

    @Override
    public boolean wasNull() throws SQLException {
        return getArray() == null;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        return (String) getObject(columnIndex);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return (Boolean) getObject(columnIndex);

    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return (Byte) getObject(columnIndex);
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        Object object = getObject(columnIndex);
        if (object instanceof BigDecimal) {
            return ((BigDecimal) object).shortValue();
        }
        return (Short) getObject(columnIndex);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        if (columnIndex == COLUMN_INDEX_INDEX) {
            return cur_pos + 1;
        }
        Object object = getObject(columnIndex);
        if (object instanceof BigDecimal) {
            return ((BigDecimal) object).intValue();
        }
        return (Integer) getObject(columnIndex);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        Object object = getObject(columnIndex);
        if (object instanceof BigDecimal) {
            return ((BigDecimal) object).longValue();
        }
        return (Long) getObject(columnIndex);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        Object object = getObject(columnIndex);
        if (object instanceof BigDecimal) {
            return ((BigDecimal) object).floatValue();
        }
        return (Float) getObject(columnIndex);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        Object object = getObject(columnIndex);
        if (object instanceof BigDecimal) {
            return ((BigDecimal) object).doubleValue();
        }
        return (Double) getObject(columnIndex);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return (BigDecimal) getObject(columnIndex);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        return (byte[]) getObject(columnIndex);
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return (Date) getObject(columnIndex);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return (Time) getObject(columnIndex);

    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return (Timestamp) getObject(columnIndex);
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        return (InputStream) getObject(columnIndex);
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return (InputStream) getObject(columnIndex);
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return (InputStream) getObject(columnIndex);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        if (columnIndex == COLUMN_INDEX_INDEX) {
            return cur_pos;
        } else if (columnIndex != COLUMN_INDEX_VALUE) {
            throw new SQLFeatureNotSupportedException();
        }
        if (this.attrData == null) {
            return null;
        }
        return this.attrData[cur_pos];
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        return (Reader) getObject(columnIndex);
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return (BigDecimal) getObject(columnIndex);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return cur_pos == -1;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return cur_pos >= getAttrCount();
    }

    @Override
    public boolean isFirst() throws SQLException {
        return cur_pos == 1;
    }

    @Override
    public boolean isLast() throws SQLException {
        return cur_pos == getAttrCount() - 1;
    }

    @Override
    public void beforeFirst() throws SQLException {
        cur_pos = -1;
    }

    @Override
    public void afterLast() throws SQLException {
        cur_pos = getAttrCount();
    }

    @Override
    public boolean first() throws SQLException {
        cur_pos = 0;
        return true;
    }

    @Override
    public boolean last() throws SQLException {
        cur_pos = getAttrCount() - 1;
        return true;
    }

    @Override
    public int getRow() throws SQLException {
        return cur_pos;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        if (row < getAttrCount()) {
            cur_pos = row;
            return true;
        }
        return false;
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        int tmp_pos = cur_pos + rows;
        if (tmp_pos >= 0 && tmp_pos < getAttrCount()) {
            cur_pos = tmp_pos;
            return true;
        }
        return false;
    }

    @Override
    public boolean previous() throws SQLException {
        if (cur_pos > 0) {
            --cur_pos;
            return true;
        }
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {

    }

    @Override
    public int getFetchDirection() throws SQLException {
        return 0;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {

    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getType() throws SQLException {
        return 0;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return 0;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        return false;
    }

    @Override
    public boolean rowInserted() throws SQLException {
        return false;
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        return false;
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length)
                                                                                throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length)
                                                                                 throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length)
                                                                                    throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void insertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void deleteRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void refreshRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Statement getStatement() throws SQLException {
        return null;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {

    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {

    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {

    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {

    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {

    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {

    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {

    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {

    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {

    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {

    }

    @Override
    public int getHoldability() throws SQLException {
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {

    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {

    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {

    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {

    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {

    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {

    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {

    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length)
                                                                                      throws SQLException {

    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length)
                                                                                 throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length)
                                                                                  throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length)
                                                                                     throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length)
                                                                                 throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length)
                                                                                    throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return null;
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return null;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
