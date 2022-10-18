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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Calendar;

public abstract class Datum implements Serializable {

    protected byte[] data;

    public Datum() {
    }

    public Datum(byte[] bytes) {
        this.data = bytes;
    }

    public boolean equals(Object object) {
        if (this == object) {
            return true;
        } else if (object != null && object instanceof Datum) {
            if (this.getClass() == object.getClass()) {
                Datum datum = (Datum) object;
                if (this.data == null && datum.data == null) {
                    return true;
                } else if (this.data == null && datum.data != null || this.data != null
                           && datum.data == null) {
                    return false;
                } else if (this.data.length != datum.data.length) {
                    return false;
                } else {
                    for (int i = 0; i < this.data.length; ++i) {
                        if (this.data[i] != datum.data[i]) {
                            return false;
                        }
                    }
                    return true;
                }
            } else {
                return false;
            }
        }
        return true;
    }

    public byte[] shareBytes() {
        return this.data;
    }

    public long getLength() {
        return null == this.data ? 0L : (long) this.data.length;
    }

    public void setBytes(byte[] bytes) {
        int len = bytes.length;
        this.data = new byte[len];
        System.arraycopy(bytes, 0, this.data, 0, len);
    }

    public void setShareBytes(byte[] bytes) {
        this.data = bytes;
    }

    public byte[] getBytes() {
        if (this.data == null) {
            return new byte[0];
        } else {
            byte[] bytes = new byte[this.data.length];
            System.arraycopy(this.data, 0, bytes, 0, this.data.length);
            return bytes;
        }
    }

    public void setByte(int index, byte b) {
        if (index < this.data.length) {
            this.data[index] = b;
        }
    }

    public InputStream getStream() {
        return new ByteArrayInputStream(this.data);
    }

    public String stringValue() throws SQLException {
        throw new SQLException("Conversion to String failed");
    }

    public String stringValue(Connection connection) throws SQLException {
        return this.stringValue();
    }

    public boolean booleanValue() throws SQLException {
        throw new SQLException("Conversion to boolean failed");
    }

    public int intValue() throws SQLException {
        throw new SQLException("Conversion to integer failed");
    }

    public long longValue() throws SQLException {
        throw new SQLException("Conversion to long failed");
    }

    public float floatValue() throws SQLException {
        throw new SQLException("Conversion to float failed");
    }

    public double doubleValue() throws SQLException {
        throw new SQLException("Conversion to double failed");
    }

    public byte byteValue() throws SQLException {
        throw new SQLException("Conversion to byte failed");
    }

    public BigDecimal bigDecimalValue() throws SQLException {
        throw new SQLException("Conversion to BigDecimal failed");
    }

    public Date dateValue() throws SQLException {
        throw new SQLException("Conversion to Date failed");
    }

    public Time timeValue() throws SQLException {
        throw new SQLException("Conversion to Time failed");
    }

    public Time timeValue(Calendar calendar) throws SQLException {
        throw new SQLException("Conversion to Time failed");
    }

    public Timestamp timestampValue() throws SQLException {
        throw new SQLException("Conversion to Timestamp failed");
    }

    public Timestamp timestampValue(Calendar calendar) throws SQLException {
        throw new SQLException("Conversion to Timestamp failed");
    }

    public Reader characterStreamValue() throws SQLException {
        throw new SQLException("Conversion to character stream failed");
    }

    public InputStream asciiStreamValue() throws SQLException {
        throw new SQLException("Conversion to ascii stream failed");
    }

    public InputStream binaryStreamValue() throws SQLException {
        throw new SQLException("Conversion to binary stream failed");
    }

    public abstract boolean isConvertibleTo(Class targetClass);

    public abstract Object toJdbc() throws SQLException;

    public abstract Object makeJdbcArray(int intVal);
}
