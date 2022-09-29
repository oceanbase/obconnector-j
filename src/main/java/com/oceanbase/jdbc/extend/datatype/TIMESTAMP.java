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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

public class TIMESTAMP extends Datum implements Serializable {

    public TIMESTAMP() {
        super(DataTypeUtilities.initTimestamp());
    }

    public TIMESTAMP(byte[] bytes) {
        super(bytes);
    }

    public TIMESTAMP(Time time) {
        super(DataTypeUtilities.TIMESTMAPToBytes(time));
    }

    public TIMESTAMP(Date date) {
        super(DataTypeUtilities.TIMESTMAPToBytes(date));
    }

    public TIMESTAMP(Timestamp timestamp) {
        super(DataTypeUtilities.TIMESTAMPToBytes(timestamp));
    }

    public TIMESTAMP(Timestamp timestamp, Calendar calendar) {
        super(DataTypeUtilities.TIMESTAMPToBytes(timestamp, calendar));
    }

    public TIMESTAMP(String str) {
        super(toBytes(str));
    }

    public static Timestamp toTimestamp(byte[] bytes) throws SQLException {
        return DataTypeUtilities.innerToTimestamp(bytes, null);
    }

    public static Timestamp toTimestamp(byte[] bytes, Calendar calendar) throws SQLException {
        return DataTypeUtilities.innerToTimestamp(bytes, calendar);
    }

    public Timestamp timestampValue() throws SQLException {
        return toTimestamp(this.getBytes());
    }

    public Timestamp timestampValue(Calendar calendar) throws SQLException {
        return toTimestamp(this.getBytes(), calendar);
    }

    public static String toString(byte[] bytes) {
        return DataTypeUtilities.TIMESTMAPBytesToString(bytes);
    }

    public byte[] toBytes() {
        return this.getBytes();
    }

    public static byte[] toBytes(String str) {
        return DataTypeUtilities.TIMESTAMPToBytes(Timestamp.valueOf(str));
    }

    public Object toJdbc() throws SQLException {
        return this.timestampValue();
    }

    public Object makeJdbcArray(int len) {
        Timestamp[] timestamps = new Timestamp[len];
        return timestamps;
    }

    public boolean isConvertibleTo(Class clazz) {
        return clazz.getName().compareTo("java.sql.Date") == 0
               || clazz.getName().compareTo("java.sql.Time") == 0
               || clazz.getName().compareTo("java.sql.Timestamp") == 0
               || clazz.getName().compareTo("java.lang.String") == 0;
    }

    public String stringValue() {
        return toString(this.getBytes());
    }

    public String toString() {
        return this.stringValue();
    }

    public Date dateValue() throws SQLException {
        return DataTypeUtilities.toDate(this.getBytes());
    }

    public Time timeValue() throws SQLException {
        return DataTypeUtilities.toTime(this.getBytes());
    }

    private boolean isValid() {
        byte[] bytes = this.getBytes();
        if (bytes.length < DataTypeUtilities.TIMESTAMP_SIZE) {
            return false;
        } else {
            return DataTypeUtilities
                .isValid((bytes[0] & 0xff) * 100 + (bytes[1] & 0xff), bytes[2] & 0xff,
                    bytes[3] & 0xff, bytes[4] & 0xff, bytes[5] & 0xff, bytes[6] & 0xff);
        }
    }

    private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
        stream.defaultReadObject();
        if (!this.isValid()) {
            throw new IOException("Invalid TIMESTAMP");
        }
    }
}
