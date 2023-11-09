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

import java.sql.*;
import java.util.Calendar;
import java.util.TimeZone;

import com.oceanbase.jdbc.OceanBaseConnection;
import com.oceanbase.jdbc.internal.protocol.Protocol;

public class TIMESTAMPLTZ extends Datum {
    public TIMESTAMPLTZ() {
        super(DataTypeUtilities.initTimestampltz());
    }

    public TIMESTAMPLTZ(byte[] bytes) {
        super(bytes);
    }

    public TIMESTAMPLTZ(Connection connection, Time time) throws SQLException {
        super(DataTypeUtilities.TIMESTMAPLTZToBytes(connection, time));
    }

    public TIMESTAMPLTZ(Connection connection, Date date) throws SQLException {
        super(DataTypeUtilities.TIMESTMAPLTZToBytes(connection, date));
    }

    public TIMESTAMPLTZ(Connection connection, Timestamp timestamp) throws SQLException {
        super(DataTypeUtilities.TIMESTMAPLTZToBytes(connection, timestamp));
    }

    public String toResultSetString(Connection connection) throws SQLException {
        return DataTypeUtilities.TIMESTMAPLTZToString(connection, getBytes(), true);
    }

    public byte[] toBytes() {
        return this.getBytes();
    }

    public static byte[] toBytes(Connection connection, String time) throws SQLException {
        return DataTypeUtilities.TIMESTMAPLTZToBytes(connection, Timestamp.valueOf(time));
    }

    public Date dateValue() throws SQLException {
        throw new SQLException("Conversion to Date failed");
    }

    public static Date toDate(Connection connection, byte[] bytes) throws SQLException {
        return new Date(DataTypeUtilities.getOriginTime(bytes,
            TimeZone.getTimeZone(((OceanBaseConnection) connection).getSessionTimeZone()), false));
    }

    public static Time toTime(Connection connection, byte[] bytes) throws SQLException {
        return new Time(DataTypeUtilities.getOriginTime(bytes,
            TimeZone.getTimeZone(((OceanBaseConnection) connection).getSessionTimeZone())));
    }

    public static Timestamp toTimestamp(Connection connection, byte[] bytes) throws SQLException {
        if (bytes.length < 12) {
            throw new SQLException("invalid bytes length");
        }
        Timestamp timestamp = new Timestamp(DataTypeUtilities.getOriginTime(bytes,
            TimeZone.getTimeZone(((OceanBaseConnection) connection).getSessionTimeZone())));
        timestamp.setNanos(DataTypeUtilities.getNanos(bytes, 7));
        return timestamp;
    }

    public static Timestamp toTimestamp(Protocol protocol, byte[] bytes) throws SQLException {
        if (bytes.length < 12) {
            throw new SQLException("invalid bytes length");
        }
        Timestamp timestamp = new Timestamp(DataTypeUtilities.getOriginTime(bytes,
            TimeZone.getTimeZone(protocol.getTimeZone().getID()), false));
        timestamp.setNanos(DataTypeUtilities.getNanos(bytes, 7));
        return timestamp;
    }

    public static Timestamp toTimestamp(Protocol protocol, byte[] bytes, boolean isResult)
                                                                                          throws SQLException {
        if (bytes.length < 12) {
            throw new SQLException("invalid bytes length");
        }
        Timestamp timestamp = new Timestamp(DataTypeUtilities.getOriginTime(bytes,
            TimeZone.getTimeZone(protocol.getTimeZone().getID()), !isResult));
        timestamp.setNanos(DataTypeUtilities.getNanos(bytes, 7));
        return timestamp;
    }

    public static TIMESTAMP toTIMESTAMP(Connection connection, byte[] bytes) throws SQLException {
        return new TIMESTAMP(toTimestamp(connection, bytes));
    }

    public static TIMESTAMP toTIMESTAMP(Protocol protocol, byte[] bytes) throws SQLException {
        return new TIMESTAMP(toTimestamp(protocol, bytes));
    }

    public static TIMESTAMP resultTIMESTAMP(Protocol protocol, byte[] bytes) throws SQLException {
        return new TIMESTAMP(toTimestamp(protocol, bytes, true));
    }

    public static TIMESTAMPTZ toTIMESTAMPTZ(Connection connection, byte[] bytes)
                                                                                throws SQLException {
        return new TIMESTAMPTZ(connection, toTimestamp(connection, bytes), Calendar.getInstance());
    }

    public Timestamp timestampValue(Connection connection) throws SQLException {
        return toTimestamp(connection, getBytes());
    }

    public String stringValue(Connection connection) throws SQLException {
        return DataTypeUtilities.TIMESTMAPLTZToString(connection, this.getBytes(), false);
    }

    public Date dateValue(Connection connection) throws SQLException {
        return toDate(connection, this.getBytes());
    }

    public Time timeValue(Connection connection) throws SQLException {
        return toTime(connection, this.getBytes());
    }

    public Object toJdbc() throws SQLException {
        return null;
    }

    public Object makeJdbcArray(int time) {
        Timestamp[] timestamps = new Timestamp[time];
        return timestamps;
    }

    public boolean isConvertibleTo(Class clazz) {
        return clazz.getName().compareTo("java.sql.Date") == 0
               || clazz.getName().compareTo("java.sql.Time") == 0
               || clazz.getName().compareTo("java.sql.Timestamp") == 0
               || clazz.getName().compareTo("java.lang.String") == 0;
    }

}
