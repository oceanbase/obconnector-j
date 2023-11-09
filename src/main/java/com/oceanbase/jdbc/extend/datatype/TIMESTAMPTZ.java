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

import com.oceanbase.jdbc.internal.protocol.Protocol;

public class TIMESTAMPTZ extends Datum {
    public TIMESTAMPTZ() {
        super(DataTypeUtilities.initTimestamptz());
    }

    public TIMESTAMPTZ(byte[] bytes) {
        super(bytes);
    }

    public TIMESTAMPTZ(Connection connection, Date date) throws SQLException {
        super(DataTypeUtilities.TIMESTAMPTZtoBytes(connection, date));
    }

    public TIMESTAMPTZ(Connection connection, Date date, Calendar calendar) throws SQLException {
        super(DataTypeUtilities.TIMESTAMPTZtoBytes(connection, date, calendar));
    }

    public TIMESTAMPTZ(Connection connection, Time time) throws SQLException {
        super(DataTypeUtilities.TIMESTAMPTZtoBytes(connection, time));
    }

    public TIMESTAMPTZ(Connection connection, Time time, Calendar calendar) throws SQLException {
        super(DataTypeUtilities.TIMESTAMPTZtoBytes(connection, time, calendar));
    }

    public TIMESTAMPTZ(Connection connection, Timestamp timestamp) throws SQLException {
        super(DataTypeUtilities.TIMESTAMPTZtoBytes(connection, timestamp));
    }

    public TIMESTAMPTZ(Connection connection, Timestamp timestamp, Calendar calendar)
                                                                                     throws SQLException {
        super(DataTypeUtilities.TIMESTAMPTZtoBytes(connection, timestamp, calendar));
    }

    public TIMESTAMPTZ(Connection connection, Timestamp timestamp, Calendar calendar,
                       boolean isTZTablesImported) throws SQLException {
        super(DataTypeUtilities.TIMESTAMPTZtoBytes(connection, timestamp, calendar,
            isTZTablesImported));
    }

    public TIMESTAMPTZ(Connection connection, String time) throws SQLException {
        super(DataTypeUtilities.TIMESTAMPTZtoBytes(connection, time));
    }

    public TIMESTAMPTZ(Connection connection, String time, Calendar calendar) throws SQLException {
        super(DataTypeUtilities.TIMESTAMPTZtoBytes(connection, time, calendar));
    }

    public static Date toDate(byte[] bytes) throws SQLException {
        if (bytes.length < 14) {
            throw new SQLException("invalid bytes length");
        }
        String tzStr = DataTypeUtilities.toTimezoneStr(bytes[12], bytes[13], "GMT", true);
        Calendar targetCalendar = Calendar.getInstance(TimeZone.getTimeZone(tzStr));

        Date date = new Date(DataTypeUtilities.getOriginTime(bytes, TimeZone.getTimeZone(tzStr)));
        targetCalendar.setTime(date);
        date.setTime(targetCalendar.getTime().getTime());

        return date;
    }

    public static Time toTime(Connection connection, byte[] bytes) throws SQLException {
        if (bytes.length < 14) {
            throw new SQLException("invalid bytes length");
        }
        String tzStr = DataTypeUtilities.toTimezoneStr(bytes[12], bytes[13], "GMT", true);
        Time time = new Time(DataTypeUtilities.getOriginTime(bytes, TimeZone.getTimeZone(tzStr),
            true));
        return time;
    }

    public static TIMESTAMP toTIMESTAMP(Protocol protocol, byte[] bytes) throws SQLException {
        return new TIMESTAMP(toTimestamp(protocol, bytes));
    }

    public static TIMESTAMP resultTIMESTAMP(Protocol protocol, byte[] bytes) throws SQLException {
        return new TIMESTAMP(toTimestamp(protocol, bytes, true));
    }

    public static TIMESTAMP toTIMESTAMP(Connection connection, byte[] bytes) throws SQLException {
        return new TIMESTAMP(toTimestamp(connection, bytes));
    }

    public Date dateValue() throws SQLException {
        return toDate(this.getBytes());
    }

    public Date dateValue(Connection conn) throws SQLException {
        return toDate(this.getBytes());
    }

    public Time timeValue() throws SQLException {
        return toTime(null, this.getBytes());
    }

    public static Timestamp toTimestamp(Protocol protocol, byte[] bytes) throws SQLException {
        if (bytes.length < 14) {
            throw new SQLException("invalid bytes length");
        }
        String tzStr = DataTypeUtilities.toTimezoneStr(bytes[12], bytes[13], "GMT", true);
        Timestamp timestamp = new Timestamp(DataTypeUtilities.getOriginTime(bytes,
            TimeZone.getTimeZone(tzStr)));
        timestamp.setNanos(DataTypeUtilities.getNanos(bytes, 7));
        return timestamp;
    }

    public static Timestamp toTimestamp(Protocol protocol, byte[] bytes, boolean isResult)
                                                                                          throws SQLException {
        if (bytes.length < 14) {
            throw new SQLException("invalid bytes length");
        }
        String tzStr = DataTypeUtilities.toTimezoneStr(bytes[12], bytes[13], "GMT", isResult);
        Timestamp timestamp = null;
        timestamp = new Timestamp(DataTypeUtilities.getOriginTime(bytes,
            TimeZone.getTimeZone(tzStr), !isResult));
        timestamp.setNanos(DataTypeUtilities.getNanos(bytes, 7));
        return timestamp;
    }

    public static Timestamp toTimestamp(Connection connection, byte[] bytes) throws SQLException {
        if (bytes.length < 14) {
            throw new SQLException("invalid bytes length");
        }
        String tzStr = DataTypeUtilities.toTimezoneStr(bytes[12], bytes[13], "GMT", true);
        Timestamp timestamp = new Timestamp(DataTypeUtilities.getOriginTime(bytes,
            TimeZone.getTimeZone(tzStr)));
        timestamp.setNanos(DataTypeUtilities.getNanos(bytes, 7));
        return timestamp;
    }

    public String toResultSetString(Connection connection) throws SQLException {
        return DataTypeUtilities.TIMESTAMPTZToString(connection, getBytes(), true);
    }

    public Timestamp timestampValue(Connection connection) throws SQLException {
        return toTimestamp(connection, this.getBytes());
    }

    public Timestamp timestampValue() throws SQLException {
        return toTimestamp((Connection) null, this.getBytes());
    }

    public byte[] toBytes() {
        return this.getBytes();
    }

    public String stringValue(Connection connection) throws SQLException {
        return DataTypeUtilities.TIMESTAMPTZToString(connection, this.getBytes(), false);
    }

    public String stringValue() throws SQLException {
        return DataTypeUtilities.TIMESTAMPTZToString(null, this.getBytes(), false);
    }

    public Time timeValue(Connection connection) throws SQLException {
        return toTime(connection, this.getBytes());
    }

    public Object toJdbc() throws SQLException {
        return null;
    }

    public Object makeJdbcArray(int temp) {
        Timestamp[] timestamps = new Timestamp[temp];
        return timestamps;
    }

    public boolean isConvertibleTo(Class claz) {
        return claz.getName().compareTo("java.sql.Date") == 0
               || claz.getName().compareTo("java.sql.Time") == 0
               || claz.getName().compareTo("java.sql.Timestamp") == 0
               || claz.getName().compareTo("java.lang.String") == 0;
    }

    private static int getHighOrderbits(int bits) {
        return (bits & 127) << 6;
    }

    private static int getLowOrderbits(int bits) {
        return (bits & 252) >> 2;
    }

}
