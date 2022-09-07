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
    public static int HOUR_MILLISECOND;
    public static int MINUTE_MILLISECOND;

    public static int SIZE_TIMESTAMPTZ = 14;

    public TIMESTAMPTZ() {
        super(initTimestamptz());
    }

    public TIMESTAMPTZ(byte[] bytes) {
        super(bytes);
    }

    public TIMESTAMPTZ(Connection connection, Date date) throws SQLException {
        super(toBytes(connection, date));
    }

    public TIMESTAMPTZ(Connection connection, Date date, Calendar calendar) throws SQLException {
        super(toBytes(connection, date, calendar));
    }

    public TIMESTAMPTZ(Connection connection, Time time) throws SQLException {
        super(toBytes(connection, time));
    }

    public TIMESTAMPTZ(Connection connection, Time time, Calendar calendar) throws SQLException {
        super(toBytes(connection, time, calendar));
    }

    public TIMESTAMPTZ(Connection connection, Timestamp timestamp) throws SQLException {
        super(toBytes(connection, timestamp));
    }

    public TIMESTAMPTZ(Connection connection, Timestamp timestamp, Calendar calendar)
                                                                                     throws SQLException {
        super(toBytes(connection, timestamp, calendar));
    }

    public TIMESTAMPTZ(Connection connection, Timestamp timestamp, Calendar calendar,
                       boolean isTZTablesImported) throws SQLException {
        super(toBytes(connection, timestamp, calendar, isTZTablesImported));
    }

    public TIMESTAMPTZ(Connection connection, String time) throws SQLException {
        super(toBytes(connection, time));
    }

    public TIMESTAMPTZ(Connection connection, String time, Calendar calendar) throws SQLException {
        super(toBytes(connection, time, calendar));
    }

    public static Date toDate(byte[] bytes) throws SQLException {
        if (bytes.length < 14) {
            throw new SQLException("invalid bytes length");
        }
        String tzStr = toTimezoneStr(bytes[12], bytes[13], "GMT", true);
        Calendar targetCalendar = Calendar.getInstance(TimeZone.getTimeZone(tzStr));

        Date date = new Date(getOriginTime(bytes, TimeZone.getTimeZone(tzStr)));
        targetCalendar.setTime(date);
        date.setTime(targetCalendar.getTime().getTime());

        return date;
    }

    public static Time toTime(Connection connection, byte[] bytes) throws SQLException {
        if (bytes.length < 14) {
            throw new SQLException("invalid bytes length");
        }
        String tzStr = toTimezoneStr(bytes[12], bytes[13], "GMT", true);
        Time time = new Time(getOriginTime(bytes, TimeZone.getTimeZone(tzStr), true));
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
        String tzStr = toTimezoneStr(bytes[12], bytes[13], "GMT", true);
        Timestamp timestamp = new Timestamp(getOriginTime(bytes, TimeZone.getTimeZone(tzStr)));
        timestamp.setNanos(TIMESTAMP.getNanos(bytes, 7));
        return timestamp;
    }

    public static Timestamp toTimestamp(Protocol protocol, byte[] bytes, boolean isResult)
                                                                                          throws SQLException {
        if (bytes.length < 14) {
            throw new SQLException("invalid bytes length");
        }
        String tzStr = toTimezoneStr(bytes[12], bytes[13], "GMT", isResult);
        Timestamp timestamp = null;
        timestamp = new Timestamp(getOriginTime(bytes, TimeZone.getTimeZone(tzStr), !isResult));
        timestamp.setNanos(TIMESTAMP.getNanos(bytes, 7));
        return timestamp;
    }

    public static Timestamp toTimestamp(Connection connection, byte[] bytes) throws SQLException {
        if (bytes.length < 14) {
            throw new SQLException("invalid bytes length");
        }
        String tzStr = toTimezoneStr(bytes[12], bytes[13], "GMT", true);
        Timestamp timestamp = new Timestamp(getOriginTime(bytes, TimeZone.getTimeZone(tzStr)));
        timestamp.setNanos(TIMESTAMP.getNanos(bytes, 7));
        return timestamp;
    }

    public static long getOriginTime(byte[] bytes, TimeZone timeZone, boolean isTSResult)
                                                                                         throws SQLException {
        if (bytes.length < 7) {
            throw new SQLException("invalid bytes length");
        }

        int[] result = new int[7];
        int i;
        for (i = 0; i < 7; ++i) {
            result[i] = bytes[i] & 255;
        }

        i = result[0] * 100 + result[1];
        // If LTZ, convert to session timezone
        // If TZ, convert to the time zone returned by ob
        Calendar calendar = Calendar.getInstance(timeZone);
        calendar.clear();
        if (!isTSResult) {
            calendar.set(Calendar.YEAR, i);
            calendar.set(Calendar.MONTH, result[2] - 1);
            calendar.set(Calendar.DAY_OF_MONTH, result[3]);
        } else {
            calendar.set(Calendar.YEAR, 1970);
            calendar.set(Calendar.MONTH, 0);
            calendar.set(Calendar.DAY_OF_MONTH, 1);
        }
        calendar.set(Calendar.HOUR_OF_DAY, result[4]);
        calendar.set(Calendar.MINUTE, result[5]);
        calendar.set(Calendar.SECOND, result[6]);
        return calendar.getTimeInMillis();
    }

    public static long getOriginTime(byte[] bytes, TimeZone timeZone) throws SQLException {
        return getOriginTime(bytes, timeZone, false);
    }

    public static String toString(Connection connection, byte[] bytes, boolean isResult)
                                                                                        throws SQLException {
        if (bytes.length < 14) {
            throw new SQLException("invalid bytes length");
        }
        String tzStr = null;
        int tzNameLen = 0;
        if (bytes.length == 14) {
            tzNameLen = 0;
        } else {
            tzNameLen = bytes[14];
        }
        if (tzNameLen != 0) {
            // with the timezone information
            byte[] tmp = new byte[tzNameLen];
            System.arraycopy(bytes, 14 + 1, tmp, 0, tzNameLen);
            String tzName = new String(tmp);
            int abbrLen = bytes[14 + 1 + tzNameLen];
            tzStr = tzName;
        } else {
            tzStr = toTimezoneStr(bytes[12], bytes[13], "", isResult);
        }

        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(tzStr));
        calendar.setTimeInMillis(getOriginTime(bytes, TimeZone.getTimeZone(tzStr)));
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH) + 1;
        int day = calendar.get(Calendar.DAY_OF_MONTH);
        int hour = calendar.get(Calendar.HOUR_OF_DAY);
        int minute = calendar.get(Calendar.MINUTE);
        int second = calendar.get(Calendar.SECOND);
        int nanos = TIMESTAMP.getNanos(bytes, 7);

        return toString(year, month, day, hour, minute, second, nanos, bytes[11], tzStr);
    }

    public String toResultSetString(Connection connection) throws SQLException {
        return toString(connection, getBytes(), true);
    }

    public static final String toString(int year, int month, int day, int hour, int minute,
                                        int second, int nanos, int scale, String timezone) {
        String time = "" + year + "-" + toStr(month) + "-" + toStr(day) + " " + toStr(hour) + ":"
                      + toStr(minute) + ":" + toStr(second);
        if (nanos >= 0) {
            String temp = String.format("%09d", nanos);
            char[] chars = temp.toCharArray();

            int index;
            for (index = chars.length; index > 1 && chars[index - 1] == '0'; --index) {
                ;
            }
            temp = temp.substring(0, index);

            time = time + "." + temp;
        }

        if (timezone != null) {
            time = time + " " + timezone;
        }

        return time;
    }

    private static final String toStr(int temp) {
        return temp < 10 ? "0" + temp : Integer.toString(temp);
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

    public static byte[] toBytes(Connection connection, Date date) throws SQLException {
        return toBytes(connection, date, null);
    }

    public static byte[] toBytes(Connection connection, Date date, Calendar calendar)
                                                                                     throws SQLException {
        if (date == null) {
            return null;
        }
        if (null == calendar) {
            calendar = Calendar.getInstance();
        }
        Calendar localCalendar = Calendar.getInstance();
        byte[] resultBytes = new byte[SIZE_TIMESTAMPTZ];
        int offset = calendar.getTimeZone().getRawOffset();
        localCalendar.setTime(date);

        int year = calendar.get(Calendar.YEAR);
        if (year >= -4712 && year <= 9999) {
            resultBytes[0] = (byte) (localCalendar.get(Calendar.YEAR) / 100);
            resultBytes[1] = (byte) (localCalendar.get(Calendar.YEAR) % 100);
            resultBytes[2] = (byte) (localCalendar.get(Calendar.MONTH) + 1);
            resultBytes[3] = (byte) localCalendar.get(Calendar.DAY_OF_MONTH);
            resultBytes[4] = (byte) (0);
            resultBytes[5] = (byte) (0);
            resultBytes[6] = (byte) (0);
            TIMESTAMP.setNanos(resultBytes, 7, 0);
            resultBytes[12] = (byte) (offset / HOUR_MILLISECOND);
            resultBytes[13] = (byte) (offset < 0 ? (-offset % HOUR_MILLISECOND / MINUTE_MILLISECOND)
                : (offset % HOUR_MILLISECOND / MINUTE_MILLISECOND));
            return resultBytes;
        } else {
            throw new SQLException(String.format("error format, timestamp = %s", date.toString()),
                "268");
        }
    }

    public static byte[] toBytes(Connection connection, Time time) throws SQLException {
        return toBytes(connection, time, null);
    }

    public static byte[] toBytes(Connection connection, Time time, Calendar calendar)
                                                                                     throws SQLException {
        if (time == null) {
            return null;
        }
        if (null == calendar) {
            calendar = Calendar.getInstance();
        }
        Calendar localCalendar = Calendar.getInstance();
        byte[] resultBytes = new byte[SIZE_TIMESTAMPTZ];
        int offset = calendar.getTimeZone().getRawOffset();
        localCalendar.setTime(time);

        short base = 1970;
        localCalendar.set(Calendar.YEAR, base);
        localCalendar.set(Calendar.MONTH, 0);
        localCalendar.set(Calendar.DAY_OF_MONTH, 1);

        int year = calendar.get(Calendar.YEAR);
        if (year >= -4712 && year <= 9999) {
            resultBytes[0] = (byte) (localCalendar.get(Calendar.YEAR) / 100);
            resultBytes[1] = (byte) (localCalendar.get(Calendar.YEAR) % 100);
            resultBytes[2] = (byte) (localCalendar.get(Calendar.MONTH) + 1);
            resultBytes[3] = (byte) localCalendar.get(Calendar.DAY_OF_MONTH);
            resultBytes[4] = (byte) (localCalendar.get(Calendar.HOUR_OF_DAY));
            resultBytes[5] = (byte) (localCalendar.get(Calendar.MINUTE));
            resultBytes[6] = (byte) (localCalendar.get(Calendar.SECOND));
            TIMESTAMP.setNanos(resultBytes, 7, 0);
            resultBytes[12] = (byte) (offset / HOUR_MILLISECOND);
            resultBytes[13] = (byte) (offset < 0 ? (-offset % HOUR_MILLISECOND / MINUTE_MILLISECOND)
                : (offset % HOUR_MILLISECOND / MINUTE_MILLISECOND));
            return resultBytes;
        } else {
            throw new SQLException(String.format("error format, timestamp = %s", time.toString()),
                "268");
        }
    }

    public static byte[] toBytes(Connection connection, Timestamp timeStamp) throws SQLException {
        return toBytes(connection, timeStamp, null);
    }

    public static TIMESTAMPTZ toTIMESTAMPTZ(Timestamp timestamp, String timeZoneStr)
                                                                                    throws SQLException {
        if (timestamp == null) {
            return null;
        }
        if (timeZoneStr == null || timeZoneStr.isEmpty()) {
            throw new SQLException("illegal time zone");
        }
        // Timestamp uses the local time zone and needs to be converted to the specified time zone of timeZoneStr first
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(timestamp);
        calendar.setTimeZone(TimeZone.getTimeZone(timeZoneStr));
        byte[] resultBytes = new byte[SIZE_TIMESTAMPTZ];
        int offset = calendar.getTimeZone().getRawOffset();

        int year = calendar.get(Calendar.YEAR);
        if (year >= -4712 && year <= 9999) {
            resultBytes[0] = (byte) (calendar.get(Calendar.YEAR) / 100);
            resultBytes[1] = (byte) (calendar.get(Calendar.YEAR) % 100);
            resultBytes[2] = (byte) (calendar.get(Calendar.MONTH) + 1);
            resultBytes[3] = (byte) calendar.get(Calendar.DAY_OF_MONTH);
            resultBytes[4] = (byte) (calendar.get(Calendar.HOUR_OF_DAY));
            resultBytes[5] = (byte) (calendar.get(Calendar.MINUTE));
            resultBytes[6] = (byte) (calendar.get(Calendar.SECOND));
            TIMESTAMP.setNanos(resultBytes, 7, timestamp.getNanos());
            resultBytes[12] = (byte) (offset / HOUR_MILLISECOND);
            resultBytes[13] = (byte) (offset < 0 ? (-offset % HOUR_MILLISECOND / MINUTE_MILLISECOND)
                : (offset % HOUR_MILLISECOND / MINUTE_MILLISECOND));
        } else {
            throw new SQLException(String.format("error format, timestamp = %s",
                timestamp.toString()), "268");
        }
        return new TIMESTAMPTZ(resultBytes);
    }

    public static byte[] toBytes(Connection connection, Timestamp timestamp, Calendar calendar)
                                                                                               throws SQLException {
        return toBytes(connection, timestamp, calendar, false);
    }

    public static byte[] toBytes(Connection connection, Timestamp timestamp, Calendar calendar,
                                 boolean isTZTablesImported) throws SQLException {
        if (timestamp == null) {
            return null;
        }
        if (null == calendar) {
            calendar = Calendar.getInstance();
        }
        Calendar localCalendar = Calendar.getInstance();
        String tz = calendar.getTimeZone().getID();
        boolean isGMT = false;
        if (tz.startsWith("GMT")) {
            tz = tz.substring(3);
            isGMT = true;
        }
        int tzLen = tz.length();
        byte[] resultBytes = null;
        if (isTZTablesImported) {
            resultBytes = new byte[SIZE_TIMESTAMPTZ + tzLen + 2];
        } else {
            resultBytes = new byte[SIZE_TIMESTAMPTZ + 2];
        }
        int offset = calendar.getTimeZone().getRawOffset();
        localCalendar.setTime(timestamp);
        int year = localCalendar.get(Calendar.YEAR);
        if (year >= -4712 && year <= 9999) {
            resultBytes[0] = (byte) (localCalendar.get(Calendar.YEAR) / 100);
            resultBytes[1] = (byte) (localCalendar.get(Calendar.YEAR) % 100);
            resultBytes[2] = (byte) (localCalendar.get(Calendar.MONTH) + 1);
            resultBytes[3] = (byte) localCalendar.get(Calendar.DAY_OF_MONTH);
            resultBytes[4] = (byte) (localCalendar.get(Calendar.HOUR_OF_DAY));
            resultBytes[5] = (byte) (localCalendar.get(Calendar.MINUTE));
            resultBytes[6] = (byte) (localCalendar.get(Calendar.SECOND));
            TIMESTAMP.setNanos(resultBytes, 7, timestamp.getNanos());
            resultBytes[12] = (byte) (offset / HOUR_MILLISECOND);
            resultBytes[13] = (byte) (offset < 0 ? (-offset % HOUR_MILLISECOND / MINUTE_MILLISECOND)
                : (offset % HOUR_MILLISECOND / MINUTE_MILLISECOND));
            if (isTZTablesImported && !isGMT) {
                resultBytes[14] = (byte) (tzLen); // tz name len
                System.arraycopy(tz.getBytes(), 0, resultBytes, 15, tzLen);
                resultBytes[14 + tzLen + 1] = (byte) (0); // tz abbr len
            } else {
                resultBytes[14] = (byte) 0; // tz name len
                resultBytes[15] = (byte) 0; // tz abbr len
            }
            return resultBytes;
        } else {
            throw new SQLException(String.format("error format, timestamp = %s",
                timestamp.toString()), "268");
        }
    }

    public static byte[] toBytes(Connection connection, String time) throws SQLException {
        return toBytes(connection, Timestamp.valueOf(time));
    }

    public static byte[] toBytes(Connection connection, String time, Calendar calendar)
                                                                                       throws SQLException {
        return toBytes(connection, Timestamp.valueOf(time), calendar);
    }

    public String stringValue(Connection connection) throws SQLException {
        return toString(connection, this.getBytes(), false);
    }

    public String stringValue() throws SQLException {
        return toString(null, this.getBytes(), false);
    }

    public Time timeValue(Connection connection) throws SQLException {
        return toTime(connection, this.getBytes());
    }

    private static byte[] initTimestamptz() {
        byte[] result = new byte[14];
        result[0] = 19;
        result[1] = 70;
        result[2] = 1;
        result[3] = 1;
        result[4] = 1;
        result[5] = 1;
        result[6] = 1;
        result[7] = 0;
        result[8] = 0;
        result[9] = 0;
        result[10] = 0;
        result[11] = 0;
        result[12] = 0;
        return result;
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

    public static String toTimezoneStr(byte hour, byte minute, String pre, boolean isResult) {
        StringBuilder offsetTimeZone = new StringBuilder();
        boolean isPostive = true;
        if (hour <= -10) { // -12 ~ 10
            isPostive = false;
            offsetTimeZone.append(-hour);
        } else if (hour < 0) { // -9 ~ 0
            isPostive = false;
            offsetTimeZone.append(-hour);
        } else if (hour < 10) { // 1~9
            offsetTimeZone.append(hour);
        } else { // 10~12
            offsetTimeZone.append(hour);
        }

        offsetTimeZone.append(":");
        if (!isPostive && !isResult) { // not getXXX
            if (minute != 0) {
                offsetTimeZone.append("-");
            }
            if (minute <= -10) { // -59 ~ 10
                isPostive = false;
                offsetTimeZone.append(-minute);
            } else if (minute < 0) { // -9 ~ 0
                offsetTimeZone.append("0");
                offsetTimeZone.append(-minute);
            } else if (minute < 10) { // 1~9
                offsetTimeZone.append("0");
                offsetTimeZone.append(minute);
            } else { // 10~59
                offsetTimeZone.append(minute);
            }
        } else {
            if (minute <= -10) { // -59 ~ 10
                isPostive = false;
                offsetTimeZone.append(-minute);
            } else if (minute < 0) { // -9 ~ 0
                isPostive = false;
                offsetTimeZone.append("0");
                offsetTimeZone.append(-minute);
            } else if (minute < 10) { // 1~9
                offsetTimeZone.append("0");
                offsetTimeZone.append(minute);
            } else { // 10~59
                offsetTimeZone.append(minute);
            }
        }
        if (isResult) {
            return (pre == null ? "" : pre)
                   + (isPostive ? "+" + offsetTimeZone : "-" + offsetTimeZone);
        } else {
            return (pre == null ? "" : pre) + (isPostive ? offsetTimeZone : "-" + offsetTimeZone);
        }
    }

    static {
        HOUR_MILLISECOND = 3600000;
        MINUTE_MILLISECOND = 60000;
    }
}
