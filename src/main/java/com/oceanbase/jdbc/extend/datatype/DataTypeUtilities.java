/*
 *
 * OceanBase Client for Java
 *
 * Copyright (c) 2022 OceanBase.
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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.InvalidParameterException;
import java.sql.*;
import java.util.Calendar;
import java.util.StringTokenizer;
import java.util.TimeZone;

import com.oceanbase.jdbc.OceanBaseConnection;

public class DataTypeUtilities {
    private final static int  INTERVALDS_BYTE_SIZE         = 14;
    private static int        INTERVALYM_BYTE_SIZE         = 7;
    private static int        MAX_LEADPREC_VALUE           = 9;
    private static int        MAX_HOUR_VALUE               = 23;
    private static int        MAX_MINUTE_VALUE             = 59;
    private static int        MAX_SECOND_VALUE             = 59;

    private static int        MASK_VALUE                   = 0xff;
    private static int        MAX_YEAR_PREC_VALUE          = 9;
    private static int        MAX_MONTH_VALUE              = 12;
    public static final int[] dayValuePerMonth             = new int[] { 31, 28, 31, 30, 31, 30,
            31, 31, 30, 31, 30, 31                        };
    public static int         TIMESTAMP_SIZE               = 12;
    private static int        TIMESTAMPLTZ_SIZE            = 12;
    public static int         TIMESTAMPTZ_SIZE             = 14;
    private static int        MIN_YEAR_VALUE               = -4712;
    private static int        MAX_YEAR_VALUE               = 9999;
    public static int         HOUR_MILLISECOND;
    public static int         MINUTE_MILLISECOND;
    public static int         GREGORIAN_CALENDAR_YEAR      = 1582;
    public static int         GREGORIAN_CALENDAR_MONTH     = 10;
    public static int         GREGORIAN_CALENDAR_DAY_START = 5;
    public static int         GREGORIAN_CALENDAR_DAY_END   = 15;

    public enum TIMETYPE {
        TIME,
        TIMESTMAP,
        DATE,
        NONE
    }

    public static class Ret {
        String str;
        int    is_negative;

        public String getStr() {
            return str;
        }

        public int getIs_negative() {
            return is_negative;
        }

        public void setStr(String str) {
            this.str = str;
        }

        public void setIs_negative(int is_negative) {
            this.is_negative = is_negative;
        }

        public Ret() {
            this.is_negative = 0;
            this.str = "";
        }
    }

    public static class IntervalDsParts {
        public int    day;
        public int    hour;
        public int    minute;
        public int    second;
        public int    fractional_second       = 0;
        public int    fractional_second_scale = 0;
        public String dayStr                  = "";

        public IntervalDsParts(int day, int hour, int minute, int second, int fractional_second,
                               String dayStr) {
            this.day = day;
            this.hour = hour;
            this.minute = minute;
            this.second = second;
            this.fractional_second = fractional_second;
            this.dayStr = dayStr;
            this.fractional_second_scale = 0;
        }

        public int getDay() {
            return day;
        }

        public int getHour() {
            return hour;
        }

        public int getMinute() {
            return minute;
        }

        public int getSecond() {
            return second;
        }

        public int getFractional_second() {
            return fractional_second;
        }

        public int getFractional_second_scale() {
            return fractional_second_scale;
        }

        public void setDay(int day) {
            this.day = day;
        }

        public void setHour(int hour) {
            this.hour = hour;
        }

        public void setMinute(int minute) {
            this.minute = minute;
        }

        public void setSecond(int second) {
            this.second = second;
        }

        public void setFractional_second(int fractional_second) {
            this.fractional_second = fractional_second;
        }

        public void setFractional_second_scale(int fractional_second_scale) {
            this.fractional_second_scale = fractional_second_scale;
        }

        public void setDayStr(String dayStr) {
            this.dayStr = dayStr;
        }

        public IntervalDsParts(int day, int hour, int minute, int second, int fractional_second,
                               String dayStr, int fractional_second_scale) {
            this.day = day;
            this.hour = hour;
            this.minute = minute;
            this.second = second;
            this.fractional_second = fractional_second;
            this.dayStr = dayStr;
            this.fractional_second_scale = fractional_second_scale;
        }

    }

    public static final int getInt(byte[] bytes, int idx) {
        return ByteBuffer.wrap(bytes, idx, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }

    public static float bytesToFloat(byte[] bytes) {
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getFloat();
    }

    public static double bytesToDouble(byte[] bytes) {
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getDouble();
    }

    public static byte[] intToBytes(int intVal) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(intVal).array();
    }

    public static byte[] doubleToBytes(double doubleVal) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putDouble(doubleVal).array();
    }

    static byte[] floatToBytes(float floatVal) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putFloat(floatVal).array();
    }

    public static float stringToFloat(String stringVal) {
        try {
            return Float.valueOf(stringVal);
        } catch (NumberFormatException e) {
            throw e;
        }
    }

    public static double stringToDouble(String stringVal) throws SQLException {
        try {
            return Double.valueOf(stringVal);
        } catch (NumberFormatException numberFormatException) {
            throw numberFormatException;
        }
    }

    public static String getSessionTimeZone(Connection connection) throws SQLException {
        if (connection instanceof OceanBaseConnection) {
            return ((OceanBaseConnection) connection).getSessionTimeZone();
        } else {
            throw new SQLException("unexpected connection type");
        }
    }

    public static Ret getOriginStringVal(String str) {
        Ret ret = new Ret();
        String trimStr = str.trim();
        char ch = trimStr.charAt(0);
        int pos;
        if (ch != '-' && ch != '+') {
            pos = 0;
        } else {
            pos = 1;
            if (ch == '-') {
                ret.setIs_negative(1);
            }
        }
        String subStr = trimStr.substring(pos);
        ret.setStr(subStr);
        return ret;
    }

    public static IntervalDsParts getIntervalDsPartsValue(String str, String origin) {
        int day;
        int hour = 0;
        int minute = 0;
        int second = 0;
        int fractional_second = 0;
        int index = str.indexOf(' '); //find pos of blanket
        String dayStr = str.substring(0, index);
        if (dayStr.length() > MAX_LEADPREC_VALUE) {
            throw new NumberFormatException("invalid daylen " + dayStr);
        } else {
            day = Integer.valueOf(dayStr);
            String subStr = str;
            subStr = subStr.substring(index + 1);
            StringTokenizer tokenizer = new StringTokenizer(subStr, ":.");
            if (tokenizer.hasMoreTokens()) {
                String hourStr;
                String minuteStr;
                String secondStr;
                String fracSecondStr;
                try {
                    hourStr = tokenizer.nextToken();
                    minuteStr = tokenizer.nextToken();
                    secondStr = tokenizer.nextToken();
                    try {
                        fracSecondStr = tokenizer.nextToken();
                    } catch (Exception ex) {
                        fracSecondStr = null;
                    }
                } catch (Exception ex) {
                    throw new NumberFormatException("invalid format " + origin);
                }
                hour = Integer.valueOf(hourStr);
                minute = Integer.valueOf(minuteStr);
                second = Integer.valueOf(secondStr);
                checkValidParam("hour", hour, 0, MAX_HOUR_VALUE, false);
                checkValidParam("minute", minute, 0, MAX_MINUTE_VALUE, false);
                checkValidParam("second", second, 0, MAX_SECOND_VALUE, false);
                if (fracSecondStr != null) {
                    if (fracSecondStr.length() > MAX_LEADPREC_VALUE) {
                        throw new NumberFormatException("invalid fracsecond length "
                                                        + fracSecondStr + " in " + origin);
                    }
                    fractional_second = Integer.valueOf(fracSecondStr);
                    int lenDiff = MAX_LEADPREC_VALUE - fracSecondStr.length();
                    for (int i = 0; i < lenDiff; i++) {
                        fractional_second *= 10;
                    }
                }
            }
        }
        return new IntervalDsParts(day, hour, minute, second, fractional_second, dayStr);
    }

    public static byte[] intervalDsToBytes(String str) {
        if (str == null) {
            return null;
        } else {
            byte[] bytes = new byte[INTERVALDS_BYTE_SIZE];
            Ret ret = getOriginStringVal(str);
            String subStr = ret.getStr();
            IntervalDsParts intervalDsParts = getIntervalDsPartsValue(subStr, str);
            bytes[0] = (byte) (ret.getIs_negative() & 0xff);
            byte[] tmpBytes = DataTypeUtilities.intToBytes(intervalDsParts.day);
            System.arraycopy(tmpBytes, 0, bytes, 1, 4);
            bytes[5] = (byte) (intervalDsParts.hour & 0xff);
            bytes[6] = (byte) (intervalDsParts.minute & 0xff);
            bytes[7] = (byte) (intervalDsParts.second & 0xff);
            tmpBytes = DataTypeUtilities.intToBytes(intervalDsParts.fractional_second);
            System.arraycopy(tmpBytes, 0, bytes, 8, 4);
            bytes[12] = (byte) (intervalDsParts.dayStr.length() & 0xff);
            bytes[13] = (byte) (MAX_LEADPREC_VALUE);
            return bytes;
        }
    }

    public static String formatIntervalDsString(int day, int hour, int minute, int second,
                                                int fractional_second, int fractional_second_scale,
                                                int is_negative) {
        String format = String.format("%%d %%d:%%d:%%d.%%0%dd", fractional_second_scale);
        String result = String.format(format, day, hour, minute, second, fractional_second);
        int endIndex = result.length() - 1;
        for (int i = result.length() - 1; result.charAt(i) != '.'; i--) {
            if (result.charAt(i) != '0') {
                endIndex = i;
                break;
            }
        }
        result = result.substring(0, endIndex + 1);
        return is_negative == 0 ? result : "-" + result;
    }

    public static void checkValidParam(String msg, int val, int min, double max, boolean maxIsDouble) {
        if (msg.equals("month")) {
            if (val < min || val >= max) {
                throw new NumberFormatException(msg + " " + val
                                                + " is not valid，should not exceed " + max);
            }
        } else {
            if (val < min || val > max) {
                if (maxIsDouble) {
                    throw new NumberFormatException(msg + " " + val
                                                    + " is not valid，should not exceed " + max);
                } else {
                    throw new NumberFormatException(msg + " " + val
                                                    + " is not valid，should not exceed "
                                                    + new Double(max).intValue());
                }

            }
        }
    }

    public static void intervalExceptionAction(boolean isIntervalDS, int day, int hour, int minute,
                                               int second, int fractional_second,
                                               int fractional_second_scale, int year, int month) {
        if (isIntervalDS) {
            checkValidParam("day", day, 0, Math.pow(10, MAX_LEADPREC_VALUE), true);
            checkValidParam("hour", hour, 0, MAX_HOUR_VALUE, false);
            checkValidParam("minute", minute, 0, MAX_MINUTE_VALUE, false);
            checkValidParam("second", second, 0, MAX_SECOND_VALUE, false);
            checkValidParam("fractional_second", fractional_second, 0,
                Math.pow(10, MAX_LEADPREC_VALUE), true);
            checkValidParam("fractional_second_scale", fractional_second_scale, 0,
                MAX_LEADPREC_VALUE, false);
        } else {
            checkValidParam("year", year, 0, Math.pow(10, MAX_YEAR_PREC_VALUE), true);
            checkValidParam("month", month, 0, Math.pow(10, MAX_YEAR_PREC_VALUE), true);
        }
    }

    public static String intervalDsToString(byte[] data) {
        if (data == null || data.length == 0) {
            return "";
        } else {
            if (data.length != INTERVALDS_BYTE_SIZE) {
                throw new InvalidParameterException("invalid len:" + data.length);
            }
            Ret ret = new Ret();
            ret.setIs_negative(data[0] & 0xff);
            IntervalDsParts intervalDsParts = new IntervalDsParts(
                DataTypeUtilities.getInt(data, 1), data[5] & 0xff, data[6] & 0xff, data[7] & 0xff,
                DataTypeUtilities.getInt(data, 8), "", data[13] & 0xff);
            intervalExceptionAction(true, intervalDsParts.getDay(), intervalDsParts.getHour(),
                intervalDsParts.getMinute(), intervalDsParts.getSecond(),
                intervalDsParts.getFractional_second(),
                intervalDsParts.getFractional_second_scale(), 0, 0);
            if (intervalDsParts.getFractional_second_scale() < MAX_LEADPREC_VALUE) {
                intervalDsParts
                    .setFractional_second(intervalDsParts.getFractional_second()
                                          / (int) Math.pow(
                                              10,
                                              MAX_LEADPREC_VALUE
                                                      - intervalDsParts
                                                          .getFractional_second_scale()));
            }
            return formatIntervalDsString(intervalDsParts.getDay(), intervalDsParts.getHour(),
                intervalDsParts.getMinute(), intervalDsParts.getSecond(),
                intervalDsParts.getFractional_second(),
                intervalDsParts.getFractional_second_scale(), ret.getIs_negative());
        }
    }

    public static byte[] intervalYmToBytes(String str) {
        if (str != null) {
            byte[] bytes = new byte[INTERVALYM_BYTE_SIZE];
            int year;
            int month;
            Ret ret = getOriginStringVal(str);
            String subStr = ret.getStr();
            int index = subStr.indexOf('-'); //find pos of blanket
            String yearStr = subStr.substring(0, index);
            if (yearStr.length() > MAX_YEAR_PREC_VALUE) {
                throw new NumberFormatException("invalid year " + yearStr + " in " + str);
            } else {
                String monthStr = subStr.substring(index + 1);
                year = Integer.valueOf(yearStr);
                month = Integer.valueOf(monthStr);
                if (month >= MAX_MONTH_VALUE) {
                    throw new NumberFormatException("invalid month " + month + " in " + str);
                }
                bytes[0] = (byte) (ret.getIs_negative() & 0xff);
                byte[] tmpBytes = DataTypeUtilities.intToBytes(year);
                System.arraycopy(tmpBytes, 0, bytes, 1, 4);
                bytes[5] = (byte) (month & 0xff);
                bytes[6] = (byte) (yearStr.length());
            }
            return bytes;
        } else {
            return null;
        }
    }

    public static String formatIntervalYmString(int year, int month, int year_scale, int is_negative) {
        String result;
        if (year_scale != 0) {
            result = String.format("%" + year_scale + "d-%2d", year, month);
        } else {
            result = String.format("0-%2d", month);
        }
        String[] s = result.split(" ");
        if (s.length > 1) {
            result = "";
            for (int i = 0; i < s.length; i++) {
                result += s[i];
            }
        }
        if (is_negative == 0) {
            return result;
        } else {
            return "-" + result;
        }
    }

    public static String intervalYmToString(byte[] data) {
        if (data != null && data.length != 0) {
            if (data.length != INTERVALYM_BYTE_SIZE) {
                throw new InvalidParameterException("invalid len:" + data.length);
            }
            int is_negative = data[0] & MASK_VALUE;
            int year = DataTypeUtilities.getInt(data, 1);
            int month = data[5] & MASK_VALUE;
            int year_scale = data[6] & MASK_VALUE;
            intervalExceptionAction(false, 0, 0, 0, 0, 0, 0, year, month);
            return formatIntervalYmString(year, month, year_scale, is_negative);
        }
        return "";
    }

    static boolean isYearValid(int year) {
        if (year >= MIN_YEAR_VALUE && year <= MAX_YEAR_VALUE) {
            return year == 0 ? false : true;
        } else {
            return false;
        }
    }

    static boolean isDayValid(int year, int month, int day) {
        if (day >= 1 && day <= 31) {
            if (day > DataTypeUtilities.dayValuePerMonth[month - 1]
                && (!DataTypeUtilities.isLeapYear(year) || month != 2 || day != 29)) {
                return false;
            }
            return true;
        }
        return false;
    }

    static boolean gregorianCalendarCheck(int year, int month, int day) {
        if (year == GREGORIAN_CALENDAR_YEAR && month == GREGORIAN_CALENDAR_MONTH
            && day >= GREGORIAN_CALENDAR_DAY_START && day < GREGORIAN_CALENDAR_DAY_END) {
            return false;
        }
        return true;
    }

    static boolean isMonthValid(int month) {
        return (month >= 1 && month <= MAX_MONTH_VALUE) ? true : false;
    }

    static boolean isHourValid(int hour) {
        return (hour >= 1 && hour <= 24) ? true : false;
    }

    static boolean isMinuteValid(int minute) {
        return (minute >= 1 && minute <= 60) ? true : false;
    }

    static boolean isSecondValid(int second) {
        return (second >= 1 && second <= 60) ? true : false;
    }

    public static boolean isValid(int year, int month, int day, int hour, int minute, int second) {
        if (isYearValid(year) && isMonthValid(month) && isDayValid(year, month, day)
            && gregorianCalendarCheck(year, month, day) && isHourValid(hour)
            && isMinuteValid(minute) && isSecondValid(second)) {
            return true;
        }
        return false;
    }

    // api for TIMESTAMP
    public static final int getNanos(byte[] bytes, int idx) {
        int nanos = (bytes[idx + 3] & 0xff) << 24;
        nanos |= (bytes[idx + 2] & 0xff) << 16;
        nanos |= (bytes[idx + 1] & 0xff) << 8;
        nanos |= bytes[idx] & 0xff;
        return nanos;
    }

    public static final void setNanos(byte[] bytes, int idx, int nanos) {
        bytes[idx + 3] = (byte) ((nanos >> 24) & 0xff);
        bytes[idx + 2] = (byte) ((nanos >> 16) & 0xff);
        bytes[idx + 1] = (byte) ((nanos >> 8) & 0xff);
        bytes[idx] = (byte) (nanos & 0xff);
    }

    public static Date toDate(byte[] bytes) {
        int[] result = new int[TIMESTAMP_SIZE];
        int i;
        for (i = 0; i < bytes.length; ++i) {
            result[i] = bytes[i] & 0xff;
        }

        i = result[0] * 100 + result[1];
        Calendar calendar = Calendar.getInstance();
        calendar.set(i, result[2] - 1, result[3], result[4] - 1, result[5] - 1, result[6] - 1);
        calendar.set(Calendar.MILLISECOND, 0);
        long time = calendar.getTime().getTime();
        return new Date(time);
    }

    public static Timestamp innerToTimestamp(byte[] bytes, Calendar cal) throws SQLException {
        int[] result = new int[TIMESTAMP_SIZE];

        int i;
        for (i = 0; i < bytes.length; ++i) {
            result[i] = bytes[i] & 0xff;
        }

        i = result[0] * 100 + result[1];
        Calendar calendar = cal;
        if (null == calendar) {
            calendar = Calendar.getInstance();
        }
        calendar.set(i, result[2] - 1, result[3], result[4], result[5], result[6]);
        calendar.set(Calendar.MILLISECOND, 0);
        long time = calendar.getTime().getTime();
        Timestamp timestamp = new Timestamp(time);
        timestamp.setNanos(DataTypeUtilities.getNanos(bytes, 7));
        return timestamp;
    }

    public static Time toTime(byte[] bytes) {
        return new Time(bytes[4] & 0xff, bytes[5] & 0xff, bytes[6] & 0xff);
    }

    public static byte[] initTimestamp() {
        byte[] bytes = new byte[TIMESTAMP_SIZE];
        setBytesValues(bytes, 19, 70, 1, 1, 0, 0, 0);
        DataTypeUtilities.setNanos(bytes, 7, 0);
        bytes[11] = 0;
        return bytes;
    }

    public static String TIMESTMAPBytesToString(byte[] bytes) {
        int[] buf = new int[bytes.length];
        int i;
        for (i = 0; i < bytes.length; ++i) {
            buf[i] = bytes[i] & 0xff;
        }
        return TIMESTMAPToString(buf[0] * 100 + buf[1], buf[2], buf[3], buf[4], buf[5], buf[6],
            DataTypeUtilities.getNanos(bytes, 7), bytes[11], null); // FIXME duplicate from TIMESTAMPTZ
    }

    public static String TIMESTMAPToString(int year, int month, int day, int hour, int minute,
                                           int second, int nanos, int scale, String timezone) {
        String time = toSplicedString(year, month, day, hour, minute, second, nanos);
        if (nanos >= 0) {
            String temp = String.format("%09d", nanos);
            char[] chars = temp.toCharArray();

            int index;
            for (index = chars.length; index > 1 && chars[index - 1] == '0'; --index) {
                ;
            }
            temp = temp.substring(0, index);
            if (scale > temp.length()) {
                int x = scale - temp.length();
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < x; i++) {
                    sb.append("0");
                }
                temp += sb.toString();
            }

            time = time + "." + temp;
        }

        if (timezone != null) {
            time = time + " " + timezone;
        }

        return time;
    }

    private static String intToString(int temp) {
        return temp < 10 ? "0" + temp : Integer.toString(temp);
    }

    private static void setBytesValues(byte[] bytes, int yearPart1, int yearPart2, int month,
                                       int day, int hour, int minute, int second) {
        bytes[0] = (byte) yearPart1;
        bytes[1] = (byte) yearPart2;
        bytes[2] = (byte) month;
        bytes[3] = (byte) day;
        bytes[4] = (byte) hour;
        bytes[5] = (byte) minute;
        bytes[6] = (byte) second;
    }

    public static byte[] TIMESTMAPToBytes(Time time) {
        if (time == null) {
            return null;
        } else {
            byte[] result = new byte[TIMESTAMP_SIZE];
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(time);
            setBytesValues(result, 19, 70, 1, 1, calendar.get(Calendar.HOUR_OF_DAY),
                calendar.get(Calendar.MINUTE), calendar.get(Calendar.SECOND));
            DataTypeUtilities.setNanos(result, 7, 0);
            result[11] = 0;
            return result;
        }
    }

    public static byte[] TIMESTMAPToBytes(Date date) {
        if (date == null) {
            return null;
        } else {
            byte[] result = new byte[TIMESTAMP_SIZE];
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            setBytesValues(result, calendar.get(Calendar.YEAR) / 100,
                calendar.get(Calendar.YEAR) % 100, calendar.get(Calendar.MONTH) + 1,
                calendar.get(Calendar.DAY_OF_MONTH), 0, 0, 0);
            DataTypeUtilities.setNanos(result, 7, 0);
            result[11] = 0;
            return result;
        }
    }

    public static byte[] TIMESTAMPToBytes(Timestamp timestamp) {
        if (timestamp == null) {
            return null;
        } else {
            int nanos = timestamp.getNanos();
            byte[] result = new byte[TIMESTAMP_SIZE];

            Calendar calendar = Calendar.getInstance();
            calendar.setTime(timestamp);
            setBytesValues(result, calendar.get(Calendar.YEAR) / 100,
                calendar.get(Calendar.YEAR) % 100, calendar.get(Calendar.MONTH) + 1,
                calendar.get(Calendar.DAY_OF_MONTH), calendar.get(Calendar.HOUR_OF_DAY),
                calendar.get(Calendar.MINUTE), calendar.get(Calendar.SECOND));
            DataTypeUtilities.setNanos(result, 7, nanos);
            return result;
        }
    }

    public static byte[] TIMESTAMPToBytes(Timestamp timestamp, Calendar calendar) {
        if (timestamp == null) {
            return null;
        } else {
            int nanos = timestamp.getNanos();
            byte[] result = new byte[TIMESTAMP_SIZE];

            if (calendar == null) {
                calendar = Calendar.getInstance();
            }

            calendar.clear();
            calendar.setTime(timestamp);
            int year = calendar.get(Calendar.YEAR);
            if (calendar.get(Calendar.ERA) == 0) {
                year = -(year - 1);
            }

            if (year >= -MIN_YEAR_VALUE && year <= MAX_YEAR_VALUE) {
                setBytesValues(result, year / 100, year % 100, calendar.get(Calendar.MONTH) + 1,
                    calendar.get(Calendar.DAY_OF_MONTH), calendar.get(Calendar.HOUR_OF_DAY),
                    calendar.get(Calendar.MINUTE), calendar.get(Calendar.SECOND));
                DataTypeUtilities.setNanos(result, 7, nanos);
                return result;
            } else {
                throw new IllegalArgumentException("Invalid year value");
            }
        }
    }

    public static boolean isLeapYear(int year) {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, year);
        return cal.getActualMaximum(Calendar.DAY_OF_YEAR) > 365;
    }

    // API for TIMESTAMPLTZ
    public static byte[] initTimestampltz() {
        byte[] result = new byte[TIMESTAMPLTZ_SIZE];
        setBytesValues(result, 19, 70, 1, 1, 1, 1, 1);
        DataTypeUtilities.setNanos(result, 7, 0);
        result[11] = 0;
        return result;
    }

    public static final String toSplicedString(int year, int month, int day, int hour, int minute,
                                               int second, int nanos) {
        String time = "" + year + "-" + intToString(month) + "-" + intToString(day) + " "
                      + intToString(hour) + ":" + intToString(minute) + ":" + intToString(second);
        return time;
    }

    public static final String toFormatTimeString(String time, int nanos) {
        String temp = String.format("%09d", nanos);
        char[] chars = temp.toCharArray();
        int index;
        for (index = chars.length; index > 1 && chars[index - 1] == '0'; --index) {
        }
        temp = temp.substring(0, index);
        time = time + "." + temp;
        return time;
    }

    public static final String toFormatTimeStringWitTimeZone(String time, String timezone) {
        if (timezone != null) {
            time = time + " " + timezone;
        }
        return time;
    }

    public static final String TIMESTMAPLTZToString(int year, int month, int day, int hour,
                                                    int minute, int second, int nanos, int scale,
                                                    String timezone, boolean isResult) {
        String time = toSplicedString(year, month, day, hour, minute, second, nanos);
        int target = isResult ? 0 : 1;
        if (nanos >= target) {
            time = toFormatTimeString(time, nanos);
        }
        return toFormatTimeStringWitTimeZone(time, timezone);
    }

    public static String TIMESTMAPLTZToString(Connection connection, byte[] bytes, boolean isResult)
                                                                                                    throws SQLException {
        if (bytes.length < 12) {
            throw new SQLException("invalid bytes length");
        }

        Calendar calendar = Calendar.getInstance(TimeZone
            .getTimeZone(((OceanBaseConnection) connection).getSessionTimeZone()));
        calendar.setTimeInMillis(getOriginTime(bytes,
            TimeZone.getTimeZone(((OceanBaseConnection) connection).getSessionTimeZone())));

        return DataTypeUtilities.TIMESTMAPLTZToString(calendar.get(Calendar.YEAR),
            calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH),
            calendar.get(Calendar.HOUR_OF_DAY), calendar.get(Calendar.MINUTE),
            calendar.get(Calendar.SECOND), DataTypeUtilities.getNanos(bytes, 7), bytes[11],
            DataTypeUtilities.getSessionTimeZone(connection), isResult);
    }

    public static byte[] TIMESTMAPLTZToBytes(Connection connection, Time time) throws SQLException {
        if (time == null) {
            return null;
        }
        Calendar calendar = Calendar.getInstance();
        return TIMESTAMAPLTZCalendarToBytes(calendar, false, null, null, time);
    }

    public static byte[] TIMESTMAPLTZToBytes(Connection connection, Date date) throws SQLException {
        if (date == null) {
            return null;
        }
        Calendar calendar = Calendar.getInstance();
        return TIMESTAMAPLTZCalendarToBytes(calendar, false, date, null, null);
    }

    public static byte[] TIMESTMAPLTZToBytes(Connection connection, Timestamp timestamp)
                                                                                        throws SQLException {
        if (timestamp == null) {
            return null;
        }
        Calendar calendar = Calendar.getInstance();
        return TIMESTAMAPLTZCalendarToBytes(calendar, false, null, timestamp, null);
    }

    public static long getOriginTime(byte[] bytes, TimeZone timeZone) throws SQLException {
        return getOriginTime(bytes, timeZone, false);
    }

    public static long getOriginTime(byte[] bytes, TimeZone timeZone, boolean isTSResult)
                                                                                         throws SQLException {
        if (bytes.length < 7) {
            throw new SQLException("invalid bytes length");
        }

        int[] result = new int[7];
        int i;
        for (i = 0; i < 7; ++i) {
            result[i] = bytes[i] & 0xff;
        }

        i = result[0] * 100 + result[1];
        // If LTZ, convert to session timezone
        // If TZ, convert to the time zone returned by ob
        Calendar calendar = Calendar.getInstance(timeZone);
        calendar.clear();
        int year, month, day;
        if (!isTSResult) {
            year = i;
            month = result[2] - 1;
            day = result[3];
        } else {
            year = 1970;
            month = 0;
            day = 1;
        }
        calendar.set(year, month, day, result[4], result[5], result[6]);
        return calendar.getTimeInMillis();
    }

    static byte[] initTimestamptz() {
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

    public static byte[] TIMESTAMPTZtoBytes(Connection connection, Date date) throws SQLException {
        return TIMESTAMPTZtoBytes(connection, date, null);
    }

    public static byte[] TIMESTAMPTZtoBytes(Connection connection, Date date, Calendar calendar)
                                                                                                throws SQLException {
        if (date == null) {
            return null;
        }
        if (null == calendar) {
            calendar = Calendar.getInstance();
        }
        return TIMESTAMAPTZCalendarToBytes(calendar, false, date, null, null);
    }

    public static byte[] TIMESTAMPTZtoBytes(Connection connection, Time time) throws SQLException {
        return TIMESTAMPTZtoBytes(connection, time, null);
    }

    public static byte[] TIMESTAMPTZtoBytes(Connection connection, Time time, Calendar calendar)
                                                                                                throws SQLException {
        if (time == null) {
            return null;
        }
        if (null == calendar) {
            calendar = Calendar.getInstance();
        }
        return TIMESTAMAPTZCalendarToBytes(calendar, false, null, null, time);
    }

    public static byte[] TIMESTAMPTZtoBytes(Connection connection, Timestamp timeStamp)
                                                                                       throws SQLException {
        return TIMESTAMPTZtoBytes(connection, timeStamp, null);
    }

    public static byte[] TIMESTAMPTZtoBytes(Connection connection, String time) throws SQLException {
        return TIMESTAMPTZtoBytes(connection, Timestamp.valueOf(time));
    }

    public static byte[] TIMESTAMPTZtoBytes(Connection connection, Timestamp timestamp,
                                            Calendar calendar) throws SQLException {
        return TIMESTAMPTZtoBytes(connection, timestamp, calendar, false);
    }

    public static byte[] TIMESTAMPTZtoBytes(Connection connection, Timestamp timestamp,
                                            Calendar calendar, boolean isTZTablesImported)
                                                                                          throws SQLException {
        if (timestamp == null) {
            return null;
        }
        if (null == calendar) {
            calendar = Calendar.getInstance();
        }
        return TIMESTAMAPTZCalendarToBytes(calendar, isTZTablesImported, null, timestamp, null);
    }

    public static byte[] TIMESTAMPTZtoBytes(Connection connection, String time, Calendar calendar)
                                                                                                  throws SQLException {
        return TIMESTAMPTZtoBytes(connection, Timestamp.valueOf(time), calendar);
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

    public static final String TIMESTAMPTZToString(int year, int month, int day, int hour,
                                                   int minute, int second, int nanos, int scale,
                                                   String timezone) {
        String time = toSplicedString(year, month, day, hour, minute, second, nanos);
        if (nanos >= 0) {
            time = toFormatTimeString(time, nanos);
        }
        return toFormatTimeStringWitTimeZone(time, timezone);
    }

    public static String TIMESTAMPTZToString(Connection connection, byte[] bytes, boolean isResult)
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
            tzStr = tzName;
        } else {
            tzStr = DataTypeUtilities.toTimezoneStr(bytes[12], bytes[13], "", isResult);
        }

        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(tzStr));
        calendar
            .setTimeInMillis(DataTypeUtilities.getOriginTime(bytes, TimeZone.getTimeZone(tzStr)));

        return DataTypeUtilities.TIMESTAMPTZToString(calendar.get(Calendar.YEAR),
            calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH),
            calendar.get(Calendar.HOUR_OF_DAY), calendar.get(Calendar.MINUTE),
            calendar.get(Calendar.SECOND), DataTypeUtilities.getNanos(bytes, 7), bytes[11], tzStr);
    }

    public static byte[] TIMESTAMAPTZCalendarToBytes(Calendar calendar, boolean isTZTablesImported,
                                                     Date date, Timestamp timestamp, Time time)
                                                                                               throws SQLException {
        TIMETYPE timetype = TIMETYPE.NONE;
        if (date != null) {
            timetype = TIMETYPE.DATE;
        }
        if (timestamp != null) {
            timetype = TIMETYPE.TIMESTMAP;
        }
        if (time != null) {
            timetype = TIMETYPE.TIME;
        }
        if (timetype == TIMETYPE.NONE) {
            throw new SQLException("Time type error");
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
        if (timetype == TIMETYPE.TIME) {
            resultBytes = new byte[TIMESTAMPTZ_SIZE];
        } else {
            if (isTZTablesImported) {
                resultBytes = new byte[TIMESTAMPTZ_SIZE + tzLen + 2];
            } else {
                resultBytes = new byte[TIMESTAMPTZ_SIZE + 2];
            }
        }
        int offset = calendar.getTimeZone().getRawOffset();
        if (timetype == TIMETYPE.TIME) {
            localCalendar.setTime(time);
            short base = 1970;
            localCalendar.set(Calendar.YEAR, base);
            localCalendar.set(Calendar.MONTH, 0);
            localCalendar.set(Calendar.DAY_OF_MONTH, 1);
        }
        if (timetype == TIMETYPE.TIMESTMAP) {
            localCalendar.setTime(timestamp);
        } else if (timetype == TIMETYPE.DATE) {
            localCalendar.setTime(date);
        }
        int year = localCalendar.get(Calendar.YEAR);
        if (year >= MIN_YEAR_VALUE && year <= MAX_YEAR_VALUE) {
            resultBytes[0] = (byte) (localCalendar.get(Calendar.YEAR) / 100);
            resultBytes[1] = (byte) (localCalendar.get(Calendar.YEAR) % 100);
            resultBytes[2] = (byte) (localCalendar.get(Calendar.MONTH) + 1);
            resultBytes[3] = (byte) localCalendar.get(Calendar.DAY_OF_MONTH);
            if (timetype == TIMETYPE.TIMESTMAP || timetype == TIMETYPE.TIME) {
                resultBytes[4] = (byte) (localCalendar.get(Calendar.HOUR_OF_DAY));
                resultBytes[5] = (byte) (localCalendar.get(Calendar.MINUTE));
                resultBytes[6] = (byte) (localCalendar.get(Calendar.SECOND));
                if (timetype == TIMETYPE.TIMESTMAP) {
                    DataTypeUtilities.setNanos(resultBytes, 7, timestamp.getNanos());
                } else {
                    DataTypeUtilities.setNanos(resultBytes, 7, 0);
                }
            } else {
                resultBytes[4] = (byte) (0);
                resultBytes[5] = (byte) (0);
                resultBytes[6] = (byte) (0);
                DataTypeUtilities.setNanos(resultBytes, 7, 0);
            }
            resultBytes[12] = (byte) (offset / HOUR_MILLISECOND);
            resultBytes[13] = (byte) (offset < 0 ? (-offset % HOUR_MILLISECOND / MINUTE_MILLISECOND)
                : (offset % HOUR_MILLISECOND / MINUTE_MILLISECOND));
            if (timetype == TIMETYPE.TIME) {
                // do nothing
            } else {
                if (isTZTablesImported && !isGMT) {
                    resultBytes[14] = (byte) (tzLen); // tz name len
                    System.arraycopy(tz.getBytes(), 0, resultBytes, 15, tzLen);
                    resultBytes[14 + tzLen + 1] = (byte) (0); // tz abbr len
                } else {
                    resultBytes[14] = (byte) 0; // tz name len
                    resultBytes[15] = (byte) 0; // tz abbr len
                }
            }
            return resultBytes;
        } else {
            if (timetype == TIMETYPE.TIMESTMAP) {
                throw new SQLException(String.format("error format, timestamp = %s",
                    timestamp.toString()), "268");
            } else if (timetype == TIMETYPE.DATE) {
                throw new SQLException(String.format("error format, timestamp = %s",
                    date.toString()), "268");
            } else {
                throw new SQLException(String.format("error format, timestamp = %s",
                    time.toString()), "268");
            }
        }
    }

    public static byte[] TIMESTAMAPLTZCalendarToBytes(Calendar calendar,
                                                      boolean isTZTablesImported, Date date,
                                                      Timestamp timestamp, Time time)
                                                                                     throws SQLException {

        TIMETYPE timetype = TIMETYPE.NONE;
        if (date != null) {
            timetype = TIMETYPE.DATE;
        }
        if (timestamp != null) {
            timetype = TIMETYPE.TIMESTMAP;
        }
        if (time != null) {
            timetype = TIMETYPE.TIME;
        }
        if (timetype == TIMETYPE.NONE) {
            throw new SQLException("Time type error");
        }
        int nanos = 0;
        if (timetype == TIMETYPE.TIME) {
            calendar.setTime(time);
            calendar.set(Calendar.YEAR, 1970);
            calendar.set(Calendar.MONTH, 0);
            calendar.set(Calendar.DAY_OF_MONTH, 1);
            nanos = 0;
        }
        if (timetype == TIMETYPE.DATE) {
            calendar.setTime(date);
            calendar.set(Calendar.HOUR_OF_DAY, 0);
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);
            nanos = 0;
        }
        if (timetype == TIMETYPE.TIMESTMAP) {
            calendar.setTime(timestamp);
            nanos = timestamp.getNanos();
        }
        byte[] result = new byte[TIMESTAMPLTZ_SIZE];

        int year = calendar.get(Calendar.YEAR);
        if (year >= MIN_YEAR_VALUE && year <= MAX_YEAR_VALUE) {
            result[0] = (byte) (calendar.get(Calendar.YEAR) / 100);
            result[1] = (byte) (calendar.get(Calendar.YEAR) % 100);
            result[2] = (byte) (calendar.get(Calendar.MONTH) + 1);
            result[3] = (byte) calendar.get(Calendar.DAY_OF_MONTH);
            result[4] = (byte) (calendar.get(Calendar.HOUR_OF_DAY));
            result[5] = (byte) (calendar.get(Calendar.MINUTE));
            result[6] = (byte) (calendar.get(Calendar.SECOND));
            DataTypeUtilities.setNanos(result, 7, nanos);
            String temp = String.format("%09d", nanos);
            char[] chars = temp.toCharArray();

            int index;
            for (index = chars.length; index > 1 && chars[index - 1] == '0'; --index) {
            }
            temp = temp.substring(0, index);
            String nanosStr = temp;

            result[11] = (byte) nanosStr.length();

            return result;
        } else {
            throw new SQLException("error format", "268");
        }
    }

    static {
        HOUR_MILLISECOND = 3600000;
        MINUTE_MILLISECOND = 60000;
    }

}
