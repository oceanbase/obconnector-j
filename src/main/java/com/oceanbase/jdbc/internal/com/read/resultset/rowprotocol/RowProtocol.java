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
package com.oceanbase.jdbc.internal.com.read.resultset.rowprotocol;

import static com.oceanbase.jdbc.util.Options.ZERO_DATETIME_CONVERT_TO_NULL;
import static com.oceanbase.jdbc.util.Options.ZERO_DATETIME_EXCEPTION;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.regex.Pattern;

import com.oceanbase.jdbc.JDBC4ResultSet;
import com.oceanbase.jdbc.extend.datatype.*;
import com.oceanbase.jdbc.internal.ColumnType;
import com.oceanbase.jdbc.internal.com.read.resultset.ColumnDefinition;
import com.oceanbase.jdbc.internal.protocol.Protocol;
import com.oceanbase.jdbc.util.Options;

public abstract class RowProtocol {

  public static final int BIT_LAST_FIELD_NOT_NULL = 0b000000;
  public static final int BIT_LAST_FIELD_NULL = 0b000001;
  public static final int BIT_LAST_ZERO_DATE = 0b000010;

  public static final int TINYINT1_IS_BIT = 1;
  public static final int YEAR_IS_DATE_TYPE = 2;

  public static final DateTimeFormatter TEXT_LOCAL_DATE_TIME;
  public static final DateTimeFormatter TEXT_OFFSET_DATE_TIME;
  public static final DateTimeFormatter TEXT_ZONED_DATE_TIME;

  public static final Pattern isIntegerRegex = Pattern.compile("^-?\\d+\\.[0-9]+$");
  protected static final int NULL_LENGTH = -1;
  private Protocol    protocol;

  private Calendar calendarWithoutTimeZone;

  private Calendar calendarWithTimeZone;

  static {
    TEXT_LOCAL_DATE_TIME =
        new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(DateTimeFormatter.ISO_LOCAL_TIME)
            .toFormatter();

    TEXT_OFFSET_DATE_TIME =
        new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(TEXT_LOCAL_DATE_TIME)
            .appendOffsetId()
            .toFormatter();

    TEXT_ZONED_DATE_TIME =
        new DateTimeFormatterBuilder()
            .append(TEXT_OFFSET_DATE_TIME)
            .optionalStart()
            .appendLiteral('[')
            .parseCaseSensitive()
            .appendZoneRegionId()
            .appendLiteral(']')
            .toFormatter();
  }

  protected final int maxFieldSize;
  protected final Options options;
  public int lastValueNull;
  public byte[] buf;
  public int pos;
  public int length;
  protected int index;
  public int[] complexEndPos;

  // mysql8 compatible tags
  protected boolean useCalLenientFlag;
  protected boolean yearIsZero;

  public RowProtocol(int maxFieldSize, Options options) {
    this.maxFieldSize = maxFieldSize;
    this.options = options;
  }

  public void resetRow(byte[] buf) {
    this.buf = buf;
    index = -1;
  }
  Charset getCurrentEncoding(ColumnType columnType) {
    switch (columnType) {
      case NVARCHAR2:
      case NCHAR:
        if (this.options.nCharacterEncoding != null && !this.options.nCharacterEncoding.isEmpty()) {
          return Charset.forName(this.options.nCharacterEncoding);
        }
      case VARCHAR:
      case VARCHAR2:
      case VARSTRING:
      case RAW:
      case STRING:
        return Charset.forName(this.options.getCharacterEncoding());
      default:
        break;
    }
    return  StandardCharsets.UTF_8;

  }
  public abstract void setPosition(int position, JDBC4ResultSet resultSet) throws SQLException;

  public int getLengthMaxFieldSize() {
    return maxFieldSize != 0 && maxFieldSize < length ? maxFieldSize : length;
  }

  public int getMaxFieldSize() {
    return maxFieldSize;
  }
  public Calendar getCalendarInstance(Calendar calendar) {
    if (calendar != null) {
      return (Calendar) calendar.clone();
    }
    if (calendarWithoutTimeZone == null) {
      calendarWithoutTimeZone = Calendar.getInstance();
    }
    return calendarWithoutTimeZone;
  }
    public Calendar getCalendarInstanceWithTimezone(Calendar calendar,TimeZone timeZone) {
      if (calendar != null) {
        return (Calendar) calendar.clone();
      }
      if (calendarWithTimeZone == null) {
        calendarWithTimeZone = Calendar.getInstance(timeZone);
      }
      calendarWithTimeZone.setTimeZone(timeZone);
      return calendarWithTimeZone;
    }
  public abstract String getInternalString(
      ColumnDefinition columnInfo, Calendar cal, TimeZone timeZone) throws SQLException;

  public abstract int getInternalInt(ColumnDefinition columnInfo) throws SQLException;

  public abstract long getInternalLong(ColumnDefinition columnInfo) throws SQLException;

  public abstract float getInternalFloat(ColumnDefinition columnInfo) throws SQLException;

  public abstract double getInternalDouble(ColumnDefinition columnInfo) throws SQLException;

  public abstract BigDecimal getInternalBigDecimal(ColumnDefinition columnInfo) throws SQLException;

  public abstract Date getInternalDate(ColumnDefinition columnInfo, Calendar cal, TimeZone timeZone)
      throws SQLException;

  public abstract Time getInternalTime(ColumnDefinition columnInfo, Calendar cal, TimeZone timeZone)
      throws SQLException;

  public abstract Timestamp getInternalTimestamp(
      ColumnDefinition columnInfo, Calendar userCalendar, TimeZone timeZone) throws SQLException;

  public abstract Array getInternalArray(ColumnDefinition columnInfo,ComplexDataType complexDataType,Connection connection) throws  SQLException;
  public abstract Struct getInternalStruct(ColumnDefinition columnInfo,ComplexDataType complexDataType,Connection connection) throws  SQLException;
  public abstract ComplexData getInternalComplexCursor(ColumnDefinition columnInfo,ComplexDataType complexDataType,Connection connection) throws  SQLException;
  public String getEncoding() {
    return options.getCharacterEncoding();
  }
  public TIMESTAMP getInternalTIMESTAMP(
      ColumnDefinition columnInfo, Calendar userCalendar, TimeZone timeZone) throws SQLException {
    if (lastValueWasNull()) {
      return null;
    }

    if (columnInfo.getColumnType() == ColumnType.TIMESTAMP_NANO) {
      if (length < 12) {
        throw new SQLException(
            "timestamp field data length is invalid, expected 12 at least, actual length is "
                + length);
      }

      return buildTIMETAMP(buf, pos, length);
    } else {
      String value = new String(buf, pos, length, getCurrentEncoding(columnInfo.getColumnType()));
      // todo check the encoding
      throw new SQLException(
          "Value type \""
              + columnInfo.getColumnType().getTypeName()
              + "\" with value \""
              + value
              + "\" cannot be parse as TIMESTAMP");
    }
  }

  public TIMESTAMPTZ getInternalTIMESTAMPTZ(
      ColumnDefinition columnInfo, Calendar userCalendar, TimeZone timeZone) throws SQLException {
    if (lastValueWasNull()) {
      return null;
    }

    if (columnInfo.getColumnType() == ColumnType.TIMESTAMP_TZ) {
      if (length < 12) {
        throw new SQLException(
            "timestamp field data length is invalid, expected 12 at least, actual length is "
                + length);
      }

      byte[] returnBytes = new byte[length];
      System.arraycopy(buf, pos, returnBytes, 0, length);
      TIMESTAMPTZ timestamptz = new TIMESTAMPTZ(returnBytes);
      timestamptz.setByte(11, returnBytes[11]);
      return timestamptz;
    } else {
      String value = new String(buf, pos, length, getCurrentEncoding(columnInfo.getColumnType()));
      // todo check the encoding
      throw new SQLException(
          "Value type \""
              + columnInfo.getColumnType().getTypeName()
              + "\" with value \""
              + value
              + "\" cannot be parse as TIMESTAMP");
    }
  }

  public TIMESTAMPLTZ getInternalTIMESTAMPLTZ(
      ColumnDefinition columnInfo, Calendar userCalendar, TimeZone timeZone) throws SQLException {
    if (lastValueWasNull()) {
      return null;
    }

    if (columnInfo.getColumnType() == ColumnType.TIMESTAMP_LTZ) {
      if (length < 12) {
        throw new SQLException(
            "timestamp field data length is invalid, expected 12 at least, actual length is "
                + length);
      }

      byte[] returnBytes = new byte[length];
      System.arraycopy(buf, pos, returnBytes, 0, length);
      return new TIMESTAMPLTZ(returnBytes);
    } else {
      String value = new String(buf, pos, length, getCurrentEncoding(columnInfo.getColumnType()));
      // todo check the encoding
      throw new SQLException(
          "Value type \""
              + columnInfo.getColumnType().getTypeName()
              + "\" with value \""
              + value
              + "\" cannot be parse as TIMESTAMP");
    }
  }

  protected TIMESTAMP buildTIMETAMP(byte[] bytes, int pos, int length) throws SQLException {
    StringBuilder sb = new StringBuilder();
    sb.append(buildTimestamp(bytes[pos])); // century
    sb.append(buildTimestamp(bytes[pos + 1])); // year
    sb.append("-");
    sb.append(buildTimestamp(bytes[pos + 2])); // month
    sb.append("-");
    sb.append(buildTimestamp(bytes[pos + 3])); // day
    sb.append(" ");
    sb.append(buildTimestamp(bytes[pos + 4])); // hour
    sb.append(":");
    sb.append(buildTimestamp(bytes[pos + 5])); // minute
    sb.append(":");
    sb.append(buildTimestamp(bytes[pos + 6])); // second
    sb.append(".");
    byte[] nanosBytes = new byte[4];
    System.arraycopy(bytes, pos + 7, nanosBytes, 0, 4);
    int nanos = DataTypeUtilities.getNanos(nanosBytes, 0); // FIXME
    String temp = String.format("%09d", nanos);
    char[] chars = temp.toCharArray();

    int index;
    for (index = chars.length; index > 1 && chars[index - 1] == '0'; --index) {;
    }
    temp = temp.substring(0, index);

    // 11 byte represents scale
    int scale = bytes[pos + 11];
    if (scale > temp.length()) {
      int x = scale - temp.length();
      StringBuilder strBuf = new StringBuilder();
      for (int i = 0; i < x; i++) {
        strBuf.append("0");
      }
      temp += strBuf.toString();
    }
    sb.append(temp);

    TIMESTAMP timestamp = new TIMESTAMP(Timestamp.valueOf(sb.toString()));
    timestamp.setByte(11, bytes[pos + 11]);

    return timestamp;
  }

  private String buildTimestamp(byte b) {
    if (b < 10) {
      return "0" + b;
    }
    return "" + b;
  }

  public abstract Object getInternalObject(ColumnDefinition columnInfo, TimeZone timeZone)
      throws SQLException;

  public abstract boolean getInternalBoolean(ColumnDefinition columnInfo) throws SQLException;

  public abstract byte getInternalByte(ColumnDefinition columnInfo) throws SQLException;

  public abstract short getInternalShort(ColumnDefinition columnInfo) throws SQLException;

  public abstract String getInternalTimeString(ColumnDefinition columnInfo) throws SQLException;

  public abstract BigInteger getInternalBigInteger(ColumnDefinition columnInfo) throws SQLException;

  public abstract ZonedDateTime getInternalZonedDateTime(
      ColumnDefinition columnInfo, Class clazz, TimeZone timeZone) throws SQLException;

  public abstract OffsetTime getInternalOffsetTime(ColumnDefinition columnInfo, TimeZone timeZone)
      throws SQLException;

    public abstract OffsetDateTime getInternalOffsetDateTime(ColumnDefinition columnInfo, TimeZone timeZone)
            throws SQLException;

  public abstract LocalTime getInternalLocalTime(ColumnDefinition columnInfo, TimeZone timeZone)
      throws SQLException;

  public abstract LocalDate getInternalLocalDate(ColumnDefinition columnInfo, TimeZone timeZone)
      throws SQLException;
  public abstract INTERVALDS getInternalINTERVALDS(ColumnDefinition columnInfo) throws SQLException;
  public abstract INTERVALYM getInternalINTERVALYM(ColumnDefinition columnInfo) throws SQLException;

  public abstract boolean isBinaryEncoded();

  public boolean lastValueWasNull() {
    return (lastValueNull & BIT_LAST_FIELD_NULL) != 0;
  }

  protected String zeroFillingIfNeeded(String value, ColumnDefinition columnDefinition) {
    if (columnDefinition.isZeroFill()) {
      StringBuilder zeroAppendStr = new StringBuilder();
      long zeroToAdd = columnDefinition.getDisplaySize() - value.length();
      while (zeroToAdd-- > 0) {
        zeroAppendStr.append("0");
      }
      return zeroAppendStr.append(value).toString();
    }
    return value;
  }

  protected int getInternalTinyInt(ColumnDefinition columnInfo) {
    if (lastValueWasNull()) {
      return 0;
    }
    int value = buf[pos];
    if (!columnInfo.isSigned()) {
      value = (buf[pos] & 0xff);
    }
    return value;
  }

  protected long parseBit() {
    if (length == 1) {
      return buf[pos];
    }
    long val = 0;
    int ind = 0;
    do {
      val += ((long) (buf[pos + ind] & 0xff)) << (8 * (length - ++ind));
    } while (ind < length);
    return val;
  }

  protected int getInternalSmallInt(ColumnDefinition columnInfo) {
    if (lastValueWasNull()) {
      return 0;
    }
    int value = ((buf[pos] & 0xff) + ((buf[pos + 1] & 0xff) << 8));
    if (!columnInfo.isSigned()) {
      return value & 0xffff;
    }
    // short cast here is important : -1 will be received as -1, -1 -> 65535
    return (short) value;
  }

  protected long getInternalMediumInt(ColumnDefinition columnInfo) {
    if (lastValueWasNull()) {
      return 0;
    }
    long value =
        ((buf[pos] & 0xff)
            + ((buf[pos + 1] & 0xff) << 8)
            + ((buf[pos + 2] & 0xff) << 16)
            + ((buf[pos + 3] & 0xff) << 24));
    if (!columnInfo.isSigned()) {
      value = value & 0xffffffffL;
    }
    return value;
  }
  boolean isNUMBERTYPE(ColumnType columnType) {
    if(columnType == ColumnType.NUMBER || columnType == ColumnType.FLOAT || columnType == ColumnType.DECIMAL || columnType == ColumnType.BINARY_DOUBLE || columnType == ColumnType.BINARY_FLOAT) {
      return true;
    }
    return false;
  }
  protected NUMBER zgetNUMBER(ColumnDefinition columnInfo) throws SQLException {
    if (lastValueWasNull()) {
      return null;
    }
//    if (columnInfo.getColumnType() == ColumnType.NUMBER) {
    if(isNUMBERTYPE(columnInfo.getColumnType())) {
      byte[] b = new byte[length];
      System.arraycopy(buf,pos,b,0,length);
      return new NUMBER(b);
    } else {
      String value = new String(buf, pos, length, getCurrentEncoding(columnInfo.getColumnType()));
      // todo check the encoding
      throw new SQLException(
          "Value type \""
              + columnInfo.getColumnType().getTypeName()
              + "\" with value \""
              + value
              + "\" cannot be parse as NUMBER");
    }
  }

  protected NUMBER_FLOAT getNUMBER_FLOAT(ColumnDefinition columnInfo) throws SQLException {
    if (lastValueWasNull()) {
      return null;
    }
    String value = new String(buf, pos, length, getCurrentEncoding(columnInfo.getColumnType()));
    // todo check the encoding
    if (columnInfo.getColumnType() == ColumnType.NUMBER_FLOAT) {
      return new NUMBER_FLOAT(new Float(value), buf);
    } else {
      throw new SQLException(
          "Value type \""
              + columnInfo.getColumnType().getTypeName()
              + "\" with value \""
              + value
              + "\" cannot be parse as NUMBER_FLOAT");
    }
  }

  protected BINARY_DOUBLE getBINARY_DOUBLE(ColumnDefinition columnInfo) throws SQLException {
    if (lastValueWasNull()) {
      return null;
    }
    if (columnInfo.getColumnType() == ColumnType.BINARY_DOUBLE) {
      return new BINARY_DOUBLE(buf[pos]);
    } else {
      String value = new String(buf, pos, length, getCurrentEncoding(columnInfo.getColumnType()));
      // todo check the encoding
      throw new SQLException(
          "Value type \""
              + columnInfo.getColumnType().getTypeName()
              + "\" with value \""
              + value
              + "\" cannot be parse as BINARY_DOUBLE");
    }
  }

  protected BINARY_FLOAT getBINARY_FLOAT(ColumnDefinition columnInfo) throws SQLException {
    if (lastValueWasNull()) {
      return null;
    }
    if (columnInfo.getColumnType() == ColumnType.BINARY_FLOAT) {
      return new BINARY_FLOAT(buf[pos]);
    } else {
      String value = new String(buf, pos, length, getCurrentEncoding(columnInfo.getColumnType()));
      // todo check the encoding
      throw new SQLException(
          "Value type \""
              + columnInfo.getColumnType().getTypeName()
              + "\" with value \""
              + value
              + "\" cannot be parse as BINARY_FLOAT");
    }
  }
  protected INTERVALDS getINTERVALDS(ColumnDefinition columnInfo) throws Exception {
    if (lastValueWasNull()) {
      return null;
    }
    if (columnInfo.getColumnType() == ColumnType.INTERVALDS) {
      return new INTERVALDS(buf);
    } else {
      String value = new String(buf, pos, length, getCurrentEncoding(columnInfo.getColumnType()));
      throw new SQLException(
              "Value type \""
                      + columnInfo.getColumnType().getTypeName()
                      + "\" with value \""
                      + value
                      + "\" cannot be parse as INTERVALDS");
    }

  }
  protected INTERVALYM getINTERVALYM(ColumnDefinition columnInfo) throws Exception {
    if (lastValueWasNull()) {
      return null;
    }
    if (columnInfo.getColumnType() == ColumnType.INTERVALYM) {
      return new INTERVALYM(buf);
    } else {
      String value = new String(buf, pos, length, getCurrentEncoding(columnInfo.getColumnType()));
      // todo check the encoding
      throw new SQLException(
              "Value type \""
                      + columnInfo.getColumnType().getTypeName()
                      + "\" with value \""
                      + value
                      + "\" cannot be parse as INTERVALYM");
    }

  }

  protected void rangeCheck(
      Object className, long minValue, long maxValue, BigDecimal value, ColumnDefinition columnInfo)
      throws SQLException {
    if (value.compareTo(BigDecimal.valueOf(minValue)) < 0
        || value.compareTo(BigDecimal.valueOf(maxValue)) > 0) {
      throw new SQLException(
          "Out of range value for column '"
              + columnInfo.getName()
              + "' : value "
              + value
              + " is not in "
              + className
              + " range",
          "22003",
          1264);
    }
  }

    protected void rangeCheck(
            Object className, long minValue, long maxValue, double value, ColumnDefinition columnInfo)
            throws SQLException {
        if (value < minValue ||value > maxValue) {
            throw new SQLException(
                    "Out of range value for column '"
                            + columnInfo.getName()
                            + "' : value "
                            + value
                            + " is not in "
                            + className
                            + " range",
                    "22003",
                    1264);
        }
    }

  protected void rangeCheck(
      Object className, long minValue, long maxValue, long value, ColumnDefinition columnInfo)
      throws SQLException {
    if (value < minValue || value > maxValue) {
      throw new SQLException(
          "Out of range value for column '"
              + columnInfo.getName()
              + "' : value "
              + value
              + " is not in "
              + className
              + " range",
          "22003",
          1264);
    }
  }

  protected int extractNanos(String timestring) throws SQLException {
    int index = timestring.indexOf('.');
    if (index == -1) {
      return 0;
    }
    int nanos = 0;
    for (int i = index + 1; i < index + 10; i++) {
      int digit;
      if (i >= timestring.length()) {
        digit = 0;
      } else {
        char value = timestring.charAt(i);
        if (value < '0' || value > '9') {
          throw new SQLException(
              "cannot parse sub-second part in timestamp string '" + timestring + "'");
        }
        digit = value - '0';
      }
      nanos = nanos * 10 + digit;
    }
    return nanos;
  }

  /**
   * Reports whether the last column read had a value of Null. Note that you must first call one of
   * the getter methods on a column to try to read its value and then call the method wasNull to see
   * if the value read was Null.
   *
   * @return true true if the last column value read was null and false otherwise
   */
  public boolean wasNull() {
    return (lastValueNull & BIT_LAST_FIELD_NULL) != 0 || (lastValueNull & BIT_LAST_ZERO_DATE) != 0;
  }

  public Protocol getProtocol() {
    return protocol;
  }

  public void setProtocol(Protocol protocol) {
    this.protocol = protocol;
	}

  public int[]  getTimestampPart(ColumnDefinition columnInfo) throws SQLException {
      int nanoCount = 0;
      int nanoBegin = -1;
      int[] timestampsPart = new int[] { 0, 0, 0, 0, 0, 0, 0 };
      boolean onlyTimeFormat = false;
      boolean withColon = false;
      int partIdx = 0;
      for (int begin = pos; begin < pos  + length; begin++) {
          byte b = buf[begin];
          if(b == ':'){
              onlyTimeFormat = true;
          }
      }
      for (int begin = pos; begin < pos + length; begin++) {
          byte b = buf[begin];
          if (b == ' ' || b == '-' || b == '/') {
              onlyTimeFormat = false;
          }
          if(b == ':') {
              withColon = true;
          }
          if (b == '-' || b == ' ' || b == ':') {
              partIdx++;
              continue;
          }

          if (b == '.') {
              partIdx++;
              nanoBegin = begin;
              continue;
          }
          if (b < '0' || b > '9') {
              throw new SQLException("cannot parse data in timestamp string '"
                      + new String(buf, pos, length,
                      getCurrentEncoding(columnInfo.getColumnType()))
                      + "'");
          }
          if (nanoBegin != -1) {
              nanoCount++;
          }
          timestampsPart[partIdx] = timestampsPart[partIdx] * 10 + b - 48;
      }
      if (timestampsPart[0] == 0 && timestampsPart[1] == 0 && timestampsPart[2] == 0
              && timestampsPart[3] == 0 && timestampsPart[4] == 0 && timestampsPart[5] == 0 && timestampsPart[6] == 0) {
          // all zero
          if(getProtocol().isOracleMode()) {
              lastValueNull |= BIT_LAST_ZERO_DATE;
              return null;
          } else {
              if(options.zeroDateTimeBehavior.equalsIgnoreCase(ZERO_DATETIME_EXCEPTION) && !onlyTimeFormat) {
                  throw new SQLException("Value '"+ new String(buf, pos, length,
                          getCurrentEncoding(columnInfo.getColumnType())) +"' can not be represented as java.sql.Timestamp");
              }  if(options.zeroDateTimeBehavior.equalsIgnoreCase(ZERO_DATETIME_CONVERT_TO_NULL) && !onlyTimeFormat) {
                  return null;
              } else {
                  // round
                  timestampsPart[0] = 1;
                  timestampsPart[1] = 1;
                  timestampsPart[2] = 1;
              }
          }
      }
      if(length == 8 && !getProtocol().isOracleMode()) {
          timestampsPart[0] = 1970;
          timestampsPart[1] = 1;
          timestampsPart[2] = 1;
      }
      // timestampsPart[6] the number of nanos
      for (int i = nanoCount; i <= 8; i++) {
          timestampsPart[6] = timestampsPart[6] * 10;
      }
      return  timestampsPart;

  }

   Timestamp buildTimestmap(int year,int month,int day,int hour,int minutes,int seconds,int nanos,Calendar calendar,TimeZone timezone) {
      Calendar localCal = getCalendarInstance(calendar);
      if (options.useLegacyDatetimeCode) {
          localCal.setTimeZone(timezone);
      }
      Timestamp tt;
      if (!getProtocol().isOracleMode() && options.compatibleMysqlVersion == 8 && year == 0) {
         yearIsZero = true;
      } else {
         yearIsZero = false;
      }
      synchronized (localCal) {
          if (!getProtocol().isOracleMode() && options.compatibleMysqlVersion == 8 && !useCalLenientFlag) {
              localCal.setLenient(false);
          }
          localCal.clear();
          localCal.set(year, month - 1, day, hour, minutes, seconds);
          try {
              tt = new Timestamp(localCal.getTimeInMillis());
          } finally {
              if (useCalLenientFlag) {
                  useCalLenientFlag = false;
              }
              localCal.setLenient(true);
          }
      }
      tt.setNanos(nanos);
      return tt;
  }

  public Timestamp  getTimestampFromString(ColumnDefinition columnInfo ,String stringValue ,Calendar calendar,TimeZone timezone) throws SQLException {
      try {
          int year = 1970;
          int month = 0;
          int day = 0;
          int hour = 0;
          int minutes = 0;
          int seconds = 0;
          int nanos = 0;
          stringValue = stringValue.trim();
          int length = stringValue.length();
          if ((length > 0) && (stringValue.charAt(0) == '0') && (stringValue.equals("0000-00-00") || stringValue.equals("0000-00-00 00:00:00") || stringValue.equals("00000000000000") || stringValue.equals("0"))) {

              if (options.zeroDateTimeBehavior.equalsIgnoreCase(ZERO_DATETIME_EXCEPTION)) {
                  if (options.compatibleMysqlVersion == 8) {
                    throw new SQLException("Zero date value prohibited");
                  }
                  throw new SQLException("Value '" + new String(buf, pos, length,
                          getCurrentEncoding(columnInfo.getColumnType())) + "' can not be represented as java.sql.Timestamp");
              }
              if (options.zeroDateTimeBehavior.equalsIgnoreCase(ZERO_DATETIME_CONVERT_TO_NULL)) {
                  lastValueNull |= BIT_LAST_ZERO_DATE;
                  return null;
              } else {
                  // round
                  year = 1;
                  month = 1;
                  day = 1;
              }
              Timestamp tt;
              Calendar localCal = getCalendarInstance(calendar);
              synchronized (localCal) {
                  localCal.clear();
                  localCal.set(year, month - 1, day, hour, minutes, seconds);
                  tt = new Timestamp(localCal.getTimeInMillis());
              }
              tt.setNanos(nanos);
              return tt;
          } else {
              int decimalIndex = stringValue.indexOf(".");
              if (decimalIndex == length - 1) {
                  length--;
              } else if (decimalIndex != -1) {
                  if ((decimalIndex + 2) <= length) {
                      nanos = Integer.parseInt(stringValue.substring(decimalIndex + 1));
                      int numDigits = length - (decimalIndex + 1);
                      if (numDigits < 9) {
                          int factor = (int) (Math.pow(10, 9 - numDigits));
                          nanos = nanos * factor;
                      }
                      length = decimalIndex;
                  } else {
                      throw new IllegalArgumentException(); // re-thrown further down with a much better error message
                  }
              }

              switch (length) {
                  case 2: // YY
                  case 4: // YYMM
                  case 6: // YYMMDD
                      year = Integer.parseInt(stringValue.substring(0, 2));
                      if (year <= 69) {
                          year = (year + 100);
                      }
                      year += 1900;
                      switch (length) {
                          case 2:
                              month = 1;
                              day = 1;
                              break;
                          case 4:
                              month = Integer.parseInt(stringValue.substring(2, 4));
                              day = 1;
                              break;
                          case 6:
                              month = Integer.parseInt(stringValue.substring(2, 4));
                              day = Integer.parseInt(stringValue.substring(4, 6));
                              break;
                      }
                      break;
                  case 12:
                      // yymmddhhmmss
                      year = Integer.parseInt(stringValue.substring(0, 2));
                      if (year <= 69) {
                          year = (year + 100);
                      }
                      year += 1900;
                      month = Integer.parseInt(stringValue.substring(2, 4));
                      day = Integer.parseInt(stringValue.substring(4, 6));
                      hour = Integer.parseInt(stringValue.substring(6, 8));
                      minutes = Integer.parseInt(stringValue.substring(8, 10));
                      seconds = Integer.parseInt(stringValue.substring(10, 12));
                      break;
                  case 14:
                      // yyyymmddhhmmss
                      year = Integer.parseInt(stringValue.substring(0, 4));
                      month = Integer.parseInt(stringValue.substring(4, 6));
                      day = Integer.parseInt(stringValue.substring(6, 8));
                      hour = Integer.parseInt(stringValue.substring(8, 10));
                      minutes = Integer.parseInt(stringValue.substring(10, 12));
                      seconds = Integer.parseInt(stringValue.substring(12, 14));
                      break;
                  case 10:
                      if ((columnInfo.getColumnType() ==ColumnType.DATE)
                              || (stringValue.indexOf("-") != -1)) {
                          //yyyy-mm-dd
                          year = Integer.parseInt(stringValue.substring(0, 4));
                          month = Integer.parseInt(stringValue.substring(5, 7));
                          day = Integer.parseInt(stringValue.substring(8, 10));
                          hour = 0;
                          minutes = 0;
                      } else {
                          //yymmddhhmm
                          year = Integer.parseInt(stringValue.substring(0, 2));
                          if (year <= 69) {
                              year = (year + 100);
                          }
                          month = Integer.parseInt(stringValue.substring(2, 4));
                          day = Integer.parseInt(stringValue.substring(4, 6));
                          hour = Integer.parseInt(stringValue.substring(6, 8));
                          minutes = Integer.parseInt(stringValue.substring(8, 10));
                          year += 1900;
                          // two-digit year
                      }
                      break;
                  case 8:
                      if (stringValue.indexOf(":") != -1) {
                          // hh:mm:ss
                          hour = Integer.parseInt(stringValue.substring(0, 2));
                          minutes = Integer.parseInt(stringValue.substring(3, 5));
                          seconds = Integer.parseInt(stringValue.substring(6, 8));
                          year = 1970;
                          month = 1;
                          day = 1;
                          break;
                      } else {
                          // yyyymmdd
                          year = Integer.parseInt(stringValue.substring(0, 4));
                          month = Integer.parseInt(stringValue.substring(4, 6));
                          day = Integer.parseInt(stringValue.substring(6, 8));
                          year -= 1900;
                          month--;
                      }
                      break;
                  default:
                      if(length >= 19 && length <= 26){
                          // yyyy-mm-dd hh:mm:ss[.f...]
                          year = Integer.parseInt(stringValue.substring(0, 4));
                          month = Integer.parseInt(stringValue.substring(5, 7));
                          day = Integer.parseInt(stringValue.substring(8, 10));
                          hour = Integer.parseInt(stringValue.substring(11, 13));
                          minutes = Integer.parseInt(stringValue.substring(14, 16));
                          seconds = Integer.parseInt(stringValue.substring(17, 19));
                          break;
                      }
                      throw new SQLException("Bad format for Timestamp ");
              }
              return  buildTimestmap(year,month,day,hour,minutes,seconds,nanos,calendar,timezone);
          }
      } catch (NumberFormatException e) {
          SQLException sqlException = new SQLException("Cannot convert value " +new String(buf, pos, length,
                  getCurrentEncoding(columnInfo.getColumnType()))+ " to TIMESTAMP.");
          sqlException.initCause(e);
          throw  sqlException;
      }
  }

  public static int getInt(byte[] buf, int offset, int endpos) throws NumberFormatException {
    int len = endpos - offset;
    int res = 0;
    for (int i = 0; i < len; i++) {
      int power = len - i - 1;
      res += (int) ((buf[offset + i ] - '0') * Math.pow(10, power));
    }
    return res;
  }

  public Timestamp getTimestampFromBytes(ColumnDefinition columnInfo  ,Calendar calendar,TimeZone timezone) throws SQLException {
    int start = pos;
    int end = pos + this.length - 1;
    while (start <= end && Character.isWhitespace(buf[start])) {
      start++;
    }
    while (end >= start && Character.isWhitespace(buf[end])) {
      end--;
    }
    int newLength = end - start + 1;

    byte[] bytes = new byte[newLength];
    System.arraycopy(buf, start, bytes, 0, newLength);

    int length = bytes.length;
    int nanosPos = -1;
    int nanosLength = 0;
    boolean haveHalfLine = false;
    boolean haveColon = false;

    for (int i = 0; i < length; i++) {
      if (bytes[i] == '-') {
        haveHalfLine = true;
      }
      if (bytes[i] == ':') {
        haveColon = true;
      }
      if (bytes[i] == '.') {
        length = i ;
        nanosPos = i;
        nanosLength = bytes.length - length - 1;
      }
    }

    try {
      int year = 1970;
      int month = 0;
      int day = 0;
      int hour = 0;
      int minutes = 0;
      int seconds = 0;
      int nanos = 0;
      if (nanosLength > 0 && nanosPos > 0) {
        nanos = getInt(bytes,  nanosPos + 1 , bytes.length);
        if (nanosLength < 9) {
          int factor = (int) (Math.pow(10, 9 - nanosLength));
          nanos = nanos * factor;
        }
      }

      switch (length) {
        case 2: // YY
        case 4: // YYMM
        case 6: // YYMMDD
          year = getInt(bytes, 0, 2);
          if (year <= 69) {
            year = (year + 100);
          }
          year += 1900;
          switch (length) {
            case 2:
              month = 1;
              day = 1;
              break;
            case 4:
              month = getInt(bytes, 2, 4);
              day = 1;
              break;
            case 6:
              month = getInt(bytes, 2, 4);
              day = getInt(bytes, 4, 6);
              break;
          }
          break;
        case 12:
          // yymmddhhmmss
          year = getInt(bytes, 0, 2);
          if (year <= 69) {
            year = (year + 100);
          }
          year += 1900;
          month = getInt(bytes, 2, 4);
          day = getInt(bytes, 4, 6);
          hour = getInt(bytes, 6, 8);
          minutes = getInt(bytes, 8, 10);
          seconds = getInt(bytes, 10, 12);
          break;
        case 14:
          // yyyymmddhhmmss
          year = getInt(bytes, 0, 4);
          month = getInt(bytes, 4, 6);
          day = getInt(bytes, 6, 8);
          hour = getInt(bytes, 8, 10);
          minutes = getInt(bytes, 10, 12);
          seconds = getInt(bytes, 12, 14);
          break;
        case 10:
          if ((columnInfo.getColumnType() == ColumnType.DATE) || haveHalfLine) {
            //yyyy-mm-dd
            year = getInt(bytes, 0, 4);
            month = getInt(bytes, 5, 7);
            day = getInt(bytes, 8, 10);
            hour = 0;
            minutes = 0;
          } else {
            //yymmddhhmm
            year = getInt(bytes, 0, 2);
            if (year <= 69) {
              year = (year + 100);
            }
            month = getInt(bytes, 2, 4);
            day = getInt(bytes, 4, 6);
            hour = getInt(bytes, 6, 8);
            minutes = getInt(bytes, 8, 10);
            year += 1900;
            // two-digit year
          }
          break;
        case 8:
          if (haveColon) {
            // hh:mm:ss
            hour = getInt(bytes, 0, 2);
            minutes = getInt(bytes, 3, 5);
            seconds = getInt(bytes, 6, 8);
            year = 1970;
            month = 1;
            day = 1;
            break;
          } else {
            // yyyymmdd
            year = getInt(bytes, 0, 4);
            month = getInt(bytes, 4, 6);
            day = getInt(bytes, 6, 8);
            year -= 1900;
            month--;
          }
          break;
        default:
          if (length >= 19 && length <= 26) {
            // yyyy-mm-dd hh:mm:ss[.f...]
            year = getInt(bytes, 0, 4);
            month = getInt(bytes, 5, 7);
            day = getInt(bytes, 8, 10);
            hour = getInt(bytes, 11, 13);
            minutes = getInt(bytes, 14, 16);
            seconds = getInt(bytes, 17, 19);
            break;
          }
          throw new SQLException("Bad format for Timestamp ");
      }
      boolean dateIsZero = (year + month + day) == 0 ;
      boolean dateTimeIsZero = (year + month + day + hour + minutes + seconds ) == 0;

      if (bytes[0] == '0' && (dateIsZero || dateTimeIsZero)) {
        if (options.zeroDateTimeBehavior.equalsIgnoreCase(ZERO_DATETIME_EXCEPTION)) {
          if (options.compatibleMysqlVersion == 8) {
            throw new SQLException("Zero date value prohibited");
          }
          throw new SQLException("Value '" + new String(buf, pos, length,
                  getCurrentEncoding(columnInfo.getColumnType())) + "' can not be represented as java.sql.Timestamp");
        }
        if (options.zeroDateTimeBehavior.equalsIgnoreCase(ZERO_DATETIME_CONVERT_TO_NULL)) {
          lastValueNull |= BIT_LAST_ZERO_DATE;
          return null;
        } else {
          // round
          year = 1;
          month = 1;
          day = 1;
        }
        Timestamp tt;
        Calendar localCal = getCalendarInstance(calendar);
        synchronized (localCal) {
          localCal.clear();
          localCal.set(year, month - 1, day, hour, minutes, seconds);
          tt = new Timestamp(localCal.getTimeInMillis());
        }
        tt.setNanos(nanos);
        return tt;
      }

      Timestamp timestamp = buildTimestmap(year, month, day, hour, minutes, seconds, nanos, calendar, timezone);
      return timestamp;
    } catch (NumberFormatException e) {
      SQLException sqlException = new SQLException("Cannot convert value " +new String(buf, pos, length,
              getCurrentEncoding(columnInfo.getColumnType()))+ " to TIMESTAMP.");
      sqlException.initCause(e);
      throw  sqlException;
    }
  }


  public Time getTimeFromString(String raw,Calendar cal) throws SQLException {
      String newRaw = raw;
      if (raw.contains(".")) {
        String[] split = raw.split("\\.");
        newRaw = split[0];
      }
      if (!options.useLegacyDatetimeCode
              && (raw.startsWith("-") || raw.split(":").length != 3 || raw.indexOf(":") > 3
              || (newRaw.length() != 5 && newRaw.length() != 8))){
          throw new SQLException("Time format \"" + raw + "\" incorrect, must be HH:mm:ss");
      }
      boolean negate = raw.startsWith("-");
      if (negate) {
          raw = raw.substring(1);
      }
      String[] rawPart = raw.split(":");
      if (rawPart.length == 3) {
          int hour = Integer.parseInt(rawPart[0]);
          int minutes = Integer.parseInt(rawPart[1]);
          int seconds = Integer.parseInt(rawPart[2].substring(0, 2));
          Calendar calendar = getCalendarInstance(cal);
          if (options.useLegacyDatetimeCode) {
              calendar.setLenient(true);
          }
          calendar.clear();
          calendar.set(1970, Calendar.JANUARY, 1, (negate ? -1 : 1) * hour, minutes, seconds);
          int nanoseconds = extractNanos(raw);
          calendar.set(Calendar.MILLISECOND, nanoseconds / 1000000);
          return new Time(calendar.getTimeInMillis());
      } else {
          throw new SQLException(
                  raw + " cannot be parse as time. time must have \"99:99:99\" format");
      }
  }

  public Date getDateFromString(String raw,Calendar cal,ColumnDefinition columnInfo) throws SQLException {
      if(raw == null) {
          return  null;
      }
      int year = 0;
      int month = 0;
      int day = 0;
      // remove fractional
      int index = raw.indexOf(".");
      if (index > -1) {
          raw = raw.substring(0, index);
      }
      if (raw.equals("00000000000000") || raw.equals("0") || raw.equals("0000-00-00") || raw.equals("0000-00-00 00:00:00")) {
          if (options.zeroDateTimeBehavior.equalsIgnoreCase(ZERO_DATETIME_EXCEPTION)) {
              throw new SQLException("Value '" + new String(buf, pos, length,
                      getCurrentEncoding(columnInfo.getColumnType())) + "' can not be represented as java.sql.Timestamp");
          }
          if (options.zeroDateTimeBehavior.equalsIgnoreCase(ZERO_DATETIME_CONVERT_TO_NULL)) {
              lastValueNull |= BIT_LAST_ZERO_DATE;
              return null;
          } else {
              // round
              year = 1;
              month = 1;
              day = 1;
          }
          Calendar calendar = getCalendarInstance(cal);
          if (options.useLegacyDatetimeCode) {
              calendar.setLenient(true);
          }
          calendar.clear();
          calendar.set(year,month-1,day);
          Date dt = new Date(calendar.getTimeInMillis());
          return dt;
      } else {
          if (raw.length() < 10) {
              if (raw.length() == 8) {
                  Calendar calendar = getCalendarInstance(cal);
                  if (options.useLegacyDatetimeCode) {
                      calendar.setLenient(true);
                  }
                  synchronized (calendar) {
                      java.util.Date origCalDate = calendar.getTime();
                      try {
                          calendar.clear();
                          calendar.set(Calendar.MILLISECOND, 0);
                          calendar.set(1970,  0, 1, 0, 0, 0);
                          long dateAsMillis = calendar.getTimeInMillis();
                          return new Date(dateAsMillis);
                      } finally {
                          calendar.setTime(origCalDate);
                      }
                  }
              }
              throw new SQLException("date format error");
          }

          if (raw.length() != 18) {
              year = Integer.parseInt(raw.substring(0, 4));
              month = Integer.parseInt(raw.substring(5, 7));
              day = Integer.parseInt(raw.substring(8, 10));
          }
          Calendar calendar = getCalendarInstance(cal);
          if (options.useLegacyDatetimeCode) {
              calendar.setLenient(true);
          }
          calendar.clear();
          //  This different than java.util.date, in the year part, but it still keeps the silly '0' for the start month
          calendar.set(year,month - 1,day,0,0,0);
          Date dt = new Date(calendar.getTimeInMillis());
          return dt;
      }
  }

  /**
   * convertToZeroWithEmptyCheck
   * @return
   * @throws SQLException
   */
  public int convertToZeroWithEmptyCheck() throws SQLException {
    if (options.emptyStringsConvertToZero) {
      return 0;
    }

    throw new SQLException("Can't convert empty string ('') to numeric");
  }

}
