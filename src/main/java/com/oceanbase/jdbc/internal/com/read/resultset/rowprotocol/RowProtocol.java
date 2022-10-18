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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.regex.Pattern;

import com.oceanbase.jdbc.extend.datatype.*;
import com.oceanbase.jdbc.internal.ColumnType;
import com.oceanbase.jdbc.internal.com.read.resultset.ColumnDefinition;
import com.oceanbase.jdbc.internal.com.read.resultset.SelectResultSet;
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
      case VARCHAR:
      case VARCHAR2:
      case VARSTRING:
      case NVARCHAR2:
      case NCHAR:
      case RAW:
      case STRING:
        return Charset.forName(this.options.characterEncoding);
      default:
        break;
    }
    return  StandardCharsets.UTF_8;

  }
  public abstract void setPosition(int position);

  public int getLengthMaxFieldSize() {
    return maxFieldSize != 0 && maxFieldSize < length ? maxFieldSize : length;
  }

  public int getMaxFieldSize() {
    return maxFieldSize;
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

  public abstract Array getInternalArray(ColumnDefinition columnInfo,ComplexDataType complexDataType) throws  SQLException;
  public abstract Struct getInternalStruct(ColumnDefinition columnInfo,ComplexDataType complexDataType) throws  SQLException;
  public abstract ComplexData getInternalComplexCursor(ColumnDefinition columnInfo,ComplexDataType complexDataType) throws  SQLException;
  public String getEncoding() {
    return options.characterEncoding;
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

  private TIMESTAMP buildTIMETAMP(byte[] bytes, int pos, int length) throws SQLException {
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

  public abstract String getInternalTimeString(ColumnDefinition columnInfo);

  public abstract BigInteger getInternalBigInteger(ColumnDefinition columnInfo) throws SQLException;

  public abstract ZonedDateTime getInternalZonedDateTime(
      ColumnDefinition columnInfo, Class clazz, TimeZone timeZone) throws SQLException;

  public abstract OffsetTime getInternalOffsetTime(ColumnDefinition columnInfo, TimeZone timeZone)
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
  public SelectResultSet sendFechRowViaCursor(long statementid,int fetchSize) {
      return (SelectResultSet) SelectResultSet.createEmptyResultSet();
  }
}
