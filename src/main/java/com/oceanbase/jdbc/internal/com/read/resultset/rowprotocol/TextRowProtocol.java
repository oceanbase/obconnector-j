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
import java.sql.*;
import java.sql.Date;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Calendar;
import java.util.TimeZone;

import com.oceanbase.jdbc.extend.datatype.*;
import com.oceanbase.jdbc.internal.ColumnType;
import com.oceanbase.jdbc.internal.com.read.resultset.ColumnDefinition;
import com.oceanbase.jdbc.internal.util.Utils;
import com.oceanbase.jdbc.internal.util.exceptions.ExceptionFactory;
import com.oceanbase.jdbc.util.Options;

public class TextRowProtocol extends RowProtocol {

    /**
     * Constructor.
     *
     * @param maxFieldSize max field size
     * @param options connection options
     */
    public TextRowProtocol(int maxFieldSize, Options options) {
        super(maxFieldSize, options);
    }

    /**
     * Set length and pos indicator to asked index.
     *
     * @param newIndex index (0 is first).
     */
    public void setPosition(int newIndex) {
        if (index != newIndex) {
            if (index == -1 || index > newIndex) {
                pos = 0;
                index = 0;
            } else {
                index++;
                if (length != NULL_LENGTH) {
                    pos += length;
                }
            }

            for (; index <= newIndex; index++) {
                if (index != newIndex) {
                    int type = this.buf[this.pos++] & 0xff;
                    switch (type) {
                        case 251:
                            break;
                        case 252:
                            pos += 2 + (0xffff & (((buf[pos] & 0xff) + ((buf[pos + 1] & 0xff) << 8))));
                            break;
                        case 253:
                            pos += 3 + (0xffffff & ((buf[pos] & 0xff)
                                                    + ((buf[pos + 1] & 0xff) << 8) + ((buf[pos + 2] & 0xff) << 16)));
                            break;
                        case 254:
                            pos += 8 + ((buf[pos] & 0xff) + ((long) (buf[pos + 1] & 0xff) << 8)
                                        + ((long) (buf[pos + 2] & 0xff) << 16)
                                        + ((long) (buf[pos + 3] & 0xff) << 24)
                                        + ((long) (buf[pos + 4] & 0xff) << 32)
                                        + ((long) (buf[pos + 5] & 0xff) << 40)
                                        + ((long) (buf[pos + 6] & 0xff) << 48) + ((long) (buf[pos + 7] & 0xff) << 56));
                            break;
                        default:
                            pos += type;
                            break;
                    }
                } else {
                    int type = this.buf[this.pos++] & 0xff;
                    switch (type) {
                        case 251:
                            length = NULL_LENGTH;
                            this.lastValueNull = BIT_LAST_FIELD_NULL;
                            return;
                        case 252:
                            length = 0xffff & ((buf[pos++] & 0xff) + ((buf[pos++] & 0xff) << 8));
                            break;
                        case 253:
                            length = 0xffffff & ((buf[pos++] & 0xff) + ((buf[pos++] & 0xff) << 8) + ((buf[pos++] & 0xff) << 16));
                            break;
                        case 254:
                            length = (int) ((buf[pos++] & 0xff) + ((long) (buf[pos++] & 0xff) << 8)
                                            + ((long) (buf[pos++] & 0xff) << 16)
                                            + ((long) (buf[pos++] & 0xff) << 24)
                                            + ((long) (buf[pos++] & 0xff) << 32)
                                            + ((long) (buf[pos++] & 0xff) << 40)
                                            + ((long) (buf[pos++] & 0xff) << 48) + ((long) (buf[pos++] & 0xff) << 56));
                            break;
                        default:
                            length = type;
                            break;
                    }
                    this.lastValueNull = BIT_LAST_FIELD_NOT_NULL;
                    return;
                }
            }
        }
        this.lastValueNull = length == NULL_LENGTH ? BIT_LAST_FIELD_NULL : BIT_LAST_FIELD_NOT_NULL;
    }

    /**
     * Get String from raw text format.
     *
     * @param columnInfo column information
     * @param cal calendar
     * @param timeZone time zone
     * @return String value
     * @throws SQLException if column type doesn't permit conversion
     */
    public String getInternalString(ColumnDefinition columnInfo, Calendar cal, TimeZone timeZone)
                                                                                                 throws SQLException {
        if (lastValueWasNull()) {
            return null;
        }
        switch (columnInfo.getColumnType()) {
            case BIT:
                return String.valueOf(parseBit());
            case DOUBLE:
            case FLOAT:
                return zeroFillingIfNeeded(new String(buf, pos, length,
                    getCurrentEncoding(columnInfo.getColumnType())), columnInfo);
            case TIME:
                return getInternalTimeString(columnInfo);
            case DATE:
                Date date = getInternalDate(columnInfo, cal, TimeZone.getDefault());
                if (date == null) {
                    if ((lastValueNull & BIT_LAST_ZERO_DATE) != 0) {
                        lastValueNull ^= BIT_LAST_ZERO_DATE;
                        return new String(buf, pos, length,
                            getCurrentEncoding(columnInfo.getColumnType()));
                    }
                    return null;
                }
                return date.toString();
            case YEAR:
                if (options.yearIsDateType) {
                    Date date1 = getInternalDate(columnInfo, cal, TimeZone.getDefault());
                    return (date1 == null) ? null : date1.toString();
                }
                break;
            case TIMESTAMP:
            case TIMESTAMP_NANO:
            case DATETIME:
                // use local time zone
                Timestamp timestamp = getInternalTimestamp(columnInfo, cal, TimeZone.getDefault());
                if (timestamp == null) {
                    if ((lastValueNull & BIT_LAST_ZERO_DATE) != 0) {
                        lastValueNull ^= BIT_LAST_ZERO_DATE;
                    }
                    return null;
                }
                if (getProtocol().isOracleMode()) {
                    LocalDateTime localDateTime = timestamp.toLocalDateTime();
                    String str = DataTypeUtilities.TIMESTAMPTZToString(localDateTime.getYear(),
                        localDateTime.getMonthValue(), localDateTime.getDayOfMonth(), localDateTime
                            .getHour(), localDateTime.getMinute(), localDateTime.getSecond(),
                        localDateTime.getNano() == 0 ? -1 : localDateTime.getNano(), 0, null);
                    return str;
                } else {
                    return timestamp.toString();
                }
            case INTERVALDS:
                INTERVALDS intervalds = getInternalINTERVALDS(columnInfo);
                return intervalds.toString();
            case INTERVALYM:
                INTERVALYM intervalym = getInternalINTERVALYM(columnInfo);
                return intervalym.toString();
            case DECIMAL:
            case OLDDECIMAL:
                BigDecimal bigDecimal = getInternalBigDecimal(columnInfo);
                return (bigDecimal == null) ? null : zeroFillingIfNeeded(bigDecimal.toString(),
                    columnInfo);
            case RAW: {
                byte[] data = new byte[length];
                System.arraycopy(buf, pos, data, 0, length);
                boolean wasNullFlag;
                if (data != null) {
                    wasNullFlag = false;
                    return Utils.toHexString(data);
                } else {
                    wasNullFlag = true;
                    return null;
                }
            }
            case BINARY_FLOAT:
                Float f = Float.valueOf(new String(buf, pos, length, getCurrentEncoding(columnInfo
                    .getColumnType())));
                return Float.toString(f);
            case BINARY_DOUBLE:
                Double d = Double.valueOf(new String(buf, pos, length,
                    getCurrentEncoding(columnInfo.getColumnType())));
                return Double.toString(d);
            case NULL:
                return null;
            default:
                break;
        }

        if (maxFieldSize > 0) {
            return new String(buf, pos, Math.min(maxFieldSize * 3, length),
                getCurrentEncoding(columnInfo.getColumnType())).substring(0,
                Math.min(maxFieldSize, length));
        }

        return new String(buf, pos, length, getCurrentEncoding(columnInfo.getColumnType()));
    }

    /**
     * Get int from raw text format.
     *
     * @param columnInfo column information
     * @return int value
     * @throws SQLException if column type doesn't permit conversion or not in Integer range
     */
    public int getInternalInt(ColumnDefinition columnInfo) throws SQLException {
        if (lastValueWasNull()) {
            return 0;
        }
        long value = getInternalLongUtil(columnInfo, Integer.class, Integer.MIN_VALUE,
            Integer.MAX_VALUE);
        rangeCheck(Integer.class, Integer.MIN_VALUE, Integer.MAX_VALUE, value, columnInfo);
        return (int) value;
    }

    /**
     * Get long from raw text format.
     *
     * @param columnInfo column information
     * @return long value
     * @throws SQLException if column type doesn't permit conversion or not in Long range (unsigned)
     */
    public long getInternalLong(ColumnDefinition columnInfo) throws SQLException {
        if (lastValueWasNull()) {
            return 0;
        }
        long value = getInternalLongUtil(columnInfo, Long.class, Long.MIN_VALUE, Long.MAX_VALUE);
        return value;
    }

    public long getInternalLongUtil(ColumnDefinition columnInfo, Object className, long minValue,
                                    long maxValue) throws SQLException {
        if (lastValueWasNull()) {
            return 0;
        }
        try {
            switch (columnInfo.getColumnType()) {
                case FLOAT:
                    Float floatValue = Float.valueOf(new String(buf, pos, length,
                        getCurrentEncoding(columnInfo.getColumnType())));
                    // check
                    if (floatValue.compareTo((float) Long.MAX_VALUE) >= 1) {
                        throw new SQLException("Out of range value for column '"
                                               + columnInfo.getName()
                                               + "' : value "
                                               + new String(buf, pos, length,
                                                   getCurrentEncoding(columnInfo.getColumnType()))
                                               + " is not in Long range", "22003", 1264);
                    }
                    return floatValue.longValue();
                case DOUBLE:
                    Double doubleValue = Double.valueOf(new String(buf, pos, length,
                        getCurrentEncoding(columnInfo.getColumnType())));
                    // check
                    if (doubleValue.compareTo((double) Long.MAX_VALUE) >= 1) {
                        throw new SQLException("Out of range value for column '"
                                               + columnInfo.getName()
                                               + "' : value "
                                               + new String(buf, pos, length,
                                                   getCurrentEncoding(columnInfo.getColumnType()))
                                               + " is not in Long range", "22003", 1264);
                    }
                    return doubleValue.longValue();
                case BIT:
                    return parseBit();
                case TINYINT:
                case SMALLINT:
                case YEAR:
                case INTEGER:
                case MEDIUMINT:
                case BIGINT:
                    long result = 0;
                    boolean negate = false;
                    int begin = pos;
                    if (length > 0 && buf[begin] == 45) { // minus sign
                        negate = true;
                        begin++;
                    }
                    for (; begin < pos + length; begin++) {
                        result = result * 10 + buf[begin] - 48;
                    }
                    // specific for BIGINT : if value > Long.MAX_VALUE , will become negative until -1
                    if (result < 0) {
                        // CONJ-399 : handle specifically Long.MIN_VALUE that has absolute value +1 compare to
                        // LONG.MAX_VALUE
                        if (result == Long.MIN_VALUE && negate) {
                            return Long.MIN_VALUE;
                        }
                        throw new SQLException("Out of range value for column '"
                                               + columnInfo.getName()
                                               + "' for value "
                                               + new String(buf, pos, length,
                                                   getCurrentEncoding(columnInfo.getColumnType())),
                            "22003", 1264);
                    }
                    return (negate ? -1 * result : result);
                default:
                    return Long.parseLong(new String(buf, pos, length,
                        getCurrentEncoding(columnInfo.getColumnType())).trim());
            }

        } catch (NumberFormatException nfe) {
            NumberFormatException exceptionTmp = null;
            // parse error.
            // if its a decimal retry without the decimal part.
            String value = new String(buf, pos, length,
                getCurrentEncoding(columnInfo.getColumnType()));
            if (isIntegerRegex.matcher(value).find()) {
                try {
                    return Long.parseLong(value.substring(0, value.indexOf(".")));
                } catch (NumberFormatException nfee) {
                    // eat exception
                }
            }
            if (!getProtocol().isOracleMode()) {
                try {
                    double doubleVal = Double.parseDouble(value);
                    // check
                    if (options.jdbcCompliantTruncation) {
                        rangeCheck(className, minValue, maxValue, doubleVal, columnInfo);
                    }
                    return (long) doubleVal;
                } catch (NumberFormatException e) {
                    throw new SQLException("Out of range value for column '" + columnInfo.getName()
                                           + "' : value " + value, "22003", 1264);
                }
            }
            throw new SQLException("Out of range value for column '" + columnInfo.getName()
                                   + "' : value " + value, "22003", 1264);
        }
    }

    /**
     * Get float from raw text format.
     *
     * @param columnInfo column information
     * @return float value
     * @throws SQLException if column type doesn't permit conversion or not in Float range
     */
    public float getInternalFloat(ColumnDefinition columnInfo) throws SQLException {
        if (lastValueWasNull()) {
            return 0;
        }

        switch (columnInfo.getColumnType()) {
            case BIT:
                return parseBit();
            case TINYINT:
            case SMALLINT:
            case YEAR:
            case INTEGER:
            case MEDIUMINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case VARSTRING:
            case VARCHAR:
            case VARCHAR2:
            case NVARCHAR2:
            case STRING:
            case OLDDECIMAL:
            case BIGINT:
            case NUMBER_FLOAT:
            case BINARY_FLOAT:
                try {
                    return Float.valueOf(new String(buf, pos, length, getCurrentEncoding(columnInfo
                        .getColumnType())));
                    // check
                } catch (NumberFormatException nfe) {
                    SQLException sqlException = new SQLException(
                        "Incorrect format \""
                                + new String(buf, pos, length,
                                    getCurrentEncoding(columnInfo.getColumnType()))
                                + "\" for getFloat for data field with type "
                                + columnInfo.getColumnType().getSqlTypeName(), "22003", 1264);
                    //noinspection UnnecessaryInitCause
                    sqlException.initCause(nfe);
                    throw sqlException;
                }
                //            case BINARY_DOUBLE:
                //                Double d = Double.valueOf(new String(buf, pos, length, getCurrentEncoding(columnInfo.getColumnType())));
                //                return (float)d;
            case OBDECIMAL:
                String value = new String(buf, pos, length,
                    getCurrentEncoding(columnInfo.getColumnType())).trim();
                BigDecimal bigDecimal = new BigDecimal(value);
                return bigDecimal.floatValue();
            default:
                throw new SQLException("getFloat not available for data field type "
                                       + columnInfo.getColumnType().getSqlTypeName());
        }
    }

    /**
     * Get double from raw text format.
     *
     * @param columnInfo column information
     * @return double value
     * @throws SQLException if column type doesn't permit conversion or not in Double range (unsigned)
     */
    public double getInternalDouble(ColumnDefinition columnInfo) throws SQLException {
        if (lastValueWasNull()) {
            return 0;
        }
        switch (columnInfo.getColumnType()) {
            case BIT:
                return parseBit();
            case TINYINT:
            case SMALLINT:
            case YEAR:
            case INTEGER:
            case MEDIUMINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case VARSTRING:
            case VARCHAR:
            case VARCHAR2:
            case STRING:
            case OLDDECIMAL:
            case BIGINT:

            case BINARY_DOUBLE:
                try {
                    return Double.valueOf(new String(buf, pos, length,
                        getCurrentEncoding(columnInfo.getColumnType())));
                } catch (NumberFormatException nfe) {
                    SQLException sqlException = new SQLException(
                        "Incorrect format \""
                                + new String(buf, pos, length,
                                    getCurrentEncoding(columnInfo.getColumnType()))
                                + "\" for getDouble for data field with type "
                                + columnInfo.getColumnType().getSqlTypeName(), "22003", 1264);
                    //noinspection UnnecessaryInitCause
                    sqlException.initCause(nfe);
                    throw sqlException;
                }
            case BINARY_FLOAT:
            case NUMBER_FLOAT:
                Float f = Float.valueOf(new String(buf, pos, length, getCurrentEncoding(columnInfo
                    .getColumnType())));
                return (double) f;
            case OBDECIMAL:
                String value = new String(buf, pos, length,
                    getCurrentEncoding(columnInfo.getColumnType()));
                BigDecimal bigDecimal = new BigDecimal(value);
                return bigDecimal.doubleValue();
            default:
                throw new SQLException("getDouble not available for data field type "
                                       + columnInfo.getColumnType().getSqlTypeName());
        }
    }

    /**
     * Get BigDecimal from raw text format.
     *
     * @param columnInfo column information
     * @return BigDecimal value
     */
    public BigDecimal getInternalBigDecimal(ColumnDefinition columnInfo) throws SQLException {
        if (lastValueWasNull()) {
            return null;
        }

        if (columnInfo.getColumnType() == ColumnType.BIT) {
            return BigDecimal.valueOf(parseBit());
        }

        switch (columnInfo.getColumnType()) {
            case BINARY_FLOAT:
                Float f = Float.valueOf(new String(buf, pos, length, getCurrentEncoding(columnInfo
                    .getColumnType())));
                // check
                return BigDecimal.valueOf(f);
            case BINARY_DOUBLE:
                Double d = Double.valueOf(new String(buf, pos, length,
                    getCurrentEncoding(columnInfo.getColumnType())));
                // check
                return BigDecimal.valueOf(d);
            default:
                break;
        }
        String value = new String(buf, pos, length, getCurrentEncoding(columnInfo.getColumnType()))
            .trim();
        try {
            BigDecimal retVal = new BigDecimal(value);
            return retVal;
        } catch (Exception e) {
            throw new SQLException("Bad format for BigDecimal '" + value + "'");
        }
    }

    /**
     * Get date from raw text format.
     *
     * @param columnInfo column information
     * @param cal calendar
     * @param timeZone time zone
     * @return date value
     * @throws SQLException if column type doesn't permit conversion
     */
    @SuppressWarnings("deprecation")
    public Date getInternalDate(ColumnDefinition columnInfo, Calendar cal, TimeZone timeZone)
                                                                                             throws SQLException {
        try {
            if (lastValueWasNull()) {
                return null;
            }

            switch (columnInfo.getColumnType()) {
                case DATE:
                    int[] datePart = new int[] { 0, 0, 0 };
                    int partIdx = 0;
                    for (int begin = pos; begin < pos + length; begin++) {
                        byte b = buf[begin];
                        if (b == '-') {
                            partIdx++;
                            continue;
                        }
                        if (b < '0' || b > '9') {
                            throw new SQLException("cannot parse data in date string '"
                                                   + new String(buf, pos, length,
                                                       getCurrentEncoding(columnInfo
                                                           .getColumnType())) + "'");
                        }
                        datePart[partIdx] = datePart[partIdx] * 10 + b - 48;
                    }

                    if (datePart[0] == 0 && datePart[1] == 0 && datePart[2] == 0) {
                        lastValueNull |= BIT_LAST_ZERO_DATE;
                        return null;
                    }

                    return new Date(datePart[0] - 1900, datePart[1] - 1, datePart[2]);

                case TIMESTAMP:
                case TIMESTAMP_NANO:
                case DATETIME:
                case TIMESTAMP_LTZ:
                    Timestamp timestamp = getInternalTimestamp(columnInfo, cal, timeZone);
                    if (timestamp == null) {
                        return null;
                    }
                    return new Date(timestamp.getTime());
                case TIMESTAMP_TZ:
                    TIMESTAMPTZ timestamptz = getInternalTIMESTAMPTZ(columnInfo, cal, timeZone);
                    if (timestamptz == null) {
                        return null;
                    }
                    return timestamptz.dateValue();
                case TIME:
                    if (length != 0) {
                        int year = 0;
                        int month = 0;
                        int day = 0;
                        int hour = 0;
                        int minute = 0;
                        int seconds = 0;
                        if (length != 0) {
                            // bits[0] // skip tm->neg
                            // binaryData.readLong(); // skip daysPart
                            hour = buf[pos + 5];
                            minute = buf[pos + 6];
                            seconds = buf[pos + 7];
                        }

                        year = 1970;
                        month = 1;
                        day = 1;
                        Calendar dateCal = getCalendarInstance(cal);
                        synchronized (dateCal) {
                            java.util.Date origCalDate = dateCal.getTime();
                            try {
                                dateCal.clear();
                                dateCal.set(Calendar.MILLISECOND, 0);

                                // why-oh-why is this different than java.util.date, in the year part, but it still keeps the silly '0' for the start month????
                                dateCal.set(year, month - 1, day, 0, 0, 0);

                                long dateAsMillis = dateCal.getTimeInMillis();

                                return new Date(dateAsMillis);
                            } finally {
                                dateCal.setTime(origCalDate);
                            }
                        }
                    }
                case YEAR:
                    int year = 0;
                    for (int begin = pos; begin < pos + length; begin++) {
                        year = year * 10 + buf[begin] - 48;
                    }
                    if (length == 2 && columnInfo.getLength() == 2) {
                        if (year <= 69) {
                            year += 2000;
                        } else {
                            year += 1900;
                        }
                    }
                    return new Date(year - 1900, 0, 1);

                default:
                    ColumnType type = columnInfo.getColumnType();
                    if (!getProtocol().isOracleMode()
                        && (type == ColumnType.STRING || type == ColumnType.VARCHAR || type == ColumnType.VARSTRING)) {
                        return getDateFromString(new String(buf, pos, length,
                            getCurrentEncoding(columnInfo.getColumnType())), cal, columnInfo);
                    }
                    try {
                        DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        sdf.setTimeZone(timeZone);
                        java.util.Date utilDate = sdf.parse(new String(buf, pos, length,
                            getCurrentEncoding(columnInfo.getColumnType())));
                        return new Date(utilDate.getTime());

                    } catch (ParseException e) {
                        throw ExceptionFactory.INSTANCE.create("Could not get object as Date : "
                                                               + e.getMessage(), "S1009", e);
                    }

            }
        } catch (SQLException sqlException) {
            throw sqlException; // don't re-wrap
        } catch (Exception e) {
            SQLException sqlException = new SQLException("Bad format for DATE "
                                                         + new String(buf, pos, length,
                                                             getCurrentEncoding(columnInfo
                                                                 .getColumnType())));
            sqlException.initCause(e);
            throw sqlException;
        }
    }

    /**
     * Get time from raw text format.
     *
     * @param columnInfo column information
     * @param cal calendar
     * @param timeZone time zone
     * @return time value
     * @throws SQLException if column type doesn't permit conversion
     */
    public Time getInternalTime(ColumnDefinition columnInfo, Calendar cal, TimeZone timeZone)
                                                                                             throws SQLException {
        try {
            if (lastValueWasNull()) {
                return null;
            }

            if (columnInfo.getColumnType() == ColumnType.TIMESTAMP
                || columnInfo.getColumnType() == ColumnType.TIMESTAMP_NANO
                || columnInfo.getColumnType() == ColumnType.DATETIME
                || columnInfo.getColumnType() == ColumnType.TIMESTAMP_TZ
                || columnInfo.getColumnType() == ColumnType.TIMESTAMP_LTZ) {
                Timestamp timestamp = getInternalTimestamp(columnInfo, cal, timeZone);
                return (timestamp == null) ? null : new Time(timestamp.getTime());

            } else if (columnInfo.getColumnType() == ColumnType.DATE) {
                if (length != 0) {
                    int year = 0;
                    int month = 0;
                    int day = 0;
                    int hour = 0;
                    int minute = 0;
                    int seconds = 0;
                    year = (buf[pos + 0] & 0xff) | ((buf[pos + 1] & 0xff) << 8);
                    month = buf[pos + 2];
                    day = buf[pos + 3];
                    Calendar calendar = getCalendarInstance(cal);
                    synchronized (calendar) {
                        java.util.Date origCalDate = calendar.getTime();
                        try {
                            calendar.clear();
                            // Set 'date' to epoch of Jan 1, 1970
                            calendar.set(1970, 0, 1, hour, minute, seconds);
                            long timeAsMillis = calendar.getTimeInMillis();
                            return new Time(timeAsMillis);
                        } finally {
                            calendar.setTime(origCalDate);
                        }
                    }
                }
                throw new SQLException("Cannot read Time using a Types.DATE field");
            } else {
                String raw = new String(buf, pos, length,
                    getCurrentEncoding(columnInfo.getColumnType()));
                if (!getProtocol().isOracleMode()) {
                    return getTimeFromString(raw, cal);
                }
                if (!options.useLegacyDatetimeCode
                    && (raw.startsWith("-") || raw.split(":").length != 3 || raw.indexOf(":") > 3)) {
                    throw new SQLException("Time format \"" + raw
                                           + "\" incorrect, must be HH:mm:ss");
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
                    calendar.set(1970, Calendar.JANUARY, 1, (negate ? -1 : 1) * hour, minutes,
                        seconds);
                    int nanoseconds = extractNanos(raw);
                    calendar.set(Calendar.MILLISECOND, nanoseconds / 1000000);

                    return new Time(calendar.getTimeInMillis());
                } else {
                    throw new SQLException(
                        raw + " cannot be parse as time. time must have \"99:99:99\" format");
                }
            }
        } catch (SQLException e) {
            throw e; // don't re-wrap
        } catch (NumberFormatException e) {
            SQLException sqlException = new SQLException("Cannot convert value "
                                                         + new String(buf, pos, length,
                                                             getCurrentEncoding(columnInfo
                                                                 .getColumnType()))
                                                         + " to TIMESTAMP.");
            sqlException.initCause(e);
            throw sqlException;
        }
    }

    /**
     * Get timestamp from raw text format.
     *
     * @param columnInfo column information
     * @param userCalendar calendar
     * @param timeZone time zone
     * @return timestamp value
     * @throws SQLException if column type doesn't permit conversion
     */
    public Timestamp getInternalTimestamp(ColumnDefinition columnInfo, Calendar userCalendar,
                                          TimeZone timeZone) throws SQLException {
        if (lastValueWasNull()) {
            return null;
        }
        if (this.getProtocol().isOracleMode()) {
            Calendar cal = getCalendarInstanceWithTimezone(userCalendar, timeZone);
            TIMESTAMP timestamp = null;
            switch (columnInfo.getColumnType()) {
                case TIMESTAMP_TZ:
                    TIMESTAMPTZ oracleTimestampZ = getInternalTIMESTAMPTZ(columnInfo, userCalendar,
                        timeZone);
                    timestamp = TIMESTAMPTZ.toTIMESTAMP(getProtocol(), oracleTimestampZ.toBytes());
                    return timestamp.timestampValue(cal);
                case TIMESTAMP_LTZ:
                    TIMESTAMPLTZ oracleTimestampLTZ = getInternalTIMESTAMPLTZ(columnInfo,
                        userCalendar, timeZone);
                    timestamp = TIMESTAMPLTZ.toTIMESTAMP(getProtocol(),
                        oracleTimestampLTZ.getBytes());
                    return timestamp.timestampValue(cal);
            }
        }
        // other time type use system timezone
        switch (columnInfo.getColumnType()) {
            case TIMESTAMP:
            case DATETIME:
            case DATE:
            case VARCHAR:
            case VARCHAR2:
            case VARSTRING:
            case STRING:
                String rawValue = new String(buf, pos, length,
                    getCurrentEncoding(columnInfo.getColumnType()));
                return getTimestampFromString(columnInfo, rawValue, userCalendar, timeZone);
            case TIME:
                // time does not go after millisecond
                rawValue = new String(buf, pos, length,
                    getCurrentEncoding(columnInfo.getColumnType()));
                Timestamp tt = new Timestamp(getInternalTime(columnInfo, userCalendar,
                    TimeZone.getDefault()).getTime());
                tt.setNanos(extractNanos(rawValue));
                return tt;

            case TIMESTAMP_NANO:
                Calendar cal = getCalendarInstance(userCalendar);
                return getInternalTIMESTAMP(columnInfo, userCalendar, TimeZone.getDefault())
                    .timestampValue(cal);

            default:
                String value = new String(buf, pos, length,
                    getCurrentEncoding(columnInfo.getColumnType()));
                throw new SQLException("Value type \"" + columnInfo.getColumnType().getTypeName()
                                       + "\" with value \"" + value
                                       + "\" cannot be parse as Timestamp");
        }
    }

    @Override
    public Array getInternalArray(ColumnDefinition columnInfo, ComplexDataType complexDataType,
                                  Connection connection) throws SQLException {
        return null;
    }

    @Override
    public Struct getInternalStruct(ColumnDefinition columnInfo, ComplexDataType complexDataType,
                                    Connection connection) throws SQLException {
        return null;
    }

    @Override
    public ComplexData getInternalComplexCursor(ColumnDefinition columnInfo,
                                                ComplexDataType complexDataType,
                                                Connection connection) throws SQLException {
        return null;
    }

    /**
     * Get Object from raw text format.
     *
     * @param columnInfo column information
     * @param timeZone time zone
     * @return Object value
     * @throws SQLException if column type doesn't permit conversion
     */
    public Object getInternalObject(ColumnDefinition columnInfo, TimeZone timeZone)
                                                                                   throws SQLException {
        if (lastValueWasNull()) {
            return null;
        }

        switch (columnInfo.getColumnType()) {
            case BIT:
                if (columnInfo.getLength() == 1) {
                    return buf[pos] != 0;
                }
                byte[] dataBit = new byte[length];
                System.arraycopy(buf, pos, dataBit, 0, length);
                return dataBit;
            case TINYINT:
                if (options.tinyInt1isBit && columnInfo.getLength() == 1) {
                    return buf[pos] != '0';
                }
                return getInternalInt(columnInfo);
            case INTEGER:
                if (!columnInfo.isSigned()) {
                    return getInternalLong(columnInfo);
                }
                return getInternalInt(columnInfo);
            case BIGINT:
                if (!columnInfo.isSigned()) {
                    return getInternalBigInteger(columnInfo);
                }
                return getInternalLong(columnInfo);
            case DOUBLE:
                return getInternalDouble(columnInfo);
            case VARCHAR:
            case VARSTRING:
            case STRING:
            case VARCHAR2:
            case ENUM:
                if (columnInfo.isBinary()) {
                    byte[] data = new byte[getLengthMaxFieldSize()];
                    System.arraycopy(buf, pos, data, 0, getLengthMaxFieldSize());
                    return data;
                }
                return getInternalString(columnInfo, null, timeZone);
            case TIMESTAMP:
            case TIMESTAMP_NANO:
            case DATETIME:
                return getInternalTimestamp(columnInfo, null, timeZone);
            case DATE:
                return getInternalDate(columnInfo, null, timeZone);
            case DECIMAL:
            case OBDECIMAL:
                return getInternalBigDecimal(columnInfo);
            case BLOB:
            case LONGBLOB:
            case MEDIUMBLOB:
            case TINYBLOB:
                byte[] dataBlob = new byte[getLengthMaxFieldSize()];
                System.arraycopy(buf, pos, dataBlob, 0, getLengthMaxFieldSize());
                return dataBlob;
            case NULL:
                return null;
            case YEAR:
                if (options.yearIsDateType) {
                    return getInternalDate(columnInfo, null, timeZone);
                }
                return getInternalShort(columnInfo);
            case SMALLINT:
            case MEDIUMINT:
                return getInternalInt(columnInfo);
            case FLOAT:
                return getInternalFloat(columnInfo);
            case TIME:
                return getInternalTime(columnInfo, null, timeZone);
            case OLDDECIMAL:
            case JSON:
                return getInternalString(columnInfo, null, timeZone);
            case GEOMETRY:
                byte[] data = new byte[length];
                System.arraycopy(buf, pos, data, 0, length);
                return data;
            case NEWDATE:
                break;
            case SET:
                break;
            case NUMBER:
            case NUMBER_FLOAT:
                return getInternalNumber(columnInfo).bigDecimalValue();
            case BINARY_FLOAT:
                Float f = Float.valueOf(new String(buf, pos, length, getCurrentEncoding(columnInfo
                    .getColumnType())));
                return f;
            case BINARY_DOUBLE:
                Double d = Double.valueOf(new String(buf, pos, length,
                    getCurrentEncoding(columnInfo.getColumnType())));
                return d;
            case NVARCHAR2:
            case NCHAR:
                return getInternalString(columnInfo, null, timeZone);
            case TIMESTAMP_TZ:
                return getInternalTIMESTAMPTZ(columnInfo, null, timeZone);
            case TIMESTAMP_LTZ:
                return getInternalTIMESTAMPLTZ(columnInfo, null, timeZone);
            case INTERVALYM:
                return getInternalINTERVALYM(columnInfo);
            case INTERVALDS:
                return getInternalINTERVALDS(columnInfo);
            case RAW:
                byte[] returnBytes = new byte[length];
                System.arraycopy(buf, pos, returnBytes, 0, length);
                return returnBytes;
            default:
                break;
        }
        throw ExceptionFactory.INSTANCE.notSupported("Type '"
                                                     + columnInfo.getColumnType().getTypeName()
                                                     + "' is not supported");
    }

    /**
     * Get boolean from raw text format.
     *
     * @param columnInfo column information
     * @return boolean value
     */
    public boolean getInternalBoolean(ColumnDefinition columnInfo) throws SQLException {
        if (lastValueWasNull()) {
            return false;
        }

        if (columnInfo.getColumnType() == ColumnType.BIT) {
            return parseBit() != 0;
        }
        long boolVal = 0;
        switch (columnInfo.getColumnType()) {
            case TINYINT:
            case SMALLINT:
            case YEAR:
            case INTEGER:
            case MEDIUMINT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case OLDDECIMAL:
                boolVal = getInternalLong(columnInfo);
                return (boolVal > 0 || boolVal == -1);
            default:
                if (columnInfo.isBinary()) {
                    byte[] bytes = new byte[length];
                    System.arraycopy(buf, pos, bytes, 0, length);
                    return Utils.convertBytesToBoolean(bytes);
                } else {
                    final String rawVal = new String(buf, pos, length,
                        getCurrentEncoding(columnInfo.getColumnType()));
                    return Utils.convertStringToBoolean(rawVal);
                }
        }
    }

    /**
     * Get byte from raw text format.
     *
     * @param columnInfo column information
     * @return byte value
     * @throws SQLException if column type doesn't permit conversion
     */
    public byte getInternalByte(ColumnDefinition columnInfo) throws SQLException {
        if (lastValueWasNull()) {
            return 0;
        }
        long value = getInternalLong(columnInfo);
        rangeCheck(Byte.class, Byte.MIN_VALUE, Byte.MAX_VALUE, value, columnInfo);
        return (byte) value;
    }

    /**
     * Get short from raw text format.
     *
     * @param columnInfo column information
     * @return short value
     * @throws SQLException if column type doesn't permit conversion or value is not in Short range
     */
    public short getInternalShort(ColumnDefinition columnInfo) throws SQLException {
        if (lastValueWasNull()) {
            return 0;
        }
        long value = getInternalLong(columnInfo);
        rangeCheck(Short.class, Short.MIN_VALUE, Short.MAX_VALUE, value, columnInfo);
        return (short) value;
    }

    /**
     * Get Time in string format from raw text format.
     *
     * @param columnInfo column information
     * @return String representation of time
     */
    public String getInternalTimeString(ColumnDefinition columnInfo) {
        if (lastValueWasNull()) {
            return null;
        }

        String rawValue = new String(buf, pos, length,
            getCurrentEncoding(columnInfo.getColumnType()));
        if ("0000-00-00".equals(rawValue)) {
            return null;
        }

        if (options.maximizeMysqlCompatibility && options.useLegacyDatetimeCode
            && rawValue.indexOf(".") > 0) {
            return rawValue.substring(0, rawValue.indexOf("."));
        }
        return rawValue;
    }

    /**
     * Get BigInteger format from raw text format.
     *
     * @param columnInfo column information
     * @return BigInteger value
     */
    public BigInteger getInternalBigInteger(ColumnDefinition columnInfo) {
        if (lastValueWasNull()) {
            return null;
        }
        return new BigInteger(new String(buf, pos, length,
            getCurrentEncoding(columnInfo.getColumnType())).trim());
    }

    /**
     * Get ZonedDateTime format from raw text format.
     *
     * @param columnInfo column information
     * @param clazz class for logging
     * @param timeZone time zone
     * @return ZonedDateTime value
     * @throws SQLException if column type doesn't permit conversion
     */
    public ZonedDateTime getInternalZonedDateTime(ColumnDefinition columnInfo, Class clazz,
                                                  TimeZone timeZone) throws SQLException {
        if (lastValueWasNull()) {
            return null;
        }
        if (length == 0) {
            lastValueNull |= BIT_LAST_FIELD_NULL;
            return null;
        }
        if (getProtocol().isOracleMode()) {
            if (columnInfo.getColumnType() == ColumnType.TIMESTAMP_NANO
                || columnInfo.getColumnType() == ColumnType.TIMESTAMP_TZ
                || columnInfo.getColumnType() == ColumnType.TIMESTAMP_LTZ) {
                Timestamp oracleTimestamp = getInternalTimestamp(columnInfo, null, timeZone);
                if (oracleTimestamp == null) {
                    return null;
                }
                LocalDateTime localDateTimeNoTimeZone = oracleTimestamp.toLocalDateTime();
                return localDateTimeNoTimeZone.atZone(timeZone.toZoneId());
            }
        }
        String raw = new String(buf, pos, length, getCurrentEncoding(columnInfo.getColumnType()));
        LocalDateTime localDateTime;
        switch (columnInfo.getColumnType().getSqlType()) {
            case Types.TIMESTAMP:
            case Types.DATE:
                Calendar cal = Calendar.getInstance(timeZone);
                Timestamp timestamp = getInternalTimestamp(columnInfo, cal, TimeZone.getDefault());
                if (timestamp == null) {
                    return null;
                }
                try {
                    localDateTime = LocalDateTime.parse(timestamp.toString(),
                        TEXT_LOCAL_DATE_TIME.withZone(timeZone.toZoneId()));
                } catch (DateTimeParseException dateParserEx) {
                    throw new SQLException(
                        timestamp.toString()
                                + " cannot be parse as LocalDateTime. time must have \"yyyy-MM-dd HH:mm:ss[.S]\" format");
                }
                break;
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.CHAR:
                if (raw.startsWith("0000-00-00 00:00:00")) {
                    return null;
                }
                try {
                    if (getProtocol().isOracleMode()) {
                        localDateTime = LocalDateTime.parse(raw,
                            TEXT_LOCAL_DATE_TIME.withZone(timeZone.toZoneId()));
                    } else {
                        cal = Calendar.getInstance(timeZone);
                        timestamp = getInternalTimestamp(columnInfo, cal, TimeZone.getDefault());
                        if (timestamp == null) {
                            return null;
                        }
                        try {
                            localDateTime = LocalDateTime.parse(timestamp.toString(),
                                TEXT_LOCAL_DATE_TIME.withZone(timeZone.toZoneId()));
                        } catch (DateTimeParseException dateParserEx) {
                            throw new SQLException(
                                timestamp.toString()
                                        + " cannot be parse as LocalDateTime. time must have \"yyyy-MM-dd HH:mm:ss[.S]\" format");
                        }
                    }
                } catch (DateTimeParseException dateParserEx) {
                    throw new SQLException(
                        raw
                                + " cannot be parse as ZonedDateTime. time must have \"yyyy-MM-dd[T/ ]HH:mm:ss[.S]\" "
                                + "with offset and timezone format (example : '2011-12-03 10:15:30+01:00[Europe/Paris]')");
                }
                break;

            default:
                throw new SQLException("Cannot read " + clazz.getName() + " using a "
                                       + columnInfo.getColumnType().getSqlTypeName() + " field");
        }
        return ZonedDateTime.of(localDateTime, timeZone.toZoneId());
    }

    /**
     * Get OffsetTime format from raw text format.
     *
     * @param columnInfo column information
     * @param timeZone time zone
     * @return OffsetTime value
     * @throws SQLException if column type doesn't permit conversion
     */
    public OffsetTime getInternalOffsetTime(ColumnDefinition columnInfo, TimeZone timeZone)
                                                                                           throws SQLException {
        if (lastValueWasNull()) {
            return null;
        }
        if (length == 0) {
            lastValueNull |= BIT_LAST_FIELD_NULL;
            return null;
        }

        ZoneId zoneId = timeZone.toZoneId().normalized();
        if (zoneId instanceof ZoneOffset) {
            ZoneOffset zoneOffset = (ZoneOffset) zoneId;
            String raw = new String(buf, pos, length,
                getCurrentEncoding(columnInfo.getColumnType()));
            switch (columnInfo.getColumnType().getSqlType()) {
                case Types.TIMESTAMP:
                    if (raw.startsWith("0000-00-00 00:00:00")) {
                        return null;
                    }
                    try {
                        return ZonedDateTime.parse(raw, TEXT_LOCAL_DATE_TIME.withZone(zoneOffset))
                            .toOffsetDateTime().toOffsetTime();
                    } catch (DateTimeParseException dateParserEx) {
                        throw new SQLException(
                            raw
                                    + " cannot be parse as OffsetTime. time must have \"yyyy-MM-dd HH:mm:ss[.S]\" format");
                    }

                case Types.TIME:
                    try {
                        LocalTime localTime = LocalTime.parse(raw,
                            DateTimeFormatter.ISO_LOCAL_TIME.withZone(zoneOffset));
                        return OffsetTime.of(localTime, zoneOffset);
                    } catch (DateTimeParseException dateParserEx) {
                        throw new SQLException(
                            raw
                                    + " cannot be parse as OffsetTime (format is \"HH:mm:ss[.S]\" for data type \""
                                    + columnInfo.getColumnType() + "\")");
                    }

                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.CHAR:
                    try {
                        return OffsetTime.parse(raw, DateTimeFormatter.ISO_OFFSET_TIME);
                    } catch (DateTimeParseException dateParserEx) {
                        throw new SQLException(
                            raw
                                    + " cannot be parse as OffsetTime (format is \"HH:mm:ss[.S]\" with offset for data type \""
                                    + columnInfo.getColumnType() + "\")");
                    }

                default:
                    throw new SQLException("Cannot read " + OffsetTime.class.getName()
                                           + " using a "
                                           + columnInfo.getColumnType().getSqlTypeName() + " field");
            }
        }

        if (options.useLegacyDatetimeCode) {
            // system timezone is not an offset
            throw new SQLException(
                "Cannot return an OffsetTime for a TIME field when default timezone is '"
                        + zoneId
                        + "' (only possible for time-zone offset from Greenwich/UTC, such as +02:00)");
        }

        // server timezone is not an offset
        throw new SQLException(
            "Cannot return an OffsetTime for a TIME field when server timezone '" + zoneId
                    + "' (only possible for time-zone offset from Greenwich/UTC, such as +02:00)");
    }

    public OffsetDateTime getInternalOffsetDateTime(ColumnDefinition columnInfo, TimeZone timeZone)
                                                                                                   throws SQLException {
        if (lastValueWasNull()) {
            return null;
        }
        if (length == 0) {
            lastValueNull |= BIT_LAST_FIELD_NULL;
            return null;
        }

        String raw = new String(buf, pos, length, getCurrentEncoding(columnInfo.getColumnType()));

        switch (columnInfo.getColumnType().getSqlType()) {
            case Types.TIME:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.CHAR:
                try {
                    return OffsetDateTime.parse(raw.replace(" ", "T"));
                } catch (DateTimeParseException dateParserEx) {
                    throw new SQLException(
                        raw
                                + " cannot be parse as LocalTime (format is \"HH:mm:ss[.S]\" for data type \""
                                + columnInfo.getColumnType() + "\")");
                }

            case Types.TIMESTAMP:
                ZonedDateTime zonedDateTime = getInternalZonedDateTime(columnInfo, LocalTime.class,
                    timeZone);
                return zonedDateTime == null ? null : zonedDateTime.withZoneSameInstant(
                    ZoneId.systemDefault()).toOffsetDateTime();

            default:
                throw new SQLException("Cannot read LocalTime using a "
                                       + columnInfo.getColumnType().getSqlTypeName() + " field");
        }

    }

    /**
     * Get LocalTime format from raw text format.
     *
     * @param columnInfo column information
     * @param timeZone time zone
     * @return LocalTime value
     * @throws SQLException if column type doesn't permit conversion
     */
    public LocalTime getInternalLocalTime(ColumnDefinition columnInfo, TimeZone timeZone)
                                                                                         throws SQLException {
        if (lastValueWasNull()) {
            return null;
        }
        if (length == 0) {
            lastValueNull |= BIT_LAST_FIELD_NULL;
            return null;
        }

        String raw = new String(buf, pos, length, getCurrentEncoding(columnInfo.getColumnType()));

        switch (columnInfo.getColumnType().getSqlType()) {
            case Types.TIME:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.CHAR:
                try {
                    return LocalTime.parse(raw,
                        DateTimeFormatter.ISO_LOCAL_TIME.withZone(timeZone.toZoneId()));
                } catch (DateTimeParseException dateParserEx) {
                    throw new SQLException(
                        raw
                                + " cannot be parse as LocalTime (format is \"HH:mm:ss[.S]\" for data type \""
                                + columnInfo.getColumnType() + "\")");
                }

            case Types.TIMESTAMP:
                ZonedDateTime zonedDateTime = getInternalZonedDateTime(columnInfo, LocalTime.class,
                    timeZone);
                return zonedDateTime == null ? null : zonedDateTime.withZoneSameInstant(
                    ZoneId.systemDefault()).toLocalTime();

            default:
                throw new SQLException("Cannot read LocalTime using a "
                                       + columnInfo.getColumnType().getSqlTypeName() + " field");
        }
    }

    /**
     * Get LocalDate format from raw text format.
     *
     * @param columnInfo column information
     * @param timeZone time zone
     * @return LocalDate value
     * @throws SQLException if column type doesn't permit conversion
     */
    public LocalDate getInternalLocalDate(ColumnDefinition columnInfo, TimeZone timeZone)
                                                                                         throws SQLException {
        if (lastValueWasNull()) {
            return null;
        }
        if (length == 0) {
            lastValueNull |= BIT_LAST_FIELD_NULL;
            return null;
        }

        String raw = new String(buf, pos, length, getCurrentEncoding(columnInfo.getColumnType()));

        switch (columnInfo.getColumnType().getSqlType()) {
            case Types.DATE:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.CHAR:
                if (raw.startsWith("0000-00-00")) {
                    return null;
                }
                try {
                    return LocalDate.parse(raw,
                        DateTimeFormatter.ISO_LOCAL_DATE.withZone(timeZone.toZoneId()));
                } catch (DateTimeParseException dateParserEx) {
                    throw new SQLException(
                        raw
                                + " cannot be parse as LocalDate (format is \"yyyy-MM-dd\" for data type \""
                                + columnInfo.getColumnType() + "\")");
                }

            case Types.TIMESTAMP:
                ZonedDateTime zonedDateTime = getInternalZonedDateTime(columnInfo, LocalDate.class,
                    timeZone);
                return zonedDateTime == null ? null : zonedDateTime.withZoneSameInstant(
                    ZoneId.systemDefault()).toLocalDate();

            default:
                throw new SQLException("Cannot read LocalDate using a "
                                       + columnInfo.getColumnType().getSqlTypeName() + " field");
        }
    }

    /**
     * Indicate if data is binary encoded.
     *
     * @return always false.
     */
    public boolean isBinaryEncoded() {
        return false;
    }

    public NUMBER getInternalNumber(ColumnDefinition columnInfo) {
        if (lastValueWasNull()) {
            return new NUMBER("0".getBytes());
        }
        byte[] b = new byte[length];
        System.arraycopy(buf, pos, b, 0, length);
        return new NUMBER(b);
    }

    public NUMBER_FLOAT getInternalNumber_float(ColumnDefinition columnInfo) {
        if (lastValueWasNull()) {
            return new NUMBER_FLOAT(0.0f);
        }
        return new NUMBER_FLOAT(new Float(new String(buf, pos, length,
            getCurrentEncoding(columnInfo.getColumnType()))));
    }

    public BINARY_DOUBLE getInternalBINARY_DOUBLE() {
        if (lastValueWasNull()) {
            return new BINARY_DOUBLE(0.0d);
        }
        byte[] b = new byte[length];
        System.arraycopy(buf, pos, b, 0, length);
        return new BINARY_DOUBLE(b); // todo fixme
    }

    public BINARY_FLOAT getInternalBINARY_FLOAT() {
        if (lastValueWasNull()) {
            return new BINARY_FLOAT(0.0f);
        }
        byte[] b = new byte[length];
        System.arraycopy(buf, pos, b, 0, length);
        return new BINARY_FLOAT(buf); // fixme
    }

    @Override
    public INTERVALDS getInternalINTERVALDS(ColumnDefinition columnInfo) throws SQLException {
        if (lastValueWasNull()) {
            return new INTERVALDS("0 0:0:0.0");
        }
        byte[] b = new byte[length];
        System.arraycopy(buf, pos, b, 0, length);
        return new INTERVALDS(b);
    }

    @Override
    public INTERVALYM getInternalINTERVALYM(ColumnDefinition columnInfo) throws SQLException {
        if (lastValueWasNull()) {
            return new INTERVALYM("0-0");
        }
        byte[] b = new byte[length];
        System.arraycopy(buf, pos, b, 0, length);
        return new INTERVALYM(b);
    }
}
