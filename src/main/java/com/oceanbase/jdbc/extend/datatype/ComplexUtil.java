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
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import com.oceanbase.jdbc.internal.ColumnType;
import com.oceanbase.jdbc.internal.io.output.PacketOutputStream;
import com.oceanbase.jdbc.util.Options;

public class ComplexUtil {
    public static void storeDateTime(PacketOutputStream pos, java.util.Date dt, int bufferType,
                                     Options options) throws SQLException, IOException {
        storeDateTime413AndNewer(pos, dt, bufferType, options);
    }

    public static void storeDateTime413AndNewer(PacketOutputStream pos, java.util.Date dt,
                                                int bufferType, Options options) throws IOException {
        Calendar sessionCalendar = null;
        if (!options.useLegacyDatetimeCode) {
            if (bufferType == ColumnType.DATE.getType()) {
                sessionCalendar = new GregorianCalendar(TimeZone.getDefault());
            } else {
                sessionCalendar = new GregorianCalendar(pos.getTimeZone());
            }
        } else {
            sessionCalendar = new GregorianCalendar();
        }

        java.util.Date oldTime = sessionCalendar.getTime();

        try {
            sessionCalendar.setTime(dt);

            if (dt instanceof java.sql.Date) {
                sessionCalendar.set(Calendar.HOUR_OF_DAY, 0);
                sessionCalendar.set(Calendar.MINUTE, 0);
                sessionCalendar.set(Calendar.SECOND, 0);
            }

            byte length = (byte) 7;

            if (dt instanceof java.sql.Timestamp) {
                length = (byte) 11;
            }
            //            pos.writeBytes((byte) length, 1); // length
            pos.writeBytes(length, 1); // length
            int year = sessionCalendar.get(Calendar.YEAR);
            int month = sessionCalendar.get(Calendar.MONTH) + 1;
            int date = sessionCalendar.get(Calendar.DAY_OF_MONTH);

            pos.writeIntV1(year);
            pos.writeBytes((byte) month, 1);
            pos.writeBytes((byte) date, 1);

            if (dt instanceof java.sql.Date) {
                pos.writeBytes((byte) 0, 1);
                pos.writeBytes((byte) 0, 1);
                pos.writeBytes((byte) 0, 1);
            } else {
                pos.writeBytes((byte) sessionCalendar.get(Calendar.HOUR_OF_DAY), 1);
                pos.writeBytes((byte) sessionCalendar.get(Calendar.MINUTE), 1);
                pos.writeBytes((byte) sessionCalendar.get(Calendar.SECOND), 1);
            }

            if (length == 11) {
                //	MySQL expects microseconds, not nanos
                pos.writeLongV1(((java.sql.Timestamp) dt).getNanos() / 1000);
            }

        } finally {
            sessionCalendar.setTime(oldTime);
        }

    }

    public static ColumnType getMysqlType(int complexType) throws SQLException {
        switch (complexType) {
            case ComplexDataType.TYPE_DATE:
                return ColumnType.DATETIME;
            case ComplexDataType.TYPE_COLLECTION:
            case ComplexDataType.TYPE_OBJECT:
                return ColumnType.COMPLEX;
            case ComplexDataType.TYPE_NUMBER:
                return ColumnType.DECIMAL;
            case ComplexDataType.TYPE_VARCHAR2:
                return ColumnType.VARCHAR;
            case ComplexDataType.TYPE_RAW:
                return ColumnType.RAW;
            default:
                throw new SQLException("unsupported complex type");
        }
    }

    public static void storeComplexStruct(PacketOutputStream pos, ComplexData data, Options options)
                                                                                                    throws Exception {
        int nullCount = (data.getAttrCount() + 7) / 8; // At least 8 bits
        int nullBitsPosition = pos.getPosition();
        for (int i = 0; i < nullCount; i++) {
            pos.writeBytes((byte) 0, 1);
        }
        byte[] nullBitsBuffer = new byte[nullCount];
        for (int i = 0; i < data.getAttrCount(); ++i) {
            if (null != data.getAttrData(i)) {
                storeComplexAttrData(pos, data.getComplexType().getAttrType(i),
                    data.getAttrData(i), options);
            } else {
                nullBitsBuffer[i / 8] |= (1 << (i % 8));
            }
        }
        int endPosition = pos.getPosition();
        pos.setPosition(nullBitsPosition);
        pos.write(nullBitsBuffer);
        pos.setPosition(endPosition);

    }

    public static void storeComplexArray(PacketOutputStream pos, ComplexData data, Options options)
                                                                                                   throws Exception {
        pos.writeFieldLength(data.getAttrCount());
        int nullCount = (data.getAttrCount() + 7) / 8;
        int nullBitsPosition = pos.getPosition();
        for (int i = 0; i < nullCount; i++) {
            pos.writeBytes((byte) 0, 1);
        }
        byte[] nullBitsBuffer = new byte[nullCount];
        for (int i = 0; i < data.getAttrCount(); ++i) {
            if (null != data.getAttrData(i)) {
                storeComplexAttrData(pos, data.getComplexType().getAttrType(0),
                    data.getAttrData(i), options);
            } else {
                nullBitsBuffer[i / 8] |= (1 << (i % 8));
            }
        }
        int endPosition = pos.getPosition();
        pos.setPosition(nullBitsPosition);
        pos.write(nullBitsBuffer);
        pos.setPosition(endPosition);
    }

    public static void storeComplexAttrData(PacketOutputStream pos, ComplexDataType type,
                                            Object value, Options options) throws Exception {
        switch (type.getType()) {
            case ComplexDataType.TYPE_COLLECTION:
                storeComplexArray(pos, (ComplexData) value, options);
                return;
            case ComplexDataType.TYPE_OBJECT:
                storeComplexStruct(pos, (ComplexData) value, options);
                return;
            case ComplexDataType.TYPE_NUMBER:
                String valueStr = String.valueOf((Integer) value);
                pos.writeFieldLength(valueStr.getBytes().length);
                pos.write(valueStr.getBytes(StandardCharsets.UTF_8));
                return;
            case ComplexDataType.TYPE_DATE:
                storeDateTime(pos, (java.util.Date) value, ColumnType.DATETIME.getType(), options);
                return;
            case ComplexDataType.TYPE_VARCHAR2:
            case ComplexDataType.TYPE_RAW:
            case ComplexDataType.TYPE_CHAR:

                if (value instanceof byte[]) {
                    byte[] tmp = (byte[]) value;
                    pos.writeFieldLength(tmp.length);
                    pos.write(tmp);
                } else {
                    String strValue = null;
                    if (value instanceof String) {
                        strValue = (String) value;
                    } else if (value instanceof BigDecimal) {
                        strValue = ((BigDecimal) value).toString();
                    } else if (value instanceof Date) {
                        strValue = ((Date) value).toString();
                    } else if (value instanceof Timestamp) {
                        strValue = ((Timestamp) value).toString();
                    } else {
                        try {
                            strValue = String.valueOf(value);
                        } catch (Exception e) {
                            throw new SQLException("unsupported complex data set for String, type:"
                                                   + value.getClass() + " and content: " + value);
                        }
                    }
                    byte[] tmp = ((String) strValue).getBytes(); //
                    pos.writeFieldLength(tmp.length);
                    pos.write(tmp);
                }

                return;
            default:
                throw new SQLException("unsupported complex data type");
        }
    }
}
