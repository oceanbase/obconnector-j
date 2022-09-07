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

import java.math.BigDecimal;
import java.sql.SQLException;

public class BINARY_DOUBLE extends Datum {
    public static final boolean TRACE = false;

    public BINARY_DOUBLE() {
    }

    public BINARY_DOUBLE(byte[] bytes) {
        super(bytes);
    }

    public BINARY_DOUBLE(double doubleVal) {
        super(doubleToCanonicalFormatBytes(doubleVal));
    }

    public BINARY_DOUBLE(Double doubleVal) {
        super(doubleToCanonicalFormatBytes(doubleVal));
    }

    public BINARY_DOUBLE(String stringVal) throws SQLException {
        this(stringToDouble(stringVal));
    }

    public BINARY_DOUBLE(Boolean booleanVal) {
        this((double) (booleanVal ? 1 : 0));
    }

    public Object toJdbc() throws SQLException {
        return new Double(canonicalFormatBytesToDouble(this.getBytes()));
    }

    public boolean isConvertibleTo(Class targetClass) {
        String className = targetClass.getName();
        return className.compareTo("java.lang.String") == 0
               || className.compareTo("java.lang.Double") == 0;
    }

    public String stringValue() {
        return Double.toString(canonicalFormatBytesToDouble(this.getBytes()));
    }

    public double doubleValue() throws SQLException {
        return canonicalFormatBytesToDouble(this.getBytes());
    }

    public BigDecimal bigDecimalValue() throws SQLException {
        return new BigDecimal(canonicalFormatBytesToDouble(this.getBytes()));
    }

    public Object makeJdbcArray(int intVal) {
        return new Double[intVal];
    }

    private static double stringToDouble(String stringVal) throws SQLException {
        try {
            return Double.valueOf(stringVal);
        } catch (NumberFormatException numberFormatException) {
            throw numberFormatException;
        }
    }

    static byte[] doubleToCanonicalFormatBytes(double doubleVal) {
        double doubleValTmp = doubleVal;
        if (doubleVal == 0.0D) {
            doubleValTmp = 0.0D;
        } else if (doubleVal != doubleVal) {
            doubleValTmp = 0.0D / 0.0;
        }

        long longVal = Double.doubleToLongBits(doubleValTmp);
        byte[] bytes = new byte[8];
        int intPart1 = (int) longVal;
        int intPart2 = (int) (longVal >> 32);
        int pos1 = intPart1;
        intPart1 >>= 8;
        int pos2 = intPart1;
        intPart1 >>= 8;
        int pos3 = intPart1;
        intPart1 >>= 8;
        int pos4 = intPart1;
        int pos5 = intPart2;
        intPart2 >>= 8;
        int pos6 = intPart2;
        intPart2 >>= 8;
        int pos7 = intPart2;
        intPart2 >>= 8;
        int pos8;
        if ((intPart2 & 128) == 0) {
            pos8 = intPart2 | 128;
        } else {
            pos8 = ~intPart2;
            pos7 = ~pos7;
            pos6 = ~pos6;
            pos5 = ~pos5;
            pos4 = ~intPart1;
            pos3 = ~pos3;
            pos2 = ~pos2;
            pos1 = ~pos1;
        }

        bytes[7] = (byte) pos1;
        bytes[6] = (byte) pos2;
        bytes[5] = (byte) pos3;
        bytes[4] = (byte) pos4;
        bytes[3] = (byte) pos5;
        bytes[2] = (byte) pos6;
        bytes[1] = (byte) pos7;
        bytes[0] = (byte) pos8;
        return bytes;
    }

    static double canonicalFormatBytesToDouble(byte[] bytes) {
        int intPos1, intPos2, intPos3, intPos4, intPos5, intPos6, intPos7, intPos8;
        if ((bytes[0] & 128) != 0) {
            intPos1 = bytes[0] & 127;
            intPos2 = bytes[1] & 255;
            intPos3 = bytes[2] & 255;
            intPos4 = bytes[3] & 255;
            intPos5 = bytes[4] & 255;
            intPos6 = bytes[5] & 255;
            intPos7 = bytes[6] & 255;
            intPos8 = bytes[7] & 255;
        } else {
            intPos1 = ~bytes[0] & 255;
            intPos2 = ~bytes[1] & 255;
            intPos3 = ~bytes[2] & 255;
            intPos4 = ~bytes[3] & 255;
            intPos5 = ~bytes[4] & 255;
            intPos6 = ~bytes[5] & 255;
            intPos7 = ~bytes[6] & 255;
            intPos8 = ~bytes[7] & 255;
        }

        int longPos1 = intPos1 << 24 | intPos2 << 16 | intPos3 << 8 | intPos4;
        int longPos2 = intPos5 << 24 | intPos6 << 16 | intPos7 << 8 | intPos8;
        long longVal = (long) longPos1 << 32 | (long) longPos2 & 4294967295L;
        return Double.longBitsToDouble(longVal);
    }
}
