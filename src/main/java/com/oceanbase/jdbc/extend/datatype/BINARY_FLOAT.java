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

public class BINARY_FLOAT extends Datum {
    public static final boolean TRACE = false;

    public BINARY_FLOAT() {
    }

    public BINARY_FLOAT(byte[] bytes) {
        super(bytes);
    }

    public BINARY_FLOAT(float floatVal) {
        super(floatToCanonicalFormatBytes(floatVal));
    }

    public BINARY_FLOAT(Float floatVal) {
        super(floatToCanonicalFormatBytes(floatVal));
    }

    public BINARY_FLOAT(String stringVal) throws SQLException {
        this(stringToFloat(stringVal));
    }

    public BINARY_FLOAT(Boolean booleanVal) {
        this((float) (booleanVal ? 1 : 0));
    }

    public Object toJdbc() throws SQLException {
        return new Float(canonicalFormatBytesToFloat(this.getBytes()));
    }

    public boolean isConvertibleTo(Class targetClass) {
        String className = targetClass.getName();
        return className.compareTo("java.lang.String") == 0
               || className.compareTo("java.lang.Float") == 0;
    }

    public String stringValue() {
        return Float.toString(canonicalFormatBytesToFloat(this.getBytes()));
    }

    public float floatValue() throws SQLException {
        return canonicalFormatBytesToFloat(this.getBytes());
    }

    public double doubleValue() throws SQLException {
        return (double) this.floatValue();
    }

    public BigDecimal bigDecimalValue() throws SQLException {
        return new BigDecimal((double) this.floatValue());
    }

    private static float stringToFloat(String stringVal) throws SQLException {
        try {
            return Float.valueOf(stringVal);
        } catch (NumberFormatException e) {
            throw e;
        }
    }

    public Object makeJdbcArray(int intVal) {
        return new Float[intVal];
    }

    static byte[] floatToCanonicalFormatBytes(float floatVal) {
        float floatValTmp = floatVal;
        if (floatVal == 0.0F) {
            floatValTmp = 0.0F;
        } else if (floatVal != floatVal) {
            floatValTmp = (float) (0.0F / 0.0);
        }

        int floatToIntBits = Float.floatToIntBits(floatValTmp);
        byte[] bytes = new byte[4];
        int floatPos1 = floatToIntBits;
        floatToIntBits >>= 8;
        int floatPos2 = floatToIntBits;
        floatToIntBits >>= 8;
        int floatPos3 = floatToIntBits;
        floatToIntBits >>= 8;
        int floatPos4;
        if ((floatToIntBits & 128) == 0) {
            floatPos4 = floatToIntBits | 128;
        } else {
            floatPos4 = ~floatToIntBits;
            floatPos3 = ~floatPos3;
            floatPos2 = ~floatPos2;
            floatPos1 = ~floatPos1;
        }

        bytes[3] = (byte) floatPos1;
        bytes[2] = (byte) floatPos2;
        bytes[1] = (byte) floatPos3;
        bytes[0] = (byte) floatPos4;
        return bytes;
    }

    static float canonicalFormatBytesToFloat(byte[] bytes) {
        int aInt1, aInt2, aInt3, aInt4;
        if ((bytes[0] & 128) != 0) {
            aInt1 = bytes[0] & 127;
            aInt2 = bytes[1] & 255;
            aInt3 = bytes[2] & 255;
            aInt4 = bytes[3] & 255;
        } else {
            aInt1 = ~bytes[0] & 255;
            aInt2 = ~bytes[1] & 255;
            aInt3 = ~bytes[2] & 255;
            aInt4 = ~bytes[3] & 255;
        }

        int intVal = aInt1 << 24 | aInt2 << 16 | aInt3 << 8 | aInt4;
        return Float.intBitsToFloat(intVal);
    }
}
