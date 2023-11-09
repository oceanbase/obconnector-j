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

    public Object toJdbc() throws SQLException {
        return new Float(DataTypeUtilities.bytesToFloat(this.getBytes()));
    }

    public boolean isConvertibleTo(Class targetClass) {
        String className = targetClass.getName();
        return className.compareTo("java.lang.String") == 0
               || className.compareTo("java.lang.Float") == 0;
    }

    public Object makeJdbcArray(int intVal) {
        return new Float[intVal];
    }

    public String stringValue() {
        return Float.toString(DataTypeUtilities.bytesToFloat(this.getBytes()));
    }

    public float floatValue() throws SQLException {
        return DataTypeUtilities.bytesToFloat(this.getBytes());
    }

    public double doubleValue() throws SQLException {
        return this.floatValue();
    }

    public BigDecimal bigDecimalValue() throws SQLException {
        return new BigDecimal(this.floatValue());
    }

    public BINARY_FLOAT(float floatVal) {
        super(DataTypeUtilities.floatToBytes(floatVal));
    }

    public BINARY_FLOAT(Float floatVal) {
        super(DataTypeUtilities.floatToBytes(floatVal));
    }

    public BINARY_FLOAT(Boolean booleanVal) {
        this((float) (booleanVal ? 1 : 0));
    }

    public BINARY_FLOAT(String stringVal) throws SQLException {
        this(DataTypeUtilities.stringToFloat(stringVal));
    }

    public BINARY_FLOAT(byte[] bytes) {
        super(bytes);
    }

    public BINARY_FLOAT() {
    }

}
