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
import java.math.BigInteger;
import java.sql.SQLException;

public class NUMBER extends Datum {
    private BigDecimal bigDecimal;

    public NUMBER() {
    }

    public String stringValue() {
        if (this.bigDecimal != null) {
            return this.bigDecimal.toString();
        }
        return null;
    }

    public boolean booleanValue() {
        if (this.bigDecimal != null) {
            return this.bigDecimal.intValue() == 0;
        }
        return false;
    }

    @Override
    public Object toJdbc() throws SQLException {
        return this.bigDecimal;
    }

    @Override
    public Object makeJdbcArray(int intVal) {
        BigDecimal[] array = new BigDecimal[intVal];
        return array;
    }

    public double doubleValue() {
        if (this.bigDecimal != null) {
            return this.bigDecimal.doubleValue();
        }
        return 0.0;
    }

    public float floatValue() {
        if (this.bigDecimal != null) {
            return this.bigDecimal.floatValue();
        }
        return 0.0f;
    }

    public long longValue() {
        if (this.bigDecimal != null) {
            return this.bigDecimal.longValue();
        }
        return 0;
    }

    public short shortValue() {
        if (this.bigDecimal != null) {
            return this.bigDecimal.shortValue();
        }
        return 0;
    }

    public byte byteValue() {
        if (this.bigDecimal != null) {
            return this.bigDecimal.byteValue();
        }
        return 0;
    }

    public BigInteger bigIntegerValue() {
        if (this.bigDecimal != null) {
            return this.bigDecimal.toBigInteger();
        }
        return null;
    }

    public int intValue() {
        if (this.bigDecimal != null) {
            return this.bigDecimal.intValue();
        }
        return 0;
    }

    public BigDecimal bigDecimalValue() throws SQLException {
        return this.bigDecimal;
    }

    @Override
    public String toString() {
        if (this.bigDecimal != null)
            return this.bigDecimal.toString();
        return null;
    }

    public NUMBER(byte[] bytes) {
        super(bytes);
        if (bytes != null) {
            bigDecimal = new BigDecimal(new String(bytes));
        }
    }

    @Override
    public boolean isConvertibleTo(Class targetClass) {
        String callName = targetClass.getName();
        switch (callName) {
            case "java.lang.Integer":
            case "java.lang.Long":
            case "java.lang.Float":
            case "java.lang.Double":
            case "java.math.BigInteger":
            case "java.math.BigDecimal":
            case "java.lang.String":
            case "java.lang.Boolean":
            case "java.lang.Byte":
            case "java.lang.Short":
                return true;
            default:
                return false;
        }
    }

}
