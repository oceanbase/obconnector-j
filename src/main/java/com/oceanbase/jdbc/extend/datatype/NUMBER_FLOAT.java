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
import java.sql.SQLException;

import com.oceanbase.jdbc.internal.ColumnType;
import com.oceanbase.jdbc.internal.io.output.PacketOutputStream;

public class NUMBER_FLOAT extends Datum {

    private final float value;
    private BigDecimal  bigDecimal = null;

    public NUMBER_FLOAT(float value) {
        this.value = value;
    }

    public NUMBER_FLOAT(float value, byte[] bytes) {
        super(bytes);
        this.value = value;
        if (bytes != null) {
            bigDecimal = new BigDecimal(new String(bytes));
        }
    }

    public void writeTo(final PacketOutputStream pos) throws IOException {
        pos.write(String.valueOf(value).getBytes(StandardCharsets.UTF_8));
    }

    public int getApproximateTextProtocolLength() {
        return String.valueOf(value).getBytes().length;
    }

    /**
     * Write data to socket in binary format.
     *
     * @param pos socket output stream
     * @throws IOException if socket error occur
     */
    public void writeBinary(final PacketOutputStream pos) throws IOException {
        pos.writeLong(Double.doubleToLongBits(value));
    }

    public ColumnType getColumnType() {
        return ColumnType.DOUBLE;
    }

    @Override
    public String toString() {
        return Double.toString(value);
    }

    public boolean isNullData() {
        return false;
    }

    public boolean isLongData() {
        return false;
    }

    @Override
    public boolean isConvertibleTo(Class targetClass) {
        String targetClassName = targetClass.getName();
        return targetClassName.compareTo("java.lang.Float") == 0
               || targetClassName.compareTo("java.lang.Double") == 0;
    }

    @Override
    public Object toJdbc() throws SQLException {
        return this.bigDecimal;
    }

    @Override
    public Object makeJdbcArray(int intVal) {
        BigDecimal[] bigDecimals = new BigDecimal[intVal];
        return bigDecimals;
    }
}
