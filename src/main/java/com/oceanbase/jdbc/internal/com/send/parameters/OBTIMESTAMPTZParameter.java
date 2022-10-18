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
package com.oceanbase.jdbc.internal.com.send.parameters;

import java.io.IOException;
import java.sql.SQLException;

import com.oceanbase.jdbc.extend.datatype.TIMESTAMPTZ;
import com.oceanbase.jdbc.internal.ColumnType;
import com.oceanbase.jdbc.internal.io.output.PacketOutputStream;

public class OBTIMESTAMPTZParameter implements Cloneable, ParameterHolder {

    private final TIMESTAMPTZ   ts;

    private static final byte[] LITERALS_TIMESTAMP = "timestamp ".getBytes();
    private static final int    ORACLE_TIME_SCALE  = 9;                      // When serializing oracle time type , fixedly fill scale 9 .
    private boolean             isTZTablesImported = false;

    /**
     * Constructor.
     *
     * @param ts timestamps
     */
    public OBTIMESTAMPTZParameter(TIMESTAMPTZ ts) {
        this.ts = ts;
    }

    public OBTIMESTAMPTZParameter(TIMESTAMPTZ ts, boolean isTZTablesImported) {
        this.ts = ts;
        this.isTZTablesImported = isTZTablesImported;
    }

    /**
     * Write timestamps to outputStream.
     *
     * @param pos the stream to write to
     */
    public void writeTo(final PacketOutputStream pos) throws IOException {
        try {
            pos.write(LITERALS_TIMESTAMP);
            pos.write(QUOTE);
            String tmString = ts.toResultSetString(null);
            pos.write(tmString.getBytes());
            pos.write(QUOTE);
        } catch (SQLException e) {
            throw new IOException("get String value error");
        }
    }

    public int getApproximateTextProtocolLength() {
        return 27;
    }

    /**
     * Write data to socket in binary format.
     *
     * @param pos socket output stream
     * @throws IOException if socket error occur
     */
    public void writeBinary(final PacketOutputStream pos) throws IOException {
        byte[] data = ts.getBytes();
        int length = data.length;
        data[11] = (byte) ORACLE_TIME_SCALE;
        pos.write((byte) length);
        pos.write(data, 0, data.length);
    }

    public ColumnType getColumnType() {
        return ColumnType.TIMESTAMP_TZ;
    }

    @Override
    public String toString() {
        return "'" + ts.toString() + "'";
    }

    public boolean isNullData() {
        return false;
    }

    public boolean isLongData() {
        return false;
    }
}
