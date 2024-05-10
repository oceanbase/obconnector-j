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

import com.oceanbase.jdbc.ObStruct;
import com.oceanbase.jdbc.extend.datatype.ComplexUtil;
import com.oceanbase.jdbc.internal.ColumnType;
import com.oceanbase.jdbc.internal.io.output.PacketOutputStream;
import com.oceanbase.jdbc.util.Options;

public class OBStructParameter implements Cloneable, ParameterHolder {
    ObStruct structValue;
    Options  options;

    public void storeStructTypeInfo(PacketOutputStream packet) throws IOException {

        ObStruct struct = structValue;
        byte[] tmp = struct.getComplexType().getSchemaName().getBytes();
        packet.writeFieldLength(tmp.length);
        packet.write(tmp);
        tmp = struct.getComplexType().getTypeName().getBytes();
        packet.writeFieldLength(tmp.length);
        packet.write(tmp);
        packet.writeFieldLength(struct.getComplexType().getVersion());
    }

    public OBStructParameter(ObStruct structImpl, Options options) {
        this.structValue = structImpl;
        this.options = options;
    }

    @Override
    public void writeTo(PacketOutputStream os) throws IOException {

    }

    @Override
    public void writeBinary(PacketOutputStream pos) throws IOException {
        try {
            int nullCount = (structValue.getAttrCount() + 7) / 8;
            int nullBitsPosition = pos.getPosition();
            for (int i = 0; i < nullCount; i++) {
                pos.writeBytes((byte) 0, 1);
            }
            byte[] nullBitsBuffer = new byte[nullCount];
            for (int i = 0; i < structValue.getAttrCount(); ++i) {
                if (null != structValue.getAttrData(i)) {
                    ComplexUtil.storeComplexAttrData(pos,
                        structValue.getComplexType().getAttrType(i), structValue.getAttrData(i),
                        options);
                } else {
                    nullBitsBuffer[i / 8] |= (1 << (i % 8));
                }
            }
            int endPosition = pos.getPosition();
            pos.setPosition(nullBitsPosition);
            pos.write(nullBitsBuffer); // null bits
            pos.setPosition(endPosition);
        } catch (Exception e) {
            throw new IOException("storeComplexAttrData exception");
        }
    }

    @Override
    public int getApproximateTextProtocolLength() throws IOException {
        return 0;
    }

    @Override
    public boolean isNullData() {
        if (structValue.getAttrCount() == 0) {
            return true;
        }
        return false;
    }

    @Override
    public ColumnType getColumnType() {
        return ColumnType.COMPLEX;
    }

    @Override
    public boolean isLongData() {
        return false;
    }
}
