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
package com.oceanbase.jdbc;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Arrays;

public class LobOutputStream extends OutputStream {
    private final Lob lob;
    private int       pos;

    public LobOutputStream(Lob lob, int pos) {
        this.lob = lob;
        this.pos = pos;
    }

    @Override
    public void write(int bit) throws IOException {

        if (this.pos >= lob.length) {
            byte[] tmp = new byte[2 * lob.length + 1];
            System.arraycopy(lob.data, lob.offset, tmp, 0, lob.length);
            lob.data = tmp;
            pos -= lob.offset;
            lob.offset = 0;
            lob.length++;
        }
        lob.data[pos] = (byte) bit;
        pos++;
    }

    @Override
    public void write(byte[] buf, int off, int len) throws IOException {
        if (off < 0) {
            throw new IOException("Invalid offset " + off);
        }
        int realLen = Math.min(buf.length - off, len);
        if (pos + realLen >= lob.length) {
            int newLen = Math.min(lob.length, pos) + realLen;
            if (this.lob.isOracleMode) {
                newLen = pos + realLen;
            }
            byte[] tmp = new byte[newLen];
            if (this.lob instanceof Clob) {
                Arrays.fill(tmp, (byte) ' ');
            }
            System.arraycopy(lob.data, lob.offset, tmp, 0, lob.length);
            lob.data = tmp;
            pos -= lob.offset;
            lob.offset = 0;
            lob.length = pos + realLen;
        }
        System.arraycopy(buf, off, lob.data, pos, realLen);
        lob.charData = new String(lob.data, 0 , lob.length,
                Charset.forName(lob.encoding == null ? "UTF-8" : lob.encoding));
        pos += realLen;
    }

    @Override
    public void write(byte[] buf) throws IOException {
        if (this.lob.isOracleMode && buf.length == 0) {
            return;
        }
        write(buf, 0, buf.length);
    }
}
