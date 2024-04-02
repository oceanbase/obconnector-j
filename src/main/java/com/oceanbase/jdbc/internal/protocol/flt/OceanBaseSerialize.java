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
package com.oceanbase.jdbc.internal.protocol.flt;

import java.io.IOException;

import com.oceanbase.jdbc.internal.com.read.Buffer;

public class OceanBaseSerialize {

    public static long OB_MAX_V1B = 1 << 7 - 1;
    public static long OB_MAX_V2B = 1 << 14 - 1;
    public static long OB_MAX_V3B = 1 << 21 - 1;
    public static long OB_MAX_V4B = 1 << 28 - 1;
    public static long OB_MAX_V5B = 1 << 35 - 1;
    public static long OB_MAX_V6B = 1 << 42 - 1;
    public static long OB_MAX_V7B = 1 << 49 - 1;
    public static long OB_MAX_V8B = 1 << 56 - 1;
    public static long OB_MAX_V9B = 1 << 63 - 1;

    // for encode
    public static long encoded_length_vi64(long val) {
        long need_bytes;
        if (val <= OB_MAX_V1B) {
            need_bytes = 1;
        } else if (val <= OB_MAX_V2B) {
            need_bytes = 2;
        } else if (val <= OB_MAX_V3B) {
            need_bytes = 3;
        } else if (val <= OB_MAX_V4B) {
            need_bytes = 4;
        } else if (val <= OB_MAX_V5B) {
            need_bytes = 5;
        } else if (val <= OB_MAX_V6B) {
            need_bytes = 6;
        } else if (val <= OB_MAX_V7B) {
            need_bytes = 7;
        } else if (val <= OB_MAX_V8B) {
            need_bytes = 8;
        } else if (val <= OB_MAX_V9B) {
            need_bytes = 9;
        } else {
            need_bytes = 10;
        }
        return need_bytes;
    }

    /**
     * Computes the encoded length of vstr(int64,data,null)
     * @param len string length
     * @return the encoded length of str
     */
    public static long encoded_length_vstr_with_len(long len) {
        return encoded_length_vi64(len) + len + 1;
    }

    /**
     * Encode a buf as vstr(int64,data,null)
     *
     * @param buf pointer to the destination buffer
     * @param vbuf pointer to the start of the input buffer
     * @param vlen length of the input buffer
     */
    public static void encode_vstr_with_len(Buffer buf, final byte[] vbuf, long vlen)
                                                                                     throws IOException {
        buf.checkRemainder(encoded_length_vstr_with_len(vlen));
        // even through it's a null string, we can serialize it with length 0, and following a '\0'
        encode_vi64(buf, vlen);
        if (vlen > 0 && null != vbuf) {
            buf.writeBytes(vbuf, 0, (int) vlen);
        }
        buf.writeByte((byte) 0);
    }

    public static void encode_int8_t(Buffer buf, byte val) throws IOException {
        encode_i8(buf, val);
    }

    public static void encode_i8(Buffer buf, byte val) throws IOException {
        buf.checkRemainder(1);
        buf.writeByte(val);
    }

    /**
     * Encode a integer (up to 64bit) in variable length encoding
     *
     * @param buf pointer to the destination buffer
     * @param val value to encode
     */
    public static void encode_vi64(Buffer buf, long val) throws IOException {
        long __v = val;
        buf.checkRemainder(encoded_length_vi64(__v));
        while (__v > OB_MAX_V1B) {
            buf.writeByte((byte) ((__v) | 0x80));
            __v >>= 7;
        }
        if (__v <= OB_MAX_V1B) {
            buf.writeByte((byte) ((__v) & 0x7f));
        }
    }

    // for decode
    public static byte decode_int8_t(Buffer buf) throws IOException {
        return decode_i8(buf);
    }

    public static byte decode_i8(Buffer buf) throws IOException {
        buf.checkRemainder(1);
        return buf.readByte();
    }

    public static long decode_vi64(final Buffer buf) throws IOException {
        long val;
        long __v = 0;
        int shift = 0;

        while ((buf.getByte() & 0x80) != 0) {
            buf.checkRemainder(1);
            __v |= ((long) buf.readByte() & 0x7f) << shift;
            shift += 7;
        }

        buf.checkRemainder(1);
        __v |= ((long) buf.readByte() & 0x7f) << shift;
        val = __v;
        return val;
    }

    public static byte[] decode_vstr_nocopy(final Buffer buf) throws IOException {
        byte[] str = null;
        long lenp = decode_vi64(buf);

        if (lenp >= 0) {
            buf.checkRemainder(lenp);
            str = buf.readBytes((int) lenp);
            buf.skipByte();
        }
        return str;
    }

}
