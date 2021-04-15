/*
 *
 * OceanBase Client for Java
 *
 * Copyright (c) 2021 OceanBase Technology Co.,Ltd.
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
    static final long           serialVersionUID = 6718338151847341754L;
    public static final boolean TRACE            = false;

    public BINARY_DOUBLE() {
    }

    public BINARY_DOUBLE(byte[] var1) {
        super(var1);
    }

    public BINARY_DOUBLE(double var1) {
        super(doubleToCanonicalFormatBytes(var1));
    }

    public BINARY_DOUBLE(Double var1) {
        super(doubleToCanonicalFormatBytes(var1));
    }

    public BINARY_DOUBLE(String var1) throws SQLException {
        this(stringToDouble(var1));
    }

    public BINARY_DOUBLE(Boolean var1) {
        this((double) (var1 ? 1 : 0));
    }

    public Object toJdbc() throws SQLException {
        return new Double(canonicalFormatBytesToDouble(this.getBytes()));
    }

    public boolean isConvertibleTo(Class var1) {
        String var2 = var1.getName();
        return var2.compareTo("java.lang.String") == 0 || var2.compareTo("java.lang.Double") == 0;
    }

    public String stringValue() {
        String var1 = Double.toString(canonicalFormatBytesToDouble(this.getBytes()));
        return var1;
    }

    public double doubleValue() throws SQLException {
        return canonicalFormatBytesToDouble(this.getBytes());
    }

    public BigDecimal bigDecimalValue() throws SQLException {
        return new BigDecimal(canonicalFormatBytesToDouble(this.getBytes()));
    }

    public Object makeJdbcArray(int var1) {
        return new Double[var1];
    }

    private static double stringToDouble(String var0) throws SQLException {
        try {
            return Double.valueOf(var0);
        } catch (NumberFormatException var3) {
            throw var3;
        }
    }

    static byte[] doubleToCanonicalFormatBytes(double var0) {
        double var2 = var0;
        if (var0 == 0.0D) {
            var2 = 0.0D;
        } else if (var0 != var0) {
            var2 = 0.0D / 0.0;
        }

        long var4 = Double.doubleToLongBits(var2);
        byte[] var6 = new byte[8];
        int var7 = (int) var4;
        int var8 = (int) (var4 >> 32);
        int var9 = var7;
        var7 >>= 8;
        int var10 = var7;
        var7 >>= 8;
        int var11 = var7;
        var7 >>= 8;
        int var12 = var7;
        int var13 = var8;
        var8 >>= 8;
        int var14 = var8;
        var8 >>= 8;
        int var15 = var8;
        var8 >>= 8;
        int var16;
        if ((var8 & 128) == 0) {
            var16 = var8 | 128;
        } else {
            var16 = ~var8;
            var15 = ~var15;
            var14 = ~var14;
            var13 = ~var13;
            var12 = ~var7;
            var11 = ~var11;
            var10 = ~var10;
            var9 = ~var9;
        }

        var6[7] = (byte) var9;
        var6[6] = (byte) var10;
        var6[5] = (byte) var11;
        var6[4] = (byte) var12;
        var6[3] = (byte) var13;
        var6[2] = (byte) var14;
        var6[1] = (byte) var15;
        var6[0] = (byte) var16;
        return var6;
    }

    static double canonicalFormatBytesToDouble(byte[] var0) {
        byte var1 = var0[0];
        byte var2 = var0[1];
        byte var3 = var0[2];
        byte var4 = var0[3];
        byte var5 = var0[4];
        byte var6 = var0[5];
        byte var7 = var0[6];
        byte var8 = var0[7];
        int var13;
        int var14;
        int var15;
        int var16;
        int var17;
        int var18;
        int var19;
        int var20;
        if ((var1 & 128) != 0) {
            var13 = var1 & 127;
            var14 = var2 & 255;
            var15 = var3 & 255;
            var16 = var4 & 255;
            var17 = var5 & 255;
            var18 = var6 & 255;
            var19 = var7 & 255;
            var20 = var8 & 255;
        } else {
            var13 = ~var1 & 255;
            var14 = ~var2 & 255;
            var15 = ~var3 & 255;
            var16 = ~var4 & 255;
            var17 = ~var5 & 255;
            var18 = ~var6 & 255;
            var19 = ~var7 & 255;
            var20 = ~var8 & 255;
        }

        int var9 = var13 << 24 | var14 << 16 | var15 << 8 | var16;
        int var10 = var17 << 24 | var18 << 16 | var19 << 8 | var20;
        long var11 = (long) var9 << 32 | (long) var10 & 4294967295L;
        return Double.longBitsToDouble(var11);
    }
}
