/*
 *
 * OceanBase Client for Java
 *
 * Copyright (c) 2023 OceanBase.
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

package com.oceanbase.jdbc.internal.util;

import java.io.UnsupportedEncodingException;
import java.sql.SQLException;

public class StringUtils {

    private static final int BYTE_RANGE = (1 + Byte.MAX_VALUE) - Byte.MIN_VALUE;
    private static byte[] allBytes = new byte[BYTE_RANGE];
    private static byte[] unknownCharsMap = new byte[65536];

    static {
        for (int i = Byte.MIN_VALUE; i <= Byte.MAX_VALUE; i++) {
            allBytes[i - Byte.MIN_VALUE] = (byte) i;
        }

        for (int i = 0; i < unknownCharsMap.length; i++) {
            unknownCharsMap[i] = (byte) '?'; // use something 'sane' for unknown chars
        }
    }

    public static byte[] getBytesFromString(String s, String characterEncoding) throws SQLException {
        if (s == null) {
            return null;
        }

        try {
            String allBytesString = new String(allBytes, 0, BYTE_RANGE, characterEncoding);
            int allBytesLen = allBytesString.length();

            byte[] charToByteMap = new byte[65536];
            System.arraycopy(unknownCharsMap, 0, charToByteMap, 0, charToByteMap.length);
            for (int i = 0; i < BYTE_RANGE && i < allBytesLen; i++) {
                char c = allBytesString.charAt(i);
                charToByteMap[c] = allBytes[i];
            }

            int length = s.length();
            byte[] bytes = new byte[length];
            for (int i = 0; i < length; i++) {
                bytes[i] = charToByteMap[s.charAt(i)];
            }
            return bytes;
        } catch (UnsupportedEncodingException uee) {
            throw new SQLException(uee.getMessage(), uee);
        }
    }
}
