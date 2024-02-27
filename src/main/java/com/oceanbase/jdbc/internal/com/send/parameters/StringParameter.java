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
package com.oceanbase.jdbc.internal.com.send.parameters;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

import com.oceanbase.jdbc.internal.ColumnType;
import com.oceanbase.jdbc.internal.io.output.PacketOutputStream;

public class StringParameter implements Cloneable, ParameterHolder {

    private final String  stringValue;
    private final boolean noBackslashEscapes;
    private final String  characterEncoding;
    private final Charset charset;

    private boolean needEscape(String str, int length) {
        boolean needed = false;
        for (int i = 0; i < length; i++) {
            char c = str.charAt(i);
            switch (c) {
                case 0:
                case '\n':
                case '\r':
                case '\\':
                case '\'':
                case '"':
                case '\032':
                    needed = true;
                    break;
            }
            if (needed) {
                break;
            }
        }
        return needed;
    }

    private String escapedString(String str, int length) {
        StringBuilder stringBuilder = new StringBuilder((int) (str.length() * 1.1));
        for (int i = 0; i < length; i++) {
            char c = str.charAt(i);
            switch (c) {
                case 0:
                    stringBuilder.append('\\');
                    stringBuilder.append('0');
                    break;
                case '\n':
                    stringBuilder.append('\\');
                    stringBuilder.append('n');
                    break;
                case '\r':
                    stringBuilder.append('\\');
                    stringBuilder.append('r');
                    break;
                case '\\':
                    stringBuilder.append('\\');
                    stringBuilder.append('\\');
                    break;
                case '\'':
                    stringBuilder.append('\\');
                    stringBuilder.append('\'');
                    break;
                case '"':
                    // not support useAnsiQuotes now
                    stringBuilder.append('"');
                    break;

                case '\032':
                    stringBuilder.append('\\');
                    stringBuilder.append('Z');
                    break;
                case '\u00a5':
                case '\u20a9':
                    CharsetEncoder charsetEncoder = Charset.forName(characterEncoding).newEncoder();
                    if (charsetEncoder != null) {
                        CharBuffer charBuffer = CharBuffer.allocate(1);
                        ByteBuffer byteBuffer = ByteBuffer.allocate(1);
                        charBuffer.put(c);
                        charBuffer.position(0);
                        charsetEncoder.encode(charBuffer, byteBuffer, true);
                        if (byteBuffer.get(0) == '\\') {
                            stringBuilder.append('\\');
                        }
                    }
                    stringBuilder.append(c);
                    break;
                default:
                    stringBuilder.append(c);
            }
        }

        return stringBuilder.toString();
    }

    public StringParameter(String str, boolean noBackslashEscapes, String characterEncoding) {
        this.stringValue = str;
        this.noBackslashEscapes = noBackslashEscapes;
        this.characterEncoding = characterEncoding;
        this.charset = Charset.forName(characterEncoding);
    }

    /**
     * Send escaped String to outputStream.
     *
     * @param pos outpustream.
     */
    public void writeTo(final PacketOutputStream pos) throws IOException {
        pos.write(stringValue, true, noBackslashEscapes);
    }

    public int getApproximateTextProtocolLength() {
        return stringValue.length() * 3;
    }

    /**
     * Write data to socket in binary format.
     *
     * @param pos socket output stream
     * @throws IOException if socket error occur
     */
    public void writeBinary(final PacketOutputStream pos) throws IOException {
        byte[] bytes = stringValue.getBytes(charset);
        pos.writeFieldLength(bytes.length);
        pos.write(bytes);
    }

    public ColumnType getColumnType() {
        return ColumnType.VARCHAR;
    }

    @Override
    public String toString() {
        String tmp = stringValue;
        if (needEscape(stringValue, stringValue.length())) {
            tmp = escapedString(stringValue, stringValue.length());
        }
        if (tmp.length() < 1024) {
            return "'" + tmp + "'";
        } else {
            return "'" + tmp.substring(0, 1024) + "...'";
        }
    }

    public String getFullString(){
        String tmp = stringValue;
        if (needEscape(stringValue, stringValue.length())) {
            tmp = escapedString(stringValue, stringValue.length());
        }
        return "'" + tmp + "'";
    }

    public boolean isNullData() {
        return false;
    }

    public boolean isLongData() {
        return false;
    }
}
