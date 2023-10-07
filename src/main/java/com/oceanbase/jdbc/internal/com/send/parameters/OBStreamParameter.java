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
import java.io.InputStream;
import java.sql.SQLException;

import com.oceanbase.jdbc.Lob;
import com.oceanbase.jdbc.internal.ColumnType;
import com.oceanbase.jdbc.internal.com.Packet;
import com.oceanbase.jdbc.internal.io.output.PacketOutputStream;
import com.oceanbase.jdbc.internal.util.Utils;
import com.oceanbase.jdbc.util.Options;

public class OBStreamParameter implements Cloneable, LongDataParameterHolder {

    private InputStream   is;
    private final Lob     lob;
    private final long    length;
    private final boolean noBackslashEscapes;
    private final boolean hasLobLocator;
    private final byte[]  bytes;

    /**
     * Constructor.
     *
     * @param is stream to write
     * @param length max length to write (if null the whole stream will be send)
     * @param noBackslashEscapes must backslash be escape
     */
    public OBStreamParameter(boolean hasLobLocator, InputStream is, long length,
                             boolean noBackslashEscapes) {
        this.is = is;
        this.length = length;
        this.noBackslashEscapes = noBackslashEscapes;
        this.hasLobLocator = hasLobLocator;
        this.bytes = null;
        this.lob = null;
    }

    public OBStreamParameter(boolean hasLobLocator, byte[] bytes, long length,
                             boolean noBackslashEscapes) {
        this.is = null;
        this.length = length;
        this.noBackslashEscapes = noBackslashEscapes;
        this.hasLobLocator = hasLobLocator;
        this.bytes = bytes;
        this.lob = null;
    }

    public OBStreamParameter(boolean hasLobLocator, Lob lob, long length, boolean noBackslashEscapes) {
        this.is = null;
        this.length = length;
        this.noBackslashEscapes = noBackslashEscapes;
        this.hasLobLocator = hasLobLocator;
        this.bytes = null;
        this.lob = lob;
    }

    public OBStreamParameter(InputStream is, long length, boolean noBackslashEscapes) {
        this(false, is, length, noBackslashEscapes);
    }

    public OBStreamParameter(InputStream is, boolean noBackSlashEscapes) {
        this(false, is, Long.MAX_VALUE, noBackSlashEscapes);
    }

    private void setInputStream() throws IOException {
        if (lob != null) {
            try {
                is = lob.getBinaryStream();
            } catch (SQLException throwables) {
                IOException ioException = new IOException();
                ioException.initCause(throwables);
                throw ioException;
            }
        }
    }

    /**
     * Write stream in text format.
     *
     * @param pos database outputStream
     * @throws IOException if any error occur when reader stream
     */
    public void writeTo(final PacketOutputStream pos) throws IOException {
        setInputStream();
        if (is != null) {
            pos.write(QUOTE);
            if (length == Long.MAX_VALUE) {
                pos.writeHex(is, false, noBackslashEscapes);
            } else {
                pos.writeHex(is, length, false, noBackslashEscapes);
            }
            pos.write(QUOTE);
        }
    }

    /**
     * Return approximated data calculated length.
     *
     * @return approximated data length.
     */
    public int getApproximateTextProtocolLength() {
        return -1;
    }

    /**
     * Write data to socket in binary format.
     *
     * @param pos socket output stream
     * @throws IOException if socket error occur
     */
    public void writeBinary(final PacketOutputStream pos) throws IOException {
        setInputStream();
        if (hasLobLocator && bytes != null) {
            pos.writeFieldLength(bytes.length);
            pos.write(bytes);
        } else if (is != null) {
            if (length == Long.MAX_VALUE) {
                pos.write(is, false, noBackslashEscapes);
            } else {
                pos.write(is, length, false, noBackslashEscapes);
            }
        }
    }

    @Override
    public String toString() {
        return "<Stream>";
    }

    public ColumnType getColumnType() {
        return this.hasLobLocator ? ColumnType.ORA_BLOB : ColumnType.STRING;
    }

    public boolean isNullData() {
        return false;
    }

    public boolean isLongData() {
        return this.hasLobLocator ? false : true;
    }

    @Override
    public boolean writePieceData(PacketOutputStream writer, boolean first, Options options)
                                                                                            throws IOException {
        if (first) {
            setInputStream();
        }
        int pieceLen = options.pieceLength;
        byte piece;
        byte[] buffer = new byte[pieceLen];
        int len = is.read(buffer);
        boolean ret = false;
        if (len < 0) {
            writer.write(Packet.OCI_LAST_PIECE);
            writer.write(0);
            writer.writeLong(0);
            writer.flush();
            return false;
        }
        byte[] data = new byte[len];

        System.arraycopy(buffer, 0, data, 0, len);
        data = Utils.toHexString(data).getBytes();
        int lastLen = is.available();
        if (first && lastLen != 0) {
            piece = Packet.OCI_FIRST_PIECE;
            ret = true;
        } else if (first && lastLen == 0) {
            piece = Packet.OCI_LAST_PIECE;
            ret = false;
        } else if (!first && lastLen != 0) {
            piece = Packet.OCI_NEXT_PIECE;
            ret = true;
        } else {
            piece = Packet.OCI_LAST_PIECE;
            ret = false;
        }
        writer.write(piece);
        writer.write(0);
        writer.writeLong(data.length);
        writer.write(data);
        writer.flush();
        return ret;
    }

    /**
     * Never used
     * @param pos
     * @param options
     * @param statementId
     * @param paramIndex
     * @return
     * @throws IOException
     */
    @Override
    public boolean writeLongData(PacketOutputStream pos, Options options, int statementId,
                                 short paramIndex) throws IOException {
        return false;
    }
}
