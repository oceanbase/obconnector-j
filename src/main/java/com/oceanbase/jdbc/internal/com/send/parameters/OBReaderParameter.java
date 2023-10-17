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
import java.io.Reader;

import com.oceanbase.jdbc.Clob;
import com.oceanbase.jdbc.internal.ColumnType;
import com.oceanbase.jdbc.internal.com.Packet;
import com.oceanbase.jdbc.internal.io.output.PacketOutputStream;
import com.oceanbase.jdbc.util.Options;

public class OBReaderParameter implements Cloneable, LongDataParameterHolder {

    private Reader        reader;
    private final Clob    clob;
    private final long    length;
    private final boolean noBackslashEscapes;
    private final boolean hasLobLocator;
    private final byte[]  bytes;

    /**
     * Constructor.
     *
     * @param reader             reader to write
     * @param length             max length to write (can be null)
     * @param noBackslashEscapes must backslash be escape
     */
    public OBReaderParameter(Reader reader, long length, boolean noBackslashEscapes) {
        this.reader = reader;
        this.hasLobLocator = false;
        this.clob = null;
        this.length = length;
        this.bytes = null;
        this.noBackslashEscapes = noBackslashEscapes;
    }

    public OBReaderParameter(Clob clob, long length, boolean noBackslashEscapes) {
        this.reader = null;
        this.hasLobLocator = false;
        this.clob = clob;
        this.length = length;
        this.bytes = null;
        this.noBackslashEscapes = noBackslashEscapes;
    }

    public OBReaderParameter(boolean hasLobLocator, byte[] bytes, long length,
                             boolean noBackslashEscapes) {
        this.reader = null;
        this.hasLobLocator = hasLobLocator;
        this.clob = null;
        this.length = length;
        this.bytes = bytes;
        this.noBackslashEscapes = noBackslashEscapes;
    }

    public OBReaderParameter(Reader reader, boolean noBackslashEscapes) {
        this(reader, Long.MAX_VALUE, noBackslashEscapes);
    }

    private void setReader() {
        if (clob != null) {
            reader = clob.getCharacterStream();
        }
    }

    /**
     * Write reader to database in text format.
     *
     * @param pos database outputStream
     * @throws IOException if any error occur when reading reader
     */
    public void writeTo(PacketOutputStream pos) throws IOException {
        setReader();
        if (!hasLobLocator && reader != null) {
            pos.write(QUOTE);
            if (length == Long.MAX_VALUE) {
                pos.writeEscapeQuote(reader, noBackslashEscapes);
            } else {
                pos.writeEscapeQuote(reader, length, noBackslashEscapes);
            }
            pos.write(QUOTE);
        }
    }

    /**
     * Return approximated data calculated length for rewriting queries.
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
        setReader();
        if (hasLobLocator && bytes != null) {
            pos.writeFieldLength(bytes.length);
            pos.write(bytes, 0, bytes.length);
        } else if (reader != null && length == Long.MAX_VALUE) {
            pos.write(reader, false, noBackslashEscapes);
        } else {
            pos.write(reader, length, false, noBackslashEscapes);
        }
    }

    public ColumnType getColumnType() {
        return this.hasLobLocator ? ColumnType.ORA_CLOB : ColumnType.BLOB;
    }

    @Override
    public String toString() {
        return "<Reader>";
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
            setReader();
        }
        int pieceLen = options.pieceLength;
        byte piece;
        char[] buffer = new char[pieceLen];
        int len = reader.read(buffer);
        boolean ret = false;
        if (len < 0) {
            writer.write(Packet.OCI_LAST_PIECE);
            writer.write(0);
            writer.writeLong(0);
            writer.flush();
            return false;
        }
        byte[] data = new String(buffer, 0, len).getBytes(options.getCharacterEncoding());
        if (first && reader.ready()) {
            piece = Packet.OCI_FIRST_PIECE;
            ret = true;
        } else if (first && !reader.ready()) {
            piece = Packet.OCI_LAST_PIECE;
            ret = false;
        } else if (!first && reader.ready()) {
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
