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
import java.io.InputStream;
import java.sql.SQLException;

import com.oceanbase.jdbc.Lob;
import com.oceanbase.jdbc.internal.ColumnType;
import com.oceanbase.jdbc.internal.com.Packet;
import com.oceanbase.jdbc.internal.io.output.PacketOutputStream;
import com.oceanbase.jdbc.util.Options;

public class StreamParameter implements Cloneable, LongDataParameterHolder {

    private InputStream   is;
    private final Lob     lob;
    private final long    length;
    private final boolean noBackslashEscapes;

    /**
     * Constructor.
     *
     * @param is stream to write
     * @param length max length to write (if null the whole stream will be send)
     * @param noBackslashEscapes must backslash be escape
     */
    public StreamParameter(InputStream is, long length, boolean noBackslashEscapes) {
        this.is = is;
        this.length = length;
        this.noBackslashEscapes = noBackslashEscapes;
        this.lob = null;
    }

    public StreamParameter(Lob lob, long length, boolean noBackslashEscapes) {
        this.lob = lob;
        this.is = null;
        this.length = length;
        this.noBackslashEscapes = noBackslashEscapes;
    }

    public StreamParameter(InputStream is, boolean noBackSlashEscapes) {
        this(is, Long.MAX_VALUE, noBackSlashEscapes);
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
        pos.write(BINARY_INTRODUCER);
        if (length == Long.MAX_VALUE) {
            pos.write(is, true, noBackslashEscapes);
        } else {
            pos.write(is, length, true, noBackslashEscapes);
        }
        pos.write(QUOTE);
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
        if (length == Long.MAX_VALUE) {
            pos.write(is, false, noBackslashEscapes);
        } else {
            pos.write(is, length, false, noBackslashEscapes);
        }
    }

    @Override
    public String toString() {
        return "<Stream>";
    }

    public ColumnType getColumnType() {
        return ColumnType.BLOB;
    }

    public boolean isNullData() {
        return false;
    }

    public boolean isLongData() {
        return true;
    }

    /**
     *  Never used
     * @param pos
     * @param first
     * @param options
     * @return
     * @throws IOException
     */
    @Override
    public boolean writePieceData(PacketOutputStream pos, boolean first, Options options)
                                                                                         throws IOException {
        return false;
    }

    /**
     * Read the data in InputStream and send Long Data Packet in batches according to the size of blobSendChunkSize
     *
     * @param writer PacketOutputStream with socket
     * @param options Connection options
     * @param statementId  Current statement id
     * @param paramIndex Current parameter index
     * @return  No effect, temporarily reserved
     * @throws IOException
     */
    @Override
    public boolean writeLongData(PacketOutputStream writer, Options options, int statementId,
                                 short paramIndex) throws IOException {
        setInputStream();
        byte[] buffer = new byte[BLOB_STREAM_READ_BUF_SIZE];
        int len;
        int totoalReadLen = 0;
        int currentPacketLen = 0;
        int lastReadLen = 0;
        boolean isEmpty = true;
        int sizeToSend = options.blobSendChunkSize;
        writer.startPacket(0);
        writer.write(Packet.COM_STMT_SEND_LONG_DATA);
        writer.writeInt(statementId);
        writer.writeShort(paramIndex);
        while ((len = is.read(buffer)) != -1) {
            isEmpty = false;
            byte[] data = new byte[len];
            System.arraycopy(buffer, 0, data, 0, len);
            writer.write(data, 0, data.length);
            totoalReadLen += len;
            currentPacketLen += len;
            if (currentPacketLen >= sizeToSend) {
                currentPacketLen = 0;
                lastReadLen = totoalReadLen;
                writer.flush();
                writer.startPacket(0);
                writer.write(Packet.COM_STMT_SEND_LONG_DATA);
                writer.writeInt(statementId);
                writer.writeShort(paramIndex);
            }
        }
        if (lastReadLen != totoalReadLen) {
            writer.flush();
        }
        if (isEmpty) {
            writer.flush();
        }
        return true;
    }
}
