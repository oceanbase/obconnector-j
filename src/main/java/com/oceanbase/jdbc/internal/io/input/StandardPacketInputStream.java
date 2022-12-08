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
package com.oceanbase.jdbc.internal.io.input;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import com.oceanbase.jdbc.internal.ColumnType;
import com.oceanbase.jdbc.internal.com.read.Buffer;
import com.oceanbase.jdbc.internal.io.TraceObject;
import com.oceanbase.jdbc.internal.logging.Logger;
import com.oceanbase.jdbc.internal.logging.LoggerFactory;
import com.oceanbase.jdbc.internal.util.Utils;
import com.oceanbase.jdbc.util.Options;

public class StandardPacketInputStream extends AbstractPacketInputStream {

    private static final Logger logger      = LoggerFactory
                                                .getLogger(StandardPacketInputStream.class);

    protected byte[]            mysqlHeader = new byte[4];
    protected int               lastPacketLength;

    /**
     * Constructor of standard socket MySQL packet stream reader.
     *
     * @param in stream
     * @param options connection options
     * @param threadId thread id
     */
    public StandardPacketInputStream(InputStream in, long threadId, Options options) {
        inputStream = options.useReadAheadInput ? new ReadAheadBufferedStream(in)
            : new BufferedInputStream(in, 16384);
        this.maxQuerySizeToLog = options.maxQuerySizeToLog;
        this.threadId = threadId;
    }

    @Override
    public Buffer getPacket(boolean reUsable) throws IOException {
        return new Buffer(getPacketArray(reUsable), lastPacketLength);
    }

    /**
     * Get next packet. If packet is more than 16M, read as many packet needed to finish packet.
     * (first that has not length = 16Mb)
     *
     * @param reUsable if can use existing reusable buffer to avoid creating array
     * @return array packet.
     * @throws IOException if socket exception occur.
     */
    public byte[] getPacketArray(boolean reUsable) throws IOException {
        // Read 4 byte mysqlHeader
        readMysqlStream(mysqlHeader, 0, 4);
        if (enableNetworkStatistics) {
            timestampAfterRead = System.currentTimeMillis();
        }

        lastPacketLength = (mysqlHeader[0] & 0xff) + ((mysqlHeader[1] & 0xff) << 8)
                           + ((mysqlHeader[2] & 0xff) << 16);
        mysqlSeqNo = mysqlHeader[3] & 0xff;
        if (this instanceof Ob20PacketInputStream
            && ((Ob20PacketInputStream) this).ob20.enableDebug) {
            System.out.println(" ---------[mysql] packetLength = " + lastPacketLength
                               + ", packetNumber = " + mysqlSeqNo);
        }

        // prepare array
        byte[] mysqlBody;
        if (reUsable && lastPacketLength < REUSABLE_BUFFER_LENGTH) {
            mysqlBody = reusableArray;
        } else {
            mysqlBody = new byte[lastPacketLength];
        }

        // Read content
        readMysqlStream(mysqlBody, 0, lastPacketLength);
        doTrace(0, lastPacketLength, mysqlBody);

        // In case content length is big, content will be separate in many 16Mb packets
        if (lastPacketLength == MAX_PACKET_SIZE) {
            int packetLength;
            do {
                readMysqlStream(mysqlHeader, 0, 4);
                packetLength = (mysqlHeader[0] & 0xff) + ((mysqlHeader[1] & 0xff) << 8)
                               + ((mysqlHeader[2] & 0xff) << 16);
                mysqlSeqNo = mysqlHeader[3] & 0xff;

                int currentBufferLength = mysqlBody.length;
                byte[] newRawBytes = new byte[currentBufferLength + packetLength];
                System.arraycopy(mysqlBody, 0, newRawBytes, 0, currentBufferLength);
                mysqlBody = newRawBytes;

                readMysqlStream(mysqlBody, currentBufferLength, packetLength);
                lastPacketLength += packetLength;

                doTrace(currentBufferLength, packetLength, mysqlBody);
            } while (packetLength == MAX_PACKET_SIZE);
        }

        return mysqlBody;
    }

    protected void readMysqlStream(byte[] rawBytes, int off, int remaining) throws IOException {
        do {
            int count = inputStream.read(rawBytes, off, remaining);
            if (count < 0) {
                throw new EOFException("unexpected end of stream, read " + off + " bytes from 4");
            }

            remaining -= count;
            off += count;
        } while (remaining > 0);
    }

    protected void doTrace(int offset, int length, byte[] rawBytes) {
        if (traceCache != null) {
            traceCache.put(new TraceObject(false, TraceObject.NOT_COMPRESSED, threadId, Arrays
                .copyOfRange(mysqlHeader, 0, 4), Arrays.copyOfRange(rawBytes, offset,
                Math.min(length, 1000))));
        }

        if (logger.isTraceEnabled()) {
            logger.trace("read: {}{}", serverThreadLog,
                Utils.hexdump(maxQuerySizeToLog - 4, offset, length, mysqlHeader, rawBytes));
        }
    }

    @Override
    public int getCompressLastPacketSeq() {
        return 0;
    }

    /**
     * Constructor for single Data (using text format).
     *
     * @param value value
     * @return Buffer
     */
    public static byte[] create(byte[] value) {
        if (value == null) {
            return new byte[] { (byte) 251 };
        }

        int length = value.length;
        if (length < 251) {

            byte[] buf = new byte[length + 1];
            buf[0] = (byte) length;
            System.arraycopy(value, 0, buf, 1, length);
            return buf;

        } else if (length < 65536) {

            byte[] buf = new byte[length + 3];
            buf[0] = (byte) 0xfc;
            buf[1] = (byte) length;
            buf[2] = (byte) (length >>> 8);
            System.arraycopy(value, 0, buf, 3, length);
            return buf;

        } else if (length < 16777216) {

            byte[] buf = new byte[length + 4];
            buf[0] = (byte) 0xfd;
            buf[1] = (byte) length;
            buf[2] = (byte) (length >>> 8);
            buf[3] = (byte) (length >>> 16);
            System.arraycopy(value, 0, buf, 4, length);
            return buf;

        } else {

            byte[] buf = new byte[length + 9];
            buf[0] = (byte) 0xfe;
            buf[1] = (byte) length;
            buf[2] = (byte) (length >>> 8);
            buf[3] = (byte) (length >>> 16);
            buf[4] = (byte) (length >>> 24);
            // byte[] cannot have a more than 4 byte length size, so buf[5] -> buf[8] = 0x00;
            System.arraycopy(value, 0, buf, 9, length);
            return buf;
        }
    }

    /**
     * Create Buffer with Text protocol values.
     *
     * @param row row data
     * @param columnTypes column types
     * @return Buffer
     */
    public static byte[] create(byte[][] row, ColumnType[] columnTypes) {

        int totalLength = 0;
        for (byte[] data : row) {
            if (data == null) {
                totalLength++;
            } else {
                int length = data.length;
                if (length < 251) {
                    totalLength += length + 1;
                } else if (length < 65536) {
                    totalLength += length + 3;
                } else if (length < 16777216) {
                    totalLength += length + 4;
                } else {
                    totalLength += length + 9;
                }
            }
        }
        byte[] buf = new byte[totalLength];

        int pos = 0;
        for (byte[] data : row) {
            if (data == null) {
                buf[pos++] = (byte) 251;
            } else {
                int length = data.length;
                if (length < 251) {
                    buf[pos++] = (byte) length;
                } else if (length < 65536) {
                    buf[pos++] = (byte) 0xfc;
                    buf[pos++] = (byte) length;
                    buf[pos++] = (byte) (length >>> 8);
                } else if (length < 16777216) {
                    buf[pos++] = (byte) 0xfd;
                    buf[pos++] = (byte) length;
                    buf[pos++] = (byte) (length >>> 8);
                    buf[pos++] = (byte) (length >>> 16);
                } else {
                    buf[pos++] = (byte) 0xfe;
                    buf[pos++] = (byte) length;
                    buf[pos++] = (byte) (length >>> 8);
                    buf[pos++] = (byte) (length >>> 16);
                    buf[pos++] = (byte) (length >>> 24);
                    // byte[] cannot have more than 4 byte length size, so buf[pos+5] -> buf[pos+8] = 0x00;
                    pos += 4;
                }
                System.arraycopy(data, 0, buf, pos, length);
                pos += length;
            }
        }
        return buf;
    }

    /**
     * Get current input stream for creating compress input stream, to avoid losing already read bytes
     * in case of pipelining.
     *
     * @return input stream.
     */
    public InputStream getInputStream() {
        return inputStream;
    }
}
