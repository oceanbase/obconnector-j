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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import com.oceanbase.jdbc.internal.com.read.Buffer;
import com.oceanbase.jdbc.internal.io.TraceObject;
import com.oceanbase.jdbc.internal.logging.Logger;
import com.oceanbase.jdbc.internal.logging.LoggerFactory;
import com.oceanbase.jdbc.internal.protocol.flt.OceanBaseProtocolV20;
import com.oceanbase.jdbc.internal.util.Utils;
import com.oceanbase.jdbc.util.OceanBaseCRC16;
import com.oceanbase.jdbc.util.OceanBaseCRC32C;
import com.oceanbase.jdbc.util.Options;

public class Ob20PacketInputStream extends StandardPacketInputStream {

    private static final Logger logger      = LoggerFactory.getLogger(Ob20PacketInputStream.class);

    OceanBaseProtocolV20        ob20;
    private OceanBaseCRC32C     crc32       = new OceanBaseCRC32C();
    private boolean             isTailRead  = true;
    private byte[]              headerBytes = new byte[OceanBaseProtocolV20.TOTAL_HEADER_LENGTH];
    private byte[]              tailBytes   = new byte[OceanBaseProtocolV20.OB20_TAIL_LENGTH];

    private long                basicRemainder;

    public Ob20PacketInputStream(InputStream in, long threadId, Options options,
                                 OceanBaseProtocolV20 ob20) {
        super(in, threadId, options);
        this.ob20 = ob20;
    }

    @Override
    public Buffer getPacket(boolean reUsable) throws IOException {
        return new Buffer(getPacketArray(reUsable), lastPacketLength);
    }

    @Override
    public byte[] getPacketArray(boolean reUsable) throws IOException {
        // read ob20 header
        if (isTailRead) {
            resolveOb20Packet();
        }

        return super.getPacketArray(reUsable);
    }

    @Override
    protected void readMysqlStream(byte[] rawBytes, int off, int remaining) throws IOException {
        do {
            // read ob20 header, if there are remaining bytes to read
            if (basicRemainder == 0 && remaining > 0) {
                resolveOb20Packet();
            }

            int lengthToRead = (int) Math.min(remaining, basicRemainder);
            if (lengthToRead > 0) {
                readFully(rawBytes, off, lengthToRead);
                crc32.update(rawBytes, off, lengthToRead);

                remaining -= lengthToRead;
                off += lengthToRead;
                basicRemainder -= lengthToRead;
                if (ob20.enableDebug) {
                    System.out.println(" ------basicRemainder = " + basicRemainder);
                }

                // read ob20 tail, if all payloads have been read from socket
                if (basicRemainder == 0) {
                    checkTailChecksum();
                }
            }
        } while (remaining > 0);
    }

    @Override
    protected void doTrace(int offset, int length, byte[] rawBytes) {
        if (traceCache != null) {
            traceCache.put(new TraceObject(false, TraceObject.OB_PROTOCOL_2_0, threadId, Arrays
                .copyOfRange(mysqlHeader, 0, 4), Arrays.copyOfRange(rawBytes, offset,
                Math.min(length, 1000))));
        }

        if (logger.isTraceEnabled()) {
            logger.trace("read: {}{}", serverThreadLog,
                Utils.hexdump(maxQuerySizeToLog - 4, offset, length, mysqlHeader, rawBytes));
        }
    }

    private void resolveOb20Packet() throws IOException {
        ob20.reset();

        // read header
        readFully(headerBytes, 0, OceanBaseProtocolV20.TOTAL_HEADER_LENGTH);
        checkHeader();
        basicRemainder = ob20.header.payloadLength;

        // read extra info
        if (ob20.isExtraInfoExist()) {
            byte[] extraLength = new byte[OceanBaseProtocolV20.OB20_EXTRA_LENGTH];
            readFully(extraLength, 0, OceanBaseProtocolV20.OB20_EXTRA_LENGTH);
            crc32.update(extraLength, 0, OceanBaseProtocolV20.OB20_EXTRA_LENGTH);
            ob20.extraInfo.extraLength = new Buffer(extraLength).readLong4BytesV1();

            ob20.extraInfo.extraBytes = new byte[(int) ob20.extraInfo.extraLength];
            readFully(ob20.extraInfo.extraBytes, 0, ob20.extraInfo.extraLength);
            crc32.update(ob20.extraInfo.extraBytes, 0, (int) ob20.extraInfo.extraLength);
            ob20.analyseExtraInfoBytes();

            basicRemainder -= (OceanBaseProtocolV20.OB20_EXTRA_LENGTH + ob20.extraInfo.extraLength);
            ob20.extraInfo.reset();
        }

        if (ob20.enableDebug) {
            System.out.println(" ------basicRemainder = " + basicRemainder);
        }

        isTailRead = false;
    }

    private void readFully(byte[] b, long off, long len) throws IOException {
        int n = 0;
        while (n < len) {
            int count = inputStream.read(b, (int) (off + n), (int) (len - n));
            if (count < 0) {
                throw new EOFException("unexpected end of stream, read " + off + " bytes from "
                                       + len + " (socket was closed by server)");
            }

            n += count;
        }
    }

    private void checkHeader() throws IOException {
        Buffer headerBuffer = new Buffer(headerBytes);
        ob20.header.compressLength = headerBuffer.readInt3Bytes();
        ob20.header.compressSeqNo = (short) (headerBuffer.readByte() & 0xff);
        ob20.header.uncompressLength = headerBuffer.readInt3Bytes();
        ob20.header.magicNum = headerBuffer.readShort();
        ob20.header.version = headerBuffer.readShort();
        ob20.header.connectionId = headerBuffer.readLong4BytesV1();
        ob20.header.requestId = headerBuffer.readInt3Bytes();
        ob20.header.obSeqNo = (short) (headerBuffer.readByte() & 0xff);
        ob20.header.payloadLength = headerBuffer.readLong4BytesV1();
        ob20.header.flag = headerBuffer.readInt();
        ob20.header.reserved = headerBuffer.readShort();
        ob20.header.headerChecksum = headerBuffer.readInt2BytesV1();
        if (ob20.enableDebug) {
            printHeader();
        }

        try {
            if (0 != ob20.header.headerChecksum) {
                int localHeaderChecksum = OceanBaseCRC16.calculate(headerBytes,
                    OceanBaseProtocolV20.TOTAL_HEADER_LENGTH - 2);

                if (localHeaderChecksum != ob20.header.headerChecksum) {
                    throw new IOException(
                        String
                            .format(
                                "header checksum mismatch, expected HeaderChecksum=%d, but received headerChecksum=%d",
                                localHeaderChecksum, ob20.header.headerChecksum));
                }
            }

            if (ob20.header.compressLength != (OceanBaseProtocolV20.OB20_HEADER_LENGTH
                                               + ob20.header.payloadLength + OceanBaseProtocolV20.OB20_TAIL_LENGTH)) {
                throw new IOException(String.format(
                    "packet length mismatch, received compressLength=%d, payloadLength=%d",
                    ob20.header.compressLength, ob20.header.payloadLength));
            }

            if (ob20.header.uncompressLength != 0) {
                throw new IOException(
                    String
                        .format(
                            "invalid uncompress length, expected uncompressedLen=0, but received uncompressLength=%d",
                            ob20.header.uncompressLength));
            }

            if (ob20.header.magicNum != OceanBaseProtocolV20.OB20_MAGIC_NUM) {
                throw new IOException(String.format(
                    "invalid magic num, expected magicNum=%d, but received magicNum=%d",
                    OceanBaseProtocolV20.OB20_MAGIC_NUM, ob20.header.magicNum));
            }

            if (ob20.header.version != OceanBaseProtocolV20.OB20_VERSION) {
                throw new IOException(String.format(
                    "invalid version, expected version=%d, but received version=%d",
                    OceanBaseProtocolV20.OB20_VERSION, ob20.header.version));
            }

            if (ob20.header.connectionId != threadId) {
                throw new IOException(String.format(
                    "connection Id mismatch, currConnectionId=%d, connId=%d", threadId,
                    ob20.header.connectionId));
            }

            if (ob20.header.requestId != (ob20.curRequestId == 0 ? 0x00ffffff
                : ob20.curRequestId - 1)) {
                throw new IOException(String.format(
                    "request Id mismatch, currRequestId=%d, but received requestId=%d",
                    ob20.curRequestId, ob20.header.requestId));
            }

            if (ob20.header.obSeqNo != ob20.getObSeqNo()) {
                throw new IOException(String.format(
                    "packet sequence mismatch, expected obSeqNo=%d, but received obSeqNo=%d",
                    ob20.curObSeqNo, ob20.header.obSeqNo));
            }
        } catch (IOException e) {
            if (!ob20.enableDebug) {
                printHeader();
            }
            throw e;
        }
    }

    private void checkTailChecksum() throws IOException {
        readFully(tailBytes, 0, OceanBaseProtocolV20.OB20_TAIL_LENGTH);
        isTailRead = true;

        ob20.tailChecksum = (tailBytes[0] & 0xff) + ((tailBytes[1] & 0xff) << 8)
                            + ((tailBytes[2] & 0xff) << 16) + ((long) (tailBytes[3] & 0xff) << 24);
        if (ob20.enableDebug) {
            System.out.println(" ---[Response] tailChecksum = " + ob20.tailChecksum);
        }

        if (0 != ob20.tailChecksum) {
            long localTailChecksum = crc32.getValue();
            if (localTailChecksum != ob20.tailChecksum) {
                if (!ob20.enableDebug) {
                    printHeaderAndTail();
                }
                throw new IOException(
                    String
                        .format(
                            "tail checksum mismatch, expected tailChecksum=%d, but received tailChecksum=%d",
                            localTailChecksum, ob20.tailChecksum));
            }
        }

        ob20.header.reset();
        ob20.tailChecksum = 0;
        crc32.reset();
    }

    private void printHeader() {
        System.out.println(" ---[Response] connectionId = " + ob20.header.connectionId
                           + ", requestId = " + ob20.header.requestId + ", obSeqNo = "
                           + ob20.header.obSeqNo + ", payloadLength = " + ob20.header.payloadLength
                           + ", headerChecksum = " + ob20.header.headerChecksum);
    }

    private void printHeaderAndTail() {
        System.out.println(" ---[Response] connectionId = " + ob20.header.connectionId
                           + ", requestId = " + ob20.header.requestId + ", obSeqNo = "
                           + ob20.header.obSeqNo + ", payloadLength = " + ob20.header.payloadLength
                           + ", headerChecksum = " + ob20.header.headerChecksum
                           + ", tailChecksum = " + ob20.tailChecksum);
    }

}
