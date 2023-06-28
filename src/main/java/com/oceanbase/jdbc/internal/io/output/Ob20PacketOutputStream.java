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
package com.oceanbase.jdbc.internal.io.output;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import com.oceanbase.jdbc.internal.com.read.Buffer;
import com.oceanbase.jdbc.internal.io.TraceObject;
import com.oceanbase.jdbc.internal.logging.Logger;
import com.oceanbase.jdbc.internal.logging.LoggerFactory;
import com.oceanbase.jdbc.internal.protocol.TimeTrace;
import com.oceanbase.jdbc.internal.protocol.flt.OceanBaseProtocolV20;
import com.oceanbase.jdbc.internal.util.Utils;
import com.oceanbase.jdbc.internal.util.exceptions.MaxAllowedPacketException;
import com.oceanbase.jdbc.util.OceanBaseCRC16;
import com.oceanbase.jdbc.util.OceanBaseCRC32C;
import com.oceanbase.jdbc.util.Options;

public class Ob20PacketOutputStream extends AbstractPacketOutputStream {

    private static final Logger  logger             = LoggerFactory
                                                        .getLogger(Ob20PacketOutputStream.class);

    private static final int     MAX_PACKET_LENGTH  = 0x00ffffff;
    private static final int     MAX_PAYLOAD_LENGTH = 0x00ffffff
                                                      - OceanBaseProtocolV20.OB20_HEADER_LENGTH
                                                      - OceanBaseProtocolV20.OB20_TAIL_LENGTH;
    private static final int     PROTO20_SPLIT_LEN  = MAX_PAYLOAD_LENGTH;

    private OceanBaseProtocolV20 ob20;
    private OceanBaseCRC32C      crc32              = new OceanBaseCRC32C();
    private boolean              enableOb20Checksum;
    byte[]                       outBytes;
    private int                  totalPacketLength;

    public Ob20PacketOutputStream(OutputStream out, long threadId, Options options,
                                  OceanBaseProtocolV20 ob20, TimeTrace timeTrace) {
        super(out, options.maxQuerySizeToLog, threadId, options.characterEncoding, timeTrace);
        maxPacketLength = MAX_PACKET_LENGTH;
        this.ob20 = ob20;
        this.enableOb20Checksum = options.enableOb20Checksum;
    }

    @Override
    public int initialPacketPos() {
        return 0;
    }

    @Override
    public void startPacket(int compressSeqNo) {
        ob20.initObSeqNo(compressSeqNo);
        mysqlSeqNo = 0;
        pos = 0;
        cmdLength = 0;
        startSendRequest();
    }

    @Override
    protected void flushBuffer(boolean commandEnd) throws IOException {
        if (enableNetworkStatistics) {
            timestampBeforeFlush = System.currentTimeMillis();
        }

        if (pos > 0) {
            // calculate length of extra info, length of basic info, total length
            int extraPayloadLength = 0;
            int basicPayloadLength = OceanBaseProtocolV20.MYSQL_PACKET_HEADER + pos;
            int totalPayloadLength = basicPayloadLength;
            byte[] totalPayload;

            // prepare whole payload
            if (ob20.isExtraInfoExist()) {
                ob20.setExtraInfoLength();
                extraPayloadLength = (int) (OceanBaseProtocolV20.OB20_EXTRA_LENGTH + ob20.extraInfo.extraLength);
                totalPayloadLength += extraPayloadLength;

                // todo: proxy can't resolve incomplete mysql packet
                if (extraPayloadLength > PROTO20_SPLIT_LEN) {
                    throw new IOException("Extra info is larger than PROTO20_SPLIT_LEN");
                }

                totalPayload = new byte[totalPayloadLength];
                Buffer tmpBuf = new Buffer(totalPayload);
                fillOb20ExtraInfo(tmpBuf);
                fillOb20BasicInfo(tmpBuf);
            } else {
                totalPayload = new byte[totalPayloadLength];
                Buffer tmpBuf = new Buffer(totalPayload);
                fillOb20BasicInfo(tmpBuf);
            }

            // fields won't change in several ob20 packets which belongs to one request
            ob20.header.connectionId = threadId;
            ob20.header.requestId = ob20.curRequestId;

            int totalPayloadPos = 0;
            while (totalPayloadPos < totalPayloadLength) {
                int uncompressSize;
                // extraPayloadLength mustn't exceed PROTO20_SPLIT_LEN at present
                if (ob20.isExtraInfoExist() && totalPayloadPos < extraPayloadLength) {
                    if (extraPayloadLength + basicPayloadLength <= PROTO20_SPLIT_LEN) {
                        // don't split
                        uncompressSize = extraPayloadLength + basicPayloadLength;
                    } else {
                        // split extra info and one mysql packet
                        uncompressSize = extraPayloadLength;
                    }
                } else {
                    // split a large mysql packet
                    uncompressSize = Math.min(PROTO20_SPLIT_LEN, totalPayloadLength
                                                                 - totalPayloadPos);
                }

                // set fields in header
                ob20.header.obSeqNo = (byte) ob20.getObSeqNo();
                ob20.header.compressSeqNo = ob20.header.obSeqNo;
                ob20.header.payloadLength = uncompressSize;
                ob20.header.compressLength = OceanBaseProtocolV20.OB20_HEADER_LENGTH
                                             + uncompressSize
                                             + OceanBaseProtocolV20.OB20_TAIL_LENGTH;
                if (ob20.isExtraInfoExist() && totalPayloadPos >= extraPayloadLength) {
                    ob20.header.flag &= (~OceanBaseProtocolV20.OB_EXTRA_INFO_EXIST);
                }
                if (commandEnd && totalPayloadPos + uncompressSize == totalPayloadLength) {
                    ob20.header.flag |= OceanBaseProtocolV20.OB_IS_LAST_PACKET;
                }

                // write into output stream
                totalPacketLength = OceanBaseProtocolV20.COMPRESS_HEADER_LENGTH
                                    + ob20.header.compressLength;
                outBytes = new byte[totalPacketLength];
                Buffer outBuffer = new Buffer(outBytes);

                writeOb20Header(outBuffer);

                outBuffer.writeBytes(totalPayload, totalPayloadPos, uncompressSize);
                totalPayloadPos += uncompressSize;

                writeOb20TailChecksum(outBuffer, totalPayload, totalPayloadPos - uncompressSize,
                    uncompressSize);

                logger.debug("prepare to send: {}", headerToString());
                out.write(outBytes, 0, totalPacketLength);
                doTrace();
            }

            if (commandEnd) {
                if (ob20.header.payloadLength == MAX_PAYLOAD_LENGTH) {
                    writeEmptyPacket();
                } else {
                    ob20.updateRequestId();
                }
            }
            pos = 0;

            // reset
            ob20.header.reset();
            ob20.extraInfo.reset();
            ob20.tailChecksum = 0;
        }
    }

    @Override
    public void writeEmptyPacket() throws IOException {
        if (enableNetworkStatistics) {
            timestampBeforeFlush = System.currentTimeMillis();
        }

        ob20.header.connectionId = threadId;
        ob20.header.requestId = ob20.curRequestId;
        ob20.updateRequestId();
        ob20.header.obSeqNo = (byte) ob20.getObSeqNo();
        ob20.header.compressSeqNo = ob20.header.obSeqNo;
        ob20.header.payloadLength = OceanBaseProtocolV20.MYSQL_PACKET_HEADER;
        ob20.header.compressLength = OceanBaseProtocolV20.OB20_HEADER_LENGTH
                                     + OceanBaseProtocolV20.MYSQL_PACKET_HEADER
                                     + OceanBaseProtocolV20.OB20_TAIL_LENGTH;
        ob20.header.flag &= (~OceanBaseProtocolV20.OB_EXTRA_INFO_EXIST);
        ob20.header.flag |= OceanBaseProtocolV20.OB_IS_LAST_PACKET;

        totalPacketLength = OceanBaseProtocolV20.COMPRESS_HEADER_LENGTH
                            + ob20.header.compressLength;
        outBytes = new byte[totalPacketLength];
        Buffer outBuffer = new Buffer(outBytes);

        writeOb20Header(outBuffer);

        // fill empty mysql packet
        byte[] totalPayload = new byte[OceanBaseProtocolV20.MYSQL_PACKET_HEADER];
        totalPayload[0] = (byte) 0x00; // mysql packet length
        totalPayload[1] = (byte) 0x00;
        totalPayload[2] = (byte) 0x00;
        totalPayload[3] = (byte) mysqlSeqNo++; // mysql packet seq
        outBuffer.writeBytes(totalPayload, 0, OceanBaseProtocolV20.MYSQL_PACKET_HEADER);

        writeOb20TailChecksum(outBuffer, totalPayload, 0, OceanBaseProtocolV20.MYSQL_PACKET_HEADER);

        logger.debug("prepare to send: {}", headerToString());
        out.write(outBytes, 0, totalPacketLength);
        doTrace();
    }

    @Override
    public void setMaxAllowedPacket(int maxAllowedPacket) {
        this.maxAllowedPacket = maxAllowedPacket;
        maxPacketLength = Math.min(MAX_PACKET_LENGTH, maxAllowedPacket
                                                      + OceanBaseProtocolV20.MYSQL_PACKET_HEADER);
    }

    private String headerToString() {
        return "connectionId = " + ob20.header.connectionId + ", requestId = "
               + ob20.header.requestId + ", obSeqNo = " + ob20.header.obSeqNo
               + ", payloadLength = " + ob20.header.payloadLength + ", headerChecksum = "
               + ob20.header.headerChecksum;
    }

    private void writeOb20Header(Buffer outBuffer) {
        outBuffer.writeLongInt(ob20.header.compressLength);
        outBuffer.writeByte((byte) ob20.header.compressSeqNo);
        outBuffer.writeLongInt(ob20.header.uncompressLength);
        outBuffer.writeShort(ob20.header.magicNum);
        outBuffer.writeShort(ob20.header.version);
        outBuffer.writeInt((int) ob20.header.connectionId);
        outBuffer.writeLongInt(ob20.header.requestId);
        outBuffer.writeByte((byte) ob20.header.obSeqNo);
        outBuffer.writeInt((int) ob20.header.payloadLength);
        outBuffer.writeInt(ob20.header.flag);
        outBuffer.writeShort(ob20.header.reserved);

        if (enableOb20Checksum) {
            ob20.header.headerChecksum = OceanBaseCRC16.calculate(outBuffer.getByteBuffer(),
                OceanBaseProtocolV20.TOTAL_HEADER_LENGTH - 2);
        } else {
            ob20.header.headerChecksum = 0;
        }
        outBuffer.writeShort((short) ob20.header.headerChecksum);
    }

    private void fillOb20ExtraInfo(Buffer outBuffer) throws IOException {
        outBuffer.writeInt((int) ob20.extraInfo.extraLength);
        outBuffer.writeBytes(ob20.getExtraInfoBytes(), 0, (int) ob20.extraInfo.extraLength);
    }

    private void fillOb20BasicInfo(Buffer outBuffer) throws MaxAllowedPacketException {
        outBuffer.writeLongInt(pos);
        outBuffer.writeByte((byte) mysqlSeqNo++);
        checkMaxAllowedLength(pos);
        outBuffer.writeBytes(buf, 0, pos);
        cmdLength += pos;
    }

    private void writeOb20TailChecksum(Buffer outBuffer, byte[] payload, int pos, int len) {
        if (enableOb20Checksum) {
            crc32.reset();
            crc32.update(payload, pos, len);
            ob20.tailChecksum = crc32.getValue();
        } else {
            ob20.tailChecksum = 0;
        }

        outBuffer.writeInt((int) ob20.tailChecksum);
    }

    private void doTrace() {
        if (traceCache != null && permitTrace) {
            // trace last packets
            traceCache.put(new TraceObject(true, TraceObject.OB_PROTOCOL_2_0, threadId, Arrays
                .copyOfRange(outBytes, 0, Math.min(totalPacketLength, 1000))));
        }

        if (logger.isTraceEnabled()) {
            logger.trace("send: {}{}", serverThreadLog,
                Utils.hexdump(maxQuerySizeToLog, 0, totalPacketLength, outBytes));
        }
    }

}
