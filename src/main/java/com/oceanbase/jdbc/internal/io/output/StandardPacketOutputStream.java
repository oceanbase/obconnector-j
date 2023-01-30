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

import com.oceanbase.jdbc.internal.io.TraceObject;
import com.oceanbase.jdbc.internal.logging.Logger;
import com.oceanbase.jdbc.internal.logging.LoggerFactory;
import com.oceanbase.jdbc.internal.util.Utils;
import com.oceanbase.jdbc.util.Options;

public class StandardPacketOutputStream extends AbstractPacketOutputStream {

    private static final Logger logger            = LoggerFactory
                                                      .getLogger(StandardPacketOutputStream.class);

    private static final int    MAX_PACKET_LENGTH = 0x00ffffff + 4;

    public StandardPacketOutputStream(OutputStream out, long threadId, Options options) {
        super(out, options.maxQuerySizeToLog, threadId, options.characterEncoding);
        maxPacketLength = MAX_PACKET_LENGTH;
    }

    @Override
    public void startPacket(int seqNo) {
        mysqlSeqNo = seqNo;
        pos = 4;
        cmdLength = 0;
    }

    @Override
    public void setMaxAllowedPacket(int maxAllowedPacket) {
        this.maxAllowedPacket = maxAllowedPacket;
        maxPacketLength = Math.min(MAX_PACKET_LENGTH, maxAllowedPacket + 4);
    }

    public int initialPacketPos() {
        return 4;
    }

    /**
     * Flush the internal buffer.
     *
     * @param commandEnd command end
     * @throws IOException id connection error occur.
     */
    protected void flushBuffer(boolean commandEnd) throws IOException {
        if (enableNetworkStatistics) {
            timestampBeforeFlush = System.currentTimeMillis();
        }
        if (pos > 4) {
            buf[0] = (byte) (pos - 4);
            buf[1] = (byte) ((pos - 4) >>> 8);
            buf[2] = (byte) ((pos - 4) >>> 16);
            buf[3] = (byte) mysqlSeqNo++;
            checkMaxAllowedLength(pos - 4);
            out.write(buf, 0, pos);
            cmdLength += pos - 4;

            doTrace(pos);

            // if last com fill the max size, must send an empty com to indicate command end.
            if (commandEnd && pos == MAX_PACKET_LENGTH) {
                writeEmptyPacket();
            }

            pos = 4;
        }
    }

    /**
     * Write an empty com.
     *
     * @throws IOException if socket error occur.
     */
    public void writeEmptyPacket() throws IOException {
        if (enableNetworkStatistics) {
            timestampBeforeFlush = System.currentTimeMillis();
        }
        buf[0] = (byte) 0x00;
        buf[1] = (byte) 0x00;
        buf[2] = (byte) 0x00;
        buf[3] = (byte) mysqlSeqNo++;
        out.write(buf, 0, 4);

        doTrace(4);
    }

    private void doTrace(int length) {
        if (traceCache != null && permitTrace) {
            // trace last packets
            traceCache.put(new TraceObject(true, TraceObject.NOT_COMPRESSED, threadId, Arrays
                .copyOfRange(buf, 0, Math.min(length, 1000))));
        }

        if (logger.isTraceEnabled()) {
            logger.trace("send: {}{}", serverThreadLog,
                Utils.hexdump(maxQuerySizeToLog, 0, length, buf));
        }
    }

}
