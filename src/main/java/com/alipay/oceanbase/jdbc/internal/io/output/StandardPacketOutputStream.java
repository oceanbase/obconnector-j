package com.alipay.oceanbase.jdbc.internal.io.output;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import com.alipay.oceanbase.jdbc.internal.io.TraceObject;
import com.alipay.oceanbase.jdbc.internal.logging.Logger;
import com.alipay.oceanbase.jdbc.internal.logging.LoggerFactory;
import com.alipay.oceanbase.jdbc.internal.util.Utils;
import com.alipay.oceanbase.jdbc.util.Options;

public class StandardPacketOutputStream extends AbstractPacketOutputStream {

    private static final Logger logger            = LoggerFactory
                                                      .getLogger(StandardPacketOutputStream.class);

    private static final int    MAX_PACKET_LENGTH = 0x00ffffff + 4;
    private int                 maxPacketLength   = MAX_PACKET_LENGTH;

    public StandardPacketOutputStream(OutputStream out, Options options, long threadId) {
        super(out, options.maxQuerySizeToLog, threadId);
    }

    public int getMaxPacketLength() {
        return maxPacketLength;
    }

    @Override
    public void startPacket(int seqNo) {
        this.seqNo = seqNo;
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
        if (pos > 4) {
            buf[0] = (byte) (pos - 4);
            buf[1] = (byte) ((pos - 4) >>> 8);
            buf[2] = (byte) ((pos - 4) >>> 16);
            buf[3] = (byte) this.seqNo++;
            checkMaxAllowedLength(pos - 4);
            out.write(buf, 0, pos);
            cmdLength += pos - 4;

            if (traceCache != null && permitTrace) {
                // trace last packets
                traceCache.put(new TraceObject(true, TraceObject.NOT_COMPRESSED, threadId, Arrays
                    .copyOfRange(buf, 0, pos > 1000 ? 1000 : pos)));
            }

            if (logger.isTraceEnabled()) {
                if (permitTrace) {
                    logger.trace("send: {}{}", serverThreadLog,
                        Utils.hexdump(maxQuerySizeToLog, 0, pos, buf));
                } else {
                    logger.trace("send: content length={} {} com=<hidden>", pos - 4,
                        serverThreadLog);
                }
            }

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
        buf[0] = (byte) 0x00;
        buf[1] = (byte) 0x00;
        buf[2] = (byte) 0x00;
        buf[3] = (byte) this.seqNo++;
        out.write(buf, 0, 4);

        if (traceCache != null) {
            // trace last packets
            traceCache.put(new TraceObject(true, TraceObject.NOT_COMPRESSED, threadId, Arrays
                .copyOfRange(buf, 0, 4)));
        }

        if (logger.isTraceEnabled()) {
            logger.trace("send com : content length=0 {}{}", serverThreadLog,
                Utils.hexdump(maxQuerySizeToLog, 0, 4, buf));
        }
    }
}
