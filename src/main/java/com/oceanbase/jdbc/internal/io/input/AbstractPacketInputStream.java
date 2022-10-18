package com.oceanbase.jdbc.internal.io.input;

import java.io.IOException;
import java.io.InputStream;

import com.oceanbase.jdbc.internal.com.read.Buffer;
import com.oceanbase.jdbc.internal.io.LruTraceCache;

public abstract class AbstractPacketInputStream implements PacketInputStream {

    protected InputStream      inputStream;
    protected long             threadId;
    protected int              mysqlSeqNo;
    protected LruTraceCache    traceCache              = null;
    protected String           serverThreadLog         = "";
    protected int              maxQuerySizeToLog;
    protected boolean          enableNetworkStatistics = false;
    protected long             timestampAfterRead      = 0;

    protected static final int REUSABLE_BUFFER_LENGTH  = 1024;
    protected static final int MAX_PACKET_SIZE         = 0xffffff;

    protected final byte[]     reusableArray           = new byte[REUSABLE_BUFFER_LENGTH];

    @Override
    public abstract Buffer getPacket(boolean reUsable) throws IOException;

    @Override
    public abstract byte[] getPacketArray(boolean reUsable) throws IOException;

    protected abstract void readMysqlStream(byte[] rawBytes, int off, int remaining)
                                                                                    throws IOException;

    protected abstract void doTrace(int offset, int length, byte[] rawBytes);

    @Override
    public int getLastPacketSeq() {
        return mysqlSeqNo;
    }

    @Override
    public abstract int getCompressLastPacketSeq();

    @Override
    public void close() throws IOException {
        inputStream.close();
    }

    /**
     * Set server thread id.
     *
     * @param serverThreadId current server thread id.
     * @param isMaster is server master
     */
    public void setServerThreadId(long serverThreadId, Boolean isMaster) {
        this.serverThreadLog = "conn=" + serverThreadId
                               + ((isMaster != null) ? "(" + (isMaster ? "M" : "S") + ")" : "");
    }

    public void setTraceCache(LruTraceCache traceCache) {
        this.traceCache = traceCache;
    }

    @Override
    public void enableNetworkStatistics(boolean flag) {
        timestampAfterRead = 0; // clear
        enableNetworkStatistics = flag;
    }

    @Override
    public long getTimestampAfterRead() {
        return timestampAfterRead;
    }

    @Override
    public void clearNetworkStatistics() {
        timestampAfterRead = 0;
    }

}
