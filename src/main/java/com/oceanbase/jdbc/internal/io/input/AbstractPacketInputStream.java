package com.oceanbase.jdbc.internal.io.input;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;

import com.oceanbase.jdbc.internal.com.read.Buffer;
import com.oceanbase.jdbc.internal.io.LruTraceCache;
import com.oceanbase.jdbc.internal.protocol.TimeTrace;

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
    protected TimeTrace        timeTrace;
    protected int              preMysqlSeqNo = -1;
    protected boolean          mysqlSeqNoReset = false;

    @Override
    public abstract Buffer getPacket(boolean reUsable) throws IOException, SQLException;

    @Override
    public abstract byte[] getPacketArray(boolean reUsable) throws IOException, SQLException;

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
        this.serverThreadLog = "connectionId=" + serverThreadId
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

    public void startReceiveResponse() {
        if (timeTrace != null) {
            timeTrace.startReceiveResponse();
        }
    }

    public void endReceiveResponse(String sql) {
        if (timeTrace != null) {
            String protocol = null;
            if (this instanceof Ob20PacketInputStream) {
                protocol = "ob20";
            } else if (this instanceof StandardPacketInputStream) {
                protocol = "mysql";
            } else if (this instanceof DecompressPacketInputStream) {
                protocol = "compress";
            }
            timeTrace.endReceiveResponse(protocol, sql);
        }
    }

    @Override
    public void resetReadMysqlSeqNo() {
        preMysqlSeqNo = 0;
        this.mysqlSeqNoReset = true;
    }

    @Override
    public void setMysqlSeqNoReset(boolean mysqlSeqNoReset) {
        this.mysqlSeqNoReset = mysqlSeqNoReset;
    }

    public void checkMysqlSeqNo(int curMysqlSeqNo) throws SQLException {
        if (!this.mysqlSeqNoReset) {
            if ((curMysqlSeqNo == -128) && (this.preMysqlSeqNo != 127)) {
                throw new SQLException("Packets out of order, expected packet # -128, but received packet # " + curMysqlSeqNo);
            }

            if ((this.preMysqlSeqNo == -1) && (curMysqlSeqNo != 0)) {
                throw new SQLException("Packets out of order, expected packet # -1, but received packet # " + curMysqlSeqNo);
            }

            if ((curMysqlSeqNo != -128) && (this.preMysqlSeqNo != -1) && (curMysqlSeqNo != (this.preMysqlSeqNo + 1))) {
                throw new SQLException("Packets out of order, expected packet # " + (this.preMysqlSeqNo + 1) + ", but received packet # " + curMysqlSeqNo);
            }
        }else {
            this.mysqlSeqNoReset = false;
        }
        this.preMysqlSeqNo = curMysqlSeqNo;
    }
}
