package com.alipay.oceanbase.jdbc.internal.io.input;

import java.io.IOException;

import com.alipay.oceanbase.jdbc.internal.com.read.Buffer;
import com.alipay.oceanbase.jdbc.internal.io.LruTraceCache;

public interface PacketInputStream {

    Buffer getPacket(boolean reUsable) throws IOException;

    byte[] getPacketArray(boolean reUsable) throws IOException;

    int getLastPacketSeq();

    int getCompressLastPacketSeq();

    void close() throws IOException;

    void setServerThreadId(long serverThreadId, Boolean isMaster);

    void setTraceCache(LruTraceCache traceCache);
}
