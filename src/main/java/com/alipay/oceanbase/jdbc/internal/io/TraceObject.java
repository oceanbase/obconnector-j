package com.alipay.oceanbase.jdbc.internal.io;

public class TraceObject {

    public static final int NOT_COMPRESSED                            = 0;
    public static final int COMPRESSED_PROTOCOL_COMPRESSED_PACKET     = 1;
    public static final int COMPRESSED_PROTOCOL_NOT_COMPRESSED_PACKET = 2;

    private final boolean   send;
    private final int       indicatorFlag;
    private final long      threadId;
    private byte[][]        buf;

    /**
     * Permit to store OceanBase packets.
     *
     * @param send was packet send or received
     * @param indicatorFlag indicator. can be NOT_COMPRESSED, COMPRESSED_PROTOCOL_COMPRESSED_PACKET or
     *     COMPRESSED_PROTOCOL_NOT_COMPRESSED_PACKET
     * @param threadId current connection thread Id
     * @param buf buffers
     */
    public TraceObject(boolean send, int indicatorFlag, long threadId, byte[]... buf) {
        this.send = send;
        this.indicatorFlag = indicatorFlag;
        this.buf = buf;
        this.threadId = threadId;
    }

    /** Clear trace array for easy garbage. */
    public void remove() {
        for (int i = 0; i < buf.length; i++) {
            buf[i] = null; // force null for easier garbage
        }
        buf = null;
    }

    public boolean isSend() {
        return send;
    }

    public int getIndicatorFlag() {
        return indicatorFlag;
    }

    public byte[][] getBuf() {
        return buf;
    }

    public long getThreadId() {
        return threadId;
    }
}
