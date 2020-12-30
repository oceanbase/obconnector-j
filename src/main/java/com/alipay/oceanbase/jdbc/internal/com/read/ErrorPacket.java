package com.alipay.oceanbase.jdbc.internal.com.read;

import java.nio.charset.StandardCharsets;

/** See protocol documentation https://mariadb.com/kb/en/err_packet/ */
public class ErrorPacket {

    private final short  errorCode;
    private final String sqlState;
    private final String message;

    /**
     * Reading error stream.
     *
     * @param buffer current stream rawBytes
     */
    public ErrorPacket(Buffer buffer) {
        buffer.skipByte(); // 0xff Error header
        errorCode = buffer.readShort();
        if (buffer.getByte() == '#') {
            buffer.skipByte(); // skip '#'
            sqlState = buffer.readString(5);
            message = buffer.readStringNullEnd(StandardCharsets.UTF_8);
        } else {
            // Pre-4.1 message, still can be output in newer versions (e.g with 'Too many connections')
            message = new String(buffer.buf, buffer.position, buffer.limit - buffer.position,
                StandardCharsets.UTF_8);
            sqlState = "HY000";
        }
    }

    public String getMessage() {
        return message;
    }

    public short getErrorCode() {
        return errorCode;
    }

    public String getSqlState() {
        return sqlState;
    }
}
