package com.alipay.oceanbase.jdbc.internal.com.send.parameters;

import java.io.IOException;
import com.alipay.oceanbase.jdbc.internal.ColumnType;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;

public class DefaultParameter implements Cloneable, ParameterHolder {

    private static final byte[] defaultBytes = "DEFAULT".getBytes();

    /**
     * Send escaped String to outputStream.
     *
     * @param pos outpustream.
     */
    public void writeTo(final PacketOutputStream pos) throws IOException {
        pos.write(defaultBytes);
    }

    public int getApproximateTextProtocolLength() {
        return 7;
    }

    /**
     * Write data to socket in binary format.
     *
     * @param pos socket output stream
     * @throws IOException if socket error occur
     */
    public void writeBinary(final PacketOutputStream pos) throws IOException {
        pos.writeFieldLength(defaultBytes.length);
        pos.write(defaultBytes);
    }

    public ColumnType getColumnType() {
        return ColumnType.VARCHAR;
    }

    @Override
    public String toString() {
        return "DEFAULT";
    }

    public boolean isNullData() {
        return false;
    }

    public boolean isLongData() {
        return false;
    }
}
