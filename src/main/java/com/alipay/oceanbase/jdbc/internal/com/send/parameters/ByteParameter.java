package com.alipay.oceanbase.jdbc.internal.com.send.parameters;

import java.io.IOException;
import com.alipay.oceanbase.jdbc.internal.ColumnType;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;

public class ByteParameter implements Cloneable, ParameterHolder {

    private static final char[] hexArray = "0123456789ABCDEF".toCharArray();
    private final int           value;

    public ByteParameter(byte value) {
        this.value = value;
    }

    /**
     * Write Byte value to stream using TEXT protocol.
     *
     * @param os the stream to write to
     * @throws IOException if any socket error occur
     */
    public void writeTo(final PacketOutputStream os) throws IOException {
        os.write(String.valueOf(value).getBytes());
    }

    public int getApproximateTextProtocolLength() {
        return String.valueOf(value).length();
    }

    /**
     * Write data to socket in binary format.
     *
     * @param pos socket output stream
     * @throws IOException if socket error occur
     */
    public void writeBinary(final PacketOutputStream pos) throws IOException {
        pos.write(value);
    }

    public ColumnType getColumnType() {
        return ColumnType.TINYINT;
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }

    public boolean isNullData() {
        return false;
    }

    public boolean isLongData() {
        return false;
    }
}
