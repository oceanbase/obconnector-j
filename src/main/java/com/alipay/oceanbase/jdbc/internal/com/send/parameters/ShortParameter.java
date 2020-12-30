package com.alipay.oceanbase.jdbc.internal.com.send.parameters;

import java.io.IOException;
import com.alipay.oceanbase.jdbc.internal.ColumnType;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;

public class ShortParameter implements Cloneable, ParameterHolder {

    private final short value;

    public ShortParameter(short value) {
        this.value = value;
    }

    public void writeTo(final PacketOutputStream pos) throws IOException {
        pos.write(String.valueOf(value).getBytes());
    }

    public int getApproximateTextProtocolLength() {
        return String.valueOf(value).getBytes().length;
    }

    /**
     * Write data to socket in binary format.
     *
     * @param pos socket output stream
     * @throws IOException if socket error occur
     */
    public void writeBinary(final PacketOutputStream pos) throws IOException {
        pos.writeShort(value);
    }

    public ColumnType getColumnType() {
        return ColumnType.SMALLINT;
    }

    @Override
    public String toString() {
        return Short.toString(value);
    }

    public boolean isNullData() {
        return false;
    }

    public boolean isLongData() {
        return false;
    }
}
