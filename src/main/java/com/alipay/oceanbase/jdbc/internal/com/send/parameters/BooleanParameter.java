package com.alipay.oceanbase.jdbc.internal.com.send.parameters;

import java.io.IOException;
import com.alipay.oceanbase.jdbc.internal.ColumnType;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;

public class BooleanParameter implements Cloneable, ParameterHolder {

    private final boolean value;

    public BooleanParameter(boolean value) {
        this.value = value;
    }

    public void writeTo(final PacketOutputStream os) throws IOException {
        os.write(value ? '1' : '0');
    }

    public int getApproximateTextProtocolLength() {
        return 1;
    }

    /**
     * Write data to socket in binary format.
     *
     * @param pos socket output stream
     * @throws IOException if socket error occur
     */
    public void writeBinary(final PacketOutputStream pos) throws IOException {
        pos.write(value ? 1 : 0);
    }

    public ColumnType getColumnType() {
        return ColumnType.TINYINT;
    }

    @Override
    public String toString() {
        return Boolean.toString(value);
    }

    public boolean isNullData() {
        return false;
    }

    public boolean isLongData() {
        return false;
    }
}
