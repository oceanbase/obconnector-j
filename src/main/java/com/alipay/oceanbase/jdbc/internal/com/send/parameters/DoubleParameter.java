package com.alipay.oceanbase.jdbc.internal.com.send.parameters;

import java.io.IOException;
import com.alipay.oceanbase.jdbc.internal.ColumnType;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;

public class DoubleParameter implements Cloneable, ParameterHolder {

    private final double value;

    public DoubleParameter(double value) {
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
        pos.writeLong(Double.doubleToLongBits(value));
    }

    public ColumnType getColumnType() {
        return ColumnType.DOUBLE;
    }

    @Override
    public String toString() {
        return Double.toString(value);
    }

    public boolean isNullData() {
        return false;
    }

    public boolean isLongData() {
        return false;
    }
}
