package com.alipay.oceanbase.jdbc.internal.com.send.parameters;

import java.io.IOException;
import com.alipay.oceanbase.jdbc.internal.ColumnType;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;

public class OBBINARY_DOUBLEParameter implements Cloneable, ParameterHolder {

    private final double value;

    public OBBINARY_DOUBLEParameter(double value) {
        this.value = value;
    }

    @Override
    public void writeTo(PacketOutputStream os) throws IOException {
        os.write(String.valueOf(value));
    }

    @Override
    public void writeBinary(PacketOutputStream pos) throws IOException {
        pos.writeLong(Double.doubleToLongBits(value));
    }

    @Override
    public int getApproximateTextProtocolLength() throws IOException {
        return String.valueOf(value).getBytes().length;
    }

    @Override
    public boolean isNullData() {
        return false;
    }

    @Override
    public ColumnType getColumnType() {
        return ColumnType.BINARY_DOUBLE;
    }

    @Override
    public boolean isLongData() {
        return false;
    }

    public Double getValue() {
        return value;
    }
}
