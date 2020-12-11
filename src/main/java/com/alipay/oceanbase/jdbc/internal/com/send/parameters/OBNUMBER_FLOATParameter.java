package com.alipay.oceanbase.jdbc.internal.com.send.parameters;

import java.io.IOException;

import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;
import com.alipay.oceanbase.jdbc.internal.ColumnType;

public class OBNUMBER_FLOATParameter implements Cloneable, ParameterHolder {
    private final float value;

    public OBNUMBER_FLOATParameter(float value) {
        this.value = value;
    }

    @Override
    public void writeTo(PacketOutputStream os) throws IOException {
        os.write(String.valueOf(value).getBytes());
    }

    @Override
    public void writeBinary(PacketOutputStream pos) throws IOException {
        pos.writeFloatV1(value);
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
        return ColumnType.NUMBER_FLOAT;
    }

    @Override
    public boolean isLongData() {
        return false;
    }

    public float getValue() {
        return value;
    }
}
