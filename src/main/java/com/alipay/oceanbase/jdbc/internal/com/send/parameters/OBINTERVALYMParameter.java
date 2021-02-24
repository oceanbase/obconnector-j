package com.alipay.oceanbase.jdbc.internal.com.send.parameters;

import com.alipay.oceanbase.jdbc.extend.datatype.INTERVALYM;
import com.alipay.oceanbase.jdbc.internal.ColumnType;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;

import java.io.IOException;

public class OBINTERVALYMParameter implements Cloneable, ParameterHolder {
    INTERVALYM                  intervalymValue;
    private static final byte[] LITERALS_INTERVAL     = "interval ".getBytes();
    private static final byte[] LITERALS_INTERVAL_END = " year(9) to month".getBytes();

    public OBINTERVALYMParameter(INTERVALYM intervalym) {
        this.intervalymValue = intervalym;
    }

    @Override
    public void writeTo(PacketOutputStream os) throws IOException {
        os.write(LITERALS_INTERVAL);
        os.write(QUOTE);
        os.write(intervalymValue.toString().getBytes());
        os.write(QUOTE);
        os.write(LITERALS_INTERVAL_END);
    }

    @Override
    public void writeBinary(PacketOutputStream pos) throws IOException {
        byte[] data = intervalymValue.getBytes();
        pos.write((byte) data.length);
        pos.write(data, 0, data.length);
    }

    @Override
    public int getApproximateTextProtocolLength() throws IOException {
        return intervalymValue.getBytes().length;
    }

    @Override
    public boolean isNullData() {
        return false;
    }

    @Override
    public ColumnType getColumnType() {
        return ColumnType.INTERVALYM;
    }

    @Override
    public boolean isLongData() {
        return false;
    }
}
