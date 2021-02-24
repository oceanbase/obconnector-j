package com.alipay.oceanbase.jdbc.internal.com.send.parameters;

import com.alipay.oceanbase.jdbc.extend.datatype.TIMESTAMPLTZ;
import com.alipay.oceanbase.jdbc.internal.ColumnType;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;
import com.alipay.oceanbase.jdbc.extend.datatype.INTERVALDS;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;

public class OBINTERVALDSParameter implements Cloneable, ParameterHolder {
    INTERVALDS                  intervaldsValue;
    private static final byte[] LITERALS_INTERVALDS     = "interval ".getBytes();
    private static final byte[] LITERALS_INTERVALDS_END = " day(9) to second(9)".getBytes();

    public OBINTERVALDSParameter(INTERVALDS intervalds) {
        this.intervaldsValue = intervalds;
    }

    @Override
    public void writeTo(PacketOutputStream os) throws IOException {
        os.write(LITERALS_INTERVALDS);
        os.write(QUOTE);
        os.write(intervaldsValue.toString().getBytes());
        os.write(QUOTE);
        os.write(LITERALS_INTERVALDS_END);
    }

    @Override
    public void writeBinary(PacketOutputStream pos) throws IOException {
        byte[] data = intervaldsValue.getBytes();
        pos.write((byte) data.length);
        pos.write(data, 0, data.length);
    }

    @Override
    public int getApproximateTextProtocolLength() throws IOException {
        return intervaldsValue.getBytes().length;
    }

    @Override
    public boolean isNullData() {
        return false;
    }

    @Override
    public ColumnType getColumnType() {
        return ColumnType.INTERVALDS;
    }

    @Override
    public boolean isLongData() {
        return false;
    }
}
