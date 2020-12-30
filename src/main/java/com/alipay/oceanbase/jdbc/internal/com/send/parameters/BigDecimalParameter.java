package com.alipay.oceanbase.jdbc.internal.com.send.parameters;

import java.io.IOException;
import java.math.BigDecimal;
import com.alipay.oceanbase.jdbc.internal.ColumnType;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;

public class BigDecimalParameter implements Cloneable, ParameterHolder {

    private final BigDecimal bigDecimal;

    public BigDecimalParameter(final BigDecimal bigDecimal) {
        this.bigDecimal = bigDecimal;
    }

    public void writeTo(PacketOutputStream pos) throws IOException {
        pos.write(bigDecimal.toPlainString().getBytes());
    }

    public int getApproximateTextProtocolLength() {
        return bigDecimal.toPlainString().getBytes().length;
    }

    /**
     * Write data to socket in binary format.
     *
     * @param pos socket output stream
     * @throws IOException if socket error occur
     */
    public void writeBinary(PacketOutputStream pos) throws IOException {
        String value = bigDecimal.toPlainString();
        pos.writeFieldLength(value.length());
        pos.write(value);
    }

    public ColumnType getColumnType() {
        return ColumnType.DECIMAL;
    }

    @Override
    public String toString() {
        return bigDecimal.toString();
    }

    public boolean isNullData() {
        return false;
    }

    public boolean isLongData() {
        return false;
    }
}
