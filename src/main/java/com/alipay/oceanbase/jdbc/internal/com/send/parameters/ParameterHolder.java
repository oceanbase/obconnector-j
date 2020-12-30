package com.alipay.oceanbase.jdbc.internal.com.send.parameters;

import java.io.IOException;
import com.alipay.oceanbase.jdbc.internal.ColumnType;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;

public interface ParameterHolder {

    byte[] BINARY_INTRODUCER = { '_', 'b', 'i', 'n', 'a', 'r', 'y', ' ', '\'' };
    byte   QUOTE             = (byte) '\'';

    /**
     * Write parameter value.
     *
     * @param os the stream to write to
     * @throws IOException when something goes wrong
     */
    void writeTo(final PacketOutputStream os) throws IOException;

    void writeBinary(PacketOutputStream pos) throws IOException;

    int getApproximateTextProtocolLength() throws IOException;

    String toString();

    boolean isNullData();

    ColumnType getColumnType();

    boolean isLongData();
}
