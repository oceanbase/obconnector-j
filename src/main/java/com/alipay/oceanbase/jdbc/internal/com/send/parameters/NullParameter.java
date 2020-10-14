package com.alipay.oceanbase.jdbc.internal.com.send.parameters;

import java.io.IOException;
import com.alipay.oceanbase.jdbc.internal.ColumnType;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;

public class NullParameter implements ParameterHolder, Cloneable {

    private static final byte[] NULL = { 'N', 'U', 'L', 'L' };
    private final ColumnType    type;

    public NullParameter() {
        type = ColumnType.NULL;
    }

    public NullParameter(ColumnType type) {
        this.type = type;
    }

    public void writeTo(final PacketOutputStream os) throws IOException {
        os.write(NULL);
    }

    public int getApproximateTextProtocolLength() {
        return 4;
    }

    /**
     * Write data to socket in binary format.
     *
     * @param pos socket output stream
     */
    public void writeBinary(final PacketOutputStream pos) {
        // null data are not send in binary format.
    }

    public ColumnType getColumnType() {
        return type;
    }

    @Override
    public String toString() {
        return "<null>";
    }

    public boolean isNullData() {
        return true;
    }

    public boolean isLongData() {
        return false;
    }
}
