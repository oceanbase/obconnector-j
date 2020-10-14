package com.alipay.oceanbase.jdbc.internal.com.send.parameters;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import com.alipay.oceanbase.jdbc.extend.datatype.TIMESTAMPLTZ;
import com.alipay.oceanbase.jdbc.internal.ColumnType;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;

public class OBTIMESTAMPLTZParameter implements Cloneable, ParameterHolder {

    private final TIMESTAMPLTZ  ts;
    private final Connection    connection;

    private static final byte[] LITERALS_TIMESTAMP = "timestamp ".getBytes();
    private static final int    ORACLE_TIME_SCALE  = 9;                      // 序列化oracle时间类型时，固定填充scale 9

    /**
     * Constructor.
     *
     * @param ts timestamps
     */
    public OBTIMESTAMPLTZParameter(TIMESTAMPLTZ ts, Connection connection) {
        this.ts = ts;
        this.connection = connection;
    }

    /**
     * Write timestamps to outputStream.
     *
     * @param pos the stream to write to
     */
    public void writeTo(final PacketOutputStream pos) throws IOException {

        Timestamp timestamp;
        try {
            timestamp = TIMESTAMPLTZ.toTimestamp(this.connection, ts.getBytes()); // FIXME should not throw SQLException
        } catch (SQLException ex) {
            throw new IOException(ex);
        }
        // oracle literals
        // timestamp '2020-01-01 00:00:00.000 timezone'
        pos.write(LITERALS_TIMESTAMP);
        pos.write(QUOTE);
        pos.write(timestamp.toString().getBytes());
        pos.write(QUOTE);
    }

    public int getApproximateTextProtocolLength() {
        return 27;
    }

    /**
     * Write data to socket in binary format.
     *
     * @param pos socket output stream
     * @throws IOException if socket error occur
     */
    public void writeBinary(final PacketOutputStream pos) throws IOException {
        byte[] data = ts.getBytes();
        pos.write((byte) data.length);
        pos.write(data, 0, 11);
        pos.write((byte) ORACLE_TIME_SCALE);
    }

    public ColumnType getColumnType() {
        return ColumnType.TIMESTAMP_LTZ;
    }

    @Override
    public String toString() {
        return "'" + ts.toString() + "'";
    }

    public boolean isNullData() {
        return false;
    }

    public boolean isLongData() {
        return false;
    }
}
