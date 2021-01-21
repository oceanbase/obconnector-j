package com.alipay.oceanbase.jdbc.internal.com.send.parameters;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;
import com.alipay.oceanbase.jdbc.extend.datatype.TIMESTAMP;
import com.alipay.oceanbase.jdbc.extend.datatype.TIMESTAMPTZ;
import com.alipay.oceanbase.jdbc.internal.ColumnType;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;

public class OBTIMESTAMPTZParameter implements Cloneable, ParameterHolder {

    private final TIMESTAMPTZ   ts;

    private static final byte[] LITERALS_TIMESTAMP = "timestamp ".getBytes();
    private static final int    ORACLE_TIME_SCALE  = 9;                      // When serializing oracle time type , fixedly fill scale 9 .

    /**
     * Constructor.
     *
     * @param ts timestamps
     */
    public OBTIMESTAMPTZParameter(TIMESTAMPTZ ts) {
        this.ts = ts;
    }

    /**
     * Write timestamps to outputStream.
     *
     * @param pos the stream to write to
     */
    public void writeTo(final PacketOutputStream pos) throws IOException {
        byte[] bytes = ts.getBytes();
        Timestamp timestamp;
        try {
            timestamp = new Timestamp(TIMESTAMPTZ.getOriginTime(bytes, TimeZone.getDefault())); // FIXME should not throw SQLException
        } catch (SQLException ex) {
            throw new IOException(ex);
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(timestamp);
        timestamp.setTime(calendar.getTime().getTime());
        timestamp.setNanos(TIMESTAMP.getNanos(bytes, 7));

        String tzStr = TIMESTAMPTZ.toTimezoneStr(bytes[12], bytes[13], "");

        // oracle literals
        // timestamp '2020-01-01 00:00:00.000 timezone'
        pos.write(LITERALS_TIMESTAMP);
        pos.write(QUOTE);
        pos.write(timestamp.toString().getBytes());
        pos.write(' ');
        pos.write(tzStr.getBytes());
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
        int length = data.length + 2;
        pos.write((byte) length);
        pos.write(data, 0, 11);
        pos.write((byte) ORACLE_TIME_SCALE);
        pos.write(data, 12, length - 14);
        pos.write((byte) 0); // tz name len
        pos.write((byte) 0); // tz abbr len
    }

    public ColumnType getColumnType() {
        return ColumnType.TIMESTAMP_TZ;
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
