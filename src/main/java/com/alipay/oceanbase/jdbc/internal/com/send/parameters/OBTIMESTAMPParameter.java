package com.alipay.oceanbase.jdbc.internal.com.send.parameters;

import java.io.*;
import java.sql.*;
import java.text.*;
import java.util.*;
import com.alipay.oceanbase.jdbc.extend.datatype.TIMESTAMP;
import com.alipay.oceanbase.jdbc.internal.ColumnType;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;

public class OBTIMESTAMPParameter implements Cloneable, ParameterHolder {

    private final SimpleDateFormat sdf                = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private final Timestamp        ts;
    private final TimeZone         timeZone;
    private final boolean          fractionalSeconds;

    private static final byte[]    LITERALS_TIMESTAMP = "timestamp ".getBytes();
    private static final int       ORACLE_TIME_SCALE  = 9;                                          // 序列化oracle时间类型时，固定填充scale 9

    /**
     * Constructor.
     *
     * @param ts timestamps
     * @param timeZone timeZone
     * @param fractionalSeconds must fractional Seconds be send to database.
     */
    public OBTIMESTAMPParameter(Timestamp ts, TimeZone timeZone, boolean fractionalSeconds) {
        this.ts = ts;
        this.timeZone = timeZone;
        this.fractionalSeconds = fractionalSeconds;
    }

    /**
     * Write timestamps to outputStream.
     *
     * @param pos the stream to write to
     */
    public void writeTo(final PacketOutputStream pos) throws IOException {
        sdf.setTimeZone(timeZone);

        // oracle literals
        // timestamp '2020-01-01 00:00:00.000'
        pos.write(LITERALS_TIMESTAMP);
        pos.write(QUOTE);
        pos.write(sdf.format(ts).getBytes());
        int microseconds = ts.getNanos() / 1000;
        if (microseconds > 0 && fractionalSeconds) {
            pos.write('.');
            int factor = 100000;
            while (microseconds > 0) {
                int dig = microseconds / factor;
                pos.write('0' + dig);
                microseconds -= dig * factor;
                factor /= 10;
            }
        }
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
        TIMESTAMP timestamp = new TIMESTAMP(ts);
        byte[] data;
        data = timestamp.getBytes();
        pos.write((byte) data.length);
        pos.write(data, 0, 11);
        pos.write((byte) ORACLE_TIME_SCALE);
    }

    public ColumnType getColumnType() {
        return ColumnType.TIMESTAMP_NANO;
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
