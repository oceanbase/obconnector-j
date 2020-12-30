package com.alipay.oceanbase.jdbc.internal.com.send.parameters;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;
import com.alipay.oceanbase.jdbc.internal.ColumnType;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;

public class TimestampParameter implements Cloneable, ParameterHolder {

    private final Timestamp ts;
    private final TimeZone  timeZone;
    private final boolean   fractionalSeconds;

    /**
     * Constructor.
     *
     * @param ts timestamps
     * @param timeZone timeZone
     * @param fractionalSeconds must fractional Seconds be send to database.
     */
    public TimestampParameter(Timestamp ts, TimeZone timeZone, boolean fractionalSeconds) {
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
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sdf.setTimeZone(timeZone);

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
        Calendar calendar = Calendar.getInstance(timeZone);
        calendar.setTimeInMillis(ts.getTime());

        pos.write((byte) (fractionalSeconds ? 11 : 7)); // length

        pos.writeShort((short) calendar.get(Calendar.YEAR));
        pos.write((byte) ((calendar.get(Calendar.MONTH) + 1) & 0xff));
        pos.write((byte) (calendar.get(Calendar.DAY_OF_MONTH) & 0xff));
        pos.write((byte) calendar.get(Calendar.HOUR_OF_DAY));
        pos.write((byte) calendar.get(Calendar.MINUTE));
        pos.write((byte) calendar.get(Calendar.SECOND));
        if (fractionalSeconds) {
            pos.writeInt(ts.getNanos() / 1000);
        }
    }

    public ColumnType getColumnType() {
        return ColumnType.DATETIME;
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
