package com.alipay.oceanbase.jdbc.internal.com.send.parameters;

import java.io.IOException;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;
import com.alipay.oceanbase.jdbc.internal.ColumnType;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;

public class TimeParameter implements Cloneable, ParameterHolder {

    private final Time     time;
    private final TimeZone timeZone;
    private final boolean  fractionalSeconds;

    /**
     * Constructor.
     *
     * @param time time to write
     * @param timeZone timezone
     * @param fractionalSeconds must fractional seconds be send.
     */
    public TimeParameter(Time time, TimeZone timeZone, boolean fractionalSeconds) {
        this.time = time;
        this.timeZone = timeZone;
        this.fractionalSeconds = fractionalSeconds;
    }

    /**
     * Write Time parameter to outputStream.
     *
     * @param pos the stream to write to
     */
    public void writeTo(final PacketOutputStream pos) throws IOException {
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
        sdf.setTimeZone(timeZone);
        String dateString = sdf.format(time);

        pos.write(QUOTE);
        pos.write(dateString.getBytes());
        int microseconds = (int) (time.getTime() % 1000) * 1000;
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
        return 15;
    }

    /**
     * Write data to socket in binary format.
     *
     * @param pos socket output stream
     * @throws IOException if socket error occur
     */
    public void writeBinary(final PacketOutputStream pos) throws IOException {
        Calendar calendar = Calendar.getInstance(timeZone);
        calendar.setTime(time);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        if (fractionalSeconds) {
            pos.write((byte) 12);
            pos.write((byte) 0);
            pos.writeInt(0);
            pos.write((byte) calendar.get(Calendar.HOUR_OF_DAY));
            pos.write((byte) calendar.get(Calendar.MINUTE));
            pos.write((byte) calendar.get(Calendar.SECOND));
            pos.writeInt(calendar.get(Calendar.MILLISECOND) * 1000);
        } else {
            pos.write((byte) 8); // length
            pos.write((byte) 0);
            pos.writeInt(0);
            pos.write((byte) calendar.get(Calendar.HOUR_OF_DAY));
            pos.write((byte) calendar.get(Calendar.MINUTE));
            pos.write((byte) calendar.get(Calendar.SECOND));
        }
    }

    public ColumnType getColumnType() {
        return ColumnType.TIME;
    }

    @Override
    public String toString() {
        return time.toString();
    }

    public boolean isNullData() {
        return false;
    }

    public boolean isLongData() {
        return false;
    }
}
