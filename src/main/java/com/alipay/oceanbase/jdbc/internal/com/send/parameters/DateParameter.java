package com.alipay.oceanbase.jdbc.internal.com.send.parameters;

import java.io.IOException;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;
import com.alipay.oceanbase.jdbc.internal.ColumnType;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;
import com.alipay.oceanbase.jdbc.util.Options;

public class DateParameter implements Cloneable, ParameterHolder {

    private final Date     date;
    private final TimeZone timeZone;
    private final Options  options;

    /**
     * Represents a date, constructed with time in millis since epoch.
     *
     * @param date the date
     * @param timeZone timezone to use
     * @param options jdbc options
     */
    public DateParameter(Date date, TimeZone timeZone, Options options) {
        this.date = date;
        this.timeZone = timeZone;
        this.options = options;
    }

    /**
     * Write to server OutputStream in text protocol.
     *
     * @param os output buffer
     */
    public void writeTo(final PacketOutputStream os) throws IOException {
        os.write(QUOTE);
        os.write(dateByteFormat());
        os.write(QUOTE);
    }

    private byte[] dateByteFormat() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        if (options.useLegacyDatetimeCode || options.maximizeMysqlCompatibility) {
            sdf.setTimeZone(Calendar.getInstance().getTimeZone());
        } else {
            sdf.setTimeZone(timeZone);
        }

        return sdf.format(date).getBytes();
    }

    public int getApproximateTextProtocolLength() {
        return 16;
    }

    /**
     * Write data to socket in binary format.
     *
     * @param pos socket output stream
     * @throws IOException if socket error occur
     */
    public void writeBinary(final PacketOutputStream pos) throws IOException {
        Calendar calendar = Calendar.getInstance(timeZone);
        calendar.setTimeInMillis(date.getTime());

        pos.write((byte) 7); // length
        pos.writeShort((short) calendar.get(Calendar.YEAR));
        pos.write((byte) ((calendar.get(Calendar.MONTH) + 1) & 0xff));
        pos.write((byte) (calendar.get(Calendar.DAY_OF_MONTH) & 0xff));
        pos.write((byte) 0);
        pos.write((byte) 0);
        pos.write((byte) 0);
    }

    public ColumnType getColumnType() {
        return ColumnType.DATE;
    }

    @Override
    public String toString() {
        return "'" + date.toString() + "'";
    }

    public boolean isNullData() {
        return false;
    }

    public boolean isLongData() {
        return false;
    }
}
