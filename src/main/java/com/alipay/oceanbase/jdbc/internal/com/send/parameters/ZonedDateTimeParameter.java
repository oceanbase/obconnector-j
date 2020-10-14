package com.alipay.oceanbase.jdbc.internal.com.send.parameters;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import com.alipay.oceanbase.jdbc.internal.ColumnType;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;
import com.alipay.oceanbase.jdbc.util.Options;

/**
 * server doesn't support temporal with timezone (MDEV-10018) for the moment. So driver parse String
 * entry and send it to Server according to server timezone
 */
public class ZonedDateTimeParameter implements Cloneable, ParameterHolder {

    private final ZonedDateTime tz;
    private final boolean       fractionalSeconds;

    /**
     * Constructor.
     *
     * @param tz zone date time
     * @param serverZoneId server session zoneId
     * @param fractionalSeconds must fractional Seconds be send to database.
     * @param options session options
     */
    public ZonedDateTimeParameter(ZonedDateTime tz, ZoneId serverZoneId, boolean fractionalSeconds,
                                  Options options) {
        ZoneId zoneId = options.useLegacyDatetimeCode ? ZoneOffset.systemDefault() : serverZoneId;
        this.tz = tz.withZoneSameInstant(zoneId);
        this.fractionalSeconds = fractionalSeconds;
    }

    /**
     * Write timestamps to outputStream.
     *
     * @param pos the stream to write to
     */
    public void writeTo(final PacketOutputStream pos) throws IOException {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(
            fractionalSeconds ? "yyyy-MM-dd HH:mm:ss.SSSSSS" : "yyyy-MM-dd HH:mm:ss",
            Locale.ENGLISH);
        pos.write(QUOTE);
        pos.write(formatter.format(tz).getBytes());
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
        pos.write((byte) (fractionalSeconds ? 11 : 7)); // length
        pos.writeShort((short) tz.getYear());
        pos.write((byte) ((tz.getMonth().getValue()) & 0xff));
        pos.write((byte) (tz.getDayOfMonth() & 0xff));
        pos.write((byte) tz.getHour());
        pos.write((byte) tz.getMinute());
        pos.write((byte) tz.getSecond());
        if (fractionalSeconds) {
            pos.writeInt(tz.getNano() / 1000);
        }
    }

    public ColumnType getColumnType() {
        return ColumnType.DATETIME;
    }

    @Override
    public String toString() {
        return "'" + tz.toString() + "'";
    }

    public boolean isNullData() {
        return false;
    }

    public boolean isLongData() {
        return false;
    }
}
