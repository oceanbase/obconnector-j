package com.alipay.oceanbase.jdbc.internal.com.send.parameters;

import java.io.IOException;
import java.sql.SQLException;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import com.alipay.oceanbase.jdbc.internal.ColumnType;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;
import com.alipay.oceanbase.jdbc.util.Options;

public class OffsetTimeParameter implements Cloneable, ParameterHolder {

    private OffsetTime time;
    private boolean    fractionalSeconds;

    /**
     * Constructor.
     *
     * @param offsetTime time with offset
     * @param serverZoneId server session zoneId
     * @param fractionalSeconds must fractional Seconds be send to database.
     * @param options session options
     * @throws SQLException if offset cannot be converted to server offset
     */
    public OffsetTimeParameter(OffsetTime offsetTime, ZoneId serverZoneId,
                               boolean fractionalSeconds, Options options) throws SQLException {
        ZoneId zoneId = options.useLegacyDatetimeCode ? ZoneOffset.systemDefault() : serverZoneId;
        if (zoneId instanceof ZoneOffset) {
            throw new SQLException("cannot set OffsetTime, since server time zone is set to '"
                                   + serverZoneId.toString()
                                   + "' (check server variables time_zone and system_time_zone)");
        }
        this.time = offsetTime.withOffsetSameInstant((ZoneOffset) zoneId);
        this.fractionalSeconds = fractionalSeconds;
    }

    /**
     * Write timestamps to outputStream.
     *
     * @param pos the stream to write to
     */
    public void writeTo(final PacketOutputStream pos) throws IOException {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(
            fractionalSeconds ? "HH:mm:ss.SSSSSS" : "HH:mm:ss", Locale.ENGLISH);
        pos.write(QUOTE);
        pos.write(formatter.format(time).getBytes());
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
        if (fractionalSeconds) {
            pos.write((byte) 12); // length
            pos.write((byte) 0);
            pos.writeInt(0);
            pos.write((byte) time.getHour());
            pos.write((byte) time.getMinute());
            pos.write((byte) time.getSecond());
            pos.writeInt(time.getNano() / 1000);
        } else {
            pos.write((byte) 8); // length
            pos.write((byte) 0);
            pos.writeInt(0);
            pos.write((byte) time.getHour());
            pos.write((byte) time.getMinute());
            pos.write((byte) time.getSecond());
        }
    }

    public ColumnType getColumnType() {
        return ColumnType.TIME;
    }

    @Override
    public String toString() {
        return "'" + time.toString() + "'";
    }

    public boolean isNullData() {
        return false;
    }

    public boolean isLongData() {
        return false;
    }
}
