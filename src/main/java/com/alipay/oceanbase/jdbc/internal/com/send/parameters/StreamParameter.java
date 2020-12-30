package com.alipay.oceanbase.jdbc.internal.com.send.parameters;

import java.io.IOException;
import java.io.InputStream;
import com.alipay.oceanbase.jdbc.internal.ColumnType;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;

public class StreamParameter implements Cloneable, ParameterHolder {

    private final InputStream is;
    private final long        length;
    private final boolean     noBackslashEscapes;

    /**
     * Constructor.
     *
     * @param is stream to write
     * @param length max length to write (if null the whole stream will be send)
     * @param noBackslashEscapes must backslash be escape
     */
    public StreamParameter(InputStream is, long length, boolean noBackslashEscapes) {
        this.is = is;
        this.length = length;
        this.noBackslashEscapes = noBackslashEscapes;
    }

    public StreamParameter(InputStream is, boolean noBackSlashEscapes) {
        this(is, Long.MAX_VALUE, noBackSlashEscapes);
    }

    /**
     * Write stream in text format.
     *
     * @param pos database outputStream
     * @throws IOException if any error occur when reader stream
     */
    public void writeTo(final PacketOutputStream pos) throws IOException {
        pos.write(BINARY_INTRODUCER);
        if (length == Long.MAX_VALUE) {
            pos.write(is, true, noBackslashEscapes);
        } else {
            pos.write(is, length, true, noBackslashEscapes);
        }
        pos.write(QUOTE);
    }

    /**
     * Return approximated data calculated length.
     *
     * @return approximated data length.
     */
    public int getApproximateTextProtocolLength() {
        return -1;
    }

    /**
     * Write data to socket in binary format.
     *
     * @param pos socket output stream
     * @throws IOException if socket error occur
     */
    public void writeBinary(final PacketOutputStream pos) throws IOException {
        if (length == Long.MAX_VALUE) {
            pos.write(is, false, noBackslashEscapes);
        } else {
            pos.write(is, length, false, noBackslashEscapes);
        }
    }

    @Override
    public String toString() {
        return "<Stream>";
    }

    public ColumnType getColumnType() {
        return ColumnType.BLOB;
    }

    public boolean isNullData() {
        return false;
    }

    public boolean isLongData() {
        return true;
    }
}
