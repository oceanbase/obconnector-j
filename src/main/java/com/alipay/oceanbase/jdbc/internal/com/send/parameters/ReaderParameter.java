package com.alipay.oceanbase.jdbc.internal.com.send.parameters;

import java.io.IOException;
import java.io.Reader;
import com.alipay.oceanbase.jdbc.internal.ColumnType;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;

public class ReaderParameter implements Cloneable, ParameterHolder {

    private final Reader  reader;
    private final long    length;
    private final boolean noBackslashEscapes;

    /**
     * Constructor.
     *
     * @param reader reader to write
     * @param length max length to write (can be null)
     * @param noBackslashEscapes must backslash be escape
     */
    public ReaderParameter(Reader reader, long length, boolean noBackslashEscapes) {
        this.reader = reader;
        this.length = length;
        this.noBackslashEscapes = noBackslashEscapes;
    }

    public ReaderParameter(Reader reader, boolean noBackslashEscapes) {
        this(reader, Long.MAX_VALUE, noBackslashEscapes);
    }

    /**
     * Write reader to database in text format.
     *
     * @param pos database outputStream
     * @throws IOException if any error occur when reading reader
     */
    public void writeTo(PacketOutputStream pos) throws IOException {
        pos.write(QUOTE);
        if (length == Long.MAX_VALUE) {
            pos.write(reader, true, noBackslashEscapes);
        } else {
            pos.write(reader, length, true, noBackslashEscapes);
        }
        pos.write(QUOTE);
    }

    /**
     * Return approximated data calculated length for rewriting queries.
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
            pos.write(reader, false, noBackslashEscapes);
        } else {
            pos.write(reader, length, false, noBackslashEscapes);
        }
    }

    public ColumnType getColumnType() {
        return ColumnType.STRING;
    }

    @Override
    public String toString() {
        return "<Reader>";
    }

    public boolean isNullData() {
        return false;
    }

    public boolean isLongData() {
        return true;
    }
}
