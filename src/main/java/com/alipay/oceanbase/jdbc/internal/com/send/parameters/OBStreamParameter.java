package com.alipay.oceanbase.jdbc.internal.com.send.parameters;

import com.alipay.oceanbase.jdbc.internal.ColumnType;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;

import java.io.IOException;
import java.io.InputStream;

public class OBStreamParameter implements Cloneable, ParameterHolder {

    private final InputStream is;
    private final long        length;
    private final boolean     noBackslashEscapes;
    private final boolean     hasLobLocator;
    private final byte[]      bytes;

    /**
     * Constructor.
     *
     * @param is stream to write
     * @param length max length to write (if null the whole stream will be send)
     * @param noBackslashEscapes must backslash be escape
     */
    public OBStreamParameter(boolean hasLobLocator, InputStream is, long length,
                             boolean noBackslashEscapes) {
        this.is = is;
        this.length = length;
        this.noBackslashEscapes = noBackslashEscapes;
        this.hasLobLocator = hasLobLocator;
        this.bytes = null;
    }

    public OBStreamParameter(boolean hasLobLocator, byte[] bytes, long length,
                             boolean noBackslashEscapes) {
        this.is = null;
        this.length = length;
        this.noBackslashEscapes = noBackslashEscapes;
        this.hasLobLocator = hasLobLocator;
        this.bytes = bytes;
    }

    public OBStreamParameter(InputStream is, long length, boolean noBackslashEscapes) {
        this(false, is, length, noBackslashEscapes);
    }

    public OBStreamParameter(InputStream is, boolean noBackSlashEscapes) {
        this(false, is, Long.MAX_VALUE, noBackSlashEscapes);
    }

    /**
     * Write stream in text format.
     *
     * @param pos database outputStream
     * @throws IOException if any error occur when reader stream
     */
    public void writeTo(final PacketOutputStream pos) throws IOException {

        if (is != null) {
            pos.write(QUOTE);
            if (length == Long.MAX_VALUE) {
                pos.writeHex(is, false, noBackslashEscapes);
            } else {
                pos.writeHex(is, length, false, noBackslashEscapes);
            }
            pos.write(QUOTE);
        }
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
        if (hasLobLocator && bytes != null) {
            pos.writeFieldLength(bytes.length);
            pos.write(bytes);
        } else if (is != null) {
            if (length == Long.MAX_VALUE) {
                pos.write(is, false, noBackslashEscapes);
            } else {
                pos.write(is, length, false, noBackslashEscapes);
            }
        }
    }

    @Override
    public String toString() {
        return "<Stream>";
    }

    public ColumnType getColumnType() {
        return this.hasLobLocator ? ColumnType.OBBLOB : ColumnType.BLOB;
    }

    public boolean isNullData() {
        return false;
    }

    public boolean isLongData() {
        return this.hasLobLocator ? false : true;
    }
}
