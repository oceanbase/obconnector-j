package com.alipay.oceanbase.jdbc.internal.com.send.parameters;

import com.alipay.oceanbase.jdbc.internal.ColumnType;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;

import java.io.IOException;
import java.util.Arrays;

public class OBByteArrayParameter implements Cloneable, ParameterHolder {

    private final byte[]  bytes;
    private final boolean noBackslashEscapes;

    public OBByteArrayParameter(byte[] bytes, boolean noBackslashEscapes) {
        this.noBackslashEscapes = noBackslashEscapes;
        byte[] x = bytes;
        StringBuilder sb = new StringBuilder();
        for (byte b : x) {
            sb.append(String.format("%02x", b));
        }
        x = sb.toString().getBytes();
        this.bytes = x;
    }

    /**
     * Write data to socket in text format.
     *
     * @param pos socket output stream
     * @throws IOException if socket error occur
     */
    public void writeTo(final PacketOutputStream pos) throws IOException {
        pos.write(QUOTE);
        pos.writeBytesEscaped(bytes, bytes.length, noBackslashEscapes);
        pos.write(QUOTE);
    }

    public int getApproximateTextProtocolLength() {
        return bytes.length * 2;
    }

    /**
     * Write data to socket in binary format.
     *
     * @param pos socket output stream
     * @throws IOException if socket error occur
     */
    public void writeBinary(final PacketOutputStream pos) throws IOException {
        pos.writeFieldLength(bytes.length);
        pos.write(bytes);
    }

    public ColumnType getColumnType() {
        return ColumnType.VARSTRING;
    }

    @Override
    public String toString() {
        if (bytes.length > 1024) {
            return "<bytearray:" + new String(Arrays.copyOfRange(bytes, 0, 1024)) + "...>";
        } else {
            return "<bytearray:" + new String(bytes) + ">";
        }
    }

    public boolean isNullData() {
        return false;
    }

    public boolean isLongData() {
        return false;
    }
}
