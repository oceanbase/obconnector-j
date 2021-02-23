package com.alipay.oceanbase.jdbc.internal.com.send.parameters;

import com.alipay.oceanbase.jdbc.internal.ColumnType;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;

import java.io.IOException;
import java.nio.charset.Charset;

public class OBStringParameter implements Cloneable, ParameterHolder {

    private final String  stringValue;
    private final boolean noBackslashEscapes;
    private final String  characterEncoding;
    private final Charset charset;

    public OBStringParameter(String str, boolean noBackslashEscapes, String characterEncoding) {
        this.stringValue = str;
        this.noBackslashEscapes = noBackslashEscapes;
        this.characterEncoding = characterEncoding;
        this.charset = Charset.forName(characterEncoding);
    }

    /**
     * Send escaped String to outputStream.
     *
     * @param pos outpustream.
     */
    public void writeTo(final PacketOutputStream pos) throws IOException {
        pos.write(QUOTE);
        pos.writeBytesEscapedQuote(stringValue.getBytes(charset),
            stringValue.getBytes(charset).length, noBackslashEscapes);
        pos.write(QUOTE);
    }

    public int getApproximateTextProtocolLength() {
        return stringValue.length() * 3;
    }

    /**
     * Write data to socket in binary format.
     *
     * @param pos socket output stream
     * @throws IOException if socket error occur
     */
    public void writeBinary(final PacketOutputStream pos) throws IOException {
        byte[] bytes = stringValue.getBytes(charset);
        pos.writeFieldLength(bytes.length);
        pos.write(bytes);
    }

    public ColumnType getColumnType() {
        return ColumnType.VARCHAR;
    }

    @Override
    public String toString() {
        if (stringValue.length() < 1024) {
            return "'" + stringValue + "'";
        } else {
            return "'" + stringValue.substring(0, 1024) + "...'";
        }
    }

    public boolean isNullData() {
        return false;
    }

    public boolean isLongData() {
        return false;
    }
}
