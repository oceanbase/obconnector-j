package com.alipay.oceanbase.jdbc.internal.com.send.parameters;

import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;

import java.io.IOException;
import java.sql.SQLException;

public class OBSerializableParameter extends SerializableParameter {
    public OBSerializableParameter(Object object, boolean noBackslashEscapes) throws SQLException {
        super(object, noBackslashEscapes);
        try {
            if (loadedStream == null) {
                writeObjectToBytes();
            }
            StringBuilder sb = new StringBuilder();
            for (byte b : loadedStream) {
                sb.append(String.format("%02x", b));
            }
            loadedStream = sb.toString().getBytes();
        } catch (IOException e) {
            throw new SQLException("IOException occur:" + e.getMessage());
        }
    }

    @Override
    public void writeTo(final PacketOutputStream pos) throws IOException {
        pos.write(QUOTE);
        pos.writeBytesEscaped(loadedStream, loadedStream.length, noBackSlashEscapes);
        pos.write(QUOTE);
    }

    @Override
    public void writeBinary(final PacketOutputStream pos) throws IOException {
        pos.writeFieldLength(loadedStream.length);
        pos.write(loadedStream);
    }
}
