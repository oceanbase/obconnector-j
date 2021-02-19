package com.alipay.oceanbase.jdbc.internal.com.send.parameters;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import com.alipay.oceanbase.jdbc.internal.ColumnType;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;

public class SerializableParameter implements Cloneable, ParameterHolder {

    protected final boolean noBackSlashEscapes;
    protected Object        object;
    protected byte[]        loadedStream = null;

    public SerializableParameter(Object object, boolean noBackslashEscapes) {
        this.object = object;
        this.noBackSlashEscapes = noBackslashEscapes;
    }

    /**
     * Write object to buffer for text protocol.
     *
     * @param pos the stream to write to
     * @throws IOException if error reading stream
     */
    public void writeTo(final PacketOutputStream pos) throws IOException {
        if (loadedStream == null) {
            writeObjectToBytes();
        }
        pos.write(BINARY_INTRODUCER);
        pos.writeBytesEscaped(loadedStream, loadedStream.length, noBackSlashEscapes);
        pos.write(QUOTE);
    }

    protected void writeObjectToBytes() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(object);
        loadedStream = baos.toByteArray();
        object = null;
    }

    /**
     * Return approximated data calculated length.
     *
     * @return approximated data length.
     * @throws IOException if error reading stream
     */
    public int getApproximateTextProtocolLength() throws IOException {
        writeObjectToBytes();
        return loadedStream.length;
    }

    /**
     * Write data to socket in binary format.
     *
     * @param pos socket output stream
     * @throws IOException if socket error occur
     */
    public void writeBinary(final PacketOutputStream pos) throws IOException {
        if (loadedStream == null) {
            writeObjectToBytes();
        }
        pos.writeFieldLength(loadedStream.length);
        pos.write(loadedStream);
    }

    @Override
    public String toString() {
        return "<Serializable>";
    }

    public ColumnType getColumnType() {
        return ColumnType.BLOB;
    }

    public boolean isNullData() {
        return false;
    }

    public boolean isLongData() {
        return false;
    }
}
