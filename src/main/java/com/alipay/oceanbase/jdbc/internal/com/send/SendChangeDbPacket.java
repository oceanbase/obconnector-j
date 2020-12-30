package com.alipay.oceanbase.jdbc.internal.com.send;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.alipay.oceanbase.jdbc.internal.com.Packet;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;

public class SendChangeDbPacket {

    /**
     * Send a COM_INIT_DB request to specify the default schema for the connection.
     *
     * @param pos Write outputStream
     * @param database database name
     * @throws IOException if connection problem occur
     */
    public static void send(final PacketOutputStream pos, final String database) throws IOException {
        pos.startPacket(0);
        pos.write(Packet.COM_INIT_DB);
        pos.write(database.getBytes(StandardCharsets.UTF_8));
        pos.flush();
    }
}
