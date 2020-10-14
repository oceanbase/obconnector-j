package com.alipay.oceanbase.jdbc.internal.com.send;

import java.io.IOException;

import com.alipay.oceanbase.jdbc.internal.com.Packet;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;

public class SendClosePacket {

    /**
     * Send close stream to server.
     *
     * @param pos write outputStream
     */
    public static void send(final PacketOutputStream pos) {
        try {
            pos.startPacket(0);
            pos.write(Packet.COM_QUIT);
            pos.flush();
        } catch (IOException ioe) {
            // eat
        }
    }
}
