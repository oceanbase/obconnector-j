package com.alipay.oceanbase.jdbc.internal.com.send;

import java.io.IOException;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;

/** Used for starting ssl connections. */
public class SendSslConnectionRequestPacket {

    /**
     * Send SSL request packet. See https://mariadb.com/kb/en/library/connection/#sslrequest-packet
     *
     * @param pos output stream
     * @param clientCapabilities client capabilities
     * @param serverLanguage connection charset
     * @throws IOException if any connection error occur
     */
    public static void send(final PacketOutputStream pos, final long clientCapabilities,
                            final byte serverLanguage) throws IOException {
        pos.startPacket(1);
        pos.writeInt((int) clientCapabilities);
        pos.writeInt(1024 * 1024 * 1024);
        pos.write(serverLanguage); // 1
        pos.writeBytes((byte) 0, 19); // 19
        pos.writeInt((int) (clientCapabilities >> 32)); // Maria extended flag
        pos.flush();
    }
}
