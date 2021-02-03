package com.alipay.oceanbase.jdbc.internal.com.send;

import com.alipay.oceanbase.jdbc.internal.com.Packet;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;

import java.io.IOException;

public class ComStmtFetch {
    /**
     *
     * @param pos
     * @param fetchSize
     * @throws IOException
     */
    public static void send(final PacketOutputStream pos, int fetchSize, int statementId)
                                                                                         throws IOException {
        pos.startPacket(0);
        pos.write(Packet.COM_STMT_FETCH);
        pos.writeInt(statementId);
        pos.writeInt(fetchSize);
        pos.flush();
    }
}
