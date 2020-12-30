package com.alipay.oceanbase.jdbc.internal.com.send.authentication;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import com.alipay.oceanbase.jdbc.authentication.AuthenticationPlugin;
import com.alipay.oceanbase.jdbc.internal.com.read.Buffer;
import com.alipay.oceanbase.jdbc.internal.io.input.PacketInputStream;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;
import com.alipay.oceanbase.jdbc.util.Options;

public class ClearPasswordPlugin implements AuthenticationPlugin {

    public static final String TYPE = "mysql_clear_password";

    private String             authenticationData;
    private String             passwordCharacterEncoding;

    @Override
    public String name() {
        return "mysql clear password";
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public boolean mustUseSsl() {
        return true;
    }

    public void initialize(String authenticationData, byte[] authData, Options options) {
        this.authenticationData = authenticationData;
        this.passwordCharacterEncoding = options.passwordCharacterEncoding;
    }

    /**
     * Send password in clear text to server.
     *
     * @param out out stream
     * @param in in stream
     * @param sequence packet sequence
     * @return response packet
     * @throws IOException if socket error
     */
    public Buffer process(PacketOutputStream out, PacketInputStream in, AtomicInteger sequence)
                                                                                               throws IOException {
        if (authenticationData == null || authenticationData.isEmpty()) {
            out.writeEmptyPacket(sequence.incrementAndGet());
        } else {
            out.startPacket(sequence.incrementAndGet());
            byte[] bytePwd;
            if (passwordCharacterEncoding != null && !passwordCharacterEncoding.isEmpty()) {
                bytePwd = authenticationData.getBytes(passwordCharacterEncoding);
            } else {
                bytePwd = authenticationData.getBytes();
            }
            out.write(bytePwd);
            out.write(0);
            out.flush();
        }

        Buffer buffer = in.getPacket(true);
        sequence.set(in.getLastPacketSeq());
        return buffer;
    }
}
