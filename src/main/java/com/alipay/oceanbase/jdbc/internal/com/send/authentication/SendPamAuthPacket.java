package com.alipay.oceanbase.jdbc.internal.com.send.authentication;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import com.alipay.oceanbase.jdbc.authentication.AuthenticationPlugin;
import com.alipay.oceanbase.jdbc.internal.com.read.Buffer;
import com.alipay.oceanbase.jdbc.internal.io.input.PacketInputStream;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;
import com.alipay.oceanbase.jdbc.util.Options;

public class SendPamAuthPacket implements AuthenticationPlugin {

    private String  authenticationData;
    private String  passwordCharacterEncoding;
    private Options options;
    private int     counter = 0;

    @Override
    public String name() {
        return "PAM client authentication";
    }

    @Override
    public String type() {
        return "dialog";
    }

    /**
     * Initialization.
     *
     * @param authenticationData authentication data (password/token)
     * @param seed server provided seed
     * @param options Connection string options
     */
    public void initialize(String authenticationData, byte[] seed, Options options) {
        this.authenticationData = authenticationData;
        this.passwordCharacterEncoding = options.passwordCharacterEncoding;
        this.options = options;
    }

    /**
     * Process PAM plugin authentication. see
     * https://mariadb.com/kb/en/library/authentication-plugin-pam/
     *
     * @param out out stream
     * @param in in stream
     * @param sequence packet sequence
     * @return response packet
     * @throws IOException if socket error
     * @throws SQLException if plugin exception
     */
    public Buffer process(PacketOutputStream out, PacketInputStream in, AtomicInteger sequence)
                                                                                               throws IOException,
                                                                                               SQLException {
        while (true) {
            counter++;
            String password;
            if (counter == 1) {
                password = authenticationData;
            } else {
                if (!options.nonMappedOptions.containsKey("password" + counter)) {
                    throw new SQLException("PAM authentication request multiple passwords, but "
                                           + "'password" + counter + "' is not set");
                }
                password = (String) options.nonMappedOptions.get("password" + counter);
            }

            out.startPacket(sequence.incrementAndGet());
            byte[] bytePwd;
            if (passwordCharacterEncoding != null && !passwordCharacterEncoding.isEmpty()) {
                bytePwd = password.getBytes(passwordCharacterEncoding);
            } else {
                bytePwd = password.getBytes();
            }

            out.write(bytePwd, 0, bytePwd.length);
            out.write(0);
            out.flush();

            Buffer buffer = in.getPacket(true);
            sequence.set(in.getLastPacketSeq());
            int type = buffer.getByteAt(0) & 0xff;

            // PAM continue until finish.
            if (type == 0xfe // Switch Request
                || type == 0x00 // OK_Packet
                || type == 0xff) { // ERR_Packet
                return buffer;
            }
        }
    }
}
