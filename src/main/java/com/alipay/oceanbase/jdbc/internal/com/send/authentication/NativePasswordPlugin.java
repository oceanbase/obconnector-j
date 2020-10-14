package com.alipay.oceanbase.jdbc.internal.com.send.authentication;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import com.alipay.oceanbase.jdbc.authentication.AuthenticationPlugin;
import com.alipay.oceanbase.jdbc.internal.com.read.Buffer;
import com.alipay.oceanbase.jdbc.internal.io.input.PacketInputStream;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;
import com.alipay.oceanbase.jdbc.internal.util.Utils;
import com.alipay.oceanbase.jdbc.util.Options;

public class NativePasswordPlugin implements AuthenticationPlugin {

    public static final String TYPE = "mysql_native_password";

    private String             authenticationData;
    private String             passwordCharacterEncoding;
    private byte[]             seed;

    @Override
    public String name() {
        return "mysql native password";
    }

    @Override
    public String type() {
        return TYPE;
    }

    /**
     * Initialized data.
     *
     * @param authenticationData authentication data (password/token)
     * @param seed server provided seed
     * @param options Connection string options
     */
    public void initialize(String authenticationData, byte[] seed, Options options) {
        this.seed = seed;
        this.authenticationData = authenticationData;
        this.passwordCharacterEncoding = options.passwordCharacterEncoding;
    }

    /**
     * Process native password plugin authentication. see
     * https://mariadb.com/kb/en/library/authentication-plugin-mysql_native_password/
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
            try {
                out.startPacket(sequence.incrementAndGet());
                byte[] truncatedSeed;
                if (seed.length > 0) {
                    // Seed is ended with a null byte value.
                    truncatedSeed = Arrays.copyOfRange(seed, 0, seed.length - 1);
                } else {
                    truncatedSeed = new byte[0];
                }
                out.write(Utils.encryptPassword(authenticationData, truncatedSeed,
                    passwordCharacterEncoding));
                out.flush();
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("Could not use SHA-1, failing", e);
            }
        }

        Buffer buffer = in.getPacket(true);
        sequence.set(in.getLastPacketSeq());
        return buffer;
    }
}
