package com.alipay.oceanbase.jdbc.internal.com.send.authentication;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import com.alipay.oceanbase.jdbc.authentication.AuthenticationPlugin;
import com.alipay.oceanbase.jdbc.internal.com.read.Buffer;
import com.alipay.oceanbase.jdbc.internal.com.send.authentication.gssapi.GssUtility;
import com.alipay.oceanbase.jdbc.internal.com.send.authentication.gssapi.GssapiAuth;
import com.alipay.oceanbase.jdbc.internal.com.send.authentication.gssapi.StandardGssapiAuthentication;
import com.alipay.oceanbase.jdbc.internal.io.input.PacketInputStream;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;
import com.alipay.oceanbase.jdbc.util.Options;

public class SendGssApiAuthPacket implements AuthenticationPlugin {

    private static final GssapiAuth gssapiAuth;

    static {
        GssapiAuth init;
        try {
            init = GssUtility.getAuthenticationMethod();
        } catch (Throwable t) {
            init = new StandardGssapiAuthentication();
        }
        gssapiAuth = init;
    }

    private byte[]                  seed;
    private String                  optionServicePrincipalName;

    @Override
    public String name() {
        return "GSSAPI client authentication";
    }

    @Override
    public String type() {
        return "auth_gssapi_client";
    }

    /**
     * Initialization.
     *
     * @param authenticationData authentication data (password/token)
     * @param seed server provided seed
     * @param options Connection string options
     */
    public void initialize(String authenticationData, byte[] seed, Options options) {
        this.seed = seed;
        this.optionServicePrincipalName = options.servicePrincipalName;
    }

    /**
     * Process gssapi plugin authentication. see
     * https://mariadb.com/kb/en/library/authentication-plugin-gssapi/
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
        Buffer buffer = new Buffer(seed);

        final String serverSpn = buffer.readStringNullEnd(StandardCharsets.UTF_8);
        // using provided connection string SPN if set, or if not, using to server information
        final String servicePrincipalName = (optionServicePrincipalName != null && !optionServicePrincipalName
            .isEmpty()) ? optionServicePrincipalName : serverSpn;
        String mechanisms = buffer.readStringNullEnd(StandardCharsets.UTF_8);
        if (mechanisms.isEmpty()) {
            mechanisms = "Kerberos";
        }

        gssapiAuth.authenticate(out, in, sequence, servicePrincipalName, mechanisms);

        buffer = in.getPacket(true);
        sequence.set(in.getLastPacketSeq());
        return buffer;
    }
}
