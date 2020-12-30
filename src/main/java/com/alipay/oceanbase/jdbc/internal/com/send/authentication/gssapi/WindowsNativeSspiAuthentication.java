package com.alipay.oceanbase.jdbc.internal.com.send.authentication.gssapi;

import com.sun.jna.platform.win32.Sspi;
import com.sun.jna.platform.win32.SspiUtil;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import com.alipay.oceanbase.jdbc.internal.com.read.Buffer;
import com.alipay.oceanbase.jdbc.internal.io.input.PacketInputStream;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;
import waffle.windows.auth.IWindowsSecurityContext;
import waffle.windows.auth.impl.WindowsSecurityContextImpl;

public class WindowsNativeSspiAuthentication implements GssapiAuth {

    /**
     * Process native windows GSS plugin authentication.
     *
     * @param out out stream
     * @param in in stream
     * @param sequence packet sequence
     * @param servicePrincipalName principal name
     * @param mechanisms gssapi mechanism
     * @throws IOException if socket error
     */
    public void authenticate(final PacketOutputStream out, final PacketInputStream in,
                             final AtomicInteger sequence, final String servicePrincipalName,
                             final String mechanisms) throws IOException {

        // initialize a security context on the client
        IWindowsSecurityContext clientContext = WindowsSecurityContextImpl.getCurrent(mechanisms,
            servicePrincipalName);

        do {

            // Step 1: send token to server
            byte[] tokenForTheServerOnTheClient = clientContext.getToken();
            out.startPacket(sequence.incrementAndGet());
            out.write(tokenForTheServerOnTheClient);
            out.flush();

            // Step 2: read server response token
            if (clientContext.isContinue()) {
                Buffer buffer = in.getPacket(true);
                sequence.set(in.getLastPacketSeq());
                byte[] tokenForTheClientOnTheServer = buffer.readRawBytes(buffer.remaining());
                Sspi.SecBufferDesc continueToken = new SspiUtil.ManagedSecBufferDesc(
                    Sspi.SECBUFFER_TOKEN, tokenForTheClientOnTheServer);
                clientContext.initialize(clientContext.getHandle(), continueToken,
                    servicePrincipalName);
            }

        } while (clientContext.isContinue());

        clientContext.dispose();
    }
}
