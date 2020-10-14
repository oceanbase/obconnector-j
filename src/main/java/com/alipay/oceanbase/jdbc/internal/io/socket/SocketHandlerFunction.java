package com.alipay.oceanbase.jdbc.internal.io.socket;

import java.io.IOException;
import java.net.Socket;
import com.alipay.oceanbase.jdbc.util.Options;

@FunctionalInterface
public interface SocketHandlerFunction {

    Socket apply(Options options, String host) throws IOException;
}
