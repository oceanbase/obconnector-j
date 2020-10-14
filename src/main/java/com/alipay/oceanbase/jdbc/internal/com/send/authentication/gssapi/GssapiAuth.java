package com.alipay.oceanbase.jdbc.internal.com.send.authentication.gssapi;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import com.alipay.oceanbase.jdbc.internal.io.input.PacketInputStream;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;

public interface GssapiAuth {

    void authenticate(PacketOutputStream writer, PacketInputStream in, AtomicInteger sequence,
                      String servicePrincipalName, String mechanisms) throws SQLException,
                                                                     IOException;
}
