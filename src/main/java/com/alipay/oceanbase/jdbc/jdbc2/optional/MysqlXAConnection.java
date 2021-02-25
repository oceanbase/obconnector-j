package com.alipay.oceanbase.jdbc.jdbc2.optional;

import com.alipay.oceanbase.jdbc.OceanBaseConnection;
import com.alipay.oceanbase.jdbc.OceanBaseXaConnection;

public class MysqlXAConnection extends OceanBaseXaConnection {
    public MysqlXAConnection(OceanBaseConnection connection) {
        super(connection);
    }
}
