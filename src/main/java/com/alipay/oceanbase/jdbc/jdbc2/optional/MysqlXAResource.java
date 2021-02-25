package com.alipay.oceanbase.jdbc.jdbc2.optional;

import com.alipay.oceanbase.jdbc.OceanBaseConnection;
import com.alipay.oceanbase.jdbc.OceanBaseXaResource;

public class MysqlXAResource extends OceanBaseXaResource {

    public MysqlXAResource(OceanBaseConnection connection) {
        super(connection);
    }
}
