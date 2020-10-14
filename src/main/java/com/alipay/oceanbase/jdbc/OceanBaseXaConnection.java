package com.alipay.oceanbase.jdbc;

import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;

public class OceanBaseXaConnection extends OceanBasePooledConnection implements XAConnection {

    public OceanBaseXaConnection(OceanBaseConnection connection) {
        super(connection);
    }

    @Override
    public XAResource getXAResource() {
        return new OceanBaseXaResource(getConnection());
    }
}
