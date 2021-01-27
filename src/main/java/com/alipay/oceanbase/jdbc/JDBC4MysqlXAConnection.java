package com.alipay.oceanbase.jdbc;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

/**
 * Compatible with Mysql connector J interface JDBC4MysqlXAConnection
 */
public class JDBC4MysqlXAConnection extends OceanBaseXaConnection implements XAResource {
    OceanBaseXaResource oceanBaseXaResource;

    public JDBC4MysqlXAConnection(OceanBaseConnection connection) {
        super(connection);
        oceanBaseXaResource = new OceanBaseXaResource(connection);

    }

    @Override
    public XAResource getXAResource() {
        return this.oceanBaseXaResource;
    }

    @Override
    public void commit(Xid xid, boolean b) throws XAException {
        oceanBaseXaResource.commit(xid, b);
    }

    @Override
    public void end(Xid xid, int i) throws XAException {
        oceanBaseXaResource.end(xid, i);
    }

    @Override
    public void forget(Xid xid) throws XAException {
        oceanBaseXaResource.forget(xid);
    }

    @Override
    public int getTransactionTimeout() throws XAException {
        return oceanBaseXaResource.getTransactionTimeout();
    }

    @Override
    public boolean isSameRM(XAResource xaResource) throws XAException {
        return oceanBaseXaResource.isSameRM(xaResource);
    }

    @Override
    public int prepare(Xid xid) throws XAException {
        return oceanBaseXaResource.prepare(xid);
    }

    @Override
    public Xid[] recover(int i) throws XAException {
        return oceanBaseXaResource.recover(i);
    }

    @Override
    public void rollback(Xid xid) throws XAException {
        oceanBaseXaResource.rollback(xid);
    }

    @Override
    public boolean setTransactionTimeout(int i) throws XAException {
        return oceanBaseXaResource.setTransactionTimeout(i);
    }

    @Override
    public void start(Xid xid, int i) throws XAException {
        oceanBaseXaResource.start(xid, i);
    }
}
