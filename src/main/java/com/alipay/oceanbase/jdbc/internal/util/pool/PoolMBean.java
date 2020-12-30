package com.alipay.oceanbase.jdbc.internal.util.pool;

public interface PoolMBean {

    long getActiveConnections();

    long getTotalConnections();

    long getIdleConnections();

    long getConnectionRequests();

    void resetStaticGlobal();
}
