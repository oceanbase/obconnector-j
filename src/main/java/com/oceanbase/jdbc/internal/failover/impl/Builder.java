package com.oceanbase.jdbc.internal.failover.impl;

import java.sql.SQLException;

public interface Builder {
    void reset();

    void buildGlobalConfigs();

    void buildGroupStrategy() throws SQLException;

    void buildBlackListStrategy() throws SQLException;

    void buildBalanceStrategy() throws SQLException;

    void buildHostListBalanceStrategies() throws SQLException;

    LoadBalanceDriver getResult();
}
