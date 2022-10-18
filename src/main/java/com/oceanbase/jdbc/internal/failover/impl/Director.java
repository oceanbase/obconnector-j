package com.oceanbase.jdbc.internal.failover.impl;

import java.sql.SQLException;

public class Director {
    Builder builder = null;

    public Director(Builder builder) {
        this.builder = builder;
    }

    public LoadBalanceDriver construct() throws SQLException {
        builder.buildGlobalConfigs();
        builder.buildGroupStrategy();
        builder.buildBlackListStrategy();
        builder.buildBalanceStrategy();
        builder.buildHostListBalanceStrategies();
        return builder.getResult();
    }
}
