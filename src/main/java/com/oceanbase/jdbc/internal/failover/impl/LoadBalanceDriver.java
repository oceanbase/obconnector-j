package com.oceanbase.jdbc.internal.failover.impl;

import java.sql.SQLException;
import java.util.concurrent.ConcurrentMap;

import com.oceanbase.jdbc.HostAddress;
import com.oceanbase.jdbc.UrlParser;
import com.oceanbase.jdbc.internal.failover.Listener;
import com.oceanbase.jdbc.internal.failover.tools.SearchFilter;
import com.oceanbase.jdbc.internal.util.pool.GlobalStateInfo;

public class LoadBalanceDriver {
    private LoadBalanceInfo loadBalanceInfo;

    public LoadBalanceDriver() {
    }

    public LoadBalanceDriver(LoadBalanceInfo loadBalanceInfo) {
        this.loadBalanceInfo = loadBalanceInfo;
    }

    public void setLoadBalanceInfo(LoadBalanceInfo loadBalanceInfo) {
        this.loadBalanceInfo = loadBalanceInfo;
    }

    public void loop(UrlParser urlParser, Listener listener, final GlobalStateInfo globalInfo,
                     SearchFilter searchFilter, ConcurrentMap<HostAddress, Long> blacklist,
                     ConcurrentMap<HostAddress, Long> pickedList) throws SQLException {
        this.loadBalanceInfo.groupBalanceStrategy.pickAddressList(loadBalanceInfo.groups,
            urlParser, listener, globalInfo, searchFilter, blacklist, pickedList);
    }
}
