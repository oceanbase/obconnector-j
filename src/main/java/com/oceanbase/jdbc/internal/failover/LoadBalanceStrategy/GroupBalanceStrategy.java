package com.oceanbase.jdbc.internal.failover.LoadBalanceStrategy;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import com.oceanbase.jdbc.HostAddress;
import com.oceanbase.jdbc.UrlParser;
import com.oceanbase.jdbc.internal.failover.Listener;
import com.oceanbase.jdbc.internal.failover.impl.LoadBalanceAddressList;
import com.oceanbase.jdbc.internal.failover.tools.SearchFilter;
import com.oceanbase.jdbc.internal.util.pool.GlobalStateInfo;

public interface GroupBalanceStrategy {
    void pickAddressList(List<LoadBalanceAddressList> groups, UrlParser urlParser,
                         Listener listener, final GlobalStateInfo globalInfo,
                         SearchFilter searchFilter, ConcurrentMap<HostAddress, Long> blacklist,
                         ConcurrentMap<HostAddress, Long> pickedList) throws SQLException;
}
