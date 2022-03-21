package com.oceanbase.jdbc.internal.failover.LoadBalanceStrategy;

import java.sql.SQLException;
import java.util.concurrent.ConcurrentMap;

import com.oceanbase.jdbc.HostAddress;
import com.oceanbase.jdbc.UrlParser;
import com.oceanbase.jdbc.internal.failover.Listener;
import com.oceanbase.jdbc.internal.failover.tools.SearchFilter;
import com.oceanbase.jdbc.internal.util.Utils;
import com.oceanbase.jdbc.internal.util.pool.GlobalStateInfo;

public interface BalanceStrategy {
    /**
     * Select a proper connection based on the current configuration information
     * @param loadBlanceInfo current load balance config info
     * @param urlParser  url parser,get options from it
     * @param listener   current listener
     * @param globalInfo  global info
     * @param searchFilter  filter
     * @param blacklist  current black host list
     * @throws SQLException
     */
    void pickConnection(Utils.LoadBlanceInfo loadBlanceInfo, UrlParser urlParser,
                        Listener listener, final GlobalStateInfo globalInfo,
                        SearchFilter searchFilter, ConcurrentMap<HostAddress, Long> blacklist)
                                                                                              throws SQLException;
}
