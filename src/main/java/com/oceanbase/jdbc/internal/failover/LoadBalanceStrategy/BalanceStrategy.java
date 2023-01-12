package com.oceanbase.jdbc.internal.failover.LoadBalanceStrategy;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import com.oceanbase.jdbc.HostAddress;
import com.oceanbase.jdbc.UrlParser;
import com.oceanbase.jdbc.internal.failover.Listener;
import com.oceanbase.jdbc.internal.failover.impl.LoadBalanceAddressList;
import com.oceanbase.jdbc.internal.failover.tools.SearchFilter;
import com.oceanbase.jdbc.internal.failover.utils.HostStateInfo;
import com.oceanbase.jdbc.internal.util.pool.GlobalStateInfo;

public interface BalanceStrategy {

    String toJson();

    static boolean allBlack(List<HostAddress> loopAddress, ConcurrentMap<HostAddress, HostStateInfo> blacklist ) {
        if(blacklist.keySet().size() == 0) {
            return false;
        }
        if(!blacklist.keySet().containsAll(loopAddress)) {
            return false;
        }
        for(HostAddress hostAddress : blacklist.keySet()) {
            if (loopAddress.contains(hostAddress) && blacklist.get(hostAddress).getState() != HostStateInfo.STATE.BLACK) {
                return false;
            }
        }
        return true;
    }

    /**
     * Select a proper connection based on the current configuration information
     * @param loadBalanceAddressList current load balance config info
     * @param urlParser  url parser,get options from it
     * @param listener   current listener
     * @param globalInfo  global info
     * @param searchFilter  filter
     * @param blacklist  current black host list
     * @throws SQLException
     */
    void pickConnection(LoadBalanceAddressList loadBalanceAddressList, UrlParser urlParser,
                        Listener listener, final GlobalStateInfo globalInfo,
                        SearchFilter searchFilter,
                        ConcurrentMap<HostAddress, HostStateInfo> blacklist,
                        Set<HostAddress> pickedList) throws SQLException;

    void pickConnectionFallThrough(LoadBalanceAddressList loadBalanceAddressList,
                                   Listener listener, GlobalStateInfo globalInfo)
                                                                                 throws SQLException;
}
