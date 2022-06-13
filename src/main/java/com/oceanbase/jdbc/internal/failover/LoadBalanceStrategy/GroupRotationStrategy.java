package com.oceanbase.jdbc.internal.failover.LoadBalanceStrategy;

import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import com.oceanbase.jdbc.HostAddress;
import com.oceanbase.jdbc.UrlParser;
import com.oceanbase.jdbc.internal.failover.Listener;
import com.oceanbase.jdbc.internal.failover.impl.LoadBalanceAddressList;
import com.oceanbase.jdbc.internal.failover.tools.SearchFilter;
import com.oceanbase.jdbc.internal.util.pool.GlobalStateInfo;

public class GroupRotationStrategy implements GroupBalanceStrategy {

    @Override
    public String toString() {
        return "GroupRotationStrategy{}";
    }

    @Override
    public void pickAddressList(List<LoadBalanceAddressList> groups, UrlParser urlParser,
                                Listener listener, final GlobalStateInfo globalInfo,
                                SearchFilter searchFilter, ConcurrentMap<HostAddress, Long> blacklist,ConcurrentMap<HostAddress, Long> pickedList) throws SQLException {
//        Collections.shuffle(groups);  rotation do nothing
        ArrayDeque<LoadBalanceAddressList> loopGroups = new ArrayDeque<>(groups);
        SQLException  sqlException = null;
        boolean connected = false;
        while (!loopGroups.isEmpty()) {
            LoadBalanceAddressList loadBalanceAddressList = loopGroups.pollFirst();
            try {
                loadBalanceAddressList.balanceStrategy.pickConnection(loadBalanceAddressList,urlParser,listener,globalInfo,searchFilter,blacklist,pickedList);
                connected = true;
            } catch (SQLException e) {
                connected =false;
                sqlException = e;
            }
            if( !connected ) {
                continue;
            } else {
                break;
            }
        }
        if(sqlException != null && !connected) {
            throw sqlException;
        }
    }
}
