package com.oceanbase.jdbc.internal.failover.LoadBalanceStrategy;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import com.oceanbase.jdbc.HostAddress;
import com.oceanbase.jdbc.UrlParser;
import com.oceanbase.jdbc.internal.failover.Listener;
import com.oceanbase.jdbc.internal.failover.impl.LoadBalanceAddressList;
import com.oceanbase.jdbc.internal.failover.tools.SearchFilter;
import com.oceanbase.jdbc.internal.protocol.MasterProtocol;
import com.oceanbase.jdbc.internal.protocol.Protocol;
import com.oceanbase.jdbc.internal.util.pool.GlobalStateInfo;

public class RandomStrategy implements BalanceStrategy {
    public RandomStrategy() {
    }

    @Override
    public String toString() {
        return "RandomStrategy{}";
    }

    @Override
    public void pickConnection(LoadBalanceAddressList loadBalanceAddressList, UrlParser urlParser, Listener listener, GlobalStateInfo globalInfo, SearchFilter searchFilter, ConcurrentMap<HostAddress, Long> blacklist,ConcurrentMap<HostAddress, Long> pickedList) throws SQLException {

        List<HostAddress> loopAddress = loadBalanceAddressList.convertToHostAddressList();
        if(loopAddress.containsAll(blacklist.keySet()) && blacklist.keySet().size() ==  loopAddress.size()) {
            // do nothing use the loopAddress
        } else {
            loopAddress.removeAll(blacklist.keySet());
            List<HostAddress> localPickedList = new ArrayList<>();
            localPickedList.addAll(pickedList.keySet());
            localPickedList.removeAll(blacklist.keySet());
            localPickedList.addAll(loopAddress);
            loopAddress = localPickedList;
        }
        Collections.shuffle(loopAddress);
        Protocol p = listener.getCurrentProtocol();
        if (p != null && !listener.isMasterHostFail()) {
            loopAddress.remove(p.getHostAddress());
        }
        MasterProtocol.loop(listener, globalInfo, loopAddress, searchFilter);

        return;
    }
}
