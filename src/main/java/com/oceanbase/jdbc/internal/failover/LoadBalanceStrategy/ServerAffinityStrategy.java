package com.oceanbase.jdbc.internal.failover.LoadBalanceStrategy;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentMap;

import com.oceanbase.jdbc.HostAddress;
import com.oceanbase.jdbc.UrlParser;
import com.oceanbase.jdbc.internal.failover.Listener;
import com.oceanbase.jdbc.internal.failover.impl.LoadBalanceAddressList;
import com.oceanbase.jdbc.internal.failover.impl.LoadBalanceHostAddress;
import com.oceanbase.jdbc.internal.failover.tools.SearchFilter;
import com.oceanbase.jdbc.internal.protocol.MasterProtocol;
import com.oceanbase.jdbc.internal.protocol.Protocol;
import com.oceanbase.jdbc.internal.util.pool.GlobalStateInfo;

public class ServerAffinityStrategy implements BalanceStrategy {
    public ServerAffinityStrategy() {

    }

    @Override
    public String toString() {
        return "ServerAffinityStrategy{}";
    }

    public static List<LoadBalanceHostAddress>  shuffleWeight(List<LoadBalanceHostAddress> list) throws SQLException {
        List<LoadBalanceHostAddress> addressList = new ArrayList<>();
        int size = list.size();
        int weightSum = 0;
        for(LoadBalanceHostAddress loadBalanceHostAddress : list) {
            weightSum += loadBalanceHostAddress.getWeight();
        }
        if (weightSum <= 0) {
            throw  new SQLException("Host list weights incorrect");
        }
        for(int j = 0;j <size ;j++) {
            Random random = new Random();
            int n = random.nextInt(weightSum);
            int m = 0;
            for (int i = 0; i < size; i++) {
                if (m <= n && n < m + list.get(i).getWeight()) {
                    addressList.add(list.get(i));
                }
                m += list.get(i).getWeight();
            }
        }
        return addressList;

    }

    @Override
  public void  pickConnection(LoadBalanceAddressList loadBalanceAddressList, UrlParser urlParser, Listener listener, GlobalStateInfo globalInfo, SearchFilter searchFilter, ConcurrentMap<HostAddress, Long> blacklist,ConcurrentMap<HostAddress, Long> pickedList) throws SQLException {
        List<LoadBalanceHostAddress>  localLoadBalanceHostAddress = shuffleWeight(loadBalanceAddressList.addressList);
        List<HostAddress> loopAddress = new ArrayList<>();
        for(LoadBalanceHostAddress key : localLoadBalanceHostAddress) {
            loopAddress.add(key);
        }
        if (loopAddress.containsAll(blacklist.keySet()) && blacklist.keySet().size() == loopAddress.size()) {
            // do nothing use the loopAddress
        } else {
            loopAddress.removeAll(blacklist.keySet());
            List<HostAddress> localPickedList = new ArrayList<>();
            localPickedList.addAll(pickedList.keySet());
            localPickedList.removeAll(blacklist.keySet());
            localPickedList.addAll(loopAddress);
            loopAddress = localPickedList;
        }
        Protocol p = listener.getCurrentProtocol();
        if (p != null && !listener.isMasterHostFail()) {
            loopAddress.remove(p.getHostAddress());
        }
        MasterProtocol.loop(listener, globalInfo, loopAddress, searchFilter);

        return;
  }
}
