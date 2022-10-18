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
import com.oceanbase.jdbc.internal.failover.utils.HostStateInfo;
import com.oceanbase.jdbc.internal.logging.Logger;
import com.oceanbase.jdbc.internal.logging.LoggerFactory;
import com.oceanbase.jdbc.internal.protocol.MasterProtocol;
import com.oceanbase.jdbc.internal.util.pool.GlobalStateInfo;

public class ServerAffinityStrategy implements BalanceStrategy {
    private static final Logger logger = LoggerFactory.getLogger(ServerAffinityStrategy.class);

    public ServerAffinityStrategy() {

    }

    @Override
    public String toString() {
        return "ServerAffinityStrategy{}";
    }

    public static List<LoadBalanceHostAddress> shuffleWeight(List<LoadBalanceHostAddress> list) throws SQLException {
        List<LoadBalanceHostAddress> addressList = new ArrayList<>();
        int size = list.size();
        int weightSum = 0;
        HashMap<LoadBalanceHostAddress,Boolean> map = new HashMap<LoadBalanceHostAddress, Boolean>();
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
                    map.put(list.get(i),true);
                }
                m += list.get(i).getWeight();
            }
        }
        for(LoadBalanceHostAddress loadBalanceHostAddress : list) {
            if(!map.containsKey(loadBalanceHostAddress)) {
                addressList.add(loadBalanceHostAddress);
            }
        }
        return addressList;

    }

    @Override
  public void pickConnection(LoadBalanceAddressList loadBalanceAddressList, UrlParser urlParser, Listener listener, GlobalStateInfo globalInfo, SearchFilter searchFilter, ConcurrentMap<HostAddress, HostStateInfo> blacklist, Set<HostAddress> pickedList) throws SQLException {
        List<LoadBalanceHostAddress>  localLoadBalanceHostAddress = shuffleWeight(loadBalanceAddressList.addressList);
        List<HostAddress> loopAddress = new ArrayList<>();
        for(LoadBalanceHostAddress key : localLoadBalanceHostAddress) {
            loopAddress.add(key);
        }

        if(BalanceStrategy.allBlack(loopAddress,blacklist)) {
            throw new SQLException("No active connection found for master");
        } else {
            // remove hosts which have been added to blacklist(but not grey list)  in previous groups from current group
            for(HostAddress hostAddress : blacklist.keySet()) {
                if (loopAddress.contains(hostAddress) && blacklist.get(hostAddress).getState() == HostStateInfo.STATE.BLACK) {
                    loopAddress.remove(hostAddress);
                }
            }
        }

        logger.debug("Current black list : " + blacklist);
        logger.debug("LoopAddress : " + loopAddress);
        SQLException sqlException = null;
        try {
            MasterProtocol.loop(listener, globalInfo, loopAddress);
        } catch (SQLException e) {
           sqlException = e;
        }
        if(sqlException != null) {
            throw sqlException;
        }
    }

    public void pickConnectionFallThrough(LoadBalanceAddressList loadBalanceAddressList,
                                          Listener listener, GlobalStateInfo globalInfo) throws SQLException {
        List<LoadBalanceHostAddress>  localLoadBalanceHostAddress = shuffleWeight(loadBalanceAddressList.addressList);
        List<HostAddress> loopAddress = new ArrayList<>();
        for(LoadBalanceHostAddress key : localLoadBalanceHostAddress) {
            loopAddress.add(key);
        }
        logger.debug("LoopAddress : " + loopAddress);
        MasterProtocol.loop(listener, globalInfo, loopAddress, true);
    }
}
