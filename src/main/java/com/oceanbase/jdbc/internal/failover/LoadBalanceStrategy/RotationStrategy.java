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
import com.oceanbase.jdbc.internal.logging.Logger;
import com.oceanbase.jdbc.internal.logging.LoggerFactory;
import com.oceanbase.jdbc.internal.protocol.MasterProtocol;
import com.oceanbase.jdbc.internal.util.pool.GlobalStateInfo;

public class RotationStrategy implements BalanceStrategy {
    private static final Logger logger = LoggerFactory.getLogger(RotationStrategy.class);

    public RotationStrategy() {

    }

    @Override
    public String toString() {
        return "RotationStrategy{}";
    }

    public String toJson() {
        return "\"OBLB_STRATEGY\":\"ROTATION\"";
    }

    @Override
    public void pickConnection(LoadBalanceAddressList loadBalanceAddressList, UrlParser urlParser,
                               Listener listener, GlobalStateInfo globalInfo,
                               SearchFilter searchFilter,
                               ConcurrentMap<HostAddress, HostStateInfo> blacklist,
                               Set<HostAddress> pickedList) throws SQLException {
        List<HostAddress> loopAddress = loadBalanceAddressList.convertToHostAddressList();
        if (BalanceStrategy.allBlack(loopAddress, blacklist)) {
            throw new SQLException("No active connection found for master");
        } else {
            // remove hosts which have been added to blacklist(but not grey list)  in previous groups from current group
            for (HostAddress hostAddress : blacklist.keySet()) {
                if (loopAddress.contains(hostAddress)
                    && blacklist.get(hostAddress).getState() == HostStateInfo.STATE.BLACK) {
                    loopAddress.remove(hostAddress);
                }
            }
        }
        logger.debug("Current black list : " + blacklist);
        logger.debug("LoopAddress : " + loopAddress);
        MasterProtocol.loop(listener, globalInfo, loopAddress);
    }

    public void pickConnectionFallThrough(LoadBalanceAddressList loadBalanceAddressList,
                                          Listener listener, GlobalStateInfo globalInfo)
                                                                                        throws SQLException {
        List<HostAddress> loopAddress = loadBalanceAddressList.convertToHostAddressList();
        logger.debug("LoopAddress : " + loopAddress);
        MasterProtocol.loop(listener, globalInfo, loopAddress, true);
    }
}
