package com.oceanbase.jdbc.internal.failover.LoadBalanceStrategy;

import java.sql.SQLException;
import java.util.ArrayDeque;
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
import com.oceanbase.jdbc.internal.util.pool.GlobalStateInfo;

public class GroupRotationStrategy implements GroupBalanceStrategy {
    private static final Logger logger = LoggerFactory.getLogger(GroupRotationStrategy.class);

    @Override
    public String toString() {
        return "GroupRotationStrategy{}";
    }

    public String toJson() {
        return "\"OBLB_GROUP_STRATEGY\":\"ROTATION\"";
    }

    @Override
    public void pickAddressList(List<LoadBalanceAddressList> groups, UrlParser urlParser,
                                Listener listener, final GlobalStateInfo globalInfo,
                                SearchFilter searchFilter, ConcurrentMap<HostAddress, HostStateInfo> blacklist, Set<HostAddress> pickedList) throws SQLException {
//        Collections.shuffle(groups);  rotation do nothing
        ArrayDeque<LoadBalanceAddressList> loopGroups = new ArrayDeque<>(groups);
        SQLException sqlException = null;
        boolean connected = false;
        int groupNum = 1;
        while (!loopGroups.isEmpty()) {
            listener.resetOldsBlackListHosts();
            LoadBalanceAddressList loadBalanceAddressList = loopGroups.pollFirst();
            logger.debug("Group " + groupNum + " hosts:" + loadBalanceAddressList);
            try {
                loadBalanceAddressList.balanceStrategy.pickConnection(loadBalanceAddressList,urlParser,listener,globalInfo,searchFilter,blacklist,pickedList);
                connected = true;
                break;
            } catch (SQLException e) {
                sqlException = e;
            }
            groupNum ++;
        }
        //add fall through mechanism because all hosts are in black list
        if(!connected && loopGroups.isEmpty()  && listener.getRetryAllDowns() > 0) {
            logger.debug("Fall through mechanism , all hosts are in black list now");
            logger.debug("retryAllDowns remains " +listener.getRetryAllDowns() );
            loopGroups = new ArrayDeque<>(groups);
            groupNum = 1;
            while (!loopGroups.isEmpty()) {
                LoadBalanceAddressList loadBalanceAddressList = loopGroups.pollFirst();
                logger.debug("Group " + groupNum + " hosts:" + loadBalanceAddressList);
                try {
                    loadBalanceAddressList.balanceStrategy.pickConnectionFallThrough(loadBalanceAddressList,listener,globalInfo);
                    connected = true;
                    break;
                } catch (SQLException e) {
                    sqlException = e;
                }
                groupNum ++;
            }
        }

        if (sqlException != null && !connected) {
            throw sqlException;
        }
    }
}
