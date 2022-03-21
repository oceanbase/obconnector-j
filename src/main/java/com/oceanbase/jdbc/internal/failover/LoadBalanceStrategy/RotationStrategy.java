package com.oceanbase.jdbc.internal.failover.LoadBalanceStrategy;

import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import com.oceanbase.jdbc.HostAddress;
import com.oceanbase.jdbc.UrlParser;
import com.oceanbase.jdbc.internal.failover.Listener;
import com.oceanbase.jdbc.internal.failover.tools.SearchFilter;
import com.oceanbase.jdbc.internal.protocol.MasterProtocol;
import com.oceanbase.jdbc.internal.protocol.Protocol;
import com.oceanbase.jdbc.internal.util.Utils;
import com.oceanbase.jdbc.internal.util.pool.GlobalStateInfo;

public class RotationStrategy implements BalanceStrategy {

    public RotationStrategy() {

    }

    @Override
  public void  pickConnection(Utils.LoadBlanceInfo loadBlanceInfo, UrlParser urlParser, Listener listener, GlobalStateInfo globalInfo, SearchFilter searchFilter, ConcurrentMap<HostAddress, Long> blacklist) throws SQLException {
    List<HostAddress> loopAddress  = loadBlanceInfo.hostAddresses;
    loopAddress.removeAll(blacklist.keySet());
    List<HostAddress> blacklistShuffle = new LinkedList<>(blacklist.keySet());
    blacklistShuffle.retainAll(loopAddress);
    Collections.shuffle(blacklistShuffle);
    loopAddress.addAll(blacklistShuffle);
    Protocol p = listener.getCurrentProtocol();
    if (listener.getCurrentProtocol() != null && !listener.isMasterHostFail()) {
      loopAddress.remove(listener.getCurrentProtocol().getHostAddress());
    }
    MasterProtocol.loop(listener, globalInfo, loopAddress, searchFilter);

    return;
  }
}
