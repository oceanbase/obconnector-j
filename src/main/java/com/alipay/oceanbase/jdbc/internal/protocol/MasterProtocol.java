package com.alipay.oceanbase.jdbc.internal.protocol;

import java.io.Closeable;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

import com.alipay.oceanbase.jdbc.HostAddress;
import com.alipay.oceanbase.jdbc.UrlParser;
import com.alipay.oceanbase.jdbc.internal.failover.FailoverProxy;
import com.alipay.oceanbase.jdbc.internal.failover.Listener;
import com.alipay.oceanbase.jdbc.internal.util.pool.GlobalStateInfo;
import com.alipay.oceanbase.jdbc.internal.failover.tools.SearchFilter;
import com.alipay.oceanbase.jdbc.internal.io.LruTraceCache;

public class MasterProtocol extends AbstractQueryProtocol implements Closeable {

    /**
     * Get a protocol instance.
     *
     * @param urlParser connection URL infos
     * @param globalInfo server global variables information
     * @param lock the lock for thread synchronisation
     * @param traceCache trace cache
     */
    public MasterProtocol(final UrlParser urlParser, final GlobalStateInfo globalInfo,
                          final ReentrantLock lock, LruTraceCache traceCache) {
        super(urlParser, globalInfo, lock, traceCache);
    }

    /**
     * Get new instance.
     *
     * @param proxy proxy
     * @param urlParser url connection object
     * @return new instance
     */
    private static MasterProtocol getNewProtocol(FailoverProxy proxy,
                                                 final GlobalStateInfo globalInfo,
                                                 UrlParser urlParser) {
        MasterProtocol newProtocol = new MasterProtocol(urlParser, globalInfo, proxy.lock,
            proxy.traceCache);
        newProtocol.setProxy(proxy);
        return newProtocol;
    }

    /**
     * loop until found the failed connection.
     *
     * @param listener current failover
     * @param globalInfo server global variables information
     * @param addresses list of HostAddress to loop
     * @param searchFilter search parameter
     * @throws SQLException if not found
     */
    public static void loop(
      Listener listener,
      final GlobalStateInfo globalInfo,
      final List<HostAddress> addresses,
      SearchFilter searchFilter)
      throws SQLException {

    MasterProtocol protocol;
    ArrayDeque<HostAddress> loopAddresses = new ArrayDeque<>(addresses);
    if (loopAddresses.isEmpty()) {
      resetHostList(listener, loopAddresses);
    }

    int maxConnectionTry = listener.getRetriesAllDown();
    boolean firstLoop = true;
    SQLException lastQueryException = null;
    while (!loopAddresses.isEmpty() || (!searchFilter.isFailoverLoop() && maxConnectionTry > 0)) {
      protocol = getNewProtocol(listener.getProxy(), globalInfo, listener.getUrlParser());

      if (listener.isExplicitClosed()) {
        return;
      }
      maxConnectionTry--;

      try {
        HostAddress host = loopAddresses.pollFirst();
        if (host == null) {
          loopAddresses.addAll(listener.getUrlParser().getHostAddresses());
          host = loopAddresses.pollFirst();
        }
        protocol.setHostAddress(host);
        protocol.connect();
        if (listener.isExplicitClosed()) {
          protocol.close();
          return;
        }
        listener.removeFromBlacklist(protocol.getHostAddress());
        listener.foundActiveMaster(protocol);
        return;

      } catch (SQLException e) {
        listener.addToBlacklist(protocol.getHostAddress());
        lastQueryException = e;
      }

      // if server has try to connect to all host, and master still fail
      // add all servers back to continue looping until maxConnectionTry is reached
      if (loopAddresses.isEmpty() && !searchFilter.isFailoverLoop() && maxConnectionTry > 0) {
        resetHostList(listener, loopAddresses);
        if (firstLoop) {
          firstLoop = false;
        } else {
          try {
            // wait 250ms before looping through all connection another time
            Thread.sleep(250);
          } catch (InterruptedException interrupted) {
            // interrupted, continue
          }
        }
      }
    }
    if (lastQueryException != null) {
      throw new SQLException(
          "No active connection found for master : " + lastQueryException.getMessage(),
          lastQueryException.getSQLState(),
          lastQueryException.getErrorCode(),
          lastQueryException);
    }
    throw new SQLException("No active connection found for master");
  }

    /**
     * Reinitialize loopAddresses with all hosts : all servers in randomize order without connected
     * host.
     *
     * @param listener current listener
     * @param loopAddresses the list to reinitialize
     */
    private static void resetHostList(Listener listener, Deque<HostAddress> loopAddresses) {
    // if all servers have been connected without result
    // add back all servers
    List<HostAddress> servers = new ArrayList<>();
    servers.addAll(listener.getUrlParser().getHostAddresses());
    Collections.shuffle(servers);

    loopAddresses.clear();
    loopAddresses.addAll(servers);
  }
}
