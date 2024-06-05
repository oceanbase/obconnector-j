/**
 *  OceanBase Client for Java
 *
 *  Copyright (c) 2012-2014 Monty Program Ab.
 *  Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *  Copyright (c) 2021 OceanBase.
 *
 *  This library is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU Lesser General Public License as published by the Free
 *  Software Foundation; either version 2.1 of the License, or (at your option)
 *  any later version.
 *
 *  This library is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License along
 *  with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 *  This particular MariaDB Client for Java file is work
 *  derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 *  the following copyright and notice provisions:
 *
 *  Copyright (c) 2009-2011, Marcus Eriksson
 *
 *  Redistribution and use in source and binary forms, with or without modification,
 *  are permitted provided that the following conditions are met:
 *  Redistributions of source code must retain the above copyright notice, this list
 *  of conditions and the following disclaimer.
 *
 *  Redistributions in binary form must reproduce the above copyright notice, this
 *  list of conditions and the following disclaimer in the documentation and/or
 *  other materials provided with the distribution.
 *
 *  Neither the name of the driver nor the names of its contributors may not be
 *  used to endorse or promote products derived from this software without specific
 *  prior written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 *  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 *  IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 *  INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 *  NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 *  PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 *  WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 *  OF SUCH DAMAGE.
 */
package com.oceanbase.jdbc.internal.protocol;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

import com.oceanbase.jdbc.HostAddress;
import com.oceanbase.jdbc.UrlParser;
import com.oceanbase.jdbc.internal.failover.BlackList.BlackListConfig;
import com.oceanbase.jdbc.internal.failover.FailoverProxy;
import com.oceanbase.jdbc.internal.failover.Listener;
import com.oceanbase.jdbc.internal.failover.utils.Consts;
import com.oceanbase.jdbc.internal.failover.utils.HostStateInfo;
import com.oceanbase.jdbc.internal.io.LruTraceCache;
import com.oceanbase.jdbc.internal.logging.Logger;
import com.oceanbase.jdbc.internal.logging.LoggerFactory;
import com.oceanbase.jdbc.internal.util.exceptions.ExceptionFactory;
import com.oceanbase.jdbc.internal.util.pool.GlobalStateInfo;

public class MasterProtocol extends AbstractQueryProtocol implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(MasterProtocol.class);

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

    public static void loop(Listener listener, final GlobalStateInfo globalInfo,
                            final List<HostAddress> addresses) throws SQLException {
        loop(listener, globalInfo, addresses, false);

    }

    /**
     * loop until found the failed connection.
     *
     * @param listener current failover
     * @param globalInfo server global variables information
     * @param addresses list of HostAddress to loop
     * @throws SQLException if not found
     */
    public static void loop(
      Listener listener,
      final GlobalStateInfo globalInfo,
      final List<HostAddress> addresses,boolean fallThrough)
      throws SQLException {
        Set<HostAddress> connectedHosts = new LinkedHashSet<>();
        int attemptedTimes = 0;
        ArrayDeque<HostAddress> loopAddresses = new ArrayDeque<>(addresses);
    if (loopAddresses.isEmpty()) {
      resetHostList(listener, loopAddresses);
    }
    int maxConnectionTry = listener.getRetryAllDowns();
    SQLException lastQueryException = null;
    MasterProtocol protocol;
    while (!loopAddresses.isEmpty() &&  maxConnectionTry > 0) {
      if (listener.isExplicitClosed()) {
        return;
      }
      protocol = getNewProtocol(listener.getProxy(), globalInfo, listener.getUrlParser());
      maxConnectionTry--;
      try {
        HostAddress host = loopAddresses.pollFirst();
        if(!fallThrough && listener.getBlacklistKeys().contains(host) && listener.getBlacklist().get(host).getState() == HostStateInfo.STATE.BLACK) {
            // if in blacklist and state = black ,continue
            continue;
        }
        if (host == null) {
          loopAddresses.addAll(listener.getUrlParser().getHostAddresses());
          host = loopAddresses.pollFirst();
        }
        logger.debug("Connect to " + host + ", RetryAllDowns=" + maxConnectionTry);
        attemptedTimes ++;
        connectedHosts.add(host);
        protocol.setHostAddress(host);
        protocol.connect();
        if (listener.isExplicitClosed()) {
          protocol.close();
          return;
        }

        listener.removeFromBlacklist(protocol.getHostAddress());
        listener.removeFromPickedList(protocol.getHostAddress());
        listener.foundActiveMaster(protocol);
        return;
      } catch (IOException ioException) {
          long failedTimeMs = System.currentTimeMillis();
          HostAddress host = protocol.getHostAddress();
          logger.debug("Failed to connect {}", host);
          if(listener.getBlacklistKeys().contains(host) && listener.getBlacklist().get(host).getState() == HostStateInfo.STATE.GREY) { // in blackList but the host is grey ,reset its timeout
              listener.resetHostStateInfo(host);
              continue;
          }

          BlackListConfig blackListConfig = listener.getCurrentLoadBalanceInfo().getBlackListConfig();
          Properties info = new Properties();
          info.setProperty(Consts.FAILED_TIME_MS, String.valueOf(failedTimeMs));
          if (blackListConfig.getAppendStrategy().needToAppend(host, info)) {
              listener.addToBlacklist(host);
              lastQueryException = ExceptionFactory.INSTANCE.create(String.format("Could not connect to %s. %s", protocol.getHostAddress(), ioException.getMessage()), "08000", ioException);
          } else {
              listener.addToPickedList(host);
              if(!fallThrough) {
                  loopAddresses.add(host);
              }
          }
      } catch (SQLException businessException) {
          throw businessException;
      } finally {
          listener.setRetryAllDowns(maxConnectionTry);
      }
    }

    if (lastQueryException != null) {
      throw new SQLException(
          "No active connection found for master : " + lastQueryException.getMessage(),
          lastQueryException.getSQLState(),
          lastQueryException.getErrorCode(),
          lastQueryException);
    }

    throw new SQLException("No active connection found for master. " +
            "\nretryAllDowns remains " +   maxConnectionTry +
            "\nloopAddresses = " +  loopAddresses +
            "\nattempted hosts:" + connectedHosts +
            "\nattempted times:" + attemptedTimes +
            "\nblacklist hosts:" + listener.getBlacklist()
    );
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
    servers.addAll(listener.getUrlParser().getHostAddresses()); // fixed
    Collections.shuffle(servers);

    loopAddresses.clear();
    loopAddresses.addAll(servers);
  }
}
