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

import java.io.IOException;
import java.net.SocketException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

import com.oceanbase.jdbc.HostAddress;
import com.oceanbase.jdbc.UrlParser;
import com.oceanbase.jdbc.internal.com.read.dao.Results;
import com.oceanbase.jdbc.internal.failover.FailoverProxy;
import com.oceanbase.jdbc.internal.failover.impl.AuroraListener;
import com.oceanbase.jdbc.internal.failover.tools.SearchFilter;
import com.oceanbase.jdbc.internal.io.LruTraceCache;
import com.oceanbase.jdbc.internal.logging.Logger;
import com.oceanbase.jdbc.internal.logging.LoggerFactory;
import com.oceanbase.jdbc.internal.util.SqlStates;
import com.oceanbase.jdbc.internal.util.exceptions.ExceptionFactory;
import com.oceanbase.jdbc.internal.util.pool.GlobalStateInfo;

public class AuroraProtocol extends MastersSlavesProtocol {

    protected static final Logger lockLogger = LoggerFactory.getLogger("JDBC-COST-LOGGER");

    public AuroraProtocol(final UrlParser url, final GlobalStateInfo globalInfo,
                          final ReentrantLock lock, LruTraceCache traceCache) {
        super(url, globalInfo, lock, traceCache);
    }

    /**
     * Connect aurora probable master. Aurora master change in time. The only way to check that a
     * server is a master is to asked him.
     *
     * @param listener aurora failover to call back if master is found
     * @param globalInfo server global variables information
     * @param probableMaster probable master host
     */
    private static void searchProbableMaster(AuroraListener listener,
                                             final GlobalStateInfo globalInfo,
                                             HostAddress probableMaster) {

        AuroraProtocol protocol = getNewProtocol(listener.getProxy(), globalInfo,
            listener.getUrlParser());
        try {

            protocol.setHostAddress(probableMaster);
            protocol.connect();
            listener.removeFromBlacklist(protocol.getHostAddress());

            if (listener.isMasterHostFailReconnect() && protocol.isMasterConnection()) {
                protocol.setMustBeMasterConnection(true);
                listener.foundActiveMaster(protocol);
            } else if (listener.isSecondaryHostFailReconnect() && !protocol.isMasterConnection()) {
                protocol.setMustBeMasterConnection(false);
                listener.foundActiveSecondary(protocol);
            } else {
                protocol.close();
                protocol = getNewProtocol(listener.getProxy(), globalInfo, listener.getUrlParser());
            }

        } catch (SQLException | IOException e) {
            listener.addToBlacklist(protocol.getHostAddress());
        }
    }

    /**
     * loop until found the failed connection.
     *
     * @param listener current failover
     * @param globalInfo server global variables information
     * @param addresses list of HostAddress to loop
     * @param initialSearchFilter search parameter
     * @throws SQLException if not found
     */
    public static void loop(
      AuroraListener listener,
      final GlobalStateInfo globalInfo,
      final List<HostAddress> addresses,
      SearchFilter initialSearchFilter)
      throws SQLException {

    SearchFilter searchFilter = initialSearchFilter;
    AuroraProtocol protocol;
    Deque<HostAddress> loopAddresses = new ArrayDeque<>(addresses);
    if (loopAddresses.isEmpty()) {
      resetHostList(listener, loopAddresses);
    }

    int maxConnectionTry = listener.getRetriesAllDown();
    SQLException lastQueryException = null;
    HostAddress probableMasterHost = null;
    boolean firstLoop = true;
    while (!loopAddresses.isEmpty() || (!searchFilter.isFailoverLoop() && maxConnectionTry > 0)) {
      protocol = getNewProtocol(listener.getProxy(), globalInfo, listener.getUrlParser());

      if (listener.isExplicitClosed()
          || (!listener.isSecondaryHostFailReconnect() && !listener.isMasterHostFailReconnect())) {
        return;
      }
      maxConnectionTry--;

      try {
        HostAddress host = loopAddresses.pollFirst();
        if (host == null) {
          for (HostAddress hostAddress : listener.getUrlParser().getHostAddresses()) {
            if (!hostAddress.equals(listener.getClusterHostAddress())) {
              loopAddresses.add(hostAddress);
            }
          }
          // Use cluster last as backup
          if (listener.getClusterHostAddress() != null
              && (listener.getUrlParser().getHostAddresses().size() < 2
                  || loopAddresses.isEmpty())) {
            loopAddresses.add(listener.getClusterHostAddress());
          }

          host = loopAddresses.pollFirst();
        }
        protocol.setHostAddress(host);
        protocol.connect();

        if (listener.isExplicitClosed()) {
          protocol.close();
          return;
        }

        listener.removeFromBlacklist(protocol.getHostAddress());

        if (listener.isMasterHostFailReconnect() && protocol.isMasterConnection()) {
          // Look for secondary when only known endpoint is the cluster endpoint
          if (searchFilter.isFineIfFoundOnlyMaster()
              && listener.getUrlParser().getHostAddresses().size() <= 1
              && protocol.getHostAddress().equals(listener.getClusterHostAddress())) {
            listener.retrieveAllEndpointsAndSet(protocol);

            if (listener.getUrlParser().getHostAddresses().size() > 1) {
              // add newly discovered end-point to loop
              loopAddresses.addAll(listener.getUrlParser().getHostAddresses());
              // since there is more than one end point, reactivate connection to a read-only host
              searchFilter = new SearchFilter(false);
            }
          }

          if (foundMaster(listener, protocol, searchFilter)) {
            return;
          }

        } else if (!protocol.isMasterConnection()) {
          if (listener.isSecondaryHostFailReconnect()) {
            // in case cluster DNS is currently pointing to a slave host
            if (listener.getUrlParser().getHostAddresses().size() <= 1
                && protocol.getHostAddress().equals(listener.getClusterHostAddress())) {
              listener.retrieveAllEndpointsAndSet(protocol);

              if (listener.getUrlParser().getHostAddresses().size() > 1) {
                // add newly discovered end-point to loop
                loopAddresses.addAll(listener.getUrlParser().getHostAddresses());
                // since there is more than one end point, reactivate connection to a read-only host
                searchFilter = new SearchFilter(false);
              }
            } else {
              if (foundSecondary(listener, protocol, searchFilter)) {
                return;
              }
            }

          } else {
            try {
              if (listener.isSecondaryHostFailReconnect()
                  || (listener.isMasterHostFailReconnect() && probableMasterHost == null)) {
                probableMasterHost =
                    listener.searchByStartName(
                        protocol, listener.getUrlParser().getHostAddresses());
                if (probableMasterHost != null) {
                  loopAddresses.remove(probableMasterHost);
                  AuroraProtocol.searchProbableMaster(listener, globalInfo, probableMasterHost);
                  if (listener.isMasterHostFailReconnect()
                      && searchFilter.isFineIfFoundOnlySlave()) {
                    return;
                  }
                }
              }
            } finally {
              protocol.close();
            }
          }
        } else {
          protocol.close();
        }

      } catch (IOException e) {
          lastQueryException = ExceptionFactory.INSTANCE.create(String.format("Could not connect to %s. %s", protocol.getHostAddress(), e.getMessage()), "08000", e);
          listener.addToBlacklist(protocol.getHostAddress());
      } catch (SQLException e) {
        lastQueryException = e;
        listener.addToBlacklist(protocol.getHostAddress());
      }

      if (!listener.isMasterHostFailReconnect() && !listener.isSecondaryHostFailReconnect()) {
        return;
      }

      // in case master not found but slave is , and allowing master down
      if (loopAddresses.isEmpty()
          && (listener.isMasterHostFailReconnect()
              && listener.urlParser.getOptions().allowMasterDownConnection
              && !listener.isSecondaryHostFailReconnect())) {
        return;
      }

      // on connection and all slaves have been tested, use master if on
      if (loopAddresses.isEmpty()
          && searchFilter.isInitialConnection()
          && !listener.isMasterHostFailReconnect()) {
        return;
      }

      // if server has try to connect to all host, and there is remaining master or slave that fail
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

      // Try to connect to the cluster if no other connection is good
      if (maxConnectionTry == 0
          && !loopAddresses.contains(listener.getClusterHostAddress())
          && listener.getClusterHostAddress() != null) {
        loopAddresses.add(listener.getClusterHostAddress());
      }
    }

    if (listener.isMasterHostFailReconnect() || listener.isSecondaryHostFailReconnect()) {
      String error = "No active connection found for replica";
      if (listener.isMasterHostFailReconnect()) {
        error = "No active connection found for master";
      }
      if (lastQueryException != null) {
        throw new SQLException(
            error,
            lastQueryException.getSQLState(),
            lastQueryException.getErrorCode(),
            lastQueryException);
      }
      throw new SQLException(error);
    }
  }

    /**
     * Reinitialize loopAddresses with all hosts : all servers in randomize order with cluster
     * address. If there is an active connection, connected host are remove from list.
     *
     * @param listener current listener
     * @param loopAddresses the list to reinitialize
     */
    private static void resetHostList(AuroraListener listener, Deque<HostAddress> loopAddresses) {
    // if all servers have been connected without result
    // add back all servers
    List<HostAddress> servers = new ArrayList<>();
    servers.addAll(listener.getUrlParser().getHostAddresses());

    Collections.shuffle(servers);

    // if cluster host is set, add it to the end of the list
    if (listener.getClusterHostAddress() != null
        && listener.getUrlParser().getHostAddresses().size() < 2) {
      servers.add(listener.getClusterHostAddress());
    }

    // remove current connected hosts to avoid reconnect them
    servers.removeAll(listener.connectedHosts());

    loopAddresses.clear();
    loopAddresses.addAll(servers);
  }

    /**
     * Initialize new protocol instance.
     *
     * @param proxy proxy
     * @param globalInfo server global variables information
     * @param urlParser connection string data's
     * @return new AuroraProtocol
     */
    public static AuroraProtocol getNewProtocol(FailoverProxy proxy,
                                                final GlobalStateInfo globalInfo,
                                                UrlParser urlParser) {
        AuroraProtocol newProtocol = new AuroraProtocol(urlParser, globalInfo, proxy.lock,
            proxy.traceCache);
        newProtocol.setProxy(proxy);
        return newProtocol;
    }

    @Override
    public boolean isMasterConnection() {
        return this.masterConnection;
    }

    @Override
    public void readPipelineCheckMaster() throws SQLException {
        Results results = new Results();
        getResult(results);
        results.commandEnd();
        ResultSet resultSet = results.getResultSet();

        this.masterConnection = !resultSet.next()
                                || (this.masterConnection = (0 == resultSet.getInt(1)));
        reader.setServerThreadId(this.serverThreadId, this.masterConnection);
        writer.setServerThreadId(this.serverThreadId, this.masterConnection);
        // Aurora replicas have read-only flag forced
        this.readOnly = !this.masterConnection;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        int initialTimeout = -1;
        try {
            initialTimeout = socket.getSoTimeout();
            this.socket.setSoTimeout(timeout);

            if (isMasterConnection()) {
                return checkIfMaster();
            }
            return ping();

        } catch (SocketException socketException) {
            throw new SQLException("Could not valid connection : " + socketException.getMessage(),
                SqlStates.CONNECTION_EXCEPTION.getSqlState(), socketException);
        } finally {

            // set back initial socket timeout
            try {
                if (initialTimeout != -1) {
                    socket.setSoTimeout(initialTimeout);
                }
            } catch (SocketException socketException) {
                // eat
            }
        }
    }

    /**
     * Aurora best way to check if a node is a master : is not in read-only mode.
     *
     * @return indicate if master has been found
     */
    @Override
    public boolean checkIfMaster() throws SQLException {
        ReentrantLock curLock = proxy.lock;
        curLock.lock();
        try {
            lockLogger.debug("AuroraProtocol.checkIfMaster locked");

            Results results = new Results();
            executeQuery(this.isMasterConnection(), results, "select @@innodb_read_only");
            results.commandEnd();
            ResultSet queryResult = results.getResultSet();
            if (queryResult != null && queryResult.next()) {
                this.masterConnection = (0 == queryResult.getInt(1));

                reader.setServerThreadId(this.serverThreadId, this.masterConnection);
                writer.setServerThreadId(this.serverThreadId, this.masterConnection);
            } else {
                this.masterConnection = true;
            }

            this.readOnly = !this.masterConnection;
            return this.masterConnection;

        } catch (SQLException sqle) {
            throw new SQLException("could not check the 'innodb_read_only' variable status on "
                                   + this.getHostAddress() + " : " + sqle.getMessage(),
                SqlStates.CONNECTION_EXCEPTION.getSqlState(), sqle);
        } finally {
            curLock.unlock();
            lockLogger.debug("AuroraProtocol.checkIfMaster unlocked");
        }
    }
}
