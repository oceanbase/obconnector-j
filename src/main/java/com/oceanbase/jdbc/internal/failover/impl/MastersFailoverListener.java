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
package com.oceanbase.jdbc.internal.failover.impl;

import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import com.oceanbase.jdbc.HostAddress;
import com.oceanbase.jdbc.UrlParser;
import com.oceanbase.jdbc.internal.failover.AbstractMastersListener;
import com.oceanbase.jdbc.internal.failover.HandleErrorResult;
import com.oceanbase.jdbc.internal.failover.thread.FailoverLoop;
import com.oceanbase.jdbc.internal.failover.tools.SearchFilter;
import com.oceanbase.jdbc.internal.failover.utils.ConfigParser;
import com.oceanbase.jdbc.internal.logging.Logger;
import com.oceanbase.jdbc.internal.logging.LoggerFactory;
import com.oceanbase.jdbc.internal.protocol.MasterProtocol;
import com.oceanbase.jdbc.internal.protocol.Protocol;
import com.oceanbase.jdbc.internal.util.constant.HaMode;
import com.oceanbase.jdbc.internal.util.dao.ReconnectDuringTransactionException;
import com.oceanbase.jdbc.internal.util.dao.ServerPrepareResult;
import com.oceanbase.jdbc.internal.util.pool.GlobalStateInfo;

public class MastersFailoverListener extends AbstractMastersListener {

    private static final Logger logger = LoggerFactory.getLogger(MastersFailoverListener.class);
    private final HaMode        mode;
    private long                ocpTimestamp;

    /**
     * Initialisation.
     *
     * @param urlParser url options.
     * @param globalInfo server global variables information
     */
    public MastersFailoverListener(final UrlParser urlParser, final GlobalStateInfo globalInfo) {
        super(urlParser, globalInfo);
        this.mode = urlParser.getHaMode();
        setMasterHostFail();
    }

    /**
     * Connect to database.
     *
     * @throws SQLException if connection is on error.
     */
    @Override
    public void initializeConnection() throws SQLException {
        super.initializeConnection();
        this.currentProtocol = null;
        // launching initial loop
        reconnectFailedConnection(new SearchFilter(true, false));
        resetMasterFailoverData();
    }

    /**
     * Before executing query, reconnect if connection is closed, and autoReconnect option is set.
     *
     * @throws SQLException if connection has been explicitly closed.
     */
    public void preExecute() throws SQLException {
        lastQueryNanos = System.nanoTime();
        // if connection is closed or failed on slave
        if (this.currentProtocol != null && this.currentProtocol.isClosed()) {
            preAutoReconnect();
        }
    }

    @Override
    public void preClose() {
        if (explicitClosed.compareAndSet(false, true)) {
            proxy.lock.lock();
            try {
                lockLogger.debug("MastersFailoverListener.preClose locked");
                removeListenerFromSchedulers();
                closeConnection(currentProtocol);
            } finally {
                proxy.lock.unlock();
                lockLogger.debug("MastersFailoverListener.preClose unlocked");
            }
        }
    }

    public long getServerThreadId() {
        return currentProtocol.getServerThreadId();
    }

    @Override
    public void preAbort() {
        if (explicitClosed.compareAndSet(false, true)) {
            proxy.lock.lock();
            try {
                lockLogger.debug("MastersFailoverListener.preAbort locked");
                removeListenerFromSchedulers();
                abortConnection(currentProtocol);
            } finally {
                proxy.lock.unlock();
                lockLogger.debug("MastersFailoverListener.preAbort unlocked");
            }
        }
    }

    @Override
    public HandleErrorResult primaryFail(Method method, Object[] args, boolean killCmd,
                                         boolean alreadyClosed) {
        boolean inTransaction = currentProtocol != null && currentProtocol.inTransaction();

        if (currentProtocol.isConnected()) {
            currentProtocol.close();
        }

        try {
            reconnectFailedConnection(new SearchFilter(true, false));
            handleFailLoop();

            if (killCmd) {
                return new HandleErrorResult(true, false);
            }

            if (alreadyClosed || !inTransaction && isQueryRelaunchable(method, args)) {
                logger
                    .info(
                        "Connection to master lost, new master {} found"
                                + ", query type permit to be re-execute on new server without throwing exception",
                        currentProtocol.getHostAddress());
                return relaunchOperation(method, args);
            }
            return new HandleErrorResult(true);
        } catch (Exception e) {
            // we will throw a Connection exception that will close connection
            if (e.getCause() != null && proxy.hasToHandleFailover((SQLException) e.getCause())
                && currentProtocol.isConnected()) {
                currentProtocol.close();
            }
            FailoverLoop.removeListener(this);
            return new HandleErrorResult();
        }
    }

    /**
     * Loop to connect failed hosts.
     *
     * @param searchFilter search parameters.
     * @throws SQLException if there is any error during reconnection
     */
    @Override
  public void reconnectFailedConnection(SearchFilter searchFilter) throws SQLException {
    proxy.lock.lock();
    try {
      lockLogger.debug("MastersFailoverListener.reconnectFailedConnection locked");

      if (!searchFilter.isInitialConnection() && (isExplicitClosed() || !isMasterHostFail())) {
        return;
      }
      currentConnectionAttempts.incrementAndGet();
      List<HostAddress> loopAddress  = null;
      if(urlParser.getHostAddresses() != null) {
          loopAddress = new LinkedList<>(urlParser.getHostAddresses());
      }
      if (HaMode.LOADBALANCE.equals(mode)) {
          logger.trace("LoadBalance on!");

          LoadBalanceInfo loadBalanceInfo = null;
          try {
              loadBalanceInfo = getGlobalLoadBalanceInfo(urlParser);
          } catch (IOException e) {
              throw new RuntimeException(e);
          }
          if (loadBalanceInfo == null) {
              StringBuilder stringBuilder = new StringBuilder("LoadBalance config error ");
              if(urlParser.getExtendDescription() != null) {
                  stringBuilder.append("extend description error :");
                  stringBuilder.append(urlParser.getExtendDescription());
                  stringBuilder.append("\n");
              } else if (urlParser.getOcpApi() != null) {
                  stringBuilder.append("OCP API error :");
                  stringBuilder.append(urlParser.getOcpApi());
                  stringBuilder.append("\n");
              } else {
                  stringBuilder.append(" net service name error :");
                  stringBuilder.append(urlParser.getTnsServiceName());
                  stringBuilder.append("\n");
              }
              logger.error(stringBuilder.toString());
              throw new SQLException(stringBuilder.toString());
          }
          logger.trace("load balance info =" + loadBalanceInfo.toString());
          LoadBalanceDriver loadBalanceDriver = new LoadBalanceDriver(loadBalanceInfo);
          if (urlParser.getOcpApi() == null) {
              loadBalanceDriver.construct();
          }
          this.setCurrentLoadBalanceInfo(loadBalanceInfo);
          this.setRetryAllDowns(loadBalanceInfo.getRetryAllDowns());
          loadBalanceDriver.loop(urlParser,this,globalInfo,searchFilter,getBlacklist(),getPickedlist());
      }
      else {
        // order in sequence
        loopAddress.removeAll(getBlacklistKeys());
        loopAddress.addAll(getBlacklistKeys());
        loopAddress.retainAll(urlParser.getHostAddresses());
        // put connected at end
        if (currentProtocol != null && !isMasterHostFail()) {
          loopAddress.remove(currentProtocol.getHostAddress());
          // loopAddress.add(currentProtocol.getHostAddress());
        }
        MasterProtocol.loop(this, globalInfo, loopAddress);
      }
      // close loop if all connection are retrieved
      if (!isMasterHostFail()) {
        FailoverLoop.removeListener(this);
      }
      // if no error, reset failover variables
      resetMasterFailoverData();
    } finally {
      proxy.lock.unlock();
      lockLogger.debug("MastersFailoverListener.reconnectFailedConnection unlocked");
    }
  }

    /**
     * Obtain the current LoadBalance configuration information in different ways, add other
     * mechanisms here
     * @param urlParser oceanbase url parser ,get options from here
     * @return the loadbalance config information
     */
    private LoadBalanceInfo getGlobalLoadBalanceInfo(UrlParser urlParser) throws SQLException,
                                                                         IOException {
        if (urlParser.getTnsServiceName() != null) {
            return ConfigParser.getLoadBalanceInfoFromTns(urlParser);
        } else if (urlParser.getExtendDescription() != null) {
            return ConfigParser.getLoadBalanceInfoFromExtendDescription(urlParser);
        } else if (urlParser.getOcpApi() != null) {
            return ConfigParser.getLoadBalanceInfoFromOcpApi(urlParser.getOcpApi(),
                urlParser.getOptions());
        } else {
            return ConfigParser.getLoadBalanceInfoByDefault(urlParser);
        }
    }

    /**
     * Force session to read-only according to options.
     *
     * @param mustBeReadOnly is read-only flag
     * @throws SQLException if a connection error occur
     */
    public void switchReadOnlyConnection(Boolean mustBeReadOnly) throws SQLException {
        if (urlParser.getOptions().assureReadOnly && currentReadOnlyAsked != mustBeReadOnly) {
            proxy.lock.lock();
            try {
                lockLogger.debug("MastersFailoverListener.switchReadOnlyConnection locked");
                // verify not updated now that hold lock, double check safe due to volatile
                if (currentReadOnlyAsked != mustBeReadOnly) {
                    currentReadOnlyAsked = mustBeReadOnly;
                    setSessionReadOnly(mustBeReadOnly, currentProtocol);
                }
            } finally {
                proxy.lock.unlock();
                lockLogger.debug("MastersFailoverListener.switchReadOnlyConnection unlocked");
            }
        }
    }

    /**
     * method called when a new Master connection is found after a fallback.
     *
     * @param protocol the new active connection
     */
    @Override
    public void foundActiveMaster(Protocol protocol) throws SQLException {
        if (isExplicitClosed()) {
            proxy.lock.lock();
            try {
                lockLogger.debug("MastersFailoverListener.foundActiveMaster isExplicitClosed locked");
                protocol.close();
            } finally {
                proxy.lock.unlock();
                lockLogger.debug("MastersFailoverListener.foundActiveMaster isExplicitClosed unlocked");
            }
            return;
        }
        syncConnection(this.currentProtocol, protocol);
        proxy.lock.lock();
        try {
            lockLogger.debug("MastersFailoverListener.foundActiveMaster !isExplicitClosed locked");
            if (currentProtocol != null && !currentProtocol.isClosed()) {
                currentProtocol.close();
            }
            currentProtocol = protocol;
        } finally {
            proxy.lock.unlock();
            lockLogger.debug("MastersFailoverListener.foundActiveMaster !isExplicitClosed unlocked");
        }

        resetMasterFailoverData();
        FailoverLoop.removeListener(this);
    }

    /**
     * Try to reconnect connection.
     *
     * @throws SQLException if reconnect a new connection but there was an active transaction.
     */
    public void reconnect() throws SQLException {
        boolean inTransaction = currentProtocol != null && currentProtocol.inTransaction();
        reconnectFailedConnection(new SearchFilter(true, false));
        handleFailLoop();
        if (inTransaction) {
            throw new ReconnectDuringTransactionException(
                "Connection reconnect automatically during an active transaction", 1401, "25S03");
        }
    }

    /**
     * Add listener to FailoverLoop if master connection is not active, so a reconnection will be
     * done. (the reconnection will be done by failover or if append before by the next query/method
     * that will use the failed connection) Remove listener from FailoverLoop is master connection is
     * active.
     */
    public void handleFailLoop() {
        if (isMasterHostFail()) {
            if (!isExplicitClosed()) {
                FailoverLoop.addListener(this);
            }
        } else {
            FailoverLoop.removeListener(this);
        }
    }

    public boolean isMasterConnected() {
        return currentProtocol != null && currentProtocol.isConnected();
    }

    /**
     * Check master status.
     *
     * @param searchFilter search filter
     * @return has some status changed
     */
    public boolean checkMasterStatus(SearchFilter searchFilter) {
        if (currentProtocol != null) {
            pingMasterProtocol(currentProtocol);
        }
        return false;
    }

    public void rePrepareOnSlave(ServerPrepareResult oldServerPrepareResult,
                                 boolean mustExecuteOnSlave) {
        // no slave
    }

    /**
     * Reset state of master connection.
     *
     * @throws SQLException if command fail.
     */
    public void reset() throws SQLException {

        if (!isMasterHostFail()) {
            currentProtocol.reset();
        }
    }

}
