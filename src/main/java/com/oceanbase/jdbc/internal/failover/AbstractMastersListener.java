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
package com.oceanbase.jdbc.internal.failover;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.SocketException;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import com.oceanbase.jdbc.HostAddress;
import com.oceanbase.jdbc.OceanBaseConnection;
import com.oceanbase.jdbc.OceanBaseStatement;
import com.oceanbase.jdbc.UrlParser;
import com.oceanbase.jdbc.internal.failover.BlackList.recover.RemoveStrategy;
import com.oceanbase.jdbc.internal.failover.BlackList.recover.TimeoutRecover;
import com.oceanbase.jdbc.internal.failover.impl.LoadBalanceInfo;
import com.oceanbase.jdbc.internal.failover.thread.ConnectionValidator;
import com.oceanbase.jdbc.internal.failover.tools.SearchFilter;
import com.oceanbase.jdbc.internal.failover.utils.Consts;
import com.oceanbase.jdbc.internal.failover.utils.HostStateInfo;
import com.oceanbase.jdbc.internal.logging.Logger;
import com.oceanbase.jdbc.internal.logging.LoggerFactory;
import com.oceanbase.jdbc.internal.protocol.Protocol;
import com.oceanbase.jdbc.internal.util.SqlStates;
import com.oceanbase.jdbc.internal.util.dao.ClientPrepareResult;
import com.oceanbase.jdbc.internal.util.dao.ServerPrepareResult;
import com.oceanbase.jdbc.internal.util.pool.GlobalStateInfo;

public abstract class AbstractMastersListener implements Listener {

  /** List the recent failedConnection. */

  private static final ConcurrentMap<HostAddress, HostStateInfo> blacklist = new ConcurrentHashMap<>();
  private static final Set<HostAddress> pickedList = Collections.synchronizedSet(new HashSet<>());
  private static final ConnectionValidator connectionValidationLoop = new ConnectionValidator();
  private static final Logger logger = LoggerFactory.getLogger(AbstractMastersListener.class);

  /* =========================== Failover variables ========================================= */
  public final UrlParser urlParser;
  protected final AtomicInteger currentConnectionAttempts = new AtomicInteger();
  protected final AtomicBoolean explicitClosed = new AtomicBoolean(false);
  protected final GlobalStateInfo globalInfo;
  private final AtomicBoolean masterHostFail = new AtomicBoolean();
  // currentReadOnlyAsked is volatile so can be queried without lock, but can only be updated when
  // proxy.lock is locked
  protected volatile boolean currentReadOnlyAsked = false;
  protected Protocol currentProtocol = null;
  protected FailoverProxy proxy;
  protected long lastRetry = 0;
  protected long lastQueryNanos = 0;
  private volatile long masterHostFailNanos = 0;

  private LoadBalanceInfo currentLBinfo;

  private int retryAllDowns;
  protected static final Logger lockLogger = LoggerFactory.getLogger("JDBC-COST-LOGGER");


  @Override
  public void setCurrentLoadBalanceInfo(LoadBalanceInfo loadBalanceInfo) {
    currentLBinfo = loadBalanceInfo;
  }

  @Override
  public LoadBalanceInfo getCurrentLoadBalanceInfo() {
    return currentLBinfo;
  }

    @Override
    public void setRetryAllDowns(int retryAllDowns) {
        this.retryAllDowns = retryAllDowns;
    }

    @Override
    public int getRetryAllDowns() {
        return this.retryAllDowns;
    }
  protected AbstractMastersListener(UrlParser urlParser, final GlobalStateInfo globalInfo) {
    this.urlParser = urlParser;
    this.globalInfo = globalInfo;
    this.masterHostFail.set(true);
    this.lastQueryNanos = System.nanoTime();
  }

  /** Clear blacklist data. */
  public static void clearBlacklist() {
      logger.debug("Clear black list.");
      blacklist.clear();
  }

  /**
   * Initialize Listener. This listener will be added to the connection validation loop according to
   * option value so the connection will be verified periodically. (Important for aurora, for other,
   * connection pool often have this functionality)
   *
   * @throws SQLException if any exception occur.
   */
  public void initializeConnection() throws SQLException {
    long connectionTimeoutMillis =
        TimeUnit.SECONDS.toMillis(urlParser.getOptions().validConnectionTimeout);
    lastQueryNanos = System.nanoTime();
    if (connectionTimeoutMillis > 0) {
      connectionValidationLoop.addListener(this, connectionTimeoutMillis);
    }
  }

  protected void removeListenerFromSchedulers() {
    connectionValidationLoop.removeListener(this);
  }

  protected void preAutoReconnect() throws SQLException {
    if (!isExplicitClosed()) {
      try {
        // save to local value in case updated while constructing SearchFilter
        boolean currentReadOnlyAsked = this.currentReadOnlyAsked;
        reconnectFailedConnection(new SearchFilter(!currentReadOnlyAsked, currentReadOnlyAsked));
      } catch (SQLException e) {
        // eat exception
      }
      handleFailLoop();
    } else {
      throw new SQLException("Connection is closed", SqlStates.CONNECTION_EXCEPTION.getSqlState());
    }
  }

  public FailoverProxy getProxy() {
    return this.proxy;
  }

  public void setProxy(FailoverProxy proxy) {
    this.proxy = proxy;
  }

    public Set<HostAddress> getPickedlist() {
        return pickedList;
    }

    /**
     * @param hostAddress the HostAddress has failed to connect but not triggered the blacklist AppendStrategy
     */
    public void addToPickedList(HostAddress hostAddress) {
        if (hostAddress != null && !isExplicitClosed()) {
            pickedList.add(hostAddress);
        }
    }

    public void removeFromPickedList(HostAddress hostAddress) {
        if (hostAddress != null) {
            pickedList.remove(hostAddress);
        }
    }

  public Set<HostAddress> getBlacklistKeys() {
    return blacklist.keySet();
  }

  public  ConcurrentMap<HostAddress, HostStateInfo> getBlacklist() {
    return blacklist;
  }

  /**
   * Call when a failover is detected on master connection. Will :
   *
   * <ol>
   *   <li>set fail variable
   *   <li>try to reconnect
   *   <li>relaunch query if possible
   * </ol>
   *
   * @param method called method
   * @param args methods parameters
   * @param protocol current protocol
   * @return a HandleErrorResult object to indicate if query has been relaunched, and the exception
   *     if not
   * @throws SQLException when method and parameters does not exist.
   */
  public HandleErrorResult handleFailover(
      SQLException qe, Method method, Object[] args, Protocol protocol, boolean isClosed)
      throws SQLException {
    if (isExplicitClosed()) {
      throw new SQLException("Connection has been closed !");
    }
    if (setMasterHostFail()) {
      logger.warn(
          "SQL Primary node [{}, conn={}, local_port={}, timeout={}] connection fail. Reason : {}",
          this.currentProtocol.getHostAddress().toString(),
          this.currentProtocol.getServerThreadId(),
          this.currentProtocol.getSocket().getLocalPort(),
          this.currentProtocol.getTimeout(),
          qe.getMessage());
      addToBlacklist(currentProtocol.getHostAddress());
    }
    if(protocol.getOptions().loadBalanceHandleFailover) {
        // check that failover is due to kill command
        boolean killCmd =
                qe != null
                        && qe.getSQLState() != null
                        && qe.getSQLState().equals("70100")
                        && 1927 == qe.getErrorCode();

        HandleErrorResult handleErrorResult = primaryFail(method, args, killCmd, isClosed);
        this.currentProtocol.setAutoCommit(protocol.getAutocommit());
        return handleErrorResult;
    } else {
        return new HandleErrorResult(false,true);
    }
  }

  /**
   * After a failover, put the hostAddress in a static list so the other connection will not take
   * this host in account for a time.
   *
   * @param hostAddress the HostAddress to add to blacklist
   */
  public void addToBlacklist(HostAddress hostAddress) {
    if (hostAddress != null && !isExplicitClosed()) {
      // todo: adding to the blacklist could be reflected in log module of slf4j framework
      logger.debug("Add into black list : " + hostAddress);
      blacklist.putIfAbsent(hostAddress, new HostStateInfo());
      pickedList.remove(hostAddress);
    }
  }

  /**
   * After a successfull connection, permit to remove a hostAddress from blacklist.
   *
   * @param hostAddress the host address which is going to be removed from blacklist
   */
  public void removeFromBlacklist(HostAddress hostAddress) {
    if (hostAddress != null) {
      logger.debug("Remove from black list : " + hostAddress);
      blacklist.remove(hostAddress);
    }
  }

    public void resetHostStateInfo(HostAddress hostAddress) {
        long timeout;
        RemoveStrategy removeStrategy = getCurrentLoadBalanceInfo().getBlackListConfig().getRemoveStrategy();
        if (removeStrategy != null) {
            timeout = ((TimeoutRecover)removeStrategy).getTimeout();
        } else {
            timeout = Long.parseLong(getCurrentLoadBalanceInfo().getBlackListConfig().getRemoveStrategyConfigs().get(Consts.TIMEOUT_MS));
        }
        HostStateInfo hostStateInfo = new HostStateInfo(HostStateInfo.STATE.BLACK,System.nanoTime() + timeout);
        if (hostAddress != null) {
            blacklist.put(hostAddress,hostStateInfo);
        }
    }

  /** Permit to remove Host to blacklist after loadBalanceBlacklistTimeout seconds. */
  public void resetOldsBlackListHosts() {
    Set<Map.Entry<HostAddress, HostStateInfo>> entries = blacklist.entrySet();
    for (Map.Entry<HostAddress, HostStateInfo> blEntry : entries) {
        HostStateInfo hostStateInfo = blEntry.getValue();
        if (getCurrentLoadBalanceInfo().getBlackListConfig().getRemoveStrategy().needToChangeStateInfo(hostStateInfo)) {
          hostStateInfo.setState(HostStateInfo.STATE.GREY);
//        blacklist.remove(blEntry.getKey(), hostStateInfo);  // not remove
       }
    }
  }



  public void resetMasterFailoverData() {
    if (masterHostFail.compareAndSet(true, false)) {
      masterHostFailNanos = 0;
    }
  }

  protected void setSessionReadOnly(boolean readOnly, Protocol protocol) throws SQLException {
    if (protocol.versionGreaterOrEqual(5, 6, 5)) {
      logger.info(
          "SQL node [{}, conn={}] is now in {} mode.",
          protocol.getHostAddress().toString(),
          protocol.getServerThreadId(),
          readOnly ? "read-only" : "write");
      protocol.executeQuery("SET SESSION TRANSACTION " + (readOnly ? "READ ONLY" : "READ WRITE"));
    }
  }

  public abstract void handleFailLoop();

  public Protocol getCurrentProtocol() {
    return currentProtocol;
  }

  public long getMasterHostFailNanos() {
    return masterHostFailNanos;
  }

  /**
   * Set master fail variables.
   *
   * @return true if was already failed
   */
  public boolean setMasterHostFail() {
    if (masterHostFail.compareAndSet(false, true)) {
      masterHostFailNanos = System.nanoTime();
      currentConnectionAttempts.set(0);
      return true;
    }
    return false;
  }

  public boolean isMasterHostFail() {
    return masterHostFail.get();
  }

  public boolean hasHostFail() {
    return masterHostFail.get();
  }

  public SearchFilter getFilterForFailedHost() {
    return new SearchFilter(isMasterHostFail(), false);
  }

  /**
   * After a failover that has bean done, relaunch the operation that was in progress. In case of
   * special operation that crash server, doesn't relaunched it;
   *
   * @param method the method accessed
   * @param args the parameters
   * @return An object that indicate the result or that the exception as to be thrown
   * @throws SQLException if there is any error relaunching initial method
   */
  public HandleErrorResult relaunchOperation(Method method, Object[] args) throws SQLException {
    HandleErrorResult handleErrorResult = new HandleErrorResult(true);
    if (method != null) {
      switch (method.getName()) {
        case "executeQuery":
          if (args[2] instanceof String) {
            String query = ((String) args[2]).toUpperCase(Locale.ROOT);
            if (!"ALTER SYSTEM CRASH".equals(query) && !query.startsWith("KILL")) {
              logger.debug(
                  "relaunch query to new connection {}",
                  ((currentProtocol != null)
                      ? "(conn=" + currentProtocol.getServerThreadId() + ")"
                      : ""));
              try {
                handleErrorResult.resultObject = method.invoke(currentProtocol, args);
                handleErrorResult.mustThrowError = false;
              } catch (IllegalAccessException | InvocationTargetException e) {
                throw new SQLException(e.getCause());
              }
            }
          }
          break;

        case "executePreparedQuery":
          // the statementId has been discarded with previous session
          try {
            boolean mustBeOnMaster = (Boolean) args[0];
            ServerPrepareResult oldServerPrepareResult = (ServerPrepareResult) args[1];
            ServerPrepareResult serverPrepareResult =
                currentProtocol.prepare(oldServerPrepareResult.getSql(), mustBeOnMaster);
            oldServerPrepareResult.failover(serverPrepareResult.getStatementId(), currentProtocol);
            logger.debug(
                "relaunch query to new connection "
                    + ((currentProtocol != null)
                        ? "server thread id " + currentProtocol.getServerThreadId()
                        : ""));
            handleErrorResult.resultObject = method.invoke(currentProtocol, args);
            handleErrorResult.mustThrowError = false;
          } catch (Exception e) {
            // if retry prepare fail, discard error. execution error will indicate the error.
          }
          break;

        default:
          try {
            handleErrorResult.resultObject = method.invoke(currentProtocol, args);
            handleErrorResult.mustThrowError = false;
            break;
          } catch (IllegalAccessException | InvocationTargetException e) {
            throw new SQLException(e);
          }
      }
    }
    return handleErrorResult;
  }

  /**
   * Check if query can be re-executed.
   *
   * @param method invoke method
   * @param args invoke arguments
   * @return true if can be re-executed
   */
  public boolean isQueryRelaunchable(Method method, Object[] args) {
    if (method != null) {
      switch (method.getName()) {
        case "executeQuery":
          if (!((Boolean) args[0])) {
            return true; // launched on slave connection
          }
          if (args[2] instanceof String) {
            return ((String) args[2]).toUpperCase(Locale.ROOT).startsWith("SELECT");
          } else if (args[2] instanceof ClientPrepareResult) {
            @SuppressWarnings("unchecked")
            String query =
                new String(((ClientPrepareResult) args[2]).getQueryParts().get(0))
                    .toUpperCase(Locale.ROOT);
            return query.startsWith("SELECT");
          }
          break;
        case "executePreparedQuery":
          if (!((Boolean) args[0])) {
            return true; // launched on slave connection
          }
          ServerPrepareResult serverPrepareResult = (ServerPrepareResult) args[1];
          return (serverPrepareResult.getSql()).toUpperCase(Locale.ROOT).startsWith("SELECT");
        case "executeBatchStmt":
        case "executeBatchClient":
        case "executeBatchServer":
          return !((Boolean) args[0]);
        default:
          return false;
      }
    }
    return false;
  }

  public Object invoke(Method method, Object[] args, Protocol specificProtocol) throws Throwable {
    return method.invoke(specificProtocol, args);
  }

  public Object invoke(Method method, Object[] args) throws Throwable {
    return method.invoke(currentProtocol, args);
  }

  /**
   * When switching between 2 connections, report existing connection parameter to the new used
   * connection.
   *
   * @param from used connection
   * @param to will-be-current connection
   * @throws SQLException if catalog cannot be set
   */
  public void syncConnection(Protocol from, Protocol to) throws SQLException {

    if (from != null) {
      ReentrantLock curLock = proxy.lock;
      curLock.lock();
      try {
        lockLogger.debug("AbstractMastersListener.syncConnection locked");
        to.resetStateAfterFailover(
            from.getMaxRows(),
            from.getTransactionIsolationLevel(),
            from.getDatabase(),
            from.getAutocommit());
      } finally {
        curLock.unlock();
        lockLogger.debug("AbstractMastersListener.syncConnection to unlock");
      }
    }
  }

  public boolean versionGreaterOrEqual(int major, int minor, int patch) {
    return currentProtocol.versionGreaterOrEqual(major, minor, patch);
  }

  public boolean isServerMariaDb() {
    return currentProtocol.isServerMariaDb();
  }

  public boolean sessionStateAware() {
    return currentProtocol.sessionStateAware();
  }

  public boolean noBackslashEscapes() {
    return currentProtocol.noBackslashEscapes();
  }

  public int getMajorServerVersion() {
    return currentProtocol.getMajorServerVersion();
  }

  public boolean isClosed() {
    return currentProtocol.isClosed();
  }

  public boolean isValid(int timeout) throws SQLException {
    return currentProtocol.isValid(timeout);
  }

  public boolean isReadOnly() {
    return currentReadOnlyAsked;
  }

  public boolean inTransaction() {
    return currentProtocol.inTransaction();
  }

  public boolean isMasterConnection() {
    return true;
  }

  public boolean isExplicitClosed() {
    return explicitClosed.get();
  }

  public int getRetriesAllDown() {
    return urlParser.getOptions().retriesAllDown;
  }

  public boolean isAutoReconnect() {
    return urlParser.getOptions().autoReconnect;
  }

  public UrlParser getUrlParser() {
    return urlParser;
  }

  public abstract void preExecute() throws SQLException;

  public abstract void preClose();

  public abstract void reconnectFailedConnection(SearchFilter filter) throws SQLException;

  public abstract void switchReadOnlyConnection(Boolean readonly) throws SQLException;

  public abstract HandleErrorResult primaryFail(
      Method method, Object[] args, boolean killCmd, boolean isClosed) throws SQLException;

  /**
   * Throw a human readable message after a failoverException.
   *
   * @param failHostAddress failedHostAddress
   * @param wasMaster was failed connection master
   * @param queryException internal error
   * @param reconnected connection status
   * @throws SQLException error with failover information
   */
  @Override
  public void throwFailoverMessage(
      HostAddress failHostAddress,
      boolean wasMaster,
      SQLException queryException,
      boolean reconnected)
      throws SQLException {
    String firstPart =
        "Communications link failure with "
            + (wasMaster ? "primary" : "secondary")
            + ((failHostAddress != null)
                ? " host " + failHostAddress.host + ":" + failHostAddress.port
                : "")
            + ". ";
    String error = "";
    if (reconnected) {
      error += " Driver has reconnect connection";
    } else {
      if (currentConnectionAttempts.get() > urlParser.getOptions().retriesAllDown) {
        error +=
            " Driver will not try to reconnect (too much failure > "
                + urlParser.getOptions().retriesAllDown
                + ")";
      }
    }

    String message;
    String sqlState;
    int vendorCode = 0;
    Throwable cause = null;

    if (queryException == null) {
      message = firstPart + error;
      sqlState = SqlStates.CONNECTION_EXCEPTION.getSqlState();
    } else {
      message = firstPart + queryException.getMessage() + ". " + error;
      sqlState = queryException.getSQLState();
      vendorCode = queryException.getErrorCode();
      cause = queryException.getCause();
    }

    if (sqlState != null && sqlState.startsWith("08")) {
      if (reconnected) {
        // change sqlState to "Transaction has been rolled back", to transaction exception, since
        // reconnection has succeed
        sqlState = "25S03";
      } else {
        throw new SQLNonTransientConnectionException(message, sqlState, vendorCode, cause);
      }
    }

    throw new SQLException(message, sqlState, vendorCode, cause);
  }

  public boolean canRetryFailLoop() {
    return currentConnectionAttempts.get() < urlParser.getOptions().failoverLoopRetries;
  }

  public void prolog(long maxRows, OceanBaseConnection connection, OceanBaseStatement statement)
      throws SQLException {
    currentProtocol.prolog(maxRows, true, connection, statement);
  }

  public String getCatalog() throws SQLException {
    return currentProtocol.getCatalog();
  }

  public int getTimeout() throws SocketException {
    return currentProtocol.getTimeout();
  }

  public abstract void reconnect() throws SQLException;

  public abstract boolean checkMasterStatus(SearchFilter searchFilter);

  public long getLastQueryNanos() {
    return lastQueryNanos;
  }

  protected boolean pingMasterProtocol(Protocol protocol) {
    try {
      if (protocol.isValid(1000)) {
        return true;
      }
    } catch (SQLException e) {
      // eat exception
    }

    proxy.lock.lock();
    try {
      protocol.close();
      if (setMasterHostFail()) {
        addToBlacklist(protocol.getHostAddress());
      }
    } finally {
      proxy.lock.unlock();
    }
    return false;
  }

  /**
   * Utility to close existing connection.
   *
   * @param protocol connection to close.
   */
  public void closeConnection(Protocol protocol) {
    if (protocol != null && protocol.isConnected()) {
      protocol.close();
    }
  }

  /**
   * Utility to force close existing connection.
   *
   * @param protocol connection to close.
   */
  public void abortConnection(Protocol protocol) {
    if (protocol != null && protocol.isConnected()) {
      protocol.abort();
    }
  }
}
