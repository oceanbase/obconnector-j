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
package com.oceanbase.jdbc.internal.util.pool;

import java.lang.management.ManagementFactory;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;

import com.oceanbase.jdbc.OceanBaseConnection;
import com.oceanbase.jdbc.OceanBasePooledConnection;
import com.oceanbase.jdbc.UrlParser;
import com.oceanbase.jdbc.internal.logging.Logger;
import com.oceanbase.jdbc.internal.logging.LoggerFactory;
import com.oceanbase.jdbc.internal.protocol.Protocol;
import com.oceanbase.jdbc.internal.util.Utils;
import com.oceanbase.jdbc.internal.util.exceptions.ExceptionFactory;
import com.oceanbase.jdbc.internal.util.scheduler.OceanBaseThreadFactory;
import com.oceanbase.jdbc.util.Options;

public class Pool implements AutoCloseable, PoolMBean {

    private static final Logger                                  logger               = LoggerFactory
                                                                                          .getLogger(Pool.class);

    private static final int                                     POOL_STATE_OK        = 0;
    private static final int                                     POOL_STATE_CLOSING   = 1;

    private final AtomicInteger                                  poolState            = new AtomicInteger();

    private final UrlParser                                      urlParser;
    private final Options                                        options;
    private final AtomicInteger                                  pendingRequestNumber = new AtomicInteger();
    private final AtomicInteger                                  totalConnection      = new AtomicInteger();

    private final LinkedBlockingDeque<OceanBasePooledConnection> idleConnections;
    private final ThreadPoolExecutor                             connectionAppender;
    private final BlockingQueue<Runnable>                        connectionAppenderQueue;

    private final String                                         poolTag;
    private final ScheduledThreadPoolExecutor                    poolExecutor;
    private final ScheduledFuture                                scheduledFuture;
    private GlobalStateInfo                                      globalInfo;

    private int                                                  maxIdleTime;
    private long                                                 timeToConnectNanos;
    private long                                                 connectionTime       = 0;

    /**
     * Create pool from configuration.
     *
     * @param urlParser configuration parser
     * @param poolIndex pool index to permit distinction of thread name
     * @param poolExecutor pools common executor
     */
    public Pool(UrlParser urlParser, int poolIndex, ScheduledThreadPoolExecutor poolExecutor) {

    this.urlParser = urlParser;
    options = urlParser.getOptions();
    this.maxIdleTime = options.maxIdleTime;
    poolTag = generatePoolTag(poolIndex);

    // one thread to add new connection to pool.
    connectionAppenderQueue = new ArrayBlockingQueue<>(options.maxPoolSize);
    connectionAppender =
        new ThreadPoolExecutor(
            1,
            1,
            10,
            TimeUnit.SECONDS,
            connectionAppenderQueue,
            new OceanBaseThreadFactory(poolTag + "-appender"));
    connectionAppender.allowCoreThreadTimeOut(true);
    // create workers, since driver only interact with queue after that (i.e. not using .execute() )
    connectionAppender.prestartCoreThread();

    idleConnections = new LinkedBlockingDeque<>();

    int scheduleDelay = Math.min(30, maxIdleTime / 2);
    this.poolExecutor = poolExecutor;
    scheduledFuture =
        poolExecutor.scheduleAtFixedRate(
            this::removeIdleTimeoutConnection, scheduleDelay, scheduleDelay, TimeUnit.SECONDS);

    if (options.registerJmxPool) {
      try {
        registerJmx();
      } catch (Exception ex) {
        logger.error("pool " + poolTag + " not registered due to exception : " + ex.getMessage());
      }
    }

    // create minimal connection in pool
    try {
      for (int i = 0; i < options.minPoolSize; i++) {
        addConnection();
      }
    } catch (SQLException sqle) {
      logger.error("error initializing pool connection", sqle);
    }
  }

    /**
     * Add new connection if needed. Only one thread create new connection, so new connection request
     * will wait to newly created connection or for a released connection.
     */
    private void addConnectionRequest() {
    if (totalConnection.get() < options.maxPoolSize && poolState.get() == POOL_STATE_OK) {

      // ensure to have one worker if was timeout
      connectionAppender.prestartCoreThread();
      connectionAppenderQueue.offer(
          () -> {
            if ((totalConnection.get() < options.minPoolSize || pendingRequestNumber.get() > 0)
                && totalConnection.get() < options.maxPoolSize) {
              try {
                addConnection();
              } catch (SQLException sqle) {
                // eat
              }
            }
          });
    }
  }

    /**
     * Removing idle connection. Close them and recreate connection to reach minimal number of
     * connection.
     */
    private void removeIdleTimeoutConnection() {

        // descending iterator since first from queue are the first to be used
        Iterator<OceanBasePooledConnection> iterator = idleConnections.descendingIterator();

        OceanBasePooledConnection item;

        while (iterator.hasNext()) {
            item = iterator.next();

            long idleTime = System.nanoTime() - item.getLastUsed().get();
            boolean timedOut = idleTime > TimeUnit.SECONDS.toNanos(maxIdleTime);

            boolean shouldBeReleased = false;

            if (globalInfo != null) {

                // idle time is reaching server @@wait_timeout
                if (idleTime > TimeUnit.SECONDS.toNanos(globalInfo.getWaitTimeout() - 45)) {
                    shouldBeReleased = true;
                }

                //  idle has reach option maxIdleTime value and pool has more connections than minPoolSiz
                if (timedOut && totalConnection.get() > options.minPoolSize) {
                    shouldBeReleased = true;
                }

            } else if (timedOut) {
                shouldBeReleased = true;
            }

            if (shouldBeReleased && idleConnections.remove(item)) {

                totalConnection.decrementAndGet();
                silentCloseConnection(item);
                addConnectionRequest();
                if (logger.isDebugEnabled()) {
                    logger
                        .debug(
                            "pool {} connection removed due to inactivity (total:{}, active:{}, pending:{})",
                            poolTag, totalConnection.get(), getActiveConnections(),
                            pendingRequestNumber.get());
                }
            }
        }
    }

    /**
     * Create new connection.
     *
     * @throws SQLException if connection creation failed
     */
    private void addConnection() throws SQLException {

        // create new connection
        Protocol protocol = Utils.retrieveProxy(urlParser, globalInfo);
        OceanBaseConnection connection = new OceanBaseConnection(protocol);
        OceanBasePooledConnection pooledConnection = createPoolConnection(connection);

        if (options.staticGlobal) {
            // on first connection load initial state
            if (globalInfo == null) {
                initializePoolGlobalState(connection);
            }
            // set default transaction isolation level to permit resetting to initial state
            connection.setDefaultTransactionIsolation(globalInfo.getDefaultTransactionIsolation());
        } else {
            // set default transaction isolation level to permit resetting to initial state
            connection.setDefaultTransactionIsolation(connection.getTransactionIsolation());
        }

        if (poolState.get() == POOL_STATE_OK
            && totalConnection.incrementAndGet() <= options.maxPoolSize) {
            idleConnections.addFirst(pooledConnection);

            if (logger.isDebugEnabled()) {
                logger.debug(
                    "pool {} new physical connection created (total:{}, active:{}, pending:{})",
                    poolTag, totalConnection.get(), getActiveConnections(),
                    pendingRequestNumber.get());
            }
            return;
        }

        silentCloseConnection(pooledConnection);
    }

    private OceanBasePooledConnection getIdleConnection() throws InterruptedException {
        return getIdleConnection(0, TimeUnit.NANOSECONDS);
    }

    /**
     * Get an existing idle connection in pool.
     *
     * @return an IDLE connection.
     */
    private OceanBasePooledConnection getIdleConnection(long timeout, TimeUnit timeUnit)
                                                                                        throws InterruptedException {

        while (true) {
            OceanBasePooledConnection item = (timeout == 0) ? idleConnections.pollFirst()
                : idleConnections.pollFirst(timeout, timeUnit);

            if (item != null) {
                OceanBaseConnection connection = item.getConnection();
                try {
                    if (TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - item.getLastUsed().get()) > options.poolValidMinDelay) {

                        // validate connection
                        if (connection.isValid(10)) { // 10 seconds timeout
                            item.lastUsedToNow();
                            return item;
                        }

                    } else {

                        // connection has been retrieved recently -> skip connection validation
                        item.lastUsedToNow();
                        return item;
                    }

                } catch (SQLException sqle) {
                    // eat
                }

                totalConnection.decrementAndGet();

                // validation failed
                silentAbortConnection(item);
                addConnectionRequest();
                if (logger.isDebugEnabled()) {
                    logger
                        .debug(
                            "pool {} connection removed from pool due to failed validation (total:{}, active:{}, pending:{})",
                            poolTag, totalConnection.get(), getActiveConnections(),
                            pendingRequestNumber.get());
                }
                continue;
            }

            return null;
        }
    }

    private void silentCloseConnection(OceanBasePooledConnection item) {
        try {
            item.close();
        } catch (SQLException ex) {
            // eat exception
        }
    }

    private void silentAbortConnection(OceanBasePooledConnection item) {
        try {
            item.abort(poolExecutor);
        } catch (SQLException ex) {
            // eat exception
        }
    }

    private OceanBasePooledConnection createPoolConnection(OceanBaseConnection connection) {
        OceanBasePooledConnection pooledConnection = new OceanBasePooledConnection(connection);
        pooledConnection.addConnectionEventListener(new ConnectionEventListener() {

            @Override
            public void connectionClosed(ConnectionEvent event) {
                OceanBasePooledConnection item = (OceanBasePooledConnection) event.getSource();
                if (poolState.get() == POOL_STATE_OK) {
                    try {
                        if (!idleConnections.contains(item)) {
                            item.getConnection().reset();
                            idleConnections.addFirst(item);
                        }
                    } catch (SQLException sqle) {

                        // sql exception during reset, removing connection from pool
                        totalConnection.decrementAndGet();
                        silentCloseConnection(item);
                        logger.debug("connection removed from pool {} due to error during reset",
                            poolTag);
                    }
                } else {
                    // pool is closed, should then not be render to pool, but closed.
                    try {
                        item.close();
                    } catch (SQLException sqle) {
                        // eat
                    }
                    totalConnection.decrementAndGet();
                }
            }

            @Override
            public void connectionErrorOccurred(ConnectionEvent event) {

                OceanBasePooledConnection item = ((OceanBasePooledConnection) event.getSource());
                if (idleConnections.remove(item)) {
                    totalConnection.decrementAndGet();
                }
                silentCloseConnection(item);
                addConnectionRequest();
                logger
                    .debug(
                        "connection {} removed from pool {} due to having throw a Connection exception (total:{}, active:{}, pending:{})",
                        item.getConnection().getServerThreadId(), poolTag, totalConnection.get(),
                        getActiveConnections(), pendingRequestNumber.get());
            }
        });
        return pooledConnection;
    }

    public OceanBasePooledConnection getPooledConnection() throws SQLException {

        pendingRequestNumber.incrementAndGet();

        OceanBasePooledConnection pooledConnection;

        try {

            // try to get Idle connection if any (with a very small timeout)
            if ((pooledConnection = getIdleConnection(totalConnection.get() > 4 ? 0 : 50,
                TimeUnit.MICROSECONDS)) != null) {
                return pooledConnection;
            }

            // ask for new connection creation if max is not reached
            addConnectionRequest();

            // try to create new connection if semaphore permit it
            if ((pooledConnection = getIdleConnection(
                TimeUnit.MILLISECONDS.toNanos(options.connectTimeout), TimeUnit.NANOSECONDS)) != null) {
                return pooledConnection;
            }

            throw ExceptionFactory.INSTANCE
                .create(String
                    .format(
                        "No connection available within the specified time (option 'connectTimeout': %s ms)",
                        NumberFormat.getInstance().format(options.connectTimeout)));

        } catch (InterruptedException interrupted) {
            throw ExceptionFactory.INSTANCE.create("Thread was interrupted", "70100", interrupted);
        } finally {
            pendingRequestNumber.decrementAndGet();
        }
    }

    /**
     * Retrieve new connection. If possible return idle connection, if not, stack connection query,
     * ask for a connection creation, and loop until a connection become idle / a new connection is
     * created.
     *
     * @return a connection object
     * @throws SQLException if no connection is created when reaching timeout (connectTimeout option)
     */
    public OceanBaseConnection getConnection() throws SQLException {
        return getPooledConnection().getConnection();
    }

    /**
     * Get new connection from pool if user and password correspond to pool. If username and password
     * are different from pool, will return a dedicated connection.
     *
     * @param username username
     * @param password password
     * @return connection
     * @throws SQLException if any error occur during connection
     */
    public OceanBaseConnection getConnection(String username, String password) throws SQLException {

        try {

            if ((urlParser.getUsername() != null ? urlParser.getUsername().equals(username)
                : username == null)
                && (urlParser.getPassword() != null ? urlParser.getPassword().equals(password)
                    : password == null)) {
                return getConnection();
            }

            UrlParser tmpUrlParser = (UrlParser) urlParser.clone();
            tmpUrlParser.setUsername(username);
            tmpUrlParser.setPassword(password);
            Protocol protocol = Utils.retrieveProxy(tmpUrlParser, globalInfo);
            return new OceanBaseConnection(protocol);

        } catch (CloneNotSupportedException cloneException) {
            // cannot occur
            throw new SQLException("Error getting connection, parameters cannot be cloned",
                cloneException);
        }
    }

    private String generatePoolTag(int poolIndex) {
        if (options.poolName == null) {
            options.poolName = "MariaDB-pool";
        }
        return options.poolName + "-" + poolIndex;
    }

    public UrlParser getUrlParser() {
        return urlParser;
    }

    /**
     * Close pool and underlying connections.
     *
     * @throws InterruptedException if interrupted
     */
    public void close() throws InterruptedException {
    synchronized (this) {
      Pools.remove(this);
      poolState.set(POOL_STATE_CLOSING);
      pendingRequestNumber.set(0);

      scheduledFuture.cancel(false);
      connectionAppender.shutdown();

      try {
        connectionAppender.awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException i) {
        // eat
      }

      if (logger.isInfoEnabled()) {
        logger.info(
            "closing pool {} (total:{}, active:{}, pending:{})",
            poolTag,
            totalConnection.get(),
            getActiveConnections(),
            pendingRequestNumber.get());
      }

      ExecutorService connectionRemover =
          new ThreadPoolExecutor(
              totalConnection.get(),
              options.maxPoolSize,
              10,
              TimeUnit.SECONDS,
              new LinkedBlockingQueue<>(options.maxPoolSize),
              new OceanBaseThreadFactory(poolTag + "-destroyer"));

      // loop for up to 10 seconds to close not used connection
      long start = System.nanoTime();
      do {
        closeAll(connectionRemover, idleConnections);
        if (totalConnection.get() > 0) {
          Thread.sleep(0, 10_00);
        }
      } while (totalConnection.get() > 0
          && TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) < 10);

      // after having wait for 10 seconds, force removal, even if used connections
      if (totalConnection.get() > 0 || idleConnections.isEmpty()) {
        closeAll(connectionRemover, idleConnections);
      }

      connectionRemover.shutdown();
      try {
        unRegisterJmx();
      } catch (Exception exception) {
        // eat
      }
      connectionRemover.awaitTermination(10, TimeUnit.SECONDS);
    }
  }

    private void closeAll(ExecutorService connectionRemover,
                          Collection<OceanBasePooledConnection> collection) {
        synchronized (collection) { // synchronized mandatory to iterate Collections.synchronizedList()
            for (OceanBasePooledConnection item : collection) {
                collection.remove(item);
                totalConnection.decrementAndGet();
                try {
                    item.abort(connectionRemover);
                } catch (SQLException ex) {
                    // eat exception
                }
            }
        }
    }

    private void initializePoolGlobalState(OceanBaseConnection connection) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      String sql =
          "SELECT @@max_allowed_packet,"
              + "@@wait_timeout,"
              + "@@autocommit,"
              + "@@auto_increment_increment,"
              + "@@time_zone,"
              + "@@system_time_zone,"
              + "@@tx_isolation";
      if (!connection.isServerMariaDb()) {
        int major = connection.getMetaData().getDatabaseMajorVersion();
        if ((major >= 8 && connection.versionGreaterOrEqual(8, 0, 3))
            || (major < 8 && connection.versionGreaterOrEqual(5, 7, 20))) {
          sql =
              "SELECT @@max_allowed_packet,"
                  + "@@wait_timeout,"
                  + "@@autocommit,"
                  + "@@auto_increment_increment,"
                  + "@@time_zone,"
                  + "@@system_time_zone,"
                  + "@@transaction_isolation";
        }
      }

      try (ResultSet rs = stmt.executeQuery(sql)) {

        rs.next();

        int transactionIsolation = Utils.transactionFromString(rs.getString(7)); // tx_isolation

        globalInfo =
            new GlobalStateInfo(
                rs.getLong(1), // max_allowed_packet
                rs.getInt(2), // wait_timeout
                rs.getBoolean(3), // autocommit
                rs.getInt(4), // autoIncrementIncrement
                rs.getString(5), // time_zone
                rs.getString(6), // system_time_zone
                transactionIsolation);

        // ensure that the options "maxIdleTime" is not > to server wait_timeout
        // removing 45s since scheduler check  status every 30s
        maxIdleTime = Math.min(options.maxIdleTime, globalInfo.getWaitTimeout() - 45);
      }
    }
  }

    public String getPoolTag() {
        return poolTag;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Pool pool = (Pool) obj;

        return poolTag.equals(pool.poolTag);
    }

    @Override
    public int hashCode() {
        return poolTag.hashCode();
    }

    public GlobalStateInfo getGlobalInfo() {
        return globalInfo;
    }

    @Override
    public long getActiveConnections() {
        return totalConnection.get() - idleConnections.size();
    }

    @Override
    public long getTotalConnections() {
        return totalConnection.get();
    }

    @Override
    public long getIdleConnections() {
        return idleConnections.size();
    }

    public long getConnectionRequests() {
        return pendingRequestNumber.get();
    }

    private void registerJmx() throws Exception {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        String jmxName = poolTag.replace(":", "_");
        ObjectName name = new ObjectName("com.oceanbase.jdbc.pool:type=" + jmxName);

        if (!mbs.isRegistered(name)) {
            mbs.registerMBean(this, name);
        }
    }

    private void unRegisterJmx() throws Exception {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        String jmxName = poolTag.replace(":", "_");
        ObjectName name = new ObjectName("com.oceanbase.jdbc.pool:type=" + jmxName);

        if (mbs.isRegistered(name)) {
            mbs.unregisterMBean(name);
        }
    }

    /**
     * For testing purpose only.
     *
     * @return current thread id's
     */
    public List<Long> testGetConnectionIdleThreadIds() {
    List<Long> threadIds = new ArrayList<>();
    for (OceanBasePooledConnection pooledConnection : idleConnections) {
      threadIds.add(pooledConnection.getConnection().getServerThreadId());
    }
    return threadIds;
  }

    /** JMX method to remove state (will be reinitialized on next connection creation). */
    public void resetStaticGlobal() {
        globalInfo = null;
    }
}
