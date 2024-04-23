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
package com.oceanbase.jdbc;

import static org.junit.Assert.*;

import java.lang.management.ManagementFactory;
import java.sql.*;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.sql.PooledConnection;

import org.junit.Assume;
import org.junit.Test;

import com.oceanbase.jdbc.internal.util.pool.Pools;
import com.oceanbase.jdbc.internal.util.scheduler.OceanBaseThreadFactory;

public class OceanBasePoolDataSourceTest extends BaseTest {

    @Test
  public void testResetDatabase() throws SQLException {
    try (OceanBasePoolDataSource pool = new OceanBasePoolDataSource(connUri + "&maxPoolSize=1")) {
      try (Connection connection = pool.getConnection()) {
        Statement statement = connection.createStatement();
        statement.execute("CREATE DATABASE IF NOT EXISTS testingReset");
        connection.setCatalog("testingReset");
      }

      try (Connection connection = pool.getConnection()) {
        assertEquals(database, connection.getCatalog());
        Statement statement = connection.createStatement();
        statement.execute("DROP DATABASE testingReset");
      }
    }
  }

    @Test
    public void testResetSessionVariable() throws SQLException {
        testResetSessionVariable(false);
        if (isMariadbServer() && minVersion(10, 2)) {
            testResetSessionVariable(true);
        }
    }

    private void testResetSessionVariable(boolean useResetConnection) throws SQLException {
    try (OceanBasePoolDataSource pool =
        new OceanBasePoolDataSource(
            connUri + "&maxPoolSize=1&useResetConnection=" + useResetConnection)) {

      long nowMillis;
      int initialWaitTimeout;

      try (Connection connection = pool.getConnection()) {
        Statement statement = connection.createStatement();

        nowMillis = getNowTime(statement);
        initialWaitTimeout = getWaitTimeout(statement);

        statement.execute(
            "SET @@timestamp=UNIX_TIMESTAMP('1970-10-01 01:00:00'), @@wait_timeout=2000");
        long newNowMillis = getNowTime(statement);
        int waitTimeout = getWaitTimeout(statement);

        assertTrue(nowMillis - newNowMillis > 23_587_200_000L);
        assertEquals(2_000, waitTimeout);
      }

      try (Connection connection = pool.getConnection()) {
        Statement statement = connection.createStatement();

        long newNowMillis = getNowTime(statement);
        int waitTimeout = getWaitTimeout(statement);

        if (useResetConnection) {
          assertTrue(nowMillis - newNowMillis < 10L);
          assertEquals(initialWaitTimeout, waitTimeout);
        } else {
          assertTrue(nowMillis - newNowMillis > 23_587_200_000L);
          assertEquals(2_000, waitTimeout);
        }
      }
    }
  }

    private long getNowTime(Statement statement) throws SQLException {
        ResultSet rs = statement.executeQuery("SELECT NOW()");
        assertTrue(rs.next());
        return rs.getTimestamp(1).getTime();
    }

    private int getWaitTimeout(Statement statement) throws SQLException {
        ResultSet rs = statement.executeQuery("SELECT @@wait_timeout");
        assertTrue(rs.next());
        return rs.getInt(1);
    }

    @Test
    public void testResetUserVariable() throws SQLException {
        testResetUserVariable(false);
        if (isMariadbServer() && minVersion(10, 2)) {
            testResetUserVariable(true);
        }
    }

    private void testResetUserVariable(boolean useResetConnection) throws SQLException {
    try (OceanBasePoolDataSource pool =
        new OceanBasePoolDataSource(
            connUri + "&maxPoolSize=1&useResetConnection=" + useResetConnection)) {
      long nowMillis;
      try (Connection connection = pool.getConnection()) {
        Statement statement = connection.createStatement();
        assertNull(getUserVariableStr(statement));

        statement.execute("SET @str = '123'");

        assertEquals("123", getUserVariableStr(statement));
      }

      try (Connection connection = pool.getConnection()) {
        Statement statement = connection.createStatement();
        if (useResetConnection) {
          assertNull(getUserVariableStr(statement));
        } else {
          assertEquals("123", getUserVariableStr(statement));
        }
      }
    }
  }

    private String getUserVariableStr(Statement statement) throws SQLException {
        ResultSet rs = statement.executeQuery("SELECT @str");
        assertTrue(rs.next());
        return rs.getString(1);
    }

    @Test
  public void testNetworkTimeout() throws SQLException {
    try (OceanBasePoolDataSource pool =
        new OceanBasePoolDataSource(connUri + "&maxPoolSize=1&socketTimeout=10000")) {
      try (Connection connection = pool.getConnection()) {
        assertEquals(10_000, connection.getNetworkTimeout());
        connection.setNetworkTimeout(null, 5_000);
      }

      try (Connection connection = pool.getConnection()) {
        assertEquals(10_000, connection.getNetworkTimeout());
      }
    }
  }

    @Test
  public void testResetReadOnly() throws SQLException {
    try (OceanBasePoolDataSource pool = new OceanBasePoolDataSource(connUri + "&maxPoolSize=1")) {
      try (Connection connection = pool.getConnection()) {
        assertFalse(connection.isReadOnly());
        connection.setReadOnly(true);
        assertTrue(connection.isReadOnly());
      }

      try (Connection connection = pool.getConnection()) {
        assertFalse(connection.isReadOnly());
      }
    }
  }

    @Test
  public void testResetAutoCommit() throws SQLException {
    try (OceanBasePoolDataSource pool = new OceanBasePoolDataSource(connUri + "&maxPoolSize=1")) {
      try (Connection connection = pool.getConnection()) {
        assertTrue(connection.getAutoCommit());
        connection.setAutoCommit(false);
        assertFalse(connection.getAutoCommit());
      }

      try (Connection connection = pool.getConnection()) {
        assertTrue(connection.getAutoCommit());
      }
    }
  }

    @Test
  public void testResetAutoCommitOption() throws SQLException {
    try (OceanBasePoolDataSource pool =
        new OceanBasePoolDataSource(connUri + "&maxPoolSize=1&autocommit=false&poolName=PoolTest")) {
      try (Connection connection = pool.getConnection()) {
        assertFalse(connection.getAutoCommit());
        connection.setAutoCommit(true);
        assertTrue(connection.getAutoCommit());
      }

      try (Connection connection = pool.getConnection()) {
        assertFalse(connection.getAutoCommit());
      }
    }
  }

    @Test
  public void testResetTransactionIsolation() throws SQLException {
    Assume.assumeTrue(!sharedIsAurora() && System.getenv("SKYSQL") == null);
    try (OceanBasePoolDataSource pool = new OceanBasePoolDataSource(connUri + "&maxPoolSize=1")) {

      try (Connection connection = pool.getConnection()) {
        assertEquals(Connection.TRANSACTION_REPEATABLE_READ, connection.getTransactionIsolation());
        connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        assertEquals(Connection.TRANSACTION_SERIALIZABLE, connection.getTransactionIsolation());
      }

      try (Connection connection = pool.getConnection()) {
        assertEquals(Connection.TRANSACTION_REPEATABLE_READ, connection.getTransactionIsolation());
      }
    }
  }

    @Test
  public void testJmx() throws Exception {
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    ObjectName filter = new ObjectName("com.oceanbase.jdbc.pool:type=PoolTestJmx-*");
    try (OceanBasePoolDataSource pool =
        new OceanBasePoolDataSource(connUri + "&maxPoolSize=5&minPoolSize=0&poolName=PoolTestJmx")) {
      try (Connection connection = pool.getConnection()) {
        Set<ObjectName> objectNames = server.queryNames(filter, null);
        assertEquals(1, objectNames.size());
        ObjectName name = objectNames.iterator().next();

        MBeanInfo info = server.getMBeanInfo(name);
        assertEquals(4, info.getAttributes().length);

        checkJmxInfo(server, name, 1, 1, 0, 0);

        try (Connection connection2 = pool.getConnection()) {
          checkJmxInfo(server, name, 2, 2, 0, 0);
        }
        checkJmxInfo(server, name, 1, 2, 1, 0);
      }
    }
  }

    @Test
  public void testNoMinConnection() throws Exception {
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    ObjectName filter = new ObjectName("com.oceanbase.jdbc.pool:type=testNoMinConnection-*");
    try (OceanBasePoolDataSource pool =
        new OceanBasePoolDataSource(connUri + "&maxPoolSize=5&poolName=testNoMinConnection")) {
      try (Connection connection = pool.getConnection()) {
        Set<ObjectName> objectNames = server.queryNames(filter, null);
        assertEquals(1, objectNames.size());
        ObjectName name = objectNames.iterator().next();

        MBeanInfo info = server.getMBeanInfo(name);
        assertEquals(4, info.getAttributes().length);

        // wait to ensure pool has time to create 5 connections
        try {
          Thread.sleep(sharedIsAurora() ? 10_000 : 500);
        } catch (InterruptedException interruptEx) {
          // eat
        }

        checkJmxInfo(server, name, 1, 5, 4, 0);

        try (Connection connection2 = pool.getConnection()) {
          checkJmxInfo(server, name, 2, 5, 3, 0);
        }
        checkJmxInfo(server, name, 1, 5, 4, 0);
      }
    }
  }

    @Test
  public void testIdleTimeout() throws Throwable {
    // not for maxscale, testing thread id is not relevant.
    // appveyor is so slow wait time are not relevant.
    Assume.assumeTrue(
        System.getenv("MAXSCALE_VERSION") == null
            && System.getenv("SKYSQL") == null
            && System.getenv("APPVEYOR_BUILD_WORKER_IMAGE") == null);

    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    ObjectName filter = new ObjectName("com.oceanbase.jdbc.pool:type=testIdleTimeout-*");
    try (OceanBasePoolDataSource pool =
        new OceanBasePoolDataSource(
            connUri + "&maxPoolSize=5&minPoolSize=3&poolName=testIdleTimeout")) {

      pool.testForceMaxIdleTime(sharedIsAurora() ? 10 : 3);
      // wait to ensure pool has time to create 3 connections
      Thread.sleep(sharedIsAurora() ? 5_000 : 1_000);

      Set<ObjectName> objectNames = server.queryNames(filter, null);
      ObjectName name = objectNames.iterator().next();
      checkJmxInfo(server, name, 0, 3, 3, 0);

      List<Long> initialThreadIds = pool.testGetConnectionIdleThreadIds();
      Thread.sleep(sharedIsAurora() ? 12_000 : 3_500);

      // must still have 3 connections, but must be other ones
      checkJmxInfo(server, name, 0, 3, 3, 0);
      List<Long> threadIds = pool.testGetConnectionIdleThreadIds();
      assertEquals(initialThreadIds.size(), threadIds.size());
      for (Long initialThread : initialThreadIds) {
        assertFalse(threadIds.contains(initialThread));
      }
    }
  }

    @Test
  public void testMinConnection() throws Throwable {
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    ObjectName filter = new ObjectName("com.oceanbase.jdbc.pool:type=testMinConnection-*");
    try (OceanBasePoolDataSource pool =
        new OceanBasePoolDataSource(
            connUri + "&maxPoolSize=5&minPoolSize=3&poolName=testMinConnection")) {
      try (Connection connection = pool.getConnection()) {
        Set<ObjectName> objectNames = server.queryNames(filter, null);
        assertEquals(1, objectNames.size());
        ObjectName name = objectNames.iterator().next();

        MBeanInfo info = server.getMBeanInfo(name);
        assertEquals(4, info.getAttributes().length);

        // to ensure pool has time to create minimal connection number
        Thread.sleep(sharedIsAurora() ? 5000 : 500);

        checkJmxInfo(server, name, 1, 3, 2, 0);

        try (Connection connection2 = pool.getConnection()) {
          checkJmxInfo(server, name, 2, 3, 1, 0);
        }
        checkJmxInfo(server, name, 1, 3, 2, 0);
      }
    }
  }

    private void checkJmxInfo(MBeanServer server, ObjectName name, long expectedActive,
                              long expectedTotal, long expectedIdle, long expectedRequest)
                                                                                          throws Exception {

        assertEquals(expectedActive,
            ((Long) server.getAttribute(name, "ActiveConnections")).longValue());
        assertEquals(expectedTotal,
            ((Long) server.getAttribute(name, "TotalConnections")).longValue());
        assertEquals(expectedIdle,
            ((Long) server.getAttribute(name, "IdleConnections")).longValue());
        assertEquals(expectedRequest,
            ((Long) server.getAttribute(name, "ConnectionRequests")).longValue());
    }

    @Test
  public void testJmxDisable() throws Exception {
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    ObjectName filter = new ObjectName("com.oceanbase.jdbc.pool:type=PoolTest-*");
    try (OceanBasePoolDataSource pool =
        new OceanBasePoolDataSource(
            connUri + "&maxPoolSize=2&registerJmxPool=false&poolName=PoolTest")) {
      try (Connection connection = pool.getConnection()) {
        Set<ObjectName> objectNames = server.queryNames(filter, null);
        assertEquals(0, objectNames.size());
      }
    }
  }

    @Test
  public void testResetRollback() throws SQLException {
    createTable(
        "testResetRollback", "id int not null primary key auto_increment, test varchar(20)");
    try (OceanBasePoolDataSource pool = new OceanBasePoolDataSource(connUri + "&maxPoolSize=1")) {
      try (Connection connection = pool.getConnection()) {
        Statement stmt = connection.createStatement();
        stmt.executeUpdate("INSERT INTO testResetRollback (test) VALUES ('heja')");
        connection.setAutoCommit(false);
        stmt.executeUpdate("INSERT INTO testResetRollback (test) VALUES ('japp')");
      }

      try (Connection connection = pool.getConnection()) {
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT count(*) FROM testResetRollback");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
      }
    }
  }

    @Test
  public void ensureUsingPool() throws Exception {
    Assume.assumeTrue(System.getenv("SKYSQL") == null);
    ThreadPoolExecutor connectionAppender =
        new ThreadPoolExecutor(
            50,
            5000,
            10,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(5000),
            new OceanBaseThreadFactory("testPool"));

    final long start = System.currentTimeMillis();
    Set<Integer> threadIds = new HashSet<>();
    for (int i = 0; i < 500; i++) {
      connectionAppender.execute(
          () -> {
            try (Connection connection =
                DriverManager.getConnection(
                    connUri + "&pool&staticGlobal&poolName=PoolEnsureUsingPool&log=true")) {
              Statement stmt = connection.createStatement();
              ResultSet rs = stmt.executeQuery("SELECT CONNECTION_ID()");
              rs.next();
              Integer connectionId = rs.getInt(1);
              threadIds.add(connectionId);
              stmt.execute("SELECT * FROM mysql.user");

            } catch (SQLException e) {
              e.printStackTrace();
            }
          });
    }
    connectionAppender.shutdown();
    connectionAppender.awaitTermination(sharedIsAurora() ? 200 : 30, TimeUnit.SECONDS);
    int numberOfConnection = 0;

    for (Integer integer : threadIds) {
      System.out.println("Connection id : " + integer);
      numberOfConnection++;
    }
    System.out.println("Size : " + threadIds.size() + " " + numberOfConnection);
    assertTrue(
        "connection ids must be less than 8 : " + numberOfConnection, numberOfConnection <= 8);
    assertTrue(System.currentTimeMillis() - start < (sharedIsAurora() ? 120_000 : 5_000));
    Pools.close("PoolTest");
  }

    @Test
  public void ensureClosed() throws Throwable {
    Thread.sleep(500); // ensure that previous close are effective
    int initialConnection = getCurrentConnections();

    try (OceanBasePoolDataSource pool =
        new OceanBasePoolDataSource(connUri + "&maxPoolSize=10&minPoolSize=1")) {

      try (Connection connection = pool.getConnection()) {
        connection.isValid(10_000);
      }

      assertTrue(getCurrentConnections() > initialConnection);

      // reuse IdleConnection
      try (Connection connection = pool.getConnection()) {
        connection.isValid(10_000);
      }

      Thread.sleep(500);
      assertTrue(getCurrentConnections() > initialConnection);
    }
    Thread.sleep(2000); // ensure that previous close are effective
    assertEquals(initialConnection, getCurrentConnections());
  }

    @Test
  public void wrongUrlHandling() throws SQLException {

    int initialConnection = getCurrentConnections();
    try (OceanBasePoolDataSource pool =
        new OceanBasePoolDataSource(
            "jdbc:oceanbase://unknownHost/db?user=wrong&maxPoolSize=10&connectTimeout=500")) {
      long start = System.currentTimeMillis();
      try (Connection connection = pool.getConnection()) {
        fail();
      } catch (SQLException sqle) {
        assertTrue(
            "timeout does not correspond to option. Elapsed time:"
                + (System.currentTimeMillis() - start),
            (System.currentTimeMillis() - start) >= 500
                && (System.currentTimeMillis() - start) < 700);
        assertTrue(
            sqle.getMessage()
                .contains(
                    "No connection available within the specified time (option 'connectTimeout': 500 ms)"));
      }
    }
  }

    @Test
  public void testPrepareReset() throws SQLException {
    try (OceanBasePoolDataSource pool =
        new OceanBasePoolDataSource(
            connUri + "&maxPoolSize=1&useServerPrepStmts=true&useResetConnection")) {
      try (Connection connection = pool.getConnection()) {
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT ?");
        preparedStatement.setString(1, "1");
        preparedStatement.execute();
      }

      try (Connection connection = pool.getConnection()) {
        // must re-prepare
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT ?");
        preparedStatement.setString(1, "1");
        preparedStatement.execute();
      }
    }
  }

    @Test
    public void testPoolConnForAone55901400() throws SQLException, InterruptedException {
        // pool == false， do not use pool
        OceanBasePoolDataSource pool = new OceanBasePoolDataSource(connUri + "&maxPoolSize=2");
        testPoolComm(pool);

        // pool == true， use pool
        OceanBasePoolDataSource pool2 = new OceanBasePoolDataSource(connUri
                                                                    + "&maxPoolSize=2&pool=true");
        testPoolComm(pool2);

        // more than maxPoolSize=2
        for (int i = 0; i < 3; i++) {
          PooledConnection pooledConnection = null;
          try {
            pooledConnection = pool2.getPooledConnection();
            if (i == 2) {
              fail();
            }
            Connection connection = pooledConnection.getConnection();
            ResultSet resultSet = connection.createStatement().executeQuery("select " + i);
            resultSet.next();
            assertEquals(i, resultSet.getInt(1));
          } catch (SQLException e) {
            assertEquals("No connection available within the specified time (option 'connectTimeout': 30,000 ms)", e.getMessage());
          }
        }
    }

    public void testPoolComm(OceanBasePoolDataSource pool) throws SQLException,
                                                          InterruptedException {
        for (int i = 0; i < 5; i++) {
            // pool == false : Create a new physical conn
            // pool == true  : take from pool
            Connection connection = pool.getConnection();
            ResultSet resultSet = connection.createStatement().executeQuery("select " + i);
            resultSet.next();
            assertEquals(i, resultSet.getInt(1));
            // pool == false : real close
            // pool == true  : recycle to connection pool
            connection.close();
        }

        for (int i = 0; i < 5; i++) {
            // pool == false : Create a new physical conn
            // pool == true  : take from pool
            PooledConnection pooledConnection = pool.getPooledConnection();
            Connection connection = pooledConnection.getConnection();
            ResultSet resultSet = connection.createStatement().executeQuery("select " + i);
            resultSet.next();
            assertEquals(i, resultSet.getInt(1));
            // pool == false : because the pooledConnection does not have a listener, closing at this point has no practical significance
            // pool == true  : recycle to connection pool
            connection.close();
        }

        for (int i = 0; i < 5; i++) {
            // pool == false : Create a new physical conn
            // pool == true  : take from pool
            PooledConnection pooledConnection = pool.getPooledConnection("test@tt1", "test");
            Connection connection = pooledConnection.getConnection();

            ResultSet resultSet = connection.createStatement().executeQuery("select " + i);
            resultSet.next();
            assertEquals(i, resultSet.getInt(1));

            // pool == false : because the pooledConnection does not have a listener, closing at this point has no practical significance
            // pool == true  : recycle to connection pool
            connection.close();

            //  pool == false : conn is truly closed. When the next getPooledConnection occurs, a new one will be created directly
            //  pool == true  : conn is truly closed，and will be removed from the pool when the next getPooledConnection occurs
            pooledConnection.close();
            // pool == true  :Verify if the conn is valid based on options.poolValidMinDelay， with a default value of 1000ms
            Thread.sleep(1100);
        }
    }

}
