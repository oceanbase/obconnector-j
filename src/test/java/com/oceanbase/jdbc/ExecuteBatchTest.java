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

import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.Thread.State;
import java.sql.*;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import joptsimple.internal.Strings;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class ExecuteBatchTest extends BaseTest {

    private static final String  oneHundredLengthString;
    private static final boolean profileSql = false;

    static {
        char[] chars = new char[100];
        for (int i = 27; i < 127; i++) {
            chars[i - 27] = (char) i;
        }
        oneHundredLengthString = new String(chars);
    }

    /**
     * Create test tables.
     *
     * @throws SQLException if connection error occur
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("ExecuteBatchTest",
            "id int not null primary key auto_increment, test varchar(100) , test2 int");
        createTable("ExecuteBatchUseBatchMultiSend", "test varchar(100)");
    }

    /**
     * CONJ-426: Test that executeBatch can be properly interrupted.
     *
     * @throws Exception If the test fails
     */
    @Test
  public void interruptExecuteBatch() throws Exception {
    Assume.assumeTrue(
        sharedOptions().useBatchMultiSend
            && !(sharedOptions().useBulkStmts && isMariadbServer() && minVersion(10, 2)));
    ExecutorService service = Executors.newFixedThreadPool(1);

    final CyclicBarrier barrier = new CyclicBarrier(2);
    final AtomicBoolean wasInterrupted = new AtomicBoolean(false);
    final AtomicReference<Exception> exceptionRef = new AtomicReference<>();

    service.submit(
        () -> {
          try {
            PreparedStatement preparedStatement =
                sharedConnection.prepareStatement(
                    "INSERT INTO ExecuteBatchTest(test, test2) values (?, ?)");

            // Send a large enough batch that will take long enough to allow us to interrupt it
            for (int i = 0; i < 1_000_000; i++) {
              preparedStatement.setString(1, String.valueOf(System.nanoTime()));
              preparedStatement.setInt(2, i);
              preparedStatement.addBatch();
            }

            barrier.await();

            preparedStatement.executeBatch();
          } catch (InterruptedException ex) {
            exceptionRef.set(ex);
            Thread.currentThread().interrupt();
          } catch (BrokenBarrierException ex) {
            exceptionRef.set(ex);
          } catch (SQLException ex) {
            exceptionRef.set(ex);
            wasInterrupted.set(Thread.currentThread().isInterrupted());
          } catch (Exception ex) {
            exceptionRef.set(ex);
          }
        });

    barrier.await();

    // Allow the query time to send
    Thread.sleep(TimeUnit.SECONDS.toMillis(1));

    // Interrupt the thread
    service.shutdownNow();

    assertTrue(service.awaitTermination(1, TimeUnit.MINUTES));

    assertNotNull(exceptionRef.get());

    // ensure that even interrupted, connection status is when sending in bulk (all corresponding
    // bulk send are read)
    ResultSet rs = sharedConnection.createStatement().executeQuery("SELECT 123456");
    assertTrue(rs.next());
    assertEquals(123456, rs.getInt(1));

    StringWriter writer = new StringWriter();
    exceptionRef.get().printStackTrace(new PrintWriter(writer));

    assertTrue(
        "Exception should be a SQLException: \n" + writer.toString(),
        exceptionRef.get() instanceof SQLException);

    assertTrue(wasInterrupted.get());
  }

    @Test
  public void serverBulk1mTest() throws SQLException {
    Assume.assumeTrue(checkMaxAllowedPacketMore8m("serverBulk1mTest"));
    Assume.assumeTrue(runLongTest);
    Assume.assumeFalse(sharedIsAurora());
    Assume.assumeTrue(isMariadbServer());

    sharedConnection.createStatement().execute("TRUNCATE TABLE ExecuteBatchTest");

    try (Connection connection =
        setConnection(
            "&useBulkStmts=true&useComMulti=false&useBatchMultiSend=true&profileSql="
                + profileSql)) {
      PreparedStatement preparedStatement =
          connection.prepareStatement("INSERT INTO ExecuteBatchTest(test, test2) values (?, ?)");
      // packet size : 7 200 068 kb
      addBatchData(preparedStatement, 10000, connection, false, false || !minVersion(10, 2));
    }
  }

    @Test
  public void serverBulk20mTest() throws SQLException {
    Assume.assumeTrue(checkMaxAllowedPacketMore20m("serverBulk20mTest"));
    Assume.assumeTrue(runLongTest);
//    Assume.assumeFalse(sharedIsAurora());
//    Assume.assumeTrue(isMariadbServer());

    sharedConnection.createStatement().execute("TRUNCATE TABLE ExecuteBatchTest");

    try (Connection connection =
        setConnection(
            "&useBulkStmts=true&useComMulti=false&useBatchMultiSend=true&profileSql="
                + profileSql)) {
      PreparedStatement preparedStatement =
          connection.prepareStatement("INSERT INTO ExecuteBatchTest(test, test2) values (?, ?)");
      // packet size : 7 200 068 kb
      addBatchData(preparedStatement, 160000, connection, false, false || !minVersion(10, 2));
    }
  }

    @Test
  public void serverStd1mTest() throws SQLException {
    Assume.assumeTrue(checkMaxAllowedPacketMore8m("serverStd1mTest"));
    Assume.assumeTrue(runLongTest);
    sharedConnection.createStatement().execute("TRUNCATE TABLE ExecuteBatchTest");

    try (Connection connection =
        setConnection("&useComMulti=false&useBatchMultiSend=false&profileSql=" + profileSql)) {
      PreparedStatement preparedStatement =
          connection.prepareStatement("INSERT INTO ExecuteBatchTest(test, test2) values (?, ?)");
      addBatchData(preparedStatement, 10000, connection, false, !sharedIsRewrite());
    }
  }

    @Test
  public void clientBulkTest() throws SQLException {
    Assume.assumeTrue(checkMaxAllowedPacketMore8m("clientBulkTest"));
    Assume.assumeTrue(runLongTest);
    Assume.assumeFalse(sharedIsAurora());
    Assume.assumeTrue(isMariadbServer());

    sharedConnection.createStatement().execute("TRUNCATE TABLE ExecuteBatchTest");

    try (Connection connection =
        setConnection(
            "&useBulkStmts=true&useComMulti=false&useBatchMultiSend=true&useServerPrepStmts"
                + "=false&profileSql="
                + profileSql)) {
      PreparedStatement preparedStatement =
          connection.prepareStatement("INSERT INTO ExecuteBatchTest(test, test2) values (?, ?)");
      addBatchData(preparedStatement, 10000, connection, false, false || !minVersion(10, 2));
    }
  }

    @Test
  public void clientMultipleTest() throws SQLException {
    Assume.assumeTrue(checkMaxAllowedPacketMore8m("clientMultipleTest"));
    Assume.assumeTrue(runLongTest);
    Assume.assumeFalse(sharedIsAurora());
    Assume.assumeTrue(isMariadbServer());

    sharedConnection.createStatement().execute("TRUNCATE TABLE ExecuteBatchTest");

    try (Connection connection =
        setConnection(
            "&useBulkStmts=false&useComMulti=false&useBatchMultiSend=true&useServerPrepStmts"
                + "=false&profileSql="
                + profileSql)) {
      PreparedStatement preparedStatement =
          connection.prepareStatement("INSERT INTO ExecuteBatchTest(test, test2) values (?, ?)");
      addBatchData(preparedStatement, 10000, connection, false, !sharedIsRewrite());
    }
  }

    @Test
  public void clientRewriteValuesNotPossible1mTest() throws SQLException {
    Assume.assumeTrue(checkMaxAllowedPacketMore8m("clientRewriteValuesNotPossible1mTest"));
    Assume.assumeTrue(runLongTest);
    sharedConnection.createStatement().execute("TRUNCATE TABLE ExecuteBatchTest");
    try (Connection connection =
        setConnection("&rewriteBatchedStatements=true&profileSql=" + profileSql)) {
      PreparedStatement preparedStatement =
          connection.prepareStatement(
              "INSERT INTO ExecuteBatchTest(test, test2) values (?, ?) ON DUPLICATE KEY UPDATE id=?");
      addBatchData(preparedStatement, 10000, connection, true, true);
    }
  }

    private void addBatchData(PreparedStatement preparedStatement, int batchNumber,
                              Connection connection) throws SQLException {
        addBatchData(preparedStatement, batchNumber, connection, false, false);
    }

    private void addBatchData(PreparedStatement preparedStatement, int batchNumber,
                              Connection connection, boolean additionnalParameter,
                              boolean sendUnique) throws SQLException {
        for (int i = 0; i < batchNumber; i++) {
            preparedStatement.setString(1, oneHundredLengthString);
            preparedStatement.setInt(2, i);
            if (additionnalParameter) {
                preparedStatement.setInt(3, i);
            }
            preparedStatement.addBatch();
        }
        int[] resultInsert = preparedStatement.executeBatch();

        // test result Size
        assertEquals(batchNumber, resultInsert.length);
        for (int i = 0; i < batchNumber; i++) {
            if (!sendUnique
                || (sharedOptions().useBulkStmts && isMariadbServer() && minVersion(10, 2))) {
                assertEquals(Statement.SUCCESS_NO_INFO, resultInsert[i]);
            } else {
                assertEquals(1, resultInsert[i]);
            }
        }

        // check that connection is OK and results are well inserted
        ResultSet resultSet = connection.createStatement().executeQuery(
            "SELECT * FROM ExecuteBatchTest");
        for (int i = 0; i < batchNumber; i++) {
            assertTrue(resultSet.next());
            if (sendUnique
                && (!sharedOptions().useBulkStmts || !isMariadbServer() || !minVersion(10, 2))) {
                assertEquals(i + 1, resultSet.getInt(1));
            }
            assertEquals(oneHundredLengthString, resultSet.getString(2));
            assertEquals(i, resultSet.getInt(3));
        }
        assertFalse(resultSet.next());
    }

    @Test
  public void useBatchMultiSend() throws Exception {
    Assume.assumeFalse(sharedIsAurora());
    try (Connection connection = setConnection("&useBatchMultiSend=true")) {
      String sql = "insert into ExecuteBatchUseBatchMultiSend (test) values (?)";
      try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
        for (int i = 0; i < 10; i++) {
          pstmt.setInt(1, i);
          pstmt.addBatch();
        }
        int[] updateCounts = pstmt.executeBatch();
        assertEquals(10, updateCounts.length);
        for (int updateCount : updateCounts) {
          if ((sharedIsRewrite()
              || (sharedOptions().useBulkStmts && isMariadbServer() && minVersion(10, 2)))) {
            assertEquals(Statement.SUCCESS_NO_INFO, updateCount);
          } else {
            assertEquals(1, updateCount);
          }
        }
      }
    }
  }

    /**
     * CONJ-553: handling RejectedExecutionException.
     *
     * @throws Exception if any error occur
     */
    @Test
  public void ensureBulkSchedulerMaxPoolSizeRejection() throws Throwable {
    Assume.assumeFalse(sharedIsAurora() || sharedOptions().profileSql);
    System.out.println(getProtocolFromConnection(sharedConnection).getHostAddress());

    Statement statement = sharedConnection.createStatement();
    ResultSet resultSet = statement.executeQuery("SELECT @@max_connections");
    assertTrue(resultSet.next());
    int maxConnection = resultSet.getInt(1);
    int limit = Math.min(1, Math.min(200, maxConnection - 10));
    System.out.println("limit:" + limit);
    for (int i = 0; i < limit; i++) {
      createTable("multipleSimultaneousBatch_" + i, "a INT NOT NULL");
    }

    AtomicInteger counter = new AtomicInteger();
    ExecutorService exec = Executors.newFixedThreadPool(limit + 50);
    for (int i = 0; i < limit; i++) {
      exec.execute(
          () -> {
            try (Connection connection = setConnection()) {
              connection.setAutoCommit(false);
              Statement stmt = connection.createStatement();
              int connectionCounter = counter.getAndIncrement();
              for (int j = 0; j < 1024; j++) {
                stmt.addBatch(
                    "INSERT INTO multipleSimultaneousBatch_"
                        + connectionCounter
                        + "(a) VALUES ("
                        + j
                        + ")");
              }
              stmt.executeBatch();
              connection.commit();
            } catch (Throwable e) {
              e.printStackTrace();
            }
          });
    }

    exec.shutdown();
    exec.awaitTermination(150, TimeUnit.SECONDS);

    // check results
    Statement stmt = sharedConnection.createStatement();
    for (int i = 0; i < limit; i++) {
      ResultSet rs = stmt.executeQuery("SELECT count(*) from multipleSimultaneousBatch_" + i);
      assertTrue(rs.next());
      assertEquals(1024, rs.getInt(1));
    }
  }

    @Test
  public void useBatchMultiSendWithError() throws Exception {
    Assume.assumeFalse(sharedIsAurora());

    Iterator<Thread> it = Thread.getAllStackTraces().keySet().iterator();
    Thread thread;

    while (it.hasNext()) {
      thread = it.next();
      if (thread.getName().contains("MariaDb-bulk-")) {
        assertEquals(State.WAITING, thread.getState());
      }
    }

    Properties properties = new Properties();
    properties.setProperty("useBatchMultiSend", "true");
    try (Connection connection = createProxyConnection(properties)) {
      Statement stmt = connection.createStatement();
      stmt.execute(
          "CREATE TEMPORARY TABLE useBatchMultiSendWithError (id INT NOT NULL,"
              + "UNIQUE INDEX `index1` (id))");
      String sql = "insert into useBatchMultiSendWithError (id) values (?)";
      try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
        for (int i = 0; i < 200000; i++) {
          pstmt.setInt(1, i);
          pstmt.addBatch();
        }

        final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        executor.schedule(
            () -> {
              stopProxy();
            },
            10,
            TimeUnit.MILLISECONDS);

        try {
          pstmt.executeBatch();
          fail();
        } catch (SQLException e) {
          Iterator<Thread> it2 = Thread.getAllStackTraces().keySet().iterator();
          Thread.sleep(500);
          Thread thread2;

          while (it2.hasNext()) {
            thread2 = it2.next();
            if (thread2.getName().contains("MariaDb-bulk-")) {
              assertEquals(State.WAITING, thread2.getState());
            }
          }
          restartProxy();
        }
      }
    }
  }

    @Test
    public void testForDima2024062200102854742() throws SQLException {
        createTable("test", "id int not null primary key auto_increment, name_version varchar(50) not null unique ");
        Connection connection = setConnection("&useAffectedRows=true");

        PreparedStatement ps = connection.prepareStatement("insert into test (id, name_version) values(?,?) on duplicate key update name_version = values(name_version)");
        ps.setInt(1, 1);
        ps.setString(2, "name" + 1);
        ps.addBatch();
        ps.setInt(1, 3);
        ps.setString(2, "name" + 3);
        ps.addBatch();
        int[] ints = ps.executeBatch();
        assertArrayEquals(new int[]{1, 1}, ints);

        // Test the same value, executeBatch returns error
        ps = connection.prepareStatement("insert into test (id, name_version) values(?,?) on duplicate key update name_version = values(name_version)");
        ps.setInt(1, 1);
        ps.setString(2, "name" + 1);
        ps.addBatch();
        ints = ps.executeBatch();
        assertArrayEquals(new int[]{0}, ints); //mysql-jdbc [0]

        // "on" exists in SQL, but not "on duplicate key update" sql
        String sql = "update test set name_version = ? where id = ?";
        PreparedStatement ps2 = connection.prepareStatement(sql);
        ps2.setString(1, "name" + 1);
        ps2.setInt(2, 1);
        ps2.addBatch();
        ints = ps2.executeBatch();
        assertArrayEquals(new int[]{0}, ints);

        // getGeneratedKeys
        String sql2 = "insert into test (name_version) values(?) on duplicate key update name_version = values(name_version)";
        ps2 = connection.prepareStatement(sql2, Statement.RETURN_GENERATED_KEYS);
        for (int i = 1; i <= 4; i++) {
            ps2.setString(1, "name" + i);
            ps2.addBatch();
        }
        ints = ps2.executeBatch();
        assertArrayEquals(new int[]{0, 1, 0, 1}, ints);

        ResultSet rs = ps2.getGeneratedKeys(); // 4 5 6 7
        for (int i = 4; i < 8; i++) {
            assertTrue(rs.next());
            assertEquals(i, rs.getInt(1));
        }

        //getGeneratedKeys 2
        String sql3 = "insert into test (id, name_version) values(?, 'name1'), (6, 'name6'), (7, 'name7'), (8, 'name8') on duplicate key update name_version = values(name_version)";
        ps2 = connection.prepareStatement(sql3, Statement.RETURN_GENERATED_KEYS);
        ps2.setInt(1, 1);
        ps2.addBatch();
        ints = ps2.executeBatch();
        assertArrayEquals(new int[]{4}, ints);
        rs = ps2.getGeneratedKeys(); // 8
        assertTrue(rs.next());
        assertEquals(8, rs.getInt(1));
    }

    @Test
    public void testForDima2025091500110972489() throws SQLException {
        Connection conn = setConnection("&useServerPrepStmts=true&rewriteBatchedStatements=true");
        createTable("batch_t1", "c1 int, c2 varchar(10)");
        PreparedStatement ps = conn.prepareStatement("insert into batch_t1 values(?, ?)");
        ParameterMetaData parameterMetaData;

        ps.setInt(1, 1);
        ps.setString(2, "test_" + 1);
        ps.execute();
        parameterMetaData = ps.getParameterMetaData();
        Assert.assertEquals(2, parameterMetaData.getParameterCount());

        for (int j = 0; j < 3; j++) {
            ps.setInt(1, j);
            ps.setString(2, "batch_" + j);
            ps.addBatch();
        }
        ps.executeBatch();
        parameterMetaData = ps.getParameterMetaData();
        Assert.assertEquals(2, parameterMetaData.getParameterCount());

        ps.setInt(1, 2);
        ps.setString(2, "test_" + 2);
        ps.execute();
        parameterMetaData = ps.getParameterMetaData();
        Assert.assertEquals(2, parameterMetaData.getParameterCount());

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select count(*) from batch_t1");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(5, rs.getInt(1));
    }

    @Test
    public void testForDima2025121200112691130() throws SQLException {
        Connection conn = setConnection("&useServerPrepStmts=false");
        createTable("test", "id int");
        StringBuilder sql = new StringBuilder();
        for(int i = 1;i <= 5000;i++){
            sql.append("delete from test where id=" + i +" ;");
        }
        PreparedStatement ps = conn.prepareStatement(sql.toString());
        Assert.assertEquals(0, ps.executeUpdate());
    }

    @Test
    public void testForDima2025071400109946593() throws SQLException {
        Connection conn = setConnection("&useServerPrepStmts=true");
        createTable("test_c", "c1 int, c2 varchar(20)");

        Statement stmt = conn.createStatement();
        stmt.addBatch("select * from test_c where c1 = 12");
        try {
            stmt.executeBatch();
            fail();
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("Can not issue SELECT"));
        }

        stmt.addBatch("select * from test_c where c1 = 12");
        try {
            stmt.executeLargeBatch();
            fail();
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("Can not issue SELECT"));
        }

        stmt.clearBatch();
        try {
            stmt.executeUpdate("select * from test_c where c1 = 1");
            fail();
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("Can not issue SELECT"));
        }

        try {
            stmt.executeLargeUpdate("select * from test_c where c1 = 1");
            fail();
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("Can not issue SELECT"));
        }

        PreparedStatement ps = conn.prepareStatement("select * from test_c where c1 = ?");
        ps.setInt(1, 1);
        ps.addBatch();
        try {
            ps.executeBatch();
            fail();
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("Can not issue SELECT"));
        }

        ps.clearParameters();
        ps.setInt(1, 1);
        ps.addBatch();
        try {
            ps.executeLargeBatch();
            fail();
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("Can not issue SELECT"));
        }

        try {
            ps.executeUpdate();
            fail();
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("Can not issue SELECT"));
        }

        try {
            ps.executeLargeUpdate();
            fail();
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("Can not issue SELECT"));
        }
    }

    @Test
    public void testForDima2025090900110882492() throws SQLException {
        Connection conn = setConnection("&useServerPrepStmts=true&useBatchMultiSend=true&rewriteBatchedStatements=true");
        createTable("t_batch", "c1 int, c2 int");

        String updateSql = "update t_batch set c1 = ? where c2 = ?";
        PreparedStatement ps = conn.prepareStatement(updateSql);
        for (int i = 0; i < 10; i++) {
            ps.setInt(1, i);
            ps.setInt(2, i);
            ps.addBatch();
        }
        ps.executeBatch();

        String sql = "insert into t_batch values(?, ?)";
        ps = conn.prepareStatement(sql);
        for (int i = 0; i < 10; i++) {
            ps.setInt(1, i);
            ps.setInt(2, i);
            ps.addBatch();
        }
        ps.executeBatch();

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select count(*) from t_batch");
        rs.next();
        Assert.assertEquals(10, rs.getInt(1));
    }

    @Test
    public void testForDima2026040300115148124() throws SQLException {
        Connection conn = setConnection("&rewriteBatchedStatements=true");
        createTable("boundary_bug_test", "id INT, data longtext");

        int dataLength = 16777163;
        String longStr = Strings.repeat('A', dataLength);
        String sql = "insert into boundary_bug_test VALUES (?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, 1);
            ps.setCharacterStream(2, new StringReader(longStr), dataLength);
            ps.addBatch();

            ps.setInt(1, 2);
            ps.setCharacterStream(2, new StringReader(longStr), dataLength);
            ps.addBatch();

            ps.executeBatch();
        } catch (ArrayIndexOutOfBoundsException e) {
            fail(e.getMessage());
        }

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select count(*) from boundary_bug_test");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(2, rs.getInt(1));
    }

    @Test
    public void testForDima2026031900114742281() throws SQLException {
        Connection conn = setConnection("&continueBatchOnError=false");
        createTable("t_batch", "c1 int");

        Statement stmt = conn.createStatement();
        stmt.addBatch("insert into t_batch values(1)");
        stmt.addBatch("insert into t_batch values(2)");
        stmt.addBatch("insert into t_batch values('s')");
        stmt.addBatch("insert into t_batch values(4)");

        try {
            stmt.executeBatch();
            fail();
        } catch (BatchUpdateException e) {
            Assert.assertEquals("[1, 1]", Arrays.toString(e.getUpdateCounts()));
        }

        conn = setConnection("&rewriteBatchedStatements=true&continueBatchOnError=false");
        PreparedStatement ps = conn.prepareStatement("insert into t_batch values(?)");
        ps.setInt(1, 1);
        ps.addBatch();

        ps.setInt(1, 2);
        ps.addBatch();

        ps.setString(1, "s");
        ps.addBatch();

        ps.setInt(1, 4);
        ps.addBatch();
        try {
            ps.executeBatch();
            fail();
        } catch (BatchUpdateException e) {
            Assert.assertEquals("[-3, -3, -3]", Arrays.toString(e.getUpdateCounts()));
        }

        conn = setConnection("&rewriteBatchedStatements=true&continueBatchOnError=true");
        PreparedStatement ps2 = conn.prepareStatement("insert into t_batch values(?)");
        ps2.addBatch("insert into t_batch values(1)");
        ps2.addBatch("insert into t_batch values(2)");
        ps2.addBatch("insert into t_batch values('s')");
        ps2.addBatch("insert into t_batch values(4)");
        try {
            ps2.executeBatch();
            fail();
        } catch (BatchUpdateException e) {
            Assert.assertEquals("[1, 1, -3, 1]", Arrays.toString(e.getUpdateCounts()));
        }
    }

    @Test
    public void testForDima2026042200115630287() throws SQLException {
        createTable("t_batch_uc", "c1 int");
        Connection conn = setConnection("&continueBatchOnError=false");
        Statement stmt = conn.createStatement();
        stmt.addBatch("insert into t_batch_uc values('s')");  // 首条失败
        stmt.addBatch("insert into t_batch_uc values(1)");
        stmt.addBatch("insert into t_batch_uc values(2)");
        try {
            stmt.executeBatch();
            fail();
        } catch (BatchUpdateException e) {
            int[] counts = e.getUpdateCounts();
            Assert.assertEquals("[]", Arrays.toString(counts));
        }
        conn.close();
    }

}
