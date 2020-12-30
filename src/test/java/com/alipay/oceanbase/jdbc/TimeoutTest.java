package com.alipay.oceanbase.jdbc;

import static org.junit.Assert.*;

import java.sql.*;
import org.junit.Assume;
import org.junit.Test;

public class TimeoutTest extends BaseTest {

    private static int selectValue(Connection conn, int value) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery("select " + value)) {
        if (rs.next()) {
          return rs.getInt(1);
        } else {
          return value;
        }
      }
    }
  }

    /** Conj-79. */
    @Test
  public void resultSetAfterSocketTimeoutTest() {
    // appveyor vm are very slow, cannot compare time
    Assume.assumeTrue(
        System.getenv("APPVEYOR") == null
            && System.getenv("DOCKER_SOCKET") == null
            && System.getenv("SKYSQL") == null);

    Assume.assumeFalse(sharedIsAurora());
    int went = 0;
    for (int j = 0; j < 100; j++) {
      try (Connection connection = setConnection("&connectTimeout=5&socketTimeout=1")) {
        boolean bugReproduced = false;

        int repetition = 1000;
        for (int i = 0; i < repetition && !connection.isClosed(); i++) {
          try {
            int v1 = selectValue(connection, 1);
            int v2 = selectValue(connection, 2);
            if (v1 != 1 || v2 != 2) {
              bugReproduced = true;
              break;
            }
            went++;
          } catch (SQLNonTransientConnectionException e) {
            // error due to socketTimeout
          } catch (SQLException e) {
            e.printStackTrace();
          }
        }
        assertFalse(bugReproduced); // either Exception or fine
      } catch (SQLException e) {
        // SQLNonTransientConnectionException error
      }
    }
    assertTrue(went > 0);
  }

    /**
     * Conj-79.
     *
     * @throws SQLException exception
     */
    @Test
  public void socketTimeoutTest() throws SQLException {
    Assume.assumeFalse(sharedIsAurora());
    // set a short connection timeout
    try (Connection connection = setConnection("&connectTimeout=1000&socketTimeout=1000")) {
      PreparedStatement ps = connection.prepareStatement("SELECT 1");
      ResultSet rs = ps.executeQuery();
      assertTrue(rs.next());
      logInfo(rs.getString(1));

      // wait for the connection to time out
      ps = connection.prepareStatement("SELECT sleep(2)");

      // a timeout should occur here
      try {
        ps.executeQuery();
        fail();
      } catch (SQLException e) {
        // check that it's a timeout that occurs
      }

      try {
        ps = connection.prepareStatement("SELECT 2");
        ps.execute();
        fail("Connection must have thrown error");
      } catch (SQLException e) {
        // normal exception
      }

      // the connection should  be closed
      assertTrue(connection.isClosed());
    }
  }

    @Test
  public void waitTimeoutStatementTest() throws SQLException, InterruptedException {
    Assume.assumeFalse(sharedIsAurora());
    try (Connection connection = setConnection()) {
      try (Statement statement = connection.createStatement()) {
        statement.execute("set session wait_timeout=1");
        Thread.sleep(2000); // Wait for the server to kill the connection

        logInfo(connection.toString());

        // here a SQLNonTransientConnectionException is expected
        // "Could not read resultset: unexpected end of stream, ..."
        try {
          statement.execute("SELECT 1");
          fail("Connection must have thrown error");
        } catch (SQLException e) {
          // normal exception
        }
      }
    }
  }

    @Test
  public void waitTimeoutResultSetTest() throws SQLException, InterruptedException {
    Assume.assumeFalse(sharedIsAurora());
    try (Connection connection = setConnection()) {
      Statement stmt = connection.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT 1");

      assertTrue(rs.next());

      stmt.execute("set session wait_timeout=1");
      Thread.sleep(3000); // Wait for the server to kill the connection

      // here a SQLNonTransientConnectionException is expected
      // "Could not read resultset: unexpected end of stream, ..."
      try {
        rs = stmt.executeQuery("SELECT 2");
        assertTrue(rs.next());
        fail("Connection must have thrown error");
      } catch (SQLException e) {
        // normal exception
      }
    }
  }
}
