package com.alipay.oceanbase.jdbc;

import java.sql.*;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class AutoReconnectTest extends BaseTest {

    @Test
  public void autoReconnect() throws SQLException, InterruptedException {
    Assume.assumeTrue(System.getenv("MAXSCALE_VERSION") == null && System.getenv("SKYSQL") == null);
    try (Connection conn = setConnection("&autoReconnect")) {
      Statement stmt = conn.createStatement();
      stmt.executeQuery("SELECT 1");

      stmt.execute("SET SESSION wait_timeout = 1");
      Thread.sleep(3000);

      try {
        stmt.executeQuery("SELECT 2");
      } catch (SQLNonTransientConnectionException e) {
        Assert.fail("Must have send a SQLTransientConnectionException !");
      } catch (SQLTransientConnectionException e) {
        try {
          stmt.executeQuery("SELECT 3");
        } catch (SQLException ee) {
          ee.printStackTrace();
          Assert.fail("Must have reconnect automatically !");
        }
      } catch (SQLException e) {
        e.printStackTrace();
        Assert.fail("Must have reconnect automatically !");
      }
    }
  }

    @Test
  public void autoReconnectPing() throws SQLException, InterruptedException {
    Assume.assumeTrue(System.getenv("MAXSCALE_VERSION") == null && System.getenv("SKYSQL") == null);
    try (Connection conn = setConnection("&autoReconnect")) {
      Statement stmt = conn.createStatement();
      stmt.executeQuery("SELECT 1");

      stmt.execute("SET SESSION wait_timeout = 1");
      Thread.sleep(30000);

      try {
        conn.isValid(0);
      } catch (SQLTransientConnectionException e) {
        Assert.fail("Must have send a SQLNonTransientConnectionException !");
      } catch (SQLNonTransientConnectionException e) {
        try {
          stmt.executeQuery("SELECT 3");
        } catch (SQLException ee) {
          ee.printStackTrace();
          Assert.fail("Must have reconnect automatically !");
        }
      } catch (SQLException e) {
        e.printStackTrace();
        Assert.fail("Must have reconnect automatically !");
      }
    }
  }
}
