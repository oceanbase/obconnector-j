package com.alipay.oceanbase.jdbc.failover;

import static org.junit.Assert.*;

import java.sql.*;
import java.util.concurrent.TimeUnit;

import com.alipay.oceanbase.jdbc.internal.protocol.Protocol;
import com.alipay.oceanbase.jdbc.internal.util.constant.HaMode;
import org.junit.*;

/**
 * Aurora test suite. Some environment parameter must be set : - defaultAuroraUrl : example
 * -DdefaultAuroraUrl=jdbc:oceanbase:aurora://instance-1.xxxx,instance-2.xxxx/testj?user=userName&password=userPwd
 * - AURORA_ACCESS_KEY = access key - AURORA_SECRET_KEY = secret key - AURORA_CLUSTER_IDENTIFIER =
 * cluster identifier. example : -DAURORA_CLUSTER_IDENTIFIER=instance-1-cluster
 *
 * <p>"AURORA" environment variable must be set to a value
 */
public class AuroraFailoverTest extends BaseReplication {

    /** Initialisation. */
    @BeforeClass()
    public static void beforeClass2() {
        proxyUrl = proxyAuroraUrl;
        Assume.assumeTrue(initialAuroraUrl != null);
    }

    /** Initialisation. */
    @Before
    public void init() {
        defaultUrl = initialAuroraUrl;
        currentType = HaMode.AURORA;
    }

    @Ignore
    @Test
  public void testErrorWriteOnReplica() throws SQLException {
    try (Connection connection = getNewConnection(false)) {
      Statement stmt = connection.createStatement();
      stmt.execute("drop table  if exists auroraDelete" + jobId);
      stmt.execute(
          "create table auroraDelete"
              + jobId
              + " (id int not null primary key auto_increment, test VARCHAR(10))");
      connection.setReadOnly(true);
      assertTrue(connection.isReadOnly());
      try {
        stmt.execute("drop table if exists auroraDelete" + jobId);
        System.out.println(
            "ERROR - > must not be able to write on slave. check if you database is start with --read-only");
        fail();
      } catch (SQLException e) {
        // normal exception
        connection.setReadOnly(false);
        stmt.execute("drop table if exists auroraDelete" + jobId);
      }
    }
  }

    @Ignore
  public void testReplication() throws SQLException, InterruptedException {
    try (Connection connection = getNewConnection(false)) {
      Statement stmt = connection.createStatement();
      stmt.execute("drop table  if exists auroraReadSlave" + jobId);
      stmt.execute(
          "create table auroraReadSlave"
              + jobId
              + " (id int not null primary key auto_increment, test VARCHAR(10))");

      // wait to be sure slave have replicate data
      Thread.sleep(1500);

      connection.setReadOnly(true);
      ResultSet rs = stmt.executeQuery("Select count(*) from auroraReadSlave" + jobId);
      assertTrue(rs.next());
      connection.setReadOnly(false);
      stmt.execute("drop table  if exists auroraReadSlave" + jobId);
    }
  }

    @Ignore
  public void testReadOnly() throws SQLException {
    try (Connection connection = getNewConnection(false)) {
      ResultSet rs = connection.createStatement().executeQuery("select @@innodb_read_only");

      assertTrue(rs.next());
      assertEquals(0, rs.getInt(1));
      assertFalse(connection.isReadOnly());

      connection.setReadOnly(true);

      rs = connection.createStatement().executeQuery("select @@innodb_read_only");

      assertTrue(rs.next());
      assertNotEquals(0, rs.getInt(1));
      assertTrue(connection.isReadOnly());
    }
  }

    @Ignore
  public void testFailMaster() throws Throwable {
    try (Connection connection = getNewConnection("&retriesAllDown=3&connectTimeout=1000", true)) {
      int previousPort = getProtocolFromConnection(connection).getPort();
      Statement stmt = connection.createStatement();
      int masterServerId = getServerId(connection);
      stopProxy(masterServerId);
      long stopTime = System.nanoTime();
      try {
        // Handles failover so may connect to another and is still able to execute
        stmt.execute("SELECT 1");
        if (getProtocolFromConnection(connection).getPort() == previousPort) {
          fail();
        }
      } catch (SQLException e) {
        // normal error
      }
      assertFalse(connection.isReadOnly());
      long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - stopTime);
      assertTrue(duration < 25 * 1000);
    }
  }

    /**
     * Conj-79.
     *
     * @throws SQLException exception
     */
    @Ignore
  public void socketTimeoutTest() throws SQLException {
    // set a short connection timeout
    try (Connection connection = getNewConnection("&socketTimeout=4000", false)) {

      PreparedStatement ps = connection.prepareStatement("SELECT 1");
      ResultSet rs = ps.executeQuery();
      assertTrue(rs.next());

      // wait for the connection to time out
      ps = connection.prepareStatement("DO sleep(20)");

      // a timeout should occur here
      try {
        ps.executeQuery();
        fail();
      } catch (SQLException e) {
        // check that it's a timeout that occurs
        assertTrue(e.getMessage().contains("timed out"));
      }
      try {
        ps = connection.prepareStatement("SELECT 2");
        ps.execute();
      } catch (Exception e) {
        fail();
      }

      try {
        ps.executeQuery();
      } catch (SQLException e) {
        fail();
      }

      // the connection should not be closed
      assertFalse(connection.isClosed());
    }
  }

    /** Conj-166 Connection error code must be thrown. */
    @Ignore
    public void testAccessDeniedErrorCode() {
        try {
            DriverManager.getConnection(defaultUrl + "&retriesAllDown=6", "foouser", "foopwd");
            fail();
        } catch (SQLException e) {
            System.out.println(e.getSQLState());
            System.out.println(e.getErrorCode());
            assertTrue("28000".equals(e.getSQLState()));
            assertEquals(1045, e.getErrorCode());
        }
    }

    @Ignore
  public void testClearBlacklist() throws Throwable {
    try (Connection connection = getNewConnection(true)) {
      connection.setReadOnly(true);
      int current = getServerId(connection);
      stopProxy(current);
      Statement st = connection.createStatement();
      try {
        st.execute("SELECT 1 ");
        // switch connection to master -> slave blacklisted
      } catch (SQLException e) {
        fail("must not have been here");
      }

      Protocol protocol = getProtocolFromConnection(connection);
      assertTrue(protocol.getProxy().getListener().getBlacklistKeys().size() == 1);
      assureBlackList();
      assertTrue(protocol.getProxy().getListener().getBlacklistKeys().size() == 0);
    }
  }

    @Ignore
    @Test
  public void testCloseFail() throws Throwable {
    assureBlackList();
    Protocol protocol;
    try (Connection connection = getNewConnection(true)) {
      connection.setReadOnly(true);
      int current = getServerId(connection);
      protocol = getProtocolFromConnection(connection);
      assertTrue(
          "Blacklist would normally be zero, but was "
              + protocol.getProxy().getListener().getBlacklistKeys().size(),
          protocol.getProxy().getListener().getBlacklistKeys().size() == 0);
      stopProxy(current);
    }
    // check that after error connection have not been put to blacklist
    assertTrue(protocol.getProxy().getListener().getBlacklistKeys().size() == 0);
  }
}
