package com.alipay.oceanbase.jdbc;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class DataSourceTest extends BaseTest {

    protected static final String defConnectToIP = null;
    protected static String       connectToIP;

    /** Initialisation. */
    @BeforeClass
    public static void beforeClassDataSourceTest() {
        connectToIP = System.getProperty("testConnectToIP", defConnectToIP);
    }

    @Test
  public void testDataSource() throws SQLException {
    Assume.assumeFalse(options.useSsl != null && options.useSsl);

    OceanBaseDataSource ds =
        new OceanBaseDataSource(hostname == null ? "localhost" : hostname, port, database);

    try (Connection connection = ds.getConnection(username, password)) {
      assertEquals(connection.isValid(0), true);
    }
  }

    @Test
  public void testDataSource2() throws SQLException {
    Assume.assumeFalse(options.useSsl != null && options.useSsl);
    OceanBaseDataSource ds =
        new OceanBaseDataSource(hostname == null ? "localhost" : hostname, port, database);
    try (Connection connection = ds.getConnection(username, password)) {
      assertEquals(connection.isValid(0), true);
    }
  }

    @Test
  public void testDataSourceEmpty() throws SQLException {
    Assume.assumeFalse(options.useSsl != null && options.useSsl);
    OceanBaseDataSource ds = new OceanBaseDataSource();
    ds.setDatabaseName(database);
    ds.setPort(port);
    ds.setServerName(hostname == null ? "localhost" : hostname);
    try (Connection connection = ds.getConnection(username, password)) {
      assertEquals(connection.isValid(0), true);
    }
  }

    @Test
  public void testDataSourceTimeout() throws SQLException {
    OceanBaseDataSource ds = new OceanBaseDataSource(hostname, port, database);
    assertEquals(ds.getLoginTimeout(), 30);
    ds.setLoginTimeout(10);
    assertEquals(ds.getLoginTimeout(), 10);
    ds.initialize();
    assertEquals(ds.getUrlParser().getOptions().connectTimeout, 10_000);
  }

    @Test
  public void testDataSourceTimeout2() throws SQLException {
    OceanBaseDataSource ds = new OceanBaseDataSource();
    ds.setUrl("jdbc:oceanbase://localhost/test?connectTimeout=2000");
    ds.initialize();
    assertEquals(ds.getUrlParser().getOptions().connectTimeout, 2_000);
  }

    @Test
  public void testDataSourceTimeout3() throws SQLException {
    OceanBaseDataSource ds = new OceanBaseDataSource();
    ds.setUrl("jdbc:oceanbase://localhost/test?connectTimeout=2000");
    ds.initialize();
    assertEquals(ds.getLoginTimeout(), 2);
    assertEquals(ds.getUrlParser().getOptions().connectTimeout, 2_000);
  }

    @Test
  public void testDataSourceTimeout4() throws SQLException {
    OceanBaseDataSource ds = new OceanBaseDataSource();
    ds.setUrl("jdbc:oceanbase://localhost/test");
    ds.setLoginTimeout(10);
    ds.initialize();
    assertEquals(ds.getLoginTimeout(), 10);
    assertEquals(ds.getUrlParser().getOptions().connectTimeout, 10_000);
  }

    /**
     * Conj-80.
     *
     * @throws SQLException exception
     */
    @Test
  public void setDatabaseNameTest() throws SQLException {
    Assume.assumeFalse(options.useSsl != null && options.useSsl);
    Assume.assumeTrue(System.getenv("MAXSCALE_VERSION") == null && System.getenv("SKYSQL") == null);
    OceanBaseDataSource ds =
        new OceanBaseDataSource(hostname == null ? "localhost" : hostname, port, database);
    try (Connection connection = ds.getConnection(username, password)) {
      connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS test2");
      ds.setDatabaseName("test2");

      try (Connection connection2 = ds.getConnection(username, password)) {
        assertEquals("test2", ds.getDatabaseName());
        assertEquals(ds.getDatabaseName(), connection2.getCatalog());
        connection2.createStatement().execute("DROP DATABASE IF EXISTS test2");
      }
    }
  }

    /**
     * Conj-80.
     *
     * @throws SQLException exception
     */
    @Test
  public void setServerNameTest() throws SQLException {
    Assume.assumeTrue(connectToIP != null);
    OceanBaseDataSource ds =
        new OceanBaseDataSource(hostname == null ? "localhost" : hostname, port, database);
    try (Connection connection = ds.getConnection(username, password)) {
      ds.setServerName(connectToIP);

      try (Connection connection2 = ds.getConnection(username, password)) {
        Statement stmt = connection2.createStatement();
        assertTrue(stmt.execute("Select 1"));
      }
    }
  }

    /**
     * Conj-80.
     *
     * @throws SQLException exception
     */
    @Test(timeout = 50000) // unless port 3307 can be used
  public void setPortTest() throws SQLException {
    Assume.assumeFalse(options.useSsl != null && options.useSsl);
    Assume.assumeFalse("true".equals(System.getenv("AURORA")));
    OceanBaseDataSource ds =
        new OceanBaseDataSource(hostname == null ? "localhost" : hostname, port, database);
    try (Connection connection2 = ds.getConnection(username, password)) {
      // delete blacklist, because can failover on 3306 is filled
      assureBlackList(connection2);
    }

    ds.setPort(3407);

    // must throw SQLException
    try {
      ds.getConnection(username, password);
      fail();
    } catch (SQLException e) {
      // normal error
    }
  }

    /**
     * Conj-123:Session variables lost and exception if set via
     * MariaDbDataSource.setProperties/setURL.
     *
     * @throws SQLException exception
     */
    @Test
  @SuppressWarnings("deprecation")
  public void setPropertiesTest() throws SQLException {
    Assume.assumeFalse(options.useSsl != null && options.useSsl);
    OceanBaseDataSource ds =
        new OceanBaseDataSource(hostname == null ? "localhost" : hostname, port, database);
    ds.setProperties("sessionVariables=sql_mode='PIPES_AS_CONCAT'");
    try (Connection connection = ds.getConnection(username, password)) {
      ResultSet rs = connection.createStatement().executeQuery("SELECT @@sql_mode");
      if (rs.next()) {
        assertEquals("PIPES_AS_CONCAT", rs.getString(1));
        ds.setUrl(connUri + "&sessionVariables=sql_mode='ALLOW_INVALID_DATES'");
        try (Connection connection2 = ds.getConnection()) {
          rs = connection2.createStatement().executeQuery("SELECT @@sql_mode");
          assertTrue(rs.next());
          assertEquals("ALLOW_INVALID_DATES", rs.getString(1));
        }
      } else {
        fail();
      }
    }
  }

    @Test
    public void setLoginTimeOut() {
        OceanBaseDataSource ds = new OceanBaseDataSource(hostname == null ? "localhost" : hostname,
            port, database);
        assertEquals(30, ds.getLoginTimeout());
        ds.setLoginTimeout(10);
        assertEquals(10, ds.getLoginTimeout());
    }
}
