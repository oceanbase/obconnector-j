package com.alipay.oceanbase.jdbc.failover;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.*;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import com.alipay.oceanbase.jdbc.BaseTest;

public class AllowMasterDownTest extends BaseTest {

    private String masterDownUrl;

    /** Initialisation. */
    @Before
    public void init() {
        Assume.assumeTrue(testSingleHost);
        Assume.assumeTrue(System.getenv("SKYSQL") == null);
        if (testSingleHost) {
            masterDownUrl = "jdbc:oceanbase:replication//"
                            + hostname
                            + ":9999"
                            + ","
                            + hostname
                            + ((port == 0) ? "" : ":" + port)
                            + "/"
                            + database
                            + "?user="
                            + username
                            + ((password != null) ? "&password=" + password : "")
                            + ((options.useSsl != null) ? "&useSsl" + options.useSsl : "")
                            + ((options.serverSslCert != null) ? "&serverSslCert="
                                                                 + options.serverSslCert : "")
                            + "&retriesAllDown=10&allowMasterDownConnection";
        }
    }

    @Test
  public void masterDownReadOnlyAvailable() throws SQLException {

    try (Connection connection = DriverManager.getConnection(masterDownUrl)) {
      Assert.assertFalse(connection.isReadOnly());
      connection.isValid(0);
      connection.setReadOnly(true);
      try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT 1")) {
        preparedStatement.executeQuery();
      }
    }
  }

    @Test
  public void masterDownThrowError() throws SQLException {
    try (Connection connection = DriverManager.getConnection(masterDownUrl)) {
      Statement statement = connection.createStatement();
      try {
        statement.executeQuery("SELECT 1");
        Assert.fail("Must have thrown a connectionException");
      } catch (SQLException sqle) {
        Assert.assertTrue(sqle instanceof SQLNonTransientConnectionException);
      }
    }
  }

    @Test
  public void masterDownAutoCommit() {
    try (Connection connection = DriverManager.getConnection(masterDownUrl)) {
      connection.getAutoCommit();
    } catch (SQLException sqle) {
      Assert.assertTrue(sqle instanceof SQLNonTransientConnectionException);
    }
  }

    @Test
  public void masterDownGetMeta() {
    try (Connection connection = DriverManager.getConnection(masterDownUrl)) {
      createTable(
          "checkMetaData",
          "xx tinyint(1) primary key auto_increment, yy year(4), zz bit, uu smallint");
      DatabaseMetaData meta = connection.getMetaData();
      meta.getColumns(null, null, "checkMetaData", null);
      Assert.fail("Must have thrown a connectionException");
    } catch (SQLException sqle) {
      Assert.assertTrue(sqle instanceof SQLNonTransientConnectionException);
    }
  }

    @Test
  public void masterDownGetMetaRead() throws SQLException {
    try (Connection connection = DriverManager.getConnection(masterDownUrl)) {
      createTable(
          "checkMetaData",
          "xx tinyint(1) primary key auto_increment, yy year(4), zz bit, uu smallint");
      connection.setReadOnly(true);
      DatabaseMetaData meta = connection.getMetaData();
      ResultSet rs = meta.getColumns(null, null, "checkMetaData", null);
      assertTrue(rs.next());
      assertEquals("BIT", rs.getString(6));
      assertTrue(rs.next());
      assertEquals("YEAR", rs.getString(6));
      assertEquals(null, rs.getString(7)); // column size
      assertEquals(null, rs.getString(9)); // decimal digit
    }
  }

    @Test
  public void masterDownCreateStatement() throws SQLException {
    try (Connection connection = DriverManager.getConnection(masterDownUrl)) {
      Statement stmt = connection.createStatement();
      try {
        stmt.executeQuery("SELECT 1");
      } catch (SQLException sqle) {
        Assert.assertTrue(sqle instanceof SQLNonTransientConnectionException);
      }
    }
  }

    @Test
  public void masterDownCreatePrepareStatement() throws SQLException {
    try (Connection connection = DriverManager.getConnection(masterDownUrl)) {
      try (PreparedStatement prepare = connection.prepareStatement("SELECT ?")) {
        prepare.setString(1, "1");
        try {
          prepare.executeQuery();
        } catch (SQLException sqle) {
          Assert.assertTrue(sqle instanceof SQLNonTransientConnectionException);
        }
      }
    }
  }

    @Test
  public void masterDownReadProperties() throws SQLException {
    try (Connection connection = DriverManager.getConnection(masterDownUrl)) {
      String db = connection.getCatalog();
      Assert.assertFalse(connection.isReadOnly());
      Assert.assertFalse(connection.isClosed());
      connection.getNetworkTimeout();
    }
  }

    @Test
  public void masterDownTransactionIsolation() throws SQLException {
    try (Connection connection = DriverManager.getConnection(masterDownUrl)) {
      try {
        connection.getTransactionIsolation();
      } catch (SQLException sqle) {
        Assert.assertTrue(sqle instanceof SQLNonTransientConnectionException);
      }
    }
  }

    @Test
  public void masterDownRollback() throws SQLException {
    try (Connection connection = DriverManager.getConnection(masterDownUrl)) {
      try {
        connection.rollback();
      } catch (SQLException sqle) {
        Assert.assertTrue(sqle instanceof SQLNonTransientConnectionException);
      }
    }
  }
}
