package com.alipay.oceanbase.jdbc;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;

import com.alipay.oceanbase.jdbc.internal.protocol.Protocol;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class ErrorMessageTest extends BaseTest {

    /**
     * Initialisation.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("testErrorMessage",
            "id int not null primary key auto_increment, test varchar(10), test2 int");
    }

    @Test
  public void testSmallRewriteErrorMessage() {
    try (Connection connection =
        setBlankConnection("&rewriteBatchedStatements=true" + "&dumpQueriesOnException")) {
      executeBatchWithException(connection);
      fail("Must Have thrown error");
    } catch (SQLException sqle) {
      assertTrue(
          sqle.getCause()
              .getMessage()
              .contains("INSERT INTO testErrorMessage(test, test2) values (?, ?)"));
    }
  }

    @Test
  public void testSmallMultiBatchErrorMessage() throws SQLException {
    try (Connection connection =
        setConnection("&allowMultiQueries=true&useServerPrepStmts=false&dumpQueriesOnException")) {
      executeBatchWithException(connection);
      fail("Must Have thrown error");
    } catch (SQLException sqle) {
      if ((isMariadbServer() && minVersion(10, 2) && sharedOptions().useBulkStmts)
          || sharedOptions().useServerPrepStmts
          || sharedOptions().rewriteBatchedStatements) {
        assertTrue(
            "message : " + sqle.getCause().getMessage(),
            sqle.getCause()
                .getMessage()
                .contains("INSERT INTO testErrorMessage(test, test2) values (?, ?)"));
      } else {
        if (!sharedOptions().useBatchMultiSend) {
          assertTrue(
              "message : " + sqle.getCause().getMessage(),
              sqle.getCause()
                  .getMessage()
                  .contains(
                      "INSERT INTO testErrorMessage(test, test2) values (?, ?), "
                          + "parameters ['more than 10 characters to provoc error',10]"));
        } else {
          assertTrue(
              sqle.getCause()
                  .getMessage()
                  .contains(
                      "INSERT INTO testErrorMessage(test, test2) values "
                          + "('more than 10 characters to provoc error', 10)"));
        }
      }
    }
  }

    @Test
  public void testSmallPrepareErrorMessage() {
    try (Connection connection =
        setBlankConnection("&useBatchMultiSend=false" + "&dumpQueriesOnException")) {
      executeBatchWithException(connection);
      fail("Must Have thrown error");
    } catch (SQLException sqle) {
      assertTrue(
          sqle.getCause()
              .getMessage()
              .contains("INSERT INTO testErrorMessage(test, test2) values (?, ?)"));
    }
  }

    @Test
  public void testSmallBulkErrorMessage() throws SQLException {
    Assume.assumeFalse(sharedIsAurora());
    try (Connection connection = setConnection("&useBatchMultiSend=true&dumpQueriesOnException")) {
      executeBatchWithException(connection);
      fail("Must Have thrown error");
    } catch (SQLException sqle) {
      if ((isMariadbServer() && minVersion(10, 2) && sharedOptions().useBulkStmts)
          || sharedOptions().useServerPrepStmts
          || sharedOptions().rewriteBatchedStatements) {
        assertTrue(
            "message : " + sqle.getCause().getMessage(),
            sqle.getCause()
                .getMessage()
                .contains("INSERT INTO testErrorMessage(test, test2) values (?, ?)"));
      } else {
        assertTrue(
            sqle.getCause()
                .getMessage()
                .contains(
                    "INSERT INTO testErrorMessage(test, test2) values "
                        + "('more than 10 characters to provoc error', 10)"));
      }
    }
  }

    @Test
  public void testSmallPrepareBulkErrorMessage() {
    Assume.assumeFalse(sharedIsAurora());
    try (Connection connection =
        setBlankConnection(
            "&useBatchMultiSend=true&useServerPrepStmts=true&dumpQueriesOnException")) {
      executeBatchWithException(connection);
      fail("Must Have thrown error");
    } catch (SQLException sqle) {
      assertTrue(
          sqle.getCause()
              .getMessage()
              .contains("INSERT INTO testErrorMessage(test, test2) values " + "(?, ?)"));
    }
  }

    @Test
  public void testBigRewriteErrorMessage() {
    try (Connection connection =
        setBlankConnection("&rewriteBatchedStatements=true" + "&dumpQueriesOnException")) {
      executeBigBatchWithException(connection);
      fail("Must Have thrown error");
    } catch (SQLException sqle) {
      assertTrue(
          sqle.getCause()
              .getMessage()
              .contains("INSERT INTO testErrorMessage(test, test2) values (?, ?)"));
    }
  }

    @Test
  public void testBigMultiErrorMessage() throws SQLException {
    try (Connection connection =
        setConnection("&allowMultiQueries=true&useServerPrepStmts=false&dumpQueriesOnException")) {
      executeBigBatchWithException(connection);
      fail("Must Have thrown error");
    } catch (SQLException sqle) {
      if ((isMariadbServer() && minVersion(10, 2) && sharedOptions().useBulkStmts)
          || sharedOptions().useServerPrepStmts
          || sharedOptions().rewriteBatchedStatements) {
        assertTrue(
            "message : " + sqle.getCause().getMessage(),
            sqle.getCause()
                .getMessage()
                .contains("INSERT INTO testErrorMessage(test, test2) values (?, ?)"));
      } else {
        if (!sharedOptions().useBatchMultiSend) {
          assertTrue(
              "message : " + sqle.getCause().getMessage(),
              sqle.getCause()
                  .getMessage()
                  .contains(
                      "INSERT INTO testErrorMessage(test, test2) "
                          + "values (?, ?), parameters ['more than 10 characters to provoc error',200]"));
        } else {
          assertTrue(
              "message : " + sqle.getCause().getMessage(),
              sqle.getCause()
                  .getMessage()
                  .contains(
                      "INSERT INTO testErrorMessage(test, test2) values "
                          + "('more than 10 characters to provoc error', 200)"));
        }
      }
    }
  }

    @Test
  public void testBigPrepareErrorMessage() throws SQLException {
    try (Connection connection =
        setBlankConnection("&useBatchMultiSend=false" + "&dumpQueriesOnException")) {
      executeBigBatchWithException(connection);
      fail("Must Have thrown error");
    } catch (SQLException sqle) {
      sqle.printStackTrace();
      if (isMariadbServer() && minVersion(10, 2)) {
        assertTrue(
            "message : " + sqle.getCause().getMessage(),
            sqle.getCause()
                .getMessage()
                .contains("INSERT INTO testErrorMessage(test, test2) values (?, ?)"));
      } else {
        assertTrue(
            "message : " + sqle.getCause().getMessage(),
            sqle.getCause()
                .getMessage()
                .contains(
                    "INSERT INTO testErrorMessage(test, test2) values (?, ?), "
                        + "parameters ['more than 10 characters to provoc error',200]"));
      }
    }
  }

    @Test
  public void testBigBulkErrorMessage() throws SQLException {
    Assume.assumeFalse(sharedIsAurora());
    try (Connection connection =
        setConnection("&useBatchMultiSend=true" + "&dumpQueriesOnException")) {
      executeBigBatchWithException(connection);
      fail("Must Have thrown error");
    } catch (SQLException sqle) {
      if ((isMariadbServer() && minVersion(10, 2) && sharedOptions().useBulkStmts
          || sharedOptions().useServerPrepStmts
          || sharedOptions().rewriteBatchedStatements)) {
        assertTrue(
            "message : " + sqle.getCause().getMessage(),
            sqle.getCause()
                .getMessage()
                .contains("INSERT INTO testErrorMessage(test, test2) values (?, ?)"));
      } else {
        assertTrue(
            sqle.getCause()
                .getMessage()
                .contains(
                    "INSERT INTO testErrorMessage(test, test2) values "
                        + "('more than 10 characters to provoc error', 200)"));
      }
    }
  }

    @Test
  public void testBigBulkErrorPrepareMessage() {
    Assume.assumeFalse(sharedIsAurora());
    try (Connection connection =
        setBlankConnection(
            "&useBatchMultiSend=true&useServerPrepStmts=true&dumpQueriesOnException")) {
      executeBigBatchWithException(connection);
      fail("Must Have thrown error");
    } catch (SQLException sqle) {
      assertTrue(
          sqle.getCause()
              .getMessage()
              .contains("INSERT INTO testErrorMessage(test, test2) values (?, ?)"));
    }
  }

    private void executeBatchWithException(Connection connection) throws SQLException {
    connection.createStatement().execute("TRUNCATE testErrorMessage");
    try (PreparedStatement preparedStatement =
        connection.prepareStatement("INSERT INTO testErrorMessage(test, test2) values (?, ?)")) {
      for (int i = 0; i < 10; i++) {
        preparedStatement.setString(1, "whoua" + i);
        preparedStatement.setInt(2, i);
        preparedStatement.addBatch();
      }
      preparedStatement.setString(1, "more than 10 characters to provoc error");
      preparedStatement.setInt(2, 10);

      preparedStatement.addBatch();
      preparedStatement.executeBatch();
    } catch (SQLException e) {
      // to be sure that packets are ok
      connection.createStatement().execute("SELECT 1");
      throw e;
    }
  }

    private void executeBigBatchWithException(Connection connection) throws SQLException {
    connection.createStatement().execute("TRUNCATE testErrorMessage");
    try (PreparedStatement preparedStatement =
        connection.prepareStatement("INSERT INTO testErrorMessage(test, test2) values (?, ?)")) {
      for (int i = 0; i < 200; i++) {
        preparedStatement.setString(1, "whoua" + i);
        preparedStatement.setInt(2, i);
        preparedStatement.addBatch();
      }
      preparedStatement.setString(1, "more than 10 characters to provoc error");
      preparedStatement.setInt(2, 200);
      preparedStatement.addBatch();
      preparedStatement.executeBatch();
    } catch (SQLException e) {
      // to be sure that packets are ok
      connection.createStatement().execute("SELECT 1");
      throw e;
    }
  }

    @Test
  public void testFailOverKillCmd() throws Throwable {
    Assume.assumeTrue(System.getenv("MAXSCALE_VERSION") == null && System.getenv("SKYSQL") == null);
    Assume.assumeTrue(isMariadbServer());
    DataSource ds =
        new OceanBaseDataSource(
            "jdbc:oceanbase:failover//"
                + ((hostname != null) ? hostname : "localhost")
                + ":"
                + port
                + ","
                + ((hostname != null) ? hostname : "localhost")
                + ":"
                + port
                + "/"
                + database
                + "?user="
                + username
                + (password != null ? "&password=" + password : "")
                + ((options.useSsl != null) ? "&useSsl=" + options.useSsl : "")
                + ((options.serverSslCert != null)
                    ? "&serverSslCert=" + options.serverSslCert
                    : ""));

    try (Connection connection = ds.getConnection()) {
      Protocol protocol = getProtocolFromConnection(connection);
      Statement stmt = connection.createStatement();
      long threadId = protocol.getServerThreadId();
      try {
        stmt.executeQuery("KILL " + threadId);
        stmt.executeQuery("SELECT 1");
        long newThreadId = protocol.getServerThreadId();
        assertNotEquals(threadId, newThreadId);
        PreparedStatement preparedStatement = connection.prepareStatement("KILL ?");
        preparedStatement.setLong(1, newThreadId);
        preparedStatement.execute();

        stmt.executeQuery("SELECT 1");
        long anotherNewThreadId = protocol.getServerThreadId();
        assertNotEquals(anotherNewThreadId, newThreadId);
      } catch (SQLException sqle) {
        // can occur in rare case, if KILL doesn't send error packet
        // not considered as error.
      }
    }
  }

    @Test
  public void testSmallRewriteErrorMessageNoBulk() {
    try (Connection connection =
        setBlankConnection(
            "&rewriteBatchedStatements=true&useBulkStmts=false&dumpQueriesOnException")) {
      executeBatchWithException(connection);
      fail("Must Have thrown error");
    } catch (SQLException sqle) {
      assertTrue(
          sqle.getCause()
              .getMessage()
              .contains("INSERT INTO testErrorMessage(test, test2) values (?, ?)"));
    }
  }

    @Test
  public void testSmallMultiBatchErrorMessageNoBulk() {
    try (Connection connection =
        setBlankConnection(
            "&allowMultiQueries=true&useServerPrepStmts=false&useBulkStmts=false"
                + "&dumpQueriesOnException")) {
      executeBatchWithException(connection);
      fail("Must Have thrown error");
    } catch (SQLException sqle) {
      if (!sharedIsAurora()) {
        assertTrue(
            "message : " + sqle.getCause().getMessage(),
            sqle.getCause()
                .getMessage()
                .contains(
                    "INSERT INTO testErrorMessage(test, test2) values "
                        + "('more than 10 characters to provoc error', 10)"));
      }
    }
  }

    @Test
  public void testSmallPrepareErrorMessageNoBulk() {
    try (Connection connection =
        setBlankConnection("&useBatchMultiSend=false&useBulkStmts=false&dumpQueriesOnException")) {
      executeBatchWithException(connection);
      fail("Must Have thrown error");
    } catch (SQLException sqle) {
      assertTrue(
          sqle.getCause()
              .getMessage()
              .contains(
                  "INSERT INTO testErrorMessage(test, test2) values (?, ?), "
                      + "parameters ['more than 10 characters to provoc error',10]"));
    }
  }

    @Test
  public void testSmallBulkErrorMessageNoBulk() {
    Assume.assumeFalse(sharedIsAurora());
    try (Connection connection =
        setBlankConnection(
            "&useBatchMultiSend=true&useBulkStmts=false" + "&dumpQueriesOnException")) {
      executeBatchWithException(connection);
      fail("Must Have thrown error");
    } catch (SQLException sqle) {
      assertTrue(
          sqle.getCause()
              .getMessage()
              .contains(
                  "INSERT INTO testErrorMessage(test, test2) values "
                      + "('more than 10 characters to provoc error', 10)"));
    }
  }

    @Test
  public void testSmallPrepareBulkErrorMessageNoBulk() {
    Assume.assumeFalse(sharedIsAurora());
    try (Connection connection =
        setBlankConnection(
            "&useBatchMultiSend=true&useServerPrepStmts=true&useBulkStmts=false"
                + "&dumpQueriesOnException")) {
      executeBatchWithException(connection);
      fail("Must Have thrown error");
    } catch (SQLException sqle) {
      assertTrue(
          sqle.getCause()
              .getMessage()
              .contains("INSERT INTO testErrorMessage(test, test2) values " + "(?, ?)"));
    }
  }

    @Test
  public void testBigRewriteErrorMessageNoBulk() {
    try (Connection connection =
        setBlankConnection(
            "&rewriteBatchedStatements=true&useBulkStmts=false&dumpQueriesOnException")) {
      executeBigBatchWithException(connection);
      fail("Must Have thrown error");
    } catch (SQLException sqle) {
      assertTrue(
          sqle.getCause()
              .getMessage()
              .contains("INSERT INTO testErrorMessage(test, test2) values (?, ?)"));
    }
  }

    @Test
  public void testBigMultiErrorMessageNoBulk() {
    try (Connection connection =
        setBlankConnection(
            "&allowMultiQueries=true&useServerPrepStmts=false&useBulkStmts=false"
                + "&dumpQueriesOnException")) {
      executeBigBatchWithException(connection);
      fail("Must Have thrown error");
    } catch (SQLException sqle) {
      if (!sharedIsAurora()) {
        assertTrue(
            "message : " + sqle.getCause().getMessage(),
            sqle.getCause()
                .getMessage()
                .contains(
                    "INSERT INTO testErrorMessage(test, test2) values "
                        + "('more than 10 characters to provoc error', 200)"));
      } else {
        assertTrue(
            "message : " + sqle.getCause().getMessage(),
            sqle.getCause()
                .getMessage()
                .contains(
                    "INSERT INTO testErrorMessage(test, test2) values (?, ?), parameters "
                        + "['more than 10 characters to provoc error',200]"));
      }
    }
  }

    @Test
  public void testBigPrepareErrorMessageNoBulk() {
    try (Connection connection =
        setBlankConnection("&useBatchMultiSend=false&useBulkStmts=false&dumpQueriesOnException")) {
      executeBigBatchWithException(connection);
      fail("Must Have thrown error");
    } catch (SQLException sqle) {
      assertTrue(
          "message : " + sqle.getCause().getMessage(),
          sqle.getCause()
              .getMessage()
              .contains(
                  "INSERT INTO testErrorMessage(test, test2) values (?, ?), parameters "
                      + "['more than 10 characters to provoc error',200]"));
    }
  }

    @Test
  public void testBigBulkErrorMessageNoBulk() {
    Assume.assumeFalse(sharedIsAurora());
    try (Connection connection =
        setBlankConnection(
            "&useBatchMultiSend=true&useBulkStmts=false" + "&dumpQueriesOnException")) {
      executeBigBatchWithException(connection);
      fail("Must Have thrown error");
    } catch (SQLException sqle) {
      assertTrue(
          sqle.getCause()
              .getMessage()
              .contains(
                  "INSERT INTO testErrorMessage(test, test2) values ("
                      + "'more than 10 characters to provoc error', 200)"));
    }
  }

    @Test
  public void testBigBulkErrorPrepareMessageNoBulk() {
    Assume.assumeFalse(sharedIsAurora());
    try (Connection connection =
        setBlankConnection(
            "&useBatchMultiSend=true&useServerPrepStmts=true&useBulkStmts=false"
                + "&dumpQueriesOnException")) {
      executeBigBatchWithException(connection);
      fail("Must Have thrown error");
    } catch (SQLException sqle) {
      assertTrue(
          sqle.getCause()
              .getMessage()
              .contains("INSERT INTO testErrorMessage(test, test2) values (?, ?)"));
    }
  }
}
