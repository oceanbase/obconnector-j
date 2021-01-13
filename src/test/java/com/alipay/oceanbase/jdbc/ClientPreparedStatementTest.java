package com.alipay.oceanbase.jdbc;

import static org.junit.Assert.*;

import java.sql.*;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;

public class ClientPreparedStatementTest extends BaseTest {

    @Ignore
    @Test
    public void closedStatement() throws SQLException {

        PreparedStatement preparedStatement = sharedConnection.prepareStatement("SELECT ?");
        preparedStatement.setString(1, "1");
        preparedStatement.execute();

        preparedStatement.setString(1, "1");
        preparedStatement.executeQuery();

        preparedStatement.setString(1, "1");
        preparedStatement.executeUpdate();

        preparedStatement.close();

        try {
            preparedStatement.setString(1, "1");
            preparedStatement.execute();
            fail();
        } catch (SQLException e) {
            // TODO need to optimize next.
            // it will be throw "java.lang.NullPointerException" for this.connection.getProtocol().isOracleMode()
            assertTrue(e.getMessage().contains("is called on closed statement"));
        }

        try {
            preparedStatement.setString(1, "1");
            preparedStatement.executeQuery();
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("is called on closed statement"));
        }

        try {
            preparedStatement.setString(1, "1");
            preparedStatement.executeUpdate();
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("is called on closed statement"));
        }
    }

    @Test
    public void timeoutStatement() throws SQLException {

        PreparedStatement preparedStatement = sharedConnection.prepareStatement("SELECT ?");
        preparedStatement.setQueryTimeout(10);
        preparedStatement.setString(1, "1");
        preparedStatement.execute();

        preparedStatement.setString(1, "1");
        preparedStatement.executeQuery();

        preparedStatement.setString(1, "1");
        preparedStatement.executeUpdate();
    }

    @Test
    public void paramNumber() throws SQLException {
        PreparedStatement preparedStatement = sharedConnection.prepareStatement("SELECT ?, ?");
        try {
            preparedStatement.setString(1, "1");
            preparedStatement.execute();
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("Parameter at position 2 is not set"));
        }
        try {
            preparedStatement.setString(1, "1");
            preparedStatement.executeQuery();
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("Parameter at position 2 is not set"));
        }
        try {
            preparedStatement.setString(1, "1");
            preparedStatement.executeUpdate();
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("Parameter at position 2 is not set"));
        }
    }

    @Test
    public void batchParamNumber() throws SQLException {
        PreparedStatement preparedStatement = sharedConnection.prepareStatement("SELECT ?, ?");
        // batch with no parameter
        assertEquals(0, preparedStatement.executeBatch().length);
        assertEquals(0, preparedStatement.executeLargeBatch().length);

        try {
            preparedStatement.setString(1, "1");
            preparedStatement.addBatch();
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains(
                "You need to set exactly 2 parameters on the prepared statement")
                       || e.getMessage().contains("Parameter at position 2 is not set"));
        }

        try {
            preparedStatement.addBatch("SOME SQL");
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("Cannot do addBatch(String) on preparedStatement"));
        }

        preparedStatement.setString(1, "1");
        preparedStatement.setString(2, "2");
        preparedStatement.addBatch();
        preparedStatement.setString(2, "2");
        preparedStatement.addBatch();
        try {
            preparedStatement.clearParameters();
            preparedStatement.setString(2, "2");
            preparedStatement.addBatch();
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains(
                "You need to set exactly 2 parameters on the prepared statement")
                       || e.getMessage().contains("Parameter at position 1 is not set"));
        }
    }

    @Test
    public void setParameterError() throws SQLException {
        Assume.assumeFalse(sharedOptions().useServerPrepStmts);
        PreparedStatement preparedStatement = sharedConnection.prepareStatement("SELECT ?, ?");
        preparedStatement.setString(1, "a");
        preparedStatement.setString(2, "a");

        try {
            preparedStatement.setString(3, "a");
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains(
                "Could not set parameter at position 3 (values was 'a')"));
        }

        try {
            preparedStatement.setString(0, "a");
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains(
                "Could not set parameter at position 0 (values was 'a')"));
        }
    }

    @Ignore
    @Test
    public void closedBatchError() throws SQLException {
        PreparedStatement preparedStatement = sharedConnection.prepareStatement("SELECT ?, ?");
        preparedStatement.setString(1, "1");
        preparedStatement.setString(2, "1");
        preparedStatement.addBatch();
        preparedStatement.executeBatch();

        preparedStatement.close();

        /* The connection has been set to null after execute preparedStatement.close(). It will be throw
        *  java.lang.NullPointerException for judge this.connection.getProtocol.isOracleMode().
        * **/
        try {
            preparedStatement.setString(1, "2");
            preparedStatement.setString(2, "2");
            preparedStatement.addBatch();
            preparedStatement.executeBatch();
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("Cannot do an operation on a closed statement"));
        }
    }

    @Test
    public void executeLargeBatchError() throws SQLException {
        PreparedStatement preparedStatement = sharedConnection
            .prepareStatement("INSERT INTO unknownTable values (?, ?)");
        preparedStatement.setString(1, "1");
        preparedStatement.setString(2, "1");
        preparedStatement.addBatch();

        try {
            preparedStatement.executeLargeBatch();
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("does not exist"));
        }
    }

    @Test
  public void executeBatchOneByOne() throws SQLException {
    try (Connection connection =
        setConnection(
            "&rewriteBatchedStatements=false&useBulkStmts=false&useBatchMultiSend=false")) {
      Statement stmt = connection.createStatement();
      stmt.execute("CREATE TEMPORARY TABLE executeBatchOneByOne (c1 varchar(16), c2 varchar(16))");
      PreparedStatement preparedStatement =
          connection.prepareStatement("INSERT INTO executeBatchOneByOne values (?, ?)");
      preparedStatement.setString(1, "1");
      preparedStatement.setString(2, "1");
      preparedStatement.addBatch();
      preparedStatement.setQueryTimeout(10);
      assertEquals(1, preparedStatement.executeBatch().length);
    }
  }

    @Test
    public void metaDataForWrongQuery() throws SQLException {
        PreparedStatement preparedStatement = sharedConnection.prepareStatement("WRONG QUERY");
        assertNull(preparedStatement.getMetaData());
    }

    @Test
    public void getMultipleMetaData() throws SQLException {
        PreparedStatement preparedStatement = sharedConnection.prepareStatement("SELECT 1 as a");
        ResultSetMetaData meta = preparedStatement.getMetaData();
        assertEquals("a", meta.getColumnName(1));

        preparedStatement.execute();

        meta = preparedStatement.getMetaData();
        assertEquals("a", meta.getColumnName(1));
    }

    @Test
    public void prepareToString() throws SQLException {
        PreparedStatement preparedStatement = sharedConnection
            .prepareStatement("SELECT ? as a, ? as b");
        preparedStatement.setString(1, "a");
        preparedStatement.setNull(2, Types.VARCHAR);
        assertEquals("sql : 'SELECT ? as a, ? as b', parameters : ['a',<null>]",
            preparedStatement.toString());
    }
}
