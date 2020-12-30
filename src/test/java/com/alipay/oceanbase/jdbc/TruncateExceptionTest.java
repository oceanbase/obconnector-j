package com.alipay.oceanbase.jdbc;

import static org.junit.Assert.*;

import java.sql.*;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class TruncateExceptionTest extends BaseTest {

    /** Tables initialisation. */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("TruncateExceptionTest", "id tinyint");
        createTable("TruncateExceptionTest2",
            "id tinyint not null primary key auto_increment, id2 tinyint ");
    }

    @Test
    public void truncationThrowError() {
        try {
            queryTruncation(true);
            fail("Must have thrown SQLException");
        } catch (SQLException e) {
            // normal error
        }
    }

    @Test
    public void truncationThrowNoError() {
        try {
            ResultSet resultSet = sharedConnection.createStatement().executeQuery(
                "SELECT @@sql_mode");
            resultSet.next();
            // if server is already throwing truncation, cancel test
            Assume.assumeFalse(resultSet.getString(1).contains("STRICT_TRANS_TABLES"));

            queryTruncation(false);
        } catch (SQLException e) {
            e.printStackTrace();

            fail("Must not have thrown exception");
        }
    }

    /**
     * Execute a query with truncated data.
     *
     * @param truncation connection parameter.
     * @throws SQLException if SQLException occur
     */
    public void queryTruncation(boolean truncation) throws SQLException {
    try (Connection connection = setConnection("&jdbcCompliantTruncation=" + truncation)) {
      try (Statement stmt = connection.createStatement()) {
        stmt.execute("INSERT INTO TruncateExceptionTest (id) VALUES (999)");
      }
    }
  }

    @Test
  public void queryTruncationFetch() throws SQLException {
    int[] autoInc = setAutoInc();
    try (Connection connection = setConnection("&jdbcCompliantTruncation=true")) {
      Statement stmt = connection.createStatement();
      stmt.execute("TRUNCATE TABLE TruncateExceptionTest2");
      stmt.setFetchSize(1);
      PreparedStatement pstmt =
          connection.prepareStatement(
              "INSERT INTO TruncateExceptionTest2 (id2) VALUES (?)",
              Statement.RETURN_GENERATED_KEYS);
      pstmt.setInt(1, 45);
      pstmt.addBatch();
      pstmt.setInt(1, 999);
      pstmt.addBatch();
      pstmt.setInt(1, 55);
      pstmt.addBatch();
      try {
        pstmt.executeBatch();
        fail("Must have thrown SQLException");
      } catch (SQLException e) {
        // expected
      }
      // resultSet must have been fetch
      ResultSet rs = pstmt.getGeneratedKeys();
      assertTrue(rs.next());
      assertEquals(autoInc[0] + autoInc[1], rs.getInt(1));
      if (sharedIsRewrite()) {
        // rewritten with semi-colons -> error has stopped
        assertFalse(rs.next());
      } else {
        assertTrue(rs.next());
        assertEquals(autoInc[1] + autoInc[0] * 2, rs.getInt(1));
        assertFalse(rs.next());
      }
    }
  }

    @Test
  public void queryTruncationBatch() throws SQLException {
    int[] autoInc = setAutoInc();
    try (Connection connection =
        setConnection(
            "&jdbcCompliantTruncation=true&useBatchMultiSendNumber=3&profileSql=true&log=true")) {
      Statement stmt = connection.createStatement();
      stmt.execute("TRUNCATE TABLE TruncateExceptionTest2");
      PreparedStatement pstmt =
          connection.prepareStatement(
              "INSERT INTO TruncateExceptionTest2 (id2) VALUES (?)",
              Statement.RETURN_GENERATED_KEYS);
      pstmt.setInt(1, 45);
      pstmt.addBatch();
      pstmt.setInt(1, 46);
      pstmt.addBatch();
      pstmt.setInt(1, 47);
      pstmt.addBatch();
      pstmt.setInt(1, 48);
      pstmt.addBatch();
      pstmt.setInt(1, 999);
      pstmt.addBatch();
      pstmt.setInt(1, 49);
      pstmt.addBatch();
      pstmt.setInt(1, 50);
      pstmt.addBatch();
      try {
        pstmt.executeBatch();
        fail("Must have thrown SQLException");
      } catch (SQLException e) {
        // expected
      }
      // resultSet must have been fetch
      ResultSet rs = pstmt.getGeneratedKeys();
      for (int i = 1; i <= (sharedIsRewrite() ? 4 : 6); i++) {
        assertTrue(rs.next());
        assertEquals(autoInc[1] + autoInc[0] * i, rs.getInt(1));
      }
      assertFalse(rs.next());
    }
  }
}
