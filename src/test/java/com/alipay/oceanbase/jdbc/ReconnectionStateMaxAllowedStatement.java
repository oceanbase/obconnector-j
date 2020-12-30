package com.alipay.oceanbase.jdbc;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import org.junit.Test;

public class ReconnectionStateMaxAllowedStatement extends BaseTest {

    @Test
  public void isolationLevelResets() throws SQLException {
    try (Connection connection = setConnection()) {
      long max = maxPacket(connection);
      if (max > Integer.MAX_VALUE - 10) {
        fail("max_allowed_packet too high for this test");
      }
      connection.prepareStatement("create table if not exists foo (x longblob)").execute();
      connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);

      assertEquals("READ-UNCOMMITTED", level((OceanBaseConnection) connection));
      try (PreparedStatement st = connection.prepareStatement("insert into foo (?)")) {
        st.setBytes(1, data((int) (max + 10)));
        st.execute();
        fail();
      } catch (SQLException e) {
        assertTrue(e.getMessage().contains("max_allowed_packet"));
        // we still have a working connection
        assertTrue(connection.isValid(0));
        // our isolation level must have stay the same
        assertEquals("READ-UNCOMMITTED", level((OceanBaseConnection) connection));
      }
    }
  }

    private String level(OceanBaseConnection connection) throws SQLException {
    String sql = "SELECT @@tx_isolation";
    if (!connection.isServerMariaDb() && connection.versionGreaterOrEqual(8, 0, 3)) {
      sql = "SELECT @@transaction_isolation";
    }
    try (ResultSet rs = connection.prepareStatement(sql).executeQuery()) {
      assertTrue(rs.next());
      return rs.getString(1);
    }
  }

    private long maxPacket(Connection connection) throws SQLException {
    try (ResultSet rs = connection.prepareStatement("select @@max_allowed_packet").executeQuery()) {
      assertTrue(rs.next());
      return rs.getLong(1);
    }
  }

    private byte[] data(int size) {
        byte[] data = new byte[size];
        Arrays.fill(data, (byte) 'a');
        return data;
    }
}
