package com.alipay.oceanbase.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.*;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class ByteTest extends BaseTest {

    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("ByteTest", "test tinyint, test2 TINYBLOB");
    }

    @Test
  public void byteSending() throws SQLException {
    Assume.assumeFalse(sharedUsePrepare());
    try (PreparedStatement prep =
        sharedConnection.prepareStatement("INSERT INTO ByteTest value " + "(?, ?)")) {
      prep.setByte(1, (byte) -6);
      prep.setByte(2, (byte) -6);
      prep.execute();
      Statement stmt = sharedConnection.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT * FROM ByteTest");
      assertTrue(rs.next());
      assertEquals(rs.getByte(1), (byte) -6);
      assertEquals(rs.getByte(2), (byte) -6);
      assertEquals(rs.getInt(1), -6);
      assertEquals(rs.getInt(2), -6);
    }
  }

    @Test
  public void byteSendingBinary() throws SQLException {
    try (Connection conn = setConnection("&useServerPrepStmts")) {
      try (PreparedStatement prep =
          conn.prepareStatement("INSERT INTO ByteTest value " + "(?, ?)")) {
        prep.setByte(1, (byte) -6);
        prep.setByte(2, (byte) -6);
        prep.execute();
        Statement stmt = sharedConnection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM ByteTest");
        assertTrue(rs.next());
        assertEquals(rs.getByte(1), (byte) -6);
        assertEquals(rs.getByte(2), (byte) -6);
        assertEquals(rs.getInt(1), -6);
        assertEquals(rs.getInt(2), -6);
      }
    }
  }
}
