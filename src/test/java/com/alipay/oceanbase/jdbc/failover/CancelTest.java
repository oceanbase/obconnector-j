package com.alipay.oceanbase.jdbc.failover;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.*;

import com.alipay.oceanbase.jdbc.internal.util.constant.HaMode;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class CancelTest extends BaseMultiHostTest {

    /** Initialisation. */
    @BeforeClass()
    public static void beforeClass2() {
        proxyUrl = proxyGaleraUrl;
        Assume.assumeTrue(initialGaleraUrl != null);
    }

    /** Initialisation. */
    @Before
    public void init() {
        defaultUrl = initialGaleraUrl;
        currentType = HaMode.LOADBALANCE;
    }

    @Test(expected = SQLTimeoutException.class)
  public void timeoutSleep() throws Exception {
    try (Connection connection = getNewConnection(false)) {
      PreparedStatement stmt =
          connection.prepareStatement(
              "select * from information_schema.columns as c1,"
                  + "information_schema.tables as t1, "
                  + "information_schema.tables as t2");
      stmt.setQueryTimeout(1);
      stmt.execute();
    }
  }

    @Test
  public void noTimeoutSleep() throws Exception {
    try (Connection connection = getNewConnection(false)) {
      Statement stmt = connection.createStatement();
      stmt.setQueryTimeout(1);
      stmt.execute("select sleep(0.5)");
    }
  }

    @Test
  public void cancelIdleStatement() throws Exception {
    try (Connection connection = getNewConnection(false)) {
      Statement stmt = connection.createStatement();
      stmt.cancel();
      ResultSet rs = stmt.executeQuery("select 1");
      assertTrue(rs.next());
      assertEquals(rs.getInt(1), 1);
    }
  }
}
