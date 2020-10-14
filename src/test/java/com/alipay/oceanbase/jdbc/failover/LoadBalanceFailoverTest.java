package com.alipay.oceanbase.jdbc.failover;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import com.alipay.oceanbase.jdbc.internal.util.constant.HaMode;
import org.junit.*;

/**
 * Exemple mvn test
 * -DdefaultLoadbalanceUrl=jdbc:oceanbase:loadbalance//localhost:3306,localhost:3307/test?user=root.
 */
public class LoadBalanceFailoverTest extends BaseMultiHostTest {

    /** Initialisation. */
    @BeforeClass()
    public static void beforeClass2() {
        proxyUrl = proxyLoadbalanceUrl;
        Assume.assumeTrue(initialLoadbalanceUrl != null);
    }

    /** Initialisation. */
    @Before
    public void init() {
        defaultUrl = initialLoadbalanceUrl;
        currentType = HaMode.LOADBALANCE;
    }

    @Test
  public void failover() throws Throwable {
    try (Connection connection = getNewConnection("&retriesAllDown=6", true)) {
      int master1ServerId = getServerId(connection);
      stopProxy(master1ServerId);
      connection.createStatement().executeQuery("SELECT 1");
      int secondServerId = getServerId(connection);
      Assert.assertNotEquals(master1ServerId, secondServerId);
    }
  }

    @Test
  public void randomConnection() throws Throwable {
    Assume.assumeTrue(initialLoadbalanceUrl.contains("loadbalance"));
    Map<String, MutableInt> connectionMap = new HashMap<>();
    for (int i = 0; i < 20; i++) {
      try (Connection connection = getNewConnection(false)) {
        int serverId = getServerId(connection);
        MutableInt count = connectionMap.get(String.valueOf(serverId));
        if (count == null) {
          connectionMap.put(String.valueOf(serverId), new MutableInt());
        } else {
          count.increment();
        }
      }
    }

    Assert.assertTrue(connectionMap.size() >= 2);
    for (String key : connectionMap.keySet()) {
      Integer connectionCount = connectionMap.get(key).get();
      Assert.assertTrue(connectionCount > 1);
    }
  }

    @Test
  public void testReadonly() {
    try (Connection connection = getNewConnection(false)) {
      connection.setReadOnly(true);

      Statement stmt = connection.createStatement();
      stmt.execute("drop table  if exists multinode");
      stmt.execute(
          "create table multinode (id int not null primary key auto_increment, test VARCHAR(10))");
    } catch (SQLException sqle) {
      // normal exception
    }
  }

    class MutableInt {

        private int value = 1; // note that we start at 1 since we're counting

        public void increment() {
            ++value;
        }

        public int get() {
            return value;
        }
    }
}
