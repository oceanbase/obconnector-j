package com.alipay.oceanbase.jdbc.failover;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.alipay.oceanbase.jdbc.HostAddress;
import com.alipay.oceanbase.jdbc.UrlParser;
import com.alipay.oceanbase.jdbc.internal.protocol.Protocol;
import com.alipay.oceanbase.jdbc.internal.util.constant.HaMode;
import com.alipay.oceanbase.jdbc.internal.util.scheduler.OceanBaseThreadFactory;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for sequential connection exemple mvn test
 * -DdefaultGaleraUrl=jdbc:oceanbase:sequential//localhost:3306,localhost:3307/test?user=root.
 */
public class SequentialFailoverTest extends BaseMultiHostTest {

    /** Initialisation. */
    @BeforeClass()
    public static void beforeClass2() {
        proxyUrl = proxySequentialUrl;
        Assume.assumeTrue(initialSequentialUrl != null);
    }

    /** Initialisation. */
    @Before
    public void init() {
        defaultUrl = initialSequentialUrl;
        currentType = HaMode.SEQUENTIAL;
    }

    @Test
  public void connectionOrder() throws Throwable {

    Assume.assumeTrue(!initialGaleraUrl.contains("failover"));
    UrlParser urlParser = UrlParser.parse(initialGaleraUrl);
    for (int i = 0; i < urlParser.getHostAddresses().size(); i++) {
      int serverNb;
      try (Connection connection = getNewConnection(true)) {
        serverNb = getServerId(connection);
        assertTrue(serverNb == i + 1);
      }
      stopProxy(serverNb);
    }
  }

    @Test
  public void checkStaticBlacklist() throws Throwable {
    assureProxy();
    try (Connection connection = getNewConnection("&loadBalanceBlacklistTimeout=500", true)) {
      Statement st = connection.createStatement();

      int firstServerId = getServerId(connection);
      int blackListSize =
          getProtocolFromConnection(connection).getProxy().getListener().getBlacklistKeys().size();
      System.out.println("blackListSize:" + blackListSize);
      stopProxy(firstServerId);

      try {
        st.execute("show variables like 'slow_query_log'");
        fail();
      } catch (SQLException e) {
        // normal exception that permit to blacklist the failing connection.
      }

      // check blacklist size
      try {
        Protocol protocol = getProtocolFromConnection(connection);
        assertEquals(
            blackListSize + 1, protocol.getProxy().getListener().getBlacklistKeys().size());

        // replace proxified HostAddress by normal one
        UrlParser urlParser = UrlParser.parse(defaultUrl);
        protocol
            .getProxy()
            .getListener()
            .addToBlacklist(urlParser.getHostAddresses().get(firstServerId - 1));
      } catch (Throwable e) {
        e.printStackTrace();
        fail();
      }

      // add first Host to blacklist
      Protocol protocol = getProtocolFromConnection(connection);
      ScheduledThreadPoolExecutor scheduler =
          new ScheduledThreadPoolExecutor(
              1, new OceanBaseThreadFactory("MariaDB-testable-scheduler"));

      // check blacklist shared
      scheduler.execute(
          new CheckBlacklist(firstServerId, protocol.getProxy().getListener().getBlacklistKeys()));
      scheduler.execute(
          new CheckBlacklist(firstServerId, protocol.getProxy().getListener().getBlacklistKeys()));

      // deterministically execute CheckBlacklists
      scheduler.shutdown();
      scheduler.awaitTermination(10, TimeUnit.SECONDS);
    }
  }

    @Test
  public void testMultiHostWriteOnMaster() {
    Assume.assumeTrue(initialGaleraUrl != null);
    try (Connection connection = getNewConnection()) {
      Statement stmt = connection.createStatement();
      stmt.execute("drop table  if exists multinode");
      stmt.execute(
          "create table multinode (id int not null primary key auto_increment, test VARCHAR(10))");
    } catch (SQLException sqle) {
      fail("must have worked");
    }
  }

    @Test
  public void pingReconnectAfterRestart() throws Throwable {
    try (Connection connection = getNewConnection("&retriesAllDown=6", true)) {
      Statement st = connection.createStatement();
      int masterServerId = getServerId(connection);
      stopProxy(masterServerId);

      try {
        st.execute("SELECT 1");
      } catch (SQLException e) {
        // eat exception
      }
      restartProxy(masterServerId);
      long restartTime = System.nanoTime();
      boolean loop = true;
      while (loop) {
        if (!connection.isClosed()) {
          loop = false;
        }
        long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - restartTime);
        if (duration > 15 * 1000) {
          fail();
        }
        Thread.sleep(250);
      }
    }
  }

    protected class CheckBlacklist implements Runnable {

        private final int              firstServerId;
        private final Set<HostAddress> blacklistKeys;

        public CheckBlacklist(int firstServerId, Set<HostAddress> blacklistKeys) {
            this.firstServerId = firstServerId;
            this.blacklistKeys = blacklistKeys;
        }

        public void run() {
      try (Connection connection2 = getNewConnection()) {
        int otherServerId = getServerId(connection2);
        assertTrue(otherServerId != firstServerId);
        Protocol protocol = getProtocolFromConnection(connection2);
        assertTrue(
            blacklistKeys.toArray()[0].equals(
                protocol.getProxy().getListener().getBlacklistKeys().toArray()[0]));

      } catch (Throwable e) {
        e.printStackTrace();
        fail();
      }
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
