package com.alipay.oceanbase.jdbc.failover;

import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alipay.oceanbase.jdbc.HostAddress;
import com.alipay.oceanbase.jdbc.OceanBaseConnection;
import com.alipay.oceanbase.jdbc.OceanBasePoolDataSource;
import com.alipay.oceanbase.jdbc.UrlParser;
import com.alipay.oceanbase.jdbc.internal.util.constant.HaMode;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * test for galera The node must be configure with specific names : node 1 : wsrep_node_name =
 * "galera1" ... node x : wsrep_node_name = "galerax" exemple mvn test
 * -DdbUrl=jdbc:oceanbase://localhost:3306,localhost:3307/test?user=root
 */
public class GaleraFailoverTest extends SequentialFailoverTest {

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

    @Test
  public void showRep() throws Exception {
    UrlParser urlParser = UrlParser.parse(initialGaleraUrl);
    List<HostAddress> initAddresses = urlParser.getHostAddresses();

    for (int i = 0; i < initAddresses.size(); i++) {
      UrlParser urlParserMono =
          new UrlParser(
              urlParser.getDatabase(),
              Arrays.asList(initAddresses.get(i)),
              urlParser.getOptions(),
              urlParser.getHaMode());
      try (Connection master = OceanBaseConnection.newConnection(urlParserMono, null)) {
        Statement stmt = master.createStatement();
        ResultSet rs = stmt.executeQuery("show status like 'wsrep_local_state'");
        assertTrue(rs.next());
        System.out.println("host:" + initAddresses.get(i) + " status:" + rs.getString(2));
      }
    }
  }

    @Test
  public void validGaleraPing() throws Exception {
    long start = System.currentTimeMillis();
    try (OceanBasePoolDataSource pool =
        new OceanBasePoolDataSource(initialGaleraUrl + "&maxPoolSize=1")) {
      try (Connection connection = pool.getConnection()) {
        Statement statement = connection.createStatement();
        statement.execute("SELECT 1 ");
      }
      Thread.sleep(2000);
      // Galera ping must occur
      try (Connection connection = pool.getConnection()) {
        Statement statement = connection.createStatement();
        statement.execute("SELECT 1 ");
      }
    }
    // if fail, will loop until connectTimeout = 30s
    assertTrue(System.currentTimeMillis() - start < 5000);
  }

    @Test
  @Override
  public void connectionOrder() throws Throwable {
    Assume.assumeTrue(initialGaleraUrl.contains("failover"));
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

    assertTrue(connectionMap.size() >= 2);
    for (String key : connectionMap.keySet()) {
      Integer connectionCount = connectionMap.get(key).get();
      assertTrue(connectionCount > 1);
    }
  }

    @Test
  public void isValidGaleraConnection() throws SQLException {
    try (Connection connection = getNewConnection(false)) {
      assertTrue(connection.isValid(0));
    }
  }
}
