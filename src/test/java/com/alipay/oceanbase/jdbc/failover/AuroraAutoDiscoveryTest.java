package com.alipay.oceanbase.jdbc.failover;

import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.util.List;

import com.alipay.oceanbase.jdbc.HostAddress;
import com.alipay.oceanbase.jdbc.internal.util.constant.HaMode;
import org.junit.*;

public class AuroraAutoDiscoveryTest extends BaseMultiHostTest {

    /** Initialisation. */
    @BeforeClass()
    public static void beforeClass2() {
        proxyUrl = proxyAuroraUrl;
        Assume.assumeTrue(initialAuroraUrl != null);
    }

    /** Initialisation. */
    @Before
    public void init() {
        defaultUrl = initialAuroraUrl;
        currentType = HaMode.AURORA;
    }

    /**
     * CONJ-392 : aurora must discover active nodes without timezone issue.
     *
     * @throws Throwable if error occur
     */
    @Ignore
  public void testTimeZoneDiscovery() throws Throwable {

    try (Connection connection =
        getNewConnection("&sessionVariables=@@time_zone='US/Central'", false)) {
      List<HostAddress> hostAddresses =
          getProtocolFromConnection(connection)
              .getProxy()
              .getListener()
              .getUrlParser()
              .getHostAddresses();
      for (HostAddress hostAddress : hostAddresses) {
        System.out.println("hostAddress:" + hostAddress);
      }
      assertTrue(hostAddresses.size() > 1);
    }
  }
}
