package com.alipay.oceanbase.jdbc;

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;

import com.alipay.oceanbase.jdbc.internal.failover.impl.AuroraListener;
import com.alipay.oceanbase.jdbc.internal.util.pool.GlobalStateInfo;
import org.junit.Ignore;
import org.junit.Test;

public class AuroraListenerTest {

    public static final String EXPECTED_CLUSTER_DNS_SUFFIX = "ZZZZZZZZZZZZ.us-east-1.rds.amazonaws.com";
    @Ignore
    public void testWithWriterCluster() throws SQLException {
        GlobalStateInfo globalInfo = new GlobalStateInfo();
        UrlParser urlParser = UrlParser
            .parse("jdbc:mysql:aurora://XXXX.cluster-ZZZZZZZZZZZZ.us-east-1.rds.amazonaws.com:3306/XXXX");
        AuroraListener listener = new AuroraListener(urlParser, globalInfo);

        assertEquals(listener.getClusterDnsSuffix(), EXPECTED_CLUSTER_DNS_SUFFIX);
    }
    @Ignore
    public void testWithReaderCluster() throws SQLException {
        GlobalStateInfo globalInfo = new GlobalStateInfo();
        UrlParser urlParser = UrlParser
            .parse("jdbc:mysql:aurora://XXXX.cluster-ro-ZZZZZZZZZZZZ.us-east-1.rds.amazonaws.com:3306/XXXX");
        AuroraListener listener = new AuroraListener(urlParser, globalInfo);

        assertEquals(listener.getClusterDnsSuffix(), EXPECTED_CLUSTER_DNS_SUFFIX);
    }
}
