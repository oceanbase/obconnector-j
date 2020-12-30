package com.alipay.oceanbase.jdbc;

import static org.junit.Assert.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

import com.alipay.oceanbase.jdbc.internal.util.constant.HaMode;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import com.alipay.oceanbase.jdbc.util.DefaultOptions;

public class ParserTest extends BaseTest {

    /**
     * Initialisation.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("table1", "id1 int auto_increment primary key");
        createTable("table2", "id2 int auto_increment primary key");
    }

    @Test
    public void malformedUrlException() throws SQLException {
        try {
            DriverManager.getConnection("jdbc:oceanbase:///" + hostname);
            fail("must have thrown exception");
        } catch (SQLException sqle) {
            assertTrue(sqle.getMessage().contains("No host is defined and pipe option is not set"));
        }
    }

    @Test
  public void poolVerification() throws Exception {
    ArrayList<HostAddress> hostAddresses = new ArrayList<>();
    hostAddresses.add(new HostAddress(hostname, port));
    UrlParser urlParser =
        new UrlParser(
            database, hostAddresses, DefaultOptions.defaultValues(HaMode.NONE), HaMode.NONE);
    urlParser.setUsername("USER");
    urlParser.setPassword("PWD");
    urlParser.parseUrl("jdbc:oceanbase://localhost:3306/db");
    assertEquals("USER", urlParser.getUsername());
    assertEquals("PWD", urlParser.getPassword());

    OceanBaseDataSource datasource = new OceanBaseDataSource();
    datasource.setUser("USER");
    datasource.setPassword("PWD");
    datasource.setUrl("jdbc:oceanbase://localhost:3306/db");
  }

    @Test
    public void isMultiMaster() throws Exception {
        Properties emptyProps = new Properties();

        assertFalse(UrlParser.parse("jdbc:oceanbase:replication://host1/", emptyProps)
            .isMultiMaster());
        assertFalse(UrlParser.parse("jdbc:oceanbase:failover://host1/", emptyProps).isMultiMaster());
        assertFalse(UrlParser.parse("jdbc:oceanbase:aurora://host1/", emptyProps).isMultiMaster());
        assertFalse(UrlParser.parse("jdbc:oceanbase:sequential://host1/", emptyProps)
            .isMultiMaster());
        assertFalse(UrlParser.parse("jdbc:oceanbase:loadbalance://host1/", emptyProps)
            .isMultiMaster());

        assertFalse(UrlParser.parse("jdbc:oceanbase:replication://host1,host2/", emptyProps)
            .isMultiMaster());
        assertTrue(UrlParser.parse("jdbc:oceanbase:failover://host1,host2/", emptyProps)
            .isMultiMaster());
        assertFalse(UrlParser.parse("jdbc:oceanbase:aurora://host1,host2/", emptyProps)
            .isMultiMaster());
        assertTrue(UrlParser.parse("jdbc:oceanbase:sequential://host1,host2/", emptyProps)
            .isMultiMaster());
        assertTrue(UrlParser.parse("jdbc:oceanbase:loadbalance://host1,host2/", emptyProps)
            .isMultiMaster());
    }

    @Test
  public void mysqlDatasourceVerification() throws Exception {
    Assume.assumeFalse(options.useSsl != null && options.useSsl);
    OceanBaseDataSource datasource = new OceanBaseDataSource();
    datasource.setUser(username);
    datasource.setPassword(password);
    datasource.setUrl("jdbc:mysql://" + hostname + ":" + port + "/" + database);
    try (Connection connection = datasource.getConnection()) {
      Statement stmt = connection.createStatement();
      assertTrue(stmt.execute("SELECT 10"));
    }
  }

    @Test
    public void libreOfficeBase() {
        String sql;
        try {
            Statement statement = sharedConnection.createStatement();
            sql = "INSERT INTO table1 VALUES (1),(2),(3),(4),(5),(6)";
            statement.execute(sql);
            sql = "INSERT INTO table2 VALUES (1),(2),(3),(4),(5),(6)";
            statement.execute(sql);
            // uppercase OJ
            sql = "SELECT table1.id1, table2.id2 FROM { OJ table1 LEFT OUTER JOIN table2 ON table1.id1 = table2.id2 }";
            ResultSet rs = statement.executeQuery(sql);
            for (int count = 1; count <= 6; count++) {
                assertTrue(rs.next());
                assertEquals(count, rs.getInt(1));
                assertEquals(count, rs.getInt(2));
            }
            // mixed oJ
            sql = "SELECT table1.id1, table2.id2 FROM { oJ table1 LEFT OUTER JOIN table2 ON table1.id1 = table2.id2 }";
            rs = statement.executeQuery(sql);
            for (int count = 1; count <= 6; count++) {
                assertTrue(rs.next());
                assertEquals(count, rs.getInt(1));
                assertEquals(count, rs.getInt(2));
            }
        } catch (SQLException e) {
            fail();
        }
    }

    @Test
    public void auroraClusterVerification() {
        try {
            DriverManager.getConnection("jdbc:oceanbase:aurora://"
                                        + "1.somehex.us-east-1.rds.amazonaws.com,"
                                        + "2.someOtherHex.us-east-1.rds.amazonaws.com/testj");
            fail("must have fail since not same cluster");
        } catch (Exception e) {
            assertEquals(
                "Connection string must contain only one aurora cluster. "
                        + "'2.someOtherHex.us-east-1.rds.amazonaws.com' doesn't correspond to DNS prefix "
                        + "'somehex.us-east-1.rds.amazonaws.com'", e.getMessage());
        }
    }
}
