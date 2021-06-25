package com.oceanbase.jdbc;

import org.junit.Assert;
import org.junit.Test;

import java.sql.*;

public class NewFeatureTest extends BaseTest {
    @Test
    public void testSocksProxy() {
        try {
            String url = System.getProperty("socksProxyOptions");
            Connection connection = setConnection(url);
            ResultSet resultSet = connection.createStatement().executeQuery("select 1");
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(1, resultSet.getInt(1));
        } catch (SQLException e) {
            Assert.fail();
            e.printStackTrace();
        }
    }

    @Test
    public void testConnectProxy() {
        try {
            String url = System.getProperty("connectProxyUrl");
            Class.forName("com.oceanbase.jdbc.Driver");
            Connection conn = DriverManager.getConnection(url);
            Statement statement = conn.createStatement();
            statement.execute("alter proxyconfig set enable_client_ssl=true;");
            statement.execute("alter proxyconfig set enable_client_ssl=false;");
            ResultSet rs = statement.executeQuery("show proxyconfig");
            int count = rs.getMetaData().getColumnCount();
            int rowCount = 0;
            while (rs.next()) {
                for (int i = 1; i <= count; i++) {
                    System.out.println(" col[ " + i + "] =" + rs.getString(i));
                }
                rowCount++;
            }
            Assert.assertNotEquals(rowCount, 0);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            Assert.fail();
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();

        }
    }
}
