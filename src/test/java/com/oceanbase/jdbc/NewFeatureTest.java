package com.oceanbase.jdbc;

import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

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
}
