package com.alipay.oceanbase.jdbc;

import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SetSessionTimeZoneOracleTest extends BaseOracleTest {
    @Test
    public void checkTimeZone() throws SQLException {
        ((com.alipay.oceanbase.jdbc.ConnectionImpl) sharedConnection).setSessionTimeZone("+07:00");
        String val = ((com.alipay.oceanbase.jdbc.ConnectionImpl) sharedConnection)
            .getSessionTimeZone();
        Assert.assertEquals("GMT", val);
        Statement statement = sharedConnection.createStatement();
        ResultSet resultSet = statement.executeQuery("select @@time_zone from dual");
        Assert.assertTrue(resultSet.next());
        System.out.println("resultSet.getString(1) = " + resultSet.getString(1));
        Assert.assertEquals("+07:00", resultSet.getString(1));
    }
}
