package com.oceanbase.jdbc;

import static org.junit.Assert.*;

import java.sql.*;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class ComplexScenarioTestCases extends BaseTest {
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
    public void testChangeUser() {
        try {
            String observerUrl = System.getProperty("observerUrl");
            Assume.assumeNotNull(observerUrl);
            String user = System.getProperty("newUser");
            String password = System.getProperty("newPwd");
            System.out.println("observerUrl = " + observerUrl);
            Connection conn = DriverManager.getConnection(observerUrl);
            ((ObConnection) conn).changeUser(user, password);
            Statement stmt = conn.createStatement();
            stmt.execute("drop table if exists test_change_user");
            stmt.execute("create table test_change_user(c1 int);");
            stmt.execute("insert into test_change_user values(100)");
            ResultSet rs = stmt.executeQuery("select * from test_change_user");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(100, rs.getInt(1));
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testConnectProxy() {
        try {
            String url = System.getProperty("connectProxyUrl");
            Class.forName("com.oceanbase.jdbc.Driver");
            Connection conn = DriverManager.getConnection(url);
            Statement statement = conn.createStatement();
            try {
                statement.execute("alter proxyconfig set enable_client_ssl=true;");
                statement.execute("alter proxyconfig set enable_client_ssl=false;");
            } catch (Exception e) {
                e.printStackTrace();
            }
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
