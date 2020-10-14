package com.alipay.oceanbase.jdbc;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import com.alipay.oceanbase.jdbc.internal.protocol.Protocol;
import org.junit.Assume;
import org.junit.Test;

public class AttributeTest extends BaseTest {

    @Test
  public void testServerHost() throws Exception {
    // test for _server_host attribute
    // session_connect_attrs does not exist in MySQL 5.5, or before MariaDB 10.0.5, and need
    // performance_schema is ON

    // check whether session_connect_attrs table exists
    Statement checkStatement = sharedConnection.createStatement();
    ResultSet checkResult =
        checkStatement.executeQuery(
            "select count(*) as count from information_schema.tables where table_schema='performance_schema' and table_name='session_connect_attrs';");
    checkResult.next();
    Assume.assumeFalse(checkResult.getInt("count") == 0);

    // check if performance_schema is ON
    checkResult = checkStatement.executeQuery("show variables like 'performance_schema';");
    checkResult.next();
    Assume.assumeFalse(checkResult.getString("Value").equals("OFF"));

    try (Connection connection = setConnection("")) {
      Field protocolField = OceanBaseConnection.class.getDeclaredField("protocol");
      protocolField.setAccessible(true);
      Protocol protocolVal = (Protocol) protocolField.get(connection);

      Statement attributeStatement = connection.createStatement();
      ResultSet result =
          attributeStatement.executeQuery(
              "select * from performance_schema.session_connect_attrs where ATTR_NAME='_server_host' and processlist_id = connection_id()");
      while (result.next()) {
        String str = result.getString("ATTR_NAME");
        String strVal = result.getString("ATTR_VALUE");
        Assume.assumeTrue(protocolVal.getHost().matches(strVal));
      }
    }
  }
}
