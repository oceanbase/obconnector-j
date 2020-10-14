package com.alipay.oceanbase.jdbc.failover;

import java.sql.Connection;
import java.sql.Statement;
import org.junit.Assert;
import org.junit.Test;
import com.alipay.oceanbase.jdbc.BaseTest;

public class OldFailoverTest extends BaseTest {

    /** Check old connection way before multihost was handle. */
    @Test
  public void isOldConfigurationValid() {
    String falseUrl =
        "jdbc:oceanbase://localhost:1111,"
            + ((hostname == null) ? "localhost" : hostname)
            + ":"
            + port
            + "/"
            + database
            + "?user="
            + username
            + (password != null && !"".equals(password) ? "&password=" + password : "")
            + (parameters != null ? "&" + parameters : "");

    try {
      // the first host doesn't exist, so with the random host selection, verifying that we connect
      // to the good
      // host
      for (int i = 0; i < 10; i++) {
        try (Connection tmpConnection = openNewConnection(falseUrl)) {
          Statement tmpStatement = tmpConnection.createStatement();
          tmpStatement.execute("SELECT 1");
        }
      }
    } catch (Exception e) {
      Assert.fail();
    }
  }

    @Test
    public void errorUrl() {
        String falseUrl = "jdbc:oceanbase://localhost:1111/test";

        try {
            openNewConnection(falseUrl);
            Assert.fail();
        } catch (Exception e) {
            // normal exception
        }
    }
}
