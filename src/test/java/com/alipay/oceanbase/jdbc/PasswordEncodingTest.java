package com.alipay.oceanbase.jdbc;

import static org.junit.Assert.fail;

import java.nio.charset.Charset;
import java.sql.*;
import org.junit.Assume;
import org.junit.Test;

public class PasswordEncodingTest extends BaseTest {

    private static final String exoticPwd = "Passw0rdabéï你好";

    @Test
  public void testPwdCharset() throws Exception {
    // aurora user has no right to create other user
    Assume.assumeFalse("true".equals(System.getenv("AURORA")));
    Assume.assumeFalse(anonymousUser());

    String[] charsets = new String[] {"UTF-8", "windows-1252", "Big5"};
    String[] charsetsMysql = new String[] {"utf8", "latin1", "big5"};
    try {
      for (int i = 0; i < charsets.length; i++) {
        createUser(charsets[i], charsetsMysql[i]);
      }

      for (String currentCharsetName : charsets) {
        try (Connection connection =
            DriverManager.getConnection(
                "jdbc:oceanbase://"
                    + ((hostname != null) ? hostname : "localhost")
                    + ":"
                    + port
                    + "/"
                    + database
                    + "?user=test"
                    + currentCharsetName
                    + "&password="
                    + exoticPwd
                    + ((options.useSsl != null) ? "&useSsl=" + options.useSsl : "")
                    + ((options.serverSslCert != null)
                        ? "&serverSslCert=" + options.serverSslCert
                        : ""))) {
          // windows-1252 and windows-1250 will work have the same conversion for this password
          if (!currentCharsetName.equals(Charset.defaultCharset().name())
              && (!"windows-1252".equals(currentCharsetName)
                  || !Charset.defaultCharset().name().startsWith("windows-125"))) {
            fail(
                "must have failed for currentCharsetName="
                    + currentCharsetName
                    + " using java default charset "
                    + Charset.defaultCharset().name());
          }
        } catch (SQLInvalidAuthorizationSpecException sqle) {
          if (currentCharsetName.equals(Charset.defaultCharset().name())) {
            fail(
                "must have not have failed for charsetName="
                    + currentCharsetName
                    + " which is java default");
          }
        }
      }

      for (String charsetName : charsets) {
        checkConnection(charsetName, charsets);
      }
    } finally {
      Statement stmt = sharedConnection.createStatement();
      for (String charsetName : charsets) {
        try {
          stmt.execute("DROP USER 'test" + charsetName + "'@'%'");
        } catch (SQLException e) {
          // nothing
          e.printStackTrace();
        }
      }
    }
  }

    private void createUser(String charsetName, String serverCharset) throws Exception {
    try (Connection connection = setConnection()) {

      OceanBaseStatement stmt = connection.createStatement().unwrap(OceanBaseStatement.class);
      stmt.execute("set @@character_set_client='" + serverCharset + "'");

      boolean useOldNotation = true;
      if ((isMariadbServer() && minVersion(10, 2, 0))
          || (!isMariadbServer() && minVersion(8, 0, 0))) {
        useOldNotation = false;
      }
      if (useOldNotation) {
        stmt.execute("CREATE USER 'test" + charsetName + "'@'%'");
        stmt.testExecute(
            "GRANT SELECT on *.* to 'test"
                + charsetName
                + "'@'%' identified by '"
                + exoticPwd
                + "'",
            Charset.forName(charsetName));

      } else {
        stmt.testExecute(
            "CREATE USER 'test" + charsetName + "'@'%' identified by '" + exoticPwd + "'",
            Charset.forName(charsetName));
        stmt.execute("GRANT SELECT on *.* to 'test" + charsetName + "'@'%'");
      }
      stmt.execute("FLUSH PRIVILEGES");
    }
  }

    private void checkConnection(String charsetName, String[] charsets) throws Exception {

    for (String currentCharsetName : charsets) {
      try (Connection connection =
          DriverManager.getConnection(
              "jdbc:oceanbase://"
                  + ((hostname != null) ? hostname : "localhost")
                  + ":"
                  + port
                  + "/"
                  + database
                  + "?user=test"
                  + charsetName
                  + "&password="
                  + exoticPwd
                  + "&passwordCharacterEncoding="
                  + currentCharsetName)) {
        if (!currentCharsetName.equals(charsetName)) {
          fail(
              "must have failed for charsetName="
                  + charsetName
                  + " using passwordCharacterEncoding="
                  + currentCharsetName);
        }

      } catch (SQLInvalidAuthorizationSpecException sqle) {
        if (currentCharsetName.equals(charsetName)) {
          fail(
              "must not have failed for charsetName="
                  + charsetName
                  + " using passwordCharacterEncoding="
                  + currentCharsetName);
        }
      }
    }
  }
}
