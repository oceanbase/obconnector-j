package com.alipay.oceanbase.jdbc.credential;

import java.sql.SQLException;
import java.util.function.Supplier;

import com.alipay.oceanbase.jdbc.HostAddress;
import com.alipay.oceanbase.jdbc.util.Options;

public interface CredentialPlugin extends Supplier<Credential> {

  String name();

  String type();

  default boolean mustUseSsl() {
    return false;
  }

  default String defaultAuthenticationPluginType() {
    return null;
  }

  default CredentialPlugin initialize(Options options, String userName, HostAddress hostAddress)
      throws SQLException {
    return this;
  }
}
