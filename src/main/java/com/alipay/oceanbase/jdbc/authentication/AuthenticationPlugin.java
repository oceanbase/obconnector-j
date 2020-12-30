package com.alipay.oceanbase.jdbc.authentication;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import com.alipay.oceanbase.jdbc.internal.com.read.Buffer;
import com.alipay.oceanbase.jdbc.internal.io.input.PacketInputStream;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;
import com.alipay.oceanbase.jdbc.util.Options;

public interface AuthenticationPlugin {
  /**
   * Authentication plugin name.
   *
   * @return authentication plugin name. ex: Mysql native password
   */
  String name();

  /**
   * Authentication plugin type.
   *
   * @return authentication plugin type. ex: mysql_native_password
   */
  String type();

  /**
   * Indicate if use of this plugins need SSL enabled.
   *
   * @return true if SSL is mandatory
   */
  default boolean mustUseSsl() {
    return false;
  }

  /**
   * Plugin initialization.
   *
   * @param authenticationData authentication data (password/token)
   * @param seed server provided seed
   * @param options Connection string options
   */
  void initialize(String authenticationData, byte[] seed, Options options);

  /**
   * Process plugin authentication.
   *
   * @param out out stream
   * @param in in stream
   * @param sequence packet sequence
   * @return response packet
   * @throws IOException if socket error
   * @throws SQLException if plugin exception
   */
  Buffer process(PacketOutputStream out, PacketInputStream in, AtomicInteger sequence)
      throws IOException, SQLException;
}
