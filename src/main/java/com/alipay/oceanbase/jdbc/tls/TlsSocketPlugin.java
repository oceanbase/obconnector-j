package com.alipay.oceanbase.jdbc.tls;

import java.io.IOException;
import java.net.Socket;
import java.sql.SQLException;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import com.alipay.oceanbase.jdbc.util.Options;

public interface TlsSocketPlugin {

  /**
   * plugin name.
   *
   * @return plugin name. ex: Mysql native password
   */
  String name();

  /**
   * plugin type.
   *
   * @return plugin type
   */
  String type();

  /**
   * Get socket factory.
   *
   * @param options connection string option. Non standard option are stored in `nonMappedOptions`
   *     if any specific option is needed.
   * @return custom SSL socket factory
   * @throws SQLException if socket factory configuration failed.
   */
  SSLSocketFactory getSocketFactory(Options options) throws SQLException;

  /**
   * Returns a socket layered over an existing socket negotiating the use of SSL over an existing
   * socket.
   *
   * @param socket existing socket
   * @param sslSocketFactory SSL socket factory
   * @return SSL socket
   * @throws IOException if any socket error occurs.
   */
  default SSLSocket createSocket(Socket socket, SSLSocketFactory sslSocketFactory)
      throws IOException {
    return (SSLSocket)
        sslSocketFactory.createSocket(
            socket,
            socket.getInetAddress() == null ? null : socket.getInetAddress().getHostAddress(),
            socket.getPort(),
            true);
  }

  /**
   * Host name verifier implementation.
   *
   * @param host hostname
   * @param sslSession ssl session
   * @param options connection string option. Non standard option are stored in * `nonMappedOptions`
   *     if any specific option is needed.
   * @param serverThreadId current server threadId
   * @throws SSLException if verification fail
   */
  void verify(String host, SSLSession sslSession, Options options, long serverThreadId)
      throws SSLException;
}
