/**
 *  OceanBase Client for Java
 *
 *  Copyright (c) 2012-2014 Monty Program Ab.
 *  Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *  Copyright (c) 2021 OceanBase.
 *
 *  This library is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU Lesser General Public License as published by the Free
 *  Software Foundation; either version 2.1 of the License, or (at your option)
 *  any later version.
 *
 *  This library is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License along
 *  with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 *  This particular MariaDB Client for Java file is work
 *  derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 *  the following copyright and notice provisions:
 *
 *  Copyright (c) 2009-2011, Marcus Eriksson
 *
 *  Redistribution and use in source and binary forms, with or without modification,
 *  are permitted provided that the following conditions are met:
 *  Redistributions of source code must retain the above copyright notice, this list
 *  of conditions and the following disclaimer.
 *
 *  Redistributions in binary form must reproduce the above copyright notice, this
 *  list of conditions and the following disclaimer in the documentation and/or
 *  other materials provided with the distribution.
 *
 *  Neither the name of the driver nor the names of its contributors may not be
 *  used to endorse or promote products derived from this software without specific
 *  prior written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 *  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 *  IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 *  INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 *  NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 *  PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 *  WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 *  OF SUCH DAMAGE.
 */
package com.oceanbase.jdbc;

import static org.junit.Assert.*;

import java.io.*;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.sql.*;
import java.util.*;

import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.oceanbase.jdbc.tls.TlsSocketPluginLoader;
import com.oceanbase.jdbc.util.Options;
import com.sun.jna.Platform;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class SslTest extends BaseTest {

    private String serverCertificatePath;
    private String clientKeystorePath;
    private String clientKeystorePassword;

    /** Enable Crypto. */
    @BeforeClass
    public static void enableCrypto() {
        Assume.assumeFalse(System.getenv("MAXSCALE_VERSION") != null
                           || System.getenv("SKYSQL") != null
                           || "true".equals(System.getenv("AURORA")));
        try {
            Field field = Class.forName("javax.crypto.JceSecurity")
                .getDeclaredField("isRestricted");
            field.setAccessible(true);
            field.set(null, Boolean.FALSE);
        } catch (Exception ex) {
            // eat
        }
    }

    /**
     * Check requirement.
     *
     * @throws SQLException exception exception
     */
    @Before
  public void checkSsl() throws SQLException {
    boolean isJava7 = System.getProperty("java.version").contains("1.7.");
    cancelForVersion(5, 6, 36); // has SSL issues with client authentication.
    Assume.assumeTrue(haveSsl(sharedConnection));
    // Skip SSL test on java 7 since SSL stream size JDK-6521495).
    Assume.assumeFalse(isJava7);
    try {
      InetAddress.getByName("oceanbase.com").isReachable(3);
    } catch (UnknownHostException hostException) {
      throw new SQLException(
          "SSL test canceled, database host must be set has "
              + "\"oceanbase.com\" to permit SSL certificate Host verification");
    } catch (IOException ioe) {
      throw new SQLException("unexpected exception :" + ioe.getMessage());
    }

    if (System.getProperty("serverCertificatePath") == null && System.getenv("SKYSQL") == null) {
      try (ResultSet rs = sharedConnection.createStatement().executeQuery("select @@ssl_cert")) {
        assertTrue(rs.next());
        serverCertificatePath = rs.getString(1);
      }
    } else {
      serverCertificatePath = System.getProperty("serverCertificatePath");
    }
    clientKeystorePath = System.getProperty("keystorePath");
    clientKeystorePassword = System.getProperty("keystorePassword");
    Statement stmt = sharedConnection.createStatement();
    try {
      stmt.execute("DROP USER 'ssltestUser'@'%'");
    } catch (SQLException e) {
      // eat
    }
    boolean useOldNotation = true;
    if ((isMariadbServer() && minVersion(10, 2, 0))
        || (!isMariadbServer() && minVersion(8, 0, 0))) {
      useOldNotation = false;
    }
    if (useOldNotation) {
      stmt.execute("CREATE USER 'ssltestUser'@'%'");
      stmt.execute(
          "GRANT SELECT ON *.* TO 'ssltestUser'@'%' IDENTIFIED BY '!Passw0rd3Works' REQUIRE SSL");
    } else {
      stmt.execute("CREATE USER 'ssltestUser'@'%' IDENTIFIED BY '!Passw0rd3Works' REQUIRE SSL");
      stmt.execute("GRANT SELECT ON *.* TO 'ssltestUser'@'%'");
    }
  }

    @Test
  public void useSsl() throws Exception {
    Assume.assumeTrue(haveSsl(sharedConnection) && isMariadbServer());
    // Skip SSL test on java 7 since SSL stream size JDK-6521495).
    Assume.assumeFalse(System.getProperty("java.version").contains("1.7."));
    try (Connection connection = setConnection("&useSSL=true&trustServerCertificate=true")) {
      connection.createStatement().execute("select 1");
    }
  }

    protected void useSslForceTls(String tls) throws Exception {
        useSslForceTls(tls, null);
    }

    /** Helper method when checking/enabling secure connections for a specific TLS protocol suite. */
    protected void useSslForceTls(String tls, String ciphers) throws Exception {
    Assume.assumeTrue(haveSsl(sharedConnection) && isMariadbServer());
    // Skip SSL test on java 7 since SSL stream size JDK-6521495).
    Assume.assumeFalse(System.getProperty("java.version").contains("1.7."));
    Properties info = new Properties();
    info.setProperty("useSSL", "true");
    info.setProperty("trustServerCertificate", "true");
    info.setProperty("enabledSslProtocolSuites", tls);
    if (ciphers != null) {
      info.setProperty("enabledSslCipherSuites", ciphers);
    }

    try (Connection connection = setConnection(info)) {
      connection.createStatement().execute("select 1");
    }
  }

    @Test
    public void testBadOptionEnabledSslProtocolSuites() throws Exception {
        try {
            useSslForceTls("TLSv1,TLSv1.5");
            fail("Must have thrown error since protocol unknown");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains(
                "Unsupported SSL protocol 'TLSv1.5'. Supported protocols : "));
        }
    }

    @Test
    public void testUnknownProtocol() throws Exception {
        try {
            useSslForceTls("TLSv0");
            fail("Must have thrown error since protocol not set");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains(
                "Unsupported SSL protocol 'TLSv0'. Supported protocols : "));
        }
    }

    @Test
    public void testServerRefuseProtocol() throws Exception {
        try {
            useSslForceTls("SSLv3");
            fail("Must have thrown error since protocol is refused by server");
        } catch (SQLNonTransientConnectionException e) {
            assertTrue(e.getMessage().contains(
                "No appropriate protocol "
                        + "(protocol is disabled or cipher suites are inappropriate)"));
        }
    }

    @Test
    public void useSslForceTlsV1() throws Exception {
        Assume.assumeFalse(isMariadbServer() && minVersion(10, 3));
        useSslForceTls("TLSv1");
    }

    @Test
    public void useSslForceTlsV11() throws Exception {
        // must either be mariadb or mysql version 5.7.10
        Assume.assumeFalse(isMariadbServer() && minVersion(10, 3));
        if (isMariadbServer() || minVersion(5, 7)) {
            useSslForceTls("TLSv1.1");
        }
    }

    @Test
    public void useSslForceTlsV12() throws Exception {
        Assume.assumeFalse(Platform.isWindows());
        // Only test with MariaDB since MySQL community is compiled with yaSSL
        if (isMariadbServer()) {
            useSslForceTls("TLSv1.2");
        }
    }

    @Test
    public void useSslForceTlsV13() throws Exception {
        Assume.assumeFalse(Platform.isWindows());
        Assume.assumeTrue(isMariadbServer());

        List<String> possibleProtocols = getSupportedProtocols();
        if (possibleProtocols.contains("TLSv1.3")) {
            useSslForceTls("TLSv1.3, TLSv1.2");
        }
    }

    private List<String> getSupportedProtocols() throws Exception {

        SSLSocketFactory sslSocketFactory = TlsSocketPluginLoader.get(null).getSocketFactory(
            new Options());
        SSLSocket socket = (SSLSocket) sslSocketFactory.createSocket();
        List<String> enabledProtocol = Arrays.asList(socket.getEnabledProtocols());
        socket.close();
        return enabledProtocol;
    }

    @Test
    public void useSslForceTlsV12AndCipher() throws Exception {
        Assume.assumeFalse((Platform.isWindows() && !isMariadbServer())
                           || (isMariadbServer() && Platform.isWindows() && !minVersion(10, 4)));
        // Only test with MariaDB since MySQL community is compiled with yaSSL
        if (isMariadbServer()) {
            useSslForceTls("TLSv1.2",
                "TLS_DHE_RSA_WITH_AES_256_CBC_SHA, TLS_DHE_RSA_WITH_AES_128_GCM_SHA256");
        }
    }

    @Test
    public void wrongCipher() throws Exception {
        // Only test with MariaDB since MySQL community is compiled with yaSSL
        try {
            if (isMariadbServer()) {
                useSslForceTls("TLSv1.2", "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256, UNKNOWN_CIPHER");
                fail("Must have thrown error since cipher is refused by server");
            }
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("Unsupported SSL cipher 'UNKNOWN_CIPHER'."));
        }
    }

    @Test
    public void wrongCipherForTls11() throws Exception {
        // Only test with MariaDB since MySQL community is compiled with yaSSL
        try {
            if (isMariadbServer()) {
                useSslForceTls("TLSv1.1",
                    "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256, TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256");
                fail("Must have thrown error since cipher aren't TLSv1.1 ciphers");
            }
        } catch (SQLException e) {
            assertTrue(e
                .getMessage()
                .contains(
                    "No appropriate protocol (protocol is disabled or cipher suites are inappropriate)"));
        }
    }

    @Test
  public void wrongCipherMysqlOptionCompatibility() {
    // Only test with MariaDB since MySQL community is compiled with yaSSL
    try {
      if (isMariadbServer()) {
        Assume.assumeTrue(haveSsl(sharedConnection));
        // Skip SSL test on java 7 since SSL stream size JDK-6521495).
        Assume.assumeFalse(System.getProperty("java.version").contains("1.7."));
        Properties info = new Properties();
        info.setProperty("useSSL", "true");
        info.setProperty("trustServerCertificate", "true");
        info.setProperty("enabledSslProtocolSuites", "TLSv1.1");
        // enabledSSLCipherSuites, not enabledSslCipherSuites (different case)
        info.setProperty("enabledSSLCipherSuites", "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256");

        try (Connection connection = setConnection(info)) {
          connection.createStatement().execute("select 1");
          fail("Must have thrown error since cipher aren't TLSv1.1 ciphers");
        }
      }
    } catch (SQLException e) {
      assertTrue(
          e.getMessage()
              .contains(
                  "No appropriate protocol (protocol is disabled or cipher suites are inappropriate)"));
    }
  }

    @Test
    public void useSslForceTlsCombination() throws Exception {
        if (isMariadbServer() && !Platform.isWindows()) {
            useSslForceTls("TLSv1,TLSv1.1,TLSv1.2");
        } else {
            useSslForceTls("TLSv1,TLSv1.1");
        }
    }

    @Test
    public void useSslForceTlsCombinationWithSpace() throws Exception {
        if (isMariadbServer() && !Platform.isWindows()) {
            useSslForceTls("TLSv1, TLSv1.1, TLSv1.2");
        } else {
            useSslForceTls("TLSv1, TLSv1.1");
        }
    }

    @Test
    public void useSslForceTlsCombinationWithOnlySpace() throws Exception {
        if (isMariadbServer() && !Platform.isWindows()) {
            useSslForceTls("TLSv1 TLSv1.1 TLSv1.2");
        } else {
            useSslForceTls("TLSv1 TLSv1.1");
        }
    }

    private String getServerCertificate() throws SQLException {
    try (BufferedReader br =
        new BufferedReader(new InputStreamReader(new FileInputStream(serverCertificatePath)))) {
      StringBuilder sb = new StringBuilder();
      String line;
      while ((line = br.readLine()) != null) {
        sb.append(line);
        sb.append("\n");
      }
      return sb.toString();
    } catch (Exception e) {
      throw new SQLException("abnormal exception", e);
    }
  }

    private void saveToFile(String path, String contents) {
    try (PrintWriter out = new PrintWriter(new FileOutputStream(path))) {
      out.print(contents);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

    private Connection createConnection(Properties info) throws SQLException {
        return createConnection(info, username, password);
    }

    private Connection createConnection(Properties info, String user, String pwd)
                                                                                 throws SQLException {
        String jdbcUrl = connDnsUri;
        Properties connProps = new Properties(info);
        connProps.setProperty("user", user);
        if (pwd != null) {
            connProps.setProperty("password", pwd);
        }
        return openNewConnection(jdbcUrl, connProps);
    }

    /**
     * Test connection.
     *
     * @param info connection properties
     * @param sslExpected is SSL expected
     * @throws SQLException exception
     */
    public void testConnect(Properties info, boolean sslExpected) throws SQLException {
        Assume.assumeTrue(haveSsl(sharedConnection) && isMariadbServer());
        testConnect(info, sslExpected, "ssltestUser", "!Passw0rd3Works");
    }

    /**
     * Test connection.
     *
     * @param info connection properties
     * @param sslExpected is SSL expected
     * @param user user
     * @param pwd password
     * @throws SQLException if exception occur
     */
    public void testConnect(Properties info, boolean sslExpected, String user, String pwd)
      throws SQLException {
    Assume.assumeTrue(System.getenv("SKYSQL") == null);
    Assume.assumeTrue(haveSsl(sharedConnection) && isMariadbServer());
    try (Connection conn = createConnection(info, user, pwd)) {
      // First do a basic select test:
      try (Statement stmt = conn.createStatement()) {
        try (ResultSet rs = stmt.executeQuery("SHOW STATUS like 'Ssl_version'")) {
          assertTrue(rs.next());
          System.out.println(rs.getString(1) + " " + rs.getString(2));
        }

        // Then check if SSL matches what is expected
        try (ResultSet rs = stmt.executeQuery("SHOW STATUS LIKE 'Ssl_cipher'")) {
          assertTrue(rs.next());
          String sslCipher = rs.getString(2);
          boolean sslActual = sslCipher != null && sslCipher.length() > 0;
          assertEquals("sslExpected does not match", sslExpected, sslActual);
        }
      }
    }
  }

    @Test
    public void testConnectNonSsl() throws SQLException {
        Assume.assumeTrue(haveSsl(sharedConnection) && isMariadbServer());
        Properties info = new Properties();
        try {
            testConnect(info, false);
            fail("Must fail since user require SSL");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("Access denied for user 'ssltestUser'"));
        }
    }

    @Test
    public void testTrustedServer() throws SQLException {
        Assume.assumeTrue(haveSsl(sharedConnection) && isMariadbServer());
        Properties info = new Properties();
        info.setProperty("useSSL", "true");
        info.setProperty("trustServerCertificate", "true");
        testConnect(info, true);
    }

    @Test
    public void testServerCertString() throws SQLException {
        Assume.assumeTrue(haveSsl(sharedConnection) && isMariadbServer());
        Assume.assumeTrue(hasSameHost());
        Assume.assumeNotNull(serverCertificatePath);
        Properties info = new Properties();
        info.setProperty("useSSL", "true");
        info.setProperty("serverSslCert", getServerCertificate());
        testConnect(info, true);
    }

    @Test(expected = SQLException.class)
    public void testBadServerCertString() throws SQLException {
        Assume.assumeTrue(haveSsl(sharedConnection) && isMariadbServer());
        Properties info = new Properties();
        info.setProperty("useSSL", "true");
        info.setProperty("serverSslCert", "foobar");
        testConnect(info, true);
    }

    @Test
    public void testServerCertFile() throws SQLException {
        Assume.assumeTrue(haveSsl(sharedConnection) && isMariadbServer());
        Assume.assumeTrue(hasSameHost());
        Assume.assumeNotNull(serverCertificatePath);
        Properties info = new Properties();
        info.setProperty("useSSL", "true");
        info.setProperty("log", "true");
        info.setProperty("serverSslCert", serverCertificatePath);
        testConnect(info, true);
    }

    @Test
    public void testServerCertClasspathFile() throws SQLException {
        Assume.assumeTrue(haveSsl(sharedConnection) && isMariadbServer());
        Assume.assumeTrue(hasSameHost());
        Assume.assumeNotNull(serverCertificatePath);
        Assume.assumeTrue(new File("target/classes").isDirectory());
        Properties info = new Properties();
        info.setProperty("useSSL", "true");
        // Copy the valid server certificate to a known location on the classpath:
        String classpathFilename = "server-ssl-cert.pem";
        saveToFile("target/classes/" + classpathFilename, getServerCertificate());
        info.setProperty("serverSslCert", "classpath:" + classpathFilename);
        testConnect(info, true);
    }

    @Test(expected = SQLException.class)
    public void testWrongServerCert() throws Throwable {
        Properties info = new Properties();
        info.setProperty("useSSL", "true");
        info.setProperty("serverSslCert", "classpath:ssl/wrong-server.crt");
        testConnect(info, true);
    }

    @Test
  public void conc71() throws SQLException {
    Assume.assumeTrue(haveSsl(sharedConnection) && isMariadbServer());
    Assume.assumeTrue(hasSameHost());
    Assume.assumeNotNull(serverCertificatePath);
    try {
      Properties info = new Properties();
      info.setProperty("serverSslCert", getServerCertificate());
      info.setProperty("useSSL", "true");
      try (Connection conn = createConnection(info)) {
        assertEquals("testj", conn.getCatalog());
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

    @Test
    public void testTruststore() throws SQLException, IOException, KeyStoreException,
                                CertificateException, NoSuchAlgorithmException {
        Assume.assumeTrue(hasSameHost());
        Assume.assumeNotNull(serverCertificatePath);
        Assume.assumeTrue(haveSsl(sharedConnection) && isMariadbServer());
        // generate a truststore from the canned serverCertificate
        File tempKeystore = File.createTempFile("keystore", ".tmp");
        String keystorePath = tempKeystore.getAbsolutePath();
        try {
            generateKeystoreFromFile(serverCertificatePath, keystorePath, null);
            Properties info = new Properties();
            info.setProperty("useSSL", "true");
            info.setProperty("trustStoreType", "JKS");
            info.setProperty("trustStore", "file:///" + keystorePath);
            testConnect(info, true);
        } finally {
            tempKeystore.delete();
        }
    }

    @Test
  public void testNoSessionResumption()
      throws SQLException, IOException, KeyStoreException, CertificateException,
          NoSuchAlgorithmException {
    Assume.assumeTrue(hasSameHost());
    Assume.assumeNotNull(serverCertificatePath);
    Assume.assumeTrue(haveSsl(sharedConnection) && isMariadbServer());
    // generate a truststore from the canned serverCertificate
    File tempKeystore = File.createTempFile("keystore2", ".tmp");
    String keystorePath = tempKeystore.getAbsolutePath();
    try {
      generateKeystoreFromFile(serverCertificatePath, keystorePath, null);

      Properties info = new Properties();
      info.setProperty("useSSL", "true");
      info.setProperty("trustStoreType", "JKS");
      info.setProperty("trustStore", "file:///" + keystorePath);

      Assume.assumeTrue(haveSsl(sharedConnection) && isMariadbServer());

      long sessionsReused = 0;
      try (Connection conn = createConnection(info, "ssltestUser", "!Passw0rd3Works")) {
        // First do a basic select test:
        Statement stmt = conn.createStatement();

        try (ResultSet rs = stmt.executeQuery("SHOW STATUS LIKE 'Ssl_cipher'")) {
          assertTrue(rs.next());
          String sslCipher = rs.getString(2);
          boolean sslActual = sslCipher != null && sslCipher.length() > 0;
          assertEquals("sslExpected does not match", true, sslActual);
        }

        try (ResultSet rs = stmt.executeQuery("SHOW STATUS LIKE 'Ssl_sessions_reused'")) {
          assertTrue(rs.next());
          sessionsReused = rs.getLong(2);
        }

        try (Connection conn2 = createConnection(info, "ssltestUser", "!Passw0rd3Works")) {
          Statement stmt2 = conn2.createStatement();

          try (ResultSet rs = stmt2.executeQuery("SHOW STATUS LIKE 'Ssl_sessions_reused'")) {
            assertTrue(rs.next());
            assertEquals(sessionsReused, rs.getLong(2));
          }
        }
      }
    } finally {
      tempKeystore.delete();
    }
  }

    @Test
    public void testTrustStoreWithPassword() throws IOException, KeyStoreException,
                                            CertificateException, NoSuchAlgorithmException,
                                            SQLException {
        Assume.assumeTrue(hasSameHost());
        Assume.assumeNotNull(serverCertificatePath);
        Assume.assumeTrue(haveSsl(sharedConnection) && isMariadbServer());
        // generate a truststore from the canned serverCertificate
        File tempKeystore = File.createTempFile("keystore", ".tmp");
        String keystorePath = tempKeystore.getAbsolutePath();
        try {
            generateKeystoreFromFile(serverCertificatePath, keystorePath, "mysecret");

            Properties info = new Properties();
            info.setProperty("useSSL", "true");
            info.setProperty("trustStore", keystorePath);
            info.setProperty("trustStoreType", "JKS");
            info.setProperty("trustStorePassword", "mysecret");
            testConnect(info, true);
        } finally {
            tempKeystore.delete();
        }
    }

    @Test
    public void testTrustStoreWithPasswordProperties() throws Exception {
        Assume.assumeTrue(hasSameHost());
        Assume.assumeTrue(haveSsl(sharedConnection) && isMariadbServer());
        Assume.assumeNotNull(serverCertificatePath);
        // generate a truststore from the canned serverCertificate
        File tempKeystore = File.createTempFile("keystore", ".tmp");
        String keystorePath = tempKeystore.getAbsolutePath();

        String initialTrustStore = System.getProperty("javax.net.ssl.trustStore");
        String initialTrustStorePwd = System.getProperty("javax.net.ssl.trustStorePassword");

        try {
            generateKeystoreFromFile(serverCertificatePath, keystorePath, "mysecret");

            System.setProperty("javax.net.ssl.trustStore", keystorePath);
            System.setProperty("javax.net.ssl.trustStorePassword", "mysecret");

            Properties info = new Properties();
            info.setProperty("useSSL", "true");
            info.setProperty("trustStoreType", "JKS");
            testConnect(info, true);
        } finally {
            if (initialTrustStore == null) {
                System.clearProperty("javax.net.ssl.trustStore");
            } else {
                System.setProperty("javax.net.ssl.trustStore", initialTrustStore);
            }
            if (initialTrustStorePwd == null) {
                System.clearProperty("javax.net.ssl.trustStorePassword");
            } else {
                System.setProperty("javax.net.ssl.trustStorePassword", initialTrustStorePwd);
            }
            tempKeystore.delete();
        }
    }

    @Test(expected = SQLException.class)
    public void testTruststoreWithWrongPassword() throws IOException, KeyStoreException,
                                                 CertificateException, NoSuchAlgorithmException,
                                                 SQLException {
        Assume.assumeTrue(hasSameHost());
        Assume.assumeTrue(haveSsl(sharedConnection) && isMariadbServer());
        Assume.assumeNotNull(serverCertificatePath);
        // generate a truststore from the canned serverCertificate
        File tempKeystore = File.createTempFile("keystore", ".tmp");
        String keystorePath = tempKeystore.getAbsolutePath();
        try {
            generateKeystoreFromFile(serverCertificatePath, keystorePath, "mysecret");

            Properties info = new Properties();
            info.setProperty("useSSL", "true");
            info.setProperty("trustStore", "file:///" + keystorePath);
            info.setProperty("trustStoreType", "JKS");
            info.setProperty("trustStorePassword", "notthepassword");
            testConnect(info, true);
        } finally {
            tempKeystore.delete();
        }
    }

    @Test(expected = SQLException.class)
    public void testTruststoreWithWrongCert() throws IOException, KeyStoreException,
                                             CertificateException, NoSuchAlgorithmException,
                                             SQLException {
        Assume.assumeTrue(haveSsl(sharedConnection) && isMariadbServer());
        Assume.assumeNotNull(serverCertificatePath);
        // generate a truststore from the canned serverCertificate
        File tempKeystore = File.createTempFile("keystore", ".tmp");
        String keystorePath = tempKeystore.getAbsolutePath();
        try {
            generateKeystoreFromFile("classpath:ssl/wrong-server.crt", keystorePath, "mysecret");

            Properties info = new Properties();
            info.setProperty("useSSL", "true");
            info.setProperty("trustStoreType", "JKS");
            info.setProperty("trustStore", "file:///" + keystorePath);
            info.setProperty("trustStorePassword", "mysecret");
            testConnect(info, true);
        } finally {
            tempKeystore.delete();
        }
    }

    @Test
    public void testTruststoreAndClientKeystore() throws SQLException, IOException,
                                                 KeyStoreException, CertificateException,
                                                 NoSuchAlgorithmException {
        Assume.assumeTrue(haveSsl(sharedConnection) && isMariadbServer());
        Assume.assumeNotNull(serverCertificatePath);
        // This test only runs if a client keystore and password have been passed in as properties
        // (-DkeystorePath and -DkeystorePassword)
        // You can create a keystore as follows:
        // echo "kspass" | openssl pkcs12 -export -in "${clientCertFile}" -inkey "${clientKeyFile}" -out
        // "${clientKeystoreFile}"
        //   -name "mysqlAlias" -pass stdin
        Assume.assumeTrue(clientKeystorePathDefined());

        String testUser = "testTsAndKs";
        // For this testcase, the testUser must be configured with ssl_type=X509
        createSslTestUser(testUser);

        // generate a truststore from the canned server certificate
        File tempTruststore = File.createTempFile("truststore", ".tmp");
        String truststorePath = tempTruststore.getAbsolutePath();
        try {
            generateKeystoreFromFile(serverCertificatePath, truststorePath, null);

            Properties info = new Properties();
            info.setProperty("useSSL", "true");
            info.setProperty("trustStore", "file:///" + truststorePath);
            info.setProperty("trustStoreType", "JKS");
            info.setProperty("keyStoreType", "JKS");
            info.setProperty("keyStore", "file:///" + clientKeystorePath);
            info.setProperty("keyStorePassword", clientKeystorePassword);
            testConnect(info, true, testUser, "ssltestpassword");
        } finally {
            tempTruststore.delete();
            deleteSslTestUser(testUser);
        }
    }

    @Test
    public void testAliases() throws SQLException, IOException, KeyStoreException,
                             CertificateException, NoSuchAlgorithmException {
        Assume.assumeTrue(haveSsl(sharedConnection) && isMariadbServer());
        Assume.assumeNotNull(serverCertificatePath);
        // This test only runs if a client keystore and password have been passed in as properties
        // (-DkeystorePath and -DkeystorePassword)
        // You can create a keystore as follows:
        // echo "kspass" | openssl pkcs12 -export -in "${clientCertFile}" -inkey "${clientKeyFile}" -out
        // "${clientKeystoreFile}"
        //   -name "mysqlAlias" -pass stdin
        Assume.assumeTrue(clientKeystorePathDefined());

        String testUser = "testTsAndKs";
        // For this testcase, the testUser must be configured with ssl_type=X509
        createSslTestUser(testUser);

        // generate a truststore from the canned server certificate
        File tempTruststore = File.createTempFile("truststore", ".tmp");
        String truststorePath = tempTruststore.getAbsolutePath();
        try {
            generateKeystoreFromFile(serverCertificatePath, truststorePath, "trustPwd");

            Properties info = new Properties();
            info.setProperty("useSSL", "true");
            info.setProperty("trustCertificateKeyStoreUrl", "file:///" + truststorePath);
            info.setProperty("trustCertificateKeyStorePassword", "trustPwd");
            info.setProperty("trustCertificateKeyStoreType", "JKS");
            info.setProperty("clientCertificateKeyStoreUrl", "file:///" + clientKeystorePath);
            info.setProperty("clientCertificateKeyStorePassword", clientKeystorePassword);
            info.setProperty("clientCertificateKeyStoreType", "JKS");
            testConnect(info, true, testUser, "ssltestpassword");
        } finally {
            tempTruststore.delete();
            deleteSslTestUser(testUser);
        }
    }

    @Test
    public void testClientKeystore() throws SQLException {
        Assume.assumeTrue(haveSsl(sharedConnection) && isMariadbServer());
        // This test only runs if a client keystore and password have been passed in as properties
        // (-DkeystorePath and -DkeystorePassword)
        // You can create a keystore as follows:
        // echo "kspass" | openssl pkcs12 -export -in "${clientCertFile}" -inkey "${clientKeyFile}" -out
        // "${clientKeystoreFile}"
        //   -name "mysqlAlias" -pass stdin
        Assume.assumeTrue(clientKeystorePathDefined());

        String testUser = "testKeystore";
        // For this testcase, the testUser must be configured with ssl_type=X509
        createSslTestUser(testUser);

        try {
            Properties info = new Properties();
            info.setProperty("useSSL", "true");
            info.setProperty("serverSslCert", serverCertificatePath);
            info.setProperty("keyStore", "file:///" + clientKeystorePath);
            info.setProperty("keyStorePassword", clientKeystorePassword);
            testConnect(info, true, testUser, "ssltestpassword");
        } finally {
            deleteSslTestUser(testUser);
        }
    }

    /**
     * Verification when private key password differ from keyStore password.
     *
     * @throws Exception if error occur
     */
    @Test
    public void testClientKeyStoreWithPrivateKeyPwd() throws Exception {
        Assume.assumeTrue(haveSsl(sharedConnection) && isMariadbServer());
        String clientKeyStore2Path = System.getProperty("keystore2Path");
        String clientKeyStore2Password = System.getProperty("keystore2Password");
        String clientKeyPassword = System.getProperty("keyPassword");
        Assume.assumeTrue(clientKeyPassword != null && clientKeyStore2Password != null
                          && clientKeyStore2Path != null);
        String testUser = "testKeystore";
        // For this testcase, the testUser must be configured with ssl_type=X509
        createSslTestUser(testUser);

        // without keyPassword
        try {
            Properties info = new Properties();
            info.setProperty("useSSL", "true");
            info.setProperty("serverSslCert", serverCertificatePath);
            info.setProperty("keyStore", "file:///" + clientKeyStore2Path);
            info.setProperty("keyStorePassword", clientKeyStore2Password);
            testConnect(info, true, testUser, "ssltestpassword");

            fail("Must have Error since client private key is protected with a password different than keystore");
        } catch (SQLException sqle) {
            sqle.printStackTrace();
            assertTrue(sqle.getMessage().contains("Access denied for user"));
        }

        try {
            Properties info = new Properties();
            info.setProperty("useSSL", "true");
            info.setProperty("serverSslCert", serverCertificatePath);
            info.setProperty("keyStore", "file:///" + clientKeyStore2Path);
            info.setProperty("keyStorePassword", clientKeyStore2Password);
            info.setProperty("keyPassword", clientKeyPassword);
            testConnect(info, true, testUser, "ssltestpassword");
        } finally {
            deleteSslTestUser(testUser);
        }
    }

    /**
     * Verification when private key password differ from keyStore password.
     *
     * @throws Exception if error occur
     */
    @Test
    public void testClientKeyStorePkcs12() throws Exception {
        Assume.assumeTrue(haveSsl(sharedConnection) && isMariadbServer());
        String clientKeyStore2Path = System.getProperty("keystore2PathP12");
        String clientKeyStore2Password = System.getProperty("keystore2Password");
        Assume.assumeTrue(clientKeyStore2Password != null && clientKeyStore2Path != null);
        String testUser = "testKeystore";
        // For this testcase, the testUser must be configured with ssl_type=X509
        createSslTestUser(testUser);

        try {
            Properties info = new Properties();
            info.setProperty("useSSL", "true");
            info.setProperty("serverSslCert", serverCertificatePath);
            info.setProperty("keyStore", "file:///" + clientKeyStore2Path);
            info.setProperty("keyStorePassword", clientKeyStore2Password);
            testConnect(info, true, testUser, "ssltestpassword");
        } finally {
            deleteSslTestUser(testUser);
        }
    }

    @Test
    public void testKeyStoreWithProperties() throws Exception {
        Assume.assumeTrue(haveSsl(sharedConnection) && isMariadbServer());
        Assume.assumeNotNull(clientKeystorePath);
        Assume.assumeNotNull(serverCertificatePath);
        // generate a trustStore from the canned serverCertificate
        File tempKeystore = File.createTempFile("keystore", ".tmp");
        String keystorePath = tempKeystore.getAbsolutePath();

        String initialTrustStore = System.getProperty("javax.net.ssl.trustStore");
        String initialTrustStorePwd = System.getProperty("javax.net.ssl.trustStorePassword");
        String initialKeyStore = System.getProperty("javax.net.ssl.keyStore");
        String initialKeyStorePwd = System.getProperty("javax.net.ssl.keyStorePassword");

        String testUser = "testKeystore";
        // For this testcase, the testUser must be configured with ssl_type=X509
        createSslTestUser(testUser);

        try {
            generateKeystoreFromFile(serverCertificatePath, keystorePath, "mysecret");

            System.setProperty("javax.net.ssl.trustStore", keystorePath);
            System.setProperty("javax.net.ssl.trustStorePassword", "mysecret");
            System.setProperty("javax.net.ssl.keyStore", clientKeystorePath);
            System.setProperty("javax.net.ssl.keyStorePassword", clientKeystorePassword);

            Properties info = new Properties();
            info.setProperty("useSSL", "true");
            info.setProperty("trustStoreType", "JKS");
            info.setProperty("keyStoreType", "JKS");
            testConnect(info, true, testUser, "ssltestpassword");
        } finally {
            if (initialTrustStore == null) {
                System.clearProperty("javax.net.ssl.trustStore");
            } else {
                System.setProperty("javax.net.ssl.trustStore", initialTrustStore);
            }
            if (initialTrustStorePwd == null) {
                System.clearProperty("javax.net.ssl.trustStorePassword");
            } else {
                System.setProperty("javax.net.ssl.trustStorePassword", initialTrustStorePwd);
            }
            if (initialKeyStore != null) {
                System.setProperty("javax.net.ssl.keyStore", initialKeyStore);
            } else {
                System.clearProperty("javax.net.ssl.keyStore");
            }
            if (initialKeyStorePwd != null) {
                System.setProperty("javax.net.ssl.keyStorePassword", initialKeyStorePwd);
            } else {
                System.clearProperty("javax.net.ssl.keyStorePassword");
            }
            tempKeystore.delete();
        }
    }

    @Test
    public void testKeyStoreWhenServerTrustedWithProperties() throws Exception {
        Assume.assumeTrue(haveSsl(sharedConnection) && isMariadbServer());
        Assume.assumeNotNull(clientKeystorePath);
        // generate a trustStore from the canned serverCertificate
        File tempKeystore = File.createTempFile("keystore", ".tmp");
        String keystorePath = tempKeystore.getAbsolutePath();

        String initialTrustStore = System.getProperty("javax.net.ssl.trustStore");
        String initialTrustStorePwd = System.getProperty("javax.net.ssl.trustStorePassword");
        String initialKeyStore = System.getProperty("javax.net.ssl.keyStore");
        String initialKeyStorePwd = System.getProperty("javax.net.ssl.keyStorePassword");

        String testUser = "testKeystore";
        // For this testcase, the testUser must be configured with ssl_type=X509
        createSslTestUser(testUser);

        try {
            generateKeystoreFromFile(serverCertificatePath, keystorePath, "mysecret");

            System.clearProperty("javax.net.ssl.trustStore");
            System.clearProperty("javax.net.ssl.trustStorePassword");
            System.setProperty("javax.net.ssl.keyStore", clientKeystorePath);
            System.setProperty("javax.net.ssl.keyStorePassword", clientKeystorePassword);

            Properties info = new Properties();
            info.setProperty("useSSL", "true");
            info.setProperty("trustServerCertificate", "true");
            info.setProperty("keyStoreType", "JKS");
            info.setProperty("trustStoreType", "JKS");

            testConnect(info, true, testUser, "ssltestpassword");
        } finally {
            if (initialTrustStore != null) {
                System.setProperty("javax.net.ssl.trustStore", initialTrustStore);
            }
            if (initialTrustStorePwd != null) {
                System.setProperty("javax.net.ssl.trustStorePassword", initialTrustStorePwd);
            }
            if (initialKeyStore != null) {
                System.setProperty("javax.net.ssl.keyStore", initialKeyStore);
            } else {
                System.clearProperty("javax.net.ssl.keyStore");
            }
            if (initialKeyStorePwd != null) {
                System.setProperty("javax.net.ssl.keyStorePassword", initialKeyStorePwd);
            } else {
                System.clearProperty("javax.net.ssl.keyStorePassword");
            }
            tempKeystore.delete();
        }
    }

    @Test
    public void testClientKeyStoreProperties() throws SQLException, IOException, KeyStoreException,
                                              CertificateException, NoSuchAlgorithmException {
        Assume.assumeTrue(haveSsl(sharedConnection) && isMariadbServer());
        Assume.assumeNotNull(serverCertificatePath);
        // This test only runs if a client keystore and password have been passed in as properties
        // (-DkeystorePath and -DkeystorePassword)
        // You can create a keystore as follows:
        // echo "kspass" | openssl pkcs12 -export -in "${clientCertFile}" -inkey "${clientKeyFile}" -out
        // "${clientKeystoreFile}"
        //   -name "mysqlAlias" -pass stdin
        Assume.assumeTrue(clientKeystorePathDefined());

        File tempKeystore = File.createTempFile("keystore", ".tmp");
        String keystorePath = tempKeystore.getAbsolutePath();

        String testUser = "testKeystore";
        // For this testcase, the testUser must be configured with ssl_type=X509
        createSslTestUser(testUser);
        String initialTrustStore = System.getProperty("javax.net.ssl.trustStore");
        String initialTrustStorePwd = System.getProperty("javax.net.ssl.trustStorePassword");
        try {
            generateKeystoreFromFile(serverCertificatePath, keystorePath, "mysecret");

            System.setProperty("javax.net.ssl.trustStore", keystorePath);
            System.setProperty("javax.net.ssl.trustStorePassword", "mysecret");

            Properties info = new Properties();
            info.setProperty("useSSL", "true");
            info.setProperty("keyStore", "file:///" + clientKeystorePath);
            info.setProperty("keyStorePassword", clientKeystorePassword);
            info.setProperty("trustStoreType", "JKS");
            info.setProperty("keyStoreType", "JKS");

            testConnect(info, true, testUser, "ssltestpassword");
        } finally {
            if (initialTrustStore == null) {
                System.clearProperty("javax.net.ssl.trustStore");
            } else {
                System.setProperty("javax.net.ssl.trustStore", initialTrustStore);
            }
            if (initialTrustStorePwd == null) {
                System.clearProperty("javax.net.ssl.trustStorePassword");
            } else {
                System.setProperty("javax.net.ssl.trustStorePassword", initialTrustStorePwd);
            }

            deleteSslTestUser(testUser);
        }
    }

    @Test(expected = SQLException.class)
    public void testTruststoreAndClientKeystoreWrongPassword() throws SQLException, IOException,
                                                              KeyStoreException,
                                                              CertificateException,
                                                              NoSuchAlgorithmException {
        Assume.assumeTrue(haveSsl(sharedConnection) && isMariadbServer());
        Assume.assumeNotNull(serverCertificatePath);
        // This test only runs if a client keystore and password have been passed in as properties
        // (-DkeystorePath and -DkeystorePassword)
        // You can create a keystore as follows:
        // echo "kspass" | openssl pkcs12 -export -in "${clientCertFile}" -inkey "${clientKeyFile}"
        // -out "${clientKeystoreFile}" -name "mysqlAlias" -pass stdin
        Assume.assumeTrue(clientKeystorePathDefined());

        String testUser = "testWrongPwd";
        // For this testcase, the testUser must be configured with ssl_type=X509
        createSslTestUser(testUser);

        // generate a truststore from the canned server certificate
        File tempTruststore = File.createTempFile("truststore", ".tmp");
        String truststorePath = tempTruststore.getAbsolutePath();
        try {
            generateKeystoreFromFile(serverCertificatePath, truststorePath, null);

            Properties info = new Properties();
            info.setProperty("useSSL", "true");
            info.setProperty("trustStoreType", "JKS");
            info.setProperty("keyStoreType", "JKS");
            info.setProperty("trustStore", "file:///" + truststorePath);
            info.setProperty("keyStore", "file:///" + clientKeystorePath);
            info.setProperty("keyStorePassword", "notthekeystorepass");
            testConnect(info, true, testUser, "ssltestpassword");
        } finally {
            tempTruststore.delete();
            deleteSslTestUser(testUser);
        }
    }

    private boolean clientKeystorePathDefined() {
        return clientKeystorePath != null && !clientKeystorePath.isEmpty()
               && clientKeystorePassword != null && !clientKeystorePassword.isEmpty();
    }

    private void createSslTestUser(String user) throws SQLException {
        boolean useOldNotation = true;
        if ((isMariadbServer() && minVersion(10, 2, 0))
            || (!isMariadbServer() && minVersion(8, 0, 0))) {
            useOldNotation = false;
        }

        Statement st = sharedConnection.createStatement();
        try {
            st.execute("DROP USER '" + user + "'@'%'");
        } catch (SQLException e) {
            // eat
        }
        st.execute("FLUSH PRIVILEGES");
        if (useOldNotation) {
            st.execute("CREATE USER '" + user + "'@'%'");
            st.execute("GRANT SELECT on *.* to '" + user
                       + "'@'%' identified by 'ssltestpassword' REQUIRE X509");
        } else {
            st.execute("CREATE USER '" + user
                       + "'@'%' identified by 'ssltestpassword' REQUIRE X509");
            st.execute("GRANT SELECT on *.* to '" + user + "'@'%'");
        }
    }

    private void deleteSslTestUser(String user) throws SQLException {
        Statement st = sharedConnection.createStatement();
        st.execute("drop user '" + user + "'@'%'");
    }

    private void generateKeystoreFromFile(
      String certificateFile, String keystoreFile, String password)
      throws KeyStoreException, CertificateException, IOException, NoSuchAlgorithmException {
    InputStream inStream;
    final KeyStore ks = KeyStore.getInstance("JKS");

    // generate a keystore from the provided cert
    if (certificateFile.startsWith("classpath:")) {
      // Load it from a classpath relative file
      String classpathFile = certificateFile.substring("classpath:".length());
      inStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(classpathFile);
    } else {
      inStream = new FileInputStream(certificateFile);
    }

    try {
      // Note: KeyStore requires it be loaded even if you don't load anything into it:
      ks.load(null);
    } catch (Exception e) {
      throw new KeyStoreException("not expected error :" + e.getMessage());
    }
    CertificateFactory cf = CertificateFactory.getInstance("X.509");
    Collection<? extends Certificate> caList = cf.generateCertificates(inStream);
    inStream.close();
    for (Certificate ca : caList) {
      ks.setCertificateEntry(UUID.randomUUID().toString(), ca);
    }

    try (ByteArrayOutputStream keyStoreOut = new ByteArrayOutputStream()) {

      ks.store(keyStoreOut, password == null ? new char[0] : password.toCharArray());

      // write the key to the file system
      try (FileOutputStream keyStoreStream = new FileOutputStream(keystoreFile)) {
        keyStoreStream.write(keyStoreOut.toByteArray());
      }
    }
  }
}
