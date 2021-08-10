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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.sql.*;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import com.sun.jna.Platform;

public class Sha256AuthenticationTest extends BaseTest {

    private String serverPublicKey;
    private String forceTls = "";

    /**
     * Check requirement.
     *
     * @throws SQLException exception exception
     */
    @Before
    public void checkSsl() throws SQLException {
        //        Assume.assumeTrue(!isMariadbServer() && minVersion(5, 7));
        serverPublicKey = System.getProperty("serverPublicKey");
        // try default if not present
        if (serverPublicKey == null) {
            File sslDir = new File(System.getProperty("user.dir") + "/../ssl");
            if (sslDir.exists() && sslDir.isDirectory()) {
                serverPublicKey = System.getProperty("user.dir") + "/../ssl/public.key";
            }
        }
        Statement stmt = sharedConnection.createStatement();
        try {
            stmt.execute("DROP USER 'sha256User'@'%'");
        } catch (SQLException e) {
            // eat
        }
        try {
            stmt.execute("DROP USER 'cachingSha256User'@'%'");
        } catch (SQLException e) {
            // eat
        }

        if (minVersion(8, 0, 0)) {
            stmt.execute("CREATE USER 'sha256User'@'%' IDENTIFIED WITH sha256_password BY 'password'");
            stmt.execute("GRANT SELECT ON *.* TO 'sha256User'@'%'");
        } else {
            stmt.execute("CREATE USER 'sha256User'@'%'");
            stmt.execute("GRANT SELECT ON *.* TO 'sha256User'@'%' IDENTIFIED WITH "
                         + "sha256_password BY 'password'");
        }
        if (minVersion(8, 0, 0)) {
            stmt.execute("CREATE USER 'cachingSha256User'@'%'  IDENTIFIED WITH caching_sha2_password BY 'password'");
            stmt.execute("GRANT SELECT ON *.* TO 'cachingSha256User'@'%'");
        } else {
            forceTls = "&enabledSslProtocolSuites=TLSv1.1";
        }
    }

    @Test
  public void sha256PluginTestWithServerRsaKey() throws SQLException {
    Assume.assumeNotNull(serverPublicKey);
    Assume.assumeTrue(minVersion(8, 0, 0));

    try (Connection conn =
        DriverManager.getConnection(
            "jdbc:oceanbase://"
                + ((hostname == null) ? "localhost" : hostname)
                + ":"
                + port
                + "/"
                + ((database == null) ? "" : database)
                + "?user=sha256User&password=password&serverRsaPublicKeyFile="
                + serverPublicKey)) {
      Statement stmt = conn.createStatement();

      ResultSet rs = stmt.executeQuery("SELECT '5'");
      Assert.assertTrue(rs.next());
      Assert.assertEquals("5", rs.getString(1));
    }
  }

    @Test
  public void sha256PluginTestWithoutServerRsaKey() throws SQLException {
    Assume.assumeTrue(!Platform.isWindows() && minVersion(8, 0, 0));

    try (Connection conn =
        DriverManager.getConnection(
            "jdbc:oceanbase://"
                + ((hostname == null) ? "localhost" : hostname)
                + ":"
                + port
                + "/"
                + ((database == null) ? "" : database)
                + "?user=sha256User&password=password&allowPublicKeyRetrieval")) {
      Statement stmt = conn.createStatement();

      ResultSet rs = stmt.executeQuery("SELECT '5'");
      Assert.assertTrue(rs.next());
      Assert.assertEquals("5", rs.getString(1));
    }
  }

    @Test
    public void sha256PluginTestException() {
        try {
            DriverManager.getConnection("jdbc:oceanbase://"
                                        + ((hostname == null) ? "localhost" : hostname) + ":"
                                        + port + "/" + ((database == null) ? "" : database)
                                        + "?user=sha256User&password=password");
            fail("must have throw exception");
        } catch (SQLException sqle) {
            assertTrue(sqle.getMessage().contains("RSA public key is not available client side"));
        }
    }

    @Test
  public void sha256PluginTestSsl() throws SQLException {
    Assume.assumeTrue(haveSsl(sharedConnection));
    try (Connection conn =
        DriverManager.getConnection(
            "jdbc:oceanbase://"
                + ((hostname == null) ? "localhost" : hostname)
                + ":"
                + port
                + "/"
                + ((database == null) ? "" : database)
                + "?user=sha256User&password=password&useSsl&trustServerCertificate"
                + forceTls)) {
      Statement stmt = conn.createStatement();

      ResultSet rs = stmt.executeQuery("SELECT '5'");
      Assert.assertTrue(rs.next());
      Assert.assertEquals("5", rs.getString(1));
    }
  }

    @Test
  public void cachingSha256PluginTestWithServerRsaKey() throws SQLException {
    Assume.assumeNotNull(serverPublicKey);
    Assume.assumeTrue(minVersion(8, 0, 0));
    try (Connection conn =
        DriverManager.getConnection(
            "jdbc:oceanbase://"
                + ((hostname == null) ? "localhost" : hostname)
                + ":"
                + port
                + "/"
                + ((database == null) ? "" : database)
                + "?user=cachingSha256User&password=password&serverRsaPublicKeyFile="
                + serverPublicKey)) {
      Statement stmt = conn.createStatement();

      ResultSet rs = stmt.executeQuery("SELECT '5'");
      Assert.assertTrue(rs.next());
      Assert.assertEquals("5", rs.getString(1));
    }
  }

    @Test
  public void cachingSha256PluginTestWithoutServerRsaKey() throws SQLException {
    Assume.assumeTrue(minVersion(8, 0, 0));
    try (Connection conn =
        DriverManager.getConnection(
            "jdbc:oceanbase://"
                + ((hostname == null) ? "localhost" : hostname)
                + ":"
                + port
                + "/"
                + ((database == null) ? "" : database)
                + "?user=cachingSha256User&password=password&allowPublicKeyRetrieval")) {
      Statement stmt = conn.createStatement();

      ResultSet rs = stmt.executeQuery("SELECT '5'");
      Assert.assertTrue(rs.next());
      Assert.assertEquals("5", rs.getString(1));
    }
  }

    @Test
    public void cachingSha256PluginTestException() {
        Assume.assumeTrue(minVersion(8, 0, 0));
        try {
            DriverManager.getConnection("jdbc:oceanbase://"
                                        + ((hostname == null) ? "localhost" : hostname) + ":"
                                        + port + "/" + ((database == null) ? "" : database)
                                        + "?user=cachingSha256User&password=password");
            fail("must have throw exception");
        } catch (SQLException sqle) {
            assertTrue(sqle.getMessage().contains("RSA public key is not available client side"));
        }
    }

    @Test
  public void cachingSha256PluginTestSsl() throws SQLException {
    Assume.assumeTrue(haveSsl(sharedConnection));
    Assume.assumeTrue(minVersion(8, 0, 0));
    try (Connection conn =
        DriverManager.getConnection(
            "jdbc:oceanbase://"
                + ((hostname == null) ? "localhost" : hostname)
                + ":"
                + port
                + "/"
                + ((database == null) ? "" : database)
                + "?user=cachingSha256User&password=password&useSsl&trustServerCertificate=true")) {
      Statement stmt = conn.createStatement();

      ResultSet rs = stmt.executeQuery("SELECT '5'");
      Assert.assertTrue(rs.next());
      Assert.assertEquals("5", rs.getString(1));
    }
  }
}
