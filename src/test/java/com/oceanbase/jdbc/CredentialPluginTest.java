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

import java.sql.*;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CredentialPluginTest extends BaseTest {

    /**
     * Create temporary test User.
     *
     * @throws SQLException if any
     */
    @Before
    public void before() throws SQLException {
        boolean useOldNotation = true;
        if ((isMariadbServer() && minVersion(10, 2, 0))
            || (!isMariadbServer() && minVersion(8, 0, 0))) {
            useOldNotation = false;
        }
        Statement stmt = sharedConnection.createStatement();
        if (useOldNotation) {
            stmt.execute("CREATE USER 'identityUser'@'%'");
            stmt.execute("GRANT SELECT ON " + database
                         + ".* TO 'identityUser'@'%' IDENTIFIED BY '!Passw0rd3Works'");
        } else {
            stmt.execute("CREATE USER 'identityUser'@'%' IDENTIFIED BY '!Passw0rd3Works'");
            stmt.execute("GRANT SELECT ON " + database + ".* TO 'identityUser'@'%'");
        }
    }

    /**
     * remove temporary test User.
     *
     * @throws SQLException if any
     */
    @After
    public void after() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        stmt.execute("DROP USER 'identityUser'@'%'");
    }

    @Test
  public void propertiesIdentityTest() throws SQLException {
    System.setProperty("mariadb.user", "identityUser");
    System.setProperty("mariadb.pwd", "!Passw0rd3Works");

    try (Connection conn =
        DriverManager.getConnection(
            "jdbc:oceanbase://"
                + ((hostname == null) ? "localhost" : hostname)
                + ":"
                + port
                + "/"
                + ((database == null) ? "" : database)
                + "?credentialType=PROPERTY"
                + ((options.useSsl != null) ? "&useSsl=" + options.useSsl : "")
                + ((options.serverSslCert != null)
                    ? "&serverSslCert=" + options.serverSslCert
                    : ""))) {
      Statement stmt = conn.createStatement();

      ResultSet rs = stmt.executeQuery("SELECT '5'");
      Assert.assertTrue(rs.next());
      Assert.assertEquals("5", rs.getString(1));
    }
  }

    @Test
  public void specificPropertiesIdentityTest() throws SQLException {
    System.setProperty("myUserKey", "identityUser");
    System.setProperty("myPwdKey", "!Passw0rd3Works");

    try (Connection conn =
        DriverManager.getConnection(
            "jdbc:oceanbase://"
                + ((hostname == null) ? "localhost" : hostname)
                + ":"
                + port
                + "/"
                + ((database == null) ? "" : database)
                + "?credentialType=PROPERTY"
                + "&userKey=myUserKey&pwdKey=myPwdKey")) {
      Statement stmt = conn.createStatement();

      ResultSet rs = stmt.executeQuery("SELECT '5'");
      Assert.assertTrue(rs.next());
      Assert.assertEquals("5", rs.getString(1));
    }
  }
}
