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

import java.sql.*;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class CancelTest extends BaseTest {

    @Before
    public void cancelSupported() throws SQLException {
        requireMinimumVersion(5, 0);
        Assume.assumeTrue(System.getenv("MAXSCALE_VERSION") == null
                          && System.getenv("SKYSQL") == null);
    }

    @Test
  public void cancelTest() {
    Assume.assumeFalse(sharedIsAurora());
    try (Connection tmpConnection = openNewConnection(connUri, new Properties())) {

      Statement stmt = tmpConnection.createStatement();
      ExecutorService exec = Executors.newFixedThreadPool(1);
      // check blacklist shared
      exec.execute(new CancelThread(stmt));
      stmt.execute(
          "select * from information_schema.columns as c1,  information_schema.tables, information_schema.tables as t2");

      // wait for thread endings
      exec.shutdown();
      Assert.fail();
    } catch (SQLException e) {
      // normal exception
    }
  }

    @Test(timeout = 20000, expected = SQLTimeoutException.class)
  public void timeoutSleep() throws Exception {
    Assume.assumeFalse(sharedIsAurora());
    try (Connection tmpConnection = openNewConnection(connUri, new Properties())) {
      Statement stmt = tmpConnection.createStatement();
      stmt.setQueryTimeout(1); // query take more than 20 seconds (local DB)
      stmt.execute(
          "select * from information_schema.columns as c1,  information_schema.tables, information_schema.tables as t2");
    }
  }

    @Test(timeout = 20000, expected = SQLTimeoutException.class)
  public void timeoutPrepareSleep() throws Exception {
    Assume.assumeFalse(sharedIsAurora());
    try (Connection tmpConnection = openNewConnection(connUri, new Properties())) {
      try (PreparedStatement stmt =
          tmpConnection.prepareStatement(
              "select * from information_schema.columns as c1,  information_schema.tables, information_schema.tables as t2")) {
        stmt.setQueryTimeout(1); // query take more than 20 seconds (local DB)
        stmt.execute();
      }
    }
  }

    @Test(timeout = 10000)
  public void timeoutBatch() throws Exception {
    Assume.assumeFalse(sharedIsAurora());
    Assume.assumeTrue(!sharedOptions().allowMultiQueries && !sharedIsRewrite());
    createTable("timeoutBatch", "id int not null primary key auto_increment, aa text");

    try (Connection connection = setConnection("&maxQuerySizeToLog=92")) {
      Statement stmt = connection.createStatement();

      char[] arr = new char[1000];
      Arrays.fill(arr, 'a');
      String str = String.valueOf(arr);
      for (int i = 0; i < 20000; i++) {
        stmt.addBatch("INSERT INTO timeoutBatch (aa) VALUES ('" + str + "')");
      }
      stmt.setQueryTimeout(1);
      try {
        stmt.executeBatch();
        fail();
      } catch (BatchUpdateException b) {
        ResultSet rs2 = stmt.executeQuery("SELECT 2");
        assertTrue(rs2.next());
        assertEquals("2", rs2.getString(1));
      }
    }
  }

    @Test(timeout = 10000)
  public void timeoutPrepareBatch() throws Exception {
    Assume.assumeFalse(sharedIsAurora());
    Assume.assumeTrue(!sharedOptions().allowMultiQueries && !sharedIsRewrite());
    Assume.assumeTrue(!(sharedOptions().useBulkStmts && isMariadbServer() && minVersion(10, 2)));
    createTable("timeoutBatch", "aa text");
    try (Connection tmpConnection = openNewConnection(connUri, new Properties())) {
      char[] arr = new char[1000];
      Arrays.fill(arr, 'a');
      String str = String.valueOf(arr);
      try (PreparedStatement stmt =
          tmpConnection.prepareStatement("INSERT INTO timeoutBatch VALUES (?)")) {
        stmt.setQueryTimeout(1);
        for (int i = 0; i < 20000; i++) {
          stmt.setString(1, str);
          stmt.addBatch();
        }
        try {
          stmt.executeBatch();
          fail();
        } catch (BatchUpdateException b) {
          ResultSet rs2 = stmt.executeQuery("SELECT 2");
          assertTrue(rs2.next());
          assertEquals("2", rs2.getString(1));
        }
      }
    }
  }

    @Test
    public void noTimeoutSleep() throws Exception {
        Statement stmt = sharedConnection.createStatement();
        stmt.setQueryTimeout(1);
        stmt.execute("select sleep(0.5)");
    }

    @Test
    public void cancelIdleStatement() throws Exception {
        Statement stmt = sharedConnection.createStatement();
        stmt.cancel();
        ResultSet rs = stmt.executeQuery("select 1");
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 1);
    }

    private static class CancelThread implements Runnable {

        private final Statement stmt;

        public CancelThread(Statement stmt) {
            this.stmt = stmt;
        }

        @Override
    public void run() {
      try {
        Thread.sleep(100);
        stmt.cancel();
      } catch (SQLException | InterruptedException e) {
        e.printStackTrace();
      }
    }
    }
}
