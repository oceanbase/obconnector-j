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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class FetchSizeTest extends BaseTest {

    /** Tables initialisation. */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("fetchSizeTest1", "id int, test varchar(100)");
        createTable("fetchSizeTest2", "id int, test varchar(100)");
        createTable("fetchSizeTest3", "id int, test varchar(100)");
        createTable("fetchSizeTest4", "id int, test varchar(100)");
    }

    @Test
    public void batchFetchSizeTest() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        PreparedStatement pstmt = sharedConnection
            .prepareStatement("INSERT INTO fetchSizeTest1 (test) values (?)");
        stmt.setFetchSize(1);
        pstmt.setFetchSize(1);
        // check that fetch isn't use by batch execution
        for (int i = 0; i < 10; i++) {
            pstmt.setString(1, "" + i);
            pstmt.addBatch();
            stmt.addBatch("INSERT INTO fetchSizeTest1 (test) values ('aaa" + i + "')");
        }
        pstmt.executeBatch();
        stmt.executeBatch();

        ResultSet resultSet = stmt.executeQuery("SELECT count(*) from fetchSizeTest1");
        if (resultSet.next()) {
            assertEquals(20, resultSet.getLong(1));
        } else {
            fail("must have resultset");
        }
    }

    @Test
    public void fetchSizeNormalTest() throws SQLException {
        prepareRecords(100, "fetchSizeTest4");

        Statement stmt = sharedConnection.createStatement();
        stmt.setFetchSize(1);
        ResultSet resultSet = stmt.executeQuery("SELECT test FROM fetchSizeTest4");
        for (int counter = 0; counter < 100; counter++) {
            assertTrue(resultSet.next());
            assertEquals("" + counter, resultSet.getString(1));
        }
        assertFalse(resultSet.next());
    }

    @Test
    public void fetchSizeErrorWhileFetchTest() throws SQLException {
        prepareRecords(100, "fetchSizeTest3");

        Statement stmt = sharedConnection.createStatement();
        stmt.setFetchSize(1);
        ResultSet resultSet = stmt.executeQuery("SELECT test FROM fetchSizeTest3");
        for (int counter = 0; counter < 50; counter++) {
            assertTrue(resultSet.next());
            assertEquals("" + counter, resultSet.getString(1));
        }
        assertFalse(resultSet.isClosed());

        try {
            ResultSet rs2 = stmt.executeQuery("SELECT 1");
            if (rs2.next()) {
                assertEquals(1, rs2.getInt(1));
            } else {
                fail("resultset must have been active");
            }
        } catch (SQLException e) {
            fail("Must have worked");
        }

        try {
            assertFalse(resultSet.isClosed());
            for (int counter = 50; counter < 100; counter++) {
                assertTrue(resultSet.next());
                assertEquals("" + counter, resultSet.getString(1));
            }
            resultSet.close();
            assertTrue(resultSet.isClosed());
        } catch (SQLException sqlexception) {
            fail("must have throw an exception, since resulset must have been closed.");
        }
    }

    @Test
    public void fetchSizeBigSkipTest() throws SQLException {
        createTable("fetchSizeTest5", "id int, test varchar(100)");
        prepareRecords(300, "fetchSizeTest5");

        Statement stmt = sharedConnection.createStatement();
        stmt.setFetchSize(1);
        ResultSet resultSet = stmt.executeQuery("SELECT test FROM fetchSizeTest5");
        for (int counter = 0; counter < 100; counter++) {
            assertTrue(resultSet.next());
            assertEquals("" + counter, resultSet.getString(1));
        }
        resultSet.close();
        try {
            resultSet.next();
            fail("Must have thrown exception");
        } catch (SQLException sqle) {
            assertTrue(sqle.getMessage().contains("Operation not permit on a closed resultSet"));
        }

        resultSet = stmt.executeQuery("SELECT test FROM fetchSizeTest5");
        for (int counter = 0; counter < 100; counter++) {
            assertTrue(resultSet.next());
            assertEquals("" + counter, resultSet.getString(1));
        }
        stmt.execute("Select 1");
        // result must be completely loaded
        try {
            resultSet.next();
            fail("Must have thrown exception");
        } catch (SQLException sqle) {
            assertTrue(sqle.getMessage().contains("Operation not permit on a closed resultSet"));
        }
        stmt.close();
    }

    private void prepareRecords(int recordNumber, String tableName) throws SQLException {
        PreparedStatement pstmt = sharedConnection.prepareStatement("INSERT INTO " + tableName
                                                                    + " (test) values (?)");
        for (int i = 0; i < recordNumber; i++) {
            pstmt.setString(1, "" + i);
            pstmt.addBatch();
        }
        pstmt.executeBatch();
    }

    /**
     * CONJ-315/CONJ-531 : statement interruption.
     *
     * @throws SQLException sqle
     */
    @Ignore
  public void fetchSizeCancel() throws SQLException {
    ifMaxscaleRequireMinimumVersion(2, 2);
    Assume.assumeTrue(!sharedOptions().profileSql);
    long start = System.currentTimeMillis();
    try (Statement stmt = sharedConnection.createStatement()) {
      stmt.executeQuery(
          "select * from information_schema.columns as c1,  information_schema.tables LIMIT 200000");
    }
    final long normalExecutionTime = System.currentTimeMillis() - start;

    start = System.currentTimeMillis();
    try (Statement stmt = sharedConnection.createStatement()) {
      stmt.setFetchSize(1);
      stmt.executeQuery(
          "select * from information_schema.columns as c1,  information_schema.tables LIMIT 200000");
      stmt.cancel();
    }
    long interruptedExecutionTime = System.currentTimeMillis() - start;

//    Assume.assumeTrue(minVersion(10, 1)); // 10.1.2 in fact
//
//    // ensure that query is a long query. if not cancelling the query (that might lead to creating a
//    // new connection)
//    // may not render the test reliable
//    String maxscaleVersion = System.getenv("MAXSCALE_VERSION");
//    if (maxscaleVersion == null && normalExecutionTime > 500) {
//      assertTrue(
//          "interruptedExecutionTime:"
//              + interruptedExecutionTime
//              + " normalExecutionTime:"
//              + normalExecutionTime,
//          interruptedExecutionTime < normalExecutionTime);
//    }
  }

    @Ignore
  public void fetchSizePrepareCancel() throws SQLException {
    ifMaxscaleRequireMinimumVersion(2, 2);
    Assume.assumeTrue(!sharedOptions().profileSql && !sharedOptions().pool);

    long start;
    long normalExecutionTime;

    try (PreparedStatement stmt =
        sharedConnection.prepareStatement(
            "select * from information_schema.columns as c1,  information_schema.tables, mysql.user LIMIT 50000")) {
      start = System.currentTimeMillis();
      stmt.executeQuery();
      normalExecutionTime = System.currentTimeMillis() - start;

      start = System.currentTimeMillis();
      stmt.setFetchSize(1);
      stmt.executeQuery();
      stmt.cancel();
    }

    long interruptedExecutionTime = System.currentTimeMillis() - start;

    System.out.println(normalExecutionTime);
    System.out.println(interruptedExecutionTime);
    // normalExecutionTime = 1500
    // interruptedExecutionTime = 77
    assertTrue(
        "interruptedExecutionTime:"
            + interruptedExecutionTime
            + " normalExecutionTime:"
            + normalExecutionTime,
        interruptedExecutionTime < normalExecutionTime);
  }
}
