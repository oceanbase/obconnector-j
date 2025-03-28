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

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class TruncateExceptionTest extends BaseTest {

    /** Tables initialisation. */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("TruncateExceptionTest", "id tinyint");
        createTable("TruncateExceptionTest2",
            "id tinyint not null primary key auto_increment, id2 tinyint ");
    }

    @Test
    public void truncationThrowError() {
        try {
            queryTruncation(true);
            fail("Must have thrown SQLException");
        } catch (SQLException e) {
            // normal error
        }
    }

    @Test
    public void truncationThrowNoError() {
        try {
            ResultSet resultSet = sharedConnection.createStatement().executeQuery(
                "SELECT @@sql_mode");
            resultSet.next();
            // if server is already throwing truncation, cancel test
            Assume.assumeFalse(resultSet.getString(1).contains("STRICT_TRANS_TABLES"));

            queryTruncation(false);
        } catch (SQLException e) {
            e.printStackTrace();

            fail("Must not have thrown exception");
        }
    }

    /**
     * Execute a query with truncated data.
     *
     * @param truncation connection parameter.
     * @throws SQLException if SQLException occur
     */
    public void queryTruncation(boolean truncation) throws SQLException {
    try (Connection connection = setConnection("&jdbcCompliantTruncation=" + truncation)) {
      try (Statement stmt = connection.createStatement()) {
        stmt.execute("INSERT INTO TruncateExceptionTest (id) VALUES (999)");
      }
    }
  }

    @Test
  public void queryTruncationFetch() throws SQLException {
    int[] autoInc = setAutoInc();
    try (Connection connection = setConnection("&jdbcCompliantTruncation=true")) {
      Statement stmt = connection.createStatement();
      stmt.execute("TRUNCATE TABLE TruncateExceptionTest2");
      stmt.setFetchSize(1);
      PreparedStatement pstmt =
          connection.prepareStatement(
              "INSERT INTO TruncateExceptionTest2 (id2) VALUES (?)",
              Statement.RETURN_GENERATED_KEYS);
      pstmt.setInt(1, 45);
      pstmt.addBatch();
      pstmt.setInt(1, 999);
      pstmt.addBatch();
      pstmt.setInt(1, 55);
      pstmt.addBatch();
      try {
        pstmt.executeBatch();
        fail("Must have thrown SQLException");
      } catch (SQLException e) {
        // expected
      }
      // resultSet must have been fetch
      ResultSet rs = pstmt.getGeneratedKeys();
      assertTrue(rs.next());
      assertEquals(autoInc[0] + autoInc[1], rs.getInt(1));
      if (sharedIsRewrite()) {
        // rewritten with semi-colons -> error has stopped
        assertFalse(rs.next());
      } else {
        assertTrue(rs.next());
        assertEquals(autoInc[1] + autoInc[0] * 2, rs.getInt(1));
        assertFalse(rs.next());
      }
    }
  }

    @Test
  public void queryTruncationBatch() throws SQLException {
    int[] autoInc = setAutoInc();
    try (Connection connection =
        setConnection(
            "&jdbcCompliantTruncation=true&useBatchMultiSendNumber=3&profileSql=true&log=true")) {
      Statement stmt = connection.createStatement();
      stmt.execute("TRUNCATE TABLE TruncateExceptionTest2");
      PreparedStatement pstmt =
          connection.prepareStatement(
              "INSERT INTO TruncateExceptionTest2 (id2) VALUES (?)",
              Statement.RETURN_GENERATED_KEYS);
      pstmt.setInt(1, 45);
      pstmt.addBatch();
      pstmt.setInt(1, 46);
      pstmt.addBatch();
      pstmt.setInt(1, 47);
      pstmt.addBatch();
      pstmt.setInt(1, 48);
      pstmt.addBatch();
      pstmt.setInt(1, 999);
      pstmt.addBatch();
      pstmt.setInt(1, 49);
      pstmt.addBatch();
      pstmt.setInt(1, 50);
      pstmt.addBatch();
      try {
        pstmt.executeBatch();
        fail("Must have thrown SQLException");
      } catch (SQLException e) {
        // expected
      }
      // resultSet must have been fetch
      ResultSet rs = pstmt.getGeneratedKeys();
      for (int i = 1; i <= (sharedIsRewrite() ? 4 : 6); i++) {
        assertTrue(rs.next());
        assertEquals(autoInc[1] + autoInc[0] * i, rs.getInt(1));
      }
      assertFalse(rs.next());
    }
  }
}
