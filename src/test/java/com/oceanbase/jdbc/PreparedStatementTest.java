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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.LocalDateTime;

import org.junit.*;

public class PreparedStatementTest extends BaseTest {

    private static final int    ER_NO_SUCH_TABLE       = 1146;
    private static final String ER_NO_SUCH_TABLE_STATE = "42S02";

    /**
     * Initialisation.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("table1", "id1 int auto_increment primary key");
        createTable("table2", "id2 int auto_increment primary key");
        createTable("`testBigintTable`", "`id` bigint(20) unsigned NOT NULL, PRIMARY KEY (`id`)",
            "ENGINE=InnoDB DEFAULT CHARSET=utf8");
        createTable("`backTicksPreparedStatements`",
            "`id` int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY," + "`SLIndex#orBV#` text,"
                    + "`isM&M'sTasty?` bit(1) DEFAULT NULL,"
                    + "`Seems:LikeParam?` bit(1) DEFAULT NULL," + "`Webinar10-TM/ProjComp` text",
            "ENGINE=InnoDB DEFAULT CHARSET=utf8");
        createTable("test_insert_select", "`field1` varchar(20)");
        createTable("test_decimal_insert", "`field1` decimal(10, 7)");
        createTable("PreparedStatementTest1",
            "id int not null primary key auto_increment, test longblob");
        createTable("PreparedStatementTest2", "my_col varchar(20)");
        createTable("PreparedStatementTest3", "my_col varchar(20)");
        createTable("test_set_params", "c1 int, c2 varchar(20), c3 Timestamp");
    }

    @Test
    public void testClosingError() throws Exception {
        PreparedStatement preparedStatement = sharedConnection.prepareStatement("SELECT ?");
        preparedStatement.close();
        preparedStatement.close();
    }

    /**
     * Conj-238 : query not preparable. check fallback.
     *
     * @throws Exception exception
     */
    @Test
    public void cannotPrepareExecuteFallback() throws Exception {
        sharedConnection.createStatement().execute("TRUNCATE test_insert_select");
        PreparedStatement stmt = sharedConnection
            .prepareStatement(
                "insert into test_insert_select ( field1) (select  TMP.field1 from (select ? `field1` from dual) TMP)",
                Statement.RETURN_GENERATED_KEYS);
        stmt.setString(1, "test");
        stmt.execute();
        ResultSet rs = sharedConnection.createStatement().executeQuery(
            "select count(*) from test_insert_select");
        assertTrue(rs.next());
    }

    /**
     * Conj-238 : query not preparable. check batch fallback.
     *
     * @throws Exception exception
     */
    @Test
    public void cannotPrepareBatchFallback() throws Exception {
        sharedConnection.createStatement().execute("TRUNCATE test_insert_select");
        PreparedStatement stmt = sharedConnection
            .prepareStatement(
                "insert into test_insert_select ( field1) (select  TMP.field1 from (select ? `field1` from dual) TMP)",
                Statement.RETURN_GENERATED_KEYS);
        stmt.setString(1, "test");
        stmt.addBatch();
        stmt.executeBatch();

        ResultSet rs = sharedConnection.createStatement().executeQuery(
            "select count(*) from test_insert_select");
        assertTrue(rs.next());
    }

    /**
     * Conj-238 : query not preparable. check metadata message.
     *
     * @throws Exception exception
     */
    @Test
    public void cannotPrepareMetadata() throws Exception {
        //        Assume.assumeTrue(isMariadbServer() && !minVersion(10, 2)); // corrected in 10.2
        PreparedStatement stmt = sharedConnection
            .prepareStatement("insert into test_insert_select ( field1) (select  TMP.field1 from (select ? `field1` from dual) TMP)");
        try {
            stmt.getMetaData();
        } catch (SQLException e) {
            assertTrue(e
                .getMessage()
                .contains(
                    "If column exists but type cannot be identified (example 'select ? `field1` from dual'). "
                            + "Use CAST function to solve this problem (example 'select CAST(? as integer) `field1` from dual')"));
        }
    }

    /**
     * Conj-90.
     *
     * @throws SQLException exception
     */
    @Test
    public void reexecuteStatementTest() throws SQLException {
        // set the allowMultiQueries parameter
        try (Connection connection = setConnection("&allowMultiQueries=true")) {
            try (PreparedStatement stmt = connection.prepareStatement("SELECT 1")) {
                stmt.setFetchSize(Integer.MIN_VALUE);
                ResultSet rs = stmt.executeQuery();
                assertTrue(rs.next());

                try (ResultSet rs2 = stmt.executeQuery()) {
                  assertTrue(rs2.next());
                }
            }
        }
    }

    @Test
    public void testNoSuchTableBatchUpdate() throws SQLException {
        sharedConnection.createStatement().execute("drop table if exists vendor_code_test");
        PreparedStatement preparedStatement = sharedConnection
            .prepareStatement("INSERT INTO vendor_code_test VALUES(?)");
        preparedStatement.setString(1, "dummyValue");
        preparedStatement.addBatch();

        try {
            preparedStatement.executeBatch();
            fail("The above statement should result in an exception");
        } catch (SQLException sqlException) {
            assertEquals(ER_NO_SUCH_TABLE, sqlException.getErrorCode());
            assertEquals(ER_NO_SUCH_TABLE_STATE, sqlException.getSQLState());
        }
    }

    /**
     * CONJ-124: BigInteger not supported when setObject is used on PreparedStatements.
     *
     * @throws SQLException exception
     */
    @Test
    public void testBigInt() throws SQLException {
        Statement st = sharedConnection.createStatement();
        st.execute("INSERT INTO `testBigintTable` (`id`) VALUES (0)");
        PreparedStatement stmt = sharedConnection
            .prepareStatement("UPDATE `testBigintTable` SET `id` = ?");
        BigInteger bigT = BigInteger.valueOf(System.currentTimeMillis());
        stmt.setObject(1, bigT);
        stmt.executeUpdate();
        stmt = sharedConnection
            .prepareStatement("SELECT `id` FROM `testBigintTable` WHERE `id` = ?");
        stmt.setObject(1, bigT);
        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(0, rs.getBigDecimal(1).toBigInteger().compareTo(bigT));
    }

    /**
     * setObject should not truncate doubles.
     *
     * @throws SQLException exception
     */
    @Test
    public void testDoubleToDecimal() throws SQLException {
        PreparedStatement stmt = sharedConnection
            .prepareStatement("INSERT INTO test_decimal_insert (field1) VALUES (?)");
        Double value = 0.3456789;
        stmt.setObject(1, value, Types.DECIMAL, 7);
        stmt.executeUpdate();
        stmt = sharedConnection.prepareStatement("SELECT `field1` FROM test_decimal_insert");
        ResultSet rs = stmt.executeQuery();

        assertTrue(rs.next());
        assertEquals(value, rs.getDouble(1), 0.00000001);
    }

    @Test
    public void testDoubleToBigDecimal() throws SQLException {
        Assume.assumeFalse(sharedUsePrepare());
        Statement s = sharedConnection.createStatement();
        ResultSet rss = s.executeQuery("select -1.0 from dual");
        assertTrue(rss.next());
        assertEquals("DECIMAL", rss.getMetaData().getColumnTypeName(1));
//        System.out.println("getDouble: " + rss.getDouble(1));
        assertEquals(-1.0, rss.getDouble(1), 0.01);
//        System.out.println("getBigDecimal: " + rss.getBigDecimal(1));
        assertEquals(new BigDecimal(-1.0).setScale(1), rss.getBigDecimal(1));
        s.close();

        // PS protocol: Fix mysql compatibility getBigDecimal ,  #issue/48112657
        Connection conn = setConnection("&useServerPrepStmts=true");
        String sql = "select ? from dual";
        PreparedStatement pstmt = conn.prepareStatement(sql);
        pstmt.setDouble(1, -1.0);
        ResultSet rs = pstmt.executeQuery();
        assertTrue(rs.next());
        assertEquals("DOUBLE", rs.getMetaData().getColumnTypeName(1));
        assertEquals(-1, rs.getInt(1));
//        System.out.println("getDouble: " + rs.getDouble(1));
        assertEquals(-1.0, rs.getDouble(1), 0.01);
//        System.out.println("getBigDecimal: " + rs.getBigDecimal(1));
        assertEquals(new BigDecimal(-1.0).setScale(31), rs.getBigDecimal(1));
        rs.close();
        pstmt.close();
    }

    @Test
    public void testPreparedStatementsWithQuotes() throws SQLException {

        String query = "INSERT INTO backTicksPreparedStatements (`SLIndex#orBV#`,`Seems:LikeParam?`,"
                       + "`Webinar10-TM/ProjComp`,`isM&M'sTasty?`)" + " VALUES (?,?,?,?)";
        PreparedStatement ps = sharedConnection.prepareStatement(query);
        ps.setString(1, "slIndex");
        ps.setBoolean(2, false);
        ps.setString(3, "webinar10");
        ps.setBoolean(4, true);
        ps.execute();
        ResultSet rs = sharedConnection.createStatement().executeQuery(
            "SELECT `SLIndex#orBV#`,`Seems:LikeParam?`,"
                    + "`Webinar10-TM/ProjComp`,`isM&M'sTasty?` FROM backTicksPreparedStatements");
        assertTrue(rs.next());
        assertEquals("slIndex", rs.getString(1));
        assertEquals(false, rs.getBoolean(2));
        assertEquals("webinar10", rs.getString(3));
        assertEquals(true, rs.getBoolean(4));
    }

    /**
     * CONJ-264: SQLException when calling PreparedStatement.executeBatch() without calling
     * addBatch().
     *
     * @throws SQLException exception
     */
    @Test
    public void testExecuteBatch() throws SQLException {
        PreparedStatement preparedStatement = sharedConnection
            .prepareStatement("INSERT INTO table1 VALUE ?");
        try {
            int[] result = preparedStatement.executeBatch();
            assertEquals(0, result.length);
        } catch (SQLException sqle) {
            fail("Must not throw error");
        }
    }

    /**
     * CONJ-345 : COLLATE keyword failed on PREPARE statement.
     *
     * @throws SQLException exception
     */
    @Test
    public void testFallbackPrepare() throws SQLException {
        createTable(
            "testFallbackPrepare",
            "`test` varchar(500) COLLATE utf8mb4_unicode_ci NOT NULL",
            "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci");
        try (Connection connection = setConnection()) {
            Statement stmt = connection.createStatement();
            stmt.execute("SET @@character_set_connection = 'utf8mb4'");
            stmt.execute(
              "SELECT * FROM `testFallbackPrepare` WHERE `test` LIKE 'jj' COLLATE utf8mb4_unicode_ci");

            try (PreparedStatement preparedStatement = connection.prepareStatement(
                  "SELECT * FROM `testFallbackPrepare` WHERE `test` LIKE ? COLLATE utf8mb4_unicode_ci")) {
                preparedStatement.setString(1, "jj");
                preparedStatement.execute();
            } catch (SQLException sqle) {
                fail("Must not have issue, because must fallback on client prepare");
            }
        }
    }

    /**
     * CONJ-263: Exception must be throwing exception if exception append in multiple query.
     *
     * @throws SQLException exception
     */
    @Test
    public void testCallExecuteErrorBatch() throws SQLException {
        PreparedStatement pstmt = sharedConnection
            .prepareStatement("SELECT 1;INSERT INTO INCORRECT_QUERY");
        try {
            pstmt.execute();
            fail("Must have thrown error");
        } catch (SQLSyntaxErrorException sqlSyntax) {
            // normal exception
        } catch (SQLException sqle) {
            fail("must have thrown an SQLSyntaxErrorException");
        }
    }

    @Test
    public void testRewriteValuesMaxSizeOneParam() throws SQLException {
        testRewriteMultiPacket(false);
    }

    @Test
    public void testRewriteMultiMaxSizeOneParam() throws SQLException {
        testRewriteMultiPacket(true);
    }

    private void testRewriteMultiPacket(boolean notRewritable) throws SQLException {
    // aurora server fail something
    Assume.assumeFalse(sharedIsAurora());

    Statement statement = sharedConnection.createStatement();
    statement.execute("TRUNCATE PreparedStatementTest1");
    ResultSet rs = statement.executeQuery("select @@max_allowed_packet");
    assertTrue(rs.next());
    int maxAllowedPacket = rs.getInt(1);
    if (maxAllowedPacket < 21_000_000) { // to avoid OutOfMemory
      String query =
          "INSERT INTO PreparedStatementTest1 VALUES (null, ?)"
              + (notRewritable ? " ON DUPLICATE KEY UPDATE id=?" : "");
      // to have query exacting maxAllowedPacket size :
      // query size minus the ?
      // add first byte COM_QUERY
      // add 2 bytes (2 QUOTES for string parameter without need of escaping)
      // add 4 bytes if compression

      char[] arr = new char[maxAllowedPacket - (query.length() + (sharedUseCompression() ? 8 : 4))];
      for (int i = 0; i < arr.length; i++) {
        arr[i] = (char) ('a' + (i % 10));
      }

      try (Connection connection = setConnection("&rewriteBatchedStatements=true")) {
        PreparedStatement pstmt = connection.prepareStatement(query);
        for (int i = 0; i < 2; i++) {
          pstmt.setString(1, new String(arr));
          if (notRewritable) {
            pstmt.setInt(2, 1);
          }
          pstmt.addBatch();
        }
        int[] results = pstmt.executeBatch();
        assertEquals(2, results.length);
        for (int result : results) {
          if (!notRewritable
              || (isMariadbServer() && minVersion(10, 2) && sharedOptions().useBulkStmts)) {
            assertEquals(Statement.SUCCESS_NO_INFO, result);
          } else {
            assertEquals(1, result);
          }
        }
      }

      rs = statement.executeQuery("select * from PreparedStatementTest1");
      int counter = 0;
      while (rs.next()) {
        counter++;
        byte[] newBytes = rs.getBytes(2);
        assertEquals(arr.length, newBytes.length);
        for (int i = 0; i < arr.length; i++) {
          assertEquals(arr[i], newBytes[i]);
        }
      }
      assertEquals(2, counter);
    }
  }

    @Test
    public void testRewriteValuesMaxSize2Param() throws SQLException {
        Assume.assumeTrue(!sharedIsRewrite());
        testRewriteMultiPacket2param(false);
    }

    @Test
    public void testRewriteMultiMaxSize2Param() throws SQLException {
        testRewriteMultiPacket2param(true);
    }

    /**
     * Goal is send rewritten query with 2 parameters with size exacting max_allowed_packet.
     *
     * @param rewritableMulti rewritableMulti
     * @throws SQLException exception
     */
    private void testRewriteMultiPacket2param(boolean rewritableMulti) throws SQLException {
    Assume.assumeFalse(sharedIsAurora());
    Statement statement = sharedConnection.createStatement();
    statement.execute("TRUNCATE PreparedStatementTest1");
    ResultSet rs = statement.executeQuery("select @@max_allowed_packet");
    assertTrue(rs.next());
    int maxAllowedPacket = rs.getInt(1);
    if (maxAllowedPacket < 21000000) { // to avoid OutOfMemory
      String query =
          "INSERT INTO PreparedStatementTest1 VALUES (null, ?)"
              + (rewritableMulti ? "" : " ON DUPLICATE KEY UPDATE id=?");
      // to have query with exactly 2 values exacting maxAllowedPacket size :
      char[] arr = new char[(maxAllowedPacket - (query.length() + 18)) / 2];
      for (int i = 0; i < arr.length; i++) {
        arr[i] = (char) ('a' + (i % 10));
      }

      try (Connection connection =
          setConnection("&rewriteBatchedStatements=true&profileSql=true")) {
        PreparedStatement pstmt = connection.prepareStatement(query);
        for (int i = 0; i < 4; i++) {
          pstmt.setString(1, new String(arr));
          if (!rewritableMulti) {
            pstmt.setInt(2, 1);
          }
          pstmt.addBatch();
        }
        int[] results = pstmt.executeBatch();
        assertEquals(4, results.length);
        if (rewritableMulti
            || sharedIsRewrite()
            || (sharedOptions().useBulkStmts && isMariadbServer() && minVersion(10, 2))) {
          for (int result : results) {
            assertEquals(Statement.SUCCESS_NO_INFO, result);
          }
        } else {
          for (int result : results) {
            assertEquals(1, result);
          }
        }
      }

      rs = statement.executeQuery("select * from PreparedStatementTest1");
      int counter = 0;
      while (rs.next()) {
        counter++;
        byte[] newBytes = rs.getBytes(2);
        assertEquals(arr.length, newBytes.length);
        for (int i = 0; i < arr.length; i++) {
          assertEquals(arr[i], newBytes[i]);
        }
      }
      assertEquals(4, counter);
    }
  }

    /**
     * CONJ-273: permit client PrepareParameter without parameters.
     *
     * @throws Throwable exception
     */
    @Test
  public void clientPrepareStatementWithoutParameter() throws Throwable {
    try (Connection connection = setConnection("&rewriteBatchedStatements=true")) {
      PreparedStatement preparedStatement =
          connection.prepareStatement(
              "INSERT INTO PreparedStatementTest2 (my_col) VALUES ('my_val')");
      preparedStatement.execute();

      PreparedStatement preparedStatementMulti =
          connection.prepareStatement(
              "INSERT INTO PreparedStatementTest2 (my_col) VALUES ('my_val1'),('my_val2')");
      preparedStatementMulti.execute();
    }
  }

    /**
     * CONJ-470: rewrite with query that contain "values" keyword.
     *
     * @throws Throwable exception
     */
    @Ignore
  public void clientPrepareStatementValuesWithoutParameter() throws Throwable {
    createTable("clientPrepareStatementValuesWithoutParameter", "created_at datetime primary key");

    String query =
        "ALTER table clientPrepareStatementValuesWithoutParameter PARTITION BY RANGE COLUMNS( created_at ) "
            + "(PARTITION test_p201605 VALUES LESS THAN ('2016-06-01'))";

    try (Connection connection = setConnection("&rewriteBatchedStatements=true")) {
      try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
        preparedStatement.execute();
      }
    }
  }

    /**
     * CONJ-361: empty string test.
     *
     * @throws Throwable exception
     */
    @Test
  public void emptyStringParameter() throws Throwable {
    try (PreparedStatement preparedStatement =
        sharedConnection.prepareStatement(
            "INSERT INTO PreparedStatementTest3 (my_col) VALUES (?)")) {
      preparedStatement.setString(1, "");
      preparedStatement.execute();
    }
  }

    @Test
  public void nullStringParameter() throws Throwable {
    try (PreparedStatement preparedStatement =
        sharedConnection.prepareStatement(
            "INSERT INTO PreparedStatementTest3 (my_col) VALUES (?)")) {
      preparedStatement.setString(1, null);
      preparedStatement.execute();
    }
  }

    @Test
  public void testInsertSelectBulk() throws SQLException {
    // bug https://jira.mariadb.org/browse/MDEV-15133
    cancelForVersion(10, 3, 0);
    cancelForVersion(10, 3, 1);
    cancelForVersion(10, 3, 2);
    cancelForVersion(10, 3, 3);
    cancelForVersion(10, 3, 4);

    try (Statement statement = sharedConnection.createStatement()) {
      statement.execute("DROP TABLE IF EXISTS myTable");
      statement.execute(
          "CREATE TABLE myTable(v1 varchar(10), v2 varchar(10), v3 varchar(10), v4 varchar(10))");

      String[][] val = {
        {null, "b1", "c1", "d1"},
        {"a2", null, "c2", "d2"},
        {"a3", "b3", null, "d3"},
        {"a4", "b4", "c4", null},
        {"a5", "b5", "c5", "d5"}
      };
      try (PreparedStatement preparedStatement =
          sharedConnection.prepareStatement("INSERT INTO myTable VALUES (?, ?, ?, ?)")) {
        for (int i = 0; i < val.length; i++) {
          for (int j = 0; j < 4; j++) {
            preparedStatement.setString(j + 1, val[i][j]);
          }
          preparedStatement.addBatch();
        }
        preparedStatement.executeBatch();
      }

      ResultSet rs = statement.executeQuery("SELECT * from myTable");
      for (int i = 0; i < val.length; i++) {
        assertTrue(rs.next());
        for (int j = 0; j < 4; j++) {
          if (val[i][j] == null) {
            assertNull(rs.getString(j + 1));
          } else {
            assertEquals(val[i][j], rs.getString(j + 1));
          }
        }
      }
    }
  }

    @Test
  public void largePrepareUpdate() throws SQLException {
    createTable(
        "largePrepareUpdate",
        "a int not null primary key auto_increment, t varchar(256)",
        "engine=innodb");
    try (PreparedStatement stmt =
        sharedConnection.prepareStatement(
            "insert into largePrepareUpdate(t) values(?)", Statement.RETURN_GENERATED_KEYS)) {
      stmt.setString(1, "a");
      long updateRes = stmt.executeLargeUpdate();
      assertEquals(1L, updateRes);
      assertEquals(1L, stmt.getUpdateCount());
      assertEquals(1L, stmt.getLargeUpdateCount());

      stmt.setString(1, "b");
      stmt.addBatch();
      stmt.setString(1, "c");
      stmt.addBatch();
      long[] batchRes = stmt.executeLargeBatch();
      assertArrayEquals(new long[] {1, 1}, batchRes);
      ResultSet rs = stmt.getGeneratedKeys();
      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1));

      assertEquals(0L, stmt.getLargeMaxRows());
      stmt.setLargeMaxRows(10_000L);
      assertEquals(10_000L, stmt.getLargeMaxRows());
    }
  }

    @Test
    public void compareWithText() throws SQLException {
        PreparedStatement pstmt = sharedConnection
            .prepareStatement("insert into test_set_params values(?,?,?)");
        pstmt.setInt(1, 1);
        pstmt.setString(2, "abcdefg");
        pstmt.setTimestamp(3, Timestamp.valueOf(LocalDateTime.now()));
        pstmt.executeUpdate();

        Connection conn = setConnection("&useServerPrepStmts=true");
        PreparedStatement pstmt1 = conn
            .prepareStatement("insert into test_set_params values(?,?,?)");
        pstmt1.setInt(1, 2);
        pstmt1.setString(2, "hijklmn");
        pstmt1.setTimestamp(3, Timestamp.valueOf(LocalDateTime.now()));
        pstmt1.executeUpdate();

    }

    @Test
    public void testEmulateUnsupportedPstmts() throws SQLException {
        Assert.assertTrue(sharedOptions().useServerPrepStmts);

        try {
            PreparedStatement pstmt = sharedConnection
                .prepareStatement("select *, rowid from PreparedStatementTest3");
            fail("PS protocol shouldn't be degraded to TEXT protocol");
        } catch (SQLException e) {
            //
        }

        Connection conn = setConnection("&emulateUnsupportedPstmts=true");
        PreparedStatement pstmt = conn
            .prepareStatement("select rowid, * from PreparedStatementTest3");

        pstmt.close();
        conn.close();
    }

    @Test
    public void testFetchSize() throws Exception {
        Connection conn = setConnection("");
        PreparedStatement pstmt = conn.prepareStatement("SELECT 1");
        pstmt.setFetchSize(Integer.MIN_VALUE);
        pstmt.executeQuery();

        try {
            conn.createStatement().executeQuery("SELECT 2");
            fail("Should have caught a streaming exception here");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        Connection conn2 = setConnection("");
        pstmt = conn2.prepareStatement("SELECT 2");
        pstmt.setFetchSize(Integer.MIN_VALUE);
        ResultSet rs = pstmt.executeQuery();

        rs.close();
        conn2.createStatement().executeQuery("SELECT 3");
    }

    @Test
    public void testClearParam() throws SQLException {
        createTable("test_clear_param","v0 int");
        PreparedStatement pstmt = sharedConnection.prepareStatement("insert into test_clear_param values (?)");
        pstmt.setInt(1,1);
        pstmt.addBatch();
        pstmt.setInt(1,2);
        pstmt.addBatch();
        pstmt.executeBatch();
        pstmt.addBatch();
        pstmt.executeBatch();
        ResultSet rs = sharedConnection.createStatement().executeQuery("select * from test_clear_param");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertFalse(rs.next());
    }

    @Test
    public void fix51317355() throws Exception {
        Connection conn = setConnectionOrigin("?useServerPrepStmts=true");
        Statement stmt = conn.createStatement();
        stmt.execute("drop table if exists testPrepStmtGeneratedKeys");
        stmt.execute("create table testPrepStmtGeneratedKeys(id int AUTO_INCREMENT PRIMARY KEY, n INT)");
        PreparedStatement ps = conn.prepareStatement(
            "INSERT INTO testPrepStmtGeneratedKeys (n) VALUES (?), (?), (?), (?), (?)",
            Statement.RETURN_GENERATED_KEYS);
        ps.setInt(1, 1);
        ps.setInt(2, 2);
        ps.setInt(3, 3);
        ps.setInt(4, 4);
        ps.setInt(5, 5);

        long count = ps.executeLargeUpdate();
        assertEquals(5, count);
        assertEquals(5, ps.getLargeUpdateCount());

        ResultSet rs = ps.getGeneratedKeys();

        long num = 0;
        while (rs.next()) {
            assertEquals(++num, rs.getLong(1));
        }
        assertEquals(5, num);
    }

    @Test
    public void fix51567691() throws Exception {
        createTable("test", "pk INT PRIMARY KEY AUTO_INCREMENT, field1 VARCHAR(4)");
        Connection conn = setConnection("&useServerPrepStmts=true");

        PreparedStatement pstmt = conn.prepareStatement("INSERT INTO test (field1) VALUES (?)",
            Statement.RETURN_GENERATED_KEYS);
        pstmt.setString(1, "abc");
        pstmt.execute();
        pstmt.getGeneratedKeys();

        pstmt = conn.prepareStatement("INSERT INTO test (field1) VALUES (?)", new int[] { 1 });
        pstmt.setString(1, "abc");
        pstmt.execute();
        pstmt.getGeneratedKeys();

        pstmt = conn.prepareStatement("INSERT INTO test (field1) VALUES (?)", new String[] { "pk" });
        pstmt.setString(1, "abc");
        pstmt.execute();
        pstmt.getGeneratedKeys();

        pstmt = conn.prepareStatement("INSERT INTO test (field1) VALUES (?)");
        pstmt.setString(1, "abc");
        pstmt.execute();

        try {
            pstmt.getGeneratedKeys();
            fail("Expected a SQLException here");
        } catch (SQLException sqlEx) {
            // expected
        }
    }

    @Test
    public void testSendRollback() throws SQLException {
        Connection conn = setConnection("&useServerPrepStmts=true&useCursorFetch=true&useOraclePrepareExecute=false");
        createTable("t0", "c0 int");
        Statement statement = conn.createStatement();
        for (int i = 0; i < 20; i++) {
            statement.addBatch("insert into t0 values(" + i + ")");
        }
        statement.executeBatch();

        conn.setAutoCommit(false);
        PreparedStatement ps = conn.prepareStatement("select * from t0 for update");
        ps.setFetchSize(10);
        ResultSet rs = ps.executeQuery();
        //assertTrue(rs.next());
        ps.close();
        conn.rollback();
        System.out.println("111");

        Connection conn2 = setConnection("&useServerPrepStmts=true&useOraclePrepareExecute=false");
        conn2.setAutoCommit(false);
        PreparedStatement ps2 = conn2.prepareStatement("select * from t0 for update");
        ResultSet rs2 = ps2.executeQuery();
        //assertTrue(rs2.next());
        ps2.close();
        conn2.rollback();
        System.out.println("222");
    }

}
