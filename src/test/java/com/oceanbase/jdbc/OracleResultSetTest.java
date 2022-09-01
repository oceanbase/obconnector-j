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
import java.sql.*;

import org.junit.*;

public class OracleResultSetTest extends BaseOracleTest {

    /**
     * Initialisation.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("result_set_test", "id int not null primary key , name char(20)");
    }

    @Ignore
    public void isBeforeFirstFetchTest() throws SQLException {
        insertRows(1);
        Statement statement = sharedConnection.createStatement();
        statement.setFetchSize(1);
        ResultSet resultSet = statement.executeQuery("SELECT * FROM result_set_test");
        assertTrue(resultSet.isBeforeFirst());
        while (resultSet.next()) {
            assertFalse(resultSet.isBeforeFirst());
        }
        assertFalse(resultSet.isBeforeFirst());
        resultSet.close();
        try {
            resultSet.isBeforeFirst();
            fail("The above row should have thrown an SQLException");
        } catch (SQLException e) {
            // Make sure an exception has been thrown informing us that the ResultSet was closed
            assertTrue(e.getMessage().contains("closed"));
        }
    }

    /**
     * CONJ-424: Calling getGeneratedKeys() two times on the same connection, with different
     * PreparedStatement on a table that does not have an auto increment.
     */
    @Test
  public void testGeneratedKeysWithoutTableAutoIncrementCalledTwice() throws SQLException {
    createTable("gen_key_test_resultset", "name VARCHAR(40) NOT NULL, xml clob");
    String sql = "INSERT INTO gen_key_test_resultset (name, xml) VALUES (?, ?)";

    for (int i = 0; i < 2; i++) {
      try (PreparedStatement preparedStatement =
          sharedConnection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {

        preparedStatement.setString(1, "John");
        preparedStatement.setString(2, "<xml/>");
        preparedStatement.executeUpdate();

        try (ResultSet generatedKeysResultSet = preparedStatement.getGeneratedKeys()) {
          assertFalse(generatedKeysResultSet.next());
        }
      }
    }
  }

    @Ignore
  public void isBeforeFirstFetchZeroRowsTest() throws SQLException {
    insertRows(2);
    Statement statement = sharedConnection.createStatement();
    statement.setFetchSize(1);
    try (ResultSet resultSet = statement.executeQuery("SELECT * FROM result_set_test")) {
      assertTrue(resultSet.isBeforeFirst());
      assertTrue(resultSet.next());
      assertFalse(resultSet.isBeforeFirst());
      resultSet.close();
      try {
        resultSet.isBeforeFirst();
        fail("The above row should have thrown an SQLException");
      } catch (SQLException e) {
        // Make sure an exception has been thrown informing us that the ResultSet was closed
        assertTrue(e.getMessage().contains("closed"));
      }
    }
  }

    @Test
    public void isClosedTest() throws SQLException {
        insertRows(1);
        ResultSet resultSet = sharedConnection.createStatement().executeQuery(
            "SELECT * FROM result_set_test");
        assertFalse(resultSet.isClosed());
        while (resultSet.next()) {
            assertFalse(resultSet.isClosed());
        }
        assertFalse(resultSet.isClosed());
        resultSet.close();
        assertTrue(resultSet.isClosed());
    }

    @Ignore
    public void isBeforeFirstTest() throws SQLException {
        insertRows(1);
        ResultSet resultSet = sharedConnection.createStatement().executeQuery(
            "SELECT * FROM result_set_test");
        assertTrue(resultSet.isBeforeFirst());
        while (resultSet.next()) {
            assertFalse(resultSet.isBeforeFirst());
        }
        assertFalse(resultSet.isBeforeFirst());
        resultSet.close();
        try {
            resultSet.isBeforeFirst();
            fail("The above row should have thrown an SQLException");
        } catch (SQLException e) {
            // Make sure an exception has been thrown informing us that the ResultSet was closed
            assertTrue(e.getMessage().contains("closed"));
        }
    }

    @Test
    public void isFirstZeroRowsTest() throws SQLException {
        insertRows(0);
        Statement stmt = sharedConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY);
        ResultSet resultSet = stmt.executeQuery("SELECT * FROM result_set_test");
        assertFalse(resultSet.isFirst());
        assertFalse(resultSet.next()); // No more rows after this
        assertFalse(resultSet.isFirst()); // connectorj compatibility
        assertFalse(resultSet.first());
        resultSet.close();
        try {
            resultSet.isFirst();
            fail("The above row should have thrown an SQLException");
        } catch (SQLException e) {
            // Make sure an exception has been thrown informing us that the ResultSet was closed
            assertTrue(e.getMessage().contains("closed"));
        }
    }

    @Test
    public void isFirstOneRowsTest() throws SQLException {
        insertRows(1);
        Statement stmt = sharedConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY);
        ResultSet resultSet = stmt.executeQuery("SELECT * FROM result_set_test");
        assertFalse(resultSet.isFirst());
        assertTrue(resultSet.next());
        assertTrue(resultSet.isFirst());
        assertFalse(resultSet.next());
        assertFalse(resultSet.isFirst());
        assertTrue(resultSet.first());
        assertEquals(1, resultSet.getInt(1));
        resultSet.close();
        try {
            resultSet.isFirst();
            fail("The above row should have thrown an SQLException");
        } catch (SQLException e) {
            // Make sure an exception has been thrown informing us that the ResultSet was closed
            assertTrue(e.getMessage().contains("closed"));
        }
    }

    @Test
    public void isFirstTwoRowsTest() throws SQLException {
        isFirstTwoRowsTest(true);
        isFirstTwoRowsTest(false);
    }

    private void isFirstTwoRowsTest(boolean fetching) throws SQLException {
    insertRows(2);
    try (Statement statement =
        sharedConnection.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
      if (fetching) {
        statement.setFetchSize(1);
      }
      ResultSet resultSet = statement.executeQuery("SELECT * FROM result_set_test");
      assertFalse(resultSet.isFirst());
      assertTrue(resultSet.next());
      assertTrue(resultSet.isFirst());
      assertTrue(resultSet.next());
      assertFalse(resultSet.isFirst());
      assertFalse(resultSet.next()); // No more rows after this
      assertFalse(resultSet.isFirst());
      assertTrue(resultSet.first());
      assertEquals(1, resultSet.getInt(1));
      resultSet.close();
      try {
        resultSet.isFirst();
        fail("The above row should have thrown an SQLException");
      } catch (SQLException e) {
        // Make sure an exception has been thrown informing us that the ResultSet was closed
        assertTrue(e.getMessage().contains("closed"));
      }
    }
  }

    @Test
    public void isLastZeroRowsTest() throws SQLException {
        insertRows(0);
        ResultSet resultSet = sharedConnection.createStatement().executeQuery(
            "SELECT * FROM result_set_test");
        Assume.assumeFalse(resultSet.getType() == ResultSet.TYPE_FORWARD_ONLY);
        // Invalid operation on TYPE_FORWARD_ONLY CURSOR ResultSet for oracle mode: isLast
        assertFalse(resultSet.isLast()); // connectorj compatibility
        assertFalse(resultSet.next()); // No more rows after this
        assertFalse(resultSet.isLast());
        assertFalse(resultSet.last());
        resultSet.close();
        try {
            resultSet.isLast();
            fail("The above row should have thrown an SQLException");
        } catch (SQLException e) {
            // Make sure an exception has been thrown informing us that the ResultSet was closed
            assertTrue(e.getMessage().contains("closed"));
        }
    }

    @Test
    public void isLastOneRowsTest() throws SQLException {
        insertRows(1);
        ResultSet resultSet = sharedConnection.createStatement().executeQuery(
            "SELECT * FROM result_set_test");
        Assume.assumeFalse(resultSet.getType() == ResultSet.TYPE_FORWARD_ONLY);
        // Invalid operation on TYPE_FORWARD_ONLY CURSOR ResultSet for oracle mode: isLast
        assertFalse(resultSet.isLast());
        assertTrue(resultSet.next());
        assertTrue(resultSet.isLast());
        assertTrue(resultSet.last());
        assertFalse(resultSet.next()); // No more rows after this
        assertFalse(resultSet.isLast());
        assertTrue(resultSet.last());

        resultSet.close();
        try {
            resultSet.isLast();
            fail("The above row should have thrown an SQLException");
        } catch (SQLException e) {
            // Make sure an exception has been thrown informing us that the ResultSet was closed
            assertTrue(e.getMessage().contains("closed"));
        }
    }

    @Test
    public void isLastTwoRowsTest() throws SQLException {
        isLastTwoRowsTest(true);
        isLastTwoRowsTest(false);
    }

    private void isLastTwoRowsTest(boolean fetching) throws SQLException {
    insertRows(2);
    try (Statement statement = sharedConnection.createStatement()) {
      if (fetching) {
        statement.setFetchSize(1);
      }
      ResultSet resultSet = statement.executeQuery("SELECT * FROM result_set_test");
      Assume.assumeFalse(resultSet.getType() == ResultSet.TYPE_FORWARD_ONLY);
      // Invalid operation on TYPE_FORWARD_ONLY CURSOR ResultSet for oracle mode: isLast
      assertFalse(resultSet.isLast());
      assertTrue(resultSet.next());
      assertFalse(resultSet.isLast());
      assertTrue(resultSet.next());
      assertTrue(resultSet.isLast());
      assertFalse(resultSet.next()); // No more rows after this
      assertFalse(resultSet.isLast());
      assertTrue(resultSet.last());
      assertEquals(2, resultSet.getInt(1));
      resultSet.close();
      try {
        resultSet.isLast();
        fail("The above row should have thrown an SQLException");
      } catch (SQLException e) {
        // Make sure an exception has been thrown informing us that the ResultSet was closed
        assertTrue(e.getMessage().contains("closed"));
      }
    }
  }

    @Test
    public void isAfterLastZeroRowsTest() throws SQLException {
        insertRows(0);
        ResultSet resultSet = sharedConnection.createStatement().executeQuery(
            "SELECT * FROM result_set_test");
        assertFalse(resultSet.isAfterLast());
        assertFalse(resultSet.next()); // No more rows after this
        assertFalse(resultSet.isAfterLast());
        resultSet.close();
        try {
            resultSet.isAfterLast();
            fail("The above row should have thrown an SQLException");
        } catch (SQLException e) {
            // Make sure an exception has been thrown informing us that the ResultSet was closed
            assertTrue(e.getMessage().contains("closed"));
        }
    }

    @Test
    public void isAfterLastTwoRowsTest() throws SQLException {
        insertRows(2);
        ResultSet resultSet = sharedConnection.createStatement().executeQuery(
            "SELECT * FROM result_set_test");
        assertFalse(resultSet.isAfterLast());
        assertTrue(resultSet.next());
        assertFalse(resultSet.isAfterLast());
        assertTrue(resultSet.next());
        assertEquals(2, resultSet.getInt(1));
        assertFalse(resultSet.isAfterLast());
        assertFalse(resultSet.next()); // No more rows after this
        assertTrue(resultSet.isAfterLast());
        resultSet.close();
        try {
            resultSet.isAfterLast();
            fail("The above row should have thrown an SQLException");
        } catch (SQLException e) {
            // Make sure an exception has been thrown informing us that the ResultSet was closed
            assertTrue(e.getMessage().contains("closed"));
        }
    }

    @Test
  public void previousTest() throws SQLException {
    insertRows(2);
    Statement stmt =
        sharedConnection.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    try (ResultSet rs = stmt.executeQuery("SELECT * FROM result_set_test")) {
      assertFalse(rs.previous());
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
      assertFalse(rs.previous());
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1));
      assertTrue(rs.previous());
      assertEquals(1, rs.getInt(1));
      assertTrue(rs.last());
      assertEquals(2, rs.getInt(1));
    }
  }

    @Test
    public void firstTest() throws SQLException {
        insertRows(2);
        Statement stmt = sharedConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = stmt.executeQuery("SELECT * FROM result_set_test");
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertTrue(rs.first());
        assertTrue(rs.isFirst());
        rs.close();
        try {
            rs.first();
            fail("cannot call first() on a closed result set");
        } catch (SQLException sqlex) {
            // eat exception
        }
    }

    @Test
    public void lastTest() throws SQLException {
        insertRows(2);
        Statement stmt = sharedConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = stmt.executeQuery("SELECT * FROM result_set_test");
        assertTrue(rs.last());
        assertTrue(rs.isLast());
        assertFalse(rs.next());
        rs.first();
        rs.close();
        try {
            rs.last();
            fail("cannot call last() on a closed result set");
        } catch (SQLException sqlex) {
            // eat exception
        }
    }

    @Ignore
    public void relativeForwardTest() throws SQLException {
        insertRows(3);
        Statement stmt = sharedConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = stmt.executeQuery("SELECT * FROM result_set_test");
        assertTrue(rs.isBeforeFirst());
        assertFalse(rs.isFirst());
        assertFalse(rs.isLast());
        assertFalse(rs.isAfterLast());

        assertTrue(rs.relative(2));
        assertFalse(rs.isBeforeFirst());
        assertFalse(rs.isFirst());
        assertFalse(rs.isLast());
        assertFalse(rs.isAfterLast());

        assertTrue(rs.relative(1));
        assertFalse(rs.isBeforeFirst());
        assertFalse(rs.isFirst());
        assertTrue(rs.isLast());
        assertFalse(rs.isAfterLast());

        assertFalse(rs.relative(1));
        assertFalse(rs.isBeforeFirst());
        assertFalse(rs.isFirst());
        assertFalse(rs.isLast());
        assertTrue(rs.isAfterLast());

        assertFalse(rs.next());
        rs.close();
    }

    @Test
    public void relativeBackwardTest() throws SQLException {
        insertRows(3);
        Statement stmt = sharedConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = stmt.executeQuery("SELECT * FROM result_set_test");
        rs.afterLast();
        assertFalse(rs.isBeforeFirst());
        assertFalse(rs.isFirst());
        assertFalse(rs.isLast());
        assertTrue(rs.isAfterLast());

        assertTrue(rs.relative(-2));
        assertFalse(rs.isBeforeFirst());
        assertFalse(rs.isFirst());
        assertFalse(rs.isLast());
        assertFalse(rs.isAfterLast());

        assertTrue(rs.relative(-1));
        assertFalse(rs.isBeforeFirst());
        assertTrue(rs.isFirst());
        assertFalse(rs.isLast());
        assertFalse(rs.isAfterLast());

        assertFalse(rs.relative(-1));
        assertTrue(rs.isBeforeFirst());
        assertFalse(rs.isFirst());
        assertFalse(rs.isLast());
        assertFalse(rs.isAfterLast());

        assertFalse(rs.previous());
        rs.close();
    }

    private void insertRows(int numberOfRowsToInsert) throws SQLException {
        sharedConnection.createStatement().execute("truncate result_set_test ");
        for (int i = 1; i <= numberOfRowsToInsert; i++) {
            sharedConnection.createStatement().executeUpdate(
                "INSERT INTO result_set_test VALUES(" + i + ", 'row" + i + "')");
        }
    }

    /**
     * CONJ-403: NPE in getGenerated keys.
     *
     * @throws SQLException if error occur
     */
    @Test
    //oracle not exists auto_increment
  public void generatedKeyNpe() throws SQLException {
    createTable("generatedKeyNpe", "id int not null primary key , val int");
    Statement statement = sharedConnection.createStatement();
    statement.execute(
        "INSERT INTO generatedKeyNpe(id,val) values (1,0)", Statement.RETURN_GENERATED_KEYS);
    try (ResultSet rs = statement.getGeneratedKeys()) {
      assertFalse(rs.next());
    }
  }

    @Test
    public void generatedKeyError() throws SQLException {
        createTable("generatedKeyNpeError", "id int not null primary key , val int");
        Statement statement = sharedConnection.createStatement();
        statement.execute("INSERT INTO generatedKeyNpeError(id,val) values (1,0)");
        try {
            statement.getGeneratedKeys();
            fail();
        } catch (SQLException sqle) {
            assertEquals(
                "Cannot return generated keys : query was not set with Statement.RETURN_GENERATED_KEYS",
                sqle.getMessage());
        }
    }

    @Test
  public void testResultSetAbsolute() throws Exception {
    insertRows(50);
    try (Statement statement =
        sharedConnection.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
        statement.setFetchSize(10);
      try (ResultSet rs = statement.executeQuery("SELECT * FROM result_set_test")) {
        assertFalse(rs.absolute(52));
        assertFalse(rs.absolute(-52));

        assertTrue(rs.absolute(42));
        assertEquals("row42", rs.getString(2).trim());

        assertTrue(rs.absolute(-11));
        assertEquals("row40", rs.getString(2).trim());

//        if (Version.version.compareToIgnoreCase("2.2.8") >= 0) {
            try {
                rs.absolute(0);
                fail();
            } catch (SQLException sqle) {
                assertEquals("Invalid parameter: absolute(0)", sqle.getMessage());
            }
            assertFalse(rs.isBeforeFirst());
//        } else {
//          assertTrue(rs.absolute(0));
//          assertTrue(rs.isBeforeFirst());
//        }

        assertFalse(rs.absolute(51));
        assertTrue(rs.isAfterLast());

        assertTrue(rs.absolute(-1));
        assertEquals("row50", rs.getString(2).trim());

        assertTrue(rs.absolute(-50));
        assertEquals("row1", rs.getString(2).trim());

      }
    }
  }

    @Test
  public void testResultSetIsAfterLast() throws Exception {
    insertRows(2);
    try (Statement statement = sharedConnection.createStatement()) {
      statement.setFetchSize(1);
      try (ResultSet rs = statement.executeQuery("SELECT * FROM result_set_test")) {
        assertFalse(rs.isAfterLast());
        assertTrue(rs.next());
        assertFalse(rs.isAfterLast());
        assertTrue(rs.next());
        assertFalse(rs.isAfterLast());
        assertFalse(rs.next());
        assertTrue(rs.isAfterLast());
      }

      insertRows(0);
      try (ResultSet rs = statement.executeQuery("SELECT * FROM result_set_test")) {
        Assume.assumeFalse(rs.getType() == ResultSet.TYPE_FORWARD_ONLY);
        // Invalid operation on TYPE_FORWARD_ONLY CURSOR ResultSet for oracle mode: isLast
        assertFalse(rs.isAfterLast());
        assertFalse(rs.isLast());
        assertFalse(rs.next());
        assertFalse(rs.isLast());
        assertFalse(rs.isAfterLast()); // jdbc indicate that results with no rows return false.
      }
    }
  }

    @Test
  public void testResultSetAfterLast() throws Exception {
    try (Statement statement = sharedConnection.createStatement()) {
      checkLastResultSet(statement);
      statement.setFetchSize(1);
      checkLastResultSet(statement);
    }
  }

    private void checkLastResultSet(Statement statement) throws SQLException {
    insertRows(10);
    try (ResultSet rs = statement.executeQuery("SELECT * FROM result_set_test")) {
      Assume.assumeFalse(rs.getType() == ResultSet.TYPE_FORWARD_ONLY);
      // Invalid operation on TYPE_FORWARD_ONLY CURSOR ResultSet
      assertTrue(rs.last());
      assertFalse(rs.isAfterLast());
      assertTrue(rs.isLast());

      rs.afterLast();
      assertTrue(rs.isAfterLast());
      assertFalse(rs.isLast());
    }

    insertRows(0);
    try (ResultSet rs = statement.executeQuery("SELECT * FROM result_set_test")) {

      assertFalse(rs.last());
      assertFalse(rs.isAfterLast());
      assertFalse(rs.isLast());

      rs.afterLast();
      assertFalse(rs.isAfterLast()); // jdbc indicate that results with no rows return false.
      assertFalse(rs.isLast());
    }
  }

    @Test
    public void testStreamInsensitive() throws Exception {
        createTable("testStreamInsensitive", "s1 varchar(20)");

        for (int r = 0; r < 20; r++) {
            sharedConnection.createStatement().executeUpdate(
                "insert into testStreamInsensitive values('V" + r + "')");
        }
        Statement stmt = sharedConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(10);

        // reading forward
        ResultSet rs = stmt.executeQuery("select * from testStreamInsensitive");
        for (int i = 0; i < 20; i++) {
            assertTrue(rs.next());
            assertEquals("V" + i, rs.getString(1));
        }
        assertFalse(rs.next());

        rs = stmt.executeQuery("select * from testStreamInsensitive");
        for (int i = 0; i < 20; i++) {
            assertFalse(rs.isAfterLast());
            assertTrue(rs.next());
            assertEquals("V" + i, rs.getString(1));
            assertFalse(rs.isAfterLast());
        }
        assertFalse(rs.isAfterLast());
        assertFalse(rs.next());
        assertTrue(rs.isAfterLast());

        rs = stmt.executeQuery("select * from testStreamInsensitive");
        assertTrue(rs.absolute(20));
        assertEquals("V19", rs.getString(1));
        assertFalse(rs.isAfterLast());
        assertFalse(rs.absolute(21));
        assertTrue(rs.isAfterLast());

        // reading backward
        rs = stmt.executeQuery("select * from testStreamInsensitive");
        rs.afterLast();
        for (int i = 19; i >= 0; i--) {
            assertTrue(rs.previous());
            assertEquals("V" + i, rs.getString(1));
        }
        assertFalse(rs.previous());

        rs = stmt.executeQuery("select * from testStreamInsensitive");
        assertTrue(rs.last());
        assertEquals("V19", rs.getString(1));

        assertTrue(rs.first());
        assertEquals("V0", rs.getString(1));
    }

    @Test
    public void testForwardCursor() throws Exception {
        Assume.assumeTrue(sharedOptions().useCursorFetch);

        createTable("testForwardCursor", "s1 varchar(20)");

        for (int r = 0; r < 20; r++) {
            sharedConnection.createStatement().executeUpdate(
                "insert into testForwardCursor values('V" + r + "')");
        }
        Statement stmt = sharedConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(10);

        // reading forward
        ResultSet rs = stmt.executeQuery("select * from testForwardCursor");
        for (int i = 0; i < 20; i++) {
            assertTrue(rs.next());
            assertEquals("V" + i, rs.getString(1));
        }
        assertFalse(rs.next());

        // checking isAfterLast that may need to fetch next result
        rs = stmt.executeQuery("select * from testForwardCursor");
        for (int i = 0; i < 20; i++) {
            assertFalse(rs.isAfterLast());
            assertTrue(rs.next());
            assertEquals("V" + i, rs.getString(1));
            assertFalse(rs.isAfterLast());
        }
        assertFalse(rs.isAfterLast());
        assertFalse(rs.next());
        assertTrue(rs.isAfterLast());

        // reading backward
        rs = stmt.executeQuery("select * from testForwardCursor");
        try {
            rs.afterLast();
            fail("Must have thrown exception since afterLast is not possible when fetching");
        } catch (SQLException sqle) {
            assertTrue(sqle.getMessage().contains(
                "Invalid operation on TYPE_FORWARD_ONLY CURSOR or STREAMING ResultSet"));
        }
        try {
            rs.previous();
            fail("Must have thrown exception since previous is not possible when fetching");
        } catch (SQLException sqle) {
            assertTrue(sqle.getMessage().contains(
                "Invalid operation on TYPE_FORWARD_ONLY CURSOR or STREAMING ResultSet"));
        }

        rs = stmt.executeQuery("select * from testForwardCursor");
        try {
            rs.last();
            fail("last operation must fail for TYPE_FORWARD_ONLY cursor");
        } catch (SQLException sqle) {
            assertTrue(sqle.getMessage().contains(
                "Invalid operation on TYPE_FORWARD_ONLY CURSOR or STREAMING ResultSet"));
        }
        try {
            rs.first();
            fail("Must have thrown exception since previous is not possible when fetching");
        } catch (SQLException sqle) {
            assertTrue(sqle.getMessage().contains(
                "Invalid operation on TYPE_FORWARD_ONLY CURSOR or STREAMING ResultSet"));
        }
    }

    @Ignore
    // need to rewrite
    public void firstForwardTest() throws SQLException {
        // First must work when complete or scrollable cursor.
        Statement stmt = sharedConnection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1 from dual");
        assertTrue(rs.first());
        assertFalse(rs.previous());
        assertTrue(rs.absolute(1));
        assertFalse(rs.relative(-1));

        // First must fail when forward cursor or streaming.
        // However, there is no streaming ResultSet for oracle at all
        PreparedStatement pstmt = sharedConnection.prepareStatement("SELECT 1 from dual ",
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        rs = pstmt.executeQuery();
        try {
            rs.first();
            fail("first operation must fail when TYPE_FORWARD_ONLY and streaming");
        } catch (SQLException sqle) {
            assertTrue(sqle.getMessage().contains(
                "Invalid operation on TYPE_FORWARD_ONLY CURSOR or STREAMING ResultSet"));
        }
        try {
            rs.previous();
            fail("previous operation must fail when TYPE_FORWARD_ONLY and streaming");
        } catch (SQLException sqle) {
            assertTrue(sqle.getMessage().contains(
                "Invalid operation on TYPE_FORWARD_ONLY CURSOR or STREAMING ResultSet"));
        }
        try {
            rs.absolute(1);
            fail("absolute operation must fail when TYPE_FORWARD_ONLY and streaming");
        } catch (SQLException sqle) {
            assertTrue(sqle.getMessage().contains(
                "Invalid operation on TYPE_FORWARD_ONLY CURSOR or STREAMING ResultSet"));
        }
        try {
            rs.relative(-1);
            fail("relative operation must fail when TYPE_FORWARD_ONLY and streaming");
        } catch (SQLException sqle) {
            assertTrue(sqle.getMessage().contains(
                "Invalid operation on TYPE_FORWARD_ONLY CURSOR or STREAMING ResultSet"));
        }
    }

    /**
     * CONJ-429 : ResultSet.getDouble/getFloat may throws a NumberFormatException.
     *
     * @throws SQLException if any abnormal error occur
     */
    @Test
  public void testNumericType() throws SQLException {
    createTable(
        "numericTypeTable",
        "t1 number(2), "
            + "t2 number(1), "
            + "t3 number(3),  "
            + "t4 number(4), "
            + "t5 int, "
            + "t6 number(12), "
            + "t7 number(12,5), "
            + "t9 number(12,4), "
            + "t11 char(10),"
            + "t12 varchar(10),"
            + "t15 clob,"
            + "t16 blob,"
            + "t17 date");

    try (Statement stmt = sharedConnection.createStatement()) {
        stmt.execute(
                "INSERT into numericTypeTable values (1, 1, 1, 1, 1, 1, 1, 1, 'a', 'a', 'a', 'a', sysdate)");
      try (ResultSet rs = stmt.executeQuery("select * from numericTypeTable")) {
        assertTrue(rs.next());
        try{
            floatDoubleCheckResult(rs);
        }catch(SQLException e){
            if(e.getMessage().contains("getDouble not available for data field type Types.FLOAT")){

            }

        }

      }
    }
    try (PreparedStatement preparedStatement =
        sharedConnection.prepareStatement("select * from numericTypeTable")) {
      try (ResultSet rs = preparedStatement.executeQuery()) {
        assertTrue(rs.next());
        floatDoubleCheckResult(rs);
      }
    }
  }

    private void floatDoubleCheckResult(ResultSet rs) throws SQLException {

        // getDouble
        // supported JDBC type :
        // TINYINT, SMALLINT, INTEGER, BIGINT, REAL, FLOAT, DOUBLE, DECIMAL, NUMERIC, BIT, BOOLEAN,
        // CHAR, VARCHAR, LONGVARCHAR
        for (int i = 1; i < 8; i++) {
            rs.getDouble(i);
            rs.getFloat(i);
        }

        for (int i = 11; i < 13; i++) {
            try {
                rs.getDouble(i);
            } catch (SQLException sqle) {
                System.out.println(sqle.getMessage());
                assertTrue(sqle.getMessage(), sqle.getMessage().contains("not available"));
            }
            try {
                rs.getFloat(i);
                fail();
            } catch (SQLException sqle) {
                System.out.println(sqle.getMessage());
                assertTrue(sqle.getMessage(), sqle.getMessage().contains("not available"));
            }
        }
    }

    /**
     * CONJ-496 : Driver not dealing with non zero decimal values.
     *
     * @throws SQLException if any abnormal error occur
     */
    @Test
    public void numericTestWithDecimal() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1 as test FROM DUAL");
        assertTrue(rs.next());
        assertTrue(rs.getInt("test") == 1);
        assertTrue(rs.getByte("test") == 1);
        assertTrue(rs.getShort("test") == 1);

        rs = stmt.executeQuery("SELECT 1.3333 as test FROM DUAL");
        assertTrue(rs.next());
        assertTrue(rs.getInt("test") == 1);
        assertTrue(rs.getByte("test") == 1);
        assertTrue(rs.getShort("test") == 1);
        assertTrue(rs.getLong("test") == 1);
        assertTrue(rs.getFloat("test") == 1.3333F);

        rs = stmt.executeQuery("SELECT 1.0 as test FROM DUAL ");
        assertTrue(rs.next());
        assertTrue(rs.getInt("test") == 1);
        assertTrue(rs.getByte("test") == 1);
        assertTrue(rs.getShort("test") == 1);
        assertTrue(rs.getLong("test") == 1);
        assertTrue(rs.getFloat("test") == 1.0F);

        rs = stmt.executeQuery("SELECT -1 as test FROM DUAL ");
        assertTrue(rs.next());
        assertTrue(rs.getInt("test") == -1);
        assertTrue(rs.getByte("test") == -1);
        assertTrue(rs.getShort("test") == -1);
        assertTrue(rs.getLong("test") == -1);

        rs = stmt.executeQuery("SELECT -1.0 as test FROM DUAL ");
        assertTrue(rs.next());
        assertTrue(rs.getInt("test") == -1);
        assertTrue(rs.getByte("test") == -1);
        assertTrue(rs.getShort("test") == -1);
        assertTrue(rs.getLong("test") == -1);
        assertTrue(rs.getFloat("test") == -1.0F);

        rs = stmt.executeQuery("SELECT -1.3333 as test FROM DUAL ");
        assertTrue(rs.next());
        assertTrue(rs.getInt("test") == -1);
        assertTrue(rs.getByte("test") == -1);
        assertTrue(rs.getShort("test") == -1);
        assertTrue(rs.getLong("test") == -1);
        assertTrue(rs.getFloat("test") == -1.3333F);

        createTable("USER2_TEST", "id number(10),username varchar2(200) ");
        stmt.execute("insert into USER2_TEST values(1,'tom')");
        PreparedStatement preparedStatement = sharedConnection
            .prepareStatement("SELECT ID as test FROM USER2_TEST WHERE ID = 1 ");
        rs = preparedStatement.executeQuery();
        assertTrue(rs.next());
        assertTrue(rs.getInt("test") == 1);
        assertTrue(rs.getByte("test") == 1);
        assertTrue(rs.getShort("test") == 1);
        assertTrue(rs.getLong("test") == 1);
        assertTrue(rs.getDouble("test") == 1.0);
        assertTrue(rs.getFloat("test") == 1.0);
        assertTrue(rs.getBoolean("test") == true);
        assertTrue(rs.getString("test").equals("1"));
    }

    @Test
    public void nullField() throws SQLException {
        createTable("nullField", "t1 varchar(50), t2 timestamp NULL, t3 date, t4 varchar2(4)");
        Statement stmt = sharedConnection.createStatement();
        stmt.execute("INSERT INTO nullField(t1,t2,t3,t4) values " + "(null, null, null, null), "
                     + "(null, null, null, null)," + "('aa', sysdate , sysdate , '2017')");
        try {
            // day month year can not be zero
            stmt.execute("INSERT INTO nullField(t1,t2,t3,t4) values "
                         + "(null, null, null, null), " + "(null, null, null, null),"
                         + "('aa', sysdate , sysdate , '2017')");
        } catch (Exception e) {
            Assert.assertTrue(
                "not exception",
                e.getMessage().contains(
                    " ORA-01841: (full) year must be between -4713 and +9999, and not be 0"));
        }

        ResultSet rs = stmt.executeQuery("SELECT * FROM nullField");
        assertTrue(rs.next());
        if (!sharedOptions().useServerPrepStmts) {
            assertNull(rs.getString(1));
            assertTrue(rs.wasNull());

            assertNull(rs.getTimestamp(2));
            assertTrue(rs.wasNull());

            assertEquals(null, rs.getString(2));
            assertTrue(rs.wasNull());

            assertNull(rs.getDate(3));
            assertTrue(rs.wasNull());

            assertEquals(null, rs.getString(3));
            assertTrue(rs.wasNull());

            assertEquals(null, rs.getDate(4));
            assertTrue(rs.wasNull());

            assertEquals(0, rs.getInt(4));
            assertTrue(rs.wasNull());

            assertEquals(null, rs.getString(4));
            assertTrue(rs.wasNull());
        }

        assertTrue(rs.next());

        assertNull(rs.getTimestamp(2));
        assertTrue(rs.wasNull());

        assertNull(rs.getString(2));
        assertTrue(rs.wasNull());

        assertNull(rs.getDate(3));
        assertTrue(rs.wasNull());

        assertNull(rs.getString(3));
        assertTrue(rs.wasNull());

        assertNull(rs.getDate(4));
        assertTrue(rs.wasNull());

        assertNull(rs.getString(2));
        assertTrue(rs.wasNull());

        assertTrue(rs.next());

        assertNotNull(rs.getTimestamp(2));
        assertFalse(rs.wasNull());

        assertNotNull(rs.getString(2));
        assertFalse(rs.wasNull());

        assertNotNull(rs.getDate(3));
        assertFalse(rs.wasNull());

        assertNotNull(rs.getString(3));
        assertFalse(rs.wasNull());

        try {
            assertNotNull(rs.getString(4));
            assertNotNull(rs.getDate(4));
            assertFalse(rs.wasNull());
        } catch (Exception e) {
            System.out.println("error: " + e.getMessage().trim());
            Assert.assertTrue("", "Could not get object as Date : Unparseable date: \"2017\""
                .equalsIgnoreCase(e.getMessage().trim()));
        }
        assertNotNull(rs.getString(2));
        assertFalse(rs.wasNull());
    }

    @Test
    public void doubleStringResults() throws SQLException {
        createTable("doubleStringResults", "i number(12,3) , j number(12,8)");
        Statement stmt = sharedConnection.createStatement();
        stmt.execute("INSERT INTO doubleStringResults VALUES (1.1, 1.2), (23, 24)");
        ResultSet rs = stmt.executeQuery("SELECT * FROM doubleStringResults");

        assertTrue(rs.next());
        assertEquals("1.1", rs.getString(1));
        assertEquals(1, rs.getInt(1));
        assertEquals(1.1, rs.getDouble(1), 0.0001);
        assertEquals("1.2", rs.getString(2));
        assertEquals(1, rs.getInt(2));
        assertEquals(1.2, rs.getFloat(2), 0.0001);

        assertTrue(rs.next());
        assertEquals("23", rs.getString(1));
        assertEquals(23, rs.getInt(1));
        assertEquals(23, rs.getDouble(1), 0.0001);
        assertEquals("24", rs.getString(2));
        assertEquals(24, rs.getInt(2));
        assertEquals(24, rs.getFloat(2), 0.0001);
    }

    @Test
  public void columnNamesMappingError() throws SQLException {
    createTable(
        " columnNamesMappingError ", " xX INT NOT NULL , " + "  PRIMARY KEY(xX)");

    Statement stmt = sharedConnection.createStatement();
    stmt.executeUpdate("INSERT INTO columnNamesMappingError VALUES (4)");
    try (PreparedStatement preparedStatement =
        sharedConnection.prepareStatement(
            "SELECT * FROM " + "  columnNamesMappingError")) {
//        ,
//        ResultSet.TYPE_FORWARD_ONLY,
//                ResultSet.CONCUR_UPDATABLE)

      ResultSet rs = preparedStatement.executeQuery();
      assertTrue(rs.next());
      assertEquals(4, rs.getInt("xx"));
      try {
        rs.getInt("wrong_column_name");
        fail("must have fail, column 'wrong_column_name' does not exists");
      } catch (SQLException e) {
        assertEquals("42S22", e.getSQLState());
        assertEquals(1054, e.getErrorCode());
        assertTrue(
            e.getMessage()
                .contains(
                    "No such column: 'wrong_column_name'. 'wrong_column_name' must be in "
                        + "[xx, columnnamesmappingerror.xx]"));
      }
    }
  }

    /**
     * CONJ-669: No such column when having empty column without alias.
     *
     * @throws SQLException exception
     */
    @Test
  public void emptyColumnName() throws SQLException {
    try (Connection connection = setConnection("")) {
      Statement stmt = connection.createStatement();
        try {
            stmt.execute("drop table emptyColumn") ;
        } catch (SQLException e) {
//           eat exception
        }
        stmt.execute("CREATE global TEMPORARY TABLE emptyColumn (id int)");
      stmt.execute("INSERT INTO emptyColumn values (1)");
      ResultSet rs = stmt.executeQuery("SELECT '' FROM emptyColumn");
      while (rs.next()) {
        Assert.assertEquals("", rs.getString(1));
        Assert.assertEquals("", rs.getString(""));
      }
      ResultSetMetaData meta = rs.getMetaData();
      Assert.assertEquals(1, meta.getColumnCount());
      Assert.assertEquals("''", meta.getColumnName(1));
      connection.close();
    }
  }

    @Test
    public void getMethodTest() throws SQLException {
        createTable("getMethodTest",
            "ID INT NOT NULL PRIMARY KEY ,NAME varchar2(100) ,CREATE_TIMESTAMP TIMESTAMP,"
                    + " CRATE_TIME TIMESTAMP,CREATE_DATE DATE,weight number(12,6) "); // ,mark clob,image blob

        PreparedStatement stmt = sharedConnection
            .prepareStatement("INSERT INTO getMethodTest VALUES (?,?,?,?,?,?)");
        long time = System.currentTimeMillis();
        Timestamp timestamp = new Timestamp(time);
        Time time1 = new Time(time);
        Date date = new Date(time);

        stmt.setInt(1, 1);
        stmt.setString(2, "TOM");
        stmt.setTimestamp(3, timestamp);
        stmt.setTimestamp(4, timestamp);
        stmt.setDate(5, date);
        stmt.setBigDecimal(6, new BigDecimal(134254.3342));
        //        stmt.setClob(7,new Clob(new byte[]{1,2,3,4,5,60},0,3));
        //        stmt.setBlob(8,new Blob(new byte[]{1,2,3,4,5,60},0,10));
        stmt.execute();
        ResultSet resultSet = stmt.executeQuery("select * from getMethodTest");
        while (resultSet.next()) {
            Assert.assertTrue("not equals", 1 == resultSet.getInt(1));
            Assert.assertTrue("not equals", "TOM".equals(resultSet.getString(2)));
            Assert.assertTrue(
                "not equals result: " + resultSet.getTimestamp(3) + "timestamp:" + timestamp,
                timestamp.toString().substring(0, 16)
                    .equals(resultSet.getTimestamp(3).toString().substring(0, 16)));
            Assert.assertTrue(
                "not equals",
                timestamp.toString().substring(0, 16)
                    .equals(resultSet.getTimestamp(4).toString().substring(0, 16)));
            Assert.assertTrue(
                "not equals,date：" + date + "resultSet.getDate(5) ：" + resultSet.getDate(5), date
                    .toString().equals(resultSet.getDate(5).toString()));
            //         Assert.assertTrue("is Array",resultSet.getArray(6) instanceof Array) ;
            //          Assert.assertTrue("not equals", resultSet.getArray(4) != null  );
            Assert
                .assertTrue("not equals", 134254.3342 == resultSet.getBigDecimal(6).doubleValue());
            //            Assert.assertTrue("not equals",resultSet.getClob(7).getSubString(0,3).equals(new Clob(new byte[]{1,2,3,4,5,60},0,3).getSubString(0,3)));
            //            Assert.assertTrue("not equals",new String(resultSet.getBlob(8).getBytes(0,5)).equals(new String (new Blob(new byte[]{1,2,3,4,5,60},0,10).getBytes(0,5))));
        }
    }

    @Test
    public void testCallable() {
        Connection conn = null;
        System.out.println("123");
        try {
            conn = setConnection();

            String cSql = "drop table test_callable";
            String dropProc = "drop PROCEDURE test_callable_proc";
            try {
                conn.createStatement().execute(cSql);
            } catch (SQLException e) {

            }
            cSql = "create table test_callable(c1 int, c2 varchar2(200), c3 varchar2(200))";

            PreparedStatement pstmt = conn.prepareStatement(cSql);
            pstmt.execute();

            pstmt = conn.prepareStatement("insert into test_callable values(?, ?, ?)");
            pstmt.setInt(1, 112);
            pstmt.setString(2, "CLOB_abcdefg");
            pstmt.setString(3, "CLOB_abcdefg");
            pstmt.executeUpdate();

            conn.createStatement()
                .execute(
                    "CREATE OR REPLACE PROCEDURE test_callable_porc(p_c1 IN int, pC2 OUT VARCHAR2, p_c3 OUT varchar2) is BEGIN  \n"
                            + "            select c2,c3 into pC2,p_c3 from test_callable ;\n"
                            + "                    end ;");

            CallableStatement cstmt = conn.prepareCall("call test_callable_porc(?,?,?)");
            cstmt.setInt(1, 112);
            cstmt.registerOutParameter(2, Types.VARCHAR);
            cstmt.registerOutParameter(3, Types.VARCHAR);

            cstmt.execute();

            String pC2 = (String) cstmt.getObject(2);
            Assert.assertEquals("CLOB_abcdefg", pC2);
            System.out.println("updateCount: " + cstmt.getUpdateCount());
            if (cstmt != null) {
                System.out.println("false23424");
                System.out.println("getMoreResults: " + cstmt.getMoreResults());
            }
            ResultSet resultSet = cstmt.getResultSet();
            System.out.println("resultSet: " + resultSet);
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    Assert.assertTrue(e.getMessage(),
                        e.getMessage().contains("Can't call rollback when autocommit enable"));
                }
            }
        }
    }
}
