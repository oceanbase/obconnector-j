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
import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Arrays;

import org.junit.*;

public class ResultSetTest extends BaseTest {

    /**
     * Initialisation.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("result_set_test", "id int not null primary key auto_increment, name char(20)");
    }

    @Test
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


    createTable("gen_key_test_resultset", "name VARCHAR(40) NOT NULL, xml varchar(2000)");
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

    @Test
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

    @Test
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
        isLastTwoRowsTest(false);
        isLastTwoRowsTest(true);
    }

    private void isLastTwoRowsTest(boolean fetching) throws SQLException {
    insertRows(2);
    try (Statement statement = sharedConnection.createStatement()) {
      if (fetching) {
        statement.setFetchSize(1);
      }
      ResultSet resultSet = statement.executeQuery("SELECT * FROM result_set_test");
      Assume.assumeTrue(!sharedOptions().useCursorFetch && statement.getFetchSize() != Integer.MIN_VALUE
            || sharedOptions().useCursorFetch && statement.getFetchSize() == 0);
      // Invalid operation on TYPE_FORWARD_ONLY CURSOR or STREAMING ResultSet
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
        assertFalse(resultSet.isAfterLast());
        assertFalse(resultSet.next()); // No more rows after this
        assertTrue(resultSet.isAfterLast());
        assertTrue(resultSet.last());
        assertEquals(2, resultSet.getInt(1));
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

    @Test
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
  public void generatedKeyNpe() throws SQLException {
    createTable("generatedKeyNpe", "id int not null primary key auto_increment, val int");
    Statement statement = sharedConnection.createStatement();
    statement.execute(
        "INSERT INTO generatedKeyNpe(val) values (0)", Statement.RETURN_GENERATED_KEYS);
    try (ResultSet rs = statement.getGeneratedKeys()) {
      assertTrue(rs.next());
    }
  }

    @Test
    public void generatedKeyError() throws SQLException {
        createTable("generatedKeyNpe", "id int not null primary key auto_increment, val int");
        Statement statement = sharedConnection.createStatement();
        statement.execute("INSERT INTO generatedKeyNpe(val) values (0)");
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
        assertEquals("row42", rs.getString(2));

        assertTrue(rs.absolute(-11));
        assertEquals("row40", rs.getString(2));

//        if (Version.version.compareToIgnoreCase("2.2.8") >= 0) {
            assertFalse(rs.absolute(0));
//        } else {
//            assertTrue(rs.absolute(0));
//        }
        assertTrue(rs.isBeforeFirst());

        assertFalse(rs.absolute(51));
        assertTrue(rs.isAfterLast());

        assertTrue(rs.absolute(-1));
        assertEquals("row50", rs.getString(2));

        assertTrue(rs.absolute(-50));
        assertEquals("row1", rs.getString(2));
      }
    }
  }

    @Test
  public void testResultSetIsAfterLast() throws Exception {
    insertRows(2);
    try (Statement statement = sharedConnection.createStatement()) {
      statement.setFetchSize(1);
      try (ResultSet rs = statement.executeQuery("SELECT * FROM result_set_test")) {
        assertFalse(rs.isLast());
        assertFalse(rs.isAfterLast());
        assertTrue(rs.next());
        assertFalse(rs.isLast());
        assertFalse(rs.isAfterLast());
        assertTrue(rs.next());
        assertTrue(rs.isLast());
        assertFalse(rs.isAfterLast());
        assertFalse(rs.next());
        assertFalse(rs.isLast());
        assertTrue(rs.isAfterLast());
      }

      insertRows(0);
      try (ResultSet rs = statement.executeQuery("SELECT * FROM result_set_test")) {
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
      Assume.assumeTrue(!sharedOptions().useCursorFetch && statement.getFetchSize() != Integer.MIN_VALUE
            || sharedOptions().useCursorFetch && statement.getFetchSize() == 0);
      // Invalid operation on TYPE_FORWARD_ONLY CURSOR or STREAMING ResultSet
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
    public void testStreamForward() throws Exception {
        createTable("testStreamForward", "s1 varchar(20)");

        for (int r = 0; r < 20; r++) {
            sharedConnection.createStatement().executeUpdate(
                "insert into testStreamForward values('V" + r + "')");
        }
        Statement stmt = sharedConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(Integer.MIN_VALUE);

        // reading forward
        ResultSet rs = stmt.executeQuery("select * from testStreamForward");
        for (int i = 0; i < 20; i++) {
            assertTrue(rs.next());
            assertEquals("V" + i, rs.getString(1));
        }
        assertFalse(rs.next());

        // checking isAfterLast that may need to fetch next result
        rs = stmt.executeQuery("select * from testStreamForward");
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
        rs = stmt.executeQuery("select * from testStreamForward");
        try {
            rs.afterLast();
            fail("Must have thrown exception since afterLast is not possible when fetching");
        } catch (SQLException sqle) {
            assertTrue(sqle.getMessage().contains("Invalid operation on STREAMING ResultSet"));
        }
        try {
            rs.previous();
            fail("Must have thrown exception since previous is not possible when fetching");
        } catch (SQLException sqle) {
            assertTrue(sqle.getMessage().contains("Invalid operation on STREAMING ResultSet"));
        }

        try {
            rs = stmt.executeQuery("select * from testStreamForward");
            fail();
        } catch (SQLException sqle) {
            assertTrue(sqle
                .getMessage()
                .contains(
                    "No statements may be issued when any streaming result sets are open and in use on a given connection"));
        }
        rs.close();

        rs = stmt.executeQuery("select * from testStreamForward");
        try {
            rs.last();
            fail("Must have thrown exception since last is not possible when fetching");
        } catch (SQLException sqle) {
            assertTrue(sqle.getMessage().contains("Invalid operation on STREAMING ResultSet"));
        }
        try {
            rs.first();
            fail("Must have thrown exception since previous is not possible when fetching");
        } catch (SQLException sqle) {
            assertTrue(sqle.getMessage().contains("Invalid operation on STREAMING ResultSet"));
        }
        rs.close();
    }

    /**
     * [CONJ-437] getString on field with ZEROFILL doesn't have the '0' leading chars when using
     * binary protocol.
     *
     * @throws SQLException if any abnormal error occur
     */
    @Test
    public void leadingZeroTest() throws SQLException {
        createTable("leadingZero", "t1 TINYINT(3) unsigned zerofill"
                                   + ", t2 TINYINT(8) unsigned zerofill"
                                   + ", t3 TINYINT unsigned zerofill"
                                   + ", t4 smallint(3) unsigned zerofill"
                                   + ", t5 smallint(8) unsigned zerofill"
                                   + ", t6 smallint unsigned zerofill"
                                   + ", t7 MEDIUMINT(3) unsigned zerofill"
                                   + ", t8 MEDIUMINT(8) unsigned zerofill"
                                   + ", t9 MEDIUMINT unsigned zerofill"
                                   + ", t10 INT(3) unsigned zerofill"
                                   + ", t11 INT(8) unsigned zerofill"
                                   + ", t12 INT unsigned zerofill"
                                   + ", t13 BIGINT(3) unsigned zerofill"
                                   + ", t14 BIGINT(8) unsigned zerofill"
                                   + ", t15 BIGINT unsigned zerofill"
                                   + ", t16 DECIMAL(6,3) unsigned zerofill"
                                   + ", t17 DECIMAL(11,3) unsigned zerofill"
                                   + ", t18 DECIMAL unsigned zerofill"
                                   + ", t19 FLOAT(6,3) unsigned zerofill"
                                   + ", t20 FLOAT(11,3) unsigned zerofill"
                                   + ", t21 FLOAT unsigned zerofill"
                                   + ", t22 DOUBLE(6,3) unsigned zerofill"
                                   + ", t23 DOUBLE(11,3) unsigned zerofill"
                                   + ", t24 DOUBLE unsigned zerofill");
        Statement stmt = sharedConnection.createStatement();
        stmt.executeUpdate("insert into leadingZero values (1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1.1,1.1,1.1,1.1,1.1,1.1,1.1,1.1,1.1), "
                           + "(20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20.2,20.2,20.2,20.2,20.2,20.2,20.2,20.2,20.2)");

        // test text resultSet
        testLeadingZeroResult(stmt.executeQuery("select * from leadingZero"));

        // test binary resultSet
        if (!sharedUsePrepare()) {
            PreparedStatement pst1 = sharedConnection.prepareStatement("select * from leadingZero");
            ResultSet rs1 = pst1.executeQuery();
            testLeadingZeroResult(rs1);
        }
    }

    private void testLeadingZeroResult(ResultSet rs1) throws SQLException {
        assertTrue(rs1.next());
        assertEquals("001", rs1.getString(1));
        assertEquals("00000001", rs1.getString(2));
        assertEquals("001", rs1.getString(3));
        assertEquals("001", rs1.getString(4));
        assertEquals("00000001", rs1.getString(5));
        assertEquals("00001", rs1.getString(6));
        assertEquals("001", rs1.getString(7));
        assertEquals("00000001", rs1.getString(8));
        assertEquals("00000001", rs1.getString(9));
        assertEquals("001", rs1.getString(10));
        assertEquals("00000001", rs1.getString(11));
        assertEquals("0000000001", rs1.getString(12));
        assertEquals("001", rs1.getString(13));
        assertEquals("00000001", rs1.getString(14));
        assertEquals("00000000000000000001", rs1.getString(15));
        assertEquals("001.100", rs1.getString(16));
        assertEquals("00000001.100", rs1.getString(17));
        assertEquals("0000000001", rs1.getString(18));
        assertEquals("01.100", rs1.getString(19));
        assertEquals("0000001.100", rs1.getString(20));
        assertEquals("0000000001.1", rs1.getString(21));
        assertEquals("01.100", rs1.getString(22));
        assertEquals("0000001.100", rs1.getString(23));
        assertEquals("000000000000000000001.1", rs1.getString(24));

        assertTrue(rs1.next());
        assertEquals("020", rs1.getString(1));
        assertEquals("00000020", rs1.getString(2));
        assertEquals("020", rs1.getString(3));
        assertEquals("020", rs1.getString(4));
        assertEquals("00000020", rs1.getString(5));
        assertEquals("00020", rs1.getString(6));
        assertEquals("020", rs1.getString(7));
        assertEquals("00000020", rs1.getString(8));
        assertEquals("00000020", rs1.getString(9));
        assertEquals("020", rs1.getString(10));
        assertEquals("00000020", rs1.getString(11));
        assertEquals("0000000020", rs1.getString(12));
        assertEquals("020", rs1.getString(13));
        assertEquals("00000020", rs1.getString(14));
        assertEquals("00000000000000000020", rs1.getString(15));
        assertEquals("020.200", rs1.getString(16));
        assertEquals("00000020.200", rs1.getString(17));
        assertEquals("0000000020", rs1.getString(18));
        assertEquals("20.200", rs1.getString(19));
        assertEquals("0000020.200", rs1.getString(20));
        assertEquals("0000000020.2", rs1.getString(21));
        assertEquals("20.200", rs1.getString(22));
        assertEquals("0000020.200", rs1.getString(23));
        assertEquals("000000000000000000020.2", rs1.getString(24));
        assertFalse(rs1.next());
    }

    @Test
    public void firstForwardTest() throws SQLException {
        // first must always work when complete
        Statement stmt = sharedConnection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1");
        assertTrue(rs.first());
        assertFalse(rs.previous());
        assertTrue(rs.absolute(1));
        assertFalse(rs.relative(-1));
        stmt.close();

        // absolute operation must fail when cursor or streaming
        Connection conn = setConnection("&useCursorFetch=true");
        stmt = conn.createStatement();
        stmt.setFetchSize(1);
        rs = stmt.executeQuery("SELECT 1");
        try {
            rs.first();
            fail("absolute operation must fail when TYPE_FORWARD_ONLY and streaming");
        } catch (SQLException sqle) {
            assertTrue(sqle.getMessage().contains(
                "Invalid operation on TYPE_FORWARD_ONLY CURSOR ResultSet"));
        }
        try {
            rs.previous();
            fail("absolute operation must fail when TYPE_FORWARD_ONLY and streaming");
        } catch (SQLException sqle) {
            assertTrue(sqle.getMessage().contains(
                "Invalid operation on TYPE_FORWARD_ONLY CURSOR ResultSet"));
        }
        try {
            rs.absolute(1);
            fail("absolute operation must fail when TYPE_FORWARD_ONLY and streaming");
        } catch (SQLException sqle) {
            assertTrue(sqle.getMessage().contains(
                "Invalid operation on TYPE_FORWARD_ONLY CURSOR ResultSet"));
        }
        try {
            rs.relative(-1);
            fail("Invalid operation on TYPE_FORWARD_ONLY CURSOR ResultSet");
        } catch (SQLException sqle) {
            assertTrue(sqle.getMessage().contains(
                "Invalid operation on TYPE_FORWARD_ONLY CURSOR ResultSet"));
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
        "t1 tinyint, "
            + "t2 boolean, "
            + "t3 smallint,  "
            + "t4 mediumint, "
            + "t5 int, "
            + "t6 bigint, "
            + "t7 decimal, "
            + "t8 float, "
            + "t9 double, "
            + "t10 bit,"
            + "t11 char(10),"
            + "t12 varchar(10),"
            + "t13 binary(10),"
            + "t14 varbinary(10),"
            + "t15 text,"
            + "t16 blob,"
            + "t17 date");

    try (Statement stmt = sharedConnection.createStatement()) {
      stmt.execute(
          "INSERT into numericTypeTable values (1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 'a', 'a', 'a', 'a', 'a', 'a', now())");
      try (ResultSet rs = stmt.executeQuery("select * from numericTypeTable")) {
        assertTrue(rs.next());
        floatDoubleCheckResult(rs);
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
        for (int i = 1; i < 11; i++) {
            rs.getDouble(i);
            rs.getFloat(i);
        }

        for (int i = 11; i < 16; i++) {
            try {
                rs.getDouble(i);
                fail();
            } catch (SQLException sqle) {
                assertTrue(sqle.getMessage().contains("Incorrect format "));
            }
            try {
                rs.getFloat(i);
                fail();
            } catch (SQLException sqle) {
                assertTrue(sqle.getMessage().contains("Incorrect format "));
            }
        }
        for (int i = 16; i < 18; i++) {
            try {
                rs.getDouble(i);
            } catch (SQLException sqle) {
                assertTrue(sqle.getMessage().contains("not available"));
            }
            try {
                rs.getFloat(i);
                fail();
            } catch (SQLException sqle) {
                assertTrue(sqle.getMessage().contains("not available"));
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
        ResultSet rs = stmt.executeQuery("SELECT 1 as test");
        assertTrue(rs.next());
        assertTrue(rs.getInt("test") == 1);
        assertTrue(rs.getByte("test") == 1);
        assertTrue(rs.getShort("test") == 1);

        rs = stmt.executeQuery("SELECT 1.3333 as test");
        assertTrue(rs.next());
        assertTrue(rs.getInt("test") == 1);
        assertTrue(rs.getByte("test") == 1);
        assertTrue(rs.getShort("test") == 1);
        assertTrue(rs.getLong("test") == 1);
        assertTrue(rs.getFloat("test") == 1.3333F);

        rs = stmt.executeQuery("SELECT 1.0 as test");
        assertTrue(rs.next());
        assertTrue(rs.getInt("test") == 1);
        assertTrue(rs.getByte("test") == 1);
        assertTrue(rs.getShort("test") == 1);
        assertTrue(rs.getLong("test") == 1);
        assertTrue(rs.getFloat("test") == 1.0F);

        rs = stmt.executeQuery("SELECT -1 as test");
        assertTrue(rs.next());
        assertTrue(rs.getInt("test") == -1);
        assertTrue(rs.getByte("test") == -1);
        assertTrue(rs.getShort("test") == -1);
        assertTrue(rs.getLong("test") == -1);

        rs = stmt.executeQuery("SELECT -1.0 as test");
        assertTrue(rs.next());
        assertTrue(rs.getInt("test") == -1);
        assertTrue(rs.getByte("test") == -1);
        assertTrue(rs.getShort("test") == -1);
        assertTrue(rs.getLong("test") == -1);
        assertTrue(rs.getFloat("test") == -1.0F);

        rs = stmt.executeQuery("SELECT -1.3333 as test");
        assertTrue(rs.next());
        assertTrue(rs.getInt("test") == -1);
        assertTrue(rs.getByte("test") == -1);
        assertTrue(rs.getShort("test") == -1);
        assertTrue(rs.getLong("test") == -1);
        assertTrue(rs.getFloat("test") == -1.3333F);
    }

    @Test
    public void nullField() throws SQLException {
        createTable("nullField", "t1 varchar(50), t2 timestamp NULL, t3 date, t4 year(4)");
        Connection conn = setConnection("&zeroDateTimeBehavior=convertToNull");
        Statement stmt = conn.createStatement();
        stmt.execute("INSERT INTO nullField(t1,t2,t3,t4) values "
                     + "(null, '0000-00-00 00:00:00', '0000-00-00', '0000'), "
                     + "(null, null, null, null)," + "('aa', now(), now(), '2017')");

        ResultSet rs = stmt.executeQuery("SELECT * FROM nullField");
        assertTrue(rs.next());
        if (!sharedOptions().useServerPrepStmts) {
            assertNull(rs.getString(1));
            assertTrue(rs.wasNull());

            assertNull(rs.getTimestamp(2));
            assertTrue(rs.wasNull());

            assertEquals(null, rs.getString(2));
            assertFalse(rs.wasNull());

            assertNull(rs.getDate(3));
            assertTrue(rs.wasNull());

            assertEquals("0000-00-00", rs.getString(3));
            assertFalse(rs.wasNull());

            assertEquals(Date.valueOf("0000-01-01"), rs.getDate(4));
            assertFalse(rs.wasNull());

            assertEquals(0, rs.getInt(4));
            assertFalse(rs.wasNull());

            assertEquals("0001-01-01", rs.getString(4));
            assertFalse(rs.wasNull());
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

        assertNotNull(rs.getDate(4));
        assertFalse(rs.wasNull());

        assertNotNull(rs.getString(2));
        assertFalse(rs.wasNull());
    }

    @Test
    public void doubleStringResults() throws SQLException {
        createTable("doubleStringResults", "i double, j float");
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

    @Ignore
  public void invisibleColumn() throws SQLException {
    // since 10.3.3
    Assume.assumeTrue(isMariadbServer() && minVersion(10, 3));
    cancelForVersion(10, 3, 0);
    cancelForVersion(10, 3, 1);
    cancelForVersion(10, 3, 2);

    createTable("invisible", "x INT, y INT INVISIBLE, z INT INVISIBLE NOT NULL DEFAULT 4");
    Statement stmt = sharedConnection.createStatement();
    stmt.execute("INSERT INTO invisible(x,y) VALUES (1,2)");

    try (PreparedStatement preparedStatement =
        sharedConnection.prepareStatement("SELECT * FROM invisible WHERE x = ?")) {
      ResultSetMetaData resultSetMetaData = preparedStatement.getMetaData();
      Assert.assertEquals(1, resultSetMetaData.getColumnCount());
      Assert.assertEquals("x", resultSetMetaData.getColumnName(1));
    }

    try (PreparedStatement preparedStatement =
        sharedConnection.prepareStatement("SELECT x,z FROM invisible WHERE x = ?")) {
      ResultSetMetaData resultSetMetaData = preparedStatement.getMetaData();
      Assert.assertEquals(2, resultSetMetaData.getColumnCount());
      Assert.assertEquals("x", resultSetMetaData.getColumnName(1));
      Assert.assertEquals("z", resultSetMetaData.getColumnName(2));
    }
  }

    @Ignore
    public void checkInvisibleMetaData() throws SQLException {
        cancelForVersion(10, 3, 0);
        cancelForVersion(10, 3, 1);
        cancelForVersion(10, 3, 2);

        createTable(
            "checkInvisibleMetaData",
            "xx tinyint(1), x2 tinyint(1) unsigned INVISIBLE primary key auto_increment, yy year(4), zz bit, uu smallint");
        DatabaseMetaData meta = sharedConnection.getMetaData();
        ResultSet rs = meta.getColumns(null, null, "checkInvisibleMetaData", null);
        assertTrue(rs.next());
        assertEquals("BIT", rs.getString(6));
        assertTrue(rs.next());
        assertEquals("BIT", rs.getString(6));
        assertTrue(rs.next());
        assertEquals("YEAR", rs.getString(6));
        assertEquals(null, rs.getString(7)); // column size
        assertEquals(null, rs.getString(9)); // decimal digit
    }

    @Test
  public void columnNamesMappingError() throws SQLException {
    createTable(
        "columnNamesMappingError", "xX INT NOT NULL AUTO_INCREMENT, " + "  PRIMARY KEY(xX)");

    Statement stmt = sharedConnection.createStatement();
    stmt.executeUpdate("INSERT INTO columnNamesMappingError VALUES (4)");
    try (PreparedStatement preparedStatement =
        sharedConnection.prepareStatement(
            "SELECT * FROM " + "columnNamesMappingError",
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_UPDATABLE)) {
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
                        + "[xX, columnnamesmappingerror.xX]"));
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
            stmt.execute("drop table emptyColumnName");
        } catch (SQLException e) {
            // eat exception
        }
        stmt.execute("CREATE TEMPORARY TABLE emptyColumn (id int)");
      stmt.execute("INSERT INTO emptyColumn value (1)");
      ResultSet rs = stmt.executeQuery("SELECT '' FROM emptyColumn");
      while (rs.next()) {
        Assert.assertEquals("", rs.getString(1));
        Assert.assertEquals("", rs.getString(""));
      }
      ResultSetMetaData meta = rs.getMetaData();
      Assert.assertEquals(1, meta.getColumnCount());
      Assert.assertEquals("", meta.getColumnName(1));
    }
  }

    @Test
    public void getMethodTest() throws SQLException {
        createTable("getMethodTest",
            "ID INT NOT NULL PRIMARY KEY ,NAME varchar(100) ,CREATE_TIMESTAMP TIMESTAMP,"
                    + " CRATE_TIME TIMESTAMP,CREATE_DATE DATE,weight number(12,6) ");
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
        stmt.execute();
        //        stmt.setArray(6,sharedConnection.createArrayOf("varchar2", new Object[]{"1","cc"}));
        ResultSet resultSet = stmt.executeQuery("select * from getMethodTest");
        while (resultSet.next()) {
            System.out.println("into while loop");
            Assert.assertTrue("not equals", 1 == resultSet.getInt(1));
            Assert.assertTrue("not equals", "TOM".equals(resultSet.getString(2)));
            Assert
                .assertTrue("not equals", 134254.3342 == resultSet.getBigDecimal(6).doubleValue());
        }
    }

    @Test
    public void testCallable() {
        Connection conn = null;
        try {
            conn = sharedConnection;

            String cSql = "drop table test_callable";
            String dropProc = "drop PROCEDURE test_callable_open ";
            PreparedStatement pstmt = conn.prepareStatement(cSql);
            PreparedStatement pstmt2 = conn.prepareStatement(dropProc);
            try {
                pstmt.execute();
            } catch (SQLException e) {
            }
            try {
                pstmt2.execute();
            } catch (SQLException e) {
                //skip
            }

            cSql = "create table test_callable(c1 int, c2 varchar(200), c3 Timestamp)";

            pstmt = conn.prepareStatement(cSql);
            pstmt.execute();

            pstmt = conn.prepareStatement("insert into test_callable values(?, ?, ?)");
            pstmt.setInt(1, 112);
            pstmt.setString(2, "CLOB_abcdefg");
            pstmt.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
            pstmt.executeUpdate();

            conn.prepareStatement(
                " CREATE PROCEDURE test_callable_open(in p_c1 int ,out p_c2 varchar(200) ,out p_c3 timestamp )  BEGIN  \n"
                        + "      select c2 ,c3 into p_c2,p_c3 from test_callable ;\n"
                        + "      end ").execute();

            CallableStatement cstmt = conn.prepareCall("{call test_callable_open(?,?,?)}");
            cstmt.setInt(1, 112);
            cstmt.registerOutParameter(2, Types.VARCHAR);
            cstmt.registerOutParameter(3, Types.TIMESTAMP);

            cstmt.execute();
            System.out.println("2: " + cstmt.getString(2) + "3: " + cstmt.getTimestamp(3));
            System.out.println("updateCount: " + cstmt.getUpdateCount()); // 1
            System.out.println("getMoreResults: " + cstmt.getMoreResults()); // false
            ResultSet resultSet = cstmt.getResultSet(); //null
            System.out.println("resultSet: " + resultSet);

        } catch (Exception e) {
            e.printStackTrace();
            fail();
        } finally {
        }
    }

    @Test
    public void testFindColumn() throws SQLException {
        Statement stmt = sharedConnection.createStatement();

        try {
            stmt.execute("drop table test_findColumn");
        } catch (SQLException e) {
        }

        stmt.execute("CREATE TABLE test_findColumn (\n"
                     + "  rankCode varchar(10) NOT NULL,\n"
                     + "  rankName varchar(70) DEFAULT NULL,\n"
                     + "  parentRankCode varchar(10) DEFAULT NULL,\n"
                     + "  rankLevel varchar(2) DEFAULT NULL,\n"
                     + "  rankType varchar(20) DEFAULT NULL,\n"
                     + "  rankLayer varchar(2) DEFAULT NULL,\n"
                     + "  PRIMARY KEY (rankCode)\n"
                     + ") DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'zstd_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 0 COMMENT = '职业代码表';");

        stmt.execute("INSERT INTO `test_findColumn` (`rankCode`,`rankName`,`parentRankCode`,`rankLevel`,`rankType`,`rankLayer`) VALUES ('4020805','光电火控工程技术人员','40208','','3','4');\n");
        stmt.execute("INSERT INTO `test_findColumn` (`rankCode`,`rankName`,`parentRankCode`,`rankLevel`,`rankType`,`rankLayer`) VALUES ('40211','电子工程技术人员','402','','','3');\n");
        stmt.execute("INSERT INTO `test_findColumn` (`rankCode`,`rankName`,`parentRankCode`,`rankLevel`,`rankType`,`rankLayer`) VALUES ('4021101','电子材料工程技术人员','40211','','2','4');\n");
        stmt.execute("INSERT INTO `test_findColumn` (`rankCode`,`rankName`,`parentRankCode`,`rankLevel`,`rankType`,`rankLayer`) VALUES ('4021102','电子元器件工程技术人员','40211','','2','4');\n");

        JDBC4ResultSet rs = (JDBC4ResultSet) stmt
            .executeQuery("        select\n"
                          + "        d.rankCode as rankCodeF,\n"
                          + "        d.rankName as rankNameF,\n"
                          + "        c.rankCode as rankCodeS,\n"
                          + "        c.rankName as rankNameS,\n"
                          + "        b.rankCode as rankCodeT,\n"
                          + "        b.rankName as rankNameT,\n"
                          + "        a.rankCode as rankCode,\n"
                          + "        a.rankName as rankName,\n"
                          + "        a.rankType as rankType\n"
                          + "        FROM\n"
                          + "        test_findColumn a join test_findColumn b on a.parentRankCode  = b.rankCode and b.rankLayer = '3'\n"
                          + "        join test_findColumn c on b.parentRankCode  = c.rankCode and c.rankLayer  = '2'\n"
                          + "        join test_findColumn d on c.parentRankCode = d.rankCode and d.rankLayer = '1'\n"
                          + "        where\n" + "        a.rankLayer = '4'\n"
                          + "        AND a.rankName LIKE '%'");

        int rankcodeIndex = rs.findColumn("rankcode");

        Assert.assertTrue("rankcode"
            .equalsIgnoreCase(rs.getMetaData().getColumnName(rankcodeIndex)));

        rs = (JDBC4ResultSet) stmt.executeQuery("select * from test_findColumn");

        rankcodeIndex = rs.findColumn("rankcode");

        Assert.assertTrue("rankcode"
            .equalsIgnoreCase(rs.getMetaData().getColumnName(rankcodeIndex)));

    }

    @Test
    public void testColumnIndex() throws Exception {
        Statement stmt = sharedConnection.createStatement();
        createTable("table1", "c1 int");

        stmt.execute("insert into table1 values (1)");
        ResultSet rs = stmt.executeQuery("select table1.c1, c1 from table1");
        assertTrue(rs.next());
        assertEquals(1, rs.findColumn("c1"));
        assertEquals(1, rs.findColumn("table1.c1"));

        rs = stmt.executeQuery("select c1 from table1");
        assertEquals(1, rs.findColumn("c1"));
        assertEquals(1, rs.findColumn("table1.c1"));
    }

    @Test
    public void testDuplicateColumnAlias() throws Exception {
        Statement stmt = sharedConnection.createStatement();
        createTable("table1", "c1 int, c2 int");

        stmt.execute("insert into table1 values (1, 0)");
        ResultSet rs = stmt.executeQuery("select c1, c2 ,c2 ,c2 from table1");
        assertTrue(rs.next());
        Assert.assertEquals(2, rs.findColumn("c2"));
    }

    @Test
    public void testColumnAlias() throws Exception {
        Statement stmt = sharedConnection.createStatement();
        createTable("table1", "c1 int, c2 int");

        stmt.execute("insert into table1 values (1, 0)");
        ResultSet rs = stmt.executeQuery("select a.c1, a.c2 from table1 as a");
        assertTrue(rs.next());
        rs.findColumn("a.c1");
    }

    @Test
    public void testOnDuplicateAnnotate() throws Exception {
        createTable("test1", "id INT AUTO_INCREMENT PRIMARY KEY, c2 CHAR(1) UNIQUE KEY, c3 INT");
        Statement stmt = sharedConnection.createStatement();

        /* ***************** &useAffectedRows=false ***************** */
        Connection conn1 = setConnection("&useAffectedRows=false");
        Statement stmt1 = conn1.createStatement();
        stmt1.execute("Insert into test1 (c2, c3) values ('A', 1), ('B', 2)");
        stmt1
            .execute(
                "INSERT INTO test1 (c2, c3) VALUES ('B', 2), ('C', 3)  ON  DUPLICATE KEY /* test */ UPDATE c3 = VALUES(c3)",
                Statement.RETURN_GENERATED_KEYS);

        ResultSet rs1 = stmt1.getGeneratedKeys();
        assertTrue(rs1.next());
        assertEquals(3, rs1.getInt(1));
        assertFalse(rs1.next());

        ResultSet rs = stmt.executeQuery("select count(*) from test1");
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        stmt.execute("truncate table test1");

        /* ***************** &useAffectedRows=true ***************** */
        Connection conn2 = setConnection("&useAffectedRows=true");
        Statement stmt2 = conn2.createStatement();
        stmt2.execute("Insert into test1 (c2, c3) values ('A', 1), ('B', 2)");
        stmt2
            .execute(
                "INSERT INTO test1 (c2, c3) VALUES ('B', 2), ('C', 3)  ON  DUPLICATE KEY /* test */ UPDATE c3 = VALUES(c3)",
                Statement.RETURN_GENERATED_KEYS);

        ResultSet rs2 = stmt2.getGeneratedKeys();
        assertTrue(rs2.next());
        assertEquals(3, rs2.getInt(1));
        assertFalse(rs2.next());

        rs = stmt.executeQuery("select count(*) from test1");
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
    }

    @Test
    public void testStmtCloseResultSet() {
        try {
            Statement stmt = sharedConnection.createStatement();
            ResultSet rs = stmt.executeQuery("select 1");
            stmt.close();
            try {
                rs.next();
                fail("Should've had an exception here");
            } catch (SQLException e) {
                Assert.assertTrue(e.getMessage().contains(
                    "Operation not permit on a closed resultSet"));
            }
            PreparedStatement ps = null;
            ps = sharedConnection.prepareStatement("select 1");
            rs = ps.executeQuery();
            ps.close();
            try {
                rs.next();
                fail("Should've had an exception here");
            } catch (SQLException e) {
                Assert.assertTrue(e.getMessage().contains(
                    "Operation not permit on a closed resultSet"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testNextAfterClose() throws SQLException {
        ResultSet rs;

        // close result set
        Statement stmt = sharedConnection.createStatement();
        rs = stmt.executeQuery("SELECT 1");
        rs.close();
        try {
            rs.next();
            fail();
        } catch (SQLException e) {
            assertEquals("Operation not permit on a closed resultSet", e.getMessage());
        }

        PreparedStatement ps = sharedConnection.prepareStatement("select 1");
        rs = ps.executeQuery();
        rs.close();
        try {
            rs.next();
            fail();
        } catch (SQLException e) {
            assertEquals("Operation not permit on a closed resultSet", e.getMessage());
        }

        // close statement
        rs = stmt.executeQuery("SELECT 1");
        stmt.close();
        try {
            rs.next();
            fail();
        } catch (SQLException e) {
            assertEquals("Operation not permit on a closed resultSet", e.getMessage());
        }

        rs = ps.executeQuery();
        ps.close();
        try {
            rs.next();
            fail();
        } catch (SQLException e) {
            assertEquals("Operation not permit on a closed resultSet", e.getMessage());
        }
    }

    @Test
    public void testGetAfterClose() throws Exception {
        Statement stmt = sharedConnection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1");
        try {
            rs.getInt(1);
        } catch (SQLException e) {
            Assert.assertEquals("Current position is before the first row", e.getMessage());
        }

        rs = stmt.executeQuery("SELECT 1");
        rs.next();
        rs.close();
        try {
            rs.getInt(1);
        } catch (SQLException | NullPointerException e) {
            Assert.assertEquals("Operation not permit on a closed resultSet", e.getMessage());
        }

        rs = stmt.executeQuery("SELECT 1");
        rs.close();
        try {
            rs.getInt(1);
        } catch (SQLException e) {
            Assert.assertEquals("Operation not permit on a closed resultSet", e.getMessage());
        }
    }

    @Test
    public void fix46494300AfterClose() throws Exception {
        Statement stmt = sharedConnection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1");
        stmt.close();
        //        rs.close();

        try {
            rs.getMetaData();
        } catch (Exception e) {
            Assert.assertEquals("Operation not permit on a closed resultSet", e.getMessage());
        }

        try {
            rs.getWarnings();
        } catch (Exception e) {
            Assert.assertEquals("Operation not permit on a closed resultSet", e.getMessage());
        }
        try {
            rs.getHoldability();
        } catch (Exception e) {
            Assert.assertEquals("Method ResultSet.getHoldability() not supported", e.getMessage());
        }

        try {
            rs.getRow();
        } catch (Exception e) {
            Assert.assertEquals("Operation not permit on a closed resultSet", e.getMessage());
        }
        try {
            rs.next();
        } catch (Exception e) {
            Assert.assertEquals("Operation not permit on a closed resultSet", e.getMessage());
        }
        try {
            rs.previous();
        } catch (Exception e) {
            Assert.assertEquals("Operation not permit on a closed resultSet", e.getMessage());
        }
        try {
            rs.first();
        } catch (Exception e) {
            Assert.assertEquals("Operation not permit on a closed resultSet", e.getMessage());
        }
        try {
            rs.last();
        } catch (Exception e) {
            Assert.assertEquals("Operation not permit on a closed resultSet", e.getMessage());
        }
        try {
            rs.beforeFirst();
        } catch (Exception e) {
            Assert.assertEquals("Operation not permit on a closed resultSet", e.getMessage());
        }
        try {
            rs.afterLast();
        } catch (Exception e) {
            Assert.assertEquals("Operation not permit on a closed resultSet", e.getMessage());
        }
        try {
            rs.isFirst();
        } catch (Exception e) {
            Assert.assertEquals("Operation not permit on a closed resultSet", e.getMessage());
        }
        try {
            rs.isLast();
        } catch (Exception e) {
            Assert.assertEquals("Operation not permit on a closed resultSet", e.getMessage());
        }
        try {
            rs.isBeforeFirst();
        } catch (Exception e) {
            Assert.assertEquals("Operation not permit on a closed resultSet", e.getMessage());
        }
        try {
            rs.isAfterLast();
        } catch (Exception e) {
            Assert.assertEquals("Operation not permit on a closed resultSet", e.getMessage());
        }
        try {
            rs.relative(0);
        } catch (Exception e) {
            Assert.assertEquals("Operation not permit on a closed resultSet", e.getMessage());
        }
        try {
            rs.absolute(0);
        } catch (Exception e) {
            Assert.assertEquals("Operation not permit on a closed resultSet", e.getMessage());
        }
    }

    @Test
    public void testMaxRows() throws SQLException {
        String tableName = "DeleteReturn_ingInto";
        createTable(tableName, "c1 INT, c2 VARCHAR(100)");
        String query = "insert into DeleteReturn_ingInto values(1,'1+string')";
        for (int i = 2; i <= 100; i++) {
            query += ",(" + i + ",'" + i + "+string')";
        }
        Statement st = sharedConnection.createStatement();
        st.execute(query);
        st.close();

        Statement stmt = sharedConnection.createStatement();
        stmt.setMaxRows(5);
        stmt.setFetchSize(3);
        ResultSet rs1 = stmt.executeQuery("select * from DeleteReturn_ingInto limit 10;");
        while (rs1.next()) {
            String c2 = rs1.getString(2);
            System.out.println(c2);
        }

        Connection conn = setConnection("&useServerPrepStmts=true&useCursorFetch=true");
        PreparedStatement pstmt = conn
            .prepareStatement("select * from DeleteReturn_ingInto limit 10;");
        pstmt.setMaxRows(5);
        pstmt.setFetchSize(3);
        ResultSet rs2 = pstmt.executeQuery();
        while (rs2.next()) {
            String c2 = rs2.getString(1);
            System.out.println(c2);
        }
    }

    @Test
    public void testFindByName() throws SQLException {
        Connection conn = sharedConnection;
        Statement stmt = null;
        ResultSet rs = null;
        ResultSetMetaData meta = null;

        createTable("quote_test",
            "`id` int primary key , `create_TIME` timestamp, `Name` varchar(20)");
        stmt = conn.createStatement();
        stmt.executeUpdate("insert into quote_test values(1, now(), 'yxy')");

        rs = stmt.executeQuery("select * from quote_test");
        assertTrue(rs.next());
        assertEquals("`", conn.getMetaData().getIdentifierQuoteString());
        Assert.assertEquals("yxy", rs.getString("Name"));
        Assert.assertEquals("yxy", rs.getString("name"));
    }

    @Test
    public void testGetObject() throws SQLException {
        Connection conn = sharedConnection;
        Statement stmt = null;
        createTable("t1", "GROUP_SEND_TYPE INT");
        stmt = conn.createStatement();
        stmt.execute("insert into t1 value (1)");
        ResultSet rs = stmt.executeQuery("select * from t1");
        assertTrue(rs.next());
        assertEquals(1, (int)rs.getObject(1, Integer.class));
        assertEquals(1, (int)rs.getObject(1, int.class));
        assertEquals(1, (int)rs.getObject(1, Integer.TYPE));

        assertEquals(true,rs.getObject(1, Boolean.class));
        assertEquals(true,rs.getObject(1, boolean.class));
        assertEquals(true,rs.getObject(1, Boolean.TYPE));


        assertEquals(1, rs.getObject(1, Long.class).longValue());
        assertEquals(1, rs.getObject(1, long.class).longValue());
        assertEquals(1, rs.getObject(1, Long.TYPE).longValue());

        assertEquals(1, rs.getObject(1, Float.class).floatValue(), 0.0001);
        assertEquals(1, rs.getObject(1, float.class).floatValue(), 0.0001);
        assertEquals(1, rs.getObject(1, Float.TYPE).floatValue(), 0.0001);

        assertEquals(1, rs.getObject(1, Double.class).doubleValue(), 0.0001);
        assertEquals(1, rs.getObject(1, double.class).doubleValue(), 0.0001);
        assertEquals(1, rs.getObject(1, Double.TYPE).doubleValue(), 0.0001);
    }

    @Test
    public void testGetIntForEmptyString() throws SQLException {
        Connection conn = sharedConnection;
        Statement stmt = null;
        createTable("c", "a int(11) DEFAULT NULL,b char(20) DEFAULT NULL");
        stmt = conn.createStatement();
        stmt.execute("INSERT INTO c(a, b) VALUES(1, '')");
        ResultSet resultSet = stmt.executeQuery("select a,b from c");
        resultSet.next();
        assertEquals(0, resultSet.getInt(2));

        conn = setConnection("&emptyStringsConvertToZero=false");
        stmt = conn.createStatement();
        resultSet = stmt.executeQuery("select a,b from c");
        resultSet.next();
        try {
            resultSet.getInt(2);
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(e.getMessage().contains(
                    "Can't convert empty string ('') to numeric"));
        }
    }

    @Test
    public void aone53147863() throws SQLException {
        createTable("t1", "c0 Tinyint(1), c1 double, c2 int");
        Statement stmt = sharedConnection.createStatement();
        stmt.execute("insert into t1 values (5.2, 5.2, 5.2)");

        ResultSet rs = sharedConnection.prepareStatement("select * from t1").executeQuery();
        assertTrue(rs.next());
        assertEquals(5, rs.getByte(1));
        assertEquals(5, rs.getByte(2));
        assertEquals(5, rs.getByte(3));
        assertEquals("[53]", Arrays.toString(rs.getBytes(1)));
        assertEquals("[53, 46, 50]", Arrays.toString(rs.getBytes(2)));
        assertEquals("[53]", Arrays.toString(rs.getBytes(3)));
    }

    @Test
    public void testGetWarnings() throws Exception {
        Connection conn = sharedConnection;
        Statement stmt = conn.createStatement();
        try {
            stmt.execute("select * from dual");
            fail();
        } catch (SQLException e) {
            assertNull( conn.getWarnings());
            SQLWarning warnings = stmt.getWarnings();
            assertEquals("No tables used", warnings.getMessage());
        }

        conn = setConnectionOrigin("?useServerPrepStmts=true&emulateUnsupportedPstmts=true");
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement("select * from dual");
            ps.execute();
            fail();
        } catch (SQLException e) {
            assertNull(conn.getWarnings());
            SQLWarning warnings = stmt.getWarnings();
            assertEquals("No tables used", warnings.getMessage());
        }
    }

    @Test
    public void testAoneID53220911() throws SQLException {
        Connection conn = setConnection("&useServerPrepStmts=true&emptyStringsConvertToZero=true");
        createTable("t_f", "c1 int, c2 varchar(20)");
        conn.createStatement().execute("insert into t_f values(1, '')");
        ResultSet rs = conn.prepareStatement("select c1 , c2 from t_f").executeQuery();
        while (rs.next()) {
            assertEquals(0, rs.getByte(2));
            assertEquals(0, rs.getShort(2));
            assertEquals(0, rs.getInt(2));
            assertEquals(0, rs.getLong(2));
            assertEquals("", rs.getString(2));
            assertEquals("", rs.getObject(2));
            assertEquals(false, rs.getBoolean(2));
            assertArrayEquals("".getBytes(), rs.getBytes(2));
            assertEquals(0, rs.getFloat(2), 0.1);
            assertEquals(0, rs.getDouble(2), 0.1);
            assertEquals(0, rs.getBigDecimal(2).intValue());
        }
    }

}
