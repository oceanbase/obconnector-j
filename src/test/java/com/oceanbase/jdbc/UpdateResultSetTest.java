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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;

import org.junit.Assume;
import org.junit.Test;

public class UpdateResultSetTest extends BaseTest {

    /**
     * Test error message when no primary key.
     *
     * @throws Exception not expected
     */
    @Test
  public void testNoPrimaryKey() throws Exception {
    createTable("testnoprimarykey", "`id` INT NOT NULL," + "`t1` VARCHAR(50) NOT NULL");
    Statement stmt = sharedConnection.createStatement();
    stmt.execute("INSERT INTO testnoprimarykey VALUES (1, 't1'), (2, 't2')");

    try (PreparedStatement preparedStatement =
        sharedConnection.prepareStatement(
            "SELECT * FROM testnoprimarykey",
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_UPDATABLE)) {
      ResultSet rs = preparedStatement.executeQuery("SELECT * FROM testnoprimarykey");
      assertTrue(rs.next());
      try {
        rs.updateString(1, "1");
        fail();
      } catch (SQLException sqle) {
        assertEquals(
            "ResultSet cannot be updated. Table "
                + "`"
                + sharedConnection.getCatalog()
                + "`.`testnoprimarykey` has no primary key",
            sqle.getMessage());
      }
    }
  }

    @Test
  public void testNoDatabase() throws Exception {
    try (PreparedStatement preparedStatement =
        sharedConnection.prepareStatement(
            "SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
      ResultSet rs = preparedStatement.executeQuery();
      assertTrue(rs.next());
      try {
        rs.updateString(1, "1");
        fail();
      } catch (SQLException sqle) {
        assertTrue(
            sqle.getMessage(),
            sqle.getMessage()
                .contains(
                    "The result-set contains fields without without any database information"));
      }
    }
  }

    @Test
  public void testMultipleTable() throws Exception {

    createTable(
        "testMultipleTable1",
        "`id1` INT NOT NULL AUTO_INCREMENT," + "`t1` VARCHAR(50) NULL," + "PRIMARY KEY (`id1`)");

    createTable(
        "testMultipleTable2",
        "`id2` INT NOT NULL AUTO_INCREMENT," + "`t2` VARCHAR(50) NULL," + "PRIMARY KEY (`id2`)");

    Statement stmt = sharedConnection.createStatement();
    stmt.executeQuery("INSERT INTO testMultipleTable1(t1) values ('1')");
    stmt.executeQuery("INSERT INTO testMultipleTable2(t2) values ('2')");

    try (PreparedStatement preparedStatement =
        sharedConnection.prepareStatement(
            "SELECT * FROM testMultipleTable1, testMultipleTable2",
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_UPDATABLE)) {
      ResultSet rs = preparedStatement.executeQuery();
      assertTrue(rs.next());
      try {
        rs.updateString("t1", "new value");
        fail("must have failed since there is different tables");
      } catch (SQLException sqle) {
        assertTrue(
            sqle.getMessage(),
            sqle.getMessage()
                .contains(
                    "ResultSet cannot be updated. "
                        + "The result-set contains fields on different tables"));
      }
    }
  }

    @Test
  public void testOneNoTable() throws Exception {
    createTable(
        "testOneNoTable",
        "`id1` INT NOT NULL AUTO_INCREMENT," + "`t1` VARCHAR(50) NULL," + "PRIMARY KEY (`id1`)");

    Statement stmt = sharedConnection.createStatement();
    stmt.executeQuery("INSERT INTO testOneNoTable(t1) values ('1')");

    try (PreparedStatement preparedStatement =
        sharedConnection.prepareStatement(
            "SELECT *, now() FROM testOneNoTable",
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_UPDATABLE)) {
      ResultSet rs = preparedStatement.executeQuery();
      assertTrue(rs.next());
      try {
        rs.updateString("t1", "new value");
        fail("must have failed since there is a field without database");
      } catch (SQLException sqle) {
        assertTrue(
            sqle.getMessage(),
            sqle.getMessage()
                .contains(
                    "ResultSet cannot be updated. "
                        + "The result-set contains fields without without any database information"));
      }
    }
  }

    @Test
  public void testMultipleDatabase() throws Exception {

    Statement stmt = sharedConnection.createStatement();
    try {
      stmt.execute("DROP DATABASE testConnectorJ");
    } catch (SQLException sqle) {
      // eat
    }

    stmt.execute("CREATE DATABASE testConnectorJ");
    createTable(
        sharedConnection.getCatalog() + ".testMultipleDatabase",
        "`id1` INT NOT NULL AUTO_INCREMENT," + "`t1` VARCHAR(50) NULL," + "PRIMARY KEY (`id1`)");

    createTable(
        "testConnectorJ.testMultipleDatabase",
        "`id2` INT NOT NULL AUTO_INCREMENT," + "`t2` VARCHAR(50) NULL," + "PRIMARY KEY (`id2`)");

    stmt.executeQuery(
        "INSERT INTO " + sharedConnection.getCatalog() + ".testMultipleDatabase(t1) values ('1')");
    stmt.executeQuery("INSERT INTO testConnectorJ.testMultipleDatabase(t2) values ('2')");

    try (PreparedStatement preparedStatement =
        sharedConnection.prepareStatement(
            "SELECT * FROM "
                + sharedConnection.getCatalog()
                + ".testMultipleDatabase, testConnectorJ.testMultipleDatabase",
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_UPDATABLE)) {
      ResultSet rs = preparedStatement.executeQuery();
      assertTrue(rs.next());
      try {
        rs.updateString("t1", "new value");
        fail("must have failed since there is different database");
      } catch (SQLException sqle) {
        assertTrue(
            sqle.getMessage(),
            sqle.getMessage().contains("The result-set contains more than one database"));
      }
    }
  }

    @Test
  public void testMeta() throws Exception {
    createTable(
        "UpdateWithoutPrimary",
        "`id` INT NOT NULL AUTO_INCREMENT,"
            + "`t1` VARCHAR(50) NOT NULL,"
            + "`t2` VARCHAR(50) NULL default 'default-value',"
            + "PRIMARY KEY (`id`)");

    Statement stmt = sharedConnection.createStatement();
    stmt.executeQuery("INSERT INTO UpdateWithoutPrimary(t1,t2) values ('1-1','1-2')");

    try (PreparedStatement preparedStatement =
        sharedConnection.prepareStatement(
            "SELECT t1, t2 FROM UpdateWithoutPrimary",
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_UPDATABLE)) {
      ResultSet rs = preparedStatement.executeQuery();
      assertTrue(rs.next());
      try {
        rs.updateString(1, "1-1-bis");
        rs.updateRow();
        fail();
      } catch (SQLException sqle) {
        assertTrue(
            sqle.getMessage(),
            sqle.getMessage()
                .contains(
                    "ResultSet cannot be updated. Primary key "
                        + "field `id` is not in result-set"));
      }
      try {
        rs.deleteRow();
        fail();
      } catch (SQLException sqle) {
        assertTrue(
            sqle.getMessage(),
            sqle.getMessage()
                .contains(
                    "ResultSet cannot be updated. "
                        + "Primary key field `id` is not in result-set"));
      }
      ResultSetMetaData rsmd = rs.getMetaData();
      assertFalse(rsmd.isReadOnly(1));
      assertFalse(rsmd.isReadOnly(2));
      assertTrue(rsmd.isWritable(1));
      assertTrue(rsmd.isWritable(2));
      assertTrue(rsmd.isDefinitelyWritable(1));
      assertTrue(rsmd.isDefinitelyWritable(2));

      try {
        rsmd.isReadOnly(3);
        fail("must have throw exception");
      } catch (SQLException sqle) {
        System.out.println(sqle.getMessage());
        assertTrue(sqle.getMessage().contains("wrong column index 3. must be in [1, 2] range"));
      }
      try {
        rsmd.isWritable(3);
        fail("must have throw exception");
      } catch (SQLException sqle) {
        assertTrue(sqle.getMessage().contains("wrong column index 3. must be in [1, 2] range"));
      }
      try {
        rsmd.isDefinitelyWritable(3);
        fail("must have throw exception");
      } catch (SQLException sqle) {
        assertTrue(sqle.getMessage().contains("wrong column index 3. must be in [1, 2] range"));
      }
    }
    int[] autoInc = setAutoInc();
    ResultSet rs = stmt.executeQuery("SELECT id, t1, t2 FROM UpdateWithoutPrimary");
    assertTrue(rs.next());
    assertEquals(autoInc[1] + autoInc[0], rs.getInt(1));
    assertEquals("1-1", rs.getString(2));
    assertEquals("1-2", rs.getString(3));

    assertFalse(rs.next());
  }

    @Test
  public void testUpdateWhenFetch() throws Exception {
    createTable(
        "testUpdateWhenFetch",
        "`id` INT NOT NULL AUTO_INCREMENT,"
            + "`t1` VARCHAR(50) NOT NULL,"
            + "`t2` VARCHAR(50) NULL default 'default-value',"
            + "PRIMARY KEY (`id`)",
        "DEFAULT CHARSET=utf8");

    final Statement stmt = sharedConnection.createStatement();
    PreparedStatement pstmt =
        sharedConnection.prepareStatement("INSERT INTO testUpdateWhenFetch(t1,t2) values (?, ?)");
    for (int i = 1; i < 100; i++) {
      pstmt.setString(1, i + "-1");
      pstmt.setString(2, i + "-2");
      pstmt.addBatch();
    }
    pstmt.executeBatch();

    String utf8escapeQuote = "你好 '' \" \\";
    try (PreparedStatement preparedStatement =
        sharedConnection.prepareStatement(
            "SELECT id, t1, t2 FROM testUpdateWhenFetch",
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_UPDATABLE)) {
      preparedStatement.setFetchSize(2);
      ResultSet rs = preparedStatement.executeQuery();

      rs.moveToInsertRow();
      rs.updateInt(1, -1);
      rs.updateString(2, "0-1");
      rs.updateString(3, "0-2");
      rs.insertRow();

      rs.next();
      rs.next();
      rs.updateString(2, utf8escapeQuote);
      rs.updateRow();
    }

    ResultSet rs = stmt.executeQuery("SELECT id, t1, t2 FROM testUpdateWhenFetch");
    assertTrue(rs.next());
    assertEquals(-1, rs.getInt(1));
    assertEquals("0-1", rs.getString(2));
    assertEquals("0-2", rs.getString(3));

    int[] autoInc = setAutoInc();

    assertTrue(rs.next());
    assertEquals(autoInc[0] + autoInc[1], rs.getInt(1));
    assertEquals("1-1", rs.getString(2));
    assertEquals("1-2", rs.getString(3));

    assertTrue(rs.next());
    assertEquals(2 * autoInc[0] + autoInc[1], rs.getInt(1));
    assertEquals(utf8escapeQuote, rs.getString(2));
    assertEquals("2-2", rs.getString(3));

    for (int i = 3; i < 100; i++) {
      assertTrue(rs.next());
      assertEquals(i + "-1", rs.getString(2));
      assertEquals(i + "-2", rs.getString(3));
    }
    assertFalse(rs.next());
  }

    @Test
  public void testPrimaryGenerated() throws Exception {
    createTable(
        "PrimaryGenerated",
        "`id` INT NOT NULL AUTO_INCREMENT,"
            + "`t1` VARCHAR(50) NOT NULL,"
            + "`t2` VARCHAR(50) NULL default 'default-value',"
            + "PRIMARY KEY (`id`)");

    Statement stmt = sharedConnection.createStatement();
    int[] autoInc = setAutoInc();

    try (PreparedStatement preparedStatement =
        sharedConnection.prepareStatement(
            "SELECT t1, t2, id FROM PrimaryGenerated",
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_UPDATABLE)) {
      ResultSet rs = preparedStatement.executeQuery();
      assertFalse(rs.next());

      rs.moveToInsertRow();
      rs.updateString(1, "1-1");
      rs.updateString(2, "1-2");
      rs.insertRow();

      rs.moveToInsertRow();
      rs.updateString(1, "2-1");
      rs.insertRow();

      rs.moveToInsertRow();
      rs.updateString(2, "3-2");
      try {
        rs.insertRow();
        fail("must not occur since t1 cannot be null");
      } catch (SQLException sqle) {
        assertTrue(
            sqle.getMessage(),
            sqle.getMessage().contains("Field doesn't have a default value"));
      }

      rs.absolute(1);
      assertEquals("1-1", rs.getString(1));
      assertEquals("1-2", rs.getString(2));
      assertEquals(autoInc[0] + autoInc[1], rs.getInt(3));

      assertTrue(rs.next());
      assertEquals("2-1", rs.getString(1));
      assertEquals("default-value", rs.getString(2));
      assertEquals(2 * autoInc[0] + autoInc[1], rs.getInt(3));

      assertFalse(rs.next());
    }

    ResultSet rs = stmt.executeQuery("SELECT id, t1, t2 FROM PrimaryGenerated");
    assertTrue(rs.next());
    assertEquals(autoInc[0] + autoInc[1], rs.getInt(1));
    assertEquals("1-1", rs.getString(2));
    assertEquals("1-2", rs.getString(3));

    assertTrue(rs.next());
    assertEquals(2 * autoInc[0] + autoInc[1], rs.getInt(1));
    assertEquals("2-1", rs.getString(2));
    assertEquals("default-value", rs.getString(3));

    assertFalse(rs.next());
  }

    @Test
  public void testPrimaryGeneratedDefault() throws Exception {
    createTable(
        "testPrimaryGeneratedDefault",
        "`id` INT NOT NULL AUTO_INCREMENT,"
            + "`t1` VARCHAR(50) NOT NULL default 'default-value1',"
            + "`t2` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,"
            + "PRIMARY KEY (`id`)");
    int[] autoInc = setAutoInc();
    try (PreparedStatement preparedStatement =
        sharedConnection.prepareStatement(
            "SELECT id, t1, t2 FROM testPrimaryGeneratedDefault",
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_UPDATABLE)) {
      ResultSet rs = preparedStatement.executeQuery();
      assertFalse(rs.next());
      rs.moveToInsertRow();
      rs.insertRow();

      rs.moveToInsertRow();
      rs.insertRow();
      rs.beforeFirst();

      assertTrue(rs.next());
      assertEquals(autoInc[1] + autoInc[0], rs.getInt(1));
      assertEquals("default-value1", rs.getString(2));
      assertNotNull(rs.getDate(3));

      assertTrue(rs.next());
      assertEquals(2 * autoInc[0] + autoInc[1], rs.getInt(1));
      assertEquals("default-value1", rs.getString(2));
      assertNotNull(rs.getDate(3));
      assertFalse(rs.next());
    }

    Statement stmt = sharedConnection.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT id, t1, t2 FROM testPrimaryGeneratedDefault");
    assertTrue(rs.next());
    assertEquals(autoInc[0] + autoInc[1], rs.getInt(1));
    assertEquals("default-value1", rs.getString(2));
    assertNotNull(rs.getDate(3));

    assertTrue(rs.next());
    assertEquals(2 * autoInc[0] + autoInc[1], rs.getInt(1));
    assertEquals("default-value1", rs.getString(2));
    assertNotNull(rs.getDate(3));

    assertFalse(rs.next());
  }

    @Test
  public void testDelete() throws Exception {
    createTable(
        "testDelete",
        "`id` INT NOT NULL,"
            + "`id2` INT NOT NULL,"
            + "`t1` VARCHAR(50),"
            + "PRIMARY KEY (`id`,`id2`)");

    Statement stmt =
        sharedConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
    stmt.execute("INSERT INTO testDelete values (1,-1,'1'), (2,-2,'2'), (3,-3,'3')");

    try (PreparedStatement preparedStatement =
        sharedConnection.prepareStatement(
            "SELECT * FROM testDelete", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
      ResultSet rs = preparedStatement.executeQuery();
      try {
        rs.deleteRow();
        fail();
      } catch (SQLException sqle) {
        assertTrue(
            sqle.getMessage(),
            sqle.getMessage().contains("Current position is before the first row"));
      }

      assertTrue(rs.next());
      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1));
      rs.deleteRow();
      assertEquals(1, rs.getInt(1));
      assertEquals(-1, rs.getInt(2));
      assertTrue(rs.next());
      assertEquals(3, rs.getInt(1));
      assertEquals(-3, rs.getInt(2));
    }

    ResultSet rs = stmt.executeQuery("SELECT * FROM testDelete");
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertEquals(-1, rs.getInt(2));
    assertTrue(rs.next());
    assertEquals(3, rs.getInt(1));
    assertEquals(-3, rs.getInt(2));
    assertFalse(rs.next());

    rs.absolute(1);
    rs.deleteRow();
    try {
      rs.getInt(1);
      fail();
    } catch (SQLException sqle) {
      assertTrue(
          sqle.getMessage(),
          sqle.getMessage().contains("Current position is before the first row"));
    }
  }

    @Test
  public void testUpdateChangingMultiplePrimaryKey() throws Exception {
    createTable(
        "testUpdateChangingMultiplePrimaryKey",
        "`id` INT NOT NULL,"
            + "`id2` INT NOT NULL,"
            + "`t1` VARCHAR(50),"
            + "PRIMARY KEY (`id`,`id2`)");

    Statement stmt = sharedConnection.createStatement();
    stmt.execute(
        "INSERT INTO testUpdateChangingMultiplePrimaryKey values (1,-1,'1'), (2,-2,'2'), (3,-3,'3')");
    try (PreparedStatement preparedStatement =
        sharedConnection.prepareStatement(
            "SELECT * FROM testUpdateChangingMultiplePrimaryKey",
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_UPDATABLE)) {
      ResultSet rs = preparedStatement.executeQuery();

      assertTrue(rs.next());
      assertTrue(rs.next());
      rs.updateInt(1, 4);
      rs.updateInt(2, -4);
      rs.updateString(3, "4");
      rs.updateRow();

      assertEquals(4, rs.getInt(1));
      assertEquals(-4, rs.getInt(2));
      assertEquals("4", rs.getString(3));
    }

    ResultSet rs = stmt.executeQuery("SELECT * FROM testUpdateChangingMultiplePrimaryKey");

    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertEquals(-1, rs.getInt(2));
    assertEquals("1", rs.getString(3));

    assertTrue(rs.next());
    assertEquals(3, rs.getInt(1));
    assertEquals(-3, rs.getInt(2));
    assertEquals("3", rs.getString(3));

    assertTrue(rs.next());
    assertEquals(4, rs.getInt(1));
    assertEquals(-4, rs.getInt(2));
    assertEquals("4", rs.getString(3));

    assertFalse(rs.next());
  }

    @Test
  public void updateBlob() throws SQLException, IOException {
      Assume.assumeFalse(sharedUsePrepare());
    createTable("updateBlob", "id int not null primary key, strm blob");

    PreparedStatement stmt =
        sharedConnection.prepareStatement("insert into updateBlob (id, strm) values (?,?)");
    byte[] theBlob = {1, 2, 3, 4, 5, 6};
    InputStream stream = new ByteArrayInputStream(theBlob);

    stmt.setInt(1, 1);
    stmt.setBlob(2, stream);
    stmt.execute();

    byte[] updatedBlob = {1, 3, 6, 9, 15, 21};

    try (PreparedStatement preparedStatement =
        sharedConnection.prepareStatement(
            "select * from updateBlob", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
      ResultSet rs = preparedStatement.executeQuery();
      assertTrue(rs.next());
      InputStream updatedStream = new ByteArrayInputStream(updatedBlob);

      rs.updateBlob(2, updatedStream);
      rs.updateRow();

      checkResult(rs, updatedBlob);
    }

    try (PreparedStatement preparedStatement =
        sharedConnection.prepareStatement(
            "select * from updateBlob", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
      ResultSet rs = preparedStatement.executeQuery();
      assertTrue(rs.next());
      checkResult(rs, updatedBlob);
    }
  }

    private void checkResult(ResultSet rs, byte[] updatedBlob) throws SQLException, IOException {
        InputStream readStuff = rs.getBlob("strm").getBinaryStream();
        int ch;
        int pos = 0;
        while ((ch = readStuff.read()) != -1) {
            assertEquals(updatedBlob[pos++], ch);
        }

        readStuff = rs.getBinaryStream("strm");

        pos = 0;
        while ((ch = readStuff.read()) != -1) {
            assertEquals(updatedBlob[pos++], ch);
        }
    }

    @Test
    public void updateMeta() throws SQLException {
        DatabaseMetaData meta = sharedConnection.getMetaData();

        assertTrue(meta.ownUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
        assertTrue(meta.ownDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
        assertTrue(meta.ownInsertsAreVisible(ResultSet.TYPE_FORWARD_ONLY));
        assertTrue(meta.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY));
        assertTrue(meta.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_UPDATABLE));

        assertTrue(meta.ownUpdatesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
        assertTrue(meta.ownDeletesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
        assertTrue(meta.ownInsertsAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
        assertTrue(meta.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY));
        assertTrue(meta.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_UPDATABLE));

        assertFalse(meta.ownUpdatesAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
        assertFalse(meta.ownDeletesAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
        assertFalse(meta.ownInsertsAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
        assertFalse(meta.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_SENSITIVE,
            ResultSet.CONCUR_READ_ONLY));
        assertFalse(meta.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_SENSITIVE,
            ResultSet.CONCUR_UPDATABLE));
    }

    @Test
    public void updateResultSetMeta() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        assertEquals(ResultSet.CONCUR_READ_ONLY, stmt.getResultSetConcurrency());
        ResultSet rs = stmt.executeQuery("SELECT 1");
        assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());

        stmt = sharedConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_UPDATABLE);
        assertEquals(ResultSet.CONCUR_UPDATABLE, stmt.getResultSetConcurrency());
        rs = stmt.executeQuery("SELECT 1");
        assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
    }

    @Test
    public void insertNoRow() throws SQLException {
        createTable("insertNoRow", "id int not null primary key, strm blob");
        Statement st = sharedConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = st.executeQuery("select * from insertNoRow");
        assertFalse(rs.next());
        rs.moveToInsertRow();
        try {
            rs.refreshRow();
            fail("Can't refresh when on the insert row.");
        } catch (SQLException sqle) {
            // expected
        }
        rs.moveToCurrentRow();
    }

    @Test
    public void refreshRow() throws SQLException {
        createTable("refreshRow", "id int not null primary key, strm blob");

        Statement st = sharedConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_UPDATABLE);
        st.execute("INSERT INTO refreshRow values (1, '555')");
        ResultSet rs = st.executeQuery("select * from refreshRow");

        st.execute("UPDATE refreshRow set strm = '666' WHERE id = 1");
        try {
            rs.refreshRow();
            fail("Can't refresh when not on row.");
        } catch (SQLException sqle) {
            // expected
        }

        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("555", rs.getString(2));
        rs.refreshRow();
        assertEquals("666", rs.getString(2));

        rs.moveToInsertRow();
        try {
            rs.refreshRow();
            fail("Can't refresh when on insert row");
        } catch (SQLException sqle) {
            // expected
        }
        rs.moveToCurrentRow();

        assertFalse(rs.next());
        try {
            rs.refreshRow();
            fail("Can't refresh when not on row.");
        } catch (SQLException sqle) {
            // expected
        }
    }

    @Test
  public void testMoveToInsertRow() throws SQLException {
    createTable("testMoveToInsertRow", "t2 text, t1 text, id int primary key");

    try (PreparedStatement preparedStatement =
        sharedConnection.prepareStatement(
            "select id, t1, t2 from testMoveToInsertRow",
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_UPDATABLE)) {
      ResultSet rs = preparedStatement.executeQuery();

      assertNotNull(rs);
      assertEquals(0, rs.getRow());
      rs.moveToInsertRow();
      rs.updateInt(1, 1);
      rs.updateString(2, "t1-value");
      rs.updateString(3, "t2-value");
      rs.insertRow();
      rs.first();
      assertEquals(1, rs.getRow());

      rs.updateInt("id", 2);
      rs.updateString("t1", "t1-bis-value");
      rs.updateRow();
      assertEquals(1, rs.getRow());

      assertEquals(2, rs.getInt("id"));
      assertEquals("t1-bis-value", rs.getString("t1"));
      assertEquals("t2-value", rs.getString("t2"));

      rs.deleteRow();
      assertEquals(0, rs.getRow());

      rs.moveToInsertRow();
      rs.updateInt("id", 3);
      rs.updateString("t1", "other-t1-value");

      rs.insertRow();
      assertEquals(1, rs.getRow());
      try {
        rs.refreshRow();
        fail("Can't refresh when on the insert row.");
      } catch (SQLException sqle) {
        assertEquals("Cannot call deleteRow() when inserting a new row", sqle.getMessage());
      }

      assertEquals(3, rs.getInt("id"));
      assertEquals("other-t1-value", rs.getString("t1"));
      assertNull(rs.getString("t2"));
    }

    try (PreparedStatement preparedStatement =
        sharedConnection.prepareStatement(
            "select id, t1, t2 from testMoveToInsertRow",
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_UPDATABLE)) {
      ResultSet rs = preparedStatement.executeQuery();

      assertTrue(rs.first());
      rs.updateInt("id", 3);
      rs.updateString("t1", "t1-3");
      rs.updateRow();
      assertEquals(3, rs.getInt("id"));
      assertEquals("t1-3", rs.getString("t1"));

      rs.moveToInsertRow();
      rs.updateInt("id", 4);
      rs.updateString("t1", "t1-4");
      rs.insertRow();

      rs.updateInt("id", 5);
      rs.updateString("t1", "t1-5");
      rs.insertRow();

      rs.moveToCurrentRow();
      assertEquals(3, rs.getInt("id"));
      assertEquals("t1-3", rs.getString("t1"));

      assertTrue(rs.next());
      assertEquals(4, rs.getInt("id"));
      assertEquals("t1-4", rs.getString("t1"));

      assertTrue(rs.next());
      assertEquals(5, rs.getInt("id"));
      assertEquals("t1-5", rs.getString("t1"));
    }
  }

    @Test
  public void cancelRowUpdatesTest() throws SQLException {
    createTable("cancelRowUpdatesTest", "c text, id int primary key");

    Statement st = sharedConnection.createStatement();
    st.executeUpdate(
        "INSERT INTO cancelRowUpdatesTest(id,c) values (1,'1'), (2,'2'),(3,'3'),(4,'4')");

    try (PreparedStatement preparedStatement =
        sharedConnection.prepareStatement(
            "select id,c from cancelRowUpdatesTest order by id",
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_UPDATABLE)) {
      ResultSet rs = preparedStatement.executeQuery();

      assertTrue(rs.next());
      assertTrue(rs.next());

      assertEquals("2", rs.getString("c"));
      rs.updateString("c", "2bis");
      rs.cancelRowUpdates();
      rs.updateRow();
      assertEquals("2", rs.getString("c"));

      rs.updateString("c", "2bis");
      rs.updateRow();
      assertEquals("2bis", rs.getString("c"));

      assertTrue(rs.first());
      assertTrue(rs.next());
      assertEquals("2bis", rs.getString("c"));
    }
  }

    @Test
  public void deleteRowsTest() throws SQLException {
    createTable("deleteRows", "c text, id int primary key");

    Statement st = sharedConnection.createStatement();
    st.executeUpdate("INSERT INTO deleteRows(id,c) values (1,'1'), (2,'2'),(3,'3'),(4,'4')");

    try (PreparedStatement preparedStatement =
        sharedConnection.prepareStatement(
            "select id,c from deleteRows order by id",
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_UPDATABLE)) {
      ResultSet rs = preparedStatement.executeQuery();

      assertTrue(rs.next());
      assertEquals(1, rs.getInt("id"));

      rs.deleteRow();

      assertTrue(rs.isBeforeFirst());

      assertTrue(rs.next());
      assertTrue(rs.next());
      assertEquals(3, rs.getInt("id"));

      rs.deleteRow();
      assertEquals(2, rs.getInt("id"));
    }
  }

    @Test
  public void updatePosTest() throws SQLException {
    createTable("updatePosTest", "c text, id int primary key");

    Statement st = sharedConnection.createStatement();
    st.executeUpdate("INSERT INTO updatePosTest(id,c) values (1,'1')");

    try (PreparedStatement preparedStatement =
        sharedConnection.prepareStatement(
            "select id,c from updatePosTest",
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_UPDATABLE)) {

      ResultSet rs = preparedStatement.executeQuery();

      try {
        rs.updateInt(1, 20);
        fail();
      } catch (SQLException sqle) {
        assertEquals("Current position is before the first row", sqle.getMessage());
      }

      try {
        rs.updateRow();
        fail();
      } catch (SQLException sqle) {
        assertEquals("Current position is before the first row", sqle.getMessage());
      }

      try {
        rs.deleteRow();
        fail();
      } catch (SQLException sqle) {
        assertEquals("Current position is before the first row", sqle.getMessage());
      }

      assertTrue(rs.next());
      rs.updateInt(1, 20);
      rs.updateRow();
      rs.deleteRow();
      assertFalse(rs.next());
      try {
        rs.updateInt(1, 20);
        fail();
      } catch (SQLException sqle) {
        assertEquals("Current position is after the last row", sqle.getMessage());
      }

      try {
        rs.updateRow();
        fail();
      } catch (SQLException sqle) {
        assertEquals("Current position is after the last row", sqle.getMessage());
      }

      try {
        rs.deleteRow();
        fail();
      } catch (SQLException sqle) {
        assertEquals("Current position is after the last row", sqle.getMessage());
      }
    }
  }

    /**
     * CONJ-519 : Updatable result-set possible NPE when same field is repeated.
     *
     * @throws SQLException if any exception occur
     */
    @Test
    public void repeatedFieldUpdatable() throws SQLException {
        createTable("repeatedFieldUpdatable",
            "t1 varchar(50) NOT NULL, t2 varchar(50), PRIMARY KEY (t1)");

        Statement stmt = sharedConnection.createStatement();
        stmt.execute("insert into repeatedFieldUpdatable values ('gg', 'hh'), ('jj', 'll')");

        PreparedStatement preparedStatement = sharedConnection.prepareStatement(
            "SELECT t1, t2, t1 as t3 FROM repeatedFieldUpdatable", ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = preparedStatement.executeQuery();
        while (rs.next()) {
            rs.getObject(3);
        }
    }

    @Test
    public void testResultUpdateObject() throws Exception {
        try {
            createTable("testUpdateObject", "id INT PRIMARY KEY, ot1 VARCHAR(100), ot2 BLOB");
            Connection conn = sharedConnection;
            Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = stmt.executeQuery("SELECT * FROM testUpdateObject");
            byte[] bytes = { 1, 2, 3 };

            rs.moveToInsertRow();
            rs.updateObject(1, 1, Types.INTEGER);
            rs.updateObject(2, "aaa", Types.VARCHAR);
            rs.updateObject(3, bytes, Types.BLOB);
            rs.insertRow();
            rs.moveToCurrentRow();

            rs = stmt.executeQuery("SELECT * FROM testUpdateObject");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("aaa", rs.getString(2));
            assertEquals(new String(bytes), new String(rs.getBytes(3)));
            assertFalse(rs.next());
        } catch (SQLException e) {
            e.printStackTrace();
            fail();
        }
    }

    // mysql mode CONCUR_UPDATABLE -> CONCUR_READ_ONLY
    @Test
    public void testSENSITIVE_UPDATABLE_1() throws SQLException {
        createTable("ResultSetType", "c1 INT, c2 VARCHAR(100)");
        createTable("ResultSetTypePRIMARY",
            "c1 INT, c2 VARCHAR(100), constraint pk primary key(c1)");
        Connection conn = setConnectionOrigin("?useServerPrepStmts=true");

        PreparedStatement ps = conn.prepareStatement("SELECT * FROM ResultSetType",
            ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = ps.executeQuery();
        assertEquals(ResultSet.TYPE_SCROLL_SENSITIVE, rs.getType());
        assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());

        ps = conn.prepareStatement("SELECT c1,c2 FROM ResultSetType",
            ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        rs = ps.executeQuery();
        assertEquals(ResultSet.TYPE_SCROLL_SENSITIVE, rs.getType());
        assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());

        ps = conn.prepareStatement("SELECT * FROM ResultSetTypePRIMARY",
            ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        rs = ps.executeQuery();
        assertEquals(ResultSet.TYPE_SCROLL_SENSITIVE, rs.getType());
        assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
        ps = conn.prepareStatement("SELECT c1,c2 FROM ResultSetTypePRIMARY",
            ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        rs = ps.executeQuery();
        assertEquals(ResultSet.TYPE_SCROLL_SENSITIVE, rs.getType());
        assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
    }

    @Test
    public void testDefaultPrimaryKey() throws Exception {
        //        createTable("testUpdatedAbleInsert", "c1 int(20) AUTO_INCREMENT, c2 int(20) , c3 int(20), PRIMARY KEY (c1, c2)",
        //                "engine=innodb"); // success
        //        createTable("testUpdatedAbleInsert", "c1 int(20) AUTO_INCREMENT, c2 int(20) , c3 int(20), PRIMARY KEY (c1, c2)",
        //                "engine=myisam"); // success
        //        createTable("testUpdatedAbleInsert", "c1 int(20) , c2 int(20) AUTO_INCREMENT, c3 int(20), PRIMARY KEY (c1, c2)",
        //                "engine=innodb"); // fail
        createTable("testUpdatedAbleInsert",
            "c1 int(20) , c2 int(20) AUTO_INCREMENT, c3 int(20), PRIMARY KEY (c1, c2)",
            "engine=myisam"); // success

        Statement stmt = sharedConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT * FROM testUpdatedAbleInsert");

        rs.moveToInsertRow();
        rs.updateInt("c1", 1);
        //        rs.updateInt("c2", 1);
        rs.updateInt("c3", 1);
        rs.insertRow();
        rs.last();
        assertEquals(1, rs.getInt("c2"));
        assertEquals(1, rs.getInt("c3"));

        rs.moveToInsertRow();
        rs.updateInt("c1", 2);
        //        rs.updateInt("c2", 1);
        rs.updateInt("c3", 2);
        rs.insertRow();
        rs.last();
        assertEquals(2, rs.getInt("c2")); //mysql:1
        assertEquals(2, rs.getInt("c3"));
    }

    @Test
    public void fix46558224() throws Exception {
        Assume.assumeFalse(sharedUsePrepare());
        //createTable("testUpdate", "c1 int, c2 varchar(20), c3 int, PRIMARY KEY (c1,c2)");
        createTable("testUpdate", "c1 int, c2 varchar(20), c3 int, PRIMARY KEY (c1,c3)");

        // text protocol
        Connection conn = sharedConnection;
        PreparedStatement ps = conn.prepareStatement("select * From testUpdate",
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = ps.executeQuery();

        rs.moveToInsertRow();
        rs.updateInt(1, 1);
        rs.updateString(2, "aaa");
        rs.updateInt(3, 2);
        rs.insertRow();

        assertTrue(rs.last());
        assertEquals(1, rs.getInt(1));
        assertEquals("aaa", rs.getString(2));
        assertEquals(2, rs.getInt(3));

        // PS protocol
        conn = setConnection("&useServerPrepStmts=true");
        ps = conn.prepareStatement("select * From testUpdate", ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_UPDATABLE);
        rs = ps.executeQuery();

        rs.moveToInsertRow();
        rs.updateInt(1, 3);
        rs.updateString(2, "bbb");
        rs.updateInt(3, 4);
        rs.insertRow();

        assertTrue(rs.last());
        assertEquals(3, rs.getInt(1));
        assertEquals("bbb", rs.getString(2));
        assertEquals(4, rs.getInt(3));
    }

}
