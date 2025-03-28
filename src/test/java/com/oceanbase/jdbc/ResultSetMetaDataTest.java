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
import java.util.*;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ResultSetMetaDataTest extends BaseTest {

    /**
     * Initialisation.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable(
            "test_rsmd",
            "id_col int not null primary key auto_increment, "
                    + "nullable_col varchar(20),unikey_col int unique, char_col char(10), us  smallint unsigned");
        createTable("t1", "id int, name varchar(20)");
        createTable("t2", "id int, name varchar(20)");
        createTable("t3", "id int, name varchar(20)");
    }

    @Test
    public void metaDataTest() throws SQLException {
        requireMinimumVersion(5, 0);
        Statement stmt = sharedConnection.createStatement();
        stmt.execute("insert into test_rsmd (id_col,nullable_col,unikey_col) values (null, 'hej', 9)");
        ResultSet rs = stmt
            .executeQuery("select id_col, nullable_col, unikey_col as something, char_col,us from test_rsmd");
        assertTrue(rs.next());
        ResultSetMetaData rsmd = rs.getMetaData();

        assertEquals(true, rsmd.isAutoIncrement(1));
        assertEquals(5, rsmd.getColumnCount());
        assertEquals(ResultSetMetaData.columnNullable, rsmd.isNullable(2));
        assertEquals(ResultSetMetaData.columnNoNulls, rsmd.isNullable(1));
        assertEquals(String.class.getName(), rsmd.getColumnClassName(2));
        assertEquals(Integer.class.getName(), rsmd.getColumnClassName(1));
        assertEquals(Integer.class.getName(), rsmd.getColumnClassName(3));
        assertEquals("id_col", rsmd.getColumnLabel(1));
        assertEquals("nullable_col", rsmd.getColumnLabel(2));
        assertEquals("something", rsmd.getColumnLabel(3));
        assertEquals("unikey_col", rsmd.getColumnName(3));
        assertEquals(Types.CHAR, rsmd.getColumnType(4));
        assertEquals(Types.SMALLINT, rsmd.getColumnType(5));
        assertFalse(rsmd.isReadOnly(1));
        assertFalse(rsmd.isReadOnly(2));
        assertFalse(rsmd.isReadOnly(3));
        assertFalse(rsmd.isReadOnly(4));
        assertFalse(rsmd.isReadOnly(5));
        assertTrue(rsmd.isWritable(1));
        assertTrue(rsmd.isWritable(2));
        assertTrue(rsmd.isWritable(3));
        assertTrue(rsmd.isWritable(4));
        assertTrue(rsmd.isWritable(5));
        assertTrue(rsmd.isDefinitelyWritable(1));
        assertTrue(rsmd.isDefinitelyWritable(2));
        assertTrue(rsmd.isDefinitelyWritable(3));
        assertTrue(rsmd.isDefinitelyWritable(4));
        assertTrue(rsmd.isDefinitelyWritable(5));

        try {
            rsmd.isReadOnly(6);
            fail("must have throw exception");
        } catch (SQLException sqle) {
            assertTrue(sqle.getMessage().contains("wrong column index 6. must be in [1, 5] range"));
        }
        try {
            rsmd.isWritable(6);
            fail("must have throw exception");
        } catch (SQLException sqle) {
            assertTrue(sqle.getMessage().contains("wrong column index 6. must be in [1, 5] range"));
        }
        try {
            rsmd.isDefinitelyWritable(6);
            fail("must have throw exception");
        } catch (SQLException sqle) {
            assertTrue(sqle.getMessage().contains("wrong column index 6. must be in [1, 5] range"));
        }

        rs = stmt.executeQuery("select count(char_col) from test_rsmd");
        assertTrue(rs.next());
        rsmd = rs.getMetaData();
        assertTrue(rsmd.isReadOnly(1));

        DatabaseMetaData md = sharedConnection.getMetaData();
        ResultSet cols = md.getColumns(null, null, "test\\_rsmd", null);
        cols.next();
        assertEquals("id_col", cols.getString("COLUMN_NAME"));
        assertEquals(Types.INTEGER, cols.getInt("DATA_TYPE"));
        cols.next(); /* nullable_col */
        cols.next(); /* unikey_col */
        cols.next(); /* char_col */
        assertEquals("char_col", cols.getString("COLUMN_NAME"));
        assertEquals(Types.CHAR, cols.getInt("DATA_TYPE"));
        cols.next(); /* us */// CONJ-96: SMALLINT UNSIGNED gives Types.SMALLINT
        assertEquals("us", cols.getString("COLUMN_NAME"));
        assertEquals(Types.SMALLINT, cols.getInt("DATA_TYPE"));
    }

    @Test
    public void conj17() throws Exception {
        requireMinimumVersion(5, 0);
        ResultSet rs = sharedConnection.createStatement().executeQuery(
            "select count(*),1 from information_schema.tables");
        assertTrue(rs.next());
        assertEquals(rs.getMetaData().getColumnName(1), "count(*)");
        assertEquals(rs.getMetaData().getColumnName(2), "1");
    }

    @Test
    public void conj84() throws Exception {
        requireMinimumVersion(5, 0);
        Statement stmt = sharedConnection.createStatement();

        stmt.execute("INSERT INTO t1 VALUES (1, 'foo')");
        stmt.execute("INSERT INTO t2 VALUES (2, 'bar')");
        ResultSet rs = sharedConnection.createStatement().executeQuery(
            "select t1.*, t2.* FROM t1 join t2");
        assertTrue(rs.next());
        assertEquals(rs.findColumn("id"), 1);
        assertEquals(rs.findColumn("name"), 2);
        assertEquals(rs.findColumn("t1.id"), 1);
        assertEquals(rs.findColumn("t1.name"), 2);
        assertEquals(rs.findColumn("t2.id"), 3);
        assertEquals(rs.findColumn("t2.name"), 4);
    }

    @Test
    public void testAlias() throws Exception {
        createTable("testAlias", "id int, name varchar(20)");
        createTable("testAlias2", "id2 int, name2 varchar(20)");
        Statement stmt = sharedConnection.createStatement();

        stmt.execute("INSERT INTO testAlias VALUES (1, 'foo')");
        stmt.execute("INSERT INTO testAlias2 VALUES (2, 'bar')");
        ResultSet rs = sharedConnection.createStatement().executeQuery(
            "select alias1.id as idalias1 , alias1.name as namealias1, id2 as idalias2 , name2, testAlias.id "
                    + "FROM testAlias as alias1 join testAlias2 as alias2 join testAlias");
        assertTrue(rs.next());

        assertEquals(rs.findColumn("idalias1"), 1);
        assertEquals(rs.findColumn("alias1.idalias1"), 1);

        assertEquals(rs.findColumn("name"), 2);
        assertEquals(rs.findColumn("namealias1"), 2);
        assertEquals(rs.findColumn("alias1.namealias1"), 2);

        assertEquals(rs.findColumn("id2"), 3);
        assertEquals(rs.findColumn("idalias2"), 3);
        assertEquals(rs.findColumn("alias2.idalias2"), 3);
        assertEquals(rs.findColumn("testAlias2.id2"), 3);

        assertEquals(rs.findColumn("name2"), 4);
        assertEquals(rs.findColumn("testAlias2.name2"), 4);
        assertEquals(rs.findColumn("alias2.name2"), 4);

        assertEquals(rs.findColumn("id"), 5);
        assertEquals(rs.findColumn("testAlias.id"), 5);

        try {
            rs.findColumn("alias2.name22");
            fail("Must have thrown exception");
        } catch (SQLException sqle) {
            // normal exception
        }

        try {
            assertEquals(rs.findColumn(""), 4);
            fail("Must have thrown exception");
        } catch (SQLException sqle) {
            // normal exception
        }

        try {
            assertEquals(rs.findColumn(null), 4);
            fail("Must have thrown exception");
        } catch (SQLException sqle) {
            // normal exception
        }
    }

    /*
     * CONJ-149: ResultSetMetaData.getTableName returns table alias instead of real table name
     *
     * @throws SQLException
     */
    @Test
  public void tableNameTest() throws Exception {
    ResultSet rs =
        sharedConnection
            .createStatement()
            .executeQuery("SELECT id AS id_alias FROM t3 AS t1_alias");
    ResultSetMetaData rsmd = rs.getMetaData();

    // this should return the original name of the table, not the alias
    logInfo(rsmd.getTableName(1));
    assertEquals(rsmd.getTableName(1), "t3");

    assertEquals(rsmd.getColumnLabel(1), "id_alias");
    assertEquals(rsmd.getColumnName(1), "id");

    // add useOldAliasMetadataBehavior to get the alias instead of the real
    // table name
    try (Connection connection = setConnection("&useOldAliasMetadataBehavior")) {
      rs = connection.createStatement().executeQuery("SELECT id AS id_alias FROM t3 AS t1_alias");
      rsmd = rs.getMetaData();

      logInfo(rsmd.getTableName(1));
      assertEquals(rsmd.getTableName(1), "t1_alias");
      assertEquals(rsmd.getColumnLabel(1), "id_alias");
      assertEquals(rsmd.getColumnName(1), "id_alias");
    }

    try (Connection connection = setConnection("&blankTableNameMeta")) {
      rs = connection.createStatement().executeQuery("SELECT id AS id_alias FROM t3 AS t1_alias");
      rsmd = rs.getMetaData();

      assertEquals(rsmd.getTableName(1), "");
      assertEquals(rsmd.getColumnLabel(1), "id_alias");
      assertEquals(rsmd.getColumnName(1), "id");
    }

    try (Connection connection = setConnection("&blankTableNameMeta&useOldAliasMetadataBehavior")) {
      rs = connection.createStatement().executeQuery("SELECT id AS id_alias FROM t3 AS t1_alias");
      rsmd = rs.getMetaData();

      assertEquals(rsmd.getTableName(1), "");
      assertEquals(rsmd.getColumnLabel(1), "id_alias");
      assertEquals(rsmd.getColumnName(1), "id_alias");
    }
  }

  @Test
  public void testLongBlob() throws SQLException {
      Statement stmt = sharedConnection.createStatement();
      createTable("test_column_display_size","c1 TINYBLOB, c2 BLOB, c3 MEDIUMBLOB, c4 LONGBLOB");
      ResultSet rs = stmt.executeQuery("select * from test_column_display_size");
      ResultSetMetaData metaData = rs.getMetaData();
      assertEquals(255,metaData.getColumnDisplaySize(1));
      assertEquals(65535,metaData.getColumnDisplaySize(2));
      assertEquals(16777215,metaData.getColumnDisplaySize(3));
      assertEquals(2147483647,metaData.getColumnDisplaySize(4));

      assertEquals(-3,metaData.getColumnType(1));
      assertEquals(-4,metaData.getColumnType(2));
      assertEquals(-4,metaData.getColumnType(3));
      assertEquals(-4,metaData.getColumnType(4));
  }

    @Test
    public void testLongTextForAone53146546() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        createTable("test_column_display_size","c1 TINYTEXT, c2 TEXT, c3 MEDIUMTEXT, c4 LongTEXT");
        ResultSet rs = stmt.executeQuery("select * from test_column_display_size");
        ResultSetMetaData metaData = rs.getMetaData();
        // for utf8mb4
        assertEquals(63,metaData.getColumnDisplaySize(1));
        assertEquals(16383,metaData.getColumnDisplaySize(2));
        assertEquals(4194303,metaData.getColumnDisplaySize(3));
        assertEquals(536870911,metaData.getColumnDisplaySize(4));

        assertEquals(-1,metaData.getColumnType(1));
        assertEquals(-1,metaData.getColumnType(2));
        assertEquals(-1,metaData.getColumnType(3));
        assertEquals(-1,metaData.getColumnType(4));

    }

    @Test
    public void testForAone57111946() throws SQLException {
        Connection conn = setConnection("&useNewResultSetMetaData=true&compatibleMysqlVersion=5");
        createTable("t_meta", "c1 boolean, c2 bigint, c3 bigint(255) zerofill , c4 tinyint, c5  tinyint(1), c6 smallint, c7 smallint(5) unsigned" +
                ", c8 mediumint, c9 mediumint(64) zerofill, c10 numeric, c11 numeric(20,5), c12 decimal, c13  decimal(65,30), c14 double, c15 double(15,5), c16 FLOAT " +
                ", c17 FLOAT(7,5), c18 bit , c19 bit(51), c20 datetime, c21 datetime(6), c22 timestamp, c23 TIMESTAMP(3), c24 date, c25 time, c26 time(3), c27 year , c28 char(0)" +
                ", c29 char(80), " +
                "d1 char(80) character set gbk, d2 varchar(0), d3 varchar(51), d4 varchar(51) character set gbk, d5 BINARY(51), d6 varbinary(51), d7 blob, " +
                "d8 blob(51), d9 tinyblob, d10 mediumblob, d11 longblob, d12 text, d13 text character set gbk, d14 tinytext, d15 tinytext character set gbk, d16 mediumtext, d17 mediumtext character set gbk" +
                ", d18 longtext, d19 enum('red', 'green', 'blue'), d20 ENUM('12', '1.2222222'), d21 set('red', 'green', 'blue'), d22 json, d23 point, d24 geometry, d25 linestring, d26 polygon, d27 multipoint, " +
                "d28 multilinestring, d29 multipolygon, d30 geometrycollection");

        class metaInfo{
            private String columnName;
            private String columnDefine;
            private int[] metainfos;
            metaInfo(String columnName, String columnDefine, int[] metainfos){
                this.columnName = columnName;
                this.columnDefine = columnDefine;
                this.metainfos = metainfos;
            }
        }

        List<metaInfo> metaInfoListlist = new ArrayList<>();
        metaInfoListlist.add(new metaInfo("c1", "boolean", new int[]{1, 1, 0 }));
        metaInfoListlist.add(new metaInfo("c2", "bigint", new int[]{20, 20, 0 }));
        metaInfoListlist.add(new metaInfo("c3", "bigint(255) zerofill", new int[]{255, 255, 0 }));
        metaInfoListlist.add(new metaInfo("c4", "tinyint", new int[]{4, 4, 0 }));
        metaInfoListlist.add(new metaInfo("c5", "tinyint(1)", new int[]{1, 1, 0 }));
        metaInfoListlist.add(new metaInfo("c6", "smallint", new int[]{6, 6, 0 }));
        metaInfoListlist.add(new metaInfo("c7", "smallint(5) unsigned", new int[]{5, 5, 0 }));
        metaInfoListlist.add(new metaInfo("c8", "mediumint", new int[]{9, 9, 0 }));
        metaInfoListlist.add(new metaInfo("c9", "mediumint(64) zerofill", new int[]{64, 64, 0 }));
        metaInfoListlist.add(new metaInfo("c10", "numeric", new int[]{11, 10, 0 }));

        metaInfoListlist.add(new metaInfo("c11", "numeric(20,5)", new int[]{22, 20, 5}));
        metaInfoListlist.add(new metaInfo("c12", "decimal", new int[]{11, 10, 0}));
        metaInfoListlist.add(new metaInfo("c13", "decimal(65,30)", new int[]{67, 65, 30}));
        metaInfoListlist.add(new metaInfo("c14", "double", new int[]{23, 23, 31}));
        metaInfoListlist.add(new metaInfo("c15", "double(15,5)", new int[]{15, 15, 5}));
        metaInfoListlist.add(new metaInfo("c16", "FLOAT", new int[]{12, 12, 31}));
        metaInfoListlist.add(new metaInfo("c17", "FLOAT(7,5)", new int[]{7, 7, 5}));
        metaInfoListlist.add(new metaInfo("c18", "bit", new int[]{1, 1, 0}));
        metaInfoListlist.add(new metaInfo("c19", "bit(51)", new int[]{51, 51, 0}));
        metaInfoListlist.add(new metaInfo("c20", "datetime", new int[]{19, 19, 0}));
        metaInfoListlist.add(new metaInfo("c21", "datetime(6)", new int[]{26, 26, 0}));
        metaInfoListlist.add(new metaInfo("c22", "timestamp", new int[]{19, 19, 0}));
        metaInfoListlist.add(new metaInfo("c23", "TIMESTAMP(3)", new int[]{23, 23, 0}));
        metaInfoListlist.add(new metaInfo("c24", "date", new int[]{10, 10, 0}));
        metaInfoListlist.add(new metaInfo("c25", "time", new int[]{10, 10, 0}));
        metaInfoListlist.add(new metaInfo("c26", "time(3)", new int[]{14, 14, 0}));
        metaInfoListlist.add(new metaInfo("c27", "year", new int[]{4, 4, 0}));
        metaInfoListlist.add(new metaInfo("c28", "char(0)", new int[]{0, 0, 0}));
        metaInfoListlist.add(new metaInfo("c29", "char(80)", new int[]{80, 80, 0}));
        metaInfoListlist.add(new metaInfo("d1", "char(80) character set gbk", new int[]{80, 80, 0}));
        metaInfoListlist.add(new metaInfo("d2", "varchar(0)", new int[]{0, 0, 0}));
        metaInfoListlist.add(new metaInfo("d3", "varchar(51)", new int[]{51, 51, 0}));
        metaInfoListlist.add(new metaInfo("d4", "varchar(51) character set gbk", new int[]{51, 51, 0}));

        metaInfoListlist.add(new metaInfo("d5", "BINARY(51)", new int[]{51, 51, 0}));
        metaInfoListlist.add(new metaInfo("d6", "varbinary(51)", new int[]{51, 51, 0}));
        metaInfoListlist.add(new metaInfo("d7", "blob", new int[]{65535, 65535, 0}));
        metaInfoListlist.add(new metaInfo("d8", "blob(51)", new int[]{255, 255, 0}));
        metaInfoListlist.add(new metaInfo("d9", "tinyblob", new int[]{255, 255, 0}));
        metaInfoListlist.add(new metaInfo("d10", "mediumblob", new int[]{16777215, 16777215, 0}));
        metaInfoListlist.add(new metaInfo("d11", "longblob", new int[]{2147483647, 2147483647, 0}));

        metaInfoListlist.add(new metaInfo("d12", "text", new int[]{16383, 16383, 0}));
        metaInfoListlist.add(new metaInfo("d13", "text character set gbk", new int[]{16383, 16383, 0}));
        metaInfoListlist.add(new metaInfo("d14", "tinytext", new int[]{63, 63, 0}));
        metaInfoListlist.add(new metaInfo("d15", "tinytext character set gbk", new int[]{63, 63, 0}));
        metaInfoListlist.add(new metaInfo("d16", "mediumtext", new int[]{4194303, 4194303, 0}));
        metaInfoListlist.add(new metaInfo("d17", "mediumtext character set gbk", new int[]{4194303, 4194303, 0}));
        metaInfoListlist.add(new metaInfo("d18", "longtext", new int[]{536870911, 536870911, 0}));
        metaInfoListlist.add(new metaInfo("d19", "enum('red', 'green', 'blue')", new int[]{5, 5, 0}));
        metaInfoListlist.add(new metaInfo("d20", "ENUM('12', '1.2222222')", new int[]{9, 9, 0}));
        metaInfoListlist.add(new metaInfo("d21", "set('red', 'green', 'blue')", new int[]{14, 14, 0}));
        metaInfoListlist.add(new metaInfo("d22", "json", new int[]{2147483647, 2147483647, 0}));

        metaInfoListlist.add(new metaInfo("d23", "point", new int[]{2147483647, 2147483647, 0}));
        metaInfoListlist.add(new metaInfo("d24", "geometry", new int[]{2147483647, 2147483647, 0}));
        metaInfoListlist.add(new metaInfo("d25", "linestring", new int[]{2147483647, 2147483647, 0}));
        metaInfoListlist.add(new metaInfo("d26", "polygon", new int[]{2147483647, 2147483647, 0}));
        metaInfoListlist.add(new metaInfo("d27", "multipoint", new int[]{2147483647, 2147483647, 0}));
        metaInfoListlist.add(new metaInfo("d28", "multilinestring", new int[]{2147483647, 2147483647, 0}));
        metaInfoListlist.add(new metaInfo("d29", "multipolygon", new int[]{2147483647, 2147483647, 0}));
        metaInfoListlist.add(new metaInfo("d30", "geometrycollection", new int[]{2147483647, 2147483647, 0}));

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select * from t_meta");
        ResultSetMetaData metaData = rs.getMetaData();
        int size = metaData.getColumnCount();
        for (int i = 1; i <= size; i++) {
            metaInfo metaInfo = metaInfoListlist.get(i - 1);
            Assert.assertEquals(metaInfo.metainfos[0], metaData.getColumnDisplaySize(i));
            Assert.assertEquals(metaInfo.metainfos[1], metaData.getPrecision(i));
            Assert.assertEquals(metaInfo.metainfos[2], metaData.getScale(i));
        }
    }

    @Test
    public void testForAone55761318() throws SQLException {
        createTable("test55761318", "c1 DECIMAL UNSIGNED, c2 TINYINT UNSIGNED, c3 SMALLINT UNSIGNED, c4 INT UNSIGNED," +
                " c5 FLOAT UNSIGNED, c6 DOUBLE UNSIGNED, c7 BIGINT UNSIGNED, c8 MEDIUMINT UNSIGNED");
        Statement stmt = sharedConnection.createStatement();
        ResultSet rs = stmt.executeQuery("select * from test55761318");
        ResultSetMetaData metaData = rs.getMetaData();
        int count = metaData.getColumnCount();
        for (int i = 1; i <= count; i++) {
            Assert.assertTrue(metaData.getColumnTypeName(i).contains("UNSIGNED"));
        }
    }
}
