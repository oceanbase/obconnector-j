/**
 * OceanBase Client for Java
 * <p>
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2020 MariaDB Corporation Ab.
 * Copyright (c) 2021 OceanBase.
 * <p>
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 * <p>
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 * <p>
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 * <p>
 * This particular MariaDB Client for Java file is work
 * derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 * the following copyright and notice provisions:
 * <p>
 * Copyright (c) 2009-2011, Marcus Eriksson
 * <p>
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 * <p>
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 * <p>
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 * <p>
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 */
package com.oceanbase.jdbc;

import static org.junit.Assert.*;

import java.sql.*;
import java.util.Locale;

import org.junit.*;

public class DatabaseMetadataTest extends BaseTest {
    static String dbpkTest          = randomTableName("dbpk_test");
    static String datetimeTest      = randomTableName("datetime_test");
    static String yTable            = randomTableName("ytab");
    static String maxCharLenght     = randomTableName("maxcharlength");
    static String conj72            = randomTableName("conj72");
    static String versionTable      = randomTableName("versionTable");
    static String manycols          = randomTableName("manycols");
    static String getPrecision      = randomTableName("getPrecision");
    static String getTimePrecision  = randomTableName("getTimePrecision");
    static String getTimePrecision2 = randomTableName("getTimePrecision2");
    static String tableName2        = randomTableName("getIndexInfoTest");
    static String testUnsigned      = randomTableName("getIndexInfoTest");

    /**
     * Initialisation.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable(dbpkTest,
            "val varchar(20), id1 int not null, id2 int not null,primary key(id1, id2)",
            "engine=innodb");
        createTable(datetimeTest, "dt datetime");
        createTable(
            "`" + manycols + "`",
            "  `tiny` tinyint(4) DEFAULT NULL,"
                    + "  `tiny_uns` tinyint(3) unsigned DEFAULT NULL,"
                    + "  `small` smallint(6) DEFAULT NULL,"
                    + "  `small_uns` smallint(5) unsigned DEFAULT NULL,"
                    + "  `medium` mediumint(9) DEFAULT NULL,"
                    + "  `medium_uns` mediumint(8) unsigned DEFAULT NULL,"
                    + "  `int_col` int(11) DEFAULT NULL,"
                    + "  `int_col_uns` int(10) unsigned DEFAULT NULL,"
                    + "  `big` bigint(20) DEFAULT NULL,"
                    + "  `big_uns` bigint(20) unsigned DEFAULT NULL,"
                    + "  `decimal_col` decimal(10,5) DEFAULT NULL,"
                    + "  `fcol` float DEFAULT NULL,"
                    + "  `fcol_uns` float unsigned DEFAULT NULL,"
                    + "  `dcol` double DEFAULT NULL,"
                    + "  `dcol_uns` double unsigned DEFAULT NULL,"
                    + "  `date_col` date DEFAULT NULL,"
                    + "  `time_col` time DEFAULT NULL,"
                    + "  `timestamp_col` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
                    + "  `year_col` year(4) DEFAULT NULL," + "  `bit_col` bit(5) DEFAULT NULL,"
                    + "  `char_col` char(5) DEFAULT NULL,"
                    + "  `varchar_col` varchar(10) DEFAULT NULL,"
                    + "  `binary_col` binary(10) DEFAULT NULL,"
                    + "  `varbinary_col` varbinary(10) DEFAULT NULL,"
                    + "  `tinyblob_col` tinyblob," + "  `blob_col` blob,"
                    + "  `mediumblob_col` mediumblob," + "  `longblob_col` longblob,"
                    + "  `tinytext_col` tinytext," + "  `text_col` text,"
                    + "  `mediumtext_col` mediumtext," + "  `longtext_col` longtext");
        createTable(yTable, "y year");
        createTable(maxCharLenght, "maxcharlength char(1)", "character set utf8");
        createTable(conj72, "t tinyint(1)");
        //        createTable(versionTable, "x INT", "WITH SYSTEM VERSIONING");
        createTable(getPrecision, "num1 NUMERIC(9,4), " + "num2 NUMERIC (9,0),"
                                  + "num3 NUMERIC (9,4) UNSIGNED," + "num4 NUMERIC (9,0) UNSIGNED,"
                                  + "num5 FLOAT(9,4)," + "num6 FLOAT(9,4) UNSIGNED,"
                                  + "num7 DOUBLE(9,4)," + "num8 DOUBLE(9,4) UNSIGNED");
        createTable(getTimePrecision, "d date, " + "t1 datetime(0)," + "t2 datetime(6),"
                                      + "t3 timestamp(0) DEFAULT '2000-01-01 00:00:00',"
                                      + "t4 timestamp(6) DEFAULT '2000-01-01 00:00:00',"
                                      + "t5 time(0)," + "t6 time(6)");
        createTable(getTimePrecision2, "d date, " + "t1 datetime(0)," + "t2 datetime(6),"
                                       + "t3 timestamp(0) DEFAULT '2000-01-01 00:00:00',"
                                       + "t4 timestamp(6) DEFAULT '2000-01-01 00:00:00',"
                                       + "t5 time(0)," + "t6 time(6)");
        createTable(tableName2, "no INT NOT NULL ,\n" + "    product_category INT NOT NULL,\n"
                                + "    product_id INT NOT NULL,\n"
                                + "    customer_id INT NOT NULL,\n" + "    PRIMARY KEY(no)\n");
        createTable(testUnsigned, "id bigint(20) unsigned AUTO_INCREMENT PRIMARY KEY");
    }

    private static void checkType(String name, int actualType, String colName, int expectedType) {
        if (name.equals(colName)) {
            assertEquals(actualType, expectedType);
        }
    }

    @Before
    public void checkSupported() throws SQLException {
        requireMinimumVersion(5, 1);
    }

    @Test
    public void primaryKeysTest() throws SQLException {
        DatabaseMetaData dbmd = sharedConnection.getMetaData();
        ResultSet rs = dbmd.getPrimaryKeys("test", null, dbpkTest);
        int counter = 0;
        while (rs.next()) {
            counter++;
            assertEquals("test", rs.getString("table_cat"));
            assertEquals(null, rs.getString("table_schem"));
            assertEquals(dbpkTest.toLowerCase(), rs.getString("table_name"));
            assertEquals("id" + counter, rs.getString("column_name"));
            assertEquals("id" + counter, rs.getString("column_name"));
            assertEquals("PRIMARY", rs.getString("PK_NAME"));
        }
        assertEquals(2, counter);
    }

    @Test
    public void primaryKeyTest2() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        stmt.execute("drop table if exists t2");
        stmt.execute("drop table if exists t1");
        stmt.execute("CREATE TABLE t1 ( id1 integer, constraint pk primary key(id1))");
        stmt.execute("CREATE TABLE t2 (id2a integer, id2b integer, constraint pk primary key(id2a, id2b), "
                     + "constraint fk12 foreign key(id2a) references t1(id1),  constraint fk22 foreign key(id2b) "
                     + "references t1(id1))");

        DatabaseMetaData dbmd = sharedConnection.getMetaData();
        ResultSet rs = dbmd.getPrimaryKeys("test", null, "t2");
        int counter = 0;
        while (rs.next()) {
            counter++;
            assertEquals("test", rs.getString("table_cat"));
            assertEquals(null, rs.getString("table_schem"));
            assertEquals("t2", rs.getString("table_name"));
            assertEquals(counter, rs.getShort("key_seq"));
            assertEquals("PRIMARY", rs.getString("pk_name"));
        }
        assertEquals(2, counter);
        stmt.execute("drop table if exists t2");
        stmt.execute("drop table if exists t1");
    }

    @Test
    public void datetimeTest() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        ResultSet rs = stmt.executeQuery("select * from " + datetimeTest);
        assertEquals(93, rs.getMetaData().getColumnType(1));
    }

    @Test
    public void functionColumns() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        DatabaseMetaData md = sharedConnection.getMetaData();

        if (md.getDatabaseMajorVersion() < 5) {
            return;
        } else if (md.getDatabaseMajorVersion() == 5 && md.getDatabaseMinorVersion() < 5) {
            return;
        }

        stmt.execute("DROP FUNCTION IF EXISTS hello");
        stmt.execute("CREATE FUNCTION hello (s CHAR(20), i int) RETURNS CHAR(50) DETERMINISTIC  "
                     + "RETURN CONCAT('Hello, ',s,'!')");
        ResultSet rs = sharedConnection.getMetaData().getFunctionColumns(null, null, "hello", null);
        assertTrue(rs.next());

        /* First row is for return value */
        assertEquals(rs.getString("FUNCTION_CAT"), sharedConnection.getCatalog());
        assertEquals(rs.getString("FUNCTION_SCHEM"), null);
        assertEquals(rs.getString("COLUMN_NAME"), ""); /* No name, since it is return value */
        assertEquals(rs.getInt("COLUMN_TYPE"), DatabaseMetaData.functionReturn);
        assertEquals(rs.getInt("DATA_TYPE"), Types.CHAR);
        assertEquals(rs.getString("TYPE_NAME"), "CHAR");

        assertTrue(rs.next());
        assertEquals(rs.getString("COLUMN_NAME"), "s"); /* input parameter 's' (CHAR) */
        assertEquals(rs.getInt("COLUMN_TYPE"), DatabaseMetaData.functionColumnIn);
        assertEquals(rs.getInt("DATA_TYPE"), Types.CHAR);
        assertEquals(rs.getString("TYPE_NAME"), "CHAR");

        assertTrue(rs.next());
        assertEquals(rs.getString("COLUMN_NAME"), "i"); /* input parameter 'i' (INT) */
        assertEquals(rs.getInt("COLUMN_TYPE"), DatabaseMetaData.functionColumnIn);
        assertEquals(rs.getInt("DATA_TYPE"), Types.INTEGER);
        assertEquals(rs.getString("TYPE_NAME"), "INT");
        stmt.execute("DROP FUNCTION IF EXISTS hello");
    }

    /**
     * Same as getImportedKeys, with one foreign key in a table in another catalog.
     */
    // not support
    @Ignore
    public void getImportedKeys() throws Exception {
        // cancel for MySQL 8.0, since CASCADE with I_S give importedKeySetDefault, not
        // importedKeyCascade
        Statement st = sharedConnection.createStatement();

        st.execute("DROP TABLE IF EXISTS product_order");
        st.execute("DROP TABLE IF EXISTS t1.product ");
        st.execute("DROP TABLE IF EXISTS `cus``tomer`");
        st.execute("DROP DATABASE IF EXISTS test1");

        st.execute("CREATE DATABASE IF NOT EXISTS t1");

        st.execute("CREATE TABLE t1.product ( category INT NOT NULL, id INT NOT NULL, price DECIMAL,"
                   + " PRIMARY KEY(category, id) )   ENGINE=INNODB");

        st.execute("CREATE TABLE `cus``tomer` (id INT NOT NULL, PRIMARY KEY (id))   ENGINE=INNODB");

        st.execute("CREATE TABLE product_order (\n" + "    no INT NOT NULL AUTO_INCREMENT,\n"
                   + "    product_category INT NOT NULL,\n" + "    product_id INT NOT NULL,\n"
                   + "    customer_id INT NOT NULL,\n" + "    PRIMARY KEY(no),\n"
                   + "    INDEX (product_category, product_id),\n" + "    INDEX (customer_id),\n"
                   + "    FOREIGN KEY (product_category, product_id)\n"
                   + "      REFERENCES t1.product(category, id)\n"
                   + "      ON UPDATE CASCADE ON DELETE RESTRICT,\n"
                   + "    FOREIGN KEY (customer_id)\n" + "      REFERENCES `cus``tomer`(id)\n"
                   + ")   ENGINE=INNODB;");

        /*
        Test that I_S implementation is equivalent to parsing "show create table" .
         Get result sets using either method and compare (ignore minor differences INT vs SMALLINT
        */
        ResultSet rs1 = ((OceanBaseDatabaseMetaData) sharedConnection.getMetaData())
            .getImportedKeysUsingShowCreateTable("test", "product_order");
        ResultSet rs2 = ((OceanBaseDatabaseMetaData) sharedConnection.getMetaData())
            .getImportedKeysUsingInformationSchema("test", "product_order");
        assertEquals(rs1.getMetaData().getColumnCount(), rs2.getMetaData().getColumnCount());

        while (rs1.next()) {
            assertTrue(rs2.next());
            for (int i = 1; i <= rs1.getMetaData().getColumnCount(); i++) {
                Object s1 = rs1.getObject(i);
                Object s2 = rs2.getObject(i);
                if (s1 instanceof Number && s2 instanceof Number) {
                    assertEquals(((Number) s1).intValue(), ((Number) s2).intValue());
                } else {
                    if (s1 != null && s2 != null && !s1.equals(s2)) {
                        fail();
                    }
                    assertEquals(s1, s2);
                }
            }
        }

        /* Also compare metadata */
        ResultSetMetaData md1 = rs1.getMetaData();
        ResultSetMetaData md2 = rs2.getMetaData();
        for (int i = 1; i <= md1.getColumnCount(); i++) {
            assertEquals(md1.getColumnLabel(i), md2.getColumnLabel(i));
        }
        st.execute("DROP TABLE IF EXISTS product_order");
        st.execute("DROP TABLE IF EXISTS t1.product ");
        st.execute("DROP TABLE IF EXISTS `cus``tomer`");
        st.execute("DROP DATABASE IF EXISTS test1");
    }

    @Test
    public void exportedKeysTest() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        stmt.execute("drop table if exists fore_key0");
        stmt.execute("drop table if exists fore_key1");
        stmt.execute("drop table if exists prim_key");
        stmt.execute("drop table if exists fore_key3");
        stmt.execute("drop table if exists prim2_key");

        stmt.execute("create table prim_key (id int not null primary key, val varchar(20)) engine=innodb");
        stmt.execute("create table prim2_key (id int not null primary key, val varchar(20)) engine=innodb");
        stmt.execute("create table fore_key0 (id int not null primary key, "
                     + "id_ref0 int, foreign key (id_ref0) references prim_key(id)) engine=innodb");
        stmt.execute("create table fore_key1 (id int not null primary key, "
                     + "id_ref1 int, foreign key (id_ref1) references prim_key(id) on update cascade) engine=innodb");
        stmt.execute("create table fore_key3 (id int not null primary key, "
                     + "id_ref0 int, foreign key (id_ref0) references prim2_key(id)) engine=innodb");

        DatabaseMetaData dbmd = sharedConnection.getMetaData();
        ResultSet rs = dbmd.getExportedKeys("test", null, "prim_key");
        int count = rs.getMetaData().getColumnCount();
        int counter = 0;
        while (rs.next()) {
            assertEquals("id", rs.getString("pkcolumn_name"));
            assertEquals("fore_key" + counter, rs.getString("fktable_name"));
            assertEquals("id_ref" + counter, rs.getString("fkcolumn_name"));
            assertEquals(null, rs.getString("pk_name")); // same as 1.x
            counter++;
        }
        assertEquals(2, counter);
        // Fuzzy query is not supported temporarily. Table must now be specified.The following test code needs to be retained.
        /*
        rs = dbmd.getExportedKeys("test", null, "prim_k%");
        counter = 0;
        while (rs.next()) {
            assertEquals("id", rs.getString("pkcolumn_name"));
            assertEquals("fore_key" + counter, rs.getString("fktable_name"));
            assertEquals("id_ref" + counter, rs.getString("fkcolumn_name"));
            assertEquals("PRIMARY", rs.getString("pk_name"));
            counter++;
        }
        assertEquals(2, counter);

        rs = dbmd.getExportedKeys("test", null, null);
        counter = 0;
        int totalCounter = 0;
        while (rs.next()) {
            if ("prim_key".equals(rs.getString("pktable_name"))) {
                assertEquals("id", rs.getString("pkcolumn_name"));
                assertEquals("fore_key" + counter, rs.getString("fktable_name"));
                assertEquals("id_ref" + counter, rs.getString("fkcolumn_name"));
                assertEquals("PRIMARY", rs.getString("PK_NAME"));
                counter++;
            }
            totalCounter++;
        }
        assertEquals(2, counter);
        assertTrue(totalCounter > 2);
        */
        stmt.execute("drop table if exists fore_key0");
        stmt.execute("drop table if exists fore_key1");
        stmt.execute("drop table if exists prim_key");
    }

    @Test
    public void importedKeysTest() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        stmt.execute("drop table if exists fore_key0");
        stmt.execute("drop table if exists fore_key1");
        stmt.execute("drop table if exists prim_key");

        stmt.execute("create table prim_key (id int not null primary key, "
                     + "val varchar(20)) engine=innodb");
        stmt.execute("create table fore_key0 (id int not null primary key, "
                     + "id_ref0 int, foreign key (id_ref0) references prim_key(id)) engine=innodb");
        stmt.execute("create table fore_key1 (id int not null primary key, "
                     + "id_ref1 int, foreign key (id_ref1) references prim_key(id) on update cascade) engine=innodb");

        DatabaseMetaData dbmd = sharedConnection.getMetaData();
        ResultSet rs = dbmd.getImportedKeys(sharedConnection.getCatalog(), null, "fore_key0");
        int counter = 0;
        while (rs.next()) {
            assertEquals("id", rs.getString("pkcolumn_name"));
            assertEquals("prim_key", rs.getString("pktable_name"));
            counter++;
        }
        assertEquals(1, counter);
        stmt.execute("drop table if exists fore_key0");
        stmt.execute("drop table if exists fore_key1");
        stmt.execute("drop table if exists prim_key");
    }

    @Test
    public void testGetCatalogs() throws SQLException {
        DatabaseMetaData dbmd = sharedConnection.getMetaData();

        ResultSet rs = dbmd.getCatalogs();

        boolean haveMysql = false;
        boolean haveInformationSchema = false;
        while (rs.next()) {
            String cat = rs.getString(1);

            if (cat.equalsIgnoreCase("mysql")) {
                haveMysql = true;
            } else if (cat.equalsIgnoreCase("information_schema")) {
                haveInformationSchema = true;
            }
        }
        assertTrue(haveMysql);
        assertTrue(haveInformationSchema);
    }

    @Test
    public void testGetTables() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        stmt.execute("drop table if exists fore_key0");
        stmt.execute("drop table if exists fore_key1");
        stmt.execute("drop table if exists prim_key");

        stmt.execute("create table prim_key (id int not null primary key, "
                     + "val varchar(20)) engine=innodb");
        stmt.execute("create table fore_key0 (id int not null primary key, "
                     + "id_ref0 int, foreign key (id_ref0) references prim_key(id)) engine=innodb");
        stmt.execute("create table fore_key1 (id int not null primary key, "
                     + "id_ref1 int, foreign key (id_ref1) references prim_key(id) on update cascade) engine=innodb");

        DatabaseMetaData dbmd = sharedConnection.getMetaData();
        ResultSet rs = dbmd.getTables(null, null, "prim_key", null);

        assertEquals(true, rs.next());
        rs = dbmd.getTables("", null, "prim_key", null);
        assertEquals(true, rs.next());
    }

    @Test
    public void testGetTables2() throws SQLException {
        DatabaseMetaData dbmd = sharedConnection.getMetaData();
        ResultSet rs = dbmd.getTables("information_schema", null, "TABLE_PRIVILEGES",
            new String[] { "SYSTEM VIEW" });
        assertEquals(true, rs.next());
        assertEquals(false, rs.next());
        rs = dbmd.getTables(null, null, "TABLE_PRIVILEGES", new String[] { "TABLE" });
        assertEquals(false, rs.next());
    }

    @Ignore
    @Test
    public void testGetTablesSystemVersionTables() throws SQLException {
        //TODO not supported
        DatabaseMetaData dbmd = sharedConnection.getMetaData();
        ResultSet rs = dbmd.getTables(null, null, "versionTable", null);
        assertEquals(true, rs.next());
        assertEquals(false, rs.next());
        rs = dbmd.getTables(null, null, "versionTable", new String[] { "TABLE" });
        assertEquals(true, rs.next());
        assertEquals(false, rs.next());
        rs = dbmd.getTables(null, null, "versionTable", new String[] { "SYSTEM VIEW" });
        assertEquals(false, rs.next());
    }

    @Test
    public void testGetTables3() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        stmt.execute("drop table if exists table_type_test");

        stmt.execute("create table table_type_test (id int not null primary key, "
                     + "val varchar(20)) engine=innodb");

        DatabaseMetaData dbmd = sharedConnection.getMetaData();
        ResultSet tableSet = dbmd.getTables(null, null, "table_type_test", null);

        assertEquals(true, tableSet.next());

        String tableName = tableSet.getString("TABLE_NAME");
        assertEquals("table_type_test", tableName);

        String tableType = tableSet.getString("TABLE_TYPE");
        assertEquals("TABLE", tableType);
        // see for possible values
        // https://docs.oracle.com/javase/7/docs/api/java/sql/DatabaseMetaData.html#getTableTypes%28%29
    }

    @Test
    public void testGetTables4() throws SQLException {
        //Fix mysql compatibility backquotes:  #issue/49071651
        Statement stmt = sharedConnection.createStatement();
        String tableNameTest = "table_backquotes_test";
        stmt.execute("drop table if exists " + tableNameTest);

        stmt.execute("create table " + tableNameTest + " (id int not null primary key, "
                     + "val varchar(20)) engine=innodb");

        DatabaseMetaData dbmd = sharedConnection.getMetaData();
        ResultSet tableSet = dbmd.getTables(null, null, "`" + tableNameTest + "`",
            new String[] { "TABLE" });

        assertEquals(true, tableSet.next());

        String tableName = tableSet.getString("TABLE_NAME");
        assertEquals(tableNameTest, tableName);

        String tableType = tableSet.getString("TABLE_TYPE");
        assertEquals("TABLE", tableType);
        tableSet.close();
    }

    @Test
    public void testGetColumns() throws SQLException {
        // mysql 5.6 doesn't permit VIRTUAL keyword
        Assume.assumeTrue(isMariadbServer() || !isMariadbServer() && minVersion(5, 7));

        if (minVersion(10, 2) || !isMariadbServer()) {
            createTable("tablegetcolumns",
                "a INT NOT NULL primary key auto_increment, b VARCHAR(32), c INT AS (CHAR_LENGTH(b)) VIRTUAL, "
                        + "d VARCHAR(5) AS (left(b,5)) STORED", "CHARACTER SET 'utf8mb4'");
        } else {
            createTable("tablegetcolumns",
                "a INT NOT NULL primary key auto_increment, b VARCHAR(32), c INT AS (CHAR_LENGTH(b)) VIRTUAL, "
                        + "d VARCHAR(5) AS (left(b,5)) PERSISTENT", "CHARACTER SET 'utf8mb4'");
        }

        DatabaseMetaData dbmd = sharedConnection.getMetaData();
        ResultSet rs = dbmd.getColumns(null, null, "tablegetcolumns", null);
        assertTrue(rs.next());
        assertEquals("testj", rs.getString(1)); // TABLE_CAT
        assertEquals(null, rs.getString(2)); // TABLE_SCHEM
        assertEquals("tablegetcolumns", rs.getString(3)); // TABLE_NAME
        assertEquals("a", rs.getString(4)); // COLUMN_NAME
        assertEquals(Types.INTEGER, rs.getInt(5)); // DATA_TYPE
        assertEquals("INT", rs.getString(6)); // "TYPE_NAME
        assertEquals(10, rs.getInt(7)); // "COLUMN_SIZE  //TODO return value is 10 (11->10)
        assertEquals(0, rs.getInt(9)); // DECIMAL_DIGITS
        assertEquals(10, rs.getInt(10)); // NUM_PREC_RADIX
        assertEquals(0, rs.getInt(11)); // NULLABLE
        assertEquals("", rs.getString(12)); // REMARKS
        assertEquals(null, rs.getString(13)); // COLUMN_DEF
        assertEquals(0, rs.getInt(16)); // CHAR_OCTET_LENGTH
        assertEquals(1, rs.getInt(17)); // ORDINAL_POSITION
        assertEquals("NO", rs.getString(18)); // IS_NULLABLE
        assertEquals(null, rs.getString(19)); // SCOPE_CATALOG
        assertEquals(null, rs.getString(20)); // SCOPE_SCHEMA
        assertEquals(null, rs.getString(21)); // SCOPE_TABLE
        assertEquals(0, rs.getShort(22)); // SOURCE_DATA_TYPE
        assertEquals("YES", rs.getString(23)); // IS_AUTOINCREMENT
        assertEquals("NO", rs.getString(24)); // IS_GENERATEDCOLUMN

        assertTrue(rs.next());
        assertEquals("testj", rs.getString(1)); // TABLE_CAT
        assertEquals(null, rs.getString(2)); // TABLE_SCHEM
        assertEquals("tablegetcolumns", rs.getString(3)); // TABLE_NAME
        assertEquals("b", rs.getString(4)); // COLUMN_NAME
        assertEquals(Types.VARCHAR, rs.getInt(5)); // DATA_TYPE
        assertEquals("VARCHAR", rs.getString(6)); // "TYPE_NAME
        assertEquals(32, rs.getInt(7)); // "COLUMN_SIZE
        assertEquals(0, rs.getInt(9)); // DECIMAL_DIGITS
        assertEquals(10, rs.getInt(10)); // NUM_PREC_RADIX
        assertEquals(1, rs.getInt(11)); // NULLABLE
        assertEquals("", rs.getString(12)); // REMARKS

        // since 10.2.7, value that are expected as String are enclosed with single quotes as javadoc
        // require
        assertTrue("null".equalsIgnoreCase(rs.getString(13)) || rs.getString(13) == null); // COLUMN_DEF
        assertEquals(32 * 4, rs.getInt(16)); // CHAR_OCTET_LENGTH
        assertEquals(2, rs.getInt(17)); // ORDINAL_POSITION
        assertEquals("YES", rs.getString(18)); // IS_NULLABLE
        assertEquals(null, rs.getString(19)); // SCOPE_CATALOG
        assertEquals(null, rs.getString(20)); // SCOPE_SCHEMA
        assertEquals(null, rs.getString(21)); // SCOPE_TABLE
        assertEquals(0, rs.getShort(22)); // SOURCE_DATA_TYPE
        assertEquals("NO", rs.getString(23)); // IS_AUTOINCREMENT
        assertEquals("NO", rs.getString(24)); // IS_GENERATEDCOLUMN

        assertTrue(rs.next());
        assertEquals("testj", rs.getString(1)); // TABLE_CAT
        assertEquals(null, rs.getString(2)); // TABLE_SCHEM
        assertEquals("tablegetcolumns", rs.getString(3)); // TABLE_NAME
        assertEquals("c", rs.getString(4)); // COLUMN_NAME
        assertEquals(Types.INTEGER, rs.getInt(5)); // DATA_TYPE
        assertEquals("INT", rs.getString(6)); // "TYPE_NAME
        assertEquals(10, rs.getInt(7)); // "COLUMN_SIZE //TODO 11->10
        assertEquals(0, rs.getInt(9)); // DECIMAL_DIGITS
        assertEquals(10, rs.getInt(10)); // NUM_PREC_RADIX
        assertEquals(1, rs.getInt(11)); // NULLABLE
        assertEquals("", rs.getString(12)); // REMARKS

        // since 10.2.7, value that are expected as String are enclosed with single quotes as javadoc
        // require
        assertTrue("CHAR_LENGTH(`b`)".equalsIgnoreCase(rs.getString(13))
                   || rs.getString(13) == null); // COLUMN_DEF

        assertEquals(0, rs.getInt(16)); // CHAR_OCTET_LENGTH
        assertEquals(3, rs.getInt(17)); // ORDINAL_POSITION
        assertEquals("YES", rs.getString(18)); // IS_NULLABLE
        assertEquals(null, rs.getString(19)); // SCOPE_CATALOG
        assertEquals(null, rs.getString(20)); // SCOPE_SCHEMA
        assertEquals(null, rs.getString(21)); // SCOPE_TABLE
        assertEquals(0, rs.getShort(22)); // SOURCE_DATA_TYPE
        assertEquals("NO", rs.getString(23)); // IS_AUTOINCREMENT
        assertEquals("YES", rs.getString(24)); // IS_GENERATEDCOLUMN //TODO NO -> YES

        assertTrue(rs.next());
        assertEquals("testj", rs.getString(1)); // TABLE_CAT
        assertEquals(null, rs.getString(2)); // TABLE_SCHEM
        assertEquals("tablegetcolumns", rs.getString(3)); // TABLE_NAME
        assertEquals("d", rs.getString(4)); // COLUMN_NAME
        assertEquals(Types.VARCHAR, rs.getInt(5)); // DATA_TYPE
        assertEquals("VARCHAR", rs.getString(6)); // "TYPE_NAME
        assertEquals(5, rs.getInt(7)); // "COLUMN_SIZE
        assertEquals(0, rs.getInt(9)); // DECIMAL_DIGITS
        assertEquals(10, rs.getInt(10)); // NUM_PREC_RADIX
        assertEquals(1, rs.getInt(11)); // NULLABLE
        assertEquals("", rs.getString(12)); // REMARKS
        // since 10.2.7, value that are expected as String are enclosed with single quotes as javadoc
        // require
        assertTrue("left(`b`,5)".equalsIgnoreCase(rs.getString(13)) || rs.getString(13) == null); // COLUMN_DEF
        assertEquals(5 * 4, rs.getInt(16)); // CHAR_OCTET_LENGTH
        assertEquals(4, rs.getInt(17)); // ORDINAL_POSITION
        assertEquals("YES", rs.getString(18)); // IS_NULLABLE
        assertEquals(null, rs.getString(19)); // SCOPE_CATALOG
        assertEquals(null, rs.getString(20)); // SCOPE_SCHEMA
        assertEquals(null, rs.getString(21)); // SCOPE_TABLE
        assertEquals(0, rs.getShort(22)); // SOURCE_DATA_TYPE
        assertEquals("NO", rs.getString(23)); // IS_AUTOINCREMENT
        assertEquals("YES", rs.getString(24)); // IS_GENERATEDCOLUMN //TODO NO -> YES
        assertFalse(rs.next());
    }

    private void testResultSetColumns(ResultSet rs, String spec) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        String[] tokens = spec.split(",");

        for (int i = 0; i < tokens.length; i++) {
            String[] splitTokens = tokens[i].trim().split(" ");
            String label = splitTokens[0];
            String type = splitTokens[1];

            int col = i + 1;
            assertEquals(label, rsmd.getColumnLabel(col));
            int columnType = rsmd.getColumnType(col);
            switch (type) {
                case "String":
                    assertTrue("invalid type  " + columnType + " for " + rsmd.getColumnLabel(col)
                               + ",expected String", columnType == Types.VARCHAR
                                                     || columnType == Types.NULL
                                                     || columnType == Types.LONGVARCHAR);
                    break;
                case "decimal":
                    assertTrue("invalid type  " + columnType + "( " + rsmd.getColumnTypeName(col)
                               + " ) for " + rsmd.getColumnLabel(col) + ",expected decimal",
                        columnType == Types.DECIMAL);
                    break;
                case "int":
                case "short":
                    assertTrue("invalid type  " + columnType + "( " + rsmd.getColumnTypeName(col)
                               + " ) for " + rsmd.getColumnLabel(col) + ",expected numeric",
                        columnType == Types.BIGINT || columnType == Types.INTEGER
                                || columnType == Types.SMALLINT || columnType == Types.TINYINT
                                || columnType == Types.DECIMAL);

                    break;
                case "boolean":
                    assertTrue("invalid type  " + columnType + "( " + rsmd.getColumnTypeName(col)
                               + " ) for " + rsmd.getColumnLabel(col) + ",expected boolean",
                        columnType == Types.BOOLEAN || columnType == Types.BIT);

                    break;
                case "null":
                    assertTrue("invalid type  " + columnType + " for " + rsmd.getColumnLabel(col)
                               + ",expected null", columnType == Types.NULL);
                    break;
                default:
                    fail("invalid type '" + type + "'");
                    break;
            }
        }
    }

    @Test
    public void getAttributesBasic() throws Exception {
        testResultSetColumns(
            sharedConnection.getMetaData().getAttributes(null, null, null, null),
            "TYPE_CAT String,TYPE_SCHEM String,TYPE_NAME String,"
                    + "ATTR_NAME String,DATA_TYPE int,ATTR_TYPE_NAME String,ATTR_SIZE int,DECIMAL_DIGITS int,"
                    + "NUM_PREC_RADIX int,NULLABLE int,REMARKS String,ATTR_DEF String,SQL_DATA_TYPE int,"
                    + "SQL_DATETIME_SUB int, CHAR_OCTET_LENGTH int,ORDINAL_POSITION int,IS_NULLABLE String,"
                    + "SCOPE_CATALOG String,SCOPE_SCHEMA String,"
                    + "SCOPE_TABLE String,SOURCE_DATA_TYPE short");
    }

    @Test
    public void identifierCaseSensitivity() throws Exception {
        Statement stmt = sharedConnection.createStatement();
        try {
            if (sharedConnection.getMetaData().supportsMixedCaseIdentifiers()) {
                /* Case-sensitive identifier handling, we can create both t1 and T1 */
                stmt.execute("create table aB (i int)");
                stmt.execute("create table AB (i int)");
                /* Check there is an entry for both T1 and t1 in getTables */
                ResultSet rs = sharedConnection.getMetaData().getTables(null, null, "aB", null);
                assertTrue(rs.next());
                assertFalse(rs.next());
                rs = sharedConnection.getMetaData().getTables(null, null, "AB", null);
                assertTrue(rs.next());
                assertFalse(rs.next());
            }

            if (sharedConnection.getMetaData().storesMixedCaseIdentifiers()) {
                /* Case-insensitive, case-preserving */
                stmt.execute("create table aB (i int)");
                try {
                    stmt.execute("create table AB (i int)");
                    fail("should not get there, since names are case-insensitive");
                } catch (SQLException e) {
                    // normal error
                }

                /* Check that table is stored case-preserving */
                ResultSet rs = sharedConnection.getMetaData().getTables(null, null, "aB%", null);
                while (rs.next()) {
                    String tableName = rs.getString("TABLE_NAME");
                    if (tableName.length() == 2) {
                        assertEquals("aB", tableName);
                    }
                }

                rs = sharedConnection.getMetaData().getTables(null, null, "AB", null);
                assertTrue(rs.next());
                assertFalse(rs.next());
            }

            if (sharedConnection.getMetaData().storesLowerCaseIdentifiers()) {
                /* case-insensitive, identifiers converted to lowercase */
                /* Case-insensitive, case-preserving */
                stmt.execute("create table aB (i int)");
                try {
                    stmt.execute("create table AB (i int)");
                    fail("should not get there, since names are case-insensitive");
                } catch (SQLException e) {
                    // normal error
                }

                /* Check that table is stored lowercase */
                ResultSet rs = sharedConnection.getMetaData().getTables(null, null, "aB%", null);
                while (rs.next()) {
                    String tableName = rs.getString("TABLE_NAME");
                    if (tableName.length() == 2) {
                        assertEquals("ab", tableName);
                    }
                }

                rs = sharedConnection.getMetaData().getTables(null, null, "AB", null);
                assertTrue(rs.next());
                assertFalse(rs.next());
            }
            assertFalse(sharedConnection.getMetaData().storesUpperCaseIdentifiers());
        } finally {
            try {
                stmt.execute("DROP TABLE aB");
            } catch (SQLException sqle) {
                // ignore
            }
            try {
                stmt.execute("DROP TABLE AB");
            } catch (SQLException sqle) {
                // ignore
            }
        }
    }

    @Test
    public void getBestRowIdentifierBasic() throws SQLException {
        testResultSetColumns(
            sharedConnection.getMetaData().getBestRowIdentifier(null, null, "test", 0, true),
            "SCOPE short,COLUMN_NAME String,DATA_TYPE int, TYPE_NAME String,"
                    + "COLUMN_SIZE int,BUFFER_LENGTH int,"
                    + "DECIMAL_DIGITS short,PSEUDO_COLUMN short");
    }

    @Test
    public void getClientInfoPropertiesBasic() throws Exception {
        testResultSetColumns(sharedConnection.getMetaData().getClientInfoProperties(),
            "NAME String, MAX_LEN int, DEFAULT_VALUE String, DESCRIPTION String");
        ResultSet rs = sharedConnection.getMetaData().getClientInfoProperties();
        assertTrue(rs.next());
        assertEquals("ApplicationName", rs.getString(1));
        assertEquals(0x00ffffff, rs.getInt(2));
        assertEquals("", rs.getString(3));
        assertEquals("The name of the application currently utilizing the connection",
            rs.getString(4));

        assertTrue(rs.next());
        assertEquals("ClientUser", rs.getString(1));
        assertEquals(0x00ffffff, rs.getInt(2));
        assertEquals("", rs.getString(3));
        assertEquals(
            "The name of the user that the application using the connection is performing work for. "
                    + "This may not be the same as the user name that was used in establishing the connection.",
            rs.getString(4));

        assertTrue(rs.next());
        assertEquals("ClientHostname", rs.getString(1));
        assertEquals(0x00ffffff, rs.getInt(2));
        assertEquals("", rs.getString(3));
        assertEquals(
            "The hostname of the computer the application using the connection is running on",
            rs.getString(4));
        assertFalse(rs.next());
    }

    @Test
    public void getCatalogsBasic() throws SQLException {
        testResultSetColumns(sharedConnection.getMetaData().getCatalogs(), "TABLE_CAT String");
    }

    @Test
    public void getColumnsBasic() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        stmt.execute("drop table if exists t2");
        stmt.execute("drop table if exists t1");
        stmt.execute("CREATE TABLE t1 ( id1 integer, constraint pk primary key(id1))");
        stmt.execute("CREATE TABLE t2 (id2a integer, id2b integer, constraint pk primary key(id2a, id2b), "
                     + "constraint fk1 foreign key(id2a) references t1(id1),  constraint fk2 foreign key(id2b) "
                     + "references t1(id1))");
        ResultSet rs = sharedConnection.getMetaData().getColumns("test", null, "t2", null);
        int count = rs.getMetaData().getColumnCount();
        testResultSetColumns(sharedConnection.getMetaData().getColumns(null, null, null, null),
            "TABLE_CAT String,TABLE_SCHEM String,TABLE_NAME String,COLUMN_NAME String,"
                    + "DATA_TYPE int,TYPE_NAME String,COLUMN_SIZE decimal,BUFFER_LENGTH int,"
                    + "DECIMAL_DIGITS int,NUM_PREC_RADIX int,NULLABLE int,"
                    + "REMARKS String,COLUMN_DEF String,SQL_DATA_TYPE int,"
                    + "SQL_DATETIME_SUB int, CHAR_OCTET_LENGTH int,"
                    + "ORDINAL_POSITION int,IS_NULLABLE String,"
                    + "SCOPE_CATALOG String,SCOPE_SCHEMA String,"
                    + "SCOPE_TABLE String,SOURCE_DATA_TYPE null");
    }

    @Test
    public void getProcedureColumnsBasic() throws SQLException {
        testResultSetColumns(
            sharedConnection.getMetaData().getProcedureColumns(null, null, null, null),
            "PROCEDURE_CAT String,PROCEDURE_SCHEM String,PROCEDURE_NAME String,COLUMN_NAME String ,"
                    + "COLUMN_TYPE short,DATA_TYPE int,TYPE_NAME String,PRECISION int,LENGTH int,SCALE short,"
                    + "RADIX short,NULLABLE short,REMARKS String,COLUMN_DEF String,SQL_DATA_TYPE int,"
                    + "SQL_DATETIME_SUB int ,CHAR_OCTET_LENGTH int,"
                    + "ORDINAL_POSITION int,IS_NULLABLE String,SPECIFIC_NAME String");
    }

    @Test
    public void getFunctionColumnsBasic() throws SQLException {
        testResultSetColumns(
            sharedConnection.getMetaData().getFunctionColumns(null, null, null, null),
            "FUNCTION_CAT String,FUNCTION_SCHEM String,FUNCTION_NAME String,COLUMN_NAME String,COLUMN_TYPE short,"
                    + "DATA_TYPE int,TYPE_NAME String,PRECISION int,LENGTH int,SCALE short,RADIX short,"
                    + "NULLABLE short,REMARKS String,CHAR_OCTET_LENGTH int,ORDINAL_POSITION int,"
                    + "IS_NULLABLE String,SPECIFIC_NAME String");
    }

    // getColumnPrivileges not support now
    @Ignore
    public void getColumnPrivilegesBasic() throws SQLException {
        testResultSetColumns(
            sharedConnection.getMetaData().getColumnPrivileges(null, null, "", null),
            "TABLE_CAT String,TABLE_SCHEM String,TABLE_NAME String,COLUMN_NAME String,"
                    + "GRANTOR String,GRANTEE String,PRIVILEGE String,IS_GRANTABLE String");
    }

    @Test
    public void getTablePrivilegesBasic() throws SQLException {
        testResultSetColumns(sharedConnection.getMetaData().getTablePrivileges(null, null, null),
            "TABLE_CAT String,TABLE_SCHEM String,TABLE_NAME String,GRANTOR String,"
                    + "GRANTEE String,PRIVILEGE String,IS_GRANTABLE String");
    }

    @Test
    public void getVersionColumnsBasic() throws SQLException {
        testResultSetColumns(sharedConnection.getMetaData().getVersionColumns(null, null, null),
            "SCOPE short, COLUMN_NAME String,DATA_TYPE int,TYPE_NAME String,"
                    + "COLUMN_SIZE int,BUFFER_LENGTH int,DECIMAL_DIGITS short,"
                    + "PSEUDO_COLUMN short");
    }

    @Test
    public void getPrimaryKeysBasic() throws SQLException {
        testResultSetColumns(
            sharedConnection.getMetaData().getPrimaryKeys(null, null, null),
            "TABLE_CAT String,TABLE_SCHEM String,TABLE_NAME String,COLUMN_NAME String,KEY_SEQ short,PK_NAME String");
    }

    @Test
    public void getImportedKeysBasic() throws SQLException {
        testResultSetColumns(
            sharedConnection.getMetaData().getImportedKeys(null, null, "test"),
            "PKTABLE_CAT String,PKTABLE_SCHEM String,PKTABLE_NAME String, PKCOLUMN_NAME String,FKTABLE_CAT String,"
                    + "FKTABLE_SCHEM String,FKTABLE_NAME String,FKCOLUMN_NAME String,KEY_SEQ short,"
                    + "UPDATE_RULE short,DELETE_RULE short,FK_NAME String,PK_NAME String,DEFERRABILITY short");
    }

    @Test
    public void getExportedKeysBasic() throws SQLException {
        testResultSetColumns(
            sharedConnection.getMetaData().getExportedKeys(null, null, "test"),
            "PKTABLE_CAT String,PKTABLE_SCHEM String,PKTABLE_NAME String, PKCOLUMN_NAME String,FKTABLE_CAT String,"
                    + "FKTABLE_SCHEM String,FKTABLE_NAME String,FKCOLUMN_NAME String,KEY_SEQ short,"
                    + "UPDATE_RULE short, DELETE_RULE short,FK_NAME String,PK_NAME String,DEFERRABILITY short");
    }

    @Test
    public void getCrossReferenceBasic() throws SQLException {
        testResultSetColumns(
            sharedConnection.getMetaData().getCrossReference(null, null, "", null, null, ""),
            "PKTABLE_CAT String,PKTABLE_SCHEM String,PKTABLE_NAME String, PKCOLUMN_NAME String,FKTABLE_CAT String,"
                    + "FKTABLE_SCHEM String,FKTABLE_NAME String,FKCOLUMN_NAME String,KEY_SEQ short,"
                    + "UPDATE_RULE short,DELETE_RULE short,FK_NAME String,PK_NAME String,DEFERRABILITY short");
    }

    @Test
    public void getUdtsBasic() throws SQLException {
        testResultSetColumns(sharedConnection.getMetaData().getUDTs(null, null, null, null),
            "TYPE_CAT String,TYPE_SCHEM String,TYPE_NAME String,CLASS_NAME String,DATA_TYPE int,"
                    + "REMARKS String,BASE_TYPE short");
    }

    @Test
    public void getSuperTypesBasic() throws SQLException {
        testResultSetColumns(sharedConnection.getMetaData().getSuperTypes(null, null, null),
            "TYPE_CAT String,TYPE_SCHEM String,TYPE_NAME String,SUPERTYPE_CAT String,"
                    + "SUPERTYPE_SCHEM String,SUPERTYPE_NAME String");
    }

    @Test
    public void getFunctionsBasic() throws SQLException {
        testResultSetColumns(
            sharedConnection.getMetaData().getFunctions(null, null, null),
            "FUNCTION_CAT String, FUNCTION_SCHEM String,FUNCTION_NAME String,REMARKS String,FUNCTION_TYPE short,SPECIFIC_NAME String");
    }

    @Test
    public void getSuperTablesBasic() throws SQLException {
        testResultSetColumns(sharedConnection.getMetaData().getSuperTables(null, null, null),
            "TABLE_CAT String,TABLE_SCHEM String,TABLE_NAME String, SUPERTABLE_NAME String");
    }

    @Test
    public void getSchemasBasic() throws SQLException {
        DatabaseMetaData dbmd = sharedConnection.getMetaData();
        ResultSet rs = dbmd.getSchemas();
        testResultSetColumns(rs, "TABLE_SCHEM String , TABLE_catalog String");
    }

    /* Verify default behavior for nullCatalogMeansCurrent (=true) */
    @Test
    public void nullCatalogMeansCurrent() throws Exception {
        String catalog = sharedConnection.getCatalog();
        ResultSet rs = sharedConnection.getMetaData().getColumns(null, null, null, null);
        while (rs.next()) {
            assertTrue(rs.getString("TABLE_CAT").equalsIgnoreCase(catalog));
        }
    }

    /* Verify that "nullCatalogMeansCurrent=false" works (i.e information_schema columns are returned)*/
    @Test
    public void nullCatalogMeansCurrent2() throws Exception {
        try (Connection connection = setConnection("&nullCatalogMeansCurrent=false")) {
            boolean haveInformationSchema = false;
            ResultSet rs = connection.getMetaData().getColumns(null, null, null, null);
            while (rs.next()) {
                if (rs.getString("TABLE_CAT").equalsIgnoreCase("information_schema")) {
                    haveInformationSchema = true;
                    break;
                }
            }
            assertTrue(haveInformationSchema);
        }
    }

    @Test
    public void testGetTypeInfoBasic() throws SQLException {
        testResultSetColumns(
            sharedConnection.getMetaData().getTypeInfo(),
            "TYPE_NAME String,DATA_TYPE int,PRECISION int,LITERAL_PREFIX String,"
                    + "LITERAL_SUFFIX String,CREATE_PARAMS String, NULLABLE short,CASE_SENSITIVE boolean,"
                    + "SEARCHABLE short,UNSIGNED_ATTRIBUTE boolean,FIXED_PREC_SCALE boolean, "
                    + "AUTO_INCREMENT boolean, LOCAL_TYPE_NAME String,MINIMUM_SCALE short,MAXIMUM_SCALE short,"
                    + "SQL_DATA_TYPE int,SQL_DATETIME_SUB int, NUM_PREC_RADIX int");
    }

    @Test
    public void getColumnsTest() throws SQLException {

        DatabaseMetaData dmd = sharedConnection.getMetaData();
        ResultSet rs = dmd.getColumns(sharedConnection.getCatalog(), null, manycols, null);
        int count = 0;
        while (rs.next()) {
            count++;
            String columnName = rs.getString("column_name");
            int type = rs.getInt("data_type");
            String typeName = rs.getString("type_name");
            assertFalse(typeName.contains("("));
            for (char c : typeName.toCharArray()) {
                assertTrue("bad typename " + typeName, c == ' ' || Character.isUpperCase(c));
            }
            checkType(columnName, type, "tiny", Types.TINYINT);
            checkType(columnName, type, "tiny_uns", Types.TINYINT);
            checkType(columnName, type, "small", Types.SMALLINT);
            checkType(columnName, type, "small_uns", Types.SMALLINT);
            checkType(columnName, type, "medium", Types.INTEGER);
            checkType(columnName, type, "medium_uns", Types.INTEGER);
            checkType(columnName, type, "int_col", Types.INTEGER);
            checkType(columnName, type, "int_col_uns", Types.INTEGER);
            checkType(columnName, type, "big", Types.BIGINT);
            checkType(columnName, type, "big_uns", Types.BIGINT);
            checkType(columnName, type, "decimal_col", Types.DECIMAL);
            checkType(columnName, type, "fcol", Types.REAL);
            checkType(columnName, type, "fcol_uns", Types.REAL);
            checkType(columnName, type, "dcol", Types.DOUBLE);
            checkType(columnName, type, "dcol_uns", Types.DOUBLE);
            checkType(columnName, type, "date_col", Types.DATE);
            checkType(columnName, type, "time_col", Types.TIME);
            checkType(columnName, type, "timestamp_col", Types.TIMESTAMP);
            checkType(columnName, type, "year_col", Types.DATE);
            checkType(columnName, type, "bit_col", Types.BIT);
            checkType(columnName, type, "char_col", Types.CHAR);
            checkType(columnName, type, "varchar_col", Types.VARCHAR);
            checkType(columnName, type, "binary_col", Types.BINARY);
            checkType(columnName, type, "tinyblob_col", Types.VARBINARY);
            checkType(columnName, type, "blob_col", Types.LONGVARBINARY);
            checkType(columnName, type, "longblob_col", Types.LONGVARBINARY);
            checkType(columnName, type, "mediumblob_col", Types.LONGVARBINARY);
            checkType(columnName, type, "tinytext_col", Types.VARCHAR);
            checkType(columnName, type, "text_col", Types.LONGVARCHAR);
            checkType(columnName, type, "mediumtext_col", Types.LONGVARCHAR);
            checkType(columnName, type, "longtext_col", Types.LONGVARCHAR);
        }
        System.out.println("count = " + count);
        Assert.assertEquals(32, count);
    }

    @Test
    public void yearIsShortType() throws Exception {
        try (Connection connection = setConnection("&yearIsDateType=false")) {
            connection.createStatement().execute("insert into " + yTable + " values(72)");

            ResultSet rs2 =
                    connection.getMetaData().getColumns(connection.getCatalog(), null, yTable, null);
            assertTrue(rs2.next());
            assertEquals(Types.SMALLINT, rs2.getInt("DATA_TYPE"));

            try (ResultSet rs =
                         connection.getMetaData().getColumns(connection.getCatalog(), null, yTable, null)) {
                assertTrue(rs.next());
                assertEquals(Types.SMALLINT, rs.getInt("DATA_TYPE"));
            }

            try (ResultSet rs1 = connection.createStatement().executeQuery("select * from " + yTable)) {
                assertEquals(rs1.getMetaData().getColumnType(1), Types.SMALLINT);
                assertTrue(rs1.next());
                assertTrue(rs1.getObject(1) instanceof Short);
                assertEquals(rs1.getShort(1), 1972);
            }
        }
    }

    @Test
    public void yearIsDateType() throws Exception {
        try (Connection connection = setConnection("&yearIsDateType=true")) {
            connection.createStatement().execute("insert into " + yTable + " values(72)");

            ResultSet rs2 =
                    connection.getMetaData().getColumns(connection.getCatalog(), null, yTable, null);
            assertTrue(rs2.next());
            assertEquals(Types.DATE, rs2.getInt("DATA_TYPE"));

            try (ResultSet rs =
                         connection.getMetaData().getColumns(connection.getCatalog(), null, yTable, null)) {
                assertTrue(rs.next());
                assertEquals(Types.DATE, rs.getInt("DATA_TYPE"));
            }

            try (ResultSet rs1 = connection.createStatement().executeQuery("select * from " + yTable)) {
                assertEquals(Types.DATE, rs1.getMetaData().getColumnType(1));
                assertTrue(rs1.next());
                assertTrue(rs1.getObject(1) instanceof Date);
                assertEquals("1972-01-01", rs1.getDate(1).toString());
            }
        }
    }

    /* CONJ-15 */
    @Test
    public void maxCharLengthUtf8() throws Exception {
        DatabaseMetaData dmd = sharedConnection.getMetaData();
        ResultSet rs = dmd.getColumns(sharedConnection.getCatalog(), null, maxCharLenght, null);
        assertTrue(rs.next());
        assertEquals(rs.getInt("COLUMN_SIZE"), 1);
    }

    @Test
    public void conj72() throws Exception {
        try (Connection connection = setConnection("&tinyInt1isBit=true")) {
            connection.createStatement().execute("insert into " + conj72 + " values(1)");
            ResultSet rs =
                    connection.getMetaData().getColumns(connection.getCatalog(), null, conj72, null);
            assertTrue(rs.next());
            assertEquals(rs.getInt("DATA_TYPE"), Types.BIT);
            ResultSet rs1 = connection.createStatement().executeQuery("select * from " + conj72);
            assertEquals(rs1.getMetaData().getColumnType(1), Types.BIT);
        }
    }

    @Test
    public void getPrecision() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM " + getPrecision);
        ResultSetMetaData rsmd = rs.getMetaData();
        assertEquals(9, rsmd.getPrecision(1));
        assertEquals(4, rsmd.getScale(1));
        assertEquals(9, rsmd.getPrecision(2));
        assertEquals(0, rsmd.getScale(2));
        assertEquals(9, rsmd.getPrecision(3));
        assertEquals(4, rsmd.getScale(3));
        assertEquals(9, rsmd.getPrecision(4));
        assertEquals(0, rsmd.getScale(4));
        assertEquals(9, rsmd.getPrecision(5));
        assertEquals(4, rsmd.getScale(5));
        assertEquals(9, rsmd.getPrecision(6));
        assertEquals(4, rsmd.getScale(6));
        assertEquals(9, rsmd.getPrecision(7));
        assertEquals(4, rsmd.getScale(7));
        assertEquals(9, rsmd.getPrecision(8));
        assertEquals(4, rsmd.getScale(8));
    }

    @Test
    public void getTimePrecision() throws SQLException {
        Assume.assumeTrue(doPrecisionTest);
        Statement stmt = sharedConnection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM " + getTimePrecision);
        ResultSetMetaData rsmd = rs.getMetaData();
        // date
        assertEquals(10, rsmd.getPrecision(1));
        assertEquals(0, rsmd.getScale(1));
        // datetime(0)
        assertEquals(19, rsmd.getPrecision(2));
        assertEquals(0, rsmd.getScale(2));
        // datetime(6)
        assertEquals(26, rsmd.getPrecision(3));
        assertEquals(0, rsmd.getScale(3));
        // timestamp(0)
        assertEquals(19, rsmd.getPrecision(4));
        assertEquals(0, rsmd.getScale(4));
        // timestamp(6)
        assertEquals(26, rsmd.getPrecision(5));
        assertEquals(0, rsmd.getScale(5));
        // time(0)
        assertEquals(10, rsmd.getPrecision(6));
        assertEquals(0, rsmd.getScale(6));
        // time(6)
        assertEquals(17, rsmd.getPrecision(7));
        assertEquals(0, rsmd.getScale(7));
    }

    @Test
    public void metaTimeResultSet() throws SQLException {
        Assume.assumeTrue(doPrecisionTest);

        final int columnSizeField = 7;

        DatabaseMetaData dmd = sharedConnection.getMetaData();
        ResultSet rs = dmd.getColumns(null, null, getTimePrecision2, null);
        // date
        assertTrue(rs.next());
        assertEquals(10, rs.getInt(columnSizeField));
        // datetime(0)
        assertTrue(rs.next());
        assertEquals(19, rs.getInt(columnSizeField));
        // datetime(6)
        assertTrue(rs.next());
        assertEquals(26, rs.getInt(columnSizeField));
        // timestamp(0)
        assertTrue(rs.next());
        assertEquals(19, rs.getInt(columnSizeField));
        // timestamp(6)
        assertTrue(rs.next());
        assertEquals(26, rs.getInt(columnSizeField));
        // time(0)
        assertTrue(rs.next());
        assertEquals(10, rs.getInt(columnSizeField));
        // time(6)
        assertTrue(rs.next());
        assertEquals(17, rs.getInt(columnSizeField));

        assertFalse(rs.next());
    }

    /**
     * CONJ-401 - getProcedureColumns precision when server doesn't support precision.
     *
     * @throws SQLException if connection error occur
     */
    @Test
    public void metaTimeNoPrecisionProcedureResultSet() throws SQLException {
        createProcedure("getProcTimePrecision2", "(IN  I date, " + "IN t1 DATETIME,"
                                                 + "IN t3 timestamp,"
                                                 + "IN t5 time) BEGIN SELECT I; END");

        final int precisionField = 8;
        final int lengthField = 9;
        final int scaleField = 10;

        DatabaseMetaData dmd = sharedConnection.getMetaData();
        ResultSet rs = dmd.getProcedureColumns(null, null, "getProcTimePrecision2", null);
        // date
        assertTrue(rs.next());
        assertEquals(10, rs.getInt(precisionField));
        assertEquals(10, rs.getInt(lengthField));
        assertEquals(0, rs.getInt(scaleField));
        assertTrue(rs.wasNull());
        // datetime(0)
        assertTrue(rs.next());
        assertEquals(19, rs.getInt(precisionField));
        assertEquals(19, rs.getInt(lengthField));
        assertEquals(0, rs.getInt(scaleField));
        // timestamp(0)
        assertTrue(rs.next());
        assertEquals(19, rs.getInt(precisionField));
        assertEquals(19, rs.getInt(lengthField));
        assertEquals(0, rs.getInt(scaleField));
        // time(0)
        assertTrue(rs.next());
        //        assertEquals(10, rs.getInt(precisionField));
        //        assertEquals(10, rs.getInt(lengthField));
        assertEquals(0, rs.getInt(scaleField));

        assertFalse(rs.next());
    }

    /**
     * CONJ-381 - getProcedureColumns returns NULL as TIMESTAMP/DATETIME precision instead of 19.
     *
     * @throws SQLException if connection error occur
     */
    @Test
    public void metaTimeProcedureResultSet() throws SQLException {
        Assume.assumeTrue(doPrecisionTest);
        createProcedure("getProcTimePrecision", "(IN  I date, " + "IN t1 DATETIME(0),"
                                                + "IN t2 DATETIME(6)," + "IN t3 timestamp(0),"
                                                + "IN t4 timestamp(6)," + "IN t5 time ,"
                                                + "IN t6 time(6)) BEGIN SELECT I; END");

        final int precisionField = 8;
        final int lengthField = 9;
        final int scaleField = 10;

        DatabaseMetaData dmd = sharedConnection.getMetaData();
        ResultSet rs = dmd.getProcedureColumns(null, null, "getProcTimePrecision", null);
        // date
        assertTrue(rs.next());
        assertEquals(10, rs.getInt(precisionField));
        assertEquals(10, rs.getInt(lengthField));
        assertEquals(0, rs.getInt(scaleField));
        assertTrue(rs.wasNull());
        // datetime(0)
        assertTrue(rs.next());
        assertEquals(19, rs.getInt(precisionField));
        assertEquals(19, rs.getInt(lengthField));
        assertEquals(0, rs.getInt(scaleField));
        // datetime(6)
        assertTrue(rs.next());
        assertEquals(19, rs.getInt(precisionField));
        assertEquals(19, rs.getInt(lengthField));
        assertEquals(0, rs.getInt(scaleField));
        // timestamp(0)
        assertTrue(rs.next());
        assertEquals(19, rs.getInt(precisionField));
        assertEquals(19, rs.getInt(lengthField));
        assertEquals(0, rs.getInt(scaleField));
        // timestamp(6)
        assertTrue(rs.next());
        assertEquals(19, rs.getInt(precisionField));
        assertEquals(19, rs.getInt(lengthField));
        assertEquals(0, rs.getInt(scaleField));
        // time(0)
        assertTrue(rs.next());
        assertEquals(8, rs.getInt(precisionField));
        assertEquals(8, rs.getInt(lengthField));
        assertEquals(0, rs.getInt(scaleField));
        // time(6)
        assertTrue(rs.next());
        assertEquals(8, rs.getInt(precisionField));
        assertEquals(8, rs.getInt(lengthField));
        assertEquals(0, rs.getInt(scaleField));

        assertFalse(rs.next());
    }

    @Test
    public void getDatabaseProductNameTest() throws SQLException {
        DatabaseMetaData dmd = sharedConnection.getMetaData();
        String databaseProductName = dmd.getDatabaseProductName();
        assertEquals(databaseProductName, "MySQL");
    }

    @Test
    public void getProceduresBasic() throws SQLException {
        ResultSet rs = sharedConnection.getMetaData().getProcedures(null, null, null);
        testResultSetColumns(
            rs,
            "PROCEDURE_CAT String,PROCEDURE_SCHEM String,PROCEDURE_NAME String,RESERVED1 String,RESERVED2 String,RESERVED3 String,PROCEDURE_TYPE int,REMARKS String,SPECIFIC_NAME String");
    }

    @Test
    public void getDriverNameTest() throws SQLException {
        assertEquals(sharedConnection.getMetaData().getDriverName(), "OceanBase Connector/J");
    }

    @Test
    public void getUserNameTest() throws SQLException {
        assertEquals(sharedConnection.getMetaData().getUserName(), sharedConnection.getUsername());
    }

    @Test
    public void getIndexInfoTest() {
        try {
            Connection connection = sharedConnection;
            ResultSet rs = connection.getMetaData().getIndexInfo(null, null,
                tableName2.toUpperCase(Locale.ROOT), false, false); // same as oracle driver ,Need to use uppercase table name
            int columnCount = rs.getMetaData().getColumnCount();
            Assert.assertEquals(13, columnCount);
            ResultSetMetaData resultSetMetaData = rs.getMetaData();
            Assert.assertTrue(rs.next());
            for (int i = 1; i <= columnCount; i++) {
                System.out.print("column " + resultSetMetaData.getColumnName(i));
                System.out.println(" value = " + rs.getString(i));
            }
            Assert.assertEquals("TABLE_CAT", resultSetMetaData.getColumnName(1));
            Assert.assertEquals("test", rs.getString(1));
            Assert.assertEquals("TABLE_NAME", resultSetMetaData.getColumnName(3));
            Assert.assertEquals(tableName2.toLowerCase(), rs.getString(3));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testUnsigned() {
        try {
            //Long.MAX_VALUE = 9223372036854775807
            //Long.MAX_VALUE*2 = 18446744073709551614
            Statement stmt = sharedConnection.createStatement();
            assertEquals(1, stmt.executeUpdate("insert into " + testUnsigned
                                               + " values(18446744073709551200)",
                Statement.RETURN_GENERATED_KEYS));

            ResultSet rs = stmt.getGeneratedKeys();
            assertTrue(rs.next());
            System.out.println(rs.getMetaData().getColumnName(1));
            System.out.println(rs.getMetaData().getColumnTypeName(1));
            assertEquals("18446744073709551200", rs.getObject(1).toString());
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testGetImportedKeys() throws SQLException {
        Connection conn = sharedConnection;
        Statement stmt = conn.createStatement();
        try {
            stmt.execute("DROP TABLE IF EXISTS foreign_33");
            stmt.execute("DROP TABLE IF EXISTS foreign_22");
            stmt.execute("DROP TABLE IF EXISTS foreign_11");

            stmt.execute("create table foreign_11 (id1 int primary key)");
            stmt.execute("create table foreign_22 (id2 int primary key)");
            stmt.execute("create table foreign_33 (foreign_1_id int, foreign_2_id int, primary key (foreign_1_id, foreign_2_id),"
                         + " foreign key (foreign_1_id) references foreign_11(id1),foreign key (foreign_2_id) references foreign_22(id2))");

            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getImportedKeys(null, null, "foreign_33");
            assertTrue(rs.next());
            assertEquals("id2", rs.getString("PKCOLUMN_NAME"));
            assertEquals("foreign_2_id", rs.getString("FKCOLUMN_NAME"));
            assertTrue(rs.next());
            assertEquals("id1", rs.getString("PKCOLUMN_NAME"));
            assertEquals("foreign_1_id", rs.getString("FKCOLUMN_NAME"));
            assertFalse(rs.next());
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        } finally {
            stmt.execute("DROP TABLE IF EXISTS foreign_33");
            stmt.execute("DROP TABLE IF EXISTS foreign_22");
            stmt.execute("DROP TABLE IF EXISTS foreign_11");
        }
    }

    /**
     * CONJ-412: tinyInt1isBit and yearIsDateType is not applied in method columnTypeClause.
     *
     * @throws Exception if exception occur
     */
    @Test
    public void testYearDataType() throws Exception {
        createTable(
                "yearTableMeta", "xx tinyint(1), x2 tinyint(1) unsigned, yy year(4), zz bit, uu smallint");
        try (Connection connection = setConnection()) {
            checkResults(connection, true, true);
        }

        try (Connection connection = setConnection("&yearIsDateType=false&tinyInt1isBit=false")) {
            checkResults(connection, false, false);
        }
    }

    private void checkResults(Connection connection, boolean yearAsDate, boolean tinyAsBit)
            throws SQLException {
        DatabaseMetaData meta = connection.getMetaData();
        ResultSet rs = meta.getColumns(null, null, "yearTableMeta", null);
        assertTrue(rs.next());
        assertEquals(tinyAsBit ? "BIT" : "TINYINT", rs.getString(6));
        assertTrue(rs.next());
        assertEquals(tinyAsBit ? "BIT" : "TINYINT UNSIGNED", rs.getString(6));
        assertTrue(rs.next());
        assertEquals(yearAsDate ? "YEAR" : "SMALLINT", rs.getString(6));
        assertEquals(yearAsDate ? null : "5", rs.getString(7)); // column size
        assertEquals(yearAsDate ? null : "0", rs.getString(9)); // decimal digit
    }

    @Test
    public void metadataNullWhenNotPossible() throws SQLException {
        try (PreparedStatement preparedStatement =
                     sharedConnection.prepareStatement(
                             "LOAD DATA LOCAL INFILE 'dummy.tsv' INTO TABLE LocalInfileInputStreamTest (id, ?)")) {
            assertNull(preparedStatement.getMetaData());
            ParameterMetaData parameterMetaData = preparedStatement.getParameterMetaData();
            assertEquals(1, parameterMetaData.getParameterCount());
            try {
                parameterMetaData.getParameterType(1);
                fail("must have throw error");
            } catch (SQLException sqle) {
                assertTrue(sqle.getMessage().contains("not supported"));
            }
            try {
                parameterMetaData.getParameterClassName(1);
                fail("must have throw error");
            } catch (SQLException sqle) {
                assertTrue(sqle.getMessage().contains("Unknown parameter metadata class name"));
            }
            try {
                parameterMetaData.getParameterTypeName(1);
                fail("must have throw error");
            } catch (SQLException sqle) {
                assertTrue(sqle.getMessage().contains("Unknown parameter metadata type name"));
            }
            try {
                parameterMetaData.getPrecision(1);
                fail("must have throw error");
            } catch (SQLException sqle) {
                assertTrue(sqle.getMessage().contains("Unknown parameter metadata precision"));
            }
            try {
                parameterMetaData.getScale(1);
                fail("must have throw error");
            } catch (SQLException sqle) {
                assertTrue(sqle.getMessage().contains("Unknown parameter metadata scale"));
            }

            try {
                parameterMetaData.getParameterType(1000);
                fail("must have throw error");
            } catch (SQLException sqle) {
                assertTrue(sqle.getMessage().contains("param was 1000 and must be in range 1 - 1"));
            }
            try {
                parameterMetaData.getParameterClassName(1000);
                fail("must have throw error");
            } catch (SQLException sqle) {
                assertTrue(sqle.getMessage().contains("param was 1000 and must be in range 1 - 1"));
            }
            try {
                parameterMetaData.getParameterTypeName(1000);
                fail("must have throw error");
            } catch (SQLException sqle) {
                assertTrue(sqle.getMessage().contains("param was 1000 and must be in range 1 - 1"));
            }
            try {
                parameterMetaData.getPrecision(1000);
                fail("must have throw error");
            } catch (SQLException sqle) {
                assertTrue(sqle.getMessage().contains("param was 1000 and must be in range 1 - 1"));
            }
            try {
                parameterMetaData.getScale(1000);
                fail("must have throw error");
            } catch (SQLException sqle) {
                assertTrue(sqle.getMessage().contains("param was 1000 and must be in range 1 - 1"));
            }
        }
    }

    @Test
    public void getIdentifierQuoteStringTest() throws Exception {
        Assume.assumeFalse(sharedOptions().useOldAliasMetadataBehavior);
        Statement stmt = null;
        ResultSet rs = null;
        ResultSetMetaData meta = null;

        createTable("quote_test", "`id` int primary key , `create_TIME` timestamp");
        stmt = sharedConnection.createStatement();
        stmt.executeUpdate("insert into quote_test values(1,now())");

        rs = stmt.executeQuery("select * from quote_test");
        assertTrue(rs.next());
        assertEquals("`", sharedConnection.getMetaData().getIdentifierQuoteString());
        meta = rs.getMetaData();
        Assert.assertEquals("create_TIME", meta.getColumnName(2));
        Assert.assertEquals("create_TIME", meta.getColumnLabel(2));
        Assert.assertEquals("", meta.getSchemaName(2));
        Assert.assertEquals("test", meta.getCatalogName(2));
        Assert.assertEquals("quote_test", meta.getTableName(2));

        rs = stmt.executeQuery("select `id`, CREATE_TIME from quote_test");
        assertTrue(rs.next());
        meta = rs.getMetaData();
        Assert.assertEquals("create_TIME", meta.getColumnName(2));//CREATE_TIME
        Assert.assertEquals("CREATE_TIME", meta.getColumnLabel(2));
        Assert.assertEquals("", meta.getSchemaName(2));
        Assert.assertEquals("test", meta.getCatalogName(2));
        Assert.assertEquals("quote_test", meta.getTableName(2));

        rs = stmt.executeQuery("select `id`, CREATE_TIME as create_time from quote_test");
        assertTrue(rs.next());
        meta = rs.getMetaData();
        Assert.assertEquals("create_TIME", meta.getColumnName(2));//create_time
        Assert.assertEquals("create_time", meta.getColumnLabel(2));
        Assert.assertEquals("", meta.getSchemaName(2));
        Assert.assertEquals("test", meta.getCatalogName(2));
        Assert.assertEquals("quote_test", meta.getTableName(2));

        rs = stmt.executeQuery("select `id`, `create_TIME` as create_time from quote_test");
        assertTrue(rs.next());
        meta = rs.getMetaData();
        Assert.assertEquals("create_TIME", meta.getColumnName(2));//create_time
        Assert.assertEquals("create_time", meta.getColumnLabel(2));
        Assert.assertEquals("", meta.getSchemaName(2));
        Assert.assertEquals("test", meta.getCatalogName(2));
        Assert.assertEquals("quote_test", meta.getTableName(2));

        createTable("quote_test_2", "id int primary key , create_TIME timestamp");
        stmt = sharedConnection.createStatement();
        stmt.executeUpdate("insert into quote_test_2 values(1,now())");

        rs = stmt.executeQuery("select * from quote_test_2");
        assertTrue(rs.next());
        assertEquals("`", sharedConnection.getMetaData().getIdentifierQuoteString());
        meta = rs.getMetaData();
        Assert.assertEquals("create_TIME", meta.getColumnName(2));
        Assert.assertEquals("create_TIME", meta.getColumnLabel(2));
        Assert.assertEquals("", meta.getSchemaName(2));
        Assert.assertEquals("test", meta.getCatalogName(2));
        Assert.assertEquals("quote_test_2", meta.getTableName(2));

        rs = stmt.executeQuery("select ID, CREATE_TIME from quote_test_2");
        assertTrue(rs.next());
        meta = rs.getMetaData();
        Assert.assertEquals("create_TIME", meta.getColumnName(2));//CREATE_TIME
        Assert.assertEquals("CREATE_TIME", meta.getColumnLabel(2));
        Assert.assertEquals("", meta.getSchemaName(2));
        Assert.assertEquals("test", meta.getCatalogName(2));
        Assert.assertEquals("quote_test_2", meta.getTableName(2));

        rs = stmt.executeQuery("select id, CREATE_TIME as create_time from quote_test_2");
        assertTrue(rs.next());
        meta = rs.getMetaData();
        Assert.assertEquals("create_TIME", meta.getColumnName(2));//create_time
        Assert.assertEquals("create_time", meta.getColumnLabel(2));
        Assert.assertEquals("", meta.getSchemaName(2));
        Assert.assertEquals("test", meta.getCatalogName(2));
        Assert.assertEquals("quote_test_2", meta.getTableName(2));
    }

    @Test
    public void getSQLKeywordsTest() throws Exception {
        Statement stmt = null;
        ResultSet rs = null;
        stmt = sharedConnection.createStatement();
        assertEquals(
                "ACCESSIBLE,ANALYZE,ASENSITIVE,BEFORE,BIGINT,BINARY,BLOB,CALL,CHANGE,CONDITION,DATABASE,DATABASES,"
                        + "DAY_HOUR,DAY_MICROSECOND,DAY_MINUTE,DAY_SECOND,DELAYED,DETERMINISTIC,DISTINCTROW,DIV,DUAL,EACH,"
                        + "ELSEIF,ENCLOSED,ESCAPED,EXIT,EXPLAIN,FLOAT4,FLOAT8,FORCE,FULLTEXT,GENERAL,HIGH_PRIORITY,"
                        + "HOUR_MICROSECOND,HOUR_MINUTE,HOUR_SECOND,IF,IGNORE,IGNORE_SERVER_IDS,INDEX,INFILE,INOUT,INT1,INT2,"
                        + "INT3,INT4,INT8,ITERATE,KEY,KEYS,KILL,LEAVE,LIMIT,LINEAR,LINES,LOAD,LOCALTIME,LOCALTIMESTAMP,LOCK,"
                        + "LONG,LONGBLOB,LONGTEXT,LOOP,LOW_PRIORITY,MASTER_HEARTBEAT_PERIOD,MASTER_SSL_VERIFY_SERVER_CERT,"
                        + "MAXVALUE,MEDIUMBLOB,MEDIUMINT,MEDIUMTEXT,MIDDLEINT,MINUTE_MICROSECOND,MINUTE_SECOND,MOD,MODIFIES,"
                        + "NO_WRITE_TO_BINLOG,OPTIMIZE,OPTIONALLY,OUT,OUTFILE,PURGE,RANGE,READ_WRITE,READS,REGEXP,RELEASE,"
                        + "RENAME,REPEAT,REPLACE,REQUIRE,RESIGNAL,RESTRICT,RETURN,RLIKE,SCHEMAS,SECOND_MICROSECOND,SENSITIVE,"
                        + "SEPARATOR,SHOW,SIGNAL,SLOW,SPATIAL,SPECIFIC,SQL_BIG_RESULT,SQL_CALC_FOUND_ROWS,SQL_SMALL_RESULT,"
                        + "SQLEXCEPTION,SSL,STARTING,STRAIGHT_JOIN,TERMINATED,TINYBLOB,TINYINT,TINYTEXT,TRIGGER,UNDO,UNLOCK,"
                        + "UNSIGNED,USE,UTC_DATE,UTC_TIME,UTC_TIMESTAMP,VARBINARY,VARCHARACTER,WHILE,XOR,YEAR_MONTH,ZEROFILL",
                sharedConnection.getMetaData().getSQLKeywords());
    }

    @Test
    public void getNumericFunctionsTest() throws Exception {
        Statement stmt = null;
        ResultSet rs = null;
        stmt = sharedConnection.createStatement();
        assertEquals(
                "DIV,ABS,ACOS,ASIN,ATAN,ATAN2,CEIL,CEILING,CONV,COS,COT,CRC32,DEGREES,EXP,FLOOR,GREATEST,LEAST,LN,LOG,"
                        + "LOG10,LOG2,MOD,OCT,PI,POW,POWER,RADIANS,RAND,ROUND,SIGN,SIN,SQRT,TAN,TRUNCATE",
                sharedConnection.getMetaData().getNumericFunctions());
    }

    @Test
    public void getStringFunctionsTest() throws Exception {
        Statement stmt = null;
        ResultSet rs = null;
        stmt = sharedConnection.createStatement();
        assertEquals(
                "ASCII,BIN,BIT_LENGTH,CAST,CHARACTER_LENGTH,CHAR_LENGTH,CONCAT,CONCAT_WS,CONVERT,ELT,EXPORT_SET,"
                        + "EXTRACTVALUE,FIELD,FIND_IN_SET,FORMAT,FROM_BASE64,HEX,INSTR,LCASE,LEFT,LENGTH,LIKE,LOAD_FILE,LOCATE,"
                        + "LOWER,LPAD,LTRIM,MAKE_SET,MATCH AGAINST,MID,NOT LIKE,NOT REGEXP,OCTET_LENGTH,ORD,POSITION,QUOTE,"
                        + "REPEAT,REPLACE,REVERSE,RIGHT,RPAD,RTRIM,SOUNDEX,SOUNDS LIKE,SPACE,STRCMP,SUBSTR,SUBSTRING,"
                        + "SUBSTRING_INDEX,TO_BASE64,TRIM,UCASE,UNHEX,UPDATEXML,UPPER,WEIGHT_STRING",
                sharedConnection.getMetaData().getStringFunctions());
    }

    @Test
    public void getSystemFunctionsTest() throws Exception {
        Statement stmt = null;
        ResultSet rs = null;
        stmt = sharedConnection.createStatement();
        assertEquals("DATABASE,USER,SYSTEM_USER,SESSION_USER,LAST_INSERT_ID,VERSION",
                sharedConnection.getMetaData().getSystemFunctions());
    }

    @Test
    public void getTimeDateFunctionsTest() throws Exception {
        Statement stmt = null;
        ResultSet rs = null;
        stmt = sharedConnection.createStatement();
        assertEquals(
                "ADDDATE,ADDTIME,CONVERT_TZ,CURDATE,CURRENT_DATE,CURRENT_TIME,CURRENT_TIMESTAMP,CURTIME,DATEDIFF,"
                        + "DATE_ADD,DATE_FORMAT,DATE_SUB,DAY,DAYNAME,DAYOFMONTH,DAYOFWEEK,DAYOFYEAR,EXTRACT,FROM_DAYS,"
                        + "FROM_UNIXTIME,GET_FORMAT,HOUR,LAST_DAY,LOCALTIME,LOCALTIMESTAMP,MAKEDATE,MAKETIME,MICROSECOND,"
                        + "MINUTE,MONTH,MONTHNAME,NOW,PERIOD_ADD,PERIOD_DIFF,QUARTER,SECOND,SEC_TO_TIME,STR_TO_DATE,SUBDATE,"
                        + "SUBTIME,SYSDATE,TIMEDIFF,TIMESTAMPADD,TIMESTAMPDIFF,TIME_FORMAT,TIME_TO_SEC,TO_DAYS,TO_SECONDS,"
                        + "UNIX_TIMESTAMP,UTC_DATE,UTC_TIME,UTC_TIMESTAMP,WEEK,WEEKDAY,WEEKOFYEAR,YEAR,YEARWEEK",
                sharedConnection.getMetaData().getTimeDateFunctions());
    }

    @Test
    public void getGeneratedKeysTest() {
        try {
            ResultSet rs;
            ResultSetMetaData rsmd;
            Statement stmt = sharedConnection.createStatement();
            try {
                stmt.execute("drop table testupdate");
            } catch (SQLException e) {
                //            e.printStackTrace();
            }
            stmt.execute("create table testupdate(c1 int AUTO_INCREMENT,c2 int)");
            stmt.execute("insert into testupdate values(null,20)", Statement.RETURN_GENERATED_KEYS);
            rs = stmt.getGeneratedKeys();
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getInt(1));
            rsmd = rs.getMetaData();
            Assert.assertEquals("BIGINT", rsmd.getColumnTypeName(1));
            Assert.assertEquals(-5, rsmd.getColumnType(1));

            stmt.execute("insert into testupdate values(null,20)", Statement.RETURN_GENERATED_KEYS);
            rs = stmt.getGeneratedKeys();
            Assert.assertTrue(rs.next());
            rsmd = rs.getMetaData();
            Assert.assertEquals(2, rs.getInt(1));
            Assert.assertEquals("BIGINT", rsmd.getColumnTypeName(1));
            Assert.assertEquals(-5, rsmd.getColumnType(1));
            stmt.execute("update testupdate set c2 = 20  where c1 = 1",
                    Statement.RETURN_GENERATED_KEYS);
            rs = stmt.getGeneratedKeys();
            rsmd = rs.getMetaData();
            Assert.assertFalse(rs.next());
            Assert.assertEquals("BIGINT", rsmd.getColumnTypeName(1));
            Assert.assertEquals(-5, rsmd.getColumnType(1));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }

    }

    @Test
    public void getTablesCommentTest() {
        try {
            Connection conn = setConnection("&useInformationSchema=true");
            Statement stmt = conn.createStatement();
            try {
                stmt.execute("drop table yxy_t11");
            } catch (Exception ignore) {

            } finally {
                stmt.execute("create table yxy_t11(c1 int)COMMENT 'test表'");
            }

            ResultSet tables = conn.getMetaData().getTables(null, "test", "yxy_t11", null);

            Assert.assertTrue(tables.next());
            Assert.assertEquals("test", tables.getString(1));//TABLE_CAT
            Assert.assertEquals(null, tables.getString(2));//TABLE_SCHEM
            Assert.assertEquals("yxy_t11", tables.getString(3));//TABLE_NAME
            Assert.assertEquals("TABLE", tables.getString(4));//TABLE_TYPE
            Assert.assertEquals("test表", tables.getString(5));//REMARKS
            Assert.assertEquals(null, tables.getString(6));//TYPE_CAT
            Assert.assertEquals(null, tables.getString(7));//TYPE_SCHEM
            Assert.assertEquals(null, tables.getString(8));//TYPE_NAME
            Assert.assertEquals(null, tables.getString(9));//SELF_REFERENCING_COL_NAME
            Assert.assertEquals(null, tables.getString(10));//REF_GENERATION
            Assert.assertFalse(tables.next());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void fix51587128() throws Exception {
        createProcedure("testPro0", "(x int, out y int)\nBEGIN\nSELECT 1;end\n");

        ResultSet rs = sharedConnection.getMetaData().getProcedureColumns(null, null, "testPro0%", "%");

        assertTrue(rs.next());
        assertEquals("testPro0", rs.getString(3));
        assertEquals("x", rs.getString(4));

        assertTrue(rs.next());
        assertEquals("testPro0", rs.getString(3));
        assertEquals("y", rs.getString(4));

        assertTrue(!rs.next());
    }
    @Test
    public void fix51587437() throws Exception {
        createProcedure("testPro1", "()\nBEGIN\nSELECT 1;\nEND");
        Connection conn = sharedConnection;
        StringBuilder queryBuf = new StringBuilder("{call ");
        String quotedId = conn.getMetaData().getIdentifierQuoteString();
        queryBuf.append(quotedId);
        queryBuf.append(conn.getCatalog());
        queryBuf.append(quotedId);
        queryBuf.append(".testPro1()}");
        Assert.assertEquals("{call `test`.testPro1()}", queryBuf.toString());
        conn.prepareCall(queryBuf.toString()).execute();
    }
}
