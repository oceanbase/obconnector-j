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

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.*;

import org.junit.*;

public class DatatypeTest extends BaseTest {

    private ResultSet resultSet;

    /**
     * Initialisation.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("Driverstreamtest", "id int not null primary key, strm text");
        createTable("Driverstreamtest2", "id int primary key not null, strm text");
        createTable("objecttest", "int_test int primary key not null, string_test varchar(30), "
                                  + "timestamp_test timestamp, serial_test blob");
        createTable("bintest",
            "id int not null primary key auto_increment, bin1 varbinary(300), bin2 varbinary(300)");
        createTable("bigdectest", "id int not null primary key auto_increment, bd decimal",
            "engine=innodb");
        createTable("bytetest", "id int not null primary key auto_increment, a int",
            "engine=innodb");
        createTable("shorttest", "id int not null primary key auto_increment,a int",
            "engine=innodb");
        createTable("doubletest", "id int not null primary key auto_increment,a double",
            "engine=innodb");
        createTable("bittest", "id int not null primary key auto_increment, b int");
        createTable("emptytest", "id int");
        createTable("test_setobjectconv",
            "id int not null primary key auto_increment, v1 varchar(40), v2 varchar(40)");
        createTable("blabla", "valsue varchar(20)");
        createTable("TestBigIntType", "t1 bigint(20), t2 bigint(20), t3 bigint(20), t4 bigint(20)");
        createTable("time_period",
            "ID int unsigned NOT NULL, START time NOT NULL, END time NOT NULL, PRIMARY KEY (ID)");
        createTable("bitBoolTest", "d1 BOOLEAN, d2 BIT");
    }

    /**
     * Get a simple Statement or a PrepareStatement.
     *
     * @param query    query
     * @param prepared flag must be a prepare statement
     * @return a statement
     * @throws SQLException exception
     */
    public static ResultSet getResultSet(String query, boolean prepared) throws SQLException {
        return getResultSet(query, prepared, sharedConnection);
    }

    /**
     * Get a simple Statement or a PrepareStatement.
     *
     * @param query      query
     * @param prepared   flag must be a prepare statement
     * @param connection the connection to use
     * @return a statement
     * @throws SQLException exception
     */
    public static ResultSet getResultSet(String query, boolean prepared, Connection connection)
                                                                                               throws SQLException {
        if (prepared) {
            PreparedStatement preparedStatement = connection.prepareStatement(query
                                                                              + " WHERE 1 = ?");
            preparedStatement.setInt(1, 1);
            return preparedStatement.executeQuery();
        } else {
            return connection.createStatement().executeQuery(query);
        }
    }

    @Before
    public void checkSupported() throws Exception {
        requireMinimumVersion(5, 0);
    }

    private void checkClass(String column, Class<?> clazz, String mysqlType, int javaSqlType)
                                                                                             throws Exception {
        int index = resultSet.findColumn(column);
        Object obj = resultSet.getObject(column);
        if (obj != null) {
            if (!clazz.equals(obj.getClass())) {
                System.out.println("test");
            }
            assertEquals("Unexpected class for column " + column, clazz, resultSet
                .getObject(column).getClass());
        }
        assertEquals("Unexpected class name for column " + column, clazz.getName(), resultSet
            .getMetaData().getColumnClassName(index));
        //        assertEquals("Unexpected MySQL type for column " + column, mysqlType, resultSet
        //            .getMetaData().getColumnTypeName(index));
        assertEquals("Unexpected java sql type for column " + column, javaSqlType, resultSet
            .getMetaData().getColumnType(index));
    }

    private void createDataTypeTables() throws SQLException {
        createTable(
            "datatypetest",
            "bit1 BIT(1) default 0,"
                    + "bit2 BIT(2) default 1,"
                    + "tinyint1 TINYINT(1) default 0,"
                    + "tinyint2 TINYINT(2) default 1,"
                    + "bool0 BOOL default 0,"
                    + "smallint0 SMALLINT default 1,"
                    + "smallint_unsigned SMALLINT UNSIGNED default 0,"
                    + "mediumint0 MEDIUMINT default 1,"
                    + "mediumint_unsigned MEDIUMINT UNSIGNED default 0,"
                    + "int0 INT default 1,"
                    + "int_unsigned INT UNSIGNED default 0,"
                    + "bigint0 BIGINT default 1,"
                    + "bigint_unsigned BIGINT UNSIGNED default 0,"
                    + "float0 FLOAT default 0,"
                    + "double0 DOUBLE default 1,"
                    + "decimal0 DECIMAL default 0,"
                    + "date0 DATE default '2001-01-01',"
                    + "datetime0 DATETIME default '2001-01-01 00:00:00',"
                    + "timestamp0 TIMESTAMP default  '2001-01-01 00:00:00',"
                    + "time0 TIME default '22:11:00',"
                    + ((minVersion(5, 6) && strictBeforeVersion(10, 0)) ? "year2 YEAR(4) default 99,"
                        : "year2 YEAR(2) default 99,") + "year4 YEAR(4) default 2011,"
                    + "char0 CHAR(1) default '0'," + "char_binary CHAR (1) binary default '0',"
                    + "varchar0 VARCHAR(1) default '1',"
                    + "varchar_binary VARCHAR(10) BINARY default 0x1,"
                    + "binary0 BINARY(10) default 0x1," + "varbinary0 VARBINARY(10) default 0x1,"
                    + "tinyblob0 TINYBLOB," + "tinytext0 TINYTEXT," + "blob0 BLOB," + "text0 TEXT,"
                    + "mediumblob0 MEDIUMBLOB," + "mediumtext0 MEDIUMTEXT," + "longblob0 LONGBLOB,"
                    + "longtext0 LONGTEXT ");
        //                    + "enum0 ENUM('a','b') default 'a',"
        //                    + "set0 SET('a','b') default 'a' ");
    }

    /**
     * Testing different date parameters.
     *
     * @param connection     current connection
     * @param tinyInt1isBit  tiny bit must be consider as boolean
     * @param yearIsDateType year must be consider as Date or int
     * @throws Exception exception
     */
    public void datatypes(Connection connection, boolean tinyInt1isBit, boolean yearIsDateType)
                                                                                               throws Exception {

        createDataTypeTables();

        connection
            .createStatement()
            .execute(
                "insert into datatypetest (tinyblob0,mediumblob0,blob0,longblob0,"
                        + "tinytext0,mediumtext0,text0, longtext0) values(0x1,0x1,0x1,0x1, 'a', 'a', 'a', 'a')");

        resultSet = connection.createStatement().executeQuery("select * from datatypetest");
        resultSet.next();

        Class<?> byteArrayClass = (new byte[0]).getClass();

        checkClass("bit1", Boolean.class, "BIT", Types.BIT);
        checkClass("bit2", byteArrayClass, "BIT", Types.VARBINARY);

        checkClass("tinyint1", tinyInt1isBit ? Boolean.class : Integer.class, "TINYINT",
            tinyInt1isBit ? Types.BIT : Types.TINYINT);
        checkClass("tinyint2", Integer.class, "TINYINT", Types.TINYINT);
        checkClass("bool0", tinyInt1isBit ? Boolean.class : Integer.class, "TINYINT",
            tinyInt1isBit ? Types.BIT : Types.TINYINT);
        checkClass("smallint0", Integer.class, "SMALLINT", Types.SMALLINT);
        checkClass("smallint_unsigned", Integer.class, "SMALLINT UNSIGNED", Types.SMALLINT);
        checkClass("mediumint0", Integer.class, "MEDIUMINT", Types.INTEGER);
        checkClass("mediumint_unsigned", Integer.class, "MEDIUMINT UNSIGNED", Types.INTEGER);
        checkClass("int0", Integer.class, "INTEGER", Types.INTEGER);
        checkClass("int_unsigned", Long.class, "INTEGER UNSIGNED", Types.INTEGER);
        checkClass("bigint0", Long.class, "BIGINT", Types.BIGINT);
        checkClass("bigint_unsigned", BigInteger.class, "BIGINT UNSIGNED", Types.BIGINT);
        checkClass("float0", Float.class, "FLOAT", Types.REAL);
        checkClass("double0", Double.class, "DOUBLE", Types.DOUBLE);
        checkClass("decimal0", BigDecimal.class, "DECIMAL", Types.DECIMAL);
        checkClass("date0", Date.class, "DATE", Types.DATE);
        checkClass("time0", Time.class, "TIME", Types.TIME);
        checkClass("timestamp0", Timestamp.class, "TIMESTAMP", Types.TIMESTAMP);

        if (minVersion(5, 6) && strictBeforeVersion(10, 0)) {
            // MySQL deprecated YEAR(2) since 5.6
            checkClass("year2", yearIsDateType ? Date.class : Short.class, "YEAR",
                yearIsDateType ? Types.DATE : Types.SMALLINT);
        }
        checkClass("year4", yearIsDateType ? Date.class : Short.class, "YEAR",
            yearIsDateType ? Types.DATE : Types.SMALLINT);
        checkClass("char0", String.class, "CHAR", Types.CHAR);
        checkClass("char_binary", String.class, "CHAR", Types.CHAR);
        checkClass("varchar0", String.class, "VARCHAR", Types.VARCHAR);
        checkClass("varchar_binary", String.class, "VARCHAR", Types.VARCHAR);
        checkClass("binary0", byteArrayClass, "BINARY", Types.BINARY);
        checkClass("varbinary0", byteArrayClass, "VARBINARY", Types.VARBINARY);
        checkClass("tinyblob0", byteArrayClass, "TINYBLOB", Types.VARBINARY);
        checkClass("tinytext0", String.class, "VARCHAR", Types.LONGVARCHAR);
        checkClass("blob0", byteArrayClass, "BLOB", Types.LONGVARBINARY);
        checkClass("text0", String.class, "VARCHAR", Types.LONGVARCHAR);
        checkClass("mediumblob0", byteArrayClass, "MEDIUMBLOB", Types.LONGVARBINARY);
        checkClass("mediumtext0", String.class, "VARCHAR", Types.LONGVARCHAR);
        checkClass("longblob0", byteArrayClass, "LONGBLOB", Types.LONGVARBINARY);
        checkClass("longtext0", String.class, "VARCHAR", Types.LONGVARCHAR);
        //        checkClass("enum0", String.class, "CHAR", Types.CHAR);
        //        checkClass("set0", String.class, "CHAR", Types.CHAR);

        resultSet = connection.createStatement().executeQuery("select NULL as foo");
        resultSet.next();
        checkClass("foo", String.class, "NULL", Types.NULL);
    }

    @Test
    public void datatypes1() throws Exception {
        datatypes(sharedConnection, true, true);
    }

    @Test
    public void datatypes2() throws Exception {
        try (Connection connection = setConnection("&tinyInt1isBit=0&yearIsDateType=0")) {
            datatypes(connection, false, false);
        }
    }

    @Test
    public void datatypes3() throws Exception {
        try (Connection connection = setConnection("&tinyInt1isBit=1&yearIsDateType=0")) {
            datatypes(connection, true, false);
        }
    }

    @Test
    public void datatypes4() throws Exception {
        try (Connection connection = setConnection("&tinyInt1isBit=0&yearIsDateType=1")) {
            datatypes(connection, false, true);
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testCharacterStreams() throws SQLException, IOException {
        Assume.assumeFalse(sharedUsePrepare()); //COM_STMT_SEND_LONG_DATA  is temporarily not supported, ps cannot be used
        PreparedStatement stmt = sharedConnection
            .prepareStatement("insert into Driverstreamtest (id, strm) values (?,?)");
        stmt.setInt(1, 2);
        String toInsert = "abcdefgh\njklmn\"";
        Reader reader = new StringReader(toInsert);
        stmt.setCharacterStream(2, reader);
        stmt.execute();
        ResultSet rs = sharedConnection.createStatement().executeQuery(
            "select * from Driverstreamtest");
        assertTrue(rs.next());
        Reader rdr = rs.getCharacterStream("strm");
        StringBuilder sb = new StringBuilder();
        int ch;
        while ((ch = rdr.read()) != -1) {
            sb.append((char) ch);
        }
        assertEquals(sb.toString(), (toInsert));
        rdr = rs.getCharacterStream(2);
        sb = new StringBuilder();

        while ((ch = rdr.read()) != -1) {
            sb.append((char) ch);
        }
        assertEquals(sb.toString(), (toInsert));
        InputStream is = rs.getAsciiStream("strm");
        sb = new StringBuilder();

        while ((ch = is.read()) != -1) {
            sb.append((char) ch);
        }
        assertEquals(sb.toString(), (toInsert));
        is = rs.getUnicodeStream("strm");
        sb = new StringBuilder();

        while ((ch = is.read()) != -1) {
            sb.append((char) ch);
        }
        assertEquals(sb.toString(), (toInsert));
        assertEquals(sb.toString(), rs.getString("strm"));
    }

    @Test
    public void testCharacterStreamWithLength() throws SQLException, IOException {
        Assume.assumeFalse(sharedUsePrepare()); //COM_STMT_SEND_LONG_DATA  is temporarily not supported, ps cannot be used
        PreparedStatement stmt = sharedConnection
            .prepareStatement("insert into Driverstreamtest2 (id, strm) values (?,?)");
        stmt.setInt(1, 2);
        String toInsert = "abcdefgh\njklmn\"";
        Reader reader = new StringReader(toInsert);
        stmt.setCharacterStream(2, reader, 5);
        stmt.execute();
        testCharacterStreamWithLength(getResultSet("select * from Driverstreamtest2", false),
            toInsert);
        testCharacterStreamWithLength(getResultSet("select * from Driverstreamtest2", true),
            toInsert);
    }

    private void testCharacterStreamWithLength(ResultSet rs, String toInsert) throws SQLException,
                                                                             IOException {
        assertTrue(rs.next());
        Reader rdr = rs.getCharacterStream("strm");
        StringBuilder sb = new StringBuilder();
        int ch;
        while ((ch = rdr.read()) != -1) {
            sb.append((char) ch);
        }
        assertEquals(sb.toString(), toInsert.substring(0, 5));
        assertEquals(rs.getString("strm"), toInsert.substring(0, 5));
    }

    @Test
    public void testEmptyResultSet() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        assertEquals(true, stmt.execute("SELECT * FROM emptytest"));
        assertEquals(false, stmt.getResultSet().next());
    }

    @Test
    public void testLongColName() throws SQLException {
        DatabaseMetaData dbmd = sharedConnection.getMetaData();
        StringBuilder str = new StringBuilder();
        for (int i = 0; i < dbmd.getMaxColumnNameLength(); i++) {
            str.append("x");
        }
        createTable("longcol", str + " int not null primary key");
        sharedConnection.createStatement().execute("insert into longcol values (1)");

        try (ResultSet rs = getResultSet("select * from longcol", false)) {
            assertEquals(true, rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(1, rs.getInt(str.toString()));
            assertEquals("1", rs.getString(1));
        }

        try (ResultSet rs = getResultSet("select * from longcol", true)) {
            assertEquals(true, rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(1, rs.getInt(str.toString()));
            assertEquals("1", rs.getString(1));
        }
    }

    @Test(expected = SQLException.class)
    public void testBadParamlist() throws SQLException {
        PreparedStatement ps = sharedConnection.prepareStatement("insert into blah values (?)");
        ps.execute();
    }

    /**
     * CONJ-299 - PreparedStatement.setObject(Type.BIT, "1") should register as true.
     */
    @Test
    public void setBitBoolObjectTest() throws SQLException {
        PreparedStatement ps =
                sharedConnection.prepareStatement("insert into bitBoolTest values (?,?)");
        ps.setObject(1, 0);
        ps.setObject(2, 0);
        ps.addBatch();

        ps.setObject(1, 1);
        ps.setObject(2, 1);
        ps.addBatch();

        ps.setObject(1, "0", Types.BOOLEAN);
        ps.setObject(2, "0", Types.BIT);
        ps.addBatch();

        ps.setObject(1, "1", Types.BOOLEAN);
        ps.setObject(2, "1", Types.BIT);
        ps.addBatch();

        ps.setObject(1, "true", Types.BOOLEAN);
        ps.setObject(2, "true", Types.BIT);
        ps.addBatch();

        ps.setObject(1, "truee", Types.BOOLEAN);
        ps.setObject(2, "truee", Types.BIT);
        ps.addBatch();

        ps.setObject(1, "false", Types.BOOLEAN);
        ps.setObject(2, "false", Types.BIT);
        ps.addBatch();

        ps.executeBatch();

        try (ResultSet rs =
                     sharedConnection.createStatement().executeQuery("select * from bitBoolTest")) {
            assertValue(rs, false);
            assertValue(rs, true);
            assertValue(rs, false);
            assertValue(rs, true);
            assertValue(rs, true);
            assertValue(rs, true);
            assertValue(rs, false);
            assertFalse(rs.next());
        }
    }

    private void assertValue(ResultSet rs, boolean bool) throws SQLException {
        assertTrue(rs.next());
        if (bool) {
            assertTrue(rs.getBoolean(1));
            assertTrue(rs.getBoolean(2));
        } else {
            assertFalse(rs.getBoolean(1));
            assertFalse(rs.getBoolean(2));
        }
    }

    @Test
    public void setObjectTest() throws SQLException, IOException, ClassNotFoundException {
        PreparedStatement ps =
                sharedConnection.prepareStatement("insert into objecttest values (?,?,?,?)");
        ps.setObject(1, 5);
        ps.setObject(2, "aaa");
        ps.setObject(3, Timestamp.valueOf("2009-01-17 15:41:01"));
        ps.setObject(4, new SerializableClass("testing", 8));
        ps.execute();

        try (ResultSet rs = getResultSet("select * from objecttest", false)) {
            if (rs.next()) {
                setObjectTestResult(rs);
            } else {
                fail();
            }
        }
        try (ResultSet rs = getResultSet("select * from objecttest", true)) {
            if (rs.next()) {
                setObjectTestResult(rs);
            } else {
                fail();
            }
        }
    }

    private void setObjectTestResult(ResultSet rs) throws SQLException, IOException,
                                                  ClassNotFoundException {
        assertEquals("5", rs.getString(1));
        assertEquals("aaa", rs.getString(2));
        assertEquals("2009-01-17 15:41:01.0", rs.getString(3));

        Object theInt = rs.getObject(1);
        assertTrue(theInt instanceof Integer);
        Object theInt2 = rs.getObject("int_test");
        assertTrue(theInt2 instanceof Integer);
        Object theString = rs.getObject(2);
        assertTrue(theString instanceof String);
        Object theTimestamp = rs.getObject(3);
        assertTrue(theTimestamp instanceof Timestamp);
        Object theBlob = rs.getObject(4);
        assertNotNull(theBlob);

        byte[] rawBytes = rs.getBytes(4);
        ByteArrayInputStream bais = new ByteArrayInputStream(rawBytes);
        ObjectInputStream ois = new ObjectInputStream(bais);
        SerializableClass sc = (SerializableClass) ois.readObject();

        assertEquals(sc.getVal(), "testing");
        assertEquals(sc.getVal2(), 8);
        rawBytes = rs.getBytes("serial_test");
        bais = new ByteArrayInputStream(rawBytes);
        ois = new ObjectInputStream(bais);
        sc = (SerializableClass) ois.readObject();

        assertEquals(sc.getVal(), "testing");
        assertEquals(sc.getVal2(), 8);
    }

    @Test
    public void setObjectBitInt() throws SQLException {

        PreparedStatement preparedStatement =
                sharedConnection.prepareStatement(
                        "INSERT INTO TestBigIntType " + "(t1, t2, t3, t4) VALUES (?, ?, ?, ?)");

        final long valueLong = System.currentTimeMillis();
        final String maxValue = String.valueOf(Long.MAX_VALUE);

        preparedStatement.setObject(1, valueLong, Types.BIGINT);
        preparedStatement.setObject(2, maxValue, Types.BIGINT);
        preparedStatement.setObject(3, valueLong);
        preparedStatement.setObject(4, maxValue);
        preparedStatement.executeUpdate();

        try (ResultSet rs = getResultSet("select * from TestBigIntType", false)) {
            validateResultBigIntType(valueLong, maxValue, rs);
        }

        try (ResultSet rs = getResultSet("select * from TestBigIntType", true)) {
            validateResultBigIntType(valueLong, maxValue, rs);
        }
    }

    private void validateResultBigIntType(long valueLong, String maxValue, ResultSet resultSet)
                                                                                               throws SQLException {
        if (resultSet.next()) {
            assertEquals(resultSet.getLong(1), valueLong);
            assertEquals(resultSet.getLong(2), Long.parseLong(maxValue));
            assertEquals(resultSet.getLong(3), valueLong);
            assertEquals(resultSet.getLong(4), Long.parseLong(maxValue));
            assertEquals(resultSet.getString(1), String.valueOf(valueLong));
            assertEquals(resultSet.getString(2), String.valueOf(Long.parseLong(maxValue)));
            assertEquals(resultSet.getString(3), String.valueOf(valueLong));
            assertEquals(resultSet.getString(4), String.valueOf(Long.parseLong(maxValue)));
        } else {
            fail();
        }
    }

    @Test
    public void binTest() throws SQLException, IOException {
        Assume.assumeFalse(sharedUsePrepare()); //COM_STMT_SEND_LONG_DATA  is temporarily not supported, ps cannot be used
        byte[] allBytes = new byte[256];
        for (int i = 0; i < 256; i++) {
            allBytes[i] = (byte) (i & 0xff);
        }
        ByteArrayInputStream bais = new ByteArrayInputStream(allBytes);
        PreparedStatement ps =
                sharedConnection.prepareStatement("insert into bintest (bin1,bin2) values (?,?)");
        ps.setBytes(1, allBytes);
        ps.setBinaryStream(2, bais);
        ps.execute();

        try (ResultSet rs = getResultSet("select bin1,bin2 from bintest", false)) {
            binTestResult(rs, allBytes);
        }

        try (ResultSet rs = getResultSet("select bin1,bin2 from bintest", true)) {
            binTestResult(rs, allBytes);
        }
    }

    private void binTestResult(ResultSet rs, byte[] allBytes) throws SQLException, IOException {
        if (rs.next()) {
            rs.getBlob(1);
            InputStream is = rs.getBinaryStream(1);

            for (int i = 0; i < 256; i++) {
                int read = is.read();
                assertEquals(i, read);
            }
            assertEquals(rs.getString(1), new String(allBytes, StandardCharsets.UTF_8));

            is = rs.getBinaryStream(2);

            for (int i = 0; i < 256; i++) {
                int read = is.read();
                assertEquals(i, read);
            }
            assertEquals(rs.getString(2), new String(allBytes, StandardCharsets.UTF_8));
        } else {
            fail("Must have result !");
        }
    }

    @Test
    public void binTest2() throws SQLException {
        Assume.assumeFalse(sharedUsePrepare()); //COM_STMT_SEND_LONG_DATA  is temporarily not supported, ps cannot be used
        createTable("bintest2", "bin1 longblob", "engine=innodb");

        byte[] buf = new byte[1000000];
        for (int i = 0; i < 1000000; i++) {
            buf[i] = (byte) i;
        }
        InputStream is = new ByteArrayInputStream(buf);
        try (Connection connection = setConnection()) {
            PreparedStatement ps = connection.prepareStatement("insert into bintest2 (bin1) values (?)");
            ps.setBinaryStream(1, is);
            ps.execute();
            ps = connection.prepareStatement("insert into bintest2 (bin1) values (?)");
            is = new ByteArrayInputStream(buf);
            ps.setBinaryStream(1, is);
            ps.execute();

            try (ResultSet rs = getResultSet("select bin1 from bintest2", false)) {
                binTest2Result(rs, buf);
            }

            try (ResultSet rs = getResultSet("select bin1 from bintest2", true)) {
                binTest2Result(rs, buf);
            }
        }
    }

    @Test
    public void binTest3() throws SQLException {
        Assume.assumeFalse(sharedUsePrepare()); //COM_STMT_SEND_LONG_DATA  is temporarily not supported, ps cannot be used
        byte[] buf = new byte[1000000];
        for (int i = 0; i < 1000000; i++) {
            buf[i] = (byte) i;
        }
        InputStream is = new ByteArrayInputStream(buf);

        try (Connection connection = setConnection()) {
            createTable("bintest3", "bin1 longblob", "engine=innodb");
            Statement stmt = connection.createStatement();

            try (PreparedStatement ps =
                         connection.prepareStatement("insert into bintest3 (bin1) values (?)")) {
                ps.setBinaryStream(1, is);
                ps.execute();
            }

            ResultSet rs = stmt.executeQuery("select bin1 from bintest3");
            assertTrue(rs.next());
            byte[] buf2 = rs.getBytes(1);
            assertEquals(1000000, buf2.length);
            for (int i = 0; i < 1000000; i++) {
                assertEquals(buf[i], buf2[i]);
            }
        }
    }

    private void binTest2Result(ResultSet rs, byte[] buf) throws SQLException {
        if (rs.next()) {
            byte[] buf2 = rs.getBytes(1);
            for (int i = 0; i < 1000000; i++) {
                assertEquals((byte) i, buf2[i]);
            }
            assertEquals(rs.getString(1), new String(buf, StandardCharsets.UTF_8));

            if (rs.next()) {
                buf2 = rs.getBytes(1);
                for (int i = 0; i < 1000000; i++) {
                    assertEquals((byte) i, buf2[i]);
                }
                assertEquals(rs.getString(1), new String(buf, StandardCharsets.UTF_8));
                if (rs.next()) {
                    fail();
                }
            } else {
                fail();
            }
        } else {
            fail();
        }
    }

    @Test
    public void bigDecimalTest() throws SQLException {
        requireMinimumVersion(5, 0);
        BigDecimal bd = BigDecimal.TEN;
        PreparedStatement ps =
                sharedConnection.prepareStatement("insert into bigdectest (bd) values (?)");
        ps.setBigDecimal(1, bd);
        ps.execute();

        try (ResultSet rs = getResultSet("select bd from bigdectest", false)) {
            bigDecimalTestResult(rs, bd);
        }
        try (ResultSet rs = getResultSet("select bd from bigdectest", true)) {
            bigDecimalTestResult(rs, bd);
        }
    }

    private void bigDecimalTestResult(ResultSet rs, BigDecimal bd) throws SQLException {
        if (rs.next()) {
            Object bb = rs.getObject(1);
            assertEquals(bd, bb);
            BigDecimal bigD = rs.getBigDecimal(1);
            BigDecimal bigD2 = rs.getBigDecimal("bd");
            assertEquals(bd, bigD);
            assertEquals(bd, bigD2);
            assertEquals(rs.getString(1), "10");
            bigD = rs.getBigDecimal("bd");
            assertEquals(bd, bigD);
        } else {
            fail("Must have resultset");
        }
    }

    @Test
    public void byteTest() throws SQLException {
        PreparedStatement ps = sharedConnection.prepareStatement("insert into bytetest (a) values (?)");
        ps.setByte(1, Byte.MAX_VALUE);
        assertFalse(ps.execute());
        try (ResultSet rs = getResultSet("select a from bytetest", false)) {
            byteTestResult(rs);
        }

        try (ResultSet rs = getResultSet("select a from bytetest", true)) {
            byteTestResult(rs);
        }
    }

    private void byteTestResult(ResultSet rs) throws SQLException {
        if (rs.next()) {
            Byte bc = rs.getByte(1);
            Byte bc2 = rs.getByte("a");

            assertTrue(Byte.MAX_VALUE == bc);
            assertEquals(bc2, bc);
            assertEquals(rs.getString(1), "127");
        } else {
            fail();
        }
    }

    @Test
    public void shortTest() throws SQLException {
        PreparedStatement ps =
                sharedConnection.prepareStatement("insert into shorttest (a) values (?)");
        ps.setShort(1, Short.MAX_VALUE);
        ps.execute();
        try (ResultSet rs = getResultSet("select a from shorttest", false)) {
            shortTestResult(rs);
        }
        try (ResultSet rs = getResultSet("select a from shorttest", true)) {
            shortTestResult(rs);
        }
    }

    private void shortTestResult(ResultSet rs) throws SQLException {
        if (rs.next()) {
            Short bc = rs.getShort(1);
            Short bc2 = rs.getShort("a");

            assertTrue(Short.MAX_VALUE == bc);
            assertEquals(bc2, bc);
            assertEquals(rs.getString(1), "32767");
        } else {
            fail("must have result !");
        }
    }

    @Test
    public void doubleTest() throws SQLException {
        PreparedStatement ps =
                sharedConnection.prepareStatement("insert into doubletest (a) values (?)");
        double sendDoubleValue = 1.5;
        ps.setDouble(1, sendDoubleValue);
        ps.execute();
        try (ResultSet rs = getResultSet("select a from doubletest", false)) {
            doubleTestResult(rs, sendDoubleValue);
        }
        try (ResultSet rs = getResultSet("select a from doubletest", true)) {
            doubleTestResult(rs, sendDoubleValue);
        }
    }

    private void doubleTestResult(ResultSet rs, Double sendDoubleValue) throws SQLException {
        if (rs.next()) {
            Object returnObject = rs.getObject(1);
            assertEquals(returnObject.getClass(), Double.class);
            Double bc = rs.getDouble(1);
            Double bc2 = rs.getDouble("a");

            assertTrue(sendDoubleValue.doubleValue() == bc);
            assertEquals(bc2, bc);
            assertEquals(rs.getString(1), "1.5");
        } else {
            fail("must have result !");
        }
    }

    @Test
    public void getUrlTest() throws SQLException {
        try (ResultSet rs = getResultSet("select 'http://mariadb.org' as url FROM dual", false)) {
            getUrlTestResult(rs);
        }
        try (ResultSet rs = getResultSet("select 'http://mariadb.org' as url FROM dual", true)) {
            getUrlTestResult(rs);
        }
    }

    private void getUrlTestResult(ResultSet rs) throws SQLException {
        if (rs.next()) {
            URL url = rs.getURL(1);
            assertEquals("http://mariadb.org", url.toString());
            url = rs.getURL("url");
            assertEquals("http://mariadb.org", url.toString());
            assertEquals("http://mariadb.org", rs.getString(1));
        } else {
            fail("must have result");
        }
    }

    @Test
    public void getUrlFailTest() throws SQLException {
        try (ResultSet rs = getResultSet("select 'asdf' as url FROM dual", false)) {
            getUrlFailTestResult(rs);
        }
        try (ResultSet rs = getResultSet("select 'asdf' as url FROM dual", true)) {
            getUrlFailTestResult(rs);
        }
    }

    private void getUrlFailTestResult(ResultSet rs) throws SQLException {
        if (rs.next()) {
            try {
                rs.getURL(1);
                fail("must have thrown error");
            } catch (SQLException e) {
                // expected
            }
            try {
                rs.getURL("url");
                fail("must have thrown error");
            } catch (SQLException e) {
                // expected
            }
        } else {
            fail("must have result !");
        }
    }

    @Test
    public void setNull() throws SQLException {
        PreparedStatement ps = sharedConnection.prepareStatement("insert blabla VALUE (?)");
        ps.setString(1, null);
        ps.executeQuery();
        try (ResultSet rs = getResultSet("select * from blabla", false)) {
            setNullResult(rs);
        }
        try (ResultSet rs = getResultSet("select * from blabla", true)) {
            setNullResult(rs);
        }
    }

    private void setNullResult(ResultSet rs) throws SQLException {
        if (rs.next()) {
            assertNull(rs.getString(1));
            assertNull(rs.getObject(1));
        } else {
            fail("must have result !");
        }
    }

    @Test
    public void testSetObject() throws SQLException {
        PreparedStatement ps = sharedConnection
            .prepareStatement("insert into test_setobjectconv values (null, ?, ?)");
        ps.setObject(1, "2009-01-01 00:00:00", Types.TIMESTAMP);
        ps.setObject(2, "33", Types.DOUBLE);
        ps.execute();
    }

    @Test
    public void testBit() throws SQLException {
        PreparedStatement stmt =
                sharedConnection.prepareStatement("insert into bittest values(null, ?)");
        stmt.setBoolean(1, true);
        stmt.execute();
        stmt.setBoolean(1, false);
        stmt.execute();

        try (ResultSet rs = getResultSet("select * from bittest", false)) {
            testBitResult(rs);
        }
        try (ResultSet rs = getResultSet("select * from bittest", true)) {
            testBitResult(rs);
        }
    }

    private void testBitResult(ResultSet rs) throws SQLException {
        if (rs.next()) {
            assertTrue(rs.getBoolean("b"));
            assertEquals(rs.getString("b"), "1");
            if (rs.next()) {
                assertFalse(rs.getBoolean("b"));
                assertEquals(rs.getString("b"), "0");
            } else {
                fail("must have result");
            }
        } else {
            fail("must have result");
        }
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testNullTimePreparedStatement() throws Exception {
        sharedConnection
                .createStatement()
                .execute("insert into time_period(id, start, end) values(1, '00:00:00', '08:00:00');");
        final String sql = "SELECT id, start, end FROM time_period WHERE id=?";
        PreparedStatement preparedStatement = sharedConnection.prepareStatement(sql);
        preparedStatement.setInt(1, 1);
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
            if (resultSet.next()) {
                Time timeStart = resultSet.getTime(2);
                Time timeEnd = resultSet.getTime(3);
                assertEquals(timeStart, new Time(0, 0, 0));
                assertEquals(timeEnd, new Time(8, 0, 0));
            }
        }
    }

    @Test
    public void longMinValueSpecificity() throws SQLException {
        createTable("longMinValueSpecificity", "ii BIGINT");
        try (Statement statement = sharedConnection.createStatement()) {

            try (PreparedStatement preparedStatement =
                         sharedConnection.prepareStatement("INSERT INTO longMinValueSpecificity values (?)")) {
                preparedStatement.setLong(1, Long.MAX_VALUE);
                preparedStatement.executeQuery();
                preparedStatement.setLong(1, Long.MIN_VALUE);
                preparedStatement.executeQuery();
            }

            try (PreparedStatement preparedStatement =
                         sharedConnection.prepareStatement("SELECT * from longMinValueSpecificity")) {
                ResultSet resultSet = preparedStatement.executeQuery();
                assertTrue(resultSet.next());
                assertEquals(Long.MAX_VALUE, resultSet.getLong(1));
                assertTrue(resultSet.next());
                assertEquals(Long.MIN_VALUE, resultSet.getLong(1));
            }

            ResultSet resultSet = statement.executeQuery("SELECT * from longMinValueSpecificity");
            assertTrue(resultSet.next());
            assertEquals(Long.MAX_VALUE, resultSet.getLong(1));
            assertTrue(resultSet.next());
            assertEquals(Long.MIN_VALUE, resultSet.getLong(1));
        }
    }

    @Ignore
    public void testBinarySetter() throws Throwable {
        createTable("LatinTable", "t1 varchar(30)", "DEFAULT CHARSET=latin1");

        try (Connection connection =
                     DriverManager.getConnection(
                             connU
                                     + "?user="
                                     + username
                                     + ((password != null && !password.isEmpty()) ? "&password=" + password : "")
                                     + ((options.useSsl != null) ? "&useSsl=" + options.useSsl : "")
                                     + ((options.serverSslCert != null) ? "&serverSslCert=" + options.serverSslCert : "")
                                     + "&useServerPrepStmts=true")) {
            checkCharactersInsert(connection);
        }

        Statement stmt = sharedConnection.createStatement();
        assertFalse(stmt.execute("truncate LatinTable"));

        try (Connection connection =
                     DriverManager.getConnection(
                             connU
                                     + "?user="
                                     + username
                                     + (password != null && !"".equals(password) ? "&password=" + password : "")
                                     + ((options.useSsl != null) ? "&useSsl=" + options.useSsl : "")
                                     + ((options.serverSslCert != null) ? "&serverSslCert=" + options.serverSslCert : "")
                                     + "&useServerPrepStmts=false")) {
            checkCharactersInsert(connection);
        }
    }

    private void checkCharactersInsert(Connection connection) throws Throwable {
        try (PreparedStatement preparedStatement =
                     connection.prepareStatement("INSERT INTO LatinTable(t1)  values (?)")) {
            String str = "你好(hello in Chinese)";
            try {
                preparedStatement.setString(1, str);
                preparedStatement.execute();
                fail("must have fail");
            } catch (SQLException sqle) {
                assertTrue(sqle.getMessage().contains("Incorrect string value"));
            }

            try {
                preparedStatement.setBytes(1, str.getBytes(StandardCharsets.UTF_8));
                preparedStatement.execute();
            } catch (SQLException sqle) {
                assertTrue(sqle.getMessage().contains("Incorrect string value"));
            }

            ByteArrayInputStream bais = new ByteArrayInputStream(str.getBytes(StandardCharsets.UTF_8));
            preparedStatement.setBinaryStream(1, bais);
            preparedStatement.execute();
        }
    }

    /**
     * CONJ-535 : BIT data type value with numeric getter.
     *
     * @throws SQLException if any exception occur.
     */
    @Test
    public void testBitValues() throws SQLException {
        createTable("testShortBit", "bitVal BIT(1), bitVal2 BIT(40)");
        Statement stmt = sharedConnection.createStatement();
        stmt.execute(
                "INSERT INTO testShortBit VALUES (0,0), (1,1), (1, b'01010101'), (1, 21845), (1, b'1101010101010101')"
                        + ", (1, b'10000000000000000000000000000000')");

        validResultSetBitValue(stmt.executeQuery("SELECT * FROM testShortBit"));

        try (PreparedStatement preparedStatement =
                     sharedConnection.prepareStatement("SELECT * FROM testShortBit")) {
            validResultSetBitValue(preparedStatement.executeQuery());
        }
    }

    /**
     * CONJ-650 : NegativeArraySizeException while executing getObject(columnName, byte[].class) with
     * null value
     *
     * @throws SQLException if any exception occur.
     */
    @Test
    public void testNullGetObject() throws SQLException {
        createTable("testTextNullValue", "id int, val text");
        Statement stmt = sharedConnection.createStatement();
        stmt.execute("INSERT INTO testTextNullValue VALUES (1, 'abc'), (2, null)");
        ResultSet rs = stmt.executeQuery("SELECT * FROM testTextNullValue");
        assertTrue(rs.next());
        assertArrayEquals(rs.getObject(2, byte[].class), "abc".getBytes());
        assertTrue(rs.next());
        assertNull(rs.getObject(2, byte[].class));
    }

    private void validResultSetBitValue(ResultSet rs) throws SQLException {
        assertTrue(rs.next());

        checkAllDataType(rs, 1, 0);
        checkAllDataType(rs, 2, 0);

        assertTrue(rs.next());
        checkAllDataType(rs, 1, 1);
        checkAllDataType(rs, 2, 1);

        assertTrue(rs.next());
        checkAllDataType(rs, 1, 1);
        checkAllDataType(rs, 2, 85);

        assertTrue(rs.next());
        checkAllDataType(rs, 1, 1);
        checkAllDataType(rs, 2, 21845);

        assertTrue(rs.next());
        checkAllDataType(rs, 1, 1);
        checkAllDataType(rs, 2, 54613);

        assertTrue(rs.next());
        checkAllDataType(rs, 1, 1);
        checkAllDataType(rs, 2, 2147483648L);
    }

    private void checkAllDataType(ResultSet rs, int index, long expectedValue) throws SQLException {
        try {
            assertEquals((byte) expectedValue, rs.getByte(index));
            if (expectedValue > Byte.MAX_VALUE) {
                fail();
            }
        } catch (SQLException sqle) {
            if (expectedValue < Byte.MAX_VALUE) {
                fail();
            }
            assertTrue(sqle.getMessage().contains("Out of range"));
        }
        try {
            assertEquals((short) expectedValue, rs.getShort(index));
            if (expectedValue > Short.MAX_VALUE) {
                fail();
            }
        } catch (SQLException sqle) {
            if (expectedValue < Short.MAX_VALUE) {
                fail();
            }
            assertTrue(sqle.getMessage().contains("Out of range"));
        }
        try {
            assertEquals((int) expectedValue, rs.getInt(index));
            if (expectedValue > Integer.MAX_VALUE) {
                fail();
            }
        } catch (SQLException sqle) {
            if (expectedValue < Integer.MAX_VALUE) {
                fail();
            }
            assertTrue(sqle.getMessage().contains("Out of range"));
        }
        assertEquals(expectedValue != 0, rs.getBoolean(index));

        assertEquals(expectedValue, rs.getLong(index));
        assertEquals((float) expectedValue, rs.getFloat(index), 0.01);
        assertEquals((double) expectedValue, rs.getDouble(index), 0.01);
        assertEquals(new BigDecimal(expectedValue), rs.getBigDecimal(index));
        assertEquals(String.valueOf(expectedValue), rs.getString(index));
    }
}
