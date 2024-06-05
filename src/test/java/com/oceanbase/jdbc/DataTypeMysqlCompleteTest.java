/*
 *
 * OceanBase Client for Java
 *
 * Copyright (c) 2021 OceanBase.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
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
 *
 */
package com.oceanbase.jdbc;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.regex.Pattern;

import org.junit.Assert;
import org.junit.Test;

public class DataTypeMysqlCompleteTest extends BaseTest {

    public void conmmonCall(String testStr, ResultSet resultSet, String type) throws Exception {
        testRefTypeMethod(testStr, resultSet, type);

        testPrimitiveTypeMethod(testStr, resultSet, type);

        testStreamMethod(testStr, resultSet, type);

        testLargeObjectMethod(testStr, resultSet, type);
    }

    public void executePreparedStatementPublic(String tableName, String testStr, String body,
                                               String type) throws Exception {
        createTable(tableName, body);
        executePreparedStatementPublic(tableName, testStr, type);
    }

    public void executePreparedStatementPublic(String tableName, String testStr, String type)
                                                                                             throws Exception {

        PreparedStatement preparedStatement = sharedConnection.prepareStatement("delete from "
                                                                                + tableName);
        preparedStatement.execute();
        preparedStatement = sharedConnection.prepareStatement("insert into " + tableName
                                                              + " values (1, ?)");
        if ("byte".equals(type)) {
            preparedStatement.setByte(1, Byte.valueOf(testStr));
        } else if ("short".equals(type)) {
            preparedStatement.setShort(1, Short.valueOf(testStr));
        } else if ("int".equals(type)) {
            preparedStatement.setInt(1, Integer.valueOf(testStr));
        } else if ("long".equals(type)) {
            preparedStatement.setLong(1, Long.valueOf(testStr));
        }
        if ("double".equals(type)) {
            preparedStatement.setDouble(1, Double.valueOf(testStr));
        } else if ("float".equals(type)) {
            preparedStatement.setFloat(1, Float.valueOf(testStr));
        } else if ("datetime".equals(type)) {
            preparedStatement.setTimestamp(1, Timestamp.valueOf(testStr));
        } else if ("date".equals(type)) {
            preparedStatement.setDate(1, Date.valueOf(testStr));
        } else if ("char".equalsIgnoreCase(type) || "varchar".equalsIgnoreCase(type)) {
            preparedStatement.setString(1, testStr);
        } else if ("dateString".equals(type)) {
            preparedStatement.setString(1, testStr);
        } else if ("datetimeString".equals(type)) {
            preparedStatement.setString(1, testStr);
        } else if ("blob".equals(type)) {
            preparedStatement.setBlob(1, new Blob(testStr.getBytes(StandardCharsets.UTF_8)));
        } else if ("clob".equals(type)) {
            preparedStatement.setClob(1, new Clob(testStr.getBytes(StandardCharsets.UTF_8)));
        } else {
            preparedStatement.setString(1, testStr);
        }

        try {
            preparedStatement.execute();
        } catch (Exception e) {
            Assert.assertTrue(" supported feature or function but ...." + e.getMessage(), e
                .getMessage().contains("Not supported feature or function"));
        }
        ResultSet resultSet2 = preparedStatement.executeQuery("select * from " + tableName);
        if (resultSet2.next()) {
            conmmonCall(testStr, resultSet2, type);
        }
    }

    @Test
    public void testChar() throws Exception {
        String tableName = "CharTypeTest";
        createTable(tableName, "id int not null primary key, strm  char(50)");
        Statement statement = sharedConnection.createStatement();
        statement.execute("insert into " + tableName + " values(1,\'123\')");
        ResultSet resultSet = statement.executeQuery("select * from " + tableName);
        resultSet.next();
        String testStr = "123";
        conmmonCall(testStr, resultSet, "char");
        executePreparedStatementPublic(tableName, testStr, "char");
    }

    @Test
    public void testVarchar() throws Exception {
        String tableName = "CharTypeTest";

        createTable(tableName, "id int not null primary key, strm  varchar(50)");
        Statement statement = sharedConnection.createStatement();
        statement.execute("insert into " + tableName + " values(1,\'123\')");
        ResultSet resultSet = statement.executeQuery("select * from " + tableName);
        resultSet.next();
        String testStr = "123";
        conmmonCall(testStr, resultSet, "varchar");
        executePreparedStatementPublic(tableName, testStr, "varchar");
    }

    @Test
    public void testByte() throws Exception {
        String tableName = "ByteTypeTest";

        createTable(tableName, "id int not null primary key, strm TINYINT ");
        Statement statement = sharedConnection.createStatement();
        statement.execute("insert into " + tableName + " values(1,\'123\')");
        ResultSet resultSet = statement.executeQuery("select * from " + tableName);
        resultSet.next();
        String testStr = "123";

        conmmonCall(testStr, resultSet, "byte");
        executePreparedStatementPublic(tableName, testStr, "byte");
    }

    @Test
    public void testShort() throws Exception {
        String tableName = "shortTypeTest";

        createTable(tableName, "id int not null primary key, strm SMALLINT ");
        Statement statement = sharedConnection.createStatement();
        statement.execute("insert into " + tableName + " values(1,\'123\')");
        ResultSet resultSet = statement.executeQuery("select * from " + tableName);
        resultSet.next();
        String testStr = "123";

        conmmonCall(testStr, resultSet, "short");
        executePreparedStatementPublic(tableName, testStr, "short");

    }

    @Test
    public void testInt() throws Exception {
        String tableName = "intTypeTest";

        createTable(tableName, "id int not null primary key, strm INT ");
        Statement statement = sharedConnection.createStatement();
        statement.execute("insert into " + tableName + " values(1,\'123\')");
        ResultSet resultSet = statement.executeQuery("select * from " + tableName);
        resultSet.next();
        String testStr = "123";

        conmmonCall(testStr, resultSet, "int");
        executePreparedStatementPublic(tableName, testStr, "int");
    }

    @Test
    public void testLong() throws Exception {
        String tableName = "longTypeTest";

        createTable(tableName, "id int not null primary key, strm MEDIUMINT ");
        Statement statement = sharedConnection.createStatement();
        statement.execute("insert into " + tableName + " values(1,\'123\')");
        ResultSet resultSet = statement.executeQuery("select * from " + tableName);
        resultSet.next();
        String testStr = "123";

        conmmonCall(testStr, resultSet, "long");
        executePreparedStatementPublic(tableName, testStr, "long");
    }

    @Test
    public void testDouble() throws Exception {
        String tableName = "longTypeTest";

        createTable(tableName, "id int not null primary key, strm DOUBLE(5,2) ");
        Statement statement = sharedConnection.createStatement();
        statement.execute("insert into " + tableName + " values(1,\'123.98\')");
        ResultSet resultSet = statement.executeQuery("select * from " + tableName);
        resultSet.next();
        String testStr = "123.98";

        conmmonCall(testStr, resultSet, "double");
        executePreparedStatementPublic(tableName, testStr, "double");
    }

    @Test
    public void testFloat() throws Exception {
        String tableName = "floatTypeTest";

        createTable(tableName, "id int not null primary key, strm FLOAT(6,3) ");
        Statement statement = sharedConnection.createStatement();
        statement.execute("insert into " + tableName + " values(1,\'123.987\')");
        ResultSet resultSet = statement.executeQuery("select * from " + tableName);
        resultSet.next();
        String testStr = "123.987";

        conmmonCall(testStr, resultSet, "float");
        executePreparedStatementPublic(tableName, testStr, "float");
    }

    @Test
    public void testDate() throws Exception {
        String tableName = "dateTypeTest";

        createTable(tableName, "id int not null primary key, strm Date ");
        Statement statement = sharedConnection.createStatement();
        statement.execute("insert into " + tableName + " values(1,\'2021-04-09\')");
        ResultSet resultSet = statement.executeQuery("select * from " + tableName);
        resultSet.next();
        String testStr = "2021-04-09";

        testRefTypeMethod(testStr, resultSet, "date");

        testStreamMethod(testStr, resultSet);

        testLargeObjectMethod(testStr, resultSet);
        executePreparedStatementPublic(tableName, testStr, "date");
    }

    @Test
    public void testDateTime() throws Exception {
        String tableName = "dateTypeTest";

        String testStr = "2021-04-09 00:00:00.0";
        String body = "id int not null primary key, strm DateTime ";
        String type = "datetime";
        executeStatementPublic(tableName, testStr, body, type);

        executeStatementPublic(tableName, "2021-04-09 12:12:12.0", body, type);

        //        executeStatementPublic(tableName,"2021-04-09 12:12:12.123",body,type);

    }

    public void executeStatementPublic(String tableName, String testStr, String body, String type)
                                                                                                  throws Exception {
        createTable(tableName, body);
        Statement statement = sharedConnection.createStatement();
        statement.execute("insert into " + tableName + " values(1," + "'" + testStr + "'" + ")");
        ResultSet resultSet = statement.executeQuery("select * from " + tableName);
        resultSet.next();
        conmmonCall(testStr, resultSet, type);
        executePreparedStatementPublic(tableName, testStr, type);
    }

    public void executeStatementOnlyPublic(String tableName, String testStr, String body,
                                           String type) throws Exception {
        createTable(tableName, body);
        Statement statement = sharedConnection.createStatement();
        statement.execute("insert into " + tableName + " values(1," + "'" + testStr + "'" + ")");
        ResultSet resultSet = statement.executeQuery("select * from " + tableName);
        resultSet.next();
        conmmonCall(testStr, resultSet, type);
    }

    @Test
    public void testText() throws Exception {
        String tableName = "textTypeTest";
        String testStr = "2021-04-09 00:00:00.0";
        String body = "id int not null primary key, strm TEXT ";
        String type = "text";
        executeStatementPublic(tableName, testStr, body, type);

        body = "id int not null primary key, strm tinyTEXT ";
        executeStatementPublic(tableName, testStr, body, type);

        body = "id int not null primary key, strm MEDIUMTEXT ";
        executeStatementPublic(tableName, testStr, body, type);

        body = "id int not null primary key, strm LONGTEXT ";
        executeStatementPublic(tableName, testStr, body, type);
    }

    @Test
    public void testBlob() throws Exception {
        String tableName = "testBlobTest";
        String testStr = "2021-04-09 00:00:00.0";
        String body = "id int not null primary key, strm BLOB ";
        String type = "blob";
        executeStatementOnlyPublic(tableName, testStr, body, type);

        //        executeStatementPublic(tableName,testStr,body,type);
    }

    public void testPrimitiveTypeMethod(String testStr, ResultSet resultSet, String type)
                                                                                         throws Exception {
        String pattern = "^(\\-|\\+)?\\d+(\\.\\d+)?$";
        boolean isMatch = Pattern.matches(pattern, testStr);
        if (!isMatch) {
            return;
        }

        switch (type) {
            case "double":
                Assert.assertTrue("getBigDecimal is not equals",
                    testStr.equals(resultSet.getBigDecimal(2).doubleValue() + ""));
                Assert.assertTrue("getBigDecimal is not equals",
                    testStr.equals(resultSet.getBigDecimal("strm").doubleValue() + ""));
                break;
            case "float":
                Assert.assertTrue("getBigDecimal is not equals",
                    testStr.equals(resultSet.getBigDecimal(2).floatValue() + ""));
                Assert.assertTrue("getBigDecimal is not equals",
                    testStr.equals(resultSet.getBigDecimal("strm").floatValue() + ""));
                break;
            default:
                Assert.assertTrue("getBigDecimal is not equals",
                    testStr.equals(resultSet.getBigDecimal(2).intValue() + ""));
                Assert.assertTrue("getBigDecimal is not equals",
                    testStr.equals(resultSet.getBigDecimal("strm").intValue() + ""));
        }

        //        Assert.assertTrue("getByte is not equals",testStr.equals(resultSet.getByte(2)+""));
        //        Assert.assertTrue("getByte is not equals",testStr.equals(resultSet.getByte("strm")+""));

        Assert.assertTrue("getFloat is not equals result: " + resultSet.getFloat(2),
            Float.valueOf(testStr) == resultSet.getFloat(2));
        Assert.assertTrue("getFloat is not equals",
            Float.valueOf(testStr) == resultSet.getFloat("strm"));

        Assert.assertTrue("getDouble is not equals",
            Double.valueOf(testStr) == resultSet.getDouble(2));
        Assert.assertTrue("getDouble is not equals",
            Double.valueOf(testStr) == resultSet.getDouble("strm"));

        testStr = testStr.contains(".") ? testStr.substring(0, testStr.indexOf(".")) : testStr;
        Assert.assertTrue("getShort is not equals", testStr.equals(resultSet.getShort(2) + ""));
        Assert
            .assertTrue("getShort is not equals", testStr.equals(resultSet.getShort("strm") + ""));

        Assert.assertTrue("getInt is not equals", testStr.equals(resultSet.getInt(2) + ""));
        Assert.assertTrue("getInt is not equals", testStr.equals(resultSet.getInt("strm") + ""));

        Assert.assertTrue("getLong is not equals", Long.valueOf(testStr) == resultSet.getLong(2));
        Assert.assertTrue("getLong is not equals",
            Long.valueOf(testStr) == resultSet.getLong("strm"));

        Assert.assertTrue("getBoolean is not equals result: " + resultSet.getBoolean(2),
            true == resultSet.getBoolean(2));
        Assert.assertTrue("getBoolean is not equals", true == resultSet.getBoolean("strm"));
    }

    public void testStreamMethod(String testStr, ResultSet resultSet) throws Exception {
        byte[] bytes = new byte[testStr.length()];
        resultSet.getAsciiStream("strm").read(bytes);

        Assert.assertTrue("getObject is not equals result: " + new String(bytes),
            testStr.equals(new String(bytes)));
        byte[] bytes2 = new byte[testStr.length()];
        resultSet.getAsciiStream(2).read(bytes2);
        Assert.assertTrue("getObject is not equals result: " + new String(bytes2),
            testStr.equals(new String(bytes2)));

        char[] chars = new char[testStr.length()];
        resultSet.getCharacterStream(2).read(chars);
        Assert.assertTrue("getCharacterStream is not equals", testStr.equals(new String(chars)));
        char[] chars2 = new char[testStr.length()];
        resultSet.getCharacterStream("strm").read(chars2);
        Assert.assertTrue("getCharacterStream is not equals", testStr.equals(new String(chars2)));

        char[] nchars = new char[testStr.length()];
        resultSet.getNCharacterStream(2).read(nchars);
        Assert.assertTrue("getNCharacterStream is not equals", testStr.equals(new String(nchars)));
        char[] nchars2 = new char[testStr.length()];
        resultSet.getNCharacterStream("strm").read(nchars2);
        Assert.assertTrue("getNCharacterStream is not equals", testStr.equals(new String(nchars2)));

        //rowid concurrenty binaryStream
        resultSet.getBinaryStream(2).read(bytes);
        Assert.assertTrue("getBinaryStream is not equals", testStr.equals(new String(bytes)));
        resultSet.getBinaryStream("strm").read(bytes2);
        Assert.assertTrue("getBinaryStream is not equals", testStr.equals(new String(bytes2)));
    }

    public void testStreamMethod(String testStr, ResultSet resultSet, String type) throws Exception {
        if (type == null) {
            testStreamMethod(testStr, resultSet);
            return;
        }
        byte[] bytes = new byte[testStr.length()];
        byte[] bytes2 = new byte[testStr.length()];
        resultSet.getAsciiStream("strm").read(bytes);
        resultSet.getAsciiStream(2).read(bytes2);

        switch (type) {
            case "datetime":
                Assert.assertTrue("getObject is not equals result: " + new String(bytes),
                    testStr.equals(Timestamp.valueOf(new String(bytes)).toString()));
                Assert.assertTrue("getObject is not equals result: " + new String(bytes2),
                    testStr.equals(Timestamp.valueOf(new String(bytes2)).toString()));
                break;
            default:
                resultSet.getAsciiStream(2).read(bytes2);
                Assert.assertTrue("getObject is not equals result: " + new String(bytes2),
                    testStr.equals(new String(bytes2)));
                break;
        }

        char[] chars = new char[testStr.length()];
        resultSet.getCharacterStream(2).read(chars);
        Assert.assertTrue("getCharacterStream is not equals", testStr.equals(new String(chars)));
        char[] chars2 = new char[testStr.length()];
        resultSet.getCharacterStream("strm").read(chars2);
        Assert.assertTrue("getCharacterStream is not equals", testStr.equals(new String(chars2)));

        char[] nchars = new char[testStr.length()];
        resultSet.getNCharacterStream(2).read(nchars);
        Assert.assertTrue("getNCharacterStream is not equals", testStr.equals(new String(nchars)));
        char[] nchars2 = new char[testStr.length()];
        resultSet.getNCharacterStream("strm").read(nchars2);
        Assert.assertTrue("getNCharacterStream is not equals", testStr.equals(new String(nchars2)));

        //rowid concurrenty binaryStream
        resultSet.getBinaryStream(2).read(bytes);
        resultSet.getBinaryStream("strm").read(bytes2);

        switch (type) {
            case "datetime":
                Assert.assertTrue("getBinaryStream is not equals result: " + new String(bytes),
                    testStr.equals(Timestamp.valueOf(new String(bytes)).toString()));
                Assert.assertTrue("getBinaryStream is not equals result: " + new String(bytes2),
                    testStr.equals(Timestamp.valueOf(new String(bytes2)).toString()));
                break;
            default:
                Assert.assertTrue("getBinaryStream is not equals result: " + new String(bytes2),
                    testStr.equals(new String(bytes)));
                Assert.assertTrue("getBinaryStream is not equals result: " + new String(bytes),
                    testStr.equals(new String(bytes2)));
                break;
        }

    }

    public void testLargeObjectMethod(String testStr, ResultSet resultSet) throws Exception {

        Assert.assertTrue("getBlob is not equals",
            testStr.equals(new String(resultSet.getBlob(2).getBytes(1, testStr.length()))));
        Assert.assertTrue("getBlob is not equals",
            testStr.equals(new String(resultSet.getBlob("strm").getBytes(1, testStr.length()))));

        Assert.assertTrue("getClob is not equals",
            testStr.equals(resultSet.getClob("strm").getSubString(1, testStr.length())));
        Assert.assertTrue("getClob is not equals",
            testStr.equals(resultSet.getClob(2).getSubString(1, testStr.length())));

        Assert.assertTrue("getNClob is not equals",
            testStr.equals(resultSet.getNClob(2).getSubString(1, testStr.length())));
        Assert.assertTrue("getNClob is not equals",
            testStr.equals(resultSet.getNClob(2).getSubString(1, testStr.length())));
    }

    public void testLargeObjectMethod(String testStr, ResultSet resultSet, String type)
                                                                                       throws Exception {
        if (type == null) {
            testLargeObjectMethod(testStr, resultSet);
            return;
        }
        switch (type) {
            case "datetime":
                Assert
                    .assertTrue("getBlob is not equals", testStr.equals(Timestamp.valueOf(
                        new String(resultSet.getBlob(2).getBytes(1, testStr.length()))).toString()));
                Assert.assertTrue("getBlob is not equals", testStr
                    .equals(Timestamp.valueOf(
                        new String(resultSet.getBlob("strm").getBytes(1, testStr.length())))
                        .toString()));

                Assert.assertTrue(
                    "getClob is not equals",
                    testStr.equals(Timestamp.valueOf(
                        resultSet.getClob("strm").getSubString(1, testStr.length())).toString()));
                Assert.assertTrue(
                    "getClob is not equals",
                    testStr.equals(Timestamp.valueOf(
                        resultSet.getClob(2).getSubString(1, testStr.length())).toString()));

                Assert.assertTrue(
                    "getNClob is not equals",
                    testStr.equals(Timestamp.valueOf(
                        resultSet.getNClob(2).getSubString(1, testStr.length())).toString()));
                Assert.assertTrue(
                    "getNClob is not equals",
                    testStr.equals(Timestamp.valueOf(
                        resultSet.getNClob(2).getSubString(1, testStr.length())).toString()));
                break;
            default:
                Assert.assertTrue("getBlob is not equals",
                    testStr.equals(new String(resultSet.getBlob(2).getBytes(1, testStr.length()))));
                Assert.assertTrue(
                    "getBlob is not equals",
                    testStr.equals(new String(resultSet.getBlob("strm").getBytes(1,
                        testStr.length()))));

                Assert.assertTrue("getClob is not equals",
                    testStr.equals(resultSet.getClob("strm").getSubString(1, testStr.length())));
                Assert.assertTrue("getClob is not equals",
                    testStr.equals(resultSet.getClob(2).getSubString(1, testStr.length())));

                Assert.assertTrue("getNClob is not equals",
                    testStr.equals(resultSet.getNClob(2).getSubString(1, testStr.length())));
                Assert.assertTrue("getNClob is not equals",
                    testStr.equals(resultSet.getNClob(2).getSubString(1, testStr.length())));
        }
    }

    public void testRefTypeMethod(String testStr, ResultSet resultSet, String type)
                                                                                   throws Exception {

        switch (type) {
            case "datetime":
                Assert.assertTrue("getString is not equals value:" + resultSet.getString(2),
                    testStr.equals(resultSet.getString(2)));
                Assert.assertTrue("getString is not equals",
                    testStr.equals(resultSet.getString("strm")));
                Assert.assertTrue("getString is not equals",
                    testStr.equals(resultSet.getNString(2)));
                Assert.assertTrue("getString is not equals",
                    testStr.equals(resultSet.getNString("strm")));
                break;
            default:
                Assert.assertTrue("getString is not equals",
                    String.valueOf(testStr).equals(resultSet.getString(2)));
                Assert.assertTrue("getString is not equals",
                    String.valueOf(testStr).equals(resultSet.getString("strm")));
                Assert.assertTrue("getString is not equals",
                    String.valueOf(testStr).equals(resultSet.getNString(2)));
                Assert.assertTrue("getString is not equals",
                    String.valueOf(testStr).equals(resultSet.getNString("strm")));
                break;
        }

        switch (type) {
            case "bigint":
            case "long":
            case "int":
            case "short":
            case "byte":
                Assert.assertTrue("getObject is not equals",
                    testStr.equals(String.valueOf(resultSet.getObject(2))));
                Assert.assertTrue("getObject is not equals",
                    testStr.equals(String.valueOf(resultSet.getObject("strm"))));
                break;

            case "float":
                Assert.assertTrue("getObject is not equals",
                    Float.valueOf(testStr).floatValue() == (Float) resultSet.getObject(2));
                Assert.assertTrue("getObject is not equals",
                    Float.valueOf(testStr).floatValue() == (Float) resultSet.getObject("strm"));
                break;
            case "double":
                Assert.assertTrue("getObject is not equals",
                    Double.valueOf(testStr).doubleValue() == (Double) resultSet.getObject(2));
                Assert.assertTrue("getObject is not equals",
                    Double.valueOf(testStr).doubleValue() == (Double) resultSet.getObject("strm"));
                break;
            case "date":
                Assert.assertTrue("getObject is not equals", Date.valueOf(testStr).toString()
                    .equals(((Date) resultSet.getObject(2)).toString()));
                Assert.assertTrue("getObject is not equals", Date.valueOf(testStr).toString()
                    .equals(((Date) resultSet.getObject("strm")).toString()));
                break;
            case "datetime":
                Assert.assertTrue("timestamp getObject is not equals", Timestamp.valueOf(testStr)
                    .equals((Timestamp) resultSet.getObject(2)));
                Assert.assertTrue("timestamp getObject is not equals", Timestamp.valueOf(testStr)
                    .equals((Timestamp) resultSet.getObject("strm")));
                break;
            case "blob":
                Assert.assertTrue("", testStr.equals(new String((byte[]) resultSet.getObject(2))));
                ;
                Assert.assertTrue("", testStr.equals(new String((byte[]) resultSet.getObject(2))));
                break;
            case "clob":
                Assert.assertTrue(
                    "",
                    testStr.equals(new String(((Clob) resultSet.getObject(2)).getBytes(1,
                        testStr.length()))));
                Assert.assertTrue("", testStr.equals(new String(
                    ((Clob) resultSet.getObject("strm")).getBytes(1, testStr.length()))));
                break;
            default:
                Assert
                    .assertTrue("getObject is not equals", testStr.equals(resultSet.getObject(2)));
                Assert.assertTrue("getObject is not equals",
                    testStr.equals(resultSet.getObject(2, String.class)));
                Assert.assertTrue("getObject is not equals",
                    testStr.equals(resultSet.getObject(2, String.class)));
                Assert.assertTrue("getObject is not equals",
                    testStr.equals(resultSet.getObject("strm", String.class)));
                Assert.assertTrue("getObject is not equals",
                    testStr.equals(resultSet.getObject("strm")));
                break;
        }

        try {
            Assert.assertTrue("getObject is not equals", testStr.equals(resultSet.getArray(2)));
            Assert
                .assertTrue("getObject is not equals", testStr.equals(resultSet.getArray("strm")));
        } catch (Exception e) {
            if (e.getClass().getName().contains("SQLFeatureNotSupportedException"))
                System.out.println("type " + type + "getArray SQLFeatureNotSupportedException");
            else
                throw e;
        }

        switch (type) {
            case "datetime":
                Assert.assertTrue(
                    "getBytes is not equals",
                    Timestamp.valueOf(testStr).equals(
                        Timestamp.valueOf(new String(resultSet.getBytes("strm")))));
                Assert.assertTrue(
                    "getBytes is not equals",
                    Timestamp.valueOf(testStr).equals(
                        Timestamp.valueOf(new String(resultSet.getBytes(2)))));
                break;
            default:
                Assert.assertTrue("getBytes is not equals",
                    testStr.equals(new String(resultSet.getBytes("strm"))));
                Assert.assertTrue("getBytes is not equals",
                    testStr.equals(new String(resultSet.getBytes(2))));
        }

        try {
            Assert.assertTrue("getBinaryStream is not equals",
                testStr.equals(resultSet.getRef(2).getObject()));
            Assert.assertTrue("getBinaryStream is not equals",
                testStr.equals(resultSet.getRef("strm").getObject()));
        } catch (Exception e) {
            Assert.assertTrue("", e.getClass().getName()
                .contains("SQLFeatureNotSupportedException"));
        }

        try {
            Assert.assertTrue("getBinaryStream is not equals",
                testStr.equals(new String(resultSet.getRowId(2).getBytes())));
            Assert.assertTrue("getBinaryStream is not equals",
                testStr.equals(new String(resultSet.getRowId("strm").getBytes())));
        } catch (Exception e) {
            Assert.assertTrue("", e.getClass().getName()
                .contains("SQLFeatureNotSupportedException"));
        }
    }

}
