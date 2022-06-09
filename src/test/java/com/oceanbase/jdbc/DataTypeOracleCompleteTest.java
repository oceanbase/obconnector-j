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

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.regex.Pattern;

import org.junit.Assert;
import org.junit.Test;

import com.oceanbase.jdbc.extend.datatype.INTERVALDS;
import com.oceanbase.jdbc.extend.datatype.INTERVALYM;
import com.oceanbase.jdbc.extend.datatype.TIMESTAMPLTZ;
import com.oceanbase.jdbc.extend.datatype.TIMESTAMPTZ;

public class DataTypeOracleCompleteTest extends BaseOracleTest {

    private static final String TIMESTAMP_LTZ        = "timestampLTZ";
    private static final String TIMESTAMP_TZ         = "timestampTZ";
    private static final String TIMESTAMP_LTZ_STRING = "timestampLTZstring";
    private static final String TIMESTAMP_TZ_STRING  = "timestampTZstring";
    private static final String INTERVALYM           = "intervalYM";
    private static final String INTERVALDS           = "intervalDS";
    private static final String RAW                  = "raw";

    public static final String  ZERO_TIME            = " 00:00:00";

    public void conmmonCall(String testStr, ResultSet resultSet, String type) throws Exception {
        testRefTypeMethod(testStr, resultSet, type);

        testPrimitiveTypeMethod(testStr, resultSet, type);

        if (!TIMESTAMP_LTZ.equals(type) && !TIMESTAMP_TZ.equals(type)
            && !TIMESTAMP_LTZ_STRING.equals(type) && !TIMESTAMP_TZ_STRING.equals(type)
            && !"datetime".equals(type) && !"datetimeString".equalsIgnoreCase(type)
            && !INTERVALYM.equals(type) && !INTERVALDS.equals(type)) {
            if (!type.equals("date")) {
                testStreamMethod(testStr, resultSet);
            }
        }
        if (!type.equals("date")) {
            testLargeObjectMethod(testStr, resultSet, type);
        }
    }

    public void executeStatementPublic(String tableName, String body, String testStr, String type)
                                                                                                  throws Exception {
        createTable(tableName, body);
        Statement statement = sharedConnection.createStatement();
        statement.execute(new StringBuilder("insert into ").append(tableName).append(" values(1,")
            .append("\'").append(testStr).append("\'").append(")").toString());
        ResultSet resultSet = statement.executeQuery("select * from " + tableName);
        resultSet.next();
        conmmonCall(testStr, resultSet, type);
        executePreparedStatementPublic(tableName, testStr, type);
    }

    @Test
    public void testChar() throws Exception {
        String tableName = "CharTypeTest";
        String body = "id int not null primary key, strm  char(50)";
        String type = "char";
        String testStr = "123";

        executeStatementPublic(tableName, body, testStr, type);

        String testStr2 = "2021-01-14";
        type = "dateString";
        executeStatementPublic(tableName, body, testStr2, type);

        String testStr3 = "2021-01-14 14:14:55";
        type = "datetimeString";
        executeStatementPublic(tableName, body, testStr3, type);

    }

    @Test
    public void testVarchar() throws Exception {
        String tableName = "CharTypeTest";
        String testStr = "123";
        String body = "id int not null primary key, strm  varchar2(50)";
        String type = "varchar";
        executeStatementPublic(tableName, body, testStr, type);

        String testStr2 = "2021-01-14";
        type = "dateString";
        executeStatementPublic(tableName, body, testStr2, type);

        String testStr3 = "2021-01-14 14:14:55";
        type = "datetimeString";
        executeStatementPublic(tableName, body, testStr3, type);

    }

    @Test
    public void testByte() throws Exception {
        String tableName = "ByteTypeTest";

        createTable(tableName, "id int not null primary key, strm number(3,0) ");
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

        createTable(tableName, "id int not null primary key, strm number(5,0) ");
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

        createTable(tableName, "id int not null primary key, strm number(10,0) ");
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

        createTable(tableName, "id int not null primary key, strm number(20,0) ");
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

        createTable(tableName, "id int not null primary key, strm NUMBER(6,3) ");
        Statement statement = sharedConnection.createStatement();
        statement.execute("insert into " + tableName + " values(1,\'123.987\')");
        ResultSet resultSet = statement.executeQuery("select * from " + tableName);
        System.out.println("columnType:" + resultSet.getMetaData().getColumnType(2) + "columnName:"
                           + resultSet.getMetaData().getColumnTypeName(2));
        resultSet.next();
        String testStr = "123.987";

        conmmonCall(testStr, resultSet, "double");

        executePreparedStatementPublic(tableName, testStr, "double");
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
        } else if (INTERVALYM.equals(type)) {
            if (preparedStatement instanceof ObPrepareStatement) {
                ((ObPrepareStatement) preparedStatement).setINTERVALYM(1, new INTERVALYM(testStr));
            }
        } else if (INTERVALDS.equals(type)) {
            if (preparedStatement instanceof ObPrepareStatement) {
                ((ObPrepareStatement) preparedStatement).setINTERVALDS(1, new INTERVALDS(testStr));
            }
        }

        preparedStatement.execute();
        ResultSet resultSet2 = preparedStatement.executeQuery("select * from " + tableName);
        if (resultSet2.next()) {
            conmmonCall(testStr, resultSet2, type);
        }
    }

    @Test
    public void testFloat() throws Exception {
        String tableName = "floatTypeTest";

        createTable(tableName, "id int not null primary key, strm NUMBER(6,3) ");
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
        statement.execute("insert into " + tableName
                          + " values(1,to_date(\'2021-04-09\',\'yyyy-MM-dd\'))");
        ResultSet resultSet = statement.executeQuery("select * from " + tableName);
        resultSet.next();
        String testStr = "2021-04-09";

        testRefTypeMethod(testStr, resultSet, "date");

        executePreparedStatementPublic(tableName, testStr, "date");
    }

    @Test
    public void testDateTime() throws Exception {
        String tableName = "timestampTypeTest";

        createTable(tableName, "id int not null primary key, strm timestamp ");
        Statement statement = sharedConnection.createStatement();
        statement.execute("insert into " + tableName + " values(1,\'2021-04-09 11:38:20.98\')");
        ResultSet resultSet = statement.executeQuery("select * from " + tableName);

        String testStr = "2021-04-09 11:38:20.98";
        if (resultSet.next()) {
            conmmonCall(testStr, resultSet, "datetime");
        }

        executePreparedStatementPublic(tableName, testStr, "datetime");
    }

    @Test
    public void testDateTime1() throws Exception {
        String tableName = "timestampTypeTest";

        createTable(tableName, "id int not null primary key, strm timestamp ");
        Statement statement = sharedConnection.createStatement();
        statement.execute("insert into " + tableName + " values(1,\'2021-04-09 11:38:20\')");
        ResultSet resultSet = statement.executeQuery("select * from " + tableName);
        resultSet.next();
        String testStr = "2021-04-09 11:38:20";
        String tmp = Timestamp.valueOf(testStr).toString();
        tmp = tmp.substring(0, tmp.length() - 2);// remove the .0
        Assert.assertTrue("getString is not equals", tmp.equals(resultSet.getString(2).trim()));
        Assert
            .assertTrue("getString is not equals", tmp.equals(resultSet.getString("strm").trim()));

        Assert.assertTrue("timestamp getObject is not equals",
            Timestamp.valueOf(testStr).equals((Timestamp) resultSet.getObject(2)));
        Assert.assertTrue("timestamp getObject is not equals",
            Timestamp.valueOf(testStr).equals((Timestamp) resultSet.getObject("strm")));

        String testStr2 = Timestamp.valueOf(testStr).toString();
        Assert.assertTrue("getTimestamp is not equals",
            Timestamp.valueOf(testStr2).equals(resultSet.getTimestamp(2)));
        Assert.assertTrue("getTimestamp is not equals",
            Timestamp.valueOf(testStr2).equals(resultSet.getTimestamp("strm")));

    }

    @Test
    public void testTimestampLocalTimeZoneString() throws Exception {

        testTimestampLocalTimeZone_public("asString", TIMESTAMP_LTZ);

    }

    @Test
    public void testTimestampLocalTimeZone() throws Exception {

        testTimestampLocalTimeZone_public("default", TIMESTAMP_LTZ);

    }

    @Test
    public void testTimestampTimeZone() throws Exception {

        testTimestampLocalTimeZone_public("default", TIMESTAMP_TZ);
    }

    @Test
    public void testTimestampTimeZoneString() throws Exception {

        testTimestampLocalTimeZone_public("asString", TIMESTAMP_TZ);

    }

    @Test
    public void testIntervalYM() throws Exception {

        String tableName = "testIntervalYearToMonth";
        createTable(tableName, "id int , strm INTERVAL YEAR(4) TO MONTH ");
        String testStr = "2021-4";
        Statement statement = sharedConnection.createStatement();
        statement.execute("insert into " + tableName + " values (1,INTERVAL " + "'" + testStr + "'"
                          + " YEAR(4) TO MONTH )");
        ResultSet resultSet = statement.executeQuery("select * from " + tableName);
        if (resultSet.next()) {
            Assert.assertTrue("", resultSet.getInt(1) == 1);
            System.out.println("interval:" + resultSet.getString(2));
            Assert.assertTrue("", resultSet.getString(2).equals("2021-4"));
            Assert.assertTrue("", resultSet.getString("strm").equals("2021-4"));
            testRefTypeMethod(testStr, resultSet, INTERVALYM);
            testPrimitiveTypeMethod("2021-4", resultSet, INTERVALYM);
            testLargeObjectMethod(testStr, resultSet, INTERVALYM);
            testStreamMethod(testStr, resultSet, INTERVALYM);
        }
        executePreparedStatementPublic(tableName, testStr, INTERVALYM);

    }

    @Test
    public void testIntervalDS() throws Exception {

        String tableName = "testIntervalDayToSecond";
        createTable(tableName, "id int , strm INTERVAL DAY(2) TO SECOND(3) ");
        String testStr = "23 12:23:42.234";
        Statement statement = sharedConnection.createStatement();
        statement.execute("insert into " + tableName + " values (1,INTERVAL " + "'" + testStr + "'"
                          + " DAY TO SECOND )");
        ResultSet resultSet = statement.executeQuery("select * from " + tableName);
        if (resultSet.next()) {
            Assert.assertTrue("", resultSet.getInt(1) == 1);
            System.out.println("interval:" + resultSet.getString(2));
            Assert.assertTrue("", resultSet.getString(2).equals(testStr));
            Assert.assertTrue("", resultSet.getString("strm").equals(testStr));
            testRefTypeMethod(testStr, resultSet, INTERVALDS);
            testPrimitiveTypeMethod(testStr, resultSet, INTERVALDS);
            testLargeObjectMethod(testStr, resultSet, INTERVALDS);
            testStreamMethod(testStr, resultSet, INTERVALDS);
        }
        executePreparedStatementPublic(tableName, testStr, INTERVALDS);

    }

    public void testTimestampLocalTimeZone_public(String selectType, String fieldType)
                                                                                      throws Exception {

        if (!TIMESTAMP_LTZ.equals(fieldType) && !TIMESTAMP_TZ.equals(fieldType)) {
            return;
        }
        String tableName = "timestampWithLocalTZTypeTest";
        String sql = "";
        String type = "";
        switch (selectType) {
            case "asString":
                // field is TIMESTAMP WITH LOCAL TIME ZONE and query as string fromat
                sql = "select  id,to_char(strm,'yyyy-MM-dd hh:mi:ss.ff') strm from " + tableName;
                if (TIMESTAMP_LTZ.equals(fieldType)) {
                    type = TIMESTAMP_LTZ_STRING;
                } else if (TIMESTAMP_TZ.equals(fieldType)) {
                    type = TIMESTAMP_TZ_STRING;
                }
                break;
            default:
                sql = "select * from " + tableName;
                if (TIMESTAMP_LTZ.equals(fieldType)) {
                    type = TIMESTAMP_LTZ;
                } else if (TIMESTAMP_TZ.equals(fieldType)) {
                    type = TIMESTAMP_TZ;
                }
        }

        String createSql = "";
        switch (fieldType) {
            case TIMESTAMP_LTZ:
                createSql = "id int not null primary key, strm TIMESTAMP WITH LOCAL TIME ZONE";
                break;
            case TIMESTAMP_TZ:
                createSql = "id int not null primary key, strm TIMESTAMP WITH TIME ZONE";
                break;
            default:
        }

        createTable(tableName, createSql);
        Statement statement = sharedConnection.createStatement();
        statement.execute("insert into " + tableName + " values(1,\'2021-04-09 11:38:20.0\')");
        ResultSet resultSet = statement.executeQuery(sql);
        resultSet.next();
        String testStr = "2021-04-09 11:38:20.0";

        testRefTypeMethod(testStr, resultSet, type);
        testStreamMethod(testStr, resultSet, type);
        testLargeObjectMethod(testStr, resultSet, type);

        statement.execute("delete from " + tableName);
        PreparedStatement preparedStatement = sharedConnection.prepareStatement("insert into "
                                                                                + tableName
                                                                                + " values(1,?)");
        testStr = "2021-04-09 11:55:20.0";
        preparedStatement.setTimestamp(1, Timestamp.valueOf(testStr));
        preparedStatement.execute();
        ResultSet resultSet2 = statement.executeQuery(sql);
        resultSet2.next();

        testRefTypeMethod(testStr, resultSet2, type);

        testStreamMethod(testStr, resultSet2, type);

        testLargeObjectMethod(testStr, resultSet2, type);

        statement.execute("delete from " + tableName);
        PreparedStatement preparedStatement2 = sharedConnection.prepareStatement("insert into "
                                                                                 + tableName
                                                                                 + " values(1,?)");
        testStr = "2021-04-09 11:55:20.12";
        System.out.println("timstamp: " + Timestamp.valueOf(testStr));
        preparedStatement.setTimestamp(1, Timestamp.valueOf(testStr));
        preparedStatement.execute();
        ResultSet resultSet3 = statement.executeQuery(sql);
        resultSet3.next();

        testRefTypeMethod(testStr, resultSet3, type);

        testStreamMethod(testStr, resultSet3, type);

        testLargeObjectMethod(testStr, resultSet3, type);
    }

    @Test
    public void testRawType() throws Exception {
        String tableName = "testRawType";
        createTable(tableName, "id int primary key ,strm RAW(10) ");
        String testStr = "123f";
        PreparedStatement preparedStatement = sharedConnection
            .prepareStatement("insert into " + tableName + " values (1,utl_raw.cast_to_raw(?))");
        preparedStatement.setString(1, testStr);
        preparedStatement.execute();
        ResultSet resultSet = preparedStatement
            .executeQuery("select id, utl_raw.cast_to_varchar2(strm) strm from " + tableName);
        if (resultSet.next()) {
            System.out.println("raw:" + resultSet.getString(2));
            Assert.assertTrue("", resultSet.getString(2).equals(testStr));
            Assert.assertTrue("", resultSet.getString("strm").equals(testStr));
            testRefTypeMethod(testStr, resultSet, RAW);
            testPrimitiveTypeMethod(testStr, resultSet, RAW);
            testLargeObjectMethod(testStr, resultSet, RAW);
            testStreamMethod(testStr, resultSet, RAW);
            //            executePreparedStatementPublic(tableName,testStr,RAW);
        }
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
            case "dateString":
            case "datetimeString":
                System.out.println(testStr);
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

        Assert.assertTrue("getAsciiStream is not equals result: " + new String(bytes),
            testStr.equals(new String(bytes)));
        byte[] bytes2 = new byte[testStr.length()];
        resultSet.getAsciiStream(2).read(bytes2);
        Assert.assertTrue("getAsciiStream is not equals result: " + new String(bytes2),
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
                Assert.assertTrue("getAsciiStream is not equals result: " + new String(bytes),
                    testStr.equals(Timestamp.valueOf(new String(bytes)).toString()));
                Assert.assertTrue("getAsciiStream is not equals result: " + new String(bytes2),
                    testStr.equals(Timestamp.valueOf(new String(bytes2)).toString()));
                break;
            case TIMESTAMP_LTZ:
            case TIMESTAMP_TZ:
                byte[] byteLTZ = new byte[36];
                byte[] byteLTZ2 = new byte[36];
                resultSet.getAsciiStream("strm").read(byteLTZ);
                resultSet.getAsciiStream(2).read(byteLTZ2);
                System.out.println("getAsciiStream is not equals result: " + new String(bytes));
                //                Assert
                //                    .assertTrue("getAsciiStream is not equals result: " + new String(bytes),
                //                        testStr.equals(Timestamp.valueOf(new String(bytes).substring(0, 21))
                //                            .toString()));
                //                Assert.assertTrue("getAsciiStream is not equals result: " + new String(bytes2),
                //                    testStr.equals(Timestamp.valueOf(new String(bytes2).substring(0, 21))
                //                        .toString()));
                break;
            case TIMESTAMP_LTZ_STRING:
            case TIMESTAMP_TZ_STRING:
                Assert.assertTrue("getAsciiStream is not equals result: " + new String(bytes2),
                    testStr.equals(Timestamp.valueOf(new String(bytes2)).toString()));
                break;
            case INTERVALDS:
            case INTERVALYM:
                System.out.println("INTERVAL " + new String(bytes));
                break;
            default:
                resultSet.getAsciiStream(2).read(bytes2);
                Assert.assertTrue("getAsciiStream is not equals result: " + new String(bytes2),
                    testStr.equals(new String(bytes2)));
                break;
        }

        char[] chars = new char[testStr.length()];
        char[] chars2 = new char[testStr.length()];
        resultSet.getCharacterStream(2).read(chars);
        resultSet.getCharacterStream("strm").read(chars2);
        char[] nchars = new char[testStr.length()];
        resultSet.getNCharacterStream(2).read(nchars);
        char[] nchars2 = new char[testStr.length()];
        resultSet.getNCharacterStream("strm").read(nchars2);
        switch (type) {
            case TIMESTAMP_LTZ_STRING:
            case TIMESTAMP_TZ_STRING:
            case TIMESTAMP_LTZ:
            case TIMESTAMP_TZ:
                //                Assert.assertTrue("getCharacterStream is not equals", testStr.equals(Timestamp.valueOf(new String(chars)).toString()));
                //                Assert.assertTrue("getCharacterStream is not equals", testStr.equals(Timestamp.valueOf(new String(chars2)).toString()));
                //                Assert.assertTrue("getNCharacterStream is not equals", testStr.equals(Timestamp.valueOf(new String(nchars)).toString()));
                //                Assert.assertTrue("getNCharacterStream is not equals", testStr.equals(Timestamp.valueOf(new String(nchars2)).toString()));
                break;
            default:
                Assert.assertTrue("getCharacterStream is not equals",
                    testStr.equals(new String(chars)));
                Assert.assertTrue("getCharacterStream is not equals",
                    testStr.equals(new String(chars2)));
                Assert.assertTrue("getNCharacterStream is not equals",
                    testStr.equals(new String(nchars)));
                Assert.assertTrue("getNCharacterStream is not equals",
                    testStr.equals(new String(nchars2)));
        }

        //rowid concurrenty binaryStream
        resultSet.getBinaryStream(2).read(bytes);
        resultSet.getBinaryStream("strm").read(bytes2);

        switch (type) {
            case "datetime":
            case TIMESTAMP_LTZ_STRING:
            case TIMESTAMP_TZ_STRING:
                Assert.assertTrue("getBinaryStream is not equals result: " + new String(bytes),
                    testStr.equals(Timestamp.valueOf(new String(bytes)).toString()));
                Assert.assertTrue("getBinaryStream is not equals result: " + new String(bytes2),
                    testStr.equals(Timestamp.valueOf(new String(bytes2)).toString()));
                break;
            case TIMESTAMP_LTZ:
            case TIMESTAMP_TZ:
                break;
            //            case TIMESTAMP_LTZ_STRING:
            //                Assert.assertTrue("getBinaryStream is not equals result: " + new String(bytes2),
            //                        testStr.equals(Timestamp.valueOf(new String(bytes)).toString()));
            //                Assert.assertTrue("getBinaryStream is not equals result: " + new String(bytes),
            //                        testStr.equals(Timestamp.valueOf(new String(bytes2)).toString()));
            //                break;
            case INTERVALDS:
            case INTERVALYM:
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

        System.out.println("getblob: "
                           + testStr.equals(new String(resultSet.getBlob(2).getBytes(1,
                               testStr.length())).trim()));
        Assert.assertTrue("getBlob is not equals value: "
                          + new String(resultSet.getBlob(2).getBytes(1, testStr.length()))
                          + "testStr:" + testStr,
            testStr.equals(new String(resultSet.getBlob(2).getBytes(1, testStr.length())).trim()));
        Assert.assertTrue("getBlob is not equals", testStr.equals(new String(resultSet.getBlob(
            "strm").getBytes(1, testStr.length())).trim()));

        Assert.assertTrue(
            "getClob is not equals value :"
                    + resultSet.getClob("strm").getSubString(1, testStr.length()),
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
        if ("datetime".equals(type) || TIMESTAMP_LTZ.equals(type) || TIMESTAMP_TZ.equals(type)
            || TIMESTAMP_LTZ_STRING.equals(type) || TIMESTAMP_TZ_STRING.equals(type)
            || INTERVALYM.equals(type) || INTERVALDS.equals(type)) {
            return;
        }
        switch (type) {
            case "datetime":
            case TIMESTAMP_LTZ_STRING:
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
            case "char":
                System.out.println("blob clob nclob 无效的列类型");
                //                Assert.assertTrue("getClob is not equals",testStr.equals(resultSet.getClob("strm").getSubString(1,testStr.length())));
                //                Assert.assertTrue("getClob is not equals",testStr.equals(resultSet.getClob(2).getSubString(1,testStr.length())));

                //                Assert.assertTrue("getNClob is not equals",testStr.equals(resultSet.getNClob(2).getSubString(1,testStr.length())));
                //                Assert.assertTrue("getNClob is not equals",testStr.equals(resultSet.getNClob(2).getSubString(1,testStr.length())));
                break;
            case INTERVALDS:
            case INTERVALYM:
                System.out
                    .println("intervalym "
                             + new String(resultSet.getBlob(2).getBytes(1, testStr.length())));
                break;
            default:
                Assert.assertTrue("getBlob is not equals",
                    testStr.getBytes(StandardCharsets.UTF_8).length == (resultSet.getBlob(2)
                        .getBytes(1, testStr.length()).length));
                Assert.assertTrue(
                    "getBlob is not equals",
                    testStr.equals(new String(resultSet.getBlob("strm").getBytes(1,
                        testStr.length()), "ISO-8859-1")));

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

            case "date":
                testStr = testStr.contains(":") ? Timestamp.valueOf(testStr).toString()
                    : testStr + ZERO_TIME;
            case "dateString":
                Assert.assertTrue(
                    "getString is not equals value: " + resultSet.getString(2).trim(),
                    testStr.equals(resultSet.getString(2).trim()));
                Assert.assertTrue("getString is not equals",
                    testStr.equals(resultSet.getString("strm").trim()));
                Assert.assertTrue("getString is not equals",
                    testStr.equals(resultSet.getNString(2).trim()));
                Assert.assertTrue("getString is not equals",
                    testStr.equals(resultSet.getNString("strm").trim()));
                String testStr1 = testStr.contains(":") ? Timestamp.valueOf(testStr).toString()
                    : testStr + ZERO_TIME;
                Assert.assertTrue("getDate is not equals", Timestamp.valueOf(testStr1).toString()
                    .substring(0, 10).equals(resultSet.getDate(2).toString()));
                Assert.assertTrue("getDate is not equals", Timestamp.valueOf(testStr1).toString()
                    .substring(0, 10).equals(resultSet.getDate("strm").toString()));
                break;
            case "datetime":
                testStr = Timestamp.valueOf(testStr).toString();
            case "datetimeString":
                Assert.assertTrue("getString is not equals",
                    testStr.equals(resultSet.getString(2).trim()));
                Assert.assertTrue("getString is not equals",
                    testStr.equals(resultSet.getString("strm").trim()));
                Assert.assertTrue("getString is not equals",
                    testStr.equals(resultSet.getNString(2).trim()));
                Assert.assertTrue("getString is not equals",
                    testStr.equals(resultSet.getNString("strm").trim()));
                String testStr2 = Timestamp.valueOf(testStr).toString();
                Assert.assertTrue("getTimestamp is not equals",
                    Timestamp.valueOf(testStr2).equals(resultSet.getTimestamp(2)));
                Assert.assertTrue("getTimestamp is not equals",
                    Timestamp.valueOf(testStr2).equals(resultSet.getTimestamp("strm")));
                break;
            case TIMESTAMP_LTZ:
            case TIMESTAMP_TZ:
                testStr = Timestamp.valueOf(testStr).toString();
                Assert.assertTrue("timestampLTZ is not equals",
                    testStr.equals(resultSet.getTimestamp(2).toString()));
                Assert.assertTrue("timestampLTZ is not equals",
                    testStr.equals(resultSet.getTimestamp("strm").toString()));
                //                testStr += " Asia/Shanghai" ;
                Assert.assertTrue(
                    "timestampLTZ is not equals",
                    Timestamp.valueOf(testStr).equals(
                        Timestamp.valueOf(resultSet.getString(2).trim()
                            .substring(0, resultSet.getString(2).trim().lastIndexOf(" ")))));
                Assert.assertTrue(
                    "timestampLTZ is not equals",
                    Timestamp.valueOf(testStr).equals(
                        Timestamp.valueOf(resultSet.getString("strm").trim()
                            .substring(0, resultSet.getString("strm").trim().lastIndexOf(" ")))));
                Assert.assertTrue(
                    "timestampLTZ is not equals",
                    testStr.equals(Timestamp.valueOf(
                        resultSet.getNString(2).trim()
                            .substring(0, resultSet.getNString(2).lastIndexOf(" "))).toString()));
                Assert.assertTrue("timestampLTZ is not equals", testStr.equals(Timestamp.valueOf(
                    resultSet.getNString("strm").trim()
                        .substring(0, resultSet.getNString("strm").trim().lastIndexOf(" ")))
                    .toString()));
                break;
            case TIMESTAMP_LTZ_STRING:
            case TIMESTAMP_TZ_STRING:
                Assert.assertTrue("timestampLTZ is not equals values: " + resultSet.getTimestamp(2)
                                  + " testStr:" + Timestamp.valueOf(testStr),
                    Timestamp.valueOf(testStr).equals(resultSet.getTimestamp(2)));
                Assert.assertTrue("timestampLTZ is not equals",
                    Timestamp.valueOf(testStr).equals(resultSet.getTimestamp("strm")));
                Assert.assertTrue(
                    "getString is not equals",
                    Timestamp.valueOf(testStr).equals(
                        Timestamp.valueOf(resultSet.getString(2).trim())));
                Assert.assertTrue(
                    "getString is not equals",
                    Timestamp.valueOf(testStr).equals(
                        Timestamp.valueOf(resultSet.getString("strm").trim())));
                Assert.assertTrue(
                    "getString is not equals",
                    Timestamp.valueOf(testStr).equals(
                        Timestamp.valueOf(resultSet.getNString(2).trim())));
                Assert.assertTrue(
                    "getString is not equals",
                    Timestamp.valueOf(testStr).equals(
                        Timestamp.valueOf(resultSet.getNString("strm").trim())));
                break;
            case INTERVALYM:
                if (resultSet instanceof ObResultSet) {
                    Assert.assertTrue("getINTERVALYM is not equals",
                        testStr.equals(((ObResultSet) resultSet).getINTERVALYM(2).stringValue()));
                    Assert.assertTrue("getINTERVALYM is not equals", testStr
                        .equals(((ObResultSet) resultSet).getINTERVALYM("strm").stringValue()));
                    Assert.assertTrue("getString is not equals value: "
                                      + resultSet.getString(2).trim(), String.valueOf(testStr)
                        .equals(resultSet.getString(2).trim()));
                    Assert.assertTrue("getString is not equals",
                        String.valueOf(testStr).equals(resultSet.getString("strm").trim()));
                    Assert.assertTrue("getString is not equals",
                        String.valueOf(testStr).equals(resultSet.getNString(2).trim()));
                    Assert.assertTrue("getString is not equals",
                        String.valueOf(testStr).equals(resultSet.getNString("strm").trim()));
                }
                break;
            case INTERVALDS:
                if (resultSet instanceof ObResultSet) {
                    Assert.assertTrue("getINTERVALDS is not equals",
                        testStr.equals(((ObResultSet) resultSet).getINTERVALDS(2).stringValue()));
                    Assert.assertTrue("getINTERVALDS is not equals", testStr
                        .equals(((ObResultSet) resultSet).getINTERVALDS("strm").stringValue()));
                    Assert.assertTrue("getString is not equals value: "
                                      + resultSet.getString(2).trim(), String.valueOf(testStr)
                        .equals(resultSet.getString(2).trim()));
                    Assert.assertTrue("getString is not equals",
                        String.valueOf(testStr).equals(resultSet.getString("strm").trim()));
                    Assert.assertTrue("getString is not equals",
                        String.valueOf(testStr).equals(resultSet.getNString(2).trim()));
                    Assert.assertTrue("getString is not equals",
                        String.valueOf(testStr).equals(resultSet.getNString("strm").trim()));
                }
                break;
            default:
                Assert.assertTrue(
                    "getString is not equals value: " + resultSet.getString(2).trim(), String
                        .valueOf(testStr).equals(resultSet.getString(2).trim()));
                Assert.assertTrue("getString is not equals",
                    String.valueOf(testStr).equals(resultSet.getString("strm").trim()));
                Assert.assertTrue("getString is not equals",
                    String.valueOf(testStr).equals(resultSet.getNString(2).trim()));
                Assert.assertTrue("getString is not equals",
                    String.valueOf(testStr).equals(resultSet.getNString("strm").trim()));
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
                    Float.valueOf(testStr).floatValue() == ((BigDecimal) resultSet.getObject(2))
                        .floatValue());
                Assert.assertTrue("getObject is not equals",
                    Float.valueOf(testStr).floatValue() == ((BigDecimal) resultSet
                        .getObject("strm")).floatValue());
                break;
            case "double":
                Assert.assertTrue("getObject is not equals",
                    Double.valueOf(testStr).doubleValue() == ((BigDecimal) resultSet.getObject(2))
                        .doubleValue());
                Assert.assertTrue("getObject is not equals",
                    Double.valueOf(testStr).doubleValue() == ((BigDecimal) resultSet
                        .getObject("strm")).doubleValue());
                break;
            case "date":
                testStr = testStr.contains(":") ? testStr.substring(0, 10) : testStr;
                Assert.assertTrue("getObject is not equals", testStr.equals(((Timestamp) resultSet
                    .getObject(2)).toString().substring(0, 10)));
                Assert.assertTrue(
                    "getObject is not equals",
                    testStr.equals(((Timestamp) resultSet.getObject("strm")).toString().substring(
                        0, 10)));
                break;
            case "datetime":
                Assert.assertTrue("timestamp getObject is not equals", Timestamp.valueOf(testStr)
                    .equals((Timestamp) resultSet.getObject(2)));
                Assert.assertTrue("timestamp getObject is not equals", Timestamp.valueOf(testStr)
                    .equals((Timestamp) resultSet.getObject("strm")));
                break;
            //            case TIMESTAMP_LTZ :
            //                Assert.assertTrue("timestamp getObject is not equals",new TIMESTAMPLTZ(sharedConnection,Timestamp.valueOf(testStr.substring(0,21))).timestampValue(sharedConnection).equals( ((TIMESTAMPLTZ) resultSet.getObject(2)).timestampValue(sharedConnection) ));
            //                Assert.assertTrue("timestamp getObject is not equals",new TIMESTAMPLTZ(sharedConnection,Timestamp.valueOf(testStr.substring(0,21))).timestampValue(sharedConnection).equals( ((TIMESTAMPLTZ) resultSet.getObject("strm")).timestampValue(sharedConnection) ));
            //                break;
            case "char":
                Assert.assertTrue("getObject is not equals",
                    testStr.equals(((String) resultSet.getObject(2)).trim()));
                Assert.assertTrue("getObject is not equals",
                    testStr.equals(((String) resultSet.getObject("strm")).trim()));
                break;
            case TIMESTAMP_LTZ_STRING:
            case TIMESTAMP_TZ_STRING:
                Assert.assertTrue(
                    "getObject is not equals",
                    Timestamp.valueOf(testStr).equals(
                        Timestamp.valueOf((String) resultSet.getObject(2))));
                Assert.assertTrue(
                    "getObject is not equals",
                    Timestamp.valueOf(testStr).equals(
                        Timestamp.valueOf((String) resultSet.getObject("strm"))));
                Assert.assertTrue(
                    "getObject is not equals",
                    Timestamp.valueOf(testStr).equals(
                        Timestamp.valueOf(resultSet.getObject(2, String.class).trim())));
                Assert.assertTrue(
                    "getObject is not equals",
                    Timestamp.valueOf(testStr).equals(
                        Timestamp.valueOf(resultSet.getObject("strm", String.class).trim())));
                break;
            case TIMESTAMP_LTZ:
                Assert.assertTrue(
                    "getObject is not equals",
                    Timestamp.valueOf(testStr).equals(
                        ((TIMESTAMPLTZ) resultSet.getObject(2)).timestampValue(sharedConnection)));
                Assert.assertTrue(
                    "getObject is not equals",
                    Timestamp.valueOf(testStr).equals(
                        ((TIMESTAMPLTZ) resultSet.getObject("strm"))
                            .timestampValue(sharedConnection)));
                Assert.assertTrue(
                    "getObject is not equals",
                    Timestamp.valueOf(testStr).equals(
                        (resultSet.getObject(2, TIMESTAMPLTZ.class)
                            .timestampValue(sharedConnection))));
                Assert.assertTrue(
                    "getObject is not equals",
                    Timestamp.valueOf(testStr).equals(
                        resultSet.getObject("strm", TIMESTAMPLTZ.class).timestampValue(
                            sharedConnection)));
                break;
            case TIMESTAMP_TZ:
                Assert.assertTrue(
                    "getObject is not equals",
                    Timestamp.valueOf(testStr).equals(
                        ((TIMESTAMPTZ) resultSet.getObject(2)).timestampValue(sharedConnection)));
                Assert.assertTrue(
                    "getObject is not equals",
                    Timestamp.valueOf(testStr).equals(
                        ((TIMESTAMPTZ) resultSet.getObject("strm"))
                            .timestampValue(sharedConnection)));
                Assert.assertTrue(
                    "getObject is not equals",
                    Timestamp.valueOf(testStr)
                        .equals(
                            (resultSet.getObject(2, TIMESTAMPTZ.class)
                                .timestampValue(sharedConnection))));
                Assert.assertTrue(
                    "getObject is not equals",
                    Timestamp.valueOf(testStr).equals(
                        resultSet.getObject("strm", TIMESTAMPTZ.class).timestampValue(
                            sharedConnection)));
                break;
            case INTERVALYM:
                Assert.assertTrue("getObject is not equals",
                    testStr.equals(((INTERVALYM) resultSet.getObject(2)).stringValue().trim()));
                Assert.assertTrue("getObject is not equals",
                    testStr.equals(resultSet.getObject(2, INTERVALYM.class).stringValue().trim()));
                Assert.assertTrue(
                    "getObject is not equals",
                    testStr.equals(resultSet.getObject("strm", INTERVALYM.class).stringValue()
                        .trim()));
                Assert
                    .assertTrue("getObject is not equals", testStr.equals(((INTERVALYM) resultSet
                        .getObject("strm")).stringValue().trim()));
                break;
            case INTERVALDS:
                Assert.assertTrue("getObject is not equals",
                    testStr.equals(((INTERVALDS) resultSet.getObject(2)).stringValue().trim()));
                Assert.assertTrue("getObject is not equals",
                    testStr.equals(resultSet.getObject(2, INTERVALDS.class).stringValue().trim()));
                Assert.assertTrue(
                    "getObject is not equals",
                    testStr.equals(resultSet.getObject("strm", INTERVALDS.class).stringValue()
                        .trim()));
                Assert
                    .assertTrue("getObject is not equals", testStr.equals(((INTERVALDS) resultSet
                        .getObject("strm")).stringValue().trim()));
                break;
            default:
                Assert.assertTrue("getObject is not equals",
                    testStr.equals(((String) resultSet.getObject(2)).trim()));
                Assert.assertTrue("getObject is not equals",
                    testStr.equals(resultSet.getObject(2, String.class).trim()));
                Assert.assertTrue("getObject is not equals",
                    testStr.equals(resultSet.getObject("strm", String.class).trim()));
                Assert.assertTrue("getObject is not equals",
                    testStr.equals(((String) resultSet.getObject("strm")).trim()));
                break;
        }

        try {
            Assert.assertTrue("getObject is not equals", testStr.equals(resultSet.getArray(2)));
            Assert
                .assertTrue("getObject is not equals", testStr.equals(resultSet.getArray("strm")));
        } catch (Exception e) {
            if (e.getClass().getName().contains("SQLFeatureNotSupportedException"))
                System.out.println("SQLFeatureNotSupportedException");
            if (e.getMessage().contains("the field type is not FIELD_TYPE_COMPLEX"))
                System.out.println("SQLFeatureNotSupportedException");
            else
                throw e;
        }

        switch (type) {
            case "datetimeString":
            case "datetime":
                //                Assert.assertFalse(
                //                    "getBytes is not equals",
                //                    Timestamp.valueOf(testStr).equals(
                //                        Timestamp.valueOf(new String(resultSet.getBytes("strm"), Charset
                //                            .forName("UTF-8")).trim())));
                //                Assert.assertFalse(
                //                    "getBytes is not equals",
                //                    Timestamp.valueOf(testStr).equals(
                //                        Timestamp.valueOf(new String(resultSet.getBytes(2)).trim())));
                System.out.println("vlaues:" + new String(resultSet.getBytes("strm")).trim());
                break;
            case "dateString":
            case "date":
                Assert.assertTrue("getDate is not equls",
                    testStr.equals(resultSet.getDate("strm").toString()));
                Assert.assertTrue("getDate is not equls",
                    testStr.equals(resultSet.getDate(2).toString()));
                //                Assert.assertTrue("getBytes is not equals",
                //                    testStr.equals(new String(resultSet.getBytes("strm")).substring(0, 10)));
                //                Assert.assertTrue("getBytes is not equals",
                //                    testStr.equals(new String(resultSet.getBytes(2)).substring(0, 10)));
                break;
            case TIMESTAMP_LTZ:
            case TIMESTAMP_LTZ_STRING:
            case TIMESTAMP_TZ_STRING:
            case TIMESTAMP_TZ:
                System.out.println("timestampLTZ:" + new String(resultSet.getBytes("strm")));
                break;
            case INTERVALYM:
            case INTERVALDS:
                System.out.println("INTERVALYM:" + new String(resultSet.getBytes(2)));
                System.out.println("INTERVALYM:" + new String(resultSet.getBytes("strm")));
                break;
            default:
                Assert.assertTrue("getBytes is not equals",
                    testStr.equals(new String(resultSet.getBytes("strm")).trim()));
                Assert.assertTrue("getBytes is not equals",
                    testStr.equals(new String(resultSet.getBytes(2)).trim()));
        }

        try {
            Assert.assertTrue("getBinaryStream is not equals",
                testStr.equals(((String) resultSet.getRef(2).getObject()).trim()));
            Assert.assertTrue("getBinaryStream is not equals",
                testStr.equals(((String) resultSet.getRef("strm").getObject()).trim()));
        } catch (Exception e) {
            if (type.equals("char")) {
                Assert.assertTrue(
                    "",
                    e.getMessage().contains(
                        "Updates are not supported when using ResultSet.CONCUR_READ_ONLY"));
            } else {
                Assert.assertTrue("",
                    e.getClass().getName().contains("SQLFeatureNotSupportedException"));
            }
        }
    }

}
