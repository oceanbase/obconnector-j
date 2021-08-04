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

import org.junit.Assert;
import org.junit.Test;

public class OceanBaseDatabaseMetaDataTest extends BaseTest {

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
        Statement stmt = null;
        ResultSet rs = null;
        createTable("quote_test", "id int primary key , create_time timestamp");
        stmt = sharedConnection.createStatement();
        stmt.executeUpdate("insert into quote_test values(1,now())");
        rs = stmt.executeQuery("select ID,CREATE_TIME from quote_test");
        assertEquals("id", rs.getMetaData().getColumnName(1)); // Oracle returns uppercase field names Mysql returns lowercase.
        assertEquals("create_time", rs.getMetaData().getColumnName(2));
        assertEquals(true, rs.next());
        assertEquals("`", sharedConnection.getMetaData().getIdentifierQuoteString());
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
}
