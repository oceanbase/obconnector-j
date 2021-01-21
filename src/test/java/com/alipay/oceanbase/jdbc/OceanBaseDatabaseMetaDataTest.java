package com.alipay.oceanbase.jdbc;

import static org.junit.Assert.*;

import java.sql.*;
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
}
