package com.alipay.oceanbase.jdbc;

import com.alipay.oceanbase.jdbc.internal.util.exceptions.ExceptionFactory;
import com.alipay.oceanbase.jdbc.util.Options;
import org.junit.Assert;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.*;

public class ClientPreparedStatementParsingOracleTest extends BaseOracleTest {

    private boolean checkParsing(String sql, int paramNumber, boolean rewritable,
                                 boolean allowMultiqueries, String[] partsRewrite,
                                 String[] partsMulti) throws Exception {
        ExceptionFactory exceptionFactory = ExceptionFactory.of(
            (int) ((OceanBaseConnection) sharedConnection).getServerThreadId(), new Options());
        ClientSidePreparedStatement statement = new ClientSidePreparedStatement(
            (OceanBaseConnection) sharedConnection, sql, ResultSet.FETCH_FORWARD,
            ResultSet.CONCUR_READ_ONLY, Statement.NO_GENERATED_KEYS, exceptionFactory);
        assertEquals(paramNumber, statement.getParameterCount());

        if (sharedIsRewrite()) {
            for (int i = 0; i < partsRewrite.length; i++) {
                assertEquals(partsRewrite[i], new String(statement.getPrepareResult()
                    .getQueryParts().get(i)));
            }
            Assert.assertEquals(rewritable, statement.getPrepareResult()
                .isQueryMultiValuesRewritable());
        } else {
            for (int i = 0; i < partsMulti.length; i++) {
                assertEquals(partsMulti[i], new String(statement.getPrepareResult().getQueryParts()
                    .get(i)));
            }
            Assert.assertEquals(allowMultiqueries, statement.getPrepareResult()
                .isQueryMultipleRewritable());
        }
        return true;
    }

    @Test
    public void stringEscapeParsing() throws Exception {
//        assertTrue(checkParsing("select '\\'' as a, ? as b, \"\\\"\" as c, ? as d", 2, false, true,
//            new String[] { "select '\\'' as a, ", "", " as b, \"\\\"\" as c, ", "", " as d" },
//            new String[] { "select '\\'' as a, ", " as b, \"\\\"\" as c, ", " as d" }));
        assertTrue(checkParsing("select '''' as a, ? as b, '\"' as c, ? as d", 2, false, true,
                new String[] { "select '''' as a, ", "", " as b, '\"' as c, ", "", " as d" },
                new String[] { "select '''' as a, ", " as b, '\"' as c, ", " as d" }));
    }

    @Test
    public void stringEscapeParsingApostrophe() throws Exception {
        assertTrue(checkParsing("INSERT INTO test_varchar_with_symbols VALUES (15, 'PJ>PK:ROf\\', 'PJ>PK?ROf')", 0, false, true,
                new String[] {"INSERT INTO test_varchar_with_symbols VALUES (15, 'PJ>PK:ROf\\', 'PJ>PK?ROf')"},
                new String[] {"INSERT INTO test_varchar_with_symbols VALUES (15, 'PJ>PK:ROf\\', 'PJ>PK?ROf')"}));
    }

    @Test
    public void testRewritableWithConstantParameter() throws Exception {
        assertTrue(checkParsing(
            "INSERT INTO TABLE(col1,col2,col3,col4, col5) VALUES (9, ?, 5, ?, 8) ON DUPLICATE KEY UPDATE col2=col2+10",
            2, true, true, new String[] { "INSERT INTO TABLE(col1,col2,col3,col4, col5) VALUES",
                    " (9, ", ", 5, ", ", 8)", " ON DUPLICATE KEY UPDATE col2=col2+10" },
            new String[] { "INSERT INTO TABLE(col1,col2,col3,col4, col5) VALUES (9, ", ", 5, ",
                    ", 8) ON DUPLICATE KEY UPDATE col2=col2+10" }));
    }

    @Test
    public void testComment() throws Exception {
        assertTrue(checkParsing(
            "/* insert Select INSERT INTO tt VALUES (?,?,?,?)  */" + " INSERT into "
                    + "/* insert Select INSERT INTO tt VALUES (?,?,?,?)  */" + " tt VALUES "
                    + "/* insert Select INSERT INTO tt VALUES (?,?,?,?)  */" + " (?) "
                    + "/* insert Select INSERT INTO tt VALUES (?,?,?,?)  */",
            1,
            true,
            true,
            new String[] {
                    "/* insert Select INSERT INTO tt VALUES (?,?,?,?)  */" + " INSERT into "
                            + "/* insert Select INSERT INTO tt VALUES (?,?,?,?)  */" + " tt VALUES",
                    " /* insert Select INSERT INTO tt VALUES (?,?,?,?)  */ (", ")",
                    " /* insert Select INSERT INTO tt VALUES (?,?,?,?)  */" }, new String[] {
                    "/* insert Select INSERT INTO tt VALUES (?,?,?,?)  */" + " INSERT into "
                            + "/* insert Select INSERT INTO tt VALUES (?,?,?,?)  */"
                            + " tt VALUES "
                            + "/* insert Select INSERT INTO tt VALUES (?,?,?,?)  */" + " (",
                    ") " + "/* insert Select INSERT INTO tt VALUES (?,?,?,?)  */" }));
    }

    @Test
    public void testRewritableWithConstantParameterAndParamAfterValue() throws Exception {
        assertTrue(checkParsing(
            "INSERT INTO TABLE(col1,col2,col3,col4, col5) VALUES (9, ?, 5, ?, 8) ON DUPLICATE KEY UPDATE col2=?",
            3, false, true, new String[] { "INSERT INTO TABLE(col1,col2,col3,col4, col5) VALUES",
                    " (9, ", ", 5, ", ", 8) ON DUPLICATE KEY UPDATE col2=", "", "" }, new String[] {
                    "INSERT INTO TABLE(col1,col2,col3,col4, col5) VALUES (9, ", ", 5, ",
                    ", 8) ON DUPLICATE KEY UPDATE col2=", "" }));
    }

    @Test
    public void testRewritableMultipleInserts() throws Exception {
        assertTrue(checkParsing("INSERT INTO TABLE(col1,col2) VALUES (?, ?), (?, ?)", 4, false,
            true, new String[] { "INSERT INTO TABLE(col1,col2) VALUES", " (", ", ", "), (", ", ",
                    ")", "" }, new String[] { "INSERT INTO TABLE(col1,col2) VALUES (", ", ",
                    "), (", ", ", ")" }));
    }

    @Test
    public void testCall() throws Exception {
        assertTrue(checkParsing("CALL dsdssd(?,?)", 2, false, true, new String[] { "CALL dsdssd(",
                "", ",", ")", "" }, new String[] { "CALL dsdssd(", ",", ")" }));
    }

    @Test
    public void testUpdate() throws Exception {
        assertTrue(checkParsing("UPDATE MultiTestt4 SET test = ? WHERE test = ?", 2, false, true,
            new String[] { "UPDATE MultiTestt4 SET test = ", "", " WHERE test = ", "", "" },
            new String[] { "UPDATE MultiTestt4 SET test = ", " WHERE test = ", "" }));
    }

    @Test
    public void testInsertSelect() throws Exception {
        assertTrue(checkParsing(
            "insert into test_insert_select ( field1) (select  TMP.field1 from "
                    + "(select CAST(? as binary) `field1` from dual) TMP)",
            1,
            false,
            true,
            new String[] {
                    "insert into test_insert_select ( field1) (select  TMP.field1 from (select CAST(",
                    "", " as binary) `field1` from dual) TMP)", "" },
            new String[] {
                    "insert into test_insert_select ( field1) (select  TMP.field1 from (select CAST(",
                    " as binary) `field1` from dual) TMP)" }));
    }

    @Test
    public void testWithoutParameter() throws Exception {
        assertTrue(checkParsing("SELECT testFunction()", 0, false, true, new String[] {
                "SELECT testFunction()", "", "" }, new String[] { "SELECT testFunction()" }));
    }

    @Test
    public void testWithoutParameterAndParenthesis() throws Exception {
        assertTrue(checkParsing("SELECT 1", 0, false, true, new String[] { "SELECT 1", "", "" },
            new String[] { "SELECT 1" }));
    }

    @Test
    public void testWithoutParameterAndValues() throws Exception {
        assertTrue(checkParsing("INSERT INTO tt VALUES (1)", 0, true, true, new String[] {
                "INSERT INTO tt VALUES", " (1)", "" }, new String[] { "INSERT INTO tt VALUES (1)" }));
    }

    @Test
    public void testSemiColon() throws Exception {
        assertTrue(checkParsing(
            "INSERT INTO tt (tt) VALUES (?); INSERT INTO tt (tt) VALUES ('multiple')", 1, false,
            true, new String[] { "INSERT INTO tt (tt) VALUES", " (", ")",
                    "; INSERT INTO tt (tt) VALUES ('multiple')" }, new String[] {
                    "INSERT INTO tt (tt) VALUES (", "); INSERT INTO tt (tt) VALUES ('multiple')" }));
    }

    @Test
    public void testSemicolonRewritableIfAtEnd() throws Exception {
        assertTrue(checkParsing("INSERT INTO table (column1) VALUES (?); ", 1, true, false,
            new String[] { "INSERT INTO table (column1) VALUES", " (", ")", "; " }, new String[] {
                    "INSERT INTO table (column1) VALUES (", "); " }));
    }

    @Test
    public void testSemicolonNotRewritableIfNotAtEnd() throws Exception {
        assertTrue(checkParsing("INSERT INTO table (column1) VALUES (?); SELECT 1", 1, false, true,
            new String[] { "INSERT INTO table (column1) VALUES", " (", ")", "; SELECT 1" },
            new String[] { "INSERT INTO table (column1) VALUES (", "); SELECT 1" }));
    }

    @Test
    public void testError() throws Exception {
        assertTrue(checkParsing(
            "INSERT INTO tt (tt) VALUES (?); INSERT INTO tt (tt) VALUES ('multiple')", 1, false,
            true, new String[] { "INSERT INTO tt (tt) VALUES", " (", ")",
                    "; INSERT INTO tt (tt) VALUES ('multiple')" }, new String[] {
                    "INSERT INTO tt (tt) VALUES (", "); INSERT INTO tt (tt) VALUES ('multiple')" }));
    }

    @Test
    public void testLineComment() throws Exception {
        assertTrue(checkParsing("INSERT INTO tt (tt) VALUES (?) --fin", 1, true, false,
            new String[] { "INSERT INTO tt (tt) VALUES", " (", ")", " --fin" }, new String[] {
                    "INSERT INTO tt (tt) VALUES (", ") --fin" }));
    }

    @Test
    public void testLineCommentFinished() throws Exception {
        assertTrue(checkParsing("INSERT INTO tt (tt) VALUES --fin\n (?)", 1, true, true,
            new String[] { "INSERT INTO tt (tt) VALUES", " --fin\n (", ")", "" }, new String[] {
                    "INSERT INTO tt (tt) VALUES --fin\n (", ")" }));
    }

    @Test
    public void testSelect1() throws Exception {
        assertTrue(checkParsing("SELECT 1", 0, false, true, new String[] { "SELECT 1", "", "" },
            new String[] { "SELECT 1" }));
    }

    @Test
  public void rewriteBatchedError() throws Exception {
    try (Connection connection =
        setConnection("&rewriteBatchedStatements=true&dumpQueriesOnException")) {
      PreparedStatement preparedStatement =
          connection.prepareStatement("INSERT INTO errorTable (a, b) VALUES (?, ?, ?)");

      preparedStatement.setString(1, "1");
      preparedStatement.setString(2, "2");
      preparedStatement.setString(3, "3");
      preparedStatement.addBatch();
      try {
        preparedStatement.executeBatch();
        fail("must have thrown error since parameters are not good");
      } catch (SQLException e) {
        assertTrue(
            e.getCause()
                .getMessage()
                .contains("Query is: INSERT INTO errorTable (a, b) VALUES (?, ?, ?)"));
      }
    }
  }

    @Test
    public void testLastInsertId() throws Exception {
        assertTrue(checkParsing("INSERT INTO tt (tt, tt2) VALUES (LAST_INSERT_ID(), ?)", 1, false,
            true,
            new String[] { "INSERT INTO tt (tt, tt2) VALUES", " (LAST_INSERT_ID(), ", ")", "" },
            new String[] { "INSERT INTO tt (tt, tt2) VALUES (LAST_INSERT_ID(), ", ")" }));
    }

    @Test
    public void testValuesForPartition() throws Exception {
        assertTrue(checkParsing(
            "ALTER table test_partitioning PARTITION BY RANGE COLUMNS( created_at ) "
                    + "(PARTITION test_p201605 VALUES LESS THAN ('2016-06-01'))",
            0,
            false,
            true,
            new String[] {
                    "ALTER table test_partitioning PARTITION BY RANGE COLUMNS( created_at ) "
                            + "(PARTITION test_p201605 VALUES", " LESS THAN ('2016-06-01'))", "" },
            new String[] { "ALTER table test_partitioning PARTITION BY RANGE COLUMNS( created_at ) "
                           + "(PARTITION test_p201605 VALUES LESS THAN ('2016-06-01'))" }));
    }
}
