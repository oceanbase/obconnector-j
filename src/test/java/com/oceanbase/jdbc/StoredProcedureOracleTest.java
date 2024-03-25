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
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

import org.junit.*;

public class StoredProcedureOracleTest extends BaseOracleTest {

    /**
     * Initialisation.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createProcedure("useParameterName", "(a IN OUT int) is begin select a into a from dual; end");
        createProcedure("useWrongParameterName", "(a int) is begin select a from dual; end");
        createProcedure("multiResultSets", " is BEGIN  SELECT 1 from dual; SELECT 2 from dual; END");
        createProcedure("inoutParam", "(p1 IN OUT INT)  is begin p1:= p1 + 1; end\n");
        createProcedure("testGetProcedures", "(p1 IN OUT INT) is begin p1 := p1 + 1; end\n");
        //createProcedure("withStrangeParameter", "(IN a DECIMAL(10,2)) begin select a; end");
        createProcedure("withStrangeParameter", "(a IN OUT number) is begin select a into a from dual; end");
        createProcedure(
                "TEST_SP1",
                " is BEGIN \n"
                        + "SELECT 'Test error from SP' from dual;\n"
                        //        + "SIGNAL SQLSTATE '70100';\n"
                        //        + "SET MESSAGE_TEXT = 'Test error from SP'; \n"
                        + "END");

        // sequence table are not in MySQL and MariaDB < 10.1, so create some basic table
        createTable("table_10", "val int");
        createTable("table_5", "val int");
        if (testSingleHost) {
            try (Statement stmt = sharedConnection.createStatement()) {
                stmt.execute("INSERT INTO table_10 VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)");
                stmt.execute("INSERT INTO table_5 VALUES (1),(2),(3),(4),(5)");
            }
        }
    }

    @Test
    public void testAdd() throws Exception {
        createProcedure(
                "calc_add_2",
                "(a1 IN int, a2 IN int, a3 OUT int) is "
                        + "begin " + "  a3:=a1+a2; " + "end;"
        );
        try (CallableStatement callableStatement = sharedConnection.prepareCall("call calc_add_2(?, ?, ?)")) {
            callableStatement.setInt(1, 1);
            callableStatement.setInt(2, 2);
            callableStatement.registerOutParameter(3, Types.INTEGER);
            callableStatement.execute();
            //System.out.println(callableStatement.getInt(2));
            ResultSet rs = callableStatement.getResultSet();
            System.out.println("rs is:" + callableStatement.getDouble(3));
            if (rs!= null && rs.next()) {
                System.out.println("THE RESULT IS:" + rs.getInt(3));
            }
      /*
      while (rs.next()) {
        System.out.println("TT : " + rs.getInt(2));
      } */
        }
    }

    @Test
    public void testStoreProcedureStreaming() throws Exception {
        // aurora doesn't send back output results parameter when having SELECT results, even with flag
        // enabled
        Assume.assumeFalse(sharedIsAurora());

        // cancel for version 10.2 beta before fix https://jira.mariadb.org/browse/MDEV-11761
        cancelForVersion(10, 2, 2);
        cancelForVersion(10, 2, 3);
        cancelForVersion(10, 2, 4);

        createProcedure(
                "StoredWithOutput",
                "(MAX_PARAM out int, MIN_PARAM out int, NULL_PARAM out int)"
                        + "is begin select 1,0,2 into MAX_PARAM, MIN_PARAM, NULL_PARAM from dual; end");
        //+ "is begin select 1,0,2 into MAX_PARAM, MIN_PARAM, NULL_PARAM from dual; SELECT * from table_10; SELECT * from table_5; end");
        //SELECT * from table_10; SELECT * from table_5;

        try (CallableStatement callableStatement =
                     sharedConnection.prepareCall("{call StoredWithOutput(?,?,?)}")) {
            // indicate to stream results
            callableStatement.setFetchSize(1);

            callableStatement.registerOutParameter(1, Types.INTEGER);
            callableStatement.registerOutParameter(2, Types.INTEGER);
            callableStatement.registerOutParameter(3, Types.INTEGER);
            callableStatement.execute();

            assertEquals(1, callableStatement.getInt(1));
            assertEquals(0, callableStatement.getInt(2));
            assertEquals(2, callableStatement.getInt(3));

      /*
      ResultSet rs = callableStatement.getResultSet();
      for (int i = 1; i <= 10; i++) {
        assertTrue(rs.next());
        assertEquals(i, rs.getInt(1));
      }
      assertFalse(rs.next());

      // force reading of all result-set since output parameter are in the end.
      assertEquals(true, callableStatement.getBoolean(1));
      assertEquals(false, callableStatement.getBoolean(2));
      assertEquals(false, callableStatement.getBoolean(3));

      assertTrue(callableStatement.getMoreResults());

      rs = callableStatement.getResultSet();
      for (int i = 1; i <= 5; i++) {
        assertTrue(rs.next());
        assertEquals(i, rs.getInt(1));
      }
      assertFalse(rs.next()); */
        }
    }

    @Test
    public void testStoreProcedureStreamingWithAnotherQuery() throws Exception {
        // aurora doesn't send back output results parameter when having SELECT results, even with flag
        // enabled
        Assume.assumeFalse(sharedIsAurora());

        // cancel for version 10.2 beta before fix https://jira.mariadb.org/browse/MDEV-11761
        cancelForVersion(10, 2, 2);
        cancelForVersion(10, 2, 3);
        cancelForVersion(10, 2, 4);

        createProcedure(
                "StreamInterrupted",
                "( MAX_PARAM out SMALLINT,  MIN_PARAM out SMALLINT,  NULL_PARAM out SMALLINT)"
                        + " is begin select 1,0,null into MAX_PARAM, MIN_PARAM, NULL_PARAM from dual;end");

        try (CallableStatement callableStatement =
                     sharedConnection.prepareCall("{call StreamInterrupted(?,?,?)}")) {
            // indicate to stream results
            callableStatement.setFetchSize(1);

            callableStatement.registerOutParameter(1, Types.BIT);
            callableStatement.registerOutParameter(2, Types.BIT);
            callableStatement.registerOutParameter(3, Types.BIT);

            callableStatement.execute();

            ResultSet rs = callableStatement.getResultSet();
            assertTrue(rs == null);
            assertEquals(1, callableStatement.getInt(1));

            // execute another query on same connection must force loading of
            // existing streaming result-set
            try (Statement stmt = sharedConnection.createStatement()) {
                ResultSet otherRs = stmt.executeQuery("SELECT 'test' from dual");
                assertTrue(otherRs.next());
                assertEquals("test", otherRs.getString(1));
            }

      /*
      for (int i = 2; i <= 10; i++) {
        assertTrue(rs.next());
        assertEquals(i, rs.getInt(1));
      }
      assertFalse(rs.next()); */


            assertEquals(true, callableStatement.getBoolean(1));
            assertEquals(false, callableStatement.getBoolean(2));
            assertEquals(false, callableStatement.getBoolean(3));

            // force reading of all result-set since output parameter are in the end.
            //assertTrue(callableStatement.getMoreResults());

      /*
      rs = callableStatement.getResultSet();
      for (int i = 1; i <= 5; i++) {
        assertTrue(rs.next());
        assertEquals(i, rs.getInt(1));
      }
      assertFalse(rs.next()); */
        }
    }

    @Test
    public void testStoreProcedureStreamingWithoutOutput() throws Exception {
        createProcedure(
                "StreamWithoutOutput",
                "(MAX_PARAM IN OUT INT)" + "is begin SELECT MAX_PARAM into MAX_PARAM from dual ;end");

        try (CallableStatement callableStatement =
                     sharedConnection.prepareCall("{call StreamWithoutOutput(?)}")) {
            // indicate to stream results
            callableStatement.setFetchSize(1);
            callableStatement.setInt(1, 100);
            callableStatement.registerOutParameter(1, Types.INTEGER);
            callableStatement.execute();

            ResultSet rs = callableStatement.getResultSet();

            assertEquals(100, callableStatement.getInt(1));
/*      assertTrue(rs.next());
      assertEquals(100, rs.getInt(1)); */
      /*
      for (int i = 1; i <= 10; i++) {
        assertTrue(rs.next());
        assertEquals(i, rs.getInt(1));
      }
      assertFalse(rs.next());

      assertTrue(callableStatement.getMoreResults());

      rs = callableStatement.getResultSet();
      for (int i = 1; i <= 5; i++) {
        assertTrue(rs.next());
        assertEquals(i, rs.getInt(1));
      }
      assertFalse(rs.next()); */
        }
    }

    @Before
    public void checkSp() throws SQLException {
        requireMinimumVersion(5, 0);
    }

    @Ignore
    //TODO oracle not support
    @Test
    public void callSimple() throws SQLException {
        CallableStatement st = sharedConnection.prepareCall("{? = call pow(?,?)}");
        st.setInt(2, 2);
        st.setInt(3, 2);
        st.execute();
        int result = st.getInt(1);
        assertEquals(result, 4);
    }

    @Ignore
    //TODO oracle mode not support
    @Test
    public void callSimpleWithNewlines() throws SQLException {
        // Violates JDBC spec, but MySQL Connector/J allows it
        //CallableStatement st = sharedConnection.prepareCall("{\r\n ? =  call pow(?,  ?  )   }");
        CallableStatement st = sharedConnection.prepareCall("{\r\n ? =  call pow(?,  ?  )   }");
        st.setInt(2, 2);
        st.setInt(3, 2);
        st.execute();
        int result = st.getInt(1);
        assertEquals(result, 4);

        st = sharedConnection.prepareCall("{\n ? = call pow(?, ?)}");
        st.setInt(2, 2);
        st.setInt(3, 2);
        st.execute();
        result = st.getInt(1);
        assertEquals(result, 4);

        st = sharedConnection.prepareCall("{? = call pow  (\n?, ?  )}");
        st.setInt(2, 2);
        st.setInt(3, 2);
        st.execute();
        result = st.getInt(1);
        assertEquals(result, 4);

        st = sharedConnection.prepareCall("\r\n{\r\n?\r\n=\r\ncall\r\npow\r\n(\n?,\r\n?\r\n)\r\n}");
        st.setInt(2, 2);
        st.setInt(3, 2);
        st.execute();
        result = st.getInt(1);
        assertEquals(result, 4);
    }

    @Test
    public void callWithOutParameter() throws SQLException {
        // cancel for version 10.2 beta before fix https://jira.mariadb.org/browse/MDEV-11761
        cancelForVersion(10, 2, 2);
        cancelForVersion(10, 2, 3);
        cancelForVersion(10, 2, 4);

        createProcedure("prepareStmtWithOutParameter", "(x int, y IN OUT int)\n" + " IS BEGIN\n"
                                                       + "y := y;end\n");
        CallableStatement callableStatement = sharedConnection
            .prepareCall("{call prepareStmtWithOutParameter(?,?)}");
        callableStatement.registerOutParameter(2, Types.INTEGER);
        callableStatement.setInt(1, 2);
        callableStatement.setInt(2, 3);
        callableStatement.execute();
        assertEquals(3, callableStatement.getInt(2));
    }

    @Test
    public void callWithResultSet() throws Exception {
        createProcedure("withResultSet", "(a IN OUT int) is begin select a into a from dual; end");
        CallableStatement stmt = sharedConnection.prepareCall("{call withResultSet(?)}");
        stmt.setInt(1, 1);
        stmt.registerOutParameter(1, Types.INTEGER);
        //stmt.setFetchSize(1);
        stmt.execute();
        /*
        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        int res = rs.getInt(1);
        assertEquals(res, 1); */
        assertEquals(1, stmt.getInt(1));
    }

    @Test
    public void callUseParameterName() throws Exception {
        CallableStatement stmt = sharedConnection.prepareCall("{call useParameterName(?)}");
        stmt.setInt("a", 1);
        stmt.registerOutParameter(1, Types.INTEGER);
        ResultSet rs = stmt.executeQuery();
        //assertTrue(rs == null);
        int res = stmt.getInt(1);
        assertEquals(res, 1);
    }

    @Test(expected = SQLException.class)
    public void callUseWrongParameterName() throws Exception {
        CallableStatement stmt = sharedConnection.prepareCall("{call useParameterName(?)}");
        stmt.setInt("b", 1);
        fail("must fail");
    }

    @Ignore
    //TODO Oracle mode not support
    @Test
    public void callMultiResultSets() throws Exception {
        executeAndCheckResult(sharedConnection.prepareCall("{call multiResultSets()}"));
    }

    @Ignore
    //TODO Oracle mode not support
    @Test
    public void prepareMultiResultSets() throws Exception {
        executeAndCheckResult(sharedConnection.prepareStatement("{call multiResultSets()}"));
    }

    private void executeAndCheckResult(PreparedStatement stmt) throws Exception {
        stmt.execute();
        ResultSet rs = stmt.getResultSet();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertFalse(rs.next());
        assertTrue(stmt.getMoreResults());
        rs = stmt.getResultSet();
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertFalse(rs.next());
    }

    @Test
    public void callInoutParam() throws SQLException {
        // cancel for version 10.2 beta before fix https://jira.mariadb.org/browse/MDEV-11761
        cancelForVersion(10, 2, 2);
        cancelForVersion(10, 2, 3);
        cancelForVersion(10, 2, 4);

        CallableStatement storedProc = sharedConnection.prepareCall("{call inOutParam(?)}");
        storedProc.registerOutParameter(1, Types.INTEGER);
        storedProc.setInt(1, 1);
        storedProc.execute();
        assertEquals(2, storedProc.getObject(1));
    }

    @Test
    public void callWithStrangeParameter() throws SQLException {
        try (CallableStatement stmt = sharedConnection.prepareCall("{call withStrangeParameter(?)}")) {
            double expected = 5.43;
            stmt.setDouble("a", expected);
            stmt.registerOutParameter(1, Types.DOUBLE);
            try (ResultSet rs = stmt.executeQuery()) {

                double res = stmt.getDouble(1);
                assertEquals(expected, res, 0);
                double tooMuch = 34.987;
                stmt.setDouble(1, tooMuch);
                stmt.registerOutParameter(1, Types.DOUBLE);
                try (ResultSet rs2 = stmt.executeQuery()) {
                    //TODO change number to number(10, 2);
                    //assertNotEquals(stmt.getDouble(1), tooMuch);
                }
            }

      /*
      try (ResultSet rs = stmt.executeQuery()) {
        assertTrue(rs.next());
        double res = rs.getDouble(1);
        assertEquals(expected, res, 0);
        // now fail due to three decimals
        double tooMuch = 34.987;
        stmt.setDouble("a", tooMuch);
        try (ResultSet rs2 = stmt.executeQuery()) {
          assertTrue(rs2.next());
          assertNotEquals(rs2.getDouble(1), tooMuch);
        }
      }*/
        }
    }

    @Ignore
    //TODO not support now, need to support next
    @Test
    public void meta() throws Exception {
        createProcedure("callabletest1", "()\n is BEGIN\n SELECT 1 from dual;end\n");
        ResultSet rs = sharedConnection.getMetaData().getProcedures(null, null, "callabletest1");
        if (rs.next()) {
            assertTrue("callabletest1".equals(rs.getString(3)));
        } else {
            fail();
        }
    }

    @Ignore //TODO check create user or grant privilege for account
    @Test
    public void testMetaCatalogNoAccessToProcedureBodies() throws Exception {
        // cancel for version 10.2 beta before fix https://jira.mariadb.org/browse/MDEV-11761
        cancelForVersion(10, 2, 2);
        cancelForVersion(10, 2, 3);
        cancelForVersion(10, 2, 4);

        Statement statement = sharedConnection.createStatement();
        try {
            statement.execute("DROP USER 'test_jdbc'@'%'");
        } catch (SQLException e) {
            // eat exception
        }
        statement.execute("CREATE USER 'test_jdbc'@'%' IDENTIFIED BY 'testJ@dc1'");
        statement.execute(
                "GRANT SELECT, EXECUTE  ON "
                        + database
                        + ".* TO 'test_jdbc'@'%' IDENTIFIED BY 'testJ@dc1' WITH GRANT OPTION");
        Properties properties = new Properties();
        properties.put("user", "test_jdbc");
        properties.put("password", "testJ@dc1");

        createProcedure("testMetaCatalog", "(x int, out y int)\nBEGIN\nSET y = 2;\n end\n");

        try (Connection connection = openConnection(connU, properties)) {
            CallableStatement callableStatement = connection.prepareCall("{call testMetaCatalog(?, ?)}");
            callableStatement.registerOutParameter(2, Types.INTEGER);
            try {
                callableStatement.setString("x", "1");
                fail("Set by named must not succeed");
            } catch (SQLException sqlException) {
                assertTrue(
                        sqlException
                                .getMessage()
                                .startsWith("Access to metaData informations not granted for current user"));
            }
            callableStatement.setString(1, "1");
            callableStatement.execute();
            try {
                callableStatement.getInt("y");
                fail("Get by named must not succeed");
            } catch (SQLException sqlException) {
                assertTrue(
                        sqlException
                                .getMessage()
                                .startsWith("Access to metaData informations not granted for current user"));
            }
            assertEquals(2, callableStatement.getInt(2));

            ResultSet resultSet =
                    connection.getMetaData().getProcedures("yahoooo", null, "testMetaCatalog");
            assertFalse(resultSet.next());

            // test without catalog
            resultSet = connection.getMetaData().getProcedures(null, null, "testMetaCatalog");
            if (resultSet.next()) {
                assertTrue("testMetaCatalog".equals(resultSet.getString(3)));
                assertFalse(resultSet.next());
            } else {
                fail();
            }
        } catch (SQLInvalidAuthorizationSpecException authentication) {
            // MySQL 5.5 doesn't permit 'test_jdbc'@'localhost'
        }
        statement.execute("DROP USER 'test_jdbc'@'%'");
    }

    //@Ignore /* not support */
    @Test
    public void testSameProcedureWithDifferentParameters() throws Exception {
        //sharedConnection.createStatement().executeUpdate("CREATE DATABASE IF NOT EXISTS testj2");

        createProcedure(
                "testSameProcedureWithDifferentParameters1",
                "(p1 OUT VARCHAR2,  p2 IN VARCHAR2)\n IS BEGIN" + "\n select 1 INTO p1 FROM DUAL;" + "\nEND");

        createProcedure(
                "testSameProcedureWithDifferentParameters2",
                "( p1 OUT VARCHAR2)\n IS BEGIN" + "\n select 2 INTO p1 FROM DUAL;" + "\nEND");

        try (CallableStatement callableStatement =
                     sharedConnection.prepareCall("{ call testSameProcedureWithDifferentParameters1(?, ?) }")) {
            callableStatement.registerOutParameter(1, Types.VARCHAR);
            callableStatement.setString(2, "mike");
            callableStatement.execute();
        }
        //sharedConnection.setCatalog("testj2");
        try (CallableStatement callableStatement =
                     sharedConnection.prepareCall("{ call test.testSameProcedureWithDifferentParameters1(?, ?) }")) {
            callableStatement.registerOutParameter(1, Types.VARCHAR);
            callableStatement.setString(2, "mike");
            try {
                callableStatement.execute();
                //fail("Should've thrown an exception");
            } catch (SQLException sqlEx) {
                assertEquals("42000", sqlEx.getSQLState());
            }
        }

        try (CallableStatement callableStatement =
                     sharedConnection.prepareCall("{ call testSameProcedureWithDifferentParameters2(?) }")) {
            callableStatement.registerOutParameter(1, Types.VARCHAR);
            callableStatement.execute();
        }
        //sharedConnection.setCatalog("testj");
        //sharedConnection.createStatement().executeUpdate("DROP DATABASE testj2");
    }

    @Test
    public void testProcDecimalComa() throws Exception {
        createProcedure(
                "testProcDecimalComa",
                "(decimalParam OUT DECIMAL)\n" + "IS BEGIN\n" + "   SELECT decimalParam INTO decimalParam from dual;\n" + "END");
        try (CallableStatement callableStatement =
                     sharedConnection.prepareCall("Call testProcDecimalComa(?)")) {
            callableStatement.setDouble(1, 18.0);
            callableStatement.registerOutParameter(1, Types.DECIMAL);
            callableStatement.execute();
        }
    }

    //@Ignore //TODO support in next.
    @Test
    public void testFunctionCall() throws Exception {
        createFunction("testFunctionCall",
            "(a float, b NUMBER, c int) RETURN INT \nIS BEGIN\nRETURN a;\nEND");
        CallableStatement callableStatement = sharedConnection
            .prepareCall("{? = CALL testFunctionCall(?,?,?)}");
        callableStatement.registerOutParameter(1, Types.INTEGER);
        callableStatement.setFloat(2, 2);
        callableStatement.setInt(3, 1);
        callableStatement.setInt(4, 1);

        //ojdbc is 4
        assertEquals(4, callableStatement.getParameterMetaData().getParameterCount());
        //assertEquals(Types.INTEGER, callableStatement.getParameterMetaData().getParameterType(1));
        ResultSetMetaData rsmd = null;
        DatabaseMetaData dbmd = null;
        /*
        DatabaseMetaData dbmd = sharedConnection.getMetaData();

        ResultSet rs =
            dbmd.getFunctionColumns(sharedConnection.getCatalog(), null, "testFunctionCall", "%");
        ResultSetMetaData rsmd = rs.getMetaData();

        assertEquals(17, rsmd.getColumnCount());
        assertEquals("FUNCTION_CAT", rsmd.getColumnName(1));
        assertEquals("FUNCTION_SCHEM", rsmd.getColumnName(2));
        assertEquals("FUNCTION_NAME", rsmd.getColumnName(3));
        assertEquals("COLUMN_NAME", rsmd.getColumnName(4));
        assertEquals("COLUMN_TYPE", rsmd.getColumnName(5));
        assertEquals("DATA_TYPE", rsmd.getColumnName(6));
        assertEquals("TYPE_NAME", rsmd.getColumnName(7));
        assertEquals("PRECISION", rsmd.getColumnName(8));
        assertEquals("LENGTH", rsmd.getColumnName(9));
        assertEquals("SCALE", rsmd.getColumnName(10));
        assertEquals("RADIX", rsmd.getColumnName(11));
        assertEquals("NULLABLE", rsmd.getColumnName(12));
        assertEquals("REMARKS", rsmd.getColumnName(13));
        assertEquals("CHAR_OCTET_LENGTH", rsmd.getColumnName(14));
        assertEquals("ORDINAL_POSITION", rsmd.getColumnName(15));
        assertEquals("IS_NULLABLE", rsmd.getColumnName(16));
        assertEquals("SPECIFIC_NAME", rsmd.getColumnName(17));

        rs.close(); */

        assertTrue(callableStatement.execute());
        assertEquals(2f, callableStatement.getInt(1), .001);
        //assertEquals("java.lang.Integer", callableStatement.getObject(1).getClass().getName());

        assertEquals(0, callableStatement.executeUpdate());
        assertEquals(2f, callableStatement.getInt(1), .001);
        //assertEquals("java.lang.Integer", callableStatement.getObject(1).getClass().getName());

        callableStatement.setFloat("a", 4);
        callableStatement.setInt("b", 1);
        callableStatement.setInt("c", 1);

        assertTrue(callableStatement.execute());
        assertEquals(4f, callableStatement.getInt(1), .001);
        //assertEquals("java.lang.Integer", callableStatement.getObject(1).getClass().getName());

        assertEquals(0, callableStatement.executeUpdate());
        assertEquals(4f, callableStatement.getInt(1), .001);
        //assertEquals("java.lang.Integer", callableStatement.getObject(1).getClass().getName());

        /*
        ResultSet rs = dbmd.getProcedures(sharedConnection.getCatalog(), null, "testFunctionCall");
        assertTrue(rs.next());
        assertEquals("testFunctionCall", rs.getString("PROCEDURE_NAME"));
        assertEquals(DatabaseMetaData.procedureReturnsResult, rs.getShort("PROCEDURE_TYPE")); */
        callableStatement.setNull(2, Types.FLOAT);
        callableStatement.setInt(3, 1);
        callableStatement.setInt(4, 1);

        assertTrue(callableStatement.execute());
        assertEquals(0f, callableStatement.getInt(1), .001);
        assertEquals(true, callableStatement.wasNull());
        assertEquals(null, callableStatement.getObject(1));
        assertEquals(true, callableStatement.wasNull());

        assertEquals(0, callableStatement.executeUpdate());
        assertEquals(0f, callableStatement.getInt(1), .001);
        assertEquals(true, callableStatement.wasNull());
        assertEquals(null, callableStatement.getObject(1));
        assertEquals(true, callableStatement.wasNull());

        callableStatement = sharedConnection.prepareCall("{? = CALL testFunctionCall(4,5,?)}");
        callableStatement.registerOutParameter(1, Types.INTEGER);
        callableStatement.setInt(2, 1);

        assertTrue(callableStatement.execute());
        assertEquals(4f, callableStatement.getInt(1), .001);
        //assertEquals("java.lang.Integer", callableStatement.getObject(1).getClass().getName());

        assertEquals(0, callableStatement.executeUpdate());
        assertEquals(4f, callableStatement.getInt(1), .001);
        //assertEquals("java.lang.Integer", callableStatement.getObject(1).getClass().getName());

        assertEquals(3, callableStatement.getParameterMetaData().getParameterCount());
        /*
        assertEquals(Types.INTEGER, callableStatement.getParameterMetaData().getParameterType(1));
        assertEquals(Types.FLOAT, callableStatement.getParameterMetaData().getParameterType(2));
        assertEquals(Types.BIGINT, callableStatement.getParameterMetaData().getParameterType(3));
        assertEquals(Types.INTEGER, callableStatement.getParameterMetaData().getParameterType(4)); */
    }

    @Ignore // not support
    @Test
    public void testCallOtherDb() throws Exception {
        sharedConnection.createStatement().executeUpdate("CREATE DATABASE IF NOT EXISTS testj2");
        createProcedure("testj2.otherDbProcedure", "()\nBEGIN\nSELECT 1;\nEND ");

        try (Connection noDbConn = setConnection()) {
            noDbConn.prepareCall("{call `testj2`.otherDbProcedure()}").execute();
        }
        sharedConnection.createStatement().executeUpdate("DROP DATABASE testj2");
    }

    @Test
    public void testMultiResultset() throws Exception {
        // cancel for version 10.2 beta before fix https://jira.mariadb.org/browse/MDEV-11761
        cancelForVersion(10, 2, 2);
        cancelForVersion(10, 2, 3);
        cancelForVersion(10, 2, 4);

        createProcedure(
                "testInOutParam",
                "(p1 IN VARCHAR2, p2 IN OUT INT)\n"
                        + "is\n"
                        +  " z int:=0;\n"
                        + " begin\n"
                        + "  z :=0;\n"
                        + " z := p2 + 1;\n"
                        + " p2 := z;\n"
                        // + " SELECT p1 from dual;\n"
                        // + " SELECT CONCAT('todo ', p1) from dual;\n"
                        + "end");
        try (CallableStatement callableStatement =
                     sharedConnection.prepareCall("{call testInOutParam(?, ?)}")) {
            callableStatement.registerOutParameter(2, Types.INTEGER);
            callableStatement.setString(1, "test");
            callableStatement.setInt(2, 1);
            ResultSet resultSet = callableStatement.executeQuery();
            assertEquals(2, callableStatement.getInt(2));
            //assertEquals("test", callableStatement.getString(1));
      /*
      if (resultSet.next()) {
        assertEquals("test", resultSet.getString(1));
      } else {
        fail("must have resultset");
      }
      assertTrue(callableStatement.getMoreResults());

      resultSet = callableStatement.getResultSet();
      if (resultSet.next()) {
        assertEquals("todo test", resultSet.getString(1));
      } else {
        fail("must have resultset");
      } */
        }
    }

    @Ignore
    //TODO need support in next
    @Test
    public void callFunctionWithNoParameters() throws SQLException {
        createFunction("callFunctionWithNoParameters", "()\n"
                                                       + "    RETURNS CHAR(50) DETERMINISTIC\n"
                                                       + "    RETURN 'mike';");

        CallableStatement callableStatement = sharedConnection
            .prepareCall("{? = call callFunctionWithNoParameters()}");
        callableStatement.registerOutParameter(1, Types.VARCHAR);
        callableStatement.execute();
        assertEquals("mike", callableStatement.getString(1));
    }

    //@Ignore //TODO support next
    @Test
    public void testFunctionWith2parameters() throws SQLException {
        createFunction("testFunctionWith2parameters", "(s VARCHAR, s2 VARCHAR)\n"
                                                      + "    RETURN VARCHAR2  IS BEGIN\n"
                                                      + "    RETURN s + s2; END;");
        //+ "    RETURN CONCAT(s,' and ', s2)");

        CallableStatement callableStatement = sharedConnection
            .prepareCall("{? = call testFunctionWith2parameters(?, ?)}");
        callableStatement.registerOutParameter(1, Types.VARCHAR);
        callableStatement.setString(2, "mike");
        callableStatement.setString(3, "bart");
        callableStatement.execute();
        assertEquals("mike and bart", callableStatement.getString(1));
    }

    //@Ignore //TODO support function in next
    @Test
    public void testFunctionWithFixedParameters() throws SQLException {
        createFunction("testFunctionWith2parameters",
            "(s VARCHAR2, s2 VARCHAR2)\n" + "    RETURN VARCHAR2 \n"
                    + "  IS  BEGIN RETURN CONCAT(s,' and ', s2); END;");

        /*
        CallableStatement callableStatement =
            sharedConnection.prepareCall("{? = call testFunctionWith2parameters('mike', ?)}"); *///TODO not support
        CallableStatement callableStatement = sharedConnection
            .prepareCall("{? = call testFunctionWith2parameters(?, ?)}");
        callableStatement.registerOutParameter(1, Types.VARCHAR);
        callableStatement.setString(2, "mike");
        callableStatement.setString(3, "bart");
        callableStatement.execute();
        assertEquals("mike and bart", callableStatement.getString(1));
    }

    @Test
    public void testResultsetWithInoutParameter() throws Exception {
        // cancel for version 10.2 beta before fix https://jira.mariadb.org/browse/MDEV-11761
        cancelForVersion(10, 2, 2);
        cancelForVersion(10, 2, 3);
        cancelForVersion(10, 2, 4);

        createTable("testResultsetWithInoutParameterTb", "test VARCHAR2(10)");
        createProcedure("testResultsetWithInoutParameter",
            "( testValue IN OUT VARCHAR2)\n" + " IS BEGIN\n"
                    + " insert into testResultsetWithInoutParameterTb(test) values (testValue);\n"
                    + " SELECT testValue into testValue from dual;\n"
                    + " testValue := UPPER(testValue);\n" + "END");
        CallableStatement cstmt = sharedConnection
            .prepareCall("{call testResultsetWithInoutParameter(?)}");
        cstmt.registerOutParameter(1, Types.VARCHAR);
        cstmt.setString(1, "mike");
        // assertEquals(1, cstmt.executeUpdate());
        cstmt.executeUpdate();
        assertEquals("MIKE", cstmt.getString(1));
        // assertTrue(cstmt.getMoreResults());
        ResultSet resultSet = cstmt.getResultSet();
        /*if (resultSet.next()) {
          assertEquals("mike", resultSet.getString(1));
        } else {
          fail("must have a resultset corresponding to the SELECT testValue");
        } */
        assertNull(resultSet);
        assertEquals("MIKE", cstmt.getString(1));
        ResultSet rs = sharedConnection.createStatement().executeQuery(
            "SELECT * FROM testResultsetWithInoutParameterTb");
        if (rs.next()) {
            assertEquals("mike", rs.getString(1));
        } else {
            fail();
        }
    }

    @Ignore //TODO ObServer not support
    @Test
    public void testSettingFixedParameter() throws SQLException {
        // cancel for version 10.2 beta before fix https://jira.mariadb.org/browse/MDEV-11761
        cancelForVersion(10, 2, 2);
        cancelForVersion(10, 2, 3);
        cancelForVersion(10, 2, 4);

        createProcedure(
                "simpleproc",
                "(inParam IN VARCHAR2,  inOutParam IN OUT VARCHAR2,  outParam OUT VARCHAR2)"
                        + "    IS BEGIN\n"
                        + "         inOutParam := UPPER(inOutParam);\n"
                        + "         outParam := CONCAT('Hello, ', inOutParam, ' and ', inParam);"
                        //+ "         SELECT 'a' FROM DUAL;\n"
                        + "     END;");

        try (CallableStatement callableStatement =
                     sharedConnection.prepareCall("{call simpleproc('mike', ?, ?)}")) {
            callableStatement.registerOutParameter(1, Types.VARCHAR);
            callableStatement.registerOutParameter(2, Types.VARCHAR);
            callableStatement.setString(1, "toto");
            callableStatement.execute();
            String result = callableStatement.getString(1);
            String result2 = callableStatement.getString(2);
            if (!"TOTO".equals(result) && !"Hello, TOTO and mike".equals(result2)) {
                fail();
            }
            callableStatement.close();
        }
    }

    @Ignore
    //TODO need to delete on need any more
    @Test
    public void testNoParenthesisCall() throws Exception {
        createProcedure("testProcedureParenthesis", "() IS BEGIN SELECT 1; END");
        createFunction("testFunctionParenthesis", "() RETURNS INT DETERMINISTIC RETURN 1;");
        sharedConnection.prepareCall("{CALL testProcedureParenthesis}").execute();
        sharedConnection.prepareCall("{? = CALL testFunctionParenthesis}").execute();
    }

    @Ignore
    //TODO not support in Oracle
    @Test
    public void testProcLinefeed() throws Exception {
        createProcedure("testProcLinefeed", "(\r\n)\r\n BEGIN SELECT 1; END");
        CallableStatement callStmt = sharedConnection.prepareCall("{CALL testProcLinefeed()}");
        callStmt.execute();

        sharedConnection.createStatement().executeUpdate(
            "DROP PROCEDURE IF EXISTS testProcLinefeed");
        sharedConnection.createStatement().executeUpdate(
            "CREATE PROCEDURE testProcLinefeed(\r\na INT)\r\n BEGIN SELECT 1; END");
        callStmt = sharedConnection.prepareCall("{CALL testProcLinefeed(?)}");
        callStmt.setInt(1, 1);
        callStmt.execute();
    }

    @Test
    public void testHugeNumberOfParameters() throws Exception {
        StringBuilder procDef = new StringBuilder("(");
        StringBuilder param = new StringBuilder();
        //TODO if i == 274, OB Server overflow: internal error code, arguments: -4019, Size overflow
        int num = 257; //MAX SIZE supported is 257
        for (int i = 0; i < num; i++) {
            if (i != 0) {
                procDef.append(",");
                param.append(",");
            }

            procDef.append(" param_").append(i).append(" OUT VARCHAR2");
            param.append("?");
        }

        procDef.append(")\nis BEGIN\nSELECT 1 into param_0 from dual;\nEND;");
        //System.out.println(procDef.toString());
        createProcedure("testHugeNumberOfParameters", procDef.toString());

        try (CallableStatement callableStatement =
                     sharedConnection.prepareCall(
                             "{call testHugeNumberOfParameters(" + param.toString() + ")}")) {
      /*
      for (int i = 1; i <= num; i++) {
        callableStatement.registerOutParameter(i, Types.VARCHAR);
      } */
            try {
                callableStatement.execute();
                Assert.fail();
            } catch (SQLException e) {
                Assert.assertTrue(e.getMessage().contains("Missing IN or OUT parameter in index::1"));
            }
        }
    }

    @Ignore //TODO not support send long data use SEND_LONG_DATA
    @Test
    public void testStreamInOutWithName() throws Exception {
        // cancel for version 10.2 beta before fix https://jira.mariadb.org/browse/MDEV-11761
        cancelForVersion(10, 2, 2);
        cancelForVersion(10, 2, 3);
        cancelForVersion(10, 2, 4);

        createProcedure(
                "testStreamInOutWithName",
                "( mblob IN OUT BLOB) IS BEGIN SELECT 1 INTO mblob FROM DUAL WHERE 1=0;\nEND");
        try (CallableStatement cstmt =
                     sharedConnection.prepareCall("{call testStreamInOutWithName(?)}")) {
            byte[] buffer = new byte[65];
            for (int i = 0; i < 65; i++) {
                buffer[i] = 1;
            }
            int il = buffer.length;
            int[] typesToTest =
                    new int[] {
                            Types.BIT,
                            Types.BINARY,
                            Types.BLOB,
                            Types.JAVA_OBJECT,
                            Types.LONGVARBINARY,
                            Types.VARBINARY
                    };

            for (int typeToTest : typesToTest) {
                cstmt.setBinaryStream("mblob", new ByteArrayInputStream(buffer), buffer.length);
                cstmt.registerOutParameter("mblob", typeToTest);
                cstmt.executeUpdate();

                InputStream is = cstmt.getBlob("mblob").getBinaryStream();
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

                int bytesRead;
                byte[] readBuf = new byte[256];

                while ((bytesRead = is.read(readBuf)) != -1) {
                    byteArrayOutputStream.write(readBuf, 0, bytesRead);
                }

                byte[] fromSelectBuf = byteArrayOutputStream.toByteArray();
                int ol = fromSelectBuf.length;
                assertEquals(il, ol);
            }

            cstmt.close();
        }
    }

    @Test
    public void testDefinerCallableStatement() throws Exception {
        Statement stmt = sharedConnection.createStatement();
        stmt.executeUpdate("DROP PROCEDURE IF EXISTS testDefinerCallableStatement");
        stmt.executeUpdate("CREATE PROCEDURE testDefinerCallableStatement(I INT) -- 'abcdefg'\nIS BEGIN\nSELECT I * 10 FROM dual;\nEND");
        sharedConnection.prepareCall("{call testDefinerCallableStatement(?)}").close();
        stmt.executeUpdate("DROP PROCEDURE IF EXISTS testDefinerCallableStatement");
    }

    @Test
    public void testProcedureComment() throws Exception {
        createProcedure(
                "testProcedureComment",
                "(a INT, b IN OUT VARCHAR2) IS BEGIN SELECT CONCAT(CONVERT(a, CHAR(50)), b) INTO b FROM DUAL; END");

        try (CallableStatement callableStatement =
                     sharedConnection.prepareCall(
                             "{ call /* just test */ testProcedureComment(?, "
                                     + "/* ? is the OUT param */?)  }")) {
            assertTrue(callableStatement.toString().contains("/*"));
            callableStatement.setInt(1, 1);
            callableStatement.setString(2, " a");
            callableStatement.registerOutParameter(2, Types.VARCHAR);
            ResultSet rs = callableStatement.executeQuery();
      /*
      if (rs.next()) {
        assertEquals("1 a", rs.getString(1));
      } else {
        fail("must have a result !");
      } */
            //assertNull(rs);
            //assertEquals("1 a", callableStatement.getString(2));
            assertTrue(callableStatement.getString(2).contains("1"));
            assertTrue(callableStatement.getString(2).contains("a"));
        }
    }

    @Test
    public void testCommentParser() throws Exception {
        createProcedure("testCommentParser",
            "(ACTION varchar2," + "`/*dumb-identifier-1*/` int," + "\n`#dumb-identifier-2` int,"
                    + "\n`--dumb-identifier-3` int," + "\nCLIENT_ID int, -- ABC"
                    + "\nLOGIN_ID  int, -- DEF" + "\nO_WHERE varchar2," + "\nSORT varchar2,"
                    + "\n  O_SQL out varchar2/* test comment */," + "\n SONG_ID int,"
                    + "\n  NOTES varchar2," + "\n  RESULT out varchar2" + "\n /*"
                    + "\n ,    -- Generic result parameter"
                    + "\n out _PERIOD_ID int,         -- Returns the period_id. "
                    + "Useful when using @PREDEFLINK to return which is the last period"
                    + "\n   _SONGS_LIST varchar(8000)," + "\n  _COMPOSERID int,"
                    + "\n  _PUBLISHERID int,"
                    + "\n   _PREDEFLINK int        -- If the user is accessing through a "
                    + "predefined link: 0=none  1=last period"
                    + "\n */) IS BEGIN SELECT 1 into O_SQL FROM DUAL; END");

        createProcedure(
            "testCommentParser_1",
            "(`/*id*/` /* before type 1 */ varchar2,"
                    + "/* after type 1 */  result2 OUT NUMBER /* p2 */) IS BEGIN SELECT  results into result2 from dual; END");

        sharedConnection
            .prepareCall("{call testCommentParser(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)}").close();
        //TODO need to support getProcedureColumns
        sharedConnection.prepareCall("{call testCommentParser_1(?, ?)}").close();

    }

    private void validateResult(ResultSet rs, String[] parameterNames, int[] parameterTypes,
                                int[] precision, int[] scale, int[] direction) throws SQLException {
        int index = 0;
        while (rs.next()) {
            assertEquals(parameterNames[index], rs.getString("COLUMN_NAME"));
            assertEquals(parameterTypes[index], rs.getInt("DATA_TYPE"));

            switch (index) {
                case 0:
                case 6:
                case 7:
                case 8:
                case 10:
                case 11:
                    assertEquals(precision[index], rs.getInt("LENGTH"));
                    break;

                default:
                    assertEquals(precision[index], rs.getInt("PRECISION"));
                    break;
            }
            assertEquals(scale[index], rs.getInt("SCALE"));
            assertEquals(direction[index], rs.getInt("COLUMN_TYPE"));

            index++;
        }
        rs.close();
    }

    //@Ignore //TODO function next
    @Test
    public void testCallableThrowException() throws Exception {
        createTable("testCallableThrowException1", "value_1 INT PRIMARY KEY");
        createTable("testCallableThrowException2", "value_2 INT PRIMARY KEY");

        sharedConnection
                .createStatement()
                .executeUpdate("INSERT INTO testCallableThrowException1 VALUES (1)");
        createFunction(
                "test_function",
                " RETURN int is \n max_value int; BEGIN "
                        + "SELECT MAX(value_1) INTO max_value FROM testCallableThrowException2; RETURN max_value; END;");

        try (CallableStatement callable = sharedConnection.prepareCall("{? = call test_function()}")) {

            callable.registerOutParameter(1, Types.BIGINT);

            try {
                callable.executeUpdate();
                fail("impossible; we should never get here.");
            } catch (SQLException sqlEx) {
                System.out.println(sqlEx.getSQLState());
                //sqlEx.printStackTrace();
                assertEquals("42S22", sqlEx.getSQLState());
            }
        }

        try {
            sharedConnection.createStatement().execute("DROP TABLE testCallableThrowException4");
        } catch (SQLException e) {
            //skip drop error
        }
        createTable("testCallableThrowException3", "value_1 NUMBER(20, 0) PRIMARY KEY");
        sharedConnection
                .createStatement()
                .executeUpdate("INSERT INTO testCallableThrowException3 VALUES (1)");
        createTable(
                "testCallableThrowException4",
                "value_2 NUMBER(20, 0) PRIMARY KEY, "
                        + " FOREIGN KEY (value_2) REFERENCES testCallableThrowException3 (value_1) ON DELETE CASCADE");
        createFunction(
                "test_function",
                "(value NUMBER) RETURN INT IS BEGIN "
                        + "INSERT INTO testCallableThrowException4 VALUES (value); RETURN value; END;");

        try (CallableStatement callable = sharedConnection.prepareCall("{? = call test_function(?)}")) {
            callable.registerOutParameter(1, Types.BIGINT);
            callable.setLong(2, 1);
            callable.executeUpdate();
            callable.setLong(2, 2);
            try {
                callable.executeUpdate();
                fail("impossible; we should never get here.");
            } catch (SQLException sqlEx) {
                assertEquals("23000", sqlEx.getSQLState());
            }
        }
    }

    @Test
    public void testCallableStatementFormat() {
        try {
            sharedConnection.prepareCall("CREATE TABLE testCallableStatementFormat(id INT)");
        } catch (Exception exception) {
            assertTrue(exception.getMessage().startsWith("invalid callable syntax"));
        }
    }

    //@Ignore //TODO support function in next
    @Test
    public void testFunctionWithFixedParameter() throws Exception {
        createFunction(
                "testFunctionWithFixedParameter",
                "(a varchar2, b number, c varchar2) RETURN NUMBER "
                        + " IS  BEGIN RETURN 1; END; ");

        try (CallableStatement callable =
                     sharedConnection.prepareCall("{? = call testFunctionWithFixedParameter(?,101,?)}")) {
            callable.registerOutParameter(1, Types.BIGINT);
            callable.setString(2, "FOO");
            callable.setString(3, "BAR");
            callable.executeUpdate();
        }
    }

    @Test
    public void testParameterNumber() throws Exception {
        // cancel for version 10.2 beta before fix https://jira.mariadb.org/browse/MDEV-11761
        cancelForVersion(10, 2, 2);
        cancelForVersion(10, 2, 3);
        cancelForVersion(10, 2, 4);
/*
    createTable(
        "TMIX91P",
        "F01SMALLINT         SMALLINT NOT NULL, F02INTEGER          INTEGER,F03REAL             NUMBER,"
            + "F04FLOAT            FLOAT,F05NUMERIC31X4      NUMERIC(31,4), F06NUMERIC16X16     NUMERIC(16,16), F07CHAR_10          CHAR(10),"
            + " F08VARCHAR_10       VARCHAR(10), F09CHAR_20          CHAR(20), F10VARCHAR_20       VARCHAR(20), F11DATE         DATE,"
            + " F12DATETIME         TIMESTAMP, PRIMARY KEY (F01SMALLINT)"); */
        //TODO need to check type date and timestampe
        createTable(
                "TMIX91P",
                "F01SMALLINT         SMALLINT NOT NULL, F02INTEGER          INTEGER,F03REAL             NUMBER,"
                        + "F04FLOAT            FLOAT,F05NUMERIC31X4      NUMERIC(31,4), F06NUMERIC16X16     NUMERIC(16,16), F07CHAR_10          CHAR(10),"
                        + " F08VARCHAR_10       VARCHAR(10), F09CHAR_20          CHAR(20), F10VARCHAR_20       VARCHAR(20), F11DATE         VARCHAR2(20),"
                        + " F12DATETIME         VARCHAR2(20), PRIMARY KEY (F01SMALLINT)");
        Statement stmt = sharedConnection.createStatement(); /*
    stmt.executeUpdate(
        "INSERT INTO TMIX91P VALUES (1,1,1234567.12,1234567.12,111111111111111111111111111.1111,.111111111111111,'1234567890',"
            + "'1234567890','CHAR20CHAR20','VARCHAR20ABCD','2001-01-01','2001-01-01 01:01:01')"); */
        stmt.executeUpdate(
                "INSERT INTO TMIX91P VALUES (1,1,1234567.12,1234567.12,111111111111111111111111111.1111,.111111111111111,'1234567890',"
                        + "'1234567890','CHAR20CHAR20','VARCHAR20ABCD','2001-01-01','2001-01-01')");
    /*
    stmt.executeUpdate(
            "INSERT INTO TMIX91P VALUES (7,1,1234567.12,1234567.12,22222222222.0001,.99999999999,'1234567896','1234567896','CHAR20',"
                    + "'VARCHAR20ABCD','2001-01-01','2001-01-01 01:01:01')"); */
        stmt.executeUpdate(
                "INSERT INTO TMIX91P VALUES (7,1,1234567.12,1234567.12,22222222222.0001,.99999999999,'1234567896','1234567896','CHAR20',"
                        + "'VARCHAR20ABCD','2001-01-01','2001-01-01')");
    /*
    stmt.executeUpdate(
            "INSERT INTO TMIX91P VALUES (12,12,1234567.12,1234567.12,111222333.4444,.1234567890,'2234567891','2234567891','CHAR20',"
                    + "'VARCHAR20VARCHAR20','2001-01-01','2001-01-01 01:01:01')"); */
        stmt.executeUpdate(
                "INSERT INTO TMIX91P VALUES (12,12,1234567.12,1234567.12,111222333.4444,.1234567890,'2234567891','2234567891','CHAR20',"
                        + "'VARCHAR20VARCHAR20','2001-01-01','2001-01-01')");

        createProcedure(
                "MSQSPR100",
                "( p1_in  INTEGER , p2_in  VARCHAR2,  p3_out OUT INTEGER,  p4_out OUT VARCHAR2)\n is BEGIN "
                        // + "\n SELECT F01SMALLINT,F02INTEGER, F11DATE,F12DATETIME,F03REAL \n FROM TMIX91P WHERE F02INTEGER = p1_in; "
                        // + "\n SELECT F08VARCHAR_10,F09CHAR_20 into p3_out, p4_out \n FROM TMIX91P WHERE  F09CHAR_20 = p2_in ORDER BY F02INTEGER ; "
                        + "\n p3_out := 144; \n p4_out  := 'CHARACTER11'; \n END");

        String sql = "{call MSQSPR100(1,'CHAR20',?,?)}";

        CallableStatement cs = sharedConnection.prepareCall(sql);

        cs.registerOutParameter(1, Types.INTEGER);
        cs.registerOutParameter(2, Types.CHAR);

        cs.execute();
        cs.close();

        createProcedure(
                "testParameterNumber_1",
                "( nfact OUT VARCHAR2,  ccuenta IN VARCHAR2,\n ffact OUT VARCHAR2,\n fdoc OUT VARCHAR2)\n is BEGIN"
                        + "\n nfact := 'ncfact string';\n ffact := 'ffact string';\n fdoc := 'fdoc string';\nEND");

        createProcedure(
                "testParameterNumber_2",
                "( ccuent1 IN VARCHAR2,  ccuent2 IN VARCHAR2,\n nfact OUT VARCHAR2,\n ffact OUT VARCHAR2,"
                        + "\n fdoc OUT VARCHAR2)\nIS BEGIN\n nfact := 'ncfact string';\n ffact := 'ffact string';\n"
                        + " fdoc := 'fdoc string';\nEND");

        Properties props = new Properties();
        props.put("jdbcCompliantTruncation", "true");
        props.put("useInformationSchema", "true");
        try (Connection conn1 = setConnection(props)) {
            //try (Connection conn1 = sharedConnection) {
            CallableStatement callSt = conn1.prepareCall("{ call testParameterNumber_1(?, ?, ?, ?) }");
            callSt.setString(2, "xxx");
            callSt.registerOutParameter(1, Types.VARCHAR);
            callSt.registerOutParameter(3, Types.VARCHAR);
            callSt.registerOutParameter(4, Types.VARCHAR);
            callSt.execute();

            assertEquals("ncfact string", callSt.getString(1));
            assertEquals("ffact string", callSt.getString(3));
            assertEquals("fdoc string", callSt.getString(4));

            CallableStatement callSt2 =
                    conn1.prepareCall("{ call testParameterNumber_2(?, ?, ?, ?, ?) }");
            callSt2.setString(1, "xxx");
            callSt2.setString(2, "yyy");
            callSt2.registerOutParameter(3, Types.VARCHAR);
            callSt2.registerOutParameter(4, Types.VARCHAR);
            callSt2.registerOutParameter(5, Types.VARCHAR);
            callSt2.execute();

            assertEquals("ncfact string", callSt2.getString(3));
            assertEquals("ffact string", callSt2.getString(4));
            assertEquals("fdoc string", callSt2.getString(5));

            CallableStatement callSt3 =
                    conn1.prepareCall("{ call testParameterNumber_2(?, 'yyy', ?, ?, ?) }");
            callSt3.setString(1, "xxx");
            // callSt3.setString(2, "yyy");
            callSt3.registerOutParameter(2, Types.VARCHAR);
            callSt3.registerOutParameter(3, Types.VARCHAR);
            callSt3.registerOutParameter(4, Types.VARCHAR);
            callSt3.execute();

            assertEquals("ncfact string", callSt3.getString(2));
            assertEquals("ffact string", callSt3.getString(3));
            assertEquals("fdoc string", callSt3.getString(4));
        }
    }

    @Ignore
    //TODO support in NEXT, need to check
    @Test
    public void testProcMultiDb() throws Exception {
        // cancel for version 10.2 beta before fix https://jira.mariadb.org/browse/MDEV-11761
        cancelForVersion(10, 2, 2);
        cancelForVersion(10, 2, 3);
        cancelForVersion(10, 2, 4);

        String originalCatalog = sharedConnection.getCatalog();
        /* */
        try {
            sharedConnection.createStatement().executeUpdate("drop TABLESPACE testProcMultiDb");
        } catch (SQLException e) {
            //skip
        }

        sharedConnection.createStatement().executeUpdate("CREATE TABLESPACE testProcMultiDb");

        createProcedure("test.testProcMultiDbProc",
            "(x int, y out int)\n is  z int:=0;\n begin\n z:= x+1, y:= z;\nend;\n");

        CallableStatement callableStatement = null;
        try {
            callableStatement = sharedConnection
                .prepareCall("{call `testProcMultiDb`.`testProcMultiDbProc`(?, ?)}");
            callableStatement.setInt(1, 5);
            callableStatement.registerOutParameter(2, Types.INTEGER);

            callableStatement.execute();
            assertEquals(6, callableStatement.getInt(2));
            callableStatement.clearParameters();
            callableStatement.close();

            sharedConnection.setCatalog("testProcMultiDb");
            callableStatement = sharedConnection
                .prepareCall("{call testProcMultiDb.testProcMultiDbProc(?, ?)}");
            callableStatement.setInt(1, 5);
            callableStatement.registerOutParameter(2, Types.INTEGER);

            callableStatement.execute();
            assertEquals(6, callableStatement.getInt(2));
            callableStatement.clearParameters();
            callableStatement.close();

            sharedConnection.setCatalog("mysql");
            callableStatement = sharedConnection
                .prepareCall("{call `testProcMultiDb`.`testProcMultiDbProc`(?, ?)}");
            callableStatement.setInt(1, 5);
            callableStatement.registerOutParameter(2, Types.INTEGER);

            callableStatement.execute();
            assertEquals(6, callableStatement.getInt(2));
        } finally {
            assert callableStatement != null;
            callableStatement.clearParameters();
            callableStatement.close();
            sharedConnection.setCatalog(originalCatalog);
            sharedConnection.createStatement().executeUpdate("DROP DATABASE testProcMultiDb");
        }
    }

    @Test
    public void callProcSendNullInOut() throws Exception {
        // cancel for version 10.2 beta before fix https://jira.mariadb.org/browse/MDEV-11761
        cancelForVersion(10, 2, 2);
        cancelForVersion(10, 2, 3);
        cancelForVersion(10, 2, 4);

        createProcedure("testProcSendNullInOut_1", "(x IN OUT INT)\n IS BEGIN\n x := x + 1;\nEND;");
        createProcedure("testProcSendNullInOut_2",
            "(x INT, y OUT INT )\nIS BEGIN\n y := x + 1;\nEND;");
        createProcedure("testProcSendNullInOut_3", "(x IN OUT INTEGER)\nIS BEGIN\n x := 10;\nEND;");

        CallableStatement call = sharedConnection
            .prepareCall("{ call testProcSendNullInOut_1(?) }");
        call.registerOutParameter(1, Types.INTEGER);
        call.setInt(1, 1);
        call.execute();
        assertEquals(2, call.getInt(1));

        call = sharedConnection.prepareCall("{ call testProcSendNullInOut_2(?, ?) }");
        call.registerOutParameter(2, Types.INTEGER);
        call.setInt(1, 1);
        call.execute();
        assertEquals(2, call.getInt(2));

        call = sharedConnection.prepareCall("{ call testProcSendNullInOut_2(?, ?) }");
        call.registerOutParameter(2, Types.INTEGER);
        call.setNull(1, Types.INTEGER);
        call.execute();
        assertEquals(0, call.getInt(2));
        assertTrue(call.wasNull());

        call = sharedConnection.prepareCall("{ call testProcSendNullInOut_1(?) }");
        call.registerOutParameter(1, Types.INTEGER);
        call.setNull(1, Types.INTEGER);
        call.execute();
        assertEquals(0, call.getInt(1));
        assertTrue(call.wasNull());

        call = sharedConnection.prepareCall("{ call testProcSendNullInOut_3(?) }");
        call.registerOutParameter(1, Types.INTEGER);
        call.setNull(1, Types.INTEGER);
        call.execute();
        assertEquals(10, call.getInt(1));
    }

    /**
     * CONJ-263: Error in stored procedure or SQL statement with allowMultiQueries does not raise
     * Exception when there is a result returned prior to erroneous statement.
     *
     * @throws SQLException exception
     */
    @Test
    public void testCallExecuteErrorBatch() throws SQLException {
        CallableStatement callableStatement = sharedConnection.prepareCall("{call TEST_SP1()}");
        try {
            callableStatement.execute();
            fail("Must have thrown error");
        } catch (SQLException sqle) {
            // must have thrown error.
            System.out.println("Message:" + sqle.getMessage());
            //TODO update next
            //assertTrue(sqle.getMessage().contains("Test error from SP"));
        }
    }

    /**
     * CONJ-298 : Callable function exception when no parameter and space before parenthesis.
     *
     * @throws SQLException exception
     */
    @Ignore
    //TODO Both Oracle and ObOracle mode not support
    @Test
    public void testFunctionWithSpace() throws SQLException {
        createFunction("hello3", "\n" + "    RETURN VARCHAR2 \n"
                                 + "  IS BEGIN  RETURN CONCAT('Hello, !'); END;");
        CallableStatement callableStatement = sharedConnection.prepareCall("{? = call hello3()}");
        callableStatement.registerOutParameter(1, Types.INTEGER);
        assertFalse(callableStatement.execute());
        assertEquals("Hello, !", callableStatement.getString(1));
    }

    /**
     * CONJ-425 : take care of registerOutParameter type.
     *
     * @throws Exception if connection error occur
     */
    @Test
    public void testOutputObjectType() throws Exception {
        // cancel for version 10.2 beta before fix https://jira.mariadb.org/browse/MDEV-11761
        cancelForVersion(10, 2, 2);
        cancelForVersion(10, 2, 3);
        cancelForVersion(10, 2, 4);

        createProcedure("issue425", "( inValue IN varchar2,  testValue OUT varchar2)\n"
                                    + "is BEGIN\n" + " testValue := CONCAT('o', inValue);\n"
                                    + "END");

        // registering with VARCHAR Type
        CallableStatement cstmt = sharedConnection.prepareCall("{call issue425(?, ?)}");
        cstmt.registerOutParameter(2, Types.VARCHAR);
        cstmt.setString(1, "x");
        cstmt.execute();

        assertEquals("ox", cstmt.getString(2));
        assertEquals("ox", cstmt.getObject(2, String.class)); // works
        assertEquals("ox", cstmt.getObject(2));
        //oracle-jdbc : SQLException: 不允许的操作: Ordinal binding and Named binding cannot be combined!
        assertEquals("ox", cstmt.getObject("testValue"));

        // registering with Binary Type
        CallableStatement cstmt2 = sharedConnection.prepareCall("{call issue425(?, ?)}");
        cstmt2.registerOutParameter(2, Types.BINARY);
        cstmt2.setString(1, "x");
        cstmt2.execute();

        assertEquals("ox", cstmt2.getString(2));
        assertEquals("ox", cstmt2.getObject(2, String.class)); // works
        assertTrue(cstmt2.getObject(2) instanceof byte[]);
        assertArrayEquals("ox".getBytes(), ((byte[]) cstmt2.getObject(2)));
        assertArrayEquals("ox".getBytes(), ((byte[]) cstmt2.getObject("testValue")));
    }

    //@Ignore //TODO support function
    @Test
    public void testOutputObjectTypeFunction() throws Exception {
        createFunction("issue425f",
            "(a VARCHAR2, b VARCHAR2) RETURN varchar2\n is BEGIN\nRETURN CONCAT(a, b);\nEND");

        // registering with VARCHAR Type
        CallableStatement cstmt = sharedConnection.prepareCall("{? = call issue425f(?, ?)}");
        cstmt.registerOutParameter(1, Types.VARCHAR);
        cstmt.setString(2, "o");
        cstmt.setString(3, "x");
        cstmt.execute();

        assertEquals("ox", cstmt.getString(1));
        assertEquals("ox", cstmt.getObject(1, String.class)); // works
        assertEquals("ox", cstmt.getObject(1));

        // registering with Binary Type
        CallableStatement cstmt2 = sharedConnection.prepareCall("{? = call issue425f(?, ?)}");
        cstmt2.registerOutParameter(1, Types.BINARY);
        cstmt2.setString(2, "o");
        cstmt2.setString(3, "x");
        cstmt2.execute();

        assertEquals("ox", cstmt2.getString(1));
        assertEquals("ox", cstmt2.getObject(1, String.class)); // works
        System.out.println("ZDW:" + cstmt2.getObject(1).getClass().toString());
        /*assertTrue(cstmt2.getObject(1) instanceof byte[]); //TODO check
        assertArrayEquals("ox".getBytes(), ((byte[]) cstmt2.getObject(1))); */
    }

    @Ignore //TODO oracle not support nothing in procedure
    @Test
    public void procedureCaching() throws SQLException {
        createProcedure("cacheCall", "(inValue IN INT)\n" + " IS BEGIN\n" + " /*do nothing*/ \n" + "END");

        CallableStatement st = sharedConnection.prepareCall("{call testj.cacheCall(?)}");
        st.setInt(1, 2);
        st.execute();

        try (CallableStatement st2 = sharedConnection.prepareCall("{call testj.cacheCall(?)}")) {
            st2.setInt(1, 2);
            st2.execute();
            st.close();

            try (CallableStatement st3 = sharedConnection.prepareCall("{call testj.cacheCall(?)}")) {
                st3.setInt(1, 2);
                st3.execute();
                st3.execute();
            }
        }

        try (CallableStatement st3 = sharedConnection.prepareCall("{?=call pow(?,?)}")) {
            st3.setInt(2, 2);
            st3.setInt(3, 2);
            st3.execute();
        }
    }

    @Test
    public void functionCaching() throws SQLException {
        createFunction(
                "hello2",
                "(a int) \n" + " RETURN INT \n" + " is b int; begin  select 3 into b from dual;  RETURN b; end;");
        CallableStatement st = sharedConnection.prepareCall("{? = call hello2(?)}");
        st.setInt(2, 1);
        st.registerOutParameter(1, Types.INTEGER);
        //assertFalse(st.execute());
        st.execute();

        st.setInt(2, 1);
        st.registerOutParameter(1, Types.INTEGER);
        //st.setFetchSize(1);
        st.registerOutParameter(1, Types.INTEGER);
        st.execute();
        //System.out.println(st.getResultSet().getInt(10));
        //System.out.println(st.getInt(1));
        assertEquals(3, st.getInt(1));


        try (CallableStatement st2 = sharedConnection.prepareCall("{? = call hello2(?)}")) {
            st2.registerOutParameter(1, Types.INTEGER);
            try {
                st2.execute();//索引中丢失  IN 或 OUT 参数:: 2
                Assert.fail();
            } catch (SQLException e) {
                e.printStackTrace();
            }

            st.close();

            try (CallableStatement st3 = sharedConnection.prepareCall("{? = call hello2(?)}")) {
                st3.registerOutParameter(1, Types.INTEGER);
                //st3.registerOutParameter(2, Types.INTEGER);

                //assertFalse(st3.execute());
            }
        }

    }

    @Test
    public void testTimestampParameterOutput() throws Exception {
        createProcedure("CONJ791",
            "( a IN CLOB,  b OUT VARCHAR) \n IS BEGIN\n b := '2006-01-01 01:01:16';\nEND");

        // registering with VARCHAR Type
        CallableStatement cstmt = sharedConnection.prepareCall("{call CONJ791(?, ?)}");
        cstmt.setString(1, "o");
        cstmt.registerOutParameter(2, Types.TIMESTAMP);
        cstmt.execute();

        assertEquals(Timestamp.valueOf("2006-01-01 01:01:16"), cstmt.getTimestamp(2));
    }

    @Test
    public void testCycleProcedure() throws Exception {
        try {
            Connection conn = sharedConnection;
            createProcedure("pro2", "( var out int)\nis BEGIN\nSELECT 1 into var from dual;\nend\n");
            CallableStatement cs = null;
            for (int i = 0; i < 2; i++) {
                cs = conn.prepareCall("{call pro2(?)}");
                cs.registerOutParameter(1, Types.INTEGER);
                cs.execute();
                Assert.assertEquals(1, cs.getInt(1));
                cs.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void fixIndexOutOfBoundsForProcedureIndex() {
        try {
            Connection conn = sharedConnection;
            createProcedure("testProc1", "(var1 in int, var2 out varchar2) is begin\n"
                                         + "var2:= 'aaa';\n" + "end;");
            CallableStatement callableStatement = conn.prepareCall("{call testProc1(?,?)}");
            callableStatement.registerOutParameter(2, Types.VARCHAR);
            callableStatement.setInt(1, 1);
            callableStatement.execute();
            Assert.assertEquals("aaa", callableStatement.getString(2));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void fixIndexOutOfBoundsForProcedureLabel() {
        try {
            Connection conn = sharedConnection;
            createProcedure("testProc1", "(var1 in int, var2 out varchar2) is begin\n"
                                         + "var2:= 'aaa';\n" + "end;");
            CallableStatement callableStatement = conn.prepareCall("{call testProc1(?,?)}");
            callableStatement.registerOutParameter("var2", Types.VARCHAR);
            callableStatement.setInt("var1", 1);
            callableStatement.execute();
            Assert.assertEquals("aaa", callableStatement.getString(2));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void functionAnonymousBlock() throws SQLException {
        createFunction("testFunctionCall",
            "(a float, b NUMBER, c int) RETURN INT IS\nBEGIN\n RETURN a + b + c;\nEND;");
        //createFunction("testFunctionCall", "(a FLOAT, b NUMBER, c INT) RETURN INT AS d INT;\nBEGIN\nd:=a+b+c;\nRETURN d;\nEND;");
        CallableStatement cs = sharedConnection
            .prepareCall("begin ? := testFunctionCall (?,?,?) ; end;");
        //CallableStatement cs = sharedConnection.prepareCall("? = call testFunctionCall(?,?,?)");

        cs.setInt(4, 3);
        cs.setInt(3, 2);
        cs.setFloat(2, 1);
        cs.registerOutParameter(1, Types.INTEGER);
        assertFalse(cs.execute());
        assertEquals(6f, cs.getInt(1), .001);

        cs.registerOutParameter(1, Types.INTEGER);
        cs.setFloat(2, 2);
        cs.setInt(3, 3);
        cs.setInt(4, 4);
        cs.executeUpdate();//assertEquals(0, cs.executeUpdate());
        assertEquals(9f, cs.getInt(1), .001);
    }

    @Test
    public void functionCall() throws SQLException {
        createFunction("testFunctionCall",
            "(a float, b NUMBER, c int) RETURN INT IS\nBEGIN\n RETURN a + b + c;\nEND;");
        //createFunction("testFunctionCall", "(a FLOAT, b NUMBER, c INT) RETURN INT AS d INT;\nBEGIN\nd:=a+b+c;\nRETURN d;\nEND;");
        CallableStatement cs = sharedConnection.prepareCall("? = call testFunctionCall(?,?,?)");

        cs.setInt(4, 3);
        cs.setInt(3, 2);
        cs.setFloat(2, 1);
        cs.registerOutParameter(1, Types.INTEGER);
        assertFalse(cs.execute());
        assertEquals(6f, cs.getInt(1), .001);

        cs.registerOutParameter(1, Types.INTEGER);
        cs.setFloat(2, 2);
        cs.setInt(3, 3);
        cs.setInt(4, 4);
        cs.executeUpdate();//assertEquals(0, cs.executeUpdate());
        assertEquals(9f, cs.getInt(1), .001);
    }

    @Test
    public void testStoreComplexAttrData() throws SQLException {
        Assume.assumeFalse(sharedUsePrepare());
        Statement stmt = sharedConnection.createStatement();
        stmt.execute("CREATE OR REPLACE TYPE MY_OBJ_1 AS OBJECT(COL1 INT,COL2 VARCHAR(20),COL3 NUMBER)");
        createProcedure("MY_PROC_EXTIN", "(X1 IN MY_OBJ_1,X2 OUT INT) IS BEGIN \n" +
                "                DBMS_OUTPUT.PUT_LINE(X1.COL1); \n" +
                "                DBMS_OUTPUT.PUT_LINE(X1.COL2); \n" +
                "                DBMS_OUTPUT.PUT_LINE(X1.COL3); \n" +
                "                X2 := X1.COL3;\n" +
                "                END;");
        try (CallableStatement prepareCall=
                     sharedConnection.prepareCall("CALL MY_PROC_EXTIN(?,?)");) {
            Struct struct = sharedConnection.createStruct("MY_OBJ_1", new Object[]{12, "Marry", 12.123});
            prepareCall.setObject(1, struct);
            prepareCall.registerOutParameter(2, Types.INTEGER);
            prepareCall.execute();
            assertEquals(12,prepareCall.getObject(2));

        }
        //ps protocol
        Connection conn = setConnection("&useServerPrepStmts=true");
        try (CallableStatement prepareCall=
                     conn.prepareCall("CALL MY_PROC_EXTIN(?,?)");) {
            Struct struct = conn.createStruct("MY_OBJ_1", new Object[]{12, "Marry", 12.123});
            prepareCall.setObject(1, struct);
            prepareCall.registerOutParameter(2, Types.INTEGER);
            prepareCall.execute();
            assertEquals(12,prepareCall.getObject(2));

        }
        stmt.close();
    }
}
