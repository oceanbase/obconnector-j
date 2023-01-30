package com.oceanbase.jdbc;

import java.sql.*;

import org.junit.*;

public class ExecuteReturnOracleTest extends BaseOracleTest {

    static String     tableName           = "Execute_Return_Test";
    static String     tableColumns        = "c1 int, c2 varchar(30)";
    static String     insertProcedureName = "test_procedure_insert_oracle";
    static String     updateProcedureName = "test_procedure_update_oracle";
    static String     deleteProcedureName = "test_procedure_delete_oracle";
    static String     insertFunctionName  = "test_function_insert_oracle";
    static String     updateFunctionName  = "test_function_update_oracle";
    static String     deleteFunctionName  = "test_function_delete_oracle";

    static String     deleteSql           = "delete from " + tableName + " where c1 >= ?";
    static String     insertSql           = "insert all into " + tableName
                                            + " values(?,'28+string')" + " into " + tableName
                                            + " values(29,?)" + " into " + tableName
                                            + " values(?,'30+string')" + " select * from dual";
    static String     recoverInsertSql    = "insert all into " + tableName
                                            + " values(?,'28+string')" + " into " + tableName
                                            + " values(29,?)" + " select * from dual";
    static String     updateSql           = "update " + tableName + " set c2 = ? where c1 = 1";
    static String     selectSql           = "select c2 from " + tableName;
    static String     recoverUpdateSql    = "update " + tableName
                                            + " set c2 = '1+string' where c1 = 1";

    static Connection conn;
    int               index;

    PreparedStatement deletePs;
    PreparedStatement insertPs;
    PreparedStatement updatePs;
    PreparedStatement deleteReturningPs;
    PreparedStatement insertReturningPs;
    PreparedStatement recoverInsertPs;
    PreparedStatement updateReturningPs;
    CallableStatement insertCs;
    CallableStatement updateCs;
    CallableStatement deleteCs;

    @BeforeClass
    public static void initTable() throws SQLException {
        createTable(tableName, tableColumns);
        String query = "insert all";
        for (int i = 1; i <= 30; i++) {
            query += " into " + tableName + " values(" + i + ",'" + i + "+string')";
        }
        query += " select * from dual";
        Statement pstmt = sharedConnection.createStatement();
        pstmt.execute(query);
        pstmt.close();

        // Oracle always use PS and cursor
        //conn = setConnection("&useServerPrepStmts=true&useCursorFetch=true&useOraclePrepareExecute=true");
        conn = sharedConnection;
    }

    private void retrieveInternal(ResultSet rs) throws SQLException {
        if (rs != null) {
            try {
                ResultSetMetaData meta = rs.getMetaData();
                try {
                    String colName = meta.getColumnName(1);
                    if (!colName.equals("C2") && !colName.equals("OC1")
                        && !colName.equals("\"TEST\".\"EXECUTE_RETURN_TEST\".\"C2\"")) {
                        Assert.fail();
                    }
                } catch (Exception e) {
                    System.out.println("getColumnName: " + e.getMessage());
                }
            } catch (Exception e) {
                System.out.println("getMetaData: " + e.getMessage());
            }

            try {
                while (rs.next()) {
                    try {
                        index++;
                        //System.out.println(rs.getString(1));
                        Assert.assertTrue(rs.getString(1).contains("+"));
                        //                    if (index == 1) {
                        //                        Assert.assertEquals("1+changed", rs.getString(1));
                        //                    } else {
                        //                        Assert.assertEquals((index + "+string"), rs.getString(1));
                        //                    }
                    } catch (Exception e) {
                        System.out.println("getString: " + e.getMessage());
                    }
                }
            } catch (Exception e) {
                System.out.println("next: " + e.getMessage());
            }
        }
    }

    /********* normal sql test *********/
    private void createPrepareStatement() throws SQLException {
        deletePs = conn.prepareStatement(deleteSql);
        deletePs.setInt(1, 28);

        insertPs = conn.prepareStatement(insertSql);
        insertPs.setInt(1, 28);
        insertPs.setString(2, "29+string");
        insertPs.setInt(3, 30);

        updatePs = conn.prepareStatement(updateSql);
        updatePs.setString(1, "1+changed");
    }

    @Ignore
    public void executeNormalSql() throws SQLException {
        //Assume.assumeTrue(sharedOptions().useOraclePrepareExecute && sharedUsePrepare() && sharedOptions().useCursorFetch);
        createPrepareStatement();
        boolean bool;
        ResultSet normalRS, returnRS;

        bool = deletePs.execute();
        Assert.assertEquals(false, bool);
        Assert.assertEquals(3, deletePs.getUpdateCount());
        normalRS = deletePs.getResultSet();
        Assert.assertEquals(null, normalRS);
        try {
            returnRS = ((JDBC4ServerPreparedStatement) deletePs).getReturnResultSet();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Statement handle not executed"));
        }

        bool = insertPs.execute();
        Assert.assertEquals(false, bool);
        Assert.assertEquals(3, insertPs.getUpdateCount());
        normalRS = insertPs.getResultSet();
        Assert.assertEquals(null, normalRS);
        try {
            returnRS = ((JDBC4ServerPreparedStatement) insertPs).getReturnResultSet();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Statement handle not executed"));
        }

        bool = updatePs.execute();
        Assert.assertEquals(false, bool);
        Assert.assertEquals(1, updatePs.getUpdateCount());
        normalRS = updatePs.getResultSet();
        Assert.assertEquals(null, normalRS);
        try {
            returnRS = ((JDBC4ServerPreparedStatement) updatePs).getReturnResultSet();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Statement handle not executed"));
        }

        PreparedStatement pstmt = conn.prepareStatement(selectSql);
        bool = pstmt.execute();
        Assert.assertEquals(true, bool);
        Assert.assertEquals(-1, pstmt.getUpdateCount());
        normalRS = pstmt.getResultSet();
        Assert.assertNotEquals(null, normalRS);
        index = 0;
        retrieveInternal(normalRS);
        Assert.assertEquals(30, index);
        try {
            returnRS = ((JDBC4ServerPreparedStatement) pstmt).getReturnResultSet();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Statement handle not executed"));
        }

        bool = pstmt.execute(recoverUpdateSql);
        Assert.assertEquals(false, bool);
        Assert.assertEquals(1, pstmt.getUpdateCount());
        normalRS = pstmt.getResultSet();
        Assert.assertEquals(null, normalRS);
        try {
            returnRS = ((JDBC4ServerPreparedStatement) pstmt).getReturnResultSet();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Statement handle not executed"));
        }
    }

    @Ignore
    public void executeUpdateNormalSql() throws SQLException {
        //Assume.assumeTrue(sharedOptions().useOraclePrepareExecute && sharedUsePrepare() && sharedOptions().useCursorFetch);
        createPrepareStatement();
        int count;
        ResultSet normalRS, returnRS;

        count = deletePs.executeUpdate();
        Assert.assertEquals(3, count);
        Assert.assertEquals(3, deletePs.getUpdateCount());
        normalRS = deletePs.getResultSet();
        Assert.assertEquals(null, normalRS);
        try {
            returnRS = ((JDBC4ServerPreparedStatement) deletePs).getReturnResultSet();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Statement handle not executed"));
        }

        count = insertPs.executeUpdate();
        Assert.assertEquals(3, count);
        Assert.assertEquals(3, insertPs.getUpdateCount());
        normalRS = insertPs.getResultSet();
        Assert.assertEquals(null, normalRS);
        try {
            returnRS = ((JDBC4ServerPreparedStatement) insertPs).getReturnResultSet();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Statement handle not executed"));
        }

        count = updatePs.executeUpdate();
        Assert.assertEquals(1, count);
        Assert.assertEquals(1, updatePs.getUpdateCount());
        normalRS = updatePs.getResultSet();
        Assert.assertEquals(null, normalRS);
        try {
            returnRS = ((JDBC4ServerPreparedStatement) updatePs).getReturnResultSet();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Statement handle not executed"));
        }

        PreparedStatement pstmt = conn.prepareStatement(selectSql);
        count = pstmt.executeUpdate();
        Assert.assertEquals(10, count); //oracle
        Assert.assertEquals(-1, pstmt.getUpdateCount());
        normalRS = pstmt.getResultSet();
        Assert.assertNotEquals(null, normalRS);
        index = 0;
        retrieveInternal(normalRS);
        Assert.assertEquals(30, index);
        try {
            returnRS = ((JDBC4ServerPreparedStatement) pstmt).getReturnResultSet();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Statement handle not executed"));
        }

        count = pstmt.executeUpdate(recoverUpdateSql);
        Assert.assertEquals(1, count);
        Assert.assertEquals(1, pstmt.getUpdateCount());
        normalRS = pstmt.getResultSet();
        Assert.assertEquals(null, normalRS);
        try {
            returnRS = ((JDBC4ServerPreparedStatement) pstmt).getReturnResultSet();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Statement handle not executed"));
        }
    }

    @Ignore
    public void executeQueryNormalSql() throws SQLException {
        //Assume.assumeTrue(sharedOptions().useOraclePrepareExecute && sharedUsePrepare() && sharedOptions().useCursorFetch);
        createPrepareStatement();
        ResultSet rs, normalRS, returnRS;

        rs = deletePs.executeQuery();
        Assert.assertNotEquals(null, rs);
        Assert.assertEquals(3, deletePs.getUpdateCount());
        index = 0;
        retrieveInternal(rs);
        Assert.assertEquals(0, index); //todo: oracle 3, validRows is 3
        normalRS = deletePs.getResultSet();
        Assert.assertEquals(null, normalRS);
        try {
            returnRS = ((JDBC4ServerPreparedStatement) deletePs).getReturnResultSet();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Statement handle not executed"));
        }

        rs = insertPs.executeQuery();
        Assert.assertNotEquals(null, rs);
        Assert.assertEquals(3, insertPs.getUpdateCount());
        //        index = 0;
        //        retrieveInternal(rs);
        //        Assert.assertEquals(0, index);
        normalRS = insertPs.getResultSet();
        Assert.assertEquals(null, normalRS);
        try {
            returnRS = ((JDBC4ServerPreparedStatement) insertPs).getReturnResultSet();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Statement handle not executed"));
        }

        rs = updatePs.executeQuery();
        Assert.assertNotEquals(null, rs);
        Assert.assertEquals(1, updatePs.getUpdateCount());
        //        index = 0;
        //        retrieveInternal(rs);
        //        Assert.assertEquals(0, index);
        normalRS = updatePs.getResultSet();
        Assert.assertEquals(null, normalRS);
        try {
            returnRS = ((JDBC4ServerPreparedStatement) updatePs).getReturnResultSet();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Statement handle not executed"));
        }

        PreparedStatement pstmt = conn.prepareStatement(selectSql);
        rs = pstmt.executeQuery();
        Assert.assertNotEquals(null, rs);
        Assert.assertEquals(-1, pstmt.getUpdateCount());
        index = 0;
        retrieveInternal(rs);
        Assert.assertEquals(30, index);
        normalRS = pstmt.getResultSet();
        Assert.assertNotEquals(null, normalRS);
        index = 0;
        retrieveInternal(normalRS);
        Assert.assertEquals(0, index);//todo: oracle 1
        // currentRow=-1, totalRowsVisited=31, validRows=0, getString: 未调用 ResultSet.next
        // currentRow=0, totalRowsVisited=32, validRows=0, next: ORA-01002: 提取违反顺序
        try {
            returnRS = ((JDBC4ServerPreparedStatement) pstmt).getReturnResultSet();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Statement handle not executed"));
        }

        rs = pstmt.executeQuery(recoverUpdateSql);
        Assert.assertNotEquals(null, rs);
        Assert.assertEquals(1, pstmt.getUpdateCount());
        //        index = 0;
        //        retrieveInternal(rs);
        //        Assert.assertEquals(0, index);
        normalRS = pstmt.getResultSet();
        Assert.assertEquals(null, normalRS);
        try {
            returnRS = ((JDBC4ServerPreparedStatement) pstmt).getReturnResultSet();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Statement handle not executed"));
        }
    }

    /********* DML returning test *********/
    private void createDmlReturningPrepareStatement() throws SQLException {
        deleteReturningPs = conn.prepareStatement(deleteSql + " returning c2 into ?");
        deleteReturningPs.setInt(1, 28);
        ((JDBC4ServerPreparedStatement) deleteReturningPs)
            .registerReturnParameter(2, Types.VARCHAR);

        //            PreparedStatement insertReturningPs = conn.prepareStatement(insertSql + " returning c2 into ?");
        //            insertReturningPs.setInt(1, 28);
        //            insertReturningPs.setString(2, "29+string");
        //            insertReturningPs.setInt(3, 30);
        //            ((JDBC4ServerPreparedStatement)insertReturningPs).registerReturnParameter(4, Types.INTEGER);
        insertReturningPs = conn.prepareStatement("insert into " + tableName
                                                  + " values(30,'30+string')"
                                                  + " returning c2, c1 into ?, ?");
        ((JDBC4ServerPreparedStatement) insertReturningPs)
            .registerReturnParameter(1, Types.VARCHAR);
        ((JDBC4ServerPreparedStatement) insertReturningPs)
            .registerReturnParameter(2, Types.INTEGER);

        recoverInsertPs = conn.prepareStatement(recoverInsertSql);
        recoverInsertPs.setInt(1, 28);
        recoverInsertPs.setString(2, "29+string");

        updateReturningPs = conn.prepareStatement(updateSql + " returning c2 into ?");
        updateReturningPs.setString(1, "1+changed");
        ((JDBC4ServerPreparedStatement) updateReturningPs)
            .registerReturnParameter(2, Types.VARCHAR);
    }

    @Test
    public void executeDmlReturning() throws SQLException {
        Assume.assumeTrue(sharedOptions().useOraclePrepareExecute && sharedUsePrepare()
                          && sharedOptions().useCursorFetch);
        createDmlReturningPrepareStatement();
        boolean bool;
        ResultSet normalRS, returnRS;

        bool = deleteReturningPs.execute();
        Assert.assertEquals(false, bool);
        Assert.assertEquals(3, deleteReturningPs.getUpdateCount());
        normalRS = deleteReturningPs.getResultSet();
        Assert.assertEquals(null, normalRS);
        returnRS = ((JDBC4ServerPreparedStatement) deleteReturningPs).getReturnResultSet();
        Assert.assertNotEquals(null, returnRS);
        index = 0;
        retrieveInternal(returnRS);
        Assert.assertEquals(3, index);

        bool = insertReturningPs.execute();
        Assert.assertEquals(false, bool);
        Assert.assertEquals(1, insertReturningPs.getUpdateCount());
        normalRS = insertReturningPs.getResultSet();
        Assert.assertEquals(null, normalRS);
        returnRS = ((JDBC4ServerPreparedStatement) insertReturningPs).getReturnResultSet();
        Assert.assertNotEquals(null, returnRS);
        index = 0;
        retrieveInternal(returnRS);
        Assert.assertEquals(1, index);

        bool = recoverInsertPs.execute();
        Assert.assertEquals(false, bool);
        Assert.assertEquals(2, recoverInsertPs.getUpdateCount());
        normalRS = recoverInsertPs.getResultSet();
        Assert.assertEquals(null, normalRS);
        try {
            returnRS = ((JDBC4ServerPreparedStatement) recoverInsertPs).getReturnResultSet();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Statement handle not executed"));
            ;
        }

        bool = updateReturningPs.execute();
        Assert.assertEquals(false, bool);
        Assert.assertEquals(1, updateReturningPs.getUpdateCount());
        normalRS = updateReturningPs.getResultSet();
        Assert.assertEquals(null, normalRS);
        returnRS = ((JDBC4ServerPreparedStatement) updateReturningPs).getReturnResultSet();
        Assert.assertNotEquals(null, returnRS);
        index = 0;
        retrieveInternal(returnRS);
        Assert.assertEquals(1, index);

        PreparedStatement pstmt = conn.prepareStatement(selectSql);
        bool = pstmt.execute();
        Assert.assertEquals(true, bool);
        Assert.assertEquals(-1, pstmt.getUpdateCount());
        normalRS = pstmt.getResultSet();
        Assert.assertNotEquals(null, normalRS);
        index = 0;
        retrieveInternal(normalRS);
        Assert.assertEquals(30, index);
        try {
            returnRS = ((JDBC4ServerPreparedStatement) pstmt).getReturnResultSet();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Statement handle not executed"));
        }

        bool = pstmt.execute(recoverUpdateSql);
        Assert.assertEquals(false, bool);
        Assert.assertEquals(1, pstmt.getUpdateCount());
        normalRS = pstmt.getResultSet();
        Assert.assertEquals(null, normalRS);
        try {
            returnRS = ((JDBC4ServerPreparedStatement) pstmt).getReturnResultSet();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Statement handle not executed"));
        }
    }

    @Test
    public void executeUpdateDmlReturning() throws SQLException {
        Assume.assumeTrue(sharedOptions().useOraclePrepareExecute && sharedUsePrepare()
                          && sharedOptions().useCursorFetch);
        createDmlReturningPrepareStatement();
        int count;
        ResultSet normalRS, returnRS;

        count = deleteReturningPs.executeUpdate();
        Assert.assertEquals(3, count);
        Assert.assertEquals(3, deleteReturningPs.getUpdateCount());
        normalRS = deleteReturningPs.getResultSet();
        Assert.assertEquals(null, normalRS);
        returnRS = ((JDBC4ServerPreparedStatement) deleteReturningPs).getReturnResultSet();
        Assert.assertNotEquals(null, returnRS);
        index = 0;
        retrieveInternal(returnRS);
        Assert.assertEquals(3, index);

        count = insertReturningPs.executeUpdate();
        Assert.assertEquals(1, count);
        Assert.assertEquals(1, insertReturningPs.getUpdateCount());
        normalRS = insertReturningPs.getResultSet();
        Assert.assertEquals(null, normalRS);
        returnRS = ((JDBC4ServerPreparedStatement) insertReturningPs).getReturnResultSet();
        Assert.assertNotEquals(null, returnRS);
        index = 0;
        retrieveInternal(returnRS);
        Assert.assertEquals(1, index);

        count = recoverInsertPs.executeUpdate();
        Assert.assertEquals(2, count);
        Assert.assertEquals(2, recoverInsertPs.getUpdateCount());
        normalRS = recoverInsertPs.getResultSet();
        Assert.assertEquals(null, normalRS);
        try {
            returnRS = ((JDBC4ServerPreparedStatement) recoverInsertPs).getReturnResultSet();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Statement handle not executed"));
            ;
        }

        count = updateReturningPs.executeUpdate();
        Assert.assertEquals(1, count);
        Assert.assertEquals(1, updateReturningPs.getUpdateCount());
        normalRS = updateReturningPs.getResultSet();
        Assert.assertEquals(null, normalRS);
        returnRS = ((JDBC4ServerPreparedStatement) updateReturningPs).getReturnResultSet();
        Assert.assertNotEquals(null, returnRS);
        index = 0;
        retrieveInternal(returnRS);
        Assert.assertEquals(1, index);

        PreparedStatement pstmt = conn.prepareStatement(selectSql);
        count = pstmt.executeUpdate();
        Assert.assertEquals(0, count); //todo: oracle 10
        Assert.assertEquals(-1, pstmt.getUpdateCount());
        normalRS = pstmt.getResultSet();
        Assert.assertNotEquals(null, normalRS);
        index = 0;
        retrieveInternal(normalRS);
        Assert.assertEquals(30, index);
        try {
            returnRS = ((JDBC4ServerPreparedStatement) pstmt).getReturnResultSet();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Statement handle not executed"));
        }

        count = pstmt.executeUpdate(recoverUpdateSql);
        Assert.assertEquals(1, count);
        Assert.assertEquals(1, pstmt.getUpdateCount());
        normalRS = pstmt.getResultSet();
        Assert.assertEquals(null, normalRS);
        try {
            returnRS = ((JDBC4ServerPreparedStatement) pstmt).getReturnResultSet();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Statement handle not executed"));
        }
    }

    @Test
    public void executeQueryDmlReturning() throws SQLException {
        Assume.assumeTrue(sharedOptions().useOraclePrepareExecute && sharedUsePrepare()
                          && sharedOptions().useCursorFetch);
        createDmlReturningPrepareStatement();
        ResultSet rs, normalRS, returnRS;

        rs = deleteReturningPs.executeQuery();
        Assert.assertNotEquals(null, rs);
        Assert.assertEquals(3, deleteReturningPs.getUpdateCount());
        index = 0;
        retrieveInternal(rs);
        Assert.assertEquals(3, index);
        normalRS = deleteReturningPs.getResultSet();
        Assert.assertEquals(null, normalRS);
        returnRS = ((JDBC4ServerPreparedStatement) deleteReturningPs).getReturnResultSet();
        Assert.assertNotEquals(null, returnRS);
        index = 0;
        retrieveInternal(returnRS);
        Assert.assertEquals(0, index);

        rs = insertReturningPs.executeQuery();
        Assert.assertNotEquals(null, rs);
        Assert.assertEquals(1, insertReturningPs.getUpdateCount());
        //        index = 0;
        //        retrieveInternal(rs);
        //        Assert.assertEquals(0, index);
        normalRS = insertReturningPs.getResultSet();
        Assert.assertEquals(null, normalRS);
        returnRS = ((JDBC4ServerPreparedStatement) insertReturningPs).getReturnResultSet();
        Assert.assertNotEquals(null, returnRS);
        index = 0;
        retrieveInternal(returnRS);
        Assert.assertEquals(1, index);

        rs = recoverInsertPs.executeQuery();
        Assert.assertNotEquals(null, rs);
        Assert.assertEquals(2, recoverInsertPs.getUpdateCount());
        normalRS = recoverInsertPs.getResultSet();
        Assert.assertEquals(null, normalRS);
        try {
            returnRS = ((JDBC4ServerPreparedStatement) recoverInsertPs).getReturnResultSet();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Statement handle not executed"));
            ;
        }

        rs = updateReturningPs.executeQuery();
        Assert.assertNotEquals(null, rs);
        Assert.assertEquals(1, updateReturningPs.getUpdateCount());
        index = 0;
        retrieveInternal(rs);
        Assert.assertEquals(1, index);
        normalRS = updateReturningPs.getResultSet();
        Assert.assertEquals(null, normalRS);
        returnRS = ((JDBC4ServerPreparedStatement) updateReturningPs).getReturnResultSet();
        Assert.assertNotEquals(null, returnRS);
        index = 0;
        retrieveInternal(returnRS);
        Assert.assertEquals(0, index);

        PreparedStatement pstmt = conn.prepareStatement(selectSql);
        rs = pstmt.executeQuery();
        Assert.assertNotEquals(null, rs);
        Assert.assertEquals(-1, pstmt.getUpdateCount());
        index = 0;
        retrieveInternal(rs);
        Assert.assertEquals(30, index);
        normalRS = pstmt.getResultSet();
        Assert.assertEquals(null, normalRS);
        //        index = 0;
        //        retrieveInternal(normalRS);
        //        Assert.assertEquals(0, index); //todo: oracle 1
        try {
            returnRS = ((JDBC4ServerPreparedStatement) pstmt).getReturnResultSet();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Statement handle not executed"));
        }

        rs = pstmt.executeQuery(recoverUpdateSql);
        Assert.assertEquals(null, rs); //todo: oracle null
        Assert.assertEquals(1, pstmt.getUpdateCount());
        //        index = 0;
        //        retrieveInternal(rs);
        //        Assert.assertEquals(0, index);
        normalRS = pstmt.getResultSet();
        Assert.assertEquals(null, normalRS);
        try {
            returnRS = ((JDBC4ServerPreparedStatement) pstmt).getReturnResultSet();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Statement handle not executed"));
        }
    }

    @Test
    public void executeNormalSqlIncludeReturningInto() throws SQLException {
        createTable("table_test", "col int, col_into varchar(30)");
        Statement stmt = conn.createStatement();
        stmt.execute("insert into table_test values(1,'1+string')");
        stmt.close();
        PreparedStatement ps = conn
            .prepareStatement("update table_test set col_into = ' returning test into ?' where col = ?");
        ps.setInt(1, 1);
        ps.execute();
    }

    /********* procedure test *********/
    private void createProcedureWithArguments() throws SQLException {
        createProcedure(deleteProcedureName, "(IC1 IN NUMBER, OC1 OUT VARCHAR) IS\n" + "begin\n"
                                             + "select c2 into OC1 from " + tableName
                                             + " where c1 = IC1;\n" + "delete from " + tableName
                                             + " where c1 >= IC1;\n" + "end;");
        deleteCs = conn.prepareCall("call " + deleteProcedureName + "(?, ?)");
        deleteCs.setInt(1, 28);
        deleteCs.registerOutParameter(2, Types.VARCHAR);

        createProcedure(insertProcedureName,
            "(IC1 IN NUMBER, IC2 IN VARCHAR, IC3 IN NUMBER, OC1 OUT VARCHAR) IS\n" + "begin\n"
                    + "insert all into " + tableName + " values(IC1,'28+string') into " + tableName
                    + " values(29,IC2) into " + tableName
                    + " values(IC3,'30+string') select * from dual;\n" + "select c2 into OC1 from "
                    + tableName + " where c1 = IC1;\n" + "end;");
        insertCs = conn.prepareCall("{call " + insertProcedureName + "(?, ?, ?, ?)}");
        insertCs.setInt(1, 28);
        insertCs.setString(2, "29+string");
        insertCs.setInt(3, 30);
        insertCs.registerOutParameter(4, Types.VARCHAR);

        createProcedure(updateProcedureName, "(IC1 IN VARCHAR) IS\n" + "begin\n" + "update "
                                             + tableName + " set c2 = IC1 where c1 = 1;\n" + "end;");
        updateCs = conn.prepareCall("call " + updateProcedureName + "(?)");
        updateCs.setString(1, "1+changed");
    }

    @Ignore
    public void executeProcedureWithArguments() throws SQLException {
        //Assume.assumeTrue(sharedOptions().useOraclePrepareExecute && sharedUsePrepare() && sharedOptions().useCursorFetch);
        createProcedureWithArguments();
        boolean bool;
        ResultSet normalRS, returnRS;

        bool = deleteCs.execute();
        Assert.assertEquals(false, bool);
        Assert.assertEquals(-1, deleteCs.getUpdateCount());
        normalRS = deleteCs.getResultSet();
        Assert.assertEquals(null, normalRS);
        try {
            returnRS = ((JDBC4ServerPreparedStatement) deleteCs).getReturnResultSet();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Statement handle not execute"));
        }

        bool = insertCs.execute();
        Assert.assertEquals(false, bool);
        Assert.assertEquals(-1, insertCs.getUpdateCount());
        normalRS = insertCs.getResultSet();
        Assert.assertEquals(null, normalRS);
        try {
            returnRS = ((JDBC4ServerPreparedStatement) insertCs).getReturnResultSet();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Statement handle not execute"));
        }

        bool = updateCs.execute();
        Assert.assertEquals(false, bool);
        Assert.assertEquals(-1, updateCs.getUpdateCount());
        normalRS = updateCs.getResultSet();
        Assert.assertEquals(null, normalRS);
        try {
            returnRS = ((JDBC4ServerPreparedStatement) updateCs).getReturnResultSet();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Statement handle not execute"));
        }

        PreparedStatement pstmt = conn.prepareStatement(selectSql);
        bool = pstmt.execute();
        Assert.assertEquals(true, bool);
        Assert.assertEquals(-1, pstmt.getUpdateCount());
        normalRS = pstmt.getResultSet();
        Assert.assertNotEquals(null, normalRS);
        index = 0;
        retrieveInternal(normalRS);
        Assert.assertEquals(30, index);
        try {
            returnRS = ((JDBC4ServerPreparedStatement) pstmt).getReturnResultSet();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Statement handle not execute"));
        }

        bool = pstmt.execute(recoverUpdateSql);
        Assert.assertEquals(false, bool);
        Assert.assertEquals(1, pstmt.getUpdateCount());
        normalRS = pstmt.getResultSet();
        Assert.assertEquals(null, normalRS);
        try {
            returnRS = ((JDBC4ServerPreparedStatement) pstmt).getReturnResultSet();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Statement handle not execute"));
        }

        bool = pstmt.execute();
        Assert.assertEquals(false, bool);
        normalRS = pstmt.getResultSet();
        Assert.assertEquals(null, normalRS);
    }

    @Ignore
    public void executeUpdateProcedureWithArguments() throws SQLException {
        //Assume.assumeTrue(sharedOptions().useOraclePrepareExecute && sharedUsePrepare() && sharedOptions().useCursorFetch);
        createProcedureWithArguments();
        int count;
        ResultSet normalRS, returnRS;

        count = deleteCs.executeUpdate();
        Assert.assertEquals(0, count);
        Assert.assertEquals(0, deleteCs.getUpdateCount());//todo: oracle -1
        normalRS = deleteCs.getResultSet();
        Assert.assertEquals(null, normalRS);
        try {
            returnRS = ((JDBC4ServerPreparedStatement) deleteCs).getReturnResultSet();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Statement handle not execute"));
        }

        count = insertCs.executeUpdate();
        Assert.assertEquals(0, count);//todo: oracle 1
        Assert.assertEquals(0, insertCs.getUpdateCount());//todo: oracle -1
        normalRS = insertCs.getResultSet();
        Assert.assertEquals(null, normalRS);
        try {
            returnRS = ((JDBC4ServerPreparedStatement) insertCs).getReturnResultSet();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Statement handle not execute"));
        }

        count = updateCs.executeUpdate();
        Assert.assertEquals(0, count);//todo: oracle 1
        Assert.assertEquals(0, updateCs.getUpdateCount());//todo: oracle -1
        normalRS = updateCs.getResultSet();
        Assert.assertEquals(null, normalRS);
        try {
            returnRS = ((JDBC4ServerPreparedStatement) updateCs).getReturnResultSet();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Statement handle not execute"));
        }

        PreparedStatement pstmt = conn.prepareStatement(selectSql);
        count = pstmt.executeUpdate();
        Assert.assertEquals(10, count);
        Assert.assertEquals(-1, pstmt.getUpdateCount());
        normalRS = pstmt.getResultSet();
        Assert.assertNotEquals(null, normalRS);
        index = 0;
        retrieveInternal(normalRS);
        Assert.assertEquals(30, index);
        try {
            returnRS = ((JDBC4ServerPreparedStatement) pstmt).getReturnResultSet();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Statement handle not execute"));
        }

        count = pstmt.executeUpdate(recoverUpdateSql);
        Assert.assertEquals(1, count);
        Assert.assertEquals(1, pstmt.getUpdateCount());
        normalRS = pstmt.getResultSet();
        Assert.assertEquals(null, normalRS);
        try {
            returnRS = ((JDBC4ServerPreparedStatement) pstmt).getReturnResultSet();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Statement handle not execute"));
        }
    }

    @Ignore
    public void executeQueryProcedureWithArguments() throws SQLException {
        //Assume.assumeTrue(sharedOptions().useOraclePrepareExecute && sharedUsePrepare() && sharedOptions().useCursorFetch);
        createProcedureWithArguments();
        ResultSet rs, normalRS, returnRS;

        rs = deleteCs.executeQuery();
        Assert.assertNotEquals(null, rs);
        Assert.assertEquals(-1, deleteCs.getUpdateCount());
        index = 0;
        retrieveInternal(rs);
        Assert.assertEquals(0, index);
        normalRS = deleteCs.getResultSet();
        Assert.assertEquals(null, normalRS);
        try {
            returnRS = ((JDBC4ServerPreparedStatement) deleteCs).getReturnResultSet();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Statement handle not execute"));
        }

        rs = insertCs.executeQuery();
        Assert.assertNotEquals(null, rs);
        Assert.assertEquals(-1, insertCs.getUpdateCount());
        //        index = 0;
        //        retrieveInternal(rs);
        //        Assert.assertEquals(0, index);
        normalRS = insertCs.getResultSet();
        Assert.assertEquals(null, normalRS);
        try {
            returnRS = ((JDBC4ServerPreparedStatement) insertCs).getReturnResultSet();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Statement handle not execute"));
        }

        rs = updateCs.executeQuery();
        Assert.assertNotEquals(null, rs);
        Assert.assertEquals(0, updateCs.getUpdateCount());//todo: oracle -1
        //        index = 0;
        //        retrieveInternal(rs);
        //        Assert.assertEquals(0, index);
        normalRS = updateCs.getResultSet();
        Assert.assertEquals(null, normalRS);
        try {
            returnRS = ((JDBC4ServerPreparedStatement) updateCs).getReturnResultSet();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Statement handle not execute"));
        }

        PreparedStatement pstmt = conn.prepareStatement(selectSql);
        rs = pstmt.executeQuery();
        Assert.assertNotEquals(null, rs);
        Assert.assertEquals(-1, pstmt.getUpdateCount());
        index = 0;
        retrieveInternal(rs);
        Assert.assertEquals(30, index);
        normalRS = pstmt.getResultSet();
        Assert.assertNotEquals(null, normalRS);
        index = 0;
        retrieveInternal(normalRS);
        Assert.assertEquals(0, index);//todo: oracle 1
        // currentRow=-1, totalRowsVisited=31, validRows=0, getString: 未调用 ResultSet.next
        // currentRow=0, totalRowsVisited=32, validRows=0, next: ORA-01002: 提取违反顺序
        try {
            returnRS = ((JDBC4ServerPreparedStatement) pstmt).getReturnResultSet();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Statement handle not execute"));
        }

        rs = pstmt.executeQuery(recoverUpdateSql);
        Assert.assertNotEquals(null, rs);
        Assert.assertEquals(1, pstmt.getUpdateCount());
        //        index = 0;
        //        retrieveInternal(rs);
        //        Assert.assertEquals(0, index);
        normalRS = pstmt.getResultSet();
        Assert.assertEquals(null, normalRS);
        try {
            returnRS = ((JDBC4ServerPreparedStatement) pstmt).getReturnResultSet();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Statement handle not execute"));
        }
    }

    /********* function test *********/
    private void createFunctionWithArguments() throws SQLException {
        // function
        createFunction(deleteFunctionName, "(IC1 IN NUMBER) return number is\n"
                                           + "OC1 number(8);\n" + "begin\n"
                                           + "select count(*) into OC1 from " + tableName
                                           + " where c1 >= IC1;\n" + "delete from " + tableName
                                           + " where c1 >= IC1\n" + "return OC1;\n" + "end "
                                           + deleteFunctionName + ";");
        deleteCs = conn.prepareCall("? = call " + deleteFunctionName + "(?)");
        deleteCs.setInt(1, 28);

        // function Call To Block
        createFunction(insertFunctionName,
            "(IC1 IN NUMBER, IC2 IN VARCHAR, IC3 IN NUMBER) return varchar is\n"
                    + "OC1 varchar(30);\n" + "begin\n" + "insert all into " + tableName
                    + " values(IC1,'28+string') into " + tableName + " values(29,IC2) into "
                    + tableName + " values(IC3,'30+string') select * from dual;\n"
                    + "select c2 into OC1 from " + tableName + " where c1 = IC1;\n"
                    + "return OC1;\n" + "end " + deleteFunctionName + ";");
        insertCs = conn.prepareCall("{? = call " + insertFunctionName + "(?, ?, ?)}");
        insertCs.registerOutParameter(1, Types.VARCHAR);
        insertCs.setInt(2, 28);
        insertCs.setString(3, "29+string");
        insertCs.setInt(4, 30);

        // function Anonymous Block
        createFunction(updateFunctionName, "(IC1 IN VARCHAR) return varchar is\n"
                                           + "OC1 varchar(30);\n" + "begin\n" + "update "
                                           + tableName + " set c2 = IC1 where c1 = 1;\n"
                                           + "select c2 into OC1 from " + tableName
                                           + " where c1 = IC1;\n" + "return OC1;\n" + "end "
                                           + deleteFunctionName + ";");
        updateCs = conn.prepareCall("begin ? := " + updateProcedureName + "(?);end;");
        updateCs.setString(1, "1+changed");
        updateCs.registerOutParameter(2, Types.VARCHAR);
    }

    @Ignore
    public void executeFunctionWithArguments() throws SQLException {
        conn.createStatement().execute("delete from " + tableName);
        CallableStatement callableStatement = null;
        ResultSet resultSet = null;

        callableStatement = conn.prepareCall("select " + insertFunctionName + "(?) from dual");
        callableStatement.setInt(1, 1);
        try {
            Assert.assertEquals(false, callableStatement.execute());
        } catch (AssertionError e) {
            //oracle return false here
            System.out.println(e.getMessage());
        }
        Assert.assertEquals(-1, callableStatement.getUpdateCount());
        resultSet = callableStatement.getResultSet();
        retrieveInternal(resultSet);

        callableStatement = conn.prepareCall("select " + updateFunctionName + "(?) from dual");
        callableStatement.setInt(1, 1);
        try {
            Assert.assertEquals(false, callableStatement.execute());
        } catch (AssertionError e) {
            //oracle return false here
            System.out.println(e.getMessage());
        }
        Assert.assertEquals(-1, callableStatement.getUpdateCount());
        resultSet = callableStatement.getResultSet();
        retrieveInternal(resultSet);

        callableStatement = conn.prepareCall("select " + deleteFunctionName + "(?) from dual");
        callableStatement.setInt(1, 1);
        try {
            Assert.assertEquals(false, callableStatement.execute());
        } catch (AssertionError e) {
            //oracle return false here
            System.out.println(e.getMessage());
        }
        Assert.assertEquals(-1, callableStatement.getUpdateCount());
        resultSet = callableStatement.getResultSet();
        retrieveInternal(resultSet);
    }

    @Ignore
    public void executeUpdateFunctionWithArguments() throws SQLException {
        conn.createStatement().execute("delete from " + tableName);
        CallableStatement callableStatement = null;
        ResultSet resultSet = null;

        callableStatement = conn.prepareCall("select " + insertFunctionName + "(?) from dual");
        callableStatement.setInt(1, 1);

        try {
            Assert.assertEquals(-1, callableStatement.executeUpdate());
        } catch (AssertionError e) {
            //oracle return false here
            System.out.println(e.getMessage());
        }
        Assert.assertEquals(-1, callableStatement.getUpdateCount());
        resultSet = callableStatement.getResultSet();
        retrieveInternal(resultSet);

        callableStatement = conn.prepareCall("select " + updateFunctionName + "(?) from dual");
        callableStatement.setInt(1, 1);
        try {
            Assert.assertEquals(-1, callableStatement.executeUpdate());
        } catch (AssertionError e) {
            //oracle return false here
            System.out.println(e.getMessage());
        }
        Assert.assertEquals(-1, callableStatement.getUpdateCount());
        resultSet = callableStatement.getResultSet();
        retrieveInternal(resultSet);

        callableStatement = conn.prepareCall("select " + deleteFunctionName + "(?) from dual");
        callableStatement.setInt(1, 1);
        try {
            Assert.assertEquals(-1, callableStatement.executeUpdate());
        } catch (AssertionError e) {
            //oracle return false here
            System.out.println(e.getMessage());
        }
        Assert.assertEquals(-1, callableStatement.getUpdateCount());
        resultSet = callableStatement.getResultSet();
        retrieveInternal(resultSet);
    }

    @Ignore
    public void executeQueryFunctionWithArguments() throws SQLException {
        conn.createStatement().execute("delete from " + tableName);
        CallableStatement callableStatement = null;
        ResultSet resultSet = null;

        callableStatement = conn.prepareCall("select " + insertFunctionName + "(?) from dual");
        callableStatement.setInt(1, 1);
        resultSet = callableStatement.executeQuery();
        resultSet.next();
        Assert.assertEquals(3, resultSet.getInt(1));
        Assert.assertEquals(-1, callableStatement.getUpdateCount());
        resultSet = callableStatement.getResultSet();
        retrieveInternal(resultSet);

        callableStatement = conn.prepareCall("select " + updateFunctionName + "(?) from dual");
        callableStatement.setInt(1, 1);
        resultSet = callableStatement.executeQuery();
        resultSet.next();
        Assert.assertEquals(3, resultSet.getInt(1));
        Assert.assertEquals(-1, callableStatement.getUpdateCount());
        resultSet = callableStatement.getResultSet();
        retrieveInternal(resultSet);

        callableStatement = conn.prepareCall("select " + deleteFunctionName + "(?) from dual");
        callableStatement.setInt(1, 1);
        resultSet = callableStatement.executeQuery();
        resultSet.next();
        Assert.assertEquals(0, resultSet.getInt(1));
        Assert.assertEquals(-1, callableStatement.getUpdateCount());
        resultSet = callableStatement.getResultSet();
        retrieveInternal(resultSet);
    }

}
