package com.oceanbase.jdbc;

import java.sql.*;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ExecuteReturnTest extends BaseTest {

    static String     tableName           = "Execute_Return_Test";
    static String     tableColumns        = "c1 int, c2 varchar(30)";
    static String     insertProcedureName = "test_procedure_insert";
    static String     updateProcedureName = "test_procedure_update";
    static String     deleteProcedureName = "test_procedure_delete";
    static String     insertFunctionName  = "test_function_insert";
    static String     updateFunctionName  = "test_function_update";
    static String     deleteFunctionName  = "test_function_delete";

    static String     deleteSql           = "delete from " + tableName + " where c1 >= ?";
    static String     insertSql           = "insert into " + tableName
                                            + " values(?,'28+string'), (29,?), (?,'30+string')";
    static String     updateSql           = "update " + tableName + " set c2 = ? where c1 = 1";
    static String     selectSql           = "select c2 from " + tableName;
    static String     recoverSql          = "update " + tableName
                                            + " set c2 = '1+string' where c1 = 1";

    static Connection conn;
    int               index;

    PreparedStatement deletePs;
    PreparedStatement insertPs;
    PreparedStatement updatePs;
    CallableStatement insertCs;
    CallableStatement updateCs;
    CallableStatement deleteCs;

    @BeforeClass
    public static void initTable() throws SQLException {
        createTable(tableName, tableColumns);
        String query = "insert into " + tableName + " values(1,'1+string')";
        for (int i = 2; i <= 30; i++) {
            query += ",(" + i + ",'" + i + "+string')";
        }
        Statement pstmt = sharedConnection.createStatement();
        pstmt.execute(query);
        pstmt.close();

        //conn = setConnection("&useServerPrepStmts=true&useCursorFetch=true");
        conn = sharedConnection;
    }

    private void retrieveInternal(ResultSet rs) throws SQLException {
        if (rs != null) {
            try {
                ResultSetMetaData meta = rs.getMetaData();
                try {
                    String colName = meta.getColumnName(1);
                    if (!colName.equalsIgnoreCase("c2")
                        && !colName.equalsIgnoreCase("@com_mysql_jdbc_outparam_OC1")) {
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

    @Test
    public void executeNormalSql() throws SQLException {
        createPrepareStatement();
        boolean bool;
        ResultSet normalRS;

        bool = deletePs.execute();
        Assert.assertEquals(false, bool);
        Assert.assertEquals(3, deletePs.getUpdateCount());
        normalRS = deletePs.getResultSet();
        Assert.assertEquals(null, normalRS);

        bool = insertPs.execute();
        Assert.assertEquals(false, bool);
        Assert.assertEquals(3, insertPs.getUpdateCount());
        normalRS = insertPs.getResultSet();
        Assert.assertEquals(null, normalRS);

        bool = updatePs.execute();
        Assert.assertEquals(false, bool);
        Assert.assertEquals(1, updatePs.getUpdateCount());
        normalRS = updatePs.getResultSet();
        Assert.assertEquals(null, normalRS);

        PreparedStatement pstmt = conn.prepareStatement(selectSql);
        bool = pstmt.execute();
        Assert.assertEquals(true, bool);
        Assert.assertEquals(-1, pstmt.getUpdateCount());
        normalRS = pstmt.getResultSet();
        Assert.assertNotEquals(null, normalRS);
        index = 0;
        retrieveInternal(normalRS);
        Assert.assertEquals(30, index);

        bool = pstmt.execute(recoverSql);
        Assert.assertEquals(false, bool);
        Assert.assertEquals(1, pstmt.getUpdateCount());
        normalRS = pstmt.getResultSet();
        Assert.assertEquals(null, normalRS);
    }

    @Test
    public void executeUpdateNormalSql() throws SQLException {
        createPrepareStatement();
        int count;
        ResultSet normalRS;

        count = deletePs.executeUpdate();
        Assert.assertEquals(3, count);
        Assert.assertEquals(3, deletePs.getUpdateCount());
        normalRS = deletePs.getResultSet();
        Assert.assertEquals(null, normalRS);

        count = insertPs.executeUpdate();
        Assert.assertEquals(3, count);
        Assert.assertEquals(3, insertPs.getUpdateCount());
        normalRS = insertPs.getResultSet();
        Assert.assertEquals(null, normalRS);

        count = updatePs.executeUpdate();
        Assert.assertEquals(1, count);
        Assert.assertEquals(1, updatePs.getUpdateCount());
        normalRS = updatePs.getResultSet();
        Assert.assertEquals(null, normalRS);

        PreparedStatement pstmt = conn.prepareStatement(selectSql);
        try {
            count = pstmt.executeUpdate();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains(
                "executeUpdate() or executeLargeUpdate() isn't available for SELECT statement"));
        }

        count = pstmt.executeUpdate(recoverSql);
        Assert.assertEquals(1, count);
        Assert.assertEquals(1, pstmt.getUpdateCount());
        normalRS = pstmt.getResultSet();
        Assert.assertEquals(null, normalRS);
    }

    @Test
    public void executeQueryNormalSql() throws SQLException {
        createPrepareStatement();
        ResultSet rs, normalRS;

        try {
            rs = deletePs.executeQuery();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains(
                "executeQuery() isn't available for DML statement"));
        }

        try {
            rs = insertPs.executeQuery();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains(
                "executeQuery() isn't available for DML statement"));
        }

        try {
            rs = updatePs.executeQuery();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains(
                "executeQuery() isn't available for DML statement"));
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
        Assert.assertEquals(0, index);

        try {
            rs = pstmt.executeQuery(recoverSql);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains(
                "executeQuery() isn't available for DML statement"));
        }
    }

    /********* procedure test *********/
    private void createProcedureWithArguments() throws SQLException {
        createProcedure(deleteProcedureName, "(IN IC1 INT, OUT OC1 VARCHAR(30))\n" + "begin\n"
                                             + "select c2 into OC1 from " + tableName
                                             + " where c1 = IC1;\n" + "delete from " + tableName
                                             + " where c1 >= IC1;\n" + "end\n");
        deleteCs = conn.prepareCall("call " + deleteProcedureName + "(?, ?)");
        deleteCs.setInt(1, 28);
        deleteCs.registerOutParameter(2, Types.VARCHAR);

        createProcedure(insertProcedureName,
            "(IN IC1 INT, IN IC2 VARCHAR(30), IN IC3 INT, OUT OC1 VARCHAR(30))\n" + "begin\n"
                    + "insert into " + tableName
                    + " values(IC1,'28+string'), (29,IC2), (IC3,'30+string');\n"
                    + "select c2 into OC1 from " + tableName + " where c1 = IC1;\n" + "end;");
        insertCs = conn.prepareCall("{call " + insertProcedureName + "(?, ?, ?, ?)}");
        insertCs.setInt(1, 28);
        insertCs.setString(2, "29+string");
        insertCs.setInt(3, 30);
        insertCs.registerOutParameter(4, Types.VARCHAR);

        createProcedure(updateProcedureName, "(IN IC1 VARCHAR(30))\n" + "begin\n" + "update "
                                             + tableName + " set c2 = IC1 where c1 = 1;\n" + "end;");
        updateCs = conn.prepareCall("call " + updateProcedureName + "(?)");
        updateCs.setString(1, "1+changed");
    }

    @Test
    public void executeProcedureWithArguments() throws SQLException {
        createProcedureWithArguments();
        boolean bool;
        ResultSet normalRS;

        bool = deleteCs.execute();
        Assert.assertEquals(true, bool);//todo: mysql false
        Assert.assertEquals(-1, deleteCs.getUpdateCount());//todo: mysql 3
        normalRS = deleteCs.getResultSet();
        Assert.assertNotEquals(null, normalRS);//todo: mysql null
        index = 0;
        retrieveInternal(normalRS);
        Assert.assertEquals(0, index);

        bool = insertCs.execute();
        Assert.assertEquals(true, bool);//todo: mysql false
        Assert.assertEquals(-1, deleteCs.getUpdateCount());//todo: mysql 1
        normalRS = insertCs.getResultSet();
        Assert.assertNotEquals(null, normalRS);//todo: mysql null
        index = 0;
        retrieveInternal(normalRS);
        Assert.assertEquals(0, index);

        bool = updateCs.execute();
        Assert.assertEquals(true, bool);//todo: mysql false
        Assert.assertEquals(-1, deleteCs.getUpdateCount());//todo: mysql 1
        normalRS = updateCs.getResultSet();
        Assert.assertEquals(null, normalRS);

        PreparedStatement pstmt = conn.prepareStatement(selectSql);
        bool = pstmt.execute();
        Assert.assertEquals(true, bool);
        Assert.assertEquals(-1, pstmt.getUpdateCount());
        normalRS = pstmt.getResultSet();
        Assert.assertNotEquals(null, normalRS);
        index = 0;
        retrieveInternal(normalRS);
        Assert.assertEquals(30, index);

        bool = pstmt.execute(recoverSql);
        Assert.assertEquals(false, bool);
        Assert.assertEquals(1, pstmt.getUpdateCount());
        normalRS = pstmt.getResultSet();
        Assert.assertEquals(null, normalRS);
    }

    @Test
    public void executeUpdateProcedureWithArguments() throws SQLException {
        createProcedureWithArguments();
        int count;
        ResultSet normalRS;

        count = deleteCs.executeUpdate();
        Assert.assertEquals(1, count);//todo: mysql 3
        Assert.assertEquals(-1, deleteCs.getUpdateCount());//todo: mysql 3
        normalRS = deleteCs.getResultSet();
        Assert.assertNotEquals(null, normalRS);//todo: mysql null
        index = 0;
        retrieveInternal(normalRS);
        Assert.assertEquals(0, index);

        count = insertCs.executeUpdate();
        Assert.assertEquals(1, count);
        Assert.assertEquals(-1, deleteCs.getUpdateCount());//todo: mysql 1
        normalRS = insertCs.getResultSet();
        Assert.assertNotEquals(null, normalRS);//todo: mysql null
        index = 0;
        retrieveInternal(normalRS);
        Assert.assertEquals(0, index);

        count = updateCs.executeUpdate();
        Assert.assertEquals(0, count);//todo: mysql 1
        Assert.assertEquals(-1, deleteCs.getUpdateCount());//todo: mysql 1
        normalRS = updateCs.getResultSet();
        Assert.assertEquals(null, normalRS);

        PreparedStatement pstmt = conn.prepareStatement(selectSql);
        try {
            count = pstmt.executeUpdate();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains(
                "executeUpdate() or executeLargeUpdate() isn't available for SELECT statement"));
        }

        count = pstmt.executeUpdate(recoverSql);
        Assert.assertEquals(1, count);
        Assert.assertEquals(1, pstmt.getUpdateCount());
        normalRS = pstmt.getResultSet();
        Assert.assertEquals(null, normalRS);
    }

    @Test
    public void executeQueryProcedureWithArguments() throws SQLException {
        createProcedureWithArguments();
        ResultSet rs, normalRS;

        rs = deleteCs.executeQuery();
        Assert.assertNotEquals(null, rs);
        Assert.assertEquals(-1, deleteCs.getUpdateCount());//todo: mysql 3
        index = 0;
        retrieveInternal(rs);
        Assert.assertEquals(0, index);
        normalRS = deleteCs.getResultSet();
        Assert.assertNotEquals(null, normalRS);//todo: mysql null
        index = 0;
        retrieveInternal(normalRS);
        Assert.assertEquals(0, index);

        rs = insertCs.executeQuery();
        Assert.assertNotEquals(null, rs);
        Assert.assertEquals(-1, deleteCs.getUpdateCount());//todo: mysql 1
        //        index = 0;
        //        retrieveInternal(rs);
        //        Assert.assertEquals(0, index);
        normalRS = insertCs.getResultSet();
        Assert.assertNotEquals(null, normalRS);//todo: mysql null
        index = 0;
        retrieveInternal(normalRS);
        Assert.assertEquals(0, index);

        rs = updateCs.executeQuery();
        Assert.assertEquals(null, rs);//todo: mysql not null
        Assert.assertEquals(-1, deleteCs.getUpdateCount());//todo: mysql 1
        //        index = 0;
        //        retrieveInternal(rs);
        //        Assert.assertEquals(0, index);
        normalRS = updateCs.getResultSet();
        Assert.assertEquals(null, normalRS);

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
        Assert.assertEquals(0, index);

        try {
            rs = pstmt.executeQuery(recoverSql);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains(
                "executeQuery() isn't available for DML statement"));
        }
    }

}
