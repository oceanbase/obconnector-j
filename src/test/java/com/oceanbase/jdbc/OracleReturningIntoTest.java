package com.oceanbase.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.*;

import org.junit.Assert;
import org.junit.Test;

public class OracleReturningIntoTest extends BaseOracleTest {

    private Connection conn;
    private Connection connPE;
    private String     tableName = "DeleteReturn_ingInto";

    @Test
    public void testDMLReturningInto() throws Exception {
        createTable(tableName, "c1 INT, c2 VARCHAR2(100)");
        String query = "insert all";
        for (int i = 1; i <= 50; i++) {
            query += " into " + tableName + " values(" + i + ",'" + i + "+string')";
        }
        query += " select * from dual";
        Statement stmt = sharedConnection.createStatement();
        stmt.execute(query);
        stmt.close();

        conn = setConnection("&useServerPrepStmts=true&useCursorFetch=true&useOraclePrepareExecute=false");
        connPE = setConnection("&useServerPrepStmts=true&useCursorFetch=true&useOraclePrepareExecute=true");
        testExecuteUpdate();
        testExecute();
        testExecuteQuery();
    }

    private void testExecuteUpdate() throws SQLException {
        PreparedStatement pstmt = conn.prepareStatement("select * from " + tableName);
        int count = pstmt.executeUpdate();
        Assert.assertEquals(0, count);
        Assert.assertEquals(-1, pstmt.getUpdateCount());

        pstmt = conn.prepareStatement("update " + tableName
                                      + " set c2 = ' returning val into ?' where c1 = 1");
        count = pstmt.executeUpdate();
        Assert.assertEquals(1, count);
        Assert.assertEquals(1, pstmt.getUpdateCount());

        PreparedStatement pstmt1 = connPE.prepareStatement("delete from " + tableName
                                                           + " where c1 < ? returning c2 into ?");
        pstmt1.setInt(1, 3);
        ((JDBC4ServerPreparedStatement) pstmt1).registerReturnParameter(2, Types.VARCHAR);
        int count1 = pstmt1.executeUpdate();
        Assert.assertEquals(2, count1);
        Assert.assertEquals(2, pstmt1.getUpdateCount());

        // getReturnResultSet
        ResultSet rset1 = ((JDBC4ServerPreparedStatement) pstmt1).getReturnResultSet();
        Assert.assertTrue(rset1.next());
        Assert.assertEquals(" returning val into ?", rset1.getString(1));
        Assert.assertTrue(rset1.next());
        Assert.assertEquals("2+string", rset1.getString(1));
        Assert.assertFalse(rset1.next());

        count1 = pstmt1.executeUpdate("insert all into " + tableName
                                      + " values(1,'1+string') into " + tableName
                                      + " values(2,'2+string') select * from dual");
        Assert.assertEquals(2, count1);
        Assert.assertEquals(2, pstmt1.getUpdateCount());
    }

    private void testExecute() throws SQLException {
        PreparedStatement pstmt = conn.prepareStatement("select * from " + tableName);
        boolean bool = pstmt.execute();
        Assert.assertTrue(bool);
        Assert.assertEquals(-1, pstmt.getUpdateCount());
        pstmt = conn.prepareStatement("update " + tableName
                                      + " set c2 = ' returning val into ?' where c1 = 1");
        bool = pstmt.execute();
        Assert.assertFalse(bool);
        Assert.assertEquals(1, pstmt.getUpdateCount());
        pstmt.close();

        PreparedStatement pstmt1 = connPE
            .prepareStatement("update "
                              + tableName
                              + " set c2 = ' returning val into ?' where c1 = ? returning c2 into ?");
        pstmt1.setInt(1, 3);
        ((JDBC4ServerPreparedStatement) pstmt1).registerReturnParameter(2, Types.VARCHAR);
        boolean bool1 = pstmt1.execute();
        Assert.assertFalse(bool1);
        Assert.assertEquals(1, pstmt1.getUpdateCount());

        // getReturnResultSet
        ResultSet rset1 = ((JDBC4ServerPreparedStatement) pstmt1).getReturnResultSet();
        Assert.assertTrue(rset1.next());
        Assert.assertEquals(" returning val into ?", rset1.getString(1));
        Assert.assertFalse(rset1.next());

        bool1 = pstmt1.execute("update " + tableName + " set c2 = '3+string' where c1 = 3");
        Assert.assertFalse(bool1);
        Assert.assertEquals(1, pstmt.getUpdateCount());
    }

    private void testExecuteQuery() throws SQLException {
        PreparedStatement pstmt = conn.prepareStatement("select * from " + tableName);
        ResultSet rs = pstmt.executeQuery();
        Assert.assertEquals(-1, pstmt.getUpdateCount());

        pstmt = conn.prepareStatement("update " + tableName
                                      + " set c2 = ' returning val into ?' where c1 = 1");
        rs = pstmt.executeQuery();
        Assert.assertEquals(1, pstmt.getUpdateCount());

        PreparedStatement pstmt1 = connPE
            .prepareStatement("insert into " + tableName
                              + " values(51,'51+string') returning c1, c2 into ?, ?");
        ((JDBC4ServerPreparedStatement) pstmt1).registerReturnParameter(1, Types.INTEGER);
        ((JDBC4ServerPreparedStatement) pstmt1).registerReturnParameter(2, Types.VARCHAR);
        ResultSet rs1 = pstmt1.executeQuery();
        Assert.assertEquals(1, pstmt1.getUpdateCount());

        Assert.assertTrue(rs1.next());
        Assert.assertEquals(51, rs1.getInt(1));
        Assert.assertEquals("51+string", rs1.getString(2));
        Assert.assertFalse(rs1.next());
    }

    @Test
    public void testReturningBatch() throws SQLException {
        createTable("testReturn", "c1 INT, c2 VARCHAR2(200)");
        Connection conn = setConnection("&useServerPrepStmts=true&useCursorFetch=true&useOraclePrepareExecute=true");
        PreparedStatement pstmt = conn
            .prepareStatement("insert into testReturn values(?,?) returning c1, c2 into ?,?");
        pstmt.setInt(1, 1);
        pstmt.setString(2, "returning c1, c2 into ?,?");
        ((JDBC4ServerPreparedStatement) pstmt).registerReturnParameter(3, Types.INTEGER);
        ((JDBC4ServerPreparedStatement) pstmt).registerReturnParameter(4, Types.LONGVARCHAR);
        try {
            pstmt.addBatch();
            Assert.fail();

            pstmt.setInt(1, 2);
            pstmt.setString(2, "10+string");
            ((JDBC4ServerPreparedStatement) pstmt).registerReturnParameter(3, Types.BIT);
            ((JDBC4ServerPreparedStatement) pstmt).registerReturnParameter(4, Types.VARCHAR);
            pstmt.addBatch();
            pstmt.executeBatch();

            ResultSet rs = ((JDBC4ServerPreparedStatement) pstmt).getReturnResultSet();
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("returning c1, c2 into ?,?", rs.getString(2));
            rs = ((JDBC4ServerPreparedStatement) pstmt).getReturnResultSet();
            assertTrue(rs.next()); //无结果集
            assertEquals(2, rs.getInt(1));
            assertEquals("10+string", rs.getString(2));

            ResultSet rs1 = conn.createStatement().executeQuery("select count(*) from testReturn");
            assertTrue(rs1.next());
            assertEquals(2, rs1.getInt(1));

        } catch (SQLFeatureNotSupportedException e) {
            Assert.assertTrue(e.getMessage().contains(
                "not support batch operation for DML_Returning_Into feature"));
        }
    }
}
