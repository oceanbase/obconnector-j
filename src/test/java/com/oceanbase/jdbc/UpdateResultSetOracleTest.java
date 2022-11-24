package com.oceanbase.jdbc;

import static org.junit.Assert.assertEquals;

import java.sql.*;

import org.junit.Assert;
import org.junit.Test;

public class UpdateResultSetOracleTest extends BaseOracleTest {

    @Test
    public void test1() throws SQLException {
        createTable("ResultSetTypePRIMARY",
            "c1 INT, c2 VARCHAR2(100), constraint pk primary key(c1)");
        Statement stmt = sharedConnection.createStatement();
        stmt.execute("insert into ResultSetTypePRIMARY values(1, 'var1')"); // cmd_query

        Connection conn = setConnectionOrigin("?useServerPrepStmts=true&useOraclePrepareExecute=true");
        PreparedStatement pstmt = conn
            .prepareStatement("insert into ResultSetTypePRIMARY values(?, ?)");

        pstmt.setInt(1, 2);
        pstmt.setString(2, "var2");
        pstmt.execute();

        pstmt.setInt(1, 3);
        pstmt.setString(2, "var3");
        pstmt.executeQuery();

        PreparedStatement ps = conn.prepareStatement(" /**/SELECT c1,c2 FROM ResultSetTypePRIMARY",
            ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = ps.executeQuery();
        assertEquals(ResultSet.TYPE_SCROLL_SENSITIVE, rs.getType());
        assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
        assertEquals("ROWID", rs.getMetaData().getColumnName(0));
        int i = 1;
        while (rs.next()) {
            assertEquals(i++, rs.getInt(1));
        }

        pstmt = conn.prepareStatement("delete from ResultSetTypePRIMARY where c1=?");
        pstmt.setInt(1, 1);
        pstmt.executeQuery();
    }

    @Test
    public void testDefaultPrimaryKey() throws Exception {
        createTable("testUpdatedAbleInsert", "c1 int , c2 int , c3 int, PRIMARY KEY (c1, c2)");

        Statement stmt = sharedConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT c1, c2, c3 FROM testUpdatedAbleInsert");

        rs.moveToInsertRow();
        rs.updateInt("c1", 1);
        //        rs.updateInt("c2", 1);
        rs.updateInt("c3", 1);
        try {
            rs.insertRow();
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("ORA-01400: cannot insert NULL into '(C2)'"));
        }

        PreparedStatement ps = sharedPSConnection.prepareStatement(
            "SELECT c1, c2, c3 FROM testUpdatedAbleInsert", ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_UPDATABLE);
        rs = ps.executeQuery();

        rs.moveToInsertRow();
        rs.updateInt("c1", 2);
        //        rs.updateInt("c2", 1);
        rs.updateInt("c3", 2);
        try {
            rs.insertRow();
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("ORA-01400: cannot insert NULL into '(C2)'"));
        }
    }

}
