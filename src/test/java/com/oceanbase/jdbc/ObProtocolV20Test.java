package com.oceanbase.jdbc;

import java.sql.*;

import org.junit.Assert;
import org.junit.Test;

public class ObProtocolV20Test extends BaseTest {
    @Test
    public void fix42037949() throws SQLException {
        Connection conn = setConnection("&enableFullLinkTrace=true");
        Statement stmt = conn.createStatement();

        stmt.execute("DROP DATABASE IF EXISTS database0");
        stmt.execute("CREATE DATABASE database0");
        stmt.execute("USE database0");
        conn.setCatalog("database0");

        stmt.execute("CREATE TABLE t0(c0 VARCHAR(500) )  PARTITION BY KEY (c0) partitions 15");

        stmt.execute("drop procedure if exists execute_pl");
        stmt.execute("create procedure execute_pl(in_sql varchar(1000)) begin set @_sql = in_sql; PREPARE stmt from @_sql; EXECUTE stmt ; DEALLOCATE PREPARE stmt; end;");
        String sql = "REPLACE INTO t0(c0) VALUES(\"_u\");";
        executeMysqlPl(conn, sql);
        sql = "INSERT INTO t1(c0) VALUES('200334016336822004');";
        executeMysqlPl(conn, sql);
    }

    static boolean executeMysqlPl(Connection con, String query) throws SQLException {
        System.out.println("query:" + query);

        CallableStatement call = con.prepareCall("{call execute_pl(?)}");
        call.setString(1, query);

        try {
            call.execute();
            call.close();
            return true;
        } catch (Exception e) {
            if (call != null) {
                call.close();
            }
            return false;
        }
    }

    @Test
    public void fix44336056() throws SQLException {
        Connection conn = setConnection("&enableFullLinkTrace=true&useServerPrepStmts=true");//&useOceanBaseProtocolV20=false
        String tableName = "test_send_split_payload";
        createTable(tableName, "c1 int, c2 varchar(30), constraint pk_yxy primary key(c1)");

        ((OceanBaseConnection) conn).setFullLinkTraceIdentifier("OB_JDBC");
        Statement stmt = conn.createStatement();
        stmt.execute("select 1 from dual");

        String x = "";
        for (int j = 0; j < 1000; j++) {
            x += "hello000000000012313123";
        }

        String query = "insert into " + tableName + " values(1,'1+" + x + "')";
        for (int i = 2; i <= 800; i++) {
            query += ",(" + i + ",'" + i + x + "')";
        }
        System.out.println("\n************* insert all into *************");
        System.out.println("\n query len:" + query.length());
        stmt.execute(query);
        stmt.execute("select 1 from dual");
        conn.commit();
        stmt.close();
        conn.close();
    }

    @Test
    public void testShowTrace() throws SQLException {
        Connection conn = setConnection("&enableFullLinkTrace=true");
        Statement stmt = conn.createStatement();

        stmt.execute("set ob_enable_show_trace = true");
        stmt.execute("drop table if exists test_trace");
        stmt.execute("create table test_trace(c1 int)");
        stmt.execute("insert into test_trace values (111)");
        // commit
        ResultSet rs = stmt.executeQuery("select * from test_trace");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(111, rs.getInt(1));
        Assert.assertFalse(rs.next());
        rs = stmt.executeQuery("show trace");
        Assert.assertTrue(rs.next());
        while (rs.next()) {
            System.out.println(rs.getString(1) + "    " + rs.getString(2) + "    " + rs.getString(3));
        }

        stmt.execute("set ob_enable_show_trace = false");
        stmt.execute("drop table test_trace");
        stmt.executeQuery("select 1 from dual");
        rs = stmt.executeQuery("show trace");
        Assert.assertFalse(rs.next());
    }

}
