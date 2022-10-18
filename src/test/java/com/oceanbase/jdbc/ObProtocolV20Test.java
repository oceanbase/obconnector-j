package com.oceanbase.jdbc;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

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
}
