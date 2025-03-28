package com.oceanbase.jdbc;

import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class CharsetOracleTest extends BaseOracleTest{

    @Test
    public void testHKSCSForDima2024062200102857062() throws SQLException {
        Connection conn = sharedConnection;
        Statement stmt = conn.createStatement();
        createTable("t_c1", "c1 varchar2(60)");
        stmt.execute("insert into t_c1 values('嘅_係_嘢_冇_咁_镬_嘥_1')");
        ResultSet rs = stmt.executeQuery("select * from t_c1");
        Assert.assertTrue(rs.next());
        Assert.assertEquals("嘅_係_嘢_冇_咁_?_嘥_1", rs.getString(1));

        conn = setConnection("&characterEncoding=HKSCS");
        createTable("t_c2", "c1 varchar2(60)");
        stmt = conn.createStatement();
        stmt.execute("insert into t_c2 values('嘅_係_嘢_冇_咁_镬_嘥_2')");
        rs = stmt.executeQuery("select * from t_c2");
        Assert.assertTrue(rs.next());
        Assert.assertEquals("嘅_係_嘢_冇_咁_镬_嘥_2", rs.getString(1));
    }

    @Test
    public void testHKSCS31ForDima2024062200102857062() throws SQLException {
        Connection conn = sharedConnection;
        Statement stmt = conn.createStatement();
        createTable("t_c1", "c1 varchar2(60)");
        stmt.execute("insert into t_c1 values('嘅_係_嘢_冇_咁_镬_嘥_1')");
        ResultSet rs = stmt.executeQuery("select * from t_c1");
        Assert.assertTrue(rs.next());
        Assert.assertEquals("嘅_係_嘢_冇_咁_?_嘥_1", rs.getString(1));

        conn = setConnection("&characterEncoding=HKSCS31");
        createTable("t_c2", "c1 varchar2(60)");
        stmt = conn.createStatement();
        stmt.execute("insert into t_c2 values('嘅_係_嘢_冇_咁_镬_嘥_2')");
        rs = stmt.executeQuery("select * from t_c2");
        Assert.assertTrue(rs.next());
        Assert.assertEquals("嘅_係_嘢_冇_咁_镬_嘥_2", rs.getString(1));
    }
}
