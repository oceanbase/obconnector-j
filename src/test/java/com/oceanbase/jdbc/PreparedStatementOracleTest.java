/*
 *
 * OceanBase Client for Java
 *
 * Copyright (c) 2022 OceanBase.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */
package com.oceanbase.jdbc;

import java.sql.*;

import org.junit.Assert;
import org.junit.Test;

public class PreparedStatementOracleTest extends BaseOracleTest {

    @Test
    public void fix46694422() throws SQLException {
        Assert.assertTrue(sharedUsePrepare());
        try {
            sharedConnection.createStatement().execute("drop table t0");
        } catch (SQLException e) {
        }
        sharedConnection.createStatement().execute("create table t0(c0 int, c1 int)");
        for (int i = 0; i < 51; i++) {
            Statement s = sharedConnection.createStatement();
            ResultSet rs = s.executeQuery("select * from t0");
            while (rs.next()) {
            }
            System.out.println(i + ":close");
            s.close();
        }
    }

    @Test
    public void testSetFixedCHAR() throws SQLException {
        Assert.assertFalse(sharedUsePrepare());
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        ObPrepareStatement pstmt = null;

        stmt = sharedConnection.createStatement();
        try {
            stmt.execute("DROP TABLE TEST_SETFIXEDCHAR");
        } catch (SQLException ignored) {
        }
        stmt.execute("CREATE TABLE TEST_SETFIXEDCHAR(C1 CHAR(10))");
        stmt.execute("INSERT INTO TEST_SETFIXEDCHAR (C1) VALUES ('ob')");
        String sql = "SELECT C1 FROM TEST_SETFIXEDCHAR WHERE C1 = ?";

        //TextRowProtocol
        pstmt = (ObPrepareStatement) sharedConnection.prepareStatement(sql);
        pstmt.setFixedCHAR(1, "ob");
        rs = pstmt.executeQuery();
        Assert.assertTrue(rs.next());
        Assert.assertEquals("ob        ", rs.getString(1));

        pstmt.setString(1, "ob");
        rs = pstmt.executeQuery();
        Assert.assertTrue(rs.next()); // oracle: Assert.assertFalse(rs.next());
        Assert.assertEquals("ob        ", rs.getString(1));

        //BinaryRowProtocol
        conn = setConnection("&useServerPrepStmts=true");
        pstmt = (ObPrepareStatement) conn.prepareStatement(sql);
        pstmt.setFixedCHAR(1, "ob");
        rs = pstmt.executeQuery();
        Assert.assertTrue(rs.next());
        Assert.assertEquals("ob        ", rs.getString(1));

        pstmt.setString(1, "ob");
        rs = pstmt.executeQuery();
        Assert.assertTrue(rs.next()); // oracle: Assert.assertFalse(rs.next());
        Assert.assertEquals("ob        ", rs.getString(1));

        rs.close();
        pstmt.close();
        stmt.close();
    }

    @Test
    public void testGeneratedKeysByColumnIndexes() throws Exception {
        Connection conn = setConnection("&useOraclePrepareExecute=true");

        Statement stmt = conn.createStatement();
        // create sequence
        try {
            stmt.execute("DROP SEQUENCE S1;");
        } catch (Exception ignored) {

        } finally {
            stmt.execute("CREATE SEQUENCE S1 START WITH 1 INCREMENT BY 1");
        }
        // create table
        try {
            stmt.execute("DROP TABLE T1;");
        } catch (Exception ignored) {

        } finally {
            stmt.execute("CREATE TABLE T1 (C1 NUMBER(10) PRIMARY KEY, C2 VARCHAR(20));");
        }
        // create trigger
        stmt.execute("CREATE OR REPLACE TRIGGER TRI1\n" + "BEFORE INSERT ON T1\n"
                     + "FOR EACH ROW\n" + "BEGIN\n" + "  SELECT S1.NEXTVAL\n" + "  INTO :new.C1\n"
                     + "  FROM dual;\n" + "END;");

        PreparedStatement pstmt = conn.prepareStatement("insert into t1(c2) values ('test1')",
            new int[] { 1, 2 });
        pstmt.executeUpdate();
        ResultSet rs = pstmt.getGeneratedKeys();
        Assert.assertTrue(rs.next());
        Assert.assertEquals("\"TEST\".\"T1\".\"C1\"", rs.getMetaData().getColumnName(1));
        Assert.assertEquals(1, rs.getInt(1));
        Assert.assertEquals("\"TEST\".\"T1\".\"C2\"", rs.getMetaData().getColumnName(2));
        Assert.assertEquals("test1", rs.getString(2));
        Assert.assertFalse(rs.next());

        try {
            pstmt = conn.prepareStatement("insert into t1(c2) values ('test2')",
                new int[] { 1, 3 });
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("index 2 must be in [0, 1]"));
        }

        try {
            pstmt = conn.prepareStatement("insert into t1(c2) values ('test3')",
                new int[] { 1, 2, 3 });
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("index 2 must be in [0, 1]"));
        }
    }

    @Test
    public void testGeneratedKeysByColumnNames() throws Exception {
        Connection conn = setConnection("&useOraclePrepareExecute=true");

        Statement stmt = conn.createStatement();
        // create sequence
        try {
            stmt.execute("DROP SEQUENCE S1;");
        } catch (Exception ignored) {

        } finally {
            stmt.execute("CREATE SEQUENCE S1 START WITH 1 INCREMENT BY 1");
        }
        // create table
        try {
            stmt.execute("DROP TABLE T1;");
        } catch (Exception ignored) {

        } finally {
            stmt.execute("CREATE TABLE T1 (C1 NUMBER(10) PRIMARY KEY, C2 VARCHAR(20));");
        }
        // create trigger
        stmt.execute("CREATE OR REPLACE TRIGGER TRI1\n" + "BEFORE INSERT ON T1\n"
                     + "FOR EACH ROW\n" + "BEGIN\n" + "  SELECT S1.NEXTVAL\n" + "  INTO :new.C1\n"
                     + "  FROM dual;\n" + "END;");

        PreparedStatement pstmt = conn.prepareStatement("insert into t1(c2) values ('test1')",
            new String[] { "C1", "C2" });
        pstmt.executeUpdate();
        ResultSet rs = pstmt.getGeneratedKeys();
        Assert.assertTrue(rs.next());
        Assert.assertEquals("\"TEST\".\"T1\".\"C1\"", rs.getMetaData().getColumnName(1));
        Assert.assertEquals(1, rs.getInt(1));
        Assert.assertEquals("\"TEST\".\"T1\".\"C2\"", rs.getMetaData().getColumnName(2));
        Assert.assertEquals("test1", rs.getString(2));
        Assert.assertFalse(rs.next());

        try {
            pstmt = conn.prepareStatement("insert into t1(c2) values ('test2')",
                new String[] {"C1", "C3" });
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Invalid parameter \"C3\""));
        }

        try {
            pstmt = conn.prepareStatement("insert into t1(c2) values ('test3')",
                new String[] {"C1", "C2", "C3" });
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Invalid parameter \"C3\""));
        }
    }

}
