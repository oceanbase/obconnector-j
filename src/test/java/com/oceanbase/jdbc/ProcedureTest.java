/*
 *
 * OceanBase Client for Java
 *
 * Copyright (c) 2021 OceanBase.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.sql.*;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ProcedureTest extends BaseTest {
    public static String procName  = "testProc" + getRandomString(5);
    public static String tableUser = "tUser" + getRandomString(5);

    @BeforeClass
    public static void initClass() throws SQLException {
        createTable(tableUser, "user_id varchar(10),name varchar(10),age varchar(10)");
        createProcedure(procName, "(\n" + "  in v_user_id varchar(10),\n"
                                  + "  in v_name varchar(10),\n" + "  in v_age varchar(10),\n"
                                  + "  out v_result1 varchar(10)\n" + ")\n" + "BEGIN    \n"
                                  + "DELETE FROM " + tableUser + " ;      \n" + "INSERT INTO "
                                  + tableUser
                                  + "(user_id, name, age) VALUES (v_user_id, v_name, v_age);    \n"
                                  + "select name INTO v_result1 from " + tableUser + ";   \n"
                                  + "END;");
    }

    @Test
    public void procTest() {
        try {
            String sql = "call " + procName + "(?,?,?,?)";
            CallableStatement cs = sharedConnection.prepareCall(sql);
            cs.setString(1, "0");
            cs.setString(2, "teststring");
            cs.setString(3, "30");
            cs.registerOutParameter(4, Types.VARCHAR);
            cs.execute();
            String res = cs.getString(4);
            Assert.assertEquals("teststring", res);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testBatch() throws Exception {
        createTable("testBatchTable", "field1 INT");
        createProcedure("testBatch",
            "(IN foo int)\nbegin\nINSERT INTO testBatchTable VALUES (foo);\nend\n");
        Statement stmt = sharedConnection.createStatement();

        CallableStatement storedProc = sharedConnection.prepareCall("{call testBatch(?)}");
        int numBatches = 3;
        for (int i = 0; i < numBatches; i++) {
            storedProc.setInt(1, i + 1);
            storedProc.addBatch();
        }
        int[] counts = storedProc.executeBatch();
        Assert.assertEquals(numBatches, counts.length);
        //        for (int i = 0; i < numBatches; i++) {
        //            Assert.assertEquals(1, counts[i]);   // wrong server result
        //        }

        ResultSet rs = stmt.executeQuery("SELECT field1 FROM testBatchTable ORDER BY field1 ASC");
        for (int i = 0; i < numBatches; i++) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(i + 1, rs.getInt(1));
        }
    }

    @Test
    public void testCycleProcedure() throws Exception {
        try {
            Connection conn = sharedConnection;
            createProcedure("pro2", "(out var int)\nBEGIN\nSELECT 1 into var;\nend\n");
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
    public void testParameterNoRegister() throws SQLException {
        createProcedure("testProc1", "(out a int) BEGIN SELECT 1; END");
        CallableStatement cs = sharedConnection.prepareCall("call testProc1(1)");
        try {
            cs.execute();
            fail();
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.assertEquals(e.getMessage(),
                "Parameter `a` is not registered as an output parameter.");
        }

        createProcedure("testProc2", "(out a int) BEGIN SELECT 1 into a; END");
        CallableStatement cs2 = sharedConnection.prepareCall("call testProc2(1)");
        try {
            cs2.execute();
            fail();
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.assertEquals(e.getMessage(),
                "Parameter `a` is not registered as an output parameter.");
        }
    }

    @Test
    public void testParameterNoRegister2() throws SQLException {
        createProcedure("testProc1", "(out a int) BEGIN SELECT 1; END");
        CallableStatement cs = sharedConnection.prepareCall("call testProc1(?)");

        cs.execute();
        Assert.assertEquals(0, cs.getInt(1));

        cs.registerOutParameter(1, Types.INTEGER);
        cs.execute();
        Assert.assertEquals(0, cs.getInt(1));

        createProcedure("testProc2", "(out a int) BEGIN SELECT 1 into a; END");
        CallableStatement cs2 = sharedConnection.prepareCall("call testProc2(?)");

        cs2.execute();
        Assert.assertEquals(1, cs2.getInt(1));

        cs2.registerOutParameter(1, Types.INTEGER);
        cs2.execute();
        Assert.assertEquals(1, cs2.getInt(1));
    }

    @Test
    public void testProcedureDefaultValue() throws Exception {
        createProcedure(
            "testPro",
            "(c1 int, out c2 int, c3 int, out c4 int, out c5 int, c6 int, c7 int) BEGIN SELECT 2 into c2; select 4 into c4; select 5 into c5; END");

        CallableStatement cstmt = sharedConnection
            .prepareCall("{CALL testPro(1, ?, 3, ?, ?, 6, 7)}");
        cstmt.registerOutParameter(2, Types.INTEGER);
        cstmt.registerOutParameter(3, Types.INTEGER);
        cstmt.execute();
        Assert.assertEquals(2, cstmt.getInt(1));
        Assert.assertEquals(4, cstmt.getInt(2));
        Assert.assertEquals(5, cstmt.getInt(3));

        cstmt.clearParameters();
        cstmt.registerOutParameter(1, Types.INTEGER);
        cstmt.registerOutParameter(2, Types.INTEGER);
        cstmt.registerOutParameter(3, Types.INTEGER);
        cstmt.execute();
        Assert.assertEquals(2, cstmt.getInt(1));
        Assert.assertEquals(4, cstmt.getInt(2));
        Assert.assertEquals(5, cstmt.getInt(3));
    }

    @Test
    public void test() throws SQLException {
        Connection conn = setConnection("&sendConnectionAttributes=false");
        createProcedure("calcutotal", "( in q int(11), in p decimal(10,2), out t decimal(10,2)) begin set t = q * p; end;");
        CallableStatement cs = conn.prepareCall("call calcutotal(?, ?, ?)");
        cs.setInt(1, 2);
        cs.setBigDecimal(2, new BigDecimal(5));
        cs.registerOutParameter(3, Types.DECIMAL);
        cs.execute();
        BigDecimal bigDecimal = cs.getBigDecimal(3);
        Assert.assertEquals("10.00", bigDecimal.toString());
    }

    @Test
    public void testAone54427985() throws SQLException {
        Connection conn = setConnection("&useServerPrepStmts=true&useOraclePrepareExecute=false");
        createTable("t0", "c1 varchar(4000), c2 varchar(4000)");
        createProcedure("addRow",
            "(inout p varchar(4000), in q varchar(4000)) begin insert into t0(c1, c2) values(p, q); end;");
        Statement statement = conn.createStatement();

        int len = 2000;
        byte[] b = new byte[len];
        StringBuffer s = new StringBuffer();
        for (int i = 0; i < len; i++) {
            s.append("a");
            b[i] = 'b';
        }

        CallableStatement prepareCall = conn.prepareCall("call addRow(?, ?)");
        prepareCall.setString(1, s.toString());
        prepareCall.setBytes(2, b);
        prepareCall.execute();
        prepareCall.close();
        ResultSet rs = statement.executeQuery("select * from t0");
        rs.next();
        assertEquals(2000, rs.getString(1).length());
        assertEquals(2000, rs.getString(2).length());
        conn.close();
    }

    @Test
    public void testJsonTypeForAone50689660() throws Exception {
        Connection conn = sharedConnection;
        createTable("testJsonType", "id INT PRIMARY KEY, jsonDoc JSON");
        createProcedure("testJsonTypeProc",
            "(OUT jsonDoc JSON) SELECT t.jsonDoc INTO jsonDoc FROM testJsonType t");
        CallableStatement testCstmt = conn.prepareCall("{CALL testJsonTypeProc(?)}");
        ParameterMetaData metaData = testCstmt.getParameterMetaData();
        assertEquals(1, metaData.getParameterCount());
        assertEquals("java.lang.String", metaData.getParameterClassName(1));
        assertEquals("JSON", metaData.getParameterTypeName(1));
    }

    @Test
    public void testFunctionParameterMetaData() throws Exception {
        Connection conn = sharedConnection;
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("DROP FUNCTION IF EXISTS testPro");
        stmt.executeUpdate("CREATE FUNCTION testPro(a float, b bigint, c int) RETURNS INT NO SQL\nBEGIN\nRETURN a;\nEND");
        CallableStatement cStmt = conn.prepareCall("{? = CALL testPro(?,?,?)}");
        Assert.assertEquals(4, cStmt.getParameterMetaData().getParameterCount());

        cStmt = conn.prepareCall("{? = CALL testPro(4,5,?)}");
        Assert.assertEquals(2, cStmt.getParameterMetaData().getParameterCount());

    }

    @Test
    public void testSetStringByParameterName() throws SQLException {
        Connection conn = sharedConnection;
        Statement stmt = conn.createStatement();
        stmt.execute("DROP PROCEDURE IF EXISTS pl_sql;");
        stmt.execute("CREATE PROCEDURE pl_sql (c1 varchar(20), c2 varchar(20)) begin select 1; end;");
        CallableStatement cs = conn.prepareCall("call pl_sql('aaa', ?)");

        cs.setString("c2", "bbb");
        cs.execute();

        cs = conn.prepareCall("call pl_sql('a'aa', ?)");
        try {
            cs.setString("c2", "bbb");
        } catch (SQLException e) {
            assertEquals("there is no parameter with the name c2", e.getMessage());
        }
    }

    @Test
    public void testProcedurForAone55269141() throws Exception {
        createProcedure("t_p", "(OUT param_1 int, OUT param_2 int UNSIGNED)"
                               + "BEGIN SELECT 1,2 INTO param_1, param_2 FROM dual; END");
        CallableStatement storedProc = sharedConnection.prepareCall("{call t_p(?, ?)}");
        storedProc.registerOutParameter(1, Types.INTEGER);
        storedProc.registerOutParameter(2, Types.INTEGER);
        storedProc.execute();
        assertEquals(1, storedProc.getInt(1));
        assertEquals(2, storedProc.getInt(2));
    }

    @Test
    public void testforAone54894992() throws SQLException {
        Connection conn = sharedConnection;
        createProcedure("pl_sql",
            "(c1 int, out c2 int, out c3 int, inout c4 int) begin set c2=c1; set c3 = c2 + c1; end;");
        CallableStatement cs = conn.prepareCall("call pl_sql(5, ?, ?, ?)");
        cs.registerOutParameter(1, Types.VARCHAR);
        cs.registerOutParameter(2, Types.VARCHAR);
        cs.registerOutParameter(3, Types.VARCHAR);
        cs.setInt(3, 15);
        cs.execute();
        assertEquals(5, cs.getInt(1));
        assertEquals(10, cs.getInt(2));
        assertEquals(15, cs.getInt(3));
    }

    @Test
    public void testGetParameterCountBug() {
        try {
            createProcedure("testP1", "(a int,  b int, c int) BEGIN SELECT a, b, c; END");
            sharedConnection.prepareCall("call testP1(?, ?)");

            createProcedure("testP2", "(a int,  b int, out c int) BEGIN SELECT a, b, c; END");
            sharedConnection.prepareCall("call testP2(?, ?)");

        } catch (SQLException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testCallSpecialParameters() throws Exception {
        createProcedure("t_p1", "(out a int) begin set a:= 1; end");
        CallableStatement cs = sharedConnection.prepareCall("call t_p1(a=>?)");
        cs.registerOutParameter(1, Types.INTEGER);
        cs.execute();
        assertEquals(1, cs.getInt(1));

        createProcedure("t_p2", "(out a int, b int) begin select b into a from dual; end");
        cs = sharedConnection.prepareCall("call t_p2(a=>?, b=>10)");
        cs.registerOutParameter(1, Types.INTEGER);
        cs.execute();
        assertEquals(10, cs.getInt(1));

        createProcedure("t_p3", "(out a int, out b int) begin set a:=5; set b:=10; end");
        cs = sharedConnection.prepareCall("call t_p3(a=>?, b=>?)");
        cs.registerOutParameter(1, Types.INTEGER);
        cs.registerOutParameter(2, Types.INTEGER);
        cs.execute();
        assertEquals(5, cs.getInt(1));
        assertEquals(10, cs.getInt(2));

        cs = sharedConnection.prepareCall("call t_p3(b=>?, a=>?)");
        cs.registerOutParameter(1, Types.INTEGER);
        cs.registerOutParameter(2, Types.INTEGER);
        cs.execute();
        assertEquals(10, cs.getInt(1));
        assertEquals(5, cs.getInt(2));

        cs = sharedConnection.prepareCall("call t_p3(b=>?, a=>?)");
        cs.registerOutParameter("b", Types.INTEGER);
        cs.registerOutParameter("a", Types.INTEGER);
        cs.execute();
        assertEquals(10, cs.getInt(1));
        assertEquals(5, cs.getInt(2));
    }


    @Test
    public void testForAone55798407() throws SQLException {
        Connection conn = sharedConnection;
        createProcedure("testP", "(c1 int, c2 int, c3 double(5,2)) begin select 1; end");
        CallableStatement cs =  conn.prepareCall("{call testP(?, 1, 2.6)}");
        cs.setInt(1, 1);
        cs.execute();
        assertEquals(1, cs.getParameterMetaData().getParameterCount());

        cs =  conn.prepareCall("{call testP(1, ?, 2.6)}");
        cs.setInt(1, 1);
        cs.execute();
        assertEquals(1, cs.getParameterMetaData().getParameterCount());

        conn.prepareCall("{call testP(1, 1, ?)}");
        cs.setDouble(1, 2.6d);
        cs.execute();
        assertEquals(1, cs.getParameterMetaData().getParameterCount());

        createFunction("testFunc", "(c1 int, c2 int, c3 double(5,2))\n returns varchar(20) begin return 'a'; end");
        CallableStatement cs1 =  conn.prepareCall("{? = call testFunc(?, 1, 2.6)}");
        cs1.registerOutParameter(1, Types.VARCHAR);
        cs1.setInt(2, 1);
        cs1.execute();
        assertEquals(2, cs1.getParameterMetaData().getParameterCount());

        cs1 =  conn.prepareCall("{? = call testFunc(1, ?, 2.6)}");
        cs1.registerOutParameter(1, Types.VARCHAR);
        cs1.setInt(2, 1);
        cs1.execute();
        assertEquals(2, cs1.getParameterMetaData().getParameterCount());

        cs1 =  conn.prepareCall("{? = call testFunc(1, 1, ?)}");
        cs1.registerOutParameter(1, Types.VARCHAR);
        cs1.setDouble(2, 2.6d);
        cs1.execute();
        assertEquals(2, cs1.getParameterMetaData().getParameterCount());

        CallableStatement cs2 = conn.prepareCall("call testP(?, 1, 2.6)");
        cs2.setInt(1,1);
        cs2.execute();
        assertEquals(1, cs2.getParameterMetaData().getParameterCount());

        cs2 = conn.prepareCall("call testP(1, ?, 2.6)");
        cs2.setInt(1,1);
        cs2.execute();
        assertEquals(1, cs2.getParameterMetaData().getParameterCount());

        cs2 = conn.prepareCall("call testP(1, 1, ?)");
        cs2.setDouble(1,2.6d);
        cs2.execute();
        assertEquals(1, cs2.getParameterMetaData().getParameterCount());
    }

    @Test
    public void testProcedureParameterForDima2024062200102855386() {
        // test for procedure
        try {
            createProcedure("testP2", "(IN a INT, INOUT b VARCHAR(100)) BEGIN SELECT a, b; END");
            CallableStatement cs = sharedConnection.prepareCall("call testP2(?,?)");
            cs.setInt(1, 1);
            cs.setString(2, "aaa");
            cs.registerOutParameter(1, Types.INTEGER);
            fail();
            cs.registerOutParameter(2, Types.VARCHAR);
            cs.execute();
            assertEquals("aaa", cs.getString(2));
        } catch (SQLException e) {

        }

        // test for function
        try {
            createFunction("testF", "(a int, b int) returns int return a + b");
            CallableStatement cs = sharedConnection.prepareCall("{? = call testF(?, ?)}");

            cs.registerOutParameter(1, Types.INTEGER);
            cs.registerOutParameter(2, Types.INTEGER);
            fail();
            cs.registerOutParameter(3, Types.INTEGER, "");
            cs.setInt(2, 1);
            cs.setInt(3, 3);
            cs.execute();
            assertEquals(4, cs.getInt(1));
        } catch (SQLException e) {
            //
        }
    }

    @Test
    public void testForDima2024101200104677510() {
        try {
            createProcedure("pro2", "(out a int) BEGIN select 1 into a; end");
            CallableStatement cs = sharedConnection.prepareCall("{call pro2(?)}");
            cs.registerOutParameter(2, Types.INTEGER);
            fail();
            cs.execute();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof SQLException);
            Assert.assertTrue(e.getMessage().contains("No parameter with index"));
        }

        try {
            createFunction("testF", "(a int, b int) returns int return a + b");
            CallableStatement cs = sharedConnection.prepareCall("{? = call testF(?, ?)}");
            cs.registerOutParameter(4, Types.INTEGER);
            fail();
            cs.execute();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof SQLException);
            Assert.assertTrue(e.getMessage().contains("No parameter with index"));
        }
    }

    @Test
    public void testforDima2024101700104726448() throws SQLException {
        createFunction("t_out", "(a int) returns int begin return a; end;");
        CallableStatement cs = sharedConnection.prepareCall("? = call t_out(?)");
        cs.registerOutParameter(1, Types.INTEGER);
        cs.setInt(2, 1);
        cs.execute();
        assertEquals(1, cs.getInt(1));
    }

}
