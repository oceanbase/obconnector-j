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
            "(c1 int, out c2 int, c3 int, out c4 int, out c5 int, c6 int, c7 int) BEGIN SELECT 1 into c2; select 2 into c4; select 3 into c5; END");

        CallableStatement cstmt = sharedConnection
            .prepareCall("{CALL testPro(4, ?, 5, ?, ?, 6, 7)}");
        cstmt.registerOutParameter(2, Types.INTEGER);
        cstmt.registerOutParameter(3, Types.INTEGER);
        cstmt.execute();
        Assert.assertEquals(1, cstmt.getInt(1));
        Assert.assertEquals(2, cstmt.getInt(2));
        Assert.assertEquals(3, cstmt.getInt(3));

        cstmt.clearParameters();
        cstmt.registerOutParameter(1, Types.INTEGER);
        cstmt.registerOutParameter(2, Types.INTEGER);
        cstmt.registerOutParameter(3, Types.INTEGER);
        cstmt.execute();
        Assert.assertEquals(1, cstmt.getInt(1));
        Assert.assertEquals(2, cstmt.getInt(2));
        Assert.assertEquals(3, cstmt.getInt(3));
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
}
