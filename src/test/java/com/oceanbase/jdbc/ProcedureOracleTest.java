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
 */package com.oceanbase.jdbc;

import static org.junit.Assert.fail;

import java.sql.CallableStatement;
import java.sql.SQLException;
import java.sql.Types;

import org.junit.Assert;
import org.junit.Test;

public class ProcedureOracleTest extends BaseOracleTest {

    @Test
    public void testParameterNoRegister() throws SQLException {
        createProcedure("testProc1", "(a out int) is BEGIN SELECT 1 from dual; END");
        CallableStatement cs = sharedConnection.prepareCall("call testProc1(1)");
        try {
            cs.execute();
            fail();
        } catch (SQLException e) {
            e.printStackTrace();
            Assert
                .assertTrue(e
                    .getMessage()
                    .contains(
                        "ORA-00600: internal error code, arguments: -4007, Not supported feature or function"));
            //            Assert.assertEquals(e.getMessage(), "ORA-06575: 程序包或函数 TESTPROC 处于无效状态\n");// oracle sends request
        }

        createProcedure("testProc2", "(a out number) is BEGIN SELECT 1 into a from dual; END;");
        CallableStatement cs2 = sharedConnection.prepareCall("call testProc2(1)");
        //        try {
        cs2.execute();
        try {
            Assert.assertEquals(0, cs2.getInt(1));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.assertEquals(e.getMessage(), "Index 1 must at maximum be 0");
        }
        //fail(); // server has no plan to fix
        //        } catch (SQLException e) {
        //            e.printStackTrace();
        //            Assert.assertEquals(e.getMessage(), "ORA-06577: 输出参数不是绑定变量\n");//oracle sends request
        //        }
    }

    @Test
    public void testParameterNoRegister2() throws SQLException {
        createProcedure("testProc1", "(a out int) is BEGIN SELECT 1 from dual; END");
        CallableStatement cs = sharedConnection.prepareCall("call testProc1(?)");

        try {
            cs.execute();
            fail();
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.assertEquals(e.getMessage(), "Missing IN or OUT parameter in index::1");
            //            Assert.assertEquals(e.getMessage(), "索引中丢失  IN 或 OUT 参数:: 1");// oracle doesn't send request
        }

        cs.registerOutParameter(1, Types.INTEGER);
        try {
            cs.execute();
            fail();
        } catch (SQLException e) {
            e.printStackTrace();
            Assert
                .assertTrue(e
                    .getMessage()
                    .contains(
                        "ORA-00600: internal error code, arguments: -4007, Not supported feature or function"));
            //            Assert.assertEquals(e.getMessage(), "ORA-06575: 程序包或函数 TESTPROC 处于无效状态\n");// oracle sends request
        }

        createProcedure("testProc2", "(a out int) is BEGIN SELECT 1 into a from dual; END");
        CallableStatement cs2 = sharedConnection.prepareCall("call testProc2(?)");

        try {
            cs2.execute();
            fail();
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.assertEquals(e.getMessage(), "Missing IN or OUT parameter in index::1");
            //            Assert.assertEquals(e.getMessage(), "索引中丢失  IN 或 OUT 参数:: 1");// oracle doesn't send request
        }

        cs2.registerOutParameter(1, Types.INTEGER);
        cs2.execute();
        Assert.assertEquals(1, cs2.getInt(1));
    }

}
