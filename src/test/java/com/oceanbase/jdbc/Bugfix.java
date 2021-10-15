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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class Bugfix extends BaseTest {
    static String testProceSetdefaultVal = "testProceSetdefaultVal" + getRandomString(5);
    static String testFunction           = "testfunction" + getRandomString(5);
    static String testSwap               = "testSwap" + getRandomString(5);

    @BeforeClass()
    public static void initClass() throws SQLException {
        createFunction(testFunction, "(c1 varchar(200),c2 int, c3  int,c4  int)   returns int  \n"
                                     + "begin \n" + "   return c2 + c3 + c4 ;\n" + "end ");

        createProcedure(testProceSetdefaultVal,
            "(inout c1 varchar(200),c2 int,out  c3  int,c4  int)    \n" + "begin \n"
                    + "  set c3 = 100  ;\n set c1 = c1;\n" + "end ");

        createProcedure(
            testSwap,
            "(inout c1 int,inout c2 int)    \n"
                    + "begin \n"
                    + "DECLARE tmp  INT DEFAULT 0;\n set tmp = c1 ;\n set c1 := c2 ;\nset c2 := tmp  ;\n"
                    + "end ");
    }

    @Test
    public void tesMysqlModeFunction() {
        try {
            Connection conn = sharedConnection;
            CallableStatement callableStatement = conn.prepareCall("? = call " + testFunction
                                                                   + "('fsfsfs',100,?,?)");
            callableStatement.registerOutParameter(1, Types.NUMERIC);
            callableStatement.setInt(2, 20);
            callableStatement.setInt(3, 20);
            callableStatement.execute();
            Assert.assertEquals(140, callableStatement.getInt(1));
            callableStatement = conn.prepareCall("? = call " + testFunction + "('fsfsfs',?,10,10)");
            callableStatement.registerOutParameter(1, Types.NUMERIC);
            callableStatement.setInt(2, 20);
            callableStatement.execute();
            Assert.assertEquals(40, callableStatement.getInt(1));

            callableStatement = conn.prepareCall("? = call " + testFunction + "(?,?,10,?)");
            callableStatement.registerOutParameter(1, Types.NUMERIC);
            callableStatement.setString(2, "sssss");
            callableStatement.setInt(3, 20);
            callableStatement.setInt(4, 20);
            callableStatement.execute();
            Assert.assertEquals(50, callableStatement.getInt(1));
            callableStatement = conn
                .prepareCall("? = call " + testFunction + "('fsfsfs',10,10,10)");
            callableStatement.registerOutParameter(1, Types.NUMERIC);
            callableStatement.execute();
            Assert.assertEquals(30, callableStatement.getInt(1));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void tesMysqlModeProc() {
        try {
            Connection conn = sharedConnection;
            CallableStatement callableStatement = conn.prepareCall(" call "
                                                                   + testProceSetdefaultVal
                                                                   + "(?,100,?,10)");
            callableStatement.registerOutParameter(1, Types.VARCHAR);
            callableStatement.registerOutParameter(2, Types.INTEGER);
            callableStatement.setString(1, "c1value");
            callableStatement.setInt(2, 20);
            callableStatement.execute();
            Assert.assertEquals("c1value", callableStatement.getString(1));
            Assert.assertEquals(100, callableStatement.getInt(2));
            callableStatement = conn.prepareCall(" call " + testProceSetdefaultVal + "(?,?,?,10)");
            callableStatement.registerOutParameter(1, Types.VARCHAR);
            callableStatement.registerOutParameter(3, Types.INTEGER);
            callableStatement.setString(1, "c1value2");
            callableStatement.setInt("c2", 20);
            callableStatement.setInt(3, 20);
            callableStatement.execute();
            Assert.assertEquals("c1value2", callableStatement.getString(1));
            Assert.assertEquals(100, callableStatement.getInt(3));

        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void tesMysqlModeProc2() {
        try {
            Connection conn = sharedConnection;
            CallableStatement callableStatement = conn.prepareCall(" call " + testSwap + "(?,?)");
            callableStatement.registerOutParameter(1, Types.INTEGER);
            callableStatement.registerOutParameter(2, Types.INTEGER);
            callableStatement.setInt(1, 10);
            callableStatement.setInt(2, 20);
            callableStatement.execute();
            Assert.assertEquals(20, callableStatement.getInt(1));
            Assert.assertEquals(10, callableStatement.getInt(2));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }
}
