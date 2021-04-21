/*
 *
 * OceanBase Client for Java
 *
 * Copyright (c) 2021 OceanBase Technology Co.,Ltd.
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

import org.junit.Test;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.Types;

import static org.junit.Assert.assertEquals;

public class StoredProcedureOracleBasicTest extends BaseOracleTest {
    @Test
    public void testAdd() throws Exception {
        String procedureName = "calc_add_" + System.currentTimeMillis();
        createProcedure(
                procedureName,
                "(a1 IN int, a2 IN int, a3 OUT int) is "
                        + "begin " + "  a3:=a1+a2; " + "end;"
        );
        try (CallableStatement callableStatement = sharedConnection.prepareCall("call " + procedureName + "(?, ?, ?)")) {
            callableStatement.setInt(1, 1);
            callableStatement.setInt(2, 2);
            callableStatement.registerOutParameter(3, Types.INTEGER);
            callableStatement.execute();
            //System.out.println(callableStatement.getInt(2));
            ResultSet rs = callableStatement.getResultSet();
            System.out.println("rs is:" + callableStatement.getDouble(3));
            if (rs != null && rs.next()) {
                System.out.println("THE RESULT IS:" + rs.getInt(3));
            }
            assertEquals(3 == callableStatement.getDouble(3), true);

        } finally {
            dropProcedure(procedureName);
        }
    }

    @Test
    public void testLongStoreProcedureWithFullName() throws Exception {
        //for issue https://aone.alibaba-inc.com/v2/project/906355/req/32951377
        String procedureName = "calc_add_" + System.currentTimeMillis();

        createProcedureWithPackage(
                "test", procedureName,
                "(a1 IN int, a2 IN int, a3 OUT int); end;",
                "(a1 IN int, a2 IN int, a3 OUT int) is "
                        + "begin " + "  a3:=a1+a2; " + "end;end;"
        );
        try (CallableStatement callableStatement = sharedConnection.prepareCall("call test." + procedureName + "(?, ?, ?)")) {
            callableStatement.setInt(1, 1);
            callableStatement.setInt(2, 2);
            callableStatement.registerOutParameter(3, Types.INTEGER);
            callableStatement.execute();
            //System.out.println(callableStatement.getInt(2));
            ResultSet rs = callableStatement.getResultSet();
            System.out.println("rs is:" + callableStatement.getDouble(3));
            if (rs != null && rs.next()) {
                System.out.println("THE RESULT IS:" + rs.getInt(3));
            }
            assertEquals(3 == callableStatement.getDouble(3), true);

        } finally {
            dropProcedure("test." + procedureName);
        }
    }
}