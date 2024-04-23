/*
 *
 * OceanBase Client for Java
 *
 * Copyright (c) 2023 OceanBase.
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

import org.junit.Ignore;
import org.junit.Test;

public class ParameterMetaDataOracleTest extends BaseOracleTest {

    @Ignore
    public void testProcedureParameterMetaData() throws SQLException {
        Connection conn = sharedConnection;
        Statement stmt = conn.createStatement();
        //        stmt.execute("drop procedure IF EXISTS testPro");
        stmt.execute("create or replace procedure testPro(c1 out int, c2 varchar, c3 in out number) is\nbegin\n"
                     + " select 1 into c1 from dual; select 3 into c3 from dual; end\n");// number(x,y) is not allowed

        CallableStatement cs = conn.prepareCall("{call testPro(?,?,?)}");// mysql
        ParameterMetaData paramMeta = cs.getParameterMetaData();// maria
        assertEquals(3, paramMeta.getParameterCount());

        // 1st parameter
        assertEquals(Types.INTEGER, paramMeta.getParameterType(1));
        assertEquals("INT", paramMeta.getParameterTypeName(1));
        assertEquals(paramMeta.parameterModeOut, paramMeta.getParameterMode(1));
        assertEquals("java.lang.Integer", paramMeta.getParameterClassName(1));// maira: int
        assertEquals(11, paramMeta.getPrecision(1));// mysql & maria: 10
        assertEquals(0, paramMeta.getScale(1));
        assertEquals(true, paramMeta.isSigned(1));// mysql: false
        assertEquals(paramMeta.parameterNullable, paramMeta.isNullable(1));// maria: parameterNullableUnknown

        // 2nd parameter
        assertEquals(Types.VARCHAR, paramMeta.getParameterType(2));
        assertEquals("VARCHAR", paramMeta.getParameterTypeName(2));
        assertEquals(paramMeta.parameterModeIn, paramMeta.getParameterMode(2));
        assertEquals("java.lang.String", paramMeta.getParameterClassName(2));
        assertEquals(20, paramMeta.getPrecision(2));
        assertEquals(0, paramMeta.getScale(2));
        assertEquals(true, paramMeta.isSigned(2));// mysql: false
        assertEquals(paramMeta.parameterNullable, paramMeta.isNullable(2));// maria: parameterNullableUnknown

        // 3rd parameter
        assertEquals(Types.DECIMAL, paramMeta.getParameterType(3));
        assertEquals("DECIMAL", paramMeta.getParameterTypeName(3));
        assertEquals(paramMeta.parameterModeInOut, paramMeta.getParameterMode(3));
        assertEquals(BigDecimal.class.getName(), paramMeta.getParameterClassName(3));
        assertEquals(5, paramMeta.getPrecision(3));
        assertEquals(3, paramMeta.getScale(3));
        assertEquals(true, paramMeta.isSigned(3));// mysql: false
        assertEquals(paramMeta.parameterNullable, paramMeta.isNullable(3));// maria: parameterNullableUnknown
    }

    @Ignore
    public void testFunctionParameterMetaData() throws SQLException {
        Connection conn = sharedConnection;
        Statement stmt = conn.createStatement();
        stmt.execute("drop function IF EXISTS testFunc");
        stmt.execute("create function testFunc( c1 int, `c2` varchar(20),  c3 decimal(5,3))\n returns char(20)\nbegin\n"
                     + " select 1 into c1; select 3 into c3; return 'hello'; end\n");

        CallableStatement cs = conn.prepareCall("{? = call testFunc(?,?,?)}");
        ParameterMetaData paramMeta = cs.getParameterMetaData();
        assertEquals(4, paramMeta.getParameterCount());

        assertEquals(Types.CHAR, paramMeta.getParameterType(1));
        assertEquals("CHAR", paramMeta.getParameterTypeName(1));
        assertEquals(paramMeta.parameterModeUnknown, paramMeta.getParameterMode(1));// mysql: 5
        assertEquals("java.lang.String", paramMeta.getParameterClassName(1));
        assertEquals(20, paramMeta.getPrecision(1));
        assertEquals(0, paramMeta.getScale(1));
        assertEquals(true, paramMeta.isSigned(1));// mysql: false
        assertEquals(paramMeta.parameterNullable, paramMeta.isNullable(1));// maria: parameterNullableUnknown

        // 1st parameter
        assertEquals(Types.INTEGER, paramMeta.getParameterType(2));
        assertEquals("INT", paramMeta.getParameterTypeName(2));
        assertEquals(paramMeta.parameterModeIn, paramMeta.getParameterMode(2));// maria: parameterModeOut
        assertEquals("java.lang.Integer", paramMeta.getParameterClassName(2));// maira: int
        assertEquals(11, paramMeta.getPrecision(2));// mysql & maria: 10
        assertEquals(0, paramMeta.getScale(2));
        assertEquals(true, paramMeta.isSigned(2));// mysql: false
        assertEquals(paramMeta.parameterNullable, paramMeta.isNullable(2));// maria: parameterNullableUnknown

        // 2nd parameter
        assertEquals(Types.VARCHAR, paramMeta.getParameterType(3));
        assertEquals("VARCHAR", paramMeta.getParameterTypeName(3));
        assertEquals(paramMeta.parameterModeIn, paramMeta.getParameterMode(3));// maria: parameterModeOut
        assertEquals("java.lang.String", paramMeta.getParameterClassName(3));
        assertEquals(20, paramMeta.getPrecision(3));
        assertEquals(0, paramMeta.getScale(3));
        assertEquals(true, paramMeta.isSigned(3));// mysql: false
        assertEquals(paramMeta.parameterNullable, paramMeta.isNullable(3));// maria: parameterNullableUnknown

        // 3rd parameter
        assertEquals(Types.DECIMAL, paramMeta.getParameterType(4));
        assertEquals("DECIMAL", paramMeta.getParameterTypeName(4));
        assertEquals(paramMeta.parameterModeIn, paramMeta.getParameterMode(4));// maria: parameterModeOut
        assertEquals(BigDecimal.class.getName(), paramMeta.getParameterClassName(4));
        assertEquals(5, paramMeta.getPrecision(4));
        assertEquals(3, paramMeta.getScale(4));
        assertEquals(true, paramMeta.isSigned(4));// mysql: false
        assertEquals(paramMeta.parameterNullable, paramMeta.isNullable(4));// maria: parameterNullableUnknown
    }

    @Test
    public void testGenerateSimpleParameterMetadata() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        try {
            stmt.execute("drop table test_SimpleParameterMetaData");
        } catch (SQLException e) {
        }
        stmt.execute("create table test_SimpleParameterMetaData (v0 int , v1 varchar(32) , v2 date)");
        Connection conn = setConnection("&useServerPrepStmts=false");
        PreparedStatement pstmt = conn.prepareStatement("insert into test_SimpleParameterMetaData (v0 , v1 , v2) values (? , ? , ?)");
        ParameterMetaData parameterMetaData = pstmt.getParameterMetaData();

        try {
            parameterMetaData.getParameterType(1);
            fail();
        } catch (SQLException e) {
            assertEquals("Getting parameter type metadata are not supported", e.getMessage());
        }
    }

}
