package com.oceanbase.jdbc;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.sql.*;

import org.junit.Test;

public class ParameterMetaDataTest extends BaseTest {

    @Test
    public void testPLParameterMetaData() throws SQLException {
        Connection conn = sharedConnection;
        Statement stmt = conn.createStatement();
        stmt.execute("drop procedure IF EXISTS testPro");
        stmt.execute("create procedure testPro(out c1 int, in c2 varchar(20), inout c3 decimal(5,3))\nbegin\n"
                     + " select 1 into c1; select 3 into c3; end\n");

        CallableStatement cs = conn.prepareCall("{call testPro(?,?,?)}");
        ParameterMetaData paramMeta = cs.getParameterMetaData();
        assertEquals(3, paramMeta.getParameterCount());

        // 1st parameter
        assertEquals(Types.INTEGER, paramMeta.getParameterType(1));
        assertEquals("INT", paramMeta.getParameterTypeName(1));
        assertEquals(paramMeta.parameterModeOut, paramMeta.getParameterMode(1));
        assertEquals("java.lang.Integer", paramMeta.getParameterClassName(1));
        assertEquals(11, paramMeta.getPrecision(1));//10
        assertEquals(0, paramMeta.getScale(1));
        assertEquals(true, paramMeta.isSigned(1));//false
        assertEquals(paramMeta.parameterNullable, paramMeta.isNullable(1));

        // 2nd parameter
        assertEquals(Types.VARCHAR, paramMeta.getParameterType(2));
        assertEquals("VARCHAR", paramMeta.getParameterTypeName(2));
        assertEquals(paramMeta.parameterModeIn, paramMeta.getParameterMode(2));
        assertEquals("java.lang.String", paramMeta.getParameterClassName(2));
        assertEquals(20, paramMeta.getPrecision(2));
        assertEquals(0, paramMeta.getScale(2));
        assertEquals(true, paramMeta.isSigned(2));//false
        assertEquals(paramMeta.parameterNullable, paramMeta.isNullable(2));

        // 3rd parameter
        assertEquals(Types.DECIMAL, paramMeta.getParameterType(3));
        assertEquals("DECIMAL", paramMeta.getParameterTypeName(3));
        assertEquals(paramMeta.parameterModeInOut, paramMeta.getParameterMode(3));
        assertEquals(BigDecimal.class.getName(), paramMeta.getParameterClassName(3));
        assertEquals(5, paramMeta.getPrecision(3));
        assertEquals(3, paramMeta.getScale(3));
        assertEquals(true, paramMeta.isSigned(3));//false
        assertEquals(paramMeta.parameterNullable, paramMeta.isNullable(3));
    }

}
