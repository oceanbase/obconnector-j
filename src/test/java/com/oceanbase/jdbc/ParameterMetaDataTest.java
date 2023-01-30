package com.oceanbase.jdbc;

import static org.junit.Assert.assertEquals;

import java.sql.*;

import org.junit.Test;

public class ParameterMetaDataTest extends BaseTest {

    @Test
    public void testPLParameterMetaData() throws SQLException {
        Connection conn = sharedConnection;
        Statement stmt = conn.createStatement();
        stmt.execute("drop procedure IF EXISTS testPro");
        stmt.execute("create procedure testPro(out c1 int, in c2 varchar(20))\nbegin\n"
                     + " select 1 into c1; end\n");

        CallableStatement cs = conn.prepareCall("{call testPro(?,?)}");
        ParameterMetaData paramMeta = cs.getParameterMetaData();
        assertEquals(2, paramMeta.getParameterCount());

        assertEquals(Types.INTEGER, paramMeta.getParameterType(1));
        assertEquals("INT", paramMeta.getParameterTypeName(1));
        assertEquals(paramMeta.parameterModeOut, paramMeta.getParameterMode(1));
        assertEquals("java.lang.Integer", paramMeta.getParameterClassName(1));
        assertEquals(11, paramMeta.getPrecision(1));//10
        assertEquals(0, paramMeta.getScale(1));
        assertEquals(false, paramMeta.isSigned(1));
        assertEquals(paramMeta.parameterNullable, paramMeta.isNullable(1));

        assertEquals(Types.VARCHAR, paramMeta.getParameterType(2));
        assertEquals("VARCHAR", paramMeta.getParameterTypeName(2));
        assertEquals(paramMeta.parameterModeIn, paramMeta.getParameterMode(2));
        assertEquals("java.lang.String", paramMeta.getParameterClassName(2));
        assertEquals(20, paramMeta.getPrecision(2));
        assertEquals(0, paramMeta.getScale(2));
        assertEquals(false, paramMeta.isSigned(2));
        assertEquals(paramMeta.parameterNullable, paramMeta.isNullable(2));
    }

}
