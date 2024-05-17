package com.oceanbase.jdbc;

import java.math.BigDecimal;
import java.sql.*;

import org.junit.Test;

import static org.junit.Assert.*;

public class ParameterMetaDataTest extends BaseTest {

    @Test
    public void testProcedureParameterMetaData() throws SQLException {
        Connection conn = sharedConnection;
        Statement stmt = conn.createStatement();
        stmt.execute("drop procedure IF EXISTS testPro");
        stmt.execute("create procedure testPro(out c1 int, `c2` varchar(20), inout c3 decimal(5,3))\nbegin\n"
                     + " select 1 into c1; select 3 into c3; end\n");

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

    @Test
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
        Connection conn = setConnection("&useServerPrepStmts=true&generateSimpleParameterMetadata=true");
        PreparedStatement pstmt = conn.prepareStatement("insert into test_SimpleParameterMetaData (v0 , v1 , v2) values (? , ? , ?)");
        ParameterMetaData parameterMetaData = pstmt.getParameterMetaData();
        assertEquals(3, parameterMetaData.getParameterCount());

        //useServerPrepStmts=true时，由于server返回值与MySQL不一致，导致部分接口的返回值与MySQL-JDBC也不一致，但此处jdbc行为一致
        assertEquals(8, parameterMetaData.getParameterType(1));
        assertEquals("BIGINT", parameterMetaData.getParameterTypeName(1));
        assertEquals("java.lang.Long", parameterMetaData.getParameterClassName(1));
        assertEquals(0, parameterMetaData.getPrecision(1));
        assertEquals(0, parameterMetaData.getScale(1));

        assertEquals(8, parameterMetaData.getParameterType(1));
        assertEquals("BIGINT", parameterMetaData.getParameterTypeName(2));
        assertEquals("java.lang.Long", parameterMetaData.getParameterClassName(2));
        assertEquals(0, parameterMetaData.getPrecision(2));
        assertEquals(0, parameterMetaData.getScale(2));

        assertEquals(8, parameterMetaData.getParameterType(1));
        assertEquals("BIGINT", parameterMetaData.getParameterTypeName(3));
        assertEquals("java.lang.Long", parameterMetaData.getParameterClassName(3));
        assertEquals(0, parameterMetaData.getPrecision(3));
        assertEquals(0, parameterMetaData.getScale(3));

        conn = setConnection("&useServerPrepStmts=false&generateSimpleParameterMetadata=true");
        pstmt = conn.prepareStatement("insert into test_SimpleParameterMetaData (v0 , v1 , v2) values (? , ? , ?)");
        parameterMetaData = pstmt.getParameterMetaData();
        assertEquals(3, parameterMetaData.getParameterCount());
        assertFalse(parameterMetaData.isSigned(1));
        assertEquals(12, parameterMetaData.getParameterType(1));
        assertEquals("VARCHAR", parameterMetaData.getParameterTypeName(1));
        assertEquals("java.lang.String", parameterMetaData.getParameterClassName(1));
        assertEquals(0, parameterMetaData.getPrecision(1));
        assertEquals(0, parameterMetaData.getScale(1));

        assertFalse(parameterMetaData.isSigned(2));
        assertEquals(12, parameterMetaData.getParameterType(1));
        assertEquals("VARCHAR", parameterMetaData.getParameterTypeName(2));
        assertEquals("java.lang.String", parameterMetaData.getParameterClassName(2));
        assertEquals(0, parameterMetaData.getPrecision(2));
        assertEquals(0, parameterMetaData.getScale(2));

        assertFalse(parameterMetaData.isSigned(3));
        assertEquals(12, parameterMetaData.getParameterType(1));
        assertEquals("VARCHAR", parameterMetaData.getParameterTypeName(3));
        assertEquals("java.lang.String", parameterMetaData.getParameterClassName(3));
        assertEquals(0, parameterMetaData.getPrecision(3));
        assertEquals(0, parameterMetaData.getScale(3));
    }

}
