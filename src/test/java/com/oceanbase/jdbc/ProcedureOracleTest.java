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

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.sql.*;

import com.oceanbase.jdbc.extend.datatype.ComplexDataType;
import org.junit.Assert;
import org.junit.Test;

public class ProcedureOracleTest extends BaseOracleTest {

    @Test
    public void testParameterNoRegister() throws SQLException {
        try {
            createProcedure("testProc1", "(a out int) is BEGIN SELECT 1 from dual; END");
            CallableStatement cs = sharedConnection.prepareCall("call testProc1(1)");
            try {
                cs.execute();
                fail();
            } catch (SQLException e) {
                e.printStackTrace();
//            Assert.assertTrue(e.getMessage().contains("ORA-00600: internal error code, arguments: -4007, Not supported feature or function"));
//            // oracle: 发请求，"ORA-06575: 程序包或函数 TESTPROC 处于无效状态\n"
            }
            try {
                cs.registerOutParameter(1, Types.INTEGER);
                fail();
            } catch (SQLException e) {
                e.printStackTrace();//无效的列索引
//            Assert.assertEquals(e.getMessage(), "No parameter with index 1");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            createProcedure("testProc2", "(a out number) is BEGIN SELECT 1 into a from dual; END;");
            CallableStatement cs2 = sharedConnection.prepareCall("call testProc2(1)");
            try {
                cs2.execute();
//                fail(); // server has no plan to fix
            } catch (SQLException e) {
                e.printStackTrace();
//            Assert.assertEquals(e.getMessage(), "ORA-06577: 输出参数不是绑定变量\n");
            }
            try {
                cs2.registerOutParameter(1, Types.INTEGER);
                fail();
            } catch (SQLException e) {
                e.printStackTrace();//无效的列索引
//            Assert.assertEquals(e.getMessage(), "No parameter with index 1");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
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
//            Assert.assertEquals(e.getMessage(), "Missing IN or OUT parameter in index::1");
//            // oracle: 不发请求，报错"索引中丢失  IN 或 OUT 参数:: 1"
        }
        cs.registerOutParameter(1, Types.INTEGER);
        try {
            cs.execute();
            fail();
        } catch (SQLException e) {
            e.printStackTrace();
//            Assert.assertTrue(e.getMessage().contains("ORA-00600: internal error code, arguments: -4007, Not supported feature or function"));
//            // oracle: 发请求，"ORA-06575: 程序包或函数 TESTPROC 处于无效状态\n"
        }

        createProcedure("testProc2", "(a out int) is BEGIN SELECT 1 into a from dual; END");
        CallableStatement cs2 = sharedConnection.prepareCall("call testProc2(?)");
        try {
            cs2.execute();
            fail();
        } catch (SQLException e) {
            e.printStackTrace();
//            Assert.assertEquals(e.getMessage(), "Missing IN or OUT parameter in index::1");
//            // oracle: 不发请求，报错"索引中丢失  IN 或 OUT 参数:: 1"
        }
        cs2.registerOutParameter(1, Types.INTEGER);
        cs2.execute();
        Assert.assertEquals(1, cs2.getInt(1));
    }

    @Test
    public void testFunctionParameterMetaData() throws SQLException {
        Connection conn = sharedConnection;
        createFunction("testFunc",
            "(a FLOAT, b NUMBER, c in INT) RETURN INT IS d INT;\nBEGIN d:=a+b+c; RETURN d;\nEND;");

        CallableStatement cs = conn.prepareCall("{? = call testFunc(?,?,?)}");
        ParameterMetaData paramMeta = cs.getParameterMetaData();
        int parameterCount = paramMeta.getParameterCount();
        assertEquals(4, parameterCount);

        cs = conn.prepareCall("{? = call testFunc(1, 2,?)}");
        paramMeta = cs.getParameterMetaData();
        parameterCount = paramMeta.getParameterCount();
        assertEquals(2, parameterCount);
    }

    @Test
    public void testAone54877716() throws SQLException {
        createFunction("testFunctionCall",
            "(a FLOAT, b NUMBER, c in out INT) RETURN INT AS d INT;\nBEGIN\nd:=a+b+c;\nc:=10;\nRETURN d;\nEND;");
        Connection conn = setConnectionOrigin("?useServerPrepStmts=true&useOraclePrepareExecute=true&cacheCallableStmts=false");
        CallableStatement cs1 = conn.prepareCall("{? = CALL testFunctionCall (?,?,?)}");

        // 问题1：function registerOutParameter不支持命名绑定
        cs1.registerOutParameter("d", Types.INTEGER);
        cs1.registerOutParameter("c", Types.INTEGER);
        setParamsAndExec(cs1);
        assertEquals(7, cs1.getInt(1));
        assertEquals(7, cs1.getInt("d"));
        assertEquals(10, cs1.getInt(2));
        assertEquals(10, cs1.getInt("c"));
        cs1.close();

        //问题2：d列索引绑定，c列出参注不注册， 248 execute报错， 249 执行通过
        CallableStatement cs2 = conn.prepareCall("{? = CALL testFunctionCall (?,?,?)}");
        cs2.registerOutParameter(1, Types.INTEGER);
        //cs2.registerOutParameter("c", Types.INTEGER);
        try {
            setParamsAndExec(cs2);
            fail();
        } catch (SQLException e) {
            assertEquals(
                "The number of parameter names '3' does not match the number of registered parameters in sql '4'.",
                e.getMessage());
        }
        cs2.close();

        //问题3：d列命名绑定，c列出参未注册，248 get c列报错。249 因问题1无法继续测试
        CallableStatement cs3 = conn.prepareCall("{? = CALL testFunctionCall (?,?,?)}");
        cs3.registerOutParameter("d", Types.INTEGER);
        //cs3.registerOutParameter("c", Types.INTEGER);
        setParamsAndExec(cs3);
        assertEquals(7, cs3.getInt(1));
        assertEquals(7, cs3.getInt("d"));
        try {
            cs3.getInt("c");
            fail();
        } catch (Exception e) {
            assertEquals(
                "Parameter 'c' is not declared as output parameter with method registerOutParameter",
                e.getMessage());
        }
        cs3.close();

        //问题4：全部使用索引绑定，248 get out参数用列名报错，249 执行通过
        CallableStatement cs4 = conn.prepareCall("{? = CALL testFunctionCall (?,?,?)}");
        cs4.registerOutParameter(1, Types.INTEGER);
        cs4.setFloat(2, 4);
        cs4.setInt(3, 2);
        cs4.setInt(4, 1);
        cs4.registerOutParameter(4, Types.INTEGER);
        cs4.execute();
        assertEquals(7, cs4.getInt(1));
        assertEquals(10, cs4.getInt(4));
        assertEquals(10, cs4.getInt("c"));
        cs4.close();
    }

    public void setParamsAndExec(CallableStatement cs) throws SQLException {
        cs.setFloat("a", 4);
        cs.setInt("b", 2);
        cs.setInt("c", 1);
        cs.execute();
    }

    @Test
    public void testByName() throws Exception {
        createProcedure("t_p1", "(a out int) is begin null; end;");
        CallableStatement cs = sharedConnection.prepareCall("call t_p1(a=>?)");
        cs.registerOutParameter(1, Types.INTEGER);
        cs.execute();
        assertEquals(0, cs.getInt(1));

        createProcedure("t_p2",
            "(a out int, b int default 5) is begin select b into a from dual; end;");
        cs = sharedConnection.prepareCall("call t_p2(a=>?, b=>10)");
        cs.registerOutParameter(1, Types.INTEGER);
        cs.execute();
        assertEquals(10, cs.getInt(1));

        createProcedure("t_p3",
            "(a out int, b int default 5) is begin select b into a from dual; end;");
        cs = sharedConnection.prepareCall("call t_p3(a=>?)");
        cs.registerOutParameter(1, Types.INTEGER);
        cs.execute();
        assertEquals(5, cs.getInt(1));

        createProcedure("t_p4",
            "(a out int, b out int) is begin select 5,10 into a,b from dual; end;");
        cs = sharedConnection.prepareCall("call t_p4(a=>?, b=>?)");
        cs.registerOutParameter(1, Types.INTEGER);
        cs.registerOutParameter(2, Types.INTEGER);
        cs.execute();
        assertEquals(5, cs.getInt(1));
        assertEquals(10, cs.getInt(2));

        cs = sharedConnection.prepareCall("call t_p4(b=>?, a=>?)");
        cs.registerOutParameter(1, Types.INTEGER);
        cs.registerOutParameter(2, Types.INTEGER);
        cs.execute();
        assertEquals(10, cs.getInt(1));
        assertEquals(5, cs.getInt(2));
    }

    @Test
    public void testBEGINAndEND() throws SQLException {
        createProcedure("t_p1111",
            "(c1 int, c2 int, abegin int, bend out int) is begin select abegin into bend from dual; end");
        ResultSet rs = sharedConnection.getMetaData().getProcedureColumns(null, null,
            "t_p1111".toUpperCase(), "%");
        assertTrue(rs.next());
        assertEquals("C1", rs.getString("COLUMN_NAME"));
        assertTrue(rs.next());
        assertEquals("C2", rs.getString("COLUMN_NAME"));
        assertTrue(rs.next());
        assertEquals("ABEGIN", rs.getString("COLUMN_NAME"));
        assertTrue(rs.next());
        assertEquals("BEND", rs.getString("COLUMN_NAME"));
    }

    @Test
    public void testFofAone55293893() throws SQLException {
        Connection connection = setConnection();
        Statement statement = connection.createStatement();
        statement.execute("CREATE OR REPLACE type ty_test as object ( c1 varchar(10), c2 char(10) );");

        createProcedure("ty_pl", "(c1 varchar, c2 char, o_ty out SYS_REFCURSOR) as begin open o_ty for SELECT ty_test(c1, c2) from (select c1 as c1, c2 as c2 from dual); end;");

        CallableStatement cstmt = connection.prepareCall("call ty_pl(?, ?, ?)");
        cstmt.setString(1, "hello");
        cstmt.setString(2, "上海");
        cstmt.registerOutParameter(3, Types.REF_CURSOR);
        cstmt.execute();

        ResultSet rs = (ResultSet)cstmt.getObject(3);
        rs.next();
        Struct struct = (Struct) rs.getObject(1);
        Object[] attributes = struct.getAttributes();
        assertEquals(2, attributes.length);
        assertEquals("hello", attributes[0]);
        assertEquals("上海    ", attributes[1]);
    }

    @Test 
    public void testForDima2024080100104013452() throws Exception {
        Connection conn = setConnection("&useServerPrepStmts=true&cacheCallableStmts=false");
        Statement stmt = conn.createStatement();
        stmt.execute("create or replace procedure p_lob(c1 int) is begin null;end;");
        stmt.execute("alter system set open_cursors = 5");
        try {
            for (int i = 1; i <= 6; i++) {
                CallableStatement cs = conn.prepareCall("call p_lob(?)");
                cs.setInt(1, 1);
                cs.execute();
            }
            fail();
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("ORA-01000: maximum open cursors exceeded"));
        }finally {
            stmt.execute("alter system set open_cursors = 50");
        }

        // cacheCallableStmts=true
        conn = setConnection("&useServerPrepStmts=true&cacheCallableStmts=true");
        try {
            stmt.execute("alter system set open_cursors = 5");
            for (int i = 1; i <= 6; i++) {
                CallableStatement cs = conn.prepareCall("call p_lob(?)");
                cs.setInt(1, 1);
                cs.execute();
            }
        } catch (SQLException e) {
            fail();
        }finally {
            stmt.execute("alter system set open_cursors = 50");
        }
    }
    
    @Test
    public void testForDima2024071500103721142() throws Exception {
        Statement stmt = sharedConnection.createStatement();
        try {
            stmt.execute("create or replace procedure p1(c1 out clob) is BEGIN \n" + " -- '//' \n"
                         + " c1 := '{a}'; end;");
        } catch (SQLException e) {
            fail();
        }
    }

    @Test
    public void testForDima2024062200102855194() throws SQLException {
        Connection conn = sharedConnection;
        conn.createStatement().execute(
            "create or replace function t_fun return varchar2 as begin return 'aaa'; end;");
        CallableStatement cs = conn.prepareCall("select t_fun() as jbo from dual");
        cs.execute();
        try {
            cs.getString(1);
            fail();
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("Index 1 must at maximum be 0"));
        }
    }

    @Test
    public void testNestedArrayForDima2024080500104042401() throws Exception {
        Connection conn = sharedConnection;
        Statement stmt = conn.createStatement();

        stmt.execute("create or replace type obj0 force as object(c0 varchar2(10))");
        stmt.execute("create or replace type tb is table of obj0");
        stmt.execute("create or replace procedure p1(a tb) is begin null;end;");
        conn.createStruct("obj0", new Object[] { "aaa" });

        stmt.execute("drop type obj0 force");
        stmt.execute("create or replace type obj0 is varray(2) of int");
        stmt.execute("create or replace procedure pro2(c1 out obj0) is begin c1 :=obj0(2,4); end;");
        CallableStatement cst = conn.prepareCall("call pro2(?)");
        cst.registerOutParameter(1, Types.ARRAY);
        cst.execute();

        Array array = cst.getArray(1);
        Object[] arrobj = (Object[]) array.getArray();
        assertEquals(2, arrobj.length);
    }

    @Test
    public void testNestedArrayForDima2024080500104042171() throws Exception {
        Connection conn = sharedConnection;
        String observer_version = ((OceanBaseConnection) conn).getProtocol().getObServerVersion();
        Statement stmt = conn.createStatement();
        createTable("t_1", "id1 int, id2 int");
        stmt.execute("insert into t_1 values (1, 2), (3, 4), (5, 6)");

        try {
            stmt.execute("drop type obj0 force");
        } catch (SQLException e) {
            //
        }
        stmt.execute("create or replace type obj0 force as object(c0 varchar2(10))");
        stmt.execute("create or replace type tb is table of obj0");
        stmt.execute("create or replace procedure p1(a tb) is begin null;end;");
        conn.createStruct("obj0", new Object[] { "aaa" });

        // modify udt
        try {
            stmt.execute("drop type obj0 force");
        } catch (SQLException e) {
            //
        }
        stmt.execute("create or replace type obj0 as object (c1 int, c2 int)");
        stmt.execute("create or replace type array0 is varray(10) of obj0");

        String str;
        if (observer_version.compareTo("4.2") >= 0) {
            str = "obj0(id1,id2)";
        } else {
            str = "id1,id2";
        }

        String sql = "create or replace procedure test_array (b out array0) is begin " + "select "
                     + str + " bulk collect into b from t_1 ; end";
        stmt.execute(sql);
        CallableStatement cs = conn.prepareCall("{call test_array(?)}");
        cs.registerOutParameter(1, Types.ARRAY);
        cs.execute();
        Array array = cs.getArray(1);
        Object[] arrobj = (Object[]) array.getArray();
        Assert.assertEquals(3, arrobj.length);

        for (int i = 0; i < 3; i++) {
            Struct struct = (Struct) arrobj[i];
            Object[] attributes = struct.getAttributes();
            Assert.assertEquals(2, attributes.length);

            Assert.assertEquals(i * 2 + 1, ((Number) attributes[0]).intValue());
            Assert.assertEquals(i * 2 + 2, ((Number) attributes[1]).intValue());
        }
    }

    @Test
    public void testPLParameterMetaData() throws SQLException {
        Connection conn = setConnection("&useServerPrepStmts=true&useOraclePrepareExecute=true");
        Statement stmt = conn.createStatement();
        CallableStatement cs;
        ParameterMetaData paramMeta;

        stmt.execute("create or replace procedure testPro(c1 out int, c2 varchar, c3 in out int) is\nbegin\n"
                     + " select 1 into c1 from dual; select 3 into c3 from dual; end\n");
        cs = conn.prepareCall("{call testPro(?,?,?)}");
        paramMeta = cs.getParameterMetaData();
        assertEquals(3, paramMeta.getParameterCount());

        stmt.execute("create or replace function testFunc( c1 int, c2 varchar(20),  c3 decimal(5,3))\n return char(20) is begin\n"
                     + " select 1,3 into c1,c3 from dual; return 'hello'; end\n");
        cs = conn.prepareCall("{? = call testFunc(?,?,?)}"); //不开启二合一协议 报错 -5055, FUNCTION TESTFUNC does not exist
        paramMeta = cs.getParameterMetaData();
        assertEquals(4, paramMeta.getParameterCount());

        cs = conn.prepareCall("begin ? := testFunc (1,?,?);end;");
        paramMeta = cs.getParameterMetaData();
        assertEquals(3, paramMeta.getParameterCount());

        cs = conn.prepareCall("begin select 1 from dual where id = ? and name = ?;end;");
        paramMeta = cs.getParameterMetaData();
        assertEquals(2, paramMeta.getParameterCount());

        cs.close();
        stmt.close();
    }

    @Test
    public void testCachePrepareCallForDima2024101200104677104() {
        try {
            createProcedure("pro2", "(var int) is BEGIN null; end");
            Connection conn = setConnectionOrigin("?cacheCallableStmts=true");
            for (int i = 0; i < 2; i++) {
                CallableStatement cs = conn.prepareCall("{call pro2(?)}");
                cs.setInt(1,2);
                cs.execute();
                cs.close();
            }
        } catch (SQLException e) {
            fail();
        }
    }

    @Test
    public void testCacheCallableStmtForDima2024101700104728671() throws Exception {
        Connection conn = setConnection("&useServerPrepStmts=true&useOraclePrepareExecute=true&cacheCallableStmts=true");
        Statement stmt = conn.createStatement();
        createProcedure("p_cur", "(c1 int) is begin null;end;");
        stmt.execute("alter system set open_cursors = 5");
        try {
            CallableStatement cs = conn.prepareCall("call p_cur(?)");
            for (int i = 1; i <= 6; i++) {
                cs = conn.prepareCall("call p_cur(?)");
                cs.setInt(1, 1);
                cs.execute();
                cs.close();
            }
        }catch (Exception e){
            fail();
        }finally {
            stmt.execute("alter system set open_cursors = 50");
        }

        conn = setConnection("&cacheCallableStmts=false");
        stmt.execute("alter system set open_cursors = 5");
        try {
            for (int i = 1; i <= 6; i++) {
                CallableStatement cs1 = conn.prepareCall("call p_cur(?)");
                cs1.setInt(1, 1);
                cs1.execute();
            }
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("maximum open cursors exceeded"));
        } finally {
            stmt.execute("alter system set open_cursors = 50");
        }
    }

    @Test
    public void testForDima2024121000106419694() throws Exception {
        Connection conn = setConnection("&useServerPrepStmts=true");
        Statement statement = conn.createStatement();
        statement.execute("create or replace type obj0 force as object(c0 int, c1 int);");
        statement.execute("create or replace procedure p1(a obj0, b out obj0)is begin b:=a;end;");

        CallableStatement call01 = conn.prepareCall("call p1(obj0(1,5),?)");
        call01.registerOutParameter(1, Types.STRUCT,"OBJ0");
        call01.execute();
        Struct struct01 = (Struct) call01.getObject(1);
        Object[] attributes = struct01.getAttributes();
        assertEquals(2, attributes.length);
        assertEquals("1", attributes[0].toString());
        assertEquals("5", attributes[1].toString());
    }

    @Test
    public void testForDima2024062200102855995() throws SQLException {
        Connection conn = setConnection();
        CallableStatement cs = null;
        Array array = null;

        // smallint
        cs = conn.prepareCall("declare type tb is table of smallint; begin ? := tb(1, 2, 3); end; ");
        cs.registerOutParameter(1, Types.ARRAY);
        cs.execute();
        array = cs.getArray(1);
        assertForDima2024062200102855995(array, "NUMBER", ComplexDataType.TYPE_NUMBER, BigDecimal.valueOf(1), BigDecimal.valueOf(2), BigDecimal.valueOf(3));

        // int
        cs = conn.prepareCall("declare type tb is table of int; begin ? := tb(1, 2, 3); end; ");
        cs.registerOutParameter(1, Types.ARRAY);
        cs.execute();
        array = cs.getArray(1);
        assertForDima2024062200102855995(array, "NUMBER", ComplexDataType.TYPE_NUMBER, BigDecimal.valueOf(1), BigDecimal.valueOf(2), BigDecimal.valueOf(3));

        // decimal
        cs = conn.prepareCall("declare type tb is table of decimal; begin ? := tb(1, 2, 3); end; ");
        cs.registerOutParameter(1, Types.ARRAY);
        cs.execute();
        array = cs.getArray(1);
        assertForDima2024062200102855995(array, "NUMBER", ComplexDataType.TYPE_NUMBER, BigDecimal.valueOf(1), BigDecimal.valueOf(2), BigDecimal.valueOf(3));

        // date, return elem_type is 12, not 10.
        cs = conn.prepareCall("declare type tb is table of date; begin ? := tb(to_date('2024-11-05', 'yyyy-mm-dd'), to_date('2024-11-06', 'yyyy-mm-dd')); end; ");
        cs.registerOutParameter(1, Types.ARRAY);
        cs.execute();
        array = cs.getArray(1);
        assertForDima2024062200102855995(array, "DATE", ComplexDataType.TYPE_DATE, Timestamp.valueOf("2024-11-05 00:00:00"), Timestamp.valueOf("2024-11-06 00:00:00"));

        // datetime
        cs = conn.prepareCall("declare type tb is table of timestamp; begin ? := tb(to_timestamp('2024-11-05 12:23:34', 'YYYY-MM-DD HH24:MI:SS'), to_timestamp('2024-11-06 12:23:34', 'YYYY-MM-DD HH24:MI:SS')); end; ");
        cs.registerOutParameter(1, Types.ARRAY);
        cs.execute();
        array = cs.getArray(1);
        assertForDima2024062200102855995(array, "TIMESTAMP", ComplexDataType.TYPE_TIMESTMAP, Timestamp.valueOf("2024-11-05 12:23:34"), Timestamp.valueOf("2024-11-06 12:23:34"));

        // char
        cs = conn.prepareCall("declare type tb is table of char; begin ? := tb('a', 'b', 'c'); end; ");
        cs.registerOutParameter(1, Types.ARRAY);
        cs.execute();
        array = cs.getArray(1);
        assertForDima2024062200102855995(array, "CHAR", ComplexDataType.TYPE_CHAR, "a", "b", "c");

        // varchar
        cs = conn.prepareCall("declare type tb is table of varchar(10); begin ? := tb('a', 'b', 'c'); end; ");
        cs.registerOutParameter(1, Types.ARRAY);
        cs.execute();
        array = cs.getArray(1);
        assertForDima2024062200102855995(array, "VARCHAR2", ComplexDataType.TYPE_VARCHAR2, "a", "b", "c");

        // varchar2
        cs = conn.prepareCall("declare type tb is table of varchar2(10); begin ? := tb('a', 'b', 'c'); end; ");
        cs.registerOutParameter(1, Types.ARRAY);
        cs.execute();
        array = cs.getArray(1);
        assertForDima2024062200102855995(array, "VARCHAR2", ComplexDataType.TYPE_VARCHAR2, "a", "b", "c");

        // blob
        cs = conn.prepareCall("declare type tb is table of blob; begin ? := tb(TO_BLOB('68656C6C6F'), TO_BLOB('74657874')); end; ");
        cs.registerOutParameter(1, Types.ARRAY);
        cs.execute();
        array = cs.getArray(1);
        assertForDima2024062200102855995(array, "BLOB", ComplexDataType.TYPE_BLOB, "hello", "text");

        // clob, elem_type return 251, not 211, becaused by server's error.
        cs = conn.prepareCall("declare type tb is table of clob; begin ? := tb(TO_CLOB('hello'), TO_CLOB('text')); end; ");
        cs.registerOutParameter(1, Types.ARRAY);
        cs.execute();
        array = cs.getArray(1);
        assertForDima2024062200102855995(array, "CLOB", ComplexDataType.TYPE_CLOB, "hello", "text");

        // raw
        cs = conn.prepareCall("declare type tb is table of raw(20); begin ? := tb(UTL_RAW.CAST_TO_RAW('hello'), UTL_RAW.CAST_TO_RAW('text')); end; ");
        cs.registerOutParameter(1, Types.ARRAY);
        cs.execute();
        array = cs.getArray(1);
        assertForDima2024062200102855995(array, "RAW", ComplexDataType.TYPE_RAW, "hello", "text");
    }

    public void assertForDima2024062200102855995(Array array, String typeName, int type, Object... value) throws SQLException {
        if (typeName.equals("CLOB")) {
            Assert.assertEquals("BLOB", array.getBaseTypeName());
            Assert.assertEquals(ComplexDataType.TYPE_BLOB, array.getBaseType());
        } else {
            Assert.assertEquals(typeName, array.getBaseTypeName());
            Assert.assertEquals(type, array.getBaseType());
        }

        Object[] objects = (Object[]) array.getArray();
        for (int i = 0; i < objects.length; i++) {
            if (typeName.equals("BLOB") || typeName.equals("CLOB") || typeName.equals("RAW")) {
                Assert.assertEquals(value[i], new String((byte[]) objects[i]));
            } else {
                Assert.assertEquals(value[i], objects[i]);
            }
        }
    }
    
    @Test
    public void testForDima2024110800105015106() throws SQLException {
        Connection conn = setConnection();
        createFunction("func_1", "(a int, b int, c int) return int as v_res int; begin v_res := a + b + c; return v_res; end;  ");
        CallableStatement cs = conn.prepareCall("call ? :=test.func_1(10, 20, ?)}");
        cs.registerOutParameter(1, Types.INTEGER);
        cs.setInt(2, 30);
        cs.execute();
        Assert.assertEquals(60, cs.getInt(1));
    } 

    @Test
    public void testForDima2024062200102856610() throws SQLException {
        Connection conn = sharedConnection;
        createProcedure("t_pl1", "(b1 int default 10, p1 out int, b2 int default 5, p2 out int, p3 out int) is begin select b1, b1 - 5, b2 + 5 into p1, p2, p3 from dual; end;");
        CallableStatement cs;
        // in order
        cs = conn.prepareCall("call t_pl1(p1 => ?, p2 => ?, p3 => ?)");
        cs.registerOutParameter(1, Types.INTEGER);
        cs.registerOutParameter(2, Types.INTEGER);
        cs.registerOutParameter(3, Types.INTEGER);
        cs.execute();
        assertEquals(10, cs.getInt(1));
        assertEquals(5, cs.getInt(2));
        assertEquals(10, cs.getInt(3));

        // out of order
        cs = conn.prepareCall("call t_pl1(p3 => ?, p1 => ?, p2 => ?, b2 => ?, b1 => ?)");
        cs.registerOutParameter(1, Types.INTEGER);
        cs.registerOutParameter(2, Types.INTEGER);
        cs.registerOutParameter(3, Types.INTEGER);
        cs.setInt(4, 6);
        cs.setInt(5, 12);
        cs.execute();
        assertEquals(11, cs.getInt(1));
        assertEquals(12, cs.getInt(2));
        assertEquals(7, cs.getInt(3));

        cs = conn.prepareCall("call t_pl1(p3 => ?, p1 => ?, p2 => ?, b2 => ?, b1 => 12)");
        cs.registerOutParameter(1, Types.INTEGER);
        cs.registerOutParameter(2, Types.INTEGER);
        cs.registerOutParameter(3, Types.INTEGER);
        cs.setInt(4, 6);
        cs.execute();
        assertEquals(11, cs.getInt(1));
        assertEquals(12, cs.getInt(2));
        assertEquals(7, cs.getInt(3));

        createProcedure("str_pl", "(p1 varchar2, p2 varchar2, p3 out varchar2) is begin p3 := p1 || p2; end;");
        CallableStatement cs2 = conn.prepareCall("call str_pl(p3 => ?, p2 => 'pl_data=>data', p1 => ?) ");
        cs2.setString(2, "test_");
        cs2.registerOutParameter(1, Types.VARCHAR);
        cs2.execute();
        assertEquals("test_pl_data=>data", cs2.getString(1));
    }

    @Test
    public void testForDima2025022100107290272() throws Exception {
        Connection conn = setConnection("&useServerPrepStmts=true&extendOracleResultSetClass=true");
        createTable("t_cur", "c1 int");
        String query = "insert all";
        for (int i = 1; i <= 20; i++) {
            query += " into t_cur values(" + i + ")";
        }
        query += "select * from dual";
        Statement stmt = conn.createStatement();
        stmt.execute(query);

        createProcedure("pro_cur", "( b out sys_refcursor) is begin open b for select * from t_cur; end;");
        CallableStatement cstmt = conn.prepareCall("{call pro_cur(?)}");
        cstmt.registerOutParameter(1, Types.REF_CURSOR);
        cstmt.executeQuery();
        ResultSet rs = (ResultSet) cstmt.getObject(1);
        int row = 0;
        int i = 0;
        while (rs.next()) {
            Assert.assertEquals(++row, rs.getRow());
            Assert.assertEquals(rs.getInt(1), ++i);
        }
        Assert.assertEquals(20, i);
    }
    
}
