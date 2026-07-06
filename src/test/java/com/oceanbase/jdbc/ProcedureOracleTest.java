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

    @Test
    public void testGetProceduresForDima2024062200102856887() throws Exception {
        Connection conn = sharedConnection;
        createProcedure("sp1", "(c2 out int) /*sp1 comment1*/ is BEGIN SELECT 1 into c2 from dual; END");
        // upper
        ResultSet rs = conn.getMetaData().getProcedures(null, null, "SP1");
        assertTrue(rs.next());

        assertNull(rs.getString("PROCEDURE_CAT"));
        assertEquals("SP1", rs.getString("PROCEDURE_NAME"));
        assertNull(rs.getString(4));
        assertNull(rs.getString(5));
        assertNull(rs.getString(6));
        assertEquals("Standalone procedure or function", rs.getString("REMARKS"));
        assertEquals(DatabaseMetaData.procedureNoResult, rs.getInt("PROCEDURE_TYPE"));
        assertNull(rs.getString("SPECIFIC_NAME"));
        rs.close();

        // lower
        rs = conn.getMetaData().getProcedures(null, null, "sp1");
        Assert.assertTrue(rs.next());

        // lower & lowercaseRoutinesInMetadata=true
        conn = setConnection("&lowercaseRoutinesInMetadata=true");
        rs = conn.getMetaData().getProcedures(null, null, "sp1");
        assertTrue(rs.next());

        assertNull(rs.getString("PROCEDURE_CAT"));
        assertEquals("SP1", rs.getString("PROCEDURE_NAME"));
        assertNull(rs.getString(4));
        assertNull(rs.getString(5));
        assertNull(rs.getString(6));
        assertEquals("Standalone procedure or function", rs.getString("REMARKS"));
        assertEquals(DatabaseMetaData.procedureNoResult, rs.getInt("PROCEDURE_TYPE"));
        assertNull(rs.getString("SPECIFIC_NAME"));
        rs.close();
    }

    @Test
    public void testForDima2025033100107911046() throws SQLException {
        /*
        * -- Created under sys user
        * create or replace procedure p1 (c1  in varchar2,  c2 in out varchar2, c3 out varchar2, c4 out varchar2) is begin c3 := '1'; c4 := 'message'; end;
        *
        * -- public synonym of same name
        * create or replace package pack_pl is procedure proc_1 (c1  in varchar2,  c2 in out varchar2, c3 out varchar2, c4 out varchar2); end pack_pl;
        * create or replace package body pack_pl is procedure proc_1 (c1  in varchar2,  c2 in out varchar2, c3 out varchar2, c4 out varchar2) as begin c3 := 'same-name-sync'; c4 := 'message'; end; end pack_pl;
        * CREATE public SYNONYM pack_pl FOR sys.pack_pl;
        *
        * */
        Connection conn = setConnection();
        CallableStatement cs = conn.prepareCall("call SYS.P1(?,?,?,?)");
        cs.setString(1, "111");
        cs.setString(2, "222");
        //cs.registerOutParameter(2, Types.VARCHAR); Don't register inout parameters
        cs.registerOutParameter(3, Types.VARCHAR);
        cs.registerOutParameter(4, Types.VARCHAR);
        cs.execute();
        Assert.assertEquals("1", cs.getString(3));
        Assert.assertEquals("message", cs.getString(4));

        //  public synonym of same name
        CallableStatement cs2 = conn.prepareCall("call pack_pl.proc_1(?,?,?,?)");
        cs2.setString(1, "111");
        cs2.setString(2, "222");
        //cs2.registerOutParameter(2, Types.VARCHAR); Don't register inout parameters
        cs2.registerOutParameter(3, Types.VARCHAR);
        cs2.registerOutParameter(4, Types.VARCHAR);
        cs2.execute();
        cs2.execute();
        Assert.assertEquals("same-name-sync", cs2.getString(3));
        Assert.assertEquals("message", cs2.getString(4));
    }

    @Test
    public void testForDima2025051600109048468() throws Exception {
        Connection conn = setConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("create or replace procedure proc_1(p1 varchar2, p2 varchar2, p3 varchar2, p4 out varchar2) is begin p4 := p1 || p2 || p3; end;");
        CallableStatement cs = conn.prepareCall("call proc_1('a', ?, ?, ?)");
        cs.setString(1, "_test_");
        cs.setString(2, "b");
        // Register the in type parameter as out type
        cs.registerOutParameter(2, Types.VARCHAR);
        cs.registerOutParameter(3, Types.VARCHAR);
        cs.execute();
        Assert.assertEquals("a_test_b", cs.getString(3));
    }

    @Test
    public void testForDima2025051600109048468_2() throws SQLException {
        Connection conn = setConnection("");
        Statement stmt = conn.createStatement();
        stmt.execute("create or replace package \"t_pack2\" is function t_fun1(a out int) return varchar; end;");
        stmt.execute("create or replace package body \"t_pack2\" is function t_fun1(a out int) return varchar as begin a:=2; return 'aaa'; end; end");

        // have a spaces in the database name
        CallableStatement cs = conn.prepareCall("{? =call " + database + " .\"t_pack2\".t_fun1(?)}");
        cs.registerOutParameter(1, Types.INTEGER);
        cs.registerOutParameter(2, Types.INTEGER);
        cs.execute();
        assertEquals("aaa", cs.getString(1));
        assertEquals(2, cs.getInt(2));
    }

    @Test
    public void testForDima2024062100102853722() throws SQLException {
        Connection conn = setConnection("&useServerPrepStmts=true&useOraclePrepareExecute=true");
        createTable("z_t2", "c1 int");
        Statement stmt = conn.createStatement();
        stmt.execute("create or replace procedure pl_insert(p1 int,  p2 int ,p3 int) is begin insert into z_t2 values (p1); insert into z_t2 values (p2); insert into z_t2 values  (p3); end;");

        CallableStatement call = conn.prepareCall("call pl_insert(?, ?, ?)");
        call.setInt(1, 1);
        call.setInt(2, 2);
        call.setInt(3, 3);
        Assert.assertEquals(0, call.executeUpdate());
        Assert.assertEquals(-1, call.getUpdateCount());
        call.close();

        CallableStatement call2 = conn.prepareCall("call pl_insert(?, ?, ?)");
        call2.setInt(1, 1);
        call2.setInt(2, 2);
        call2.setInt(3, 3);
        Assert.assertEquals(0, call2.executeLargeUpdate());
        Assert.assertEquals(-1, call2.getLargeUpdateCount());

        // out pl
        createProcedure("testPro3", "(a out int) is begin a := 1; end;");
        CallableStatement cs = conn.prepareCall("call testPro3(?)");
        cs.registerOutParameter(1, Types.INTEGER);
        Assert.assertEquals(0, cs.executeUpdate());
        Assert.assertEquals(-1, cs.getUpdateCount());

        CallableStatement cs2 = conn.prepareCall("{call testPro3(?)}");
        cs2.registerOutParameter(1, Types.INTEGER);
        Assert.assertEquals(0, cs2.executeUpdate()); // oracle-jdbc = 1
        Assert.assertEquals(-1, cs2.getUpdateCount());

        CallableStatement cs3 = conn.prepareCall("begin testPro3(?); end;");
        cs3.registerOutParameter(1, Types.INTEGER);
        Assert.assertEquals(1, cs3.executeUpdate());
        Assert.assertEquals(-1, cs3.getUpdateCount());

        CallableStatement cs4 = conn.prepareCall("declare a int; begin a := 1; testPro3(?); end;");
        cs4.registerOutParameter(1, Types.INTEGER);
        Assert.assertEquals(1, cs4.executeUpdate());
        Assert.assertEquals(-1, cs4.getUpdateCount());

        // DML
        PreparedStatement ps = conn.prepareStatement("insert into z_t2 values (1),(2),(3),(4)");
        Assert.assertEquals(4, ps.executeUpdate());
        Assert.assertEquals(4, ps.getUpdateCount());

        PreparedStatement ps2 = conn.prepareStatement("insert into z_t2 values (1),(2)");
        Assert.assertEquals(2, ps2.executeLargeUpdate());
        Assert.assertEquals(2, ps2.getLargeUpdateCount());
    }

    @Test
    public void testForDima2024062100102853722_2() throws SQLException {
        Connection conn = setConnection("&useServerPrepStmts=true&useOraclePrepareExecute=true");
        Statement stmt = conn.createStatement();
        createTable("call_name", "c1 int, c2 varchar2(20)");
        String sql = "insert into call_name values(:1, :2)";
        CallableStatement call = conn.prepareCall(sql);
        call.setInt(1, 1);
        call.setString(2, "aaa");
        assertEquals(1, call.executeUpdate());
        assertEquals(1, call.getUpdateCount());

        call = conn.prepareCall("insert into call_name values(:1, :2),(:a, :b)");  //oracle-jdbc 不支持语法
        call.setInt(1, 2);
        call.setString(2, "bbb");
        call.setInt(3, 3);
        call.setString(4, "ccc");
        assertEquals(2, call.executeUpdate());
        assertEquals(2, call.getUpdateCount());

        call = conn.prepareCall(sql);
        call.setInt(1, 4);
        call.setString(2, "aaa");
        call.addBatch();
        call.setInt(1, 5);
        call.setString(2, "aaa");
        call.addBatch();
        call.executeBatch();
        assertEquals(2, call.getUpdateCount());

        createTable("z_t3", "c1 int");
        for (int i = 1; i <= 3 ; i++) {
            for (int j = 1; j <= i; j++) {
                stmt.execute("insert into z_t3 values (" + i + ")");
            }
        }
        CallableStatement batchPs = conn.prepareCall("delete from z_t3 where c1 = ?");
        batchPs.setInt(1, 0);
        batchPs.addBatch();
        batchPs.setInt(1, 1);
        batchPs.addBatch();
        batchPs.setInt(1, 3);
        batchPs.addBatch();
        batchPs.setInt(1, 2);
        batchPs.addBatch();
        int[] result = batchPs.executeBatch();
        Assert.assertArrayEquals(new int[]{0, 1, 3, 2}, result);
        Assert.assertEquals(6, batchPs.getUpdateCount());
    }

    @Test
    public void testForDimatestForDima2025041600108299276() throws SQLException {
        boolean prepareExecuteProtocol = true;
        CallableStatement cs = null;

        Connection conn = setConnection("&useOraclePrepareExecute=true&obIncludeOutOrNullParamTypeInfo=true");
        Statement stmt = conn.createStatement();
        stmt.execute("create or replace package pack_pl is \n" +
                "  procedure proc_1(a1 in varchar2, a2 in varchar2, a3 out varchar2, a4 out varchar2);\n" +
                "  procedure proc_1(b1 in number, b2 in out number, b3 in number, b4 out number);\n" +
                "  procedure proc_1(c1 in varchar2, c2 in out varchar2, c3 out varchar2);\n" +
                "  end pack_pl; ");

        stmt.execute("create or replace package body pack_pl is \n" +
                "  procedure proc_1(a1 in varchar2, a2 in varchar2, a3 out varchar2, a4 out varchar2) as begin a3 := 'a3'; a4 := 'a4'; end;\n" +
                "  procedure proc_1(b1 in number, b2 in out number, b3 in number, b4 out number) is begin b4 := b1 + b2 + b3 ; b2 := 0; end;\n" +
                "  procedure proc_1(c1 in varchar2, c2 in out varchar2, c3 out varchar2) is begin c3 := 'c3'; end;\n" +
                "end pack_pl;");

        stmt.execute("create or replace procedure proc_1(p1 in number, p2 in number, p3 in out number, p4 out number) is begin p4 := p1 + p2 + p3; end ;");

        for (int i = 0; i < 2; i++) {
            if (i == 1) {
                conn = setConnection("&useServerPrepStmts=true&obIncludeOutOrNullParamTypeInfo=true");
                prepareExecuteProtocol = false;
            }

            // case1: same number of parameters, different data types.
            cs = conn.prepareCall("{call pack_pl.proc_1(?, ?, ?, ?)}");
            cs.setInt(1, 1);
            cs.setInt(2, 2);
            cs.setInt(3, 3);
            cs.registerOutParameter(4, Types.INTEGER);
            cs.execute();

            if (prepareExecuteProtocol) {
                Assert.assertEquals(0, cs.getInt(2));
                Assert.assertEquals(6, cs.getInt(4));
            } else {
                try {
                    cs.getInt(2);
                    fail();
                } catch (SQLException e) {
                    Assert.assertTrue(e.getMessage().contains("is not declared as output parameter with method registerOutParameter"));
                }
                // A missing registration of an out parameter caused a misalignment in the value retrieval. Expected :6
                Assert.assertEquals(0, cs.getInt(4));
            }

            // case2: has default value
            cs = conn.prepareCall("call proc_1(1, 2, ?, ?)");
            cs.setInt(1, 3);
            cs.registerOutParameter(2, Types.INTEGER);
            cs.execute();
            if (prepareExecuteProtocol) {
                Assert.assertEquals(3, cs.getInt(1));
                Assert.assertEquals(6, cs.getInt(2));
            } else {
                try {
                    cs.getInt(1);
                    fail();
                } catch (SQLException e) {
                    Assert.assertTrue(e.getMessage().contains("is not out parameter"));
                }
                // A missing registration of an out parameter caused a misalignment in the value retrieval. Expected :6
                Assert.assertEquals(3, cs.getInt(2));
            }

            // case3: different number of parameters
            cs = conn.prepareCall("call pack_pl.proc_1('c1', ?, ?)");
            cs.setString(1, "c2");
            cs.registerOutParameter(2, Types.VARCHAR);
            cs.execute();
            if (prepareExecuteProtocol) {
                Assert.assertEquals("c2", cs.getString(1));
                Assert.assertEquals("c3", cs.getString(2));
            } else {
                try {
                    cs.getString(1);
                    fail();
                } catch (SQLException e) {
                    Assert.assertTrue(e.getMessage().contains("is not out parameter"));
                }
                // A missing registration of an out parameter caused a misalignment in the value retrieval. Expected :c3
                Assert.assertEquals("c2", cs.getString(2));
            }

            // case4: pl anonymous block
            cs = conn.prepareCall("begin proc_1(?, ?, ?, ?); end;");
            cs.setInt(1, 1);
            cs.setInt(2, 2);
            cs.setInt(3, 3);
            cs.registerOutParameter(4, Types.INTEGER);
            cs.execute();

            if (prepareExecuteProtocol) {
                Assert.assertEquals(3, cs.getInt(3));
                Assert.assertEquals(6, cs.getInt(4));
            } else {
                try {
                    cs.getInt(3);
                    fail();
                } catch (SQLException e) {
                    Assert.assertTrue(e.getMessage().contains("is not out parameter"));
                }
                // A missing registration of an out parameter caused a misalignment in the value retrieval. Expected :6
                Assert.assertEquals(3, cs.getInt(4));
            }

            cs.close();
            conn.close();
        }

    }

    @Test
    public void testForDima2025063000109741787() throws SQLException {
        Connection conn = setConnection("&useServerPrepStmts=true&useOraclePrepareExecute=true&obIncludeOutOrNullParamTypeInfo=true");

        Statement stmt = conn.createStatement();
        stmt.execute("create or replace type plps_obj0 force as object(c0 int)");
        stmt.execute("create or replace type plps_obj1 force as object(c0 int, c1 int)");
        stmt.execute("create or replace package plps_pkg1 as procedure p1(a plps_obj0);end;");
        stmt.execute("create or replace package body plps_pkg1 as procedure p1(a plps_obj0) is begin null;end;end;");

        stmt.execute("create or replace type obj0 force as object(c0 int)");

        stmt.execute("create or replace procedure p1(a out sys_refcursor) is begin "
                + "  open a for select obj0(123) from dual; " + "end;");

        CallableStatement cs = conn.prepareCall("call p1(?)");
        cs.registerOutParameter(1, Types.REF_CURSOR);
        cs.execute();
        ResultSet rs = (ResultSet) cs.getObject(1);
        Assert.assertTrue(rs.next());
        Struct obj = (Struct) rs.getObject(1);
        Object[] attributes = obj.getAttributes();
        Assert.assertEquals(1, attributes.length);
        Assert.assertEquals(new BigDecimal(123), attributes[0]);
    }

    @Test
    public void testForDima2025052200109137103() throws SQLException {
        Connection conn = setConnection("&useServerPrepStmts=true&useOraclePrepareExecute=false&obIncludeOutOrNullParamTypeInfo=true");
        Statement stmt = conn.createStatement();
        CallableStatement cs;
        stmt.execute("create or replace package plkg_pk1 is " +
                " procedure proc_p1 (p1 in varchar2, p2 out varchar2); " +
                " procedure proc_p1 (p1 in number, p2 out number); " +
                " end plkg_pk1;");

        stmt.execute("create or replace package body plkg_pk1 is " +
                " procedure proc_p1 (p1 in varchar2, p2 out varchar2) is begin p2 := 'a'; end; " +
                " procedure proc_p1 (p1 in number, p2 out number) is begin p2 := 1; end; " +
                " end plkg_pk1;");

        CallableStatement call = conn.prepareCall("call plkg_pk1.proc_p1(?, ?)");
        call.setNull(1, Types.INTEGER);
        call.registerOutParameter(2, Types.INTEGER);
        call.execute();
        Assert.assertEquals(1, call.getInt(2));

        // testForDima2025082100110574385
        stmt.execute("create or replace package pack4 is " +
                "procedure proc5(a in number, b in out number, c in number default 10); " +
                "procedure proc5(a in number, b in varchar2, c out number); " +
                "end;");
        stmt.execute("create or replace package body pack4 is " +
                "procedure proc5(a in number, b in out number, c in number default 10) is  begin b := a + b + c; end; " +
                "procedure proc5(a in number, b in varchar2, c out number) is begin c := a || b ; end; " +
                "end;");
        cs = conn.prepareCall("{call pack4.proc5(a => ?,b => ?)}");
        cs.setInt(1, 7); // a
        cs.setInt(2, 10); // b
        cs.registerOutParameter(2, Types.INTEGER);
        cs.execute();
        Assert.assertEquals(27, cs.getInt(2));

        // testForDima2025082200110581469
        stmt.execute("create or replace package pack2 is procedure proc2(a1 in out number, a2 in out number, a3 out number); end;");
        stmt.execute("create or replace package body pack2 is procedure proc2(a1 in out number, a2 in out number, a3 out number) is begin a3 := a1 + a2;a1 := 100; end; end;");
        stmt.execute("create or replace procedure proc2(a1 in out number, a2 in out number, a3 out number) is begin a3 := a1 + a2 + 10; a1 := 110; end;");

        cs = conn.prepareCall("{call pack2.proc2(a2 => ?, a1 => ?, a3 => ?)}");
        cs.setInt(1, 20); // a2
        cs.setInt(2, 10); // a1
        cs.registerOutParameter(1, Types.INTEGER); //a2
        cs.registerOutParameter(2, Types.INTEGER); // a1
        cs.registerOutParameter(3, Types.INTEGER); // a3
        cs.execute();
        Assert.assertEquals(cs.getInt(1), 20);
        Assert.assertEquals(cs.getInt(2), 100);
        Assert.assertEquals(cs.getInt(3), 30);

        cs = conn.prepareCall("call proc2(a3 => ?, a1 => ?, a2 => ?)");
        cs.setInt(2, 10); // a1
        cs.setInt(3, 20); // a2
        cs.registerOutParameter(1, Types.INTEGER); // a3
        cs.registerOutParameter(2, Types.INTEGER); // a1
        cs.registerOutParameter(3, Types.INTEGER); // a2
        cs.execute();
        Assert.assertEquals(cs.getInt(1), 40);
        Assert.assertEquals(cs.getInt(2),110);
        Assert.assertEquals(cs.getInt(3), 20);
    }

    @Test
    public void testForDima2025071600109984548() throws Exception {
        Connection conn = setConnection("&useServerPrepStmts=true&useOraclePrepareExecute=true&obIncludeOutOrNullParamTypeInfo=true");
        Statement stmt = conn.createStatement();
        stmt.execute(
                "create or replace package pack3 is " +
                        "procedure proc4(x in out number, y in number, z out number); " +
                        "procedure proc4(x in out number, y in number, z out varchar2); " +
                        "end;");
        stmt.execute(
                "create or replace package body pack3 is " +
                        "procedure proc4(x in out number, y in number, z out number) is begin z := x*y;  x := y * z; end; " +
                        "procedure proc4(x in out number, y in number, z out varchar2) is begin z := x || y;  x := x + 1; end; " +
                        "end;");

        CallableStatement cs = conn.prepareCall("{call pack3.proc4(?, ?, ?)}");
        cs.setInt(1, 5); // x
        cs.registerOutParameter(1, Types.INTEGER); // x
        cs.setInt(2, 6); // y
        cs.registerOutParameter(3, Types.VARCHAR); // z
        cs.execute();
        Assert.assertEquals(6, cs.getInt(1));
        Assert.assertEquals(56, cs.getInt(3));
    }

    @Test
    public void testForDima2025072300110097845() throws SQLException {
        Connection conn = setConnection("useServerPrepStmts=true&useOraclePrepareExecute=true&obIncludeOutOrNullParamTypeInfo=true");
        Statement stmt = conn.createStatement();

        String packageSql = "CREATE OR REPLACE PACKAGE test_exception IS " +
                "PROCEDURE proc5(p1 IN NUMBER, p2 OUT VARCHAR2); " +
                "PROCEDURE proc5(p1 IN VARCHAR2, p2 OUT VARCHAR2); " +
                "END test_exception;";

        String bodySql = "CREATE OR REPLACE PACKAGE BODY test_exception IS " +
                "PROCEDURE proc5(p1 IN NUMBER, p2 OUT VARCHAR2) IS " +
                "BEGIN " +
                "  IF p1 > 0 THEN p2 := 'POSITIVE_' || p1; " +
                "  ELSE p2 := 'NEGATIVE_' || ABS(p1); END IF; " +
                "EXCEPTION WHEN OTHERS THEN p2 := 'ERROR_NUM'; END; " +
                "PROCEDURE proc5(p1 IN VARCHAR2, p2 OUT VARCHAR2) IS " +
                "BEGIN " +
                "  IF LENGTH(p1) > 0 THEN p2 := 'STRING_' || p1; " +
                "  ELSE p2 := 'EMPTY_STRING'; END IF; " +
                "EXCEPTION WHEN OTHERS THEN p2 := 'ERROR_STR'; END; " +
                "END test_exception;";

        stmt.execute(packageSql);
        stmt.execute(bodySql);

        CallableStatement cs = conn.prepareCall("call test_exception.proc5(?, ?)");
        cs.setInt(1, 50);
        cs.registerOutParameter(2, Types.VARCHAR);
        cs.execute();
        Assert.assertEquals("POSITIVE_50", cs.getString(2));

        cs.setNull(1, Types.VARCHAR);
        cs.execute();
        Assert.assertEquals("EMPTY_STRING", cs.getString(2));
    }

    @Test
    public void testForDima2025072200110062707() throws SQLException {
        Connection conn = setConnection("&useServerPrepStmts=true&useOraclePrepareExecute=true&obConvertCallsToBlocks=true");
        Statement stmt = conn.createStatement();
        stmt.execute("create or replace procedure call_proc as c number; begin select 1 into c from dual where 1 <> 1; end;");
        CallableStatement cs = conn.prepareCall("call call_proc()");
        cs.execute();

        try {
            cs = conn.prepareCall(" begin call_proc(); end; ");
            cs.execute();
            Assert.fail();
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("no data found"));
        }

        try {
            cs= conn.prepareCall(" {call call_proc() } ");
            cs.execute();
            Assert.fail();
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("no data found"));
        }
    }

    @Test
    public void testForDima2025092300111121722() throws SQLException {
        Connection conn = setConnection("&useServerPrepStmts=true&useOraclePrepareExecute=true&obConvertCallsToBlocks=true&cacheCallableStmts=true");
        Statement stmt = conn.createStatement();
        String packageSql = "create or replace package pkg3 is procedure p1(p1 in varchar2, p2 out varchar2);"
                            + " procedure p1(p1 in varchar2, p2 in varchar2, p3 in varchar2, p4 out varchar2); end; ";

        String bodySql = "create or replace package body pkg3 is"
                         + " procedure p1(p1 in varchar2, p2 out varchar2) is begin p2 := p1 || '_1'; end;"
                         + " procedure p1(p1 in varchar2, p2 in varchar2, p3 in varchar2, p4 out varchar2) is begin p4 := p1 || p2 || p3; end; end;";

        stmt.execute(packageSql);
        stmt.execute(bodySql);

        CallableStatement cs = conn.prepareCall("call pkg3.p1(?,?,?,?)");
        cs.setString(1, "test");
        cs.registerOutParameter(2, Types.VARCHAR);
        cs.registerOutParameter(3, Types.VARCHAR);
        cs.registerOutParameter(4, Types.VARCHAR);
        cs.execute();
        try {
            cs.getString(2);
        } catch (SQLException e) {
            assertEquals("Parameter in index '2' is not out parameter", e.getMessage());
        }
        assertEquals("test", cs.getString(4));

        cs = conn.prepareCall("call pkg3.p1(?,?,?,?)");
        cs.setString(1, "test");
        cs.setString(2, "bbb");
        cs.setString(3, "aaa");
        cs.registerOutParameter(4, Types.VARCHAR);
        cs.execute();
        assertEquals("testbbbaaa", cs.getString(4));

        cs = conn.prepareCall("call pkg3.p1(?,?,?,?)");
        cs.setString(1, "test");
        cs.setString(2, "222");
        cs.setString(3, "aaa");
        cs.registerOutParameter(4, Types.VARCHAR);
        cs.execute();
        assertEquals("test222aaa", cs.getString(4));
    }

    @Test
    public void testForPLFix() throws SQLException {
        Connection conn = setConnection("&obIncludeOutOrNullParamTypeInfo=true&useServerPrepStmts=true&useOraclePrepareExecute=false");
        Statement stmt = conn.createStatement();
        stmt.execute("create or replace package pkg1 as procedure p1(a out int);procedure p1(a out varchar2);end;");
        stmt.execute("create or replace package body pkg1 as procedure p1(a out int) is begin a:=111;end;procedure p1(a out varchar2)is begin a:='aaa';end;end;");
        CallableStatement cs = conn.prepareCall("call pkg1.p1(?)");
        cs.registerOutParameter(1, Types.INTEGER);
        cs.execute();
        assertEquals("111", cs.getString(1));

        cs = conn.prepareCall("call pkg1.p1(?)");
        cs.registerOutParameter(1, Types.VARCHAR);
        cs.execute();
        assertEquals("aaa", cs.getString(1));

        cs = conn.prepareCall("{call pkg1.p1(a => ?)}");
        cs.registerOutParameter(1, Types.INTEGER);
        cs.execute();
        assertEquals("111", cs.getString(1));

        // testForDima2025092300111130039
        cs = conn.prepareCall("begin pkg1.p1(?); end;");
        cs.registerOutParameter(1, Types.VARCHAR);
        cs.execute();
        assertEquals("aaa", cs.getString(1));

        // testForDima2025092300111130363
        cs = conn.prepareCall("declare v varchar2(10); begin pkg1.p1(?); end;");
        cs.registerOutParameter(1, Types.VARCHAR);
        cs.execute();
        assertEquals("aaa", cs.getString(1));
    }

    @Test
    public void testForDima2025101100111371470() throws SQLException {
        Connection conn = setConnection();
        createProcedure("test_proc1", "(a varchar2, b out varchar2, c varchar2) is begin b := a || c; end;");
        CallableStatement cs = conn.prepareCall("call test_proc1(to_char(?), ?, '_oceanbase_' || ?)");
        cs.setString(1, "test1");
        cs.registerOutParameter(2, Types.VARCHAR);
        cs.setString(3, "test1");
        cs.execute();
        Assert.assertEquals("test1_oceanbase_test1", cs.getString(2));

        cs = conn.prepareCall("call test_proc1(to_char(:a), :b, ':ob_oceanbase_' || :c)");
        cs.setString(1, "test2");
        cs.registerOutParameter(2, Types.VARCHAR);
        cs.setString(3, "test2");
        cs.execute();
        Assert.assertEquals("test2:ob_oceanbase_test2", cs.getString(2));
    }

    @Test
    public void testForDima2025111300111925940() throws SQLException {
        Connection conn = setConnection("&useOraclePrepareExecute=true");
        Statement stmt = conn.createStatement();
        stmt.execute("create or replace procedure fqguo(p1 number, pcur out number, p2 number) as begin pcur := 1024; end;");

        CallableStatement cs = conn.prepareCall("{call fqguo(?, ?, ?)}");
        cs.setInt(1, 1);
        cs.registerOutParameter(2, Types.INTEGER);
        cs.setInt(3, 2);
        cs.execute();
        assertEquals(1024, cs.getInt(2));

        cs = conn.prepareCall("{call fqguo(15, ?, 8)}");
        cs.registerOutParameter(1, Types.INTEGER);
        cs.execute();
        assertEquals(1024, cs.getInt(1));
    }

    @Test
    public void testForDima2025092400111147975() throws SQLException {
        Connection conn = setConnection("&useOraclePrepareExecute=true&obIncludeOutOrNullParamTypeInfo=true");
        Statement stmt = conn.createStatement();

        stmt.execute("create or replace package param_type_pkg as " +
                "procedure test_proc(a in int, b out varchar2); " +
                "procedure test_proc(a in int, b out number); " +
                "end;");

        stmt.execute("create or replace package body param_type_pkg as " +
                "procedure test_proc(a in int, b out varchar2) is begin b := 'str_' || a; end; " +
                "procedure test_proc(a in int, b out number) is begin b := a * 10; end; " +
                "end;");

        try {
            CallableStatement cs1 = conn.prepareCall("call param_type_pkg.test_proc(?, ?)");
            cs1.setInt(1, 5);
            cs1.registerOutParameter(2, Types.VARCHAR);
            cs1.execute();
            assertEquals("str_5", cs1.getString(2));

            CallableStatement cs2 = conn.prepareCall("call param_type_pkg.test_proc(?, ?)");
            cs2.setInt(1, 5);
            cs2.registerOutParameter(2, Types.NUMERIC);
            cs2.execute();
            assertEquals(50, cs2.getInt(2));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testForDima2025092300111131115() throws SQLException {
        Connection conn = setConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("create or replace package PKF_NAMED as function j(a in out varchar2, b in varchar2, c in varchar2) return varchar2; end;");
        stmt.execute("create or replace package body PKF_NAMED as function j(a in out varchar2, b in varchar2, c in varchar2) return varchar2 is begin return a||b||c; end; end;");

        CallableStatement cs = conn.prepareCall("? = call PKF_NAMED.j(b=>?, a=>?, c=>?)");
        cs.registerOutParameter(1, Types.VARCHAR);
        cs.registerOutParameter(3, Types.VARCHAR);
        cs.setString(2, "B");
        cs.setString(3, "A");
        cs.setString(4, "C");
        cs.execute();
        assertEquals("ABC", cs.getString(1));
        assertEquals("A", cs.getString(3));
    }

    @Test
    public void testForDima2026012600113705588() throws Exception {
        Connection conn = setConnection("&useServerPrepStmts=true&useOraclePrepareExecute=true");
        Statement stmt = conn.createStatement();
        stmt.execute("create or replace procedure test_proc1(a in varchar2, b out varchar2, c in varchar2) is begin b := a || c; end;");
        CallableStatement cs;

        cs = conn.prepareCall("call test_proc1(?, ?, ?)");
        Assert.assertEquals(3, cs.getParameterMetaData().getParameterCount());
        cs.setString(1, "hello");
        cs.registerOutParameter(2, Types.VARCHAR);
        cs.setString(3, "OB");
        cs.execute();
        Assert.assertEquals("helloOB", cs.getString(2));
        Assert.assertEquals("helloOB", cs.getString("B"));

        cs = conn.prepareCall("call test_proc1(" +
                "case when length(?) > 5 then substr(?, 1, 5) || '...' else ? end, ?, case when instr(?, '@') > 0 then 'Email' else 'text' end)");
        Assert.assertEquals(5, cs.getParameterMetaData().getParameterCount());
        cs.setString(1, "OceanBase");
        cs.setString(2, "OceanBase");
        cs.setString(3, "OceanBase");
        cs.registerOutParameter(4, Types.VARCHAR);
        cs.setString(5, "ob@oceanbase.com");
        cs.execute();
        Assert.assertEquals("Ocean...Email", cs.getString(4));

        cs = conn.prepareCall("call test_proc1(" +
                "case when length(?) > 5 then '...' else ? end, ?, 'Email')");
        Assert.assertEquals(3, cs.getParameterMetaData().getParameterCount());
        cs.setString(1, "OceanBase");
        cs.setString(2, "OceanBase");
        cs.registerOutParameter(3, Types.VARCHAR);
        cs.execute();
        Assert.assertEquals("...Email", cs.getString(3));

        cs = conn.prepareCall("call test_proc1(" +
                "case when length(:A1) > 5 then substr(:A2, 1, 5) || '...' else :A3 end, :B, case when instr(:C, '@') > 0 then 'Email' else 'text' end)");
        Assert.assertEquals(5, cs.getParameterMetaData().getParameterCount());
        cs.setString("A1", "OceanBase");
        cs.setString("A2", "OceanBase");
        cs.setString("A3", "OceanBase");
        cs.registerOutParameter("B", Types.VARCHAR);
        cs.setString("C", "ob@oceanbase.com");
        cs.execute();
        Assert.assertEquals("Ocean...Email", cs.getString(4));
        Assert.assertEquals("Ocean...Email", cs.getString("B"));

        cs = conn.prepareCall("begin test_proc1(" +
                "case when length(?) > 5 then substr(?, 1, 5) || '...' else ? end, ?, case when instr(?, '@') > 0 then 'Email' else 'text' end); end;");
        Assert.assertEquals(5, cs.getParameterMetaData().getParameterCount());
        cs.setString(1, "OceanBase");
        cs.setString(2, "OceanBase");
        cs.setString(3, "OceanBase");
        cs.registerOutParameter(4, Types.VARCHAR);
        cs.setString(5, "ob@oceanbase.com");
        cs.execute();
        Assert.assertEquals("Ocean...Email", cs.getString(4));

        cs = conn.prepareCall("begin test_proc1(" +
                "case when length(:A1) > 5 then substr(:A2, 1, 5) || '...' else :A3 end, :B, case when instr(:C, '@') > 0 then 'Email' else 'text' end); end;");
        Assert.assertEquals(5, cs.getParameterMetaData().getParameterCount());
        cs.setString("A1", "OceanBase");
        cs.setString("A2", "OceanBase");
        cs.setString("A3", "OceanBase");
        cs.registerOutParameter("B", Types.VARCHAR);
        cs.setString("C", "ob@oceanbase.com");
        cs.execute();
        Assert.assertEquals("Ocean...Email", cs.getString(4));
        Assert.assertEquals("Ocean...Email", cs.getString("B"));
    }

    @Test
    public void testForDima2026042200115626855() throws SQLException {
        Connection conn = setConnection("&useServerPrepStmts=true&useOraclePrepareExecute=true");
        Statement stmt = conn.createStatement();
        stmt.execute("create or replace procedure p_expr_concat(a in varchar2, b out varchar2) is begin b := a; end;");
        CallableStatement cs = conn.prepareCall("call p_expr_concat(  ? || ?, ?)");
        cs.setString(1, "hello");
        cs.setString(2, "world");
        cs.registerOutParameter(3, Types.VARCHAR);
        cs.execute();
        Assert.assertEquals("helloworld", cs.getString(3));
    }

    @Test
    public void testForDima2026050900115989186() throws SQLException {
        Connection conn = setConnection("&obIncludeOutOrNullParamTypeInfo=true&usePieceData=true");
        Statement stmt = conn.createStatement();
        stmt.execute("create or replace package pkg1 is " +
                "procedure proc_1(p1 number, p2 out number, p3 out varchar2) ; " +
                "procedure proc_1(p1 number, p2 out date, p3 out varchar2) ; " +
                "procedure proc_1(p1 number, p2 out clob, p3 out varchar2) ; " +
                "procedure proc_1(p1 number, p2 out varchar, p3 out varchar2) ; " +
                "procedure proc_1(p1 number, p2 out binary_float, p3 out varchar2) ; " +
                "end;");

        stmt.execute("create or replace package body pkg1 is " +
                "procedure proc_1(p1 number, p2 out number, p3 out varchar2) is begin p3 := 'type is number'; end; " +
                "procedure proc_1(p1 number, p2 out date, p3 out varchar2) is begin p3 := 'type is date'; end; " +
                "procedure proc_1(p1 number, p2 out clob, p3 out varchar2) is begin p3 := 'type is clob'; end; " +
                "procedure proc_1(p1 number, p2 out varchar, p3 out varchar2) is begin p3 := 'type is varchar'; end; " +
                "procedure proc_1(p1 number, p2 out binary_float, p3 out varchar2) is begin p3 := 'type is binary_float'; end; " +
                "end; ");

        CallableStatement cs = conn.prepareCall("call pkg1.proc_1(?, ?, ?)");
        cs.setInt(1, 1);
        cs.registerOutParameter(2, Types.TIME);
        cs.registerOutParameter(3, Types.VARCHAR);
        cs.execute();
        Assert.assertEquals("type is date", cs.getString(3));

        cs.setInt(1, 1);
        cs.registerOutParameter(2, Types.CLOB);
        cs.registerOutParameter(3, Types.VARCHAR);
        cs.execute();
        Assert.assertEquals("type is clob", cs.getString(3));

        cs.setInt(1, 1);
        cs.registerOutParameter(2, Types.DECIMAL);
        cs.registerOutParameter(3, Types.VARCHAR);
        cs.execute();
        Assert.assertEquals("type is number", cs.getString(3));

        cs.setInt(1, 1);
        cs.registerOutParameter(2, Types.FLOAT);
        cs.registerOutParameter(3, Types.VARCHAR);
        cs.execute();
        Assert.assertEquals("type is binary_float", cs.getString(3));

        cs.setInt(1, 1);
        cs.registerOutParameter(2, Types.CHAR);
        cs.registerOutParameter(3, Types.VARCHAR);
        cs.execute();
        Assert.assertEquals("type is varchar", cs.getString(3));
    }

    @Test
    public void testForDima2026050900115989760() throws SQLException {
        Connection conn = setConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("create or replace procedure proc_1(c1 number, c2 out number) is begin null; end;");
        CallableStatement cs = conn.prepareCall("call proc_1(?, ?)");
        cs.setInt(1, 1);
        cs.registerOutParameter(2, Types.NUMERIC);
        cs.execute();
        cs.close();
        try {
            cs.close();
        } catch (SQLException e) {
            fail("close statement failed");
        }
    }

    @Test
    public void testForDima2026062300116920238() throws SQLException {
        Connection conn = setConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("create or replace package pkg_proc_1 is" +
                " procedure test_proc_920238(p_a varchar2, p_b varchar2, p_d out varchar2); " +
                " procedure test_proc_920238(p_a varchar2, p_b varchar2, p_c number, p_d out varchar2); " +
                " end pkg_proc_1");

        stmt.execute("create or replace package body pkg_proc_1 as " +
                " procedure test_proc_920238(p_a varchar2, p_b varchar2, p_d out varchar2) is begin p_d := p_b;  end; " +
                " procedure test_proc_920238(p_a varchar2, p_b varchar2, p_c number, p_d out varchar2) is begin p_d := p_b || '_p2'; end; " +
                " end pkg_proc_1" );

        CallableStatement call = conn.prepareCall("{call pkg_proc_1.test_proc_920238(?, ?, ?, ?)}");
        call.setString(1, "1");
        call.setString(2, "name");
        call.setInt(3, 10);
        call.registerOutParameter(4, Types.VARCHAR);
        call.execute();
        Assert.assertEquals("name_p2", call.getString(4));

        CallableStatement call2 = conn.prepareCall("{call pkg_proc_1.test_proc_920238(:p_a, :p_b, :p_c, :p_d)}");
        call2.setString("p_a", "1");
        call2.setString("p_b", "name");
        call2.setInt("p_c", 10);
        call2.registerOutParameter("p_d", Types.VARCHAR);
        call2.execute();
        Assert.assertEquals("name_p2", call2.getString("p_d"));

        CallableStatement call3 = conn.prepareCall("{call pkg_proc_1.test_proc_920238(:p_a, :p_b, :p_c, :p_d)}");
        call3.setString("p_b", "name");
        call3.setString("p_a", "1");
        call3.setInt("p_c", 10);
        call3.registerOutParameter("p_d", Types.VARCHAR);
        call3.execute();
        Assert.assertEquals("1_p2", call3.getString("p_d"));

        CallableStatement call4 = conn.prepareCall("{call pkg_proc_1.test_proc_920238(p_a=>?, p_b=>?, p_c=>?, p_d=>?)}");
        call4.setString("p_b", "name");
        call4.setString("p_a", "1");
        call4.setInt("p_c", 10);
        call4.registerOutParameter("p_d", Types.VARCHAR);
        call4.execute();
        Assert.assertEquals("name_p2", call4.getString("p_d"));
    }
 
}
