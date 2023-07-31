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

import static org.junit.Assert.assertEquals;

import java.sql.*;

import org.junit.Assert;
import org.junit.Test;

public class StoredProcedureOracleBasicTest extends BaseOracleTest {
    // todo fix getResultSet error
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
//            ResultSet rs = callableStatement.getResultSet();
//            System.out.println("rs is:" + callableStatement.getDouble(3));
//            if (rs != null && rs.next()) {
//                System.out.println("THE RESULT IS:" + rs.getInt(3));
//            }
            assertEquals(3 == callableStatement.getDouble(3), true);

        } finally {
            dropProcedure(procedureName);
        }
    }

    @Test
    public void testLongStoreProcedureWithFullName() throws Exception {
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
//            System.out.println("rs is:" + callableStatement.getDouble(3));
//            if (rs != null && rs.next()) {
//                System.out.println("THE RESULT IS:" + rs.getInt(3));
//            }
            assertEquals(3 == callableStatement.getDouble(3), true);

        } finally {
            dropProcedure("test." + procedureName);
        }
    }

    @Test
    public void packageAndProcDoubleuotes1() {
        try {
            Connection conn = sharedConnection;
            Statement stmt = conn.createStatement();
            String pkgHead = "CREATE OR REPLACE PACKAGE \"test_pkg\" AS\n"
                             + "FUNCTION \"fun_example\" (p1 IN NUMBER) RETURN NUMBER;\n"
                             + "PROCEDURE \"proc_example\"(p1 IN out NUMBER);\n"
                             + "END \"test_pkg\";";
            String pkgBody = "CREATE OR REPLACE PACKAGE BODY \"test_pkg\" as\n"
                             + "function \"fun_example\" (p1 in number) return number AS\nBEGIN\n"
                             + "  return p1 + 1; \nEND;\n\n"
                             + "procedure \"proc_example\"(p1 in out number) AS\nBEGIN\n "
                             + " p1 := p1 * 2; END;\n\nend \"test_pkg\"";
            stmt.execute(pkgHead);
            stmt.execute(pkgBody);
            CallableStatement callableStatement = conn
                .prepareCall("call \"test_pkg\".\"proc_example\"(?)");
            callableStatement.registerOutParameter(1, Types.INTEGER);
            callableStatement.setInt(1, 10);
            ResultSet rs = callableStatement.executeQuery();
            int a = callableStatement.getInt(1);
            Assert.assertEquals(20, a);
        } catch (Exception e) {
            Assert.fail();
            e.printStackTrace();
        }
    }

    @Test
    public void packageAndProcDoubleuotes2() {
        try {
            Connection conn = sharedConnection;
            Statement stmt = conn.createStatement();
            String pkgHead = "CREATE OR REPLACE PACKAGE test_pkg AS\n"
                             + "FUNCTION \"fun_example\" (p1 IN NUMBER) RETURN NUMBER;\n"
                             + "PROCEDURE \"proc_example\"(p1 IN out NUMBER);\n" + "END test_pkg;";
            String pkgBody = "CREATE OR REPLACE PACKAGE BODY test_pkg as\n"
                             + "function \"fun_example\" (p1 in number) return number AS\nBEGIN\n"
                             + "  return p1 + 1; \nEND;\n\n"
                             + "procedure \"proc_example\"(p1 in out number) AS\nBEGIN\n "
                             + " p1 := p1 * 2; END;\n\nend test_pkg";
            stmt.execute(pkgHead);
            stmt.execute(pkgBody);
            CallableStatement callableStatement = conn
                .prepareCall("call test_pkg.\"proc_example\"(?)");
            callableStatement.registerOutParameter(1, Types.INTEGER);
            callableStatement.setInt(1, 10);
            ResultSet rs = callableStatement.executeQuery();
            int a = callableStatement.getInt(1);
            Assert.assertEquals(20, a);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void packageAndProcDoubleuotes3() {
        try {
            Connection conn = sharedConnection;
            Statement stmt = conn.createStatement();
            String pkgHead = "CREATE OR REPLACE PACKAGE \"test_pkg\" AS\n"
                             + "FUNCTION fun_example (p1 IN NUMBER) RETURN NUMBER;\n"
                             + "PROCEDURE proc_example(p1 IN out NUMBER);\n" + "END \"test_pkg\";";
            String pkgBody = "CREATE OR REPLACE PACKAGE BODY \"test_pkg\" as\n"
                             + "function fun_example (p1 in number) return number AS\nBEGIN\n"
                             + "  return p1 + 1; \nEND;\n\n"
                             + "procedure proc_example(p1 in out number) AS\nBEGIN\n "
                             + " p1 := p1 * 2; END;\n\nend \"test_pkg\"";
            stmt.execute(pkgHead);
            stmt.execute(pkgBody);
            CallableStatement callableStatement = conn
                .prepareCall("call \"test_pkg\".proc_example(?)");
            callableStatement.registerOutParameter(1, Types.INTEGER);
            callableStatement.setInt(1, 10);
            ResultSet rs = callableStatement.executeQuery();
            int a = callableStatement.getInt(1);
            Assert.assertEquals(20, a);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void schemaAndProcDoubleuotes() {
        try {
            Connection conn = sharedConnection;
            Statement stmt = conn.createStatement();
            String pkgBody = "CREATE OR REPLACE procedure \"proc_example\"(p1 in out number) AS\nBEGIN\n "
                             + " p1 := p1 * 2; END;";
            stmt.execute(pkgBody);
            CallableStatement callableStatement = conn
                .prepareCall("call \"TEST\".\"proc_example\"(?)");
            callableStatement.registerOutParameter(1, Types.INTEGER);
            callableStatement.setInt(1, 10);
            ResultSet rs = callableStatement.executeQuery();
            int a = callableStatement.getInt(1);
            Assert.assertEquals(20, a);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void schemaAndProcDoubleuotes2() {
        try {
            Connection conn = sharedConnection;
            Statement stmt = conn.createStatement();
            String pkgBody = "CREATE OR REPLACE procedure \"proc_example\"(p1 in out number) AS\nBEGIN\n "
                             + " p1 := p1 * 2; END;";
            stmt.execute(pkgBody);
            CallableStatement callableStatement = conn
                .prepareCall("call TEST.\"proc_example\"(?)");
            callableStatement.registerOutParameter(1, Types.INTEGER);
            callableStatement.setInt(1, 10);
            ResultSet rs = callableStatement.executeQuery();
            int a = callableStatement.getInt(1);
            Assert.assertEquals(20, a);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void schemaAndProcDoubleuotes3() {
        try {
            Connection conn = sharedConnection;
            Statement stmt = conn.createStatement();
            String pkgBody = "CREATE OR REPLACE procedure proc_example(p1 in out number) AS\nBEGIN\n "
                             + " p1 := p1 * 2; END;";
            stmt.execute(pkgBody);
            CallableStatement callableStatement = conn
                .prepareCall("call TEST.proc_example(?)");
            callableStatement.registerOutParameter(1, Types.INTEGER);
            callableStatement.setInt(1, 10);
            ResultSet rs = callableStatement.executeQuery();
            int a = callableStatement.getInt(1);
            Assert.assertEquals(20, a);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void schemaAndPackageAndProc1() {
        try {
            Connection conn = sharedConnection;
            Statement stmt = conn.createStatement();
            String pkgHead = "CREATE OR REPLACE PACKAGE \"test_pkg\" AS\n"
                             + "FUNCTION \"fun_example\" (p1 IN NUMBER) RETURN NUMBER;\n"
                             + "PROCEDURE \"proc_example\"(p1 IN out NUMBER);\n"
                             + "END \"test_pkg\";";
            String pkgBody = "CREATE OR REPLACE PACKAGE BODY \"test_pkg\" as\n"
                             + "function \"fun_example\" (p1 in number) return number AS\nBEGIN\n"
                             + "  return p1 + 1; \nEND;\n\n"
                             + "procedure \"proc_example\"(p1 in out number) AS\nBEGIN\n "
                             + " p1 := p1 * 2; END;\n\nend \"test_pkg\"";
            stmt.execute(pkgHead);
            stmt.execute(pkgBody);
            CallableStatement callableStatement = conn
                .prepareCall("call \"TEST\".\"test_pkg\".\"proc_example\"(?)");
            callableStatement.registerOutParameter(1, Types.INTEGER);
            callableStatement.setInt(1, 10);
            ResultSet rs = callableStatement.executeQuery();
            int a = callableStatement.getInt(1);
            Assert.assertEquals(20, a);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void schemaAndPackageAndProc2() {
        try {
            Connection conn = sharedConnection;
            Statement stmt = conn.createStatement();
            String pkgHead = "CREATE OR REPLACE PACKAGE \"test_pkg\" AS\n"
                             + "FUNCTION \"fun_example\" (p1 IN NUMBER) RETURN NUMBER;\n"
                             + "PROCEDURE \"proc_example\"(p1 IN out NUMBER);\n"
                             + "END \"test_pkg\";";
            String pkgBody = "CREATE OR REPLACE PACKAGE BODY \"test_pkg\" as\n"
                             + "function \"fun_example\" (p1 in number) return number AS\nBEGIN\n"
                             + "  return p1 + 1; \nEND;\n\n"
                             + "procedure \"proc_example\"(p1 in out number) AS\nBEGIN\n "
                             + " p1 := p1 * 2; END;\n\nend \"test_pkg\"";
            stmt.execute(pkgHead);
            stmt.execute(pkgBody);
            CallableStatement callableStatement = conn
                .prepareCall("call TEST.\"test_pkg\".\"proc_example\"(?)");
            callableStatement.registerOutParameter(1, Types.INTEGER);
            callableStatement.setInt(1, 10);
            ResultSet rs = callableStatement.executeQuery();
            int a = callableStatement.getInt(1);
            Assert.assertEquals(20, a);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void schemaAndPackageAndProc3() {
        try {
            Connection conn = sharedConnection;
            Statement stmt = conn.createStatement();
            String pkgHead = "CREATE OR REPLACE PACKAGE test_pkg AS\n"
                             + "FUNCTION \"fun_example\" (p1 IN NUMBER) RETURN NUMBER;\n"
                             + "PROCEDURE \"proc_example\"(p1 IN out NUMBER);\n"
                             + "END \"test_pkg\";";
            String pkgBody = "CREATE OR REPLACE PACKAGE BODY test_pkg as\n"
                             + "function \"fun_example\" (p1 in number) return number AS\nBEGIN\n"
                             + "  return p1 + 1; \nEND;\n\n"
                             + "procedure \"proc_example\"(p1 in out number) AS\nBEGIN\n "
                             + " p1 := p1 * 2; END;\n\nend test_pkg";
            stmt.execute(pkgHead);
            stmt.execute(pkgBody);
            CallableStatement callableStatement = conn
                .prepareCall("call TEST.test_pkg.\"proc_example\"(?)");
            callableStatement.registerOutParameter(1, Types.INTEGER);
            callableStatement.setInt(1, 10);
            ResultSet rs = callableStatement.executeQuery();
            int a = callableStatement.getInt(1);
            Assert.assertEquals(20, a);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void schemaAndPackageAndProc4() {
        try {
            Connection conn = sharedConnection;
            Statement stmt = conn.createStatement();
            String pkgHead = "CREATE OR REPLACE PACKAGE test_pkg AS\n"
                             + "FUNCTION \"fun_example\" (p1 IN NUMBER) RETURN NUMBER;\n"
                             + "PROCEDURE proc_example(p1 IN out NUMBER);\n" + "END \"test_pkg\";";
            String pkgBody = "CREATE OR REPLACE PACKAGE BODY test_pkg as\n"
                             + "function \"fun_example\" (p1 in number) return number AS\nBEGIN\n"
                             + "  return p1 + 1; \nEND;\n\n"
                             + "procedure proc_example(p1 in out number) AS\nBEGIN\n "
                             + " p1 := p1 * 2; END;\n\nend test_pkg";
            stmt.execute(pkgHead);
            stmt.execute(pkgBody);
            CallableStatement callableStatement = conn
                .prepareCall("call TEST.test_pkg.proc_example(?)");
            callableStatement.registerOutParameter(1, Types.INTEGER);
            callableStatement.setInt(1, 10);
            ResultSet rs = callableStatement.executeQuery();
            int a = callableStatement.getInt(1);
            Assert.assertEquals(20, a);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void schemaAndPackageAndProc5() {
        try {
            Connection conn = sharedConnection;
            Statement stmt = conn.createStatement();
            String pkgHead = "CREATE OR REPLACE PACKAGE \"test_pkg\" AS\n"
                             + "FUNCTION \"fun_example\" (p1 IN NUMBER) RETURN NUMBER;\n"
                             + "PROCEDURE proc_example(p1 IN out NUMBER);\n" + "END \"test_pkg\";";
            String pkgBody = "CREATE OR REPLACE PACKAGE BODY \"test_pkg\" as\n"
                             + "function \"fun_example\" (p1 in number) return number AS\nBEGIN\n"
                             + "  return p1 + 1; \nEND;\n\n"
                             + "procedure proc_example(p1 in out number) AS\nBEGIN\n "
                             + " p1 := p1 * 2; END;\n\nend \"test_pkg\"";
            stmt.execute(pkgHead);
            stmt.execute(pkgBody);
            CallableStatement callableStatement = conn
                .prepareCall("call TEST.\"test_pkg\".proc_example(?)");
            callableStatement.registerOutParameter(1, Types.INTEGER);
            callableStatement.setInt(1, 10);
            ResultSet rs = callableStatement.executeQuery();
            int a = callableStatement.getInt(1);
            Assert.assertEquals(20, a);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void odcProcedureCommandTest1() {
        try {
            Connection conn = sharedConnection;
            conn.createStatement().execute(
                "create or replace procedure pl_test (p1 in int, p2 in int, p3 in out int) is begin \n"
                        + "p3 := p1 + p2;" + "end ;");
            System.out.println("index  binding");
            CallableStatement callableStatement = conn
                .prepareCall("call /* comment test */ \npl_test\n\n(?,?,?)");
            callableStatement.setInt(1, 1);
            callableStatement.setInt(2, 2);
            callableStatement.setInt(3, 3);
            callableStatement.registerOutParameter(3, Types.INTEGER);
            ResultSet rs = callableStatement.executeQuery();
            int a = callableStatement.getInt(3);
            System.out.println("a = " + a);
            System.out.println("name binding");
            callableStatement.setInt("p1", 1);
            callableStatement.setInt("p2", 2);
            callableStatement.setInt("p3", 3);
            callableStatement.registerOutParameter("p3", Types.INTEGER);
            rs = callableStatement.executeQuery();
            a = callableStatement.getInt("p3");
            System.out.println("a = " + a);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void odcProcedureCommandTest2() {
        try {
            Connection conn = sharedConnection;
            conn.createStatement().execute(
                "create or replace procedure pl_t (listing in out varchar2) is begin \n"
                        + "listing := 'abc';" + "end pl_t;");
            System.out.println("index binding");
            CallableStatement callableStatement = conn.prepareCall("call \nPL_T\n(?\n)");
            callableStatement.setString(1, "woshilize");
            callableStatement.registerOutParameter(1, Types.VARCHAR);
            ResultSet rs = callableStatement.executeQuery();
            String a = callableStatement.getString(1);
            System.out.println("a = " + a);

            System.out.println("index binding");
            callableStatement = conn.prepareCall("call PL_T(?)");
            callableStatement.setString("listing", "woshilize");
            callableStatement.registerOutParameter("listing", Types.VARCHAR);
            rs = callableStatement.executeQuery();
            a = callableStatement.getString("listing");
            System.out.println("a = " + a);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void odcFcuntionCommandTest1() {
        try {
            Connection conn = sharedConnection;
            try {
                conn.createStatement().execute("drop function func_test");
            } catch (SQLException e) {
                //
            }
            conn.createStatement().execute(
                "create or replace function func_test(p1 int, p2 int) return int as v_result int; begin\n"
                        + " return p1+p2;end;");

            System.out.println("index binding");
            CallableStatement callableStatement = conn.prepareCall("{? = call \nfunc_test\n(?,?)}");
            callableStatement.registerOutParameter(1, Types.INTEGER);
            callableStatement.setInt(2, 10);
            callableStatement.setInt(3, 10);
            callableStatement.execute();
            Assert.assertEquals(20, callableStatement.getInt(1));

            System.out.println("name binding");
            callableStatement = conn.prepareCall("{? = call func_test(?,?)}");
            callableStatement.registerOutParameter(1, Types.INTEGER);
            callableStatement.setInt("p1", 10);
            callableStatement.setInt("p2", 10);
            try {
                callableStatement.execute();
                Assert.fail();
                //Assert.assertEquals(20, callableStatement.getInt(1));
            } catch (SQLException e) {
                e.printStackTrace();
                assertEquals(
                    "The number of parameter names '2' does not match the number of registered parameters in sql '3'.",
                    e.getMessage());
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void odcFcuntionCommandTest2() {
        try {
            Connection conn = sharedConnection;
            try {
                conn.createStatement().execute("drop function func_test2");
            } catch (SQLException e) {
                //
            }
            conn.createStatement()
                .execute(
                    "create or replace function func_test2\n(p1 int, p2 int, total out int) return int as v_result int; begin\n"
                            + " total := p1+p2; return total; end;");

            System.out.println("index binding");
            CallableStatement callableStatement = conn
                .prepareCall("{? = call \nfunc_test2\n(?,?,?)}");
            callableStatement.registerOutParameter(1, Types.INTEGER);
            callableStatement.registerOutParameter(4, Types.INTEGER);
            callableStatement.setInt(2, 10);
            callableStatement.setInt(3, 10);
            callableStatement.execute();
            Assert.assertEquals(20, callableStatement.getInt(1));

            System.out.println("name binding");
            callableStatement = conn.prepareCall("{? = call func_test2(?,?,?)}");
            callableStatement.registerOutParameter(1, Types.INTEGER);
            callableStatement.registerOutParameter(4, Types.INTEGER);
            callableStatement.setInt("p1", 10);
            callableStatement.setInt("p2", 10);
            try {
                callableStatement.execute();
                Assert.fail();
                //Assert.assertEquals(20, callableStatement.getInt(1));
            } catch (SQLException e) {
                e.printStackTrace();
                assertEquals(
                    "The number of parameter names '2' does not match the number of registered parameters in sql '4'.",
                    e.getMessage());
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void odcFcuntionCommandTest3() {
        try {
            Connection conn = sharedConnection;
            try {
                conn.createStatement().execute("drop function func_test3");
            } catch (SQLException e) {
                //
            }
            conn.createStatement()
                .execute(
                    "create or replace function func_test3\n(p1 int, p2 int, total out int) return int as v_result int; begin\n"
                            + " total := p1+p2; return total; end;");

            CallableStatement callableStatement = conn
                .prepareCall("{? = call \nfunc_test3\n(?,?,?)}");
            callableStatement.registerOutParameter(1, Types.INTEGER);
            callableStatement.registerOutParameter(4, Types.INTEGER);
            callableStatement.setInt(2, 10);
            callableStatement.setInt(3, 10);
            callableStatement.execute();
            Assert.assertEquals(20, callableStatement.getInt(1));

            callableStatement = conn.prepareCall("{? = call func_test3(?,?,?)}");
            callableStatement.registerOutParameter(1, Types.INTEGER);
            callableStatement.registerOutParameter(4, Types.INTEGER);
            callableStatement.setInt("p1", 10);
            callableStatement.setInt("p2", 10);
            try {
                callableStatement.execute();
                Assert.fail();
                //Assert.assertEquals(20, callableStatement.getInt(1));
            } catch (SQLException e) {
                assertEquals(
                    "The number of parameter names '2' does not match the number of registered parameters in sql '4'.",
                    e.getMessage());
            }

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void functionWithOutputParams() {
        try {
            Connection conn = sharedConnection;
            try {
                conn.createStatement().execute("drop function func_test_4");
            } catch (SQLException e) {
                //
            }
            conn.createStatement()
                .execute(
                    "create or replace function func_test_4(p1 in varchar2, p2 out varchar2, p3 out varchar2) return  NUMBER as ret NUMBER; begin\n"
                            + " p2 := '44';" + " p3 := '33';" + "return 250; end;");
            CallableStatement callableStatement = conn.prepareCall("{? = call func_test_4(?,?,?)}");

            callableStatement.registerOutParameter(1, Types.NUMERIC);
            callableStatement.setString(2, "xxx");
            callableStatement.registerOutParameter(3, Types.VARCHAR);
            callableStatement.registerOutParameter(4, Types.VARCHAR);
            callableStatement.execute();
            int retVal = callableStatement.getInt(1);
            Assert.assertEquals(retVal, 250);
            String p2 = callableStatement.getString(3);
            Assert.assertEquals("44", p2);
            String p3 = callableStatement.getString(4);
            Assert.assertEquals("33", p3);
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void prepareCallWithNormalSql() {
        try {
            Connection conn = sharedConnection;
            CallableStatement callableStatement = null;
            try {
                callableStatement = conn.prepareCall("drop table tabletest_normal_sql");
                callableStatement.execute();
            } catch (SQLException e) {
                //                e.printStackTrace();
            }
            callableStatement = conn
                .prepareCall("create table tabletest_normal_sql(c1 int,c2 varchar2(200))");
            callableStatement.execute();
            callableStatement = conn.prepareCall("insert into tabletest_normal_sql values(?,?)");
            callableStatement.setInt(1, 1);
            callableStatement.setString(2, "string_val");
            callableStatement.execute();
            callableStatement = conn.prepareCall("select * from tabletest_normal_sql");
            ResultSet rs = callableStatement.executeQuery();
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getInt(1));
            Assert.assertEquals("string_val", rs.getString(2));

        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void prepareCallWithWrongSql() {
        try {
            Connection conn = sharedConnection;
            CallableStatement callableStatement = null;
            try {
                callableStatement = conn.prepareCall("drop table tabletest_wrong_sql");
                callableStatement.execute();
            } catch (SQLException e) {
                //                e.printStackTrace();
            }
            callableStatement = conn
                .prepareCall("create table tabletest_wrong_sql(c1 int,c2 varchar2(200))");
            callableStatement.execute();
            callableStatement = conn
                .prepareCall("insert into tabletest_wrong_sql values(:name2,:name2) :name3");
            callableStatement.setInt(1, 1);
            callableStatement.setString(2, "string_val");
            callableStatement.execute();
            Assert.fail(); // The program cannot be executed to this code segment
        } catch (Throwable e) {
            Assert
                .assertTrue(e
                    .getMessage()
                    .contains(
                        "You have an error in your SQL syntax; check the manual that corresponds to your OceanBase version for the right syntax to use near '?' at line 1"));
        }
    }

    @Test
    public void prepareCalllAnonymousBlockWithName() {
        try {
            Connection conn = sharedConnection;
            CallableStatement callableStatement = null;
            try {
                callableStatement = conn.prepareCall("drop table result_table");
                callableStatement.execute();
            } catch (SQLException e) {
                //                e.printStackTrace();
            }
            callableStatement = conn
                .prepareCall("create table result_table(c1 varchar2(200),c2 number)");
            callableStatement.execute();
            String sql = "DECLARE\n" + "  V_TABLENAME      VARCHAR2(20);\n"
                         + "  V_NUMBER         NUMBER;\n" + "BEGIN\n"
                         + "  V_TABLENAME     := :V_TABLENAME ;\n"
                         + "  V_NUMBER     := to_number(:NUMBER1) ;\n"
                         + "  insert into result_table values(V_TABLENAME,V_NUMBER);\n" + "\n"
                         + "end ; ";
            callableStatement = conn.prepareCall(sql);
            callableStatement.setString(1, "test_val");
            callableStatement.setInt(2, 100);
            callableStatement.execute();
            callableStatement = conn.prepareCall("select * from result_table");
            ResultSet rs = callableStatement.executeQuery();
            Assert.assertTrue(rs.next());
            Assert.assertEquals(100, rs.getInt(2));
            Assert.assertEquals("test_val", rs.getString(1));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void prepareAnonymousBlockWithName() {
        try {
            Connection conn = sharedConnection;
            CallableStatement callableStatement = null;
            try {
                callableStatement = conn.prepareCall("drop table result_table");
                callableStatement.execute();
            } catch (SQLException e) {
                //                e.printStackTrace();
            }
            callableStatement = conn
                .prepareCall("create table result_table(c1 varchar2(200),c2 number)");
            callableStatement.execute();
            String sql = "DECLARE\n" + "  V_TABLENAME      VARCHAR2(20);\n"
                         + "  V_NUMBER         NUMBER;\n" + "BEGIN\n"
                         + "  V_TABLENAME     := :V_TABLENAME ;\n"
                         + "  V_NUMBER     := to_number(:NUMBER1) ;\n"
                         + "  insert into result_table values(V_TABLENAME,V_NUMBER);\n" + "\n"
                         + "end ; ";
            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            preparedStatement.setString(1, "test_val");
            preparedStatement.setInt(2, 100);
            preparedStatement.execute();
            preparedStatement = conn.prepareStatement("select * from result_table");
            ResultSet rs = preparedStatement.executeQuery();
            Assert.assertTrue(rs.next());
            Assert.assertEquals(100, rs.getInt(2));
            Assert.assertEquals("test_val", rs.getString(1));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void prepareAnonymousBlockWithName2() {
        try {
            Connection conn = sharedConnection;
            CallableStatement callableStatement = null;
            try {
                callableStatement = conn.prepareCall("drop table result_table2");
                callableStatement.execute();
            } catch (SQLException e) {
                //                e.printStackTrace();
            }
            callableStatement = conn
                .prepareCall("create table result_table2(c1 varchar2(200),c2 number)");
            callableStatement.execute();
            String sql = "DECLARE\n" + "  V_FREEVOL      NUMBER;\n" + "  V_USAGEVOL     NUMBER;\n"
                         + "  V_OVERVOL      NUMBER;\n" + "  V_TARIFFPLAN   NUMBER;\n"
                         + "  V_ENDTIME      VARCHAR2(20);\n" + "  V_ATTRADD      VARCHAR2(20);\n"
                         + "BEGIN\n" + "  V_FREEVOL      := to_number(:FREEVOL );\n"
                         + "  V_USAGEVOL     := to_number(:USAGEVOL );\n"
                         + "  V_OVERVOL      := to_number(:OVERVOL );\n"
                         + "  V_TARIFFPLAN   := to_number(:TARIFFPLAN );\n"
                         + "  V_ENDTIME      := :ENDTIME ;\n" + "  V_ATTRADD      := :ATTRADD ;\n"
                         + "\n" + "  insert into result_table2 values(V_ENDTIME,V_FREEVOL);\n"
                         + "\n" + "end ;\n";
            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            preparedStatement.setString(5, "test_val_5");
            preparedStatement.setString(6, "test_val_6");
            preparedStatement.setInt(1, 100);
            preparedStatement.setInt(2, 200);
            preparedStatement.setInt(3, 300);
            preparedStatement.setInt(4, 400);
            preparedStatement.execute();
            preparedStatement = conn.prepareStatement("select * from result_table2");
            ResultSet rs = preparedStatement.executeQuery();
            Assert.assertTrue(rs.next());
            Assert.assertEquals(100, rs.getInt(2));
            Assert.assertEquals("test_val_5", rs.getString(1));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testDBPackageFunction1() throws SQLException {
        String head = "create or replace package t_pack4 as a int;"
                      + "function t_fun1 (a in int, b in int ,c in int) return int; end;";
        String body = "create or replace package body t_pack4 as "
                      + "function t_fun1 (a in int, b in int ,c in int) return int as a_result int ;begin "
                      + "return a + b + c; end; end;";
        Statement statement = sharedConnection.createStatement();
        try {
            statement.execute("drop package t_pack4");
        } catch (SQLException e) {
            //            e.printStackTrace();
        }
        statement.execute(head);
        statement.execute(body);

        CallableStatement cs = sharedConnection.prepareCall("{? = call t_pack4.t_fun1(?,?,?)}");
        cs.registerOutParameter(1, Types.INTEGER);
        cs.setInt(2, 1);
        cs.setInt(3, 1);
        cs.setInt(4, 1);
        cs.execute();
        assertEquals(3, cs.getInt(1));
    }

}
