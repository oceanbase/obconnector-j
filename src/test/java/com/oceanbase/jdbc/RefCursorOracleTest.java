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
 */
package com.oceanbase.jdbc;

import static org.junit.Assert.fail;

import java.sql.*;

import org.junit.Assert;
import org.junit.Test;

public class RefCursorOracleTest extends BaseOracleTest {

    @Test
    public void Fix45605157() throws SQLException {
        Connection conn = setConnection("&serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true");

        Statement s = conn.createStatement();
        createTable("t0", "c0 VARCHAR2(100)  , c1 VARCHAR(100) , c2 INT  NULL");
        //s.execute("CREATE TABLE t0(c0 VARCHAR2(100)  , c1 VARCHAR(100) , c2 INT  NULL);");

        s.execute("insert into t0(c0) values('iq[b4(5p|w');");
        s.execute("insert into t0(c1) values('~jcO7q+j');");
        s.execute("insert into t0(c1) values(595056412);");

        createProcedure(
            "get_result",
            "(v_sql in varchar2, c out sys_refcursor, errMsg out varchar2) is begin errMsg:=''; open c for v_sql; exception   when others then   errMsg := SQLERRM; end;");
        //s.execute("create or replace procedure get_result(v_sql in varchar2, c out sys_refcursor, errMsg out varchar2) is begin errMsg:=''; open c for v_sql; exception   when others then   errMsg := SQLERRM; end;");

        int count = 0;
        CallableStatement cs = conn.prepareCall("{call get_result(?,?,?)}");
        cs.setString(1, "SELECT * FROM T0 WHERE (T0.C1) BETWEEN (CAST(T0.C1 AS int)) AND (T0.C0)");
        //cs.setString(1, "select * from t0 where nullif('a',1)='1'");
        cs.registerOutParameter(2, Types.REF_CURSOR);
        cs.registerOutParameter(3, Types.VARCHAR);
        cs.execute();
        String r = cs.getString(3);
        if (r != null && !r.equals("")) {
            Assert.assertEquals(r, "ORA-01722: invalid number");
        } else {
            ResultSet rs = (ResultSet) cs.getObject(2);
            while (rs.next()) {
                count++;
            }
        }
        Assert.assertEquals(count, 0);
        cs.close();
    }

    @Test
    public void testRefCursorProxyIntOutNull() {
        try {
            Connection conn = sharedConnection;
            String sqla = "create or replace procedure pro1(a int, a2 in out int, a3 in int, b out sys_refcursor) is begin open b for select 1,2,3 from dual; end;";
            conn.createStatement().execute(sqla);
            CallableStatement cstmt = conn.prepareCall("call pro1(?,?,?,?);");
            cstmt.setInt(1, 1);
            cstmt.setInt(2, 1);
            cstmt.setInt(3, 2);
            cstmt.registerOutParameter(2, Types.INTEGER);
            cstmt.registerOutParameter(4, Types.REF_CURSOR);
            cstmt.execute();
            ResultSet rs = (ResultSet) cstmt.getObject(4);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getInt(1));
            Assert.assertEquals(2, rs.getInt(2));
        } catch (SQLException e) {
            e.printStackTrace();
            fail();
        }
    }

}
