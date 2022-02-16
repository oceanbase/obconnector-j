/**
 *  OceanBase Client for Java
 *
 *  Copyright (c) 2012-2014 Monty Program Ab.
 *  Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *  Copyright (c) 2021 OceanBase.
 *
 *  This library is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU Lesser General Public License as published by the Free
 *  Software Foundation; either version 2.1 of the License, or (at your option)
 *  any later version.
 *
 *  This library is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License along
 *  with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 *  This particular MariaDB Client for Java file is work
 *  derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 *  the following copyright and notice provisions:
 *
 *  Copyright (c) 2009-2011, Marcus Eriksson
 *
 *  Redistribution and use in source and binary forms, with or without modification,
 *  are permitted provided that the following conditions are met:
 *  Redistributions of source code must retain the above copyright notice, this list
 *  of conditions and the following disclaimer.
 *
 *  Redistributions in binary form must reproduce the above copyright notice, this
 *  list of conditions and the following disclaimer in the documentation and/or
 *  other materials provided with the distribution.
 *
 *  Neither the name of the driver nor the names of its contributors may not be
 *  used to endorse or promote products derived from this software without specific
 *  prior written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 *  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 *  IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 *  INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 *  NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 *  PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 *  WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 *  OF SUCH DAMAGE.
 */
package com.oceanbase.jdbc;

import static org.junit.Assert.*;

import java.sql.*;
import java.time.LocalDateTime;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author yexinyan.yxy
 * @version 1.0: OracleCmdPreparedExecuteTest, 2021-09-09 11:10 yexinyan.yxy Exp $
 */
public class OracleCmdPreparedExecuteTest extends BaseOracleTest {

    @Test
    public void normalSqlTest() throws SQLException {
        createTable("test_set_params", "c1 int, c2 varchar(20), c3 Timestamp");

        // not select
        Connection conn = setConnection("&useServerPrepStmts=true&useCursorFetch=true&useOraclePrepareExecute=true");
        PreparedStatement ps1 = conn.prepareStatement("insert into test_set_params values(?,?,?)");

        ps1.setInt(1, 1);
        ps1.setString(2, "abcdefg");
        ps1.setTimestamp(3, Timestamp.valueOf(LocalDateTime.now()));
        assertEquals(1, ps1.executeUpdate());

        // reuse a statement
        ps1.setInt(1, 2);
        ps1.setString(2, "hijklmn");
        ps1.setTimestamp(3, Timestamp.valueOf(LocalDateTime.now()));
        assertEquals(1, ps1.executeUpdate());

        ps1 = conn.prepareStatement("insert into test_set_params values(?,?,?)");
        ps1.setInt(1, 3);
        ps1.setString(2, "opqrst");
        ps1.setTimestamp(3, Timestamp.valueOf(LocalDateTime.now()));
        assertEquals(1, ps1.executeUpdate());

        // select
        PreparedStatement ps2 = conn.prepareStatement("select * from test_set_params where 1=?");
        ps2.setInt(1, 1);
        ResultSet rs = ps2.executeQuery();
        Assert.assertTrue(rs.next());
        Assert.assertEquals(1, rs.getInt(1));
        Assert.assertEquals("abcdefg", rs.getString(2));
        Assert.assertTrue(rs.next());
        Assert.assertEquals(2, rs.getInt(1));
        Assert.assertEquals("hijklmn", rs.getString(2));
        Assert.assertTrue(rs.next());
        Assert.assertEquals(3, rs.getInt(1));
        Assert.assertEquals("opqrst", rs.getString(2));

    }

    @Test
    public void functionAnonymousBlockTest() throws SQLException {
        createFunction("testFunctionCall",
            "(a float, b NUMBER, c int) RETURN INT IS\nBEGIN\n RETURN a + b + c;\nEND;");

        Connection conn = setConnection("&useServerPrepStmts=true&useCursorFetch=true&useOraclePrepareExecute=true");
        CallableStatement cs = conn.prepareCall("begin ? := testFunctionCall(?,?,?);end;");

        cs.registerOutParameter(1, Types.INTEGER);
        cs.setFloat(2, 1);
        cs.setInt(3, 2);
        cs.setInt(4, 3);
        assertFalse(cs.execute());
        assertEquals(6f, cs.getInt(1), .001);

        cs.registerOutParameter(1, Types.INTEGER);
        cs.setFloat(2, 2);
        cs.setInt(3, 3);
        cs.setInt(4, 4);
        assertEquals(0, cs.executeUpdate()); // &useOraclePrepareExecute=false
        assertEquals(9f, cs.getInt(1), .001);

        // reuse a statement, and set by name. java.sql.SQLException: there is no parameter with the name a
        //        cs.registerOutParameter(1, Types.INTEGER);
        //        cs.setFloat("a", 4);//begin: there is no parameter with the name a
        //        cs.setInt("b", 2);
        //        cs.setInt("c", 1);
        //        assertEquals(0, cs.executeUpdate());
        //        assertEquals(7f, cs.getInt(1), .001);

    }

    @Test
    public void functionCallTest() throws SQLException {
        createFunction("testFunctionCall",
            "(a float, b NUMBER, c int) RETURN INT IS\nBEGIN\n RETURN a + b + c;\nEND;");

        Connection conn = setConnection("&useServerPrepStmts=true&useCursorFetch=true&useOraclePrepareExecute=true");
        CallableStatement cs = conn.prepareCall("? = CALL testFunctionCall(?,?,?)");
        //CallableStatement cs = conn.prepareCall("{? = CALL testFunctionCall(?,?,?)}");

        cs.registerOutParameter(1, Types.INTEGER);
        cs.setFloat(2, 1);
        cs.setInt(3, 2);
        cs.setInt(4, 3);
        assertFalse(cs.execute());
        assertEquals(6f, cs.getInt(1), .001);

        cs.registerOutParameter(1, Types.INTEGER);
        cs.setFloat(2, 2);
        cs.setInt(3, 3);
        cs.setInt(4, 4);
        assertEquals(0, cs.executeUpdate()); // &useOraclePrepareExecute=false
        assertEquals(9f, cs.getInt(1), .001);

        // reuse a statement, and set by name
        cs.registerOutParameter(1, Types.INTEGER);
        cs.setFloat("a", 4);
        cs.setInt("b", 2);
        cs.setInt("c", 1);
        assertEquals(0, cs.executeUpdate());
        assertEquals(7f, cs.getInt(1), .001);

    }

    @Test
    public void functionCallToBlockTest() throws SQLException {
        createFunction("testFunctionCall",
            "(a float, b NUMBER, c int) RETURN INT IS\nBEGIN\n RETURN a + b + c;\nEND;");

        Connection conn = setConnection("&useServerPrepStmts=true&useCursorFetch=true&useOraclePrepareExecute=true");
        CallableStatement cs = conn.prepareCall("{? = CALL testFunctionCall(?,?,?)}");

        cs.registerOutParameter(1, Types.INTEGER);
        cs.setFloat(2, 1);
        cs.setInt(3, 2);
        cs.setInt(4, 3);
        assertFalse(cs.execute());
        assertEquals(6f, cs.getInt(1), .001);

        cs.registerOutParameter(1, Types.INTEGER);
        cs.setFloat(2, 2);
        cs.setInt(3, 3);
        cs.setInt(4, 4);
        assertEquals(0, cs.executeUpdate()); // &useOraclePrepareExecute=false
        assertEquals(9f, cs.getInt(1), .001);

        // reuse a statement, and set by name
        cs.registerOutParameter(1, Types.INTEGER);
        cs.setFloat("a", 4);
        cs.setInt("b", 2);
        cs.setInt("c", 1);
        assertEquals(0, cs.executeUpdate());
        assertEquals(7f, cs.getInt(1), .001);

    }

    @Test
    public void procedureCallTest() throws SQLException {
        createProcedureWithPackage("test", "calc_add", "(a1 IN int, a2 IN int, a3 OUT int);end;",
            "(a1 IN int, a2 IN int, a3 OUT int) is\nbegin\n a3 := a1 + a2;\nend;\nend;");

        Connection conn = setConnection("&useServerPrepStmts=true&useCursorFetch=true&useOraclePrepareExecute=true");
        CallableStatement cs = conn.prepareCall("call test.calc_add(?, ?, ?)");
        cs.setInt(1, 1);
        cs.setInt(2, 2);
        cs.registerOutParameter(3, Types.INTEGER);
        assertFalse(cs.execute());
        assertEquals(3, cs.getInt(3));

        // reuse a statement, and set by name
        cs.setInt("a1", 2);
        cs.setInt("a2", 4);
        cs.registerOutParameter("a3", Types.INTEGER);
        assertEquals(0, cs.executeUpdate());
        assertEquals(6, cs.getInt("a3"));

    }
}
