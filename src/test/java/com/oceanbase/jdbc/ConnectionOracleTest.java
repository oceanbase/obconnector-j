/*
 *
 * OceanBase Client for Java
 *
 * Copyright (c) 2024 OceanBase.
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

import com.oceanbase.jdbc.credential.Credential;
import org.junit.Assert;
import org.junit.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class ConnectionOracleTest extends BaseOracleTest{

    @Test
    public void testPoolOption() throws SQLException {
        Connection conn = setConnection("&pool=true");
        PreparedStatement preparedStatement = conn.prepareStatement("select 333 from dual");
        preparedStatement.executeQuery();
        preparedStatement.close();
    }


    @Test
    public void testProxyUser() throws SQLException {
        // user = padmin[admin]@oracle
        Connection connection = sharedConnection;
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("select * from sys.proxy_users");
        rs.next();
        Assert.assertEquals("PADMIN", rs.getString(1));
        Assert.assertEquals("ADMIN", rs.getString(2));
        Assert.assertEquals("NO", rs.getString(3));
        Assert.assertEquals("PROXY MAY ACTIVATE ALL CLIENT ROLES", rs.getString(4));

        ResultSet rs2 = statement.executeQuery("select sys_context('userenv', 'current_user'), sys_context('userenv','proxy_user'), sys_context('userenv','authenticated_identity'), SYS_CONTEXT ('USERENV', 'SESSION_USER')  from dual");
        rs2.next();
        Assert.assertEquals("ADMIN", rs2.getString(1));
        Assert.assertEquals("PADMIN", rs2.getString(2));
        Assert.assertEquals("ADMIN", rs2.getString(3));
        Assert.assertEquals("ADMIN", rs2.getString(4));
    }

    @Test
    public void testForAone56523104() throws SQLException {
        // with useProxyUser
        OceanBaseConnection conn = (OceanBaseConnection) sharedConnection;
        Assert.assertTrue(conn.getProtocol().getOptions().useProxyUser);
        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery("select user from dual");
        rs.next();
        Assert.assertNotEquals(rs.getString(1), conn.getMetaData().getUserName());
    }
}
