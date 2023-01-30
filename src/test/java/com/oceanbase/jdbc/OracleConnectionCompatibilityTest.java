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

import static org.junit.Assert.assertEquals;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class OracleConnectionCompatibilityTest extends BaseOracleTest {

    @BeforeClass
    public static void initClass() throws SQLException {
        createTable("oracle_conn__test", "id int primary key");
    }

    @Before
    public void checkSupported() throws SQLException {
        requireMinimumVersion(5, 1);
    }

    @Test
    public void testAutoCommitAndRollback() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        sharedConnection.setAutoCommit(true);
        stmt.execute("delete from oracle_conn__test;");
        assertEquals(true, sharedConnection.getAutoCommit());
        sharedConnection.setAutoCommit(false);
        assertEquals(false, sharedConnection.getAutoCommit());
        stmt.execute("insert into oracle_conn__test values(10);");
        sharedConnection.rollback();
        stmt.execute("insert into oracle_conn__test values(11);");
        sharedConnection.commit();
        ResultSet rs = stmt.executeQuery("select * from oracle_conn__test;");
        assertEquals(true, rs.next());
        assertEquals(false, rs.next());

    }

    @Test
    public void testTransactionIsolation() throws SQLException {
        boolean support = true;

        assertEquals(2, sharedConnection.getTransactionIsolation());

        try {
            sharedConnection.setTransactionIsolation(Connection.TRANSACTION_NONE);
        } catch (SQLException e) {
            support = false;
        }

        assertEquals(false, support);

        assertEquals(Connection.TRANSACTION_READ_COMMITTED,
            sharedConnection.getTransactionIsolation());

        support = true;
        try {
            sharedConnection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        } catch (SQLException e) {
            support = false;
        }
        assertEquals(false, support);
        assertEquals(Connection.TRANSACTION_READ_COMMITTED,
            sharedConnection.getTransactionIsolation());

        support = true;
        try {
            sharedConnection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        } catch (SQLException e) {
            support = false;
        }
        assertEquals(false, support);
        assertEquals(Connection.TRANSACTION_READ_COMMITTED,
            sharedConnection.getTransactionIsolation());

        support = true;
        try {
            sharedConnection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        } catch (SQLException e) {
            support = false;
        }
        assertEquals(true, support);
        assertEquals(Connection.TRANSACTION_SERIALIZABLE,
            sharedConnection.getTransactionIsolation());
    }

    @Test
    public void testSavePoint() throws SQLException {
        Statement stmt = sharedConnection.createStatement();

        boolean succeed = true;
        try {
            sharedConnection.setSavepoint();
        } catch (SQLException e) {
            //ignore , autoCommit is true , setSavepoint Is bound to fail
            succeed = false;
        }
        assertEquals(false, succeed);

        sharedConnection.setAutoCommit(false);
        stmt.execute("insert into oracle_conn__test values(0);");
        Savepoint savepoint = sharedConnection.setSavepoint();
        stmt.execute("insert into oracle_conn__test values(1);");
        sharedConnection.rollback(savepoint);
        sharedConnection.commit();
        ResultSet rs = stmt.executeQuery("select * from oracle_conn__test");
        assertEquals(true, rs.next());
        assertEquals(false, rs.next());
    }

    @Test
    public void testTypeMap() throws SQLException {
        HashMap map = new HashMap<String, Class<?>>();
        map.put("1", Integer.class);
        map.put("2", Double.class);
        map.put("3", Float.class);

        sharedConnection.setTypeMap(map);
        Map<String, Class<?>> typeMap = sharedConnection.getTypeMap();
        assertEquals(map.size(),typeMap.size());
        typeMap.keySet().forEach(s -> assertEquals(typeMap.get(s),map.get(s)));
    }

    @Test
    public void testCatalog() throws SQLException {

        //In Oracle, return null here
        assertEquals(null, sharedConnection.getCatalog());

        //In Oracle,This is not implemented
        sharedConnection.setCatalog("test");
        assertEquals(null, sharedConnection.getCatalog());
    }
}
