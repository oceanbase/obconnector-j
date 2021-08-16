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

import java.sql.*;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class NewFeatureOracleTest extends BaseOracleTest {
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("testcommit", "c1 int");
    }

    @Test
    public void testSocksProxy() {
        try {
            String url = System.getProperty("socksProxyOptions");
            Connection connection = setConnection(url);
            ResultSet resultSet = connection.createStatement().executeQuery("select 1 from dual");
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(1, resultSet.getInt(1));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    // SET SESSION TRANSACTION xxx twice
    @Test
    public void testReadOnly() {
        try {
            boolean isReadOnly = sharedConnection.isReadOnly();
            Assert.assertEquals(isReadOnly, false);
            sharedConnection.setReadOnly(isReadOnly);
            isReadOnly = sharedConnection.isReadOnly();
            Assert.assertEquals(isReadOnly, false);
            sharedConnection.setReadOnly(!isReadOnly);
            isReadOnly = sharedConnection.isReadOnly();
            Assert.assertEquals(isReadOnly, true);
            sharedConnection.setReadOnly(false);
            isReadOnly = sharedConnection.isReadOnly();
            Assert.assertEquals(isReadOnly, false);

        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testAutoCommit() {
        try {
            boolean autoCommit = sharedConnection.getAutoCommit();
            Assert.assertEquals(autoCommit, true);
            sharedConnection.setAutoCommit(false);
            autoCommit = sharedConnection.getAutoCommit();
            Assert.assertEquals(autoCommit, false);
            sharedConnection.setAutoCommit(false);
            autoCommit = sharedConnection.getAutoCommit();
            Assert.assertEquals(autoCommit, false);
            sharedConnection.setAutoCommit(true);
            autoCommit = sharedConnection.getAutoCommit();
            Assert.assertEquals(autoCommit, true);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testTransactionIsolation() {
        try {
            int level = sharedConnection.getTransactionIsolation();
            Assert.assertEquals(2, level);
            sharedConnection.setTransactionIsolation(level);
            level = sharedConnection.getTransactionIsolation();
            Assert.assertEquals(2, level);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testUrlCharsetError() {
        try {
            Connection conn = setConnection("&characterEncoding=UTF-8");
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

}
