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

public class CursorFetchOracleTest extends BaseOracleTest {
    static String tableName1 = "test_cursor_oracle";
    static String tableName2 = "test_scrollable_cursor_oracle";

    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable(tableName1, "c1 int,c2 varchar(30)");
        createTable(tableName2, "c1 int,c2 varchar(30)");
    }

    @Test
    public void testCursorFetch() {
        try {
            Connection conn = setConnection("&useCursorFetch=true");
            Statement statement = conn.createStatement();
            int i;
            String query = "insert into " + tableName1 + " values(1,'1+string')";
            for (i = 2; i < 100; i++) {
                query += ",(" + i + ",'" + i + "+string')";
            }
            statement.execute(query);
            statement.setFetchSize(50);
            ResultSet rs = statement.executeQuery("select * from " + tableName1 + "");
            int count = 1;
            int j = 1;
            while (rs.next()) {
                Assert.assertEquals(j, rs.getInt(1));
                Assert.assertEquals(j + "+string", rs.getString(2));
                j++;
                count++;
            }
            Assert.assertEquals(count, 100);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testScrollableCursor() throws Exception {
        try {
            Connection conn = setConnection("&useServerPrepStmts=true&useCursorFetch=true");

            String query = "insert all";
            for (int i = 1; i <= 100; i++) {
                query += " into " + tableName2 + " values(" + i + ",'" + i + "+string')";
            }
            query += "select * from dual";
            Statement stmt = conn.createStatement();
            stmt.execute(query);

            PreparedStatement ps = conn.prepareStatement("select * from " + tableName2,
                    ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            ps.setFetchSize(40);
            ResultSet rs = ps.executeQuery();

            Assert.assertTrue(rs.next());
            Assert.assertFalse(rs.previous());

            Assert.assertTrue(rs.absolute(39));
            Assert.assertEquals(39, rs.getInt(1));
            Assert.assertTrue(rs.relative(2));
            Assert.assertEquals(41, rs.getInt(1));
            Assert.assertTrue(rs.last());
            Assert.assertEquals(100, rs.getInt(1));
            Assert.assertTrue(rs.previous());
            Assert.assertEquals(99, rs.getInt(1));
            Assert.assertTrue(rs.first());
            Assert.assertEquals(1, rs.getInt(1));

            Assert.assertFalse(rs.relative(-3));
            Assert.assertTrue(rs.isBeforeFirst());
            rs.afterLast();
            Assert.assertTrue(rs.isAfterLast());
            rs.beforeFirst();
            Assert.assertTrue(rs.isBeforeFirst());

            stmt.close();
            ps.close();
            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }
}
