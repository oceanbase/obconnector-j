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

import java.sql.*;

import org.junit.Assert;
import org.junit.Test;

public class StatementOracleTest extends BaseOracleTest {

    @Test
    public void testMaxRowsAndLimit() throws SQLException {
        createTable("testMaxRows", "c1 INT, c2 VARCHAR(100)");
        String query = "insert all";
        for (int i = 1; i <= 20; i++) {
            query += " into testMaxRows values(" + i + ",'" + i + "+string')";
        }
        query += "select * from dual";
        Statement st = sharedConnection.createStatement();
        st.execute(query);
        st.close();

        Statement stmt = sharedConnection.createStatement();
        stmt.setMaxRows(5);
        stmt.setFetchSize(3);
        ResultSet rs = stmt.executeQuery("select * from testMaxRows");
        for (int i = 1; i <= 5; i++) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), i);
        }
        try {
            Assert.assertTrue(rs.isLast());
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Invalid operation on TYPE_FORWARD_ONLY ResultSet");
        }
        Assert.assertFalse(rs.next());
        Assert.assertTrue(rs.isAfterLast());

        Connection conn = setConnectionOrigin("?useServerPrepStmts=true");
        PreparedStatement pstmt1 = conn.prepareStatement("select * from testMaxRows");
        pstmt1.setMaxRows(5);
        pstmt1.setFetchSize(3);
        ResultSet rs1 = pstmt1.executeQuery();
        for (int i = 1; i <= 5; i++) {
            Assert.assertTrue(rs1.next());
            Assert.assertEquals(rs1.getInt(1), i);
        }
        try {
            Assert.assertTrue(rs1.isLast());
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Invalid operation on TYPE_FORWARD_ONLY ResultSet");
        }
        Assert.assertFalse(rs1.next());
        Assert.assertTrue(rs1.isAfterLast());

        conn = setConnectionOrigin("?useCursorFetch=true");
        PreparedStatement pstmt2 = conn.prepareStatement("select * from testMaxRows");
        pstmt2.setMaxRows(5);
        pstmt2.setFetchSize(3);
        ResultSet rs2 = pstmt2.executeQuery();
        for (int i = 1; i <= 5; i++) {
            Assert.assertTrue(rs2.next());
            Assert.assertEquals(rs2.getInt(1), i);
        }
        try {
            Assert.assertTrue(rs2.isLast());
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Invalid operation on TYPE_FORWARD_ONLY ResultSet");
        }
        Assert.assertFalse(rs2.next());
        Assert.assertTrue(rs2.isAfterLast());

        conn = setConnectionOrigin("?useCursorFetch=true");
        PreparedStatement pstmt3 = conn.prepareStatement("select * from testMaxRows",
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        pstmt3.setMaxRows(5);
        pstmt3.setFetchSize(3);
        ResultSet rs3 = pstmt3.executeQuery();
        Assert.assertTrue(rs3.absolute(5));
        Assert.assertTrue(rs3.isLast());
        Assert.assertFalse(rs3.next());
        Assert.assertTrue(rs3.isAfterLast());
    }

}
