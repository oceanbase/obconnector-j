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

public class GBKTestOracle extends BaseOracleTest {
    static String nchartable = "nchartest" + getRandomString(10);
    static String dateTest   = "dateTest" + getRandomString(10);
    static String varchar1   = "varchar1" + getRandomString(10);
    static String varchar2   = "varchar2" + getRandomString(10);
    static String varchar3   = "varchar3" + getRandomString(10);
    static String number1    = "number1" + getRandomString(10);

    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable(nchartable, "c1 NVARCHAR2(150)");
        createTable(dateTest, "c1 nchar(200),c2 nvarchar2(200)");
        createTable(varchar1, "c1 varchar2(200)");
        createTable(varchar2, "c1 varchar2(200)");
        createTable(varchar3, "c1 varchar2(200)");
        createTable(number1, "c1 int");
    }

    @Test
    public void testNcharUncommonChinese() {
        try {
            Connection conn = sharedConnection;
            PreparedStatement pstmt = conn.prepareStatement("insert into " + nchartable
                                                            + " values(?);");
            pstmt.setNString(1, "张\uE863");
            pstmt.execute();
            pstmt.setNString(1, "中文");
            pstmt.execute();
            ResultSet rs = conn.createStatement().executeQuery("select * from " + nchartable);
            Assert.assertTrue(rs.next());
            Assert.assertEquals("张\uE863", rs.getNString(1));
            Assert.assertTrue(rs.next());
            Assert.assertEquals("中文", rs.getNString(1));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void test2() {
        try {
            Statement stmt = sharedConnection.createStatement();
            stmt.execute("insert into " + dateTest + " values('我是123abc','我是abc123')");
            stmt.execute("insert into " + dateTest + " values('我是123abc','我是abc123')");
            ResultSet resultSet = stmt.executeQuery("select * from " + dateTest);
            Assert.assertTrue(resultSet.next());
            Assert.assertTrue(resultSet.getNString(1).contains("我是123abc"));
            Assert.assertTrue(resultSet.getNString(2).contains("我是abc123"));
            Assert.assertTrue(resultSet.next());
            Assert.assertTrue(resultSet.getNString(1).contains("我是123abc"));
            Assert.assertTrue(resultSet.getNString(2).contains("我是abc123"));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    // test for GBK String
    @Test
    public void testCharacterEncodingStatement() {
        try {
            Connection conn = setConnection("&characterEncoding=GBK");
            Statement stmt = conn.createStatement();
            stmt.execute("insert into " + varchar1 + " values('我是123abc')");
            ResultSet resultSet = stmt.executeQuery("select * from " + varchar1);
            Assert.assertTrue(resultSet.next());
            Assert.assertTrue(resultSet.getString(1).contains("我是123abc"));
            PreparedStatement ps = conn.prepareStatement("select * from " + varchar1);
            resultSet = ps.executeQuery();
            Assert.assertTrue(resultSet.next());
            Assert.assertTrue(resultSet.getString(1).contains("我是123abc"));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testCharacterEncodingPrepareStatment() {
        try {
            Connection conn = setConnection("&characterEncoding=GBK");
            Statement stmt = conn.createStatement();
            PreparedStatement ps = conn.prepareStatement("insert into " + varchar2 + " values(?)");
            ps.setString(1, "我是123abc");
            ps.execute();
            ResultSet resultSet = stmt.executeQuery("select * from " + varchar2);
            Assert.assertTrue(resultSet.next());
            Assert.assertTrue(resultSet.getString(1).contains("我是123abc"));
            ps = conn.prepareStatement("select * from " + varchar2);
            resultSet = ps.executeQuery();
            Assert.assertTrue(resultSet.next());
            Assert.assertTrue(resultSet.getString(1).contains("我是123abc"));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testCharacterEncodingPrepareStatment2() {
        try {
            Connection conn = setConnection("&characterEncoding=GBK");
            Statement stmt = conn.createStatement();
            PreparedStatement ps = conn.prepareStatement("insert into " + varchar3
                                                         + " values('我是123abc')");
            ps.execute();
            ResultSet resultSet = stmt.executeQuery("select * from " + varchar3);
            Assert.assertTrue(resultSet.next());
            Assert.assertTrue(resultSet.getString(1).contains("我是123abc"));
            ps = conn.prepareStatement("select * from " + varchar3);
            resultSet = ps.executeQuery();
            Assert.assertTrue(resultSet.next());
            Assert.assertTrue(resultSet.getString(1).contains("我是123abc"));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    // test for GBK NUMBER
    @Test
    public void testCharacterEncodingStatementNumber() {
        try {
            Connection conn = setConnection("&characterEncoding=GBK");
            Statement stmt = conn.createStatement();
            stmt.execute("insert into " + number1 + " values(100)");
            ResultSet resultSet = stmt.executeQuery("select * from " + number1);
            Assert.assertTrue(resultSet.next());
            Assert.assertTrue(resultSet.getString(1).contains("100"));
            PreparedStatement ps = conn.prepareStatement("select * from " + number1);
            resultSet = ps.executeQuery();
            Assert.assertTrue(resultSet.next());
            Assert.assertTrue(resultSet.getString(1).contains("100"));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }
}
