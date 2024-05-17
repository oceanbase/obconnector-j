/*
 *
 * OceanBase Client for Java
 *
 * Copyright (c) 2021 OceanBase Technology Co.,Ltd.
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

public class OracleNcharNvarcharTest extends BaseOracleTest {

    @Test
    public void test1() throws SQLException {
        String dateTest = "dateTest" + getRandomString(10);
        createTable(dateTest, "c1 nchar(200),c2 nvarchar2(200)");
        try {
            Statement stmt = sharedConnection.createStatement();
            stmt.execute("insert into " + dateTest + " values('我是123abc','我是abc123')");
            stmt.execute("insert into " + dateTest + " values('我是123abc','我是abc123')");
            ResultSet resultSet = stmt.executeQuery("select * from " + dateTest);
            Assert.assertTrue(resultSet.next());
            // return value length  of  getNString  is 200
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

    @Test
    public void fix49372928() throws Exception {
        createTable("test_nv", "c_var varchar2(128), n_var nvarchar2(128), n_char nchar(20)");

        //        setNStringGBKFunction(setConnection("&characterEncoding=gbk"));
        //        setNStringGBKFunction(setConnection("&characterEncoding=gbk&nCharacterEncoding=utf8"));
        //        setNStringGBKFunction(setConnection("&characterEncoding=gbk&nCharacterEncoding=utf16"));

        testCore(setConnection("&characterEncoding=gbk"));
        testCore(setConnection("&characterEncoding=gbk&nCharacterEncoding=utf8"));
        testCore(setConnection("&characterEncoding=gbk&nCharacterEncoding=utf16"));
    }

    private void setNStringGBKFunction(Connection conn) throws Exception {
        createTable("test_nv", "c_value varchar2(128), n_value nvarchar2(128)");
        Statement stmt = conn.createStatement();

        //        ResultSet rs = stmt.executeQuery("SELECT * FROM nls_database_parameters WHERE parameter IN ('NLS_CHARACTERSET', 'NLS_NCHAR_CHARACTERSET', 'NLS_LANG', 'NLS_LANGUAGE')");
        //        while (rs.next()) {
        //            System.out.println(rs.getString(1) + " = " + rs.getString(2));
        //        }
        //        rs = stmt.executeQuery("select userenv('language') from dual");
        //        while (rs.next()) {
        //            System.out.println("userenv('language') = " + rs.getString(1));
        //        }

        //        stmt.execute("insert into test_nv values (utl_raw.CAST_TO_NVARCHAR2('3400'), '北')");
        //        printRsAndResSting(stmt);
        //        stmt.execute("select utl_raw.CAST_TO_NVARCHAR2('3400') from dual");

        PreparedStatement pstmt = conn.prepareStatement("insert into test_nv values (?, ?)");
        pstmt.setNString(1, "㐀");
        pstmt.setNString(2, "㐀");
        pstmt.execute();
        printRsAndResSting(stmt);

        pstmt.setString(1, "㐀");
        pstmt.setString(2, "㐀");
        pstmt.execute();
        printRsAndResSting(stmt);

        stmt.execute("insert into test_nv values ('㐀', '㐀')");
        printRsAndResSting(stmt);
        stmt.execute("insert into test_nv values (N'㐀', N'㐀')");
        printRsAndResSting(stmt);

        pstmt.setNString(1, "丂");
        pstmt.setNString(2, "丂");
        pstmt.execute();
        printRsAndResSting(stmt);

        pstmt.setString(1, "丂");
        pstmt.setString(2, "丂");
        pstmt.execute();
        printRsAndResSting(stmt);

        stmt.execute("insert into test_nv values ('丂', '丂')");
        printRsAndResSting(stmt);
        stmt.execute("insert into test_nv values (N'丂', N'丂')");
        printRsAndResSting(stmt);

        pstmt.setNString(1, "㐀丂");
        pstmt.setNString(2, "㐀丂");
        pstmt.execute();
        printRsAndResSting(stmt);

        pstmt.setString(1, "㐀丂");
        pstmt.setString(2, "㐀丂");
        pstmt.execute();
        printRsAndResSting(stmt);

        stmt.execute("insert into test_nv values ('㐀丂', '㐀丂')");
        printRsAndResSting(stmt);
        stmt.execute("insert into test_nv values (N'㐀丂', N'㐀丂')");
        printRsAndResSting(stmt);
    }

    private void printRsAndResSting(Statement stmt) throws SQLException {
        ResultSet rs = stmt.executeQuery("select * from test_nv");
        rs.next();
        System.out.println("varchar2---" + rs.getNString(1) + ", nvarchar2---" + rs.getNString(2));
        stmt.execute("delete from test_nv");
    }

    private void testCore(Connection conn) throws SQLException {
        PreparedStatement pstmt = conn.prepareStatement("insert into test_nv values (?, ?, ?)");
        pstmt.setNString(1, "㐀");
        pstmt.setNString(2, "㐀");
        pstmt.setNString(3, "㐀");
        pstmt.execute();

        pstmt.setNString(1, "丂");
        pstmt.setNString(2, "丂");
        pstmt.setNString(3, "丂");
        pstmt.execute();

        pstmt.setNString(1, "㐀丂");
        pstmt.setNString(2, "㐀丂");
        pstmt.setNString(3, "㐀丂");
        pstmt.execute();

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select * from test_nv");
        Assert.assertTrue(rs.next());
        Assert.assertEquals("?", rs.getNString(1));
        Assert.assertEquals("㐀", rs.getNString(2));
        Assert.assertEquals("㐀                   ", rs.getNString(3));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("丂", rs.getNString(1));
        Assert.assertEquals("丂", rs.getNString(2));
        Assert.assertEquals("丂                   ", rs.getNString(3));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("?丂", rs.getNString(1));
        Assert.assertEquals("㐀丂", rs.getNString(2));
        Assert.assertEquals("㐀丂                  ", rs.getNString(3));
        Assert.assertFalse(rs.next());

        stmt.execute("delete from test_nv");
    }

    @Test
    public void fix52808791() throws SQLException {
        try {
            Connection conn = setConnection("&characterEncoding=gbk&nCharacterEncoding=utf8");
            createTable("t_nchar", "id number, c1 nchar(200), c2 nvarchar2(200), c3 char(200), c4 varchar2(200)");

            String str = "123中文abc㐀~";
            PreparedStatement ps = conn.prepareStatement("insert into t_nchar values (1, ?, ?, ?, ?)");
            ps.setNString(1, str);
            ps.setNString(2, str);
            ps.setString(3, str);
            ps.setString(4, str);
            Assert.assertEquals(1, ps.executeUpdate());
            ps.setString(1, str);
            ps.setString(2, str);
            ps.setNString(3, str);
            ps.setNString(4, str);
            Assert.assertEquals(1, ps.executeUpdate());

            ResultSet rs = conn.prepareStatement("select * from t_nchar").executeQuery();
            Assert.assertTrue(rs.next());
//            System.out.println(rs.getString(2));
//            System.out.println(rs.getString(3));
//            System.out.println(rs.getString(4));
//            System.out.println(rs.getString(5));
            Assert.assertTrue(rs.getString(2).startsWith("123中文abc㐀~"));
            Assert.assertTrue(rs.getString(3).startsWith("123中文abc㐀~"));
            Assert.assertTrue(rs.getString(4).startsWith("123中文abc?~"));
            Assert.assertTrue(rs.getString(5).startsWith("123中文abc?~"));
            Assert.assertTrue(rs.next());
//            System.out.println(rs.getString(2));
//            System.out.println(rs.getString(3));
//            System.out.println(rs.getString(4));
//            System.out.println(rs.getString(5));
            Assert.assertTrue(rs.getString(2).startsWith("123中文abc?~"));
            Assert.assertTrue(rs.getString(3).startsWith("123中文abc?~"));
            Assert.assertTrue(rs.getString(4).startsWith("123中文abc?~"));
            Assert.assertTrue(rs.getString(5).startsWith("123中文abc?~"));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

}
