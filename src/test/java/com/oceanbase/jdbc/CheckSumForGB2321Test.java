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

import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Assert;
import org.junit.Test;

public class CheckSumForGB2321Test extends BaseOracleTest {
    @Test
    public void testInvalidSql() {
        Connection conn = null;
        try {
            try {
                conn = sharedConnection;
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                PreparedStatement ps = conn.prepareStatement("drop table pub_menu");
                ps.execute();
                ps.close();
            } catch (Exception e) {
                // ignore, maybe table does not exist
                //            e.printStackTrace();
            }
            try {
                PreparedStatement ps = conn.prepareStatement("drop table pub_grade_task");
                ps.execute();
                ps.close();
            } catch (Exception e) {
                // ignore, maybe table does not exist
                //            e.printStackTrace();
            }

            try {
                PreparedStatement ps = conn.prepareStatement("drop table pub_user");
                ps.execute();
                ps.close();
            } catch (Exception e) {
                // ignore, maybe table does not exist
                //            e.printStackTrace();
            }
            try {
                PreparedStatement ps = conn.prepareStatement("drop table pub_user_grade");
                ps.execute();
                ps.close();
            } catch (Exception e) {
                // ignore, maybe table does not exist
                //            e.printStackTrace();
            }
            PreparedStatement ps = conn
                .prepareStatement("CREATE TABLE pub_menu (menuid varchar(20), uppermenuid varchar(20), menulevel varchar(20),"
                                  + "systemcode varchar(20),menucname varchar(20), actionurl varchar(20),target varchar(20), "
                                  + "displayno varchar(20), image varchar(20), taskcode varchar(20),validind varchar(20), remark varchar(20))");
            ps.execute();
            ps.close();
            ps = conn
                .prepareStatement("CREATE TABLE pub_grade_task (taskcode varchar(20), gradecode varchar(20))");
            ps.execute();
            ps.close();
            ps = conn
                .prepareStatement("CREATE TABLE pub_user (usercode varchar(20),validind varchar(20))");
            ps.execute();
            ps.close();
            ps = conn
                .prepareStatement("CREATE TABLE pub_user_grade (gradecode varchar(20), usercode varchar(20),validind varchar(20))");
            ps.execute();
            ps.close();
            String csn = Charset.defaultCharset().name();
            System.out.println("csn = " + csn);
            ps = conn.prepareStatement("select\n" + "  t.menuid as menuID,\n"
                                       + "  t.uppermenuid as upperMenuID,\n"
                                       + "  t.menulevel as menulevel,\n"
                                       + "  t.systemcode as systemcode,\n"
                                       + "  t.menucname as menucname,\n"
                                       + "  t.actionurl as actionUrl,\n"
                                       + "  t.target as target,\n"
                                       + "  t.displayno as displayno,\n" + "  t.image as image,\n"
                                       + "  t.taskcode as taskcode,\n"
                                       + "  t.validind as validind\n" + "from\n" + "  pub_menu t\n"
                                       + "where\n" + "  1 = 1\n" + "  and (\n"
                                       + "    t.remark is null\n"
                                       + "    or t.remark != '����̨�˵�'\n" + "  )\n"
                                       + "  and t.taskcode in (\n" + "    select\n"
                                       + "      gt.taskcode\n" + "    from\n"
                                       + "      pub_grade_task gt,\n" + "      pub_user u,\n"
                                       + "      pub_user_grade ug\n" + "    where\n"
                                       + "      1 = 1\n"
                                       + "      and gt.gradecode = ug.gradecode\n"
                                       + "      and ug.usercode = u.usercode\n"
                                       + "      and u.usercode = 'email@xxxxx.com.cn'\n"
                                       + "      and u.validind = '1'\n"
                                       + "      and ug.validind = '1'\n" + "  )\n"
                                       + "  and t.systemcode = 'lawsuit'\n"
                                       + "  and t.menulevel = 1\n" + "  and t.validind = '1'\n"
                                       + "order by\n" + "  t.displayno");
            try {
                ps.execute();
            } catch (SQLException ex) {
                ex.printStackTrace();
                Assert.fail();
                return;
            }
            ResultSet rs = ps.getResultSet();
            while (rs.next()) {
                System.out.println("val is " + rs.getString(1) + "," + rs.getString(2));
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }
}
