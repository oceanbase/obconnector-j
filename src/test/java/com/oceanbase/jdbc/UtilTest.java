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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.oceanbase.jdbc.internal.util.Utils;
import org.junit.Assert;
import org.junit.Test;

public class UtilTest extends BaseTest {

    @Test
    public void escape() throws SQLException {
        String[] inputs = new String[] {
                "select {fn timestampdiff(SQL_TSI_HOUR, {fn convert('SQL_', SQL_INTEGER)})}",
                "{call foo({fn now()})}", "{?=call foo({fn now()})}",
                "SELECT 'David_' LIKE 'David|_' {escape '|'}",
                "select {fn dayname ({fn abs({fn now()})})}", "{d '1997-05-24'}",
                "{d'1997-05-24'}", "{t '10:30:29'}", "{t'10:30:29'}",
                "{ts '1997-05-24 10:30:29.123'}", "{ts'1997-05-24 10:30:29.123'}",
                "'{string data with { or } will not be altered'",
                "`{string data with { or } will not be altered`",
                "--  Also note that you can safely include { and } in comments" };
        String[] outputs = new String[] { "select timestampdiff(HOUR, convert('SQL_', INTEGER))",
                "call foo(now())", "?=call foo(now())",
                "SELECT 'David_' LIKE 'David|_' escape '|'", "select dayname (abs(now()))",
                "'1997-05-24'", "'1997-05-24'", "'10:30:29'", "'10:30:29'",
                "'1997-05-24 10:30:29.123'", "'1997-05-24 10:30:29.123'",
                "'{string data with { or } will not be altered'",
                "`{string data with { or } will not be altered`",
                "--  Also note that you can safely include { and } in comments" };
        for (int i = 0; i < inputs.length; i++) {
            assertEquals(sharedConnection.nativeSQL(inputs[i]), outputs[i]);
        }
    }

    @Test
  public void backTickQuote() throws SQLException {
    try (Connection conn = setConnection()) {
      Statement stmt = conn.createStatement();
      stmt.execute("CREATE TEMPORARY TABLE `{tt1}`(`{Document id}` int, tt text)");
    }
  }

    @Test
    public void testString() throws SQLException {
        String tableName = "test";
        String sql1 = "insert /* woshilize */ into t values( 'wo',?,:name) /* c43 */ -- sfs \n";
        String sql2 = "{:ret = call /* sfs */testproc(?,'test',:name1,:name2)};";
        String sql3 = "insert /* c1  */ /* c2 */ into t values( 'wo','111') /* c43 */ -- sfs \n ";
        String sql4 = "select * from test where c1 = ? and c2 = :name and c3 = 10 and c4 = :name2;";
        String sql5 = "select * /* comment */ from testcolonb4NcQ where  c3 = :col3";
        String sql6 = "select 1 from dual";
        String sql7 = "insert into astest1 values(:name1),(:name2)";
        String sql8 = "insert into astest1 values(?),(?)";
        String sql9 = "/*t1,t2,t3*/ -- t1\n-- t2\n-- t3\n/*t1, t2, t3*/ insert into " + tableName
                      + " values (?, '中文', ?, ?, ?, ?);";
        String sql10 = "-- t1\n-- t2\n-- t3\n insert into " + tableName
                       + " values (?, ?, ?, ?, ?, ?)";
        String sql11 = "insert into test_batch1 values (?, ?, ?, ?, ?, ?)";
        String sql12 = "SELECT 1 AS scope, 'ROWID' AS column_name, -8 AS data_type,\n"
                       + " 'ROWID' AS type_name, 0 AS column_size, 0 AS buffer_length,\n"
                       + "       0 AS decimal_digits, 2 AS pseudo_column\n" + "FROM DUAL";
        String sql13 = "SELECT\n" + "  -- Packaged procedures with no arguments\n"
                       + "  package_name AS procedure_cat,\n" + "  owner AS procedure_schem,\n"
                       + "  object_name AS procedure_name,\n" + "  NULL,\n" + "  NULL,\n"
                       + "  NULL,\n" + "  'Packaged procedure' AS remarks,\n"
                       + "  1 AS procedure_type\n" + ",  NULL AS specific_name\n"
                       + "FROM all_arguments\n" + "WHERE argument_name IS NULL\n"
                       + "  AND data_type IS NULL\n" + "  AND package_name LIKE ? \n"
                       + "  AND owner LIKE ? \n" + "  AND object_name LIKE ? \n"
                       + "UNION ALL SELECT\n" + "  -- Packaged procedures with arguments\n"
                       + "  package_name AS procedure_cat,\n" + "  owner AS procedure_schem,\n"
                       + "  object_name AS procedure_name,\n" + "  NULL,\n" + "  NULL,\n"
                       + "  NULL,\n" + "  'Packaged procedure' AS remarks,\n"
                       + "  1 AS procedure_type\n" + ",  NULL AS specific_name\n"
                       + "FROM all_arguments";
        Assert.assertEquals("insert  into t values( 'wo',?,?)  ",
            Utils.trimSQLString(sql1, false, true));
        Assert.assertEquals("{? = call testproc(?,'test',?,?)};",
            Utils.trimSQLString(sql2, false, true));
        Assert.assertEquals("insert   into t values( 'wo','111')   ",
            Utils.trimSQLString(sql3, false, false));
        Assert.assertEquals("select * from test where c1 = ? and c2 = ? and c3 = 10 and c4 = ?;",
            Utils.trimSQLString(sql4, false, true));
        Assert.assertEquals("select *  from testcolonb4NcQ where  c3 = ?",
            Utils.trimSQLString(sql5, false, true));
        Assert.assertEquals("select 1 from dual", Utils.trimSQLString(sql6, false, false));
        Assert.assertEquals("insert into astest1 values(?),(?)",
            Utils.trimSQLString(sql7, false, true));
        Assert.assertEquals("insert into astest1 values(?),(?)",
            Utils.trimSQLString(sql8, false, false));
        Assert.assertEquals("  insert into test values (?, '中文', ?, ?, ?, ?);",
            Utils.trimSQLString(sql9, false, false));
        Assert.assertEquals(" insert into test values (?, ?, ?, ?, ?, ?)",
            Utils.trimSQLString(sql10, false, false));
        Assert.assertEquals("insert into test_batch1 values (?, ?, ?, ?, ?, ?)",
            Utils.trimSQLString(sql11, false, false));
        Assert.assertEquals("SELECT 1 AS scope, 'ROWID' AS column_name, -8 AS data_type,\n"
                            + " 'ROWID' AS type_name, 0 AS column_size, 0 AS buffer_length,\n"
                            + "       0 AS decimal_digits, 2 AS pseudo_column\n" + "FROM DUAL",
            Utils.trimSQLString(sql12, false, true));
        Assert.assertEquals("SELECT 1 AS scope, 'ROWID' AS column_name, -8 AS data_type,\n"
                            + " 'ROWID' AS type_name, 0 AS column_size, 0 AS buffer_length,\n"
                            + "       0 AS decimal_digits, 2 AS pseudo_column\n" + "FROM DUAL",
            Utils.trimSQLString(sql12, false, true));

        /* skip the comment tests */
        Assert.assertEquals("insert /* woshilize */ into t values( 'wo',?,?) /* c43 */ -- sfs \n",
            Utils.trimSQLString(sql1, false, true, true));
        Assert.assertEquals("{? = call /* sfs */testproc(?,'test',?,?)};",
            Utils.trimSQLString(sql2, false, true, true));

    }
}
