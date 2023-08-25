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
import java.util.ArrayList;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import com.oceanbase.jdbc.internal.util.constant.HaMode;
import com.oceanbase.jdbc.util.DefaultOptions;

public class ParserTest extends BaseTest {

    /**
     * Initialisation.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("table1", "id1 int auto_increment primary key");
        createTable("table2", "id2 int auto_increment primary key");
    }

    @Test
    public void malformedUrlException() throws SQLException {
        try {
            DriverManager.getConnection("jdbc:oceanbase:///" + hostname);
            fail("must have thrown exception");
        } catch (SQLException sqle) {
            assertTrue(sqle.getMessage().contains("No host is defined and pipe option is not set"));
        }
    }

    @Test
  public void poolVerification() throws Exception {
    ArrayList<HostAddress> hostAddresses = new ArrayList<>();
    hostAddresses.add(new HostAddress(hostname, port));
    UrlParser urlParser =
        new UrlParser(
            database, hostAddresses, DefaultOptions.defaultValues(HaMode.NONE), HaMode.NONE);
    urlParser.setUsername("USER");
    urlParser.setPassword("PWD");
    urlParser.parseUrl("jdbc:oceanbase://localhost:3306/db");
    assertEquals("USER", urlParser.getUsername());
    assertEquals("PWD", urlParser.getPassword());

    OceanBaseDataSource datasource = new OceanBaseDataSource();
    datasource.setUser("USER");
    datasource.setPassword("PWD");
    datasource.setUrl("jdbc:oceanbase://localhost:3306/db");
  }

    @Test
    public void isMultiMaster() throws Exception {
        Properties emptyProps = new Properties();

        assertFalse(UrlParser.parse("jdbc:oceanbase:replication://host1/", emptyProps)
            .isMultiMaster());
        assertFalse(UrlParser.parse("jdbc:oceanbase:failover://host1/", emptyProps).isMultiMaster());
        assertFalse(UrlParser.parse("jdbc:oceanbase:aurora://host1/", emptyProps).isMultiMaster());
        assertFalse(UrlParser.parse("jdbc:oceanbase:sequential://host1/", emptyProps)
            .isMultiMaster());
        assertFalse(UrlParser.parse("jdbc:oceanbase:loadbalance://host1/", emptyProps)
            .isMultiMaster());

        assertFalse(UrlParser.parse("jdbc:oceanbase:replication://host1,host2/", emptyProps)
            .isMultiMaster());
        assertTrue(UrlParser.parse("jdbc:oceanbase:failover://host1,host2/", emptyProps)
            .isMultiMaster());
        assertFalse(UrlParser.parse("jdbc:oceanbase:aurora://host1,host2/", emptyProps)
            .isMultiMaster());
        assertTrue(UrlParser.parse("jdbc:oceanbase:sequential://host1,host2/", emptyProps)
            .isMultiMaster());
        assertTrue(UrlParser.parse("jdbc:oceanbase:loadbalance://host1,host2/", emptyProps)
            .isMultiMaster());
    }

    @Test
  public void mysqlDatasourceVerification() throws Exception {
    Assume.assumeFalse(options.useSsl != null && options.useSsl);
    OceanBaseDataSource datasource = new OceanBaseDataSource();
    datasource.setUser(username);
    datasource.setPassword(password);
    datasource.setUrl("jdbc:mysql://" + hostname + ":" + port + "/" + database);
    try (Connection connection = datasource.getConnection()) {
      Statement stmt = connection.createStatement();
      assertTrue(stmt.execute("SELECT 10"));
    }
  }

    @Test
    public void libreOfficeBase() {
        String sql;
        try {
            Statement statement = sharedConnection.createStatement();
            sql = "INSERT INTO table1 VALUES (1),(2),(3),(4),(5),(6)";
            statement.execute(sql);
            sql = "INSERT INTO table2 VALUES (1),(2),(3),(4),(5),(6)";
            statement.execute(sql);
            // uppercase OJ
            sql = "SELECT table1.id1, table2.id2 FROM { OJ table1 LEFT OUTER JOIN table2 ON table1.id1 = table2.id2 }";
            ResultSet rs = statement.executeQuery(sql);
            for (int count = 1; count <= 6; count++) {
                assertTrue(rs.next());
                assertEquals(count, rs.getInt(1));
                assertEquals(count, rs.getInt(2));
            }
            // mixed oJ
            sql = "SELECT table1.id1, table2.id2 FROM { oJ table1 LEFT OUTER JOIN table2 ON table1.id1 = table2.id2 }";
            rs = statement.executeQuery(sql);
            for (int count = 1; count <= 6; count++) {
                assertTrue(rs.next());
                assertEquals(count, rs.getInt(1));
                assertEquals(count, rs.getInt(2));
            }
        } catch (SQLException e) {
            fail();
        }
    }

    @Test
    public void auroraClusterVerification() {
        try {
            DriverManager.getConnection("jdbc:oceanbase:aurora://"
                                        + "1.somehex.us-east-1.rds.amazonaws.com,"
                                        + "2.someOtherHex.us-east-1.rds.amazonaws.com/testj");
            fail("must have fail since not same cluster");
        } catch (Exception e) {
            assertEquals(
                "Connection string must contain only one aurora cluster. "
                        + "'2.someOtherHex.us-east-1.rds.amazonaws.com' doesn't correspond to DNS prefix "
                        + "'somehex.us-east-1.rds.amazonaws.com'", e.getMessage());
        }
    }

    @Test
    public void testOnDuplicateKeyUpdateIndex() throws ClassNotFoundException, SQLException {
        Assert.assertFalse(sharedOptions().useServerPrepStmts);
        Connection conn = setConnection("&rewriteBatchedStatements=true");
        createTable("test_onduplicatekeyupdateindex", "a int not null,b int, primary key (a)");
        String sql = "insert into test_onduplicatekeyupdateindex(a) with r as (select 1 as a union all select 2 as a union all select 3 as a)"
                + " select a from r on duplicate key update b = values(a) ;";
        PreparedStatement pstmt = conn.prepareStatement(sql);
        pstmt.execute();
        ResultSet rs = pstmt.executeQuery("select * from test_onduplicatekeyupdateindex");
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertNull(rs.getObject(2));
        rs.next();
        assertEquals(2, rs.getInt(1));
        assertNull(rs.getObject(2));
        rs.next();
        assertEquals(3, rs.getInt(1));
        assertNull(rs.getObject(2));
        rs.close();
        pstmt.close();
    }
}
