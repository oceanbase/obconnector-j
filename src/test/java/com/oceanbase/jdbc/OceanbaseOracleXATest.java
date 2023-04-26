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
import java.util.UUID;

import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.junit.*;

import com.oceanbase.jdbc.jdbc2.optional.JDBC4MysqlXAConnection;
import com.oceanbase.jdbc.jdbc2.optional.MysqlXid;

public class OceanbaseOracleXATest extends BaseOracleTest {
    public static String tableName1 = "tablename1";
    public static String tableName2 = "tablename2";
    public static String tableName3 = "tablename3";
    public static String tableName4 = "tablename4";

    @BeforeClass
    public static void initClass() throws SQLException {
        createTable(tableName1, "c1 int,c2 int");
        createTable(tableName2, "c1 int,c2 int");
        createTable(tableName3, "c1 varchar(200)");
        createTable(tableName4, "c1 int,c2 varchar(200)");
    }

    @Test
    public void obOracleXAOne() throws Exception {
        Assume.assumeTrue(sharedUsePrepare());
        Connection conn = setConnection();
        conn.createStatement().execute(" insert into " + tableName1 + "  values(1,2)");
        JDBC4MysqlXAConnection mysqlXAConnection = new JDBC4MysqlXAConnection(
            (OceanBaseConnection) conn);
        String gtridStr = "gtrid_test_wgs_ob_oracle_xa_one";
        String bqualStr = "bqual_test_wgs_ob_oracle_xa_one";

        Xid xid = new MysqlXid(gtridStr.getBytes(), bqualStr.getBytes(), 123);
        try {
            mysqlXAConnection.start(xid, XAResource.TMNOFLAGS);
            // ps test
            PreparedStatement pstmt = null;
            ResultSet rs = null;
            pstmt = conn.prepareStatement("select c1 from " + tableName1);
            rs = pstmt.executeQuery();

            while (rs.next()) {
                System.out.println(rs.getInt(1));
            }
            pstmt.close();
            pstmt = conn.prepareStatement("insert into " + tableName1 + " (c1, c2) values(?, ?)");
            pstmt.setInt(1, 12);
            pstmt.setInt(2, 12);
            pstmt.executeUpdate();
            mysqlXAConnection.end(xid, XAResource.TMSUCCESS);
            mysqlXAConnection.prepare(xid);
            mysqlXAConnection.commit(xid, false);
        } catch (Exception e) {
            e.printStackTrace();
            mysqlXAConnection.rollback(xid);
            throw e;
        }
    }

    @Test
    public void obOracleXAOnePhase() throws Exception {
        Assume.assumeTrue(sharedUsePrepare());
        Connection conn = null;

        conn = setConnection();
        conn.createStatement().execute(" insert into " + tableName2 + "  values(1,2)");

        JDBC4MysqlXAConnection mysqlXAConnection = new JDBC4MysqlXAConnection(
            (OceanBaseConnection) conn);
        String gtridStr = "gtrid_test_wgs_ob_oracle_xa_one_phase";
        String bqualStr = "bqual_test_wgs_ob_oracle_xa_one_phase";

        Xid xid = new MysqlXid(gtridStr.getBytes(), bqualStr.getBytes(), 123);
        try {
            mysqlXAConnection.start(xid, XAResource.TMNOFLAGS);
            // ps test
            PreparedStatement pstmt = null;
            ResultSet rs = null;
            pstmt = conn.prepareStatement("select c1 from " + tableName2 + "");
            rs = pstmt.executeQuery();

            while (rs.next()) {
                System.out.println(rs.getInt(1));
            }

            pstmt.close();

            pstmt = conn.prepareStatement("insert into " + tableName2 + " (c1, c2) values(?, ?)");
            pstmt.setInt(1, 12);
            pstmt.setInt(2, 12);
            pstmt.executeUpdate();

            mysqlXAConnection.end(xid, XAResource.TMSUCCESS);
            mysqlXAConnection.commit(xid, true);
        } catch (Exception e) {
            mysqlXAConnection.rollback(xid);
            throw e;
        }
    }

    @Test
    public void obOracleXAWithError() throws Exception {
        Assume.assumeTrue(sharedUsePrepare());
        Connection conn = null;
        conn = setConnection();
        conn.setAutoCommit(false);

        JDBC4MysqlXAConnection mysqlXAConnection = new JDBC4MysqlXAConnection(
            (OceanBaseConnection) conn);
        String gtridStr = "gtrid_test_wgs_ob_oracle_xa_with_error";
        String bqualStr = "bqual_test_wgs_ob_oracle_xa_with_error";

        Xid xid = new MysqlXid(gtridStr.getBytes(), bqualStr.getBytes(), 123);
        // This flag will cause an exception
        try {
            mysqlXAConnection.start(xid, 123);
            Assert.assertTrue(false);
        } catch (XAException e) {
            Assert.assertEquals(XAException.XAER_INVAL, e.errorCode);
        }

        try {
            mysqlXAConnection.end(xid, 123);
            Assert.assertTrue(false);
        } catch (XAException e) {
            Assert.assertEquals(XAException.XAER_PROTO, e.errorCode);
        }

        try {
            mysqlXAConnection.prepare(xid);
            Assert.assertTrue(false);
        } catch (XAException e) {
            Assert.assertEquals(XAException.XAER_NOTA, e.errorCode);
        }

        try {
            mysqlXAConnection.commit(xid, true);
            Assert.assertTrue(false);
        } catch (XAException e) {
            Assert.assertEquals(XAException.XAER_NOTA, e.errorCode);
        }

        mysqlXAConnection.rollback(xid);
    }

    @Test
    public void obOracleXACheckAcAndError() throws Exception {
        Assume.assumeTrue(sharedUsePrepare());
        Connection conn = null;

        conn = setConnection();

        OceanBaseXaConnection mysqlXAConnection = new OceanBaseXaConnection(
            (OceanBaseConnection) conn);
        String gtridStr = "gtrid_test_wgs_ob_oracle_xa_check_ac_and_error";
        String bqualStr = "bqual_test_wgs_ob_oracle_xa_check_ac_and_error";

        Xid xid = new OceanBaseXid(gtridStr.getBytes(), bqualStr.getBytes(), 123);
        XAResource xaResource = mysqlXAConnection.getXAResource();
        try {
            Assert.assertTrue(conn.getAutoCommit());
            // This flag will cause an exception
            try {
                xaResource.start(xid, 123);
                Assert.assertTrue(false);
            } catch (XAException e) {
                Assert.assertEquals(XAException.XAER_INVAL, e.errorCode);
            }
            Assert.assertTrue(conn.getAutoCommit());
        } catch (Exception e) {
            xaResource.rollback(xid);
            throw e;
        }
    }

    @Test
    public void obOracleXACheckAcAndEndError() throws Exception {
        Assume.assumeTrue(sharedUsePrepare());
        Connection conn = null;

        conn = setConnection();

        JDBC4MysqlXAConnection mysqlXAConnection = new JDBC4MysqlXAConnection(
            (OceanBaseConnection) conn);
        String gtridStr = "gtrid_test_wgs_ob_oracle_xa_check_ac_and_end_error_3";
        String bqualStr = "bqual_test_wgs_ob_oracle_xa_check_ac_and_end_error_3";

        Xid xid = new MysqlXid(gtridStr.getBytes(), bqualStr.getBytes(), 123);
        try {
            Assert.assertTrue(conn.getAutoCommit());
            mysqlXAConnection.start(xid, XAResource.TMNOFLAGS);
            Assert.assertFalse(conn.getAutoCommit());

            // ps test
            PreparedStatement pstmt = null;
            ResultSet rs = null;
            pstmt = conn.prepareStatement("select c1 from " + tableName3 + "");
            rs = pstmt.executeQuery();

            while (rs.next()) {
                System.out.println(rs.getString(1));
            }

            pstmt.close();

            pstmt = conn.prepareStatement("insert into " + tableName3 + " (c1) values(?)");
            pstmt.setString(1, "abc");
            pstmt.executeUpdate();

            try {
                mysqlXAConnection.end(xid, 123);
                Assert.assertTrue(false);
            } catch (XAException e) {
                Assert.assertEquals(XAException.XAER_INVAL, e.errorCode);
            }
            Assert.assertFalse(conn.getAutoCommit());

            mysqlXAConnection.end(xid, XAResource.TMSUCCESS);
            Assert.assertTrue(conn.getAutoCommit());

            mysqlXAConnection.prepare(xid);
            mysqlXAConnection.commit(xid, false);
        } catch (XAException e) {
            if (e.errorCode == XAException.XAER_DUPID) {
                mysqlXAConnection.start(xid, XAResource.TMJOIN);
                mysqlXAConnection.end(xid, XAResource.TMSUCCESS);
            }
            mysqlXAConnection.rollback(xid);
            throw e;
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }
    }

    @Test
    public void obOracleXAStartFlags() throws Exception {
        XAConnection xaConn = null;
        Connection conn = null;
        Xid xid = null;
        OceanBaseDataSource ds = new OceanBaseDataSource();
        ds.setUrl(connU + "?useServerPrepStmts=true");
        try {
            Assert.assertTrue(ds.getUrlParser().getOptions().useServerPrepStmts);
            xaConn = ds.getXAConnection(username, password);
            xid = newXid();
            XAResource xaRes1 = xaConn.getXAResource();
            conn = xaConn.getConnection();
            xaRes1.start(xid, OceanBaseXaResource.ORATRANSLOOSE);
            conn.createStatement().executeQuery("SELECT 1 FROM DUAL");
            xaRes1.end(xid, XAResource.TMSUCCESS);
            xid = newXid();
            xaRes1.start(xid, OceanBaseXaResource.ORATMSERIALIZABLE);
            conn.createStatement().executeQuery("SELECT 1 FROM DUAL");
            xaRes1.end(xid, XAResource.TMSUCCESS);
            xid = newXid();
            Statement statement = conn.createStatement();
            try {
                xaRes1.start(xid, OceanBaseXaResource.ORATMREADONLY);
                statement.executeUpdate("insert into tableName4 values(4,'XA')");
                Assert.fail();
            } catch (SQLException ex) {
                xaConn.close();
            }
        } catch (XAException xaex) {
            Assert.fail();
        } finally {
            if (xaConn != null) {
                xaConn.close();
            }
        }
    }

    private Xid newXid() {
        return new OceanBaseXid(1, UUID.randomUUID().toString().getBytes(), UUID.randomUUID()
            .toString().getBytes());
    }

    // error on observer 3.x
    @Ignore
    public void obOracleXARecover() throws Exception {
        Assume.assumeTrue(sharedUsePrepare());
        Connection conn = null;
        conn = setConnection();
        OceanBaseXaConnection mysqlXAConnection = new OceanBaseXaConnection(
            (OceanBaseConnection) conn);
        XAResource xaResource = mysqlXAConnection.getXAResource();
        xaResource.recover(XAResource.TMSTARTRSCAN);
    }

}
