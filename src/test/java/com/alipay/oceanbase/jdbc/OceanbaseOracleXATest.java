package com.alipay.oceanbase.jdbc;

import com.alipay.oceanbase.jdbc.jdbc2.optional.JDBC4MysqlXAConnection;
import com.alipay.oceanbase.jdbc.jdbc2.optional.MysqlXid;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class OceanbaseOracleXATest extends BaseOracleTest {
    public static String tableName1 = "tablename1";
    public static String tableName2 = "tablename2";
    public static String tableName3 = "tablename3";

    @BeforeClass
    public static void initClass() throws SQLException {
        createTable(tableName1, "c1 int,c2 int");
        createTable(tableName2, "c1 int,c2 int");
        createTable(tableName3, "c1 varchar(200)");
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
            (com.alipay.oceanbase.jdbc.OceanBaseConnection) conn);
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
