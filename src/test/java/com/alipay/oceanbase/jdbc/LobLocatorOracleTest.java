package com.alipay.oceanbase.jdbc;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.sql.*;

import static org.junit.Assert.fail;

public class LobLocatorOracleTest extends BaseOracleTest {
    public static String tableBlob             = "test_blob";
    public static String tableLobLoad          = "test_blob1";
    public static String tableBlobSetBytes     = "test_blob2";
    public static String tableBlobTrim         = "test_blob3";
    public static String tableOBOracleModeBlob = "test_blob4";
    public static String tableClob             = "test_clob";
    public static String tableClobSetString    = "test_clob2";
    public static String tableClobTrim         = "test_clob3";
    public static String tableOBOracleModeClob = "test_clob4";
    public static String tableClobEncoding     = "zdw_test_clob";

    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable(tableBlob, "c1 varchar(10), c2 Blob");
        createTable(tableLobLoad, "c1 varchar(10), c2 Blob");
        createTable(tableBlobSetBytes, "c1 varchar(10), c2 Blob");
        createTable(tableBlobTrim, "c1 varchar(10), c2 Blob");
        createTable(tableOBOracleModeBlob, "c1 varchar(10), c2 Blob");
        createTable(tableBlob, "c1 varchar(10), c2 Clob");
        createTable(tableClobSetString, "c1 varchar(10), c2 Clob");
        createTable(tableClobTrim, "c1 varchar(10), c2 Clob");
        createTable(tableOBOracleModeClob, "c1 varchar(10), c2 Clob");
        createTable(tableClobEncoding, "c1 varchar(10), c2 Clob");
    }

    @Test
    public void oracleLobRead() throws SQLException, IOException {
        Connection conn = sharedPSLobConnection;
        PreparedStatement pstmtt = conn.prepareStatement("SELECT * FROM " + tableLobLoad
                                                         + " WHERE c1=? for update");
        pstmtt.setString(1, "112");
        ResultSet rs = null;//pstmtt.executeQuery("SELECT * FROM zdw_test_blob WHERE c1='112' for update;");
        rs = pstmtt.executeQuery();
        String c1 = null;
        Blob blob = null;
        while (rs.next()) {
            c1 = rs.getString(1);
            blob = (Blob) rs.getBlob(2);
            byte blob_byte[] = new byte[100];
            blob.getBinaryStream().read(blob_byte);
            StringBuilder bu = new StringBuilder();
            for (int i = 0; i < blob.length(); i++) {
                bu.append(Long.toHexString(Integer.toUnsignedLong(blob_byte[i])));
                //System.out.println("->" + blob_byte[i]);
                if (i < blob.length() - 1)
                    bu.append(',');
            }
            System.out.println("0:--> c1:" + rs.getString("c1") + " " + bu.toString());
        }
    }

    @Test
    public void oracleClobSetString() {
        Connection conn = sharedPSLobConnection;
        try {

            PreparedStatement pstmt = null;
            /*
                        pstmt = conn.prepareStatement("insert into zdw_test_clob values(?, ?)");
                        pstmt.setString(1, "112");
                        pstmt.setBytes(2, new byte[] { 'a', 'b', 'c', 'd', 'e', 'f', 'g' });
                        pstmt.executeUpdate();
            */
            String c_sql = "insert into " + tableClobSetString + " values('112', 'CLOB_1111111');";
            pstmt = conn.prepareStatement(c_sql);
            pstmt.execute();

            Clob clob = null;
            //pstmt.
            conn.setAutoCommit(false);

            ResultSet rs = pstmt.executeQuery("SELECT * FROM " + tableClobSetString
                                              + " WHERE c1='112' for update;");
            String c1 = null;
            while (rs.next()) {
                c1 = rs.getString(1);
                clob = (Clob) rs.getClob(2);
                System.out.println("3:--> c1:" + rs.getString("c1") + " "
                                   + clob.getSubString(1, (int) clob.length()));
            }

            clob.setString(10, "ABC");

            System.out.println("0:--> c1:" + " " + clob.getSubString(1, (int) clob.length()));
            //           blob.trimBlobToServer(9);
            //            blob.updateBlobToServer();

            rs = pstmt.executeQuery("SELECT * FROM " + tableClobSetString
                                    + " WHERE c1='112' for update;");
            c1 = null;
            while (rs.next()) {
                c1 = rs.getString(1);
                clob = (Clob) rs.getClob(2);

                System.out.println("1:--> c1:" + rs.getString("c1") + " "
                                   + clob.getSubString(1, (int) clob.length()));
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
        }
    }

    @Test
    public void oracleClobTrim() {
        Connection conn = null;
        try {
            conn = sharedPSLobConnection;

            PreparedStatement pstmt = null;

            //c_sql = "insert into zdw_test_clob values('112', 'CLOB_1111111');";
            String c_sql = "insert into " + tableClobTrim + " values('112', 'CLOB_中华人民共和国');";
            pstmt = conn.prepareStatement(c_sql);
            pstmt.execute();

            Clob clob = null;
            //pstmt.
            conn.setAutoCommit(false);

            ResultSet rs = pstmt.executeQuery("SELECT * FROM " + tableClobTrim
                                              + " WHERE c1='112' for update;");
            String c1 = null;
            while (rs.next()) {
                c1 = rs.getString(1);
                clob = (Clob) rs.getClob(2);
                System.out.println("3:--> c1:" + rs.getString("c1") + " "
                                   + clob.getSubString(1, (int) clob.length()));
            }

            clob.truncate(10);

            System.out.println("0:--> c1:" + " " + clob.getSubString(1, (int) clob.length()));
            //           blob.trimBlobToServer(9);
            //            blob.updateBlobToServer();

            rs = pstmt.executeQuery("SELECT * FROM " + tableClobTrim
                                    + " WHERE c1='112' for update;");
            c1 = null;
            while (rs.next()) {
                c1 = rs.getString(1);
                clob = (Clob) rs.getClob(2);
                System.out.println("1:--> c1:" + rs.getString("c1") + " "
                                   + clob.getSubString(1, (int) clob.length()));
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
        }
    }

    @Test
    public void oracleClobEncoding() {
        Connection conn = null;
        try {
            String encoding_str[] = { "utf8", "utf8" };
            //                String encoding_str[] = {"utf8", "utf8", "gbk"};
            String encoding_str2[] = { "utf8", "utf8mb4" };
            //                String encoding_str2[] = {"utf8", "utf8mb4", "gbk"};
            for (int i = 2; i < encoding_str.length; i++) {

                System.out.println("Encoding:" + encoding_str2[i]);
                conn = getObOracleLobConnectionWithCharset(encoding_str[i]);

                String c_sql = "insert into " + tableClobEncoding
                               + " values('112', 'CLOB_奥星贝斯数据库，输入的是中文！');";
                PreparedStatement pstmt = conn.prepareStatement(c_sql);
                pstmt.execute();

                Clob clob = null;
                //pstmt.
                conn.setAutoCommit(false);

                ResultSet rs = pstmt.executeQuery("SELECT * FROM " + tableClobEncoding
                                                  + " WHERE c1='112' for update;");
                String c1 = null;
                while (rs.next()) {
                    c1 = rs.getString(1);
                    clob = (Clob) rs.getClob(2);
                    System.out.println("clob.length() = " + clob.length());
                    System.out.println("1:--> c1:" + rs.getString("c1") + " "
                                       + clob.getSubString(1, (int) clob.length()));
                }

                clob.truncate(9);
                System.out
                    .println("2:--> c1:112" + " " + clob.getSubString(1, (int) clob.length()));
                Assert.assertTrue("CLOB_奥星贝斯".equals(clob.getSubString(1, (int) clob.length())));

                rs = pstmt.executeQuery("SELECT * FROM " + tableClobEncoding
                                        + " WHERE c1='112' for update;");
                c1 = null;
                while (rs.next()) {
                    c1 = rs.getString(1);
                    clob = (Clob) rs.getClob(2);
                    System.out.println("3:--> c1:" + rs.getString("c1") + " "
                                       + clob.getSubString(1, (int) clob.length()));
                }
                clob.setString(12, "GOOD 好");
                System.out
                    .println("4:--> c1:112" + " " + clob.getSubString(1, (int) clob.length()));
                Assert.assertTrue("CLOB_奥星贝斯  GOOD 好".equals(clob.getSubString(1,
                    (int) clob.length())));
                conn.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

        }
    }

    @Test
    public void oracleBlobSetBytes() {
        Connection conn = null;
        try {
            conn = sharedPSLobConnection;
            PreparedStatement pstmt = null;

            pstmt = conn.prepareStatement("insert into " + tableBlobSetBytes + " values(?, ?)");
            pstmt.setString(1, "112");
            pstmt.setBytes(2, new byte[] { 'a', 'b', 'c', 'd', 'e', 'f', 'g' });
            pstmt.executeUpdate();

            Blob blob = null;
            //pstmt.
            conn.setAutoCommit(false);

            ResultSet rs = pstmt.executeQuery("SELECT * FROM " + tableBlobSetBytes
                                              + " WHERE c1='112' for update;");
            String c1 = null;
            while (rs.next()) {
                c1 = rs.getString(1);
                blob = (Blob) rs.getBlob(2);
                byte blob_byte[] = new byte[100];
                blob.getBinaryStream().read(blob_byte);
                StringBuilder bu = new StringBuilder();
                for (int i = 0; i < blob.length(); i++) {
                    bu.append(Long.toHexString(Integer.toUnsignedLong(blob_byte[i])));
                    //System.out.println("->" + blob_byte[i]);
                    if (i < blob.length() - 1)
                        bu.append(',');
                }
                System.out.println("3:--> c1:" + rs.getString("c1") + " " + bu.toString());
            }

            blob.setBytes(10, "ABC".getBytes());
            blob.truncate(8);
            byte blob_by[] = new byte[100];
            blob.getBinaryStream().read(blob_by);
            StringBuilder bu_ = new StringBuilder();
            for (int i = 0; i < blob.length(); i++) {
                bu_.append(Long.toHexString(Integer.toUnsignedLong(blob_by[i])));
                //System.out.println("->" + blob_byte[i]);
                if (i < blob.length() - 1)
                    bu_.append(',');
            }
            System.out.println("0:--> c1:" + " " + bu_.toString());
            //           blob.trimBlobToServer(9);
            //            blob.updateBlobToServer();

            rs = pstmt.executeQuery("SELECT * FROM " + tableBlobSetBytes
                                    + " WHERE c1='112' for update;");
            c1 = null;
            while (rs.next()) {
                c1 = rs.getString(1);
                blob = (Blob) rs.getBlob(2);
                byte blob_byte[] = new byte[100];
                blob.getBinaryStream().read(blob_byte);
                StringBuilder bu = new StringBuilder();
                for (int i = 0; i < blob.length(); i++) {
                    bu.append(Long.toHexString(Integer.toUnsignedLong(blob_byte[i])));
                    //System.out.println("->" + blob_byte[i]);
                    if (i < blob.length() - 1)
                        bu.append(',');
                }
                System.out.println("1:--> c1:" + rs.getString("c1") + " " + bu.toString());
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
        }
    }

    @Test
    public void oracleBlobTrim() {
        Connection conn = null;
        try {
            conn = sharedPSLobConnection;

            String c_sql = "drop table zdw_test_blob";
            PreparedStatement pstmt = null;

            pstmt = conn.prepareStatement("insert into " + tableBlobTrim + " values(?, ?)");
            pstmt.setString(1, "112");
            pstmt.setBytes(2, new byte[] { 'a', 'b', 'c', 'd', 'e', 'f', 'g' });
            pstmt.executeUpdate();

            Blob blob = null;
            conn.setAutoCommit(false);

            ResultSet rs = pstmt.executeQuery("SELECT * FROM " + tableBlobTrim
                                              + " WHERE c1='112' for update;");
            String c1 = null;
            while (rs.next()) {
                c1 = rs.getString(1);
                blob = (Blob) rs.getBlob(2);
                byte blob_byte[] = new byte[100];
                blob.getBinaryStream().read(blob_byte);
                StringBuilder bu = new StringBuilder();
                for (int i = 0; i < blob.length(); i++) {
                    bu.append(Long.toHexString(Integer.toUnsignedLong(blob_byte[i])));
                    //System.out.println("->" + blob_byte[i]);
                    if (i < blob.length() - 1)
                        bu.append(',');
                }
                System.out.println("0:--> c1:" + rs.getString("c1") + " " + bu.toString());
            }

            blob.truncate(1);
            //            blob.updateBlobToServer();

            rs = pstmt.executeQuery("SELECT * FROM " + tableBlobTrim
                                    + " WHERE c1='112' for update;");
            c1 = null;
            while (rs.next()) {
                c1 = rs.getString(1);
                blob = (Blob) rs.getBlob(2);
                byte blob_byte[] = new byte[100];
                blob.getBinaryStream().read(blob_byte);
                StringBuilder bu = new StringBuilder();
                for (int i = 0; i < blob.length(); i++) {
                    bu.append(Long.toHexString(Integer.toUnsignedLong(blob_byte[i])));
                    //System.out.println("->" + blob_byte[i]);
                    if (i < blob.length() - 1)
                        bu.append(',');
                }
                System.out.println("1:--> c1:" + rs.getString("c1") + " " + bu.toString());
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
        }
    }

    @Ignore
    public void oracleClobAndBlobDbmsLobFunction() {
        Connection conn = null;
        try {
            conn = sharedPSLobConnection;

            String c_sql = "drop table test_lob_func";
            PreparedStatement pstmt = conn.prepareStatement(c_sql);
            try {
                pstmt.execute();
            } catch (SQLException e) {
            }
            c_sql = "create table test_lob_func(c1 int, c2 clob, c3 blob)";

            pstmt = conn.prepareStatement(c_sql);
            pstmt.execute();

            pstmt = conn.prepareStatement("insert into test_lob_func values(?, ?, ?)");
            pstmt.setInt(1, 112);
            pstmt.setString(2, "CLOB_abcdefg");
            pstmt.setBytes(3, "BLOB_abcdefg".getBytes());
            pstmt.executeUpdate();

            Statement statement = conn.createStatement();
            String createPlSql = "CREATE OR REPLACE PROCEDURE test_lob_open(p_c1 IN int, p_c2 OUT clob, p_c3 OUT blob) "
                                 + " is BEGIN "
                                 + " select c2 into p_c2 FROM test_lob_func WHERE c1 = p_c1;"
                                 + " select c3 into p_c3 FROM test_lob_func WHERE c1 = p_c1;"
                                 + " DBMS_LOB.open(p_c2, DBMS_LOB.LOB_READONLY);"
                                 + " DBMS_LOB.open(p_c3, DBMS_LOB.LOB_READONLY);"
                                 + "end test_lob_open;";
            statement.execute(createPlSql);

            //CallableStatement cstmt = conn.prepareCall("call test_lob_open(?, ?, ?)");
            CallableStatement cstmt = conn.prepareCall("begin test_lob_open(?, ?, ?); end;");
            cstmt.setInt(1, 112);
            //            cstmt.registerOutParameter(2, MysqlDefs.FIELD_TYPE_OB_CLOB);
            //            cstmt.registerOutParameter(3, MysqlDefs.FIELD_TYPE_OB_BLOB);

            cstmt.execute();
            Clob c = (Clob) cstmt.getClob(2);
            Blob b = (Blob) cstmt.getBlob(3);
            System.out.println("Clob:" + c.getSubString(1, (int) c.length()));
            String str = new String(b.getBytes(1, (int) b.length()));
            System.out.println("Blob:" + str);

            cstmt = conn.prepareCall("begin\ntest_lob_open(?, ?, ?);\nend;");
            cstmt.setInt(1, 112);
            //            cstmt.registerOutParameter(2, MysqlDefs.FIELD_TYPE_OB_CLOB);
            //            cstmt.registerOutParameter(3, MysqlDefs.FIELD_TYPE_OB_BLOB);

            cstmt.execute();
            c = (Clob) cstmt.getClob(2);
            b = (Blob) cstmt.getBlob(3);

            System.out.println("Clob:" + c.getSubString(1, (int) c.length()));
            str = new String(b.getBytes(1, (int) b.length()));
            System.out.println("Blob:" + str);

        } catch (Exception e) {
            e.printStackTrace();
            fail();
        } finally {

        }
    }

    @Test
    public void oracleMode() {
        Connection conn = null;
        try {

            conn = sharedPSLobConnection;
            Statement stmt = conn.createStatement();
            //            if (sharedUsePrepare()) {
            //                return;
            //            }
            PreparedStatement pstmt = null;
            PreparedStatement pstmtt = null;
            pstmtt = conn.prepareStatement("");
            String c_sql = null;

            ResultSet rs = null;

            c_sql = "insert into " + tableOBOracleModeClob + " values('111', 'CLOB_1111111');";
            pstmt = conn.prepareStatement(c_sql);
            pstmt.execute();

            c_sql = "SELECT  * FROM " + tableOBOracleModeClob + ";";
            pstmt = conn.prepareStatement(c_sql);
            rs = pstmt.executeQuery();

            while (rs.next()) {
                String id = rs.getString(1);
                StringBuffer note = new StringBuffer();
                java.sql.Clob c = rs.getClob(2);
                System.out.println("id:" + id + " " + c.getSubString(1, (int) c.length()));
                Assert.assertTrue((c.getSubString(1, (int) c.length())).equals(c.getSubString(1,
                    (int) (c.length() + 10))));
            }

            PreparedStatement apstmt = conn.prepareStatement("insert into " + tableOBOracleModeBlob
                                                             + " values(?, ?)");

            apstmt.setString(1, "112");
            //apstmt.setString(2, "ABC");
            apstmt.setBytes(2, new byte[] { 'a', 'b', 'c' });
            apstmt.executeUpdate();

            Blob blob = null;

            rs = pstmtt.executeQuery("SELECT * FROM " + tableOBOracleModeBlob
                                     + " WHERE c1='112' for update;");
            String c1 = null;
            while (rs.next()) {
                c1 = rs.getString(1);
                blob = (Blob) rs.getBlob(2);
                byte blob_byte[] = new byte[100];
                blob.getBinaryStream().read(blob_byte);
                StringBuilder bu = new StringBuilder();
                for (int i = 0; i < blob.length(); i++) {
                    bu.append(Long.toHexString(Integer.toUnsignedLong(blob_byte[i])));
                    //System.out.println("->" + blob_byte[i]);
                    if (i < blob.length() - 1)
                        bu.append(',');
                }
                System.out.println("0:--> c1:" + rs.getString("c1") + " " + bu.toString());
            }

            try {
                blob.setBytes(5, "ABC".getBytes());
            } catch (SQLException e) {
                Assert.assertTrue("Row has not been locked:",
                    e.getMessage().contains("Row has not been locked"));
            }

            conn.setAutoCommit(false);

            rs = pstmtt.executeQuery("SELECT * FROM " + tableOBOracleModeBlob
                                     + " WHERE c1='112' for update;");
            c1 = null;
            while (rs.next()) {
                c1 = rs.getString(1);
                blob = (Blob) rs.getBlob(2);
                byte blob_byte[] = new byte[100];
                blob.getBinaryStream().read(blob_byte);
                StringBuilder bu = new StringBuilder();
                for (int i = 0; i < blob.length(); i++) {
                    bu.append(Long.toHexString(Integer.toUnsignedLong(blob_byte[i])));
                    //System.out.println("->" + blob_byte[i]);
                    if (i < blob.length() - 1)
                        bu.append(',');
                }
                System.out.println("1:--> c1:" + rs.getString("c1") + " " + bu.toString());
                System.out.println("tt:" + new String(blob.getBytes(1, 10)));
            }

            apstmt.setBlob(2, blob);
            apstmt.executeUpdate();

            rs = apstmt.executeQuery("SELECT * FROM " + tableOBOracleModeBlob + "");

            byte b[] = new byte[100];
            while (rs.next()) {
                blob = (Blob) rs.getBlob(2);
                blob.getBinaryStream().read(b);

                StringBuffer builder = new StringBuffer();
                for (int i = 0; i < blob.length(); i++) {
                    builder.append(Long.toHexString(Integer.toUnsignedLong(b[i])));
                    if (i < blob.length() - 1) {
                        builder.append(',');
                    }
                }
                System.out.println(builder.toString());
            }

            //mysql查询语句
            String sql = "create or replace PROCEDURE calc_add(a1 IN int, a2 IN OUT int) is "
                         + "begin " + "  a2:=a1+a2; " + "end;";
            stmt.execute(sql);

            conn.setAutoCommit(true);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

        }
    }

    @Ignore
    public void bcLogUseAnonymous() {
        Connection conn = null;
        try {

            conn = sharedPSLobConnection;
            Statement stmt = conn.createStatement();
            CallableStatement cs1 = null;

            {
                cs1 = conn.prepareCall("begin read_blob_pl1(?); end;");
                cs1.registerOutParameter(1, Types.VARBINARY);
                cs1.setNull(1, Types.VARBINARY);
                cs1.execute();
                Object o = cs1.getObject(1);
            }
            cs1 = conn.prepareCall("BEGIN    read_blob_pl1(?);    END;");
            cs1.registerOutParameter(1, Types.BLOB);
            cs1.execute();
            cs1.getObject(1);
            cs1 = conn.prepareCall("BEGIN    read_blob_pl1(?);    END;");
            cs1.registerOutParameter(1, Types.BLOB);
            cs1.execute(); //Blob c1=(Blob) cs1.getObject(1);//c1.isOpen();
            PreparedStatement ps1 = conn.prepareStatement("select read_clob_f1() from dual");
            ps1.execute();
            ResultSet rs1 = ps1.getResultSet();
            rs1.next();
            Clob c = (Clob) rs1.getObject(1);
            System.out.println("tt:" + c.getSubString(1, (int) c.length()));
            PreparedStatement ps2 = conn.prepareStatement("select c1 from WRITE_TRIM_CLOB_T1");
            ps2.execute();
            ResultSet rs2 = ps2.getResultSet();
            rs2.next();
            rs2.getObject(1);
            PreparedStatement ps3 = conn
                .prepareStatement("DECLARE return_raw raw(2000); BEGIN  read_blob_pl1(return_raw);  END;");
            ps3.execute();
            ps3.getResultSet();
            Statement st2 = conn.createStatement();
            st2.executeQuery(" select c1 from READ_BLOB_T1");
            rs2 = st2.getResultSet();
            rs2.next();
            Blob blob = (Blob) rs2.getObject(1);

            byte b[] = new byte[100];
            blob.getBinaryStream().read(b);

            StringBuffer builder = new StringBuffer();
            for (int i = 0; i < blob.length(); i++) {
                builder.append(Long.toHexString(Integer.toUnsignedLong(b[i])));
                if (i < blob.length() - 1) {
                    builder.append(',');
                }
            }
            System.out.println(builder.toString());

        } catch (Exception e) {
            e.printStackTrace();
        } finally {

        }
    }

}
