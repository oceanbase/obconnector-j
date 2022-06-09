package com.oceanbase.jdbc;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.*;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class ComplexScenarioOracleTestCases extends BaseOracleTest {
    static String testLargeStream = "testLargeStream" + getRandomString(5);

    static String testLargeReader = "testLargeReader" + getRandomString(5);
    static String pieceReuse      = "pieceReuse" + getRandomString(5);

    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable(testLargeReader, "c1 clob,c2 varchar2(20)");
        createTable(pieceReuse, "c1 clob, c2 number, c3 clob, c4 int");
        createTable(testLargeStream, "c1 blob,c2 varchar2(20)");

    }

    @Test
    public void testLargeStreamValue() {
        Assume.assumeTrue(sharedUsePrepare());
        try {
            Connection conn = setConnection("&usePieceData=true");
            int size = 1024 * 1024 * 40; // 40mb
            byte[] theBlob = new byte[size];
            InputStream stream = new ByteArrayInputStream(theBlob);
            int pos = 0;
            int ch = 0;
            ResultSet rs;
            PreparedStatement ps = conn.prepareStatement("insert into " + testLargeStream
                                                         + " values(?, ?)");
            ps.setAsciiStream(1, stream);
            ps.setString(2, "aaa");
            ps.execute();
            ps = conn.prepareStatement("select * from " + testLargeStream);
            rs = ps.executeQuery();
            assertTrue(rs.next());
            InputStream is = rs.getAsciiStream(1);
            while ((ch = is.read()) != -1) {
                assertEquals(theBlob[pos++], ch);
            }
            Assert.assertEquals(size, pos);
        } catch (Throwable e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testAone39812040() {
        try {
            Connection conn = setConnection("&socketTimeout=60000");
            Statement stmt = conn.createStatement();
            stmt.execute("create or replace procedure SP_FAE_DEAL_FLOW(\n"
                         + "VI_CDATADATE in varchar2) is \n" + "begin \n"
                         + "    dbms_lock.sleep(120);\n"
                         + "    dbms_output.put_line(VI_CDATADATE);\n" + "end SP_FAE_DEAL_FLOW;");
            CallableStatement callableStatement = conn
                .prepareCall("call SP_FAE_DEAL_FLOW('2022-02-25');");
            callableStatement.execute();
            Assert.fail();
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("Connection timed out"));
        }
    }

    @Test
    public void testSendPieceReuseConn() {
        Assume.assumeTrue(sharedUsePrepare());
        try {
            Connection conn = setConnection("&usePieceData=true");
            Statement stmt = conn.createStatement();
            for (int j = 0; j < 5; j++) {
                System.out.println("test turn  = " + j);
                for (int i = 0; i < 20; i++) {
                    PreparedStatement ps = conn.prepareStatement("insert into " + pieceReuse
                                                                 + " values(?,?,?,?)");
                    String str = "abcd";
                    if (i >= 10) {
                        ps.setObject(1, str);
                    } else {
                        ps.setCharacterStream(1, new StringReader(str));
                    }
                    ps.setObject(3, str);
                    ps.setInt(2, 100);
                    ps.setInt(4, 100);
                    ps.execute();
                    if (i <= 10) {
                        ps.setObject(1, str);
                    } else {
                        ps.setCharacterStream(1, new StringReader(str));
                    }
                    ps.setObject(3, str);
                    ps.setInt(2, 100);
                    ps.setInt(4, 100);
                    ps.execute();

                    ps = conn.prepareStatement("insert into " + pieceReuse + " values(?,?,?,?)");
                    if (i > 10) {
                        ps.setObject(1, str);
                    } else {
                        ps.setCharacterStream(1, new StringReader(str));
                    }
                    ps.setObject(3, str);
                    ps.setInt(2, 100);
                    ps.setInt(4, 100);
                    ps.addBatch();
                    if (i > 10) {
                        ps.setObject(1, str);
                    } else {
                        ps.setCharacterStream(1, new StringReader(str));
                    }
                    ps.setObject(3, str);
                    ps.setInt(2, 100);
                    ps.setInt(4, 100);
                    ps.addBatch();
                    ps.executeBatch();
                    ps = conn.prepareStatement("select * from " + pieceReuse);
                    ResultSet rs = ps.executeQuery();
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals(str, rs.getString(1));
                    Assert.assertEquals(str, rs.getString(3));
                    Assert.assertEquals(100, rs.getInt(2));
                    Assert.assertEquals(100, rs.getInt(4));
                    stmt = conn.createStatement();
                    rs = ps.executeQuery("select * from " + pieceReuse);
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals(str, rs.getString(1));
                    Assert.assertEquals(str, rs.getString(3));
                    Assert.assertEquals(100, rs.getInt(2));
                    Assert.assertEquals(100, rs.getInt(4));
                    ps.close();
                    stmt.close();
                }
            }
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testLargeReaderValue() {
        Assume.assumeTrue(sharedUsePrepare());
        try {
            Connection conn = setConnection("&usePieceData=true");
            String str = getRandomString(1024 * 1024 * 40); // 40MB
            StringReader stringReader = new StringReader(str);
            ResultSet rs;
            PreparedStatement ps = conn.prepareStatement("insert into " + testLargeReader
                                                         + " values(?, ?)");
            ps.setClob(1, stringReader);
            ps.setString(2, "aaa");
            ps.execute();
            ps = conn.prepareStatement("select * from " + testLargeReader);
            rs = ps.executeQuery();
            assertTrue(rs.next());
            String str1 = rs.getString(1);
            Assert.assertEquals(str1, str);
        } catch (Throwable e) {
            e.printStackTrace();
            fail();
        }
    }

    // test for setClob
    @org.junit.Test
    public void testPieceClob() {
        Assume.assumeTrue(sharedOptions().useServerPrepStmts);
        try {
            String url = System.getProperty("db3");
            Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            PreparedStatement ps;
            conn.setAutoCommit(true);
            try {
                stmt.execute("drop table clobtest");
            } catch (SQLException e) {
                //                e.printStackTrace();
            }
            stmt.execute("create table clobtest(c1 clob,c2 int,c3 clob,c4 int)");
            ps = conn.prepareStatement("insert into clobtest values(?,?,?,?)");
            String str1 = getRandomString(100);
            String str2 = getRandomString(21);
            Reader reader1 = new StringReader(str1);
            Reader reader2 = new StringReader(str2);
            ps.setClob(1, reader1);
            ps.setClob(3, reader2);
            ps.setInt(2, 100);
            ps.setInt(4, 100);
            ps.execute();

            ps = conn.prepareStatement("select * from clobtest");
            ResultSet rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            assertEquals(str1, rs.getString(1));
            assertEquals(100, rs.getInt(2));
            assertEquals(str2, rs.getString(3));
            assertEquals(100, rs.getInt(4));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    // test for setObject
    @org.junit.Test
    public void testPieceClob2() {
        Assume.assumeTrue(sharedOptions().useServerPrepStmts);
        try {
            String url = System.getProperty("db3");
            Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            PreparedStatement ps;
            conn.setAutoCommit(true);
            try {
                stmt.execute("drop table clobtest");
            } catch (SQLException e) {
                //                e.printStackTrace();c
            }
            stmt.execute("create table clobtest(c1 clob,c2 int,c3 clob,c4 int)");
            ps = conn.prepareStatement("insert into clobtest values(?,?,?,?)");
            String str1 = getRandomString(100);
            String str2 = getRandomString(21);
            Reader reader1 = new StringReader(str1);
            Reader reader2 = new StringReader(str2);
            ps.setObject(1, reader1);
            ps.setObject(3, reader2);
            ps.setInt(2, 100);
            ps.setInt(4, 100);
            ps.execute();

            ps = conn.prepareStatement("select * from clobtest");
            ResultSet rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            assertEquals(str1, rs.getString(1));
            assertEquals(100, rs.getInt(2));
            assertEquals(str2, rs.getString(3));
            assertEquals(100, rs.getInt(4));

        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    // test for setCharacterStream
    @org.junit.Test
    public void testPieceClob3() {
        Assume.assumeTrue(sharedOptions().useServerPrepStmts);
        try {
            String url = System.getProperty("db3");
            Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            PreparedStatement ps;
            conn.setAutoCommit(true);
            try {
                stmt.execute("drop table clobtest");
            } catch (SQLException e) {
                //                e.printStackTrace();c
            }
            stmt.execute("create table clobtest(c1 clob,c2 int,c3 clob,c4 int)");

            ps = conn.prepareStatement("insert into clobtest values(?,?,?,?)");
            String str1 = getRandomString(100);
            String str2 = getRandomString(21);
            Reader reader1 = new StringReader(str1);
            Reader reader2 = new StringReader(str2);
            ps.setCharacterStream(1, reader1);
            ps.setCharacterStream(3, reader2);
            ps.setInt(2, 100);
            ps.setInt(4, 100);
            ps.execute();

            ps = conn.prepareStatement("select * from clobtest");
            ResultSet rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            assertEquals(str1, rs.getString(1));
            assertEquals(100, rs.getInt(2));
            assertEquals(str2, rs.getString(3));
            assertEquals(100, rs.getInt(4));

        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    // test the setBlob(int parameterIndex, InputStream value)
    @org.junit.Test
    public void testPieceBlob() {
        Assume.assumeTrue(sharedOptions().useServerPrepStmts);
        try {
            String url = System.getProperty("db3");
            Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            PreparedStatement ps;
            conn.setAutoCommit(true);
            try {
                stmt.execute("drop table blobtest");
            } catch (SQLException e) {
            }
            stmt.execute("create table blobtest(c1 blob,c2 int,c3 blob,c4 int)");
            ps = conn.prepareStatement("insert into blobtest values(?,?,?,?)");
            byte[] theBlob = { 1, 2, 3, 4, 5, 6 };
            InputStream stream = new ByteArrayInputStream(theBlob);
            InputStream stream2 = new ByteArrayInputStream(theBlob);
            ps.setBlob(1, stream);
            ps.setBlob(3, stream2);
            ps.setInt(2, 100);
            ps.setInt(4, 100);
            ps.execute();
            ps = conn.prepareStatement("select * from blobtest");
            ResultSet rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            InputStream is = rs.getBinaryStream(1);
            int pos = 0;
            int ch;
            while ((ch = is.read()) != -1) {
                assertEquals(theBlob[pos++], ch);
            }
            Assert.assertNotEquals(0, pos);
            Assert.assertEquals(100, rs.getInt(4));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    // test for  setObject(int parameterIndex, java.io.InputStream x) ; x = null
    @org.junit.Test
    public void testPieceBlobNullValue() {
        Assume.assumeTrue(sharedOptions().useServerPrepStmts);
        try {
            String url = System.getProperty("db3");
            Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            PreparedStatement ps;
            conn.setAutoCommit(true);
            try {
                stmt.execute("drop table blobtest");
            } catch (SQLException e) {
            }
            stmt.execute("create table blobtest(c1 blob,c2 int,c3 blob,c4 int)");
            byte[] theBlob = { 1, 2, 3, 4, 5, 6 };
            InputStream stream = null;
            InputStream stream2 = null;
            int pos = 0;
            int ch = 0;
            ResultSet rs;
            InputStream is;
            ps = conn.prepareStatement("insert into blobtest values(?,?,?,?)");
            ps.setObject(1, stream);
            ps.setObject(3, stream2);
            ps.setInt(2, 100);
            ps.setInt(4, 100);
            ps.execute();
            ps = conn.prepareStatement("select * from blobtest");
            rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            is = rs.getAsciiStream(1);
            Assert.assertNull(is);
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    // test for  void setBinaryStream(int parameterIndex, java.io.InputStream x)
    @org.junit.Test
    public void testPieceBlob1() {
        Assume.assumeTrue(sharedOptions().useServerPrepStmts);
        try {
            String url = System.getProperty("db3");
            Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            PreparedStatement ps;
            conn.setAutoCommit(true);
            try {
                stmt.execute("drop table blobtest");
            } catch (SQLException e) {
            }
            stmt.execute("create table blobtest(c1 blob,c2 int,c3 blob,c4 int)");
            byte[] theBlob = { 1, 2, 3, 4, 5, 6 };
            InputStream stream = new ByteArrayInputStream(theBlob);
            InputStream stream2 = new ByteArrayInputStream(theBlob);
            ResultSet rs;
            InputStream is;
            int pos = 0;
            int ch;
            ps = conn.prepareStatement("insert into blobtest values(?,?,?,?)");
            ps.setBinaryStream(1, stream);
            ps.setBinaryStream(3, stream2);
            ps.setInt(2, 100);
            ps.setInt(4, 100);
            ps.execute();
            ps = conn.prepareStatement("select * from blobtest");
            rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            is = rs.getBinaryStream(1);
            pos = 0;
            while ((ch = is.read()) != -1) {
                assertEquals(theBlob[pos++], ch);
            }
            Assert.assertNotEquals(0, pos);
            Assert.assertEquals(100, rs.getInt(4));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    // test for  void setBinaryStream(int parameterIndex, java.io.InputStream x) x = null
    @org.junit.Test
    public void testPieceBlob1NullValue() {
        Assume.assumeTrue(sharedOptions().useServerPrepStmts);
        try {
            String url = System.getProperty("db3");
            Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            PreparedStatement ps;
            conn.setAutoCommit(true);
            try {
                stmt.execute("drop table blobtest");
            } catch (SQLException e) {
            }
            stmt.execute("create table blobtest(c1 blob,c2 int,c3 blob,c4 int)");
            byte[] theBlob = { 1, 2, 3, 4, 5, 6 };
            InputStream stream = null;
            InputStream stream2 = null;
            ResultSet rs;
            InputStream is;
            int pos = 0;
            int ch;
            ps = conn.prepareStatement("insert into blobtest values(?,?,?,?)");
            ps.setBinaryStream(1, stream);
            ps.setBinaryStream(3, stream2);
            ps.setInt(2, 100);
            ps.setInt(4, 100);
            ps.execute();
            ps = conn.prepareStatement("select * from blobtest");
            rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            is = rs.getBinaryStream(1);
            Assert.assertNull(is);
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    // test for  setAsciiStream(int parameterIndex, java.io.InputStream x)
    @org.junit.Test
    public void testPieceBlob3() {
        Assume.assumeTrue(sharedOptions().useServerPrepStmts);
        try {
            String url = System.getProperty("db3");
            Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            PreparedStatement ps;
            conn.setAutoCommit(true);
            try {
                stmt.execute("drop table blobtest");
            } catch (SQLException e) {
            }
            stmt.execute("create table blobtest(c1 blob,c2 int,c3 blob,c4 int)");
            byte[] theBlob = { 1, 2, 3, 4, 5, 6 };
            InputStream stream = new ByteArrayInputStream(theBlob);
            InputStream stream2 = new ByteArrayInputStream(theBlob);
            stream = new ByteArrayInputStream(theBlob);
            stream2 = new ByteArrayInputStream(theBlob);
            int pos = 0;
            int ch = 0;
            ResultSet rs;
            ps = conn.prepareStatement("insert into blobtest values(?,?,?,?)");
            ps.setAsciiStream(1, stream);
            ps.setAsciiStream(3, stream2);
            ps.setInt(2, 100);
            ps.setInt(4, 100);
            ps.execute();
            ps = conn.prepareStatement("select * from blobtest");
            rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            InputStream is = rs.getAsciiStream(1);
            while ((ch = is.read()) != -1) {
                assertEquals(theBlob[pos++], ch);
            }
            Assert.assertNotEquals(0, pos);
            Assert.assertEquals(100, rs.getInt(4));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    // test for  setAsciiStream(int parameterIndex, java.io.InputStream x) x = null
    @org.junit.Test
    public void testPieceBlob3NullValue() {
        Assume.assumeTrue(sharedOptions().useServerPrepStmts);
        try {
            String url = System.getProperty("db3");
            Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            PreparedStatement ps;
            conn.setAutoCommit(true);
            try {
                stmt.execute("drop table blobtest");
            } catch (SQLException e) {
            }
            stmt.execute("create table blobtest(c1 blob,c2 int,c3 blob,c4 int)");
            byte[] theBlob = { 1, 2, 3, 4, 5, 6 };
            InputStream stream = null;
            InputStream stream2 = null;
            int pos = 0;
            int ch = 0;
            ResultSet rs;
            ps = conn.prepareStatement("insert into blobtest values(?,?,?,?)");
            ps.setAsciiStream(1, stream);
            ps.setAsciiStream(3, stream2);
            ps.setInt(2, 100);
            ps.setInt(4, 100);
            ps.execute();
            ps = conn.prepareStatement("select * from blobtest");
            rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            InputStream is = rs.getAsciiStream(1);
            Assert.assertNull(is);
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    // test for  setAsciiStream(int parameterIndex, java.io.InputStream x)
    @org.junit.Test
    public void testPieceBlob4() {
        Assume.assumeTrue(sharedOptions().useServerPrepStmts);
        try {
            String url = System.getProperty("db3");
            Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            PreparedStatement ps;
            conn.setAutoCommit(true);
            try {
                stmt.execute("drop table blobtest");
            } catch (SQLException e) {
            }
            stmt.execute("create table blobtest(c1 blob,c2 int,c3 blob,c4 int)");
            byte[] theBlob = { 1, 2, 3, 4, 5, 6 };
            InputStream stream = new ByteArrayInputStream(theBlob);
            InputStream stream2 = new ByteArrayInputStream(theBlob);
            int pos = 0;
            int ch = 0;
            ResultSet rs;
            InputStream is;
            ps = conn.prepareStatement("insert into blobtest values(?,?,?,?)");
            ps.setAsciiStream(1, stream);
            ps.setAsciiStream(3, stream2);
            ps.setInt(2, 100);
            ps.setInt(4, 100);
            ps.execute();
            ps = conn.prepareStatement("select * from blobtest");
            rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            is = rs.getAsciiStream(1);
            pos = 0;
            while ((ch = is.read()) != -1) {
                assertEquals(theBlob[pos++], ch);
            }
            Assert.assertNotEquals(0, pos);
            Assert.assertEquals(100, rs.getInt(4));
            Assert.assertEquals(100, rs.getInt(4));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    // test for  setAsciiStream(int parameterIndex, java.io.InputStream x)
    @org.junit.Test
    public void testPieceBlob4NullValue() {
        Assume.assumeTrue(sharedOptions().useServerPrepStmts);
        try {
            String url = System.getProperty("db3");
            System.out.println("url = " + url);
            Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            PreparedStatement ps;
            conn.setAutoCommit(true);
            try {
                stmt.execute("drop table blobtest");
            } catch (SQLException e) {
            }
            stmt.execute("create table blobtest(c1 blob,c2 int,c3 blob,c4 int)");
            byte[] theBlob = { 1, 2, 3, 4, 5, 6 };
            InputStream stream = null;
            InputStream stream2 = null;
            int pos = 0;
            int ch = 0;
            ResultSet rs;
            InputStream is;
            ps = conn.prepareStatement("insert into blobtest values(?,?,?,?)");
            ps.setAsciiStream(1, stream);
            ps.setAsciiStream(3, stream2);
            ps.setInt(2, 100);
            ps.setInt(4, 100);
            ps.execute();
            ps = conn.prepareStatement("select * from blobtest");
            rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            is = rs.getAsciiStream(1);
            Assert.assertNull(is);
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    // test for  setObject(int parameterIndex, java.io.InputStream x)
    @org.junit.Test
    public void testPieceBlob5() {
        Assume.assumeTrue(sharedOptions().useServerPrepStmts);
        try {
            String url = System.getProperty("db3");
            Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            PreparedStatement ps;
            conn.setAutoCommit(true);
            try {
                stmt.execute("drop table blobtest");
            } catch (SQLException e) {
            }
            stmt.execute("create table blobtest(c1 blob,c2 int,c3 blob,c4 int)");
            byte[] theBlob = { 1, 2, 3, 4, 5, 6 };
            InputStream stream = new ByteArrayInputStream(theBlob);
            InputStream stream2 = new ByteArrayInputStream(theBlob);
            int pos = 0;
            int ch = 0;
            ResultSet rs;
            InputStream is;
            ps = conn.prepareStatement("insert into blobtest values(?,?,?,?)");
            ps.setObject(1, stream);
            ps.setObject(3, stream2);
            ps.setInt(2, 100);
            ps.setInt(4, 100);
            ps.execute();
            ps = conn.prepareStatement("select * from blobtest");
            rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            is = rs.getAsciiStream(1);
            pos = 0;
            while ((ch = is.read()) != -1) {
                assertEquals(theBlob[pos++], ch);
            }
            Assert.assertNotEquals(0, pos);
            Assert.assertEquals(100, rs.getInt(4));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    // test for  setObject(int parameterIndex, java.io.InputStream x) x = null
    @org.junit.Test
    public void testPieceBlob5Nullalue() {
        Assume.assumeTrue(sharedOptions().useServerPrepStmts);
        try {
            String url = System.getProperty("db3");
            Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            PreparedStatement ps;
            conn.setAutoCommit(true);
            try {
                stmt.execute("drop table blobtest");
            } catch (SQLException e) {
            }
            stmt.execute("create table blobtest(c1 blob,c2 int,c3 blob,c4 int)");
            byte[] theBlob = { 1, 2, 3, 4, 5, 6 };
            InputStream stream = null;
            InputStream stream2 = null;
            int pos = 0;
            int ch = 0;
            ResultSet rs;
            InputStream is;
            ps = conn.prepareStatement("insert into blobtest values(?,?,?,?)");
            ps.setObject(1, stream);
            ps.setObject(3, stream2);
            ps.setInt(2, 100);
            ps.setInt(4, 100);
            ps.execute();
            ps = conn.prepareStatement("select * from blobtest");
            rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            is = rs.getAsciiStream(1);
            Assert.assertNull(is);
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testDbWithQuotes() throws SQLException {
        Connection conn = null;
        try {
            String url = System.getProperty("dbWithQuotes");
            conn = DriverManager.getConnection(url);
            Statement statement = conn.createStatement();
            try {
                statement.execute("drop procedure  pl_t2");
            } catch (SQLException e) {
            }
            statement
                .execute("create procedure pl_t2 (listing in out varchar2) is begin listing := 'abc';  end pl_t2; ");
            CallableStatement callableStatement = conn.prepareCall("call pl_t2(?)");
            callableStatement.registerOutParameter(1, Types.VARCHAR);
            callableStatement.setString(1, "1212121");
            callableStatement.execute();
            Assert.assertEquals("abc", callableStatement.getString(1));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testChangeUser() {
        try {
            String observerUrl = System.getProperty("observerUrl");
            Assume.assumeNotNull(observerUrl);
            String user = System.getProperty("newUser");
            String password = System.getProperty("newPwd");
            System.out.println("observerUrl = " + observerUrl);
            Connection conn = DriverManager.getConnection(observerUrl);
            ((ObConnection) conn).changeUser(user, password);
            Statement stmt = conn.createStatement();
            stmt.execute("drop table if exists test_change_user");
            stmt.execute("create table test_change_user(c1 int);");
            stmt.execute("insert into test_change_user values(100)");
            ResultSet rs = stmt.executeQuery("select * from test_change_user");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(100, rs.getInt(1));
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testConnectProxy() {
        try {
            String url = System.getProperty("connectProxyUrl");
            Class.forName("com.oceanbase.jdbc.Driver");
            Connection conn = DriverManager.getConnection(url);
            Statement statement = conn.createStatement();
            try {
                statement.execute("alter proxyconfig set enable_client_ssl=true;");
                statement.execute("alter proxyconfig set enable_client_ssl=false;");
            } catch (Exception e) {
                e.printStackTrace();
            }
            ResultSet rs = statement.executeQuery("show proxyconfig");
            int count = rs.getMetaData().getColumnCount();
            int rowCount = 0;
            while (rs.next()) {
                for (int i = 1; i <= count; i++) {
                    System.out.println(" col[ " + i + "] =" + rs.getString(i));
                }
                rowCount++;
            }
            Assert.assertNotEquals(rowCount, 0);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            Assert.fail();
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();

        }
    }

    @Test
    public void testUrlWithoutDatabase() {
        try {
            String url = System.getProperty("urlWithoutDatabase");
            Class.forName("com.oceanbase.jdbc.Driver");
            Connection conn = DriverManager.getConnection(url);
            conn.createStatement().execute(
                "create or replace  procedure proctest35 (x out int)\n is BEGIN\n x :=1 ;end\n ");
            CallableStatement callableStatement = conn.prepareCall("call proctest35(?) ");
            callableStatement.registerOutParameter(1, Types.INTEGER);
            callableStatement.execute();
            System.out.println("callableStatement = " + callableStatement.getInt(1));
            Assert.assertEquals(1, callableStatement.getInt(1));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testSocksProxy() {
        try {
            String url = System.getProperty("socksProxyOptions");
            Connection connection = setConnection(url);
            ResultSet resultSet = connection.createStatement().executeQuery("select 1 from dual");
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(1, resultSet.getInt(1));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @org.junit.Test
    public void testSendPiece2() {
        Assume.assumeTrue(sharedOptions().useServerPrepStmts);
        try {
            String url = System.getProperty("db3");
            Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            PreparedStatement ps;
            try {
                stmt.execute("drop table OMS_BULLETIN");
            } catch (SQLException e) {
                // e.printStackTrace();
            }
            stmt.execute("CREATE TABLE \"OMS_BULLETIN\" (\n"
                         + "  \"BULLETIN_ID\" VARCHAR2(32) DEFAULT ' ' NOT NULL ENABLE,\n"
                         + "  \"BULLETIN_TITLE\" VARCHAR2(255) DEFAULT ' ' NOT NULL ENABLE,\n"
                         + "  \"BULLETIN_CONTENT\" CLOB,\n"
                         + "  \"BULLETIN_CATEGORY\" CHAR(1) DEFAULT ' ',\n"
                         + "  \"BULLETIN_LEVEL\" CHAR(1) DEFAULT ' ' NOT NULL ENABLE,\n"
                         + "  \"STICKY_FLAG\" CHAR(1) DEFAULT ' ' NOT NULL ENABLE,\n"
                         + "  \"REPLY_FLAG\" CHAR(1) DEFAULT ' ' NOT NULL ENABLE,\n"
                         + "  \"DRAFT_FLAG\" CHAR(1) DEFAULT ' ' NOT NULL ENABLE,\n"
                         + "  \"NOTIFY_FLAG\" CHAR(1) DEFAULT ' ' NOT NULL ENABLE,\n"
                         + "  \"PUBLISH_STATUS\" CHAR(1) DEFAULT ' ' NOT NULL ENABLE,\n"
                         + "  \"PUBLISH_TYPE\" CHAR(1) DEFAULT ' ' NOT NULL ENABLE,\n"
                         + "  \"PUBLISH_DATE_TIME\" NUMBER(14) DEFAULT 0 NOT NULL ENABLE,\n"
                         + "  \"PUBLISH_CHANNEL_TYPE\" CHAR(1) DEFAULT ' ' NOT NULL ENABLE,\n"
                         + "  \"ASSIGN_ORG_RANGE\" CHAR(1) DEFAULT ' ' NOT NULL ENABLE,\n"
                         + "  \"RECEIVER_IDS\" CLOB NOT NULL ENABLE,\n"
                         + "  \"FILE_GUIDS\" VARCHAR2(2000) DEFAULT ' ',\n"
                         + "  \"CREATOR\" VARCHAR2(64) DEFAULT ' ' NOT NULL ENABLE,\n"
                         + "  \"CREATE_DATE_TIME\" NUMBER(14) DEFAULT 0 NOT NULL ENABLE,\n"
                         + "  \"BULLETIN_OFF_SHELF_TYPE\" CHAR(1) DEFAULT ' ' NOT NULL ENABLE,\n"
                         + "  \"BULLETIN_OFF_SHELF_DATE_TIME\" NUMBER(14) DEFAULT 0 NOT NULL ENABLE,\n"
                         + "  CONSTRAINT \"UK_BULLETIN\" UNIQUE (\"BULLETIN_ID\")\n" + ")");
            String sql = "insert into oms_bulletin(bulletin_id,bulletin_title,bulletin_content,bulletin_category,bulletin_level,"
                         + "sticky_flag,reply_flag,draft_flag,notify_flag,publish_status,publish_type,publish_date_time,"
                         + "publish_channel_type,assign_org_range,receiver_ids,file_guids,creator,create_date_time,"
                         + "bulletin_off_shelf_type,bulletin_off_shelf_date_time) values(nvl(?,' '),nvl(?,' '),?,nvl(?,' '),nvl(?,' '),"
                         + "nvl(?,' '),nvl(?,' '),nvl(?,' '),nvl(?,' '),nvl(?,' '),nvl(?,' '),?,nvl(?,' '),nvl(?,' '),?,nvl(?,' '),nvl(?,"
                         + "' '),?,nvl(?,' '),?)";
            ps = conn.prepareStatement(sql);
            ps.setString(1, "20775f1480aa44e5b69e32d209025602");
            ps.setString(2, "20200511,测试新增接口，即刻发送，不可修改和删除");
            ps.setClob(3, new StringReader("string values"));
            ps.setString(4, " ");
            ps.setString(5, "1");
            ps.setString(6, "1");
            ps.setString(7, "1");
            ps.setString(8, "0");
            ps.setString(9, "");
            ps.setString(10, "1");
            ps.setString(11, "2");
            ps.setLong(12, 20211019135330L);
            ps.setString(13, " ");
            ps.setString(14, "1");
            ps.setClob(15, new StringReader("string values"));
            ps.setString(16, " ");
            ps.setString(17, "admin");
            ps.setLong(18, 20211019135330L);
            ps.setString(19, " ");
            ps.setLong(20, 0);
            ps.execute();
            String sqlselect = "select rownum,bulletin_id,bulletin_content,publish_type,publish_date_time from oms_bulletin where rownum < 3";
            ps = conn.prepareStatement(sqlselect);
            ResultSet rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            Object obj = rs.getObject(3, String.class);
            Assert.assertTrue(obj instanceof String);
            Assert.assertEquals("string values", obj);
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @org.junit.Test
    public void testSendPiece() {
        Assume.assumeTrue(sharedOptions().useServerPrepStmts);
        try {
            String url = System.getProperty("db3");
            Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            PreparedStatement ps;
            try {
                stmt.execute("drop table clobtest");
            } catch (SQLException e) {
                // e.printStackTrace();
            }
            stmt.execute("create table clobtest(c1 clob, c2 number, c3 clob, c4 int)");
            ps = conn.prepareStatement("select * from clobtest"); // prepare not useless sql
            ps = conn.prepareStatement("insert into clobtest values(?,?,?,?)");
            String str = "abcd";
            String str2 = "中文abcd";
            ps.setCharacterStream(1, new StringReader(str));
            ps.setObject(3, str);
            ps.setInt(2, 100);
            ps.setInt(4, 100);
            ps.execute();
            ps.setCharacterStream(1, new StringReader(str2));
            ps.setObject(3, str2);
            ps.setInt(2, 100);
            ps.setInt(4, 100);
            ps.execute();
            ps = conn.prepareStatement("select * from clobtest");
            ResultSet rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            Assert.assertEquals(str, rs.getString(1));
            Assert.assertEquals(str, rs.getString(3));
            Assert.assertEquals(100, rs.getInt(2));
            Assert.assertEquals(100, rs.getInt(4));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(str2, rs.getString(1));
            Assert.assertEquals(str2, rs.getString(3));
            Assert.assertEquals(100, rs.getInt(2));
            Assert.assertEquals(100, rs.getInt(4));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testSendBatchBugForRwrite() {
        Assume.assumeTrue(sharedUsePrepare());
        try {
            Connection conn = setConnection("&usePieceData=true&rewriteBatchedStatements=true&maxBatchTotalParamsNum=200");
            Statement stmt = conn.createStatement();
            try {
                stmt.execute("drop table rewriteclob");
            } catch (SQLException e) {
                //                e.printStackTrace();
            }
            stmt.execute("create table rewriteclob(c1 clob,c2 varchar2(200),c3 varchar2(200),c4 clob,c5 int,c6 number(12,6),c7 date,c8 varchar2(200),c9 number(12,6),c10 varchar2(200),c11 varchar2(200))");
            PreparedStatement preparedStatement = conn
                .prepareStatement("insert into rewriteclob values(?,?,?,?,?,?,?,?,?,null,null)");
            int val = 0;
            preparedStatement.setCharacterStream(1, new StringReader("string value"));
            preparedStatement.setString(2, "string value");
            preparedStatement.setString(3, "string value");
            preparedStatement.setString(4, "string value");
            preparedStatement.setLong(5, val++);

            preparedStatement.setBigDecimal(6, new BigDecimal(134254.3342));
            preparedStatement.setDate(7, null);

            preparedStatement.setString(8, "string value");
            preparedStatement.setBigDecimal(9, new BigDecimal(134254.3342));
            preparedStatement.execute();
            ResultSet rs = stmt.executeQuery("select * from rewriteclob");
            val = 0;
            while (rs.next()) {
                Assert.assertEquals("string value", rs.getString(1));
                Assert.assertEquals(val++, rs.getLong(5));
            }
            stmt.execute("delete from rewriteclob");
            int batchTimes = 99;
            val = 0;
            System.out.println("test piece");
            preparedStatement = conn
                .prepareStatement("insert into rewriteclob values(?,?,?,?,?,?,?,?,?,null,null)");
            for (int i = 0; i < batchTimes; i++) {
                preparedStatement.setCharacterStream(1, new StringReader("string value")); //  test for long data
                preparedStatement.setString(2, "string value");
                preparedStatement.setString(3, "string value");
                preparedStatement.setCharacterStream(4, new StringReader("string value")); // test for long data
                preparedStatement.setLong(5, val++);

                preparedStatement.setBigDecimal(6, new BigDecimal(134254.3342));
                preparedStatement.setDate(7, null);

                preparedStatement.setString(8, "string value");
                preparedStatement.setBigDecimal(9, new BigDecimal(134254.3342));
                preparedStatement.addBatch();
            }
            val = 0;
            preparedStatement.executeBatch();
            rs = stmt.executeQuery("select * from rewriteclob");
            long count = 0;
            while (rs.next()) {
                count++;
                if (count < 10) {
                    Assert.assertEquals("string value", rs.getString(1));
                    Assert.assertEquals("string value", rs.getString(4));
                }
                Assert.assertEquals(val++, rs.getLong(5));
            }
            Assert.assertEquals(batchTimes, count);

            stmt.execute("delete from rewriteclob");

            batchTimes = 30001;
            val = 0;
            preparedStatement = conn
                .prepareStatement("insert into rewriteclob values(?,?,?,?,?,?,?,?,?,null,null)");
            for (int i = 0; i < batchTimes; i++) {
                preparedStatement.setString(1, "string value");
                preparedStatement.setString(2, "string value");
                preparedStatement.setString(3, "string value");
                preparedStatement.setString(4, "string value");
                preparedStatement.setLong(5, val++);
                preparedStatement.setBigDecimal(6, new BigDecimal(134254.3342));
                preparedStatement.setDate(7, null);
                preparedStatement.setString(8, "string value");
                preparedStatement.setBigDecimal(9, new BigDecimal(134254.3342));
                preparedStatement.addBatch();
            }
            val = 0;
            preparedStatement.executeBatch();
            rs = stmt.executeQuery("select * from rewriteclob");
            count = 0;
            while (rs.next()) {
                count++;
                if (count < 10) {
                    Assert.assertEquals("string value", rs.getString(1));
                    Assert.assertEquals("string value", rs.getString(4));
                }
                Assert.assertEquals(val++, rs.getLong(5));
            }
            Assert.assertEquals(batchTimes, count);
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

}
