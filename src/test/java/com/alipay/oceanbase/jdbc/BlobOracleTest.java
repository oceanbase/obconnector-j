package com.alipay.oceanbase.jdbc;

import org.junit.*;

import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.*;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.*;

public class BlobOracleTest extends BaseOracleTest {

    /**
     * Initialisation.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("bug716378", "id int not null primary key, test blob, test2 blob, test3 clob");
        createTable("BlobTeststreamtest2", "id int primary key not null, st varchar(20), strm clob"
                                           + ", strm2 clob, strm3 clob");
        createTable("BlobTeststreamtest3", "id int primary key not null, strm clob");
        createTable("BlobTestclobtest", "id int not null primary key, strm clob");
        createTable("BlobTestclobtest2", "strm clob");
        createTable("BlobTestclobtest3", "id int not null primary key, strm clob");
        createTable("BlobTestclobtest4", "id int not null primary key, strm clob");
        createTable("BlobTestclobtest5", "id int not null primary key, strm clob");
        createTable("BlobTestblobtest", "id int not null primary key, strm blob");
        createTable("BlobTestblobtest2", "id int not null primary key, strm blob");
        createTable("conj77_test", "Name VARCHAR(100) NOT NULL,Archive BLOB, PRIMARY KEY (Name)");
        createTable("bug716378_ps",
            "id int not null primary key, test blob, test2 blob, test3 clob");
        createTable("BlobTeststreamtest2_ps",
            "id int primary key not null, st varchar(20), strm clob" + ", strm2 clob, strm3 clob");
        createTable("BlobTeststreamtest3_ps", "id int primary key not null, strm clob");
        createTable("BlobTestclobtest_ps", "id int not null primary key, strm clob");
        createTable("BlobTestclobtest2_ps", "strm clob");
        createTable("BlobTestclobtest3_ps", "id int not null primary key, strm clob");
        createTable("BlobTestclobtest4_ps", "id int not null primary key, strm clob");
        createTable("BlobTestclobtest5_ps", "id int not null primary key, strm clob");
        createTable("BlobTestblobtest_ps", "id int not null primary key, strm blob");
        createTable("BlobTestblobtest2_ps", "id int not null primary key, strm blob");
        createTable("conj77_test_ps", "Name VARCHAR(100) NOT NULL,Archive BLOB, PRIMARY KEY (Name)");
    }

    @Test
    public void testPosition() throws SQLException {
        byte[] blobContent = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
        byte[] pattern = new byte[] { 3, 4 };
        Blob blob = new OceanBaseBlob(blobContent);
        assertEquals(3, blob.position(pattern, 1));
        pattern = new byte[] { 12, 13 };
        assertEquals(-1, blob.position(pattern, 1));
        pattern = new byte[] { 11, 12 };
        assertEquals(11, blob.position(pattern, 1));
        pattern = new byte[] { 1, 2 };
        assertEquals(1, blob.position(pattern, 1));
    }

    @Test(expected = SQLException.class)
    public void testBadStart() throws SQLException {
        byte[] blobContent = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
        byte[] pattern = new byte[] { 3, 4 };
        Blob blob = new OceanBaseBlob(blobContent);
        blob.position(pattern, 0);
    }

    @Test(expected = SQLException.class)
    public void testBadStart2() throws SQLException {
        byte[] blobContent = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
        byte[] pattern = new byte[] { 3, 4 };
        Blob blob = new OceanBaseBlob(blobContent);
        blob.position(pattern, 44);
    }

    @Test
    public void testBug716378() throws SQLException {
        Statement stmt = sharedConnection.createStatement();

        stmt.executeUpdate("insert into bug716378 values(1, 'a','b','c')");
        ResultSet rs = stmt.executeQuery("select * from bug716378");
        assertTrue(rs.next());
        byte[] arr = new byte[0];
        assertEquals(arr.getClass(), rs.getObject(2).getClass());
        assertEquals(arr.getClass(), rs.getObject(3).getClass());
        assertEquals(String.class, rs.getObject(4).getClass());
    }

    @Test
    public void testCharacterStreamWithMultibyteCharacterAndLength() throws Throwable {
        String toInsert1 = "Øbbcdefgh\njklmn\"";
        String toInsert2 = "Øabcdefgh\njklmn\"";
        PreparedStatement stmt = sharedConnection
            .prepareStatement("insert into BlobTeststreamtest2 (id, st, strm, strm2, strm3) values (?,?,?,?,?)");
        stmt.setInt(1, 2);
        stmt.setString(2, toInsert1);
        Reader reader = new StringReader(toInsert2);
        stmt.setCharacterStream(3, reader, 5);
        stmt.setCharacterStream(4, null);
        stmt.setCharacterStream(5, null, 5);
        stmt.execute();

        ResultSet rs = sharedConnection.createStatement().executeQuery(
            "select * from BlobTeststreamtest2");
        assertTrue(rs.next());
        Reader rdr = rs.getCharacterStream("strm");
        StringBuilder sb = new StringBuilder();
        int ch;
        while ((ch = rdr.read()) != -1) {
            sb.append((char) ch);
        }

        assertEquals(toInsert1, rs.getString(2));
        assertEquals(toInsert2.substring(0, 5), sb.toString());
        assertNull(rs.getString(4));
        assertNull(rs.getString(5));
    }

    //@Ignore //TODO FIXME
    @Test
    public void testPSCharacterStreamWithMultibyteCharacterAndLength() throws Throwable {
        String toInsert1 = "Øbbcdefgh\njklmn\"";
        String toInsert2 = "Øabcdefgh\njklmn\"";
        PreparedStatement stmt = sharedPSConnection
            .prepareStatement("insert into BlobTeststreamtest2_ps (id, st, strm, strm2, strm3) values (?,?,?,?,?)");
        stmt.setInt(1, 2);
        stmt.setString(2, toInsert1);
        Reader reader = new StringReader(toInsert2);
        stmt.setCharacterStream(3, reader, 5);
        stmt.setCharacterStream(4, null);
        stmt.setCharacterStream(5, null, 5);
        stmt.execute();

        ResultSet rs = sharedPSConnection.createStatement().executeQuery(
            "select * from BlobTeststreamtest2_ps");
        assertTrue(rs.next());
        Reader rdr = rs.getCharacterStream("strm");
        StringBuilder sb = new StringBuilder();
        int ch;
        while ((ch = rdr.read()) != -1) {
            sb.append((char) ch);
        }

        assertEquals(toInsert1, rs.getString(2));
        assertEquals(toInsert2.substring(0, 5), sb.toString());
        assertNull(rs.getString(4));
        assertNull(rs.getString(5));
    }

    @Test
    public void testCharacterStreamWithMultibyteCharacter() throws Throwable {
        PreparedStatement stmt = sharedConnection
            .prepareStatement("insert into BlobTeststreamtest3 (id, strm) values (?,?)");
        stmt.setInt(1, 2);
        String toInsert = "Øabcdefgh\njklmn\"";
        Reader reader = new StringReader(toInsert);
        stmt.setCharacterStream(2, reader);
        stmt.execute();
        ResultSet rs = sharedConnection.createStatement().executeQuery(
            "select * from BlobTeststreamtest3");
        assertTrue(rs.next());
        Reader rdr = rs.getCharacterStream("strm");
        StringBuilder sb = new StringBuilder();
        int ch;
        while ((ch = rdr.read()) != -1) {
            sb.append((char) ch);
        }
        assertEquals(toInsert, sb.toString());
    }

    @Test
    public void testPSCharacterStreamWithMultibyteCharacter() throws Throwable {
        PreparedStatement stmt = sharedPSConnection
            .prepareStatement("insert into BlobTeststreamtest3_ps (id, strm) values (?,?)");
        stmt.setInt(1, 2);
        String toInsert = "Øabcdefgh\njklmn\"";
        Reader reader = new StringReader(toInsert);
        stmt.setCharacterStream(2, reader);
        stmt.execute();
        ResultSet rs = sharedPSConnection.createStatement().executeQuery(
            "select * from BlobTeststreamtest3_ps");
        assertTrue(rs.next());
        Reader rdr = rs.getCharacterStream("strm");
        StringBuilder sb = new StringBuilder();
        int ch;
        while ((ch = rdr.read()) != -1) {
            sb.append((char) ch);
        }
        assertEquals(toInsert, sb.toString());
    }

    @Test
    public void testReaderWithLength() throws SQLException, IOException {
        PreparedStatement stmt = sharedConnection
            .prepareStatement("insert into BlobTestclobtest5 (id, strm) values (?,?)");
        byte[] arr = new byte[32000];
        Arrays.fill(arr, (byte) 'b');

        stmt.setInt(1, 1);
        String clob = new String(arr);
        stmt.setCharacterStream(2, new StringReader(clob), 20000);
        stmt.execute();
        ResultSet rs = sharedConnection.createStatement().executeQuery(
            "select * from BlobTestclobtest5");
        assertTrue(rs.next());
        Reader readStuff = rs.getCharacterStream("strm");

        char[] chars = new char[50000];
        //noinspection ResultOfMethodCallIgnored
        readStuff.read(chars);

        byte[] arrResult = new byte[20000];
        Arrays.fill(arrResult, (byte) 'b');

        for (int i = 0; i < chars.length; i++) {
            if (i < 20000) {
                assertEquals(arrResult[i], chars[i]);
            } else {
                assertEquals(chars[i], '\u0000');
            }
        }
    }

    @Ignore
    //TODO FIXME
    @Test
    public void testPSReaderWithLength() throws SQLException, IOException {
        PreparedStatement stmt = sharedPSConnection
            .prepareStatement("insert into BlobTestclobtest5_ps (id, strm) values (?,?)");
        byte[] arr = new byte[32000];
        Arrays.fill(arr, (byte) 'b');

        stmt.setInt(1, 1);
        String clob = new String(arr);
        stmt.setCharacterStream(2, new StringReader(clob), 20000);
        stmt.execute();
        ResultSet rs = sharedPSConnection.createStatement().executeQuery(
            "select * from BlobTestclobtest5_ps");
        assertTrue(rs.next());
        Reader readStuff = rs.getCharacterStream("strm");

        char[] chars = new char[50000];
        //noinspection ResultOfMethodCallIgnored
        readStuff.read(chars);

        byte[] arrResult = new byte[20000];
        Arrays.fill(arrResult, (byte) 'b');

        for (int i = 0; i < chars.length; i++) {
            if (i < 20000) {
                assertEquals(arrResult[i], chars[i]);
            } else {
                assertEquals(chars[i], '\u0000');
            }
        }
    }

    @Test
    public void testBlobWithLength() throws SQLException, IOException {
        PreparedStatement stmt = sharedConnection
            .prepareStatement("insert into BlobTestblobtest2 (id, strm) values (?,?)");
        byte[] arr = new byte[32000];
        Random rand = new Random();
        rand.nextBytes(arr);
        InputStream stream = new ByteArrayInputStream(arr);
        stmt.setInt(1, 1);
        stmt.setBlob(2, stream, 20000);
        stmt.execute();

        // check what stream not read after length:
        int remainRead = 0;
        while (stream.read() >= 0) {
            remainRead++;
        }
        assertEquals(12000, remainRead);

        ResultSet rs = sharedConnection.createStatement().executeQuery(
            "select * from BlobTestblobtest2");
        assertTrue(rs.next());
        InputStream readStuff = rs.getBlob("strm").getBinaryStream();
        int pos = 0;
        int ch;
        while ((ch = readStuff.read()) != -1) {
            assertEquals(arr[pos++] & 0xff, ch);
        }
        assertEquals(20000, pos);
    }

    @Test
    public void testPSBlobWithLength() throws SQLException, IOException {
        PreparedStatement stmt = sharedPSConnection
            .prepareStatement("insert into BlobTestblobtest2_ps (id, strm) values (?,?)");
        byte[] arr = new byte[32000];
        Random rand = new Random();
        rand.nextBytes(arr);
        InputStream stream = new ByteArrayInputStream(arr);
        stmt.setInt(1, 1);
        stmt.setBlob(2, stream, 20000);
        stmt.execute();

        // check what stream not read after length:
        int remainRead = 0;
        while (stream.read() >= 0) {
            remainRead++;
        }
        assertEquals(12000, remainRead);

        ResultSet rs = sharedPSConnection.createStatement().executeQuery(
            "select * from BlobTestblobtest2_ps");
        assertTrue(rs.next());
        InputStream readStuff = rs.getBlob("strm").getBinaryStream();
        int pos = 0;
        int ch;
        while ((ch = readStuff.read()) != -1) {
            assertEquals(arr[pos++] & 0xff, ch);
        }
        assertEquals(20000, pos);
    }

    @Test
    public void testClobWithLengthAndMultibyteCharacter() throws SQLException, IOException {
        PreparedStatement stmt = sharedConnection
            .prepareStatement("insert into BlobTestclobtest (id, strm) values (?,?)");
        String clob = "Øclob";
        stmt.setInt(1, 1);
        stmt.setClob(2, new StringReader(clob));
        stmt.execute();
        ResultSet rs = sharedConnection.createStatement().executeQuery(
            "select * from BlobTestclobtest");
        if (rs.next()) {
            Reader readStuff = rs.getClob("strm").getCharacterStream();
            char[] chars = new char[5];
            //noinspection ResultOfMethodCallIgnored
            readStuff.read(chars);
            assertEquals(new String(chars), clob);
        } else {
            fail();
        }
    }

    @Test
    public void testPSClobWithLengthAndMultibyteCharacter() throws SQLException, IOException {
        PreparedStatement stmt = sharedPSConnection
            .prepareStatement("insert into BlobTestclobtest_ps (id, strm) values (?,?)");
        String clob = "Øclob";
        stmt.setInt(1, 1);
        stmt.setClob(2, new StringReader(clob));
        stmt.execute();
        ResultSet rs = sharedPSConnection.createStatement().executeQuery(
            "select * from BlobTestclobtest_ps");
        if (rs.next()) {
            Reader readStuff = rs.getClob("strm").getCharacterStream();
            char[] chars = new char[5];
            //noinspection ResultOfMethodCallIgnored
            readStuff.read(chars);
            assertEquals(new String(chars), clob);
        } else {
            fail();
        }
    }

    @Test
    public void testClob3() throws Exception {
        PreparedStatement stmt = sharedConnection
            .prepareStatement("insert into BlobTestclobtest2 (strm) values (?)");
        Clob clob = sharedConnection.createClob();
        Writer writer = clob.setCharacterStream(1);
        writer.write("Øhello", 0, 6);
        writer.flush();
        stmt.setClob(1, clob);
        stmt.execute();
        ResultSet rs = sharedConnection.createStatement().executeQuery(
            "select * from BlobTestclobtest2");
        assertTrue(rs.next());
        assertTrue(rs.getObject(1) instanceof String);
        String result = rs.getString(1);
        assertEquals("Øhello", result);
    }

    @Ignore
    //TODO FIXME
    @Test
    public void testPSClob3() throws Exception {
        PreparedStatement stmt = sharedPSConnection
            .prepareStatement("insert into BlobTestclobtest2_ps (strm) values (?)");
        Clob clob = sharedPSConnection.createClob();
        Writer writer = clob.setCharacterStream(1);
        writer.write("Øhello", 0, 6);
        writer.flush();
        stmt.setClob(1, clob);
        stmt.execute();
        ResultSet rs = sharedPSConnection.createStatement().executeQuery(
            "select * from BlobTestclobtest2_ps");
        assertTrue(rs.next());
        assertTrue(rs.getObject(1) instanceof String);
        String result = rs.getString(1);
        assertEquals("Øhello", result);
    }

    @Test
    public void testBlob() throws SQLException, IOException {
        PreparedStatement stmt = sharedConnection
            .prepareStatement("insert into BlobTestblobtest (id, strm) values (?,?)");
        byte[] theBlob = { 1, 2, 3, 4, 5, 6 };
        InputStream stream = new ByteArrayInputStream(theBlob);
        stmt.setInt(1, 1);
        stmt.setBlob(2, stream);
        stmt.execute();
        ResultSet rs = sharedConnection.createStatement().executeQuery(
            "select * from BlobTestblobtest");
        assertTrue(rs.next());
        InputStream readStuff = rs.getBlob("strm").getBinaryStream();
        int ch;
        int pos = 0;
        while ((ch = readStuff.read()) != -1) {
            System.out.println((pos) + " --> " + ch + " and ");
            assertEquals(theBlob[pos++], ch);
        }

        readStuff = rs.getBinaryStream("strm");

        pos = 0;
        while ((ch = readStuff.read()) != -1) {
            assertEquals(theBlob[pos++], ch);
        }
    }

    @Ignore
    //TODO FIXME
    @Test
    public void testPSBlob() throws SQLException, IOException {
        PreparedStatement stmt = sharedPSConnection
            .prepareStatement("insert into BlobTestblobtest_ps (id, strm) values (?,?)");
        byte[] theBlob = { 1, 2, 3, 4, 5, 6 };
        InputStream stream = new ByteArrayInputStream(theBlob);
        stmt.setInt(1, 1);
        stmt.setBlob(2, stream);
        stmt.execute();
        ResultSet rs = sharedPSConnection.createStatement().executeQuery(
            "select * from BlobTestblobtest_ps");
        assertTrue(rs.next());
        InputStream readStuff = rs.getBlob("strm").getBinaryStream();
        int ch;
        int pos = 0;
        while ((ch = readStuff.read()) != -1) {
            System.out.println((pos) + " --> " + ch + " and ");
            assertEquals(theBlob[pos++], ch);
        }

        readStuff = rs.getBinaryStream("strm");

        pos = 0;
        while ((ch = readStuff.read()) != -1) {
            assertEquals(theBlob[pos++], ch);
        }
    }

    @Test
    public void testClobWithLength() throws SQLException, IOException {
        PreparedStatement stmt = sharedConnection
            .prepareStatement("insert into BlobTestclobtest3 (id, strm) values (?,?)");
        String clob = "clob";
        stmt.setInt(1, 1);
        stmt.setClob(2, new StringReader(clob));
        stmt.execute();
        ResultSet rs = sharedConnection.createStatement().executeQuery(
            "select * from BlobTestclobtest3");
        assertTrue(rs.next());
        Reader readStuff = rs.getClob("strm").getCharacterStream();
        char[] chars = new char[4];
        //noinspection ResultOfMethodCallIgnored
        readStuff.read(chars);
        assertEquals(new String(chars), clob);
    }

    @Ignore
    //TODO FIXME
    @Test
    public void testPSClobWithLength() throws SQLException, IOException {
        PreparedStatement stmt = sharedPSConnection
            .prepareStatement("insert into BlobTestclobtest3_ps (id, strm) values (?,?)");
        String clob = "clob";
        stmt.setInt(1, 1);
        stmt.setClob(2, new StringReader(clob));
        stmt.execute();
        ResultSet rs = sharedPSConnection.createStatement().executeQuery(
            "select * from BlobTestclobtest3_ps");
        assertTrue(rs.next());
        Reader readStuff = rs.getClob("strm").getCharacterStream();
        char[] chars = new char[4];
        //noinspection ResultOfMethodCallIgnored
        readStuff.read(chars);
        assertEquals(new String(chars), clob);
    }

    @Test
    public void testClob2() throws SQLException, IOException {
        PreparedStatement stmt = sharedConnection
            .prepareStatement("insert into BlobTestclobtest4 (id, strm) values (?,?)");
        Clob clob = sharedConnection.createClob();
        OutputStream ostream = clob.setAsciiStream(1);
        byte[] bytes = "hello".getBytes();
        ostream.write(bytes);
        stmt.setInt(1, 1);
        stmt.setClob(2, clob);
        stmt.execute();
        ResultSet rs = sharedConnection.createStatement().executeQuery(
            "select * from BlobTestclobtest4");
        assertTrue(rs.next());
        assertTrue(rs.getObject(2) instanceof String);
        assertTrue(rs.getString(2).equals("hello"));
    }

    @Ignore
    //TODO FIXME
    @Test
    public void testPSClob2() throws SQLException, IOException {
        PreparedStatement stmt = sharedPSConnection
            .prepareStatement("insert into BlobTestclobtest4_ps (id, strm) values (?,?)");
        Clob clob = sharedPSConnection.createClob();
        OutputStream ostream = clob.setAsciiStream(1);
        byte[] bytes = "hello".getBytes();
        ostream.write(bytes);
        stmt.setInt(1, 1);
        stmt.setClob(2, clob);
        stmt.execute();
        ResultSet rs = sharedPSConnection.createStatement().executeQuery(
            "select * from BlobTestclobtest4_ps");
        assertTrue(rs.next());
        assertTrue(rs.getObject(2) instanceof String);
        assertTrue(rs.getString(2).equals("hello"));
    }

    @Test
    public void blobSerialization() throws Exception {
        Blob blob = new OceanBaseBlob(new byte[] { 1, 2, 3 });
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(blob);

        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
        OceanBaseBlob blob2 = (OceanBaseBlob) ois.readObject();
        byte[] blobBytes = blob2.getBytes(1, (int) blob2.length());
        assertEquals(3, blobBytes.length);
        assertEquals(1, blobBytes[0]);
        assertEquals(2, blobBytes[1]);
        assertEquals(3, blobBytes[2]);

        Clob clob = new OceanBaseClob(new byte[] { 1, 2, 3 });
        baos = new ByteArrayOutputStream();
        oos = new ObjectOutputStream(baos);
        oos.writeObject(clob);

        ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
        OceanBaseClob c2 = (OceanBaseClob) ois.readObject();
        blobBytes = c2.getBytes(1, (int) c2.length());
        assertEquals(3, blobBytes.length);
        assertEquals(1, blobBytes[0]);
        assertEquals(2, blobBytes[1]);
        assertEquals(3, blobBytes[2]);
    }

    @Test
    public void conj73() throws Exception {
        /* CONJ-73: Assertion error: UTF8 length calculation reports invalid ut8 characters */
        Clob clob = new OceanBaseClob(new byte[] { (byte) 0x10, (byte) 0xD0, (byte) 0xA0,
                (byte) 0xe0, (byte) 0xa1, (byte) 0x8e });
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(clob);

        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
        OceanBaseClob c2 = (OceanBaseClob) ois.readObject();

        assertEquals(3, c2.length());
    }

    @Test
  public void conj77() throws Exception {

    byte[][] values = new byte[3][];
    values[0] = "".getBytes();
    values[1] = "hello".getBytes();
    values[2] = null;

    try (Statement sta1 = sharedConnection.createStatement()) {
      try {
        PreparedStatement pre =
                sharedConnection.prepareStatement(
                        "INSERT INTO conj77_test (Name,Archive) VALUES (?,?)");
        pre.setString(1, "1-Empty String");
        pre.setBytes(2, values[0]);
        //TODO to support batch()
        //pre.addBatch();
        pre.executeUpdate();

        pre.setString(1, "2-Data Hello");
        pre.setBytes(2, values[1]);
        //TODO to support batch()
        //pre.addBatch();
        pre.executeUpdate();

        pre.setString(1, "3-Empty Data null");
        pre.setBytes(2, values[2]);
        //TODO to support batch()
        //pre.addBatch();
        pre.executeUpdate();

        //TODO support batch()
        //pre.executeBatch();

      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    try (Statement sta2 = sharedConnection.createStatement()) {
      try (ResultSet set = sta2.executeQuery("Select name,archive as text FROM conj77_test")) {
        int pos = 0;
        while (set.next()) {
          final Blob blob = set.getBlob("text");
          if (blob != null) {

            try (ByteArrayOutputStream bout = new ByteArrayOutputStream((int) blob.length())) {
              try (InputStream bin = blob.getBinaryStream()) {
                final byte[] buffer = new byte[1024 * 4];
                for (int read = bin.read(buffer); read != -1; read = bin.read(buffer)) {
                  bout.write(buffer, 0, read);
                }
              }
              assertArrayEquals(bout.toByteArray(), values[pos++]);
            }
          } else {
            if (values[pos] != null) {
              Assert.assertTrue(values[pos++].length == 0);
            } else {
              assertNull(values[pos++]);
            }
          }
        }
        assertEquals(pos, 3);
      }
    }
  }

    @Test
  public void conj77PS() throws Exception {

    byte[][] values = new byte[3][];
    values[0] = "".getBytes();
    values[1] = "hello".getBytes();
    values[2] = null;

    try (Statement sta1 = sharedPSConnection.createStatement()) {
      try {
        PreparedStatement pre =
                sharedPSConnection.prepareStatement(
                        "INSERT INTO conj77_test_ps (Name,Archive) VALUES (?,?)");
        pre.setString(1, "1-Empty String");
        pre.setBytes(2, values[0]);
        //TODO to support batch()
        //pre.addBatch();
        pre.executeUpdate();

        pre.setString(1, "2-Data Hello");
        pre.setBytes(2, values[1]);
        //TODO to support batch()
        //pre.addBatch();
        pre.executeUpdate();

        pre.setString(1, "3-Empty Data null");
        pre.setBytes(2, values[2]);
        //TODO to support batch()
        //pre.addBatch();
        pre.executeUpdate();

        //TODO support batch()
        //pre.executeBatch();

      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    try (Statement sta2 = sharedPSConnection.createStatement()) {
      try (ResultSet set = sta2.executeQuery("Select name,archive as text FROM conj77_test_ps")) {
        int pos = 0;
        while (set.next()) {
          final Blob blob = set.getBlob("text");
          if (blob != null) {

            try (ByteArrayOutputStream bout = new ByteArrayOutputStream((int) blob.length())) {
              try (InputStream bin = blob.getBinaryStream()) {
                final byte[] buffer = new byte[1024 * 4];
                for (int read = bin.read(buffer); read != -1; read = bin.read(buffer)) {
                  bout.write(buffer, 0, read);
                }
              }
              assertArrayEquals(bout.toByteArray(), values[pos++]);
            }
          } else {
            if (values[pos] != null) {
              Assert.assertTrue(values[pos++].length == 0);
            } else {
              assertNull(values[pos++]);
            }
          }
        }
        assertEquals(pos, 3);
      }
    }
  }

    @Test
  public void sendEmptyBlobPreparedQuery() throws SQLException {
    createTable("emptyBlob", "test blob, test2 clob, test3 clob");
    //try (Connection conn = setConnection()) {
    //  PreparedStatement ps = conn.prepareStatement("insert into emptyBlob values(?,?,?)");
    try (PreparedStatement ps = sharedConnection.prepareStatement("insert into emptyBlob values(?,?,?)")) {
      ps.setBlob(1, new OceanBaseBlob(new byte[0]));
      ps.setString(2, "a 'a ");
      ps.setNull(3, Types.VARCHAR);
      ps.executeUpdate();

      Statement stmt = sharedConnection.createStatement();
      ResultSet rs = stmt.executeQuery("select * from emptyBlob");
      assertTrue(rs.next());
      //assertEquals(0, rs.getBytes(1).length);
      assertEquals("a 'a ", rs.getString(2));
      assertNull(rs.getBytes(3));
    }
  }

    @Test
  public void sendEmptyBlobPreparedQueryPS() throws SQLException {
    createTable("emptyBlob_ps", "test blob, test2 clob, test3 clob");
    //try (Connection conn = setConnection()) {
    //  PreparedStatement ps = conn.prepareStatement("insert into emptyBlob values(?,?,?)");
    try (PreparedStatement ps = sharedPSConnection.prepareStatement("insert into emptyBlob_ps values(?,?,?)")) {
      ps.setBlob(1, new OceanBaseBlob(new byte[0]));
      ps.setString(2, "a 'a ");
      ps.setNull(3, Types.VARCHAR);
      ps.executeUpdate();

      Statement stmt = sharedPSConnection.createStatement();
      ResultSet rs = stmt.executeQuery("select * from emptyBlob_ps");
      assertTrue(rs.next());
      //assertEquals(0, rs.getBytes(1).length);
      assertEquals("a 'a ", rs.getString(2));
      assertNull(rs.getBytes(3));
    }
  }

    @Test
    public void blobSerializationWithOffset() throws Exception {
        Blob blob = new OceanBaseBlob(new byte[] { 1, 2, 3, 4, 5 }, 1, 2);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(blob);

        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
        OceanBaseBlob blob2 = (OceanBaseBlob) ois.readObject();
        byte[] blobBytes = blob2.getBytes(1, (int) blob2.length());
        assertEquals(2, blobBytes.length);
        assertEquals(2, blobBytes[0]);
        assertEquals(3, blobBytes[1]);

        Clob clob = new OceanBaseClob(new byte[] { 1, 2, 3, 4, 5 }, 1, 2);
        baos = new ByteArrayOutputStream();
        oos = new ObjectOutputStream(baos);
        oos.writeObject(clob);

        ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
        OceanBaseClob c2 = (OceanBaseClob) ois.readObject();
        blobBytes = c2.getBytes(1, (int) c2.length());
        assertEquals(2, blobBytes.length);
        assertEquals(2, blobBytes[0]);
        assertEquals(3, blobBytes[1]);
    }

    @Test
    public void blobDeserializationFilter() throws Exception {
        blobDeserializationFilterInternal(false, 0);
        if (System.getProperty("java.version").startsWith("9.")) {
            blobDeserializationFilterInternal(true, 2000);
            try {
                blobDeserializationFilterInternal(true, 500);
                fail("must have thrown exception, since filter limit is set lower than blob size");
            } catch (InvalidClassException e) {
                assertTrue(e.getMessage().contains("REJECTED"));
            }
        }
    }

    private void blobDeserializationFilterInternal(boolean addFilter, int filterSize)
      throws Exception {
    Assume.assumeTrue(System.getProperty("java.version").startsWith("9."));
    byte[] bb = new byte[1000];
    for (int i = 0; i < 1000; i++) {
      bb[i] = (byte) i;
    }
    OceanBaseBlob blob = new OceanBaseBlob(bb, 50, 750);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(bos);
    oos.writeObject(blob);
    oos.flush();
    oos.close();
    bos.close();

    byte[] data = bos.toByteArray();

    ByteArrayInputStream bis = new ByteArrayInputStream(data);
    ObjectInputStream ois = new ObjectInputStream(bis);

    if (addFilter) {
      // equivalent of :  (but permit compilation if java < 9)
      // ois.setObjectInputFilter(new ObjectInputFilter() {
      //  @Override
      //  public Status checkInput(FilterInfo filterInfo) {
      //      if (filterInfo.arrayLength() > 500) return Status.REJECTED;
      //      return Status.ALLOWED;
      //  }
      // });

      ClassLoader cl = BlobOracleTest.class.getClassLoader();
      Class<?> objectInputFilterClass = Class.forName("java.io.ObjectInputFilter");

      Object objectInputFilterImpl =
          Proxy.newProxyInstance(
              cl,
              new Class[] {objectInputFilterClass},
              (proxy, method, args) -> {
                Class<?> filterInfoClass = Class.forName("java.io.ObjectInputFilter$FilterInfo");
                Method arrayLengthMethod = filterInfoClass.getDeclaredMethod("arrayLength");
                Long arrayLength = (Long) arrayLengthMethod.invoke(args[0]);
                Class<?> statusClass = Class.forName("java.io.ObjectInputFilter$Status");
                Field rejected = statusClass.getField("REJECTED");
                Field allowed = statusClass.getField("ALLOWED");
                if (arrayLength > filterSize) {
                  return rejected.get(null);
                }
                return allowed.get(null);
              });

      Method setObjectInputFilterMethod =
          ObjectInputStream.class.getDeclaredMethod("setObjectInputFilter", objectInputFilterClass);
      setObjectInputFilterMethod.invoke(ois, objectInputFilterImpl);
    }

    OceanBaseBlob resultBlob = (OceanBaseBlob) ois.readObject();
    assertEquals(750, resultBlob.data.length);
    assertEquals(0, resultBlob.offset);
    assertEquals(750, resultBlob.length);

    byte[] blobBytes = resultBlob.getBytes(1, 1000);
    for (int i = 0; i < 750; i++) {
      assertEquals(bb[i + 50], blobBytes[i]);
    }
  }

    @Test
    public void connectionBlob() throws SQLException {
        Blob blob = sharedConnection.createBlob();
        assertArrayEquals(new byte[] { 0 }, blob.getBytes(1, 1));
        blob.setBytes(5, new byte[] { 1, 2, 3, 4, 5, 6 });
        assertArrayEquals(new byte[] { 0, 0, 0, 0, 1, 2, 3, 4, 5 }, blob.getBytes(1, 9));
    }

    @Test
    public void connectionBlobPS() throws SQLException {
        Blob blob = sharedPSConnection.createBlob();
        assertArrayEquals(new byte[] { 0 }, blob.getBytes(1, 1));
        blob.setBytes(5, new byte[] { 1, 2, 3, 4, 5, 6 });
        assertArrayEquals(new byte[] { 0, 0, 0, 0, 1, 2, 3, 4, 5 }, blob.getBytes(1, 9));
    }
}
