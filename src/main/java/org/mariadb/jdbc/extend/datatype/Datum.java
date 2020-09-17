package org.mariadb.jdbc.extend.datatype;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Calendar;

public abstract class Datum implements Serializable {
  private byte[] data;
  static final long serialVersionUID = 4645732484621936751L;
  static final String dbTimeZone = "GMT+00:00";

  public Datum() {}

  public static String getSessionTimeZone(Connection connection) throws SQLException {
    /* FIXME
    if (connection instanceof MariaDbConnection) {
        return ((MariaDbConnection) connection).getSessionTimeZone();
    } else {
        throw new SQLException("unexpected connection type");
    }
     */
    return null;
  }

  protected static final int getInt(byte[] bytes, int idx) {
    int value = (bytes[idx + 3] & 255) << 24;
    value |= (bytes[idx + 2] & 255) << 16;
    value |= (bytes[idx + 1] & 255) << 8;
    value |= bytes[idx] & 255;
    return value;
  }

  protected static byte[] intToBytes(int val) {
    byte[] b = new byte[4];
    b[0] = (byte) (val & 0xff);
    b[1] = (byte) ((val >> 8) & 0xff);
    b[2] = (byte) ((val >> 16) & 0xff);
    b[3] = (byte) ((val >> 24) & 0xff);
    return b;
  }

  public Datum(byte[] bytes) {
    this.data = bytes;
  }

  public boolean equals(Object object) {
    if (this == object) {
      return true;
    } else if (object != null && object instanceof Datum) {
      if (this.getClass() == object.getClass()) {
        Datum datum = (Datum) object;
        if (this.data == null && datum.data == null) {
          return true;
        } else if (this.data == null && datum.data != null
            || this.data != null && datum.data == null) {
          return false;
        } else if (this.data.length != datum.data.length) {
          return false;
        } else {
          for (int i = 0; i < this.data.length; ++i) {
            if (this.data[i] != datum.data[i]) {
              return false;
            }
          }

          return true;
        }
      } else {
        return false;
      }
    } else {
      return false;
    }
  }

  public byte[] shareBytes() {
    return this.data;
  }

  public long getLength() {
    return null == this.data ? 0L : (long) this.data.length;
  }

  public void setBytes(byte[] bytes) {
    int len = bytes.length;
    this.data = new byte[len];
    System.arraycopy(bytes, 0, this.data, 0, len);
  }

  public void setShareBytes(byte[] bytes) {
    this.data = bytes;
  }

  public byte[] getBytes() {
    if (this.data == null) {
      return new byte[0];
    } else {
      byte[] bytes = new byte[this.data.length];
      System.arraycopy(this.data, 0, bytes, 0, this.data.length);
      return bytes;
    }
  }

  public void setByte(int index, byte b) {
    if (index < this.data.length) {
      this.data[index] = b;
    }
  }

  public InputStream getStream() {
    return new ByteArrayInputStream(this.data);
  }

  public String stringValue() throws SQLException {
    throw new SQLException("Conversion to String failed");
  }

  public String stringValue(Connection var1) throws SQLException {
    return this.stringValue();
  }

  public boolean booleanValue() throws SQLException {
    throw new SQLException("Conversion to boolean failed");
  }

  public int intValue() throws SQLException {
    throw new SQLException("Conversion to integer failed");
  }

  public long longValue() throws SQLException {
    throw new SQLException("Conversion to long failed");
  }

  public float floatValue() throws SQLException {
    throw new SQLException("Conversion to float failed");
  }

  public double doubleValue() throws SQLException {
    throw new SQLException("Conversion to double failed");
  }

  public byte byteValue() throws SQLException {
    throw new SQLException("Conversion to byte failed");
  }

  public BigDecimal bigDecimalValue() throws SQLException {
    throw new SQLException("Conversion to BigDecimal failed");
  }

  public Date dateValue() throws SQLException {
    throw new SQLException("Conversion to Date failed");
  }

  public Time timeValue() throws SQLException {
    throw new SQLException("Conversion to Time failed");
  }

  public Time timeValue(Calendar var1) throws SQLException {
    throw new SQLException("Conversion to Time failed");
  }

  public Timestamp timestampValue() throws SQLException {
    throw new SQLException("Conversion to Timestamp failed");
  }

  public Timestamp timestampValue(Calendar var1) throws SQLException {
    throw new SQLException("Conversion to Timestamp failed");
  }

  public Reader characterStreamValue() throws SQLException {
    throw new SQLException("Conversion to character stream failed");
  }

  public InputStream asciiStreamValue() throws SQLException {
    throw new SQLException("Conversion to ascii stream failed");
  }

  public InputStream binaryStreamValue() throws SQLException {
    throw new SQLException("Conversion to binary stream failed");
  }

  public abstract boolean isConvertibleTo(Class var1);

  public abstract Object toJdbc() throws SQLException;

  public abstract Object makeJdbcArray(int var1);
}
