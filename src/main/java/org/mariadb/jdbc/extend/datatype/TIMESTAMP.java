package org.mariadb.jdbc.extend.datatype;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

public class TIMESTAMP extends Datum implements Serializable {
  static final int[] daysInMonth = new int[] {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  static final long serialVersionUID = -7964732752952728545L;

  public static int SIZE_TIMESTAMP = 12;

  public TIMESTAMP() {
    super(initTimestamp());
  }

  public TIMESTAMP(byte[] bytes) {
    super(bytes);
  }

  public TIMESTAMP(Time time) {
    super(toBytes(time));
  }

  public TIMESTAMP(Date date) {
    super(toBytes(date));
  }

  public TIMESTAMP(Timestamp timestamp) {
    super(toBytes(timestamp));
  }

  public TIMESTAMP(Timestamp timestamp, Calendar calendar) {
    super(toBytes(timestamp, calendar));
  }

  public static final int getNanos(byte[] bytes, int idx) {
    int nanos = (bytes[idx + 3] & 255) << 24;
    nanos |= (bytes[idx + 2] & 255) << 16;
    nanos |= (bytes[idx + 1] & 255) << 8;
    nanos |= bytes[idx] & 255;
    return nanos;
  }

  public static final void setNanos(byte[] bytes, int idx, int nanos) {
    bytes[idx + 3] = (byte) ((nanos >> 24) & 0xff);
    bytes[idx + 2] = (byte) ((nanos >> 16) & 0xff);
    bytes[idx + 1] = (byte) ((nanos >> 8) & 0xff);
    bytes[idx] = (byte) (nanos & 0xff);
  }

  public TIMESTAMP(String str) {
    super(toBytes(str));
  }

  public static Date toDate(byte[] bytes) throws SQLException {
    int[] result = new int[SIZE_TIMESTAMP];

    int i;
    for (i = 0; i < bytes.length; ++i) {
      result[i] = bytes[i] & 255;
    }

    i = result[0] * 100 + result[1];
    Calendar calendar = Calendar.getInstance();
    calendar.set(1, i);
    calendar.set(2, result[2] - 1);
    calendar.set(5, result[3]);
    calendar.set(11, result[4] - 1);
    calendar.set(12, result[5] - 1);
    calendar.set(13, result[6] - 1);
    calendar.set(14, 0);
    long time = calendar.getTime().getTime();
    return new Date(time);
  }

  public static Time toTime(byte[] bytes) throws SQLException {
    int hour = bytes[4] & 255;
    int minute = bytes[5] & 255;
    int second = bytes[6] & 255;
    return new Time(hour, minute, second);
  }

  public static Timestamp toTimestamp(byte[] bytes) throws SQLException {
    return innerToTimestamp(bytes, null);
  }

  private static Timestamp innerToTimestamp(byte[] bytes, Calendar cal) throws SQLException {
    int[] result = new int[SIZE_TIMESTAMP];

    int i;
    for (i = 0; i < bytes.length; ++i) {
      result[i] = bytes[i] & 255;
    }

    i = result[0] * 100 + result[1];
    Calendar calendar = cal;
    if (null == calendar) {
      calendar = Calendar.getInstance();
    }
    calendar.set(Calendar.YEAR, i);
    calendar.set(Calendar.MONTH, result[2] - 1);
    calendar.set(Calendar.DAY_OF_MONTH, result[3]);
    calendar.set(Calendar.HOUR_OF_DAY, result[4]);
    calendar.set(Calendar.MINUTE, result[5]);
    calendar.set(Calendar.SECOND, result[6]);
    calendar.set(Calendar.MILLISECOND, 0);
    long time = calendar.getTime().getTime();
    Timestamp timestamp = new Timestamp(time);
    timestamp.setNanos(getNanos(bytes, 7));
    return timestamp;
  }

  public static Timestamp toTimestamp(byte[] bytes, Calendar calendar) throws SQLException {
    return innerToTimestamp(bytes, calendar);
  }

  public Timestamp timestampValue() throws SQLException {
    return toTimestamp(this.getBytes());
  }

  public Timestamp timestampValue(Calendar var1) throws SQLException {
    return toTimestamp(this.getBytes(), var1);
  }

  public static String toString(byte[] bytes) {
    int[] var1 = new int[bytes.length];

    int i;
    for (i = 0; i < bytes.length; ++i) {
      var1[i] = bytes[i] & 255;
    }

    i = var1[0] * 100 + var1[1];
    int month = var1[2];
    int day = var1[3];
    int hour = var1[4];
    int minute = var1[5];
    int second = var1[6];
    int nanos = getNanos(bytes, 7);

    return toString(
        i, month, day, hour, minute, second, nanos, bytes[11],
        null); // FIXME duplicate from TIMESTAMPTZ
  }

  public static String toString(
      int year,
      int month,
      int day,
      int hour,
      int minute,
      int second,
      int nanos,
      int scale,
      String timezone) {
    String time =
        ""
            + year
            + "-"
            + toStr(month)
            + "-"
            + toStr(day)
            + " "
            + toStr(hour)
            + ":"
            + toStr(minute)
            + ":"
            + toStr(second);
    if (nanos >= 0) {
      String temp = String.format("%09d", nanos);
      char[] chars = temp.toCharArray();

      int index;
      for (index = chars.length; index > 1 && chars[index - 1] == '0'; --index) {;
      }
      temp = temp.substring(0, index);
      if (scale > temp.length()) {
        int x = scale - temp.length();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < x; i++) {
          sb.append("0");
        }
        temp += sb.toString();
      }

      time = time + "." + temp;
    }

    if (timezone != null) {
      time = time + " " + timezone;
    }

    return time;
  }

  private static String toStr(int temp) {
    return temp < 10 ? "0" + temp : Integer.toString(temp);
  }

  public byte[] toBytes() {
    return this.getBytes();
  }

  public static byte[] toBytes(Time time) {
    if (time == null) {
      return null;
    } else {
      byte[] result = new byte[SIZE_TIMESTAMP];
      Calendar var2 = Calendar.getInstance();
      var2.setTime(time);
      result[0] = 19;
      result[1] = 70;
      result[2] = 1;
      result[3] = 1;
      result[4] = (byte) (var2.get(Calendar.HOUR_OF_DAY));
      result[5] = (byte) (var2.get(Calendar.MINUTE));
      result[6] = (byte) (var2.get(Calendar.SECOND));
      setNanos(result, 7, 0);
      result[11] = 0;
      return result;
    }
  }

  public static byte[] toBytes(Date date) {
    if (date == null) {
      return null;
    } else {
      byte[] result = new byte[SIZE_TIMESTAMP];
      Calendar calendar = Calendar.getInstance();
      calendar.setTime(date);
      result[0] = (byte) (calendar.get(Calendar.YEAR) / 100);
      result[1] = (byte) (calendar.get(Calendar.YEAR) % 100);
      result[2] = (byte) (calendar.get(Calendar.MONTH) + 1);
      result[3] = (byte) calendar.get(Calendar.DAY_OF_MONTH);
      result[4] = 0;
      result[5] = 0;
      result[6] = 0;
      setNanos(result, 7, 0);
      result[11] = 0;
      return result;
    }
  }

  public static byte[] toBytes(Timestamp timestamp) {
    if (timestamp == null) {
      return null;
    } else {
      int nanos = timestamp.getNanos();
      byte[] result = new byte[SIZE_TIMESTAMP];

      Calendar calendar = Calendar.getInstance();
      calendar.setTime(timestamp);
      result[0] = (byte) (calendar.get(Calendar.YEAR) / 100);
      result[1] = (byte) (calendar.get(Calendar.YEAR) % 100);
      result[2] = (byte) (calendar.get(Calendar.MONTH) + 1);
      result[3] = (byte) calendar.get(Calendar.DAY_OF_MONTH);
      result[4] = (byte) (calendar.get(Calendar.HOUR_OF_DAY));
      result[5] = (byte) (calendar.get(Calendar.MINUTE));
      result[6] = (byte) (calendar.get(Calendar.SECOND));
      setNanos(result, 7, nanos);

      return result;
    }
  }

  public static byte[] toBytes(Timestamp timestamp, Calendar calendar) {
    if (timestamp == null) {
      return null;
    } else {
      int nanos = timestamp.getNanos();
      byte[] result = new byte[SIZE_TIMESTAMP];

      if (calendar == null) {
        calendar = Calendar.getInstance();
      }

      calendar.clear();
      calendar.setTime(timestamp);
      int var4 = calendar.get(Calendar.YEAR);
      if (calendar.get(Calendar.ERA) == 0) {
        var4 = -(var4 - 1);
      }

      if (var4 >= -4712 && var4 <= 9999) {
        result[0] = (byte) (var4 / 100);
        result[1] = (byte) (var4 % 100);
        result[2] = (byte) (calendar.get(Calendar.MONTH) + 1);
        result[3] = (byte) calendar.get(Calendar.DAY_OF_MONTH);
        result[4] = (byte) (calendar.get(Calendar.HOUR_OF_DAY));
        result[5] = (byte) (calendar.get(Calendar.MINUTE));
        result[6] = (byte) (calendar.get(Calendar.SECOND));
        setNanos(result, 7, nanos);

        return result;
      } else {
        throw new IllegalArgumentException("Invalid year value");
      }
    }
  }

  public static byte[] toBytes(String str) {
    return toBytes(Timestamp.valueOf(str));
  }

  public Object toJdbc() throws SQLException {
    return this.timestampValue();
  }

  public Object makeJdbcArray(int len) {
    Timestamp[] timestamps = new Timestamp[len];
    return timestamps;
  }

  public boolean isConvertibleTo(Class clazz) {
    return clazz.getName().compareTo("java.sql.Date") == 0
        || clazz.getName().compareTo("java.sql.Time") == 0
        || clazz.getName().compareTo("java.sql.Timestamp") == 0
        || clazz.getName().compareTo("java.lang.String") == 0;
  }

  public String stringValue() {
    return toString(this.getBytes());
  }

  public String toString() {
    return this.stringValue();
  }

  public Date dateValue() throws SQLException {
    return toDate(this.getBytes());
  }

  public Time timeValue() throws SQLException {
    return toTime(this.getBytes());
  }

  private static byte[] initTimestamp() {
    byte[] bytes = new byte[SIZE_TIMESTAMP];
    bytes[0] = 19;
    bytes[1] = 70;
    bytes[2] = 1;
    bytes[3] = 1;
    bytes[4] = 0;
    bytes[5] = 0;
    bytes[6] = 0;
    setNanos(bytes, 7, 0);
    bytes[11] = 0;
    return bytes;
  }

  private boolean isLeapYear(int time) {
    boolean isLeapYear;
    label:
    {
      if (time % 4 == 0) {
        if (time <= 1582) {
          if (time != -4712) {
            break label;
          }
        } else if (time % 100 != 0 || time % 400 == 0) {
          break label;
        }
      }

      isLeapYear = false;
      return isLeapYear;
    }

    isLeapYear = true;
    return isLeapYear;
  }

  private boolean isValid() {
    byte[] bytes = this.getBytes();
    if (bytes.length < SIZE_TIMESTAMP) {
      return false;
    } else {
      int year = (bytes[0] & 255) * 100 + (bytes[1] & 255);
      if (year >= -4712 && year <= 9999) {
        if (year == 0) {
          return false;
        } else {
          int month = bytes[2] & 255;
          if (month >= 1 && month <= 12) {
            int day = bytes[3] & 255;
            if (day >= 1 && day <= 31) {
              if (day > daysInMonth[month - 1]
                  && (!this.isLeapYear(year) || month != 2 || day != 29)) {
                return false;
              } else if (year == 1582 && month == 10 && day >= 5 && day < 15) {
                return false;
              } else {
                int hour = bytes[4] & 255;
                if (hour >= 1 && hour <= 24) {
                  int second = bytes[5] & 255;
                  if (second >= 1 && second <= 60) {
                    int temp = bytes[6] & 255;
                    return temp >= 1 && temp <= 60;
                  } else {
                    return false;
                  }
                } else {
                  return false;
                }
              }
            } else {
              return false;
            }
          } else {
            return false;
          }
        }
      } else {
        return false;
      }
    }
  }

  private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
    stream.defaultReadObject();
    if (!this.isValid()) {
      throw new IOException("Invalid TIMESTAMP");
    }
  }
}
