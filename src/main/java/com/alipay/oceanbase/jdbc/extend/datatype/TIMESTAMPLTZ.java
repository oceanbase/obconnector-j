package com.alipay.oceanbase.jdbc.extend.datatype;

import java.sql.*;
import java.util.Calendar;
import java.util.TimeZone;

import com.alipay.oceanbase.jdbc.OceanBaseConnection;
import com.alipay.oceanbase.jdbc.internal.protocol.Protocol;

public class TIMESTAMPLTZ extends Datum {
    private static int      SIZE_TIMESTAMPLTZ        = 12;
    private static int      SIZE_TIMESTAMPLTZ_NOFRAC = 7;
    private static int      SIZE_DATE                = 7;
    private static int      HOUR_MILLISECOND         = 3600000;
    private static int      MINUTE_MILLISECOND       = 60000;
    private static int      JAVA_YEAR                = 1970;
    private static int      JAVA_MONTH               = 0;
    private static int      JAVA_DATE                = 1;
    private static int      MINYEAR                  = -4712;
    private static int      MAXYEAR                  = 9999;
    private static boolean  cached                   = false;
    private static Calendar dbtz;
    static final long       serialVersionUID         = 2045880772054757133L;

    public TIMESTAMPLTZ() {
        super(initTimestampltz());
    }

    public TIMESTAMPLTZ(byte[] var1) {
        super(var1);
    }

    public TIMESTAMPLTZ(Connection connection, Time time) throws SQLException {
        super(toBytes(connection, time));
    }

    public TIMESTAMPLTZ(Connection connection, Date date) throws SQLException {
        super(toBytes(connection, date));
    }

    public TIMESTAMPLTZ(Connection connection, Timestamp timestamp) throws SQLException {
        super(toBytes(connection, timestamp));
    }

    /*
     * @return original timestamp from ob server,
     * which is formatted with session timezone,
     * other than local timezone
     */
    public static String toString(Connection connection, byte[] bytes) throws SQLException {
        if (bytes.length < 12) {
            throw new SQLException("invalid bytes length");
        }

        Calendar calendar = Calendar.getInstance(TimeZone
            .getTimeZone(((OceanBaseConnection) connection).getSessionTimeZone().getID()));
        calendar.setTimeInMillis(TIMESTAMPTZ.getOriginTime(bytes,
            TimeZone.getTimeZone(((OceanBaseConnection) connection).getSessionTimeZone().getID())));
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH) + 1;
        int day = calendar.get(Calendar.DAY_OF_MONTH);
        int hour = calendar.get(Calendar.HOUR_OF_DAY);
        int minute = calendar.get(Calendar.MINUTE);
        int second = calendar.get(Calendar.SECOND);
        int nanos = TIMESTAMP.getNanos(bytes, 7);

        return TIMESTAMPTZ.toString(year, month, day, hour, minute, second, nanos, bytes[11],
            getSessionTimeZone(connection));
    }

    public String toResultSetString(Connection connection) throws SQLException {
        return toString(connection, getBytes());
    }

    public byte[] toBytes() {
        return this.getBytes();
    }

    public static byte[] toBytes(Connection connection, Time time) throws SQLException {
        if (time == null) {
            return null;
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(time);
        short base = 1970;

        calendar.set(Calendar.YEAR, base);
        calendar.set(Calendar.MONTH, 0);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        byte[] bytes = toBytes(connection, calendar, 0);
        return bytes;
    }

    public static byte[] toBytes(Connection connection, Date date) throws SQLException {
        if (date == null) {
            return null;
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        byte[] result = toBytes(connection, calendar, 0);
        return result;
    }

    public static byte[] toBytes(Connection connection, Timestamp timestamp) throws SQLException {
        if (timestamp == null) {
            return null;
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(timestamp);
        int nanos = timestamp.getNanos();
        byte[] bytes = toBytes(connection, calendar, nanos);
        return bytes;
    }

    public static byte[] toBytes(Connection connection, String time) throws SQLException {
        return toBytes(connection, Timestamp.valueOf(time));
    }

    public static Date toDate(Connection connection, byte[] bytes) throws SQLException {
        return new Date(TIMESTAMPTZ.getOriginTime(bytes,
            TimeZone.getTimeZone(((OceanBaseConnection) connection).getSessionTimeZone().getID())));
    }

    public static Time toTime(Connection connection, byte[] bytes) throws SQLException {
        return new Time(TIMESTAMPTZ.getOriginTime(bytes,
            TimeZone.getTimeZone(((OceanBaseConnection) connection).getSessionTimeZone().getID())));
    }

    public static Timestamp toTimestamp(Connection connection, byte[] bytes) throws SQLException {
        if (bytes.length < 12) {
            throw new SQLException("invalid bytes length");
        }
        Timestamp timestamp = new Timestamp(TIMESTAMPTZ.getOriginTime(bytes,
            TimeZone.getTimeZone(((OceanBaseConnection) connection).getSessionTimeZone().getID())));
        timestamp.setNanos(TIMESTAMP.getNanos(bytes, 7));
        return timestamp;
    }

    public static Timestamp toTimestamp(Protocol protocol, byte[] bytes) throws SQLException {
        if (bytes.length < 12) {
            throw new SQLException("invalid bytes length");
        }
        Timestamp timestamp = new Timestamp(TIMESTAMPTZ.getOriginTime(bytes,
            TimeZone.getTimeZone(protocol.getTimeZone().getID())));
        timestamp.setNanos(TIMESTAMP.getNanos(bytes, 7));
        return timestamp;
    }

    public static TIMESTAMP toTIMESTAMP(Connection connection, byte[] bytes) throws SQLException {
        return new TIMESTAMP(toTimestamp(connection, bytes));
    }

    public static TIMESTAMP toTIMESTAMP(Protocol protocol, byte[] bytes) throws SQLException {
        return new TIMESTAMP(toTimestamp(protocol, bytes));
    }

    public static TIMESTAMPTZ toTIMESTAMPTZ(Connection connection, byte[] bytes)
                                                                                throws SQLException {
        return new TIMESTAMPTZ(connection, toTimestamp(connection, bytes), Calendar.getInstance());
    }

    public Timestamp timestampValue(Connection connection) throws SQLException {
        return toTimestamp(connection, getBytes());
    }

    public String stringValue(Connection connection) throws SQLException {
        return toString(connection, this.getBytes());
    }

    public Date dateValue(Connection connection) throws SQLException {
        return toDate(connection, this.getBytes());
    }

    public Time timeValue(Connection connection) throws SQLException {
        return toTime(connection, this.getBytes());
    }

    public Object toJdbc() throws SQLException {
        return null;
    }

    public Object makeJdbcArray(int time) {
        Timestamp[] timestamps = new Timestamp[time];
        return timestamps;
    }

    public boolean isConvertibleTo(Class clazz) {
        return clazz.getName().compareTo("java.sql.Date") == 0
               || clazz.getName().compareTo("java.sql.Time") == 0
               || clazz.getName().compareTo("java.sql.Timestamp") == 0
               || clazz.getName().compareTo("java.lang.String") == 0;
    }

    private static byte[] initTimestampltz() {
        byte[] result = new byte[SIZE_TIMESTAMPLTZ];
        result[0] = 19;
        result[1] = 70;
        result[2] = 1;
        result[3] = 1;
        result[4] = 1;
        result[5] = 1;
        result[6] = 1;
        TIMESTAMP.setNanos(result, 7, 0);
        result[11] = 0;
        return result;
    }

    @SuppressWarnings("unused")
    private static byte[] toBytes(Connection connection, Calendar calendar, int nanos)
                                                                                      throws SQLException {
        byte[] result = new byte[SIZE_TIMESTAMPLTZ];

        int year = calendar.get(Calendar.YEAR);
        if (year >= MINYEAR && year <= MAXYEAR) {
            result[0] = (byte) (calendar.get(Calendar.YEAR) / 100);
            result[1] = (byte) (calendar.get(Calendar.YEAR) % 100);
            result[2] = (byte) (calendar.get(Calendar.MONTH) + 1);
            result[3] = (byte) calendar.get(Calendar.DAY_OF_MONTH);
            result[4] = (byte) (calendar.get(Calendar.HOUR_OF_DAY));
            result[5] = (byte) (calendar.get(Calendar.MINUTE));
            result[6] = (byte) (calendar.get(Calendar.SECOND));
            TIMESTAMP.setNanos(result, 7, nanos);
            String temp = String.format("%09d", nanos);
            char[] chars = temp.toCharArray();

            int index;
            for (index = chars.length; index > 1 && chars[index - 1] == '0'; --index) {
                ;
            }

            temp = temp.substring(0, index);
            String nanosStr = String.valueOf(temp);

            result[11] = (byte) nanosStr.length();

            return result;
        } else {
            throw new SQLException("error format", "268");
        }
    }
}
