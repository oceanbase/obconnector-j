package com.alipay.oceanbase.jdbc.extend.datatype;

import java.security.InvalidParameterException;
import java.sql.SQLException;

public class INTERVALYM extends Datum {
    private static int MASKVAL             = 255;
    private static int INTYMYEAROFFSET     = -2147483648;
    private static int INTYMMONTHOFFSET    = 60;
    private static int INTERVALYMMAXLENGTH = 5;
    private static int MAXYEARPREC         = 9;
    private static int MAXMONTH            = 12;

    private static int INTERVALYM_BYTE_NUM = 7;
    static final long  serialVersionUID    = 8393284561907159296L;

    public INTERVALYM() {
        super(toBytes("0-0"));
    }

    public INTERVALYM(byte[] bytes) {
        super(bytes);
    }

    public INTERVALYM(String str) {
        super(toBytes(str));
    }

    //+0123-02
    public static byte[] toBytes(String var) {
        if (var == null) {
            return null;
        } else {
            byte[] intervalymBytes = new byte[INTERVALYM_BYTE_NUM];
            String trimStr = var.trim();
            char ch = var.charAt(0);
            int is_negative = 0;
            int year = 0;
            int month = 0;
            int pos = 0;
            if (ch != '-' && ch != '+') {
                pos = 0;
            } else {
                pos = 1;
                if (ch == '-') {
                    is_negative = 1;
                }
            }
            String subStr = trimStr.substring(pos);
            int index = subStr.indexOf('-'); //find pos of blanket
            String yearStr = subStr.substring(0, index);
            if (yearStr.length() > MAXYEARPREC) {
                throw new NumberFormatException("invalid year " + yearStr + " in " + var);
            } else {
                String monthStr = subStr.substring(index + 1);
                year = Integer.valueOf(yearStr);
                month = Integer.valueOf(monthStr);
                if (month > MAXMONTH) {
                    throw new NumberFormatException("invalid month " + month + " in " + var);
                }
                intervalymBytes[0] = (byte) (is_negative & 255);
                byte[] tmpBytes = intToBytes(year);
                System.arraycopy(tmpBytes, 0, intervalymBytes, 1, 4);
                intervalymBytes[5] = (byte) (month & 255);
                intervalymBytes[6] = (byte) (yearStr.length());
            }
            return intervalymBytes;
        }
    }

    public static String toString(byte[] data) {
        if (data != null && data.length != 0) {
            if (data.length != INTERVALYM_BYTE_NUM) {
                throw new InvalidParameterException("invalid len:" + data.length);
            }
            int is_negative = data[0] & MASKVAL;
            int year = getInt(data, 1);
            int month = data[5] & MASKVAL;
            int year_scale = data[6] & MASKVAL;
            if (year < 0 || year > Math.pow(10, MAXYEARPREC)) {
                throw new NumberFormatException("year should not exceed "
                                                + Math.pow(10, MAXYEARPREC) + ", now is " + year);
            }
            if (month < 0 || month > MAXMONTH) {
                throw new NumberFormatException("month should not exceed " + MAXMONTH + ", now is "
                                                + month);
            }
            String format = String.format("%%0%dd-%%02d", year_scale);
            String result = String.format(format, year, month);
            if (is_negative == 0) {
                return "+" + result;
            } else {
                return "-" + result;
            }
        }
        return new String();
    }

    public String stringValue() {
        return toString(this.getBytes());
    }

    @Override
    public boolean isConvertibleTo(Class var1) {
        return var1.getName().compareTo("java.lang.String") == 0;
    }

    @Override
    public Object toJdbc() throws SQLException {
        return this;
    }

    @Override
    public Object makeJdbcArray(int var1) {
        INTERVALYM[] object = new INTERVALYM[var1];
        return object;
    }

    public String toString() {
        return toString(this.getBytes());
    }
}
