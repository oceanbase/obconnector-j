/*
 *
 * OceanBase Client for Java
 *
 * Copyright (c) 2021 OceanBase Technology Co.,Ltd.
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
package com.alipay.oceanbase.jdbc.extend.datatype;

import java.security.InvalidParameterException;
import java.sql.SQLException;
import java.util.StringTokenizer;

/**
 *  datatype name INTERVAL DAY TO SECOND
 */
public class INTERVALDS extends Datum {
    private static int       MAXLEADPREC         = 9;
    private static int       MAXHOUR             = 23;
    private static int       MAXMINUTE           = 59;
    private static int       MAXSECOND           = 59;
    private final static int INTERVALDS_BYTE_NUM = 14;
    private static int       INTERVALDSMAXLENGTH = 11;
    private static int       INTERVALDSOFFSET    = 60;
    private static int       INTERVALDAYOFFSET   = -2147483648;

    static final long        serialVersionUID    = 7164731704878764759L;

    public INTERVALDS() {
        super(toBytes("0 0:0:0.0"));
    }

    public INTERVALDS(byte[] bytes) {
        super(bytes);
    }

    /**
     *
     * @param var:+002324 05:12:10.21200
     */
    public INTERVALDS(String var) {
        super(toBytes(var));
    }

    public static byte[] toBytes(String var) {
        if (var == null) {
            return null;
        } else {
            byte[] intervaldsBytes = new byte[INTERVALDS_BYTE_NUM];
            String trimStr = var.trim();
            char ch = var.charAt(0);
            int is_negative = 0;
            int day = 0;
            int hour = 0;
            int minute = 0;
            int second = 0;
            int fractional_second = 0;
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
            int index = subStr.indexOf(32); //find pos of blanket
            String dayStr = subStr.substring(0, index);
            if (dayStr.length() > MAXLEADPREC) {
                throw new NumberFormatException("invalid daylen " + dayStr);
            } else {
                day = Integer.valueOf(dayStr);
                subStr = subStr.substring(index + 1);
                StringTokenizer tokenizer = new StringTokenizer(subStr, ":.");
                if (tokenizer.hasMoreTokens()) {
                    String hourStr;
                    String minuteStr;
                    String secondStr;
                    String fracSecondStr;
                    try {
                        hourStr = tokenizer.nextToken();
                        minuteStr = tokenizer.nextToken();
                        secondStr = tokenizer.nextToken();
                        try {
                            fracSecondStr = tokenizer.nextToken();
                        } catch (Exception ex) {
                            fracSecondStr = null;
                        }
                    } catch (Exception ex) {
                        throw new NumberFormatException("invalid format " + var);
                    }
                    hour = Integer.valueOf(hourStr);
                    minute = Integer.valueOf(minuteStr);
                    second = Integer.valueOf(secondStr);
                    if (hour > MAXHOUR) {
                        throw new NumberFormatException("invalid hour " + hour + " in " + var);
                    } else if (minute > MAXMINUTE) {
                        throw new NumberFormatException("invalid minute  " + minute + " in " + var);
                    } else if (second > MAXSECOND) {
                        throw new NumberFormatException("invalid second  " + second + " in " + var);
                    } else {
                        if (fracSecondStr != null) {
                            if (fracSecondStr.length() > MAXLEADPREC) {
                                throw new NumberFormatException("invalid fracsecond length "
                                                                + fracSecondStr + " in " + var);
                            }
                            fractional_second = Integer.valueOf(fracSecondStr);
                            int lenDiff = MAXLEADPREC - fracSecondStr.length();
                            for (int i = 0; i < lenDiff; i++) {
                                fractional_second *= 10;
                            }
                        }
                    }
                    intervaldsBytes[0] = (byte) (is_negative & 255);
                    byte[] tmpBytes = intToBytes(day);
                    System.arraycopy(tmpBytes, 0, intervaldsBytes, 1, 4);
                    intervaldsBytes[5] = (byte) (hour & 255);
                    intervaldsBytes[6] = (byte) (minute & 255);
                    intervaldsBytes[7] = (byte) (second & 255);
                    tmpBytes = intToBytes(fractional_second);
                    System.arraycopy(tmpBytes, 0, intervaldsBytes, 8, 4);
                    intervaldsBytes[12] = (byte) (dayStr.length() & 255);
                    intervaldsBytes[13] = (byte) (MAXLEADPREC);
                } else {
                    throw new NumberFormatException("invalid format " + var);
                }
            }
            return intervaldsBytes;
        }
    }

    public static String toString(byte[] data) {
        if (data != null && data.length != 0) {
            if (data.length != INTERVALDS_BYTE_NUM) {
                throw new InvalidParameterException("invalid len:" + data.length);
            }
            int is_negative = data[0] & 255;
            int day = getInt(data, 1);
            int hour = data[5] & 255;
            int minute = data[6] & 255;
            int second = data[7] & 255;
            int fractional_second = getInt(data, 8);
            int day_scale = data[12] & 255;
            int fractional_second_scale = data[13] & 255;
            if (day < 0 || day > Math.pow(10, MAXLEADPREC)) {
                throw new NumberFormatException("day should not exceed "
                                                + Math.pow(10, MAXLEADPREC) + ", now is " + day);
            }
            if (hour < 0 || hour > MAXHOUR) {
                throw new NumberFormatException("hour should not exceed " + MAXHOUR + ", now is "
                                                + hour);
            }
            if (minute < 0 || minute > MAXMINUTE) {
                throw new NumberFormatException("minute should not exceed " + MAXMINUTE
                                                + ", now is " + minute);
            }
            if (second < 0 || second > MAXSECOND) {
                throw new NumberFormatException("second should not exceed " + MAXSECOND
                                                + ", now is " + second);
            }
            if (fractional_second < 0 || fractional_second > Math.pow(10, MAXLEADPREC)) {
                throw new NumberFormatException("fractional_second should not exceed "
                                                + Math.pow(10, MAXLEADPREC) + ", now is "
                                                + fractional_second);
            }
            if (fractional_second_scale < 0 || fractional_second_scale > MAXLEADPREC) {
                throw new NumberFormatException("fractional_second_scale should not exceed "
                                                + MAXLEADPREC + ", now is "
                                                + fractional_second_scale);
            }
            if (fractional_second_scale < MAXLEADPREC) {
                fractional_second = fractional_second
                                    / (int) Math.pow(10, MAXLEADPREC - fractional_second_scale);
            }
            String format = String.format("%%d %%d:%%d:%%d.%%0%dd", fractional_second_scale);
            String result = String.format(format, day, hour, minute, second, fractional_second);
            int endIndex = result.length() - 1;
            for (int i = result.length() - 1; result.charAt(i) != '.'; i--) {
                if (result.charAt(i) != '0') {
                    endIndex = i;
                    break;
                }
            }
            result = result.substring(0, endIndex + 1);
            if (is_negative == 0) {
                return result;
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
        INTERVALDS[] object = new INTERVALDS[var1];
        return object;
    }

    public String toString() {
        return toString(this.getBytes());
    }

}
