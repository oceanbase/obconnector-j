/**
 *  OceanBase Client for Java
 *
 *  Copyright (c) 2012-2014 Monty Program Ab.
 *  Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *  Copyright (c) 2021 OceanBase.
 *
 *  This library is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU Lesser General Public License as published by the Free
 *  Software Foundation; either version 2.1 of the License, or (at your option)
 *  any later version.
 *
 *  This library is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License along
 *  with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 *  This particular MariaDB Client for Java file is work
 *  derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 *  the following copyright and notice provisions:
 *
 *  Copyright (c) 2009-2011, Marcus Eriksson
 *
 *  Redistribution and use in source and binary forms, with or without modification,
 *  are permitted provided that the following conditions are met:
 *  Redistributions of source code must retain the above copyright notice, this list
 *  of conditions and the following disclaimer.
 *
 *  Redistributions in binary form must reproduce the above copyright notice, this
 *  list of conditions and the following disclaimer in the documentation and/or
 *  other materials provided with the distribution.
 *
 *  Neither the name of the driver nor the names of its contributors may not be
 *  used to endorse or promote products derived from this software without specific
 *  prior written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 *  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 *  IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 *  INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 *  NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 *  PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 *  WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 *  OF SUCH DAMAGE.
 */
package com.oceanbase.jdbc.internal;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Locale;

import com.oceanbase.jdbc.Blob;
import com.oceanbase.jdbc.Clob;
import com.oceanbase.jdbc.extend.datatype.*;
import com.oceanbase.jdbc.util.Options;

public enum ColumnType {

    NULL(6, Types.NULL, "Types.NULL", String.class.getName(), ColumnType.BOTH_TYPE),
    TINYINT(1, Types.TINYINT, "Types.TINYINT", Integer.class.getName(), ColumnType.BOTH_TYPE), // DONE Types.SMALLINT  -> Types.TINYINT
    SMALLINT(2, Types.SMALLINT, "Types.SMALLINT", Integer.class.getName(), ColumnType.BOTH_TYPE),
    INTEGER(3, Types.INTEGER, "Types.INTEGER", Integer.class.getName(), ColumnType.MYSQL_TYPE),
    NUMBER(3, Types.INTEGER, "Types.INTEGER", com.oceanbase.jdbc.extend.datatype.NUMBER.class.getName(), ColumnType.ORACLE_TYPE),
    BIGINT(8, Types.BIGINT, "Types.BIGINT", Long.class.getName(), ColumnType.BOTH_TYPE),
    FLOAT(4, Types.REAL, "Types.REAL", Float.class.getName(), ColumnType.MYSQL_TYPE),
    BINARY_FLOAT(4, Types.FLOAT, "Types.FLOAT", java.lang.Float.class.getName(), ColumnType.ORACLE_TYPE), // todo: 256
    DOUBLE(5, Types.DOUBLE, "Types.DOUBLE", Double.class.getName(), ColumnType.MYSQL_TYPE),
    BINARY_DOUBLE(5, Types.DOUBLE, "Types.DOUBLE", java.lang.Double.class.getName(), ColumnType.ORACLE_TYPE), // todo: 257
    TIMESTAMP(7, Types.TIMESTAMP, "Types.TIMESTAMP", Timestamp.class.getName(), ColumnType.MYSQL_TYPE),
    DATE(10, Types.DATE, "Types.DATE", Date.class.getName(), ColumnType.MYSQL_TYPE),
    TIME(11, Types.TIME, "Types.TIME", Time.class.getName(), ColumnType.MYSQL_TYPE),
    DATETIME(12, Types.TIMESTAMP, "Types.TIMESTAMP", Timestamp.class.getName(), ColumnType.BOTH_TYPE),
    YEAR(13, Types.DATE, "Types.SMALLINT", Short.class.getName(), ColumnType.MYSQL_TYPE), // DONE Types.SMALLINT -> Types.DATE
    COMPLEX(160, Types.OTHER, "Types.OTHER", ComplexData.class.getName(), ColumnType.ORACLE_TYPE),
    TIMESTAMP_TZ(200, -101, "Types.TIMESTAMP", TIMESTAMPTZ.class.getName(), ColumnType.ORACLE_TYPE), // Types.TIMESTAMP -> -101
    TIMESTAMP_LTZ(201, -102, "Types.TIMESTAMP", TIMESTAMPLTZ.class.getName(), ColumnType.ORACLE_TYPE), // Types.TIMESTAMP -> -102
    TIMESTAMP_NANO(202, Types.TIMESTAMP, "Types.TIMESTAMP", Timestamp.class.getName(), ColumnType.ORACLE_TYPE),
    INTERVALYM(204, Types.OTHER, "Types.OTHER", java.lang.String.class.getName(), ColumnType.ORACLE_TYPE),
    INTERVALDS(205, Types.OTHER, "Types.OTHER", java.lang.String.class.getName(), ColumnType.ORACLE_TYPE),
    NUMBER_FLOAT(206, Types.DOUBLE, "Types.FLOAT", java.lang.Double.class.getName(), ColumnType.ORACLE_TYPE),
    ROWID(209, Types.ROWID, "Types.ROWID", java.lang.String.class.getName(), ColumnType.ORACLE_TYPE), //
    JSON(245, Types.VARCHAR, "Types.VARCHAR", String.class.getName(), ColumnType.MYSQL_TYPE), // DONE VARCHAR -> CHAR
    DECIMAL(246, Types.DECIMAL, "Types.DECIMAL", BigDecimal.class.getName(), ColumnType.MYSQL_TYPE),
    OBDECIMAL(246, Types.DECIMAL, "Types.DECIMAL", BigDecimal.class.getName(), ColumnType.ORACLE_TYPE),
    TINYBLOB(249, Types.VARBINARY, "Types.VARBINARY", "[B", ColumnType.MYSQL_TYPE),
    MEDIUMBLOB(250, Types.VARBINARY, "Types.VARBINARY", "[B", ColumnType.MYSQL_TYPE), // DONE VARBINARY -> LONGVARBINARY
    LONGBLOB(251, Types.LONGVARBINARY, "Types.LONGVARBINARY", "[B", ColumnType.MYSQL_TYPE), //Types.CHAR -> Types.DECIMAL
    BLOB(252, Types.LONGVARBINARY, "Types.LONGVARBINARY", "[B", ColumnType.MYSQL_TYPE),
    GEOMETRY(255, Types.CHAR, "Types.VARBINARY", "[B", ColumnType.MYSQL_TYPE), // DONE VARCHAR -> CHAR
    VARCHAR(15, Types.VARCHAR, "Types.VARCHAR", String.class.getName(), ColumnType.BOTH_TYPE),
    NVARCHAR2(207, Types.VARCHAR, "Types.NVARCHAR2", java.lang.String.class.getName(), ColumnType.ORACLE_TYPE),
    NCHAR(208, Types.NCHAR, "Types.NCHAR", java.lang.String.class.getName(), ColumnType.ORACLE_TYPE),
    ORA_BLOB(210, Types.BLOB, "Types.LONGVARBINARY", "com.oceanbase.jdbc.Blob", ColumnType.ORACLE_TYPE), // ObLobType
    ORA_CLOB(211, Types.CLOB, "Types.LONGVARBINARY", "com.oceanbase.jdbc.Clob", ColumnType.ORACLE_TYPE), // ObLobType
    VARSTRING(253, Types.VARCHAR, "Types.VARCHAR", String.class.getName(), ColumnType.MYSQL_TYPE),
    VARCHAR2(253, Types.VARCHAR, "Types.VARCHAR", String.class.getName(), ColumnType.ORACLE_TYPE),
    STRING(254, Types.CHAR, "Types.CHAR", String.class.getName(), ColumnType.BOTH_TYPE), // DONE VARCHAR -> CHAR
    // todo: check unknown
    OLDDECIMAL(0, Types.DECIMAL, "Types.DECIMAL", BigDecimal.class.getName(), ColumnType.BOTH_TYPE),
    MEDIUMINT(9, Types.INTEGER, "Types.INTEGER", Integer.class.getName(), ColumnType.BOTH_TYPE), // in V1 it is FIELD_TYPE_INT24
    NEWDATE(14, Types.DATE, "Types.DATE", Date.class.getName(), ColumnType.BOTH_TYPE),
    BIT(16, Types.BIT, "Types.BIT", "[B", ColumnType.BOTH_TYPE),
    ENUM(247, Types.CHAR, "Types.VARCHAR", String.class.getName(), ColumnType.BOTH_TYPE), // DONE VARCHAR -> CHAR
    SET(248, Types.CHAR, "Types.VARCHAR", String.class.getName(), ColumnType.BOTH_TYPE), // DONE VARCHAR -> CHAR
    ARRAY(161, Types.ARRAY, "Types.Array", ArrayImpl.class.getName(), ColumnType.ORACLE_TYPE),
    STRUCT(162, Types.STRUCT, "Types.STRUCT", StructImpl.class.getName(), ColumnType.ORACLE_TYPE),
    RAW(203, Types.VARBINARY, "Types.VARBINARY", "[B", ColumnType.ORACLE_TYPE),
    CURSOR(163, Types.OTHER, "Types.OTHER", "CURSORTEMP", ColumnType.ORACLE_TYPE);

    /* Add one protocol type need to check:
     *      1.java.sql.Types, must same as JDBC/O-JDBC, it not in java.sql.Types
     * */

    static final ColumnType[] typeMysqlMap;
    static final ColumnType[] typeOracleMap;
    static final int          MYSQL_TYPE  = 1;
    static final int          ORACLE_TYPE = 2;
    static final int          BOTH_TYPE   = 3;

    static {
        typeMysqlMap = new ColumnType[256];
        typeOracleMap = new ColumnType[256];
        for (ColumnType v : values()) {
            if (v.serverType == ColumnType.MYSQL_TYPE) {
                typeMysqlMap[v.protocolType] = v;
            } else if (v.serverType == ColumnType.ORACLE_TYPE) {
                typeOracleMap[v.protocolType] = v;
            } else if (v.serverType == ColumnType.BOTH_TYPE) {
                typeMysqlMap[v.protocolType] = v;
                typeOracleMap[v.protocolType] = v;
            }
            /* others do nothing */
        }
    }

    private final short       protocolType;
    private final int         sqlType;
    private final String      sqlTypeName;
    private final String      className;
    private final int         serverType;     /* 1 just for MySQL / 2 just for Oracle / 3 for both */

    ColumnType(int protocolType, int sqlType, String sqlTypeName, String className, int serverType) {
        this.protocolType = (short) protocolType;
        this.sqlType = sqlType;
        this.sqlTypeName = sqlTypeName;
        this.className = className;
        this.serverType = serverType;
    }

    public static int convertDbTypeToSqlType(String str) {
        switch (str.toUpperCase(Locale.ROOT)) {
            case "BIT":
                return Types.BIT;
            case "TINYINT":
                return Types.TINYINT;
            case "SMALLINT":
                return Types.SMALLINT;
            case "MEDIUMINT":
                return Types.INTEGER;
            case "INT":
                return Types.INTEGER;
            case "INTEGER":
                return Types.INTEGER;
            case "LONG":
                return Types.INTEGER;
            case "BIGINT":
                return Types.BIGINT;
            case "INT24":
                return Types.INTEGER;
            case "REAL":
                return Types.DOUBLE;
            case "FLOAT":
                return Types.FLOAT;
            case "DECIMAL":
                return Types.DECIMAL;
            case "NUMERIC":
                return Types.NUMERIC;
            case "DOUBLE":
                return Types.DOUBLE;
            case "CHAR":
                return Types.CHAR;
            case "VARCHAR":
                return Types.VARCHAR;
            case "DATE":
                return Types.DATE;
            case "TIME":
                return Types.TIME;
            case "YEAR":
                return Types.SMALLINT;
            case "TIMESTAMP":
                return Types.TIMESTAMP;
            case "DATETIME":
                return Types.TIMESTAMP;
            case "TINYBLOB":
                return Types.BINARY;
            case "BLOB":
                return Types.LONGVARBINARY;
            case "MEDIUMBLOB":
                return Types.LONGVARBINARY;
            case "LONGBLOB":
                return Types.LONGVARBINARY;
            case "TINYTEXT":
                return Types.VARCHAR;
            case "TEXT":
                return Types.LONGVARCHAR;
            case "MEDIUMTEXT":
                return Types.LONGVARCHAR;
            case "LONGTEXT":
                return Types.LONGVARCHAR;
            case "ENUM":
                return Types.VARCHAR;
            case "SET":
                return Types.VARCHAR;
            case "GEOMETRY":
                return Types.LONGVARBINARY;
            case "VARBINARY":
                return Types.VARBINARY;
            case "JSON":
                return Types.VARCHAR;
            default:
                return Types.OTHER;
        }
    }

    /**
     * Permit to know java result class according to java.sql.Types.
     *
     * @param type java.sql.Type value
     * @return Class name.
     */
    public static Class convertSqlTypeToClass(int type) {
        switch (type) {
            case Types.BOOLEAN:
            case Types.BIT:
                return Boolean.class;

            case Types.TINYINT:
                //return Byte.class;
                return Integer.class;

            case Types.SMALLINT:
                //return Short.class;
                return Integer.class;

            case Types.INTEGER:
                return Integer.class;

            case Types.BIGINT:
                return Long.class;

            case Types.DOUBLE:
            case Types.FLOAT:
                return Double.class;

            case Types.REAL:
                return Float.class;

            case Types.TIMESTAMP:
                return Timestamp.class;

            case Types.DATE:
                return Date.class;

            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.CHAR:
            case Types.NCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.NCLOB:
                //TODO need to update
                return String.class;

            case Types.DECIMAL:
            case Types.NUMERIC:
                return BigDecimal.class;

            case Types.VARBINARY:
            case Types.BINARY:
            case Types.LONGVARBINARY:

            case Types.JAVA_OBJECT:
                return byte[].class;

            case Types.NULL:
                return null;
            case Types.TIME:
                return Time.class;
            case Types.BLOB:
                return Blob.class;
            case Types.CLOB:
                return Clob.class;
            default:
                // DISTINCT
                // STRUCT
                // ARRAY
                // REF
                // DATALINK
                // ROWID
                // SQLXML
                // REF_CURSOR
                // TIME_WITH_TIMEZONE
                // TIMESTAMP_WITH_TIMEZONE
                break;
        }
        return null;
    }

    /**
     * Convert protocol type to ColumnType.
     *
     * @param typeValue     type value
     * @param charsetNumber charset
     * @return MariaDb type
     */
    public static ColumnType convertProtocolTypeToColumnType(int typeValue, int charsetNumber,
                                                             boolean isOracleMode) {

        ColumnType columnType;
        if (isOracleMode) {
            columnType = typeOracleMap[typeValue];
        } else {
            columnType = typeMysqlMap[typeValue];
        }

        if (columnType == null) {
            // Potential fallback for types that are not implemented.
            // Should not be normally used.
            columnType = BLOB;
        }

        if (charsetNumber != 63 && typeValue >= 249 && typeValue <= 252) {
            // MariaDB Text dataType
            return ColumnType.VARCHAR;
        }

        return columnType;
    }

    /**
     * Convert sql type to ColumnType.
     *
     * @param sqlType sql type value
     * @return mariaDb type value
     */
    public static ColumnType convertSqlTypeToColumnType(int sqlType) {
        for (ColumnType v : values()) {
            if (v.sqlType == sqlType) {
                return v;
            }
        }
        return ColumnType.BLOB;
    }

    /**
     * Get columnTypeName.
     *
     * @param type   type
     * @param len    len
     * @param signed signed
     * @param binary binary
     * @return type
     */
    public static String getColumnTypeName(ColumnType type, long len, boolean signed,
                                           boolean binary, boolean isOracelMode) {
        long l = len;
        switch (type) {
            case SMALLINT:
            case MEDIUMINT:
            case BIGINT:
                if (!signed) {
                    return type.getTypeName() + " UNSIGNED";
                } else {
                    return type.getTypeName();
                }
            case INTEGER:
                if (!signed) {
                    return isOracelMode ? type.getTypeName() : "INT" + " UNSIGNED";
                } else {
                    return isOracelMode ? type.getTypeName() : "INT";
                }
            case BLOB:
                /*
                 map to different blob types based on datatype length
                 see https://mariadb.com/kb/en/library/data-types/
                */
                /*
                Each BLOB value is stored using a two-byte length prefix that
                indicates the number of bytes in the value.
                 */

                if (binary) {
                    l -= 2;
                }
                if (len < 0) {
                    return "LONGBLOB";
                } else if (l <= 255) {
                    return "TINYBLOB";
                } else if (l <= 65535) {
                    return "BLOB";
                } else if (l <= 16777215) {
                    return "MEDIUMBLOB";
                } else {
                    return "LONGBLOB";
                }
            case VARSTRING:
            case VARCHAR:
                if (binary) {
                    return "VARBINARY";
                }
                return "VARCHAR";
            case STRING:
                if (binary) {
                    return "BINARY";
                }
                return "CHAR";
            case ORA_CLOB:
                return "CLOB";
            case ORA_BLOB:
                return "BLOB";
            case TIMESTAMP_NANO:
                return "TIMESTAMP";
            case TIMESTAMP_TZ:
                return "TIMESTAMP WITH TIME ZONE";
            case TIMESTAMP_LTZ:
                return "TIMESTAMP WITH LOCAL TIME ZONE";
            case OLDDECIMAL:
            case DECIMAL:
                return !signed ? "DECIMAL UNSIGNED" : "DECIMAL";
            case OBDECIMAL:
                return !signed ? "NUMBER UNSIGNED" : "NUMBER";
            case NUMBER_FLOAT:
                return "NUMBER";
            case DATETIME:
                if (isOracelMode) {
                    return "DATE";
                }
            default:
                return type.getTypeName();
        }
    }

    /**
     * Get class name.
     *
     * @param type    type
     * @param len     len
     * @param signed  signed
     * @param binary  binary
     * @param options options
     * @return class name
     */
    public static String getClassName(ColumnType type, int len, boolean signed, boolean binary,
                                      Options options) {
        switch (type) {
            case TINYINT:
                if (len == 1 && options.tinyInt1isBit) {
                    return Boolean.class.getName();
                }
                return Integer.class.getName();
            case INTEGER:
                return (signed) ? Integer.class.getName() : Long.class.getName();
            case BIGINT:
                return (signed) ? Long.class.getName() : BigInteger.class.getName();
            case YEAR:
                if (options.yearIsDateType) {
                    return Date.class.getName();
                }
                return Short.class.getName();
            case BIT:
                return (len == 1) ? Boolean.class.getName() : "[B";
            case STRING:
            case VARCHAR:
            case VARSTRING:
                return binary ? "[B" : String.class.getName();
            default:
                break;
        }
        return type.getClassName();
    }

    public String getClassName() {
        return className;
    }

    public int getSqlType() {
        return sqlType;
    }

    public String getTypeName() {
        return name();
    }

    public short getType() {
        return protocolType;
    }

    public String getSqlTypeName() {
        return sqlTypeName;
    }

    /**
     * Is type numeric.
     *
     * @param type mariadb type
     * @return true if type is numeric
     */
    public static boolean isNumeric(ColumnType type) {
        switch (type) {
            case OLDDECIMAL:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case FLOAT:
            case DOUBLE:
            case BIGINT:
            case MEDIUMINT:
            case BIT:
            case DECIMAL:
                return true;
            default:
                return false;
        }
    }

}
