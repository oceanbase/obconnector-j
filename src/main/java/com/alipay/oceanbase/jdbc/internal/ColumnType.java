package com.alipay.oceanbase.jdbc.internal;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import com.alipay.oceanbase.jdbc.extend.datatype.*;
import com.alipay.oceanbase.jdbc.util.Options;

public enum ColumnType {
    OLDDECIMAL(0, Types.DECIMAL, "Types.DECIMAL", BigDecimal.class.getName()), //
    TINYINT(1, Types.SMALLINT, "Types.SMALLINT", Integer.class.getName()), //
    SMALLINT(2, Types.SMALLINT, "Types.SMALLINT", Integer.class.getName()), //
    INTEGER(3, Types.INTEGER, "Types.INTEGER", Integer.class.getName()), //
    FLOAT(4, Types.REAL, "Types.REAL", Float.class.getName()), //
    DOUBLE(5, Types.DOUBLE, "Types.DOUBLE", Double.class.getName()), //
    NULL(6, Types.NULL, "Types.NULL", String.class.getName()), //
    TIMESTAMP(7, Types.TIMESTAMP, "Types.TIMESTAMP", Timestamp.class.getName()), //
    BIGINT(8, Types.BIGINT, "Types.BIGINT", Long.class.getName()), //
    MEDIUMINT(9, Types.INTEGER, "Types.INTEGER", Integer.class.getName()), //
    DATE(10, Types.DATE, "Types.DATE", Date.class.getName()), //
    TIME(11, Types.TIME, "Types.TIME", Time.class.getName()), //
    DATETIME(12, Types.TIMESTAMP, "Types.TIMESTAMP", Timestamp.class.getName()), //
    YEAR(13, Types.SMALLINT, "Types.SMALLINT", Short.class.getName()), //
    NEWDATE(14, Types.DATE, "Types.DATE", Date.class.getName()), //
    VARCHAR(15, Types.VARCHAR, "Types.VARCHAR", String.class.getName()), //
    BIT(16, Types.BIT, "Types.BIT", "[B"), //
    JSON(245, Types.VARCHAR, "Types.VARCHAR", String.class.getName()), //
    DECIMAL(246, Types.DECIMAL, "Types.DECIMAL", BigDecimal.class.getName()), //
    ENUM(247, Types.VARCHAR, "Types.VARCHAR", String.class.getName()), //
    SET(248, Types.VARCHAR, "Types.VARCHAR", String.class.getName()), //
    TINYBLOB(249, Types.VARBINARY, "Types.VARBINARY", "[B"), //
    MEDIUMBLOB(250, Types.VARBINARY, "Types.VARBINARY", "[B"), //
    LONGBLOB(251, Types.LONGVARBINARY, "Types.LONGVARBINARY", "[B"), //
    BLOB(252, Types.LONGVARBINARY, "Types.LONGVARBINARY", "[B"), //
    OBBLOB(210, Types.LONGVARBINARY, "Types.LONGVARBINARY", "[B"), //
    VARSTRING(253, Types.VARCHAR, "Types.VARCHAR", String.class.getName()), //
    STRING(254, Types.VARCHAR, "Types.VARCHAR", String.class.getName()), //
    GEOMETRY(255, Types.VARBINARY, "Types.VARBINARY", "[B"), //

    TIMESTAMP_TZ(200, Types.TIMESTAMP, "Types.TIMESTAMP", TIMESTAMPTZ.class.getName()), //
    TIMESTAMP_LTZ(201, Types.TIMESTAMP, "Types.TIMESTAMP", TIMESTAMPLTZ.class.getName()), //
    TIMESTAMP_NANO(202, Types.TIMESTAMP, "Types.TIMESTAMP", Timestamp.class.getName()), //
    NUMBER(3, Types.INTEGER, "Types.INTEGER",
           com.alipay.oceanbase.jdbc.extend.datatype.NUMBER.class.getName()), //
    NUMBER_FLOAT(206, Types.FLOAT, "Types.FLOAT",
                 com.alipay.oceanbase.jdbc.extend.datatype.NUMBER_FLOAT.class.getName()), //
    BINARY_DOUBLE(5, Types.DOUBLE, "Types.DOUBLE",
                  com.alipay.oceanbase.jdbc.extend.datatype.BINARY_DOUBLE.class.getName()), //
    INTERVALYM(204, Types.OTHER, "Types.OTHER",
               com.alipay.oceanbase.jdbc.extend.datatype.INTERVALYM.class.getName()), //
    INTERVALDS(205, Types.OTHER, "Types.OTHER",
               com.alipay.oceanbase.jdbc.extend.datatype.INTERVALDS.class.getName()), //
    COMPLEX(160, Types.OTHER, "Types.OTHER",
            com.alipay.oceanbase.jdbc.extend.datatype.ComplexData.class.getName()), //
    ARRAY(161, Types.ARRAY, "Types.Array",
          com.alipay.oceanbase.jdbc.extend.datatype.ArrayImpl.class.getName()), //
    STRUCT(162, Types.STRUCT, "Types.STRUCT",
           com.alipay.oceanbase.jdbc.extend.datatype.StructImpl.class.getName()), //
    RAW(203, Types.VARBINARY, "Types.VARBINARY",
        com.alipay.oceanbase.jdbc.extend.datatype.RAW.class.getName()), //
    CURSOR(163, Types.OTHER, "Types.OTHER", "CURSORTEMP"), //
    BINARY_FLOAT(206, Types.FLOAT, "Types.VARBINARY;", BINARY_DOUBLE.class.getName());

    static final ColumnType[] typeMap;

    static {
        typeMap = new ColumnType[256];
        for (ColumnType v : values()) {
            typeMap[v.mariadbType] = v;
        }
    }

    private final short       mariadbType;
    private final int         javaType;
    private final String      javaTypeName;
    private final String      className;

    ColumnType(int mariadbType, int javaType, String javaTypeName, String className) {
        this.mariadbType = (short) mariadbType;
        this.javaType = javaType;
        this.javaTypeName = javaTypeName;
        this.className = className;
    }

    /**
     * Permit to know java result class according to java.sql.Types.
     *
     * @param type java.sql.Type value
     * @return Class name.
     */
    public static Class classFromJavaType(int type) {
        switch (type) {
            case Types.BOOLEAN:
            case Types.BIT:
                return Boolean.class;

            case Types.TINYINT:
                return Byte.class;

            case Types.SMALLINT:
                return Short.class;

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
            case Types.CLOB:
            case Types.NCLOB:
                return String.class;

            case Types.DECIMAL:
            case Types.NUMERIC:
                return BigDecimal.class;

            case Types.VARBINARY:
            case Types.BINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
            case Types.JAVA_OBJECT:
                return byte[].class;

            case Types.NULL:
                return null;

            case Types.TIME:
                return Time.class;

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

    /**
     * Get columnTypeName.
     *
     * @param type type
     * @param len len
     * @param signed signed
     * @param binary binary
     * @return type
     */
    public static String getColumnTypeName(ColumnType type, long len, boolean signed, boolean binary) {
        switch (type) {
            case SMALLINT:
            case MEDIUMINT:
            case INTEGER:
            case BIGINT:
                if (!signed) {
                    return type.getTypeName() + " UNSIGNED";
                } else {
                    return type.getTypeName();
                }
            case BLOB:
                /*
                 map to different blob types based on datatype length
                 see https://mariadb.com/kb/en/library/data-types/
                */
                if (len < 0) {
                    return "LONGBLOB";
                } else if (len <= 255) {
                    return "TINYBLOB";
                } else if (len <= 65535) {
                    return "BLOB";
                } else if (len <= 16777215) {
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
            default:
                return type.getTypeName();
        }
    }

    /**
     * Convert server Type to server type.
     *
     * @param typeValue type value
     * @param charsetNumber charset
     * @return MariaDb type
     */
    public static ColumnType fromServer(int typeValue, int charsetNumber) {

        ColumnType columnType = typeMap[typeValue];

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
     * Convert javatype to ColumnType.
     *
     * @param javaType javatype value
     * @return mariaDb type value
     */
    public static ColumnType toServer(int javaType) {
        for (ColumnType v : values()) {
            if (v.javaType == javaType) {
                return v;
            }
        }
        return ColumnType.BLOB;
    }

    /**
     * Get class name.
     *
     * @param type type
     * @param len len
     * @param signed signed
     * @param binary binary
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
        return javaType;
    }

    public String getTypeName() {
        return name();
    }

    public short getType() {
        return mariadbType;
    }

    public String getJavaTypeName() {
        return javaTypeName;
    }
}
