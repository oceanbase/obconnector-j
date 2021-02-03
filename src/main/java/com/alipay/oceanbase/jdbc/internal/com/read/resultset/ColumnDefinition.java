package com.alipay.oceanbase.jdbc.internal.com.read.resultset;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Types;
import com.alipay.oceanbase.jdbc.internal.ColumnType;
import com.alipay.oceanbase.jdbc.internal.ObOracleDefs;
import com.alipay.oceanbase.jdbc.internal.com.read.Buffer;
import com.alipay.oceanbase.jdbc.internal.util.constant.ColumnFlags;

/** Protocol details : https://mariadb.com/kb/en/resultset/#column-definition-packet */
public class ColumnDefinition {

    // This array stored character length for every collation id up to collation id 256
    // It is generated from the information schema using
    // "select  id, maxlen from information_schema.character_sets, information_schema.collations
    // where character_sets.character_set_name = collations.character_set_name order by id"
    private static final int[] maxCharlen              = { 0, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 3,
            2, 1, 1, 1, 0, 1, 2, 1, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 1, 3, 1, 2, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 4, 4, 1, 1, 1, 1, 1, 1, 1, 4, 4, 0, 1, 1, 1, 4, 4, 0, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, 3, 2, 2, 2, 2, 2, 1, 2, 3, 1, 1, 1, 2, 2, 3, 3, 1,
            0, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 0, 0, 0, 0, 0, 0, 0, 2,
            2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 2, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0, 3, 4, 4,
            0, 0, 0, 0, 0, 0, 0, 3, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 0,
            4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0           };

    private final Buffer       buffer;
    private final short        charsetNumber;
    private final long         length;
    private final ColumnType   type;
    private final int          decimals;
    private final int          precision;
    private final int          inoutType;

    private final int          sqltype;

    private int                catalogNameStart;
    private int                catalogNameLength;
    private int                databaseNameStart;
    private int                databaseNameLength;
    private int                tableNameStart;
    private int                tableNameLength;
    private int                originalTableNameStart;
    private int                originalTableNameLength;
    private int                nameStart;
    private int                nameLength;
    private int                originalColumnNameStart;
    private int                originalColumnNameLength;
    private short              longColFlag;
    private int                complexSchemaNameStart  = -1;
    private int                complexSchemaNameLength = -1;
    private int                complexTypeNameStart    = -1;
    private int                complexTypeNameLength   = -1;
    private int                complexVersion          = 0;
    private final boolean      isOracleMode;
    private int                precisionAdjustFactor   = 0;

    /**
     * Constructor for extent.
     *
     * @param other other columnInformation
     */
    public ColumnDefinition(ColumnDefinition other) {
        this.buffer = other.buffer;
        this.charsetNumber = other.charsetNumber;
        this.length = other.length;
        this.sqltype = other.sqltype;
        this.type = other.type;
        this.decimals = other.decimals;
        this.isOracleMode = other.isOracleMode;
        this.precision = other.precision;
        this.inoutType = other.inoutType;
        catalogNameLength = other.nameLength;
        catalogNameStart = other.catalogNameStart;
        databaseNameStart = other.nameStart;
        databaseNameLength = other.nameLength;
        tableNameLength = other.tableNameLength;
        tableNameStart = other.tableNameStart;
        originalTableNameStart = other.originalTableNameStart;
        originalTableNameLength = other.originalTableNameLength;
        originalColumnNameLength = other.originalColumnNameLength;
        originalColumnNameStart = other.originalColumnNameStart;
        nameStart = other.nameStart;
        nameLength = other.nameLength;
        longColFlag = other.longColFlag;
    }

    /**
     * Read column information from buffer.
     *
     * @param buffer buffer
     */
    public ColumnDefinition(Buffer buffer, boolean OracleMode) {
        this.buffer = buffer;
        // has41NewNewProt fixme :Value should be based on connection properties

        /*
        lenenc_str     catalog
        lenenc_str     schema
        lenenc_str     table
        lenenc_str     org_table
        lenenc_str     name
        lenenc_str     org_name
        lenenc_int     length of fixed-length fields [0c]
        2              character set
        4              column length
        1              type
        2              flags
        1              decimals
        2              filler [00] [00]
        */
        /**
         *  Record the offset of related information to achieve the purpose of reusing the buffer
         */
        catalogNameStart = buffer.getPosition() + 1;
        catalogNameLength = buffer.fastSkipLenString();
        catalogNameStart = adjustStartForFieldLength(catalogNameStart, catalogNameLength);
        databaseNameStart = buffer.getPosition() + 1;
        databaseNameLength = buffer.fastSkipLenString();
        databaseNameStart = adjustStartForFieldLength(databaseNameStart, databaseNameLength);
        tableNameStart = buffer.getPosition() + 1;
        tableNameLength = buffer.fastSkipLenString();
        tableNameStart = adjustStartForFieldLength(tableNameStart, tableNameLength);
        originalTableNameStart = buffer.getPosition() + 1;
        originalTableNameLength = buffer.fastSkipLenString();
        originalTableNameStart = adjustStartForFieldLength(originalTableNameStart,
            originalTableNameLength);
        originalColumnNameStart = buffer.getPosition() + 1;
        originalColumnNameLength = buffer.fastSkipLenString();
        originalColumnNameStart = adjustStartForFieldLength(originalColumnNameStart,
            originalColumnNameLength);
        nameStart = buffer.getPosition() + 1;
        nameLength = buffer.fastSkipLenString();
        nameStart = adjustStartForFieldLength(nameStart, nameLength);
        buffer.readByte();
        charsetNumber = buffer.readShort();
        length = buffer.readInt();
        sqltype = buffer.readByte() & 0xff;
        longColFlag = buffer.readShort();
        type = ColumnType.fromServer(sqltype, charsetNumber, OracleMode);

        decimals = buffer.readByte() & 0xff;
        precision = buffer.readByte() & 0xff;
        inoutType = buffer.readByte() & 0xff;

        if (ColumnType.COMPLEX.getType() == type.getType()) {
            buffer.setPosition(buffer.getPosition());
            complexSchemaNameStart = buffer.getPosition() + 1;
            complexSchemaNameLength = buffer.fastSkipLenString();
            complexSchemaNameStart = adjustStartForFieldLength(complexSchemaNameStart,
                complexSchemaNameLength);

            complexTypeNameStart = buffer.getPosition() + 1;
            complexTypeNameLength = buffer.fastSkipLenString();
            complexTypeNameStart = adjustStartForFieldLength(complexTypeNameStart,
                complexTypeNameLength);

            complexVersion = (int) buffer.readFieldLength();
        }
        isOracleMode = OracleMode;
        if (isSigned()) {
            switch (type) {
                case DECIMAL:
                case OBDECIMAL:
                case OLDDECIMAL:
                    precisionAdjustFactor = -1;
                    break;
                case DOUBLE:
                case BINARY_DOUBLE:
                case FLOAT:
                case BINARY_FLOAT:
                    precisionAdjustFactor = 1;
            }
        } else {
            switch (type) {
                case DOUBLE:
                case BINARY_DOUBLE:
                case FLOAT:
                case BINARY_FLOAT:
                    precisionAdjustFactor = 1;
                    break;
            }
        }
    }

    /**
     * Constructor.
     *
     * @param name column name
     * @param type column type
     * @return ColumnInformation
     */
    public static ColumnDefinition create(String name, ColumnType type, boolean isOracleMode) {
        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);

        byte[] arr = new byte[19 + 2 * nameBytes.length];
        int pos = 0;

        // lenenc_str     catalog
        // lenenc_str     schema
        // lenenc_str     table
        // lenenc_str     org_table
        for (int i = 0; i < 4; i++) {
            arr[pos++] = 0;
        }

        // lenenc_str     name
        // lenenc_str     org_name
        for (int i = 0; i < 2; i++) {
            arr[pos++] = (byte) nameBytes.length;
            System.arraycopy(nameBytes, 0, arr, pos, nameBytes.length);
            pos += nameBytes.length;
        }

        // lenenc_int     length of fixed-length fields [0c]
        arr[pos++] = 0xc;

        // 2              character set
        arr[pos++] = 33; /* charset  = UTF8 */
        arr[pos++] = 0;

        int len;

        /* Sensible predefined length - since we're dealing with I_S here, most char fields are 64 char long */
        switch (type.getSqlType()) {
            case Types.VARCHAR:
            case Types.CHAR:
                len = 64 * 3; /* 3 bytes per UTF8 char */
                break;
            case Types.SMALLINT:
                len = 5;
                break;
            case Types.NULL:
                len = 0;
                break;
            default:
                len = 1;
                break;
        }

        //
        arr[pos] = (byte) len; /* 4 bytes : column length */
        pos += 4;

        arr[pos++] = (byte) ColumnType.toServer(type.getSqlType()).getType(); /* 1 byte : type */

        return new ColumnDefinition(new Buffer(arr), isOracleMode);
    }

    private int adjustStartForFieldLength(int nameStart, int nameLength) {
        if (nameLength < 251) {
            return nameStart;
        }

        if (nameLength >= 251 && nameLength < 65536) {
            return nameStart + 2;
        }

        if (nameLength >= 65536 && nameLength < 16777216) {
            return nameStart + 3;
        }

        return nameStart + 8;
    }

    private String getString(int idx) {
        buffer.position = 0;
        for (int i = 0; i < idx; i++) {
            buffer.skipLengthEncodedBytes();
        }
        return buffer.readStringLengthEncoded(StandardCharsets.UTF_8);
    }

    public String getDatabase() {
        return getStringFromBytes(databaseNameStart, databaseNameLength);
    }

    public String getTable() {
        return getStringFromBytes(tableNameStart, tableNameLength);
    }

    public String getOriginalTable() {
        return getStringFromBytes(originalTableNameStart, originalTableNameLength);
    }

    public String getName() {
        return getStringFromBytes(nameStart, nameLength);
    }

    public String getOriginalName() {
        //        return getString(5);
        return getStringFromBytes(originalColumnNameStart, originalColumnNameLength);
    }

    public short getCharsetNumber() {
        return charsetNumber;
    }

    public long getLength() {
        return length;
    }

    /**
     * Return metadata precision.
     *
     * @return precision
     */
    public long getPrecision() {
        switch (type.getSqlType()) {
            case Types.BIT:
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
            case Types.NUMERIC:
            case Types.DECIMAL:
                if (decimals > 0) {
                    return length - 1 + precisionAdjustFactor;
                } else {
                    return length + precisionAdjustFactor;
                }
            default:
                break;
        }
        switch (type) {
            case NVARCHAR2:
            case VARCHAR2:
            case STRING:
            case VARSTRING:
            case NCHAR:
            case OBCLOB:
                int maxWidth = maxCharlen[charsetNumber & 0xff];
                if (maxWidth == 0) {
                    maxWidth = 1;
                }
                return length / maxWidth;
            default:
                return length;
        }
    }

    /**
     * Get column size.
     *
     * @return size
     */
    public int getDisplaySize() {
        int vtype = type.getSqlType();
        if (vtype == Types.VARCHAR || vtype == Types.CHAR || vtype == Types.NVARCHAR
            || vtype == Types.NCHAR || vtype == Types.CLOB) {
            int maxWidth = maxCharlen[charsetNumber & 0xff];
            if (maxWidth == 0) {
                maxWidth = 1;
            }

            return (int) length / maxWidth;
        }
        return (int) length;
    }

    public int getDecimals() {
        switch (type.getSqlType()) {
            case Types.BIT:
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
            case Types.NUMERIC:
            case Types.DECIMAL:
                return decimals;
        }
        return 0;
    }

    public ColumnType getColumnType() {
        return type;
    }

    public short getFlags() {
        return longColFlag;
    }

    public boolean isSigned() {
        return ((longColFlag & ColumnFlags.UNSIGNED) == 0);
    }

    public boolean isNotNull() {
        return ((this.longColFlag & 1) > 0);
    }

    public boolean isPrimaryKey() {
        return ((this.longColFlag & 2) > 0);
    }

    public boolean isUniqueKey() {
        return ((this.longColFlag & 4) > 0);
    }

    public boolean isMultipleKey() {
        return ((this.longColFlag & 8) > 0);
    }

    public boolean isBlob() {
        return ((this.longColFlag & 16) > 0);
    }

    public boolean isZeroFill() {
        return ((this.longColFlag & 64) > 0);
    }

    // doesn't use & 128 bit filter, because char binary and varchar binary are not binary (handle
    // like string), but have the binary flag
    public boolean isBinary() {
        return (getCharsetNumber() == 63);
    }

    public String getComplexTypeName() throws SQLException {
        return getStringFromBytes(this.complexTypeNameStart, this.complexTypeNameLength);
    }

    public static String toAsciiString(byte[] buffer, int startPos, int length) {
        char[] charArray = new char[length];
        int readpoint = startPos;

        for (int i = 0; i < length; i++) {
            charArray[i] = (char) buffer[readpoint];
            readpoint++;
        }

        return new String(charArray);
    }

    private String getStringFromBytes(int stringStart, int stringLength) {
        if ((stringStart == -1) || (stringLength == -1)) {
            return null;
        }
        return toAsciiString(buffer.buf, stringStart, stringLength);
    }

    public int getSqltype() {
        return sqltype;
    }
}
