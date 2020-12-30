package com.alipay.oceanbase.jdbc.internal.com.read.resultset;

import java.nio.charset.StandardCharsets;
import java.sql.Types;
import com.alipay.oceanbase.jdbc.internal.ColumnType;
import com.alipay.oceanbase.jdbc.internal.com.read.Buffer;
import com.alipay.oceanbase.jdbc.internal.util.constant.ColumnFlags;

/** Protocol details : https://mariadb.com/kb/en/resultset/#column-definition-packet */
public class ColumnDefinition {

    // This array stored character length for every collation id up to collation id 256
    // It is generated from the information schema using
    // "select  id, maxlen from information_schema.character_sets, information_schema.collations
    // where character_sets.character_set_name = collations.character_set_name order by id"
    private static final int[] maxCharlen = { 0, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 3, 2, 1, 1, 1, 0,
            1, 2, 1, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 1, 3, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 4, 4,
            1, 1, 1, 1, 1, 1, 1, 4, 4, 0, 1, 1, 1, 4, 4, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            0, 1, 1, 1, 1, 1, 1, 3, 2, 2, 2, 2, 2, 1, 2, 3, 1, 1, 1, 2, 2, 3, 3, 1, 0, 4, 4, 4, 4,
            4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 0, 0, 0, 0, 0, 0, 0, 2, 2, 2, 2, 2, 2,
            2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 4, 4, 4,
            4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0, 3, 4, 4, 0, 0, 0, 0, 0,
            0, 0, 3, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 0, 4, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0             };

    private final Buffer       buffer;
    private final short        charsetNumber;
    private final long         length;
    private final ColumnType   type;
    private final byte         decimals;
    private final short        flags;
    private final boolean      isOracleMode;

    /**
     * Constructor for extent.
     *
     * @param other other columnInformation
     */
    public ColumnDefinition(ColumnDefinition other) {
        this.buffer = other.buffer;
        this.charsetNumber = other.charsetNumber;
        this.length = other.length;
        this.type = other.type;
        this.decimals = other.decimals;
        this.flags = other.flags;
        this.isOracleMode = other.isOracleMode;
    }

    /**
     * Read column information from buffer.
     *
     * @param buffer buffer
     */
    public ColumnDefinition(Buffer buffer, boolean OracleMode) {
        this.buffer = buffer;

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

        // set position after length encoded value, not read most of the time
        buffer.position = buffer.limit - 12;

        charsetNumber = buffer.readShort();
        length = buffer.readInt();
        type = ColumnType.fromServer(buffer.readByte() & 0xff, charsetNumber, OracleMode);
        flags = buffer.readShort();
        decimals = buffer.readByte();
        isOracleMode = OracleMode;
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

    private String getString(int idx) {
        buffer.position = 0;
        for (int i = 0; i < idx; i++) {
            buffer.skipLengthEncodedBytes();
        }
        return buffer.readStringLengthEncoded(StandardCharsets.UTF_8);
    }

    public String getDatabase() {
        return getString(1);
    }

    public String getTable() {
        return getString(2);
    }

    public String getOriginalTable() {
        return getString(3);
    }

    public String getName() {
        return getString(4);
    }

    public String getOriginalName() {
        return getString(5);
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
        switch (type) {
            case OLDDECIMAL:
            case DECIMAL:
                // DECIMAL and OLDDECIMAL are  "exact" fixed-point number.
                // so :
                // - if can be signed, 1 byte is saved for sign
                // - if decimal > 0, one byte more for dot
                if (isSigned()) {
                    return length - ((decimals > 0) ? 2 : 1);
                } else {
                    return length - ((decimals > 0) ? 1 : 0);
                }
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
        if (vtype == Types.VARCHAR || vtype == Types.CHAR) {
            int maxWidth = maxCharlen[charsetNumber & 0xff];
            if (maxWidth == 0) {
                maxWidth = 1;
            }

            return (int) length / maxWidth;
        }
        return (int) length;
    }

    public byte getDecimals() {
        return decimals;
    }

    public ColumnType getColumnType() {
        return type;
    }

    public short getFlags() {
        return flags;
    }

    public boolean isSigned() {
        return ((flags & ColumnFlags.UNSIGNED) == 0);
    }

    public boolean isNotNull() {
        return ((this.flags & 1) > 0);
    }

    public boolean isPrimaryKey() {
        return ((this.flags & 2) > 0);
    }

    public boolean isUniqueKey() {
        return ((this.flags & 4) > 0);
    }

    public boolean isMultipleKey() {
        return ((this.flags & 8) > 0);
    }

    public boolean isBlob() {
        return ((this.flags & 16) > 0);
    }

    public boolean isZeroFill() {
        return ((this.flags & 64) > 0);
    }

    // doesn't use & 128 bit filter, because char binary and varchar binary are not binary (handle
    // like string), but have the binary flag
    public boolean isBinary() {
        return (getCharsetNumber() == 63);
    }
}
