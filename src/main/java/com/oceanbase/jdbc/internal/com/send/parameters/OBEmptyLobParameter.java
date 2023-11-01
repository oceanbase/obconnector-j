package com.oceanbase.jdbc.internal.com.send.parameters;

import java.io.IOException;

import com.oceanbase.jdbc.internal.ColumnType;
import com.oceanbase.jdbc.internal.io.output.PacketOutputStream;

public class OBEmptyLobParameter implements Cloneable, ParameterHolder {

    int                         lobType          = 0;   // 0 blob / 1 clob
    private byte[]              lobLocatorBinary = null;
    private static final byte[] EMPTY_CLOB       = { 'E', 'M', 'P', 'T', 'Y', '_', 'C', 'L', 'O',
            'B', '(', ')'                       };
    private static final byte[] EMPTY_BLOB       = { 'E', 'M', 'P', 'T', 'Y', '_', 'B', 'L', 'O',
            'B', '(', ')'                       };

    public OBEmptyLobParameter(int lobType) {
        this.lobType = lobType;
    }

    public OBEmptyLobParameter(int lobType, byte[] lobLocatorBinary) {
        this.lobType = lobType;
        this.lobLocatorBinary = lobLocatorBinary;
    }

    @Override
    public void writeTo(PacketOutputStream os) throws IOException {
        if (this.lobType == 0) {
            os.write(EMPTY_BLOB);
        } else {
            os.write(EMPTY_CLOB);
        }
    }

    @Override
    public void writeBinary(PacketOutputStream pos) throws IOException {
        if (lobLocatorBinary != null) {
            pos.writeFieldLength(lobLocatorBinary.length);
            pos.write(lobLocatorBinary, 0, lobLocatorBinary.length);
        }
        // writing through reader is equivalent to writing nothing
    }

    @Override
    public int getApproximateTextProtocolLength() throws IOException {
        return 0;
    }

    @Override
    public boolean isNullData() {
        return false;
    }

    /**
     *
     * @return ColumnType.ORA_CLOB/ORA_CLOB write through locator, and ColumnType.STRING write through reader
     */
    @Override
    public ColumnType getColumnType() {
        return lobLocatorBinary != null ? (lobType == 0 ? ColumnType.ORA_BLOB : ColumnType.ORA_CLOB)
            : ColumnType.STRING;
    }

    @Override
    public boolean isLongData() {
        return false;
    }
}
