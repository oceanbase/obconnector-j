package com.oceanbase.jdbc.internal.com.send.parameters;

import java.io.IOException;

import com.oceanbase.jdbc.internal.ColumnType;
import com.oceanbase.jdbc.internal.io.output.PacketOutputStream;

public class OBEmptyLobParameter implements Cloneable, ParameterHolder {
    int                         lobType    = 0; // 0 blob / 1 clob
    private static final byte[] EMPTY_CLOB = { 'E', 'M', 'P', 'T', 'Y', '_', 'C', 'L', 'O', 'B',
            '(', ')'                      };
    private static final byte[] EMPTY_BLOB = { 'E', 'M', 'P', 'T', 'Y', '_', 'B', 'L', 'O', 'B',
            '(', ')'                      };

    public OBEmptyLobParameter(int lobType) {
        this.lobType = lobType;
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
        // never used
    }

    @Override
    public int getApproximateTextProtocolLength() throws IOException {
        return 0;
    }

    @Override
    public boolean isNullData() {
        return false;
    }

    @Override
    public ColumnType getColumnType() {
        return ColumnType.STRING;
    }

    @Override
    public boolean isLongData() {
        return false;
    }
}
