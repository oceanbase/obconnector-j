package com.oceanbase.jdbc.internal.com.send.parameters;

import com.oceanbase.jdbc.internal.ColumnType;

public class OBStringParameter extends OBVarcharParameter {

    public OBStringParameter(String str, boolean noBackslashEscapes, String characterEncoding) {
        super(str, noBackslashEscapes, characterEncoding);
    }

    public ColumnType getColumnType() {
        return ColumnType.STRING;
    }

}
