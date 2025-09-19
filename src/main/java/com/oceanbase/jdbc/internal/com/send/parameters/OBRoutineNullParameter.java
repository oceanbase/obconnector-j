package com.oceanbase.jdbc.internal.com.send.parameters;

import com.oceanbase.jdbc.internal.ColumnType;

public class OBRoutineNullParameter extends NullParameter {

    private ColumnType realColumnType;

    public OBRoutineNullParameter(ColumnType realColumnType) {
        this.realColumnType = realColumnType;
    }

    @Override
    public ColumnType getColumnType() {
        return realColumnType;
    }

    public void setRealColumnType(ColumnType realColumnType) {
        this.realColumnType = realColumnType;
    }
}
