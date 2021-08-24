package com.oceanbase.jdbc.extend.datatype;

public class RowIdImpl implements java.sql.RowId {
    private String val;

    public RowIdImpl(String value) {
        val = value;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof RowIdImpl) {
            if (obj.hashCode() == this.hashCode()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public byte[] getBytes() {
        if (val != null) {
            return val.getBytes();
        } else {
            return new byte[0];
        }
    }

    @Override
    public String toString() {
        return val;
    }

    @Override
    public int hashCode() {
        return val != null ? val.hashCode() : 0;

    }
}
