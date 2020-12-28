package com.alipay.oceanbase.jdbc.extend.datatype;

import java.sql.SQLException;

public class RAW extends Datum {
    public RAW(byte[] bytes) {
        super(bytes);
    }

    public RAW(Object val) {
        // todo
    }

    @Override
    public boolean isConvertibleTo(Class var1) {
        return false;
    }

    @Override
    public Object toJdbc() throws SQLException {
        return null;
    }

    @Override
    public Object makeJdbcArray(int var1) {
        return null;
    }
}
