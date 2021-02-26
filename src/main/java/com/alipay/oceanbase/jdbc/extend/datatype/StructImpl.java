package com.alipay.oceanbase.jdbc.extend.datatype;

import com.alipay.oceanbase.jdbc.ObStruct;

import java.sql.SQLException;
import java.util.Map;

public class StructImpl extends ComplexData implements ObStruct {
    public StructImpl(ComplexDataType type) {
        super(type);
    }

    @Override
    public String getSQLTypeName() throws SQLException {
        return this.getComplexType().getTypeName();
    }

    @Override
    public Object[] getAttributes() throws SQLException {
        return this.getAttrData();
    }

    @Override
    public Object[] getAttributes(Map<String, Class<?>> map) throws SQLException {
        return new Object[0];
    }
}
