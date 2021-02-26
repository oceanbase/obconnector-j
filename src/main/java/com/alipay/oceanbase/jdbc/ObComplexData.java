package com.alipay.oceanbase.jdbc;

import com.alipay.oceanbase.jdbc.extend.datatype.ComplexDataType;

public interface ObComplexData {
    ComplexDataType getComplexType();

    void setComplexType(ComplexDataType complexType);

    Object getAttrData(int attrIndex);

    Object[] getAttrData();

    void setAttrData(Object[] attrData);

    int getAttrCount();

    void setAttrCount(int attrCount);

    void addAttrData(int index, Object value);
}
