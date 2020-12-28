package com.alipay.oceanbase.jdbc.extend.datatype;

public class ComplexData {
    protected ComplexDataType complexType;
    protected Object[]        attrData  = null;
    protected int             attrCount = 0;

    public ComplexData(ComplexDataType type) {
        this.complexType = type;
    }

    public ComplexDataType getComplexType() {
        return complexType;
    }

    public void setComplexType(ComplexDataType complexType) {
        this.complexType = complexType;
    }

    public Object getAttrData(int attrIndex) {
        if (attrIndex >= this.getAttrCount()) {
            return null;
        }
        return this.attrData[attrIndex];
    }

    public Object[] getAttrData() {
        return this.attrData;
    }

    public void setAttrData(Object[] attrData) {
        if (null != attrData) {
            this.attrCount = attrData.length;
            this.attrData = attrData;
        }
    }

    public int getAttrCount() {
        return attrCount;
    }

    public void setAttrCount(int attrCount) {
        this.attrCount = attrCount;
    }

    public void addAttrData(int index, Object value) {
        if (null == this.attrData) {
            this.attrData = new Object[attrCount];
        }
        if (index < attrCount) {
            this.attrData[index] = value;
        }
    }

}