package com.alipay.oceanbase.jdbc.extend.datatype;

import java.sql.SQLException;

public class ComplexDataType {
    private String            typeName;
    private String            schemaName;
    private long              version         = 0;
    boolean                   isValid         = false;
    private int               type;
    private int               attrCount;
    private int               initAttrCount   = 0;
    private ComplexDataType[] attrTypes       = null;

    public static final int   TYPE_NUMBER     = 0;
    public static final int   TYPE_VARCHAR2   = 1;
    public static final int   TYPE_DATE       = 2;
    public static final int   TYPE_OBJECT     = 3;
    public static final int   TYPE_COLLECTION = 4;
    public static final int   TYPE_CURSOR     = 5;
    public static final int   TYPE_RAW        = 6;
    public static final int   TYPE_MAX        = 7;

    public ComplexDataType(String typeName, String schemaName, int type) {
        this.schemaName = schemaName;
        this.type = type;
        this.typeName = typeName;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public int getType() {
        return this.type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getAttrCount() {
        return attrCount;
    }

    public void setAttrCount(int attrCount) {
        this.attrCount = attrCount;
    }

    public ComplexDataType[] getAttrTypes() {
        return this.attrTypes;
    }

    public ComplexDataType getAttrType(int attrIndex) {
        if (attrIndex >= this.attrCount) {
            return null;
        }
        return this.attrTypes[attrIndex];
    }

    public void setAttrType(int attrIndex, ComplexDataType attrType) {
        if (null == this.attrTypes) {
            this.attrTypes = new ComplexDataType[this.attrCount];
        }
        this.attrTypes[attrIndex] = attrType;
    }

    public boolean isValid() {
        return isValid;
    }

    public void setValid(boolean valid) {
        isValid = valid;
    }

    public int getInitAttrCount() {
        return initAttrCount;
    }

    public void incInitAttrCount() {
        ++this.initAttrCount;
    }

    public static int getObComplexType(String attrType) throws SQLException {
        if (attrType.equalsIgnoreCase("COLLECTION"))
            return TYPE_COLLECTION;
        if (attrType.equalsIgnoreCase("OBJECT"))
            return TYPE_OBJECT;
        if (attrType.equalsIgnoreCase("NUMBER") || attrType.equalsIgnoreCase("INTEGER")
            || attrType.equalsIgnoreCase("DECIMAL"))
            return TYPE_NUMBER;
        if (attrType.equalsIgnoreCase("VARCHAR2") || attrType.equalsIgnoreCase("VARCHAR"))
            return TYPE_VARCHAR2;
        if (attrType.equalsIgnoreCase("DATE"))
            return TYPE_DATE;
        if (attrType.equalsIgnoreCase("CURSOR"))
            return TYPE_CURSOR;
        if (attrType.equalsIgnoreCase("RAW"))
            return TYPE_RAW;
        return TYPE_MAX;
    }

    public static boolean isBaseDataType(int type) {
        return TYPE_NUMBER == type || TYPE_VARCHAR2 == type || TYPE_DATE == type;
    }

    public static byte[] getBytes() {
        return null;
    }
}
