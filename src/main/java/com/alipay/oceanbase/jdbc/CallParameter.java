package com.alipay.oceanbase.jdbc;

import java.sql.Types;

/** Info about in/out parameters. */
class CallParameter {

    private boolean isInput;
    private boolean isOutput;
    private int     sqlType;
    private int     outputSqlType;
    private int     scale;
    private String  typeName;
    private boolean isSigned;
    private int     canBeNull;
    private int     precision;
    private String  className;
    private String  name;

    // New add for OceanBase
    private int     index;

    public CallParameter() {
        sqlType = Types.OTHER;
        outputSqlType = Types.OTHER;
    }

    public CallParameter(String name, int index, boolean isInput, boolean isOutput, int sqlType,
                         String typeName, int precision, int scale, int canBeNull, int outputSqlType) {
        this.name = name;
        this.index = index;
        this.isInput = isInput;
        this.isOutput = isOutput;
        this.sqlType = sqlType;
        this.typeName = typeName;
        this.precision = precision;
        this.scale = scale;
        this.canBeNull = canBeNull;
        this.outputSqlType = outputSqlType;
    }

    public boolean isInput() {
        return isInput;
    }

    public void setInput(boolean input) {
        isInput = input;
    }

    public boolean isOutput() {
        return isOutput;
    }

    public void setOutput(boolean output) {
        isOutput = output;
    }

    public int getSqlType() {
        return sqlType;
    }

    public void setSqlType(int sqlType) {
        this.sqlType = sqlType;
    }

    public int getOutputSqlType() {
        return outputSqlType;
    }

    public void setOutputSqlType(int outputSqlType) {
        this.outputSqlType = outputSqlType;
    }

    public int getScale() {
        return scale;
    }

    public void setScale(int scale) {
        this.scale = scale;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public boolean isSigned() {
        return isSigned;
    }

    public void setSigned(boolean signed) {
        isSigned = signed;
    }

    public int getCanBeNull() {
        return canBeNull;
    }

    public void setCanBeNull(int canBeNull) {
        this.canBeNull = canBeNull;
    }

    public int getPrecision() {
        return precision;
    }

    public void setPrecision(int precision) {
        this.precision = precision;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
