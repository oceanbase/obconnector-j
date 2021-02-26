package com.alipay.oceanbase.jdbc;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public interface ObArray extends ResultSet, Array, ObComplexData {
    String getBaseTypeName() throws SQLException;

    int getBaseType() throws SQLException;

    Object getArray() throws SQLException;

    Object getArray(Map<String, Class<?>> var1) throws SQLException;

    Object getArray(long var1, int var3) throws SQLException;

    Object getArray(long var1, int var3, Map<String, Class<?>> var4) throws SQLException;

    ResultSet getResultSet() throws SQLException;

    ResultSet getResultSet(Map<String, Class<?>> var1) throws SQLException;

    ResultSet getResultSet(long var1, int var3) throws SQLException;

    ResultSet getResultSet(long var1, int var3, Map<String, Class<?>> var4) throws SQLException;

    void free() throws SQLException;
}
