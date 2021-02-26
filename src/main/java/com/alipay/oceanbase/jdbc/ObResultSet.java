package com.alipay.oceanbase.jdbc;

import com.alipay.oceanbase.jdbc.extend.datatype.*;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface ObResultSet extends ResultSet {
    TIMESTAMPTZ getTIMESTAMPTZ(int columnIndex) throws SQLException;

    TIMESTAMPTZ getTIMESTAMPTZ(String columnName) throws SQLException;

    TIMESTAMPLTZ getTIMESTAMPLTZ(int columnIndex) throws SQLException;

    TIMESTAMPLTZ getTIMESTAMPLTZ(String columnName) throws SQLException;

    INTERVALYM getINTERVALYM(int columnIndex) throws SQLException;

    INTERVALYM getINTERVALYM(String columnName) throws SQLException;

    INTERVALDS getINTERVALDS(int columnIndex) throws SQLException;

    INTERVALDS getINTERVALDS(String columnName) throws SQLException;

    NUMBER getNUMBER(int columnIndex) throws SQLException;

    NUMBER getNUMBER(String columnName) throws SQLException;
}
