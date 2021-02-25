package com.alipay.oceanbase.jdbc;

import java.sql.Connection;

public class OceanBaseDatabaseMetaData extends JDBC4DatabaseMetaData {
    /**
     * Constructor.
     *
     * @param connection connection
     * @param urlParser  Url parser
     */
    public OceanBaseDatabaseMetaData(Connection connection, UrlParser urlParser) {
        super(connection, urlParser);
    }
}