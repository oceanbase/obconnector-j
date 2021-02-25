package com.alipay.oceanbase.jdbc;

import com.alipay.oceanbase.jdbc.internal.util.exceptions.ExceptionFactory;

import java.sql.SQLException;

public class OceanBaseFunctionStatement extends JDBC4CallableStatement {
    /**
     * Specific implementation of CallableStatement to handle function call, represent by call like
     * {?= call procedure-name[(arg1,arg2, ...)]}.
     *
     * @param connection           current connection
     * @param databaseName         database name
     * @param procedureName        function name
     * @param arguments            function args
     * @param resultSetType        a result set type; one of <code>ResultSet.TYPE_FORWARD_ONLY</code>, <code>
     *                             ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @param resultSetConcurrency a concurrency type; one of <code>ResultSet.CONCUR_READ_ONLY</code>
     *                             or <code>ResultSet.CONCUR_UPDATABLE</code>
     * @param exceptionFactory     Exception factory
     * @throws SQLException exception
     */
    public OceanBaseFunctionStatement(OceanBaseConnection connection, String databaseName,
                                      String procedureName, String arguments, int resultSetType,
                                      int resultSetConcurrency, ExceptionFactory exceptionFactory)
                                                                                                  throws SQLException {
        super(connection, databaseName, procedureName, arguments, resultSetType,
            resultSetConcurrency, exceptionFactory);
    }
}
