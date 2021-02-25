package com.alipay.oceanbase.jdbc;

import com.alipay.oceanbase.jdbc.internal.util.exceptions.ExceptionFactory;

import java.sql.SQLException;

public class OceanBaseProcedureStatement extends JDBC4ServerCallableStatement {
    /**
     * Specific implementation of CallableStatement to handle function call, represent by call like
     * {?= call procedure-name[(arg1,arg2, ...)]}.
     *
     * @param isObFunction
     * @param query                query
     * @param connection           current connection
     * @param procedureName        procedure name
     * @param database             database
     * @param resultSetType        a result set type; one of <code>ResultSet.TYPE_FORWARD_ONLY</code>, <code>
     *                             ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @param resultSetConcurrency a concurrency type; one of <code>ResultSet.CONCUR_READ_ONLY</code>
     *                             or <code>ResultSet.CONCUR_UPDATABLE</code>
     * @param exceptionFactory     Exception Factory
     * @throws SQLException exception
     */
    public OceanBaseProcedureStatement(boolean isObFunction, String query,
                                       OceanBaseConnection connection, String procedureName,
                                       String database, int resultSetType,
                                       int resultSetConcurrency, ExceptionFactory exceptionFactory)
                                                                                                   throws SQLException {
        super(isObFunction, query, connection, procedureName, database, resultSetType,
            resultSetConcurrency, exceptionFactory);
    }
}
