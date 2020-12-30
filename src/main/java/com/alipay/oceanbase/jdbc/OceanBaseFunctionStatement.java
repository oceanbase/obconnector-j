package com.alipay.oceanbase.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import com.alipay.oceanbase.jdbc.internal.com.read.resultset.SelectResultSet;
import com.alipay.oceanbase.jdbc.internal.com.send.parameters.ParameterHolder;
import com.alipay.oceanbase.jdbc.internal.util.dao.CloneableCallableStatement;
import com.alipay.oceanbase.jdbc.internal.util.exceptions.ExceptionFactory;

public class OceanBaseFunctionStatement extends CallableFunctionStatement implements
                                                                         CloneableCallableStatement {

    private SelectResultSet outputResultSet = null;

    /**
     * Specific implementation of CallableStatement to handle function call, represent by call like
     * {?= call procedure-name[(arg1,arg2, ...)]}.
     *
     * @param connection current connection
     * @param databaseName database name
     * @param procedureName function name
     * @param arguments function args
     * @param resultSetType a result set type; one of <code>ResultSet.TYPE_FORWARD_ONLY</code>, <code>
     *     ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @param resultSetConcurrency a concurrency type; one of <code>ResultSet.CONCUR_READ_ONLY</code>
     *     or <code>ResultSet.CONCUR_UPDATABLE</code>
     * @param exceptionFactory Exception factory
     * @throws SQLException exception
     */
    public OceanBaseFunctionStatement(OceanBaseConnection connection, String databaseName,
                                      String procedureName, String arguments, int resultSetType,
                                      final int resultSetConcurrency,
                                      ExceptionFactory exceptionFactory) throws SQLException {
        super(connection, "SELECT " + procedureName + ((arguments == null) ? "()" : arguments),
            resultSetType, resultSetConcurrency, exceptionFactory);
        parameterMetadata = new CallableParameterMetaData(connection, databaseName, procedureName,
            true);
        super.initFunctionData(getParameterCount() + 1);
    }

    protected SelectResultSet getResult() throws SQLException {
        if (outputResultSet == null) {
            throw new SQLException("No output result");
        }
        return outputResultSet;
    }

    /**
     * Clone statement.
     *
     * @param connection connection
     * @return Clone statement.
     * @throws CloneNotSupportedException if any error occur.
     */
    public OceanBaseFunctionStatement clone(OceanBaseConnection connection)
                                                                           throws CloneNotSupportedException {
        OceanBaseFunctionStatement clone = (OceanBaseFunctionStatement) super.clone(connection);
        clone.outputResultSet = null;
        return clone;
    }

    /**
     * Executes the CALL statement.
     *
     * @return either (1) the row count for SQL Data Manipulation Language (DML) statements or (2) 0
     *     for SQL statements that return nothing
     * @throws SQLException if a database access error occurs; this method is called on a closed
     *     <code>PreparedStatement</code> or the SQL statement returns a <code>ResultSet</code> object
     */
    @Override
    public int executeUpdate() throws SQLException {
        connection.lock.lock();
        try {
            super.execute();
            retrieveOutputResult();
            if (results != null && results.getResultSet() == null) {
                return 0;
            }
            return getUpdateCount();
        } finally {
            connection.lock.unlock();
        }
    }

    private void retrieveOutputResult() throws SQLException {
        outputResultSet = results.getResultSet();
        if (outputResultSet != null) {
            outputResultSet.next();
        }
    }

    public void setParameter(final int parameterIndex, final ParameterHolder holder)
                                                                                    throws SQLException {
        super.setParameter(parameterIndex - 1, holder);
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        connection.lock.lock();
        try {
            super.execute();
            retrieveOutputResult();
            if (results != null && results.getResultSet() == null) {
                return results.getResultSet();
            }
            return SelectResultSet.createEmptyResultSet();
        } finally {
            connection.lock.unlock();
        }
    }

    @Override
    public boolean execute() throws SQLException {
        connection.lock.lock();
        try {
            super.execute();
            retrieveOutputResult();
            return results != null && results.getResultSet() == null;
        } finally {
            connection.lock.unlock();
        }
    }
}
