package com.alipay.oceanbase.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import com.alipay.oceanbase.jdbc.internal.com.read.resultset.SelectResultSet;
import com.alipay.oceanbase.jdbc.internal.com.send.parameters.NullParameter;
import com.alipay.oceanbase.jdbc.internal.com.send.parameters.ParameterHolder;
import com.alipay.oceanbase.jdbc.internal.util.dao.CloneableCallableStatement;
import com.alipay.oceanbase.jdbc.internal.util.exceptions.ExceptionFactory;

public class OceanBaseProcedureStatement extends CallableProcedureStatement implements
                                                                           CloneableCallableStatement {

    private SelectResultSet outputResultSet = null;

    private boolean         isObFunction    = false;

    /**
     * Specific implementation of CallableStatement to handle function call, represent by call like
     * {?= call procedure-name[(arg1,arg2, ...)]}.
     *
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
        super(isObFunction, connection, query, resultSetType, resultSetConcurrency,
            exceptionFactory);
        this.isObFunction = isObFunction;
        if (!this.connection.getProtocol().isOracleMode()) {
            this.parameterMetadata = new CallableParameterMetaData(connection, database,
                procedureName, false);
            this.parameterMetadata.readMetadataFromDbIfRequired(query);
        } else {
            this.parameterMetadata = new OceanBaseCallableParameterMetaData(connection, database,
                procedureName, isObFunction);
            this.parameterMetadata.readMetadataFromDbIfRequired();
        }
        //        determineParameterTypes(query,database,procedureName);
        setParamsAccordingToSetArguments();
        setParametersVariables();
    }

    private void setParamsAccordingToSetArguments() throws SQLException {
        if (parameterCount > this.parameterMetadata.getParameterCount()) {
            System.out.println("SIZE:" + parameterCount + " " + this.parameterMetadata.getParameterCount());
            throw new SQLException("Internal error for arguments number: parameter size not equal with prepare.");
        }
        parameterCount = this.parameterMetadata.getParameterCount();
        /*
        OceanBaseResultSetMetaData metaData = (OceanBaseResultSetMetaData)this.getMetaData();
        if (parameterCount != metaData.getColumnCount()) {
            //System.out.println("SIZE2:" + parameterCount + " " + metaData.getColumnCount());
            throw new SQLException("Internal error for arguments number: parameter size not equal with prepare.");
        } */
        //System.out.println("SQL:" + this.originalSql);
        params = new ArrayList<>(parameterCount);
        /*
        for (int index = 0; index < parameterCount; index++) {
            CallParameter addParamMeta = null;
            if (null != metaData.getColumnName(index + 1)) {
                addParamMeta = (CallParameter)this.parameterMetadata.getParamByName(metaData.getColumnName(index + 1));
            }
            System.out.println(addParamMeta.toString() + " " + addParamMeta.isOutput() + " " + addParamMeta.getName());
            //params.add(new CallParameter());
            //if (this.parameterMetadata.getName(index))
            //params.add(this.parameterMetadata.getParam(index + 1));
            params.add(addParamMeta);
        }*/
        for (int index = 0; index < this.parameterMetadata.getParameterCount(); index++) {
            this.params.add(this.parameterMetadata.getParam(index + 1)); //TODO if check registerOutputParamter need to add it by arguments
        }
    }

    private void setInputOutputParameterMap() {
        if (outputParameterMapper == null) {
            outputParameterMapper = new int[params.size()];
            int currentOutputMapper = 1;

            for (int index = 0; index < params.size(); index++) {
                outputParameterMapper[index] = params.get(index).isOutput() ? currentOutputMapper++
                    : -1;
            }
        }
    }

    protected SelectResultSet getOutputResult() throws SQLException {
        if (outputResultSet == null) {
            if (this.isObFunction) { // For ObFunction
                outputResultSet = results.getResultSet();
                if (outputResultSet != null) {
                    outputResultSet.next();
                    return outputResultSet;
                }
            }
            if (fetchSize != 0) {
                results.loadFully(false, protocol);
                outputResultSet = results.getCallableResultSet();
                if (outputResultSet != null) {
                    outputResultSet.next();
                    return outputResultSet;
                }
            }
            throw new SQLException("No output result.");
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
    public OceanBaseProcedureStatement clone(OceanBaseConnection connection)
                                                                            throws CloneNotSupportedException {
        OceanBaseProcedureStatement clone = (OceanBaseProcedureStatement) super.clone(connection);
        clone.outputResultSet = null;
        return clone;
    }

    private void retrieveOutputResult() throws SQLException {
        // resultSet will be just before last packet
        outputResultSet = results.getCallableResultSet();
        if (outputResultSet != null) {
            outputResultSet.next();
        }
    }

    public void setParameter(final int parameterIndex, final ParameterHolder holder)
                                                                                    throws SQLException {
        params.get(parameterIndex - 1).setInput(true);
        super.setParameter(parameterIndex, holder);
    }

    private final String PARAMETER_NAMESPACE_PREFIX = "@com_mysql_jdbc_outparam_";

    private String mangleParameterName(String origParameterName) {
        //Fixed for 5.5+ in callers
        if (origParameterName == null) {
            return null;
        }

        int offset = 0;

        if (origParameterName.length() > 0 && origParameterName.charAt(0) == '@') {
            offset = 1;
        }

        StringBuilder paramNameBuf = new StringBuilder(PARAMETER_NAMESPACE_PREFIX.length()
                                                       + origParameterName.length());
        paramNameBuf.append(PARAMETER_NAMESPACE_PREFIX);
        paramNameBuf.append(origParameterName.substring(offset));

        return paramNameBuf.toString();
    }

    private void setInOutParamsOnServer() throws SQLException {
        int size = this.params.size();
        setInputOutputParameterMap();
        for (int i = 0; i < size; i++) {
            CallParameter inParamInfo = params.get(i);
            if (inParamInfo.isInput() && inParamInfo.isOutput()) {
                String inOutParameterName = mangleParameterName(inParamInfo.getName());
                StringBuilder queryBuf = new StringBuilder(4 + inOutParameterName.length() + 1 + 1);
                ParameterHolder holder = currentParameterHolder.get(i);
                queryBuf.append("SET ");
                queryBuf.append(inOutParameterName);
                queryBuf.append("=");
                if (holder.toString() == "<null>") {
                    queryBuf.append("null");
                } else {
                    //                    queryBuf.append("'");
                    queryBuf.append(holder.toString());
                    //                    queryBuf.append("'");
                }

                String query = queryBuf.toString().replaceAll("\\`(\\w+)\\`", "$1");
                this.connection.createStatement().execute(query);
            }
        }
        boolean afterFirst = false;
        int index = sql.indexOf('?');
        StringBuilder querySB = new StringBuilder();
        querySB.append(sql.substring(0, index));
        for (int i = 0; i < size; i++) {
            CallParameter inParamInfo = params.get(i);
            if (afterFirst) {
                querySB.append(",");
            }
            if (inParamInfo.isOutput()) {
                String inOutParameterName = mangleParameterName(inParamInfo.getName());
                inOutParameterName = inOutParameterName.replaceAll("\\`(\\w+)\\`", "$1");
                querySB.append(inOutParameterName);
            } else {
                ParameterHolder holder = currentParameterHolder.get(i);
                if (holder.toString() == "<null>") {
                    querySB.append("null");
                } else {
                    querySB.append(holder.toString());
                }
            }
            afterFirst = true;
        }
        querySB.append(")");
        this.connection.createStatement().execute(querySB.toString());
        StringBuilder selectSB = new StringBuilder();
        selectSB.append("SELECT ");
        afterFirst = false;
        boolean getResult = false;
        for (int i = 0; i < size; i++) {
            CallParameter inParamInfo = params.get(i);
            if (afterFirst) {
                selectSB.append(",");
            }
            if (inParamInfo.isOutput()) {
                String inOutParameterName = mangleParameterName(inParamInfo.getName());
                inOutParameterName = inOutParameterName.replaceAll("\\`(\\w+)\\`", "$1");
                selectSB.append(inOutParameterName);
                afterFirst = true;
                getResult = true;
            }
        }
        if (getResult) {
            ResultSet resultSet = this.connection.createStatement().executeQuery(
                selectSB.toString());
            resultSet.next();
            outputResultSet = (SelectResultSet) resultSet;
        }
    }

    private void executeProcedureOnServer() throws SQLException {
        int size = this.params.size();
        for (int i = 0; i < size; i++) {
            CallParameter inParamInfo = params.get(i);
            if (inParamInfo.isInput() || inParamInfo.isOutput()) {
                String inOutParameterName = mangleParameterName(inParamInfo.getName());
                StringBuilder queryBuf = new StringBuilder(4 + inOutParameterName.length() + 1 + 1);
                ParameterHolder holder = currentParameterHolder.get(i);
                queryBuf.append("call ");
                queryBuf.append(inOutParameterName);
                queryBuf.append("");
                queryBuf.append(holder.toString());
                String query = queryBuf.toString().replaceAll("\\`(\\w+)\\`", "$1");
                this.connection.createStatement().execute(query);
            }
        }
    }

    @Override
    public boolean execute() throws SQLException {
        connection.lock.lock();
        try {
            if (!protocol.isOracleMode()) {
                setInOutParamsOnServer();
                return true;
            }
            validAllParameters();
            super.executeInternal(fetchSize);
            retrieveOutputResult();
            return results != null && results.getResultSet() != null;
        } finally {
            connection.lock.unlock();
        }
    }

    /**
     * Valid that all parameters are set.
     *
     * @throws SQLException if set parameters is not right
     */
    private void validAllParameters() throws SQLException {

        setInputOutputParameterMap();
        // Set value for OUT parameters
        for (int index = 0; index < params.size(); index++) {
            if (!params.get(index).isInput()) {
                super.setParameter(index + 1, new NullParameter());
            }
        }
        validParameters();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        if (!hasInOutParameters) {
            return super.executeBatch();
        } else {
            throw new SQLException("executeBatch not permit for procedure with output parameter");
        }
    }
}
