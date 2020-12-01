package com.alipay.oceanbase.jdbc;

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

    /**
     * Specific implementation of CallableStatement to handle function call, represent by call like
     * {?= call procedure-name[(arg1,arg2, ...)]}.
     *
     * @param query query
     * @param connection current connection
     * @param procedureName procedure name
     * @param database database
     * @param resultSetType a result set type; one of <code>ResultSet.TYPE_FORWARD_ONLY</code>, <code>
     *     ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @param resultSetConcurrency a concurrency type; one of <code>ResultSet.CONCUR_READ_ONLY</code>
     *     or <code>ResultSet.CONCUR_UPDATABLE</code>
     * @param exceptionFactory Exception Factory
     * @throws SQLException exception
     */
    public OceanBaseProcedureStatement(String query, OceanBaseConnection connection,
                                       String procedureName, String database, int resultSetType,
                                       int resultSetConcurrency, ExceptionFactory exceptionFactory)
                                                                                                   throws SQLException {
        super(connection, query, resultSetType, resultSetConcurrency, exceptionFactory);
        this.parameterMetadata = new CallableParameterMetaData(connection, database, procedureName,
            false);
        setParamsAccordingToSetArguments();
        setParametersVariables();
    }

    private void setParamsAccordingToSetArguments() {
    params = new ArrayList<>(parameterCount);
    for (int index = 0; index < parameterCount; index++) {
      params.add(new CallParameter());
    }
  }

    private void addParametersFromDBOD(java.sql.ResultSet paramTypesRs) throws SQLException {
        int i = 0;
        while (paramTypesRs.next()) {
            String paramName = paramTypesRs.getString("ARGUMENT_NAME");
            String inOut = paramTypesRs.getString("IN_OUT");
            //            int inOutModifier = DatabaseMetaData.procedureColumnUnknown;
            //            boolean isOutParameter = false;
            //            boolean isInParameter = false;
            //            if (this.numberOfParameters() == 0 && this.getfucn) {
            //                isOutParameter = true;
            //                isInParameter = false;
            //            } else if (null == inOut || inOut.equalsIgnoreCase("IN")) {
            //                isOutParameter = false;
            //                isInParameter = true;
            //                inOutModifier = DatabaseMetaData.procedureColumnIn;
            //            } else if (inOut.equalsIgnoreCase("INOUT")) {
            //                isOutParameter = true;
            //                isInParameter = true;
            //                inOutModifier = DatabaseMetaData.procedureColumnInOut;
            //            } else if (inOut.equalsIgnoreCase("OUT")) {
            //                isOutParameter = true;
            //                isInParameter = false;
            //                inOutModifier = DatabaseMetaData.procedureColumnOut;
            //            } else {
            //                isInParameter = true;
            //                isOutParameter = false;
            //                inOutModifier = DatabaseMetaData.procedureColumnIn;
            //            }

            String typeName = paramTypesRs.getString("DATA_TYPE");
            int precision = paramTypesRs.getInt("DATA_PRECISION");
            int scale = paramTypesRs.getInt("DATA_SCALE");
            //++this.numParameters;
        }
    }

    private void determineParameterTypes(String database, String procedureName) throws SQLException {
        if (database != null) {
            database = database.replace("`", "");
        } else {
            database = null;
        }
        String proc = procedureName.replace("`", "");
        java.sql.ResultSet paramTypesRs = null; // NOPMD
        java.sql.Statement paramRetrievalStmt = null; // NOPMD
        paramRetrievalStmt = this.connection.createStatement();
        /* TODO due to the ObServer will have two same procedure for user (with schema and without schema), we add DISTINCT for ARGUMENT_NAME
           to get one from them. */
        String sqlHeader = "SELECT DISTINCT(ARGUMENT_NAME), IN_OUT, DATA_TYPE, DATA_PRECISION, DATA_SCALE, POSITION FROM ALL_ARGUMENTS WHERE"
                           + " (OVERLOAD is NULL OR OVERLOAD = 1) and POSITION != 0 AND object_name = '";
        StringBuilder paramMetaSql = new StringBuilder(sqlHeader);

        String objectName = database + "." + proc;
        String[] objectNameList = objectName.split("\\.");
        if (objectNameList.length == 2) {
            paramMetaSql.append(objectNameList[1].toUpperCase());
            paramMetaSql.append("' and package_name = '").append(objectNameList[0].toUpperCase());
        } else {
            paramMetaSql.append(objectNameList[0].toUpperCase());
        }

        paramMetaSql.append("' order by POSITION");
        paramTypesRs = paramRetrievalStmt.executeQuery(paramMetaSql.toString());

        if (paramTypesRs.next()) {
            paramTypesRs.previous();
            //                convertGetProcedureColumnsToInternalDescriptors(paramTypesRs);
            //                addParametersFromDBOD(paramTypesRs);
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

    @Override
    public boolean execute() throws SQLException {
        connection.lock.lock();
        try {
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
