/**
 *  OceanBase Client for Java
 *
 *  Copyright (c) 2012-2014 Monty Program Ab.
 *  Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *  Copyright (c) 2021 OceanBase.
 *
 *  This library is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU Lesser General Public License as published by the Free
 *  Software Foundation; either version 2.1 of the License, or (at your option)
 *  any later version.
 *
 *  This library is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License along
 *  with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 *  This particular MariaDB Client for Java file is work
 *  derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 *  the following copyright and notice provisions:
 *
 *  Copyright (c) 2009-2011, Marcus Eriksson
 *
 *  Redistribution and use in source and binary forms, with or without modification,
 *  are permitted provided that the following conditions are met:
 *  Redistributions of source code must retain the above copyright notice, this list
 *  of conditions and the following disclaimer.
 *
 *  Redistributions in binary form must reproduce the above copyright notice, this
 *  list of conditions and the following disclaimer in the documentation and/or
 *  other materials provided with the distribution.
 *
 *  Neither the name of the driver nor the names of its contributors may not be
 *  used to endorse or promote products derived from this software without specific
 *  prior written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 *  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 *  IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 *  INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 *  NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 *  PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 *  WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 *  OF SUCH DAMAGE.
 */
package com.oceanbase.jdbc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;

import com.oceanbase.jdbc.internal.ColumnType;
import com.oceanbase.jdbc.internal.com.read.resultset.ColumnDefinition;
import com.oceanbase.jdbc.internal.com.read.resultset.SelectResultSet;
import com.oceanbase.jdbc.internal.com.send.parameters.NullParameter;
import com.oceanbase.jdbc.internal.com.send.parameters.ParameterHolder;
import com.oceanbase.jdbc.internal.com.send.parameters.StringParameter;
import com.oceanbase.jdbc.internal.util.ParsedCallParameters;
import com.oceanbase.jdbc.internal.util.Utils;
import com.oceanbase.jdbc.internal.util.dao.CallableStatementCacheKey;
import com.oceanbase.jdbc.internal.util.dao.CloneableCallableStatement;
import com.oceanbase.jdbc.internal.util.dao.ServerPrepareResult;
import com.oceanbase.jdbc.internal.util.exceptions.ExceptionFactory;

import static com.oceanbase.jdbc.OceanBaseConnection.ACTUAL_SQL_FUNC_PATTERN;
import static com.oceanbase.jdbc.OceanBaseConnection.CALLABLE_STATEMENT_PATTERN;

public class JDBC4ServerCallableStatement extends CallableProcedureStatement implements
                                                                            CloneableCallableStatement {

    private SelectResultSet outputResultSet;
    private final String    PARAMETER_NAMESPACE_PREFIX = "@com_mysql_jdbc_outparam_";

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
    public JDBC4ServerCallableStatement(boolean isObFunction, String query,
                                        OceanBaseConnection connection, String procedureName,
                                        String database, String arguments, int resultSetType,
                                        int resultSetConcurrency, ExceptionFactory exceptionFactory)
                                                                                                    throws SQLException {
        super(isObFunction, connection, query, resultSetType, resultSetConcurrency,
            exceptionFactory);
        this.isObFunction = isObFunction;
        this.arguments = arguments;
        if (!this.connection.getProtocol().isOracleMode()) {
            this.parameterMetadata = new CallableParameterMetaData(connection, database,
                procedureName, false);
        } else {
            this.parameterMetadata = new OceanBaseCallableParameterMetaData(connection, database,
                procedureName, isObFunction);
        }
        parameterMetadata.readMetadataFromDbIfRequired(this.originalSql, this.arguments,
            this.isObFunction);
        setParamsAccordingToSetArguments();
    }

    // only for oracle mode
    public JDBC4ServerCallableStatement(boolean isObFunction, String query,
                                        OceanBaseConnection connection, String procedureName,
                                        String database, String arguments, int resultSetType,
                                        int resultSetConcurrency,
                                        ExceptionFactory exceptionFactory, boolean isAnonymousBlock)
                                                                                                    throws SQLException {
        super(isObFunction, connection, query, resultSetType, resultSetConcurrency,
            exceptionFactory);
        this.arguments = arguments;
        this.parameterMetadata = new OceanBaseCallableParameterMetaData(connection, database,
            procedureName, isObFunction);
        //        if (!protocol.supportStmtPrepareExecute()) {
        //            this.parameterMetadata.generateMetadataFromPrepareResultSet(this.serverPrepareResult);
        ////            parameterMetadata.readMetadataFromDbIfRequired(this.originalSql, this.arguments, this.isObFunction);
        //        }
        parameterMetadata.readMetadataFromDbIfRequired(this.originalSql, this.arguments,
            this.isObFunction);
        setParamsAccordingToSetArguments();
        parameterMetadata.parameterCountOfStmt = this.parameterCount;
    }

    private void setParamsAccordingToSetArguments() throws SQLException {
        if (parameterCount == -1) {
            if (arguments == null || arguments.equals("")) {
                parameterCount = 0;
            } else {
                List<ParsedCallParameters> paramList = Utils.argumentsSplit(arguments, ",", "'\"",
                        "'\"");
                parameterCount = paramList.size();
            }
            if (isObFunction) {
                parameterCount ++;
            }
        }

        params = new ArrayList<>(parameterCount);
        for (int index = 0; index < parameterCount; index++) {
            CallParameter callParameter = new CallParameter();

            CallParameter callParameterFromOBServer = parameterMetadata.getPlaceholderParam(index);
            if (null != callParameterFromOBServer) {
                if (!protocol.isOracleMode() && callParameterFromOBServer.isOutput()) {
                    callParameter = callParameterFromOBServer;
                } else {
                    callParameter.setName(callParameterFromOBServer.getName());
                    callParameter.setIndex(callParameterFromOBServer.getIndex());
                }
            }
            params.add(callParameter);
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
    public JDBC4ServerCallableStatement clone(OceanBaseConnection connection)
                                                                             throws CloneNotSupportedException {
        JDBC4ServerCallableStatement clone = (JDBC4ServerCallableStatement) super.clone(connection);
        clone.outputResultSet = null;
        return clone;
    }

    private void retrieveOutputResult() throws SQLException {
        // resultSet will be just before last packet
        outputResultSet = results.getCallableResultSet();
        if (outputResultSet != null) {
            outputResultSet.next();
            SelectResultSet selectResultSet = outputResultSet;
            selectResultSet.row.complexEndPos = selectResultSet.complexEndPos;
            ColumnDefinition[] ci = selectResultSet.getColumnsInformation();
            for (int i = 1; i <= ci.length; i++) {
                ColumnType columnTypes = ci[i - 1].getColumnType();
                if (columnTypes == ColumnType.COMPLEX || columnTypes == ColumnType.ARRAY
                    || columnTypes == ColumnType.STRUCT) {
                    selectResultSet.getComplex(i);
                } else if (columnTypes == ColumnType.CURSOR) {
                    selectResultSet.getComplexCursor(i);
                }
            }
        }
    }

    public void setParameter(final int parameterIndex, final ParameterHolder holder)
                                                                                    throws SQLException {
        if (protocol.isOracleMode() && parameterIndex > parameterCount) {
            throw new SQLException("invalid parameter index " + parameterIndex);
        }
        getParameter(parameterIndex).setInput(true);
        super.setParameter(parameterIndex, holder);
    }

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

    private int setInOutParamsOnServer() throws SQLException {
        if (protocol != null) {
            protocol.startCallInterface();
        }

        this.parameterMetadata.readMetadataFromDbIfRequired(this.originalSql, this.arguments, this.isObFunction);
        validAllParameters();

        List<String> mysqlParamNames = new ArrayList<>(parameterMetadata.allParams.size());
        for (int i = 0; i < parameterMetadata.allParams.size(); i++) {
            String paramName = parameterMetadata.allParams.get(i).getName();
            String inOutParameterName;
            if (paramName == null) {
                throw new SQLException("param[" + i + "] name is null.");
            } else {
                inOutParameterName = mangleParameterName(paramName);
            }
            mysqlParamNames.add(inOutParameterName);
        }

        for (int i = 0; i < parameterMetadata.placeholderParams.size(); i++) {
            String paramName = parameterMetadata.placeholderParams.get(i).getName();
            String inOutParameterName;
            if (paramName == null) {
                throw new SQLException("param[" + i + "] name is null.");
            } else {
                inOutParameterName = mangleParameterName(paramName);
            }

            CallParameter inParamInfo = parameterMetadata.placeholderParams.get(i);
            if (inParamInfo.isInput() && inParamInfo.isOutput()) {
                StringBuilder queryBuf = new StringBuilder(4 + inOutParameterName.length() + 1 + 1);
                ParameterHolder holder = currentParameterHolder.get(i);
                if (holder != null) {
                    String holderStr;
                    if (holder instanceof StringParameter) {
                        holderStr = ((StringParameter) holder).getFullString();
                    } else {
                        holderStr = holder.toString();
                    }
                    queryBuf.append("SET ");
                    queryBuf.append(inOutParameterName);
                    queryBuf.append("=");
                    if (holderStr == "<null>") {
                        queryBuf.append("null");
                    } else {
                        queryBuf.append(holderStr);
                    }

                    String query = queryBuf.toString().replaceAll("\\`(\\w+)\\`", "$1");
                    this.execute(query);
                }
            }
        }
        if (this.parameterMetadata.outputParamSetParamValueException != null) {
            // throwing at this stage is compatible with mysql
            throw this.parameterMetadata.outputParamSetParamValueException;
        }

        Utils.TrimSQLInfo trimSQLStringInternal = Utils.trimSQLStringInternal(originalSql, false, false, false, false);
        String trimedString = trimSQLStringInternal.getTrimedString();
        int paramCount = trimSQLStringInternal.getParamCount();
        int beginIndex = 0;
        StringBuilder sb = new StringBuilder();
        List<Integer> paramIndexs = trimSQLStringInternal.getParamsIndexs();
        if (paramCount > 0  && paramIndexs.size() > 0) {
            for (int i = 0; i < paramIndexs.size() ; i++) {
                int parameterIndex = parameterMetadata.placeholderToParameterIndexMap[i];
                int end = paramIndexs.get(i);
                String partPre = trimedString.substring(beginIndex,end);
                CallParameter inParamInfo = parameterMetadata.allParams.get(parameterIndex);
                sb.append(partPre);
                beginIndex = end + 1;
                if (inParamInfo.isOutput()) {
                    String inOutParameterName = mysqlParamNames.get(parameterIndex);
                    inOutParameterName = inOutParameterName.replaceAll("\\`(\\w+)\\`", "$1");
                    sb.append(inOutParameterName);
                } else {
                    ParameterHolder holder = currentParameterHolder.get(i);
                    String holderStr;
                    if (holder instanceof StringParameter) {
                        holderStr = ((StringParameter) holder).getFullString();
                    } else {
                        holderStr = holder.toString();
                    }
                    if (holderStr.equals("<null>")) {
                        sb.append("null" );
                    } else {
                        sb.append(holderStr);
                    }
                }
            }
        }
        sb.append(trimedString.substring(beginIndex));

        int r = this.executeUpdate(sb.toString());
        outputResultSet = results.getCallableResultSet();
        if (outputResultSet != null) {
            outputResultSet.next();
        }

        if (protocol != null) {
            protocol.endCallInterface("JDBC4ServerCallableStatement.setInOutParamsOnServer");
        }
        return r;
    }

    @Override
    public boolean execute() throws SQLException {
        ReentrantLock curLock = connection.lock;
        curLock.lock();
        try {
            lockLogger.debug("JDBC4ServerCallableStatement.execute locked");

            if (!protocol.isOracleMode()) {
                checkParameterInMySQLMode();
                setInOutParamsOnServer();
                return true;
            }
            validAllParameters();
            super.execute();
            // TODO: set this.parameterMetadata according to results from server
            retrieveOutputResult();
            return results != null && results.getResultSet() != null;
        } finally {
            curLock.unlock();
            lockLogger.debug("JDBC4ServerCallableStatement.execute unlocked");
        }
    }

    /**
     * Valid that all parameters are set.
     *
     * @throws SQLException if set parameters is not right
     */
    private void validAllParameters() throws SQLException {
        if (protocol.isOracleMode() && params.size() != parameterCount) {
            throw new SQLException(
                "The number of parameter names '" + params.size()
                        + "' does not match the number of registered parameters in sql '"
                        + parameterCount + "'.");
        }

        // calculate outputParameterMapper
        if (protocol.isOracleMode() && parameterMetadata.hasEmptyPlaceholderParams()) {
            if (outputParameterMapper == null) {
                outputParameterMapper = new int[params.size()];
            }
            int currentOutputMapper = 1;
            for (int index = 0; index < params.size(); index++) {
                outputParameterMapper[index] = params.get(index).isOutput() ? currentOutputMapper++
                    : -1;
            }
        } else {
            if (outputParameterMapper == null) {
                outputParameterMapper = new int[parameterMetadata.placeholderParams.size()];
            }
            int currentOutputMapper = 1;
            for (int index = 0; index < parameterMetadata.placeholderParams.size(); index++) {
                outputParameterMapper[index] = parameterMetadata.getPlaceholderParam(index)
                    .isOutput() ? currentOutputMapper++ : -1;
            }
        }

        // Set value for OUT parameters
        for (int index = 0; index < params.size(); index++) {
            if (!params.get(index).isInput()) {
                if (!protocol.isOracleMode() || params.get(index).isOutput()) {
                    super.setParameter(index + 1, new NullParameter());
                } else {
                    throw new SQLException("Missing IN or OUT parameter in index::" + (index + 1));
                }
            }
        }
    }

    @Override
    public int[] executeBatch() throws SQLException {
        if (!hasInOutParameters) {
            if (!protocol.isOracleMode() && !isObFunction) {
                int[] retBatchQuery = new int[0];
                int[] retBatch = new int[0];
                int querySize = parametersList.size();
                if (querySize == 0) {
                    retBatch = new int[0];
                } else {
                    retBatch = executeBatchInternal(querySize);
                }

                if (batchQueries != null && (batchQueries.size()) > 0) {
                    retBatchQuery = super.executeBatchQuerys();
                }
                int[] ret = new int[retBatch.length + retBatchQuery.length];
                int cur = 0;
                for (int i = 0; i < retBatch.length; i++) {
                    ret[cur++] = retBatch[i];
                }
                for (int i = 0; i < retBatchQuery.length; i++) {
                    ret[cur++] = retBatchQuery[i];
                }
                return ret;
            } else {
                return super.executeBatch();
            }
        } else {
            throw new SQLException("executeBatch not permit for procedure with output parameter");
        }
    }

    /**
     * execute batch one by one for mysql mode procedure
     * @param querySize
     * @return int array of the row count for SQL Data Manipulation Language (DML) statements
     * @throws SQLException
     */
    private int[] executeBatchInternal(int querySize) throws SQLException {
        int[] rows = new int[querySize];
        for (int i = 0; i < querySize; i++) {
            ParameterHolder[] parameterHolder = parametersList.get(i);
            clearParameters();
            for (int j = 0; j < parameterHolder.length; j++) {
                currentParameterHolder.put(j, parameterHolder[j]);
            }
            rows[i] = setInOutParamsOnServer();
        }
        return rows;
    }

    /**
     * Execute batch, like executeBatch(), with returning results with long[]. For when row count may
     * exceed Integer.MAX_VALUE.
     *
     * @return an array of update counts (one element for each command in the batch)
     * @throws SQLException if a database error occur.
     */
    @Override
    public long[] executeLargeBatch() throws SQLException {
        if (!hasInOutParameters) {
            if (!protocol.isOracleMode() && !isObFunction) {
                long[] retBatchQuery = new long[0];
                long[] retBatch = new long[0];
                int querySize = parametersList.size();
                if (querySize == 0) {
                    retBatch = new long[0];
                } else {
                    retBatch = executeLargeBatchInternal(querySize);
                }

                if (batchQueries != null && (batchQueries.size()) > 0) {
                    retBatchQuery = super.executeLargeBatchQuerys();
                }
                long[] ret = new long[retBatch.length + retBatchQuery.length];
                int cur = 0;
                for (int i = 0; i < retBatch.length; i++) {
                    ret[cur++] = retBatch[i];
                }
                for (int i = 0; i < retBatchQuery.length; i++) {
                    ret[cur++] = retBatchQuery[i];
                }
                return ret;
            } else {
                return super.executeLargeBatch();
            }
        } else {
            throw new SQLException("executeBatch not permit for procedure with output parameter");
        }
    }

    /**
     * execute batch one by one for mysql mode procedure
     * @param querySize
     * @return int array of the row count for SQL Data Manipulation Language (DML) statements
     * @throws SQLException
     */
    private long[] executeLargeBatchInternal(int querySize) throws SQLException {
        long[] rows = new long[querySize];
        for (int i = 0; i < querySize; i++) {
            ParameterHolder[] parameterHolder = parametersList.get(i);
            clearParameters();
            for (int j = 0; j < parameterHolder.length; j++) {
                currentParameterHolder.put(j, parameterHolder[j]);
            }
            rows[i] = setInOutParamsOnServer();
        }
        return rows;
    }

    private void checkParameterInMySQLMode() throws SQLException {
        for (CallParameter param : params) {
            if (!param.isOutput() && !param.isInput()) {
                throw new SQLException("missing param error");
            }
        }
    }

    public boolean inCallableStatementCache(OceanBaseConnection connection){
        String database ;
        String querySetToServer = this.originalSql;
        Matcher matcher = CALLABLE_STATEMENT_PATTERN.matcher(querySetToServer);
        if (matcher.matches()) {
            querySetToServer = matcher.group(2);
            database = matcher.group(10) == null ? null : matcher.group(10).trim();
        } else if (this.isObFunction) {
            Matcher matcherOfunc = ACTUAL_SQL_FUNC_PATTERN.matcher(querySetToServer);
            if (!matcherOfunc.matches()) {
                return false;
            }
            database = matcherOfunc.group(8) == null ? null : matcherOfunc.group(8).trim();
        } else {
            return false;
        }

        if (database == null) {
            database = connection.getProtocol().getDatabase();
        }
        if (database != null && options.cacheCallableStmts) {
            CallableStatementCacheKey cacheKey = new CallableStatementCacheKey(database, querySetToServer);
            return connection.getCallableStatementCache().containsKey(cacheKey);
        }
        return false;
    }

    public void addCallableStatementCacheIfNeed(ServerPrepareResult serverPrepareResult) {
        if (!options.cacheCallableStmts) {
            return;
        }
        String databaseOfStmt = null;
        if (serverPrepareResult != null && serverPrepareResult.getStatementId() > 0) {
            String originalSql = this.getOriginalSql();
            Matcher matcher = CALLABLE_STATEMENT_PATTERN.matcher(originalSql);
            if (matcher.matches()) {
                databaseOfStmt = matcher.group(10) == null ? null : matcher.group(10).trim();
            }
            if (databaseOfStmt == null && this.protocol.sessionStateAware()) {
                databaseOfStmt = protocol.getDatabase();
            }
            if (databaseOfStmt != null) {
                CallableStatementCacheKey cacheKey = new CallableStatementCacheKey(databaseOfStmt, originalSql);
                this.connection.getCallableStatementCache().put(cacheKey, this);
            }
        }
    }

}
