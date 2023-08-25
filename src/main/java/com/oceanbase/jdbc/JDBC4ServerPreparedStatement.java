/**
 * OceanBase Client for Java
 * <p>
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2020 MariaDB Corporation Ab.
 * Copyright (c) 2021 OceanBase.
 * <p>
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 * <p>
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 * <p>
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 * <p>
 * This particular MariaDB Client for Java file is work
 * derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 * the following copyright and notice provisions:
 * <p>
 * Copyright (c) 2009-2011, Marcus Eriksson
 * <p>
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 * <p>
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 * <p>
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 * <p>
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 */
package com.oceanbase.jdbc;

import static com.oceanbase.jdbc.OceanBaseConnection.CALLABLE_STATEMENT_PATTERN;

import java.io.UnsupportedEncodingException;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;

import com.oceanbase.jdbc.internal.ColumnType;
import com.oceanbase.jdbc.internal.com.Packet;
import com.oceanbase.jdbc.internal.com.read.dao.Results;
import com.oceanbase.jdbc.internal.com.read.resultset.ColumnDefinition;
import com.oceanbase.jdbc.internal.com.read.resultset.SelectResultSet;
import com.oceanbase.jdbc.internal.com.send.parameters.NullParameter;
import com.oceanbase.jdbc.internal.com.send.parameters.ParameterHolder;
import com.oceanbase.jdbc.internal.logging.Logger;
import com.oceanbase.jdbc.internal.logging.LoggerFactory;
import com.oceanbase.jdbc.internal.util.Utils;
import com.oceanbase.jdbc.internal.util.dao.ClientPrepareResult;
import com.oceanbase.jdbc.internal.util.dao.ServerPrepareResult;
import com.oceanbase.jdbc.internal.util.exceptions.ExceptionFactory;
import com.oceanbase.jdbc.util.OceanBaseCRC32C;

public class JDBC4ServerPreparedStatement extends BasePrepareStatement implements Cloneable {

  private static final Logger logger = LoggerFactory.getLogger(JDBC4ServerPreparedStatement.class);
  private   List<ReturnParameter>           returnParams = new ArrayList<>();
  private int returnParamCount = 0;
  protected Map<Integer, ParameterHolder>   currentParameterHolder;
  protected List<ParameterHolder[]>         parametersList = new ArrayList<>();
  protected ServerPrepareResult             serverPrepareResult = null;
  private   OceanBaseResultSetMetaData      resultSetMetaData;
  private   OceanBaseParameterMetaData      parameterMetaData;
  private   boolean released;
  protected boolean isObFunction;
  private   boolean sendTypesToServer = false;
  private   boolean mustExecuteOnMaster;
  private   int     iterationCount;
  private   int     executeMode = 0x0000;
  private   long    checksum = 1;
  private   OceanBaseCRC32C                 crc32C = new OceanBaseCRC32C();

  /**
   * Constructor for creating Server prepared statement.
   *
   * @param connection           current connection
   * @param sql                  Sql String to prepare
   * @param resultSetScrollType  one of the following <code>ResultSet</code> constants: <code>
   *                             ResultSet.TYPE_FORWARD_ONLY</code>, <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or
   *                             <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
   * @param resultSetConcurrency a concurrency type; one of <code>ResultSet.CONCUR_READ_ONLY</code>
   *                             or <code>ResultSet.CONCUR_UPDATABLE</code>
   * @param autoGeneratedKeys    a flag indicating whether auto-generated keys should be returned; one
   *                             of <code>Statement.RETURN_GENERATED_KEYS</code> or <code>Statement.NO_GENERATED_KEYS</code>
   * @param exceptionFactory     Exception factory
   * @throws SQLException exception
   */
  public JDBC4ServerPreparedStatement(
      boolean isObFunction,
      OceanBaseConnection connection,
      String sql,
      int resultSetScrollType,
      int resultSetConcurrency,
      int autoGeneratedKeys,
      ExceptionFactory exceptionFactory) throws SQLException {
    super(connection, resultSetScrollType, resultSetConcurrency, autoGeneratedKeys, exceptionFactory);
    if (protocol != null) {
        protocol.startCallInterface();
    }

    this.isObFunction = isObFunction;
    currentParameterHolder = Collections.synchronizedMap(new TreeMap<>());
    mustExecuteOnMaster = protocol.isMasterConnection();

    originalSql = sql;
    Utils.TrimSQLInfo tmp = Utils.trimSQLStringInternal(originalSql, protocol.noBackslashEscapes(), protocol.isOracleMode(), false);
    simpleSql = tmp.getTrimedString();
    selectEndPos = tmp.getSelectEndPos();
    whereEndPos = tmp.getWhereEndPos();
    sqlType = Utils.getStatementType(simpleSql);
    actualSql = originalSql;
    // add rowid if needed
    if (protocol.isOracleMode() && sqlType == STMT_SELECT) {
      if (!(resultSetScrollType == ResultSet.TYPE_FORWARD_ONLY && resultSetConcurrency == ResultSet.CONCUR_READ_ONLY)) {
        if (!(resultSetScrollType == ResultSet.TYPE_SCROLL_INSENSITIVE && resultSetConcurrency == ResultSet.CONCUR_READ_ONLY)) {
          // sensitive ResultSet needs rowid to refetch
          addRowid = true;
          actualSql = actualSql.substring(0, selectEndPos + 1) + " rowid," + actualSql.substring(selectEndPos + 1);
        }
      }
    }

    // get count of return parameters for DML Returning Into characteristic
    initializeReturnParams();

    if (!protocol.isOracleMode()) {
      if (!simpleSql.startsWith("CALL") && !simpleSql.startsWith("call")) {
        prepare(actualSql);
      }
    } else {
      if (!protocol.supportStmtPrepareExecute()) {
        prepare(actualSql);
      } else {
        parameterCount = tmp.getParamCount();
      }
    }

    if (protocol != null) {
        protocol.endCallInterface("JDBC4ServerPreparedStatement");
    }
  }

  /**
   * Clone statement.
   *
   * @param connection connection
   * @return Clone statement.
   * @throws CloneNotSupportedException if any error occur.
   */
  public JDBC4ServerPreparedStatement clone(OceanBaseConnection connection) throws CloneNotSupportedException {
    JDBC4ServerPreparedStatement clone = (JDBC4ServerPreparedStatement) super.clone(connection);
    clone.released = false;
    clone.resultSetMetaData = resultSetMetaData;
    clone.parameterMetaData = parameterMetaData;
    clone.parametersList = new ArrayList<>();

    clone.isObFunction = isObFunction;
    clone.mustExecuteOnMaster = mustExecuteOnMaster;
    clone.originalSql = originalSql;
    clone.simpleSql = simpleSql;
    clone.selectEndPos = selectEndPos;
    clone.whereEndPos = whereEndPos;
    clone.sqlType = sqlType;
    clone.actualSql = actualSql;
    clone.addRowid = addRowid;

    // force prepare
    try {
      if (!clone.protocol.isOracleMode()) {
        if (!simpleSql.startsWith("CALL") && !simpleSql.startsWith("call")) {
          clone.prepare(actualSql);
        }
      } else {
        if (clone.protocol.supportStmtPrepareExecute()) {
          clone.parameterCount = parameterCount;
        }
        clone.prepare(actualSql);
      }
    } catch (SQLException e) {
      throw new CloneNotSupportedException("PrepareStatement not ");
    }
    return clone;
  }

  private void initializeReturnParams() {
      // check whether this sql is a DML statement
      if (!isDml(sqlType)) {
          return;
      }

      // calculate where "returning" starts and ends
      String completeStmt = simpleSql.toLowerCase(Locale.ROOT);
      int returningStartPos = completeStmt.indexOf("returning");
      if (returningStartPos < 1 || completeStmt.charAt(returningStartPos - 1) != ' ') {
          return;
      }
      int returningEndPos = returningStartPos + "returning".length();
      if (returningEndPos >= completeStmt.length() || completeStmt.charAt(returningEndPos) != ' ') {
          return;
      }

      // calculate where "into" starts and ends
      String returningClause = completeStmt.substring(returningStartPos);
      int intoStartPos = returningClause.indexOf("into");
      if (intoStartPos < 1 || returningClause.charAt(intoStartPos - 1) != ' ') {
          return;
      }
      int intoEndPos = intoStartPos + "into".length();
      if (intoEndPos >= returningClause.length() || returningClause.charAt(intoEndPos) != ' ') {
          return;
      }

      // calculate the count of ":var" or "?"
      for (int i = 0; i < returningClause.length(); i++) {
          if (returningClause.charAt(i) == ':' || returningClause.charAt(i) == '?') {
              returnParamCount++;
          }
      }

      // initialize return parameter list
      returnParams = new ArrayList<>(returnParamCount);
      for (int i = 0; i < returnParamCount; i++) {
          returnParams.add(new ReturnParameter());
      }
  }

  /***
   * Oracle：The refreshRow method is supported for the following result set categories:
   * scroll-sensitive/read-only, scroll-sensitive/updatable, scroll-insensitive/updatable
   * If a result set as mentioned above can't be refreshed due to other restriction like sql syntax,
   * its scrollType or concurrency will be degraded as following
   */
  private void degradeResultSetType() {
      if ((resultSetScrollType == ResultSet.TYPE_SCROLL_SENSITIVE && resultSetConcurrency == ResultSet.CONCUR_UPDATABLE)
        || (resultSetScrollType == ResultSet.TYPE_SCROLL_SENSITIVE && resultSetConcurrency == ResultSet.CONCUR_READ_ONLY)
        || (resultSetScrollType == ResultSet.TYPE_SCROLL_INSENSITIVE && resultSetConcurrency == ResultSet.CONCUR_UPDATABLE)) {
          resultSetScrollType = ResultSet.TYPE_SCROLL_INSENSITIVE;
      } else {
          resultSetScrollType = ResultSet.TYPE_FORWARD_ONLY;
      }
      resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
  }

  private void calculateCheckSum(String sql) throws SQLException {
      try {
          crc32C.reset();
          byte[] b; //  Consider the case of multiple character set
          b = sql.getBytes(options.characterEncoding);
          crc32C.update(b, 0, b.length);
          this.checksum = crc32C.getValue();
      } catch (UnsupportedEncodingException e) {
          try {
              this.close();
          } catch (Exception ee) {
              // eat exception.
          }
          SQLException sqlException = new SQLException("sql string getBytes error" + e.getMessage());
          logger.error("error preparing query", sqlException);
          throw exceptionFactory.raiseStatementError(connection, this).create(sqlException);
      }
  }

  private void prepare(String sql) throws SQLException {
    calculateCheckSum(sql);

    try {
      if (!protocol.supportStmtPrepareExecute()) {
        serverPrepareResult = protocol.prepare(sql, mustExecuteOnMaster);
        setMetaFromResult(); // Get result for prepare, but the type not precision.
      }
    } catch (SQLException e) {
      if (addRowid) {
        degradeResultSetType();
        addRowid = false;
        actualSql = originalSql;
        prepare(actualSql);
      } else {
        try {
          this.close();
        } catch (Exception ee) {
          // eat exception.
        }
        logger.error("error preparing query", e);
        throw exceptionFactory.raiseStatementError(connection, this).create(e);
      }
    }
  }

  private void setMetaFromResult() {
    parameterCount = serverPrepareResult.getParamCount();
    resultSetMetaData = new OceanBaseResultSetMetaData(serverPrepareResult.getColumns(), protocol.getUrlParser().getOptions(),
        false, this.protocol.isOracleMode(), addRowid ? 1 : 0);
    parameterMetaData = new OceanBaseParameterMetaData(serverPrepareResult.getParameters());
  }

  public void setParameter(final int parameterIndex, final ParameterHolder holder) throws SQLException {
    currentParameterHolder.put(parameterIndex - 1, holder);
  }

    public void registerReturnParameter(int parameterIndex, int externalType) throws SQLException {
        if (parameterCount <= 0) {
            throw new SQLException("The count of bind parameter must be larger than 0.");
        } else if (returnParamCount <= 0) {
            throw new SQLException("The count of return parameter must be larger than 0.");
        }

        int index = parameterIndex - 1;
        if ((index >= parameterCount - returnParamCount) && parameterIndex <= parameterCount) {
            setParameter(parameterIndex, new NullParameter(ColumnType.convertSqlTypeToColumnType(externalType)));
        } else {
            throw new SQLException("Invalid parameter index.");
        }
    }

    public void registerReturnParameter(int parameterIndex, int externalType, int maximumSize) throws SQLException {
        if (parameterCount <= 0) {
            throw new SQLException("The count of bind parameter must be larger than 0.");
        }

        int index = parameterIndex - 1;
        if (index >= 0 && parameterIndex <= parameterCount) {
            if (externalType != Types.CHAR && externalType != Types.VARCHAR && externalType != Types.LONGVARCHAR && externalType != Types.BINARY && externalType != Types.VARBINARY && externalType != Types.LONGVARBINARY) {
                throw new SQLException("Invalid parameter type for this interface.");
            } else if (maximumSize <= 0) {
                throw new SQLException("Maximum size of return parameter must be larger than 0.");
            } else {
                setParameter(parameterIndex, new NullParameter(ColumnType.convertSqlTypeToColumnType(externalType)));
            }
        } else {
            throw new SQLException("Invalid parameter index.");
        }
    }

    public void registerReturnParameter(int parameterIndex, int externalType, String internalTypeName) throws SQLException {
        if (parameterCount <= 0) {
            throw new SQLException("The count of bind parameter must be larger than 0.");
        }

        int index = parameterIndex - 1;
        if (index >= 0 && parameterIndex <= parameterCount) {
            if (externalType != Types.REF && externalType != Types.STRUCT && externalType != Types.ARRAY && externalType != Types.SQLXML) {
                throw new SQLException("Invalid parameter type for this interface.");
            } else {
                setParameter(parameterIndex, new NullParameter(ColumnType.convertSqlTypeToColumnType(externalType)));
            }
        } else {
            throw new SQLException("Invalid parameter index.");
        }
    }

  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException {
    return parameterMetaData;
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return resultSetMetaData;
  }

  public ParameterHolder[] getParameters() {
    return currentParameterHolder.values().toArray(new ParameterHolder[0]);
  }

  public void setParameters(ParameterHolder[] paramArray) {
    for (int i = 0; i < paramArray.length; i++) {
      currentParameterHolder.put(i, paramArray[i]);
    }
  }

  @Override
  public void clearParameters() throws SQLException {
    checkClose();
    currentParameterHolder.clear();
  }

  protected void validParameters() throws SQLException {
    if (!(this instanceof JDBC4ServerCallableStatement)) {
      for (int i = 0; i < parameterCount; i++) {
        if (currentParameterHolder.get(i) == null) {
          logger.error("Parameter at position {} is not set", (i + 1));
          throw exceptionFactory
              .raiseStatementError(connection, this)
              .create("Parameter at position " + (i + 1) + " is not set", "07004");
        }
      }
    }
  }

  @Override
  public void addBatch() throws SQLException {
    if (returnParamCount > 0) {
        throw new SQLFeatureNotSupportedException("not support batch operation for DML_Returning_Into feature.");
    }

    validParameters();
    parametersList.add(currentParameterHolder.values().toArray(new ParameterHolder[0]));
  }

  @Override
  public void addBatch(final String sql) throws SQLException {
    if (returnParamCount > 0) {
      throw new SQLException("not support batch operation for DML_Returning_Into feature.");
    }

    String querySetToServer = sql;
    Matcher matcher = CALLABLE_STATEMENT_PATTERN.matcher(querySetToServer);
    if (this.protocol.isOracleMode()) {
      if(matcher.matches()) {
        querySetToServer = matcher.group(2);
      }
      if (options.supportNameBinding) {
        querySetToServer = Utils.trimSQLString(querySetToServer, protocol.noBackslashEscapes(),
            protocol.isOracleMode(), true);
      }
    } else {
      if(matcher.matches()) {
        querySetToServer = matcher.group(2);
      }
    }
    super.addBatch(querySetToServer);
  }

  public void clearBatch() {
    parametersList.clear();
    hasLongData = false;
  }

  /**
   * Submits a batch of send to the database for execution and if all send execute successfully,
   * returns an array of update counts. The <code>int</code> elements of the array that is returned
   * are ordered to correspond to the send in the batch, which are ordered according to the order in
   * which they were added to the batch. The elements in the array returned by the method <code>
   * executeBatch</code> may be one of the following:
   *
   * <ol>
   *   <li>A number greater than or equal to zero -- indicates that the command was processed
   *       successfully and is an update count giving the number of rows in the database that were
   *       affected by the command's execution
   *   <li>A value of <code>SUCCESS_NO_INFO</code> -- indicates that the command was processed
   *       successfully but that the number of rows affected is unknown. If one of the send in a
   *       batch update fails to execute properly, this method throws a <code>BatchUpdateException
   *       </code>, and a JDBC driver may or may not continue to process the remaining send in the
   *       batch. However, the driver's behavior must be consistent with a particular DBMS, either
   *       always continuing to process send or never continuing to process send. If the driver
   *       continues processing after a failure, the array returned by the method <code>
   *       BatchUpdateException.getUpdateCounts</code> will contain as many elements as there are
   *       send in the batch, and at least one of the elements will be the following:
   *   <li>A value of <code>EXECUTE_FAILED</code> -- indicates that the command failed to execute
   *       successfully and occurs only if a driver continues to process send after a command fails
   * </ol>
   *
   * <p>The possible implementations and return values have been modified in the Java 2 SDK,
   * Standard Edition, version 1.3 to accommodate the option of continuing to proccess send in a
   * batch update after a <code>BatchUpdateException</code> object has been thrown.
   *
   * @return an array of update counts containing one element for each command in the batch. The
   * elements of the array are ordered according to the order in which send were added to the
   * batch.
   * @throws SQLException if a database access error occurs, this method is called on a closed
   *                      <code>Statement</code> or the driver does not support batch statements. Throws {@link
   *                      BatchUpdateException} (a subclass of <code>SQLException</code>) if one of the send sent to
   *                      the database fails to execute properly or attempts to return a result set.
   * @see #addBatch
   * @see DatabaseMetaData#supportsBatchUpdates
   * @since 1.3
   */
  @Override
  public int[] executeBatch() throws SQLException {
    checkClose();
    int [] retBatchQuery = new int[0];
    int [] retBatch = new int[0];
    int queryParameterSize = parametersList.size();
    if (queryParameterSize == 0) {
      retBatch =  new int[0];
    } else {
      executeBatchInternal(queryParameterSize);
      retBatch = results.getCmdInformation().getUpdateCounts();
    }
    if (batchQueries != null && (batchQueries.size()) >  0) {
      retBatchQuery = super.executeBatch();
    }
    // merge the return values
    int [] ret = new int [retBatch.length + retBatchQuery.length];
    int cur = 0;
    for(int i = 0;i<retBatch.length;i++) {
      ret[cur++] = retBatch[i];
    }
    for(int i = 0;i<retBatchQuery.length;i++) {
      ret[cur++] = retBatchQuery[i];
    }
    return  ret;
  }

  public int[] executeBatchQuerys() throws SQLException {
    checkClose();
    int [] retBatchQuery = new int[0];

    if (batchQueries != null && (batchQueries.size()) >  0) {
      retBatchQuery =  super.executeBatch();
    }
    return  retBatchQuery;
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
    checkClose();
    long [] retBatchQuery = new long[0];
    long [] retBatch = new long[0];
    int queryParameterSize = parametersList.size();
    if (queryParameterSize == 0) {
      retBatch = new long[0];
    } else {
      executeBatchInternal(queryParameterSize);
      retBatch = results.getCmdInformation().getLargeUpdateCounts();
    }
    if (batchQueries != null && (batchQueries.size()) >  0) {
      retBatchQuery =  super.executeLargeBatch();
    }
    long [] ret = new long [retBatch.length + retBatchQuery.length];
    int cur = 0;
    for(int i = 0;i<retBatch.length;i++) {
      ret[cur++] = retBatch[i];
    }
    for(int i = 0;i<retBatchQuery.length;i++) {
      ret[cur++] = retBatchQuery[i];
    }
    return  ret;
  }

  public long[] executeLargeBatchQuerys() throws SQLException {
    checkClose();
    long [] retBatchQuery = new long[0];

    if (batchQueries != null && (batchQueries.size()) >  0) {
      retBatchQuery =  super.executeLargeBatch();
    }
    return  retBatchQuery;
  }

  boolean hasLongData(ParameterHolder[] parameterHolders) {
    if (parameterHolders == null) {
      return false;
    }
    for(ParameterHolder var : parameterHolders) {
      if(var.isLongData()) {
        return true;
      }
    }
    return  false;
  }

  /**
   * Handling arrayBinding helper functions
   * @param startIndex  The start index of parameters list.
   * @param endIndex  The end index of parameters list.
   * @param queryParameterSize The size of list that currently needs to be processed.
   * @return Return sqlException  continueBatchOnError=true.
   * @throws SQLException Throw sqlException if  continueBatchOnError=false.
   */
  SQLException  executeArrayBinding(int startIndex,int endIndex,int queryParameterSize) throws SQLException {
    SQLException exception = null;
    try {
      if (queryTimeout > 0) {
        protocol.stopIfInterrupted();
      }
      if (serverPrepareResult != null) {
        serverPrepareResult.resetParameterTypeHeader();
      }
        if(protocol.supportStmtPrepareExecute()) {
          int paramCount = parametersList.get(0).length;
          serverPrepareResult = protocol.executePreparedQueryArrayBinding(paramCount,
              mustExecuteOnMaster, serverPrepareResult, results, parametersList.subList(startIndex, endIndex), queryParameterSize);

        } else {
          protocol.executePreparedQueryArrayBinding(
              mustExecuteOnMaster, serverPrepareResult, results, parametersList.subList(startIndex, endIndex), queryParameterSize);
        }
    } catch (SQLException queryException) {
      if (options.continueBatchOnError) {
        if (exception == null) {
          exception = queryException;
        }
      } else {
        throw queryException;
      }
    }
    return  exception;
  }

  private void executeBatchInternal(int queryParameterSize) throws SQLException {
    lock.lock();
    if (protocol != null) {
      protocol.startCallInterface();
    }

    executing = true;
    boolean executeBatchByArrayBinding=false; // just use for prepareExecute
    try {
      executeQueryPrologue(serverPrepareResult);
      if (queryTimeout != 0 && options.enableQueryTimeouts) {
        setTimerTask(true);
      }
      int remainParameterSize = queryParameterSize;
      int currentTurnParamSize = queryParameterSize;
      boolean continueRewrite = true;
      int parameterCountReal = parameterCount;
      int preIndex = 0;
      ParameterHolder[] currentQueryParameters = null;
      String curString = originalSql;
      boolean isInsert = false;
      // rewrite batch not work for update sql
      while (continueRewrite) {
        if (options.rewriteBatchedStatements) {
          if ((remainParameterSize * parameterCountReal) > options.maxBatchTotalParamsNum) {
            currentTurnParamSize = (options.maxBatchTotalParamsNum / parameterCountReal);
            remainParameterSize -= currentTurnParamSize;
          } else {
            continueRewrite = false;
            currentTurnParamSize = remainParameterSize;
            remainParameterSize = 0;
          }
          String sqlString = curString;
          if (protocol.isOracleMode() && options.supportNameBinding) {
            sqlString = Utils.trimSQLString(curString, protocol.noBackslashEscapes(), true, true);
          }
          List<String> list = ClientPrepareResult.rewritablePartsInsertSql(sqlString, false, this.protocol.isOracleMode(), this.options.characterEncoding);
          if (list != null) {
            StringBuilder sb = new StringBuilder();
            sb.append(list.get(0));
            sb.append(list.get(1));
            for (int i = 0; i < parameterCountReal; i++) {
              sb.append('?');
              sb.append(list.get(i + 2));
            }

            int remain = currentTurnParamSize - 1;
            while (remain != 0) {
              sb.append(',');
              sb.append(list.get(1));
              for (int i = 0; i < parameterCountReal; i++) {
                sb.append('?');
                sb.append(list.get(i + 2));
              }
              remain--;
            }
            if(parameterCountReal == 0) {
                for(int i = 2 ;i< list.size() ;i++) {
                    sb.append(list.get(i));
                }
            } else {
                sb.append(list.get(list.size() - 1));
            }

            int total = currentTurnParamSize * parameterCountReal;
            ParameterHolder[] parameterHolder;
            ParameterHolder[] allParams = new ParameterHolder[total];
            int cur = 0;
            for (int counter = preIndex; counter < currentTurnParamSize + preIndex; counter++) {
              parameterHolder = parametersList.get(counter);
              for (int i = 0; i < parameterHolder.length; i++) {
                allParams[cur++] = parameterHolder[i];
              }
            }
            queryParameterSize = 1;
            preIndex = preIndex + currentTurnParamSize;
            currentQueryParameters = allParams;
            this.actualSql = sb.toString();
            prepare(actualSql);
            parameterCount = parameterCountReal;
            isInsert = true;
          } else {
            isInsert = false;
          }
        } else {
          continueRewrite = false;
        }

        results = new Results(this, 0, true, currentTurnParamSize, true, resultSetScrollType,
            resultSetConcurrency, autoGeneratedKeys, protocol.getAutoIncrementIncrement(), null, null);

        if (protocol.supportStmtPrepareExecute()) {
          //According to the protocol definition, if using arrayBinding iterationCount > 1.
          if(options.useServerPrepStmts && !this.protocol.getAutocommit() && protocol.isOracleMode() && options.useArrayBinding && !options.rewriteBatchedStatements) {
            executeBatchByArrayBinding = true;
            iterationCount = 2;
          } else {
            iterationCount = 1;
          }
          executeMode |= Packet.OCI_BATCH_MODE;
          if (parameterCount > 0) {
            sendTypesToServer = true;
          }
          calculateCheckSum(actualSql);
          protocol.setComStmtPrepareExecuteField(this.iterationCount, this.executeMode, this.checksum);
        } else {
          protocol.setChecksum(this.checksum);
        }

        // if multi send capacity
        if (options.useBatchMultiSend || options.useBulkStmts) {
          serverPrepareResult = protocol.executeBatchServer(serverPrepareResult, results, actualSql, parametersList, hasLongData);
          if (results.getBatchSucceed()) {
            if (resultSetMetaData == null) {
              setMetaFromResult(); // first prepare
            }
            protocol.resetChecksum();
            results.commandEnd();
            return;
          }
        }

        // send query one by one, reading results for each query before sending another one
        SQLException exception = null;
        SQLException exceptionRet = null;
        if (options.rewriteBatchedStatements && isInsert ) {
          results.setRewritten(true);
          ParameterHolder[] parameterHolder = currentQueryParameters;
          try {
            if (queryTimeout > 0) {
              protocol.stopIfInterrupted();
            }
            if (serverPrepareResult != null) {
              serverPrepareResult.resetParameterTypeHeader();
            }
            if (protocol.supportStmtPrepareExecute()) {
              // under rewriteBatchedStatements
              serverPrepareResult = protocol.executePreparedQuery(parameterHolder.length, parameterHolder, serverPrepareResult, results);
              if(!hasLongData(parameterHolder)) {
                serverPrepareResult = null; // reset serverPrepareResult every time
              }
            } else {
              protocol.executePreparedQuery(mustExecuteOnMaster, serverPrepareResult, results, parameterHolder);
            }
            // under rewriteBatchedStatements  reset parameterCount,originalSql to origin
            parameterCount = parameterCountReal;
            originalSql = curString;
          } catch (SQLException queryException) {
            if (options.continueBatchOnError) {
              if (exception == null) {
                exception = queryException;
              }
            } else {
              throw queryException;
            }
          }
        } else {
          if(options.useServerPrepStmts && !this.protocol.getAutocommit() && protocol.isOracleMode() && options.useArrayBinding) {
            if(protocol.supportStmtPrepareExecute()) {
                executeBatchByArrayBinding = true;
            } else {
                executeBatchByArrayBinding = false;
            }
            ParameterHolder[] curParameterHolder;
            ParameterHolder[] preParameterHolder = null;
            int startIndex = 0;
            int endIndex   = 0;
            for (int counter = 0; counter < queryParameterSize; counter++) {
              curParameterHolder = parametersList.get(counter);
              endIndex = counter;
              if(counter != 0) {
                for(int i= 0; i < curParameterHolder.length;i++) {
                  if ((curParameterHolder[i].getColumnType().getType() !=  preParameterHolder[i].getColumnType().getType())) {
                    exceptionRet = executeArrayBinding(startIndex, endIndex, endIndex - startIndex);
                      if (exceptionRet != null) {
                        exception = exceptionRet;
                      }
                    // any type different send a arrray Binding packet (except the long data).
                    startIndex = counter;
                    break;
                  }
                }
              }
              preParameterHolder = curParameterHolder;
            }
            endIndex ++ ;
            if(startIndex != endIndex) {
              exceptionRet = executeArrayBinding(startIndex,endIndex,endIndex-startIndex);
                if(exceptionRet != null) {
                  exception = exceptionRet;
                }
            }
          } else {
            for (int counter = 0; counter < queryParameterSize; counter++) {
              ParameterHolder[] parameterHolder = parametersList.get(counter);
              try {
                if (queryTimeout > 0) {
                  protocol.stopIfInterrupted();
                }
                if (serverPrepareResult != null) {
                  serverPrepareResult.resetParameterTypeHeader();
                }
                if (protocol.supportStmtPrepareExecute()) {
                  serverPrepareResult = protocol.executePreparedQuery(parameterCountReal, parameterHolder, serverPrepareResult, results);
                } else {
                  protocol.executePreparedQuery(mustExecuteOnMaster, serverPrepareResult, results, parameterHolder);
                }
              } catch (SQLException queryException) {
                if (options.continueBatchOnError) {
                  if (exception == null) {
                    exception = queryException;
                  }
                } else {
                  throw queryException;
                }
              }
            }
          }
        }
        if (exception != null) {
          throw exception;
        }
        protocol.resetChecksum();
        results.commandEnd();
      }
    } catch (SQLException initialSqlEx) {
      throw executeBatchExceptionEpilogue(initialSqlEx, queryParameterSize,executeBatchByArrayBinding);
    } finally {
      executeBatchEpilogue();
      if (protocol != null) {
          protocol.endCallInterface("JDBC4ServerPreparedStatement.executeBatchInternal");
      }
      lock.unlock();
    }
  }

  // must have "lock" locked before invoking
  private void executeQueryPrologue(ServerPrepareResult serverPrepareResult) throws SQLException {
    executing = true;
    if (isClosed()) {
      throw exceptionFactory
          .raiseStatementError(connection, this)
          .create("execute() is called on closed statement");
    }
    protocol.prologProxy(
        serverPrepareResult, maxRows, protocol.getProxy() != null, connection, this);
  }

  @Override
  public ResultSet executeQuery() throws SQLException {
    if (execute()) {
      if (results != null && results.getResultSet() != null) {
        return results.getResultSet();
      }
    }

    // DML returning rs
    if (results.isReturning()) {
      return results.getResultSet();
    }
    return SelectResultSet.createEmptyResultSet();
  }

  @Override
  public long executeLargeUpdate() throws SQLException {
      if (execute()) {
          return 0;
      }

      // DML returning rs
      if (results.isReturning() && results.getResultSet() != null) {
          return results.getResultSet().getProcessedRows();
      }
      return getLargeUpdateCount();
  }
  /**
   * Executes the SQL statement in this <code>PreparedStatement</code> object, which must be an SQL
   * Data Manipulation Language (DML) statement, such as <code>INSERT</code>, <code>UPDATE</code> or
   * <code>DELETE</code>; or an SQL statement that returns nothing, such as a DDL statement.
   * Result-set are permitted for historical reason, even if spec indicate to throw exception.
   *
   * @return either (1) the row count for SQL Data Manipulation Language (DML) statements or (2) 0
   * for SQL statements that return nothing
   * @throws SQLException if a database access error occurs; this method is called on a closed
   *                      <code>PreparedStatement</code>
   */
  @Override
  public int executeUpdate() throws SQLException {
    if (execute()) {
      return 0;
    }

    // DML returning rs
    if (results.isReturning() && results.getResultSet() != null) {
      return (int) results.getResultSet().getProcessedRows();
    }
    return getUpdateCount();
  }

  @Override
  public boolean execute() throws SQLException {
    if (protocol.supportStmtPrepareExecute()) {
      return prepareExecuteInternal(getFetchSize());
    } else {
      return executeInternal(getFetchSize());
    }
  }

  protected boolean executeInternal(int fetchSize) throws SQLException {
    validParameters();
    lock.lock();
    if (protocol != null) {
      protocol.startCallInterface();
    }

    try {
      executeQueryPrologue(serverPrepareResult);
      if (queryTimeout != 0 && options.enableQueryTimeouts) {
        setTimerTask(false);
      }

      ParameterHolder[] parameterHolders = currentParameterHolder.values().toArray(new ParameterHolder[0]);

      results = new Results(this, fetchSize, false, 1, true, resultSetScrollType,
          resultSetConcurrency, autoGeneratedKeys, protocol.getAutoIncrementIncrement(), actualSql, parameterHolders);

      protocol.setChecksum(this.checksum);
      serverPrepareResult.resetParameterTypeHeader();
      protocol.executePreparedQuery(mustExecuteOnMaster, serverPrepareResult, results, parameterHolders);
      protocol.resetChecksum();

      results.commandEnd();
      if (results.getCallableResultSet() != null) {
        return true;
      } else {
        return results.getResultSet() != null;
      }
    } catch (SQLException exception) {
      throw executeExceptionEpilogue(exception);
    } finally {
      executeEpilogue();
      if (protocol != null) {
          protocol.endCallInterface("JDBC4ServerPreparedStatement.executeInternal");
      }
      lock.unlock();
    }
  }

  protected boolean prepareExecuteInternal(int fetchSize) throws SQLException {
    validParameters();
    lock.lock();
    if (protocol != null) {
      protocol.startCallInterface();
    }

    try {
      if (sqlType == OceanBaseStatement.STMT_SELECT) {
        iterationCount = isFetchSizeSet ? fetchSize : 10; // in Oracle-JDBC default fetch size is 10
        // TODO: more execute-mode
        if (resultSetScrollType != ResultSet.TYPE_FORWARD_ONLY) {
          executeMode |= Packet.OCI_STMT_SCROLLABLE_READONLY;
        }
      } else {
        // TODO: iteration-count is larger than 1 for batch
        iterationCount = 1;
      }
      calculateCheckSum(actualSql);
      protocol.setComStmtPrepareExecuteField(this.iterationCount, this.executeMode, this.checksum);

      // set num-params
      if (parameterCount > 0) {
        sendTypesToServer = true;
      }

      executeQueryPrologue(serverPrepareResult);
      if (queryTimeout != 0 && options.enableQueryTimeouts) {
        setTimerTask(false);
      }

      ParameterHolder[] parameterHolders = currentParameterHolder.values().toArray(new ParameterHolder[0]);
      results = new Results(this, fetchSize, false, 1, true, resultSetScrollType,
          resultSetConcurrency, autoGeneratedKeys, protocol.getAutoIncrementIncrement(), actualSql, parameterHolders);
      if (serverPrepareResult != null) {
        serverPrepareResult.resetParameterTypeHeader();
        results.setStatementId(serverPrepareResult.getStatementId());
      }

      serverPrepareResult = protocol.executePreparedQuery(parameterCount, parameterHolders, serverPrepareResult, results);
      if (resultSetMetaData == null) {
        setMetaFromResult(); // first prepare
      }
      protocol.resetChecksum();

      results.commandEnd();
      if (results.getCallableResultSet() != null) {
        return true;
      } else {
        // for DML Returning stmt(insert, delete and update), FALSE would be returned for execute() and executeUpdate()
        return !results.isReturning() && results.getResultSet() != null;
      }

    } catch (SQLException exception) {
        if (addRowid) {
            degradeResultSetType();
            addRowid = false;
            actualSql = originalSql;
            return prepareExecuteInternal(fetchSize);
        } else {
            throw executeExceptionEpilogue(exception);
        }
    } finally {
      executeEpilogue();
      if (protocol != null) {
          protocol.endCallInterface("JDBC4ServerPreparedStatement.prepareExecuteInternal");
      }
      lock.unlock();
    }
  }

  public ColumnDefinition[] cursorFetch(int cursorId, int fetchSize) throws SQLException {
      lock.lock();
      try {
          ColumnDefinition[] ci = this.protocol.fetchRowViaCursor(cursorId, fetchSize, results);
          results.commandEnd();
          return ci;
      } finally {
          lock.unlock();
      }
  }

  public void closeCursor(int cursorId) throws SQLException {
      lock.lock();
      if (protocol != null) {
          protocol.startCallInterface();
      }

      try {
          this.protocol.forceReleasePrepareStatement(cursorId);
          results.commandEnd();
      } finally {
          if (protocol != null) {
              protocol.endCallInterface("JDBC4ServerPreparedStatement.closeCursor");
          }
          lock.unlock();
      }
  }

  public ColumnDefinition[] cursorFetchForOracle(int cursorId, int numRows, byte offsetType, int offset) throws SQLException {
      lock.lock();
      try {
          ColumnDefinition[] ci = this.protocol.fetchRowViaCursorForOracle(cursorId, numRows, offsetType, offset, results);
          results.commandEnd();
          return ci;
      } finally {
          lock.unlock();
      }
  }

  /**
   * Releases this <code>Statement</code> object's database and JDBC resources immediately instead
   * of waiting for this to happen when it is automatically closed. It is generally good practice to
   * release resources as soon as you are finished with them to avoid tying up database resources.
   *
   * <p>Calling the method <code>close</code> on a <code>Statement</code> object that is already
   * closed has no effect.
   *
   * <p><B>Note:</B>When a <code>Statement</code> object is closed, its current <code>ResultSet
   * </code> object, if one exists, is also closed.
   *
   * @throws SQLException if a database access error occurs
   */
  @Override
  public void close() throws SQLException {
      realClose(true, true);
  }

  @Override
  public void realClose(boolean calledExplicitly, boolean closeOpenResults) throws SQLException {
    // No possible future use for the cached results, so these can be cleared
    // This makes the cache eligible for garbage collection earlier if the statement is not
    // immediately garbage collected
    if (!released && protocol != null && serverPrepareResult != null) {
      if (protocol != null) {
          protocol.startCallInterface();
      }
      try {
        serverPrepareResult.getUnProxiedProtocol().releasePrepareStatement(serverPrepareResult);
        released = true;
      } catch (SQLException e) {
        // eat
      } finally {
        if (protocol != null) {
            protocol.endCallInterface("JDBC4ServerPreparedStatement.releasePrepareStatement");
        }
      }
    }
    super.realClose(calledExplicitly, closeOpenResults);
  }

  /**
   * Return sql String value.
   *
   * @return String representation
   */
  public String toString() {
    StringBuilder sb = new StringBuilder("sql : '" + actualSql + "'");
    if (parameterCount > 0) {
      sb.append(", parameters : [");
      for (int i = 0; i < parameterCount; i++) {
        ParameterHolder holder = currentParameterHolder.get(i);
        if (holder == null) {
          sb.append("null");
        } else {
          sb.append(holder.toString());
        }
        if (i != parameterCount - 1) {
          sb.append(",");
        }
      }
      sb.append("]");
    }
    return sb.toString();
  }

  /**
   * Permit to retrieve current connection thread id, or -1 if unknown.
   *
   * @return current connection thread id.
   */
  public long getServerThreadId() {
    return serverPrepareResult.getUnProxiedProtocol().getServerThreadId();
  }

    public ResultSet getReturnResultSet() throws SQLException {
        checkClose();
        if (returnParamCount != 0 && returnParams != null) {
            return (results != null && results.isReturning()) ? results.getResultSet() : null;
        } else {
            throw new SQLException("Statement handle not executed");
        }
    }

}
