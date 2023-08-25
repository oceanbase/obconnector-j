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
package com.oceanbase.jdbc.internal.protocol;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.ReentrantLock;

import com.oceanbase.jdbc.LocalInfileInterceptor;
import com.oceanbase.jdbc.OceanBaseConnection;
import com.oceanbase.jdbc.OceanBaseStatement;
import com.oceanbase.jdbc.UrlParser;
import com.oceanbase.jdbc.credential.Credential;
import com.oceanbase.jdbc.internal.ColumnType;
import com.oceanbase.jdbc.internal.com.Packet;
import com.oceanbase.jdbc.internal.com.read.Buffer;
import com.oceanbase.jdbc.internal.com.read.ErrorPacket;
import com.oceanbase.jdbc.internal.com.read.dao.Results;
import com.oceanbase.jdbc.internal.com.read.resultset.ColumnDefinition;
import com.oceanbase.jdbc.internal.com.read.resultset.CursorResultSet;
import com.oceanbase.jdbc.internal.com.read.resultset.SelectResultSet;
import com.oceanbase.jdbc.internal.com.read.resultset.SensitiveCursorResultSet;
import com.oceanbase.jdbc.internal.com.send.*;
import com.oceanbase.jdbc.internal.com.send.parameters.LongDataParameterHolder;
import com.oceanbase.jdbc.internal.com.send.parameters.ParameterHolder;
import com.oceanbase.jdbc.internal.io.LruTraceCache;
import com.oceanbase.jdbc.internal.io.output.PacketOutputStream;
import com.oceanbase.jdbc.internal.logging.Logger;
import com.oceanbase.jdbc.internal.logging.LoggerFactory;
import com.oceanbase.jdbc.internal.protocol.flt.FullLinkTrace;
import com.oceanbase.jdbc.internal.util.BulkStatus;
import com.oceanbase.jdbc.internal.util.LogQueryTool;
import com.oceanbase.jdbc.internal.util.SqlStates;
import com.oceanbase.jdbc.internal.util.Utils;
import com.oceanbase.jdbc.internal.util.constant.ServerStatus;
import com.oceanbase.jdbc.internal.util.constant.StateChange;
import com.oceanbase.jdbc.internal.util.dao.ClientPrepareResult;
import com.oceanbase.jdbc.internal.util.dao.PrepareResult;
import com.oceanbase.jdbc.internal.util.dao.ServerPrepareResult;
import com.oceanbase.jdbc.internal.util.exceptions.ExceptionFactory;
import com.oceanbase.jdbc.internal.util.exceptions.MaxAllowedPacketException;
import com.oceanbase.jdbc.internal.util.exceptions.OceanBaseSqlException;
import com.oceanbase.jdbc.internal.util.pool.GlobalStateInfo;
import com.oceanbase.jdbc.internal.util.scheduler.SchedulerServiceProviderHolder;

public class AbstractQueryProtocol extends AbstractConnectProtocol implements Protocol {

  private static final Logger logger = LoggerFactory.getLogger(AbstractQueryProtocol.class);
  private static final Set<Integer> LOCK_DEADLOCK_ERROR_CODES = new HashSet<>(Arrays.asList(1205, 1213, 1614));

  private ThreadPoolExecutor readScheduler = null;
  private InputStream localInfileInputStream;
  private long maxRows; //returned by a statement
  private volatile int statementIdToRelease = -1;
  private FutureTask activeFutureTask = null;
  private boolean interrupted;
  private long checksum = 1;
  private int iterationCount;
  private int executeMode;
  private boolean enableNetworkStatistics = false;

  /**
   * Get a protocol instance.
   *
   * @param urlParser  connection URL information's
   * @param lock       the lock for thread synchronisation
   * @param traceCache trace cache
   */
  AbstractQueryProtocol(
      final UrlParser urlParser,
      final GlobalStateInfo globalInfo,
      final ReentrantLock lock,
      LruTraceCache traceCache) {
    super(urlParser, globalInfo, lock, traceCache);
  }

  /**
   * Reset connection state.
   *
   * <ol>
   *   <li>Transaction will be rollback
   *   <li>transaction isolation will be reset
   *   <li>user variables will be removed
   *   <li>sessions variables will be reset to global values
   * </ol>
   *
   * @throws SQLException if command failed
   */
  @Override
  public void reset() throws SQLException {
    cmdPrologue();
    lock.lock();
    UUID spanId = ob20BeginTraceAndSpan("reset connection");

    try {
      writer.startPacket(0);
      writer.write(Packet.COM_RESET_CONNECTION);
      writer.flush();

      getResult(new Results());

      // clear prepare statement cache
      if (options.cachePrepStmts && options.useServerPrepStmts) {
        serverPrepareStatementCache.clear();
      }
    } catch (SQLException sqlException) {
      throw exceptionWithQuery("COM_RESET_CONNECTION failed.", sqlException, explicitClosed);
    } catch (IOException e) {
      throw exceptionWithQuery(
          "COM_RESET_CONNECTION failed.", handleIoException(e), explicitClosed);
    } finally {
      ob20EndSpanAndTrace(spanId);
      lock.unlock();
    }
  }

  private OceanBaseSqlException exceptionWithQuery(
      ParameterHolder[] parameters,
      PrepareResult serverPrepareResult,
      SQLException sqlException,
      boolean explicitClosed) {
    return exceptionWithQuery(
        LogQueryTool.queryWithParams(serverPrepareResult, parameters, options),
        sqlException,
        explicitClosed);
  }

  private OceanBaseSqlException exceptionWithQuery(
      String sql,
      SQLException sqlException,
      boolean explicitClosed) {
    OceanBaseSqlException ex;
    if (explicitClosed) {
      ex =
          new OceanBaseSqlException(
              "Connection has explicitly been closed/aborted.", sql, sqlException);
    } else {
      if (sqlException.getCause() instanceof SocketTimeoutException) {
        ex = new OceanBaseSqlException("Connection timed out", sql, "08000", sqlException);
      } else {
        ex = OceanBaseSqlException.of(sqlException, sql);
      }
    }

    if (options.includeThreadDumpInDeadlockExceptions || sqlException.getErrorCode() == 1064) {
      ex.withThreadName(Thread.currentThread().getName());
    }

    // Add innoDB status if asked
    if (options.includeInnodbStatusInDeadlockExceptions
        && sqlException.getSQLState() != null
        && LOCK_DEADLOCK_ERROR_CODES.contains(sqlException.getErrorCode())) {
      try {
        lock.lock();
        cmdPrologue();
        Results results = new Results();
        executeQuery(isMasterConnection(), results, "SHOW ENGINE INNODB STATUS");
        results.commandEnd();
        ResultSet rs = results.getResultSet();
        if (rs.next()) {
          return ex.withDeadLockInfo(rs.getString(3));
        }
      } catch (SQLException sqle) {
        // eat
      } finally {
        lock.unlock();
      }
    }
    return ex;
  }

  /**
   * Prepare query on server side. Will permit to know the parameter number of the query, and permit
   * to send only the data on next results.
   *
   * <p>For failover, two additional information are in the result-set object : - current connection
   * : Since server maintain a state of this prepare statement, all query will be executed on this
   * particular connection. - executeOnMaster : state of current connection when creating this
   * prepareStatement (if was on master, will only be executed on master. If was on a slave, can be
   * execute temporary on master, but we keep this flag, so when a slave is connected back to
   * relaunch this query on slave)
   *
   * @param sql             the query
   * @param executeOnMaster state of current connection when creating this prepareStatement
   * @return a ServerPrepareResult object that contain prepare result information.
   * @throws SQLException if any error occur on connection.
   */
  @Override
  public ServerPrepareResult prepare(String sql, boolean executeOnMaster) throws SQLException {
    cmdPrologue();
    lock.lock();
    UUID spanId = ob20BeginTraceAndSpan("prepare");

    try {
      // search in cache first
      if (options.cachePrepStmts && options.useServerPrepStmts) {
        ServerPrepareResult pr = serverPrepareStatementCache.get(database + "-" + sql);
        if (pr != null && pr.incrementShareCounter()) {
          return pr;
        }
      }

      writer.startPacket(0);
      writer.write(Packet.COM_STMT_PREPARE);
      writer.write(sql.getBytes(options.characterEncoding));
      writer.flush();

      return ComStmtPrepare.read(reader, eofDeprecated, this, sql);
    } catch (IOException e) {
      throw exceptionWithQuery(sql, handleIoException(e), explicitClosed);
    } finally {
      ob20EndSpanAndTrace(spanId);
      lock.unlock();
    }
  }

  /**
   * Execute internal query.
   *
   * <p>!! will not support multi values queries !!
   *
   * @param sql sql
   * @throws SQLException in any exception occur
   */
  public void executeQuery(final String sql) throws SQLException {
    executeQuery(isMasterConnection(), new Results(), sql);
  }

  /**
   * Execute query directly to outputStream.
   *
   * @param mustExecuteOnMaster was intended to be launched on master connection
   * @param results             result
   * @param sql                 the query to executeInternal
   * @throws SQLException exception
   */
  @Override
  public void executeQuery(boolean mustExecuteOnMaster, Results results, final String sql) throws SQLException {
    cmdPrologue();
    lock.lock();
    UUID spanId = ob20BeginTraceAndSpan("execute query");

    try {
      writer.startPacket(0);
      writer.write(Packet.COM_QUERY);
      writer.write(sql.getBytes(options.characterEncoding));
      writer.flush();

      getResult(results);
    } catch (SQLException sqlException) {
      if ("70100".equals(sqlException.getSQLState()) && 1927 == sqlException.getErrorCode()) {
        throw handleIoException(sqlException);
      }
      throw exceptionWithQuery(sql, sqlException, explicitClosed);
    } catch (IOException e) {
      throw exceptionWithQuery(sql, handleIoException(e), explicitClosed);
    } finally {
      ob20EndSpanAndTrace(spanId);
      lock.unlock();
    }
  }

  @Override
  public void executeQuery(boolean mustExecuteOnMaster, Results results, final String sql, Charset charset) throws SQLException {
    cmdPrologue();
    lock.lock();
    UUID spanId = ob20BeginTraceAndSpan("execute query");

    try {
      writer.startPacket(0);
      writer.write(Packet.COM_QUERY);
      writer.write(sql.getBytes(charset));
      writer.flush();

      getResult(results);
    } catch (SQLException sqlException) {
      throw exceptionWithQuery(sql, sqlException, explicitClosed);
    } catch (IOException e) {
      throw exceptionWithQuery(sql, handleIoException(e), explicitClosed);
    } finally {
      ob20EndSpanAndTrace(spanId);
      lock.unlock();
    }
  }

  /**
   * Execute a unique clientPrepareQuery.
   *
   * @param mustExecuteOnMaster was intended to be launched on master connection
   * @param results             results
   * @param clientPrepareResult clientPrepareResult
   * @param parameters          parameters
   * @throws SQLException exception
   */
  public void executeQuery(
      boolean mustExecuteOnMaster,
      Results results,
      final ClientPrepareResult clientPrepareResult,
      ParameterHolder[] parameters) throws SQLException {
    cmdPrologue();
    lock.lock();
    UUID spanId = ob20BeginTraceAndSpan("execute query");

    try {
      if (clientPrepareResult.getParamCount() == 0
          && !clientPrepareResult.isQueryMultiValuesRewritable()) {
        if (clientPrepareResult.getQueryParts().size() == 1) {
            ComQuery.sendDirect(writer, clientPrepareResult.getQueryParts().get(0));
        } else {
            ComQuery.sendMultiDirect(writer, clientPrepareResult.getQueryParts());
        }
      } else {
          ComQuery.sendSubCmd(writer, clientPrepareResult, parameters, -1);
      }

      getResult(results);
    } catch (SQLException queryException) {
      throw exceptionWithQuery(parameters, clientPrepareResult, queryException, false);
    } catch (IOException e) {
      throw exceptionWithQuery(parameters, clientPrepareResult, handleIoException(e), false);
    } finally {
      ob20EndSpanAndTrace(spanId);
      lock.unlock();
    }
  }

  /**
   * Execute a unique clientPrepareQuery.
   *
   * @param mustExecuteOnMaster was intended to be launched on master connection
   * @param results             results
   * @param clientPrepareResult clientPrepareResult
   * @param parameters          parameters
   * @param queryTimeout        if timeout is set and must use max_statement_time
   * @throws SQLException exception
   */
  public void executeQuery(
      boolean mustExecuteOnMaster,
      Results results,
      final ClientPrepareResult clientPrepareResult,
      ParameterHolder[] parameters,
      int queryTimeout) throws SQLException {
    cmdPrologue();
    lock.lock();
    UUID spanId = ob20BeginTraceAndSpan("execute query");

    try {
      if (clientPrepareResult.getParamCount() == 0
          && !clientPrepareResult.isQueryMultiValuesRewritable()) {
        if (clientPrepareResult.getQueryParts().size() == 1) {
            ComQuery.sendDirect(writer, clientPrepareResult.getQueryParts().get(0), queryTimeout);
        } else {
            ComQuery.sendMultiDirect(writer, clientPrepareResult.getQueryParts(), queryTimeout);
        }
      } else {
          ComQuery.sendSubCmd(writer, clientPrepareResult, parameters, queryTimeout);
      }

      getResult(results);
    } catch (SQLException queryException) {
      throw exceptionWithQuery(parameters, clientPrepareResult, queryException, false);
    } catch (IOException e) {
      throw exceptionWithQuery(parameters, clientPrepareResult, handleIoException(e), false);
    } finally {
      ob20EndSpanAndTrace(spanId);
      lock.unlock();
    }
  }

  /**
   * Execute a query that is already prepared.
   *
   * @param mustExecuteOnMaster must execute on master
   * @param serverPrepareResult prepare result
   * @param results             execution result
   * @param parameters          parameters
   * @throws SQLException exception
   */
  @Override
  public void executePreparedQuery(
      boolean mustExecuteOnMaster,
      ServerPrepareResult serverPrepareResult,
      Results results,
      ParameterHolder[] parameters) throws SQLException {
    cmdPrologue();
    lock.lock();
    UUID spanId = ob20BeginTraceAndSpan("execute prepared query");

    try {
      int parameterCount = serverPrepareResult.getParameters().length;
      if (isOracleMode()) {
        // send binary data in a separate stream
        if (options.useServerPrepStmts && options.usePieceData) {
          for (int i = 0; i < parameterCount; i++) {
            if (parameters[i].isLongData()) {
              boolean continueWrite = true;
              boolean first = true;
              while (continueWrite) {
                writer.startPacket(0);
                writer.write(Packet.COM_STMT_SEND_PIECE_DATA);
                writer.writeInt(serverPrepareResult.getStatementId());
                writer.writeShort((short) (i));
                continueWrite = ((LongDataParameterHolder) parameters[i]).writePieceData(writer, first, options);
                first = false;
                getResult(new Results());
              }
            }
          }
        } else {
          for (int i = 0; i < parameterCount; i++) {
            if (parameters[i].isLongData()) {
              throw new SQLException("Not supported send long data on ob oracle");
            }
          }
        }
      } else { // mysql
        for (int i = 0; i < parameterCount; i++) {
          if (parameters[i].isLongData()) {
            ((LongDataParameterHolder) parameters[i]).writeLongData(writer, options, serverPrepareResult.getStatementId(), (short) i);
          }
        }
      }
      writer.setTimeZone(this.getServerTimeZone());

      // send execute query
      boolean withRefCursor = false;
      ColumnDefinition[] ci = serverPrepareResult.getColumns();
      for (ColumnDefinition columnDefinition : ci) {
        if (columnDefinition.getColumnType() == ColumnType.CURSOR) {
          withRefCursor = true;
          break;
        }
      }

      byte cursorFlag = 0;
      if (!withRefCursor && results.getStatement().getSqlType() == OceanBaseStatement.STMT_SELECT
          && (isOracleMode()
          || !isOracleMode() && options.useCursorFetch && results.getFetchSize() > 0
          && results.getResultSetScrollType() == ResultSet.TYPE_FORWARD_ONLY
          && results.getResultSetConcurrency() == ResultSet.CONCUR_READ_ONLY)) {
        cursorFlag = Packet.CURSOR_TYPE_READ_ONLY;
        results.setToCursorFetch(true);
        results.setStatementId(serverPrepareResult.getStatementId());
      } else {
        cursorFlag = Packet.CURSOR_TYPE_NO_CURSOR;
      }

      ComStmtExecute.send(writer, serverPrepareResult.getStatementId(), parameters, parameterCount,
              serverPrepareResult.getParameterTypeHeader(), cursorFlag, this);
      getResult(results);
      results.setToCursorFetch(false);
    } catch (SQLException qex) {
      throw exceptionWithQuery(parameters, serverPrepareResult, qex, false);
    } catch (IOException e) {
      throw exceptionWithQuery(parameters, serverPrepareResult, handleIoException(e), false);
    } finally {
      ob20EndSpanAndTrace(spanId);
      lock.unlock();
    }
  }

  /**
   * Execute a query that is to be prepared on server in Oracle mode
   *
   * @throws SQLException exception
   */
  @Override
  public ServerPrepareResult executePreparedQuery(
      int parameterCount,
      ParameterHolder[] parameters,
      ServerPrepareResult serverPrepareResult,
      Results results) throws SQLException{
    cmdPrologue();
    lock.lock();
    UUID spanId = ob20BeginTraceAndSpan("prepare and execute query");

    try {
      if (isOracleMode()) {
        // use send piece data
        if (options.useServerPrepStmts && options.usePieceData) {
          for (int i = 0; i < parameterCount; i++) {
            if (parameters[i].isLongData()) {
              if (serverPrepareResult == null) {
                  // prepare here
                  serverPrepareResult = prepare(results.getStatement().getActualSql(), true);
              }

              boolean continueWrite = true;
              boolean first = true;
              while (continueWrite) {
                writer.startPacket(0);
                writer.write(Packet.COM_STMT_SEND_PIECE_DATA);
                writer.writeInt(serverPrepareResult.getStatementId());
                writer.writeShort((short) (i));
                continueWrite = ((LongDataParameterHolder) parameters[i]).writePieceData(writer, first, options);
                first = false;
                getResult(new Results());
              }
            }
          }
        } else {
          for (int i = 0; i < parameterCount; i++) {
            if (parameters[i].isLongData()) {
              throw new SQLException("Not supported send long data on ob oracle");
            }
          }
        }
      } else {
        // mysql mode
        for (int i = 0; i < parameters.length; i++) {
          if (parameters[i].isLongData()) {
            ((LongDataParameterHolder) parameters[i]).writeLongData(writer, options, serverPrepareResult.getStatementId(), (short) i);
          }
        }
      }
      writer.setTimeZone(this.getServerTimeZone());

      // send execute query
      boolean withRefCursor = false;
      if (serverPrepareResult != null) {
        ColumnDefinition[] ci = serverPrepareResult.getColumns();
        for (ColumnDefinition columnDefinition : ci) {
          if (columnDefinition.getColumnType() == ColumnType.CURSOR) {
            withRefCursor = true;
            break;
          }
        }
      }

      byte cursorFlag = 0;
      if (!withRefCursor && results.getStatement().getSqlType() == OceanBaseStatement.STMT_SELECT && isOracleMode()) {
        cursorFlag = Packet.CURSOR_TYPE_READ_ONLY;
        results.setToCursorFetch(true);
      } else {
        cursorFlag = Packet.CURSOR_TYPE_NO_CURSOR;
      }

      ComStmtPrepareExecute.send(writer, results, parameterCount, parameters,
          (serverPrepareResult != null && serverPrepareResult.isReturnByPrepareExecute()) ? serverPrepareResult.getParameterTypeHeader() : null, cursorFlag, this, serverPrepareResult);

      serverPrepareResult = ComStmtPrepareExecute.read(this, reader, (serverPrepareResult != null && serverPrepareResult.isReturnByPrepareExecute()) ? serverPrepareResult : null, results);
      results.setToCursorFetch(false);
      return serverPrepareResult;
    } catch (SQLException qex) {
      throw exceptionWithQuery(results.getParameters(), serverPrepareResult, qex, false);
    } catch (IOException e) {
      throw exceptionWithQuery(results.getParameters(), serverPrepareResult, handleIoException(e), false);
    } finally {
      ob20EndSpanAndTrace(spanId);
      lock.unlock();
    }
  }

  /**
   * Execute clientPrepareQuery batch.
   *
   * @param mustExecuteOnMaster was intended to be launched on master connection
   * @param results             results
   * @param prepareResult       ClientPrepareResult
   * @param parametersList      List of parameters
   * @param hasLongData         has parameter with long data (stream)
   * @throws SQLException exception
   */
  public boolean executeBatchClient(
      boolean mustExecuteOnMaster,
      Results results,
      final ClientPrepareResult prepareResult,
      final List<ParameterHolder[]> parametersList,
      boolean hasLongData) throws SQLException {

    // ***********************************************************************************************************
    // Multiple solution for batching :
    // - rewrite as multi-values (only if generated keys are not needed and query can be rewritten)
    // - multiple INSERT separate by semi-columns
    // - use pipeline
    // - use bulk
    // - one after the other
    // ***********************************************************************************************************

    if (options.rewriteBatchedStatements) {
      if (prepareResult.isQueryMultiValuesRewritable()
          && results.getAutoGeneratedKeys() == Statement.NO_GENERATED_KEYS) {

        // values rewritten in one query :
        // INSERT INTO X(a,b) VALUES (1,2), (3,4), ...
          if(options.rewriteInsertByMultiQueries) {
              executeBatchRewrite(results, prepareResult, parametersList, false);
          } else {
              executeBatchRewrite(results, prepareResult, parametersList, true);

          }
        return true;

      } else if (prepareResult.isQueryMultipleRewritable()) {

        if (options.useBulkStmts
            && !hasLongData
            && results.getAutoGeneratedKeys() == Statement.NO_GENERATED_KEYS
            && versionGreaterOrEqual(10, 2, 7)
            && executeBulkBatch(results, prepareResult.getSql(), null, parametersList)) {
          return true;
        }

        // multi rewritten in one query :
        // INSERT INTO X(a,b) VALUES (1,2);INSERT INTO X(a,b) VALUES (3,4); ...
        executeBatchRewrite(results, prepareResult, parametersList, false);
        return true;
      }
    }

    if (options.useBulkStmts
        && !hasLongData
        && results.getAutoGeneratedKeys() == Statement.NO_GENERATED_KEYS
        && versionGreaterOrEqual(10, 2, 7)
        && executeBulkBatch(results, prepareResult.getSql(), null, parametersList)) {
      return true;
    }

    if (options.useBatchMultiSend) {
      // send by bulk : send data by bulk before reading corresponding results
      executeBatchMulti(results, prepareResult, parametersList);
      return true;
    }

    return false;
  }

  /**
   * Execute clientPrepareQuery batch.
   *
   * @param results             results
   * @param sql                 sql command
   * @param serverPrepareResult prepare result if exist
   * @param parametersList      List of parameters
   * @return if executed
   * @throws SQLException exception
   */
  private boolean executeBulkBatch(
      Results results,
      String sql,
      ServerPrepareResult serverPrepareResult,
      final List<ParameterHolder[]> parametersList)
      throws SQLException {
    // **************************************************************************************
    // Ensure BULK can be use :
    // - server version >= 10.2.7
    // - no stream
    // - parameter type doesn't change
    // - avoid INSERT FROM SELECT
    // **************************************************************************************

    // ensure that there is no long data and type doesn't change
    ParameterHolder[] initParameters = parametersList.get(0);
    int parameterCount = initParameters.length;
    short[] types = new short[parameterCount];
    for (int i = 0; i < parameterCount; i++) {
      types[i] = initParameters[i].getColumnType().getType();
    }

    // must ensure that data type doesn't change
    for (ParameterHolder[] parameters : parametersList) {
      for (int i = 0; i < parameterCount; i++) {
        if (parameters[i].getColumnType().getType() != types[i]) {
          return false;
        }
      }
    }

    // any select query is not applicable to bulk
    if (sql.toLowerCase(Locale.ROOT).contains("select")) {
      return false;
    }

    cmdPrologue();
    lock.lock();
    UUID spanId = null;

    ParameterHolder[] parameters = null;
    ServerPrepareResult tmpServerPrepareResult = serverPrepareResult;
    try {
      SQLException exception = null;

      // send PREPARE if needed
      if (serverPrepareResult == null) {
        tmpServerPrepareResult = prepare(sql, true);
      }
      spanId = ob20BeginTraceAndSpan("execute bulk batch");

      // send BULK
      // **************************************************************************************
      int statementId =
          tmpServerPrepareResult != null ? tmpServerPrepareResult.getStatementId() : -1;

      byte[] lastCmdData = null;
      int index = 0;
      ParameterHolder[] firstParameters = parametersList.get(0);

      do {
        writer.startPacket(0);
        writer.write(Packet.COM_STMT_BULK_EXECUTE);
        writer.writeInt(statementId);
        writer.writeShort((short) 128); // always SEND_TYPES_TO_SERVER

        for (ParameterHolder param : firstParameters) {
          writer.writeShort(param.getColumnType().getType());
        }

        if (lastCmdData != null) {
          writer.checkMaxAllowedLength(lastCmdData.length);
          writer.write(lastCmdData);
          writer.mark();
          index++;
          lastCmdData = null;
        }

        for (; index < parametersList.size(); index++) {
          parameters = parametersList.get(index);
          for (int i = 0; i < parameterCount; i++) {
            ParameterHolder holder = parameters[i];
            if (holder.isNullData()) {
              writer.write(1); // NULL
            } else {
              writer.write(0); // NONE
              holder.writeBinary(writer);
            }
          }

          // if buffer > MAX_ALLOWED_PACKET, flush until last mark.
          if (writer.exceedMaxLength() && writer.isMarked()) {
            writer.flushBufferStopAtMark();
          }

          // if flushed, quit loop
          if (writer.bufferIsDataAfterMark()) {
            break;
          }

          writer.checkMaxAllowedLength(0);
          writer.mark();
        }

        if (writer.bufferIsDataAfterMark()) {
          // flush has been done
          lastCmdData = writer.resetMark();
        } else {
          writer.flush();
          writer.resetMark();
        }

        try {
          getResult(results);
        } catch (SQLException sqle) {
          if ("HY000".equals(sqle.getSQLState()) && sqle.getErrorCode() == 1295) {
            // query contain commands that cannot be handled by BULK protocol
            // clear error and special error code, so it won't leak anywhere
            // and wouldn't be misinterpreted as an additional update count
            results.getCmdInformation().reset();
            return false;
          }
          if (exception == null) {
            exception = exceptionWithQuery(sql, sqle, explicitClosed);
            if (!options.continueBatchOnError) {
              throw exception;
            }
          }
        }

      } while (index < parametersList.size() - 1);

      if (lastCmdData != null) {
        writer.startPacket(0);
        writer.write(Packet.COM_STMT_BULK_EXECUTE);
        writer.writeInt(statementId);
        writer.writeShort((byte) 0x80); // always SEND_TYPES_TO_SERVER

        for (ParameterHolder param : firstParameters) {
          writer.writeShort(param.getColumnType().getType());
        }
        writer.write(lastCmdData);
        writer.flush();
        try {
          getResult(results);
        } catch (SQLException sqle) {
          if ("HY000".equals(sqle.getSQLState()) && sqle.getErrorCode() == 1295) {
            // query contain SELECT. cannot be handle by BULK protocol
            return false;
          }
          if (exception == null) {
            exception = exceptionWithQuery(sql, sqle, explicitClosed);
            if (!options.continueBatchOnError) {
              throw exception;
            }
          }
        }
      }

      if (exception != null) {
        throw exception;
      }
      results.setRewritten(true);

      return true;
    } catch (IOException e) {
      throw exceptionWithQuery(
          parameters, tmpServerPrepareResult, handleIoException(e), explicitClosed);
    } finally {
      if (serverPrepareResult == null && tmpServerPrepareResult != null) {
        releasePrepareStatement(tmpServerPrepareResult);
      }
      writer.resetMark();

      ob20EndSpanAndTrace(spanId);
      lock.unlock();
    }
  }

  private void initializeBatchReader() {
    if (options.useBatchMultiSend) {
      readScheduler = SchedulerServiceProviderHolder.getBulkScheduler();
    }
  }

  /**
   * Execute clientPrepareQuery batch.
   *
   * @param results             results
   * @param clientPrepareResult ClientPrepareResult
   * @param parametersList      List of parameters
   * @throws SQLException exception
   */
  private void executeBatchMulti(
      Results results,
      final ClientPrepareResult clientPrepareResult,
      final List<ParameterHolder[]> parametersList) throws SQLException {
    cmdPrologue();
    lock.lock();
    UUID spanId = ob20BeginTraceAndSpan("execute batch multi");

    try {
        initializeBatchReader();
        new AbstractMultiSend(
                this, writer, results, clientPrepareResult, parametersList, readScheduler) {

            @Override
            public void sendCmd(
                    PacketOutputStream writer,
                    Results results,
                    List<ParameterHolder[]> parametersList,
                    List<String> queries,
                    int paramCount,
                    BulkStatus status,
                    PrepareResult prepareResult)
                    throws IOException {

                ParameterHolder[] parameters = parametersList.get(status.sendCmdCounter);
                ComQuery.sendSubCmd(writer, clientPrepareResult, parameters, -1);
            }

            @Override
            public SQLException handleResultException(
                    SQLException qex,
                    Results results,
                    List<ParameterHolder[]> parametersList,
                    List<String> queries,
                    int currentCounter,
                    int sendCmdCounter,
                    int paramCount,
                    PrepareResult prepareResult) {

                int counter = results.getCurrentStatNumber() - 1;
                ParameterHolder[] parameters = parametersList.get(counter);
                List<byte[]> queryParts = clientPrepareResult.getQueryParts();
                StringBuilder sql = new StringBuilder(new String(queryParts.get(0)));

                for (int i = 0; i < paramCount; i++) {
                    sql.append(parameters[i].toString()).append(new String(queryParts.get(i + 1)));
                }

                return exceptionWithQuery(sql.toString(), qex, explicitClosed);
            }

            @Override
            public int getParamCount() {
                return clientPrepareResult.getQueryParts().size() - 1;
            }

            @Override
            public int getTotalExecutionNumber() {
                return parametersList.size();
            }
        }.executeBatch();
    } finally {
        ob20EndSpanAndTrace(spanId);
        lock.unlock();
    }
  }

  /**
   * Execute batch from Statement.executeBatch().
   *
   * @param mustExecuteOnMaster was intended to be launched on master connection
   * @param results             results
   * @param queries             queries
   * @throws SQLException if any exception occur
   */
  public void executeBatchStmt(
      boolean mustExecuteOnMaster,
      Results results,
      final List<String> queries) throws SQLException {
    cmdPrologue();
    results.setExecuteBatchStmt(true);
    if (this.options.rewriteBatchedStatements) {

      // check that queries are rewritable
      boolean canAggregateSemiColumn = true;
      for (String query : queries) {
        if (!ClientPrepareResult.canAggregateSemiColon(query, noBackslashEscapes(), isOracleMode())) {
          canAggregateSemiColumn = false;
          break;
        }
      }

      if (isInterrupted()) {
        // interrupted by timeout, must throw an exception manually
        throw new SQLTimeoutException("Timeout during batch execution");
      }

      if (canAggregateSemiColumn) {
        executeBatchAggregateSemiColon(results, queries);
      } else {
        executeBatch(results, queries);
      }

    } else {
      executeBatch(results, queries);
    }
  }

  /**
   * Execute list of queries not rewritable.
   *
   * @param results result object
   * @param queries list of queries
   * @throws SQLException exception
   */
  private void executeBatch(Results results, final List<String> queries) throws SQLException {
    lock.lock();
    UUID spanId = ob20BeginTraceAndSpan("execute batch");

    try {
        if (!options.useBatchMultiSend) {
            String sql = null;
            SQLException exception = null;

            for (int i = 0; i < queries.size() && !isInterrupted(); i++) {
                try {
                    sql = queries.get(i);
                    writer.startPacket(0);
                    writer.write(Packet.COM_QUERY);
                    writer.write(sql);
                    writer.flush();
                    getResult(results);
                } catch (SQLException sqlException) {
                    if (exception == null) {
                        exception = exceptionWithQuery(sql, sqlException, explicitClosed);
                        if (!options.continueBatchOnError) {
                            throw exception;
                        }
                    }
                } catch (IOException e) {
                    if (exception == null) {
                        exception = exceptionWithQuery(sql, handleIoException(e), explicitClosed);
                        if (!options.continueBatchOnError) {
                            throw exception;
                        }
                    }
                }
            }
            stopIfInterrupted();

            if (exception != null) {
                throw exception;
            }
            return;
        }

        initializeBatchReader();
        new AbstractMultiSend(this, writer, results, queries, readScheduler) {

            @Override
            public void sendCmd(
                    PacketOutputStream pos,
                    Results results,
                    List<ParameterHolder[]> parametersList,
                    List<String> queries,
                    int paramCount,
                    BulkStatus status,
                    PrepareResult prepareResult)
                    throws IOException {

                String sql = queries.get(status.sendCmdCounter);
                pos.startPacket(0);
                pos.write(Packet.COM_QUERY);
                pos.write(sql);
                pos.flush();
            }

            @Override
            public SQLException handleResultException(
                    SQLException qex,
                    Results results,
                    List<ParameterHolder[]> parametersList,
                    List<String> queries,
                    int currentCounter,
                    int sendCmdCounter,
                    int paramCount,
                    PrepareResult prepareResult) {

                String sql = queries.get(currentCounter + sendCmdCounter);
                return exceptionWithQuery(sql, qex, explicitClosed);
            }

            @Override
            public int getParamCount() {
                return -1;
            }

            @Override
            public int getTotalExecutionNumber() {
                return queries.size();
            }
        }.executeBatch();
    } finally {
        ob20EndSpanAndTrace(spanId);
        lock.unlock();
    }
  }

  /**
   * Execute list of queries. This method is used when using text batch statement and using
   * rewriting (allowMultiQueries || rewriteBatchedStatements). queries will be send to server
   * according to max_allowed_packet size.
   *
   * @param results result object
   * @param queries list of queries
   * @throws SQLException exception
   */
  private void executeBatchAggregateSemiColon(Results results, final List<String> queries) throws SQLException {
    lock.lock();
    UUID spanId = ob20BeginTraceAndSpan("execute batch aggregate semicolon");

    try {
        String firstSql = null;
        int currentIndex = 0;
        int totalQueries = queries.size();
        SQLException exception = null;

        do {
            try {
                firstSql = queries.get(currentIndex++);

                if (totalQueries == 1) {
                    writer.startPacket(0);
                    writer.write(Packet.COM_QUERY);
                    writer.write(firstSql);
                    writer.flush();
                } else {
                    currentIndex =
                            ComQuery.sendBatchAggregateSemiColon(writer, firstSql, queries, currentIndex);
                }
                getResult(results);
            } catch (SQLException sqlException) {
                if (exception == null) {
                    exception = exceptionWithQuery(firstSql, sqlException, explicitClosed);
                    if (!options.continueBatchOnError) {
                        throw exception;
                    }
                }
            } catch (IOException e) {
                throw exceptionWithQuery(firstSql, handleIoException(e), explicitClosed);
            }
            stopIfInterrupted();

        } while (currentIndex < totalQueries);

        if (exception != null) {
            throw exception;
        }
    } finally {
        ob20EndSpanAndTrace(spanId);
        lock.unlock();
    }
  }

  /**
   * Specific execution for batch rewrite that has specific query for memory.
   *
   * @param results       result
   * @param prepareResult prepareResult
   * @param parameterList parameters
   * @param rewriteValues is rewritable flag
   * @throws SQLException exception
   */
  private void executeBatchRewrite(
      Results results,
      final ClientPrepareResult prepareResult,
      List<ParameterHolder[]> parameterList,
      boolean rewriteValues) throws SQLException {
    cmdPrologue();
    lock.lock();
    UUID spanId = ob20BeginTraceAndSpan("execute batch rewrite");

    int currentIndex = 0;
    int totalParameterList = parameterList.size();

    try {
      do {
        currentIndex =
            ComQuery.sendRewriteCmd(
                writer,
                prepareResult.getQueryParts(),
                currentIndex,
                prepareResult.getParamCount(),
                parameterList,
                rewriteValues);
        getResult(results);

        if (Thread.currentThread().isInterrupted()) {
          throw new SQLException(
              "Interrupted during batch", SqlStates.INTERRUPTED_EXCEPTION.getSqlState(), -1);
        }
      } while (currentIndex < totalParameterList);
    } catch (SQLException sqlEx) {
      throw OceanBaseSqlException.of(sqlEx, prepareResult.getSql());
    } catch (IOException e) {
      throw exceptionWithQuery(
          parameterList.get(currentIndex), prepareResult, handleIoException(e), explicitClosed);
    } finally {
      results.setRewritten(rewriteValues);
      ob20EndSpanAndTrace(spanId);
      lock.unlock();
    }
  }

    /**
     * Execute Prepare if needed, and execute COM_STMT_EXECUTE queries in batch.
     *
     * @param serverPrepareResult prepare result. can be null if not prepared.
     * @param results             execution results
     * @param sql                 sql query if needed to be prepared
     * @param parametersList      parameter list
     * @param hasLongData         has long data (stream)
     * @return executed
     * @throws SQLException if parameter error or connection error occur.
     */
    public ServerPrepareResult executeBatchServer(ServerPrepareResult serverPrepareResult, Results results,
                                                  String sql, final List<ParameterHolder[]> parametersList, boolean hasLongData) throws SQLException {
        cmdPrologue();

    // TODO: server does not support "COM_STMT_BULK_EXECUTE" feature yet
    if (options.useBulkStmts && !hasLongData
        && results.getAutoGeneratedKeys() == Statement.NO_GENERATED_KEYS
        && versionGreaterOrEqual(10, 2, 7)
        && executeBulkBatch(results, sql, serverPrepareResult, parametersList)) {
      results.setBatchSucceed(true);
      return serverPrepareResult;
    }

    if (!options.useBatchMultiSend) {
      results.setBatchSucceed(false);
      return serverPrepareResult;
    }

    lock.lock();
    UUID spanId = ob20BeginTraceAndSpan("execute batch on server");

    try {
        initializeBatchReader();
        ServerPrepareResult tempServerPrepareResult = null;
        serverPrepareResult = (ServerPrepareResult) new AbstractMultiSend(this, writer, results, tempServerPrepareResult, parametersList, true, sql, readScheduler) {
            @Override
            public void sendCmd(PacketOutputStream writer, Results results, List<ParameterHolder[]> parametersList,
                                List<String> queries, int paramCount, BulkStatus status, PrepareResult prepareResult) throws SQLException, IOException {

        ParameterHolder[] parameters = parametersList.get(status.sendCmdCounter);

        // validate parameter set
        if (parameters.length < paramCount) {
          throw new SQLException(
              "Parameter at position " + (paramCount - 1) + " is not set", "07004");
        }

        if (isOracleMode()) {
          // send binary data in a separate stream
          if (options.useServerPrepStmts && options.usePieceData) {
            for (int i = 0; i < paramCount; i++) {
              if (parameters[i].isLongData()) {
                writer.startPacket(0);
                writer.write(Packet.COM_STMT_SEND_PIECE_DATA);
                writer.writeInt(statementId);
                writer.writeShort((short) i);
                writer.writeShort(Packet.OCI_LAST_PIECE); // piece type
                parameters[i].writeBinary(writer);
                writer.flush();
              }
            }
          } else {
            for (int i = 0; i < paramCount; i++) {
              if (parameters[i].isLongData()) {
                throw new SQLException("Not supported send long data on ob oracle");
              }
            }
          }
        } else {  // mysql mode
          for (int i = 0; i < paramCount; i++) {
            if (parameters[i].isLongData()) {
              ((LongDataParameterHolder) parameters[i]).writeLongData(writer, options, statementId, (short) i);
            }
          }
        }

        if (supportStmtPrepareExecute()) {
          results.setParameters(parameters);
          ComStmtPrepareExecute.send(writer, results, paramCount, parameters, tempServerPrepareResult != null ? tempServerPrepareResult.getParameterTypeHeader() : null, Packet.CURSOR_TYPE_NO_CURSOR, getProtocol(), tempServerPrepareResult);
        } else {
          ComStmtExecute.send(writer, statementId, parameters, paramCount, parameterTypeHeader,
              Packet.CURSOR_TYPE_NO_CURSOR, getProtocol());
        }
      }

      @Override
      public SQLException handleResultException(SQLException qex, Results results, List<ParameterHolder[]> parametersList,
                                                List<String> queries, int currentCounter, int sendCmdCounter, int paramCount, PrepareResult prepareResult) {

        return OceanBaseSqlException.of(qex, prepareResult.getSql());
      }

      @Override
      public int getParamCount() {

        return getPrepareResult() == null ? parametersList.get(0).length
            : ((ServerPrepareResult) getPrepareResult()).getParameters().length;
      }

            @Override
            public int getTotalExecutionNumber() {
                return parametersList.size();
            }
        }.executeBatch();
        results.setBatchSucceed(true);

        return serverPrepareResult;
    } finally {
        ob20EndSpanAndTrace(spanId);
        lock.unlock();
    }
  }

  public ColumnDefinition[] fetchRowViaCursor(int cursorId, int fetchSize, Results results) throws SQLException {
    cmdPrologue();
    lock.lock();
    UUID spanId = ob20BeginTraceAndSpan("fetch row");

    try {
      writer.startPacket(0);
      writer.write(Packet.COM_STMT_FETCH);
      writer.writeInt(cursorId);
      writer.writeInt(fetchSize);
      writer.flush();

      if (this.isOracleMode()) {
        return getResultWithoutValue(results);
      } else {
        return null;
      }
    } catch (IOException e) {
      throw exceptionWithQuery(
          " fetchRowViaCursor failed.", handleIoException(e), explicitClosed);
    } finally {
      ob20EndSpanAndTrace(spanId);
      lock.unlock();
    }
  }

  public ColumnDefinition[] fetchRowViaCursorForOracle(int cursorId, int numRows, byte offsetType, int offset, Results results) throws SQLException {
    cmdPrologue();
    lock.lock();
    UUID spanId = ob20BeginTraceAndSpan("fetch row for oracle");

    try {
      writer.startPacket(0);
      writer.write(Packet.COM_STMT_FETCH);
      writer.writeInt(cursorId);
      writer.writeInt(numRows);
      writer.writeShort(offsetType);
      writer.writeInt(offset);
      writer.writeInt(1);
      writer.flush();

      if (this.isOracleMode()) {
        return getResultWithoutValue(results);
      } else {
        return null;
      }
    } catch (IOException e) {
      throw exceptionWithQuery(
          " fetchRowViaCursorForOracle failed.", handleIoException(e), explicitClosed);
    } finally {
      ob20EndSpanAndTrace(spanId);
      lock.unlock();
    }
  }

  /**
   * Protocol processing mechanism when prepareExecute is not used
   *
   * @param mustExecuteOnMaster
   * @param serverPrepareResult
   * @param results
   * @param queryParameters
   * @param queryParameterSize
   * @throws SQLException
   */
  @Override
  public void executePreparedQueryArrayBinding(
      boolean mustExecuteOnMaster,
      ServerPrepareResult serverPrepareResult,
      Results results,
      List<ParameterHolder[]> queryParameters,
      int queryParameterSize)
      throws SQLException {
    cmdPrologue();
    lock.lock();
    UUID spanId = ob20BeginTraceAndSpan("execute prepared query by array binding");

    try {
      int paramCount = serverPrepareResult.getParameters().length;// send binary data in a separate stream without send piece
      for (int j = 0; j < queryParameterSize; j++) {
        ParameterHolder[] parameters = queryParameters.get(j);
        for (int i = 0; i < paramCount; i++) {
          if (parameters[i].isLongData()) {
            throw new SQLException("Not supported send long data on ob oracle");
          }
        }
      }
      writer.setTimeZone(this.getServerTimeZone());
      // send execute query
      boolean withRefCursor = false;
      ColumnDefinition[] ci = serverPrepareResult.getColumns();
      for (ColumnDefinition columnDefinition : ci) {
        if (columnDefinition.getColumnType() == ColumnType.CURSOR) {
          withRefCursor = true;
          break;
        }
      }

      byte cursorFlag = 0;
      if (!withRefCursor && results.getStatement().getSqlType() == OceanBaseStatement.STMT_SELECT
          && (isOracleMode()
          || !isOracleMode() && options.useCursorFetch && results.getFetchSize() > 0
          && results.getResultSetScrollType() == ResultSet.TYPE_FORWARD_ONLY
          && results.getResultSetConcurrency() == ResultSet.CONCUR_READ_ONLY)) {
        cursorFlag = Packet.CURSOR_TYPE_READ_ONLY;
        results.setToCursorFetch(true);
        results.setStatementId(serverPrepareResult.getStatementId());
      } else {
        cursorFlag = Packet.CURSOR_TYPE_NO_CURSOR;
      }

      ComStmtExecute.sendArrayBinding(
          writer,
          serverPrepareResult.getStatementId(),
          queryParameters,
          queryParameterSize,
          paramCount,
          serverPrepareResult.getParameterTypeHeader(),
          cursorFlag,
          this);

      getResult(results);
      results.setToCursorFetch(false);

    } catch (SQLException qex) {
      throw exceptionWithQuery(null, serverPrepareResult, qex, false);
    } catch (IOException e) {
      throw exceptionWithQuery(null, serverPrepareResult, handleIoException(e), false);
    } finally {
      ob20EndSpanAndTrace(spanId);
      lock.unlock();
    }
  }

  /**
   * Protocol processing mechanism when prepareExecute is used.
   *
   * @param parameterCount      is count of  parameters.
   * @param mustExecuteOnMaster
   * @param serverPrepareResult
   * @param results
   * @param queryParameters     is sublist of parameters list.
   * @param queryParamtersSize
   * @return
   * @throws SQLException
   */
  @Override
  public ServerPrepareResult executePreparedQueryArrayBinding(int parameterCount, boolean mustExecuteOnMaster, ServerPrepareResult serverPrepareResult, Results results, List<ParameterHolder[]> queryParameters, int queryParamtersSize) throws SQLException {
    cmdPrologue();
    lock.lock();
    UUID spanId = ob20BeginTraceAndSpan("execute query to be prepared by array binding");

    try {
      if (options.usePieceData) {
        boolean firstPrepared = true;
        for (int i = 0; i < parameterCount; i++) {
          ParameterHolder holder = queryParameters.get(0)[i];
          if (holder.isLongData()) {
            for (int j = 0; j < queryParamtersSize; ++j) {
              holder = queryParameters.get(j)[i];
              boolean continueWrite = true;
              boolean first = true;
              while (continueWrite) {
                if (firstPrepared) {
                  // prepre here
                  serverPrepareResult = prepare(results.getStatement().getActualSql(), true);
                  serverPrepareResult.resetParameterTypeHeader();
                  firstPrepared = false;
                }
                writer.startPacket(0);
                writer.write(Packet.COM_STMT_SEND_PIECE_DATA);
                writer.writeInt(serverPrepareResult.getStatementId());
                writer.writeShort((short) (i));
                continueWrite = ((LongDataParameterHolder) holder).writePieceData(writer, first, options);
                first = false;
                getResult(new Results());
              }
            }
          }
        }
      } else {
        for (int j = 0; j < queryParamtersSize; j++) {
          ParameterHolder[] parameters = queryParameters.get(j);
          for (int i = 0; i < parameterCount; i++) {
            if (parameters[i].isLongData()) {
              throw new SQLException("Not supported send long data on ob oracle");
            }
          }
        }
      }
      writer.setTimeZone(this.getServerTimeZone());
      // send execute query
      boolean withRefCursor = false;
      if (serverPrepareResult != null) {
        ColumnDefinition[] ci = serverPrepareResult.getColumns();
        for (ColumnDefinition columnDefinition : ci) {
          if (columnDefinition.getColumnType() == ColumnType.CURSOR) {
            withRefCursor = true;
            break;
          }
        }
      }

      byte cursorFlag = 0;
      if (!withRefCursor && results.getStatement().getSqlType() == OceanBaseStatement.STMT_SELECT && isOracleMode()) {
        cursorFlag = Packet.CURSOR_TYPE_READ_ONLY;
        results.setToCursorFetch(true);
      } else {
        cursorFlag = Packet.CURSOR_TYPE_NO_CURSOR;
      }
      int stmtId = serverPrepareResult != null ? serverPrepareResult.getStatementId() : results.getStatementId();
      ComStmtPrepareExecute.sendArrayBinding(writer, stmtId, queryParameters, queryParamtersSize,
          parameterCount, serverPrepareResult != null ? serverPrepareResult.getParameterTypeHeader() : null, cursorFlag, this, results, serverPrepareResult);

      serverPrepareResult = ComStmtPrepareExecute.read(this, reader, serverPrepareResult, results);
      results.setToCursorFetch(false);
      return serverPrepareResult;
    } catch (SQLException qex) {
      throw exceptionWithQuery(results.getParameters(), serverPrepareResult, qex, false);
    } catch (IOException e) {
      throw exceptionWithQuery(results.getParameters(), serverPrepareResult, handleIoException(e), false);
    } finally {
      ob20EndSpanAndTrace(spanId);
      lock.unlock();
    }
  }

  @Override
  public long getLastPacketCostTime() throws SQLException {
    if (!enableNetworkStatistics) {
      throw new SQLException("Cant get network cost info while setNetworkStatisticsFlag(true)");
    } else {
      return reader.getTimestampAfterRead() - writer.getTimestampBeforeFlush();
    }
  }

  @Override
  public boolean getNetworkStatisticsFlag() {
    return enableNetworkStatistics;
  }

  @Override
  public void setNetworkStatisticsFlag(boolean flag) {
    enableNetworkStatistics = flag;
    writer.enableNetworkStatistics(flag);
    reader.enableNetworkStatistics(flag);
  }

  @Override
  public long getLastPacketResponseTimestamp() {
    return reader.getTimestampAfterRead();
  }

  @Override
  public long getLastPacketSendTimestamp() {
    return writer.getTimestampBeforeFlush();
  }

  @Override
  public void clearNetworkStatistics() {
    writer.clearNetworkStatistics();
    reader.clearNetworkStatistics();
  }

  @Override
  public void changeUser(String user, String pwd) throws SQLException {
    cmdPrologue();
    lock.lock();
    UUID spanId = ob20BeginTraceAndSpan("change user");

    try {
      Credential credential = new Credential(user, pwd);
      String clientIp = this.socket.getLocalAddress().getHostAddress();
      SendHandshakeResponsePacket.sendChangeUser(writer, credential, host, database, clientCapabilities,
          serverCapabilities, exchangeCharset, (byte) (Boolean.TRUE.equals(options.useSsl) ? 0x02
              : 0x01), options, authenticationPluginType, seed, clientIp, this.isOracleMode());
      getResult(new Results());
      setUsername(user);
      postConnectionQueries();
    } catch (IOException e) {
      throw exceptionWithQuery(
          " change user failed.", handleIoException(e), explicitClosed);
    } finally {
        ob20EndSpanAndTrace(spanId);
        lock.unlock();
    }
  }


  /**
   * Rollback transaction.
   */
  public void rollback() throws SQLException {
    cmdPrologue();
    lock.lock();

    if (inTransaction()) {
      UUID spanId = ob20BeginTraceAndSpan("rollback");

      try {
          executeQuery("ROLLBACK");
      } catch (Exception e) {
          /* eat exception */
      } finally {
          ob20EndSpanAndTrace(spanId);
      }
    }

    lock.unlock();
  }

  @Override
  public void setUsername(String username) {
    this.username = username;
  }

  /**
   * Force release of prepare statement that are not used. This method will be call when adding a
   * new prepare statement in cache, so the packet can be send to server without problem.
   *
   * @param statementId prepared statement Id to remove.
   * @return true if successfully released
   * @throws SQLException if connection exception.
   */
  public boolean forceReleasePrepareStatement(int statementId) throws SQLException {
    if (lock.tryLock()) {
        UUID spanId = null;
        try {
          checkClose();
          spanId = ob20BeginTraceAndSpan("close prepared statement");
          
          writer.startPacket(0);
          writer.write(Packet.COM_STMT_CLOSE);
          writer.writeInt(statementId);
          writer.flush();

          return true;
        } catch (IOException e) {
          connected = false;
          whyConnectedIsFalse();

          throw new SQLNonTransientConnectionException(
              "Could not deallocate query: " + e.getMessage(), "08000", e);
        } finally {
          ob20EndSpanAndTrace(spanId);
          lock.unlock();
        }
    } else {
      // lock is used by another thread (bulk reading)
      statementIdToRelease = statementId;
    }

    return false;
  }

  /**
   * Force release of prepare statement that are not used. This permit to deallocate a statement
   * that cannot be release due to multi-thread use.
   *
   * @throws SQLException if connection occur
   */
  public void forceReleaseWaitingPrepareStatement() throws SQLException {
    if (statementIdToRelease != -1 && forceReleasePrepareStatement(statementIdToRelease)) {
      statementIdToRelease = -1;
    }
  }

  @Override
  public boolean ping() throws SQLException {
    cmdPrologue();
    lock.lock();
    UUID spanId = ob20BeginTraceAndSpan("ping");

    try {
      writer.startPacket(0);
      writer.write(Packet.COM_PING);
      writer.flush();

      Buffer buffer = reader.getPacket(true);
      return buffer.getByteAt(0) == Packet.OK;
    } catch (IOException e) {
      connected = false;
      whyConnectedIsFalse();

      throw new SQLNonTransientConnectionException("Could not ping: " + e.getMessage(), "08000", e);
    } finally {
      ob20EndSpanAndTrace(spanId);
      lock.unlock();
    }
  }

  /**
   * Check that connection is valid. !! careful, timeout is in milliseconds,
   * connection.isValid(timeout) is in seconds !!
   *
   * @param timeout timeout in milliseconds
   * @return true is valid
   * @throws SQLException if any error occur
   */
  @Override
  public boolean isValid(int timeout) throws SQLException {

    int initialTimeout = -1;
    try {
      initialTimeout = this.socketTimeout;
      if (initialTimeout == 0) {
        this.changeSocketSoTimeout(timeout);
      }
      if (isMasterConnection() && !galeraAllowedStates.isEmpty()) {
        // this is a galera node.
        // checking not only that node is responding, but that galera state is allowed.
        Results results = new Results();
        executeQuery(true, results, CHECK_GALERA_STATE_QUERY);
        results.commandEnd();
        ResultSet rs = results.getResultSet();

        return rs != null && rs.next() && galeraAllowedStates.contains(rs.getString(2));
      }

      return ping();

    } catch (SocketException socketException) {
      logger.trace("Connection is not valid", socketException);
      connected = false;
      whyConnectedIsFalse();

      return false;
    } finally {

      // set back initial socket timeout
      try {
        if (initialTimeout != -1) {
          this.changeSocketSoTimeout(initialTimeout);
        }
      } catch (SocketException socketException) {
        logger.warn("Could not set socket timeout back to " + initialTimeout, socketException);
        connected = false;
        whyConnectedIsFalse();

        // eat
      }
    }
  }

  @Override
  public String getCatalog() throws SQLException {

    if ((serverCapabilities & OceanBaseCapabilityFlag.CLIENT_SESSION_TRACK) != 0) {
      // client session track return empty value, not null value. Java require sending null if empty
      if (database != null && database.isEmpty()) {
        return "";
      }
      return database;
    }

    cmdPrologue();
    lock.lock();
    try {
      Results results = new Results();
      executeQuery(isMasterConnection(), results, "select database()");
      results.commandEnd();
      ResultSet rs = results.getResultSet();
      if (rs.next()) {
        this.database = rs.getString(1);
        return database;
      }
      return null;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void setCatalog(final String database) throws SQLException {
    cmdPrologue();
    lock.lock();
    UUID spanId = ob20BeginTraceAndSpan("set database");

    try {
      SendChangeDbPacket.send(writer, database);
      final Buffer buffer = reader.getPacket(true);

      if (buffer.getByteAt(0) == Packet.ERROR) {
        final ErrorPacket ep = new ErrorPacket(buffer);
        throw new SQLException(
            "Could not select database '" + database + "' : " + ep.getMessage(),
            ep.getSqlState(),
            ep.getErrorCode());
      }
      this.database = database;
    } catch (IOException e) {
      throw exceptionWithQuery("COM_INIT_DB", handleIoException(e), false);
    } finally {
      ob20EndSpanAndTrace(spanId);
      lock.unlock();
    }
  }

  @Override
  public void resetDatabase() throws SQLException {
    if (!database.equals(urlParser.getDatabase())) {
      setCatalog(urlParser.getDatabase());
    }
  }

  /**
   * Cancels the current query - clones the current protocol and executes a query using the new
   * connection.
   *
   * @throws SQLException never thrown
   */
  @Override
  public void cancelCurrentQuery() throws SQLException {
    try (MasterProtocol copiedProtocol =
             new MasterProtocol(urlParser, new GlobalStateInfo(), new ReentrantLock(), traceCache)) {
      copiedProtocol.setHostAddress(getHostAddress());
      copiedProtocol.connect();
      // no lock, because there is already a query running that possessed the lock.
      copiedProtocol.executeQuery("KILL QUERY " + serverThreadId);
    } catch (IOException e) {
        throw ExceptionFactory.INSTANCE.create(String.format("Could not connect to %s. %s%s", getHostAddress(), e.getMessage(), getTraces()), "08000", e);
    }
    interrupted = true;
  }

  /**
   * Get current autocommit status.
   *
   * @return autocommit status
   */
  @Override
  public boolean getAutocommit() {
    return this.autoCommit;
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    if (this.autoCommit != autoCommit) {
      executeQuery("set autocommit = " + (autoCommit ? "1" : "0"));
    } else {
      return;
    }
    this.autoCommit = autoCommit;
  }

  @Override
  public boolean inTransaction() {
    return ((serverStatus & ServerStatus.IN_TRANSACTION) != 0);
  }

  public void closeExplicit() {
    this.explicitClosed = true;
    close();
  }

  /**
   * Deallocate prepare statement if not used anymore.
   *
   * @param serverPrepareResult allocation result
   * @throws SQLException if de-allocation failed.
   */
  @Override
  public void releasePrepareStatement(ServerPrepareResult serverPrepareResult) throws SQLException {
    // If prepared cache is enable, the ServerPrepareResult can be shared in many PrepStatement,
    // so synchronised use count indicator will be decrement.
    serverPrepareResult.decrementShareCounter();

    // deallocate from server if not cached
    if (serverPrepareResult.canBeDeallocate()) {
      forceReleasePrepareStatement(serverPrepareResult.getStatementId());
      serverPrepareResult.release();
    }
  }

  public long getMaxRows() {
    return maxRows;
  }

  @Override
  public void setMaxRows(long max) throws SQLException {
    if (maxRows != max) {
      if (max == 0) {
        executeQuery("set @@SQL_SELECT_LIMIT=DEFAULT");
      } else {
        executeQuery("set @@SQL_SELECT_LIMIT=" + max);
      }
      maxRows = max;
    }
  }

  @Override
  public void setLocalInfileInputStream(InputStream inputStream) {
    this.localInfileInputStream = inputStream;
  }

  /**
   * Returns the connection timeout in milliseconds.
   *
   * @return the connection timeout in milliseconds.
   */
  @Override
  public int getTimeout() {
    return this.socketTimeout;
  }

  /**
   * Sets the connection timeout.
   *
   * @param timeout the timeout, in milliseconds
   * @throws SocketException if there is an error in the underlying protocol, such as a TCP error.
   */
  @Override
  public void setTimeout(int timeout) throws SocketException {
    lock.lock();
    try {
      this.changeSocketSoTimeout(timeout);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Puts this connection in read-only / read-write mode
   *
   * @param readOnly true enables read-only mode; false disables it
   * @throws SQLException If socket error.
   */
  public void setReadonly(final boolean readOnly) throws SQLException {
    if (options.assureReadOnly && this.readOnly != readOnly && versionGreaterOrEqual(5, 6, 5)) {
      executeQuery("SET SESSION TRANSACTION " + (readOnly ? "READ ONLY" : "READ WRITE"));
    }
    this.readOnly = readOnly;
  }

  /**
   * Set transaction isolation.
   *
   * @param level transaction level.
   * @throws SQLException if transaction level is unknown
   */
  public void setTransactionIsolation(final int level) throws SQLException {
    cmdPrologue();
    lock.lock();
    try {
      if (transactionIsolationLevel == level) {
        return;
      }
      String query = "SET SESSION TRANSACTION ISOLATION LEVEL";
      switch (level) {
        case Connection.TRANSACTION_READ_UNCOMMITTED:
          query += " READ UNCOMMITTED";
          break;
        case Connection.TRANSACTION_READ_COMMITTED:
          query += " READ COMMITTED";
          break;
        case Connection.TRANSACTION_REPEATABLE_READ:
          query += " REPEATABLE READ";
          break;
        case Connection.TRANSACTION_SERIALIZABLE:
          query += " SERIALIZABLE";
          break;
        default:
          throw new SQLException("Unsupported transaction isolation level");
      }
      executeQuery(query);
      transactionIsolationLevel = level;
    } finally {
      lock.unlock();
    }
  }

  public int getTransactionIsolationLevel() {
    return transactionIsolationLevel;
  }

  private void checkClose() throws SQLException {
    if (!this.connected) {
      throw new SQLException("Connection is close", "08000", 1220);
    }
  }

    @Override
    public void getResult(Results results) throws SQLException {
        reader.startReceiveResponse();

        try {
            readPacket(results);
            while (hasMoreResults()) {
                readPacket(results);
            }
        } finally {
            reader.endReceiveResponse(results.getSql());
        }
    }

    public ColumnDefinition[] getResultWithoutValue(Results results) throws SQLException {
        reader.startReceiveResponse();

        try {
            Buffer buffer;
            ColumnDefinition[] ci = null;
            try {
                buffer = reader.getPacket(true);
            } catch (IOException e) {
                throw handleIoException(e);
            }

            switch (buffer.getByteAt(0)) {

                // *********************************************************************************************************
                // * OK response
                // *********************************************************************************************************
                case Packet.OK:
                    readOkPacket(buffer, results);
                    break;

                // *********************************************************************************************************
                // * ERROR response
                // *********************************************************************************************************
                case Packet.ERROR:
                    throw readErrorPacket(buffer, results);

                    // *********************************************************************************************************
                    // * LOCAL INFILE response
                    // *********************************************************************************************************
                case Packet.LOCAL_INFILE:
                    readLocalInfilePacket(buffer, results);
                    break;

                // *********************************************************************************************************
                // * ResultSet
                // *********************************************************************************************************
                default:
                    ci = readResultSetColumnDef(buffer, results);
                    break;
            }
            return ci;
        } finally {
            reader.endReceiveResponse(results.getSql());
        }
    }

  private ColumnDefinition[] readResultSetColumnDef(Buffer buffer, Results results) throws SQLException {
    long fieldCount = buffer.getLengthEncodedNumeric();
    // read columns information's
    try {
      ColumnDefinition[] ci = new ColumnDefinition[(int) fieldCount];
      for (int i = 0; i < fieldCount; i++) {
        ci[i] = new ColumnDefinition(reader.getPacket(false), isOracleMode(), options.characterEncoding);
      }

      if (!eofDeprecated) {
        Buffer bufferEof = reader.getPacket(true);
        if (bufferEof.readByte() != Packet.EOF) {
          // using IOException to close connection,
          throw new IOException(
              "Packets out of order when reading field packets, expected was EOF stream."
                  + ((options.enablePacketDebug) ? getTraces() : "Packet contents (hex) = "
                  + Utils.hexdump(options.maxQuerySizeToLog, 0, bufferEof.limit, bufferEof.buf)));
        }
        bufferEof.skipBytes(2);
        bufferEof.readShort();
      }
      return ci;
    } catch (IOException e) {
      throw handleIoException(e);
    }
  }

  /**
   * Read server response packet.
   *
   * @param results result object
   * @throws SQLException if sub-result connection fail
   * @see <a href="https://mariadb.com/kb/en/mariadb/4-server-response-packets/">server response
   * packets</a>
   */
  private void readPacket(Results results) throws SQLException {
    Buffer buffer;
    try {
      buffer = reader.getPacket(true);
    } catch (IOException e) {
      throw handleIoException(e);
    }

    switch (buffer.getByteAt(0)) {

      // *********************************************************************************************************
      // * OK response
      // *********************************************************************************************************
      case Packet.OK:
        readOkPacket(buffer, results);
        break;

      // *********************************************************************************************************
      // * ERROR response
      // *********************************************************************************************************
      case Packet.ERROR:
        throw readErrorPacket(buffer, results);

        // *********************************************************************************************************
        // * LOCAL INFILE response
        // *********************************************************************************************************
      case Packet.LOCAL_INFILE:
        readLocalInfilePacket(buffer, results);
        break;

      // *********************************************************************************************************
      // * ResultSet
      // *********************************************************************************************************
      default:
        long fieldCount = buffer.getLengthEncodedNumeric();
        ColumnDefinition[] columns = new ColumnDefinition[(int) fieldCount];
        readResultSet(columns, results);
        break;
    }
  }

  private void handleStateChange(Buffer buf, Results results) {
    buf.skipLengthEncodedBytes(); // info
    while (buf.remaining() > 0) {
      Buffer stateInfo = buf.getLengthEncodedBuffer();
      if (stateInfo.remaining() > 0) {
        switch (stateInfo.readByte()) {
          case StateChange.SESSION_TRACK_SYSTEM_VARIABLES:
            Buffer sessionVariableBuf = stateInfo.getLengthEncodedBuffer();
            String variable = sessionVariableBuf.readStringLengthEncoded(StandardCharsets.UTF_8);
            String value = sessionVariableBuf.readStringLengthEncoded(StandardCharsets.UTF_8);
            logger.debug("System variable change :  {} = {}", variable, value);

            // only variable uses
            switch (variable) {
              case "auto_increment_increment":
                autoIncrementIncrement = Integer.parseInt(value);
                results.setAutoIncrement(autoIncrementIncrement);
                break;

              default:
                // variable not used by driver
            }
            break;

          case StateChange.SESSION_TRACK_SCHEMA:
            Buffer sessionSchemaBuf = stateInfo.getLengthEncodedBuffer();
            database = sessionSchemaBuf.readStringLengthEncoded(StandardCharsets.UTF_8);
            logger.debug("Database change : now is '{}'", database);
            break;

          default:
            stateInfo.skipLengthEncodedBytes();
        }
      }
    }
  }

  /**
   * Get current auto increment increment. *** no lock needed ****
   *
   * @return auto increment increment.
   * @throws SQLException if cannot retrieve auto increment value
   */
  public int getAutoIncrementIncrement() throws SQLException {
    if (!options.connectProxy) {
      if (autoIncrementIncrement == 0) {
        try {
          Results results = new Results();
          executeQuery(true, results, "select @@auto_increment_increment");
          results.commandEnd();
          ResultSet rs = results.getResultSet();
          rs.next();
          autoIncrementIncrement = rs.getInt(1);
        } catch (SQLException e) {
          if (e.getSQLState().startsWith("08")) {
            throw e;
          }
          autoIncrementIncrement = 1;
        }
      }
    }
    return autoIncrementIncrement;
  }

  /**
   * Read OK_Packet.
   *
   * @param buffer  current buffer
   * @param results result object
   * @see <a href="https://mariadb.com/kb/en/mariadb/ok_packet/">OK_Packet</a>
   */
  public void readOkPacket(Buffer buffer, Results results) {
    buffer.skipByte(); // fieldCount
    final long updateCount = buffer.getLengthEncodedNumeric();
    final long insertId = buffer.getLengthEncodedNumeric();

    serverStatus = buffer.readShort();
    hasWarnings = (buffer.readShort() > 0);

    if ((serverStatus & ServerStatus.SERVER_SESSION_STATE_CHANGED) != 0) {
      handleStateChange(buffer, results);
    }

    results.addStats(updateCount, insertId, hasMoreResults());
  }

  /**
   * Read ERR_Packet.
   *
   * @param buffer  current buffer
   * @param results result object
   * @return SQLException if sub-result connection fail
   * @see <a href="https://mariadb.com/kb/en/mariadb/err_packet/">ERR_Packet</a>
   */
  public SQLException readErrorPacket(Buffer buffer, Results results) {
    removeHasMoreResults();
    this.hasWarnings = false;

    ErrorPacket ep = new ErrorPacket(buffer);

    results.addStatsError(false);
    // force current status to in transaction to ensure rollback/commit, since command may have issue a transaction
    serverStatus |= ServerStatus.IN_TRANSACTION;
    removeActiveStreamingResult();

    if (1054 == ep.getErrorCode()) {
      return new SQLException(
          ep.getMessage()
              + "\nIf column exists but type cannot be identified (example 'select ? `field1` from dual'). "
              + "Use CAST function to solve this problem (example 'select CAST(? as integer) `field1` from dual')",
          ep.getSqlState(), ep.getErrorCode());
    } else {
      return new SQLException(ep.getMessage(), ep.getSqlState(), ep.getErrorCode());
    }
  }

  /**
   * Read Local_infile Packet.
   *
   * @param buffer  current buffer
   * @param results result object
   * @throws SQLException if sub-result connection fail
   * @see <a href="https://mariadb.com/kb/en/mariadb/local_infile-packet/">local_infile packet</a>
   */
  private void readLocalInfilePacket(Buffer buffer, Results results) throws SQLException {
    UUID spanId = ob20BeginTraceAndSpan("read local infile packet");

    int seq = 2;
    buffer.getLengthEncodedNumeric(); // field pos
    String fileName = buffer.readStringNullEnd(StandardCharsets.UTF_8);

    try {
      // Server request the local file (LOCAL DATA LOCAL INFILE)
      // We do accept general URLs, too. If the localInfileStream is
      // set, use that.
      InputStream is;
      writer.startPacket(seq);
      if (localInfileInputStream == null) {
        if (!getUrlParser().getOptions().allowLocalInfile) {
          writer.writeEmptyPacket();
          reader.getPacket(true);
          throw new SQLException(
              "Usage of LOCAL INFILE is disabled. To use it enable it via the connection property allowLocalInfile=true",
              SqlStates.FEATURE_NOT_SUPPORTED.getSqlState(),
              -1);
        }

        // validate all defined interceptors
        ServiceLoader<LocalInfileInterceptor> loader = ServiceLoader.load(LocalInfileInterceptor.class);
        for (LocalInfileInterceptor interceptor : loader) {
          if (!interceptor.validate(fileName)) {
            writer.writeEmptyPacket();
            reader.getPacket(true);
            throw new SQLException(
                "LOAD DATA LOCAL INFILE request to send local file named \""
                    + fileName
                    + "\" not validated by interceptor \""
                    + interceptor.getClass().getName()
                    + "\"");
          }
        }

        if (results.getSql() == null) {
          writer.writeEmptyPacket();
          reader.getPacket(true);
          throw new SQLException(
              "LOAD DATA LOCAL INFILE not permit in batch. file '" + fileName + "'",
              SqlStates.INVALID_AUTHORIZATION.getSqlState(),
              -1);
        } else if (!Utils.validateFileName(results.getSql(), results.getParameters(), fileName)) {
          writer.writeEmptyPacket();
          reader.getPacket(true);
          throw new SQLException(
              "LOAD DATA LOCAL INFILE asked for file '"
                  + fileName
                  + "' that doesn't correspond to initial query "
                  + results.getSql()
                  + ". Possible malicious proxy changing server answer ! Command interrupted",
              SqlStates.INVALID_AUTHORIZATION.getSqlState(),
              -1);
        }

        try {
          URL url = new URL(fileName);
          is = url.openStream();
        } catch (IOException ioe) {
          try {
            is = new FileInputStream(fileName);
          } catch (FileNotFoundException f) {
            writer.writeEmptyPacket();
            reader.getPacket(true);
            throw new SQLException("Could not send file : " + f.getMessage(), "22000", -1, f);
          }
        }
      } else {
        is = localInfileInputStream;
        localInfileInputStream = null;
      }

      try {
        byte[] buf = new byte[8192];
        int len;
        while ((len = is.read(buf)) > 0) {
          writer.startPacket(seq++);
          writer.write(buf, 0, len);
          writer.flush();
        }
        writer.writeEmptyPacket();
      } catch (IOException ioe) {
        throw handleIoException(ioe);
      } finally {
        is.close();
      }
      getResult(results);
    } catch (IOException e) {
      throw handleIoException(e);
    } finally {
      ob20EndSpanAndTrace(spanId);
    }
  }

  /**
   * Read ResultSet Packet.
   *
   * @param results result object
   * @throws SQLException if sub-result connection fail
   * @see <a href="https://mariadb.com/kb/en/mariadb/resultset/">resultSet packets</a>
   */
  public void readResultSet(ColumnDefinition[] ci, Results results) throws SQLException {
    try {
      // read columns information's
      for (int i = 0; i < ci.length; i++) {
        ci[i] = new ColumnDefinition(reader.getPacket(false), isOracleMode(), options.characterEncoding);
      }

      boolean callableResult = false;
      boolean isPsOutParamter = false;
      if (!eofDeprecated) {
        /** read EOF packet
         * EOF status is mandatory because :
         * Call query will have an callable resultSet for OUT parameters
         * -> this resultSet must be identified and not listed in JDBC statement.getResultSet()
         * after a callable resultSet, a OK packet is send, but mysql does send the bad "more result flag"
         */
        Buffer bufferEof = reader.getPacket(true);
        if (bufferEof.readByte() != Packet.EOF) {
          // using IOException to close connection,
          throw new IOException(
              "Packets out of order when reading field packets, expected was EOF stream."
                  + ((options.enablePacketDebug)
                  ? getTraces()
                  : "Packet contents (hex) = "
                  + Utils.hexdump(
                  options.maxQuerySizeToLog, 0, bufferEof.limit, bufferEof.buf)));
        }
        bufferEof.skipBytes(2); // Skip warningCount
        short currentStatus = bufferEof.readShort();
        /**
         * We don't send an ok packet after all result set rows;
         * Ob server send an execution response packet, but the PS_OUT_PARAMETERS is set
         */
        callableResult = (currentStatus & ServerStatus.PS_OUT_PARAMETERS) != 0;
      }

      SelectResultSet selectResultSet;
      if (results.isToCursorFetch() || (results.isToPrepareExecute() && !results.isInternalResult() && !callableResult)) {
        // cursor result set
        if (!this.isOracleMode()
            || results.getResultSetScrollType() == ResultSet.TYPE_FORWARD_ONLY
            || (results.getResultSetScrollType() == ResultSet.TYPE_SCROLL_INSENSITIVE && results.getResultSetConcurrency() == ResultSet.CONCUR_READ_ONLY)) {
          selectResultSet = new CursorResultSet(ci, results, this, callableResult, eofDeprecated, isPsOutParamter);
        } else {
          selectResultSet = new SensitiveCursorResultSet(ci, results, this, callableResult, eofDeprecated, isPsOutParamter);
        }
      } else {
        // streaming or complete result set, not support TYPE_SCROLL_SENSITIVE ResultSet for MySQL mode
        selectResultSet = new SelectResultSet(ci, results, this, reader, callableResult, eofDeprecated, isPsOutParamter);
      }

      results.addResultSet(selectResultSet, hasMoreResults() || results.getFetchSize() > 0);
    } catch (IOException e) {
      throw handleIoException(e);
    }
  }

  public void prologProxy(
      ServerPrepareResult serverPrepareResult,
      long maxRows,
      boolean hasProxy,
      OceanBaseConnection connection,
      OceanBaseStatement statement) throws SQLException {
    prolog(maxRows, hasProxy, connection, statement);
  }

  /**
   * Preparation before command.
   *
   * @param maxRows    query max rows
   * @param hasProxy   has proxy
   * @param connection current connection
   * @param statement  current statement
   * @throws SQLException if any error occur.
   */
  public void prolog(
      long maxRows,
      boolean hasProxy,
      OceanBaseConnection connection,
      OceanBaseStatement statement) throws SQLException {
    if (explicitClosed) {
      throw new SQLNonTransientConnectionException(
          "execute() is called on closed connection", "08000");
    }
    // old failover handling
    if (!hasProxy && shouldReconnectWithoutProxy()) {
      try {
        connectWithoutProxy();
      } catch (SQLException qe) {
        throw ExceptionFactory.of((int) serverThreadId, options).create(qe);
      }
    }

    try {
      setMaxRows(maxRows);
    } catch (SQLException qe) {
      throw ExceptionFactory.of((int) serverThreadId, options).create(qe);
    }

    connection.reenableWarnings();
  }

  public ServerPrepareResult addPrepareInCache(String key, ServerPrepareResult serverPrepareResult) {
    return serverPrepareStatementCache.put(key, serverPrepareResult);
  }

  private void cmdPrologue() throws SQLException {
    // load active result if any so buffer are clean for next query
    if (activeStreamingResult != null) {
      if (!options.clobberStreamingResults) {
        throw new SQLException("Streaming result set " + activeStreamingResult + " is still active. No statements may be issued when any streaming result sets are open and in use on a given connection. Ensure that you have called .close() on any active streaming result sets before attempting more queries.");
      }
      activeStreamingResult.loadFully(true, this);
      activeStreamingResult = null;
    }

    if (activeFutureTask != null) {
      // wait for remaining batch result to be read, to ensure correct connection state
      try {
        activeFutureTask.get();
      } catch (ExecutionException executionException) {
        // last batch exception are to be discarded
      } catch (InterruptedException interruptedException) {
        Thread.currentThread().interrupt();
        throw new SQLException(
            "Interrupted reading remaining batch response ",
            SqlStates.INTERRUPTED_EXCEPTION.getSqlState(),
            -1,
            interruptedException);
      } finally {
        // bulk can prepare, and so if prepare cache is enable, can replace an already cached
        // prepareStatement
        // this permit to release those old prepared statement without conflict.
        forceReleaseWaitingPrepareStatement();
      }
      activeFutureTask = null;
    }

    if (!this.connected) {
      throw exceptionFactory.create("Connection is closed", "08000", 1220);
    }
    interrupted = false;
  }

  /**
   * Set current state after a failover.
   *
   * @param maxRows                   current Max rows
   * @param transactionIsolationLevel current transactionIsolationLevel
   * @param database                  current database
   * @param autocommit                current autocommit state
   * @throws SQLException if any error occur.
   */
  // TODO set all client affected variables when implementing CONJ-319
  public void resetStateAfterFailover(
      long maxRows,
      int transactionIsolationLevel,
      String database,
      boolean autocommit) throws SQLException {
    setMaxRows(maxRows);

    if (transactionIsolationLevel != 0) {
      setTransactionIsolation(transactionIsolationLevel);
    }

    if (database != null && !"".equals(database) && !getDatabase().equals(database)) {
      setCatalog(database);
    }

    if (getAutocommit() != autocommit) {
      executeQuery("set autocommit=" + (autocommit ? "1" : "0"));
    }
  }

  /**
   * Handle IoException (reconnect if Exception is due to having send too much data, making server
   * close the connection.
   *
   * <p>There is 3 kind of IOException :
   *
   * <ol>
   *   <li>MaxAllowedPacketException : without need of reconnect : thrown when driver don't send
   *       packet that would have been too big then error is not a CONNECTION_EXCEPTION
   *   <li>packets size is greater than max_allowed_packet (can be checked with
   *       writer.isAllowedCmdLength()). Need to reconnect
   *   <li>unknown IO error throw a CONNECTION_EXCEPTION
   * </ol>
   *
   * @param initialException initial Io error
   * @return the resulting error to return to client.
   */
  public SQLException handleIoException(Exception initialException) {
    boolean mustReconnect = options.autoReconnect;
    boolean maxSizeError;

    if (initialException instanceof MaxAllowedPacketException) {
      maxSizeError = true;
      if (((MaxAllowedPacketException) initialException).isMustReconnect()) {
        mustReconnect = true;
      } else {
        return new SQLNonTransientConnectionException(
            initialException.getMessage() + getTraces(),
            SqlStates.UNDEFINED_SQLSTATE.getSqlState(),
            initialException);
      }
    } else {
      maxSizeError = writer.exceedMaxLength();
      if (maxSizeError) {
        mustReconnect = true;
      }
    }

    if (mustReconnect && !explicitClosed) {
      String traces = getTraces();
      try {
        connect();
        try {
          resetStateAfterFailover(
              getMaxRows(), getTransactionIsolationLevel(), getDatabase(), getAutocommit());

          if (maxSizeError) {
            return new SQLTransientConnectionException(
                "Could not send query: query size is >= to max_allowed_packet ("
                    + writer.getMaxAllowedPacket()
                    + ")"
                    + traces,
                "HY000",
                initialException);
          }
          cleanMemory();

          return new SQLTransientConnectionException(
              initialException.getMessage() + traces, "HY000", initialException);

        } catch (SQLException queryException) {
          return new SQLTransientConnectionException(
              "reconnection succeed, but resetting previous state failed" + traces,
              "HY000",
              initialException);
        }

      } catch (SQLException | IOException queryException) {
        connected = false;
        whyConnectedIsFalse();

        return new SQLNonTransientConnectionException(
            initialException.getMessage() + "\nError during reconnection" + traces,
            "08000",
            queryException);
      }
    }
    connected = false;
    whyConnectedIsFalse();

    return new SQLNonTransientConnectionException(
        initialException.getMessage() + getTraces(), "08000", initialException);
  }

  public void setActiveFutureTask(FutureTask activeFutureTask) {
    this.activeFutureTask = activeFutureTask;
  }

  public void interrupt() {
    interrupted = true;
  }

  public boolean isInterrupted() {
    return interrupted;
  }

  /**
   * Throw TimeoutException if timeout has been reached.
   *
   * @throws SQLTimeoutException to indicate timeout exception.
   */
  public void stopIfInterrupted() throws SQLTimeoutException {
    if (isInterrupted()) {
      // interrupted during read, must throw an exception manually
      throw new SQLTimeoutException("Timeout during batch execution");
    }
  }

  @Override
  public long getChecksum() {
    return checksum;
  }

  @Override
  public void setChecksum(long checksum) {
    this.checksum = checksum;
  }

  @Override
  public void resetChecksum() {
    this.checksum = 1;
  }

  @Override
  public int getIterationCount() {
    return iterationCount;
  }

  @Override
  public void setIterationCount(int iterationCount) {
    this.iterationCount = iterationCount;
  }

  @Override
  public int getExecuteMode() {
    return executeMode;
  }

  @Override
  public void setExecuteMode(int executeMode) {
    this.executeMode = executeMode;
  }

  @Override
  public void setComStmtPrepareExecuteField(int iterationCount, int executeMode, long checksum) {
    this.iterationCount = iterationCount;
    this.executeMode = executeMode;
    this.checksum = checksum;
  }

  private UUID ob20BeginTraceAndSpan(String tagStr) {
    ob20BeginTrace();
    return ob20BeginSpan(tagStr);
  }

  private void ob20BeginTrace() {
    if (enableFullLinkTrace) {
      try {
        if (fullLinkTrace.isShowTraceEnabled() || !inTransaction()) {
          fullLinkTrace.beginTrace();
        }
      } catch (Exception e) {
        logger.warn("Full link trace module failed.", e);
      }
    }
  }

  private UUID ob20BeginSpan(String tagStr) {
    UUID spanId = null;
    if (enableFullLinkTrace) {
      try {
        spanId = fullLinkTrace.beginSpan(0);
        fullLinkTrace.setSpanTag(1, FullLinkTrace.TagKey.COMMAND_NAME.getString(), tagStr);
        fullLinkTrace.setSpanTag(1, FullLinkTrace.TagKey.CLIENT_HOST.getString(), socket.getLocalAddress().getHostAddress() + ":" + socket.getLocalPort());
        fullLinkTrace.buildRequest(ob20);
      } catch (Exception e) {
        logger.warn("Full link trace module failed.", e);
      }
    }
    return spanId;
  }

  private void ob20EndSpanAndTrace(UUID spanId) {
    if (enableFullLinkTrace) {
      try {
        if (spanId != null) {
          fullLinkTrace.endSpan(spanId);

          if (fullLinkTrace.isShowTraceEnabled() || !inTransaction()) {
            fullLinkTrace.endTrace();
          }
        }
      } catch (Exception e) {
        logger.warn("Full link trace module failed.", e);
      }
    }
  }

}
