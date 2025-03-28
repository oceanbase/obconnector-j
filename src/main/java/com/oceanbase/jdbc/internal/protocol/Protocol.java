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
package com.oceanbase.jdbc.internal.protocol;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.FutureTask;
import java.util.concurrent.locks.ReentrantLock;

import com.oceanbase.jdbc.*;
import com.oceanbase.jdbc.internal.com.read.Buffer;
import com.oceanbase.jdbc.internal.com.read.dao.Results;
import com.oceanbase.jdbc.internal.com.read.resultset.ColumnDefinition;
import com.oceanbase.jdbc.internal.com.send.parameters.ParameterHolder;
import com.oceanbase.jdbc.internal.failover.FailoverProxy;
import com.oceanbase.jdbc.internal.io.input.PacketInputStream;
import com.oceanbase.jdbc.internal.io.output.PacketOutputStream;
import com.oceanbase.jdbc.internal.util.ServerPrepareStatementCache;
import com.oceanbase.jdbc.internal.util.dao.ClientPrepareResult;
import com.oceanbase.jdbc.internal.util.dao.ServerPrepareResult;
import com.oceanbase.jdbc.util.Options;

public interface Protocol {

    ServerPrepareResult prepare(String sql, boolean executeOnMaster) throws SQLException;

    boolean getAutocommit() throws SQLException;

    void setAutoCommit(boolean autoCommit) throws SQLException;

    boolean noBackslashEscapes();

    void connect() throws SQLException, IOException;

    UrlParser getUrlParser();

    boolean inTransaction();

    boolean isOracleMode();

    boolean isTZTablesImported();

    FailoverProxy getProxy();

    void setProxy(FailoverProxy proxy);

    Options getOptions();

    boolean hasMoreResults();

    void close();

    void abort();

    void reset() throws SQLException;

    void closeExplicit();

    boolean isClosed();

    void resetDatabase() throws SQLException;

    String getCatalog() throws SQLException;

    void setCatalog(String database) throws SQLException;

    String getServerVersion();

    void setObServerVersion(String version);

    String getObServerVersion();

    boolean haveInformationSchemaParameters();

    boolean supportStmtPrepareExecute();

    boolean supportFetchWithOffset();

    void setFullLinkTraceModule(String module, String action);

    String getFullLinkTraceModule();

    void setFullLinkTraceAction(String action);

    String getFullLinkTraceAction();

    void setFullLinkTraceClientInfo(String clientInfo);

    String getFullLinkTraceClientInfo();

    void setFullLinkTraceIdentifier(String clientIdentifier);

    String getFullLinkTraceIdentifier();

    byte getFullLinkTraceLevel();

    double getFullLinkTraceSamplePercentage();

    byte getFullLinkTraceRecordPolicy();

    double getFullLinkTracePrintSamplePercentage();

    long getFullLinkTraceSlowQueryThreshold();

    boolean isConnected();

    boolean getReadonly() throws SQLException;

    @SuppressWarnings("RedundantThrows")
    void setReadonly(boolean readOnly) throws SQLException;

    boolean isMasterConnection();

    boolean mustBeMasterConnection();

    HostAddress getHostAddress();

    void setHostAddress(HostAddress hostAddress);

    String getHost();

    int getPort();

    void rollback() throws SQLException;

    String getDatabase();

    String getUsername();

    void setUsername(String username);

    boolean ping() throws SQLException;

    boolean isValid(int timeout) throws SQLException;

    void executeQuery(String sql) throws SQLException;

    void executeQuery(boolean mustExecuteOnMaster, Results results, final String sql)
                                                                                     throws SQLException;

    void executeQuery(boolean mustExecuteOnMaster, Results results, final String sql,
                      Charset charset) throws SQLException;

    void executeQuery(boolean mustExecuteOnMaster, Results results,
                      final ClientPrepareResult clientPrepareResult, ParameterHolder[] parameters)
                                                                                                  throws SQLException;

    void executeQuery(boolean mustExecuteOnMaster, Results results,
                      final ClientPrepareResult clientPrepareResult, ParameterHolder[] parameters,
                      int timeout) throws SQLException;

    void executePreparedQuery(boolean mustExecuteOnMaster, ServerPrepareResult serverPrepareResult,
                              Results results, ParameterHolder[] parameters) throws SQLException;

    ServerPrepareResult executePreparedQuery(int parameterCount, ParameterHolder[] parameters,
                                             ServerPrepareResult serverPrepareResult,
                                             Results results) throws SQLException;

    ServerPrepareResult executeBatchServer(ServerPrepareResult serverPrepareResult,
                                           Results results, String sql,
                                           List<ParameterHolder[]> parameterList,
                                           boolean hasLongData) throws SQLException;

    boolean executeBatchClient(boolean mustExecuteOnMaster, Results results,
                               final ClientPrepareResult prepareResult,
                               final List<ParameterHolder[]> parametersList, boolean hasLongData)
                                                                                                 throws SQLException;

    void executeBatchStmt(boolean mustExecuteOnMaster, Results results, final List<String> queries)
                                                                                                   throws SQLException;

    void getResult(Results results) throws SQLException;

    void cancelCurrentQuery() throws SQLException;

    void interrupt();

    void skip() throws SQLException;

    boolean checkIfMaster() throws SQLException;

    boolean hasWarnings();

    long getMaxRows();

    void setMaxRows(long max) throws SQLException;

    int getMajorServerVersion();

    int getMinorServerVersion();

    void parseVersion(String serverVersion);

    boolean versionGreaterOrEqual(int major, int minor, int patch);

    void setLocalInfileInputStream(InputStream inputStream);

    int getTimeout();

    void setTimeout(int timeout) throws SocketException;

    boolean getPinGlobalTxToPhysicalConnection();

    long getServerThreadId();

    Socket getSocket();

    void setTransactionIsolation(int level) throws SQLException;

    int getTransactionIsolationLevel();

    boolean isExplicitClosed();

    void connectWithoutProxy() throws SQLException;

    boolean shouldReconnectWithoutProxy();

    void setHostFailedWithoutProxy();

    void releasePrepareStatement(ServerPrepareResult serverPrepareResult) throws SQLException;

    boolean forceReleasePrepareStatement(int statementId) throws SQLException;

    ServerPrepareStatementCache prepareStatementCache();

    TimeZone getTimeZone();

    void prolog(long maxRows, boolean hasProxy, OceanBaseConnection connection,
                OceanBaseStatement statement) throws SQLException;

    void prologProxy(ServerPrepareResult serverPrepareResult, long maxRows, boolean hasProxy,
                     OceanBaseConnection connection, OceanBaseStatement statement)
                                                                                  throws SQLException;

    Results getActiveStreamingResult();

    void setActiveStreamingResult(Results mariaSelectResultSet);

    ReentrantLock getLock();

    void setServerStatus(short serverStatus);

    void removeHasMoreResults();

    void setHasWarnings(boolean hasWarnings);

    ServerPrepareResult addPrepareInCache(String key, ServerPrepareResult serverPrepareResult);

    void readOkPacket(Buffer buffer, Results results);

    void readEofPacket() throws SQLException, IOException;

    void skipEofPacket() throws SQLException, IOException;

    SQLException readErrorPacket(Buffer buffer, Results results);

    void readResultSet(ColumnDefinition[] ci, Results results) throws SQLException;

    void changeSocketTcpNoDelay(boolean setTcpNoDelay);

    void changeSocketSoTimeout(int setSoTimeout) throws SocketException;

    void removeActiveStreamingResult();

    void resetStateAfterFailover(long maxRows, int transactionIsolationLevel, String database,
                                 boolean autocommit) throws SQLException;

    void setActiveFutureTask(FutureTask activeFutureTask);

    boolean isServerMariaDb();

    SQLException handleIoException(Exception initialException);

    PacketInputStream getReader();

    boolean isEofDeprecated();

    int getAutoIncrementIncrement() throws SQLException;

    boolean sessionStateAware();

    String getTraces();

    boolean isInterrupted();

    void stopIfInterrupted() throws SQLTimeoutException;

    void setChecksum(long checksum);

    void resetChecksum();

    long getChecksum();

    void setIterationCount(int iterationCount);

    int getIterationCount();

    void setExecuteMode(int executeMode);

    int getExecuteMode();

    public void setComStmtPrepareExecuteField(int iterationCount, int executeMode, long checksum);

    ColumnDefinition[] fetchRowViaCursor(int cursorId, int fetchSize, Results results)
                                                                                      throws SQLException;

    ColumnDefinition[] fetchRowViaCursorForOracle(int cursorId, int numRows, byte offsetType,
                                                  int offset, Results results) throws SQLException;

    long getLastPacketCostTime() throws SQLException;

    void setNetworkStatisticsFlag(boolean flag);

    boolean getNetworkStatisticsFlag();

    long getLastPacketResponseTimestamp();

    long getLastPacketSendTimestamp();

    void clearNetworkStatistics();

    void changeUser(String user, String pwd) throws SQLException;

    String getEncoding();

    void executePreparedQueryArrayBinding(boolean mustExecuteOnMaster,
                                          ServerPrepareResult serverPrepareResult, Results results,
                                          List<ParameterHolder[]> queryParameters,
                                          int queryParameterSize) throws SQLException;

    ServerPrepareResult executePreparedQueryArrayBinding(int parameterCount,
                                                         boolean mustExecuteOnMaster,
                                                         ServerPrepareResult serverPrepareResult,
                                                         Results results,
                                                         List<ParameterHolder[]> queryParameters,
                                                         int queryParameterSize)
                                                                                throws SQLException;

    PacketOutputStream getWriter();

    HostAddress getCurrentHost();

    TimeTrace getTimeTrace();

    void startCallInterface();

    void endCallInterface(String message);

    int getServerStatus();
}
