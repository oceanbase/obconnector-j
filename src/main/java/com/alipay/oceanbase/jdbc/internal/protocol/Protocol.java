package com.alipay.oceanbase.jdbc.internal.protocol;

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

import com.alipay.oceanbase.jdbc.HostAddress;
import com.alipay.oceanbase.jdbc.OceanBaseConnection;
import com.alipay.oceanbase.jdbc.UrlParser;
import com.alipay.oceanbase.jdbc.internal.com.read.dao.Results;
import com.alipay.oceanbase.jdbc.internal.com.send.parameters.ParameterHolder;
import com.alipay.oceanbase.jdbc.internal.failover.FailoverProxy;
import com.alipay.oceanbase.jdbc.internal.util.ServerPrepareStatementCache;
import com.alipay.oceanbase.jdbc.internal.util.dao.ClientPrepareResult;
import com.alipay.oceanbase.jdbc.internal.util.dao.ServerPrepareResult;
import com.alipay.oceanbase.jdbc.OceanBaseStatement;
import com.alipay.oceanbase.jdbc.internal.io.input.PacketInputStream;
import com.alipay.oceanbase.jdbc.util.Options;

public interface Protocol {

    ServerPrepareResult prepare(String sql, boolean executeOnMaster) throws SQLException;

    boolean getAutocommit() throws SQLException;

    boolean noBackslashEscapes();

    void connect() throws SQLException;

    UrlParser getUrlParser();

    boolean inTransaction();

    boolean isOracleMode();

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

    boolean executeBatchClient(boolean mustExecuteOnMaster, Results results,
                               final ClientPrepareResult prepareResult,
                               final List<ParameterHolder[]> parametersList, boolean hasLongData)
                                                                                                 throws SQLException;

    void executeBatchStmt(boolean mustExecuteOnMaster, Results results, final List<String> queries)
                                                                                                   throws SQLException;

    void executePreparedQuery(boolean mustExecuteOnMaster, ServerPrepareResult serverPrepareResult,
                              Results results, ParameterHolder[] parameters) throws SQLException;

    boolean executeBatchServer(boolean mustExecuteOnMaster,
                               ServerPrepareResult serverPrepareResult, Results results,
                               String sql, List<ParameterHolder[]> parameterList,
                               boolean hasLongData) throws SQLException;

    void sendFechRowViaCursor(ServerPrepareResult serverPrepareResult, int cursorId, int fetchSize,
                              Results results) throws SQLException;

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

    void forceReleaseWaitingPrepareStatement() throws SQLException;

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

    void readEofPacket() throws SQLException, IOException;

    void skipEofPacket() throws SQLException, IOException;

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

    void sendFechRowViaCursor2(ServerPrepareResult serverPrepareResult, int cursorId,
                               int fetchSize, Results results) throws SQLException;
}
