package com.alipay.oceanbase.jdbc.internal.failover;

import java.lang.reflect.Method;
import java.net.SocketException;
import java.sql.SQLException;
import java.util.Set;

import com.alipay.oceanbase.jdbc.HostAddress;
import com.alipay.oceanbase.jdbc.OceanBaseConnection;
import com.alipay.oceanbase.jdbc.UrlParser;
import com.alipay.oceanbase.jdbc.internal.util.dao.ServerPrepareResult;
import com.alipay.oceanbase.jdbc.OceanBaseStatement;
import com.alipay.oceanbase.jdbc.internal.failover.tools.SearchFilter;
import com.alipay.oceanbase.jdbc.internal.protocol.Protocol;

public interface Listener {

    FailoverProxy getProxy();

    void setProxy(FailoverProxy proxy);

    void initializeConnection() throws SQLException;

    void preExecute() throws SQLException;

    void preClose();

    void preAbort();

    long getServerThreadId();

    void reconnectFailedConnection(SearchFilter filter) throws SQLException;

    void switchReadOnlyConnection(Boolean readonly) throws SQLException;

    HandleErrorResult primaryFail(Method method, Object[] args, boolean killCmd, boolean wasClosed)
                                                                                                   throws SQLException;

    Object invoke(Method method, Object[] args, Protocol specificProtocol) throws Throwable;

    Object invoke(Method method, Object[] args) throws Throwable;

    HandleErrorResult handleFailover(SQLException qe, Method method, Object[] args,
                                     Protocol protocol, boolean wasClosed) throws Throwable;

    void foundActiveMaster(Protocol protocol) throws SQLException;

    Set<HostAddress> getBlacklistKeys();

    void addToBlacklist(HostAddress hostAddress);

    void removeFromBlacklist(HostAddress hostAddress);

    void syncConnection(Protocol from, Protocol to) throws SQLException;

    UrlParser getUrlParser();

    void throwFailoverMessage(HostAddress failHostAddress, boolean wasMaster,
                              SQLException queryException, boolean reconnected) throws SQLException;

    boolean isAutoReconnect();

    int getRetriesAllDown();

    boolean isExplicitClosed();

    void reconnect() throws SQLException;

    boolean isReadOnly();

    boolean inTransaction();

    int getMajorServerVersion();

    boolean isMasterConnection();

    boolean isClosed();

    boolean versionGreaterOrEqual(int major, int minor, int patch);

    boolean isServerMariaDb();

    boolean sessionStateAware();

    boolean noBackslashEscapes();

    boolean isValid(int timeout) throws SQLException;

    void prolog(long maxRows, OceanBaseConnection connection, OceanBaseStatement statement)
                                                                                           throws SQLException;

    String getCatalog() throws SQLException;

    int getTimeout() throws SocketException;

    Protocol getCurrentProtocol();

    boolean hasHostFail();

    boolean canRetryFailLoop();

    SearchFilter getFilterForFailedHost();

    boolean isMasterConnected();

    boolean setMasterHostFail();

    boolean isMasterHostFail();

    long getLastQueryNanos();

    boolean checkMasterStatus(SearchFilter searchFilter);

    void rePrepareOnSlave(ServerPrepareResult oldServerPrepareResult, boolean mustExecuteOnMaster)
                                                                                                  throws SQLException;

    void reset() throws SQLException;
}
