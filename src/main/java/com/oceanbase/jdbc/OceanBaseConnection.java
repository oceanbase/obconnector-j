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

import java.net.SocketException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.oceanbase.jdbc.extend.datatype.ArrayImpl;
import com.oceanbase.jdbc.extend.datatype.ComplexDataType;
import com.oceanbase.jdbc.extend.datatype.StructImpl;
import com.oceanbase.jdbc.internal.logging.Logger;
import com.oceanbase.jdbc.internal.logging.LoggerFactory;
import com.oceanbase.jdbc.internal.protocol.Protocol;
import com.oceanbase.jdbc.internal.util.CallableStatementCache;
import com.oceanbase.jdbc.internal.util.ConnectionState;
import com.oceanbase.jdbc.internal.util.LRUCache;
import com.oceanbase.jdbc.internal.util.StringCacheUtil;
import com.oceanbase.jdbc.internal.util.Utils;
import com.oceanbase.jdbc.internal.util.dao.CallableStatementCacheKey;
import com.oceanbase.jdbc.internal.util.dao.CloneableCallableStatement;
import com.oceanbase.jdbc.internal.util.exceptions.ExceptionFactory;
import com.oceanbase.jdbc.internal.util.pool.GlobalStateInfo;
import com.oceanbase.jdbc.internal.util.pool.Pools;
import com.oceanbase.jdbc.util.Options;

@SuppressWarnings("Annotator")
public class OceanBaseConnection implements ConnectionImpl {

    private static final Logger      logger                              = LoggerFactory
                                                                             .getLogger(OceanBaseConnection.class);

    /**
     * Pattern to check the correctness of callable statement query string Legal queries, as
     * documented in JDK have the form: {[?=]call[(arg1,..,,argn)]}
     */
    public static final Pattern      CALLABLE_STATEMENT_PATTERN          = Pattern
                                                                             .compile(
                                                                                 "^(\\s*\\{)?\\s*((\\?\\s*=)?(\\s*\\/\\*([^\\*]|\\*[^\\/])*\\*\\/)*\\s*"
                                                                                         + "call(\\s*\\/\\*([^\\*]|\\*[^\\/])*\\*\\/)*\\s*((((`[^`]+`)|([^`\\}]+))\\.)?"
                                                                                         + "((`[^`]+`)|([^`\\}\\(]+)))\\s*(\\(.*\\))?(\\s*\\/\\*([^\\*]|\\*[^\\/])*\\*\\/)*"
                                                                                         + "\\s*(#.*)?)\\s*(\\}\\s*)?$",
                                                                                 Pattern.CASE_INSENSITIVE
                                                                                         | Pattern.DOTALL);
    /**
     * Check that query can be executed with PREPARE.
     */
    private static final Pattern     PREPARABLE_STATEMENT_PATTERN_ORACLE = Pattern
                                                                             .compile(
                                                                                 "^(\\s*\\/\\*([^\\*]|\\*[^\\/])*\\*\\/)*\\s*(SELECT|UPDATE|INSERT|DELETE|REPLACE|DO|CALL|DECLARE|SHOW)",
                                                                                 Pattern.CASE_INSENSITIVE);

    private static final Pattern     PREPARABLE_STATEMENT_PATTERN_MYSQL  = Pattern
                                                                             .compile(
                                                                                 "^(\\s*\\/\\*([^\\*]|\\*[^\\/])*\\*\\/)*\\s*(SELECT|UPDATE|INSERT|DELETE|REPLACE|DO|DECLARE|SHOW)",
                                                                                 Pattern.CASE_INSENSITIVE);

    public final ReentrantLock       lock;
    /**
     * the protocol to communicate with.
     */
    private final Protocol           protocol;
    /**
     * the properties for the client.
     */
    private final Options            options;

    public OceanBasePooledConnection pooledConnection;
    protected boolean                nullCatalogMeansCurrent;
    private CallableStatementCache   callableStatementCache;
    private volatile int             lowercaseTableNames                 = -1;
    private boolean                  canUseServerTimeout;
    private boolean                  sessionStateAware;
    private int                      stateFlag                           = 0;
    private int                      defaultTransactionIsolation         = 0;
    private ExceptionFactory         exceptionFactory;

    private boolean                  warningsCleared;

    private LRUCache                 serverSideStatementCheckCache;
    private LRUCache                 serverSideStatementCache;
    private LRUCache                 complexDataCache;
    private UrlParser                urlParser;
    private GlobalStateInfo          globalStateInfo;

    private TimeZone                 sessionTimeZone                     = TimeZone.getDefault();                  // FIXME what is this? for TIMESTAMPLTZ?
    private static final int         DEPTH_INDEX                         = 1;
    private static final int         PARENT_TYPE_INDEX                   = 3;
    private static final int         CHILD_TYPE_INDEX                    = 4;
    private static final int         ATTR_NO_INDEX                       = 5;
    private static final int         CHILD_OWNER_INDEX                   = 6;
    private static final int         ATTR_TYPE_INDEX                     = 7;
    private String                   origHostToConnectTo;
    // we don't want to be able to publicly clone this... // todo
    /**
     * Is this connection associated with a global tx?
     */
    private boolean                  isInGlobalTx                        = false;
    private boolean                  enableNetworkStatistics             = false;
    private int                      origPortToConnectTo;
    private String                   origDatabaseToConnectTo;
    private java.sql.Connection      complexConnection                   = null;
    private boolean                  autoCommit                          = true;
    private Map<String, Class<?>>    typeMap;
    private static final String      complexTypeSql                      = "SELECT * from (SELECT\n"
                                                                           + "  0 DEPTH,\n"
                                                                           + "  NULL PARENT_OWNER,\n"
                                                                           + "  NULL PARENT_TYPE,\n"
                                                                           + "  to_char(TYPE_NAME) CHILD_TYPE,\n"
                                                                           + "  0 ATTR_NO,\n"
                                                                           + "  SYS_CONTEXT('USERENV', 'CURRENT_USER') CHILD_TYPE_OWNER,\n"
                                                                           + "  A.TYPECODE ATTR_TYPE_CODE,\n"
                                                                           + "  NULL LENGTH,\n"
                                                                           + "  NULL NUMBER_PRECISION,\n"
                                                                           + "  NULL SCALE,\n"
                                                                           + "  NULL CHARACTER_SET_NAME\n"
                                                                           + "FROM\n"
                                                                           + "  USER_TYPES A WHERE TYPE_NAME = ?\n"
                                                                           + "UNION\n"
                                                                           + "(\n"
                                                                           + "WITH \n"
                                                                           + "CTE_RESULT(PARENT_OWNER, PARENT_TYPE, CHILD_TYPE, ATTR_NO, CHILD_TYPE_OWNER, ATTR_TYPE_CODE, LENGTH, NUMBER_PRECISION, SCALE, CHARACTER_SET_NAME) \n"
                                                                           + "AS (\n"
                                                                           + "    SELECT\n"
                                                                           + "      SYS_CONTEXT('USERENV','CURRENT_USER') PARENT_OWNER,\n"
                                                                           + "      B.TYPE_NAME PARENT_TYPE,\n"
                                                                           + "      B.ELEM_TYPE_NAME CHILD_TYPE,\n"
                                                                           + "      0 ATTR_NO,\n"
                                                                           + "      B.ELEM_TYPE_OWNER CHILD_TYPE_OWNER,\n"
                                                                           + "      NVL(A.TYPECODE, B.ELEM_TYPE_NAME) AS ATTR_TYPE_CODE,\n"
                                                                           + "      B.LENGTH LENGTH,\n"
                                                                           + "      B.NUMBER_PRECISION NUMBER_PRECISION,\n"
                                                                           + "      B.SCALE SCALE,\n"
                                                                           + "      B.CHARACTER_SET_NAME CHARACTER_SET_NAME\n"
                                                                           + "    FROM\n"
                                                                           + "      USER_COLL_TYPES B LEFT JOIN USER_TYPES A ON A.TYPE_NAME = B.ELEM_TYPE_NAME\n"
                                                                           + "    UNION\n"
                                                                           + "    SELECT\n"
                                                                           + "      SYS_CONTEXT('USERENV','CURRENT_USER') PARENT_OWNER,\n"
                                                                           + "      B.TYPE_NAME PARENT_TYPE,\n"
                                                                           + "      B.ATTR_TYPE_NAME CHILD_TYPE,\n"
                                                                           + "      B.ATTR_NO ATTR_NO,\n"
                                                                           + "      B.ATTR_TYPE_OWNER CHILD_TYPE_OWNER,\n"
                                                                           + "      NVL(A.TYPECODE, B.ATTR_TYPE_NAME) AS ATTR_TYPE_CODE,\n"
                                                                           + "      B.LENGTH LENGTH,\n"
                                                                           + "      B.NUMBER_PRECISION NUMBER_PRECISION,\n"
                                                                           + "      B.SCALE SCALE,\n"
                                                                           + "      B.CHARACTER_SET_NAME CHARACTER_SET_NAME\n"
                                                                           + "    FROM USER_TYPE_ATTRS B LEFT JOIN USER_TYPES A ON B.ATTR_TYPE_NAME = A.TYPE_NAME ORDER BY ATTR_NO\n"
                                                                           + ") ,\n"
                                                                           + "CTE(DEPTH, PARENT_OWNER, PARENT_TYPE, CHILD_TYPE, ATTR_NO, CHILD_TYPE_OWNER, ATTR_TYPE_CODE, LENGTH, NUMBER_PRECISION, SCALE, CHARACTER_SET_NAME)\n"
                                                                           + "AS (\n"
                                                                           + "  SELECT\n"
                                                                           + "    1 DEPTH,\n"
                                                                           + "    PARENT_OWNER,\n"
                                                                           + "    PARENT_TYPE,\n"
                                                                           + "    CHILD_TYPE,\n"
                                                                           + "    ATTR_NO,\n"
                                                                           + "    CHILD_TYPE_OWNER,\n"
                                                                           + "    ATTR_TYPE_CODE,\n"
                                                                           + "    LENGTH,\n"
                                                                           + "    NUMBER_PRECISION,\n"
                                                                           + "    SCALE, CHARACTER_SET_NAME\n"
                                                                           + "  FROM CTE_RESULT WHERE PARENT_TYPE = ?\n"
                                                                           + "  UNION ALL\n"
                                                                           + "  SELECT\n"
                                                                           + "    DEPTH + 1 DEPTH,\n"
                                                                           + "    CTE_RESULT.PARENT_OWNER,\n"
                                                                           + "    CTE_RESULT.PARENT_TYPE,\n"
                                                                           + "    CTE_RESULT.CHILD_TYPE,\n"
                                                                           + "    CTE_RESULT.ATTR_NO,\n"
                                                                           + "    CTE_RESULT.CHILD_TYPE_OWNER,\n"
                                                                           + "    CTE_RESULT.ATTR_TYPE_CODE,\n"
                                                                           + "    CTE_RESULT.LENGTH,\n"
                                                                           + "    CTE_RESULT.NUMBER_PRECISION,\n"
                                                                           + "    CTE_RESULT.SCALE,\n"
                                                                           + "    CTE_RESULT.CHARACTER_SET_NAME\n"
                                                                           + "  FROM CTE_RESULT INNER JOIN CTE ON CTE_RESULT.PARENT_TYPE = CTE.CHILD_TYPE\n"
                                                                           + ")\n"
                                                                           + "SELECT * FROM CTE\n"
                                                                           + ") ) ORDER BY DEPTH;";
    private Map<String, Integer>     indexMap;
    private int                      isolationLevel                      = 2;
    private static ReentrantLock     threadLock                          = new ReentrantLock();
    private static Thread            lbThread                            = null;
    private boolean                  remarksReporting                    = false;
    protected static final Logger lockLogger = LoggerFactory.getLogger("JDBC-COST-LOGGER");

    /**
     * Creates a new connection with a given protocol and query factory.
     *
     * @param protocol the protocol to use.
     */
    public OceanBaseConnection(Protocol protocol) {
        this.protocol = protocol;
        options = protocol.getOptions();
        canUseServerTimeout = protocol.versionGreaterOrEqual(10, 1, 2);
        sessionStateAware = protocol.sessionStateAware();
        nullCatalogMeansCurrent = options.nullCatalogMeansCurrent;
        if (options.cacheCallableStmts) {
            callableStatementCache = CallableStatementCache
                .newInstance(options.callableStmtCacheSize);
        }
        this.lock = protocol.getLock();
        this.exceptionFactory = ExceptionFactory.of(this.getServerThreadId(), this.options);
        //        if (getCacheComplexData()) {
        //            this.complexDataCache = new LRUCache(getComplexDataCacheSize());
        //        }
        //todo add connection propertis in the future
        this.complexDataCache = new LRUCache(50);
        this.urlParser = protocol.getUrlParser();
    }

    public OceanBaseConnection(UrlParser urlParser, GlobalStateInfo globalInfo, boolean flag)
                                                                                             throws SQLException {
        threadLock.lock();
        try {
            lockLogger.debug("OceanBaseConnection.OceanBaseConnection locked");

            Protocol protocol = Utils.retrieveProxy(urlParser, globalInfo);
            this.urlParser = urlParser;
            this.globalStateInfo = globalInfo;
            this.protocol = protocol;
            options = protocol.getOptions();
            canUseServerTimeout = protocol.versionGreaterOrEqual(10, 1, 2);
            sessionStateAware = protocol.sessionStateAware();
            nullCatalogMeansCurrent = options.nullCatalogMeansCurrent;
            if (options.cacheCallableStmts) {
                callableStatementCache = CallableStatementCache
                    .newInstance(options.callableStmtCacheSize);
            }
            this.lock = protocol.getLock();
            this.exceptionFactory = ExceptionFactory.of(this.getServerThreadId(), this.options);
            //        if (getCacheComplexData()) {
            //            this.complexDataCache = new LRUCache(getComplexDataCacheSize());
            //        }
            //todo add connection propertis in the future
            this.complexDataCache = new LRUCache(50);
        } finally {
            threadLock.unlock();
            lockLogger.debug("OceanBaseConnection.OceanBaseConnection unlocked");
        }
    }

    /**
     * Create new connection Object.
     *
     * @param urlParser  parser
     * @param globalInfo global info
     * @return connection object
     * @throws SQLException if any connection error occur
     */
    public static OceanBaseConnection newConnection(UrlParser urlParser, GlobalStateInfo globalInfo)
                                                                                                    throws SQLException {
        if (urlParser.getOptions().pool) {
            return Pools.retrievePool(urlParser).getConnection();
        }
        Protocol protocol = Utils.retrieveProxy(urlParser, globalInfo);
        OceanBaseConnection conn = new OceanBaseConnection(protocol);
        return conn;
    }

    public static String quoteIdentifier(String string) {
        return "`" + string.replaceAll("`", "``") + "`";
    }

    /**
     * UnQuote string.
     *
     * @param string value
     * @return unquote string
     * @deprecated since 1.3.0
     */
    @Deprecated
    public static String unquoteIdentifier(String string) {
        if (string != null && string.startsWith("`") && string.endsWith("`")
            && string.length() >= 2) {
            return string.substring(1, string.length() - 1).replace("``", "`");
        }
        return string;
    }

    public Protocol getProtocol() {
        return protocol;
    }

    /**
     * creates a new statement.
     *
     * @return a statement
     * @throws SQLException if we cannot create the statement.
     */
    public Statement createStatement() throws SQLException {
        checkConnection();
        return new OceanBaseStatement((OceanBaseConnection) this, ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY, exceptionFactory);
    }

    /**
     * Creates a <code>Statement</code> object that will generate <code>ResultSet</code> objects with
     * the given type and concurrency. This method is the same as the <code>createStatement</code>
     * method above, but it allows the default result set type and concurrency to be overridden. The
     * holdability of the created result sets can be determined by calling {@link #getHoldability}.
     *
     * @param resultSetType        a result set type; one of <code>ResultSet.TYPE_FORWARD_ONLY</code>, <code>
     *                             ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @param resultSetConcurrency a concurrency type; one of <code>ResultSet.CONCUR_READ_ONLY</code>
     *                             or <code>ResultSet.CONCUR_UPDATABLE</code>
     * @return a new <code>Statement</code> object that will generate <code>ResultSet</code> objects
     * with the given type and concurrency
     */
    public Statement createStatement(final int resultSetType, final int resultSetConcurrency) {
        return new OceanBaseStatement((OceanBaseConnection) this, resultSetType,
            resultSetConcurrency, exceptionFactory);
    }

    /**
     * Creates a <code>Statement</code> object that will generate <code>ResultSet</code> objects with
     * the given type, concurrency, and holdability. This method is the same as the <code>
     * createStatement</code> method above, but it allows the default result set type, concurrency,
     * and holdability to be overridden.
     *
     * @param resultSetType        one of the following <code>ResultSet</code> constants: <code>
     *                             ResultSet.TYPE_FORWARD_ONLY</code>, <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or
     *                             <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @param resultSetConcurrency one of the following <code>ResultSet</code> constants: <code>
     *                             ResultSet.CONCUR_READ_ONLY</code> or <code>ResultSet.CONCUR_UPDATABLE</code>
     * @param resultSetHoldability one of the following <code>ResultSet</code> constants: <code>
     *                             ResultSet.HOLD_CURSORS_OVER_COMMIT</code> or <code>ResultSet.CLOSE_CURSORS_AT_COMMIT</code>
     * @return a new <code>Statement</code> object that will generate <code>ResultSet</code> objects
     * with the given type, concurrency, and holdability
     * @see ResultSet
     */
    public Statement createStatement(final int resultSetType, final int resultSetConcurrency,
                                     final int resultSetHoldability) {
        return new OceanBaseStatement((OceanBaseConnection) this, resultSetType,
            resultSetConcurrency, exceptionFactory);
    }

    private void checkConnection() throws SQLException {
        if (protocol.isExplicitClosed()) {
            throw exceptionFactory.create("createStatement() is called on closed connection",
                "08000");
        }
        if (protocol.isClosed() && protocol.getProxy() != null) {
            lock.lock();
            try {
                lockLogger.debug("OceanBaseConnection.checkConnection locked");
                protocol.getProxy().reconnect();
            } finally {
                lock.unlock();
                lockLogger.debug("OceanBaseConnection.checkConnection unlocked");
            }
        }
    }

    /**
     * Create a new client prepared statement.
     *
     * @param sql the query.
     * @return a client prepared statement.
     * @throws SQLException if there is a problem preparing the statement.
     */
    public ClientSidePreparedStatement clientPrepareStatement(final String sql) throws SQLException {
        return clientPrepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
    }

    public ClientSidePreparedStatement clientPrepareStatement(final String sql, int autoGeneratedKeys) throws SQLException {
        return new ClientSidePreparedStatement(this, sql, ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY, autoGeneratedKeys, exceptionFactory);
    }

    /**
     * Create a new server prepared statement.
     *
     * @param sql the query.
     * @return a server prepared statement.
     * @throws SQLException if there is a problem preparing the statement.
     */
    public ServerSidePreparedStatement serverPrepareStatement(final String sql) throws SQLException {
        return new ServerSidePreparedStatement(false, (OceanBaseConnection) this, sql,
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
            Statement.RETURN_GENERATED_KEYS, exceptionFactory);
    }

    /**
     * creates a new prepared statement.
     *
     * @param sql the query.
     * @return a prepared statement.
     * @throws SQLException if there is a problem preparing the statement.
     */
    public PreparedStatement prepareStatement(final String sql) throws SQLException {

        return internalPrepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY, Statement.NO_GENERATED_KEYS);
    }

    /**
     * Creates a <code>PreparedStatement</code> object that will generate <code>ResultSet</code>
     * objects with the given type and concurrency. This method is the same as the <code>
     * prepareStatement</code> method above, but it allows the default result set type and concurrency
     * to be overridden. The holdability of the created result sets can be determined by calling
     * {@link #getHoldability}.
     *
     * @param sql                  a <code>String</code> object that is the SQL statement to be sent to the database;
     *                             may contain one or more '?' IN parameters
     * @param resultSetType        a result set type; one of <code>ResultSet.TYPE_FORWARD_ONLY</code>, <code>
     *                             ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @param resultSetConcurrency a concurrency type; one of <code>ResultSet.CONCUR_READ_ONLY</code>
     *                             or <code>ResultSet.CONCUR_UPDATABLE</code>
     * @return a new PreparedStatement object containing the pre-compiled SQL statement that will
     * produce <code>ResultSet</code> objects with the given type and concurrency
     * @throws SQLException if a database access error occurs, this method is called on a closed
     *                      connection or the given parameters are not<code>ResultSet</code> constants indicating type
     *                      and concurrency
     */
    public PreparedStatement prepareStatement(final String sql, final int resultSetType,
                                              final int resultSetConcurrency) throws SQLException {
        return internalPrepareStatement(sql, resultSetType, resultSetConcurrency,
            Statement.NO_GENERATED_KEYS);
    }

    /**
     * Creates a <code>PreparedStatement</code> object that will generate <code>ResultSet</code>
     * objects with the given type, concurrency, and holdability.
     *
     * <p>This method is the same as the <code>prepareStatement</code> method above, but it allows the
     * default result set type, concurrency, and holdability to be overridden.
     *
     * @param sql                  a <code>String</code> object that is the SQL statement to be sent to the database;
     *                             may contain one or more '?' IN parameters
     * @param resultSetType        one of the following <code>ResultSet</code> constants: <code>
     *                             ResultSet.TYPE_FORWARD_ONLY</code>, <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or
     *                             <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @param resultSetConcurrency one of the following <code>ResultSet</code> constants: <code>
     *                             ResultSet.CONCUR_READ_ONLY</code> or <code>ResultSet.CONCUR_UPDATABLE</code>
     * @param resultSetHoldability one of the following <code>ResultSet</code> constants: <code>
     *                             ResultSet.HOLD_CURSORS_OVER_COMMIT</code> or <code>ResultSet.CLOSE_CURSORS_AT_COMMIT</code>
     * @return a new <code>PreparedStatement</code> object, containing the pre-compiled SQL statement,
     * that will generate <code>ResultSet</code> objects with the given type, concurrency, and
     * holdability
     * @throws SQLException if a database access error occurs, this method is called on a closed
     *                      connection or the given parameters are not <code>ResultSet</code> constants indicating
     *                      type, concurrency, and holdability
     * @see ResultSet
     */
    public PreparedStatement prepareStatement(final String sql, final int resultSetType,
                                              final int resultSetConcurrency,
                                              final int resultSetHoldability) throws SQLException {
        return internalPrepareStatement(sql, resultSetType, resultSetConcurrency,
            Statement.NO_GENERATED_KEYS);
    }

    /**
     * Creates a default <code>PreparedStatement</code> object that has the capability to retrieve
     * auto-generated keys. The given constant tells the driver whether it should make auto-generated
     * keys available for retrieval. This parameter is ignored if the SQL statement is not an <code>
     * INSERT</code> statement, or an SQL statement able to return auto-generated keys (the list of
     * such statements is vendor-specific).
     *
     * <p><B>Note:</B> This method is optimized for handling parametric SQL statements that benefit
     * from precompilation. If the driver supports precompilation, the method <code>prepareStatement
     * </code> will send the statement to the database for precompilation. Some drivers may not
     * support precompilation. In this case, the statement may not be sent to the database until the
     * <code>PreparedStatement</code> object is executed. This has no direct effect on users; however,
     * it does affect which methods throw certain SQLExceptions.
     *
     * <p>Result sets created using the returned <code>PreparedStatement</code> object will by default
     * be type <code>TYPE_FORWARD_ONLY</code> and have a concurrency level of <code>CONCUR_READ_ONLY
     * </code>. The holdability of the created result sets can be determined by calling {@link
     * #getHoldability}.
     *
     * @param sql               an SQL statement that may contain one or more '?' IN parameter placeholders
     * @param autoGeneratedKeys a flag indicating whether auto-generated keys should be returned; one
     *                          of <code>Statement.RETURN_GENERATED_KEYS</code> or <code>Statement.NO_GENERATED_KEYS</code>
     * @return a new <code>PreparedStatement</code> object, containing the pre-compiled SQL statement,
     * that will have the capability of returning auto-generated keys
     * @throws SQLException if a database access error occurs, this method is called on a closed
     *                      connection or the given parameter is not a <code>Statement</code> constant indicating
     *                      whether auto-generated keys should be returned
     */
    public PreparedStatement prepareStatement(final String sql, final int autoGeneratedKeys)
                                                                                            throws SQLException {
        return internalPrepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY, autoGeneratedKeys);
    }

    /**
     * Creates a default <code>PreparedStatement</code> object capable of returning the auto-generated
     * keys designated by the given array. This array contains the indexes of the columns in the
     * target table that contain the auto-generated keys that should be made available. The driver
     * will ignore the array if the SQL statement is not an <code>INSERT</code> statement, or an SQL
     * statement able to return auto-generated keys (the list of such statements is vendor-specific).
     *
     * <p>An SQL statement with or without IN parameters can be pre-compiled and stored in a <code>
     * PreparedStatement</code> object. This object can then be used to efficiently execute this
     * statement multiple times.
     *
     * <p><B>Note:</B> This method is optimized for handling parametric SQL statements that benefit
     * from precompilation. If the driver supports precompilation, the method <code>prepareStatement
     * </code> will send the statement to the database for precompilation. Some drivers may not
     * support precompilation. In this case, the statement may not be sent to the database until the
     * <code>PreparedStatement</code> object is executed. This has no direct effect on users; however,
     * it does affect which methods throw certain SQLExceptions.
     *
     * <p>Result sets created using the returned <code>PreparedStatement</code> object will by default
     * be type <code>TYPE_FORWARD_ONLY</code> and have a concurrency level of <code>CONCUR_READ_ONLY
     * </code>. The holdability of the created result sets can be determined by calling {@link
     * #getHoldability}.
     *
     * @param sql           an SQL statement that may contain one or more '?' IN parameter placeholders
     * @param columnIndexes an array of column indexes indicating the columns that should be returned
     *                      from the inserted row or rows
     * @return a new <code>PreparedStatement</code> object, containing the pre-compiled statement,
     * that is capable of returning the auto-generated keys designated by the given array of
     * column indexes
     * @throws SQLException if a database access error occurs or this method is called on a closed
     *                      connection
     */
    public PreparedStatement prepareStatement(final String sql, final int[] columnIndexes)
                                                                                          throws SQLException {
        if (!protocol.isOracleMode()) {
            return prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
        }

        AutoGeneratedKey autoKey = new AutoGeneratedKey(sql, columnIndexes, protocol);
        if (!autoKey.isInsertSql()) {
            return prepareStatement(sql);
        }

        PreparedStatement ps = prepareStatement(autoKey.getNewSqlByIndex(),
            Statement.RETURN_GENERATED_KEYS);
        try {
            ((JDBC4ServerPreparedStatement) ps).registerReturnAutoGeneratedKey(autoKey);
        } catch (ClassCastException e) {
            if (e.getMessage().contains("com.oceanbase.jdbc.ClientSidePreparedStatement cannot be cast to com.oceanbase.jdbc.JDBC4ServerPreparedStatement")) {
                throw new SQLException("Add JDBC URL option \"useOraclePrepareExecute=true\" to create a default PreparedStatement object capable of returning the auto-generated keys designated by the given array");
            }
        }
        return ps;
    }

    /**
     * Creates a default <code>PreparedStatement</code> object capable of returning the auto-generated
     * keys designated by the given array. This array contains the names of the columns in the target
     * table that contain the auto-generated keys that should be returned. The driver will ignore the
     * array if the SQL statement is not an <code>INSERT</code> statement, or an SQL statement able to
     * return auto-generated keys (the list of such statements is vendor-specific).
     *
     * <p>An SQL statement with or without IN parameters can be pre-compiled and stored in a <code>
     * PreparedStatement</code> object. This object can then be used to efficiently execute this
     * statement multiple times.
     *
     * <p><B>Note:</B> This method is optimized for handling parametric SQL statements that benefit
     * from precompilation. If the driver supports precompilation, the method <code>prepareStatement
     * </code> will send the statement to the database for precompilation. Some drivers may not
     * support precompilation. In this case, the statement may not be sent to the database until the
     * <code>PreparedStatement</code> object is executed. This has no direct effect on users; however,
     * it does affect which methods throw certain SQLExceptions.
     *
     * <p>Result sets created using the returned <code>PreparedStatement</code> object will by default
     * be type <code>TYPE_FORWARD_ONLY</code> and have a concurrency level of <code>CONCUR_READ_ONLY
     * </code>. The holdability of the created result sets can be determined by calling {@link
     * #getHoldability}.
     *
     * @param sql         an SQL statement that may contain one or more '?' IN parameter placeholders
     * @param columnNames an array of column names indicating the columns that should be returned from
     *                    the inserted row or rows
     * @return a new <code>PreparedStatement</code> object, containing the pre-compiled statement,
     * that is capable of returning the auto-generated keys designated by the given array of
     * column names
     * @throws SQLException if a database access error occurs or this method is called on a closed
     *                      connection
     */
    public PreparedStatement prepareStatement(final String sql, final String[] columnNames)
                                                                                           throws SQLException {
        if (!protocol.isOracleMode()) {
            return prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
        }

        AutoGeneratedKey autoKey = new AutoGeneratedKey(sql, columnNames, protocol);
        if (!autoKey.isInsertSql()) {
            return prepareStatement(sql);
        }

        PreparedStatement ps = prepareStatement(autoKey.getNewSqlByName(),
            Statement.RETURN_GENERATED_KEYS);
        try {
            ((JDBC4ServerPreparedStatement) ps).registerReturnAutoGeneratedKey(autoKey);
        } catch (ClassCastException e) {
            if (e.getMessage().contains("com.oceanbase.jdbc.ClientSidePreparedStatement cannot be cast to com.oceanbase.jdbc.JDBC4ServerPreparedStatement")) {
                throw new SQLException("Add JDBC URL option \"useOraclePrepareExecute=true\" to create a default PreparedStatement object capable of returning the auto-generated keys designated by the given array");
            }
        }

        return ps;
    }

    private String getCachedSql(String originSql) {
        if (StringCacheUtil.sqlStringCache == null) {
            return originSql;
        }
        String sqlCache = (String) StringCacheUtil.sqlStringCache.get(originSql);
        if (sqlCache == null) {
            StringCacheUtil.sqlStringCache.put(originSql, originSql);
            sqlCache = originSql;
        }
        return sqlCache;
    }

    /**
     * Send ServerPrepareStatement or ClientPrepareStatement depending on SQL query and options If
     * server side and PREPARE can be delayed, a facade will be return, to have a fallback on client
     * prepareStatement.
     *
     * @param sql                  sql query
     * @param resultSetScrollType  one of the following <code>ResultSet</code> constants: <code>
     *                             ResultSet.TYPE_FORWARD_ONLY</code>, <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or
     *                             <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @param resultSetConcurrency a concurrency type; one of <code>ResultSet.CONCUR_READ_ONLY</code>
     *                             or <code>ResultSet.CONCUR_UPDATABLE</code>
     * @param autoGeneratedKeys    a flag indicating whether auto-generated keys should be returned; one
     *                             of <code>Statement.RETURN_GENERATED_KEYS</code> or <code>Statement.NO_GENERATED_KEYS</code>
     * @return PrepareStatement
     * @throws SQLException if a connection error occur during the server preparation.
     */
    private PreparedStatement internalPrepareStatement(final String sql,
                                                       final int resultSetScrollType,
                                                       final int resultSetConcurrency,
                                                       final int autoGeneratedKeys)
                                                                                   throws SQLException {

        if (sql != null) {
            String sqlQuery = Utils.nativeSql(sql, protocol);
            if (options.useSqlStringCache) {
                sqlQuery = getCachedSql(sqlQuery);
            }
            // Oracle mode don't determine sql string, so that anonymous blocks can be executed through PS . Maybe ORACLE_PREPARABLE_STATEMENT_PATTERN  should be added (todo).
            if (options.useServerPrepStmts && (this.protocol.isOracleMode() || PREPARABLE_STATEMENT_PATTERN_MYSQL.matcher(sqlQuery).find())) {
                // prepare isn't delayed -> if prepare fail, fallback to client preparedStatement?
                checkConnection();
                try {
                    if (this.protocol.isOracleMode() && options.supportNameBinding) {
                        sqlQuery = Utils.trimSQLString(sqlQuery, protocol.noBackslashEscapes(),
                            protocol.isOracleMode(), true);
                    }
                    ServerSidePreparedStatement ret = new ServerSidePreparedStatement(false,
                        (OceanBaseConnection) this, sqlQuery, resultSetScrollType,
                        resultSetConcurrency, autoGeneratedKeys, exceptionFactory);
                    return ret;
                } catch (SQLNonTransientConnectionException e) {
                    throw e;
                } catch (SQLException e) {
                    // on some specific case, server cannot prepared data (CONJ-238)
                    // will use clientPreparedStatement
                    if (!options.emulateUnsupportedPstmts) {
                        throw e;
                    }
                }
            }
            if (this.protocol.isOracleMode() && options.supportNameBinding) {
                sqlQuery = Utils.trimSQLString(sqlQuery, protocol.noBackslashEscapes(),
                    protocol.isOracleMode(), true);
            }
            ClientSidePreparedStatement ret = new ClientSidePreparedStatement(
                (OceanBaseConnection) this, sqlQuery, resultSetScrollType, resultSetConcurrency,
                autoGeneratedKeys, exceptionFactory);
            return ret;
        } else {
            throw new SQLException("SQL value can not be NULL");
        }
    }

    /**
     * Creates a <code>CallableStatement</code> object for calling database stored procedures. The
     * <code>CallableStatement</code> object provides methods for setting up its IN and OUT
     * parameters, and methods for executing the call to a stored procedure. example : {?= call
     * &lt;procedure-name&gt;[(&lt;arg1&gt;,&lt;arg2&gt;, ...)]} or {call
     * &lt;procedure-name&gt;[(&lt;arg1&gt;,&lt;arg2&gt;, ...)]}
     *
     * <p><b>Note:</b> This method is optimized for handling stored procedure call statements.
     *
     * @param sql an SQL statement that may contain one or more '?' parameter placeholders. Typically
     *            this statement is specified using JDBC call escape syntax.
     * @return a new default <code>CallableStatement</code> object containing the pre-compiled SQL
     * statement
     * @throws SQLException if a database access error occurs or this method is called on a closed
     *                      connection
     */
    public CallableStatement prepareCall(final String sql) throws SQLException {
        return prepareCall(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    /**
     * Creates a <code>CallableStatement</code> object that will generate <code>ResultSet</code>
     * objects with the given type and concurrency. This method is the same as the <code>prepareCall
     * </code> method above, but it allows the default result set type and concurrency to be
     * overridden. The holdability of the created result sets can be determined by calling {@link
     * #getHoldability}.
     *
     * @param sql                  a <code>String</code> object that is the SQL statement to be sent to the database;
     *                             may contain on or more '?' parameters
     * @param resultSetType        a result set type; one of <code>ResultSet.TYPE_FORWARD_ONLY</code>, <code>
     *                             ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @param resultSetConcurrency a concurrency type; one of <code>ResultSet.CONCUR_READ_ONLY</code>
     *                             or <code>ResultSet.CONCUR_UPDATABLE</code>
     * @return a new <code>CallableStatement</code> object containing the pre-compiled SQL statement
     * that will produce <code>ResultSet</code> objects with the given type and concurrency
     * @throws SQLException if a database access error occurs, this method is called on a closed
     *                      connection or the given parameters are not <code>ResultSet</code> constants indicating type
     *                      and concurrency
     */
    public CallableStatement prepareCall(final String sql, final int resultSetType,
                                         final int resultSetConcurrency) throws SQLException {
        checkConnection();
        // Support for Oracle anonymous block

        String querySetToServer = sql;
        Matcher matcher = CALLABLE_STATEMENT_PATTERN.matcher(querySetToServer);
        if (this.protocol.isOracleMode() && !matcher.matches()) { // oracle mode prepare sql by prepareCall
            if (options.supportNameBinding) {
                querySetToServer = Utils.trimSQLString(sql, protocol.noBackslashEscapes(),
                    protocol.isOracleMode(), true);
            }
            return new OceanBaseProcedureStatement(false, querySetToServer,
                (OceanBaseConnection) this, "", null, null, resultSetType, resultSetConcurrency,
                exceptionFactory, true);
        } else {

            if (!matcher.matches()) {
                throw new SQLSyntaxErrorException(
                    "invalid callable syntax. must be like {[?=]call <procedure/function name>[(?,?, ...)]}\n but was : "
                            + sql);
            }
            querySetToServer = matcher.group(2);
            boolean isFunction = (matcher.group(3) != null);
            String databaseAndProcedure = matcher.group(8) == null ? null : matcher.group(8).trim();
            if (databaseAndProcedure != null && databaseAndProcedure.startsWith("?")) {
                isFunction = true;
            }
            String database = matcher.group(10) == null ? null : matcher.group(10).trim();
            String procedureName = matcher.group(13) == null ? null : matcher.group(13).trim();
            String arguments = matcher.group(16) == null ? null : matcher.group(16).trim();
            if (database == null && sessionStateAware) {
                database = protocol.getDatabase();
            }
            if (database != null && options.cacheCallableStmts) {
                CallableStatementCacheKey cacheKey = new CallableStatementCacheKey(database,
                    querySetToServer);
                if (callableStatementCache.containsKey(cacheKey)) {
                    try {
                        CallableStatement callableStatement = callableStatementCache.get(cacheKey);
                        if (callableStatement != null) {
                            // Clone to avoid side effect like having some open resultSet.
                            return ((CloneableCallableStatement) callableStatement).clone(this);
                        }
                    } catch (CloneNotSupportedException cloneNotSupportedException) {
                        cloneNotSupportedException.printStackTrace();
                    }
                }
                // Convert the arguments string to normal format
                if (this.protocol.isOracleMode() && options.supportNameBinding && isFunction
                    && arguments != null) {
                    arguments = Utils.trimSQLString(arguments, protocol.noBackslashEscapes(),
                        protocol.isOracleMode(), false); // arguments do not need comments
                }
                CallableStatement callableStatement = createNewCallableStatement(querySetToServer,
                    procedureName, isFunction, databaseAndProcedure, database, arguments,
                    resultSetType, resultSetConcurrency, exceptionFactory);
                callableStatementCache.put(cacheKey, callableStatement);
                return callableStatement;
            }
            return createNewCallableStatement(querySetToServer, procedureName, isFunction,
                databaseAndProcedure, database, arguments, resultSetType, resultSetConcurrency,
                exceptionFactory);
        }
    }

    /**
     * Creates a <code>CallableStatement</code> object that will generate <code>ResultSet</code>
     * objects with the given type and concurrency. This method is the same as the <code>prepareCall
     * </code> method above, but it allows the default result set type, result set concurrency type
     * and holdability to be overridden.
     *
     * @param sql                  a <code>String</code> object that is the SQL statement to be sent to the database;
     *                             may contain on or more '?' parameters
     * @param resultSetType        one of the following <code>ResultSet</code> constants: <code>
     *                             ResultSet.TYPE_FORWARD_ONLY</code>, <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or
     *                             <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @param resultSetConcurrency one of the following <code>ResultSet</code> constants: <code>
     *                             ResultSet.CONCUR_READ_ONLY</code> or <code>ResultSet.CONCUR_UPDATABLE</code>
     * @param resultSetHoldability one of the following <code>ResultSet</code> constants: <code>
     *                             ResultSet.HOLD_CURSORS_OVER_COMMIT</code> or <code>ResultSet.CLOSE_CURSORS_AT_COMMIT</code>
     * @return a new <code>CallableStatement</code> object, containing the pre-compiled SQL statement,
     * that will generate <code>ResultSet</code> objects with the given type, concurrency, and
     * holdability
     * @throws SQLException if a database access error occurs, this method is called on a closed
     *                      connection or the given parameters are not <code>ResultSet</code> constants indicating
     *                      type, concurrency, and holdability
     * @see ResultSet
     */
    public CallableStatement prepareCall(final String sql, final int resultSetType,
                                         final int resultSetConcurrency,
                                         final int resultSetHoldability) throws SQLException {
        return prepareCall(sql);
    }

    private CallableStatement createNewCallableStatement(String query, String procedureName,
                                                         boolean isFunction,
                                                         String databaseAndProcedure,
                                                         String database, String arguments,
                                                         int resultSetType,
                                                         final int resultSetConcurrency,
                                                         ExceptionFactory exceptionFactory)
                                                                                           throws SQLException {
        if (isFunction) {
            if (!this.getProtocol().isOracleMode()) {
                return new OceanBaseFunctionStatement((OceanBaseConnection) this, database,
                    databaseAndProcedure, (arguments == null) ? "()" : arguments, resultSetType,
                    resultSetConcurrency, exceptionFactory);
            } else {
                String actualQuery;
                if (databaseAndProcedure.startsWith("?")) {
                    if (database.startsWith("?")){
                        database = correctDatabaseAndProcedureName(database);
                    } else if (procedureName.startsWith("?")){
                        procedureName = correctDatabaseAndProcedureName(procedureName);
                    }
                    actualQuery = "BEGIN " + databaseAndProcedure + ((arguments == null) ? "()" : arguments) + ";END;";
                } else {
                    actualQuery = "BEGIN ?:=" + databaseAndProcedure + ((arguments == null) ? "()" : arguments) + ";END;";
                }
                return new OceanBaseProcedureStatement(true, actualQuery,
                    (OceanBaseConnection) this, procedureName, database, arguments, resultSetType,
                    resultSetConcurrency, exceptionFactory);
            }
        } else {
            if (databaseAndProcedure != null && arguments == null) { // for call procname,add () to the end
                String callableSql = "call " + databaseAndProcedure + "()";
                return new OceanBaseProcedureStatement(false, callableSql,
                    (OceanBaseConnection) this, procedureName, database, arguments, resultSetType,
                    resultSetConcurrency, exceptionFactory);
            } else {
                return new OceanBaseProcedureStatement(false, query, (OceanBaseConnection) this,
                    procedureName, database, arguments, resultSetType, resultSetConcurrency,
                    exceptionFactory);
            }
        }
    }

    private String correctDatabaseAndProcedureName(String name) throws SQLException {
        boolean isColonFound = false;
        int index = 1;
        // skip '?', skip n ' ', skip ":= ", then append
        while (index < name.length()) {
            char currentChar = name.charAt(index);
            if (!isColonFound) {
                if (Character.isWhitespace(currentChar)) {
                    index++;
                } else if (currentChar == ':' && name.charAt(index + 1) == '=' && name.charAt(index + 2) == ' ') {
                    isColonFound = true;
                    index += 3;
                } else {
                    throw new SQLException("invalid syntax");
                }
            } else if (Character.isWhitespace(currentChar)) {
                index++;
            } else {
                break;
            }
        }
        return name.substring(index);
    }

    @Override
    public String nativeSQL(final String sql) throws SQLException {
        return Utils.nativeSql(sql, protocol);
    }

    /**
     * returns true if statements on this connection are auto commited.
     *
     * @return true if auto commit is on.
     * @throws SQLException if there is an error
     */
    public boolean getAutoCommit() throws SQLException {
        return protocol.getAutocommit();
    }

    /**
     * Sets whether this connection is auto commited.
     *
     * @param autoCommit if it should be auto commited.
     * @throws SQLException if something goes wrong talking to the server.
     */
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if (protocol != null) {
            protocol.startCallInterface();
        }
        this.protocol.setAutoCommit(autoCommit);
        if (protocol != null) {
            protocol.endCallInterface("OceanBaseConnection.setAutoCommit");
        }
    }

    /**
     * Sends commit to the server.
     *
     * @throws SQLException if there is an error commiting.
     */
    public void commit() throws SQLException {
        if (protocol != null) {
            protocol.startCallInterface();
        }
        lock.lock();
        try {
            lockLogger.debug("OceanBaseConnection.commit locked");

            if (protocol.inTransaction()) {
                try (Statement st = createStatement()) {
                    st.execute("COMMIT");
                }
            }
        } finally {
            lock.unlock();
            lockLogger.debug("OceanBaseConnection.commit unlocked");
            if (protocol != null) {
                protocol.endCallInterface("OceanBaseConnection.commit");
            }
        }
    }

    /**
     * Rolls back a transaction.
     *
     * @throws SQLException if there is an error rolling back.
     */
    public void rollback() throws SQLException {
        if (this.getAutoCommit()) {
            throw new SQLException("Can't call rollback when autocommit enable");
        }
        if (protocol != null) {
            protocol.endCallInterface("OceanBaseConnection.rollback");
        }
        lock.lock();
        try {
            lockLogger.debug("OceanBaseConnection.rollback locked");

            if (protocol.inTransaction()) {
                try (Statement st = createStatement()) {
                    st.execute("ROLLBACK");
                }
            }
        } finally {
            lock.unlock();
            lockLogger.debug("OceanBaseConnection.rollback unlocked");
            if (protocol != null) {
                protocol.endCallInterface("OceanBaseConnection.rollback");
            }
        }
    }

    /**
     * Undoes all changes made after the given <code>Savepoint</code> object was set.
     *
     * <p>This method should be used only when auto-commit has been disabled.
     *
     * @param savepoint the <code>Savepoint</code> object to roll back to
     * @throws SQLException if a database access error occurs, this method is called while
     *                      participating in a distributed transaction, this method is called on a closed connection,
     *                      the <code>Savepoint</code> object is no longer valid, or this <code>Connection</code>
     *                      object is currently in auto-commit mode
     * @see Savepoint
     * @see #rollback
     */
    public void rollback(final Savepoint savepoint) throws SQLException {
        if (this.getAutoCommit()) {
            throw new SQLException("Can't call rollback when autocommit enable");
        }
        if (protocol != null) {
            protocol.startCallInterface();
        }

        try (Statement st = createStatement()) {
            if (!this.protocol.isOracleMode()) {
                st.execute("ROLLBACK TO SAVEPOINT `" + savepoint.getSavepointName() + "`");
            } else {
                st.execute("ROLLBACK TO " + savepoint.getSavepointName());
            }
        } finally {
            if (protocol != null) {
                protocol.endCallInterface("OceanBaseConnection.rollback");
            }
        }
    }

    /**
     * close the connection.
     *
     * @throws SQLException if there is a problem talking to the server.
     */
    public void close() throws SQLException {
        if (protocol != null) {
            protocol.startCallInterface();
        }

        if (pooledConnection != null) {
            if (!getAutoCommit()) {
                rollback();
            }
            pooledConnection.fireConnectionClosed();
            return;
        }
        protocol.closeExplicit();

        if (protocol != null) {
            protocol.endCallInterface("OceanBaseConnection.close");
        }
    }

    /**
     * checks if the connection is closed.
     *
     * @return true if the connection is closed
     */
    public boolean isClosed() {
        return protocol.isClosed();
    }

    /**
     * returns the meta data about the database.
     *
     * @return meta data about the db.
     */
    public DatabaseMetaData getMetaData() {
        return new OceanBaseDatabaseMetaData(this, protocol.getUrlParser());
    }

    /**
     * Retrieves whether this <code>Connection</code> object is in read-only mode.
     *
     * @return <code>true</code> if this <code>Connection</code> object is read-only; <code>false
     * </code> otherwise
     * @throws SQLException SQLException if a database access error occurs or this method is called on
     *                      a closed connection
     */
    public boolean isReadOnly() throws SQLException {
        return protocol.getReadonly();
    }

    /**
     * Sets whether this connection is read only.
     *
     * @param readOnly true if it should be read only.
     * @throws SQLException if there is a problem
     */
    public void setReadOnly(final boolean readOnly) throws SQLException {
        if (protocol != null) {
            protocol.startCallInterface();
        }
        try {
            logger.debug("conn={}({}) - set read-only to value {} {}",
                protocol.getServerThreadId(), protocol.isMasterConnection() ? "M" : "S", readOnly);
            if (protocol.isOracleMode() && options.oracleChangeReadOnlyToRepeatableRead) {
                setTransactionIsolation(readOnly ? TRANSACTION_REPEATABLE_READ : TRANSACTION_READ_COMMITTED);
            } else {
                stateFlag |= ConnectionState.STATE_READ_ONLY;
                protocol.setReadonly(readOnly);
            }
        } catch (SQLException e) {
            throw exceptionFactory.create(e);
        } finally {
            if (protocol != null) {
                protocol.endCallInterface("OceanBaseConnection.setReadOnly");
            }
        }
    }

    /**
     * Retrieves this <code>Connection</code> object's current catalog name.
     *
     * @return the current catalog name or <code>null</code> if there is none
     * @throws SQLException if a database access error occurs or this method is called on a closed
     *                      connection
     * @see #setCatalog
     */
    public String getCatalog() throws SQLException {
        if (this.getProtocol().isOracleMode()) {
            if (options.compatibleOjdbcVersion == 8) {
                checkClosed();
            }
            return null;
        } else {
            return protocol.getCatalog();
        }
    }

    /**
     * Sets the given catalog name in order to select a subspace of this <code>Connection</code>
     * object's database in which to work.
     *
     * <p>If the driver does not support catalogs, it will silently ignore this request. MariaDB
     * treats catalogs and databases as equivalent
     *
     * @param catalog the name of a catalog (subspace in this <code>Connection</code> object's
     *                database) in which to work
     * @throws SQLException if a database access error occurs or this method is called on a closed
     *                      connection
     * @see #getCatalog
     */
    public void setCatalog(final String catalog) throws SQLException {
        if (this.getProtocol().isOracleMode()) {
            if (options.compatibleOjdbcVersion == 8) {
                checkClosed();
            }
        } else {
            checkClosed();
            if (catalog == null) {
                throw new SQLException("The catalog name may not be null", "XAE05");
            }
            if (catalog.equals(this.protocol.getCatalog())) {
                return;
            }
            if (protocol != null) {
                protocol.startCallInterface();
            }

            try {
                stateFlag |= ConnectionState.STATE_DATABASE;
                protocol.setCatalog(catalog);
            } catch (SQLException e) {
                throw exceptionFactory.create(e);
            } finally {
                if (protocol != null) {
                    protocol.endCallInterface("OceanBaseConnection.setCatalog");
                }
            }
        }
    }

    public boolean isServerMariaDb() throws SQLException {
        return protocol.isServerMariaDb();
    }

    public boolean versionGreaterOrEqual(int major, int minor, int patch) {
        return protocol.versionGreaterOrEqual(major, minor, patch);
    }

    /**
     * Retrieves this <code>Connection</code> object's current transaction isolation level.
     *
     * @return the current transaction isolation level, which will be one of the following constants:
     * <code>Connection.TRANSACTION_READ_UNCOMMITTED</code>, <code>
     * Connection.TRANSACTION_READ_COMMITTED</code>, <code>Connection.TRANSACTION_REPEATABLE_READ
     * </code>, <code>Connection.TRANSACTION_SERIALIZABLE</code>, or <code>
     * Connection.TRANSACTION_NONE</code>.
     * @throws SQLException if a database access error occurs or this method is called on a closed
     *                      connection
     * @see #setTransactionIsolation
     */
    public int getTransactionIsolation() throws SQLException {
        return protocol.getTransactionIsolationLevel();
    }

    /**
     * Attempts to change the transaction isolation level for this <code>Connection</code> object to
     * the one given. The constants defined in the interface <code>Connection</code> are the possible
     * transaction isolation levels.
     *
     * <p><B>Note:</B> If this method is called during a transaction, the result is
     * implementation-defined.
     *
     * @param level one of the following <code>Connection</code> constants: <code>
     *              Connection.TRANSACTION_READ_UNCOMMITTED</code>, <code>Connection.TRANSACTION_READ_COMMITTED
     *              </code>, <code>Connection.TRANSACTION_REPEATABLE_READ</code>, or <code>
     *              Connection.TRANSACTION_SERIALIZABLE</code>. (Note that <code>Connection.TRANSACTION_NONE
     *              </code> cannot be used because it specifies that transactions are not supported.)
     * @throws SQLException if a database access error occurs, this method is called on a closed
     *                      connection or the given parameter is not one of the <code>Connection</code> constants
     * @see DatabaseMetaData#supportsTransactionIsolationLevel
     * @see #getTransactionIsolation
     */
    public void setTransactionIsolation(final int level) throws SQLException {
        if (this.protocol.isOracleMode()
            && (level != TRANSACTION_READ_COMMITTED && level != TRANSACTION_REPEATABLE_READ && level != TRANSACTION_SERIALIZABLE)) {
            throw exceptionFactory.create("Unsupported transaction isolation level by OracleModel");
        }
        if (protocol != null) {
            protocol.startCallInterface();
        }

        try {
            stateFlag |= ConnectionState.STATE_TRANSACTION_ISOLATION;
            protocol.setTransactionIsolation(level);
        } catch (SQLException e) {
            throw exceptionFactory.create(e);
        } finally {
            if (protocol != null) {
                protocol.endCallInterface("OceanBaseConnection.setTransactionIsolation");
            }
        }
    }

    /**
     * Retrieves the first warning reported by calls on this <code>Connection</code> object. If there
     * is more than one warning, subsequent warnings will be chained to the first one and can be
     * retrieved by calling the method <code>SQLWarning.getNextWarning</code> on the warning that was
     * retrieved previously.
     *
     * <p>This method may not be called on a closed connection; doing so will cause an <code>
     * SQLException</code> to be thrown.
     *
     * <p><B>Note:</B> Subsequent warnings will be chained to this SQLWarning.
     *
     * @return the first <code>SQLWarning</code> object or <code>null</code> if there are none
     * @throws SQLException if a database access error occurs or this method is called on a closed
     *                      connection
     * @see SQLWarning
     */
    public SQLWarning getWarnings() throws SQLException {
        //mysql mode no longer concern the hasWarnings flag
        if (warningsCleared || isClosed() || (!protocol.hasWarnings() && protocol.isOracleMode())) {
            return null;
        }
        // If it is oracle mode and protocol.hasWarnings() , need to return a SQLWarning to inform the user
        if (protocol.isOracleMode()) {
            return new SQLWarning("The execution is complete, but with warnings");
        }
        if (protocol != null) {
            protocol.startCallInterface();
        }

        SQLWarning last = null;
        SQLWarning first = null;
        try (Statement st = this.createStatement()) {
            try (ResultSet rs = st.executeQuery("show warnings")) {
                // returned result set has 'level', 'code' and 'message' columns, in this order.
                while (rs.next()) {
                    int code = rs.getInt(2);
                    String message = rs.getString(3);
                    SQLWarning warning = new SQLWarning(message, null, code);
                    if (first == null) {
                        first = warning;
                        last = warning;
                    } else {
                        last.setNextWarning(warning);
                        last = warning;
                    }
                }
            }
        }

        if (protocol != null) {
            protocol.endCallInterface("OceanBaseConnection.getWarnings");
        }
        return first;
    }

    /**
     * Clears all warnings reported for this <code>Connection</code> object. After a call to this
     * method, the method <code>getWarnings</code> returns <code>null</code> until a new warning is
     * reported for this <code>Connection</code> object.
     *
     * @throws SQLException SQLException if a database access error occurs or this method is called on
     *                      a closed connection
     */
    public void clearWarnings() throws SQLException {
        if (this.isClosed()) {
            throw exceptionFactory
                .create("Connection.clearWarnings cannot be called on a closed connection");
        }
        warningsCleared = true;
    }

    /**
     * Reenable warnings, when next statement is executed.
     */
    public void reenableWarnings() {
        warningsCleared = false;
    }

    /**
     * Retrieves the <code>Map</code> object associated with this <code>Connection</code> object.
     * Unless the application has added an entry, the type map returned will be empty.
     *
     * @return the <code>java.util.Map</code> object associated with this <code>Connection</code>
     * object
     * @see #setTypeMap
     * @since 1.2
     */
    public Map<String, Class<?>> getTypeMap() {
        return this.typeMap;
    }

    /**
     * Installs the given <code>TypeMap</code> object as the type map for this <code>Connection</code>
     * object. The type map will be used for the custom mapping of SQL structured types and distinct
     * types.
     *
     * @param map the <code>java.util.Map</code> object to install as the replacement for this <code>
     *            Connection</code> object's default type map
     * @throws SQLException if a database access error occurs, this method is called on a closed
     *                      connection or the given parameter is not a <code>java.util.Map</code> object
     * @see #getTypeMap
     */
    public void setTypeMap(final Map<String, Class<?>> map) throws SQLException {
//        throw exceptionFactory.notSupported("TypeMap are not supported");
        if (this.typeMap != null && this.typeMap.size() != 0) {
            Set<String> set = this.typeMap.keySet();
            set.forEach(s -> map.put(s, this.typeMap.get(s)));
        }
        typeMap = map;
    }

    /**
     * Retrieves the current holdability of <code>ResultSet</code> objects created using this <code>
     * Connection</code> object.
     *
     * @return the holdability, one of <code>ResultSet.HOLD_CURSORS_OVER_COMMIT</code> or <code>
     * ResultSet.CLOSE_CURSORS_AT_COMMIT</code>
     * @see #setHoldability
     * @see DatabaseMetaData#getResultSetHoldability
     * @see ResultSet
     * @since 1.4
     */
    public int getHoldability() {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    /**
     * Changes the default holdability of <code>ResultSet</code> objects created using this <code>
     * Connection</code> object to the given holdability. The default holdability of <code>ResultSet
     * </code> objects can be be determined by invoking {@link
     * DatabaseMetaData#getResultSetHoldability}.
     *
     * @param holdability a <code>ResultSet</code> holdability constant; one of <code>
     *                    ResultSet.HOLD_CURSORS_OVER_COMMIT</code> or <code>ResultSet.CLOSE_CURSORS_AT_COMMIT</code>
     * @see #getHoldability
     * @see DatabaseMetaData#getResultSetHoldability
     * @see ResultSet
     */
    @Override
    public void setHoldability(final int holdability) {
        // not handled
    }

    /**
     * Creates an unnamed savepoint in the current transaction and returns the new <code>Savepoint
     * * </code> object that represents it.
     *
     * <p>if setSavepoint is invoked outside of an active transaction, a transaction will be started
     * at this newly created savepoint.
     *
     * @return the new <code>Savepoint</code> object
     * @throws SQLException if a database access error occurs, this method is called while
     *                      participating in a distributed transaction, this method is called on a closed connection or
     *                      this <code>Connection</code> object is currently in auto-commit mode
     * @see Savepoint
     * @since 1.4
     */
    public Savepoint setSavepoint() throws SQLException {
        String randomName = "";
        if (this.protocol.isOracleMode()) {
            if (this.getAutoCommit()) {
                throw new SQLException("Unable to set a savepoint with auto-commit enabled");
            }
            for (int i = 0; i < 10; i++) {
                char c = (char) ((Math.random() * 26) + 97);
                randomName += c;
            }
        } else {
            randomName = UUID.randomUUID().toString();
        }
        return setSavepoint(randomName);
    }

    /**
     * Creates a savepoint with the given name in the current transaction and returns the new <code>
     * Savepoint</code> object that represents it. if setSavepoint is invoked outside of an active
     * transaction, a transaction will be started at this newly created savepoint.
     *
     * @param name a <code>String</code> containing the name of the savepoint
     * @return the new <code>Savepoint</code> object
     * @throws SQLException if a database access error occurs, this method is called while
     *                      participating in a distributed transaction, this method is called on a closed connection or
     *                      this <code>Connection</code> object is currently in auto-commit mode
     * @see Savepoint
     * @since 1.4
     */
    public Savepoint setSavepoint(final String name) throws SQLException {
        if (protocol != null) {
            protocol.startCallInterface();
        }

        Savepoint savepoint = new OceanBaseSavepoint(name);
        try (Statement st = createStatement()) {
            if (this.protocol.isOracleMode()) {
                st.execute("SAVEPOINT " + savepoint.getSavepointName());
            } else {
                st.execute("SAVEPOINT `" + savepoint.getSavepointName() + "`");
            }
//            st.execute("SAVEPOINT `" + savepoint.getSavepointName() + "`");
        }

        if (protocol != null) {
            protocol.endCallInterface("OceanBaseConnection.setSavepoint");
        }
        return savepoint;
    }

    /**
     * Removes the specified <code>Savepoint</code> and subsequent <code>Savepoint</code> objects from
     * the current transaction. Any reference to the savepoint after it have been removed will cause
     * an <code>SQLException</code> to be thrown.
     *
     * @param savepoint the <code>Savepoint</code> object to be removed
     * @throws SQLException if a database access error occurs, this method is called on a closed
     *                      connection or the given <code>Savepoint</code> object is not a valid savepoint in the
     *                      current transaction
     */
    public void releaseSavepoint(final Savepoint savepoint) throws SQLException {
        //this is a no-op in mysql-jdbc
        if (protocol.isOracleMode()) {
            throw exceptionFactory.notSupported("releaseSavepoint is not supported");
        }
        if (protocol != null) {
            protocol.startCallInterface();
        }
        try (Statement st = createStatement()) {
            st.execute("RELEASE SAVEPOINT `" + savepoint.getSavepointName() + "`");
        } finally {
            if (protocol != null) {
                protocol.endCallInterface("OceanBaseConnection.releaseSavepoint");
            }
        }
    }

    /**
     * Constructs an object that implements the <code>Clob</code> interface. The object returned
     * initially contains no data. The <code>setAsciiStream</code>, <code>setCharacterStream</code>
     * and <code>setString</code> methods of the <code>Clob</code> interface may be used to add data
     * to the <code>Clob</code>.
     *
     * @return An object that implements the <code>Clob</code> interface
     */
    public java.sql.Clob createClob() {
        return new Clob();
    }

    /**
     * Constructs an object that implements the <code>Blob</code> interface. The object returned
     * initially contains no data. The <code>setBinaryStream</code> and <code>setBytes</code> methods
     * of the <code>Blob</code> interface may be used to add data to the <code>Blob</code>.
     *
     * @return An object that implements the <code>Blob</code> interface
     */
    public java.sql.Blob createBlob() {
        return new Blob();
    }

    /**
     * Constructs an object that implements the <code>NClob</code> interface. The object returned
     * initially contains no data. The <code>setAsciiStream</code>, <code>setCharacterStream</code>
     * and <code>setString</code> methods of the <code>NClob</code> interface may be used to add data
     * to the <code>NClob</code>.
     *
     * @return An object that implements the <code>NClob</code> interface
     */
    public NClob createNClob() {
        return new JDBC4NClob("", null);
    }

    /**
     * Constructs an object that implements the <code>SQLXML</code> interface. The object returned
     * initially contains no data. The <code>createXmlStreamWriter</code> object and <code>setString
     * </code> method of the <code>SQLXML</code> interface may be used to add data to the <code>SQLXML
     * </code> object.
     *
     * @return An object that implements the <code>SQLXML</code> interface
     * @throws SQLException if an object that implements the <code>SQLXML</code> interface can not be
     *                      constructed, this method is called on a closed connection or a database access error
     *                      occurs.
     */
    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw exceptionFactory.notSupported("SQLXML type is not supported");
    }

    /**
     * Returns true if the connection has not been closed and is still valid. The driver shall submit
     * a query on the connection or use some other mechanism that positively verifies the connection
     * is still valid when this method is called.
     *
     * <p>The query submitted by the driver to validate the connection shall be executed in the
     * context of the current transaction.
     *
     * @param timeout - The time in seconds to wait for the database operation used to validate the
     *                connection to complete. If the timeout period expires before the operation completes, this
     *                method returns false. A value of 0 indicates a timeout is not applied to the database
     *                operation.
     * @return true if the connection is valid, false otherwise
     * @throws SQLException if the value supplied for <code>timeout</code> is less then 0
     * @see DatabaseMetaData#getClientInfoProperties
     * @since 1.6
     */
    public boolean isValid(final int timeout) throws SQLException {
        if (timeout < 0) {
            throw new SQLException("the value supplied for timeout is negative");
        }
        if (isClosed()) {
            return false;
        }
        if (protocol != null) {
            protocol.startCallInterface();
        }

        try {
            return protocol.isValid(timeout * 1000);
        } catch (SQLException e) {
            // eat
            return false;
        } finally {
            if (protocol != null) {
                protocol.endCallInterface("OceanBaseConnection.isValid");
            }
        }
    }

    private void checkClientClose(final String name) throws SQLClientInfoException {
        if (protocol.isExplicitClosed()) {
            Map<String, ClientInfoStatus> failures = new HashMap<>();
            failures.put(name, ClientInfoStatus.REASON_UNKNOWN);
            throw new SQLClientInfoException("setClientInfo() is called on closed connection", failures);
        }
    }

    private void checkClientReconnect(final String name) throws SQLClientInfoException {
        if (protocol.isClosed() && protocol.getProxy() != null) {
            lock.lock();
            try {
                lockLogger.debug("OceanBaseConnection.checkClientReconnect locked");
                protocol.getProxy().reconnect();
            } catch (SQLException sqle) {
                Map<String, ClientInfoStatus> failures = new HashMap<>();
                failures.put(name, ClientInfoStatus.REASON_UNKNOWN);
                throw new SQLClientInfoException("Connection closed", failures, sqle);
            } finally {
                lock.unlock();
                lockLogger.debug("OceanBaseConnection.checkClientReconnect unlocked");
            }
        }
    }

    private void checkClientValidProperty(final String name) throws SQLClientInfoException {
        if (name == null
                || (!"ApplicationName".equals(name)
                && !"ClientUser".equals(name)
                && !"ClientHostname".equals(name))) {
            Map<String, ClientInfoStatus> failures = new HashMap<>();
            failures.put(name, ClientInfoStatus.REASON_UNKNOWN_PROPERTY);
            throw new SQLClientInfoException(
                    "setClientInfo() parameters can only be \"ApplicationName\",\"ClientUser\" or \"ClientHostname\", "
                            + "but was : "
                            + name,
                    failures);
        }
    }

    private String buildClientQuery(final String name, final String value) {
        StringBuilder escapeQuery = new StringBuilder("SET @").append(name).append("=");
        if (value == null) {
            escapeQuery.append("null");
        } else {
            escapeQuery.append("'");
            int charsOffset = 0;
            int charsLength = value.length();
            char charValue;
            if (protocol.noBackslashEscapes()) {
                while (charsOffset < charsLength) {
                    charValue = value.charAt(charsOffset);
                    if (charValue == '\'') {
                        escapeQuery.append('\''); // add a single escape quote
                    }
                    escapeQuery.append(charValue);
                    charsOffset++;
                }
            } else {
                while (charsOffset < charsLength) {
                    charValue = value.charAt(charsOffset);
                    if (charValue == '\'' || charValue == '\\' || charValue == '"'
                        || charValue == 0) {
                        escapeQuery.append('\\'); // add escape slash
                    }
                    escapeQuery.append(charValue);
                    charsOffset++;
                }
            }
            escapeQuery.append("'");
        }
        return escapeQuery.toString();
    }

    /**
     * Sets the value of the client info property specified by name to the value specified by value.
     *
     * <p>Applications may use the <code>DatabaseMetaData.getClientInfoProperties</code> method to
     * determine the client info properties supported by the driver and the maximum length that may be
     * specified for each property.
     *
     * <p>The driver stores the value specified in a suitable location in the database. For example in
     * a special register, session parameter, or system table column. For efficiency the driver may
     * defer setting the value in the database until the next time a statement is executed or
     * prepared. Other than storing the client information in the appropriate place in the database,
     * these methods shall not alter the behavior of the connection in anyway. The values supplied to
     * these methods are used for accounting, diagnostics and debugging purposes only.
     *
     * <p>The driver shall generate a warning if the client info name specified is not recognized by
     * the driver.
     *
     * <p>If the value specified to this method is greater than the maximum length for the property
     * the driver may either truncate the value and generate a warning or generate a <code>
     * SQLClientInfoException</code>. If the driver generates a <code>SQLClientInfoException</code>,
     * the value specified was not set on the connection.
     *
     * <p>The following are standard client info properties. Drivers are not required to support these
     * properties however if the driver supports a client info property that can be described by one
     * of the standard properties, the standard property name should be used.
     *
     * <ul>
     *   <li>ApplicationName - The name of the application currently utilizing the connection
     *   <li>ClientUser - The name of the user that the application using the connection is performing
     *       work for. This may not be the same as the user name that was used in establishing the
     *       connection.
     *   <li>ClientHostname - The hostname of the computer the application using the connection is
     *       running on.
     * </ul>
     *
     * @param name  The name of the client info property to set
     * @param value The value to set the client info property to. If the value is null, the current
     *              value of the specified property is cleared.
     * @throws SQLClientInfoException if the database server returns an error while setting the client
     *                                info value on the database server or this method is called on a closed connection
     * @since 1.6
     */
    public void setClientInfo(final String name, final String value) throws SQLClientInfoException {
        checkClientClose(name);
        checkClientReconnect(name);
        checkClientValidProperty(name);
        if (protocol != null) {
            protocol.startCallInterface();
        }

        try (Statement statement = createStatement()) {
            statement.execute(buildClientQuery(name, value));
        } catch (SQLException sqle) {
            Map<String, ClientInfoStatus> failures = new HashMap<>();
            failures.put(name, ClientInfoStatus.REASON_UNKNOWN);
            throw new SQLClientInfoException("unexpected error during setClientInfo", failures, sqle);
        } finally {
            if (protocol != null) {
                protocol.endCallInterface("OceanBaseConnection.setClientInfo");
            }
        }
    }

    /**
     * Returns a list containing the name and current value of each client info property supported by
     * the driver. The value of a client info property may be null if the property has not been set
     * and does not have a default value.
     *
     * @return A <code>Properties</code> object that contains the name and current value of each of
     * the client info properties supported by the driver.
     * @throws SQLException if the database server returns an error when fetching the client info
     *                      values from the database or this method is called on a closed connection
     */
    public Properties getClientInfo() throws SQLException {
        checkConnection();
        if (protocol != null) {
            protocol.startCallInterface();
        }

        String sql;
        if(this.protocol.isOracleMode()) {
            sql = "SELECT @ApplicationName, @ClientUser, @ClientHostname from dual";
        } else {
            sql = "SELECT @ApplicationName, @ClientUser, @ClientHostname";
        }
        Properties properties = new Properties();
        try (Statement statement = createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                if (rs.next()) {
                    if (rs.getString(1) != null) {
                        properties.setProperty("ApplicationName", rs.getString(1));
                    }
                    if (rs.getString(2) != null) {
                        properties.setProperty("ClientUser", rs.getString(2));
                    }
                    if (rs.getString(3) != null) {
                        properties.setProperty("ClientHostname", rs.getString(3));
                    }
                    return properties;
                }
            }
        }
        properties.setProperty("ApplicationName", null);
        properties.setProperty("ClientUser", null);
        properties.setProperty("ClientHostname", null);

        if (protocol != null) {
            protocol.endCallInterface("OceanBaseConnection.getClientInfo");
        }
        return properties;
    }

    /**
     * Sets the value of the connection's client info properties. The <code>Properties</code> object
     * contains the names and values of the client info properties to be set. The set of client info
     * properties contained in the properties list replaces the current set of client info properties
     * on the connection. If a property that is currently set on the connection is not present in the
     * properties list, that property is cleared. Specifying an empty properties list will clear all
     * of the properties on the connection. See <code>setClientInfo (String, String)</code> for more
     * information.
     *
     * <p>If an error occurs in setting any of the client info properties, a <code>
     * SQLClientInfoException</code> is thrown. The <code>SQLClientInfoException</code> contains
     * information indicating which client info properties were not set. The state of the client
     * information is unknown because some databases do not allow multiple client info properties to
     * be set atomically. For those databases, one or more properties may have been set before the
     * error occurred.
     *
     * @param properties the list of client info properties to set
     * @throws SQLClientInfoException if the database server returns an error while setting the
     *                                clientInfo values on the database server or this method is called on a closed connection
     * @see Connection#setClientInfo(String, String) setClientInfo(String, String)
     * @since 1.6
     */
    public void setClientInfo(final Properties properties) throws SQLClientInfoException {
        Map<String, ClientInfoStatus> propertiesExceptions = new HashMap<>();
        for (String name : new String[]{"ApplicationName", "ClientUser", "ClientHostname"}) {
            try {
                setClientInfo(name, properties.getProperty(name));
            } catch (SQLClientInfoException e) {
                propertiesExceptions.putAll(e.getFailedProperties());
            }
        }

        if (!propertiesExceptions.isEmpty()) {
            String errorMsg =
                    "setClientInfo errors : the following properties where not set : "
                            + propertiesExceptions.keySet();
            throw new SQLClientInfoException(errorMsg, propertiesExceptions);
        }
    }

    /**
     * Returns the value of the client info property specified by name. This method may return null if
     * the specified client info property has not been set and does not have a default value. This
     * method will also return null if the specified client info property name is not supported by the
     * driver. Applications may use the <code>DatabaseMetaData.getClientInfoProperties</code> method
     * to determine the client info properties supported by the driver.
     *
     * @param name The name of the client info property to retrieve
     * @return The value of the client info property specified
     * @throws SQLException if the database server returns an error when fetching the client info
     *                      value from the database or this method is called on a closed connection
     * @see DatabaseMetaData#getClientInfoProperties
     * @since 1.6
     */
    public String getClientInfo(final String name) throws SQLException {
        checkConnection();
        if (!"ApplicationName".equals(name)
                && !"ClientUser".equals(name)
                && !"ClientHostname".equals(name)) {
            throw new SQLException(
                    "name must be \"ApplicationName\", \"ClientUser\" or \"ClientHostname\", but was \""
                            + name
                            + "\"");
        }
        if (protocol != null) {
            protocol.startCallInterface();
        }

        String sql;
        if(this.protocol.isOracleMode()) {
            sql = "SELECT @" + name + " from dual";
        } else {
            sql = "SELECT @" + name;
        }
        try (Statement statement = createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                if (rs.next()) {
                    return rs.getString(1);
                }
            }
        } finally {
            if (protocol != null) {
                protocol.endCallInterface("OceanBaseConnection.getClientInfo");
            }
        }
        return null;
    }

    /**
     * Factory method for creating Array objects. <b>Note: </b>When <code>createArrayOf</code> is used
     * to create an array object that maps to a primitive data type, then it is implementation-defined
     * whether the <code>Array</code> object is an array of that primitive data type or an array of
     * <code>Object</code>. <b>Note: </b>The JDBC driver is responsible for mapping the elements
     * <code>Object</code> array to the default JDBC SQL type defined in java.sql.Types for the given
     * class of <code>Object</code>. The default mapping is specified in Appendix B of the JDBC
     * specification. If the resulting JDBC type is not the appropriate type for the given typeName
     * then it is implementation defined whether an <code>SQLException</code> is thrown or the driver
     * supports the resulting conversion.
     *
     * @param typeName the SQL name of the type the elements of the array map to. The typeName is a
     *                 database-specific name which may be the name of a built-in type, a user-defined type or a
     *                 standard SQL type supported by this database. This is the value returned by <code>
     *                 Array.getBaseTypeName</code>
     * @param elements the elements that populate the returned object
     * @return an Array object whose elements map to the specified SQL type
     * @throws SQLException if a database error occurs, the JDBC type is not appropriate for the
     *                      typeName and the conversion is not supported, the typeName is null or this method is called
     *                      on a closed connection
     */
    public Array createArrayOf(final String typeName, final Object[] elements) throws SQLException {
        if (!this.getProtocol().isOracleMode()) {
            throw exceptionFactory.notSupported("Array type is not supported");
        }
        boolean fetchTypeFromRemote = true;
        ComplexDataType attrType = null;
        if (getCacheComplexData()) {
            synchronized (this.complexDataCache) {
                attrType = (ComplexDataType) this.complexDataCache.get(typeName.toUpperCase());
                if (null != attrType && attrType.isValid()) {
                    fetchTypeFromRemote = false;
                } else if (null == attrType
                           && ComplexDataType.isBaseDataType(ComplexDataType
                               .getObComplexType(typeName))) {
                    fetchTypeFromRemote = false;
                    attrType = new ComplexDataType(typeName, this.getOracleSchemaInternal(),
                        ComplexDataType.getObComplexType(typeName));
                }
            }
        }
        if (fetchTypeFromRemote) {
            attrType = getComplexDataTypeFromRemote(typeName);
        }
        if (attrType.getType() == ComplexDataType.TYPE_COLLECTION) {
            ObArray array = new ArrayImpl(attrType);
            array.setAttrData(elements);
            return array;
        }
        ComplexDataType parentType = new ComplexDataType("", this.getOracleSchemaInternal(),
            ComplexDataType.TYPE_COLLECTION);
        parentType.setAttrCount(1);
        parentType.setAttrType(0, attrType);
        ObArray array = new ArrayImpl(parentType);
        array.setAttrData(elements);
        return array;
    }

    public boolean getCacheComplexData() { // todo connection properties
        // todo
        return true;
    }

    public void recacheComplexDataType(ComplexDataType type) {
        synchronized (this.protocol) { // todo don't know
            synchronized (this.complexDataCache) {
                this.complexDataCache.put(type.getTypeName().toUpperCase(), type);
            }
        }
    }

    public ComplexDataType getComplexDataType(String typeName) throws SQLException {
        ComplexDataType type = null;
        type = getComplexDataTypeFromCache(typeName);
        if (null != type && type.isValid()) {
            return type;
        }
        type = getComplexDataTypeFromRemote(typeName);
        return type;
    }

    public ComplexDataType getComplexDataTypeFromCache(String typeName) {
        synchronized (this.complexDataCache) {
            return (ComplexDataType) this.complexDataCache.get(typeName.toUpperCase());
        }
    }

    private java.sql.Connection getComplexConnection() throws SQLException {
        if (null == this.complexConnection || this.complexConnection.isClosed()) {
            this.complexConnection = new OceanBaseConnection(this.protocol.getUrlParser(),
                this.globalStateInfo, true); // duplicate a new connection todo
        }
        return this.complexConnection;
    }

    public ComplexDataType getComplexDataTypeFromRemote(String typeName) throws SQLException {
        try {
            java.sql.Connection conn = this.getComplexConnection(); // NOPMD
            java.sql.PreparedStatement ps = conn.prepareStatement(complexTypeSql,
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

            ps.setString(1, typeName.toUpperCase());
            ps.setString(2, typeName.toUpperCase());
            java.sql.ResultSet rs = ps.executeQuery(); // NOPMD
            if (rs.next()) {
                rs.beforeFirst();
            } else {
                rs.close();
                ps.close();

                String tmpString = null;
                if (typeName.startsWith("DBMS_XA")) {
                    tmpString = "'SYS'";
                } else {
                    tmpString = "SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')";
                }
                String complexAllTypeSql = "SELECT * from (SELECT\n" + "    0 DEPTH,\n"
                                           + "    NULL PARENT_OWNER,\n" + "    NULL PARENT_TYPE,\n"
                                           + "    to_char(TYPE_NAME) CHILD_TYPE,\n"
                                           + "    0 ATTR_NO,\n" + "    OWNER CHILD_TYPE_OWNER,\n"
                                           + "    A.TYPECODE ATTR_TYPE_CODE,\n"
                                           + "    NULL LENGTH,\n" + "    NULL NUMBER_PRECISION,\n"
                                           + "    NULL SCALE,\n" + "    NULL CHARACTER_SET_NAME\n"
                                           + "  FROM\n"
                                           + "    ALL_TYPES A WHERE TYPE_NAME = ? AND OWNER = "
                                           + tmpString
                                           + "\n"
                                           + "  UNION\n"
                                           + "  (\n"
                                           + "  WITH\n"
                                           + "  CTE_RESULT(PARENT_OWNER, PARENT_TYPE, CHILD_TYPE, ATTR_NO, CHILD_TYPE_OWNER, ATTR_TYPE_CODE, LENGTH, NUMBER_PRECISION, SCALE, CHARACTER_SET_NAME)\n"
                                           + "  AS (\n"
                                           + "      SELECT\n"
                                           + "        B.OWNER PARENT_OWNER,\n"
                                           + "        B.TYPE_NAME PARENT_TYPE,\n"
                                           + "        B.ELEM_TYPE_NAME CHILD_TYPE,\n"
                                           + "        0 ATTR_NO,\n"
                                           + "        B.ELEM_TYPE_OWNER CHILD_TYPE_OWNER,\n"
                                           + "        NVL(A.TYPECODE, B.ELEM_TYPE_NAME) AS ATTR_TYPE_CODE,\n"
                                           + "        B.LENGTH LENGTH,\n"
                                           + "        B.NUMBER_PRECISION NUMBER_PRECISION,\n"
                                           + "        B.SCALE SCALE,\n"
                                           + "        B.CHARACTER_SET_NAME CHARACTER_SET_NAME\n"
                                           + "      FROM\n"
                                           + "        ALL_COLL_TYPES B LEFT JOIN ALL_TYPES A ON A.TYPE_NAME = B.ELEM_TYPE_NAME AND A.OWNER = B.ELEM_TYPE_OWNER\n"
                                           + "      UNION\n"
                                           + "      SELECT\n"
                                           + "        B.OWNER PARENT_OWNER,\n"
                                           + "        B.TYPE_NAME PARENT_TYPE,\n"
                                           + "        B.ATTR_TYPE_NAME CHILD_TYPE,\n"
                                           + "        B.ATTR_NO ATTR_NO,\n"
                                           + "        B.ATTR_TYPE_OWNER CHILD_TYPE_OWNER,\n"
                                           + "        NVL(A.TYPECODE, B.ATTR_TYPE_NAME) AS ATTR_TYPE_CODE,\n"
                                           + "        B.LENGTH LENGTH,\n"
                                           + "        B.NUMBER_PRECISION NUMBER_PRECISION,\n"
                                           + "        B.SCALE SCALE,\n"
                                           + "        B.CHARACTER_SET_NAME CHARACTER_SET_NAME\n"
                                           + "      FROM ALL_TYPE_ATTRS B LEFT JOIN ALL_TYPES A ON A.TYPE_NAME = B.ATTR_TYPE_NAME AND A.OWNER = B.ATTR_TYPE_OWNER ORDER BY ATTR_NO\n"
                                           + "  ) ,\n"
                                           + "  CTE(DEPTH, PARENT_OWNER, PARENT_TYPE, CHILD_TYPE, ATTR_NO, CHILD_TYPE_OWNER, ATTR_TYPE_CODE, LENGTH, NUMBER_PRECISION, SCALE, CHARACTER_SET_NAME)\n"
                                           + "  AS (\n"
                                           + "    SELECT\n"
                                           + "      1 DEPTH,\n"
                                           + "      PARENT_OWNER,\n"
                                           + "      PARENT_TYPE,\n"
                                           + "      CHILD_TYPE,\n"
                                           + "      ATTR_NO,\n"
                                           + "      CHILD_TYPE_OWNER,\n"
                                           + "      ATTR_TYPE_CODE,\n"
                                           + "      LENGTH,\n"
                                           + "      NUMBER_PRECISION,\n"
                                           + "      SCALE, CHARACTER_SET_NAME\n"
                                           + "    FROM CTE_RESULT WHERE PARENT_TYPE = ? AND PARENT_OWNER = "
                                           + tmpString
                                           + "\n"
                                           + "    UNION ALL\n"
                                           + "    SELECT\n"
                                           + "      DEPTH + 1 DEPTH,\n"
                                           + "      CTE_RESULT.PARENT_OWNER,\n"
                                           + "      CTE_RESULT.PARENT_TYPE,\n"
                                           + "      CTE_RESULT.CHILD_TYPE,\n"
                                           + "      CTE_RESULT.ATTR_NO,\n"
                                           + "      CTE_RESULT.CHILD_TYPE_OWNER,\n"
                                           + "      CTE_RESULT.ATTR_TYPE_CODE,\n"
                                           + "      CTE_RESULT.LENGTH,\n"
                                           + "      CTE_RESULT.NUMBER_PRECISION,\n"
                                           + "      CTE_RESULT.SCALE,\n"
                                           + "      CTE_RESULT.CHARACTER_SET_NAME\n"
                                           + "    FROM CTE_RESULT INNER JOIN CTE ON CTE_RESULT.PARENT_TYPE = CTE.CHILD_TYPE AND CTE_RESULT.PARENT_OWNER = CTE.CHILD_TYPE_OWNER\n"
                                           + "  )\n"
                                           + "  SELECT * FROM CTE\n"
                                           + "  ) ) ORDER BY DEPTH;";
                ps = conn.prepareStatement(complexAllTypeSql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);

                ps.setString(1, typeName.toUpperCase());
                ps.setString(2, typeName.toUpperCase());
                rs = ps.executeQuery(); // NOPMD
            }

            while (rs.next()) {
                ComplexDataType complexType = null;
                String childTypeName = rs.getString(CHILD_TYPE_INDEX);
                int type = ComplexDataType.getObComplexType(rs.getString(ATTR_TYPE_INDEX));
                if (ComplexDataType.TYPE_OBJECT == type || ComplexDataType.TYPE_COLLECTION == type) {
                    complexType = getComplexDataTypeFromCache(childTypeName);
                    if (null == complexType) {
                        complexType = new ComplexDataType(childTypeName,
                            rs.getString(CHILD_OWNER_INDEX), type);
                        complexType.setValid(false);
                        recacheComplexDataType(complexType);
                    }
                } else {
                    // for base data type
                    complexType = getComplexDataTypeFromCache(childTypeName);
                    if (null == complexType) {
                        complexType = new ComplexDataType(childTypeName, "", type);
                        complexType.setValid(true);
                        recacheComplexDataType(complexType);
                    }
                }
                if (rs.getInt(DEPTH_INDEX) > 0) {
                    // depth > 0 means parent is object  or collection
                    String parentTypeName = rs.getString(PARENT_TYPE_INDEX);
                    ComplexDataType parentType = getComplexDataTypeFromCache(parentTypeName);
                    int attrIndex = rs.getInt(ATTR_NO_INDEX);
                    if (ComplexDataType.TYPE_OBJECT == parentType.getType()
                        && parentType.getAttrCount() < attrIndex) {
                        parentType.setAttrCount(attrIndex);
                    }
                }
            }
            // second loop
            rs.first();
            do {
                if (rs.getInt(DEPTH_INDEX) > 0) {
                    String parentTypeName = rs.getString(PARENT_TYPE_INDEX);
                    ComplexDataType parentComplexType = getComplexDataTypeFromCache(parentTypeName);
                    String attrTypeName = rs.getString(CHILD_TYPE_INDEX);
                    ComplexDataType attrComplexType = getComplexDataTypeFromCache(attrTypeName);

                    if (parentComplexType.getType() == ComplexDataType.TYPE_OBJECT) {
                        parentComplexType
                            .setAttrType(rs.getInt(ATTR_NO_INDEX) - 1, attrComplexType);
                        parentComplexType.incInitAttrCount();
                        if (parentComplexType.getInitAttrCount() == parentComplexType
                            .getAttrCount()) {
                            parentComplexType.setValid(true);
                        }
                    } else if (parentComplexType.getType() == ComplexDataType.TYPE_COLLECTION) {
                        parentComplexType.setAttrCount(1);
                        parentComplexType.setAttrType(0, attrComplexType);
                        parentComplexType.setValid(true);
                    }
                }
            } while (rs.next());

            rs.close();
            ps.close();
            conn.close();
        } catch (SQLException e) {
            throw e;
        }
        return getComplexDataTypeFromCache(typeName);
    }

    /**
     * Factory method for creating Struct objects.
     *
     * @param typeName   the SQL type name of the SQL structured type that this <code>Struct</code>
     *                   object maps to. The typeName is the name of a user-defined type that has been defined for
     *                   this database. It is the value returned by <code>Struct.getSQLTypeName</code>.
     * @param attributes the attributes that populate the returned object
     * @return a Struct object that maps to the given SQL type and is populated with the given
     * attributes
     * @throws SQLException if a database error occurs, the typeName is null or this method is called
     *                      on a closed connection
     */
    public Struct createStruct(final String typeName, final Object[] attributes)
                                                                                throws SQLException {
        if (!this.getProtocol().isOracleMode()) {
            throw exceptionFactory.notSupported("Struct type is not supported");
        }
        boolean fetchTypeFromRemote = true;
        ComplexDataType type = null;
        if (getCacheComplexData()) { // todo
            synchronized (this.complexDataCache) {
                type = (ComplexDataType) this.complexDataCache.get(typeName.toUpperCase());
                if (null != type && type.isValid()) {
                    fetchTypeFromRemote = false;
                }
            }
        }
        if (fetchTypeFromRemote) {
            type = getComplexDataTypeFromRemote(typeName);
        }
        ObStruct struct = new StructImpl(type);
        struct.setAttrData(attributes);
        return struct;
    }

    /**
     * Returns an object that implements the given interface to allow access to non-standard methods,
     * or standard methods not exposed by the proxy. If the receiver implements the interface then the
     * result is the receiver or a proxy for the receiver. If the receiver is a wrapper and the
     * wrapped object implements the interface then the result is the wrapped object or a proxy for
     * the wrapped object. Otherwise return the the result of calling <code>unwrap</code> recursively
     * on the wrapped object or a proxy for that result. If the receiver is not a wrapper and does not
     * implement the interface, then an <code>SQLException</code> is thrown.
     *
     * @param iface A Class defining an interface that the result must implement.
     * @return an object that implements the interface. May be a proxy for the actual implementing
     * object.
     * @throws SQLException If no object found that implements the interface
     * @since 1.6
     */
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        try {
            if (isWrapperFor(iface)) {
                return iface.cast(this);
            } else {
                throw new SQLException("The receiver is not a wrapper for " + iface.getName());
            }
        } catch (Exception e) {
            throw new SQLException(
                "The receiver is not a wrapper and does not implement the interface");
        }
    }

    /**
     * Returns true if this either implements the interface argument or is directly or indirectly a
     * wrapper for an object that does. Returns false otherwise. If this implements the interface then
     * return true, else if this is a wrapper then return the result of recursively calling <code>
     * isWrapperFor</code> on the wrapped object. If this does not implement the interface and is not
     * a wrapper, return false. This method should be implemented as a low-cost operation compared to
     * <code>unwrap</code> so that callers can use this method to avoid expensive <code>unwrap</code>
     * calls that may fail. If this method returns true then calling <code>unwrap</code> with the same
     * argument should succeed.
     *
     * @param iface a Class defining an interface.
     * @return true if this implements the interface or directly or indirectly wraps an object that
     * does.
     * @since 1.6
     */
    public boolean isWrapperFor(final Class<?> iface) {
        return iface.isInstance(this);
    }

    /**
     * returns the username for the connection.
     *
     * @return the username.
     */
    @Deprecated
    public String getUsername() {
        return protocol.getUsername();
    }

    /**
     * returns the hostname for the connection.
     *
     * @return the hostname.
     */
    @Deprecated
    public String getHostname() {
        return protocol.getHost();
    }

    /**
     * returns the port for the connection.
     *
     * @return the port
     */
    @Deprecated
    public int getPort() {
        return protocol.getPort();
    }

    protected boolean getPinGlobalTxToPhysicalConnection() {
        return protocol.getPinGlobalTxToPhysicalConnection();
    }

    /**
     * If failover is not activated, will close connection when a connection error occur.
     */
    public void setHostFailed() {
        if (protocol.getProxy() == null) {
            protocol.setHostFailedWithoutProxy();
        }
    }

    /**
     * Are table case sensitive or not . Default Value: 0 (Unix), 1 (Windows), 2 (Mac OS X). If set to
     * 0 (the default on Unix-based systems), table names and aliases and database names are compared
     * in a case-sensitive manner. If set to 1 (the default on Windows), names are stored in lowercase
     * and not compared in a case-sensitive manner. If set to 2 (the default on Mac OS X), names are
     * stored as declared, but compared in lowercase.
     *
     * @return int value.
     * @throws SQLException if a connection error occur
     */
    public int getLowercaseTableNames() throws SQLException {
        if (lowercaseTableNames == -1) {
            try (Statement st = createStatement()) {
                try (ResultSet rs = st.executeQuery("select @@lower_case_table_names")) {
                    rs.next();
                    lowercaseTableNames = rs.getInt(1);
                }
            }
        }
        return lowercaseTableNames;
    }

    /**
     * Abort connection.
     *
     * @param executor executor
     * @throws SQLException if security manager doesn't permit it.
     */
    public void abort(Executor executor) throws SQLException {
        if (this.isClosed()) {
            return;
        }
        if (protocol != null) {
            protocol.startCallInterface();
        }

        SQLPermission sqlPermission = new SQLPermission("callAbort");
        SecurityManager securityManager = System.getSecurityManager();
        if (securityManager != null) {
            securityManager.checkPermission(sqlPermission);
        }
        if (executor == null) {
            throw exceptionFactory.create("Cannot abort the connection: null executor passed");
        }
        executor.execute(protocol::abort);

        if (protocol != null) {
            protocol.endCallInterface("OceanBaseConnection.abort");
        }
    }

    /**
     * Get network timeout.
     *
     * @return timeout
     * @throws SQLException if database socket error occur
     */
    public int getNetworkTimeout() throws SQLException {
        return this.protocol.getTimeout();
    }

    public String getSchema() throws SQLException {
        if (!this.protocol.isOracleMode()) {
            checkClosed();
            return null;
        } else {
            if (options.compatibleOjdbcVersion == 8) {
                checkClosed();
                return getOracleSchemaInternal();
            } else {
                throw new AbstractMethodError("Unimplemented method: getSchema()");
            }
        }
    }

    private String getOracleSchemaInternal() throws SQLException {
        String schema = null;
        Statement stmt = null;
        ResultSet rs;

        try {
            stmt = this.createStatement();
            stmt.setFetchSize(1);
            rs = stmt.executeQuery("SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL");
            if (rs.next()) {
                schema = rs.getString(1);
            }
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }
        return schema;
    }

    public String getDatabase() {
        return this.urlParser.getDatabase();
    }

    public void setSchema(String schema) throws SQLException {
        if (!this.getProtocol().isOracleMode()) {
            checkClosed();
        } else {
            if (options.compatibleOjdbcVersion == 8) {
                checkClosed();
                if (schema == null) {
                    throw new SQLException("invalid parameter, schema must not be null");
                }
                String regex = "("
                               + "\"[^\u0000\"]{0,28}\""
                               + ")|("
                               + "(\\p{javaLowerCase}|\\p{javaUpperCase})(\\p{javaLowerCase}|\\p{javaUpperCase}|\\d|_|\\$|#){0,29}"
                               + ")";
                if (!schema.matches(regex)) {
                    throw new SQLException("invalid parameter");
                }

                Statement stmt = null;
                try {
                    stmt = this.createStatement();
                    stmt.execute("alter session set current_schema = " + schema);
                } finally {
                    if (stmt != null) {
                        stmt.close();
                    }
                }
            } else {
                throw new AbstractMethodError("Unimplemented method: getSchema()");
            }
        }
    }

    /**
     * Change network timeout.
     *
     * @param executor     executor (can be null)
     * @param milliseconds network timeout in milliseconds.
     * @throws SQLException if security manager doesn't permit it.
     */
    public void setNetworkTimeout(Executor executor, final int milliseconds) throws SQLException {
        if (this.isClosed()) {
            throw exceptionFactory
                .create("Connection.setNetworkTimeout cannot be called on a closed connection");
        }
        if (milliseconds < 0) {
            throw exceptionFactory
                .create("Connection.setNetworkTimeout cannot be called with a negative timeout");
        }
        if (protocol != null) {
            protocol.startCallInterface();
        }

        SQLPermission sqlPermission = new SQLPermission("setNetworkTimeout");
        SecurityManager securityManager = System.getSecurityManager();
        if (securityManager != null) {
            securityManager.checkPermission(sqlPermission);
        }
        try {
            stateFlag |= ConnectionState.STATE_NETWORK_TIMEOUT;
            protocol.setTimeout(milliseconds);
        } catch (SocketException se) {
            throw exceptionFactory.create("Cannot set the network timeout", se);
        } finally {
            if (protocol != null) {
                protocol.endCallInterface("OceanBaseConnection.setNetworkTimeout");
            }
        }
    }

    public long getServerThreadId() {
        return protocol.getServerThreadId();
    }

    public boolean canUseServerTimeout() {
        return canUseServerTimeout;
    }

    public void setDefaultTransactionIsolation(int defaultTransactionIsolation) {
        this.defaultTransactionIsolation = defaultTransactionIsolation;
    }

    /**
     * Reset connection set has it was after creating a "fresh" new connection.
     * defaultTransactionIsolation must have been initialized.
     *
     * <p>BUT : - session variable state are reset only if option useResetConnection is set and - if
     * using the option "useServerPrepStmts", PREPARE statement are still prepared
     *
     * @throws SQLException if resetting operation failed
     */
    public void reset() throws SQLException {
        // COM_RESET_CONNECTION exist since mysql 5.7.3 and mariadb 10.2.4
        // but not possible to use it with mysql waiting for https://bugs.mysql.com/bug.php?id=97633
        // correction.
        // and mariadb only since https://jira.mariadb.org/browse/MDEV-18281
        boolean useComReset = options.useResetConnection
                              && protocol.isServerMariaDb()
                              && (protocol.versionGreaterOrEqual(10, 3, 13) || (protocol
                                  .getMajorServerVersion() == 10
                                                                                && protocol
                                                                                    .getMinorServerVersion() == 2 && protocol
                                  .versionGreaterOrEqual(10, 2, 22)));

        if (useComReset) {
            protocol.reset();
        }

        if (stateFlag != 0) {

            try {

                if ((stateFlag & ConnectionState.STATE_NETWORK_TIMEOUT) != 0) {
                    setNetworkTimeout(null, options.socketTimeout);
                }

                if ((stateFlag & ConnectionState.STATE_AUTOCOMMIT) != 0) {
                    setAutoCommit(options.autocommit);
                }

                if ((stateFlag & ConnectionState.STATE_DATABASE) != 0) {
                    protocol.resetDatabase();
                }

                if ((stateFlag & ConnectionState.STATE_READ_ONLY) != 0) {
                    setReadOnly(false); // default to master connection
                }

                // COM_RESET_CONNECTION reset transaction isolation
                if (!useComReset && (stateFlag & ConnectionState.STATE_TRANSACTION_ISOLATION) != 0) {
                    setTransactionIsolation(defaultTransactionIsolation);
                }

                stateFlag = 0;

            } catch (SQLException sqle) {
                throw exceptionFactory.create("error resetting connection");
            }
        }

        warningsCleared = true;
    }

    public void checkClosed() throws SQLException {
        if (isClosed()) {
            throw new SQLException("No operations allowed after connection closed.");
        }
    }

    public boolean includeDeadLockInfo() {
        return options.includeInnodbStatusInDeadlockExceptions;
    }

    public boolean includeThreadsTraces() {
        return options.includeThreadDumpInDeadlockExceptions;
    }

    @Override
    public String getSessionTimeZone() {
        return sessionTimeZone.getID();
    }

    public java.sql.Statement getMetadataSafeStatement() throws SQLException {
        java.sql.Statement stmt = createStatement();

        if (stmt.getMaxRows() != 0) {
            stmt.setMaxRows(0);
        }

        stmt.setEscapeProcessing(false);

        if (stmt.getFetchSize() != 0) {
            stmt.setFetchSize(0);
        }

        stmt.closeOnCompletion();
        ((OceanBaseStatement) stmt).setInternal();

        return stmt;
    }

    @Override
    public void setSessionTimeZone(String zoneID) throws SQLException {
        checkConnection();
        if (this.protocol.isOracleMode()) {
            boolean needSetSessionTimeZone = true;
            TimeZone targetTimeZone = TimeZone.getTimeZone(zoneID);
            if (!protocol.isTZTablesImported()) {
                if (null != sessionTimeZone
                    && targetTimeZone.getRawOffset() == sessionTimeZone.getRawOffset()) {
                    needSetSessionTimeZone = false;
                }
            } else {
                if (null != sessionTimeZone && sessionTimeZone.getID().equals(zoneID)) {
                    needSetSessionTimeZone = false;
                }
            }
            if (needSetSessionTimeZone) {
                if (protocol != null) {
                    protocol.startCallInterface();
                }

                java.sql.Statement stmt = getMetadataSafeStatement();
                try {
                    String sql = String.format("alter session set time_zone = '%s'", zoneID);
                    stmt.execute(sql);
                    sessionTimeZone = targetTimeZone;
                } catch (SQLException e) {
                    throw e;
                } finally {
                    if (stmt != null) {
                        stmt.close();
                    }
                    if (protocol != null) {
                        protocol.endCallInterface("OceanBaseConnection.setSessionTimeZone");
                    }
                }
            }
        } else {
            throw new SQLFeatureNotSupportedException();
        }

    }

    public boolean isInGlobalTx() {
        return this.isInGlobalTx;
    }

    public void setInGlobalTx(boolean flag) {
        this.isInGlobalTx = flag;
    }

    /*
        Get the interactive time of jdbc api to send sql statement and get response in  milliseconds.
        Throw an exception when the network statistics flag is false.
     */
    public long getLastPacketCostTime() throws SQLException {
        return protocol.getLastPacketCostTime();
    }

    /*
      Set the network statistics flag.
     */
    public void networkStatistics(boolean flag) {
        protocol.setNetworkStatisticsFlag(flag);
    }

    /*
       Clean up the remnants of the previous interaction time
     */
    public void clearNetworkStatistics() {
        protocol.clearNetworkStatistics();
    }

    /**
     *
     * @return the timestamp of the returned packet that received the previous packet
     */
    public long getLastPacketResponseTimestamp() {
        return protocol.getLastPacketResponseTimestamp();
    }

    /**
     *
     * @return  the timestamp when the last packet was sent
     */
    public long getLastPacketSendTimestamp() {
        return protocol.getLastPacketSendTimestamp();
    }

    @Override
    public void changeUser(String userName, String newPassword) throws SQLException {
        if (protocol != null) {
            protocol.startCallInterface();
        }

        if ((userName == null) || userName.equals("")) {
            userName = "";
        }
        if (newPassword == null) {
            newPassword = "";
        }
        protocol.changeUser(userName, newPassword);

        if (protocol != null) {
            protocol.endCallInterface("OceanBaseConnection.changeUser");
        }
    }

    @Override
    public void setRemarksReporting(boolean value) {
        remarksReporting = value;
    }

    @Override
    public boolean getRemarksReporting() {
        return remarksReporting;
    }

    public void setFullLinkTraceModule(String module, String action) {
        protocol.setFullLinkTraceModule(module, action);
    }

    public String getFullLinkTraceModule() {
        return protocol.getFullLinkTraceModule();
    }

    public void setFullLinkTraceAction(String action) {
        protocol.setFullLinkTraceAction(action);
    }

    public String getFullLinkTraceAction() {
        return protocol.getFullLinkTraceAction();
    }

    public void setFullLinkTraceClientInfo(String clientInfo) {
        protocol.setFullLinkTraceClientInfo(clientInfo);
    }

    public String getFullLinkTraceClientInfo() {
        return protocol.getFullLinkTraceClientInfo();
    }

    public void setFullLinkTraceIdentifier(String clientIdentifier) {
        protocol.setFullLinkTraceIdentifier(clientIdentifier);
    }

    public String getFullLinkTraceIdentifier() {
        return protocol.getFullLinkTraceIdentifier();
    }

    public byte getFullLinkTraceLevel() {
        return protocol.getFullLinkTraceLevel();
    }

    public double getFullLinkTraceSamplePercentage() {
        return protocol.getFullLinkTraceSamplePercentage();
    }

    public byte getFullLinkTraceRecordPolicy() {
        return protocol.getFullLinkTraceRecordPolicy();
    }

    public double getFullLinkTracePrintSamplePercentage() {
        return protocol.getFullLinkTracePrintSamplePercentage();
    }

    public long getFullLinkTraceSlowQueryThreshold() {
        return protocol.getFullLinkTraceSlowQueryThreshold();
    }

}
